// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/csv"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeTableFunctionScan executes a table function scan (read_csv, read_json, etc.).
func (e *Executor) executeTableFunctionScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	switch strings.ToLower(plan.FunctionName) {
	case "read_csv":
		return e.executeReadCSV(ctx, plan)
	case "read_csv_auto":
		return e.executeReadCSVAuto(ctx, plan)
	case "read_json":
		return e.executeReadJSON(ctx, plan)
	case "read_json_auto":
		return e.executeReadJSONAuto(ctx, plan)
	case "read_ndjson":
		return e.executeReadNDJSON(ctx, plan)
	case "read_parquet":
		return e.executeReadParquet(ctx, plan)
	case "read_arrow":
		return e.executeReadArrow(ctx, plan)
	case "read_arrow_auto":
		return e.executeReadArrowAuto(ctx, plan)
	// Secret system functions
	case "which_secret":
		return e.executeWhichSecret(ctx, plan)
	case "duckdb_secrets":
		return e.executeDuckDBSecrets(ctx, plan)
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("unknown table function: %s", plan.FunctionName),
		}
	}
}

// executeReadCSV executes a read_csv table function.
func (e *Executor) executeReadCSV(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := csv.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "delimiter", "delim", "sep":
			if s, ok := val.(string); ok && s != "" {
				opts.Delimiter = rune(s[0])
			}
		case "quote":
			if s, ok := val.(string); ok && s != "" {
				opts.Quote = rune(s[0])
			}
		case "header":
			if b, ok := val.(bool); ok {
				opts.Header = b
			}
		case "nullstr", "null":
			if s, ok := val.(string); ok {
				opts.NullStr = s
			}
		case "skip":
			switch v := val.(type) {
			case int64:
				opts.Skip = int(v)
			case int:
				opts.Skip = v
			}
		case "ignore_errors":
			if b, ok := val.(bool); ok {
				opts.IgnoreErrors = b
			}
		}
	}

	// Create the CSV reader - use filesystem for cloud URLs, local file for local paths
	var reader *csv.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open CSV file: %v", err),
			}
		}
		closer = file

		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create CSV reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		file, err := os.Open(plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open CSV file: %v", err),
			}
		}
		closer = file

		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create CSV reader: %v", err),
			}
		}
	}
	defer func() {
		_ = reader.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get CSV schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get CSV types: %v", err),
		}
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = make([]*catalog.ColumnDef, len(columns))
		for i, colName := range columns {
			var colType dukdb.Type
			if i < len(types) {
				colType = types[i]
			} else {
				colType = dukdb.TYPE_VARCHAR
			}
			plan.TableFunction.Columns[i] = catalog.NewColumnDef(colName, colType)
		}
	}

	// Collect all rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: columns,
	}

	// Read all chunks
	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read CSV chunk: %v", err),
			}
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j := 0; j < len(columns); j++ {
				row[columns[j]] = chunk.GetValue(i, j)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// openFileForReading opens a file for reading using the filesystem provider.
// This supports both local and cloud URLs (S3, GCS, Azure, HTTP/HTTPS).
func (e *Executor) openFileForReading(ctx context.Context, path string) (filesystem.File, error) {
	provider := NewFileSystemProvider(e.getSecretManager())

	fs, err := provider.GetFileSystem(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get filesystem: %w", err)
	}

	// For cloud filesystems with context support, use OpenContext
	if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
		return ctxFS.OpenContext(ctx, path)
	}

	return fs.Open(path)
}

// executeReadCSVAuto executes a read_csv_auto table function with automatic format detection.
// This function uses auto-detection for delimiter, quote character, header row, and column types.
func (e *Executor) executeReadCSVAuto(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Use default reader options which enable auto-detection:
	// - Delimiter: 0 (auto-detect comma, tab, semicolon, pipe)
	// - Header: true (first row is header)
	// - Quote: '"' (default quote character)
	// Type inference is also automatically performed
	opts := csv.DefaultReaderOptions()

	// Create the CSV reader - use filesystem for cloud URLs, local file for local paths
	var reader *csv.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open CSV file: %v", err),
			}
		}
		closer = file

		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create CSV reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		file, err := os.Open(plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open CSV file: %v", err),
			}
		}
		closer = file

		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create CSV reader: %v", err),
			}
		}
	}
	defer func() {
		_ = reader.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get CSV schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get CSV types: %v", err),
		}
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = make([]*catalog.ColumnDef, len(columns))
		for i, colName := range columns {
			var colType dukdb.Type
			if i < len(types) {
				colType = types[i]
			} else {
				colType = dukdb.TYPE_VARCHAR
			}
			plan.TableFunction.Columns[i] = catalog.NewColumnDef(colName, colType)
		}
	}

	// Collect all rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: columns,
	}

	// Read all chunks
	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read CSV chunk: %v", err),
			}
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j := 0; j < len(columns); j++ {
				row[columns[j]] = chunk.GetValue(i, j)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}
