// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"io"
	"os"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/xlsx"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeReadXLSX executes a read_xlsx table function.
func (e *Executor) executeReadXLSX(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := xlsx.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "sheet":
			if s, ok := val.(string); ok {
				opts.Sheet = s
			}
		case "sheet_index":
			switch v := val.(type) {
			case int64:
				opts.SheetIndex = int(v)
			case int:
				opts.SheetIndex = v
			}
		case "range":
			if s, ok := val.(string); ok {
				opts.Range = s
			}
		case "header":
			if b, ok := val.(bool); ok {
				opts.Header = b
			}
		case "skip":
			switch v := val.(type) {
			case int64:
				opts.Skip = int(v)
			case int:
				opts.Skip = v
			}
		case "empty_as_null":
			if b, ok := val.(bool); ok {
				opts.EmptyAsNull = b
			}
		case "start_row":
			switch v := val.(type) {
			case int64:
				opts.StartRow = int(v)
			case int:
				opts.StartRow = v
			}
		case "end_row":
			switch v := val.(type) {
			case int64:
				opts.EndRow = int(v)
			case int:
				opts.EndRow = v
			}
		case "start_col":
			if s, ok := val.(string); ok {
				opts.StartCol = s
			}
		case "end_col":
			if s, ok := val.(string); ok {
				opts.EndCol = s
			}
		case "infer_types":
			if b, ok := val.(bool); ok {
				opts.InferTypes = b
			}
		case "date_format":
			if s, ok := val.(string); ok {
				opts.DateFormat = s
			}
		case "null_values":
			// Handle null values as a list of strings
			switch v := val.(type) {
			case []string:
				opts.NullValues = v
			case []any:
				nullVals := make([]string, 0, len(v))
				for _, nv := range v {
					if s, ok := nv.(string); ok {
						nullVals = append(nullVals, s)
					}
				}
				opts.NullValues = nullVals
			}
		}
	}

	// Create the XLSX reader - use filesystem for cloud URLs, local file for local paths
	var reader *xlsx.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open XLSX file: %v", err),
			}
		}
		closer = file

		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create XLSX reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		file, err := os.Open(plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open XLSX file: %v", err),
			}
		}
		closer = file

		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create XLSX reader: %v", err),
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
			Msg:  fmt.Sprintf("failed to get XLSX schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get XLSX types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read XLSX chunk: %v", err),
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

// executeReadXLSXAuto executes a read_xlsx_auto table function with automatic format detection.
// This function uses auto-detection for header row and column types.
// It is a convenience wrapper around read_xlsx with sensible defaults for automatic operation:
// - Header: true (first row is header)
// - InferTypes: true (automatically detect column types)
// - EmptyAsNull: true (treat empty cells as NULL)
func (e *Executor) executeReadXLSXAuto(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Use default reader options which enable auto-detection
	opts := xlsx.DefaultReaderOptions()

	// Ensure auto-detection options are enabled
	opts.Header = true
	opts.InferTypes = true
	opts.EmptyAsNull = true

	// Create the XLSX reader - use filesystem for cloud URLs, local file for local paths
	var reader *xlsx.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open XLSX file: %v", err),
			}
		}
		closer = file

		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create XLSX reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		file, err := os.Open(plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to open XLSX file: %v", err),
			}
		}
		closer = file

		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create XLSX reader: %v", err),
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
			Msg:  fmt.Sprintf("failed to get XLSX schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get XLSX types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read XLSX chunk: %v", err),
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
