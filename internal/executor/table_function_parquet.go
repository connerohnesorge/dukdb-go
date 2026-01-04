// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"io"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeReadParquet executes a read_parquet table function.
func (e *Executor) executeReadParquet(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := parquet.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "columns":
			// Handle column projection specified as an option
			// This can be a list of column names
			switch v := val.(type) {
			case []string:
				opts.Columns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.Columns = cols
			}
		}
	}

	// Create the Parquet reader - use filesystem for cloud URLs, local file for local paths
	var reader *parquet.Reader
	var err error

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		reader, err = e.createCloudParquetReader(ctx, plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Parquet reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		reader, err = parquet.NewReaderFromPath(plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Parquet reader: %v", err),
			}
		}
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Parquet schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Parquet types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read Parquet chunk: %v", err),
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

// createCloudParquetReader creates a Parquet reader for cloud URLs.
// Parquet requires a ReaderAtSeeker, so for non-seekable cloud streams,
// we read the entire file into memory first.
func (e *Executor) createCloudParquetReader(
	ctx *ExecutionContext,
	path string,
	opts *parquet.ReaderOptions,
) (*parquet.Reader, error) {
	file, err := e.openFileForReading(ctx.Context, path)
	if err != nil {
		return nil, err
	}

	// Check if the file supports ReaderAt (needed for Parquet)
	if ras, ok := file.(parquet.ReaderAtSeeker); ok {
		// Get file size
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to get file size: %w", err)
		}
		// Create reader with the seekable file - the Reader will close it
		return parquet.NewReader(ras, info.Size(), opts)
	}

	// Fallback: read entire file into memory for non-seekable streams
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Create a bytes reader that implements ReaderAtSeeker
	br := &bytesReaderAt{data: data}
	return parquet.NewReader(br, int64(len(data)), opts)
}
