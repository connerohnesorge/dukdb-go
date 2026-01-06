// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"io"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/arrow"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeReadArrow executes a read_arrow table function.
// This function reads Arrow IPC file format (random access).
func (e *Executor) executeReadArrow(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := arrow.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "columns":
			// Handle column projection specified as an option
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

	// Create the Arrow reader - use filesystem for cloud URLs, local file for local paths
	var reader *arrow.Reader
	var err error

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		reader, err = e.createCloudArrowReader(ctx, plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Arrow reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		reader, err = arrow.NewReaderFromPath(plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Arrow reader: %v", err),
			}
		}
	}
	defer func() { _ = reader.Close() }()

	return e.readArrowData(reader, plan)
}

// executeReadArrowAuto executes a read_arrow_auto table function.
// This function auto-detects the Arrow IPC format (file vs stream).
func (e *Executor) executeReadArrowAuto(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := arrow.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "columns":
			// Handle column projection specified as an option
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

	// Detect format from file extension
	format := arrow.DetectFormatFromPath(plan.Path)

	if format == arrow.FormatStream {
		// Use stream reader for stream format
		return e.executeReadArrowStream(ctx, plan, opts)
	}

	// Try file format first
	if filesystem.IsCloudURL(plan.Path) {
		reader, err := e.createCloudArrowReader(ctx, plan.Path, opts)
		if err != nil {
			// Try stream format as fallback
			return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
		}
		defer func() { _ = reader.Close() }()
		return e.readArrowData(reader, plan)
	}

	reader, err := arrow.NewReaderFromPath(plan.Path, opts)
	if err != nil {
		// Try stream format as fallback
		return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
	}
	defer func() { _ = reader.Close() }()

	return e.readArrowData(reader, plan)
}

// executeReadArrowStream reads an Arrow IPC stream format file.
func (e *Executor) executeReadArrowStream(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
	opts *arrow.ReaderOptions,
) (*ExecutionResult, error) {
	return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
}

// executeReadArrowStreamWithOpts reads an Arrow IPC stream with custom options.
func (e *Executor) executeReadArrowStreamWithOpts(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
	opts *arrow.ReaderOptions,
) (*ExecutionResult, error) {
	var reader *arrow.StreamReader
	var err error

	if filesystem.IsCloudURL(plan.Path) {
		// Use FileSystemProvider for cloud URLs
		reader, err = e.createCloudArrowStreamReader(ctx, plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Arrow stream reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		reader, err = arrow.NewStreamReaderFromPath(plan.Path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Arrow stream reader: %v", err),
			}
		}
	}
	defer func() { _ = reader.Close() }()

	return e.readArrowStreamData(reader, plan)
}

// readArrowData reads data from an Arrow file reader.
func (e *Executor) readArrowData(
	reader *arrow.Reader,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Arrow schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Arrow types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read Arrow chunk: %v", err),
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

// readArrowStreamData reads data from an Arrow stream reader.
func (e *Executor) readArrowStreamData(
	reader *arrow.StreamReader,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Arrow schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Arrow types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read Arrow chunk: %v", err),
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

// createCloudArrowReader creates an Arrow reader for cloud URLs.
// Arrow IPC file format requires a ReadAtSeeker, so for non-seekable cloud streams,
// we read the entire file into memory first.
func (e *Executor) createCloudArrowReader(
	ctx *ExecutionContext,
	path string,
	opts *arrow.ReaderOptions,
) (*arrow.Reader, error) {
	file, err := e.openFileForReading(ctx.Context, path)
	if err != nil {
		return nil, err
	}

	// Check if the file supports ReadAt (needed for Arrow IPC file format)
	if ras, ok := file.(arrow.ReadAtSeeker); ok {
		// Create reader with the seekable file - the Reader will close it
		return arrow.NewReader(ras, opts)
	}

	// Fallback: read entire file into memory for non-seekable streams
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read arrow data: %w", err)
	}

	// Create a bytes reader that implements ReadAtSeeker
	br := &bytesReaderAt{data: data}
	return arrow.NewReader(br, opts)
}

// createCloudArrowStreamReader creates an Arrow stream reader for cloud URLs.
func (e *Executor) createCloudArrowStreamReader(
	ctx *ExecutionContext,
	path string,
	opts *arrow.ReaderOptions,
) (*arrow.StreamReader, error) {
	file, err := e.openFileForReading(ctx.Context, path)
	if err != nil {
		return nil, err
	}

	// Create stream reader - it takes ownership of the file
	return arrow.NewStreamReader(file, opts)
}
