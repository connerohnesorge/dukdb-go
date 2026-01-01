// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"io"
	"os"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/json"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeReadJSON executes a read_json table function.
func (e *Executor) executeReadJSON(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Open the file
	file, err := os.Open(plan.Path)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to open JSON file: %v", err),
		}
	}
	defer func() { _ = file.Close() }()

	// Build reader options from plan options
	opts := json.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "format":
			if s, ok := val.(string); ok {
				switch strings.ToLower(s) {
				case "array":
					opts.Format = json.FormatArray
				case "newline_delimited", "ndjson":
					opts.Format = json.FormatNDJSON
				case "auto":
					opts.Format = json.FormatAuto
				}
			}
		case "maximum_depth", "max_depth":
			switch v := val.(type) {
			case int64:
				opts.MaxDepth = int(v)
			case int:
				opts.MaxDepth = v
			}
		case "sample_size":
			switch v := val.(type) {
			case int64:
				opts.SampleSize = int(v)
			case int:
				opts.SampleSize = v
			}
		case "ignore_errors":
			if b, ok := val.(bool); ok {
				opts.IgnoreErrors = b
			}
		case "date_format":
			if s, ok := val.(string); ok {
				opts.DateFormat = s
			}
		case "timestamp_format":
			if s, ok := val.(string); ok {
				opts.TimestampFormat = s
			}
		}
	}

	// Create the JSON reader
	reader, err := json.NewReader(file, opts)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create JSON reader: %v", err),
		}
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get JSON schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get JSON types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read JSON chunk: %v", err),
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

// executeReadJSONAuto executes a read_json_auto table function with automatic format detection.
// This function uses auto-detection for format (JSON array vs NDJSON) and schema.
func (e *Executor) executeReadJSONAuto(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Open the file
	file, err := os.Open(plan.Path)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to open JSON file: %v", err),
		}
	}
	defer func() { _ = file.Close() }()

	// Use default reader options which enable auto-detection:
	// - Format: "auto" (auto-detect array vs NDJSON)
	// - Type inference is automatically performed
	opts := json.DefaultReaderOptions()

	// Create the JSON reader with auto-detection
	reader, err := json.NewReader(file, opts)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create JSON reader: %v", err),
		}
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get JSON schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get JSON types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read JSON chunk: %v", err),
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

// executeReadNDJSON executes a read_ndjson table function.
// This is an alias for read_json with format='newline_delimited'.
func (e *Executor) executeReadNDJSON(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Open the file
	file, err := os.Open(plan.Path)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to open NDJSON file: %v", err),
		}
	}
	defer func() { _ = file.Close() }()

	// Build reader options with NDJSON format
	opts := json.DefaultReaderOptions()
	opts.Format = json.FormatNDJSON

	// Apply any additional options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "maximum_depth", "max_depth":
			switch v := val.(type) {
			case int64:
				opts.MaxDepth = int(v)
			case int:
				opts.MaxDepth = v
			}
		case "sample_size":
			switch v := val.(type) {
			case int64:
				opts.SampleSize = int(v)
			case int:
				opts.SampleSize = v
			}
		case "ignore_errors":
			if b, ok := val.(bool); ok {
				opts.IgnoreErrors = b
			}
		case "date_format":
			if s, ok := val.(string); ok {
				opts.DateFormat = s
			}
		case "timestamp_format":
			if s, ok := val.(string); ok {
				opts.TimestampFormat = s
			}
		}
	}

	// Create the JSON reader
	reader, err := json.NewReader(file, opts)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create NDJSON reader: %v", err),
		}
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get NDJSON schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get NDJSON types: %v", err),
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
				Msg:  fmt.Sprintf("failed to read NDJSON chunk: %v", err),
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
