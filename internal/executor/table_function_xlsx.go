// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/xlsx"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// XLSXMultiFileOptions contains options for multi-file XLSX reading.
type XLSXMultiFileOptions struct {
	// Filename adds a filename column to the output.
	Filename bool
	// FileRowNumber adds a 1-indexed row number within each file.
	FileRowNumber bool
	// FileIndex adds the 0-indexed file index.
	FileIndex bool
	// FilesToSniff is the number of files to sample for schema detection (default: 1).
	FilesToSniff int
	// FileGlobBehavior controls handling of empty glob results.
	FileGlobBehavior FileGlobBehavior
	// UnionByName aligns schemas by column name (default: true).
	UnionByName bool
}

// DefaultXLSXMultiFileOptions returns the default options for multi-file XLSX reading.
func DefaultXLSXMultiFileOptions() *XLSXMultiFileOptions {
	return &XLSXMultiFileOptions{
		Filename:         false,
		FileRowNumber:    false,
		FileIndex:        false,
		FilesToSniff:     1,
		FileGlobBehavior: FileGlobDisallowEmpty,
		UnionByName:      true,
	}
}

// executeReadXLSX executes a read_xlsx table function.
func (e *Executor) executeReadXLSX(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := xlsx.DefaultReaderOptions()
	multiOpts := DefaultXLSXMultiFileOptions()

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
		// Multi-file options
		case "filename":
			if b, ok := val.(bool); ok {
				multiOpts.Filename = b
			}
		case "file_row_number":
			if b, ok := val.(bool); ok {
				multiOpts.FileRowNumber = b
			}
		case "file_index":
			if b, ok := val.(bool); ok {
				multiOpts.FileIndex = b
			}
		case "files_to_sniff":
			switch v := val.(type) {
			case int64:
				multiOpts.FilesToSniff = int(v)
			case int:
				multiOpts.FilesToSniff = v
			}
		case "file_glob_behavior":
			if s, ok := val.(string); ok {
				switch strings.ToUpper(s) {
				case "DISALLOW_EMPTY":
					multiOpts.FileGlobBehavior = FileGlobDisallowEmpty
				case "ALLOW_EMPTY":
					multiOpts.FileGlobBehavior = FileGlobAllowEmpty
				case "FALLBACK_GLOB":
					multiOpts.FileGlobBehavior = FileGlobFallback
				}
			}
		case "union_by_name":
			if b, ok := val.(bool); ok {
				multiOpts.UnionByName = b
			}
		}
	}

	// Get file paths - check for array syntax first, then glob patterns
	var paths []string
	var err error

	if plan.TableFunction != nil && len(plan.TableFunction.Paths) > 0 {
		// Array syntax: read_xlsx(['file1.xlsx', 'file2.xlsx'])
		// Expand any glob patterns within the array
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandXLSXGlobPattern(ctx.Context, p, multiOpts)
				if err != nil {
					return nil, err
				}
				paths = append(paths, expanded...)
			} else {
				paths = append(paths, p)
			}
		}
	} else {
		// Single path or glob pattern
		paths, err = e.resolveXLSXFilePaths(ctx.Context, plan.Path, multiOpts)
		if err != nil {
			return nil, err
		}
	}

	// Handle empty results based on FileGlobBehavior
	if len(paths) == 0 {
		if multiOpts.FileGlobBehavior == FileGlobAllowEmpty {
			return &ExecutionResult{
				Rows:    make([]map[string]any, 0),
				Columns: []string{},
			}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("no files match pattern: %s", plan.Path),
		}
	}

	// Single file path - use optimized single file reader
	if len(paths) == 1 && !multiOpts.Filename && !multiOpts.FileRowNumber && !multiOpts.FileIndex {
		return e.executeReadXLSXSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadXLSXMultiFile(ctx, paths, opts, multiOpts, plan)
}

// resolveXLSXFilePaths resolves a path or glob pattern to a list of file paths.
func (e *Executor) resolveXLSXFilePaths(ctx context.Context, pathOrPattern string, multiOpts *XLSXMultiFileOptions) ([]string, error) {
	// Check if it's a glob pattern
	if fileio.IsGlobPattern(pathOrPattern) {
		return e.expandXLSXGlobPattern(ctx, pathOrPattern, multiOpts)
	}

	// Single path - verify it exists
	var fs filesystem.FileSystem
	if filesystem.IsCloudURL(pathOrPattern) {
		provider := NewFileSystemProvider(e.getSecretManager())
		var err error
		fs, err = provider.GetFileSystem(ctx, pathOrPattern)
		if err != nil {
			return nil, fmt.Errorf("failed to get filesystem: %w", err)
		}
	} else {
		fs = filesystem.NewLocalFileSystem("")
	}

	exists, err := fs.Exists(pathOrPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to check file existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("file not found: %s", pathOrPattern)
	}

	return []string{pathOrPattern}, nil
}

// expandXLSXGlobPattern expands a glob pattern to a list of file paths.
func (e *Executor) expandXLSXGlobPattern(ctx context.Context, pattern string, multiOpts *XLSXMultiFileOptions) ([]string, error) {
	var fs filesystem.FileSystem
	if filesystem.IsCloudURL(pattern) {
		provider := NewFileSystemProvider(e.getSecretManager())
		var err error
		fs, err = provider.GetFileSystem(ctx, pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to get filesystem for glob: %w", err)
		}
	} else {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Use the filesystem's glob method
	paths, err := fs.Glob(pattern)
	if err != nil {
		// Handle FALLBACK_GLOB behavior
		if multiOpts.FileGlobBehavior == FileGlobFallback {
			// Treat the pattern as a literal path
			exists, existErr := fs.Exists(pattern)
			if existErr == nil && exists {
				return []string{pattern}, nil
			}
		}
		return nil, fmt.Errorf("glob expansion failed: %w", err)
	}

	if len(paths) == 0 && multiOpts.FileGlobBehavior == FileGlobFallback {
		// Treat the pattern as a literal path
		exists, existErr := fs.Exists(pattern)
		if existErr == nil && exists {
			return []string{pattern}, nil
		}
	}

	// Sort paths alphabetically
	sort.Strings(paths)

	// Check max files limit
	if err := fileio.ValidateMaxFiles(len(paths), nil); err != nil {
		return nil, err
	}

	return paths, nil
}

// executeReadXLSXSingleFile executes read_xlsx for a single file (optimized path).
func (e *Executor) executeReadXLSXSingleFile(
	ctx *ExecutionContext,
	path string,
	opts *xlsx.ReaderOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Create the XLSX reader - use filesystem for cloud URLs, local file for local paths
	var reader *xlsx.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, path)
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
		file, err := os.Open(path)
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

// executeReadXLSXMultiFile executes read_xlsx for multiple files with schema merging.
func (e *Executor) executeReadXLSXMultiFile(
	ctx *ExecutionContext,
	paths []string,
	opts *xlsx.ReaderOptions,
	multiOpts *XLSXMultiFileOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Determine which files to sniff for schema detection
	filesToSniff := multiOpts.FilesToSniff
	if filesToSniff <= 0 {
		filesToSniff = 1
	}
	if filesToSniff > len(paths) {
		filesToSniff = len(paths)
	}

	// Collect schemas from files to sniff
	fileSchemas := make([]fileio.FileSchema, len(paths))
	for i, path := range paths {
		if i < filesToSniff {
			// Actually read schema from file
			schema, err := e.sniffXLSXSchema(ctx.Context, path, opts)
			if err != nil {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeIO,
					Msg:  fmt.Sprintf("failed to sniff schema from %s: %v", path, err),
				}
			}
			fileSchemas[i] = schema
		} else {
			// Use first file's schema for remaining files
			fileSchemas[i] = fileSchemas[0]
		}
		fileSchemas[i].Path = path
	}

	// Merge schemas
	mergeResult, err := fileio.MergeSchemas(fileSchemas, multiOpts.UnionByName)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to merge schemas: %v", err),
		}
	}

	// Determine output columns (data columns + metadata columns)
	outputColumns := mergeResult.Names()
	outputTypes := mergeResult.Types()

	// Track metadata column positions
	metadataStartIdx := len(outputColumns)
	var filenameColIdx, fileRowNumColIdx, fileIndexColIdx int = -1, -1, -1

	if multiOpts.Filename {
		filenameColIdx = len(outputColumns)
		outputColumns = append(outputColumns, "filename")
		outputTypes = append(outputTypes, dukdb.TYPE_VARCHAR)
	}
	if multiOpts.FileRowNumber {
		fileRowNumColIdx = len(outputColumns)
		outputColumns = append(outputColumns, "file_row_number")
		outputTypes = append(outputTypes, dukdb.TYPE_BIGINT)
	}
	if multiOpts.FileIndex {
		fileIndexColIdx = len(outputColumns)
		outputColumns = append(outputColumns, "file_index")
		outputTypes = append(outputTypes, dukdb.TYPE_INTEGER)
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = make([]*catalog.ColumnDef, len(outputColumns))
		for i, colName := range outputColumns {
			plan.TableFunction.Columns[i] = catalog.NewColumnDef(colName, outputTypes[i])
		}
	}

	// Read all files and combine results
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: outputColumns,
	}

	for fileIdx, path := range paths {
		// Read this file
		chunks, err := e.readXLSXFileChunks(ctx.Context, path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read %s: %v", path, err),
			}
		}

		// Process each chunk
		fileRowNumber := int64(0)
		for _, chunk := range chunks {
			if chunk == nil || chunk.Count() == 0 {
				continue
			}

			// Align chunk to merged schema
			mapping := mergeResult.Mappings[fileIdx]
			aligned := fileio.AlignChunk(chunk, mapping, mergeResult.Types())

			// Convert to rows and add metadata
			baseColumnNames := mergeResult.Names()
			for i := 0; i < aligned.Count(); i++ {
				fileRowNumber++
				row := make(map[string]any)

				// Add data columns
				for j := 0; j < len(baseColumnNames); j++ {
					row[baseColumnNames[j]] = aligned.GetValue(i, j)
				}

				// Add metadata columns
				if filenameColIdx >= 0 {
					row["filename"] = path
				}
				if fileRowNumColIdx >= 0 {
					row["file_row_number"] = fileRowNumber
				}
				if fileIndexColIdx >= 0 {
					row["file_index"] = int32(fileIdx)
				}

				result.Rows = append(result.Rows, row)
			}
		}
	}

	// Suppress warnings about unused variables
	_ = metadataStartIdx

	return result, nil
}

// sniffXLSXSchema reads the schema from an XLSX file without reading all data.
func (e *Executor) sniffXLSXSchema(ctx context.Context, path string, opts *xlsx.ReaderOptions) (fileio.FileSchema, error) {
	var reader *xlsx.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open XLSX file: %w", err)
		}
		closer = file
		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return fileio.FileSchema{}, fmt.Errorf("failed to create XLSX reader: %w", err)
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open XLSX file: %w", err)
		}
		closer = file
		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return fileio.FileSchema{}, fmt.Errorf("failed to create XLSX reader: %w", err)
		}
	}
	defer func() {
		_ = reader.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()

	columns, err := reader.Schema()
	if err != nil {
		return fileio.FileSchema{}, fmt.Errorf("failed to get schema: %w", err)
	}

	types, err := reader.Types()
	if err != nil {
		return fileio.FileSchema{}, fmt.Errorf("failed to get types: %w", err)
	}

	schema := fileio.FileSchema{
		Path:    path,
		Columns: make([]fileio.ColumnInfo, len(columns)),
	}
	for i, col := range columns {
		colType := dukdb.TYPE_VARCHAR
		if i < len(types) {
			colType = types[i]
		}
		schema.Columns[i] = fileio.ColumnInfo{
			Name: col,
			Type: colType,
		}
	}

	return schema, nil
}

// readXLSXFileChunks reads all chunks from an XLSX file.
func (e *Executor) readXLSXFileChunks(ctx context.Context, path string, opts *xlsx.ReaderOptions) ([]*storage.DataChunk, error) {
	var reader *xlsx.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to open XLSX file: %w", err)
		}
		closer = file
		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to create XLSX reader: %w", err)
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open XLSX file: %w", err)
		}
		closer = file
		reader, err = xlsx.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to create XLSX reader: %w", err)
		}
	}
	defer func() {
		_ = reader.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()

	var chunks []*storage.DataChunk
	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
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
	multiOpts := DefaultXLSXMultiFileOptions()

	// Ensure auto-detection options are enabled
	opts.Header = true
	opts.InferTypes = true
	opts.EmptyAsNull = true

	// Apply multi-file options from query options
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "filename":
			if b, ok := val.(bool); ok {
				multiOpts.Filename = b
			}
		case "file_row_number":
			if b, ok := val.(bool); ok {
				multiOpts.FileRowNumber = b
			}
		case "file_index":
			if b, ok := val.(bool); ok {
				multiOpts.FileIndex = b
			}
		case "files_to_sniff":
			switch v := val.(type) {
			case int64:
				multiOpts.FilesToSniff = int(v)
			case int:
				multiOpts.FilesToSniff = v
			}
		case "file_glob_behavior":
			if s, ok := val.(string); ok {
				switch strings.ToUpper(s) {
				case "DISALLOW_EMPTY":
					multiOpts.FileGlobBehavior = FileGlobDisallowEmpty
				case "ALLOW_EMPTY":
					multiOpts.FileGlobBehavior = FileGlobAllowEmpty
				case "FALLBACK_GLOB":
					multiOpts.FileGlobBehavior = FileGlobFallback
				}
			}
		case "union_by_name":
			if b, ok := val.(bool); ok {
				multiOpts.UnionByName = b
			}
		}
	}

	// Get file paths - check for array syntax first, then glob patterns
	var paths []string
	var err error

	if plan.TableFunction != nil && len(plan.TableFunction.Paths) > 0 {
		// Array syntax: read_xlsx_auto(['file1.xlsx', 'file2.xlsx'])
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandXLSXGlobPattern(ctx.Context, p, multiOpts)
				if err != nil {
					return nil, err
				}
				paths = append(paths, expanded...)
			} else {
				paths = append(paths, p)
			}
		}
	} else {
		paths, err = e.resolveXLSXFilePaths(ctx.Context, plan.Path, multiOpts)
		if err != nil {
			return nil, err
		}
	}

	// Handle empty results based on FileGlobBehavior
	if len(paths) == 0 {
		if multiOpts.FileGlobBehavior == FileGlobAllowEmpty {
			return &ExecutionResult{
				Rows:    make([]map[string]any, 0),
				Columns: []string{},
			}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("no files match pattern: %s", plan.Path),
		}
	}

	// Check if we need multi-file handling
	if len(paths) == 1 && !multiOpts.Filename && !multiOpts.FileRowNumber && !multiOpts.FileIndex {
		return e.executeReadXLSXSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadXLSXMultiFile(ctx, paths, opts, multiOpts, plan)
}
