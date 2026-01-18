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
	"github.com/dukdb/dukdb-go/internal/io/arrow"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ArrowMultiFileOptions contains options for multi-file Arrow reading.
type ArrowMultiFileOptions struct {
	// Filename adds a filename column to the output.
	Filename bool
	// FileRowNumber adds a 1-indexed row number within each file.
	FileRowNumber bool
	// FileIndex adds the 0-indexed file index.
	FileIndex bool
	// FilesToSniff is the number of files to sample for schema detection (default: 1).
	// Note: Arrow has embedded schema, so this is mainly for schema merging validation.
	FilesToSniff int
	// FileGlobBehavior controls handling of empty glob results.
	FileGlobBehavior FileGlobBehavior
	// UnionByName aligns schemas by column name (default: true).
	UnionByName bool
}

// DefaultArrowMultiFileOptions returns the default options for multi-file Arrow reading.
func DefaultArrowMultiFileOptions() *ArrowMultiFileOptions {
	return &ArrowMultiFileOptions{
		Filename:         false,
		FileRowNumber:    false,
		FileIndex:        false,
		FilesToSniff:     1,
		FileGlobBehavior: FileGlobDisallowEmpty,
		UnionByName:      true,
	}
}

// executeReadArrow executes a read_arrow table function.
// This function reads Arrow IPC file format (random access).
// It supports glob patterns (e.g., "data/*.arrow") and multiple files with schema merging.
func (e *Executor) executeReadArrow(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := arrow.DefaultReaderOptions()
	multiOpts := DefaultArrowMultiFileOptions()

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
		// Array syntax: read_arrow(['file1.arrow', 'file2.arrow'])
		// Expand any glob patterns within the array
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandArrowGlobPattern(ctx.Context, p, multiOpts)
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
		paths, err = e.resolveArrowFilePaths(ctx.Context, plan.Path, multiOpts)
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
		return e.executeReadArrowSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadArrowMultiFile(ctx, paths, opts, multiOpts, plan)
}

// executeReadArrowAuto executes a read_arrow_auto table function.
// This function auto-detects the Arrow IPC format (file vs stream).
// It supports glob patterns (e.g., "data/*.arrow") and multiple files with schema merging.
func (e *Executor) executeReadArrowAuto(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := arrow.DefaultReaderOptions()
	multiOpts := DefaultArrowMultiFileOptions()

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
		// Array syntax: read_arrow_auto(['file1.arrow', 'file2.arrow'])
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandArrowGlobPattern(ctx.Context, p, multiOpts)
				if err != nil {
					return nil, err
				}
				paths = append(paths, expanded...)
			} else {
				paths = append(paths, p)
			}
		}
	} else {
		paths, err = e.resolveArrowFilePaths(ctx.Context, plan.Path, multiOpts)
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
		return e.executeReadArrowAutoSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadArrowMultiFile(ctx, paths, opts, multiOpts, plan)
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

// resolveArrowFilePaths resolves a path or glob pattern to a list of file paths.
func (e *Executor) resolveArrowFilePaths(
	ctx context.Context,
	pathOrPattern string,
	multiOpts *ArrowMultiFileOptions,
) ([]string, error) {
	// Check if it's a glob pattern
	if fileio.IsGlobPattern(pathOrPattern) {
		return e.expandArrowGlobPattern(ctx, pathOrPattern, multiOpts)
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

// expandArrowGlobPattern expands a glob pattern to a list of file paths.
func (e *Executor) expandArrowGlobPattern(
	ctx context.Context,
	pattern string,
	multiOpts *ArrowMultiFileOptions,
) ([]string, error) {
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

// executeReadArrowSingleFile executes read_arrow for a single file (optimized path).
func (e *Executor) executeReadArrowSingleFile(
	ctx *ExecutionContext,
	path string,
	opts *arrow.ReaderOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Create the Arrow reader - use filesystem for cloud URLs, local file for local paths
	var reader *arrow.Reader
	var err error

	if filesystem.IsCloudURL(path) {
		// Use FileSystemProvider for cloud URLs
		reader, err = e.createCloudArrowReader(ctx, path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Arrow reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		reader, err = arrow.NewReaderFromPath(path, opts)
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

// executeReadArrowAutoSingleFile executes read_arrow_auto for a single file (optimized path).
func (e *Executor) executeReadArrowAutoSingleFile(
	ctx *ExecutionContext,
	path string,
	opts *arrow.ReaderOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Detect format from file extension
	format := arrow.DetectFormatFromPath(path)

	if format == arrow.FormatStream {
		// Use stream reader for stream format
		return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
	}

	// Try file format first
	if filesystem.IsCloudURL(path) {
		reader, err := e.createCloudArrowReader(ctx, path, opts)
		if err != nil {
			// Try stream format as fallback
			return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
		}
		defer func() { _ = reader.Close() }()
		return e.readArrowData(reader, plan)
	}

	reader, err := arrow.NewReaderFromPath(path, opts)
	if err != nil {
		// Try stream format as fallback
		return e.executeReadArrowStreamWithOpts(ctx, plan, opts)
	}
	defer func() { _ = reader.Close() }()

	return e.readArrowData(reader, plan)
}

// executeReadArrowMultiFile executes read_arrow for multiple files with schema merging.
func (e *Executor) executeReadArrowMultiFile(
	ctx *ExecutionContext,
	paths []string,
	opts *arrow.ReaderOptions,
	multiOpts *ArrowMultiFileOptions,
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
			schema, err := e.sniffArrowSchema(ctx.Context, path, opts)
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
	filenameColIdx, fileRowNumColIdx, fileIndexColIdx := -1, -1, -1

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
		chunks, err := e.readArrowFileChunks(ctx.Context, path, opts)
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

// sniffArrowSchema reads the schema from an Arrow file without reading all data.
func (e *Executor) sniffArrowSchema(
	ctx context.Context,
	path string,
	opts *arrow.ReaderOptions,
) (fileio.FileSchema, error) {
	var reader *arrow.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open Arrow file: %w", err)
		}
		closer = file

		// Check if the file supports ReadAt (needed for Arrow IPC file format)
		if ras, ok := file.(arrow.ReadAtSeeker); ok {
			reader, err = arrow.NewReader(ras, opts)
			if err != nil {
				_ = file.Close()
				return fileio.FileSchema{}, fmt.Errorf("failed to create Arrow reader: %w", err)
			}
		} else {
			// Read entire file into memory
			data, readErr := io.ReadAll(file)
			_ = file.Close()
			if readErr != nil {
				return fileio.FileSchema{}, fmt.Errorf("failed to read arrow data: %w", readErr)
			}
			br := &bytesReaderAt{data: data}
			reader, err = arrow.NewReader(br, opts)
			if err != nil {
				return fileio.FileSchema{}, fmt.Errorf("failed to create Arrow reader: %w", err)
			}
			closer = nil // bytesReaderAt doesn't need closing
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open Arrow file: %w", err)
		}
		closer = file
		reader, err = arrow.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return fileio.FileSchema{}, fmt.Errorf("failed to create Arrow reader: %w", err)
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

// readArrowFileChunks reads all chunks from an Arrow file.
func (e *Executor) readArrowFileChunks(
	ctx context.Context,
	path string,
	opts *arrow.ReaderOptions,
) ([]*storage.DataChunk, error) {
	var reader *arrow.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to open Arrow file: %w", err)
		}
		closer = file

		// Check if the file supports ReadAt (needed for Arrow IPC file format)
		if ras, ok := file.(arrow.ReadAtSeeker); ok {
			reader, err = arrow.NewReader(ras, opts)
			if err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
			}
		} else {
			// Read entire file into memory
			data, readErr := io.ReadAll(file)
			_ = file.Close()
			if readErr != nil {
				return nil, fmt.Errorf("failed to read arrow data: %w", readErr)
			}
			br := &bytesReaderAt{data: data}
			reader, err = arrow.NewReader(br, opts)
			if err != nil {
				return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
			}
			closer = nil // bytesReaderAt doesn't need closing
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open Arrow file: %w", err)
		}
		closer = file
		reader, err = arrow.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
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
