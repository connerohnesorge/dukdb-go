// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ParquetMultiFileOptions contains options for multi-file Parquet reading.
type ParquetMultiFileOptions struct {
	// Filename adds a filename column to the output.
	Filename bool
	// FileRowNumber adds a 1-indexed row number within each file.
	FileRowNumber bool
	// FileIndex adds the 0-indexed file index.
	FileIndex bool
	// FilesToSniff is the number of files to sample for schema detection (default: 1).
	// Note: Parquet has embedded schema, so this is mainly for schema merging validation.
	FilesToSniff int
	// FileGlobBehavior controls handling of empty glob results.
	FileGlobBehavior FileGlobBehavior
	// UnionByName aligns schemas by column name (default: true).
	UnionByName bool
	// HivePartitioning enables Hive-style partition column extraction.
	HivePartitioning interface{} // bool or "auto"
	// HiveTypesAutocast enables auto-casting partition values to appropriate types.
	HiveTypesAutocast bool
	// HiveTypes specifies explicit types for partition columns.
	HiveTypes map[string]string
}

// DefaultParquetMultiFileOptions returns the default options for multi-file Parquet reading.
func DefaultParquetMultiFileOptions() *ParquetMultiFileOptions {
	return &ParquetMultiFileOptions{
		Filename:          false,
		FileRowNumber:     false,
		FileIndex:         false,
		FilesToSniff:      1,
		FileGlobBehavior:  FileGlobDisallowEmpty,
		UnionByName:       true,
		HivePartitioning:  false,
		HiveTypesAutocast: true,
		HiveTypes:         nil,
	}
}

// executeReadParquet executes a read_parquet table function.
func (e *Executor) executeReadParquet(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := parquet.DefaultReaderOptions()
	multiOpts := DefaultParquetMultiFileOptions()

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
		case "hive_partitioning":
			switch v := val.(type) {
			case bool:
				multiOpts.HivePartitioning = v
			case string:
				if strings.ToLower(v) == "auto" {
					multiOpts.HivePartitioning = "auto"
				} else {
					multiOpts.HivePartitioning = v == "true" || v == "1"
				}
			}
		case "hive_types_autocast":
			if b, ok := val.(bool); ok {
				multiOpts.HiveTypesAutocast = b
			}
		case "hive_types":
			if m, ok := val.(map[string]string); ok {
				multiOpts.HiveTypes = m
			}
		}
	}

	// Get file paths - check for array syntax first, then glob patterns
	var paths []string
	var err error

	if plan.TableFunction != nil && len(plan.TableFunction.Paths) > 0 {
		// Array syntax: read_parquet(['file1.parquet', 'file2.parquet'])
		// Expand any glob patterns within the array
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandParquetGlobPattern(ctx.Context, p, multiOpts)
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
		paths, err = e.resolveParquetFilePaths(ctx.Context, plan.Path, multiOpts)
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
	if len(paths) == 1 && !multiOpts.Filename && !multiOpts.FileRowNumber && !multiOpts.FileIndex &&
		!isParquetHivePartitioningEnabled(multiOpts) {
		return e.executeReadParquetSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadParquetMultiFile(ctx, paths, opts, multiOpts, plan)
}

// resolveParquetFilePaths resolves a path or glob pattern to a list of file paths.
func (e *Executor) resolveParquetFilePaths(
	ctx context.Context,
	pathOrPattern string,
	multiOpts *ParquetMultiFileOptions,
) ([]string, error) {
	// Check if it's a glob pattern
	if fileio.IsGlobPattern(pathOrPattern) {
		return e.expandParquetGlobPattern(ctx, pathOrPattern, multiOpts)
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

// expandParquetGlobPattern expands a glob pattern to a list of file paths.
func (e *Executor) expandParquetGlobPattern(
	ctx context.Context,
	pattern string,
	multiOpts *ParquetMultiFileOptions,
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

// executeReadParquetSingleFile executes read_parquet for a single file (optimized path).
func (e *Executor) executeReadParquetSingleFile(
	ctx *ExecutionContext,
	path string,
	opts *parquet.ReaderOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Create the Parquet reader - use filesystem for cloud URLs, local file for local paths
	var reader *parquet.Reader
	var err error

	if filesystem.IsCloudURL(path) {
		// Use FileSystemProvider for cloud URLs
		reader, err = e.createCloudParquetReader(ctx, path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to create Parquet reader: %v", err),
			}
		}
	} else {
		// Use local file for local paths
		reader, err = parquet.NewReaderFromPath(path, opts)
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

// executeReadParquetMultiFile executes read_parquet for multiple files with schema merging.
func (e *Executor) executeReadParquetMultiFile(
	ctx *ExecutionContext,
	paths []string,
	opts *parquet.ReaderOptions,
	multiOpts *ParquetMultiFileOptions,
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
			schema, err := e.sniffParquetSchema(ctx.Context, path, opts)
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

	// Determine output columns (data columns + metadata columns + hive partitions)
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

	// Track hive partition columns if enabled
	var hivePartitionCols []string
	var hivePartitionTypes []dukdb.Type
	if isParquetHivePartitioningEnabled(multiOpts) {
		// Detect hive partitions from the first path
		partitions := extractHivePartitions(paths[0])
		for key := range partitions {
			hivePartitionCols = append(hivePartitionCols, key)
			// Determine type from hive_types or autocast
			colType := dukdb.TYPE_VARCHAR
			if multiOpts.HiveTypes != nil {
				if typeName, ok := multiOpts.HiveTypes[key]; ok {
					colType = parseTypeName(typeName)
				}
			}
			hivePartitionTypes = append(hivePartitionTypes, colType)
		}
		// Sort partition columns for consistent ordering
		sort.Strings(hivePartitionCols)

		// Add partition columns to output
		for i, col := range hivePartitionCols {
			outputColumns = append(outputColumns, col)
			outputTypes = append(outputTypes, hivePartitionTypes[i])
		}
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
		chunks, err := e.readParquetFileChunks(ctx.Context, path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read %s: %v", path, err),
			}
		}

		// Get hive partitions for this file
		var hivePartitions map[string]string
		if isParquetHivePartitioningEnabled(multiOpts) {
			hivePartitions = extractHivePartitions(path)
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

				// Add hive partition columns
				if hivePartitions != nil {
					for _, col := range hivePartitionCols {
						val := hivePartitions[col]
						if multiOpts.HiveTypesAutocast {
							row[col] = autocastPartitionValue(val)
						} else {
							row[col] = val
						}
					}
				}

				result.Rows = append(result.Rows, row)
			}
		}
	}

	// Suppress warnings about unused variables
	_ = metadataStartIdx

	return result, nil
}

// sniffParquetSchema reads the schema from a Parquet file without reading all data.
func (e *Executor) sniffParquetSchema(
	ctx context.Context,
	path string,
	opts *parquet.ReaderOptions,
) (fileio.FileSchema, error) {
	var reader *parquet.Reader
	var err error

	if filesystem.IsCloudURL(path) {
		reader, err = e.createCloudParquetReaderWithContext(ctx, path, opts)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open Parquet file: %w", err)
		}
	} else {
		reader, err = parquet.NewReaderFromPath(path, opts)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open Parquet file: %w", err)
		}
	}
	defer func() { _ = reader.Close() }()

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

// readParquetFileChunks reads all chunks from a Parquet file.
func (e *Executor) readParquetFileChunks(
	ctx context.Context,
	path string,
	opts *parquet.ReaderOptions,
) ([]*storage.DataChunk, error) {
	var reader *parquet.Reader
	var err error

	if filesystem.IsCloudURL(path) {
		reader, err = e.createCloudParquetReaderWithContext(ctx, path, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open Parquet file: %w", err)
		}
	} else {
		reader, err = parquet.NewReaderFromPath(path, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open Parquet file: %w", err)
		}
	}
	defer func() { _ = reader.Close() }()

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

// isParquetHivePartitioningEnabled returns true if hive partitioning is enabled for Parquet.
func isParquetHivePartitioningEnabled(opts *ParquetMultiFileOptions) bool {
	switch v := opts.HivePartitioning.(type) {
	case bool:
		return v
	case string:
		return v == "auto" || strings.ToLower(v) == "true"
	default:
		return false
	}
}

// createCloudParquetReader creates a Parquet reader for cloud URLs.
// Parquet requires a ReaderAtSeeker, so for non-seekable cloud streams,
// we read the entire file into memory first.
func (e *Executor) createCloudParquetReader(
	ctx *ExecutionContext,
	path string,
	opts *parquet.ReaderOptions,
) (*parquet.Reader, error) {
	return e.createCloudParquetReaderWithContext(ctx.Context, path, opts)
}

// createCloudParquetReaderWithContext creates a Parquet reader for cloud URLs using a context.
func (e *Executor) createCloudParquetReaderWithContext(
	ctx context.Context,
	path string,
	opts *parquet.ReaderOptions,
) (*parquet.Reader, error) {
	file, err := e.openFileForReading(ctx, path)
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
