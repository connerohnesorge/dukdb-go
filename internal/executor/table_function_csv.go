// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/csv"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// FileGlobBehavior defines how to handle glob pattern results.
type FileGlobBehavior string

const (
	// FileGlobDisallowEmpty returns an error when no files match (default).
	FileGlobDisallowEmpty FileGlobBehavior = "DISALLOW_EMPTY"
	// FileGlobAllowEmpty returns empty result when no files match.
	FileGlobAllowEmpty FileGlobBehavior = "ALLOW_EMPTY"
	// FileGlobFallback treats the pattern as a literal path if no matches found.
	FileGlobFallback FileGlobBehavior = "FALLBACK_GLOB"
)

// CSVMultiFileOptions contains options for multi-file CSV reading.
type CSVMultiFileOptions struct {
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
	// HivePartitioning enables Hive-style partition column extraction.
	HivePartitioning interface{} // bool or "auto"
	// HiveTypesAutocast enables auto-casting partition values to appropriate types.
	HiveTypesAutocast bool
	// HiveTypes specifies explicit types for partition columns.
	HiveTypes map[string]string
}

// DefaultCSVMultiFileOptions returns the default options for multi-file CSV reading.
func DefaultCSVMultiFileOptions() *CSVMultiFileOptions {
	return &CSVMultiFileOptions{
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
	case "read_xlsx":
		return e.executeReadXLSX(ctx, plan)
	case "read_xlsx_auto":
		return e.executeReadXLSXAuto(ctx, plan)
	case "read_arrow":
		return e.executeReadArrow(ctx, plan)
	case "read_arrow_auto":
		return e.executeReadArrowAuto(ctx, plan)
	// Iceberg table functions
	case "iceberg_scan":
		return e.executeIcebergScan(ctx, plan)
	case "iceberg_metadata":
		return e.executeIcebergMetadata(ctx, plan)
	case "iceberg_snapshots":
		return e.executeIcebergSnapshots(ctx, plan)
	case "duckdb_iceberg_tables":
		return e.executeIcebergTables(ctx, plan)
	// Secret system functions
	case "which_secret":
		return e.executeWhichSecret(ctx, plan)
	case "duckdb_secrets":
		return e.executeDuckDBSecrets(ctx, plan)
	// Array/list expansion functions
	case "unnest":
		return e.executeUnnest(ctx, plan)
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
	multiOpts := DefaultCSVMultiFileOptions()

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
		// Array syntax: read_csv(['file1.csv', 'file2.csv'])
		// Expand any glob patterns within the array
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandGlobPattern(ctx.Context, p, multiOpts)
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
		paths, err = e.resolveFilePaths(ctx.Context, plan.Path, multiOpts)
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
	if len(paths) == 1 && !multiOpts.Filename && !multiOpts.FileRowNumber && !multiOpts.FileIndex && !isHivePartitioningEnabled(multiOpts) {
		return e.executeReadCSVSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadCSVMultiFile(ctx, paths, opts, multiOpts, plan)
}

// resolveFilePaths resolves a path or glob pattern to a list of file paths.
func (e *Executor) resolveFilePaths(ctx context.Context, pathOrPattern string, multiOpts *CSVMultiFileOptions) ([]string, error) {
	// Check if it's a glob pattern
	if fileio.IsGlobPattern(pathOrPattern) {
		return e.expandGlobPattern(ctx, pathOrPattern, multiOpts)
	}

	// Check if it's an array of paths (passed via options)
	// This is handled by the binder which may pass multiple paths

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

// expandGlobPattern expands a glob pattern to a list of file paths.
func (e *Executor) expandGlobPattern(ctx context.Context, pattern string, multiOpts *CSVMultiFileOptions) ([]string, error) {
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

// executeReadCSVSingleFile executes read_csv for a single file (optimized path).
func (e *Executor) executeReadCSVSingleFile(
	ctx *ExecutionContext,
	path string,
	opts *csv.ReaderOptions,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Create the CSV reader - use filesystem for cloud URLs, local file for local paths
	var reader *csv.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		// Use FileSystemProvider for cloud URLs
		file, err := e.openFileForReading(ctx.Context, path)
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
		file, err := os.Open(path)
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

// executeReadCSVMultiFile executes read_csv for multiple files with schema merging.
func (e *Executor) executeReadCSVMultiFile(
	ctx *ExecutionContext,
	paths []string,
	opts *csv.ReaderOptions,
	multiOpts *CSVMultiFileOptions,
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
			schema, err := e.sniffCSVSchema(ctx.Context, path, opts)
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
	if isHivePartitioningEnabled(multiOpts) {
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
		chunks, err := e.readCSVFileChunks(ctx.Context, path, opts)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read %s: %v", path, err),
			}
		}

		// Get hive partitions for this file
		var hivePartitions map[string]string
		if isHivePartitioningEnabled(multiOpts) {
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

// sniffCSVSchema reads the schema from a CSV file without reading all data.
func (e *Executor) sniffCSVSchema(ctx context.Context, path string, opts *csv.ReaderOptions) (fileio.FileSchema, error) {
	var reader *csv.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open CSV file: %w", err)
		}
		closer = file
		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return fileio.FileSchema{}, fmt.Errorf("failed to create CSV reader: %w", err)
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return fileio.FileSchema{}, fmt.Errorf("failed to open CSV file: %w", err)
		}
		closer = file
		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return fileio.FileSchema{}, fmt.Errorf("failed to create CSV reader: %w", err)
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

// readCSVFileChunks reads all chunks from a CSV file.
func (e *Executor) readCSVFileChunks(ctx context.Context, path string, opts *csv.ReaderOptions) ([]*storage.DataChunk, error) {
	var reader *csv.Reader
	var closer io.Closer

	if filesystem.IsCloudURL(path) {
		file, err := e.openFileForReading(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to open CSV file: %w", err)
		}
		closer = file
		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to create CSV reader: %w", err)
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open CSV file: %w", err)
		}
		closer = file
		reader, err = csv.NewReader(file, opts)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to create CSV reader: %w", err)
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

// isHivePartitioningEnabled returns true if hive partitioning is enabled.
func isHivePartitioningEnabled(opts *CSVMultiFileOptions) bool {
	switch v := opts.HivePartitioning.(type) {
	case bool:
		return v
	case string:
		return v == "auto" || strings.ToLower(v) == "true"
	default:
		return false
	}
}

// extractHivePartitions extracts key=value pairs from a path.
// Example: /data/year=2024/month=01/file.csv returns {"year": "2024", "month": "01"}
func extractHivePartitions(path string) map[string]string {
	partitions := make(map[string]string)

	// Split path into components
	parts := strings.Split(filepath.ToSlash(path), "/")

	// Look for key=value patterns
	hivePattern := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)=(.+)$`)
	for _, part := range parts {
		if matches := hivePattern.FindStringSubmatch(part); matches != nil {
			key := matches[1]
			value := matches[2]
			partitions[key] = value
		}
	}

	return partitions
}

// detectHivePartitioning detects if a path has Hive-style partitions.
func detectHivePartitioning(path string) bool {
	partitions := extractHivePartitions(path)
	return len(partitions) > 0
}

// autocastPartitionValue attempts to convert a partition value to an appropriate type.
func autocastPartitionValue(value string) any {
	// Try integer
	if i, err := parseInt64(value); err == nil {
		return i
	}

	// Try float
	if f, err := parseFloat64(value); err == nil {
		return f
	}

	// Try boolean
	lower := strings.ToLower(value)
	if lower == "true" {
		return true
	}
	if lower == "false" {
		return false
	}

	// Return as string
	return value
}

// parseInt64 parses an integer with proper error handling.
func parseInt64(s string) (int64, error) {
	var result int64
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}

// parseFloat64 parses a float with proper error handling.
func parseFloat64(s string) (float64, error) {
	var result float64
	_, err := fmt.Sscanf(s, "%f", &result)
	return result, err
}

// parseTypeName converts a type name string to a dukdb.Type.
func parseTypeName(name string) dukdb.Type {
	switch strings.ToUpper(name) {
	case "INTEGER", "INT", "INT32":
		return dukdb.TYPE_INTEGER
	case "BIGINT", "INT64":
		return dukdb.TYPE_BIGINT
	case "SMALLINT", "INT16":
		return dukdb.TYPE_SMALLINT
	case "TINYINT", "INT8":
		return dukdb.TYPE_TINYINT
	case "DOUBLE", "FLOAT8":
		return dukdb.TYPE_DOUBLE
	case "FLOAT", "FLOAT4", "REAL":
		return dukdb.TYPE_FLOAT
	case "BOOLEAN", "BOOL":
		return dukdb.TYPE_BOOLEAN
	case "DATE":
		return dukdb.TYPE_DATE
	case "TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "TIME":
		return dukdb.TYPE_TIME
	default:
		return dukdb.TYPE_VARCHAR
	}
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
	multiOpts := DefaultCSVMultiFileOptions()

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
		// Array syntax: read_csv_auto(['file1.csv', 'file2.csv'])
		// Expand any glob patterns within the array
		for _, p := range plan.TableFunction.Paths {
			if fileio.IsGlobPattern(p) {
				expanded, err := e.expandGlobPattern(ctx.Context, p, multiOpts)
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
		paths, err = e.resolveFilePaths(ctx.Context, plan.Path, multiOpts)
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
	if len(paths) == 1 && !multiOpts.Filename && !multiOpts.FileRowNumber && !multiOpts.FileIndex && !isHivePartitioningEnabled(multiOpts) {
		return e.executeReadCSVSingleFile(ctx, paths[0], opts, plan)
	}

	// Multiple files - use multi-file reader
	return e.executeReadCSVMultiFile(ctx, paths, opts, multiOpts, plan)
}
