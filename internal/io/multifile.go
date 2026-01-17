// Package io provides file I/O interfaces and utilities for reading and writing
// various file formats (CSV, JSON, Parquet) to and from DataChunks.
package io

import (
	"errors"
	"fmt"
	"sort"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// MaxFilesPerGlob is the default limit on files matched by a glob pattern.
// This limit prevents memory exhaustion when glob patterns match too many files.
// Use dukdb.DefaultMaxFilesPerGlob for the canonical default value.
const MaxFilesPerGlob = dukdb.DefaultMaxFilesPerGlob

// ErrTooManyFiles is returned when a glob pattern matches more files than allowed.
var ErrTooManyFiles = errors.New("glob pattern matches too many files")

// ErrNoFilesMatched is returned when a glob pattern matches no files.
var ErrNoFilesMatched = errors.New("no files match the glob pattern")

// ErrIncompatibleTypes is returned when schemas have incompatible column types.
var ErrIncompatibleTypes = errors.New("incompatible column types")

// MultiFileOptions controls multi-file reading behavior.
type MultiFileOptions struct {
	// MaxFiles is the maximum number of files to read (0 = MaxFilesPerGlob).
	MaxFiles int
	// UnionByName aligns schemas by column name (default: true).
	// If false, schemas are aligned by position.
	UnionByName bool
	// Filename adds a filename column to the output.
	Filename bool
	// FileRowNumber adds a file_row_number column to the output.
	FileRowNumber bool
	// FileIndex adds a file_index column to the output (0-based index of file).
	FileIndex bool
}

// DefaultMultiFileOptions returns the default options for multi-file reading.
func DefaultMultiFileOptions() *MultiFileOptions {
	return &MultiFileOptions{
		MaxFiles:      0, // Use MaxFilesPerGlob
		UnionByName:   true,
		Filename:      false,
		FileRowNumber: false,
		FileIndex:     false,
	}
}

// ColumnInfo represents column metadata with name and type.
type ColumnInfo struct {
	Name string
	Type dukdb.Type
}

// SchemaMergeResult holds the merged schema and column mappings.
type SchemaMergeResult struct {
	// Columns holds the merged column information.
	Columns []ColumnInfo
	// Mappings maps[fileIdx][mergedColIdx] -> originalColIdx.
	// A value of -1 indicates the column is missing in that file (should be NULL).
	Mappings [][]int
}

// Types returns just the types from the merged schema.
func (r *SchemaMergeResult) Types() []dukdb.Type {
	types := make([]dukdb.Type, len(r.Columns))
	for i, col := range r.Columns {
		types[i] = col.Type
	}

	return types
}

// Names returns just the column names from the merged schema.
func (r *SchemaMergeResult) Names() []string {
	names := make([]string, len(r.Columns))
	for i, col := range r.Columns {
		names[i] = col.Name
	}

	return names
}

// FileSchema represents a single file's schema with column names and types.
type FileSchema struct {
	// Path is the file path.
	Path string
	// Columns holds the column information.
	Columns []ColumnInfo
}

// MultiFileReader reads multiple files and combines their results.
type MultiFileReader struct {
	// Paths is the list of file paths to read.
	Paths []string
	// Options controls multi-file reading behavior.
	Options *MultiFileOptions
	// FileSystem is the filesystem to use for file operations.
	FileSystem filesystem.FileSystem
	// mergeResult holds the cached schema merge result.
	mergeResult *SchemaMergeResult
	// fileSchemas holds the schemas for each file.
	fileSchemas []FileSchema
}

// NewMultiFileReader creates a new multi-file reader.
// fs can be nil, in which case a local filesystem is used.
func NewMultiFileReader(
	paths []string,
	options *MultiFileOptions,
	fs filesystem.FileSystem,
) (*MultiFileReader, error) {
	if len(paths) == 0 {
		return nil, ErrNoFilesMatched
	}

	opts := options
	if opts == nil {
		opts = DefaultMultiFileOptions()
	}

	maxFiles := opts.MaxFiles
	if maxFiles == 0 {
		maxFiles = MaxFilesPerGlob
	}

	if len(paths) > maxFiles {
		return nil, fmt.Errorf("%w: %d files (limit: %d)", ErrTooManyFiles, len(paths), maxFiles)
	}

	return &MultiFileReader{
		Paths:      paths,
		Options:    opts,
		FileSystem: fs,
	}, nil
}

// SetSchemas sets the file schemas for merging.
// This should be called after schema detection for each file.
func (r *MultiFileReader) SetSchemas(schemas []FileSchema) error {
	if len(schemas) != len(r.Paths) {
		return fmt.Errorf("schema count (%d) does not match file count (%d)", len(schemas), len(r.Paths))
	}
	r.fileSchemas = schemas
	r.mergeResult = nil // Reset cached result

	return nil
}

// MergedSchema returns the merged schema from all files.
// It performs union-by-name schema merging if Options.UnionByName is true.
func (r *MultiFileReader) MergedSchema() (*SchemaMergeResult, error) {
	if r.mergeResult != nil {
		return r.mergeResult, nil
	}

	if len(r.fileSchemas) == 0 {
		return nil, errors.New("no schemas set; call SetSchemas first")
	}

	result, err := MergeSchemas(r.fileSchemas, r.Options.UnionByName)
	if err != nil {
		return nil, err
	}

	r.mergeResult = result
	return result, nil
}

// MergeSchemas creates a merged schema from multiple file schemas.
// If unionByName is true, columns are matched by name. Otherwise, by position.
func MergeSchemas(schemas []FileSchema, unionByName bool) (*SchemaMergeResult, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no schemas to merge")
	}

	if unionByName {
		return mergeSchemasByName(schemas)
	}
	return mergeSchemasByPosition(schemas)
}

// mergeSchemasByName merges schemas using union-by-name semantics.
// Columns are matched by name, with type widening for compatible types.
func mergeSchemasByName(schemas []FileSchema) (*SchemaMergeResult, error) {
	// Track column order by first appearance
	columnOrder := make([]string, 0)
	columnTypes := make(map[string]dukdb.Type)
	columnSeen := make(map[string]bool)

	// First pass: collect all unique columns and determine widened types
	for _, schema := range schemas {
		for _, col := range schema.Columns {
			name := col.Name
			if !columnSeen[name] {
				columnOrder = append(columnOrder, name)
				columnTypes[name] = col.Type
				columnSeen[name] = true
			} else {
				// Column exists, check type compatibility and widen if needed
				existingType := columnTypes[name]
				widenedType, err := ValidateTypeCompatibility(existingType, col.Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", name, err)
				}
				columnTypes[name] = widenedType
			}
		}
	}

	// Build merged columns
	columns := make([]ColumnInfo, len(columnOrder))
	for i, name := range columnOrder {
		columns[i] = ColumnInfo{
			Name: name,
			Type: columnTypes[name],
		}
	}

	// Build column name to merged index mapping
	nameToIdx := make(map[string]int)
	for i, name := range columnOrder {
		nameToIdx[name] = i
	}

	// Build mappings for each file
	mappings := make([][]int, len(schemas))
	for fileIdx, schema := range schemas {
		mapping := make([]int, len(columns))
		// Initialize all to -1 (missing)
		for i := range mapping {
			mapping[i] = -1
		}

		// Map original columns to merged positions
		for origIdx, col := range schema.Columns {
			mergedIdx := nameToIdx[col.Name]
			mapping[mergedIdx] = origIdx
		}

		mappings[fileIdx] = mapping
	}

	return &SchemaMergeResult{
		Columns:  columns,
		Mappings: mappings,
	}, nil
}

// mergeSchemasByPosition merges schemas by column position.
// All files must have the same number of columns.
func mergeSchemasByPosition(schemas []FileSchema) (*SchemaMergeResult, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no schemas to merge")
	}

	numCols := len(schemas[0].Columns)
	// Verify all files have the same number of columns
	for i, schema := range schemas {
		if len(schema.Columns) != numCols {
			return nil, fmt.Errorf(
				"column count mismatch: file %d has %d columns, expected %d",
				i, len(schema.Columns), numCols,
			)
		}
	}

	// Build merged columns using first file's names and widened types
	columns := make([]ColumnInfo, numCols)
	for colIdx := range numCols {
		// Use name from first file
		name := schemas[0].Columns[colIdx].Name
		typ := schemas[0].Columns[colIdx].Type

		// Widen types across all files
		numSchemas := len(schemas)
		for fileIdx := 1; fileIdx < numSchemas; fileIdx++ {
			otherType := schemas[fileIdx].Columns[colIdx].Type
			widenedType, err := ValidateTypeCompatibility(typ, otherType)
			if err != nil {
				return nil, fmt.Errorf("column %d (%q): %w", colIdx, name, err)
			}
			typ = widenedType
		}

		columns[colIdx] = ColumnInfo{
			Name: name,
			Type: typ,
		}
	}

	// Build identity mappings (1:1 mapping by position)
	mappings := make([][]int, len(schemas))
	for fileIdx := range schemas {
		mapping := make([]int, numCols)
		for i := range numCols {
			mapping[i] = i
		}
		mappings[fileIdx] = mapping
	}

	return &SchemaMergeResult{
		Columns:  columns,
		Mappings: mappings,
	}, nil
}

// ValidateTypeCompatibility checks if two types can be merged.
// Returns the resulting type (potentially widened) or an error if incompatible.
func ValidateTypeCompatibility(t1, t2 dukdb.Type) (dukdb.Type, error) {
	// Same type is always compatible
	if t1 == t2 {
		return t1, nil
	}

	// Check if both are numeric and can be widened
	if isSignedInteger(t1) && isSignedInteger(t2) {
		return WidenType(t1, t2), nil
	}
	if isUnsignedInteger(t1) && isUnsignedInteger(t2) {
		return WidenType(t1, t2), nil
	}
	if isFloatingPoint(t1) && isFloatingPoint(t2) {
		return WidenType(t1, t2), nil
	}

	// Allow signed integer to widen to larger signed integer that can hold unsigned
	if isSignedInteger(t1) && isUnsignedInteger(t2) {
		// Check if t1 can hold all values of t2
		wider := widenSignedUnsigned(t1, t2)
		if wider != dukdb.TYPE_INVALID {
			return wider, nil
		}
	}
	if isUnsignedInteger(t1) && isSignedInteger(t2) {
		wider := widenSignedUnsigned(t2, t1)
		if wider != dukdb.TYPE_INVALID {
			return wider, nil
		}
	}

	// Allow integers to widen to floating point
	if isInteger(t1) && isFloatingPoint(t2) {
		return WidenType(dukdb.TYPE_DOUBLE, t2), nil
	}
	if isFloatingPoint(t1) && isInteger(t2) {
		return WidenType(t1, dukdb.TYPE_DOUBLE), nil
	}

	// VARCHAR is compatible with itself only (already handled by t1 == t2)
	// Timestamp types: allow widening within timestamp family
	if isTimestamp(t1) && isTimestamp(t2) {
		return widenTimestamp(t1, t2), nil
	}

	// Types are incompatible
	return dukdb.TYPE_INVALID, fmt.Errorf(
		"%w: cannot merge %s with %s",
		ErrIncompatibleTypes, t1.String(), t2.String(),
	)
}

// WidenType returns the wider of two compatible numeric types.
// This follows DuckDB's type widening rules.
func WidenType(t1, t2 dukdb.Type) dukdb.Type {
	// Same type
	if t1 == t2 {
		return t1
	}

	// Use type rankings for comparison
	rank1 := typeRank(t1)
	rank2 := typeRank(t2)

	if rank1 >= rank2 {
		return t1
	}
	return t2
}

// typeRank returns a numeric rank for type widening.
// Higher rank = wider type.
//
//nolint:exhaustive // Only numeric types need ranks for widening
func typeRank(t dukdb.Type) int {
	switch t {
	// Signed integers: TINYINT < SMALLINT < INTEGER < BIGINT < HUGEINT
	case dukdb.TYPE_TINYINT:
		return 10
	case dukdb.TYPE_SMALLINT:
		return 20
	case dukdb.TYPE_INTEGER:
		return 30
	case dukdb.TYPE_BIGINT:
		return 40
	case dukdb.TYPE_HUGEINT:
		return 50

	// Unsigned integers: UTINYINT < USMALLINT < UINTEGER < UBIGINT < UHUGEINT
	case dukdb.TYPE_UTINYINT:
		return 15
	case dukdb.TYPE_USMALLINT:
		return 25
	case dukdb.TYPE_UINTEGER:
		return 35
	case dukdb.TYPE_UBIGINT:
		return 45
	case dukdb.TYPE_UHUGEINT:
		return 55

	// Floating point: FLOAT < DOUBLE
	case dukdb.TYPE_FLOAT:
		return 60
	case dukdb.TYPE_DOUBLE:
		return 70

	// Timestamps
	case dukdb.TYPE_TIMESTAMP_S:
		return 100
	case dukdb.TYPE_TIMESTAMP_MS:
		return 101
	case dukdb.TYPE_TIMESTAMP:
		return 102
	case dukdb.TYPE_TIMESTAMP_NS:
		return 103

	default:
		return 0
	}
}

// isSignedInteger returns true if the type is a signed integer.
//
//nolint:exhaustive // Only checking specific integer types
func isSignedInteger(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT, dukdb.TYPE_HUGEINT:
		return true
	default:
		return false
	}
}

// isUnsignedInteger returns true if the type is an unsigned integer.
//
//nolint:exhaustive // Only checking specific integer types
func isUnsignedInteger(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT, dukdb.TYPE_UHUGEINT:
		return true
	default:
		return false
	}
}

// isInteger returns true if the type is any integer type.
func isInteger(t dukdb.Type) bool {
	return isSignedInteger(t) || isUnsignedInteger(t)
}

// isFloatingPoint returns true if the type is a floating point type.
func isFloatingPoint(t dukdb.Type) bool {
	return t == dukdb.TYPE_FLOAT || t == dukdb.TYPE_DOUBLE
}

// isTimestamp returns true if the type is a timestamp type.
//
//nolint:exhaustive // Only checking specific timestamp types
func isTimestamp(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		return true
	default:
		return false
	}
}

// widenSignedUnsigned determines the wider type when mixing signed and unsigned.
// Returns TYPE_INVALID if no safe widening exists.
//
//nolint:exhaustive // Only handling unsigned integer types
func widenSignedUnsigned(signed, unsigned dukdb.Type) dukdb.Type {
	// We need a signed type that can hold the unsigned range
	// UTINYINT (0-255) -> SMALLINT (can hold up to 32767)
	// USMALLINT (0-65535) -> INTEGER (can hold up to 2^31-1)
	// UINTEGER (0-4B) -> BIGINT (can hold up to 2^63-1)
	// UBIGINT (0-2^64) -> HUGEINT (can hold up to 2^127-1)

	var minSignedForUnsigned dukdb.Type
	switch unsigned {
	case dukdb.TYPE_UTINYINT:
		minSignedForUnsigned = dukdb.TYPE_SMALLINT
	case dukdb.TYPE_USMALLINT:
		minSignedForUnsigned = dukdb.TYPE_INTEGER
	case dukdb.TYPE_UINTEGER:
		minSignedForUnsigned = dukdb.TYPE_BIGINT
	case dukdb.TYPE_UBIGINT:
		minSignedForUnsigned = dukdb.TYPE_HUGEINT
	case dukdb.TYPE_UHUGEINT:
		// No signed type can hold UHUGEINT
		return dukdb.TYPE_INVALID
	default:
		return dukdb.TYPE_INVALID
	}

	// Return the wider of the two signed types
	return WidenType(signed, minSignedForUnsigned)
}

// widenTimestamp returns the appropriate timestamp type for merging.
// DuckDB uses microsecond precision by default.
func widenTimestamp(t1, t2 dukdb.Type) dukdb.Type {
	// Default to microsecond precision (TYPE_TIMESTAMP)
	// but if either is nanosecond, use nanosecond
	if t1 == dukdb.TYPE_TIMESTAMP_NS || t2 == dukdb.TYPE_TIMESTAMP_NS {
		return dukdb.TYPE_TIMESTAMP_NS
	}
	return dukdb.TYPE_TIMESTAMP
}

// AlignChunk realigns a chunk to match the target schema.
// Missing columns are filled with NULL values.
// mapping[mergedColIdx] -> originalColIdx, -1 if missing
func AlignChunk(
	chunk *storage.DataChunk,
	mapping []int,
	targetTypes []dukdb.Type,
) *storage.DataChunk {
	if chunk == nil || chunk.Count() == 0 {
		return storage.NewDataChunk(targetTypes)
	}

	rowCount := chunk.Count()

	// Create new chunk with target schema
	aligned := storage.NewDataChunkWithCapacity(targetTypes, rowCount)

	// Copy data according to mapping
	for mergedIdx, origIdx := range mapping {
		targetVec := aligned.GetVector(mergedIdx)
		if origIdx == -1 {
			// Column missing in source - all values are NULL
			fillNullColumn(targetVec, rowCount)
		} else {
			// Copy from source, potentially with type conversion
			srcVec := chunk.GetVector(origIdx)
			srcType := srcVec.Type()
			targetType := targetTypes[mergedIdx]
			copyColumn(srcVec, targetVec, srcType, targetType, rowCount)
		}
	}

	aligned.SetCount(rowCount)
	return aligned
}

// fillNullColumn fills a vector with NULL values.
func fillNullColumn(vec *storage.Vector, rowCount int) {
	for row := range rowCount {
		vec.SetValue(row, nil)
	}
}

// copyColumn copies values from source to target vector with optional type conversion.
func copyColumn(
	srcVec, targetVec *storage.Vector,
	srcType, targetType dukdb.Type,
	rowCount int,
) {
	needsConversion := srcType != targetType
	for row := range rowCount {
		val := srcVec.GetValue(row)
		switch {
		case val == nil:
			targetVec.SetValue(row, nil)
		case needsConversion:
			converted := convertValue(val, srcType, targetType)
			targetVec.SetValue(row, converted)
		default:
			targetVec.SetValue(row, val)
		}
	}
}

// convertValue converts a value from source type to target type.
// Assumes the conversion is valid (types have been validated as compatible).
func convertValue(val any, srcType, targetType dukdb.Type) any {
	if val == nil {
		return nil
	}

	// If same type, no conversion needed
	if srcType == targetType {
		return val
	}

	// Integer widening
	if isInteger(srcType) && isInteger(targetType) {
		return convertInteger(val, targetType)
	}

	// Integer to float
	if isInteger(srcType) && isFloatingPoint(targetType) {
		return convertToFloat(val, targetType)
	}

	// Float widening
	if isFloatingPoint(srcType) && isFloatingPoint(targetType) {
		return convertFloat(val, targetType)
	}

	// Timestamp conversion (same underlying representation)
	if isTimestamp(srcType) && isTimestamp(targetType) {
		return val // Timestamps use same underlying int64 representation
	}

	// Default: return as-is
	return val
}

// convertInteger converts an integer value to a wider integer type.
//
//nolint:exhaustive // Only handling integer target types
func convertInteger(val any, targetType dukdb.Type) any {
	var i64 int64
	switch v := val.(type) {
	case int8:
		i64 = int64(v)
	case int16:
		i64 = int64(v)
	case int32:
		i64 = int64(v)
	case int64:
		i64 = v
	case uint8:
		i64 = int64(v)
	case uint16:
		i64 = int64(v)
	case uint32:
		i64 = int64(v)
	case uint64:
		i64 = int64(v)
	default:
		return val
	}

	switch targetType {
	case dukdb.TYPE_TINYINT:
		return int8(i64)
	case dukdb.TYPE_SMALLINT:
		return int16(i64)
	case dukdb.TYPE_INTEGER:
		return int32(i64)
	case dukdb.TYPE_BIGINT:
		return i64
	case dukdb.TYPE_UTINYINT:
		return uint8(i64)
	case dukdb.TYPE_USMALLINT:
		return uint16(i64)
	case dukdb.TYPE_UINTEGER:
		return uint32(i64)
	case dukdb.TYPE_UBIGINT:
		return uint64(i64)
	default:
		return i64
	}
}

// convertToFloat converts an integer to a floating point type.
func convertToFloat(val any, targetType dukdb.Type) any {
	var f64 float64
	switch v := val.(type) {
	case int8:
		f64 = float64(v)
	case int16:
		f64 = float64(v)
	case int32:
		f64 = float64(v)
	case int64:
		f64 = float64(v)
	case uint8:
		f64 = float64(v)
	case uint16:
		f64 = float64(v)
	case uint32:
		f64 = float64(v)
	case uint64:
		f64 = float64(v)
	case float32:
		f64 = float64(v)
	case float64:
		f64 = v
	default:
		return val
	}

	if targetType == dukdb.TYPE_FLOAT {
		return float32(f64)
	}
	return f64
}

// convertFloat widens a float value.
func convertFloat(val any, targetType dukdb.Type) any {
	switch v := val.(type) {
	case float32:
		if targetType == dukdb.TYPE_DOUBLE {
			return float64(v)
		}
		return v
	case float64:
		if targetType == dukdb.TYPE_FLOAT {
			return float32(v)
		}
		return v
	default:
		return val
	}
}

// ConcatenateChunks combines multiple data chunks into a single chunk.
// All chunks must have the same schema (types).
// If any chunk is nil, it is skipped.
func ConcatenateChunks(chunks []*storage.DataChunk) *storage.DataChunk {
	// Count total rows and determine types from first non-nil chunk
	totalRows := 0
	var types []dukdb.Type
	for _, chunk := range chunks {
		if chunk != nil && chunk.Count() > 0 {
			totalRows += chunk.Count()
			if types == nil {
				types = chunk.Types()
			}
		}
	}

	if totalRows == 0 || types == nil {
		return nil
	}

	numCols := len(types)

	// Create result chunk
	result := storage.NewDataChunkWithCapacity(types, totalRows)

	// Copy rows from each chunk
	dstRow := 0
	for _, chunk := range chunks {
		if chunk == nil || chunk.Count() == 0 {
			continue
		}
		chunkRows := chunk.Count()
		for row := range chunkRows {
			for col := range numCols {
				val := chunk.GetValue(row, col)
				result.SetValue(dstRow, col, val)
			}
			dstRow++
		}
	}

	result.SetCount(totalRows)
	return result
}

// AddMetadataColumns adds virtual metadata columns to a chunk.
// Supported columns: filename, file_row_number, file_index.
func AddMetadataColumns(
	chunk *storage.DataChunk,
	filename string,
	fileIndex int,
	startRowNumber int,
	options *MultiFileOptions,
) *storage.DataChunk {
	if chunk == nil || options == nil {
		return chunk
	}

	// Count how many metadata columns to add
	metaCols := 0
	if options.Filename {
		metaCols++
	}
	if options.FileRowNumber {
		metaCols++
	}
	if options.FileIndex {
		metaCols++
	}

	if metaCols == 0 {
		return chunk
	}

	rowCount := chunk.Count()

	// Build new types with metadata columns appended
	origTypes := chunk.Types()
	newTypes := make([]dukdb.Type, len(origTypes)+metaCols)
	copy(newTypes, origTypes)

	metaIdx := len(origTypes)
	if options.Filename {
		newTypes[metaIdx] = dukdb.TYPE_VARCHAR
		metaIdx++
	}
	if options.FileRowNumber {
		newTypes[metaIdx] = dukdb.TYPE_BIGINT
		metaIdx++
	}
	if options.FileIndex {
		newTypes[metaIdx] = dukdb.TYPE_INTEGER
		metaIdx++
	}

	// Create new chunk with expanded schema
	result := storage.NewDataChunkWithCapacity(newTypes, rowCount)

	// Copy original data
	numOrigCols := len(origTypes)
	for col := range numOrigCols {
		for row := range rowCount {
			val := chunk.GetValue(row, col)
			result.SetValue(row, col, val)
		}
	}

	// Fill metadata columns
	metaIdx = len(origTypes)
	if options.Filename {
		for row := range rowCount {
			result.SetValue(row, metaIdx, filename)
		}
		metaIdx++
	}
	if options.FileRowNumber {
		for row := range rowCount {
			result.SetValue(row, metaIdx, int64(startRowNumber+row+1)) // 1-indexed
		}
		metaIdx++
	}
	if options.FileIndex {
		for row := range rowCount {
			result.SetValue(row, metaIdx, int32(fileIndex))
		}
		// Note: metaIdx not incremented here since it's the last use
	}

	result.SetCount(chunk.Count())
	return result
}

// ValidateMaxFiles checks if the number of files exceeds the limit.
// Returns an error if the limit is exceeded.
func ValidateMaxFiles(fileCount int, options *MultiFileOptions) error {
	maxFiles := MaxFilesPerGlob
	if options != nil && options.MaxFiles > 0 {
		maxFiles = options.MaxFiles
	}

	if fileCount > maxFiles {
		return fmt.Errorf("%w: %d files (limit: %d)", ErrTooManyFiles, fileCount, maxFiles)
	}

	return nil
}

// ExpandGlobPattern expands a glob pattern to a sorted list of file paths.
// Returns an error if no files match or too many files match.
func ExpandGlobPattern(
	fs filesystem.FileSystem,
	pattern string,
	options *MultiFileOptions,
) ([]string, error) {
	paths, err := fs.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob expansion failed: %w", err)
	}

	if len(paths) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoFilesMatched, pattern)
	}

	if err := ValidateMaxFiles(len(paths), options); err != nil {
		return nil, err
	}

	// Sort alphabetically (should already be sorted by Glob, but ensure it)
	sort.Strings(paths)

	return paths, nil
}

// IsGlobPattern returns true if the path contains glob wildcard characters.
func IsGlobPattern(path string) bool {
	for _, c := range path {
		switch c {
		case '*', '?', '[':
			return true
		}
	}
	return false
}
