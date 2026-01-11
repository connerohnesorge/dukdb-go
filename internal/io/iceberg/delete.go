// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file provides delete file handling (positional and equality deletes).
// Delete files are used to mark rows as deleted without rewriting data files.
package iceberg

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/hamba/avro/v2/ocf"
)

// DeleteFileApplier is the interface for applying delete files to data.
type DeleteFileApplier interface {
	// ApplyDeletes applies delete files to a data chunk, removing deleted rows.
	// The currentDataFile parameter identifies which data file the chunk came from.
	// The startRowPosition is the 0-indexed position of the first row in the chunk within the data file.
	ApplyDeletes(chunk *storage.DataChunk, currentDataFile string, startRowPosition int64) (*storage.DataChunk, error)

	// LoadDeleteFiles loads delete files for a data file.
	LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error

	// HasDeletes returns true if there are pending deletes to apply.
	HasDeletes() bool

	// Close releases resources held by the applier.
	Close() error
}

// DeleteFile represents an Iceberg delete file.
type DeleteFile struct {
	// Path is the file path.
	Path string
	// Format is the file format (parquet, avro).
	Format FileFormat
	// DeleteType is the type of delete file.
	DeleteType DeleteFileType
	// RecordCount is the number of delete records.
	RecordCount int64
	// FileSizeBytes is the file size.
	FileSizeBytes int64
	// ReferencedDataFile is the data file this delete applies to (for positional deletes).
	// Empty for equality deletes which apply to all data files.
	ReferencedDataFile string
	// EqualityFieldIDs are the field IDs used for equality matching.
	EqualityFieldIDs []int
	// PartitionData contains partition values.
	PartitionData map[string]any
}

// PositionalDeleteRecord represents a single positional delete.
type PositionalDeleteRecord struct {
	// FilePath is the path of the data file containing the deleted row.
	FilePath string
	// Position is the row position (0-indexed) within the data file.
	Position int64
}

// EqualityDeleteRecord represents a single equality delete.
type EqualityDeleteRecord struct {
	// Values contains the field values that identify rows to delete.
	// Map from field name to value.
	Values map[string]any
}

// NoOpDeleteApplier is a delete applier that does nothing (for tables without deletes).
type NoOpDeleteApplier struct{}

// ApplyDeletes returns the chunk unchanged.
func (a *NoOpDeleteApplier) ApplyDeletes(chunk *storage.DataChunk, currentDataFile string, startRowPosition int64) (*storage.DataChunk, error) {
	return chunk, nil
}

// LoadDeleteFiles does nothing.
func (a *NoOpDeleteApplier) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	return nil
}

// HasDeletes returns false.
func (a *NoOpDeleteApplier) HasDeletes() bool {
	return false
}

// Close does nothing.
func (a *NoOpDeleteApplier) Close() error {
	return nil
}

// PositionalDeleteApplier applies positional delete files.
// Positional deletes specify which rows to delete by file path and row position.
type PositionalDeleteApplier struct {
	// deletedPositions maps file paths to sorted slices of deleted positions.
	// Using sorted slices for efficient binary search.
	deletedPositions map[string][]int64
	// fs is the filesystem for reading delete files.
	fs filesystem.FileSystem
	// tableLocation is the base location of the table for resolving relative paths.
	tableLocation string
	// loaded tracks whether delete files have been loaded.
	loaded bool
}

// NewPositionalDeleteApplier creates a new PositionalDeleteApplier.
func NewPositionalDeleteApplier(fs filesystem.FileSystem, tableLocation string) *PositionalDeleteApplier {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}
	return &PositionalDeleteApplier{
		deletedPositions: make(map[string][]int64),
		fs:               fs,
		tableLocation:    tableLocation,
	}
}

// ApplyDeletes filters out deleted rows from a chunk based on positional deletes.
func (a *PositionalDeleteApplier) ApplyDeletes(chunk *storage.DataChunk, currentDataFile string, startRowPosition int64) (*storage.DataChunk, error) {
	if !a.loaded || len(a.deletedPositions) == 0 {
		return chunk, nil
	}

	// Normalize the data file path for matching
	normalizedPath := normalizeFilePath(currentDataFile)

	// Find deleted positions for this data file
	deletedPos, exists := a.deletedPositions[normalizedPath]
	if !exists {
		// Also try with the original path in case normalization differs
		deletedPos, exists = a.deletedPositions[currentDataFile]
		if !exists {
			// No deletes for this file
			return chunk, nil
		}
	}

	// Calculate which positions in the chunk are deleted
	endRowPosition := startRowPosition + int64(chunk.Count())

	// Binary search to find relevant delete positions
	startIdx := sort.Search(len(deletedPos), func(i int) bool {
		return deletedPos[i] >= startRowPosition
	})
	endIdx := sort.Search(len(deletedPos), func(i int) bool {
		return deletedPos[i] >= endRowPosition
	})

	if startIdx >= endIdx {
		// No deletes affect this chunk
		return chunk, nil
	}

	// Build set of deleted row indices within the chunk
	deletedIndices := make(map[int]bool)
	for i := startIdx; i < endIdx; i++ {
		localIdx := int(deletedPos[i] - startRowPosition)
		if localIdx >= 0 && localIdx < chunk.Count() {
			deletedIndices[localIdx] = true
		}
	}

	if len(deletedIndices) == 0 {
		return chunk, nil
	}

	// If all rows are deleted, return empty chunk
	if len(deletedIndices) >= chunk.Count() {
		return createEmptyChunk(chunk), nil
	}

	// Create selection vector with non-deleted rows
	return filterChunkWithDeletes(chunk, deletedIndices)
}

// LoadDeleteFiles loads positional delete files for a data file.
func (a *PositionalDeleteApplier) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	for _, df := range deleteFiles {
		// Only process positional delete files
		if df.Format != FileFormatParquet && df.Format != FileFormatAvro {
			continue
		}

		// Determine content type from file metadata
		// Content type 1 = positional deletes, 2 = equality deletes
		// For positional deletes, the file has columns: file_path, pos
		if err := a.loadPositionalDeleteFile(df); err != nil {
			return fmt.Errorf("failed to load positional delete file %q: %w", df.Path, err)
		}
	}

	a.loaded = true
	return nil
}

// loadPositionalDeleteFile loads a single positional delete file.
func (a *PositionalDeleteApplier) loadPositionalDeleteFile(df *DataFile) error {
	filePath := a.resolveFilePath(df.Path)

	switch df.Format {
	case FileFormatParquet:
		return a.loadPositionalDeleteParquet(filePath)
	case FileFormatAvro:
		return a.loadPositionalDeleteAvro(filePath)
	default:
		return fmt.Errorf("unsupported delete file format: %s", df.Format)
	}
}

// loadPositionalDeleteParquet loads positional deletes from a Parquet file.
// Positional delete files have two columns: file_path (string) and pos (long).
func (a *PositionalDeleteApplier) loadPositionalDeleteParquet(filePath string) error {
	reader, err := parquet.NewReaderFromPath(filePath, &parquet.ReaderOptions{
		Columns:         []string{"file_path", "pos"},
		MaxRowsPerChunk: storage.StandardVectorSize,
	})
	if err != nil {
		return fmt.Errorf("failed to open positional delete file: %w", err)
	}
	defer func() { _ = reader.Close() }()

	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read delete chunk: %w", err)
		}

		// Extract file_path and pos columns
		filePathVec := chunk.GetVector(0)
		posVec := chunk.GetVector(1)

		for i := 0; i < chunk.Count(); i++ {
			filePathVal := filePathVec.GetValue(i)
			posVal := posVec.GetValue(i)

			if filePathVal == nil || posVal == nil {
				continue
			}

			filePath, ok := filePathVal.(string)
			if !ok {
				continue
			}

			pos := toInt64(posVal)
			normalizedPath := normalizeFilePath(filePath)
			a.deletedPositions[normalizedPath] = append(a.deletedPositions[normalizedPath], pos)
		}
	}

	// Sort all position slices for binary search
	for path := range a.deletedPositions {
		sort.Slice(a.deletedPositions[path], func(i, j int) bool {
			return a.deletedPositions[path][i] < a.deletedPositions[path][j]
		})
	}

	return nil
}

// loadPositionalDeleteAvro loads positional deletes from an AVRO file.
func (a *PositionalDeleteApplier) loadPositionalDeleteAvro(filePath string) error {
	file, err := a.fs.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open AVRO delete file: %w", err)
	}
	defer func() { _ = file.Close() }()

	decoder, err := ocf.NewDecoder(file)
	if err != nil {
		return fmt.Errorf("failed to create AVRO decoder: %w", err)
	}

	// AVRO positional delete schema has file_path and pos fields
	type PositionalDeleteEntry struct {
		FilePath string `avro:"file_path"`
		Pos      int64  `avro:"pos"`
	}

	for decoder.HasNext() {
		var entry PositionalDeleteEntry
		if err := decoder.Decode(&entry); err != nil {
			return fmt.Errorf("failed to decode positional delete entry: %w", err)
		}

		normalizedPath := normalizeFilePath(entry.FilePath)
		a.deletedPositions[normalizedPath] = append(a.deletedPositions[normalizedPath], entry.Pos)
	}

	// Sort all position slices for binary search
	for path := range a.deletedPositions {
		sort.Slice(a.deletedPositions[path], func(i, j int) bool {
			return a.deletedPositions[path][i] < a.deletedPositions[path][j]
		})
	}

	return nil
}

// resolveFilePath resolves a file path relative to the table location.
func (a *PositionalDeleteApplier) resolveFilePath(path string) string {
	if filepath.IsAbs(path) ||
		strings.HasPrefix(path, "s3://") ||
		strings.HasPrefix(path, "gs://") ||
		strings.HasPrefix(path, "http://") ||
		strings.HasPrefix(path, "https://") ||
		strings.HasPrefix(path, "file://") {
		return path
	}
	return filepath.Join(a.tableLocation, path)
}

// HasDeletes returns true if there are pending deletes.
func (a *PositionalDeleteApplier) HasDeletes() bool {
	return a.loaded && len(a.deletedPositions) > 0
}

// Close releases resources.
func (a *PositionalDeleteApplier) Close() error {
	a.deletedPositions = nil
	a.loaded = false
	return nil
}

// EqualityDeleteApplier applies equality delete files.
// Equality deletes specify rows to delete by matching column values.
type EqualityDeleteApplier struct {
	// deleteRecords contains the equality delete records.
	deleteRecords []EqualityDeleteRecord
	// fieldMapping maps field IDs to column names.
	fieldMapping map[int]string
	// columnIndices maps column names to their indices in data chunks.
	columnIndices map[string]int
	// fs is the filesystem for reading delete files.
	fs filesystem.FileSystem
	// tableLocation is the base location of the table.
	tableLocation string
	// loaded tracks whether delete files have been loaded.
	loaded bool
	// schema is the table schema for type mapping.
	schema *SchemaMapper
}

// NewEqualityDeleteApplier creates a new EqualityDeleteApplier.
func NewEqualityDeleteApplier(fs filesystem.FileSystem, tableLocation string, columnNames []string) *EqualityDeleteApplier {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Build column indices mapping
	columnIndices := make(map[string]int)
	for i, name := range columnNames {
		columnIndices[name] = i
	}

	return &EqualityDeleteApplier{
		deleteRecords: []EqualityDeleteRecord{},
		fieldMapping:  make(map[int]string),
		columnIndices: columnIndices,
		fs:            fs,
		tableLocation: tableLocation,
		schema:        NewSchemaMapper(),
	}
}

// ApplyDeletes filters out deleted rows from a chunk based on equality deletes.
func (a *EqualityDeleteApplier) ApplyDeletes(chunk *storage.DataChunk, currentDataFile string, startRowPosition int64) (*storage.DataChunk, error) {
	if !a.loaded || len(a.deleteRecords) == 0 {
		return chunk, nil
	}

	// Build set of deleted row indices
	deletedIndices := make(map[int]bool)

	for row := 0; row < chunk.Count(); row++ {
		for _, deleteRec := range a.deleteRecords {
			if a.rowMatchesDelete(chunk, row, deleteRec) {
				deletedIndices[row] = true
				break
			}
		}
	}

	if len(deletedIndices) == 0 {
		return chunk, nil
	}

	// If all rows are deleted, return empty chunk
	if len(deletedIndices) >= chunk.Count() {
		return createEmptyChunk(chunk), nil
	}

	return filterChunkWithDeletes(chunk, deletedIndices)
}

// rowMatchesDelete checks if a row matches an equality delete record.
func (a *EqualityDeleteApplier) rowMatchesDelete(chunk *storage.DataChunk, row int, deleteRec EqualityDeleteRecord) bool {
	for colName, deleteValue := range deleteRec.Values {
		colIdx, exists := a.columnIndices[colName]
		if !exists {
			// Column not in chunk, can't match
			return false
		}

		vec := chunk.GetVector(colIdx)
		if vec == nil {
			return false
		}

		rowValue := vec.GetValue(row)

		// Handle NULL comparison
		if deleteValue == nil && rowValue == nil {
			continue
		}
		if deleteValue == nil || rowValue == nil {
			return false
		}

		// Compare values with type coercion
		if !valuesEqual(rowValue, deleteValue) {
			return false
		}
	}

	return true
}

// LoadDeleteFiles loads equality delete files.
func (a *EqualityDeleteApplier) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	for _, df := range deleteFiles {
		// Only process equality delete files
		if df.Format != FileFormatParquet && df.Format != FileFormatAvro {
			continue
		}

		if err := a.loadEqualityDeleteFile(df); err != nil {
			return fmt.Errorf("failed to load equality delete file %q: %w", df.Path, err)
		}
	}

	a.loaded = true
	return nil
}

// loadEqualityDeleteFile loads a single equality delete file.
func (a *EqualityDeleteApplier) loadEqualityDeleteFile(df *DataFile) error {
	filePath := a.resolveFilePath(df.Path)

	switch df.Format {
	case FileFormatParquet:
		return a.loadEqualityDeleteParquet(filePath, df.EqualityFieldIDs)
	case FileFormatAvro:
		return a.loadEqualityDeleteAvro(filePath)
	default:
		return fmt.Errorf("unsupported delete file format: %s", df.Format)
	}
}

// loadEqualityDeleteParquet loads equality deletes from a Parquet file.
func (a *EqualityDeleteApplier) loadEqualityDeleteParquet(filePath string, equalityFieldIDs []int) error {
	reader, err := parquet.NewReaderFromPath(filePath, &parquet.ReaderOptions{
		MaxRowsPerChunk: storage.StandardVectorSize,
	})
	if err != nil {
		return fmt.Errorf("failed to open equality delete file: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Get column names from the delete file schema
	colNames, err := reader.Schema()
	if err != nil {
		return fmt.Errorf("failed to get delete file schema: %w", err)
	}

	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read delete chunk: %w", err)
		}

		for row := 0; row < chunk.Count(); row++ {
			deleteRec := EqualityDeleteRecord{
				Values: make(map[string]any),
			}

			for colIdx, colName := range colNames {
				vec := chunk.GetVector(colIdx)
				if vec != nil {
					deleteRec.Values[colName] = vec.GetValue(row)
				}
			}

			a.deleteRecords = append(a.deleteRecords, deleteRec)
		}
	}

	return nil
}

// loadEqualityDeleteAvro loads equality deletes from an AVRO file.
func (a *EqualityDeleteApplier) loadEqualityDeleteAvro(filePath string) error {
	file, err := a.fs.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open AVRO delete file: %w", err)
	}
	defer func() { _ = file.Close() }()

	decoder, err := ocf.NewDecoder(file)
	if err != nil {
		return fmt.Errorf("failed to create AVRO decoder: %w", err)
	}

	// Equality delete files use a dynamic schema based on the delete columns
	// We decode into a map to handle any schema
	for decoder.HasNext() {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			return fmt.Errorf("failed to decode equality delete entry: %w", err)
		}

		deleteRec := EqualityDeleteRecord{
			Values: entry,
		}
		a.deleteRecords = append(a.deleteRecords, deleteRec)
	}

	return nil
}

// resolveFilePath resolves a file path relative to the table location.
func (a *EqualityDeleteApplier) resolveFilePath(path string) string {
	if filepath.IsAbs(path) ||
		strings.HasPrefix(path, "s3://") ||
		strings.HasPrefix(path, "gs://") ||
		strings.HasPrefix(path, "http://") ||
		strings.HasPrefix(path, "https://") ||
		strings.HasPrefix(path, "file://") {
		return path
	}
	return filepath.Join(a.tableLocation, path)
}

// HasDeletes returns true if there are pending deletes.
func (a *EqualityDeleteApplier) HasDeletes() bool {
	return a.loaded && len(a.deleteRecords) > 0
}

// Close releases resources.
func (a *EqualityDeleteApplier) Close() error {
	a.deleteRecords = nil
	a.loaded = false
	return nil
}

// CompositeDeleteApplier combines positional and equality delete appliers.
type CompositeDeleteApplier struct {
	positional *PositionalDeleteApplier
	equality   *EqualityDeleteApplier
}

// NewCompositeDeleteApplier creates a composite delete applier.
func NewCompositeDeleteApplier(fs filesystem.FileSystem, tableLocation string, columnNames []string) *CompositeDeleteApplier {
	return &CompositeDeleteApplier{
		positional: NewPositionalDeleteApplier(fs, tableLocation),
		equality:   NewEqualityDeleteApplier(fs, tableLocation, columnNames),
	}
}

// ApplyDeletes applies both positional and equality deletes.
func (a *CompositeDeleteApplier) ApplyDeletes(chunk *storage.DataChunk, currentDataFile string, startRowPosition int64) (*storage.DataChunk, error) {
	// Apply positional deletes first
	result, err := a.positional.ApplyDeletes(chunk, currentDataFile, startRowPosition)
	if err != nil {
		return nil, err
	}

	// Then apply equality deletes
	result, err = a.equality.ApplyDeletes(result, currentDataFile, startRowPosition)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// LoadDeleteFiles loads delete files, routing to appropriate handler.
func (a *CompositeDeleteApplier) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	positionalDeletes := make([]*DataFile, 0)
	equalityDeletes := make([]*DataFile, 0)

	for _, df := range deleteFiles {
		// Determine delete type based on content field or presence of equality_ids
		if len(df.EqualityFieldIDs) > 0 {
			equalityDeletes = append(equalityDeletes, df)
		} else {
			// Assume positional if no equality field IDs
			positionalDeletes = append(positionalDeletes, df)
		}
	}

	if len(positionalDeletes) > 0 {
		if err := a.positional.LoadDeleteFiles(dataFile, positionalDeletes); err != nil {
			return err
		}
	}

	if len(equalityDeletes) > 0 {
		if err := a.equality.LoadDeleteFiles(dataFile, equalityDeletes); err != nil {
			return err
		}
	}

	return nil
}

// HasDeletes returns true if there are any pending deletes.
func (a *CompositeDeleteApplier) HasDeletes() bool {
	return a.positional.HasDeletes() || a.equality.HasDeletes()
}

// Close releases resources from both appliers.
func (a *CompositeDeleteApplier) Close() error {
	var firstErr error
	if err := a.positional.Close(); err != nil {
		firstErr = err
	}
	if err := a.equality.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// DeleteApplierWithTracking wraps a DeleteFileApplier to track row positions.
type DeleteApplierWithTracking struct {
	applier              DeleteFileApplier
	currentDataFile      string
	currentStartPosition int64
}

// NewDeleteApplierWithTracking creates a new tracking wrapper.
func NewDeleteApplierWithTracking(applier DeleteFileApplier) *DeleteApplierWithTracking {
	return &DeleteApplierWithTracking{
		applier: applier,
	}
}

// SetCurrentFile sets the current data file being read.
func (a *DeleteApplierWithTracking) SetCurrentFile(dataFile string) {
	a.currentDataFile = dataFile
	a.currentStartPosition = 0
}

// ApplyDeletes applies deletes and updates position tracking.
func (a *DeleteApplierWithTracking) ApplyDeletes(chunk *storage.DataChunk) (*storage.DataChunk, error) {
	result, err := a.applier.ApplyDeletes(chunk, a.currentDataFile, a.currentStartPosition)
	if err != nil {
		return nil, err
	}
	// Update position for next chunk
	a.currentStartPosition += int64(chunk.Count())
	return result, nil
}

// LoadDeleteFiles delegates to underlying applier.
func (a *DeleteApplierWithTracking) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	return a.applier.LoadDeleteFiles(dataFile, deleteFiles)
}

// HasDeletes delegates to underlying applier.
func (a *DeleteApplierWithTracking) HasDeletes() bool {
	return a.applier.HasDeletes()
}

// Close delegates to underlying applier.
func (a *DeleteApplierWithTracking) Close() error {
	return a.applier.Close()
}

// CreateDeleteApplier creates the appropriate delete applier based on delete files.
// If there are no delete files, returns a NoOpDeleteApplier for efficiency.
func CreateDeleteApplier(ctx context.Context, deleteFiles []*DataFile, fs filesystem.FileSystem, tableLocation string, columnNames []string) DeleteFileApplier {
	if len(deleteFiles) == 0 {
		return &NoOpDeleteApplier{}
	}

	// Determine what types of deletes we have
	hasPositional := false
	hasEquality := false

	for _, df := range deleteFiles {
		if len(df.EqualityFieldIDs) > 0 {
			hasEquality = true
		} else {
			hasPositional = true
		}
	}

	// Create appropriate applier based on detected delete types
	switch {
	case hasPositional && hasEquality:
		return NewCompositeDeleteApplier(fs, tableLocation, columnNames)
	case hasEquality:
		return NewEqualityDeleteApplier(fs, tableLocation, columnNames)
	default:
		return NewPositionalDeleteApplier(fs, tableLocation)
	}
}

// IsDeleteSupported returns true since delete file support is now implemented.
func IsDeleteSupported() bool {
	return true
}

// DeleteFileSummary summarizes delete file information for a table.
type DeleteFileSummary struct {
	// TotalDeleteFiles is the total number of delete files.
	TotalDeleteFiles int
	// PositionalDeleteFiles is the count of positional delete files.
	PositionalDeleteFiles int
	// EqualityDeleteFiles is the count of equality delete files.
	EqualityDeleteFiles int
	// TotalDeleteRecords is the total number of delete records.
	TotalDeleteRecords int64
}

// SummarizeDeleteFiles creates a summary of delete files.
func SummarizeDeleteFiles(deleteFiles []*DataFile) *DeleteFileSummary {
	summary := &DeleteFileSummary{}

	for _, df := range deleteFiles {
		summary.TotalDeleteFiles++
		summary.TotalDeleteRecords += df.RecordCount

		if len(df.EqualityFieldIDs) > 0 {
			summary.EqualityDeleteFiles++
		} else {
			summary.PositionalDeleteFiles++
		}
	}

	return summary
}

// Helper functions

// normalizeFilePath normalizes a file path for consistent matching.
// Removes file:// prefix and converts to absolute path form.
func normalizeFilePath(path string) string {
	// Remove file:// prefix if present
	path = strings.TrimPrefix(path, "file://")

	// Convert to clean path
	return filepath.Clean(path)
}

// createEmptyChunk creates an empty chunk with the same schema as the input.
func createEmptyChunk(original *storage.DataChunk) *storage.DataChunk {
	types := original.Types()
	newChunk := storage.NewDataChunkWithCapacity(types, 0)
	newChunk.SetCount(0)
	return newChunk
}

// filterChunkWithDeletes creates a new chunk without the deleted rows.
func filterChunkWithDeletes(chunk *storage.DataChunk, deletedIndices map[int]bool) (*storage.DataChunk, error) {
	// Count non-deleted rows
	keepCount := chunk.Count() - len(deletedIndices)
	if keepCount <= 0 {
		return createEmptyChunk(chunk), nil
	}

	// Create new chunk with capacity for kept rows
	types := chunk.Types()
	newChunk := storage.NewDataChunkWithCapacity(types, keepCount)

	// Copy non-deleted rows
	newRow := 0
	for oldRow := 0; oldRow < chunk.Count(); oldRow++ {
		if deletedIndices[oldRow] {
			continue
		}

		for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
			srcVec := chunk.GetVector(colIdx)
			dstVec := newChunk.GetVector(colIdx)

			if srcVec == nil || dstVec == nil {
				continue
			}

			val := srcVec.GetValue(oldRow)
			if val == nil {
				dstVec.Validity().SetInvalid(newRow)
			} else {
				dstVec.SetValue(newRow, val)
			}
		}
		newRow++
	}

	newChunk.SetCount(newRow)
	return newChunk, nil
}

// valuesEqual compares two values for equality with type coercion.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try direct comparison first
	if reflect.DeepEqual(a, b) {
		return true
	}

	// Try numeric comparison with type coercion
	aNum, aIsNum := toNumber(a)
	bNum, bIsNum := toNumber(b)
	if aIsNum && bIsNum {
		return aNum == bNum
	}

	// Try string comparison
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return aStr == bStr
	}

	// Try byte slice comparison
	aBytes, aIsBytes := a.([]byte)
	bBytes, bIsBytes := b.([]byte)
	if aIsBytes && bIsBytes {
		return string(aBytes) == string(bBytes)
	}

	return false
}

// toNumber converts a value to float64 for comparison.
func toNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// toInt64 converts a value to int64.
func toInt64(val any) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// LegacyDeleteFileApplier provides backward compatibility with the old interface.
// This wrapper adapts the old interface (without file path and position) to the new interface.
type LegacyDeleteFileApplier struct {
	inner         DeleteFileApplier
	currentFile   string
	currentOffset int64
}

// NewLegacyDeleteFileApplier creates a legacy adapter.
func NewLegacyDeleteFileApplier(inner DeleteFileApplier) *LegacyDeleteFileApplier {
	return &LegacyDeleteFileApplier{
		inner: inner,
	}
}

// SetCurrentDataFile sets the current file context for delete application.
func (a *LegacyDeleteFileApplier) SetCurrentDataFile(path string) {
	a.currentFile = path
	a.currentOffset = 0
}

// ApplyDeletes applies deletes using the stored context.
func (a *LegacyDeleteFileApplier) ApplyDeletes(chunk *storage.DataChunk) (*storage.DataChunk, error) {
	result, err := a.inner.ApplyDeletes(chunk, a.currentFile, a.currentOffset)
	if err == nil {
		a.currentOffset += int64(chunk.Count())
	}
	return result, err
}

// LoadDeleteFiles delegates to inner applier.
func (a *LegacyDeleteFileApplier) LoadDeleteFiles(dataFile *DataFile, deleteFiles []*DataFile) error {
	return a.inner.LoadDeleteFiles(dataFile, deleteFiles)
}

// HasDeletes delegates to inner applier.
func (a *LegacyDeleteFileApplier) HasDeletes() bool {
	return a.inner.HasDeletes()
}

// Close delegates to inner applier.
func (a *LegacyDeleteFileApplier) Close() error {
	return a.inner.Close()
}

// DataFileContentType represents the content type of a data/delete file.
type DataFileContentType int

const (
	// DataFileContentData indicates a data file.
	DataFileContentData DataFileContentType = 0
	// DataFileContentPositionDeletes indicates a positional delete file.
	DataFileContentPositionDeletes DataFileContentType = 1
	// DataFileContentEqualityDeletes indicates an equality delete file.
	DataFileContentEqualityDeletes DataFileContentType = 2
)

// GetDataFileContentType returns the content type from a DataFile.
func GetDataFileContentType(df *DataFile) DataFileContentType {
	// Check equality field IDs first
	if len(df.EqualityFieldIDs) > 0 {
		return DataFileContentEqualityDeletes
	}

	// Check file extension or other heuristics
	path := strings.ToLower(df.Path)
	if strings.Contains(path, "delete") || strings.Contains(path, "deletes") {
		return DataFileContentPositionDeletes
	}

	return DataFileContentData
}


// Compile-time interface checks
var _ DeleteFileApplier = (*NoOpDeleteApplier)(nil)
var _ DeleteFileApplier = (*PositionalDeleteApplier)(nil)
var _ DeleteFileApplier = (*EqualityDeleteApplier)(nil)
var _ DeleteFileApplier = (*CompositeDeleteApplier)(nil)

// Compile-time check for dukdb.Type usage
var _ = dukdb.TYPE_BIGINT
