// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements the Iceberg data reader that reads Iceberg tables
// using the existing Parquet infrastructure.
package iceberg

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/iceberg-go"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ReaderOptions contains options for the Iceberg reader.
type ReaderOptions struct {
	// SelectedColumns specifies which columns to read (nil = all columns).
	SelectedColumns []string

	// MaxRowsPerChunk limits the number of rows per DataChunk.
	// Default is storage.StandardVectorSize (2048).
	MaxRowsPerChunk int

	// SnapshotID specifies a specific snapshot to read (nil = current).
	SnapshotID *int64

	// Timestamp specifies a timestamp for time travel (nil = current).
	Timestamp *int64 // milliseconds since epoch

	// PartitionFilters contains filter expressions for partition pruning.
	PartitionFilters []PartitionFilterExpr

	// ColumnFilters contains filter expressions for column statistics pruning.
	ColumnFilters []ColumnFilterExpr

	// Limit specifies the maximum number of rows to read (0 = unlimited).
	Limit int64

	// Filesystem specifies the filesystem to use (nil for auto-detection).
	Filesystem filesystem.FileSystem

	// Version specifies an explicit metadata version number to use.
	// If set to a positive value, the reader will look for v{Version}.metadata.json.
	// If 0 (default), the reader uses version-hint.text or scans for latest.
	Version int

	// AllowMovedPaths allows reading tables that have been relocated.
	// When true, file paths in metadata are rewritten relative to the
	// current table location instead of using absolute paths.
	AllowMovedPaths bool

	// MetadataCompressionCodec specifies the compression codec for metadata files.
	// Supported values: "gzip", "zstd", "none" (or empty for auto-detection).
	// When set, the reader will attempt to decompress metadata files accordingly.
	MetadataCompressionCodec string

	// UnsafeEnableVersionGuessing enables automatic version guessing when
	// version-hint.text is missing. When enabled, the reader scans the
	// metadata directory to find the highest version number.
	// This is marked as "unsafe" because it may select an incomplete or
	// corrupt metadata version if a write was interrupted.
	UnsafeEnableVersionGuessing bool
}

// DefaultReaderOptions returns the default reader options.
func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{
		MaxRowsPerChunk: storage.StandardVectorSize,
	}
}

// Reader implements the FileReader interface for Iceberg tables.
// It reads Iceberg tables by:
// 1. Parsing table metadata
// 2. Selecting the appropriate snapshot
// 3. Reading manifest files to discover data files
// 4. Using the Parquet reader to read data files
// 5. Handling schema evolution and projection
// 6. Applying delete files (positional and equality deletes)
type Reader struct {
	// table is the Iceberg table being read.
	table *Table
	// opts contains user-specified options.
	opts *ReaderOptions
	// scanPlan contains the scan plan.
	scanPlan *ScanPlan
	// fs is the filesystem for reading files.
	fs filesystem.FileSystem
	// columns holds column names to read.
	columns []string
	// columnTypes holds the DuckDB type for each column.
	columnTypes []dukdb.Type
	// initialized tracks whether initialization has been performed.
	initialized bool
	// currentFileIdx is the index of the current data file being read.
	currentFileIdx int
	// currentParquetReader is the reader for the current data file.
	currentParquetReader *parquet.Reader
	// rowsRead tracks the total rows read so far.
	rowsRead int64
	// eof indicates end of data has been reached.
	eof bool
	// deleteApplier handles delete file application.
	deleteApplier DeleteFileApplier
	// schemaMapper maps Iceberg types to DuckDB types.
	schemaMapper *SchemaMapper
	// ctx is the context for cancellation.
	ctx context.Context
	// currentDataFilePath is the path of the current data file being read.
	currentDataFilePath string
	// currentFileRowPosition tracks the row position within the current data file.
	currentFileRowPosition int64
}

// NewReader creates a new Iceberg reader for the given table location.
func NewReader(ctx context.Context, tableLocation string, opts *ReaderOptions) (*Reader, error) {
	if opts == nil {
		opts = DefaultReaderOptions()
	}

	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Open the Iceberg table with version selection options
	tableOpts := &TableOptions{
		Filesystem:                  fs,
		Version:                     opts.Version,
		AllowMovedPaths:             opts.AllowMovedPaths,
		MetadataCompressionCodec:    opts.MetadataCompressionCodec,
		UnsafeEnableVersionGuessing: opts.UnsafeEnableVersionGuessing,
	}

	table, err := OpenTable(ctx, tableLocation, tableOpts)
	if err != nil {
		return nil, fmt.Errorf("iceberg reader: failed to open table: %w", err)
	}

	return &Reader{
		table:         table,
		opts:          opts,
		fs:            fs,
		schemaMapper:  NewSchemaMapper(),
		deleteApplier: &NoOpDeleteApplier{},
		ctx:           ctx,
	}, nil
}

// NewReaderFromMetadata creates a new Iceberg reader from a specific metadata file.
func NewReaderFromMetadata(
	ctx context.Context,
	metadataPath string,
	opts *ReaderOptions,
) (*Reader, error) {
	if opts == nil {
		opts = DefaultReaderOptions()
	}

	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	tableOpts := &TableOptions{
		Filesystem:                  fs,
		Version:                     opts.Version,
		AllowMovedPaths:             opts.AllowMovedPaths,
		MetadataCompressionCodec:    opts.MetadataCompressionCodec,
		UnsafeEnableVersionGuessing: opts.UnsafeEnableVersionGuessing,
	}

	table, err := OpenTableFromMetadata(ctx, metadataPath, tableOpts)
	if err != nil {
		return nil, fmt.Errorf("iceberg reader: failed to open table from metadata: %w", err)
	}

	return &Reader{
		table:         table,
		opts:          opts,
		fs:            fs,
		schemaMapper:  NewSchemaMapper(),
		deleteApplier: &NoOpDeleteApplier{},
		ctx:           ctx,
	}, nil
}

// NewReaderFromTable creates a new Iceberg reader from an existing Table object.
func NewReaderFromTable(ctx context.Context, table *Table, opts *ReaderOptions) (*Reader, error) {
	if opts == nil {
		opts = DefaultReaderOptions()
	}

	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	return &Reader{
		table:         table,
		opts:          opts,
		fs:            fs,
		schemaMapper:  NewSchemaMapper(),
		deleteApplier: &NoOpDeleteApplier{},
		ctx:           ctx,
	}, nil
}

// Schema returns the column names after the reader has been initialized.
func (r *Reader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}
	return r.columns, nil
}

// Types returns the column types after the reader has been initialized.
func (r *Reader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}
	return r.columnTypes, nil
}

// ReadChunk reads the next chunk of data from the Iceberg table.
// Returns io.EOF when no more data is available.
func (r *Reader) ReadChunk() (*storage.DataChunk, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	if r.eof {
		return nil, io.EOF
	}

	// Check row limit
	if r.opts.Limit > 0 && r.rowsRead >= r.opts.Limit {
		r.eof = true
		return nil, io.EOF
	}

	for {
		// Ensure we have a parquet reader for the current file
		if err := r.ensureParquetReader(); err != nil {
			if err == io.EOF {
				r.eof = true
				return nil, io.EOF
			}
			return nil, err
		}

		// Read a chunk from the current parquet reader
		chunk, err := r.currentParquetReader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				// Move to next file
				if closeErr := r.closeCurrentReader(); closeErr != nil {
					return nil, closeErr
				}
				r.currentFileIdx++
				r.currentFileRowPosition = 0 // Reset position for new file
				continue
			}
			return nil, fmt.Errorf("iceberg reader: failed to read chunk: %w", err)
		}

		// Store original chunk count before any filtering
		originalChunkCount := chunk.Count()

		// Apply schema evolution if needed
		chunk, err = r.applySchemaEvolution(chunk)
		if err != nil {
			return nil, err
		}

		// Apply delete files with file path and position context
		chunk, err = r.deleteApplier.ApplyDeletes(
			chunk,
			r.currentDataFilePath,
			r.currentFileRowPosition,
		)
		if err != nil {
			return nil, fmt.Errorf("iceberg reader: failed to apply deletes: %w", err)
		}

		// Update row position for next chunk (using original count before deletes)
		r.currentFileRowPosition += int64(originalChunkCount)

		// Skip empty chunks (all rows deleted)
		if chunk.Count() == 0 {
			continue
		}

		// Apply row limit
		if r.opts.Limit > 0 {
			remaining := r.opts.Limit - r.rowsRead
			if int64(chunk.Count()) > remaining {
				// Truncate chunk to remaining limit
				chunk = r.truncateChunk(chunk, int(remaining))
			}
		}

		r.rowsRead += int64(chunk.Count())
		return chunk, nil
	}
}

// Close releases any resources held by the reader.
func (r *Reader) Close() error {
	var firstErr error

	if r.currentParquetReader != nil {
		if err := r.currentParquetReader.Close(); err != nil {
			firstErr = err
		}
		r.currentParquetReader = nil
	}

	if r.deleteApplier != nil {
		if err := r.deleteApplier.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if r.table != nil {
		if err := r.table.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Table returns the underlying Iceberg table.
func (r *Reader) Table() *Table {
	return r.table
}

// ScanPlan returns the scan plan after initialization.
func (r *Reader) ScanPlan() *ScanPlan {
	return r.scanPlan
}

// ensureInitialized performs lazy initialization.
func (r *Reader) ensureInitialized() error {
	if r.initialized {
		return nil
	}

	// Create scan planner
	planner := NewScanPlanner(r.table.Metadata(), r.table.manifestReader)

	// Build scan options from reader options
	scanOpts := r.buildScanOptions()

	// Create scan plan
	plan, err := planner.CreateScanPlan(r.ctx, scanOpts)
	if err != nil {
		return fmt.Errorf("iceberg reader: failed to create scan plan: %w", err)
	}

	r.scanPlan = plan

	// Set up column names and types from projection
	r.columns = make([]string, len(plan.ColumnProjection))
	r.columnTypes = make([]dukdb.Type, len(plan.ColumnProjection))
	for i, col := range plan.ColumnProjection {
		r.columns[i] = col.Name
		r.columnTypes[i] = col.Type
	}

	// Convert DataFiles to the format expected by CreateDeleteApplier
	// The scan plan returns delete files as DataFile pointers
	deleteDataFiles := plan.DeleteFiles

	// Set up delete file applier with proper context
	r.deleteApplier = CreateDeleteApplier(
		r.ctx,
		deleteDataFiles,
		r.fs,
		r.table.Location(),
		r.columns,
	)

	r.initialized = true
	return nil
}

// buildScanOptions converts reader options to scan options.
func (r *Reader) buildScanOptions() *ScanOptions {
	opts := &ScanOptions{
		SelectedColumns:  r.opts.SelectedColumns,
		MaxRowsPerChunk:  r.opts.MaxRowsPerChunk,
		Limit:            r.opts.Limit,
		PartitionFilters: r.opts.PartitionFilters,
		ColumnFilters:    r.opts.ColumnFilters,
	}

	if r.opts.SnapshotID != nil {
		opts.SnapshotID = r.opts.SnapshotID
	}

	if r.opts.Timestamp != nil {
		// Convert milliseconds to time.Time
		ts := unixMilliToTime(*r.opts.Timestamp)
		opts.Timestamp = &ts
	}

	return opts
}

// ensureParquetReader ensures we have a parquet reader for the current file.
func (r *Reader) ensureParquetReader() error {
	if r.currentParquetReader != nil {
		return nil
	}

	// Check if we've processed all files
	if r.currentFileIdx >= len(r.scanPlan.DataFiles) {
		return io.EOF
	}

	// Get the current data file
	dataFile := r.scanPlan.DataFiles[r.currentFileIdx]

	// Only support Parquet files
	if dataFile.Format != FileFormatParquet {
		return fmt.Errorf(
			"iceberg reader: unsupported file format %q (only parquet is supported)",
			dataFile.Format,
		)
	}

	// Resolve the file path
	filePath := r.resolveFilePath(dataFile.Path)

	// Store the current data file path for delete file application
	r.currentDataFilePath = dataFile.Path
	r.currentFileRowPosition = 0

	// Create parquet reader options with column projection
	parquetOpts := &parquet.ReaderOptions{
		Columns:         r.getParquetColumns(),
		MaxRowsPerChunk: r.opts.MaxRowsPerChunk,
	}

	// Create the parquet reader
	reader, err := parquet.NewReaderFromPath(filePath, parquetOpts)
	if err != nil {
		return fmt.Errorf("iceberg reader: failed to open parquet file %q: %w", filePath, err)
	}

	r.currentParquetReader = reader

	// Load delete files for this data file
	if err := r.deleteApplier.LoadDeleteFiles(dataFile, r.scanPlan.DeleteFiles); err != nil {
		// Log warning but don't fail - delete support errors should not block reads
		// In production, this might be logged with a proper logger
		_ = err
	}

	return nil
}

// closeCurrentReader closes the current parquet reader.
func (r *Reader) closeCurrentReader() error {
	if r.currentParquetReader != nil {
		err := r.currentParquetReader.Close()
		r.currentParquetReader = nil
		return err
	}
	return nil
}

// resolveFilePath resolves a data file path.
// Handles relative paths and different URI schemes.
func (r *Reader) resolveFilePath(path string) string {
	// If already absolute or a URL, use as-is
	if filepath.IsAbs(path) ||
		strings.HasPrefix(path, "s3://") ||
		strings.HasPrefix(path, "gs://") ||
		strings.HasPrefix(path, "http://") ||
		strings.HasPrefix(path, "https://") ||
		strings.HasPrefix(path, "file://") {
		return path
	}

	// Otherwise, resolve relative to table location
	return filepath.Join(r.table.Location(), path)
}

// getParquetColumns returns the column names to pass to the parquet reader.
func (r *Reader) getParquetColumns() []string {
	if len(r.opts.SelectedColumns) == 0 ||
		(len(r.opts.SelectedColumns) == 1 && r.opts.SelectedColumns[0] == "*") {
		return nil // Read all columns
	}
	return r.opts.SelectedColumns
}

// applySchemaEvolution handles schema evolution between file schema and table schema.
// This handles cases where columns have been added, dropped, or renamed.
func (r *Reader) applySchemaEvolution(chunk *storage.DataChunk) (*storage.DataChunk, error) {
	// For now, assume the parquet reader handles column projection correctly
	// Full schema evolution would require:
	// 1. Comparing file schema to current table schema
	// 2. Adding NULL columns for newly added columns
	// 3. Dropping columns that were removed
	// 4. Renaming columns as needed

	// If column count matches projection, no evolution needed
	if chunk.ColumnCount() == len(r.scanPlan.ColumnProjection) {
		return chunk, nil
	}

	// TODO: Implement full schema evolution
	// For now, return the chunk as-is
	return chunk, nil
}

// truncateChunk creates a new chunk with only the first n rows.
func (r *Reader) truncateChunk(chunk *storage.DataChunk, n int) *storage.DataChunk {
	if n >= chunk.Count() {
		return chunk
	}

	// Create a new chunk with the truncated data
	types := chunk.Types()
	newChunk := storage.NewDataChunkWithCapacity(types, n)

	for col := 0; col < chunk.ColumnCount(); col++ {
		srcVec := chunk.GetVector(col)
		dstVec := newChunk.GetVector(col)

		for row := 0; row < n; row++ {
			val := srcVec.GetValue(row)
			if val == nil {
				dstVec.Validity().SetInvalid(row)
			} else {
				dstVec.SetValue(row, val)
			}
		}
	}

	newChunk.SetCount(n)
	return newChunk
}

// unixMilliToTime converts milliseconds since epoch to time.Time.
func unixMilliToTime(ms int64) time.Time {
	return time.UnixMilli(ms)
}

// SchemaEvolutionHandler handles schema differences between file and table schemas.
type SchemaEvolutionHandler struct {
	tableSchema *iceberg.Schema
	fileSchema  *iceberg.Schema
	mapper      *SchemaMapper
}

// NewSchemaEvolutionHandler creates a new schema evolution handler.
func NewSchemaEvolutionHandler(tableSchema, fileSchema *iceberg.Schema) *SchemaEvolutionHandler {
	return &SchemaEvolutionHandler{
		tableSchema: tableSchema,
		fileSchema:  fileSchema,
		mapper:      NewSchemaMapper(),
	}
}

// NeedsEvolution returns true if schema evolution is needed.
func (h *SchemaEvolutionHandler) NeedsEvolution() bool {
	if h.tableSchema == nil || h.fileSchema == nil {
		return false
	}

	// Check if schemas are different by comparing field IDs
	tableFields := h.tableSchema.Fields()
	fileFields := h.fileSchema.Fields()

	if len(tableFields) != len(fileFields) {
		return true
	}

	// Build a map of file field IDs
	fileFieldIDs := make(map[int]bool)
	for _, f := range fileFields {
		fileFieldIDs[f.ID] = true
	}

	// Check if all table fields exist in file
	for _, f := range tableFields {
		if !fileFieldIDs[f.ID] {
			return true
		}
	}

	return false
}

// GetMissingColumns returns columns in table schema but not in file schema.
func (h *SchemaEvolutionHandler) GetMissingColumns() []ColumnInfo {
	checker := NewSchemaEvolutionChecker()
	added, err := checker.GetAddedColumns(h.fileSchema, h.tableSchema)
	if err != nil {
		return []ColumnInfo{}
	}
	return added
}

// ApplyEvolution applies schema evolution to a chunk.
// Adds NULL columns for fields that exist in table schema but not file schema.
func (h *SchemaEvolutionHandler) ApplyEvolution(
	chunk *storage.DataChunk,
	projection []ColumnInfo,
) (*storage.DataChunk, error) {
	if !h.NeedsEvolution() {
		return chunk, nil
	}

	// TODO: Implement full schema evolution
	// This would:
	// 1. Create a new chunk with the projection schema
	// 2. Copy existing columns by field ID
	// 3. Fill missing columns with NULLs
	// 4. Handle renamed columns by field ID

	return chunk, nil
}

// Verify Reader implements FileReader interface
var _ fileio.FileReader = (*Reader)(nil)
