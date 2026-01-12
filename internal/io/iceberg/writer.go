// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements the Writer for writing data to Iceberg tables.
package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	parquetio "github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Compression codec constants.
const (
	codecSnappy = "snappy"
)

// Iceberg type string constants.
const (
	icebergTypeString = "string"
)

// WriterOptions contains configuration options for the Iceberg writer.
type WriterOptions struct {
	// TableLocation is the base directory for the Iceberg table.
	// The writer will create metadata/ and data/ subdirectories.
	TableLocation string

	// Schema is the Iceberg schema for the table.
	// If nil, a schema is inferred from the first DataChunk written.
	Schema *iceberg.Schema

	// PartitionSpec is the partition specification for the table.
	// If nil, an unpartitioned spec is used.
	PartitionSpec *iceberg.PartitionSpec

	// Properties are table properties to store in metadata.
	Properties map[string]string

	// CompressionCodec is the Parquet compression codec to use.
	// Supported values: snappy, gzip, zstd, lz4, brotli, none.
	// Default is snappy.
	CompressionCodec string

	// RowGroupSize is the number of rows per Parquet row group.
	// Default is 100000.
	RowGroupSize int

	// Filesystem specifies the filesystem to use (nil for local filesystem).
	Filesystem filesystem.FileSystem

	// FormatVersion specifies the Iceberg format version (1 or 2).
	// Default is 2.
	FormatVersion FormatVersion
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		CompressionCodec: codecSnappy,
		RowGroupSize:     parquetio.DefaultRowGroupSize,
		FormatVersion:    FormatVersionV2,
		Properties:       make(map[string]string),
	}
}

// Writer writes DataChunks to an Iceberg table format.
// It creates Parquet data files and generates the necessary metadata
// (manifest files, manifest list, and metadata.json).
type Writer struct {
	// opts contains the writer configuration.
	opts *WriterOptions

	// fs is the filesystem for writing files.
	fs filesystem.FileSystem

	// tableUUID is the unique identifier for this table.
	tableUUID uuid.UUID

	// schema is the Iceberg schema.
	schema *iceberg.Schema

	// partitionSpec is the partition specification.
	partitionSpec iceberg.PartitionSpec

	// columns are the column names.
	columns []string

	// columnTypes are the DuckDB column types.
	columnTypes []dukdb.Type

	// dataFiles tracks all written data files.
	dataFiles []*WrittenDataFile

	// currentFile is the current Parquet writer.
	currentFile *DataFileWriter

	// rowsInCurrentFile tracks rows in the current file.
	rowsInCurrentFile int64

	// totalRowsWritten tracks total rows written.
	totalRowsWritten int64

	// closed tracks if the writer has been closed.
	closed bool

	// schemaID is the ID of the current schema.
	schemaID int

	// snapshotID is the ID of the snapshot being created.
	snapshotID int64

	// sequenceNumber is the sequence number for this snapshot.
	sequenceNumber int64

	// ctx is the context for operations.
	ctx context.Context
}

// WrittenDataFile represents a data file that has been written.
type WrittenDataFile struct {
	// Path is the absolute path to the data file.
	Path string
	// RelativePath is the path relative to the table location.
	RelativePath string
	// Format is the file format (always parquet for now).
	Format FileFormat
	// RecordCount is the number of records in the file.
	RecordCount int64
	// FileSizeBytes is the size of the file in bytes.
	FileSizeBytes int64
	// PartitionData contains partition values for this file.
	PartitionData map[string]any
	// ColumnSizes maps column IDs to their sizes.
	ColumnSizes map[int]int64
	// ValueCounts maps column IDs to value counts.
	ValueCounts map[int]int64
	// NullCounts maps column IDs to null counts.
	NullCounts map[int]int64
	// LowerBounds maps column IDs to lower bound values.
	LowerBounds map[int][]byte
	// UpperBounds maps column IDs to upper bound values.
	UpperBounds map[int][]byte
}

// DataFileWriter writes a single Parquet data file.
type DataFileWriter struct {
	// path is the file path.
	path string
	// relativePath is the path relative to table location.
	relativePath string
	// writer is the underlying Parquet writer.
	writer *parquetio.Writer
	// rowCount tracks rows written.
	rowCount int64
}

// NewWriter creates a new Iceberg table writer.
func NewWriter(ctx context.Context, opts *WriterOptions) (*Writer, error) {
	if opts == nil {
		opts = DefaultWriterOptions()
	}

	if opts.TableLocation == "" {
		return nil, fmt.Errorf("iceberg: TableLocation is required")
	}

	// Apply defaults
	if opts.CompressionCodec == "" {
		opts.CompressionCodec = codecSnappy
	}
	if opts.RowGroupSize <= 0 {
		opts.RowGroupSize = parquetio.DefaultRowGroupSize
	}
	if opts.FormatVersion == 0 {
		opts.FormatVersion = FormatVersionV2
	}
	if opts.Properties == nil {
		opts.Properties = make(map[string]string)
	}

	// Use local filesystem if not specified
	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Create table directories
	if err := fs.MkdirAll(filepath.Join(opts.TableLocation, "metadata")); err != nil {
		return nil, fmt.Errorf("iceberg: failed to create metadata directory: %w", err)
	}
	if err := fs.MkdirAll(filepath.Join(opts.TableLocation, "data")); err != nil {
		return nil, fmt.Errorf("iceberg: failed to create data directory: %w", err)
	}

	// Generate table UUID
	tableUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("iceberg: failed to generate table UUID: %w", err)
	}

	// Generate snapshot ID (use timestamp in microseconds)
	snapshotID := time.Now().UnixMicro()

	// Default partition spec if not provided
	partitionSpec := iceberg.NewPartitionSpec()
	if opts.PartitionSpec != nil {
		partitionSpec = *opts.PartitionSpec
	}

	w := &Writer{
		opts:           opts,
		fs:             fs,
		tableUUID:      tableUUID,
		schema:         opts.Schema,
		partitionSpec:  partitionSpec,
		dataFiles:      make([]*WrittenDataFile, 0),
		snapshotID:     snapshotID,
		sequenceNumber: 1,
		ctx:            ctx,
	}

	return w, nil
}

// SetSchema sets the column names for the output.
// This must be called before WriteChunk if custom column names are needed.
func (w *Writer) SetSchema(columns []string) error {
	if w.closed {
		return fmt.Errorf("iceberg: writer is closed")
	}

	w.columns = make([]string, len(columns))
	copy(w.columns, columns)

	return nil
}

// SetTypes sets the column types for the output.
// This must be called before WriteChunk to define the schema.
func (w *Writer) SetTypes(types []dukdb.Type) error {
	if w.closed {
		return fmt.Errorf("iceberg: writer is closed")
	}

	w.columnTypes = make([]dukdb.Type, len(types))
	copy(w.columnTypes, types)

	return nil
}

// WriteChunk writes a DataChunk to the Iceberg table.
// Data is written as Parquet files in the data/ directory.
func (w *Writer) WriteChunk(chunk *storage.DataChunk) error {
	if w.closed {
		return fmt.Errorf("iceberg: writer is closed")
	}

	if chunk == nil || chunk.Count() == 0 {
		return nil
	}

	// Infer schema from first chunk if not set
	if w.columns == nil {
		w.columns = generateColumnNames(chunk.ColumnCount())
	}

	if w.columnTypes == nil {
		w.columnTypes = chunk.Types()
	}

	// Build Iceberg schema if not provided
	if w.schema == nil {
		schema, err := w.buildIcebergSchema()
		if err != nil {
			return fmt.Errorf("iceberg: failed to build schema: %w", err)
		}
		w.schema = schema
	}

	// Create a new data file if needed
	if w.currentFile == nil {
		if err := w.startNewDataFile(); err != nil {
			return err
		}
	}

	// Write the chunk to the current file
	if err := w.currentFile.writer.WriteChunk(chunk); err != nil {
		return fmt.Errorf("iceberg: failed to write chunk: %w", err)
	}

	w.currentFile.rowCount += int64(chunk.Count())
	w.rowsInCurrentFile += int64(chunk.Count())
	w.totalRowsWritten += int64(chunk.Count())

	return nil
}

// startNewDataFile creates a new data file for writing.
func (w *Writer) startNewDataFile() error {
	// Generate unique file name
	fileUUID, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("iceberg: failed to generate file UUID: %w", err)
	}

	relativePath := filepath.Join("data", fmt.Sprintf("%s.parquet", fileUUID.String()))
	absolutePath := filepath.Join(w.opts.TableLocation, relativePath)

	// Create Parquet writer options
	parquetOpts := &parquetio.WriterOptions{
		Codec:        w.opts.CompressionCodec,
		RowGroupSize: w.opts.RowGroupSize,
		Overwrite:    true,
	}

	// Create the Parquet writer
	parquetWriter, err := parquetio.NewWriterToPath(absolutePath, parquetOpts)
	if err != nil {
		return fmt.Errorf("iceberg: failed to create parquet writer: %w", err)
	}

	// Set schema
	if err := parquetWriter.SetSchema(w.columns); err != nil {
		return fmt.Errorf("iceberg: failed to set schema: %w", err)
	}

	// Set types
	if err := parquetWriter.SetTypes(w.columnTypes); err != nil {
		return fmt.Errorf("iceberg: failed to set types: %w", err)
	}

	w.currentFile = &DataFileWriter{
		path:         absolutePath,
		relativePath: relativePath,
		writer:       parquetWriter,
		rowCount:     0,
	}

	w.rowsInCurrentFile = 0

	return nil
}

// finishCurrentFile closes the current data file and records it.
func (w *Writer) finishCurrentFile() error {
	if w.currentFile == nil {
		return nil
	}

	// Close the Parquet writer
	if err := w.currentFile.writer.Close(); err != nil {
		return fmt.Errorf("iceberg: failed to close parquet writer: %w", err)
	}

	// Get file size
	var fileSize int64
	if info, err := w.fs.Stat(w.currentFile.path); err == nil {
		fileSize = info.Size()
	}

	// Record the written file
	dataFile := &WrittenDataFile{
		Path:          w.currentFile.path,
		RelativePath:  w.currentFile.relativePath,
		Format:        FileFormatParquet,
		RecordCount:   w.currentFile.rowCount,
		FileSizeBytes: fileSize,
		PartitionData: make(map[string]any),
		ColumnSizes:   make(map[int]int64),
		ValueCounts:   make(map[int]int64),
		NullCounts:    make(map[int]int64),
		LowerBounds:   make(map[int][]byte),
		UpperBounds:   make(map[int][]byte),
	}

	w.dataFiles = append(w.dataFiles, dataFile)
	w.currentFile = nil

	return nil
}

// Close finalizes the Iceberg table by writing all metadata.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Finish the current data file
	if err := w.finishCurrentFile(); err != nil {
		return err
	}

	// If no data was written, create an empty table
	if len(w.dataFiles) == 0 && w.schema == nil {
		// Create a minimal schema
		w.schema = iceberg.NewSchema(0)
	}

	// Write metadata files
	metadataWriter := NewMetadataWriter(w.fs, w.opts.TableLocation)

	// Build table metadata
	metadata, err := w.buildTableMetadata()
	if err != nil {
		return fmt.Errorf("iceberg: failed to build table metadata: %w", err)
	}

	// Write manifest files and manifest list
	var manifestListPath string
	if len(w.dataFiles) > 0 {
		manifestPath, err := metadataWriter.WriteManifest(w.ctx, w.dataFiles, w.schema, w.snapshotID, w.sequenceNumber)
		if err != nil {
			return fmt.Errorf("iceberg: failed to write manifest: %w", err)
		}

		manifestListPath, err = metadataWriter.WriteManifestList(w.ctx, manifestPath, w.dataFiles, w.snapshotID, w.sequenceNumber)
		if err != nil {
			return fmt.Errorf("iceberg: failed to write manifest list: %w", err)
		}
	}

	// Add snapshot if we have data
	if manifestListPath != "" {
		metadata.Snapshots = append(metadata.Snapshots, IcebergSnapshot{
			SnapshotID:       w.snapshotID,
			SequenceNumber:   w.sequenceNumber,
			TimestampMs:      time.Now().UnixMilli(),
			ManifestList:     manifestListPath,
			Summary:          w.buildSnapshotSummary(),
			SchemaID:         w.schemaID,
			ParentSnapshotID: nil,
		})
		metadata.CurrentSnapshotID = &w.snapshotID
		metadata.SnapshotLog = append(metadata.SnapshotLog, IcebergSnapshotLogEntry{
			SnapshotID:  w.snapshotID,
			TimestampMs: time.Now().UnixMilli(),
		})
	}

	// Write metadata.json
	if err := metadataWriter.WriteMetadataJSON(w.ctx, metadata); err != nil {
		return fmt.Errorf("iceberg: failed to write metadata.json: %w", err)
	}

	// Write version hint
	if err := metadataWriter.WriteVersionHint(w.ctx, 1); err != nil {
		return fmt.Errorf("iceberg: failed to write version-hint.text: %w", err)
	}

	return nil
}

// buildIcebergSchema creates an Iceberg schema from the column names and types.
func (w *Writer) buildIcebergSchema() (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, len(w.columns))

	for i, name := range w.columns {
		var colType dukdb.Type
		if i < len(w.columnTypes) {
			colType = w.columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}

		icebergType, err := duckDBTypeToIcebergType(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %q: %w", name, err)
		}

		fields[i] = iceberg.NestedField{
			ID:       i + 1, // Iceberg field IDs start at 1
			Name:     name,
			Type:     icebergType,
			Required: false, // All columns are nullable by default
		}
	}

	return iceberg.NewSchema(w.schemaID, fields...), nil
}

// buildTableMetadata creates the IcebergTableMetadata structure.
func (w *Writer) buildTableMetadata() (*IcebergTableMetadata, error) {
	// Convert schema to raw format
	schemas := make([]IcebergSchema, 1)
	schemas[0] = schemaToIcebergSchema(w.schema)

	// Convert partition spec
	partitionSpecs := make([]IcebergPartitionSpec, 1)
	partitionSpecs[0] = partitionSpecToIcebergPartitionSpec(w.partitionSpec)

	metadata := &IcebergTableMetadata{
		FormatVersion:          int(w.opts.FormatVersion),
		TableUUID:              w.tableUUID.String(),
		Location:               w.opts.TableLocation,
		LastUpdatedMs:          time.Now().UnixMilli(),
		LastColumnID:           len(w.columns),
		CurrentSchemaID:        w.schemaID,
		Schemas:                schemas,
		DefaultSpecID:          0,
		PartitionSpecs:         partitionSpecs,
		LastPartitionID:        w.partitionSpec.NumFields(),
		Properties:             w.opts.Properties,
		CurrentSnapshotID:      nil,
		Snapshots:              make([]IcebergSnapshot, 0),
		SnapshotLog:            make([]IcebergSnapshotLogEntry, 0),
		LastSequenceNumber:     w.sequenceNumber,
		DefaultSortOrderID:     0,
		SortOrders:             []IcebergSortOrder{{OrderID: 0, Fields: []IcebergSortField{}}},
	}

	return metadata, nil
}

// buildSnapshotSummary creates the snapshot summary.
func (w *Writer) buildSnapshotSummary() map[string]string {
	summary := map[string]string{
		"operation":           "append",
		"added-data-files":    fmt.Sprintf("%d", len(w.dataFiles)),
		"added-records":       fmt.Sprintf("%d", w.totalRowsWritten),
		"total-records":       fmt.Sprintf("%d", w.totalRowsWritten),
		"total-data-files":    fmt.Sprintf("%d", len(w.dataFiles)),
		"total-delete-files":  "0",
		"total-position-deletes": "0",
		"total-equality-deletes": "0",
	}

	var totalSize int64
	for _, df := range w.dataFiles {
		totalSize += df.FileSizeBytes
	}
	summary["added-files-size"] = fmt.Sprintf("%d", totalSize)
	summary["total-files-size"] = fmt.Sprintf("%d", totalSize)

	return summary
}

// generateColumnNames creates default column names.
func generateColumnNames(count int) []string {
	columns := make([]string, count)
	for i := range count {
		columns[i] = fmt.Sprintf("column%d", i)
	}

	return columns
}

// duckDBTypeToIcebergType converts a DuckDB type to an Iceberg type.
//
//nolint:exhaustive // We handle common types; others default to string.
func duckDBTypeToIcebergType(typ dukdb.Type) (iceberg.Type, error) {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return iceberg.BooleanType{}, nil
	case dukdb.TYPE_TINYINT:
		return iceberg.Int32Type{}, nil
	case dukdb.TYPE_SMALLINT:
		return iceberg.Int32Type{}, nil
	case dukdb.TYPE_INTEGER:
		return iceberg.Int32Type{}, nil
	case dukdb.TYPE_BIGINT:
		return iceberg.Int64Type{}, nil
	case dukdb.TYPE_UTINYINT:
		return iceberg.Int32Type{}, nil
	case dukdb.TYPE_USMALLINT:
		return iceberg.Int32Type{}, nil
	case dukdb.TYPE_UINTEGER:
		return iceberg.Int64Type{}, nil
	case dukdb.TYPE_UBIGINT:
		return iceberg.Int64Type{}, nil
	case dukdb.TYPE_FLOAT:
		return iceberg.Float32Type{}, nil
	case dukdb.TYPE_DOUBLE:
		return iceberg.Float64Type{}, nil
	case dukdb.TYPE_VARCHAR:
		return iceberg.StringType{}, nil
	case dukdb.TYPE_BLOB:
		return iceberg.BinaryType{}, nil
	case dukdb.TYPE_DATE:
		return iceberg.DateType{}, nil
	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		return iceberg.TimeType{}, nil
	case dukdb.TYPE_TIMESTAMP:
		return iceberg.TimestampType{}, nil
	case dukdb.TYPE_TIMESTAMP_TZ:
		return iceberg.TimestampTzType{}, nil
	case dukdb.TYPE_TIMESTAMP_MS:
		return iceberg.TimestampType{}, nil
	case dukdb.TYPE_TIMESTAMP_NS:
		return iceberg.TimestampType{}, nil
	case dukdb.TYPE_UUID:
		return iceberg.UUIDType{}, nil
	case dukdb.TYPE_DECIMAL:
		// Default decimal precision/scale
		return iceberg.DecimalTypeOf(38, 9), nil
	default:
		// Default to string for unknown types
		return iceberg.StringType{}, nil
	}
}

// schemaToIcebergSchema converts an iceberg.Schema to IcebergSchema for JSON serialization.
func schemaToIcebergSchema(schema *iceberg.Schema) IcebergSchema {
	fields := make([]IcebergField, 0)
	if schema != nil {
		for _, f := range schema.Fields() {
			fields = append(fields, IcebergField{
				ID:       f.ID,
				Name:     f.Name,
				Required: f.Required,
				Type:     icebergTypeToString(f.Type),
			})
		}
	}

	schemaID := 0
	if schema != nil {
		schemaID = schema.ID
	}

	return IcebergSchema{
		Type:     "struct",
		SchemaID: schemaID,
		Fields:   fields,
	}
}

// icebergTypeToString converts an Iceberg type to its string representation.
func icebergTypeToString(t iceberg.Type) string {
	if t == nil {
		return icebergTypeString
	}

	switch t := t.(type) {
	case iceberg.BooleanType:
		return "boolean"
	case iceberg.Int32Type:
		return "int"
	case iceberg.Int64Type:
		return "long"
	case iceberg.Float32Type:
		return "float"
	case iceberg.Float64Type:
		return "double"
	case iceberg.StringType:
		return icebergTypeString
	case iceberg.BinaryType:
		return "binary"
	case iceberg.DateType:
		return "date"
	case iceberg.TimeType:
		return "time"
	case iceberg.TimestampType:
		return "timestamp"
	case iceberg.TimestampTzType:
		return "timestamptz"
	case iceberg.UUIDType:
		return "uuid"
	case iceberg.FixedType:
		return fmt.Sprintf("fixed[%d]", t.Len())
	case iceberg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale())
	default:
		return icebergTypeString
	}
}

// partitionSpecToIcebergPartitionSpec converts an iceberg.PartitionSpec to IcebergPartitionSpec.
func partitionSpecToIcebergPartitionSpec(spec iceberg.PartitionSpec) IcebergPartitionSpec {
	fields := make([]IcebergPartitionField, 0)
	for i := 0; i < spec.NumFields(); i++ {
		f := spec.Field(i)
		fields = append(fields, IcebergPartitionField{
			SourceID:  f.SourceID,
			FieldID:   f.FieldID,
			Name:      f.Name,
			Transform: f.Transform.String(),
		})
	}

	return IcebergPartitionSpec{
		SpecID: spec.ID(),
		Fields: fields,
	}
}

// Verify Writer implements FileWriter interface at compile time.
var _ fileio.FileWriter = (*Writer)(nil)
