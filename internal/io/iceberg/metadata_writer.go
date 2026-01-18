// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements the MetadataWriter for writing Iceberg metadata files.
package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// MetadataWriter provides methods for writing Iceberg metadata files.
type MetadataWriter struct {
	// fs is the filesystem for writing files.
	fs filesystem.FileSystem

	// tableLocation is the base location of the table.
	tableLocation string
}

// NewMetadataWriter creates a new MetadataWriter.
func NewMetadataWriter(fs filesystem.FileSystem, tableLocation string) *MetadataWriter {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	return &MetadataWriter{
		fs:            fs,
		tableLocation: tableLocation,
	}
}

// IcebergTableMetadata represents the metadata.json structure.
type IcebergTableMetadata struct {
	FormatVersion      int                       `json:"format-version"`
	TableUUID          string                    `json:"table-uuid"`
	Location           string                    `json:"location"`
	LastUpdatedMs      int64                     `json:"last-updated-ms"`
	LastColumnID       int                       `json:"last-column-id"`
	CurrentSchemaID    int                       `json:"current-schema-id"`
	Schemas            []IcebergSchema           `json:"schemas"`
	DefaultSpecID      int                       `json:"default-spec-id"`
	PartitionSpecs     []IcebergPartitionSpec    `json:"partition-specs"`
	LastPartitionID    int                       `json:"last-partition-id"`
	Properties         map[string]string         `json:"properties"`
	CurrentSnapshotID  *int64                    `json:"current-snapshot-id"`
	Snapshots          []IcebergSnapshot         `json:"snapshots"`
	SnapshotLog        []IcebergSnapshotLogEntry `json:"snapshot-log"`
	LastSequenceNumber int64                     `json:"last-sequence-number"`
	DefaultSortOrderID int                       `json:"default-sort-order-id"`
	SortOrders         []IcebergSortOrder        `json:"sort-orders"`
}

// IcebergSchema represents a schema in metadata.json.
type IcebergSchema struct {
	Type     string         `json:"type"`
	SchemaID int            `json:"schema-id"`
	Fields   []IcebergField `json:"fields"`
}

// IcebergField represents a field in a schema.
type IcebergField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// IcebergPartitionSpec represents a partition spec in metadata.json.
type IcebergPartitionSpec struct {
	SpecID int                     `json:"spec-id"`
	Fields []IcebergPartitionField `json:"fields"`
}

// IcebergPartitionField represents a partition field.
type IcebergPartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// IcebergSnapshot represents a snapshot in metadata.json.
type IcebergSnapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary"`
	SchemaID         int               `json:"schema-id"`
}

// IcebergSnapshotLogEntry represents a snapshot log entry.
type IcebergSnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

// IcebergSortOrder represents a sort order in metadata.json.
type IcebergSortOrder struct {
	OrderID int                `json:"order-id"`
	Fields  []IcebergSortField `json:"fields"`
}

// IcebergSortField represents a sort field.
type IcebergSortField struct {
	Transform string `json:"transform"`
	SourceID  int    `json:"source-id"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

// WriteMetadataJSON writes the metadata.json file.
func (w *MetadataWriter) WriteMetadataJSON(
	_ context.Context,
	metadata *IcebergTableMetadata,
) error {
	// Determine metadata file name
	metadataPath := filepath.Join(w.tableLocation, "metadata", "v1.metadata.json")

	// Create the file
	file, err := w.fs.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Encode as JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to write metadata JSON: %w", err)
	}

	return nil
}

// WriteVersionHint writes the version-hint.text file.
func (w *MetadataWriter) WriteVersionHint(_ context.Context, version int) error {
	versionHintPath := filepath.Join(w.tableLocation, "metadata", "version-hint.text")

	file, err := w.fs.Create(versionHintPath)
	if err != nil {
		return fmt.Errorf("failed to create version-hint.text: %w", err)
	}
	defer func() { _ = file.Close() }()

	if _, err := fmt.Fprintf(file, "%d", version); err != nil {
		return fmt.Errorf("failed to write version hint: %w", err)
	}

	return nil
}

// ManifestListEntryWrite represents an entry in a manifest list for writing.
type ManifestListEntryWrite struct {
	ManifestPath       string `avro:"manifest_path"`
	ManifestLength     int64  `avro:"manifest_length"`
	PartitionSpecID    int32  `avro:"partition_spec_id"`
	Content            int32  `avro:"content"`
	SequenceNumber     int64  `avro:"sequence_number"`
	MinSequenceNumber  int64  `avro:"min_sequence_number"`
	AddedSnapshotID    int64  `avro:"added_snapshot_id"`
	AddedFilesCount    int32  `avro:"added_files_count"`
	ExistingFilesCount int32  `avro:"existing_files_count"`
	DeletedFilesCount  int32  `avro:"deleted_files_count"`
	AddedRowsCount     int64  `avro:"added_rows_count"`
	ExistingRowsCount  int64  `avro:"existing_rows_count"`
	DeletedRowsCount   int64  `avro:"deleted_rows_count"`
}

// ManifestEntryWrite represents a manifest entry for writing.
type ManifestEntryWrite struct {
	Status      int32              `avro:"status"`
	SnapshotID  *int64             `avro:"snapshot_id"`
	SequenceNum *int64             `avro:"sequence_number"`
	DataFile    DataFileEntryWrite `avro:"data_file"`
}

// DataFileEntryWrite represents the data_file field in a manifest entry.
type DataFileEntryWrite struct {
	ContentType   int32            `avro:"content"`
	FilePath      string           `avro:"file_path"`
	FileFormat    string           `avro:"file_format"`
	RecordCount   int64            `avro:"record_count"`
	FileSizeBytes int64            `avro:"file_size_in_bytes"`
	ColumnSizes   map[int32]int64  `avro:"column_sizes"`
	ValueCounts   map[int32]int64  `avro:"value_counts"`
	NullCounts    map[int32]int64  `avro:"null_value_counts"`
	NaNCounts     map[int32]int64  `avro:"nan_value_counts"`
	LowerBounds   map[int32][]byte `avro:"lower_bounds"`
	UpperBounds   map[int32][]byte `avro:"upper_bounds"`
}

// manifestListSchema is the AVRO schema for manifest list entries.
const manifestListSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int", "default": 0},
    {"name": "sequence_number", "type": "long", "default": 0},
    {"name": "min_sequence_number", "type": "long", "default": 0},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": "int"},
    {"name": "existing_files_count", "type": "int"},
    {"name": "deleted_files_count", "type": "int"},
    {"name": "added_rows_count", "type": "long"},
    {"name": "existing_rows_count", "type": "long"},
    {"name": "deleted_rows_count", "type": "long"}
  ]
}`

// manifestEntrySchema is the AVRO schema for manifest entries.
const manifestEntrySchema = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null},
    {"name": "sequence_number", "type": ["null", "long"], "default": null},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int", "default": 0},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null}
      ]
    }}
  ]
}`

// WriteManifest writes a manifest file containing data file entries.
// Returns the path to the written manifest file.
func (w *MetadataWriter) WriteManifest(
	_ context.Context,
	dataFiles []*WrittenDataFile,
	schema *iceberg.Schema,
	snapshotID int64,
	sequenceNumber int64,
) (string, error) {
	// Generate manifest file name
	manifestUUID, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate manifest UUID: %w", err)
	}

	manifestFileName := fmt.Sprintf("%s-m0.avro", manifestUUID.String())
	manifestPath := filepath.Join(w.tableLocation, "metadata", manifestFileName)

	// Create the manifest file
	file, err := w.fs.Create(manifestPath)
	if err != nil {
		return "", fmt.Errorf("failed to create manifest file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Create OCF encoder with metadata
	metadata := map[string][]byte{
		"schema":         []byte(schemaToJSONString(schema)),
		"partition-spec": []byte("[]"),
		"format-version": []byte("2"),
		"content":        []byte("data"),
	}

	encoder, err := ocf.NewEncoder(manifestEntrySchema, file, ocf.WithMetadata(metadata))
	if err != nil {
		return "", fmt.Errorf("failed to create manifest encoder: %w", err)
	}
	defer func() { _ = encoder.Close() }()

	// Write each data file as a manifest entry
	for _, df := range dataFiles {
		entry := map[string]any{
			"status":          int32(1), // ADDED status
			"snapshot_id":     map[string]any{"long": snapshotID},
			"sequence_number": map[string]any{"long": sequenceNumber},
			"data_file": map[string]any{
				"content":            int32(0), // DATA content type
				"file_path":          df.Path,
				"file_format":        string(df.Format),
				"record_count":       df.RecordCount,
				"file_size_in_bytes": df.FileSizeBytes,
				"column_sizes":       map[string]any{"null": nil},
				"value_counts":       map[string]any{"null": nil},
				"null_value_counts":  map[string]any{"null": nil},
				"nan_value_counts":   map[string]any{"null": nil},
				"lower_bounds":       map[string]any{"null": nil},
				"upper_bounds":       map[string]any{"null": nil},
			},
		}

		if err := encoder.Encode(entry); err != nil {
			return "", fmt.Errorf("failed to write manifest entry: %w", err)
		}
	}

	return manifestPath, nil
}

// WriteManifestList writes a manifest list file.
// Returns the path to the written manifest list file.
func (w *MetadataWriter) WriteManifestList(
	_ context.Context,
	manifestPath string,
	dataFiles []*WrittenDataFile,
	snapshotID int64,
	sequenceNumber int64,
) (string, error) {
	// Generate manifest list file name
	manifestListUUID, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate manifest list UUID: %w", err)
	}

	manifestListFileName := fmt.Sprintf("snap-%d-%s.avro", snapshotID, manifestListUUID.String())
	manifestListPath := filepath.Join(w.tableLocation, "metadata", manifestListFileName)

	// Create the manifest list file
	file, err := w.fs.Create(manifestListPath)
	if err != nil {
		return "", fmt.Errorf("failed to create manifest list file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Create OCF encoder
	encoder, err := ocf.NewEncoder(manifestListSchema, file)
	if err != nil {
		return "", fmt.Errorf("failed to create manifest list encoder: %w", err)
	}
	defer func() { _ = encoder.Close() }()

	// Get manifest file size
	var manifestLength int64
	if info, err := w.fs.Stat(manifestPath); err == nil {
		manifestLength = info.Size()
	}

	// Calculate total rows
	var totalRows int64
	for _, df := range dataFiles {
		totalRows += df.RecordCount
	}

	// Write manifest list entry
	entry := ManifestListEntryWrite{
		ManifestPath:       manifestPath,
		ManifestLength:     manifestLength,
		PartitionSpecID:    0,
		Content:            0, // DATA content
		SequenceNumber:     sequenceNumber,
		MinSequenceNumber:  sequenceNumber,
		AddedSnapshotID:    snapshotID,
		AddedFilesCount:    int32(len(dataFiles)),
		ExistingFilesCount: 0,
		DeletedFilesCount:  0,
		AddedRowsCount:     totalRows,
		ExistingRowsCount:  0,
		DeletedRowsCount:   0,
	}

	if err := encoder.Encode(entry); err != nil {
		return "", fmt.Errorf("failed to write manifest list entry: %w", err)
	}

	return manifestListPath, nil
}

// schemaToJSONString converts an Iceberg schema to a JSON string for metadata.
func schemaToJSONString(schema *iceberg.Schema) string {
	if schema == nil {
		return `{"type":"struct","schema-id":0,"fields":[]}`
	}

	fields := make([]map[string]any, 0)
	for _, f := range schema.Fields() {
		field := map[string]any{
			"id":       f.ID,
			"name":     f.Name,
			"required": f.Required,
			"type":     icebergTypeToString(f.Type),
		}
		fields = append(fields, field)
	}

	result := map[string]any{
		"type":      "struct",
		"schema-id": schema.ID,
		"fields":    fields,
	}

	data, err := json.Marshal(result)
	if err != nil {
		return `{"type":"struct","schema-id":0,"fields":[]}`
	}

	return string(data)
}

// Verify MetadataWriter works with filesystem.
var _ io.Writer = (filesystem.File)(nil)
