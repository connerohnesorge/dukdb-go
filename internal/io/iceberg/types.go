// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file defines core Iceberg type definitions and constants.
package iceberg

import (
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// FormatVersion represents the Iceberg table format version.
type FormatVersion int

const (
	// FormatVersionV1 is Iceberg format version 1.
	FormatVersionV1 FormatVersion = 1
	// FormatVersionV2 is Iceberg format version 2.
	FormatVersionV2 FormatVersion = 2
)

// SupportedFormatVersions lists the format versions supported by this implementation.
var SupportedFormatVersions = []FormatVersion{FormatVersionV1, FormatVersionV2}

// FileFormat represents the data file format used in Iceberg tables.
type FileFormat string

const (
	// FileFormatParquet indicates Apache Parquet file format.
	FileFormatParquet FileFormat = "parquet"
	// FileFormatAvro indicates Apache Avro file format.
	FileFormatAvro FileFormat = "avro"
	// FileFormatORC indicates Apache ORC file format.
	FileFormatORC FileFormat = "orc"
)

// ManifestContentType indicates the type of data in a manifest.
type ManifestContentType int

const (
	// ManifestContentData indicates the manifest contains data files.
	ManifestContentData ManifestContentType = 0
	// ManifestContentDeletes indicates the manifest contains delete files.
	ManifestContentDeletes ManifestContentType = 1
)

// DeleteFileType indicates the type of delete file.
type DeleteFileType int

const (
	// DeleteFilePositional indicates a positional delete file.
	DeleteFilePositional DeleteFileType = 1
	// DeleteFileEquality indicates an equality delete file.
	DeleteFileEquality DeleteFileType = 2
)

// TableMetadata contains metadata about an Iceberg table.
type TableMetadata struct {
	// Version is the Iceberg format version (1 or 2).
	Version FormatVersion
	// TableUUID is the unique identifier for the table.
	TableUUID uuid.UUID
	// Location is the base location of the table.
	Location string
	// LastUpdatedMs is the timestamp of the last update in milliseconds.
	LastUpdatedMs int64
	// LastColumnID is the highest column ID in the table.
	LastColumnID int
	// CurrentSchemaID is the ID of the current schema.
	CurrentSchemaID int
	// CurrentSnapshotID is the ID of the current snapshot (nil if no snapshots).
	CurrentSnapshotID *int64
	// DefaultPartitionSpecID is the ID of the default partition spec.
	DefaultPartitionSpecID int
	// Properties contains table properties.
	Properties map[string]string

	// schemas holds all schemas in the table.
	schemas []*iceberg.Schema
	// currentSchema holds the current schema.
	currentSchema *iceberg.Schema
	// partitionSpecs holds all partition specs.
	partitionSpecs []iceberg.PartitionSpec
	// currentPartitionSpec holds the current partition spec.
	currentPartitionSpec iceberg.PartitionSpec
	// snapshots holds all snapshots.
	snapshots []Snapshot
	// snapshotLog holds the snapshot log entries.
	snapshotLog []SnapshotLogEntry
}

// Schemas returns all schemas in the table.
func (tm *TableMetadata) Schemas() []*iceberg.Schema {
	return tm.schemas
}

// CurrentSchema returns the current schema of the table.
func (tm *TableMetadata) CurrentSchema() *iceberg.Schema {
	return tm.currentSchema
}

// PartitionSpecs returns all partition specs in the table.
func (tm *TableMetadata) PartitionSpecs() []iceberg.PartitionSpec {
	return tm.partitionSpecs
}

// PartitionSpec returns the current partition spec.
func (tm *TableMetadata) PartitionSpec() iceberg.PartitionSpec {
	return tm.currentPartitionSpec
}

// Snapshots returns all snapshots.
func (tm *TableMetadata) Snapshots() []Snapshot {
	return tm.snapshots
}

// SnapshotLogs returns the snapshot log entries.
func (tm *TableMetadata) SnapshotLogs() []SnapshotLogEntry {
	return tm.snapshotLog
}

// CurrentSnapshot returns the current snapshot.
func (tm *TableMetadata) CurrentSnapshot() *Snapshot {
	if tm.CurrentSnapshotID == nil {
		return nil
	}

	for i := range tm.snapshots {
		if tm.snapshots[i].SnapshotID == *tm.CurrentSnapshotID {
			return &tm.snapshots[i]
		}
	}

	return nil
}

// SnapshotByID returns a snapshot by its ID.
func (tm *TableMetadata) SnapshotByID(id int64) *Snapshot {
	for i := range tm.snapshots {
		if tm.snapshots[i].SnapshotID == id {
			return &tm.snapshots[i]
		}
	}

	return nil
}

// SnapshotByName returns a snapshot by reference name.
func (tm *TableMetadata) SnapshotByName(name string) *Snapshot {
	// For now, just support "main" as the current snapshot
	if name == "main" {
		return tm.CurrentSnapshot()
	}

	return nil
}

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	// SnapshotID is the unique identifier for the snapshot.
	SnapshotID int64
	// ParentSnapshotID is the ID of the parent snapshot (nil for first snapshot).
	ParentSnapshotID *int64
	// SequenceNumber is the sequence number of the snapshot.
	SequenceNumber int64
	// TimestampMs is the creation timestamp in milliseconds since epoch.
	TimestampMs int64
	// ManifestListLocation is the path to the manifest list file.
	ManifestListLocation string
	// Summary contains snapshot summary information.
	Summary map[string]string
	// SchemaID is the ID of the schema at the time of the snapshot.
	SchemaID *int
}

// Timestamp returns the snapshot creation time.
func (s *Snapshot) Timestamp() time.Time {
	return time.UnixMilli(s.TimestampMs)
}

// DataFile represents a data file in an Iceberg table.
type DataFile struct {
	// Path is the file path.
	Path string
	// Format is the file format (parquet, avro, orc).
	Format FileFormat
	// PartitionData contains the partition values for this file.
	PartitionData map[string]any
	// RecordCount is the number of records in the file.
	RecordCount int64
	// FileSizeBytes is the size of the file in bytes.
	FileSizeBytes int64
	// ColumnSizes maps column IDs to their sizes in bytes.
	ColumnSizes map[int]int64
	// ValueCounts maps column IDs to value counts.
	ValueCounts map[int]int64
	// NullValueCounts maps column IDs to null value counts.
	NullValueCounts map[int]int64
	// NaNValueCounts maps column IDs to NaN value counts.
	NaNValueCounts map[int]int64
	// LowerBounds maps column IDs to lower bound values.
	LowerBounds map[int][]byte
	// UpperBounds maps column IDs to upper bound values.
	UpperBounds map[int][]byte
	// SplitOffsets contains split offsets for parallel reading.
	SplitOffsets []int64
	// SortOrderID is the ID of the sort order used to write the file.
	SortOrderID *int
	// ContentType indicates the type of file (0=data, 1=position deletes, 2=equality deletes).
	ContentType int
	// EqualityFieldIDs are the field IDs used for equality deletes (only for equality delete files).
	EqualityFieldIDs []int
}

// NewDataFile creates a DataFile from an iceberg-go DataFile.
func NewDataFile(df iceberg.DataFile) *DataFile {
	return &DataFile{
		Path:            df.FilePath(),
		Format:          FileFormat(df.FileFormat()),
		RecordCount:     df.Count(),
		FileSizeBytes:   df.FileSizeBytes(),
		ColumnSizes:     df.ColumnSizes(),
		ValueCounts:     df.ValueCounts(),
		NullValueCounts: df.NullValueCounts(),
		NaNValueCounts:  df.NaNValueCounts(),
		LowerBounds:     df.LowerBoundValues(),
		UpperBounds:     df.UpperBoundValues(),
		SplitOffsets:    df.SplitOffsets(),
		SortOrderID:     df.SortOrderID(),
	}
}

// ManifestFile represents an Iceberg manifest file entry from the manifest list.
type ManifestFile struct {
	// Path is the path to the manifest file.
	Path string
	// Length is the length of the manifest file in bytes.
	Length int64
	// PartitionSpecID is the ID of the partition spec used.
	PartitionSpecID int
	// Content indicates whether this is a data or delete manifest.
	Content ManifestContentType
	// SequenceNumber is the sequence number when the manifest was added.
	SequenceNumber int64
	// MinSequenceNumber is the minimum sequence number of files in the manifest.
	MinSequenceNumber int64
	// AddedSnapshotID is the snapshot ID when the manifest was added.
	AddedSnapshotID int64
	// AddedFilesCount is the number of files added.
	AddedFilesCount int
	// ExistingFilesCount is the number of existing files.
	ExistingFilesCount int
	// DeletedFilesCount is the number of deleted files.
	DeletedFilesCount int
	// AddedRowsCount is the number of rows added.
	AddedRowsCount int64
	// ExistingRowsCount is the number of existing rows.
	ExistingRowsCount int64
	// DeletedRowsCount is the number of deleted rows.
	DeletedRowsCount int64

	// raw holds the underlying iceberg-go manifest file.
	raw iceberg.ManifestFile
}

// NewManifestFile creates a ManifestFile from an iceberg-go ManifestFile.
func NewManifestFile(mf iceberg.ManifestFile) *ManifestFile {
	return &ManifestFile{
		Path:               mf.FilePath(),
		Length:             mf.Length(),
		PartitionSpecID:    int(mf.PartitionSpecID()),
		Content:            ManifestContentType(mf.ManifestContent()),
		SequenceNumber:     mf.SequenceNum(),
		MinSequenceNumber:  mf.MinSequenceNum(),
		AddedSnapshotID:    mf.SnapshotID(),
		AddedFilesCount:    int(mf.AddedDataFiles()),
		ExistingFilesCount: int(mf.ExistingDataFiles()),
		DeletedFilesCount:  int(mf.DeletedDataFiles()),
		AddedRowsCount:     mf.AddedRows(),
		ExistingRowsCount:  mf.ExistingRows(),
		DeletedRowsCount:   mf.DeletedRows(),
		raw:                mf,
	}
}

// Raw returns the underlying iceberg-go manifest file.
func (mf *ManifestFile) Raw() iceberg.ManifestFile {
	return mf.raw
}

// IsDataManifest returns true if this is a data manifest.
func (mf *ManifestFile) IsDataManifest() bool {
	return mf.Content == ManifestContentData
}

// IsDeleteManifest returns true if this is a delete manifest.
func (mf *ManifestFile) IsDeleteManifest() bool {
	return mf.Content == ManifestContentDeletes
}

// SnapshotLogEntry represents an entry in the snapshot log.
type SnapshotLogEntry struct {
	// SnapshotID is the snapshot ID.
	SnapshotID int64
	// TimestampMs is the timestamp in milliseconds.
	TimestampMs int64
}

// MetadataLogEntry represents an entry in the metadata log.
type MetadataLogEntry struct {
	// TimestampMs is the timestamp in milliseconds.
	TimestampMs int64
	// MetadataFile is the path to the metadata file.
	MetadataFile string
}
