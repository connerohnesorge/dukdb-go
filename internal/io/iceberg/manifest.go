// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements manifest list and manifest file parsing using AVRO.
package iceberg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/iceberg-go"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// ManifestReader provides methods for reading Iceberg manifest files.
type ManifestReader struct {
	// fs is the filesystem for reading files.
	fs filesystem.FileSystem
}

// NewManifestReader creates a new ManifestReader with the given filesystem.
// If fs is nil, the local filesystem is used.
func NewManifestReader(fs filesystem.FileSystem) *ManifestReader {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	return &ManifestReader{fs: fs}
}

// ManifestListEntry represents an entry in a manifest list file.
type ManifestListEntry struct {
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

// ManifestDataEntry represents a data file entry in a manifest file.
type ManifestDataEntry struct {
	Status        int32                  `avro:"status"`
	SnapshotID    *int64                 `avro:"snapshot_id"`
	SequenceNum   *int64                 `avro:"sequence_number"`
	FileSequence  *int64                 `avro:"file_sequence_number"`
	DataFile      ManifestDataFile       `avro:"data_file"`
}

// ManifestDataFile represents the data_file field in a manifest entry.
type ManifestDataFile struct {
	ContentType      int32              `avro:"content"`
	FilePath         string             `avro:"file_path"`
	FileFormat       string             `avro:"file_format"`
	RecordCount      int64              `avro:"record_count"`
	FileSizeBytes    int64              `avro:"file_size_in_bytes"`
	ColumnSizes      map[int32]int64    `avro:"column_sizes"`
	ValueCounts      map[int32]int64    `avro:"value_counts"`
	NullCounts       map[int32]int64    `avro:"null_value_counts"`
	NaNCounts        map[int32]int64    `avro:"nan_value_counts"`
	LowerBounds      map[int32][]byte   `avro:"lower_bounds"`
	UpperBounds      map[int32][]byte   `avro:"upper_bounds"`
	SplitOffsets     []int64            `avro:"split_offsets"`
	SortOrderID      *int32             `avro:"sort_order_id"`
	PartitionData    map[string]any     `avro:"partition"`
	EqualityFieldIDs []int32            `avro:"equality_ids"`
}

// ReadManifestList reads the manifest list for a snapshot and returns all manifest files.
func (r *ManifestReader) ReadManifestList(ctx context.Context, snapshot *Snapshot) ([]*ManifestFile, error) {
	if snapshot == nil {
		return nil, ErrNoCurrentSnapshot
	}

	// Open the manifest list file
	file, err := r.openFile(snapshot.ManifestListLocation)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrManifestListReadFailed, err)
	}
	defer func() { _ = file.Close() }()

	// Read the AVRO file
	reader, err := ocf.NewDecoder(file)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create AVRO decoder: %w", ErrManifestListReadFailed, err)
	}

	var manifests []*ManifestFile
	for reader.HasNext() {
		var entry ManifestListEntry
		if err := reader.Decode(&entry); err != nil {
			return nil, fmt.Errorf("%w: failed to decode manifest list entry: %w", ErrManifestListReadFailed, err)
		}

		manifests = append(manifests, &ManifestFile{
			Path:               entry.ManifestPath,
			Length:             entry.ManifestLength,
			PartitionSpecID:    int(entry.PartitionSpecID),
			Content:            ManifestContentType(entry.Content),
			SequenceNumber:     entry.SequenceNumber,
			MinSequenceNumber:  entry.MinSequenceNumber,
			AddedSnapshotID:    entry.AddedSnapshotID,
			AddedFilesCount:    int(entry.AddedFilesCount),
			ExistingFilesCount: int(entry.ExistingFilesCount),
			DeletedFilesCount:  int(entry.DeletedFilesCount),
			AddedRowsCount:     entry.AddedRowsCount,
			ExistingRowsCount:  entry.ExistingRowsCount,
			DeletedRowsCount:   entry.DeletedRowsCount,
		})
	}

	return manifests, nil
}

// openFile opens a file from either a local path or URL.
func (r *ManifestReader) openFile(path string) (io.ReadCloser, error) {
	// For now, only support local files
	// TODO: Add support for S3, GCS, Azure, etc.
	return r.fs.Open(path)
}

// ReadDataFiles reads all data file entries from a manifest file.
func (r *ManifestReader) ReadDataFiles(ctx context.Context, manifest *ManifestFile) ([]*DataFile, error) {
	if manifest == nil {
		return nil, fmt.Errorf("%w: manifest is nil", ErrManifestReadFailed)
	}

	entries, err := r.ReadManifestEntries(ctx, manifest)
	if err != nil {
		return nil, err
	}

	dataFiles := make([]*DataFile, 0, len(entries))
	for _, entry := range entries {
		// Only include data files that are ADDED or EXISTING (not DELETED)
		if entry.Status == EntryStatusDeleted {
			continue
		}

		dataFiles = append(dataFiles, entry.DataFile)
	}

	return dataFiles, nil
}

// ReadAllDataFiles reads all data files from all manifests in a snapshot.
// This is a convenience method that combines ReadManifestList and ReadDataFiles.
func (r *ManifestReader) ReadAllDataFiles(ctx context.Context, snapshot *Snapshot) ([]*DataFile, error) {
	manifests, err := r.ReadManifestList(ctx, snapshot)
	if err != nil {
		return nil, err
	}

	var allDataFiles []*DataFile

	for _, manifest := range manifests {
		// Skip delete manifests for now
		if manifest.IsDeleteManifest() {
			continue
		}

		dataFiles, err := r.ReadDataFiles(ctx, manifest)
		if err != nil {
			return nil, err
		}

		allDataFiles = append(allDataFiles, dataFiles...)
	}

	return allDataFiles, nil
}

// ReadDataManifests reads only data manifests (not delete manifests) from a snapshot.
func (r *ManifestReader) ReadDataManifests(ctx context.Context, snapshot *Snapshot) ([]*ManifestFile, error) {
	manifests, err := r.ReadManifestList(ctx, snapshot)
	if err != nil {
		return nil, err
	}

	dataManifests := make([]*ManifestFile, 0)
	for _, mf := range manifests {
		if mf.IsDataManifest() {
			dataManifests = append(dataManifests, mf)
		}
	}

	return dataManifests, nil
}

// ReadDeleteManifests reads only delete manifests from a snapshot.
func (r *ManifestReader) ReadDeleteManifests(ctx context.Context, snapshot *Snapshot) ([]*ManifestFile, error) {
	manifests, err := r.ReadManifestList(ctx, snapshot)
	if err != nil {
		return nil, err
	}

	deleteManifests := make([]*ManifestFile, 0)
	for _, mf := range manifests {
		if mf.IsDeleteManifest() {
			deleteManifests = append(deleteManifests, mf)
		}
	}

	return deleteManifests, nil
}

// ManifestEntry represents an entry in a manifest file with its status.
type ManifestEntry struct {
	// Status indicates whether the file is added, existing, or deleted.
	Status EntryStatus
	// SnapshotID is the snapshot ID when this entry was added.
	SnapshotID int64
	// SequenceNumber is the sequence number when this entry was added.
	SequenceNumber int64
	// DataFile is the data file information.
	DataFile *DataFile
}

// EntryStatus represents the status of a manifest entry.
type EntryStatus int

const (
	// EntryStatusExisting indicates the file existed in the previous snapshot.
	EntryStatusExisting EntryStatus = 0
	// EntryStatusAdded indicates the file was added in this snapshot.
	EntryStatusAdded EntryStatus = 1
	// EntryStatusDeleted indicates the file was deleted in this snapshot.
	EntryStatusDeleted EntryStatus = 2
)

// String returns a string representation of the entry status.
func (s EntryStatus) String() string {
	switch s {
	case EntryStatusExisting:
		return "existing"
	case EntryStatusAdded:
		return "added"
	case EntryStatusDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// ReadManifestEntries reads all entries from a manifest file, including status information.
func (r *ManifestReader) ReadManifestEntries(ctx context.Context, manifest *ManifestFile) ([]ManifestEntry, error) {
	if manifest == nil {
		return nil, fmt.Errorf("%w: manifest is nil", ErrManifestReadFailed)
	}

	file, err := r.openFile(manifest.Path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrManifestReadFailed, err)
	}
	defer func() { _ = file.Close() }()

	// Read the AVRO file
	reader, err := ocf.NewDecoder(file)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create AVRO decoder: %w", ErrManifestReadFailed, err)
	}

	var entries []ManifestEntry
	for reader.HasNext() {
		var rawEntry ManifestDataEntry
		if err := reader.Decode(&rawEntry); err != nil {
			return nil, fmt.Errorf("%w: failed to decode manifest entry: %w", ErrManifestReadFailed, err)
		}

		df := &DataFile{
			Path:          rawEntry.DataFile.FilePath,
			Format:        FileFormat(rawEntry.DataFile.FileFormat),
			RecordCount:   rawEntry.DataFile.RecordCount,
			FileSizeBytes: rawEntry.DataFile.FileSizeBytes,
			PartitionData: rawEntry.DataFile.PartitionData,
			ContentType:   int(rawEntry.DataFile.ContentType),
		}

		// Convert equality field IDs (for delete files)
		if rawEntry.DataFile.EqualityFieldIDs != nil {
			df.EqualityFieldIDs = make([]int, len(rawEntry.DataFile.EqualityFieldIDs))
			for i, id := range rawEntry.DataFile.EqualityFieldIDs {
				df.EqualityFieldIDs[i] = int(id)
			}
		}

		// Convert column stats maps
		if rawEntry.DataFile.ColumnSizes != nil {
			df.ColumnSizes = make(map[int]int64)
			for k, v := range rawEntry.DataFile.ColumnSizes {
				df.ColumnSizes[int(k)] = v
			}
		}
		if rawEntry.DataFile.ValueCounts != nil {
			df.ValueCounts = make(map[int]int64)
			for k, v := range rawEntry.DataFile.ValueCounts {
				df.ValueCounts[int(k)] = v
			}
		}
		if rawEntry.DataFile.NullCounts != nil {
			df.NullValueCounts = make(map[int]int64)
			for k, v := range rawEntry.DataFile.NullCounts {
				df.NullValueCounts[int(k)] = v
			}
		}
		if rawEntry.DataFile.NaNCounts != nil {
			df.NaNValueCounts = make(map[int]int64)
			for k, v := range rawEntry.DataFile.NaNCounts {
				df.NaNValueCounts[int(k)] = v
			}
		}
		if rawEntry.DataFile.LowerBounds != nil {
			df.LowerBounds = make(map[int][]byte)
			for k, v := range rawEntry.DataFile.LowerBounds {
				df.LowerBounds[int(k)] = v
			}
		}
		if rawEntry.DataFile.UpperBounds != nil {
			df.UpperBounds = make(map[int][]byte)
			for k, v := range rawEntry.DataFile.UpperBounds {
				df.UpperBounds[int(k)] = v
			}
		}
		df.SplitOffsets = rawEntry.DataFile.SplitOffsets
		if rawEntry.DataFile.SortOrderID != nil {
			sortOrderID := int(*rawEntry.DataFile.SortOrderID)
			df.SortOrderID = &sortOrderID
		}

		var snapshotID int64
		if rawEntry.SnapshotID != nil {
			snapshotID = *rawEntry.SnapshotID
		}

		var seqNum int64
		if rawEntry.SequenceNum != nil {
			seqNum = *rawEntry.SequenceNum
		}

		entries = append(entries, ManifestEntry{
			Status:         EntryStatus(rawEntry.Status),
			SnapshotID:     snapshotID,
			SequenceNumber: seqNum,
			DataFile:       df,
		})
	}

	return entries, nil
}

// FilterManifestsByPartition filters manifests that may contain data matching
// the given partition filter. This is used for partition pruning.
func (r *ManifestReader) FilterManifestsByPartition(
	manifests []*ManifestFile,
	filter PartitionFilter,
) []*ManifestFile {
	if filter == nil {
		return manifests
	}

	result := make([]*ManifestFile, 0, len(manifests))
	for _, mf := range manifests {
		if filter.MayMatch(mf) {
			result = append(result, mf)
		}
	}

	return result
}

// PartitionFilter is an interface for filtering manifests by partition values.
type PartitionFilter interface {
	// MayMatch returns true if the manifest may contain matching data.
	MayMatch(manifest *ManifestFile) bool
}

// CountDataFiles returns the total number of data files in a snapshot
// without reading all the file details.
func (r *ManifestReader) CountDataFiles(ctx context.Context, snapshot *Snapshot) (int64, error) {
	manifests, err := r.ReadManifestList(ctx, snapshot)
	if err != nil {
		return 0, err
	}

	var count int64
	for _, mf := range manifests {
		if mf.IsDataManifest() {
			// Use the counts from the manifest metadata instead of reading all entries
			count += int64(mf.AddedFilesCount) + int64(mf.ExistingFilesCount)
		}
	}

	return count, nil
}

// CountRows returns the total number of rows in a snapshot
// without reading all the file details.
func (r *ManifestReader) CountRows(ctx context.Context, snapshot *Snapshot) (int64, error) {
	manifests, err := r.ReadManifestList(ctx, snapshot)
	if err != nil {
		return 0, err
	}

	var count int64
	for _, mf := range manifests {
		if mf.IsDataManifest() {
			// Use the counts from the manifest metadata
			count += mf.AddedRowsCount + mf.ExistingRowsCount
		}
	}

	return count, nil
}

// ReadManifestListFromFile reads a manifest list from a local file path.
func ReadManifestListFromFile(path string) ([]*ManifestFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrManifestListReadFailed, err)
	}
	defer func() { _ = file.Close() }()

	// Create an AVRO schema for manifest list entries
	// This is a simplified version - real implementation would use the schema from the file
	reader, err := ocf.NewDecoder(file)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create AVRO decoder: %w", ErrManifestListReadFailed, err)
	}

	var manifests []*ManifestFile
	for reader.HasNext() {
		var entry ManifestListEntry
		if err := reader.Decode(&entry); err != nil {
			return nil, fmt.Errorf("%w: failed to decode manifest list entry: %w", ErrManifestListReadFailed, err)
		}

		manifests = append(manifests, &ManifestFile{
			Path:               entry.ManifestPath,
			Length:             entry.ManifestLength,
			PartitionSpecID:    int(entry.PartitionSpecID),
			Content:            ManifestContentType(entry.Content),
			SequenceNumber:     entry.SequenceNumber,
			MinSequenceNumber:  entry.MinSequenceNumber,
			AddedSnapshotID:    entry.AddedSnapshotID,
			AddedFilesCount:    int(entry.AddedFilesCount),
			ExistingFilesCount: int(entry.ExistingFilesCount),
			DeletedFilesCount:  int(entry.DeletedFilesCount),
			AddedRowsCount:     entry.AddedRowsCount,
			ExistingRowsCount:  entry.ExistingRowsCount,
			DeletedRowsCount:   entry.DeletedRowsCount,
		})
	}

	return manifests, nil
}

// ResolveManifestPath resolves a manifest path relative to a table location.
func ResolveManifestPath(tableLocation, manifestPath string) string {
	// If manifestPath is already absolute or a URL, use it as-is
	if filepath.IsAbs(manifestPath) ||
		len(manifestPath) > 0 && (manifestPath[0] == '/' ||
			(len(manifestPath) > 5 && manifestPath[:5] == "s3://") ||
			(len(manifestPath) > 5 && manifestPath[:5] == "gs://") ||
			(len(manifestPath) > 8 && manifestPath[:8] == "https://")) {
		return manifestPath
	}

	// Otherwise, resolve relative to table location
	return filepath.Join(tableLocation, manifestPath)
}

// Compile-time check to ensure avro is used
var _ = avro.DefaultConfig

// Compile-time check to ensure iceberg is used
var _ iceberg.Type
