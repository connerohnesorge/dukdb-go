// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file provides additional manifest writing utilities and statistics tracking.
package iceberg

import (
	"encoding/binary"
	"math"
	"time"
)

// DataFileStats tracks statistics for a data file during writing.
type DataFileStats struct {
	// RecordCount is the total number of records.
	RecordCount int64

	// FileSizeBytes is the file size in bytes.
	FileSizeBytes int64

	// ColumnSizes maps column ID to size in bytes.
	ColumnSizes map[int]int64

	// ValueCounts maps column ID to value count.
	ValueCounts map[int]int64

	// NullCounts maps column ID to null count.
	NullCounts map[int]int64

	// NaNCounts maps column ID to NaN count (for float types).
	NaNCounts map[int]int64

	// LowerBounds maps column ID to lower bound value.
	LowerBounds map[int][]byte

	// UpperBounds maps column ID to upper bound value.
	UpperBounds map[int][]byte
}

// NewDataFileStats creates a new DataFileStats instance.
func NewDataFileStats() *DataFileStats {
	return &DataFileStats{
		ColumnSizes: make(map[int]int64),
		ValueCounts: make(map[int]int64),
		NullCounts:  make(map[int]int64),
		NaNCounts:   make(map[int]int64),
		LowerBounds: make(map[int][]byte),
		UpperBounds: make(map[int][]byte),
	}
}

// ManifestBuilder helps build manifest file entries.
type ManifestBuilder struct {
	// entries are the manifest entries being built.
	entries []ManifestBuilderEntry

	// snapshotID is the snapshot ID for new entries.
	snapshotID int64

	// sequenceNumber is the sequence number for new entries.
	sequenceNumber int64

	// partitionSpecID is the partition spec ID.
	partitionSpecID int
}

// ManifestBuilderEntry represents an entry in the manifest being built.
type ManifestBuilderEntry struct {
	// Status is the entry status (0=existing, 1=added, 2=deleted).
	Status int

	// SnapshotID is when the entry was added.
	SnapshotID int64

	// SequenceNumber is the sequence number.
	SequenceNumber int64

	// DataFile is the data file information.
	DataFile *WrittenDataFile
}

// NewManifestBuilder creates a new ManifestBuilder.
func NewManifestBuilder(snapshotID, sequenceNumber int64, partitionSpecID int) *ManifestBuilder {
	return &ManifestBuilder{
		entries:         make([]ManifestBuilderEntry, 0),
		snapshotID:      snapshotID,
		sequenceNumber:  sequenceNumber,
		partitionSpecID: partitionSpecID,
	}
}

// AddDataFile adds a data file to the manifest.
func (b *ManifestBuilder) AddDataFile(dataFile *WrittenDataFile) {
	entry := ManifestBuilderEntry{
		Status:         1, // ADDED
		SnapshotID:     b.snapshotID,
		SequenceNumber: b.sequenceNumber,
		DataFile:       dataFile,
	}
	b.entries = append(b.entries, entry)
}

// Entries returns all entries in the manifest.
func (b *ManifestBuilder) Entries() []ManifestBuilderEntry {
	return b.entries
}

// TotalRowCount returns the total row count across all entries.
func (b *ManifestBuilder) TotalRowCount() int64 {
	var total int64
	for _, e := range b.entries {
		if e.Status != 2 { // Not deleted
			total += e.DataFile.RecordCount
		}
	}
	return total
}

// TotalFileSize returns the total file size across all entries.
func (b *ManifestBuilder) TotalFileSize() int64 {
	var total int64
	for _, e := range b.entries {
		if e.Status != 2 { // Not deleted
			total += e.DataFile.FileSizeBytes
		}
	}
	return total
}

// AddedFilesCount returns the count of added files.
func (b *ManifestBuilder) AddedFilesCount() int {
	count := 0
	for _, e := range b.entries {
		if e.Status == 1 { // ADDED
			count++
		}
	}
	return count
}

// ExistingFilesCount returns the count of existing files.
func (b *ManifestBuilder) ExistingFilesCount() int {
	count := 0
	for _, e := range b.entries {
		if e.Status == 0 { // EXISTING
			count++
		}
	}
	return count
}

// DeletedFilesCount returns the count of deleted files.
func (b *ManifestBuilder) DeletedFilesCount() int {
	count := 0
	for _, e := range b.entries {
		if e.Status == 2 { // DELETED
			count++
		}
	}
	return count
}

// SnapshotSummaryBuilder helps build snapshot summary metadata.
type SnapshotSummaryBuilder struct {
	summary map[string]string
}

// NewSnapshotSummaryBuilder creates a new SnapshotSummaryBuilder.
func NewSnapshotSummaryBuilder() *SnapshotSummaryBuilder {
	return &SnapshotSummaryBuilder{
		summary: make(map[string]string),
	}
}

// SetOperation sets the operation type.
func (b *SnapshotSummaryBuilder) SetOperation(op string) {
	b.summary["operation"] = op
}

// SetAddedDataFiles sets the number of added data files.
func (b *SnapshotSummaryBuilder) SetAddedDataFiles(count int) {
	b.summary["added-data-files"] = intToString(count)
}

// SetAddedRecords sets the number of added records.
func (b *SnapshotSummaryBuilder) SetAddedRecords(count int64) {
	b.summary["added-records"] = int64ToString(count)
}

// SetTotalRecords sets the total number of records.
func (b *SnapshotSummaryBuilder) SetTotalRecords(count int64) {
	b.summary["total-records"] = int64ToString(count)
}

// SetTotalDataFiles sets the total number of data files.
func (b *SnapshotSummaryBuilder) SetTotalDataFiles(count int) {
	b.summary["total-data-files"] = intToString(count)
}

// SetAddedFilesSize sets the total size of added files.
func (b *SnapshotSummaryBuilder) SetAddedFilesSize(size int64) {
	b.summary["added-files-size"] = int64ToString(size)
}

// SetTotalFilesSize sets the total size of all files.
func (b *SnapshotSummaryBuilder) SetTotalFilesSize(size int64) {
	b.summary["total-files-size"] = int64ToString(size)
}

// Build returns the summary map.
func (b *SnapshotSummaryBuilder) Build() map[string]string {
	// Ensure required fields have defaults
	if _, ok := b.summary["operation"]; !ok {
		b.summary["operation"] = "append"
	}
	if _, ok := b.summary["total-delete-files"]; !ok {
		b.summary["total-delete-files"] = "0"
	}
	if _, ok := b.summary["total-position-deletes"]; !ok {
		b.summary["total-position-deletes"] = "0"
	}
	if _, ok := b.summary["total-equality-deletes"]; !ok {
		b.summary["total-equality-deletes"] = "0"
	}
	return b.summary
}

// Helper functions for converting values to strings.
func intToString(v int) string {
	return int64ToString(int64(v))
}

func int64ToString(v int64) string {
	// Simple int64 to string conversion
	if v == 0 {
		return "0"
	}

	neg := v < 0
	if neg {
		v = -v
	}

	// Build string in reverse
	buf := make([]byte, 0, 20)
	for v > 0 {
		buf = append(buf, byte('0'+v%10))
		v /= 10
	}

	if neg {
		buf = append(buf, '-')
	}

	// Reverse
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}

	return string(buf)
}

// Value serialization helpers for column bounds.

// SerializeInt32 serializes an int32 value to bytes for column bounds.
func SerializeInt32(v int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	return buf
}

// SerializeInt64 serializes an int64 value to bytes for column bounds.
func SerializeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

// SerializeFloat32 serializes a float32 value to bytes for column bounds.
func SerializeFloat32(v float32) []byte {
	buf := make([]byte, 4)
	bits := math.Float32bits(v)
	binary.LittleEndian.PutUint32(buf, bits)
	return buf
}

// SerializeFloat64 serializes a float64 value to bytes for column bounds.
func SerializeFloat64(v float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(v)
	binary.LittleEndian.PutUint64(buf, bits)
	return buf
}

// SerializeString serializes a string value to bytes for column bounds.
// Strings are truncated to the first 16 bytes for lower bounds
// and the last 16 bytes for upper bounds to keep metadata compact.
func SerializeString(v string) []byte {
	if len(v) <= 16 {
		return []byte(v)
	}
	return []byte(v[:16])
}

// SerializeDate serializes a date value (days since epoch) to bytes.
func SerializeDate(v int32) []byte {
	return SerializeInt32(v)
}

// SerializeTimestamp serializes a timestamp value (microseconds since epoch) to bytes.
func SerializeTimestamp(v int64) []byte {
	return SerializeInt64(v)
}

// SerializeTime serializes a time value (microseconds since midnight) to bytes.
func SerializeTime(v int64) []byte {
	return SerializeInt64(v)
}

// UnixMillisToIceberg converts Unix milliseconds to Iceberg timestamp format.
func UnixMillisToIceberg(ms int64) int64 {
	return ms * 1000 // Convert to microseconds
}

// TimeToIcebergTimestamp converts a time.Time to Iceberg timestamp format.
func TimeToIcebergTimestamp(t time.Time) int64 {
	return t.UnixMicro()
}

// TimeToIcebergDate converts a time.Time to Iceberg date format.
func TimeToIcebergDate(t time.Time) int32 {
	// Days since Unix epoch (1970-01-01)
	return int32(t.Unix() / 86400)
}

// TimeToIcebergTime converts a time.Time to Iceberg time format.
func TimeToIcebergTime(t time.Time) int64 {
	// Microseconds since midnight
	hour, min, sec := t.Clock()
	nano := t.Nanosecond()
	micros := int64(hour)*3600000000 + int64(min)*60000000 + int64(sec)*1000000 + int64(nano)/1000
	return micros
}
