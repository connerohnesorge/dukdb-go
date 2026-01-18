// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements the main Table type that provides access to Iceberg tables.
package iceberg

import (
	"context"
	"time"

	"github.com/apache/iceberg-go"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// TableOptions contains options for opening an Iceberg table.
type TableOptions struct {
	// SnapshotID specifies a specific snapshot to use (nil for current).
	SnapshotID *int64
	// AsOfTimestamp specifies a timestamp to use for time travel (nil for current).
	AsOfTimestamp *time.Time
	// AllowMovedPaths allows reading tables that have been moved.
	// When true, file paths in metadata are rewritten relative to the
	// current table location instead of using absolute paths.
	AllowMovedPaths bool
	// MetadataCompressionCodec specifies the compression codec for metadata files.
	// Supported values: "gzip", "zstd", "none" (or empty for auto-detection).
	MetadataCompressionCodec string
	// Filesystem specifies the filesystem to use (nil for auto-detection).
	Filesystem filesystem.FileSystem
	// Version specifies an explicit metadata version number to use.
	// If set to a positive value, the reader will look for v{Version}.metadata.json.
	// If 0 (default), the reader uses version-hint.text or scans for latest.
	Version int
	// UnsafeEnableVersionGuessing enables automatic version guessing when
	// version-hint.text is missing. When enabled, the reader scans the
	// metadata directory to find the highest version number.
	// This is marked as "unsafe" because it may select an incomplete or
	// corrupt metadata version if a write was interrupted.
	UnsafeEnableVersionGuessing bool
}

// DefaultTableOptions returns the default table options.
func DefaultTableOptions() *TableOptions {
	return &TableOptions{
		AllowMovedPaths: false,
	}
}

// Table represents an Iceberg table and provides methods for reading metadata and data.
type Table struct {
	// location is the base location of the table.
	location string
	// metadata is the parsed table metadata.
	metadata *TableMetadata
	// metadataLocation is the path to the metadata.json file.
	metadataLocation string
	// snapshot is the selected snapshot (nil if using current).
	snapshot *Snapshot
	// fs is the filesystem used for reading files.
	fs filesystem.FileSystem
	// schemaMapper is used for schema mapping.
	schemaMapper *SchemaMapper
	// snapshotSelector is used for snapshot operations.
	snapshotSelector *SnapshotSelector
	// manifestReader is used for reading manifest files.
	manifestReader *ManifestReader
}

// OpenTable opens an Iceberg table at the given location.
func OpenTable(ctx context.Context, location string, opts *TableOptions) (*Table, error) {
	if opts == nil {
		opts = DefaultTableOptions()
	}

	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Create metadata reader with options
	metadataOpts := MetadataReaderOptions{
		Version:                     opts.Version,
		AllowMovedPaths:             opts.AllowMovedPaths,
		MetadataCompressionCodec:    opts.MetadataCompressionCodec,
		UnsafeEnableVersionGuessing: opts.UnsafeEnableVersionGuessing,
	}
	metadataReader := NewMetadataReaderWithOptions(fs, metadataOpts)
	metadata, err := metadataReader.ReadMetadata(ctx, location)
	if err != nil {
		return nil, err
	}

	tbl := &Table{
		location:         location,
		metadata:         metadata,
		metadataLocation: location + "/metadata",
		fs:               fs,
		schemaMapper:     NewSchemaMapper(),
		snapshotSelector: NewSnapshotSelector(metadata),
		manifestReader:   NewManifestReader(fs),
	}

	// Select snapshot based on options
	if opts.SnapshotID != nil {
		snap, err := tbl.snapshotSelector.SnapshotByID(*opts.SnapshotID)
		if err != nil {
			return nil, err
		}
		tbl.snapshot = snap
	} else if opts.AsOfTimestamp != nil {
		snap, err := tbl.snapshotSelector.SnapshotAsOfTimestamp(*opts.AsOfTimestamp, true)
		if err != nil {
			return nil, err
		}
		tbl.snapshot = snap
	}

	return tbl, nil
}

// OpenTableFromMetadata opens an Iceberg table from a specific metadata file.
func OpenTableFromMetadata(
	ctx context.Context,
	metadataPath string,
	opts *TableOptions,
) (*Table, error) {
	if opts == nil {
		opts = DefaultTableOptions()
	}

	fs := opts.Filesystem
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	// Create metadata reader with options
	metadataOpts := MetadataReaderOptions{
		Version:                     opts.Version,
		AllowMovedPaths:             opts.AllowMovedPaths,
		MetadataCompressionCodec:    opts.MetadataCompressionCodec,
		UnsafeEnableVersionGuessing: opts.UnsafeEnableVersionGuessing,
	}
	metadataReader := NewMetadataReaderWithOptions(fs, metadataOpts)
	metadata, err := metadataReader.ReadMetadataFromPath(ctx, metadataPath)
	if err != nil {
		return nil, err
	}

	location := metadata.Location

	tbl := &Table{
		location:         location,
		metadata:         metadata,
		metadataLocation: metadataPath,
		fs:               fs,
		schemaMapper:     NewSchemaMapper(),
		snapshotSelector: NewSnapshotSelector(metadata),
		manifestReader:   NewManifestReader(fs),
	}

	// Select snapshot based on options
	if opts.SnapshotID != nil {
		snap, err := tbl.snapshotSelector.SnapshotByID(*opts.SnapshotID)
		if err != nil {
			return nil, err
		}
		tbl.snapshot = snap
	} else if opts.AsOfTimestamp != nil {
		snap, err := tbl.snapshotSelector.SnapshotAsOfTimestamp(*opts.AsOfTimestamp, true)
		if err != nil {
			return nil, err
		}
		tbl.snapshot = snap
	}

	return tbl, nil
}

// Location returns the base location of the table.
func (t *Table) Location() string {
	return t.location
}

// Metadata returns the table metadata.
func (t *Table) Metadata() *TableMetadata {
	return t.metadata
}

// Schema returns the current schema of the table.
func (t *Table) Schema() *iceberg.Schema {
	return t.metadata.CurrentSchema()
}

// PartitionSpec returns the current partition spec.
func (t *Table) PartitionSpec() (*PartitionSpec, error) {
	return NewPartitionSpec(t.metadata.PartitionSpec())
}

// CurrentSnapshot returns the current snapshot of the table.
func (t *Table) CurrentSnapshot() *Snapshot {
	if t.snapshot != nil {
		return t.snapshot
	}

	return t.snapshotSelector.CurrentSnapshot()
}

// SnapshotByID returns the snapshot with the given ID.
func (t *Table) SnapshotByID(snapshotID int64) (*Snapshot, error) {
	return t.snapshotSelector.SnapshotByID(snapshotID)
}

// SnapshotAsOfTimestamp returns the snapshot that was current at the given timestamp.
func (t *Table) SnapshotAsOfTimestamp(timestamp time.Time) (*Snapshot, error) {
	return t.snapshotSelector.SnapshotAsOfTimestamp(timestamp, true)
}

// Snapshots returns all snapshots in the table.
func (t *Table) Snapshots() []*Snapshot {
	return t.snapshotSelector.Snapshots()
}

// SnapshotHistory returns the snapshot history.
func (t *Table) SnapshotHistory() []SnapshotLogEntry {
	return t.snapshotSelector.SnapshotHistory()
}

// DataFiles returns all data files for the selected snapshot.
func (t *Table) DataFiles(ctx context.Context) ([]*DataFile, error) {
	snapshot := t.CurrentSnapshot()
	if snapshot == nil {
		return nil, ErrNoCurrentSnapshot
	}

	return t.manifestReader.ReadAllDataFiles(ctx, snapshot)
}

// DataFilesForSnapshot returns all data files for a specific snapshot.
func (t *Table) DataFilesForSnapshot(ctx context.Context, snapshot *Snapshot) ([]*DataFile, error) {
	if snapshot == nil {
		return nil, ErrNoCurrentSnapshot
	}

	return t.manifestReader.ReadAllDataFiles(ctx, snapshot)
}

// Manifests returns all manifest files for the selected snapshot.
func (t *Table) Manifests(ctx context.Context) ([]*ManifestFile, error) {
	snapshot := t.CurrentSnapshot()
	if snapshot == nil {
		return nil, ErrNoCurrentSnapshot
	}

	return t.manifestReader.ReadManifestList(ctx, snapshot)
}

// SchemaColumns returns the schema column information.
func (t *Table) SchemaColumns() ([]ColumnInfo, error) {
	return t.schemaMapper.MapSchemaToColumnInfo(t.Schema())
}

// RowCount returns the approximate row count for the selected snapshot.
func (t *Table) RowCount(ctx context.Context) (int64, error) {
	snapshot := t.CurrentSnapshot()
	if snapshot == nil {
		return 0, ErrNoCurrentSnapshot
	}

	return t.manifestReader.CountRows(ctx, snapshot)
}

// FileCount returns the number of data files in the selected snapshot.
func (t *Table) FileCount(ctx context.Context) (int64, error) {
	snapshot := t.CurrentSnapshot()
	if snapshot == nil {
		return 0, ErrNoCurrentSnapshot
	}

	return t.manifestReader.CountDataFiles(ctx, snapshot)
}

// Close releases any resources held by the table.
func (t *Table) Close() error {
	// Currently no resources to release
	return nil
}

// Scan creates a table scan for reading data.
func (t *Table) Scan(opts ...ScanOption) *TableScan {
	scan := &TableScan{
		table:          t,
		selectedFields: []string{"*"},
	}

	for _, opt := range opts {
		opt(scan)
	}

	return scan
}

// TableScan represents a scan operation on an Iceberg table.
type TableScan struct {
	table          *Table
	selectedFields []string
	snapshot       *Snapshot
	limit          int64
}

// ScanOption is a function that configures a TableScan.
type ScanOption func(*TableScan)

// WithSelectedColumns specifies which columns to read.
func WithSelectedColumns(columns ...string) ScanOption {
	return func(scan *TableScan) {
		scan.selectedFields = columns
	}
}

// WithSnapshot specifies which snapshot to read.
func WithSnapshot(snapshot *Snapshot) ScanOption {
	return func(scan *TableScan) {
		scan.snapshot = snapshot
	}
}

// WithSnapshotID specifies which snapshot to read by ID.
func WithSnapshotID(snapshotID int64) ScanOption {
	return func(scan *TableScan) {
		snap, err := scan.table.SnapshotByID(snapshotID)
		if err == nil {
			scan.snapshot = snap
		}
	}
}

// WithTimestamp specifies time travel to a specific timestamp.
func WithTimestamp(timestamp time.Time) ScanOption {
	return func(scan *TableScan) {
		snap, err := scan.table.SnapshotAsOfTimestamp(timestamp)
		if err == nil {
			scan.snapshot = snap
		}
	}
}

// WithLimit specifies the maximum number of rows to read.
func WithLimit(limit int64) ScanOption {
	return func(scan *TableScan) {
		scan.limit = limit
	}
}

// SelectedFields returns the columns to read.
func (s *TableScan) SelectedFields() []string {
	return s.selectedFields
}

// Snapshot returns the snapshot to read.
func (s *TableScan) Snapshot() *Snapshot {
	if s.snapshot != nil {
		return s.snapshot
	}

	return s.table.CurrentSnapshot()
}

// DataFiles returns the data files to read based on scan options.
func (s *TableScan) DataFiles(ctx context.Context) ([]*DataFile, error) {
	snapshot := s.Snapshot()
	if snapshot == nil {
		return nil, ErrNoCurrentSnapshot
	}

	return s.table.DataFilesForSnapshot(ctx, snapshot)
}
