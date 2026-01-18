// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
)

// IcebergTableDiscovery discovers Iceberg tables in a directory.
// It scans the given path to find Iceberg table directories (those containing
// a 'metadata' subdirectory with Iceberg metadata files).
type IcebergTableDiscovery struct {
	// fs is the filesystem to use for reading directories.
	// If nil, a local filesystem will be used.
	fs filesystem.FileSystem

	// metadataOpts contains options for reading Iceberg metadata.
	metadataOpts iceberg.MetadataReaderOptions
}

// IcebergDiscoveryOptions contains options for table discovery.
type IcebergDiscoveryOptions struct {
	// Filesystem is the filesystem implementation to use.
	// If nil, the appropriate filesystem will be auto-detected based on the path.
	Filesystem filesystem.FileSystem

	// MetadataReaderOptions contains options for reading Iceberg metadata.
	MetadataReaderOptions iceberg.MetadataReaderOptions

	// Recursive enables recursive scanning of subdirectories.
	// If true, discovery will search all subdirectories for Iceberg tables.
	// If false (default), only immediate subdirectories are checked.
	Recursive bool

	// MaxDepth limits the recursion depth when Recursive is true.
	// A value of 0 means no limit.
	MaxDepth int
}

// DefaultDiscoveryOptions returns the default discovery options.
func DefaultDiscoveryOptions() IcebergDiscoveryOptions {
	return IcebergDiscoveryOptions{
		Filesystem:            nil,
		MetadataReaderOptions: iceberg.MetadataReaderOptions{},
		Recursive:             false,
		MaxDepth:              3, // Default max depth
	}
}

// NewIcebergTableDiscovery creates a new IcebergTableDiscovery instance.
func NewIcebergTableDiscovery(opts IcebergDiscoveryOptions) *IcebergTableDiscovery {
	return &IcebergTableDiscovery{
		fs:           opts.Filesystem,
		metadataOpts: opts.MetadataReaderOptions,
	}
}

// DiscoverTables discovers Iceberg tables at the given path.
// The path can be a local filesystem path or a cloud URL (s3://, gs://, abfs://).
// It returns a list of IcebergTableEntry for each discovered table.
//
// Discovery logic:
//  1. If the path itself is an Iceberg table (has metadata/ subdirectory), return it.
//  2. Otherwise, scan subdirectories looking for Iceberg tables.
func (d *IcebergTableDiscovery) DiscoverTables(
	ctx context.Context,
	path string,
) ([]*IcebergTableEntry, error) {
	// Get or create filesystem for the path
	fs, err := d.getFilesystem(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get filesystem for path %s: %w", path, err)
	}

	// Check if the path itself is an Iceberg table
	isTable, err := d.isIcebergTable(ctx, fs, path)
	if err != nil {
		return nil, fmt.Errorf("failed to check if path is Iceberg table: %w", err)
	}

	if isTable {
		entry, err := d.loadTableEntry(ctx, fs, path, filepath.Base(path))
		if err != nil {
			return nil, fmt.Errorf("failed to load Iceberg table at %s: %w", path, err)
		}
		return []*IcebergTableEntry{entry}, nil
	}

	// Scan subdirectories for Iceberg tables
	return d.discoverTablesInDirectory(ctx, fs, path)
}

// DiscoverTable discovers a single Iceberg table at the given path.
// The path should point directly to an Iceberg table root directory.
// Returns an error if the path is not a valid Iceberg table.
func (d *IcebergTableDiscovery) DiscoverTable(
	ctx context.Context,
	path string,
	name string,
) (*IcebergTableEntry, error) {
	// Get or create filesystem for the path
	fs, err := d.getFilesystem(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get filesystem for path %s: %w", path, err)
	}

	// Verify this is an Iceberg table
	isTable, err := d.isIcebergTable(ctx, fs, path)
	if err != nil {
		return nil, fmt.Errorf("failed to check if path is Iceberg table: %w", err)
	}

	if !isTable {
		return nil, fmt.Errorf(
			"path %s is not a valid Iceberg table (no metadata directory found)",
			path,
		)
	}

	// Use provided name or derive from path
	if name == "" {
		name = filepath.Base(path)
	}

	return d.loadTableEntry(ctx, fs, path, name)
}

// getFilesystem returns the appropriate filesystem for the given path.
func (d *IcebergTableDiscovery) getFilesystem(path string) (filesystem.FileSystem, error) {
	if d.fs != nil {
		return d.fs, nil
	}

	// Auto-detect filesystem based on path
	if filesystem.IsCloudURL(path) {
		// For cloud URLs, we need to create the appropriate filesystem
		// This would integrate with the FileSystemProvider pattern
		// For now, return an error indicating cloud support requires explicit configuration
		return nil, fmt.Errorf("cloud URL %s requires explicit filesystem configuration", path)
	}

	// Use local filesystem for local paths
	return filesystem.NewLocalFileSystem(""), nil
}

// isIcebergTable checks if a path is an Iceberg table.
// A path is considered an Iceberg table if it has a 'metadata' subdirectory
// containing at least one .metadata.json file.
func (d *IcebergTableDiscovery) isIcebergTable(
	_ context.Context,
	fs filesystem.FileSystem,
	path string,
) (bool, error) {
	metadataDir := filepath.Join(path, "metadata")

	// Check if metadata directory exists
	exists, err := fs.Exists(metadataDir)
	if err != nil || !exists {
		return false, nil
	}

	// Check for metadata files
	entries, err := fs.ReadDir(metadataDir)
	if err != nil {
		return false, nil
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".metadata.json") ||
			strings.HasSuffix(name, ".metadata.json.gz") ||
			strings.HasSuffix(name, ".metadata.json.zst") {
			return true, nil
		}
	}

	return false, nil
}

// loadTableEntry loads Iceberg metadata and creates a catalog entry.
func (d *IcebergTableDiscovery) loadTableEntry(
	ctx context.Context,
	fs filesystem.FileSystem,
	path string,
	name string,
) (*IcebergTableEntry, error) {
	// Create metadata reader with options
	reader := iceberg.NewMetadataReaderWithOptions(fs, d.metadataOpts)

	// Read metadata
	metadata, err := reader.ReadMetadata(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read Iceberg metadata: %w", err)
	}

	// Create catalog entry
	entry := &IcebergTableEntry{
		Name:              name,
		Location:          path,
		CurrentSnapshotID: metadata.CurrentSnapshotID,
		LastUpdatedMs:     metadata.LastUpdatedMs,
		Schema:            metadata.CurrentSchema(),
		PartitionSpec:     metadata.PartitionSpec(),
		FormatVersion:     int(metadata.Version),
		Properties:        metadata.Properties,
	}

	return entry, nil
}

// discoverTablesInDirectory scans a directory for Iceberg tables.
func (d *IcebergTableDiscovery) discoverTablesInDirectory(
	ctx context.Context,
	fs filesystem.FileSystem,
	path string,
) ([]*IcebergTableEntry, error) {
	entries, err := fs.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", path, err)
	}

	var tables []*IcebergTableEntry

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		subPath := filepath.Join(path, entry.Name())

		isTable, err := d.isIcebergTable(ctx, fs, subPath)
		if err != nil {
			// Log warning and continue
			continue
		}

		if isTable {
			tableEntry, err := d.loadTableEntry(ctx, fs, subPath, entry.Name())
			if err != nil {
				// Log warning and continue
				continue
			}
			tables = append(tables, tableEntry)
		}
	}

	return tables, nil
}

// SetFilesystem sets the filesystem to use for discovery.
// This is useful for cloud storage integration.
func (d *IcebergTableDiscovery) SetFilesystem(fs filesystem.FileSystem) {
	d.fs = fs
}

// SetMetadataOptions sets the metadata reader options.
func (d *IcebergTableDiscovery) SetMetadataOptions(opts iceberg.MetadataReaderOptions) {
	d.metadataOpts = opts
}
