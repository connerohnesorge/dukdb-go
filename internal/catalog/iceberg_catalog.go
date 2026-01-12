// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	"sync"

	"github.com/apache/iceberg-go"
)

// IcebergTableEntry represents an Iceberg table entry in the catalog.
// It stores metadata about an Iceberg table discovered from a filesystem location.
type IcebergTableEntry struct {
	// Name is the logical name of the Iceberg table.
	Name string

	// Location is the filesystem path or URL to the Iceberg table root directory.
	// This can be a local path or a cloud URL (s3://, gs://, abfs://).
	Location string

	// CurrentSnapshotID is the ID of the current snapshot.
	// nil if the table has no snapshots (empty table).
	CurrentSnapshotID *int64

	// LastUpdatedMs is the timestamp of the last update in milliseconds since epoch.
	LastUpdatedMs int64

	// Schema is the current Iceberg schema for the table.
	Schema *iceberg.Schema

	// PartitionSpec is the current partition specification.
	PartitionSpec iceberg.PartitionSpec

	// FormatVersion is the Iceberg format version (1 or 2).
	FormatVersion int

	// Properties contains table-level properties from the Iceberg metadata.
	Properties map[string]string
}

// NewIcebergTableEntry creates a new IcebergTableEntry with the given parameters.
func NewIcebergTableEntry(
	name string,
	location string,
	currentSnapshotID *int64,
	lastUpdatedMs int64,
	schema *iceberg.Schema,
	partitionSpec iceberg.PartitionSpec,
) *IcebergTableEntry {
	return &IcebergTableEntry{
		Name:              name,
		Location:          location,
		CurrentSnapshotID: currentSnapshotID,
		LastUpdatedMs:     lastUpdatedMs,
		Schema:            schema,
		PartitionSpec:     partitionSpec,
		FormatVersion:     2, // Default to v2
		Properties:        make(map[string]string),
	}
}

// Clone creates a deep copy of the IcebergTableEntry.
func (e *IcebergTableEntry) Clone() *IcebergTableEntry {
	clone := &IcebergTableEntry{
		Name:          e.Name,
		Location:      e.Location,
		LastUpdatedMs: e.LastUpdatedMs,
		FormatVersion: e.FormatVersion,
		Schema:        e.Schema,
		PartitionSpec: e.PartitionSpec,
	}

	if e.CurrentSnapshotID != nil {
		snapshotID := *e.CurrentSnapshotID
		clone.CurrentSnapshotID = &snapshotID
	}

	if e.Properties != nil {
		clone.Properties = make(map[string]string)
		for k, v := range e.Properties {
			clone.Properties[k] = v
		}
	}

	return clone
}

// HasSnapshot returns true if the table has at least one snapshot.
func (e *IcebergTableEntry) HasSnapshot() bool {
	return e.CurrentSnapshotID != nil
}

// GetProperty returns a table property value by key.
// Returns empty string and false if the property doesn't exist.
func (e *IcebergTableEntry) GetProperty(key string) (string, bool) {
	if e.Properties == nil {
		return "", false
	}
	val, ok := e.Properties[key]
	return val, ok
}

// SetProperty sets a table property.
func (e *IcebergTableEntry) SetProperty(key, value string) {
	if e.Properties == nil {
		e.Properties = make(map[string]string)
	}
	e.Properties[key] = value
}

// IcebergCatalog manages Iceberg table entries in the catalog.
// It provides methods to register, lookup, and list Iceberg tables.
type IcebergCatalog struct {
	mu     sync.RWMutex
	tables map[string]*IcebergTableEntry // name -> entry (case-insensitive)
}

// NewIcebergCatalog creates a new IcebergCatalog instance.
func NewIcebergCatalog() *IcebergCatalog {
	return &IcebergCatalog{
		tables: make(map[string]*IcebergTableEntry),
	}
}

// RegisterTable registers an Iceberg table entry in the catalog.
// If a table with the same name already exists, it will be replaced.
// Table names are case-insensitive.
func (c *IcebergCatalog) RegisterTable(entry *IcebergTableEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeKey(entry.Name)
	c.tables[key] = entry
}

// GetTable returns an Iceberg table entry by name (case-insensitive).
// Returns nil and false if the table is not found.
func (c *IcebergCatalog) GetTable(name string) (*IcebergTableEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.tables[normalizeKey(name)]
	return entry, ok
}

// UnregisterTable removes an Iceberg table entry from the catalog.
// Table names are case-insensitive.
// Returns true if the table was found and removed, false otherwise.
func (c *IcebergCatalog) UnregisterTable(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := c.tables[key]; !exists {
		return false
	}

	delete(c.tables, key)
	return true
}

// ListTables returns a list of all registered Iceberg table entries.
func (c *IcebergCatalog) ListTables() []*IcebergTableEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries := make([]*IcebergTableEntry, 0, len(c.tables))
	for _, entry := range c.tables {
		entries = append(entries, entry)
	}
	return entries
}

// TableCount returns the number of registered Iceberg tables.
func (c *IcebergCatalog) TableCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tables)
}

// Clear removes all Iceberg table entries from the catalog.
func (c *IcebergCatalog) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = make(map[string]*IcebergTableEntry)
}

// Clone creates a deep copy of the IcebergCatalog.
func (c *IcebergCatalog) Clone() *IcebergCatalog {
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := &IcebergCatalog{
		tables: make(map[string]*IcebergTableEntry),
	}

	for key, entry := range c.tables {
		clone.tables[key] = entry.Clone()
	}

	return clone
}
