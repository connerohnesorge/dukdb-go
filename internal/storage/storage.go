// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"strings"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
)

// normalizeTableKey converts a table name to lowercase for case-insensitive lookup.
func normalizeTableKey(name string) string {
	return strings.ToLower(name)
}

// Storage manages all table data in the database.
type Storage struct {
	mu      sync.RWMutex
	tables  map[string]*Table
	indexes map[string]*HashIndex // Key: "schema.indexname"
}

// NewStorage creates a new Storage instance.
func NewStorage() *Storage {
	return &Storage{
		tables:  make(map[string]*Table),
		indexes: make(map[string]*HashIndex),
	}
}

// GetTable returns a table by name (case-insensitive).
func (s *Storage) GetTable(
	name string,
) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[normalizeTableKey(name)]

	return t, ok
}

// CreateTable creates a new table in storage.
// Table names are case-insensitive; the original case is preserved.
func (s *Storage) CreateTable(
	name string,
	columnTypes []dukdb.Type,
) (*Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeTableKey(name)
	if _, exists := s.tables[key]; exists {
		return nil, dukdb.ErrTableAlreadyExists
	}

	t := NewTable(name, columnTypes)
	s.tables[key] = t

	return t, nil
}

// DropTable drops a table from storage.
// Table name is case-insensitive.
func (s *Storage) DropTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeTableKey(name)
	if _, exists := s.tables[key]; !exists {
		return dukdb.ErrTableNotFound
	}

	delete(s.tables, key)

	return nil
}

// RenameTable renames a table in storage.
// Table names are case-insensitive; the new name's case is preserved.
func (s *Storage) RenameTable(oldName, newName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldKey := normalizeTableKey(oldName)
	newKey := normalizeTableKey(newName)

	table, exists := s.tables[oldKey]
	if !exists {
		return dukdb.ErrTableNotFound
	}

	// Check if new name already exists (unless renaming to same case-insensitive name)
	if oldKey != newKey {
		if _, exists := s.tables[newKey]; exists {
			return dukdb.ErrTableAlreadyExists
		}
	}

	// Update table's internal name
	table.Rename(newName)

	// Move table to new key in map
	delete(s.tables, oldKey)
	s.tables[newKey] = table

	return nil
}

// Close closes the storage and releases all resources.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear all tables
	s.tables = make(map[string]*Table)

	return nil
}

// Tables returns a copy of the tables map for serialization
func (s *Storage) Tables() map[string]*Table {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make(map[string]*Table)
	for name, table := range s.tables {
		tables[name] = table
	}

	return tables
}

// normalizeIndexKey creates a case-insensitive key for index lookup.
func normalizeIndexKey(schema, name string) string {
	return strings.ToLower(schema) + "." + strings.ToLower(name)
}

// GetIndex returns an index by schema and name (case-insensitive).
func (s *Storage) GetIndex(schema, name string) *HashIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.indexes[normalizeIndexKey(schema, name)]
}

// CreateIndex creates a new index in storage.
// Returns an error if an index with the same name already exists.
func (s *Storage) CreateIndex(schema string, index *HashIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeIndexKey(schema, index.Name)
	if _, exists := s.indexes[key]; exists {
		return dukdb.ErrTableAlreadyExists // Reuse for now
	}

	s.indexes[key] = index
	return nil
}

// DropIndex removes an index from storage.
// Returns an error if the index does not exist.
func (s *Storage) DropIndex(schema, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeIndexKey(schema, name)
	if _, exists := s.indexes[key]; !exists {
		return dukdb.ErrTableNotFound // Reuse for now
	}

	delete(s.indexes, key)
	return nil
}

// GetIndexesForTable returns all indexes for a specific table.
// Table name is case-insensitive.
func (s *Storage) GetIndexesForTable(schema, tableName string) []*HashIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*HashIndex
	tableKey := strings.ToLower(tableName)
	schemaKey := strings.ToLower(schema)

	for key, idx := range s.indexes {
		// Check if index is in the right schema and for the right table
		if strings.HasPrefix(key, schemaKey+".") && strings.ToLower(idx.TableName) == tableKey {
			result = append(result, idx)
		}
	}

	return result
}

// ImportTable imports a table into storage.
// Table names are case-insensitive.
func (s *Storage) ImportTable(
	name string,
	table *Table,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tables[normalizeTableKey(name)] = table

	return nil
}

// Clone creates a deep copy of the storage for transaction rollback support.
// Note: This clones the table names but NOT the data. For DDL rollback,
// we only need to track which tables exist, not their data.
// DML rollback is handled separately by the transaction manager.
func (s *Storage) Clone() *Storage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	newStorage := &Storage{
		tables:  make(map[string]*Table),
		indexes: make(map[string]*HashIndex),
	}

	// Copy table references - DDL rollback needs to track which tables exist
	// For full DDL rollback (CREATE TABLE then ROLLBACK), we need the actual table names
	for key := range s.tables {
		// We store a nil marker - the actual data is not cloned
		// On restore, we check what tables should exist vs what do exist
		newStorage.tables[key] = nil
	}

	// Copy index references for DDL rollback
	for key := range s.indexes {
		newStorage.indexes[key] = nil
	}

	return newStorage
}

// RestoreFrom restores this storage from a snapshot.
// This is used for DDL transaction rollback.
// Tables that were created after the snapshot are dropped.
// Tables that were dropped after the snapshot are NOT restored (data is lost).
func (s *Storage) RestoreFrom(snapshot *Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot.mu.RLock()
	defer snapshot.mu.RUnlock()

	// Find tables that were created after the snapshot and drop them
	for key := range s.tables {
		if _, existed := snapshot.tables[key]; !existed {
			// This table was created after the snapshot, drop it
			delete(s.tables, key)
		}
	}

	// Find indexes that were created after the snapshot and drop them
	for key := range s.indexes {
		if _, existed := snapshot.indexes[key]; !existed {
			// This index was created after the snapshot, drop it
			delete(s.indexes, key)
		}
	}
	// Note: Tables/indexes that were dropped cannot be restored - their data is gone
	// This is acceptable for DDL rollback as the primary use case is
	// rolling back newly created objects
}
