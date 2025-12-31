// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
)

// Storage manages all table data in the database.
type Storage struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// NewStorage creates a new Storage instance.
func NewStorage() *Storage {
	return &Storage{
		tables: make(map[string]*Table),
	}
}

// GetTable returns a table by name.
func (s *Storage) GetTable(
	name string,
) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[name]

	return t, ok
}

// CreateTable creates a new table in storage.
func (s *Storage) CreateTable(
	name string,
	columnTypes []dukdb.Type,
) (*Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; exists {
		return nil, dukdb.ErrTableAlreadyExists
	}

	t := NewTable(name, columnTypes)
	s.tables[name] = t

	return t, nil
}

// DropTable drops a table from storage.
func (s *Storage) DropTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; !exists {
		return dukdb.ErrTableNotFound
	}

	delete(s.tables, name)

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

// ImportTable imports a table into storage
func (s *Storage) ImportTable(name string, table *Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tables[name] = table

	return nil
}
