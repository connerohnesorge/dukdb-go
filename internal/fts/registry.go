package fts

import "sync"

// Registry manages all FTS indexes, keyed by table name.
// It is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	indexes map[string]*InvertedIndex // key: tableName
}

// NewRegistry creates a new empty FTS registry.
func NewRegistry() *Registry {
	return &Registry{indexes: make(map[string]*InvertedIndex)}
}

// CreateIndex creates and registers a new FTS index for the given table and column.
// If an index already exists for the table, it is replaced.
func (r *Registry) CreateIndex(tableName, columnName string) *InvertedIndex {
	r.mu.Lock()
	defer r.mu.Unlock()
	idx := NewInvertedIndex(tableName, columnName)
	r.indexes[tableName] = idx
	return idx
}

// GetIndex returns the FTS index for the given table, if one exists.
func (r *Registry) GetIndex(tableName string) (*InvertedIndex, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.indexes[tableName]
	return idx, ok
}

// DropIndex removes the FTS index for the given table.
func (r *Registry) DropIndex(tableName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, existed := r.indexes[tableName]
	delete(r.indexes, tableName)
	return existed
}

// HasIndex returns true if an FTS index exists for the given table.
func (r *Registry) HasIndex(tableName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.indexes[tableName]
	return ok
}
