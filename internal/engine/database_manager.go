package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// AttachedDatabase represents a single attached database.
type AttachedDatabase struct {
	Name     string
	Path     string
	Catalog  *catalog.Catalog
	Storage  *storage.Storage
	ReadOnly bool
}

// DatabaseManager maintains a registry of attached databases.
type DatabaseManager struct {
	mu        sync.RWMutex
	databases map[string]*AttachedDatabase
	defaultDB string
}

// NewDatabaseManager creates a new DatabaseManager with "memory" as the default.
func NewDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		databases: make(map[string]*AttachedDatabase),
		defaultDB: "memory",
	}
}

// Attach registers a new attached database. Returns an error if a database
// with the same name is already attached.
func (dm *DatabaseManager) Attach(name, path string, readOnly bool, cat *catalog.Catalog, stor *storage.Storage) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	n := strings.ToLower(name)
	if _, exists := dm.databases[n]; exists {
		return fmt.Errorf("database %q is already attached", name)
	}
	dm.databases[n] = &AttachedDatabase{
		Name:     name,
		Path:     path,
		Catalog:  cat,
		Storage:  stor,
		ReadOnly: readOnly,
	}
	return nil
}

// Detach removes an attached database from the registry.
// Returns an error if the database is not attached (unless ifExists is true)
// or if it is the current default database.
func (dm *DatabaseManager) Detach(name string, ifExists bool) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	n := strings.ToLower(name)
	if n == strings.ToLower(dm.defaultDB) {
		return fmt.Errorf("cannot detach the default database")
	}
	if _, exists := dm.databases[n]; !exists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("database %q is not attached", name)
	}
	delete(dm.databases, n)
	return nil
}

// Use sets the default database. Returns an error if the database is not attached.
func (dm *DatabaseManager) Use(database string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	n := strings.ToLower(database)
	if _, exists := dm.databases[n]; !exists {
		return fmt.Errorf("database %q is not attached", database)
	}
	dm.defaultDB = database
	return nil
}

// Get returns the attached database with the given name.
func (dm *DatabaseManager) Get(name string) (*AttachedDatabase, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	db, ok := dm.databases[strings.ToLower(name)]
	return db, ok
}

// GetAttached returns the catalog and storage for an attached database by name.
// Returns false if no database with that name is attached.
// This satisfies the executor.DatabaseManager interface.
func (dm *DatabaseManager) GetAttached(name string) (*catalog.Catalog, *storage.Storage, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	db, ok := dm.databases[strings.ToLower(name)]
	if !ok {
		return nil, nil, false
	}
	return db.Catalog, db.Storage, true
}

// Default returns the name of the default database.
func (dm *DatabaseManager) Default() string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.defaultDB
}

// List returns all attached databases.
func (dm *DatabaseManager) List() []*AttachedDatabase {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	result := make([]*AttachedDatabase, 0, len(dm.databases))
	for _, db := range dm.databases {
		result = append(result, db)
	}
	return result
}

// AttachedCatalogs returns a snapshot map of database-alias → catalog for all
// attached databases that are not the default (main) database.  The map keys
// are lower-cased aliases and are suitable for passing to
// binder.Binder.WithAttachedCatalogs.
//
// Performance note: AttachedCatalogs builds a new map on every call. For
// hot paths (per-statement binder setup), consider caching the result and
// invalidating on Attach/Detach/Create/Drop mutations.
func (dm *DatabaseManager) AttachedCatalogs() map[string]*catalog.Catalog {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	defaultLower := strings.ToLower(dm.defaultDB)
	result := make(map[string]*catalog.Catalog, len(dm.databases))
	for name, db := range dm.databases {
		if name == defaultLower {
			continue // skip the primary/default database
		}
		result[name] = db.Catalog
	}
	return result
}

// CreateDatabase creates and registers a new in-memory database.
// Returns an error if a database with the same name already exists
// (unless ifNotExists is true).
func (dm *DatabaseManager) CreateDatabase(name string, ifNotExists bool) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	n := strings.ToLower(name)
	if _, exists := dm.databases[n]; exists {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("database %q already exists", name)
	}
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	dm.databases[n] = &AttachedDatabase{
		Name:    name,
		Path:    ":memory:",
		Catalog: cat,
		Storage: stor,
	}
	return nil
}

// DropDatabase removes a database from the registry.
// Returns an error if the database does not exist (unless ifExists is true)
// or if it is the current default database.
func (dm *DatabaseManager) DropDatabase(name string, ifExists bool) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	n := strings.ToLower(name)
	if n == strings.ToLower(dm.defaultDB) {
		return fmt.Errorf("cannot drop the default database")
	}
	if _, exists := dm.databases[n]; !exists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("database %q does not exist", name)
	}
	delete(dm.databases, n)
	return nil
}
