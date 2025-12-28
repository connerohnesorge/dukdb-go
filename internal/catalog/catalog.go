// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
)

// Catalog manages database metadata including schemas and tables.
type Catalog struct {
	mu      sync.RWMutex
	schemas map[string]*Schema
}

// NewCatalog creates a new Catalog instance.
func NewCatalog() *Catalog {
	c := &Catalog{
		schemas: make(map[string]*Schema),
	}
	// Create default schema
	c.schemas["main"] = NewSchema("main")
	return c
}

// GetSchema returns a schema by name.
func (c *Catalog) GetSchema(
	name string,
) (*Schema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.schemas[name]
	return s, ok
}

// CreateSchema creates a new schema.
func (c *Catalog) CreateSchema(
	name string,
) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.schemas[name]; exists {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema already exists: " + name,
		}
	}

	s := NewSchema(name)
	c.schemas[name] = s
	return s, nil
}

// DropSchema drops a schema by name.
func (c *Catalog) DropSchema(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if name == "main" {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Cannot drop main schema",
		}
	}

	if _, exists := c.schemas[name]; !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + name,
		}
	}

	delete(c.schemas, name)
	return nil
}

// GetTable returns a table from the default schema.
func (c *Catalog) GetTable(
	name string,
) (*TableDef, bool) {
	return c.GetTableInSchema("main", name)
}

// GetTableInSchema returns a table from a specific schema.
func (c *Catalog) GetTableInSchema(
	schemaName, tableName string,
) (*TableDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	return schema.GetTable(tableName)
}

// CreateTable creates a new table in the default schema.
func (c *Catalog) CreateTable(
	table *TableDef,
) error {
	return c.CreateTableInSchema("main", table)
}

// CreateTableInSchema creates a new table in a specific schema.
func (c *Catalog) CreateTableInSchema(
	schemaName string,
	table *TableDef,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[schemaName]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.CreateTable(table)
}

// DropTable drops a table from the default schema.
func (c *Catalog) DropTable(name string) error {
	return c.DropTableInSchema("main", name)
}

// DropTableInSchema drops a table from a specific schema.
func (c *Catalog) DropTableInSchema(
	schemaName, tableName string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[schemaName]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.DropTable(tableName)
}

// ListTables returns all tables in the default schema.
func (c *Catalog) ListTables() []*TableDef {
	return c.ListTablesInSchema("main")
}

// ListTablesInSchema returns all tables in a specific schema.
func (c *Catalog) ListTablesInSchema(
	schemaName string,
) []*TableDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil
	}
	return schema.ListTables()
}

// Schema represents a database schema (namespace for tables).
type Schema struct {
	mu     sync.RWMutex
	name   string
	tables map[string]*TableDef
}

// NewSchema creates a new Schema instance.
func NewSchema(name string) *Schema {
	return &Schema{
		name:   name,
		tables: make(map[string]*TableDef),
	}
}

// Name returns the schema name.
func (s *Schema) Name() string {
	return s.name
}

// GetTable returns a table by name.
func (s *Schema) GetTable(
	name string,
) (*TableDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[name]
	return t, ok
}

// CreateTable creates a new table in the schema.
func (s *Schema) CreateTable(
	table *TableDef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[table.Name]; exists {
		return dukdb.ErrTableAlreadyExists
	}

	table.Schema = s.name
	s.tables[table.Name] = table
	return nil
}

// DropTable drops a table from the schema.
func (s *Schema) DropTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; !exists {
		return dukdb.ErrTableNotFound
	}

	delete(s.tables, name)
	return nil
}

// ListTables returns all tables in the schema.
func (s *Schema) ListTables() []*TableDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]*TableDef, 0, len(s.tables))
	for _, t := range s.tables {
		tables = append(tables, t)
	}
	return tables
}
