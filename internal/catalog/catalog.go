// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	"strings"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/optimizer"
)

// normalizeKey converts a name to lowercase for case-insensitive lookup.
// SQL identifiers are case-insensitive by default.
func normalizeKey(name string) string {
	return strings.ToLower(name)
}

// Catalog manages database metadata including schemas and tables.
type Catalog struct {
	mu            sync.RWMutex
	schemas       map[string]*Schema
	virtualTables map[string]*VirtualTableDef // name -> virtual table
}

// NewCatalog creates a new Catalog instance.
func NewCatalog() *Catalog {
	c := &Catalog{
		schemas: make(map[string]*Schema),
		virtualTables: make(
			map[string]*VirtualTableDef,
		),
	}
	// Create default schema
	c.schemas[normalizeKey("main")] = NewSchema("main")

	return c
}

// GetSchema returns a schema by name (case-insensitive).
func (c *Catalog) GetSchema(
	name string,
) (*Schema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.schemas[normalizeKey(name)]

	return s, ok
}

// ListSchemas returns all schemas in the catalog.
func (c *Catalog) ListSchemas() []*Schema {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schemas := make([]*Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		schemas = append(schemas, s)
	}

	return schemas
}

// CreateSchema creates a new schema.
func (c *Catalog) CreateSchema(
	name string,
) (*Schema, error) {
	return c.CreateSchemaIfNotExists(name, false)
}

// CreateSchemaIfNotExists creates a new schema with IF NOT EXISTS support.
// Schema names are case-insensitive; the original case is preserved.
func (c *Catalog) CreateSchemaIfNotExists(
	name string,
	ifNotExists bool,
) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeKey(name)
	if existing, exists := c.schemas[key]; exists {
		if ifNotExists {
			return existing, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema already exists: " + name,
		}
	}

	s := NewSchema(name)
	c.schemas[key] = s

	return s, nil
}

// DropSchema drops a schema by name.
func (c *Catalog) DropSchema(name string) error {
	return c.DropSchemaIfExists(name, false, false)
}

// DropSchemaIfExists drops a schema with IF EXISTS and CASCADE support.
// Schema names are case-insensitive.
func (c *Catalog) DropSchemaIfExists(name string, ifExists bool, cascade bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeKey(name)
	if key == normalizeKey("main") {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Cannot drop main schema",
		}
	}

	schema, exists := c.schemas[key]
	if !exists {
		if ifExists {
			return nil
		}
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + name,
		}
	}

	// Check if schema has objects (tables, views, indexes, sequences)
	schema.mu.RLock()
	hasObjects := len(schema.tables) > 0 || len(schema.views) > 0 ||
		len(schema.indexes) > 0 || len(schema.sequences) > 0
	schema.mu.RUnlock()

	if hasObjects && !cascade {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Cannot drop schema " + name + " because it contains objects (use CASCADE)",
		}
	}

	delete(c.schemas, key)
	return nil
}

// GetTable returns a table from the default schema.
func (c *Catalog) GetTable(
	name string,
) (*TableDef, bool) {
	return c.GetTableInSchema("main", name)
}

// GetTableInSchema returns a table from a specific schema.
// Both schema and table names are case-insensitive.
func (c *Catalog) GetTableInSchema(
	schemaName, tableName string,
) (*TableDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil, false
	}

	return schema.GetTable(tableName)
}

// GetTableInfo returns table information for the given schema and table name.
// Returns nil if the table does not exist.
// This method satisfies the optimizer.CatalogProvider interface.
func (c *Catalog) GetTableInfo(schema, table string) optimizer.TableInfo {
	tableDef, ok := c.GetTableInSchema(schema, table)
	if !ok || tableDef == nil {
		return nil
	}
	return tableDef
}

// CreateTable creates a new table in the default schema.
func (c *Catalog) CreateTable(
	table *TableDef,
) error {
	return c.CreateTableInSchema("main", table)
}

// CreateTableInSchema creates a new table in a specific schema.
// Schema name is case-insensitive.
func (c *Catalog) CreateTableInSchema(
	schemaName string,
	table *TableDef,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
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
// Both schema and table names are case-insensitive.
func (c *Catalog) DropTableInSchema(
	schemaName, tableName string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
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
// Schema name is case-insensitive.
func (c *Catalog) ListTablesInSchema(
	schemaName string,
) []*TableDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil
	}

	return schema.ListTables()
}

// GetView returns a view from the default schema.
func (c *Catalog) GetView(name string) (*ViewDef, bool) {
	return c.GetViewInSchema("main", name)
}

// GetViewInSchema returns a view from a specific schema.
// Both schema and view names are case-insensitive.
func (c *Catalog) GetViewInSchema(schemaName, viewName string) (*ViewDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil, false
	}

	return schema.GetView(viewName)
}

// CreateView creates a new view in the default schema.
func (c *Catalog) CreateView(view *ViewDef) error {
	return c.CreateViewInSchema("main", view)
}

// CreateViewInSchema creates a new view in a specific schema.
// Schema name is case-insensitive.
func (c *Catalog) CreateViewInSchema(schemaName string, view *ViewDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.CreateView(view)
}

// DropView drops a view from the default schema.
func (c *Catalog) DropView(name string) error {
	return c.DropViewInSchema("main", name)
}

// DropViewInSchema drops a view from a specific schema.
// Both schema and view names are case-insensitive.
func (c *Catalog) DropViewInSchema(schemaName, viewName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.DropView(viewName)
}

// GetViewsDependingOnTable returns all views that depend on the given table in a schema.
// Both schema and table names are case-insensitive.
func (c *Catalog) GetViewsDependingOnTable(schemaName, tableName string) []*ViewDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil
	}

	return schema.GetViewsDependingOnTable(tableName)
}

// GetIndex returns an index from the default schema.
func (c *Catalog) GetIndex(name string) (*IndexDef, bool) {
	return c.GetIndexInSchema("main", name)
}

// GetIndexInSchema returns an index from a specific schema.
// Both schema and index names are case-insensitive.
func (c *Catalog) GetIndexInSchema(schemaName, indexName string) (*IndexDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil, false
	}

	return schema.GetIndex(indexName)
}

// CreateIndex creates a new index in the default schema.
func (c *Catalog) CreateIndex(index *IndexDef) error {
	return c.CreateIndexInSchema("main", index)
}

// CreateIndexInSchema creates a new index in a specific schema.
// Schema name is case-insensitive.
func (c *Catalog) CreateIndexInSchema(schemaName string, index *IndexDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.CreateIndex(index)
}

// DropIndex drops an index from the default schema.
func (c *Catalog) DropIndex(name string) error {
	return c.DropIndexInSchema("main", name)
}

// DropIndexInSchema drops an index from a specific schema.
// Both schema and index names are case-insensitive.
func (c *Catalog) DropIndexInSchema(schemaName, indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.DropIndex(indexName)
}

// GetIndexesForTable returns all indexes for a specific table in a schema.
// Both schema and table names are case-insensitive.
func (c *Catalog) GetIndexesForTable(schemaName, tableName string) []*IndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil
	}

	return schema.GetIndexesForTable(tableName)
}

// GetSequence returns a sequence from the default schema.
func (c *Catalog) GetSequence(name string) (*SequenceDef, bool) {
	return c.GetSequenceInSchema("main", name)
}

// GetSequenceInSchema returns a sequence from a specific schema.
// Both schema and sequence names are case-insensitive.
func (c *Catalog) GetSequenceInSchema(schemaName, sequenceName string) (*SequenceDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return nil, false
	}

	return schema.GetSequence(sequenceName)
}

// CreateSequence creates a new sequence in the default schema.
func (c *Catalog) CreateSequence(sequence *SequenceDef) error {
	return c.CreateSequenceInSchema("main", sequence)
}

// CreateSequenceInSchema creates a new sequence in a specific schema.
// Schema name is case-insensitive.
func (c *Catalog) CreateSequenceInSchema(schemaName string, sequence *SequenceDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.CreateSequence(sequence)
}

// DropSequence drops a sequence from the default schema.
func (c *Catalog) DropSequence(name string) error {
	return c.DropSequenceInSchema("main", name)
}

// DropSequenceInSchema drops a sequence from a specific schema.
// Both schema and sequence names are case-insensitive.
func (c *Catalog) DropSequenceInSchema(schemaName, sequenceName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.schemas[normalizeKey(schemaName)]
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Schema not found: " + schemaName,
		}
	}

	return schema.DropSequence(sequenceName)
}

// RegisterVirtualTable registers a virtual table in the catalog.
// Virtual tables appear as regular tables for query resolution but
// read data from external sources.
// Virtual table names are case-insensitive.
func (c *Catalog) RegisterVirtualTable(
	vt dukdb.VirtualTable,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	name := vt.Name()
	if name == "" {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: virtual table name cannot be empty",
		}
	}

	key := normalizeKey(name)

	// Check for conflicts with existing virtual tables
	if _, exists := c.virtualTables[key]; exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: virtual table already exists: " + name,
		}
	}

	// Check for conflicts with existing regular tables in main schema
	if schema, ok := c.schemas[normalizeKey("main")]; ok {
		if _, exists := schema.tables[key]; exists {
			return &dukdb.Error{
				Type: dukdb.ErrorTypeCatalog,
				Msg:  "Catalog Error: table already exists: " + name,
			}
		}
	}

	c.virtualTables[key] = NewVirtualTableDef(vt)

	return nil
}

// UnregisterVirtualTable removes a virtual table from the catalog.
// Virtual table names are case-insensitive.
func (c *Catalog) UnregisterVirtualTable(
	name string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := c.virtualTables[key]; !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: virtual table not found: " + name,
		}
	}

	delete(c.virtualTables, key)

	return nil
}

// GetVirtualTableDef returns a virtual table definition by name.
// This is used internally for query binding.
// Virtual table names are case-insensitive.
func (c *Catalog) GetVirtualTableDef(
	name string,
) (*VirtualTableDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vt, ok := c.virtualTables[normalizeKey(name)]

	return vt, ok
}

// GetVirtualTable returns a virtual table as the dukdb.VirtualTable interface.
// This method satisfies the dukdb.VirtualTableRegistry interface.
// Virtual table names are case-insensitive.
func (c *Catalog) GetVirtualTable(
	name string,
) (dukdb.VirtualTable, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vt, ok := c.virtualTables[normalizeKey(name)]
	if !ok {
		return nil, false
	}

	return vt.VirtualTable(), true
}

// IsVirtualTable returns true if the given table name is a virtual table.
// Virtual table names are case-insensitive.
func (c *Catalog) IsVirtualTable(
	name string,
) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.virtualTables[normalizeKey(name)]

	return ok
}

// Schema represents a database schema (namespace for tables).
type Schema struct {
	mu        sync.RWMutex
	name      string
	tables    map[string]*TableDef
	views     map[string]*ViewDef
	indexes   map[string]*IndexDef
	sequences map[string]*SequenceDef
}

// NewSchema creates a new Schema instance.
func NewSchema(name string) *Schema {
	return &Schema{
		name:      name,
		tables:    make(map[string]*TableDef),
		views:     make(map[string]*ViewDef),
		indexes:   make(map[string]*IndexDef),
		sequences: make(map[string]*SequenceDef),
	}
}

// Name returns the schema name.
func (s *Schema) Name() string {
	return s.name
}

// GetTable returns a table by name (case-insensitive).
func (s *Schema) GetTable(
	name string,
) (*TableDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[normalizeKey(name)]

	return t, ok
}

// CreateTable creates a new table in the schema.
// Table names are case-insensitive; the original case is preserved.
func (s *Schema) CreateTable(
	table *TableDef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(table.Name)
	if _, exists := s.tables[key]; exists {
		return dukdb.ErrTableAlreadyExists
	}

	table.Schema = s.name
	s.tables[key] = table

	return nil
}

// DropTable drops a table from the schema.
// Table names are case-insensitive.
func (s *Schema) DropTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := s.tables[key]; !exists {
		return dukdb.ErrTableNotFound
	}

	delete(s.tables, key)

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

// GetView returns a view by name (case-insensitive).
func (s *Schema) GetView(name string) (*ViewDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.views[normalizeKey(name)]
	return v, ok
}

// CreateView creates a new view in the schema.
// View names are case-insensitive; the original case is preserved.
func (s *Schema) CreateView(view *ViewDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(view.Name)
	if _, exists := s.views[key]; exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: View already exists: " + view.Name,
		}
	}

	view.Schema = s.name
	s.views[key] = view
	return nil
}

// DropView drops a view from the schema.
// View names are case-insensitive.
func (s *Schema) DropView(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := s.views[key]; !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: View not found: " + name,
		}
	}

	delete(s.views, key)
	return nil
}

// ListViews returns all views in the schema.
func (s *Schema) ListViews() []*ViewDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	views := make([]*ViewDef, 0, len(s.views))
	for _, v := range s.views {
		views = append(views, v)
	}
	return views
}

// GetViewsDependingOnTable returns all views that depend on the given table.
// Table name is case-insensitive.
func (s *Schema) GetViewsDependingOnTable(tableName string) []*ViewDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var dependentViews []*ViewDef
	for _, v := range s.views {
		if v.DependsOnTable(tableName) {
			dependentViews = append(dependentViews, v)
		}
	}
	return dependentViews
}

// GetIndex returns an index by name (case-insensitive).
func (s *Schema) GetIndex(name string) (*IndexDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.indexes[normalizeKey(name)]
	return idx, ok
}

// CreateIndex creates a new index in the schema.
// Index names are case-insensitive; the original case is preserved.
func (s *Schema) CreateIndex(index *IndexDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(index.Name)
	if _, exists := s.indexes[key]; exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Index already exists: " + index.Name,
		}
	}

	index.Schema = s.name
	s.indexes[key] = index
	return nil
}

// DropIndex drops an index from the schema.
// Index names are case-insensitive.
func (s *Schema) DropIndex(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := s.indexes[key]; !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Index not found: " + name,
		}
	}

	delete(s.indexes, key)
	return nil
}

// GetIndexesForTable returns all indexes for a specific table.
// Table name is case-insensitive.
func (s *Schema) GetIndexesForTable(tableName string) []*IndexDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tableKey := normalizeKey(tableName)
	indexes := make([]*IndexDef, 0)
	for _, idx := range s.indexes {
		if normalizeKey(idx.Table) == tableKey {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// ListIndexes returns all indexes in the schema.
func (s *Schema) ListIndexes() []*IndexDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	indexes := make([]*IndexDef, 0, len(s.indexes))
	for _, idx := range s.indexes {
		indexes = append(indexes, idx)
	}
	return indexes
}

// GetSequence returns a sequence by name (case-insensitive).
func (s *Schema) GetSequence(name string) (*SequenceDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq, ok := s.sequences[normalizeKey(name)]
	return seq, ok
}

// CreateSequence creates a new sequence in the schema.
// Sequence names are case-insensitive; the original case is preserved.
func (s *Schema) CreateSequence(sequence *SequenceDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(sequence.Name)
	if _, exists := s.sequences[key]; exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Sequence already exists: " + sequence.Name,
		}
	}

	sequence.Schema = s.name
	s.sequences[key] = sequence
	return nil
}

// DropSequence drops a sequence from the schema.
// Sequence names are case-insensitive.
func (s *Schema) DropSequence(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := normalizeKey(name)
	if _, exists := s.sequences[key]; !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "Catalog Error: Sequence not found: " + name,
		}
	}

	delete(s.sequences, key)
	return nil
}

// ListSequences returns all sequences in the schema.
func (s *Schema) ListSequences() []*SequenceDef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sequences := make([]*SequenceDef, 0, len(s.sequences))
	for _, seq := range s.sequences {
		sequences = append(sequences, seq)
	}
	return sequences
}

// Clone creates a deep copy of the schema for transaction rollback support.
func (s *Schema) Clone() *Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()

	newSchema := &Schema{
		name:      s.name,
		tables:    make(map[string]*TableDef),
		views:     make(map[string]*ViewDef),
		indexes:   make(map[string]*IndexDef),
		sequences: make(map[string]*SequenceDef),
	}

	// Clone tables
	for key, table := range s.tables {
		newSchema.tables[key] = table.Clone()
	}

	// Clone views
	for key, view := range s.views {
		newSchema.views[key] = view.Clone()
	}

	// Clone indexes
	for key, index := range s.indexes {
		newSchema.indexes[key] = index.Clone()
	}

	// Clone sequences
	for key, seq := range s.sequences {
		newSchema.sequences[key] = seq.Clone()
	}

	return newSchema
}

// Clone creates a deep copy of the catalog for transaction rollback support.
// This copies all schemas and their objects (tables, views, indexes, sequences).
// Virtual tables are NOT cloned as they are external references.
func (c *Catalog) Clone() *Catalog {
	c.mu.RLock()
	defer c.mu.RUnlock()

	newCatalog := &Catalog{
		schemas:       make(map[string]*Schema),
		virtualTables: c.virtualTables, // Share virtual tables (external references)
	}

	for key, schema := range c.schemas {
		newCatalog.schemas[key] = schema.Clone()
	}

	return newCatalog
}

// RestoreFrom restores this catalog from a snapshot.
// This is used for DDL transaction rollback.
func (c *Catalog) RestoreFrom(snapshot *Catalog) {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot.mu.RLock()
	defer snapshot.mu.RUnlock()

	// Clear current schemas and restore from snapshot
	c.schemas = make(map[string]*Schema)
	for key, schema := range snapshot.schemas {
		c.schemas[key] = schema.Clone()
	}
	// Virtual tables are shared, not restored
}

// Compile-time assertion that Catalog implements dukdb.VirtualTableRegistry
var _ dukdb.VirtualTableRegistry = (*Catalog)(nil)
