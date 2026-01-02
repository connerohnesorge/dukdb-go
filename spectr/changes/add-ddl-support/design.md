# Design: Full DDL Support

## Context

This design document outlines the technical approach for implementing comprehensive DDL (Data Definition Language) support in dukdb-go. The implementation builds upon existing infrastructure:
- WAL entries already defined but not wired (`CreateViewEntry`, `CreateIndexEntry`, etc.)
- Catalog has `Schema` struct that only manages `tables` map
- Parser only handles `CREATE TABLE` and `DROP TABLE`
- Binder has no view resolution or sequence support

**Stakeholders**:
- Application developers needing view abstractions
- Data engineers requiring index-based query optimization
- Users migrating from DuckDB with existing DDL

**Constraints**:
- Pure Go implementation (no CGO)
- API compatibility with `go-duckdb` driver
- Follow existing parser/binder patterns exactly

## Goals / Non-Goals

**Goals**:
1. Implement CREATE VIEW, DROP VIEW with view resolution
2. Implement CREATE INDEX, DROP INDEX with hash index storage
3. Implement CREATE SEQUENCE, DROP SEQUENCE with NEXTVAL/CURRVAL
4. Implement CREATE SCHEMA, DROP SCHEMA with namespace support
5. Extend ALTER TABLE with rename and column operations
6. Wire up existing WAL entry types to execution handlers
7. Maintain API compatibility with go-duckdb

**Non-Goals**:
1. ART (Adaptive Radix Tree) indexes (future enhancement)
2. Unique indexes enforcement (beyond basic index creation)
3. Foreign key constraints
4. CHECK constraints
5. Generated columns

## Decisions

### Decision 1: Parser Structure

**Options**:
A. Create separate parser files per statement type (parser_create.go, parser_drop.go, etc.)
B. Keep all DDL parsing in parser.go with helper functions
C. Create parser_ddl.go for all DDL parsing functions

**Choice**: C - Create `internal/parser/parser_ddl.go`

**Rationale**:
- Follows pattern of eventual separation as codebase grows
- Keeps parser.go focused on dispatch logic
- Makes DDL-related code easy to locate
- Mirrors structure used in other packages (e.g., binder has bind_stmt.go for all statements)

```go
// internal/parser/parser_ddl.go
package parser

func (p *parser) parseCreate() (Statement, error) {
    // Existing: check for TABLE, dispatch to parseCreateTable
    // New: add checks for VIEW, INDEX, SEQUENCE, SCHEMA
}

func (p *parser) parseDrop() (Statement, error) {
    // Existing: check for TABLE, dispatch to parseDropTable
    // New: add checks for VIEW, INDEX, SEQUENCE, SCHEMA
}

func (p *parser) parseCreateView() (*CreateViewStmt, error)
func (p *parser) parseDropView() (*DropViewStmt, error)
func (p *parser) parseCreateIndex() (*CreateIndexStmt, error)
func (p *parser) parseDropIndex() (*DropIndexStmt, error)
func (p *parser) parseCreateSequence() (*CreateSequenceStmt, error)
func (p *parser) parseDropSequence() (*DropSequenceStmt, error)
func (p *parser) parseCreateSchema() (*CreateSchemaStmt, error)
func (p *parser) parseDropSchema() (*DropSchemaStmt, error)
func (p *parser) parseAlterTable() (*AlterTableStmt, error)
```

### Decision 2: AST Node Design

**Options**:
A. Flat structure with one statement type per DDL operation
B. Unified DDL statement with operation type enum
C. Separate statement types per operation (chosen)

**Choice**: C - Separate statement types

**Rationale**:
- Matches existing pattern (CreateTableStmt, DropTableStmt)
- Easier binder/planner/executor implementation
- Clearer semantics in visitor pattern
- Type-safe switch statements

```go
// internal/parser/ast.go

// CREATE VIEW statement
type CreateViewStmt struct {
    Schema        string
    View          string
    IfNotExists   bool
    Query         *SelectStmt // The view definition
}

func (*CreateViewStmt) stmtNode() {}
func (*CreateViewStmt) Type() dukdb.StmtType { return STATEMENT_TYPE_CREATE }
func (s *CreateViewStmt) Accept(v Visitor) { v.VisitCreateViewStmt(s) }

// DROP VIEW statement
type DropViewStmt struct {
    Schema   string
    View     string
    IfExists bool
}

// CREATE INDEX statement
type CreateIndexStmt struct {
    Schema      string
    Table       string
    Index       string
    IfNotExists bool
    Columns     []string
    IsUnique    bool
}

// DROP INDEX statement
type DropIndexStmt struct {
    Schema    string
    Index     string
    IfExists  bool
}

// CREATE SEQUENCE statement
type CreateSequenceStmt struct {
    Schema      string
    Sequence    string
    IfNotExists bool
    StartWith   int64
    IncrementBy int64
    MinValue    *int64
    MaxValue    *int64
    IsCycle     bool
}

// DROP SEQUENCE statement
type DropSequenceStmt struct {
    Schema    string
    Sequence  string
    IfExists  bool
}

// CREATE SCHEMA statement
type CreateSchemaStmt struct {
    Schema    string
    IfNotExists bool
}

// DROP SCHEMA statement
type DropSchemaStmt struct {
    Schema    string
    IfExists  bool
    Cascade   bool // If true, drop all objects in schema
}

// ALTER TABLE statement
type AlterTableStmt struct {
    Schema       string
    Table        string
    Operation    AlterTableOp
    // Operation-specific fields:
    NewTableName string      // RENAME TO
    OldColumn    string      // RENAME COLUMN, DROP COLUMN
    NewColumn    string      // RENAME COLUMN
    Column       string      // DROP COLUMN
}

type AlterTableOp int

const (
    AlterTableRenameTo AlterTableOp = iota
    AlterTableRenameColumn
    AlterTableDropColumn
    AlterTableSet
)
```

### Decision 3: Catalog Extension

**Options**:
A. Add new maps to existing Schema struct (chosen)
B. Create separate CatalogView, CatalogIndex, CatalogSequence singletons
C. Use inheritance hierarchy with CatalogEntry interface

**Choice**: A - Extend Schema struct

**Rationale**:
- Consistent with existing design (Schema only had tables)
- Simple and maintainable
- Natural namespace grouping (views belong to schemas)
- Easy lookup: `schema.GetView(name)`, `schema.GetIndex(name)`

```go
// internal/catalog/schema.go

type Schema struct {
    mu      sync.RWMutex
    name    string
    tables  map[string]*TableDef
    views   map[string]*ViewDef    // NEW
    indexes map[string]*IndexDef   // NEW
    sequences map[string]*SequenceDef // NEW
}

// internal/catalog/view.go (NEW FILE)

type ViewDef struct {
    Name   string
    Schema string
    Query  string // Serialized SELECT statement
}

func NewViewDef(name, schema, query string) *ViewDef {
    return &ViewDef{
        Name:   name,
        Schema: schema,
        Query:  query,
    }
}

// internal/catalog/index.go (NEW FILE)

type IndexDef struct {
    Name      string
    Schema    string
    Table     string
    Columns   []string
    IsUnique  bool
    IsPrimary bool // For PRIMARY KEY indexes
}

func NewIndexDef(name, schema, table string, columns []string, isUnique bool) *IndexDef {
    return &IndexDef{
        Name:      name,
        Schema:    schema,
        Table:     table,
        Columns:   columns,
        IsUnique:  isUnique,
    }
}

// internal/catalog/sequence.go (NEW FILE)

type SequenceDef struct {
    Name        string
    Schema      string
    CurrentVal  int64
    StartWith   int64
    IncrementBy int64
    MinValue    int64
    MaxValue    int64
    IsCycle     bool
}

func NewSequenceDef(name, schema string) *SequenceDef {
    return &SequenceDef{
        Name:        name,
        Schema:      schema,
        CurrentVal:  1,
        StartWith:   1,
        IncrementBy: 1,
        MinValue:    math.MinInt64,
        MaxValue:    math.MaxInt64,
        IsCycle:     false,
    }
}
```

### Decision 4: View Resolution Strategy

**Options**:
A. Materialized views (store query results, refresh on demand)
B. Virtual views (expand view definition at query time) (chosen)
C. Hybrid approach with both types

**Choice**: B - Virtual views with expansion at query time

**Rationale**:
- Matches DuckDB behavior (DuckDB uses virtual views)
- Simpler implementation (no storage or refresh logic)
- Always reflects current table data
- Matches existing subquery handling pattern in binder

```go
// internal/binder/bind_stmt.go - modify bindTableRef

func (b *Binder) bindTableRef(ref parser.TableRef) (*BoundTableRef, error) {
    // ... existing subquery and table function handling ...

    schema := ref.Schema
    if schema == "" {
        schema = "main"
    }

    // Check for virtual tables first (existing behavior)
    if schema == "main" {
        if vtDef, ok := b.catalog.GetVirtualTableDef(ref.TableName); ok {
            // ... existing virtual table handling ...
        }
    }

    // NEW: Check for views (takes precedence over regular tables)
    if viewDef, ok := b.catalog.GetViewInSchema(schema, ref.TableName); ok {
        return b.bindViewRef(viewDef, ref.Alias)
    }

    // NEW: Check for table functions
    if ref.TableFunction != nil {
        return b.bindTableFunction(ref)
    }

    // ... existing regular table lookup ...
}

// bindViewRef binds a view reference by expanding its definition
func (b *Binder) bindViewRef(viewDef *catalog.ViewDef, alias string) (*BoundTableRef, error) {
    // Parse the view's SELECT statement
    viewStmt, err := parser.Parse(viewDef.Query)
    if err != nil {
        return nil, b.errorf("failed to parse view definition: %v", err)
    }

    // Cast to SelectStmt (views are always SELECT)
    selectStmt, ok := viewStmt.(*parser.SelectStmt)
    if !ok {
        return nil, b.errorf("invalid view definition: expected SELECT statement")
    }

    // Bind the view's SELECT statement (recursive - handles nested views)
    boundSelect, err := b.bindSelect(selectStmt)
    if err != nil {
        return nil, err
    }

    // Create bound columns from the expanded query
    boundRef := &BoundTableRef{
        Schema:    viewDef.Schema,
        TableName: viewDef.Name,
        Alias:     alias,
        ViewDef:   viewDef,
    }

    for i, col := range boundSelect.Columns {
        colName := col.Alias
        if colName == "" {
            colName = fmt.Sprintf("col%d", i)
        }
        boundRef.Columns = append(boundRef.Columns, &BoundColumn{
            Table:      alias,
            Column:     colName,
            ColumnIdx:  i,
            Type:       col.Expr.ResultType(),
            SourceType: "view",
        })
    }

    b.scope.tables[alias] = boundRef
    b.scope.aliases[alias] = viewDef.Name

    return boundRef, nil
}
```

### Decision 5: Sequence Implementation

**Options**:
A. Sequence as catalog entry with current value (chosen)
B. Sequence as separate storage with transaction log
C. Sequence as table with single row

**Choice**: A - Sequence as catalog entry

**Rationale**:
- Simple and consistent with other catalog entries
- Current value stored with metadata
- NEXTVAL function will increment and return
- No separate persistence needed beyond WAL

```go
// Sequence operations in executor

type SequenceState struct {
    CurrentVal int64
    Increment  int64
    MinValue   int64
    MaxValue   int64
    IsCycle    bool
}

func (e *Executor) NextVal(sequenceName string) (int64, error) {
    seq, ok := e.catalog.GetSequence(sequenceName)
    if !ok {
        return 0, fmt.Errorf("sequence not found: %s", sequenceName)
    }

    newVal := seq.CurrentVal + seq.IncrementBy

    // Check bounds
    if seq.IncrementBy > 0 && newVal > seq.MaxValue {
        if seq.IsCycle {
            newVal = seq.StartWith
        } else {
            return 0, fmt.Errorf("sequence %s reached max value %d", sequenceName, seq.MaxValue)
        }
    } else if seq.IncrementBy < 0 && newVal < seq.MinValue {
        if seq.IsCycle {
            newVal = seq.StartWith
        } else {
            return 0, fmt.Errorf("sequence %s reached min value %d", sequenceName, seq.MinValue)
        }
    }

    seq.CurrentVal = newVal
    return newVal, nil
}
```

### Decision 6: Index Storage

**Options**:
A. No persistent index storage (just metadata)
B. Simple hash index with in-memory storage (chosen)
C. Full ART (Adaptive Radix Tree) implementation

**Choice**: B - Simple hash index initially

**Rationale**:
- Matches scope of initial implementation
- Provides basic indexing capability
- ART can be added as future enhancement
- Hash index sufficient for equality lookups

```go
// internal/storage/index.go (NEW FILE)

package storage

// HashIndex implements a simple hash-based index
type HashIndex struct {
    Name       string
    TableName  string
    Columns    []string
    IsUnique   bool
    entries    map[hashKey][]RowID
}

type RowID uint64

type hashKey struct {
    // Composite key from indexed columns
    // Will use serialization of column values
}

// Index lookups will use this structure
```

### Decision 7: ALTER TABLE Extensions

**Options**:
A. Implement all ALTER TABLE operations at once
B. Implement in phases (RENAME first, then columns)
C. Implement only most common operations

**Choice**: B - Implement in phases, starting with rename operations

**Rationale**:
- Reduces scope and risk
- Easier to test incrementally
- Can add more operations later
- Matches implementation approach for other DDL

```go
// ALTER TABLE operations supported initially
type AlterTableOp int

const (
    AlterTableRenameTo AlterTableOp = iota
    AlterTableRenameColumn
    AlterTableDropColumn
    AlterTableAddConstraint
    AlterTableSetOption
)
```

## WAL Integration

The existing WAL entry types in `internal/wal/entry.go` need handlers:

```go
// internal/wal/entry_handler.go (NEW FILE)

package wal

type EntryHandler interface {
    HandleCreateTable(entry *CreateTableEntry) error
    HandleDropTable(entry *DropTableEntry) error
    HandleCreateView(entry *CreateViewEntry) error
    HandleDropView(entry *DropViewEntry) error
    HandleCreateIndex(entry *CreateIndexEntry) error
    HandleDropIndex(entry *DropIndexEntry) error
    HandleCreateSequence(entry *CreateSequenceEntry) error
    HandleDropSequence(entry *DropSequenceEntry) error
    HandleCreateSchema(entry *CreateSchemaEntry) error
    HandleDropSchema(entry *DropSchemaEntry) error
    HandleAlterTable(entry *AlterTableEntry) error
}
```

The existing `CreateViewEntry`, `CreateIndexEntry`, etc. in `entry_catalog.go` are already implemented - just need handlers.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| View circular dependency | High (infinite recursion) | Detect during binding, prevent cycles |
| Index storage complexity | Medium | Start with simple hash index, add ART later |
| Sequence concurrency | Medium | Use mutex in catalog for increment |
| ALTER TABLE column drops | Medium | Handle dependent objects carefully |
| Performance of view expansion | Low | Views are typically small queries |

## Performance Considerations

1. **View Resolution**: Views expand at bind time, so repeated queries re-bind. This is acceptable as binding is relatively cheap compared to execution.

2. **Index Lookups**: Hash index O(1) lookup for equality predicates.

3. **Sequence Increment**: Single catalog update with mutex - acceptable for typical sequence usage patterns.

## Migration Plan

### Phase 1: Parser and AST
1. Add DDL AST nodes to `ast.go`
2. Add DDL parsing functions to `parser_ddl.go`
3. Update `Visitor` interface
4. Test parsing with sample statements

### Phase 2: Catalog Extension
1. Add `ViewDef`, `IndexDef`, `SequenceDef` types
2. Extend `Schema` with new maps
3. Add catalog management methods
4. Test catalog CRUD operations

### Phase 3: Binder Integration
1. Add `bindCreateView`, `bindCreateIndex`, etc.
2. Implement view resolution in `bindTableRef`
3. Add sequence functions (NEXTVAL, CURRVAL)
4. Test binding with valid and invalid statements

### Phase 4: WAL Integration
1. Implement entry handlers
2. Wire up WAL recovery for DDL
3. Test crash recovery with DDL statements

### Phase 5: Planner and Executor
1. Add DDL logical plan nodes
2. Add DDL physical plan nodes
3. Implement DDL execution operators
4. Test full DML flow

## Open Questions

1. **View Column Aliases**: Should we preserve column aliases from view definition or infer from query?
   - Current decision: Infer from query, matching DuckDB behavior

2. **Index Naming**: Should we require explicit index names or generate automatically?
   - Current decision: Require explicit names, matching DuckDB behavior

3. **Sequence Precision**: Should sequences support BIGINT only or allow other integer types?
   - Current decision: BIGINT only initially, matching DuckDB default
