# Design: ATTACH/DETACH/USE Database Execution

## Implementation Details

### 1. Binder Changes (internal/binder/)

The query pipeline is Parser -> Binder -> Planner -> Executor. All statements pass through `binder.Bind()` (at `internal/binder/binder.go`), which switches on parser AST types and produces `BoundStatement` types.

Add five new bound statement types to `internal/binder/statements.go`:

```go
type BoundAttachStmt struct {
    Path     string
    Alias    string
    ReadOnly bool
    Options  map[string]string
}

type BoundDetachStmt struct {
    Name     string
    IfExists bool
}

type BoundUseStmt struct {
    Database string
    Schema   string
}

type BoundCreateDatabaseStmt struct {
    Name        string
    IfNotExists bool
}

type BoundDropDatabaseStmt struct {
    Name     string
    IfExists bool
}
```

Each type must implement `BoundStatement` (the `boundStatement()` marker method). Add corresponding cases to the `Bind()` switch in `binder.go` for `*parser.AttachStmt`, `*parser.DetachStmt`, `*parser.UseStmt`, `*parser.CreateDatabaseStmt`, and `*parser.DropDatabaseStmt`.

### 2. Logical Plan Nodes (internal/planner/logical.go)

Add five new logical plan node types following the existing pattern (e.g., `LogicalCreateTable`, `LogicalDropTable`):

```go
type LogicalAttach struct {
    Path     string
    Alias    string
    ReadOnly bool
    Options  map[string]string
}

func (*LogicalAttach) logicalPlanNode()            {}
func (*LogicalAttach) Children() []LogicalPlan     { return nil }
func (*LogicalAttach) OutputColumns() []ColumnBinding { return nil }

type LogicalDetach struct {
    Name     string
    IfExists bool
}

func (*LogicalDetach) logicalPlanNode()            {}
func (*LogicalDetach) Children() []LogicalPlan     { return nil }
func (*LogicalDetach) OutputColumns() []ColumnBinding { return nil }

type LogicalUse struct {
    Database string
    Schema   string
}

func (*LogicalUse) logicalPlanNode()               {}
func (*LogicalUse) Children() []LogicalPlan        { return nil }
func (*LogicalUse) OutputColumns() []ColumnBinding { return nil }

type LogicalCreateDatabase struct {
    Name        string
    IfNotExists bool
}

func (*LogicalCreateDatabase) logicalPlanNode()            {}
func (*LogicalCreateDatabase) Children() []LogicalPlan     { return nil }
func (*LogicalCreateDatabase) OutputColumns() []ColumnBinding { return nil }

type LogicalDropDatabase struct {
    Name     string
    IfExists bool
}

func (*LogicalDropDatabase) logicalPlanNode()            {}
func (*LogicalDropDatabase) Children() []LogicalPlan     { return nil }
func (*LogicalDropDatabase) OutputColumns() []ColumnBinding { return nil }
```

Add cases in `createLogicalPlan()` (in `internal/planner/physical.go`) to convert each `Bound*Stmt` to its corresponding `Logical*` node.

### 3. Physical Plan Nodes (internal/planner/physical.go)

Add five new physical plan node types. Physical plan types use the `physicalPlanNode()` marker interface method (there is no `PhysicalPlanType` enum):

```go
// PhysicalAttach represents ATTACH [DATABASE] 'path' [AS alias] [(options)].
type PhysicalAttach struct {
    Path     string
    Alias    string
    ReadOnly bool
    Options  map[string]string
}

func (*PhysicalAttach) physicalPlanNode() {}

// PhysicalDetach represents DETACH [DATABASE] [IF EXISTS] name.
type PhysicalDetach struct {
    Name     string
    IfExists bool
}

func (*PhysicalDetach) physicalPlanNode() {}

// PhysicalUse represents USE database[.schema].
type PhysicalUse struct {
    Database string
    Schema   string
}

func (*PhysicalUse) physicalPlanNode() {}

// PhysicalCreateDatabase represents CREATE DATABASE [IF NOT EXISTS] name.
type PhysicalCreateDatabase struct {
    Name        string
    IfNotExists bool
}

func (*PhysicalCreateDatabase) physicalPlanNode() {}

// PhysicalDropDatabase represents DROP DATABASE [IF EXISTS] name.
type PhysicalDropDatabase struct {
    Name     string
    IfExists bool
}

func (*PhysicalDropDatabase) physicalPlanNode() {}
```

Add cases in `createPhysicalPlan()` to convert each `Logical*` node to its corresponding `Physical*` node.

### 4. Planner Conversion (internal/planner/physical.go)

The planner pipeline is: `Plan()` calls `createLogicalPlan(BoundStatement)` -> `createPhysicalPlan(LogicalPlan)`. Add cases in both functions for the five new statement/plan types:

In `createLogicalPlan()`, add cases for `*BoundAttachStmt`, `*BoundDetachStmt`, `*BoundUseStmt`, `*BoundCreateDatabaseStmt`, `*BoundDropDatabaseStmt` converting to their `Logical*` equivalents.

In `createPhysicalPlan()`, add cases for `*LogicalAttach`, `*LogicalDetach`, `*LogicalUse`, `*LogicalCreateDatabase`, `*LogicalDropDatabase` converting to their `Physical*` equivalents.

### 5. Executor Dispatch (internal/executor/ddl.go)

Add execution handlers that delegate to `engine.DatabaseManager`:

```go
func (e *Executor) executeAttach(ctx *ExecContext, plan *planner.PhysicalAttach) error {
    // 1. Resolve path (relative to current database directory)
    absPath := e.resolveDatabasePath(plan.Path)

    // 2. Open or create the database file (OpenDatabase is a new method; see section 7)
    cat, stor, err := e.engine.OpenDatabase(absPath, plan.ReadOnly)
    if err != nil {
        return &dukdb.Error{
            Type: dukdb.ErrorTypeIO,
            Msg:  fmt.Sprintf("cannot attach database '%s': %v", plan.Path, err),
        }
    }

    // 3. Determine alias (default: filename without extension)
    alias := plan.Alias
    if alias == "" {
        alias = filepath.Base(strings.TrimSuffix(absPath, filepath.Ext(absPath)))
    }

    // 4. Register with DatabaseManager
    return e.engine.DatabaseManager().Attach(alias, absPath, plan.ReadOnly, cat, stor)
}

func (e *Executor) executeDetach(ctx *ExecContext, plan *planner.PhysicalDetach) error {
    return e.engine.DatabaseManager().Detach(plan.Name, plan.IfExists)
}

func (e *Executor) executeUse(ctx *ExecContext, plan *planner.PhysicalUse) error {
    // DatabaseManager.Use() takes a single database name argument.
    // Schema switching (if plan.Schema is set) is handled separately
    // by setting the current schema on the connection/session context.
    return e.engine.DatabaseManager().Use(plan.Database)
}
```

### 6. Cross-Database Name Resolution (internal/binder/)

When resolving a three-part name like `analytics.main.sales`:
1. First part = database name → look up in DatabaseManager
2. Second part = schema name → resolve in that database's Catalog
3. Third part = table/view name → resolve in that schema

When resolving a two-part name like `analytics.sales`:
1. Check if first part is a known database name → if so, resolve table in that database's default schema
2. Otherwise, treat as schema.table in the current database (existing behavior)

```go
func (b *Binder) resolveTableRef(ref *parser.TableRef) (*catalog.TableEntry, error) {
    parts := strings.Split(ref.Name, ".")
    switch len(parts) {
    case 3: // db.schema.table
        db, ok := b.engine.DatabaseManager().Get(parts[0])
        if !ok {
            return nil, fmt.Errorf("database %q not found", parts[0])
        }
        return db.Catalog.ResolveTable(parts[1], parts[2])
    case 2: // schema.table OR db.table
        if db, ok := b.engine.DatabaseManager().Get(parts[0]); ok {
            return db.Catalog.ResolveTable("main", parts[1])
        }
        return b.catalog.ResolveTable(parts[0], parts[1])
    case 1: // table
        return b.catalog.ResolveTable(b.currentSchema, parts[0])
    }
}
```

### 7. Engine: Database Open/Create (internal/engine/engine.go)

**Note**: Neither `OpenDatabase()` nor `loadCatalogFromStorage()` exist yet; both must be created as new methods on `Engine`.

Add `OpenDatabase()` method that:
1. Creates a new `Catalog` instance for the attached database
2. Creates a new `Storage` instance pointing to the file path
3. Loads existing catalog metadata from the file if it exists
4. Returns the catalog and storage for registration

```go
func (eng *Engine) OpenDatabase(path string, readOnly bool) (*catalog.Catalog, *storage.Storage, error) {
    cat := catalog.NewCatalog()
    stor, err := storage.Open(path, readOnly)
    if err != nil {
        return nil, nil, err
    }
    // Load existing tables, views, etc. from the file's metadata
    if err := eng.loadCatalogFromStorage(cat, stor); err != nil {
        stor.Close()
        return nil, nil, err
    }
    return cat, stor, nil
}
```

### 8. SHOW DATABASES / duckdb_databases()

Add a system function or table function to list attached databases:

```go
// In information_schema or system functions:
// SHOW DATABASES → list all attached databases with their paths and options
```

## Context

DuckDB's ATTACH system allows working with multiple database files simultaneously, which is essential for:
- Analytics workflows that join data across databases
- ETL pipelines that read from one database and write to another
- Testing with isolated databases
- Read-only access to shared databases

The existing `DatabaseManager` in `internal/engine/database_manager.go` already implements the core registration logic. This proposal wires it to the SQL layer.

## Goals / Non-Goals

- **Goals**: Execute ATTACH/DETACH/USE statements, cross-database table resolution, CREATE/DROP DATABASE
- **Non-Goals**: Remote database attachment (network), attach from different database engines (SQLite, PostgreSQL), concurrent cross-database transactions (single-transaction-per-database model)

## Decisions

- **Default alias**: When no AS clause is given, use the filename without extension (matching DuckDB behavior)
- **Path resolution**: Relative paths resolve relative to the directory of the primary database file
- **Read-only mode**: ATTACH with `(READ_ONLY)` option prevents writes to the attached database
- **Name collision**: Error if attaching with an alias that already exists (no implicit overwrite)

## Risks / Trade-offs

- **Risk**: Cross-database joins may have complex transaction semantics → Mitigation: Start with single-database-per-statement semantics; cross-database joins read from attached database's latest committed state
- **Risk**: File locking between primary and attached databases → Mitigation: Use the existing WAL/locking infrastructure per database file
- **Risk**: Memory overhead of multiple open catalogs → Mitigation: Lazy catalog loading; only load metadata when tables are first referenced

## Open Questions

- Should we support `ATTACH ':memory:' AS temp` for temporary in-memory attached databases? **Recommendation**: Yes, it's a common DuckDB pattern.
- Should cross-database transactions span multiple databases atomically? **Recommendation**: Not in initial implementation. Each database has independent transactions.
