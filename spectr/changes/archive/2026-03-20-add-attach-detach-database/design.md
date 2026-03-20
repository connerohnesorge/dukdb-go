## Implementation Details

### Core Data Structures

#### DatabaseManager

The `DatabaseManager` is the central registry that manages all attached databases for a connection. It lives in `internal/engine/database_manager.go`.

```go
// DatabaseManager maintains a registry of attached databases.
// Each connection gets its own DatabaseManager instance.
type DatabaseManager struct {
    mu        sync.RWMutex
    databases map[string]*AttachedDatabase // name -> database
    defaultDB string                       // current default database name
}

// AttachedDatabase represents a single attached database with its own
// catalog, storage, and WAL.
type AttachedDatabase struct {
    Name       string           // alias used to reference this database
    Path       string           // file path or ":memory:"
    Catalog    *catalog.Catalog // per-database catalog
    Storage    *storage.Storage // per-database storage
    WALWriter  *wal.Writer      // per-database WAL (nil for read-only)
    ReadOnly   bool             // true if attached as READ_ONLY
    IsDefault  bool             // true if this is the default (initial) database
    AccessMode AccessMode       // read_only, read_write
}

type AccessMode int

const (
    AccessModeAutomatic AccessMode = iota
    AccessModeReadOnly
    AccessModeReadWrite
)
```

#### AST Nodes

New parser AST nodes in `internal/parser/ast.go`:

```go
// AttachStmt represents an ATTACH [DATABASE] statement.
type AttachStmt struct {
    Path     string            // file path or ":memory:"
    Alias    string            // optional AS alias
    ReadOnly bool              // READ_ONLY option
    Options  map[string]string // additional options (type, etc.)
}

// DetachStmt represents a DETACH [DATABASE] statement.
type DetachStmt struct {
    Name     string // database name to detach
    IfExists bool   // IF EXISTS clause
}

// UseStmt represents a USE database[.schema] statement.
type UseStmt struct {
    Database string // database name
    Schema   string // optional schema name
}

// CreateDatabaseStmt represents a CREATE DATABASE statement.
type CreateDatabaseStmt struct {
    Name        string // database name
    IfNotExists bool
    Path        string            // optional file path
    Options     map[string]string // optional options
}

// DropDatabaseStmt represents a DROP DATABASE statement.
type DropDatabaseStmt struct {
    Name     string
    IfExists bool
    Cascade  bool
}
```

#### CatalogResolver Interface and Binder Integration

The binder currently only takes a `*catalog.Catalog` via `NewBinder(cat *catalog.Catalog)`. To support multi-database name resolution, we introduce a `CatalogResolver` interface that the binder uses to look up catalogs by name. This avoids coupling the binder directly to `DatabaseManager` (which lives in `internal/engine/`).

```go
// CatalogResolver is implemented by DatabaseManager and injected into the
// Binder. It lives in internal/binder/catalog_resolver.go so the binder
// package has no dependency on internal/engine.
type CatalogResolver interface {
    // ResolveCatalog returns the catalog for the given database name.
    // An empty name returns the default database's catalog.
    ResolveCatalog(name string) (*catalog.Catalog, error)

    // DefaultDatabaseName returns the current default database name.
    DefaultDatabaseName() string

    // IsDatabaseName reports whether the given identifier is a known
    // attached database name. Used for 2-part name disambiguation.
    IsDatabaseName(name string) bool
}
```

`NewBinder` gains an optional `CatalogResolver` parameter:

```go
// NewBinder creates a new Binder. The resolver parameter is optional; when
// nil, the binder operates in single-database mode using only the provided
// catalog (backward compatible with all existing call sites).
func NewBinder(cat *catalog.Catalog, resolver ...CatalogResolver) *Binder {
    var r CatalogResolver
    if len(resolver) > 0 {
        r = resolver[0]
    }
    return &Binder{
        catalog:  cat,
        resolver: r,
        scope:    newBindScope(nil),
    }
}
```

The `EngineConn.Execute()` call site passes the resolver:

```go
b := binder.NewBinder(c.engine.catalog, c.databaseManager)
```

`DatabaseManager` satisfies `CatalogResolver` by implementing all three methods.

#### 3-Part Name Resolution

The binder already has `TableRef.Catalog` field but it's unused. Implementation extends the binder's `bindTableRef` to resolve 3-part names.

The key challenge is **2-part name disambiguation**: when a user writes `foo.bar`, the binder must determine whether `foo` is a database name or a schema name. DuckDB resolves this as follows:

```go
// resolveQualifiedName resolves a potentially qualified name into the
// correct catalog, schema, and table. It implements DuckDB's resolution
// logic for 1-, 2-, and 3-part names.
func (b *Binder) resolveQualifiedName(
    catName, schemaName, tableName string,
) (*catalog.Catalog, string, string, error) {
    r := b.resolver // may be nil for single-database mode

    // --- 3-part name: database.schema.table ---
    if catName != "" && schemaName != "" {
        if r == nil {
            return nil, "", "", fmt.Errorf("3-part names require multi-database mode")
        }
        cat, err := r.ResolveCatalog(catName)
        if err != nil {
            return nil, "", "", fmt.Errorf("database %q not found", catName)
        }
        return cat, schemaName, tableName, nil
    }

    // --- 2-part name: X.table ---
    // DuckDB resolution order:
    //   1. If X is a known database name  -> resolve as database.default_schema.table
    //   2. If X is a schema in the default database -> resolve as default_db.X.table
    //   3. Error: neither database nor schema found
    if schemaName != "" && catName == "" {
        firstPart := schemaName // the "X" in X.table

        // Step 1: check if X is a database name
        if r != nil && r.IsDatabaseName(firstPart) {
            cat, err := r.ResolveCatalog(firstPart)
            if err != nil {
                return nil, "", "", err
            }
            return cat, "main", tableName, nil
        }

        // Step 2: treat X as a schema in the default database
        defaultCat := b.catalog
        if r != nil {
            resolved, err := r.ResolveCatalog("")
            if err == nil {
                defaultCat = resolved
            }
        }
        // Verify the schema exists in the default catalog
        if defaultCat.HasSchema(firstPart) {
            return defaultCat, firstPart, tableName, nil
        }

        // If no resolver, fall back to treating X as schema (original behavior)
        if r == nil {
            return b.catalog, firstPart, tableName, nil
        }

        return nil, "", "", fmt.Errorf("%q is neither a known database nor a schema in the default database", firstPart)
    }

    // --- 1-part name: table ---
    defaultCat := b.catalog
    defaultSchema := "main"
    if r != nil {
        resolved, err := r.ResolveCatalog("")
        if err == nil {
            defaultCat = resolved
        }
    }
    return defaultCat, defaultSchema, tableName, nil
}
```

### Engine Integration

The `Engine` struct gains a `DatabaseManager` field initialized on `Open()`:

```go
func (e *Engine) Open(path string, config *dukdb.Config) (dukdb.BackendConn, error) {
    // ... existing code ...

    conn := &Conn{
        engine:          e,
        databaseManager: NewDatabaseManager(),
        // ...
    }

    // Register the primary database as "main" (or from config)
    conn.databaseManager.Register("main", &AttachedDatabase{
        Name:      "main",
        Path:      path,
        Catalog:   e.catalog,
        Storage:   e.storage,
        WALWriter: e.walWriter,
        IsDefault: true,
    })

    return conn, nil
}
```

### Transaction Coordination

For cross-database transactions, each database maintains its own transaction state. The connection's transaction manager coordinates:

```go
// BeginMultiDB starts transactions on all databases that will be accessed.
// For simplicity in the initial implementation, only the default database
// participates in the transaction. Cross-database writes require explicit
// savepoint management.
func (tm *TransactionManager) BeginMultiDB(
    dbs []*AttachedDatabase,
) (*MultiDBTransaction, error) {
    // Phase 1: Begin transactions on all databases
    // Phase 2: If any fails, rollback all that succeeded
    // Phase 3: Return composite transaction handle
}
```

**Initial implementation simplification**: Cross-database transactions only support reading from non-default databases within a transaction. Write transactions are limited to the default database. This matches DuckDB's behavior where cross-database writes have limited transactional guarantees.

### Execution Flow

ATTACH, DETACH, and USE are **connection-level operations**, not plan-based queries. They modify the connection's `DatabaseManager` state directly, just like BEGIN/COMMIT/ROLLBACK are handled at the connection level in `EngineConn.Execute()`. They are intercepted after parsing but **before** the binder/planner/executor pipeline.

This matches the existing pattern in `internal/engine/conn.go` where transaction statements are handled in a `switch` block immediately after `parser.Parse()`:

```go
// In EngineConn.Execute(), add cases alongside existing transaction handling:
func (c *EngineConn) Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
    // ... existing locking, closed check, statement time ...

    stmt, err := parser.Parse(query)
    if err != nil {
        return 0, err
    }

    // Handle transaction statements at connection level
    switch s := stmt.(type) {
    case *parser.BeginStmt:
        return c.handleBeginWithIsolation(s)
    case *parser.CommitStmt:
        return c.handleCommit()
    // ... existing cases ...

    // Handle database management statements at connection level
    // (same pattern as transaction statements — they modify connection state,
    // not table data, so they bypass binder/planner/executor)
    case *parser.AttachStmt:
        return c.handleAttach(s)
    case *parser.DetachStmt:
        return c.handleDetach(s)
    case *parser.UseStmt:
        return c.handleUse(s)
    case *parser.CreateDatabaseStmt:
        return c.handleCreateDatabase(s)
    case *parser.DropDatabaseStmt:
        return c.handleDropDatabase(s)
    }

    // ... existing binder/planner/executor pipeline ...
}
```

The `handleAttach()` method on `EngineConn`:

```go
func (c *EngineConn) handleAttach(s *parser.AttachStmt) (int64, error) {
    // Reject ATTACH if there is a pending transaction on the connection
    if c.inTxn {
        return 0, fmt.Errorf("cannot ATTACH database inside a transaction")
    }
    return 0, c.databaseManager.AttachDatabase(s.Path, s.Alias, s.ReadOnly)
}

func (c *EngineConn) handleDetach(s *parser.DetachStmt) (int64, error) {
    // Reject DETACH if there is a pending transaction
    if c.inTxn {
        return 0, fmt.Errorf("cannot DETACH database inside a transaction")
    }
    return 0, c.databaseManager.DetachDatabase(s.Name, s.IfExists)
}

func (c *EngineConn) handleUse(s *parser.UseStmt) (int64, error) {
    return 0, c.databaseManager.SetDefaultDatabase(s.Database)
}
```

**Why connection-level, not executor-level:**
- ATTACH/DETACH modify the connection's `DatabaseManager`, which the binder needs access to. Running them through the binder would create a circular dependency (binder needs DatabaseManager, but ATTACH creates entries in it).
- Like BEGIN/COMMIT, these are session-state operations, not data operations.
- They don't produce query plans or return result sets.

4. **Cross-database query**: Binder resolves 3-part names using `CatalogResolver` (backed by `DatabaseManager`), planner creates scan nodes that reference the correct catalog/storage, executor reads from the appropriate storage

### File and Directory Structure

```
internal/engine/
├── database_manager.go      # DatabaseManager implementation
├── database_manager_test.go # Unit tests
├── attach.go                # ATTACH/DETACH execution logic
├── attach_test.go           # Integration tests
```

### ATTACH Options

DuckDB v1.4.3 supports these ATTACH options:

| Option | Description | Default |
|--------|-------------|---------|
| `READ_ONLY` | Attach as read-only | false |
| `TYPE` | Database type (duckdb, sqlite, etc.) | duckdb |
| `BLOCK_SIZE` | Storage block size | 262144 |

Initial implementation supports `READ_ONLY` only. `TYPE` is always `duckdb` (no SQLite scanner). `BLOCK_SIZE` deferred to future work.

## Context

- The `TableRef.Catalog` field already exists in the parser AST but is unused
- `STATEMENT_TYPE_ATTACH` and `STATEMENT_TYPE_DETACH` constants already exist in `backend.go`
- The statement type detector already recognizes ATTACH/DETACH keywords in `stmt_detector.go`
- DuckDB's `PRAGMA database_list` and `duckdb_databases()` system function already exist in specs but return only the default database
- The `internal/storage/duckdb/` package can read/write DuckDB files, enabling file-based ATTACH

## Goals / Non-Goals

**Goals:**
- Parse and execute ATTACH, DETACH, USE, CREATE DATABASE, DROP DATABASE
- Support 3-part qualified names (database.schema.table) in SELECT, INSERT, UPDATE, DELETE
- Support cross-database reads in queries (JOIN across databases)
- Support in-memory and file-based attached databases
- Update system functions to reflect attached databases

**Non-Goals:**
- Remote database attachment (S3, HTTP) — separate proposal
- SQLite scanner (TYPE sqlite) — out of scope
- Full cross-database write transactions — initial implementation limits writes to default database
- PostgreSQL scanner or other external database types

## Decisions

- **Per-connection DatabaseManager**: Each connection gets its own manager. Attached databases are not shared across connections. This matches DuckDB behavior.
- **Reuse existing Engine infrastructure**: Each attached database gets its own Catalog + Storage + WAL. This keeps the implementation simple and leverages existing persistence code.
- **CatalogResolver interface for binder**: Rather than giving the binder a direct `DatabaseManager` reference (which would create an import cycle with `internal/engine`), we define a `CatalogResolver` interface in the binder package. `DatabaseManager` implements this interface. This keeps the binder decoupled from engine internals.
- **ATTACH/DETACH/USE at connection level**: These are session-state operations handled in `EngineConn.Execute()` before the binder/planner/executor pipeline, matching the pattern used for BEGIN/COMMIT/ROLLBACK. They do not produce query plans.
- **3-part name resolution in binder**: Extend existing `bindTableRef` rather than adding a new resolution layer. The `TableRef.Catalog` field is already there. 2-part names use DuckDB's disambiguation: check database names first, then schema names.
- **Simplified transaction model**: Cross-database reads only in transactions initially. Writes to non-default databases return an explicit error.
- **ATTACH/DETACH rejected inside transactions**: To avoid complex partial-rollback scenarios, ATTACH and DETACH are rejected when a transaction is active.

## Risks / Trade-offs

- **Memory overhead**: Each attached database creates its own Catalog/Storage/WAL instances. For many attached databases, this could use significant memory.
  → Mitigation: Lazy initialization of storage; only allocate when first accessed.
- **Transaction complexity**: Full distributed transactions across databases are complex.
  → Mitigation: Initial implementation limits cross-database transactions to reads. Document limitation clearly.
- **Name resolution performance**: 3-part name resolution adds lookup overhead.
  → Mitigation: Cache resolved references per-statement. Most queries use 1-2 databases.

## Open Questions

- Should `ATTACH ':memory:' AS db2` create a fully isolated in-memory database or share the connection's memory pool?
  → **Resolved**: Fully isolated (separate Catalog/Storage), matching DuckDB behavior.
- Should detaching a database auto-commit pending transactions?
  → **Resolved**: No. DETACH is rejected outright if a transaction is active. This is simpler and safer than auto-commit. The user must explicitly COMMIT or ROLLBACK before detaching. This avoids surprising implicit commits.
- Should the same file be attachable under multiple aliases?
  → **Resolved**: No. Attaching a file that is already attached (under any alias) returns an error. This prevents data corruption from concurrent access to the same file through different database handles.
