# PostgreSQL Compatibility Mode - Implementation Specification

## Executive Summary

This document is the consolidated implementation specification for adding PostgreSQL compatibility mode to dukdb-go. PostgreSQL compatibility enables dukdb-go to accept connections from PostgreSQL clients (psql, pgx, JDBC, ORMs) over the PostgreSQL wire protocol, making it a drop-in replacement for PostgreSQL in many scenarios.

### Goals

1. **Wire Protocol Compatibility**: Accept PostgreSQL client connections using the `jeroenrinzema/psql-wire` library
2. **Type System Compatibility**: Map PostgreSQL types to DuckDB types bidirectionally
3. **Function Compatibility**: Provide PostgreSQL function name aliases and system functions
4. **Catalog Compatibility**: Implement pg_catalog views for tool/ORM introspection
5. **Zero CGO**: Maintain pure Go implementation with no C dependencies

### Non-Goals

1. Full PostgreSQL behavioral compatibility (e.g., exact MVCC semantics)
2. PostgreSQL extensions or custom types beyond basic mapping
3. Replication protocol support
4. PostgreSQL-specific storage format

---

## Package Structure

All PostgreSQL compatibility code lives under `internal/postgres/`:

```
internal/postgres/
    compat.go                   # Public API: StartServer(), Config

    server/
        server.go               # PostgresServer main implementation
        config.go               # ServerConfig, AuthConfig, TLSConfig
        handler.go              # Query handler (simple + extended protocol)
        session.go              # Session struct and SessionManager
        prepared.go             # PreparedStatement and Portal management
        transaction.go          # Transaction state machine
        auth.go                 # Authentication handlers (trust, md5, scram)
        tls.go                  # TLS configuration helpers
        params.go               # Server parameter management
        cancel.go               # Query cancellation handling

    wire/
        result_writer.go        # ResultWriter wrapping psql-wire DataWriter
        field_desc.go           # FieldDescription builder for RowDescription
        error.go                # PostgreSQL error code mapping (SQLSTATE)
        codec.go                # Value encoding/decoding (text/binary format)
        messages.go             # Helper functions for sending protocol messages

    types/
        oids.go                 # PostgreSQL OID constants
        mapper.go               # TypeMapper interface and implementation
        aliases.go              # PostgreSQL type name -> DuckDB type aliases
        convert.go              # Value conversion between formats
        serial.go               # Serial type (smallserial, serial, bigserial) handling

    functions/
        registry.go             # FunctionAliasRegistry
        aliases_direct.go       # Direct function name mappings
        aliases_transform.go    # Functions requiring argument transformation
        pg_typeof.go            # pg_typeof() implementation
        pg_current.go           # current_schema(), current_schemas()
        pg_privilege.go         # has_*_privilege() functions (return true)
        pg_catalog_funcs.go     # pg_get_*() catalog functions
        pg_size.go              # pg_*_size() functions
        pg_backend.go           # pg_backend_pid(), inet_*() functions
        pg_settings.go          # current_setting(), set_config()

    catalog/
        pg_catalog.go           # pg_catalog schema views
        pg_class.go             # pg_class view implementation
        pg_attribute.go         # pg_attribute view implementation
        pg_type.go              # pg_type view implementation
        pg_namespace.go         # pg_namespace view implementation
        pg_index.go             # pg_index view implementation
        pg_constraint.go        # pg_constraint view implementation
        information_schema.go   # information_schema views
        virtual_table.go        # Virtual table infrastructure for catalog views
```

---

## Dependencies

### Required Go Module

Add to `go.mod`:

```go
require (
    github.com/jeroenrinzema/psql-wire v0.12.0  // PostgreSQL wire protocol server
)
```

The `psql-wire` library provides:
- High-level PostgreSQL wire protocol server
- Simple query protocol support
- Extended query protocol (prepared statements)
- Authentication strategies (trust, MD5, SCRAM-SHA-256)
- TLS support
- Built on `jackc/pgx/v5/pgproto3` for protocol primitives

### Transitive Dependencies

`psql-wire` brings in:
- `github.com/jackc/pgx/v5` - PostgreSQL protocol primitives
- No CGO dependencies (pure Go)

---

## Integration Points

### 1. Engine Integration

The PostgreSQL server creates BackendConn instances for each client session:

```go
// internal/postgres/server/session.go

type Session struct {
    ID               uint64
    BackendConn      dukdb.BackendConn  // Engine connection
    PreparedStmts    map[string]*PreparedStatement
    Portals          map[string]*Portal
    TransactionState TransactionState
    // ...
}
```

Integration with existing engine:
- Use `engine.NewEngine()` as the shared backend
- Each wire session gets its own `BackendConn` via `engine.Open()`
- Transactions map 1:1 to engine transaction support
- Prepared statements use `BackendConn.Prepare()`

### 2. Parser Integration

PostgreSQL type aliases integrate with the parser:

```go
// internal/parser/types.go (modification)

// When PostgresCompatMode is enabled, resolve type aliases
func (p *Parser) resolveTypeName(name string) (Type, error) {
    if p.pgCompatMode {
        if mapping := postgres.TypeMapper.PostgreSQLToDuckDB(name); mapping != nil {
            return mapping.DuckDBType, nil
        }
    }
    // Fall through to normal type resolution
}
```

### 3. Binder Integration

Function aliases integrate with the binder:

```go
// internal/binder/bind_expr.go (modification)

func (b *Binder) bindFunctionCall(f *FunctionCall) (BoundExpr, error) {
    funcName := f.Name

    // Check PostgreSQL function aliases
    if b.pgCompatMode {
        if alias := b.aliasRegistry.Resolve(funcName); alias != nil {
            return b.bindAliasedFunction(f, alias)
        }
    }

    // Normal function binding...
}
```

### 4. Catalog Integration

Virtual tables for pg_catalog integrate with the catalog:

```go
// internal/catalog/catalog.go (modification)

func (c *Catalog) GetTable(schema, name string) (Table, error) {
    // Check for virtual pg_catalog tables
    if schema == "pg_catalog" {
        if vt := postgres.GetVirtualTable(name); vt != nil {
            return vt, nil
        }
    }
    // Normal table lookup...
}
```

---

## Configuration

### Server Configuration

```go
// internal/postgres/compat.go

// Config holds configuration for PostgreSQL compatibility mode.
type Config struct {
    // ListenAddr is the address to listen on (e.g., ":5432").
    ListenAddr string

    // DatabasePath is the path to the dukdb database.
    DatabasePath string

    // DatabaseConfig is optional dukdb configuration.
    DatabaseConfig *dukdb.Config

    // Auth configures authentication.
    Auth AuthConfig

    // TLS configures TLS (nil for no TLS).
    TLS *TLSConfig

    // MaxConnections limits concurrent connections (0 = unlimited).
    MaxConnections int

    // Logger for server events.
    Logger *slog.Logger
}

// AuthConfig configures authentication.
type AuthConfig struct {
    // Method: "trust", "password", "md5", "scram-sha-256"
    Method string

    // Users maps username to password (for simple auth).
    Users map[string]string

    // PasswordCallback for custom authentication.
    PasswordCallback func(user, password string) bool
}

// TLSConfig configures TLS.
type TLSConfig struct {
    CertFile   string
    KeyFile    string
    ClientAuth string // "none", "request", "require", "verify"
    CAFile     string
}
```

### Enabling PostgreSQL Mode

```go
// Example usage

import "github.com/dukdb/dukdb-go/internal/postgres"

func main() {
    config := &postgres.Config{
        ListenAddr:   ":5432",
        DatabasePath: "/path/to/db.dukdb",
        Auth: postgres.AuthConfig{
            Method: "trust",  // No password for local dev
        },
    }

    server, err := postgres.StartServer(config)
    if err != nil {
        log.Fatal(err)
    }
    defer server.Shutdown(context.Background())

    // Server runs in background goroutine
    // Can also use native driver simultaneously
    db, _ := sql.Open("dukdb", "/path/to/db.dukdb")
    // Both access the same database
}
```

---

## Implementation Phases

### Phase 1: Core Infrastructure (Tasks 2.1, 5.1)

**Objective**: Establish foundational types and server skeleton.

**Files to Create**:
- `internal/postgres/types/oids.go` - PostgreSQL OID constants
- `internal/postgres/types/mapper.go` - TypeMapper interface
- `internal/postgres/types/aliases.go` - Type alias map
- `internal/postgres/server/server.go` - Server skeleton
- `internal/postgres/server/config.go` - Configuration structs
- `internal/postgres/compat.go` - Public API

**Key Interfaces**:

```go
// TypeMapper provides bidirectional type mapping.
type TypeMapper interface {
    // PostgreSQLToDuckDB resolves PostgreSQL type name to DuckDB type.
    PostgreSQLToDuckDB(pgType string) (*TypeMapping, error)

    // DuckDBToPostgresOID returns PostgreSQL OID for DuckDB type.
    DuckDBToPostgresOID(duckType dukdb.Type) uint32

    // GetTypeSize returns type size for wire protocol.
    GetTypeSize(oid uint32) int16
}

// TypeMapping holds mapping details.
type TypeMapping struct {
    DuckDBType   dukdb.Type
    PostgresOID  uint32
    IsSerial     bool   // Auto-increment type
    HasModifiers bool   // Accepts (n) or (p,s)
}
```

**Deliverables**:
- OID constants for all PostgreSQL types
- Type alias map (serial, text, bytea, etc.)
- Basic server that accepts connections
- Configuration validation

**Success Criteria**:
- `psql -h localhost -p 5432` connects (may not execute queries yet)
- Type mapping unit tests pass

---

### Phase 2: Simple Query Protocol (Tasks 5.2, 5.3)

**Objective**: Handle basic query execution over wire protocol.

**Files to Create**:
- `internal/postgres/server/handler.go` - Query handler
- `internal/postgres/server/session.go` - Session management
- `internal/postgres/server/auth.go` - Trust authentication
- `internal/postgres/wire/result_writer.go` - Result formatting
- `internal/postgres/wire/field_desc.go` - FieldDescription builder
- `internal/postgres/wire/error.go` - Error code mapping

**Implementation**:

```go
// handleSimpleQuery processes Query messages.
func (s *Server) handleSimpleQuery(ctx context.Context, query string) error {
    session := SessionFromContext(ctx)

    // Execute against backend
    results, columns, err := session.BackendConn.Query(ctx, query, nil)
    if err != nil {
        return s.sendError(ctx, mapError(err))
    }

    // Build and send RowDescription
    fields := buildFieldDescriptions(columns, results)
    if err := s.sendRowDescription(ctx, fields); err != nil {
        return err
    }

    // Send DataRow messages
    for _, row := range results {
        if err := s.sendDataRow(ctx, row, columns); err != nil {
            return err
        }
    }

    // Send CommandComplete
    return s.sendCommandComplete(ctx, "SELECT", len(results))
}
```

**Deliverables**:
- Simple query protocol working
- SELECT/INSERT/UPDATE/DELETE via psql
- Proper error responses with SQLSTATE codes
- Connection cleanup on disconnect

**Success Criteria**:
- `psql -c "SELECT 1, 'hello'"` returns results
- `psql -c "CREATE TABLE t(id INT); INSERT INTO t VALUES(1); SELECT * FROM t"` works
- Error messages include PostgreSQL error codes

---

### Phase 3: Type Mapping and Conversion (Tasks 2.2, 2.3, 2.4)

**Objective**: Complete bidirectional type mapping.

**Files to Create**:
- `internal/postgres/types/convert.go` - Value conversion
- `internal/postgres/types/serial.go` - Serial type expansion
- `internal/postgres/wire/codec.go` - Wire format encoding

**Type Mapping Table**:

| PostgreSQL | OID | DuckDB | Notes |
|------------|-----|--------|-------|
| boolean | 16 | BOOLEAN | |
| smallint | 21 | SMALLINT | |
| integer | 23 | INTEGER | |
| bigint | 20 | BIGINT | |
| real | 700 | FLOAT | |
| double precision | 701 | DOUBLE | |
| numeric | 1700 | DECIMAL | |
| text | 25 | VARCHAR | |
| varchar | 1043 | VARCHAR | |
| bytea | 17 | BLOB | |
| date | 1082 | DATE | |
| timestamp | 1114 | TIMESTAMP | |
| timestamptz | 1184 | TIMESTAMPTZ | |
| uuid | 2950 | UUID | |
| json/jsonb | 114/3802 | JSON | |
| serial | 23 | INTEGER | + sequence |
| bigserial | 20 | BIGINT | + sequence |

**Serial Type Handling**:

```sql
-- Input:
CREATE TABLE users (id serial PRIMARY KEY);

-- Transforms to:
CREATE SEQUENCE users_id_seq;
CREATE TABLE users (
    id INTEGER NOT NULL DEFAULT nextval('users_id_seq'),
    PRIMARY KEY (id)
);
```

**Deliverables**:
- Complete type mapping both directions
- Text format encoding for all types
- Serial type expansion in parser
- Bytea hex encoding (`\xDEADBEEF`)
- Type conversion tests

**Success Criteria**:
- All DuckDB types map to PostgreSQL OIDs
- psql displays correct type names in `\d` output
- Serial columns auto-increment correctly

---

### Phase 4: Function Aliases (Tasks 3.1, 3.2, 3.3)

**Objective**: PostgreSQL function names resolve to DuckDB equivalents.

**Files to Create**:
- `internal/postgres/functions/registry.go` - Alias registry
- `internal/postgres/functions/aliases_direct.go` - Direct mappings
- `internal/postgres/functions/aliases_transform.go` - Transformations
- `internal/postgres/functions/pg_typeof.go` - pg_typeof
- `internal/postgres/functions/pg_current.go` - current_schema
- `internal/postgres/functions/pg_privilege.go` - has_*_privilege

**Function Categories**:

1. **Direct Aliases** (same semantics):
   - `now()` -> `current_timestamp`
   - `array_agg()` -> `list()`
   - String functions (already compatible)

2. **Transformed Functions** (different semantics):
   - `generate_series(1, 5)` -> `range(1, 6)` (inclusive vs exclusive)
   - `to_char(ts, 'YYYY-MM-DD')` -> `strftime('%Y-%m-%d', ts)` (format + arg order)

3. **System Functions** (custom implementation):
   - `pg_typeof(x)` - Return PostgreSQL type name
   - `current_schema()` - Return 'main'
   - `has_table_privilege(...)` - Return true (no privilege system)

**Registry Design**:

```go
type FunctionAlias struct {
    PostgreSQLName string
    DuckDBName     string
    Category       AliasCategory
    Transformer    func(args []BoundExpr) (string, []BoundExpr, error)
    MinArgs, MaxArgs int
}

type FunctionAliasRegistry struct {
    aliases map[string]*FunctionAlias
}

func (r *FunctionAliasRegistry) Resolve(name string) *FunctionAlias
func (r *FunctionAliasRegistry) Register(alias *FunctionAlias)
```

**Deliverables**:
- Function alias registry
- Direct aliases for common functions
- `generate_series` transformation
- `to_char`/`to_date` format conversion
- System functions (pg_typeof, current_schema, etc.)
- Binder integration

**Success Criteria**:
- `SELECT now()` returns current timestamp
- `SELECT generate_series(1, 5)` returns 1,2,3,4,5
- `SELECT pg_typeof(123)` returns 'integer'
- `SELECT has_table_privilege('users', 'SELECT')` returns true

---

### Phase 5: Extended Query Protocol (Tasks 5.4, 5.5, 5.6)

**Objective**: Support prepared statements for driver compatibility.

**Files to Create**:
- `internal/postgres/server/prepared.go` - Prepared statement management
- `internal/postgres/server/transaction.go` - Transaction state machine

**Protocol Messages**:

```
Parse     -> ParseComplete
Bind      -> BindComplete
Describe  -> RowDescription / ParameterDescription
Execute   -> DataRow* + CommandComplete
Sync      -> ReadyForQuery
```

**Session State**:

```go
type Session struct {
    PreparedStmts map[string]*PreparedStatement
    Portals       map[string]*Portal
    TransactionState TransactionState // Idle, InProgress, Failed
}

type PreparedStatement struct {
    Name        string
    Query       string
    BackendStmt dukdb.BackendStmt
    ParamOIDs   []uint32
}

type Portal struct {
    Name       string
    Statement  *PreparedStatement
    Parameters []any
    Suspended  bool
}
```

**Transaction State Machine**:

```
          BEGIN
[Idle] -----------> [InProgress]
  ^                      |
  |    COMMIT            | (error)
  +----------------------+
  |                      |
  |    ROLLBACK          v
  +------------------ [Failed]
```

**Deliverables**:
- Parse/Bind/Execute handlers
- Prepared statement lifecycle
- Portal management
- Transaction state tracking
- ReadyForQuery with correct status

**Success Criteria**:
- pgx driver connects and queries
- Prepared statements work
- Transactions commit/rollback correctly
- Failed transactions reject commands until ROLLBACK

---

### Phase 6: pg_catalog Views (Tasks 6.1, 6.2, 6.3)

**Objective**: Implement catalog views for tool compatibility.

**Files to Create**:
- `internal/postgres/catalog/pg_catalog.go` - Schema registration
- `internal/postgres/catalog/pg_class.go` - Tables/indexes/views
- `internal/postgres/catalog/pg_attribute.go` - Columns
- `internal/postgres/catalog/pg_type.go` - Types
- `internal/postgres/catalog/pg_namespace.go` - Schemas
- `internal/postgres/catalog/information_schema.go` - Standard views

**Required Views**:

| View | Purpose | Used By |
|------|---------|---------|
| `pg_catalog.pg_class` | Tables, indexes, views | psql `\d`, ORMs |
| `pg_catalog.pg_attribute` | Columns | psql `\d`, ORMs |
| `pg_catalog.pg_type` | Types | Type introspection |
| `pg_catalog.pg_namespace` | Schemas | Schema introspection |
| `pg_catalog.pg_index` | Index metadata | Index introspection |
| `pg_catalog.pg_constraint` | Constraints | FK/PK introspection |
| `information_schema.tables` | Standard tables view | JDBC, ORMs |
| `information_schema.columns` | Standard columns view | JDBC, ORMs |

**Virtual Table Infrastructure**:

```go
// VirtualTable provides catalog data without physical storage.
type VirtualTable interface {
    Table

    // Scan returns rows from the virtual table.
    Scan(ctx context.Context) ([]Row, error)
}

// pgClassTable implements pg_catalog.pg_class.
type pgClassTable struct {
    catalog *Catalog
}

func (t *pgClassTable) Scan(ctx context.Context) ([]Row, error) {
    rows := []Row{}

    // Add tables
    for _, table := range t.catalog.Tables() {
        rows = append(rows, Row{
            "oid":      generateOID(table),
            "relname":  table.Name,
            "relkind":  "r", // regular table
            // ...
        })
    }

    return rows, nil
}
```

**Deliverables**:
- Virtual table infrastructure
- pg_catalog views
- information_schema views
- OID generation for objects

**Success Criteria**:
- `psql \d` lists tables
- `psql \d tablename` shows columns
- GORM schema introspection works
- SQLAlchemy reflection works

---

### Phase 7: Integration Testing (Tasks 7.1, 7.2, 7.3, 7.4)

**Objective**: Validate compatibility with real clients.

**Test Categories**:

1. **psql Client**
   - Connection and authentication
   - Basic queries
   - `\d` commands
   - Transaction commands
   - Copy protocol (if supported)

2. **pgx Driver (Go)**
   - Connection pool
   - Prepared statements
   - Type scanning
   - Batch queries
   - Context cancellation

3. **ORM Testing**
   - GORM: Model definition, CRUD, migrations
   - SQLAlchemy: Engine creation, ORM queries
   - TypeORM: Entity definition, repository

4. **Edge Cases**
   - Large result sets (streaming)
   - Binary format requests
   - Concurrent connections
   - Connection drops
   - Invalid queries

**Test Files**:
```
internal/postgres/
    server/
        server_test.go          # Server unit tests
        session_test.go         # Session management tests
        handler_test.go         # Query handler tests

    integration_test.go         # Integration tests with real clients

tests/
    postgres/
        psql_test.go            # psql client tests
        pgx_test.go             # pgx driver tests
        gorm_test.go            # GORM compatibility tests
        sqlalchemy_test.py      # SQLAlchemy tests (run via go test)
```

**Deliverables**:
- Comprehensive test suite
- CI integration
- Performance benchmarks
- Compatibility matrix documentation

**Success Criteria**:
- psql connects and basic operations work
- pgx driver test suite passes
- GORM basic operations work
- No regressions in native driver tests

---

### Phase 8: Documentation (Tasks 3.4, 8.1, 8.2, 8.3, 8.4)

**Objective**: Document PostgreSQL compatibility mode.

**Documentation**:

1. **User Guide** (`docs/postgres-compat.md`)
   - Enabling PostgreSQL mode
   - Configuration options
   - Connecting with clients
   - Supported features
   - Limitations

2. **Type Reference** (`docs/postgres-types.md`)
   - Complete type mapping table
   - Serial type behavior
   - Array type handling
   - Type conversion notes

3. **Function Reference** (`docs/postgres-functions.md`)
   - Supported function aliases
   - System function behavior
   - Known differences from PostgreSQL

4. **Examples** (`examples/postgres/`)
   - Basic server setup
   - TLS configuration
   - Authentication setup
   - ORM integration examples

**Deliverables**:
- Complete documentation
- Example configurations
- Migration guide from PostgreSQL

---

## Interface Definitions

### Core Interfaces

```go
// Server is the PostgreSQL wire protocol server.
type Server interface {
    // Serve starts serving connections (blocking).
    Serve() error

    // Shutdown gracefully shuts down the server.
    Shutdown(ctx context.Context) error

    // Addr returns the server's listen address.
    Addr() net.Addr
}

// TypeMapper provides type mapping between PostgreSQL and DuckDB.
type TypeMapper interface {
    // PostgreSQLToDuckDB maps PostgreSQL type name to DuckDB type.
    PostgreSQLToDuckDB(pgType string) (*TypeMapping, error)

    // DuckDBToPostgresOID maps DuckDB type to PostgreSQL OID.
    DuckDBToPostgresOID(duckType dukdb.Type) uint32

    // GetTypeSize returns wire protocol type size.
    GetTypeSize(oid uint32) int16

    // IsArrayType checks if OID is an array type.
    IsArrayType(oid uint32) bool
}

// TypeConverter handles value conversion.
type TypeConverter interface {
    // EncodeText converts DuckDB value to PostgreSQL text format.
    EncodeText(value any, duckType dukdb.Type) ([]byte, error)

    // DecodeText converts PostgreSQL text to DuckDB value.
    DecodeText(data []byte, pgOID uint32) (any, error)
}

// FunctionAliasRegistry resolves PostgreSQL function names.
type FunctionAliasRegistry interface {
    // Resolve looks up a function alias.
    Resolve(name string) *FunctionAlias

    // Register adds a function alias.
    Register(alias *FunctionAlias)
}

// VirtualTable provides catalog data without storage.
type VirtualTable interface {
    // Schema returns the table schema.
    Schema() TableSchema

    // Scan returns all rows.
    Scan(ctx context.Context) ([]map[string]any, error)
}
```

---

## Testing Requirements

### Unit Tests

| Component | Test File | Coverage |
|-----------|-----------|----------|
| Type Mapping | `types/mapper_test.go` | All type mappings |
| Type Conversion | `types/convert_test.go` | All value formats |
| Function Registry | `functions/registry_test.go` | Alias resolution |
| Transformers | `functions/transform_test.go` | Argument transformation |
| Session Management | `server/session_test.go` | Lifecycle, cleanup |
| Transaction State | `server/transaction_test.go` | State machine |
| Error Mapping | `wire/error_test.go` | SQLSTATE codes |

### Integration Tests

| Test | Description | Client |
|------|-------------|--------|
| `TestPsqlConnect` | Basic connection | psql |
| `TestPsqlQuery` | SELECT queries | psql |
| `TestPsqlTransaction` | BEGIN/COMMIT/ROLLBACK | psql |
| `TestPsqlDescribe` | `\d` commands | psql |
| `TestPgxConnection` | Connection pool | pgx |
| `TestPgxPrepared` | Prepared statements | pgx |
| `TestPgxTypes` | Type scanning | pgx |
| `TestGormBasic` | CRUD operations | GORM |
| `TestGormMigration` | Schema migration | GORM |

### Performance Tests

| Test | Metric | Target |
|------|--------|--------|
| Connection throughput | conn/sec | > 1000 |
| Simple query latency | ms | < 1 |
| Prepared statement latency | ms | < 0.5 |
| Large result streaming | MB/sec | > 100 |

---

## Success Criteria

### Minimum Viable Product (MVP)

1. psql connects and executes queries
2. Simple SELECT/INSERT/UPDATE/DELETE work
3. Basic type mapping (integers, strings, timestamps)
4. `\d` shows tables and columns
5. Transactions work (BEGIN/COMMIT/ROLLBACK)

### Full Compatibility Target

1. pgx driver test suite passes
2. GORM basic operations work
3. All standard PostgreSQL types mapped
4. Function aliases work (now(), generate_series, etc.)
5. Prepared statements work
6. Concurrent connections handled
7. Proper error codes returned

### Known Limitations

1. No PostgreSQL-specific extensions
2. No custom type creation (CREATE TYPE)
3. No advanced constraint types (EXCLUDE)
4. No inheritance (INHERITS)
5. No table partitioning (PARTITION BY)
6. Privilege functions always return true
7. Some pg_catalog columns may be NULL or placeholder values

---

## Task Mapping

This specification supports the following tasks from `tasks.jsonc`:

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 2.1, 5.1 | Core infrastructure |
| 2 | 5.2, 5.3 | Simple query protocol |
| 3 | 2.2, 2.3, 2.4 | Type mapping |
| 4 | 3.1, 3.2, 3.3 | Function aliases |
| 5 | 5.4, 5.5, 5.6 | Extended query protocol |
| 6 | 6.1, 6.2, 6.3 | Catalog views |
| 7 | 7.1, 7.2, 7.3, 7.4 | Integration testing |
| 8 | 3.4, 8.1, 8.2, 8.3, 8.4 | Documentation |

---

## References

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [psql-wire Documentation](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire)
- [Wire Protocol Research](../wire-protocol/research.md)
- [Wire Protocol Spec](../wire-protocol/spec.md)
- [Type Mapping Spec](../type-mapping/spec.md)
- [Function Aliases Spec](../function-aliases/spec.md)
