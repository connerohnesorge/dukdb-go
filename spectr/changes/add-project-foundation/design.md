## Context

dukdb-go aims to be a drop-in replacement for the existing duckdb-go driver but without any cgo dependencies. This enables use in environments where cgo is unavailable or undesirable (TinyGo, WebAssembly, cross-compilation scenarios).

Since DuckDB is an embedded database without a native wire protocol, the pure Go implementation must use an alternative approach for database operations. This foundation establishes the interfaces that will be implemented by concrete backends in subsequent changes.

## Goals / Non-Goals

**Goals:**
- Establish clean module structure compatible with Go 1.21+
- Define interfaces matching duckdb-go's public API exactly
- Create extensible architecture for multiple backend implementations
- Ensure zero cgo contamination through build constraints
- Provide clear error types matching duckdb-go patterns

**Non-Goals:**
- Implement actual database functionality (covered in subsequent changes)
- Create the specific backend (subprocess, WASM, etc.)
- Performance optimization (premature at this stage)

## Decisions

### Decision 1: Package Layout

**What:** Use a flat package structure with all public API in the root `dukdb` package.

**Why:** Matches duckdb-go's API surface and simplifies import paths for users. Internal implementation details use `internal/` packages.

**Alternatives considered:**
- Nested packages (e.g., `dukdb/driver`, `dukdb/types`) - rejected as it complicates the drop-in replacement goal

### Decision 2: Backend Interface Pattern

**What:** Define a `Backend` interface that abstracts the database communication mechanism.

```go
// Backend abstracts database communication for different implementations.
type Backend interface {
    // Open initializes a connection to the database at the given path.
    // For in-memory databases, path is empty string.
    // Returns a BackendConn that can execute queries.
    Open(path string, config *Config) (BackendConn, error)

    // Close shuts down the backend and releases all resources.
    Close() error
}

// BackendConn represents a single database connection.
type BackendConn interface {
    // Execute runs a SQL statement and returns affected row count.
    Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error)

    // Query runs a SQL query and returns results as JSON-compatible data.
    Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error)

    // Prepare creates a prepared statement.
    Prepare(ctx context.Context, query string) (BackendStmt, error)

    // Close releases the connection resources.
    Close() error

    // Ping verifies the connection is still alive.
    Ping(ctx context.Context) error
}

// BackendStmt represents a prepared statement.
type BackendStmt interface {
    // Execute runs the prepared statement with given arguments.
    Execute(ctx context.Context, args []driver.NamedValue) (int64, error)

    // Query runs the prepared statement as a query.
    Query(ctx context.Context, args []driver.NamedValue) ([]map[string]any, []string, error)

    // Close releases the prepared statement.
    Close() error

    // NumInput returns the number of placeholder parameters.
    NumInput() int
}
```

**Why:** Allows swapping implementations (subprocess, WASM, future native) without changing the public API.

### Decision 3: Build Constraints

**What:** Use `//go:build !cgo` build tag on all source files to prevent compilation with CGO_ENABLED=1.

**Why:** Enforces the zero-cgo constraint at build time. When CGO_ENABLED=1, the build fails with clear indication that this package requires `CGO_ENABLED=0`.

**Error behavior:** When built with `CGO_ENABLED=1`, Go will skip all source files and fail with "no Go files" or similar, indicating build constraint failure.

### Decision 4: Go Version Requirement

**What:** Require Go 1.21 or later.

**Why:**
- Go 1.21 introduced `reflect.TypeFor[T]()` used for type reflection
- Go 1.21 has improved generics support needed for `Composite[T]` type
- Go 1.21 is the oldest version still receiving security updates as of 2024

### Decision 5: Module Path and Coexistence

**What:** This package uses module path `github.com/connerohnesorge/dukdb-go`.

**Why:**
- Distinct from `github.com/marcboeker/go-duckdb` (original duckdb-go)
- Distinct from `github.com/duckdb/duckdb-go` (official duckdb-go)
- Users can import both packages if needed during migration

**Driver registration:**
- Driver name: `"dukdb"` (not `"duckdb"`)
- Auto-registers on import via `init()` function
- Usage: `sql.Open("dukdb", ":memory:")`

### Decision 6: Error Type Design

**What:** Define error types matching duckdb-go's actual implementation exactly:

```go
// Error represents a DuckDB error with type and message.
type Error struct {
    Type ErrorType
    Msg  string
}

// Error returns just the message (matches duckdb-go behavior)
func (e *Error) Error() string {
    return e.Msg
}

// Is compares by message content (matches duckdb-go behavior)
func (e *Error) Is(err error) bool {
    if other, ok := err.(*Error); ok {
        return other.Msg == e.Msg
    }
    return false
}

// ErrorType categorizes DuckDB errors (values match duckdb-go exactly).
type ErrorType int

const (
    ErrorTypeInvalid              ErrorType = 0
    ErrorTypeOutOfRange           ErrorType = 1
    ErrorTypeConversion           ErrorType = 2
    ErrorTypeUnknownType          ErrorType = 3
    ErrorTypeDecimal              ErrorType = 4
    ErrorTypeMismatchType         ErrorType = 5
    ErrorTypeDivideByZero         ErrorType = 6
    ErrorTypeObjectSize           ErrorType = 7
    ErrorTypeInvalidType          ErrorType = 8
    ErrorTypeSerialization        ErrorType = 9
    ErrorTypeTransaction          ErrorType = 10
    ErrorTypeNotImplemented       ErrorType = 11
    ErrorTypeExpression           ErrorType = 12
    ErrorTypeCatalog              ErrorType = 13
    ErrorTypeParser               ErrorType = 14
    ErrorTypePlanner              ErrorType = 15
    ErrorTypeScheduler            ErrorType = 16
    ErrorTypeExecutor             ErrorType = 17
    ErrorTypeConstraint           ErrorType = 18
    ErrorTypeIndex                ErrorType = 19
    ErrorTypeStat                 ErrorType = 20
    ErrorTypeConnection           ErrorType = 21
    ErrorTypeSyntax               ErrorType = 22
    ErrorTypeSettings             ErrorType = 23
    ErrorTypeBinder               ErrorType = 24
    ErrorTypeNetwork              ErrorType = 25
    ErrorTypeOptimizer            ErrorType = 26
    ErrorTypeNullPointer          ErrorType = 27
    ErrorTypeIO                   ErrorType = 28
    ErrorTypeInterrupt            ErrorType = 29
    ErrorTypeFatal                ErrorType = 30
    ErrorTypeInternal             ErrorType = 31
    ErrorTypeInvalidInput         ErrorType = 32
    ErrorTypeOutOfMemory          ErrorType = 33
    ErrorTypePermission           ErrorType = 34
    ErrorTypeParameterNotResolved ErrorType = 35
    ErrorTypeParameterNotAllowed  ErrorType = 36
    ErrorTypeDependency           ErrorType = 37
    ErrorTypeHTTP                 ErrorType = 38
    ErrorTypeMissingExtension     ErrorType = 39
    ErrorTypeAutoLoad             ErrorType = 40
    ErrorTypeSequence             ErrorType = 41
    ErrorTypeInvalidConfiguration ErrorType = 42
)

// errorPrefixMap maps DuckDB error prefixes to ErrorType
var errorPrefixMap = map[string]ErrorType{
    "Invalid Error":                ErrorTypeInvalid,
    "Out of Range Error":           ErrorTypeOutOfRange,
    "Conversion Error":             ErrorTypeConversion,
    "Error":                        ErrorTypeUnknownType,
    "Catalog Error":                ErrorTypeCatalog,
    "Parser Error":                 ErrorTypeParser,
    "Binder Error":                 ErrorTypeBinder,
    // ... (full map matching duckdb-go/errors.go lines 208-251)
}

// getDuckDBError parses a DuckDB error message string and extracts the error type.
// It looks for known error prefixes (e.g., "Parser Error:", "Catalog Error:") in
// the error message and maps them to the corresponding ErrorType.
// If no known prefix is found, returns ErrorTypeInvalid.
// This function is used to convert raw error strings from the engine into
// structured Error values that can be inspected programmatically.
func getDuckDBError(errMsg string) error {
    errType := ErrorTypeInvalid
    if idx := strings.Index(errMsg, ": "); idx != -1 {
        if typ, ok := errorPrefixMap[errMsg[:idx]]; ok {
            errType = typ
        }
    }
    return &Error{Type: errType, Msg: errMsg}
}
```

**Why:** Must match duckdb-go's exact behavior for `errors.Is` compatibility.

### Decision 7: Config Field Details

**What:** Config struct with typed fields and documented valid values:

```go
type Config struct {
    AccessMode   string // "read_only", "read_write" (default: "read_write")
    Threads      int    // Worker threads (0 = auto, 1-128 = explicit)
    MaxMemory    string // Memory limit: "1GB", "512MB", "80%" (default: "" = unlimited)
    DefaultOrder string // "asc", "desc" (default: "asc")
}
```

**Why:** Matches DuckDB's configuration options.

### Decision 8: Driver Interface Compliance

**What:** Implement all database/sql/driver interfaces that duckdb-go implements:

```go
// Conn implements these driver interfaces (matching duckdb-go):
// - driver.Conn
// - driver.ConnPrepareContext
// - driver.ExecerContext
// - driver.QueryerContext
// - driver.ConnBeginTx
// - driver.NamedValueChecker
type Conn struct { ... }

// Required methods:
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error)
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error

// Stmt implements:
// - driver.Stmt
// - driver.StmtExecContext
// - driver.StmtQueryContext
type Stmt struct { ... }

// Stmt additional methods (matching duckdb-go EXACTLY - note error returns):
func (s *Stmt) NumInput() int
func (s *Stmt) ParamName(n int) (string, error)      // Returns error!
func (s *Stmt) ColumnCount() (int, error)            // Returns error!
func (s *Stmt) ColumnName(n int) (string, error)     // Returns error!
func (s *Stmt) ColumnType(n int) (Type, error)       // Returns error!
func (s *Stmt) StatementType() (StmtType, error)     // Returns error!

// rows is unexported in duckdb-go (lowercase 'r')
// Implements:
// - driver.Rows
// - driver.RowsColumnTypeScanType
// - driver.RowsColumnTypeDatabaseTypeName
type rows struct { ... }
```

**Why:** Drop-in replacement requires implementing the same driver interfaces with exact method signatures.

### Decision 9: Statement Types

**What:** Define StmtType enum matching duckdb-go EXACTLY (note: STATEMENT_TYPE prefix, not STMT_TYPE):

```go
type StmtType int

const (
    STATEMENT_TYPE_INVALID StmtType = iota
    STATEMENT_TYPE_SELECT
    STATEMENT_TYPE_INSERT
    STATEMENT_TYPE_UPDATE
    STATEMENT_TYPE_EXPLAIN
    STATEMENT_TYPE_DELETE
    STATEMENT_TYPE_PREPARE
    STATEMENT_TYPE_CREATE
    STATEMENT_TYPE_EXECUTE
    STATEMENT_TYPE_ALTER
    STATEMENT_TYPE_TRANSACTION
    STATEMENT_TYPE_COPY
    STATEMENT_TYPE_ANALYZE
    STATEMENT_TYPE_VARIABLE_SET
    STATEMENT_TYPE_CREATE_FUNC
    STATEMENT_TYPE_DROP
    STATEMENT_TYPE_EXPORT
    STATEMENT_TYPE_PRAGMA
    STATEMENT_TYPE_VACUUM
    STATEMENT_TYPE_CALL
    STATEMENT_TYPE_SET
    STATEMENT_TYPE_LOAD
    STATEMENT_TYPE_RELATION
    STATEMENT_TYPE_EXTENSION
    STATEMENT_TYPE_LOGICAL_PLAN
    STATEMENT_TYPE_ATTACH
    STATEMENT_TYPE_DETACH
    STATEMENT_TYPE_MULTI
)
```

**Why:** Allows users to inspect statement type before execution. Must match duckdb-go naming exactly.

### Decision 10: Testing Strategy

**What:** Dual-import test suite that verifies identical behavior with duckdb-go.

```go
// compatibility_test.go
//go:build compatibility

package compatibility_test

import (
    "database/sql"
    "testing"

    duckdb "github.com/duckdb/duckdb-go"
    dukdb "github.com/connerohnesorge/dukdb-go"
)

func TestIdenticalBehavior(t *testing.T) {
    // Open both drivers
    duckConn, _ := sql.Open("duckdb", ":memory:")
    dukConn, _ := sql.Open("dukdb", ":memory:")
    defer duckConn.Close()
    defer dukConn.Close()

    tests := []string{
        "SELECT 1",
        "SELECT 1 + 2",
        "SELECT 'hello' || ' world'",
        // ... comprehensive query list
    }

    for _, query := range tests {
        duckRows, duckErr := duckConn.Query(query)
        dukRows, dukErr := dukConn.Query(query)

        // Compare errors
        if (duckErr != nil) != (dukErr != nil) {
            t.Errorf("Error mismatch for %q: duckdb=%v, dukdb=%v", query, duckErr, dukErr)
        }

        // Compare results
        if !compareRows(duckRows, dukRows) {
            t.Errorf("Result mismatch for %q", query)
        }
    }
}

// Type compatibility tests
func TestTypeCompatibility(t *testing.T) {
    var duckUUID duckdb.UUID
    var dukUUID dukdb.UUID

    // Verify types have identical size and methods
    if unsafe.Sizeof(duckUUID) != unsafe.Sizeof(dukUUID) {
        t.Error("UUID size mismatch")
    }
}
```

**Test categories:**
1. **Unit tests:** Individual component testing (parser, executor, storage)
2. **Integration tests:** End-to-end query execution
3. **Compatibility tests:** Dual-import verification against duckdb-go
4. **File format tests:** Read/write DuckDB files, verify with DuckDB CLI
5. **Concurrency tests:** Race condition detection with `-race`

**CI pipeline:**
```yaml
test:
  - CGO_ENABLED=0 go test ./...           # Core tests
  - CGO_ENABLED=1 go test -tags=compat    # Compatibility tests (needs duckdb)
  - go test -race ./...                    # Race detection
```

## Risks / Trade-offs

- **Risk:** API drift from upstream duckdb-go
  - Mitigation: Dual-import compatibility test suite catches drift automatically

- **Trade-off:** Build constraints may confuse users who have CGO_ENABLED=1 by default
  - Mitigation: Package documentation prominently states CGO_ENABLED=0 requirement; error message guides users

- **Risk:** DuckDB file format changes between versions
  - Mitigation: Pin to specific DuckDB version for compatibility testing

## Migration Plan

1. Users add `import "github.com/connerohnesorge/dukdb-go"` to new code
2. Change driver registration from `sql.Open("duckdb", ...)` to `sql.Open("dukdb", ...)`
3. Type aliases ensure `dukdb.UUID` is compatible with `duckdb.UUID` by value
4. For blank import: `import _ "github.com/connerohnesorge/dukdb-go"`
