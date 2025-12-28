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

### Decision 5: Coexistence with duckdb-go

**What:** This package uses module path `github.com/dukdb/dukdb-go` (note: dukdb, not duckdb) which is distinct from `github.com/marcboeker/go-duckdb`.

**Why:** Users can import both packages if needed during migration. The different module path prevents import conflicts.

### Decision 6: Error Type Design

**What:** Define error types matching duckdb-go's exported error patterns:

```go
// Error represents a DuckDB error with type and message.
type Error struct {
    Type    ErrorType
    Msg     string
}

func (e *Error) Error() string {
    return fmt.Sprintf("%s: %s", e.Type.String(), e.Msg)
}

// Is enables errors.Is matching by Type only (ignores Msg)
func (e *Error) Is(target error) bool {
    if t, ok := target.(*Error); ok {
        return e.Type == t.Type
    }
    return false
}

// ErrorType categorizes DuckDB errors.
type ErrorType int

const (
    ErrorTypeInvalid        ErrorType = 0  // Invalid/unknown error
    ErrorTypeOutOfRange     ErrorType = 1  // Value out of range
    ErrorTypeConversion     ErrorType = 2  // Type conversion error
    ErrorTypeUnknownType    ErrorType = 3  // Unknown type
    ErrorTypeDecimal        ErrorType = 4  // Decimal error
    ErrorTypeMismatchType   ErrorType = 5  // Type mismatch
    ErrorTypeDivideByZero   ErrorType = 6  // Division by zero
    ErrorTypeObjectSize     ErrorType = 7  // Object size error
    ErrorTypeInvalidType    ErrorType = 8  // Invalid type
    ErrorTypeSerialization  ErrorType = 9  // Serialization error
    ErrorTypeTransaction    ErrorType = 10 // Transaction error
    ErrorTypeNotImplemented ErrorType = 11 // Not implemented
    ErrorTypeExpression     ErrorType = 12 // Expression error
    ErrorTypeCatalog        ErrorType = 13 // Catalog error
    ErrorTypeParser         ErrorType = 14 // Parser/syntax error
    ErrorTypePlanner        ErrorType = 15 // Query planner error
    ErrorTypeScheduler      ErrorType = 16 // Scheduler error
    ErrorTypeExecutor       ErrorType = 17 // Executor error
    ErrorTypeConstraint     ErrorType = 18 // Constraint violation
    ErrorTypeIndex          ErrorType = 19 // Index error
    ErrorTypeStat           ErrorType = 20 // Statistics error
    ErrorTypeConnection     ErrorType = 21 // Connection error
    ErrorTypeSyntax         ErrorType = 22 // Syntax error
    ErrorTypeSettings       ErrorType = 23 // Settings error
    ErrorTypeBinder         ErrorType = 24 // Binder error
    ErrorTypeNetwork        ErrorType = 25 // Network error
    ErrorTypeOptimizer      ErrorType = 26 // Optimizer error
    ErrorTypeNullPointer    ErrorType = 27 // Null pointer error
    ErrorTypeIO             ErrorType = 28 // I/O error
    ErrorTypeInterrupt      ErrorType = 29 // Interrupt error
    ErrorTypeFatal          ErrorType = 30 // Fatal error
    ErrorTypeInternal       ErrorType = 31 // Internal error
    ErrorTypeInvalidInput   ErrorType = 32 // Invalid input
    ErrorTypeOutOfMemory    ErrorType = 33 // Out of memory
    ErrorTypePermission     ErrorType = 34 // Permission denied
    ErrorTypeParameterNotResolved ErrorType = 35 // Unresolved parameter
    ErrorTypeParameterNotAllowed  ErrorType = 36 // Parameter not allowed
    ErrorTypeDependency     ErrorType = 37 // Dependency error
    ErrorTypeClosed         ErrorType = 38 // Resource closed
    ErrorTypeBadState       ErrorType = 39 // Invalid state
    ErrorTypeSequence       ErrorType = 40 // Sequence error
)

// String returns human-readable error type name
func (t ErrorType) String() string {
    names := [...]string{
        "Invalid Error", "Out Of Range Error", "Conversion Error",
        "Unknown Type Error", "Decimal Error", "Mismatch Type Error",
        "Divide By Zero Error", "Object Size Error", "Invalid Type Error",
        "Serialization Error", "Transaction Error", "Not Implemented Error",
        "Expression Error", "Catalog Error", "Parser Error", "Planner Error",
        "Scheduler Error", "Executor Error", "Constraint Error", "Index Error",
        "Stat Error", "Connection Error", "Syntax Error", "Settings Error",
        "Binder Error", "Network Error", "Optimizer Error", "Null Pointer Error",
        "IO Error", "Interrupt Error", "Fatal Error", "Internal Error",
        "Invalid Input Error", "Out Of Memory Error", "Permission Error",
        "Parameter Not Resolved Error", "Parameter Not Allowed Error",
        "Dependency Error", "Closed Error", "Bad State Error", "Sequence Error",
    }
    if int(t) < len(names) {
        return names[t]
    }
    return "Unknown Error"
}
```

**Why:** Drop-in replacement requires `errors.Is` and `errors.As` to work identically.

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

## Risks / Trade-offs

- **Risk:** API drift from upstream duckdb-go
  - Mitigation: Maintain compatibility test suite that imports both packages and verifies type signatures match

- **Trade-off:** Build constraints may confuse users who have CGO_ENABLED=1 by default
  - Mitigation: Package documentation prominently states CGO_ENABLED=0 requirement; error message guides users

## Migration Plan

1. Users add `import "github.com/dukdb/dukdb-go"` to new code
2. Change driver registration from `sql.Open("duckdb", ...)` to `sql.Open("dukdb", ...)`
3. Type aliases ensure `dukdb.UUID` is compatible with `duckdb.UUID` by value
