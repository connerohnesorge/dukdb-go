# Change: Implement Prepared Statements

## Why

Prepared statements provide a clean API for repeated queries through the database/sql interface. While DuckDB CLI doesn't support server-side prepared statements, we implement client-side statement preparation matching duckdb-go's behavior.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Extend `stmt.go` with PreparedStmt wrapper around Stmt
- Add parameter count detection via query parsing
- Implement statement lifecycle (prepare, execute, close)
- Reuse parameter binding from add-query-execution (no reimplementation)

## Architecture

Prepared statements are thin wrappers over the query execution layer:

```go
type PreparedStmt struct {
    conn      *Conn
    query     string
    numParams int
    closed    bool
    mu        sync.Mutex
}

func (c *Conn) Prepare(query string) (*PreparedStmt, error) {
    numParams := countPlaceholders(query)
    return &PreparedStmt{
        conn:      c,
        query:     query,
        numParams: numParams,
    }, nil
}

func (s *PreparedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.closed {
        return nil, &Error{Type: ErrorTypeClosed, Message: "statement is closed"}
    }
    // Delegate to query-execution's BindParams and execute
    return s.conn.ExecContext(ctx, s.query, args)
}
```

**No statement caching:** We decided against statement caching as it adds complexity without benefit for CLI-based execution. Each execution goes through the full query path.

## Parameter Binding Delegation

This change does NOT reimplement parameter binding. It delegates to `add-query-execution`:
- `BindParams()` from query-execution handles all placeholder replacement
- `FormatValue()` from query-execution handles all type formatting
- PreparedStmt just validates param count before delegating

## Dependencies (Explicit)

This change depends on:
1. `add-project-foundation` - Error types (ErrorTypeClosed, ErrorTypeInvalid, ErrorTypeConnection)
2. `add-type-system` - Type definitions for parameter values
3. `add-query-execution` - BindParams and FormatValue for parameter binding

**Note:** Error types are defined in add-project-foundation and must be implemented first.

## Placeholder Parsing

| Query | NumInput() | Notes |
|-------|------------|-------|
| `SELECT $1` | 1 | Simple positional |
| `SELECT $1, $3` | 3 | Gap in sequence, max=3 |
| `SELECT $1, $1` | 1 | Duplicate positional, max=1 |
| `SELECT @foo, @bar` | 2 | Two named params |
| `SELECT @foo, @foo` | 1 | Duplicate named, unique count=1 |
| `SELECT $1, @foo` | 2 | Mixed mode (execution may fail) |

## Impact

- Affected specs: `prepared-statements` (new capability)
- Affected code: Extend `stmt.go` in root package
- Dependencies: Requires all previous proposals (see explicit list above)
- Enables: Clean API for repeated query execution via database/sql
