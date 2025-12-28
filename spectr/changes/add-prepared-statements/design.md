# Design: Prepared Statements

## Architecture Overview

Prepared statements are client-side wrappers that provide the database/sql PreparedStatement API. Since DuckDB CLI doesn't support server-side prepared statements, we implement client-side preparation that counts parameters and delegates execution to the query-execution layer.

```
sql.Stmt ──→ PreparedStmt ──→ Conn.ExecContext/QueryContext
                │                       │
                │                       └─ Actual execution (from query-execution)
                └─ numParams, query storage
```

## Design Decisions

### Decision 1: Error Types

All error types are defined in add-project-foundation:
- **ErrorTypeClosed**: Statement has been closed
- **ErrorTypeInvalid**: Wrong parameter count or invalid parameter
- **ErrorTypeConnection**: Connection is closed or invalid

### Decision 2: Placeholder Parsing

**countPlaceholders algorithm:**

```go
func countPlaceholders(query string) int {
    positional := findPositionalParams(query)  // Returns max $N found
    named := findNamedParams(query)            // Returns count of unique @name

    if len(positional) > 0 && len(named) > 0 {
        // Mixed mode - return total for counting, execution may fail
        return len(positional) + len(named)
    }

    if len(positional) > 0 {
        return maxPositionalIndex(positional)  // $1, $3 → 3 (gaps allowed)
    }

    return len(named)  // Count unique names: @a, @a, @b → 2
}

func findPositionalParams(query string) []int {
    // Regex: \$(\d+)
    // Match $1, $2, etc., not inside strings
    // Returns list of indices found
}

func findNamedParams(query string) []string {
    // Regex: @([a-zA-Z_][a-zA-Z0-9_]*)
    // Match @name, @param, etc., not inside strings
    // Returns unique names
}
```

**Edge cases:**
| Query | NumInput() | Notes |
|-------|------------|-------|
| `SELECT $1` | 1 | Simple positional |
| `SELECT $1, $3` | 3 | Gap in sequence, max=3 |
| `SELECT $1, $1` | 1 | Duplicate positional, max=1 |
| `SELECT @foo` | 1 | Simple named |
| `SELECT @foo, @bar` | 2 | Two named params |
| `SELECT @foo, @foo` | 1 | Duplicate named, unique count=1 |
| `SELECT $1, @foo` | 2 | Mixed mode (may fail at execution) |
| `SELECT 1` | 0 | No parameters |

**Invalid patterns (detected at execution, not prepare):**
- `$0` - Invalid positional index
- `$-1` - Invalid positional index
- `@` without name - Invalid named param

### Decision 3: Mixed Parameter Mode

Mixed positional and named parameters are NOT supported for execution, but we count them for API compatibility.

**Behavior:**
1. `countPlaceholders` returns sum of positional and named
2. `NumInput()` returns this combined count
3. At execution time, `BindParams` from query-execution will fail with ErrorTypeInvalid: "mixed positional and named parameters not supported"

### Decision 4: Connection Validity Check

Before execution, check if the underlying connection is still valid:

```go
func (s *PreparedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return nil, &Error{Type: ErrorTypeClosed, Msg: "statement is closed"}
    }

    // Check connection validity
    if s.conn.closed {
        return nil, &Error{Type: ErrorTypeConnection, Msg: "connection is closed"}
    }

    // Validate parameter count
    if len(args) != s.numParams {
        return nil, &Error{Type: ErrorTypeInvalid,
            Msg: fmt.Sprintf("expected %d parameter, got %d", s.numParams, len(args))}
    }

    // Delegate to connection
    return s.conn.execContextWithArgs(ctx, s.query, args)
}
```

### Decision 5: Close Semantics

Close is idempotent and always succeeds:

```go
func (s *PreparedStmt) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.closed = true  // Idempotent - setting true multiple times is fine
    return nil       // No resources to release in CLI model
}
```

**Post-close behavior:**
- `NumInput()` still works (returns stored numParams)
- `ExecContext()` returns ErrorTypeClosed
- `QueryContext()` returns ErrorTypeClosed
- Second `Close()` returns nil

### Decision 6: Thread Safety

PreparedStmt is thread-safe via mutex:

```go
type PreparedStmt struct {
    conn      *Conn
    query     string
    numParams int
    closed    bool
    mu        sync.Mutex
}
```

**Lock held during:**
- ExecContext (entire execution)
- QueryContext (entire execution)
- Close (just for setting closed flag)

**Not locked:**
- NumInput (read-only, numParams is immutable)

### Decision 7: Context Cancellation

PreparedStmt respects context cancellation by delegating to Conn methods which check context:

```go
func (s *PreparedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    // ... mutex and validity checks ...

    // Conn.ExecContext checks ctx.Err() before execution
    return s.conn.execContextWithArgs(ctx, s.query, args)
}
```

If context is cancelled before PreparedStmt checks, the error is returned immediately. If cancelled during execution, the underlying query continues but error is returned to caller.

## Error Types Summary

| Scenario | Error Type | Message |
|----------|------------|---------|
| Execute after close | ErrorTypeClosed | "statement is closed" |
| Query after close | ErrorTypeClosed | "statement is closed" |
| Wrong param count | ErrorTypeInvalid | "expected N parameter, got M" |
| Connection closed | ErrorTypeConnection | "connection is closed" |
| Context cancelled | context.Canceled | (from context package) |
