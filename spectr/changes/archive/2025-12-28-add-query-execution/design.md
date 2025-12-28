## Context

The query execution layer implements database/sql's Stmt and Tx interfaces, translating Go method calls into SQL commands sent to the process backend. This is NEW code for the pure Go `dukdb-go` driver.

## Goals / Non-Goals

**Goals:**
- Safe parameter binding preventing SQL injection
- Transaction support via BEGIN/COMMIT/ROLLBACK
- Context deadline enforcement via backend timeout
- Clear error types for all failure modes

**Non-Goals:**
- Mid-query cancellation (subprocess limitation)
- Server-side prepared statements (use client-side query construction)
- Savepoints (not supported by this driver)

## Decisions

### Decision 1: Parameter Binding Ownership

**What:** Query execution owns ALL parameter binding. Prepared statements (add-prepared-statements) reuse this via composition, not reimplementation.

**Implementation:**
```go
// params.go

// BindParams replaces $N and @name placeholders with SQL literals
func BindParams(query string, args []driver.NamedValue) (string, error) {
    // 1. Extract all placeholders from query (skip those inside string literals)
    // 2. Match to args by position ($1) or name (@name)
    // 3. Format each value as SQL literal via FormatValue
    // 4. Replace placeholders with literals
    // 5. Return error if param count mismatches
}

// FormatValue converts a Go value to SQL literal string
func FormatValue(v any) (string, error) {
    switch val := v.(type) {
    case nil:
        return "NULL", nil
    case bool:
        if val { return "TRUE", nil }
        return "FALSE", nil
    case int, int8, int16, int32, int64:
        return fmt.Sprintf("%d", val), nil
    case uint, uint8, uint16, uint32, uint64:
        return fmt.Sprintf("%d", val), nil
    case float32:
        return formatFloat(float64(val)), nil
    case float64:
        return formatFloat(val), nil
    case string:
        return formatString(val), nil
    case []byte:
        return formatBlob(val), nil
    case time.Time:
        return formatTimestamp(val), nil
    case UUID:
        return formatString(val.String()), nil
    case Decimal:
        return val.String(), nil
    case Interval:
        return fmt.Sprintf("INTERVAL '%d months %d days %d microseconds'",
            val.Months, val.Days, val.Micros), nil
    case *big.Int:
        return val.String(), nil
    default:
        return "", fmt.Errorf("unsupported type %T", v)
    }
}
```

**Why:** Single source of truth for SQL formatting. Prepared statements become thin wrappers.

### Decision 1b: Placeholder Parsing Rules

**What:** Rules for extracting placeholders from queries:

| Rule | Description |
|------|-------------|
| Skip string literals | Placeholders inside `'...'` are NOT replaced |
| Positional indexing | `$1` is first arg, `$2` is second, etc. (1-based) |
| Invalid $0 | `$0` is NOT a valid placeholder (ignored) |
| Gaps allowed | `$1, $3` means 3 args required (arg[0], arg[1], arg[2]) |
| Duplicates reuse | `$1, $1` uses arg[0] twice, only 1 arg required |
| Named matching | `@foo` matches arg with Name="foo" (case-sensitive) |
| Mixed mode error | Query with both `$N` and `@name` returns ErrorTypeInvalid |

**String literal detection algorithm:**
```go
func isInsideStringLiteral(query string, pos int) bool {
    inString := false
    for i := 0; i < pos; i++ {
        if query[i] == '\'' {
            // Check for escaped quote ('')
            if i+1 < len(query) && query[i+1] == '\'' {
                i++ // Skip escaped quote
                continue
            }
            inString = !inString
        }
    }
    return inString
}
```

**Error messages:**
- Missing positional: `"missing parameter $N"`
- Missing named: `"named parameter @name not found in args"`
- Extra args: `"expected N parameters, got M"`
- Mixed mode: `"cannot mix positional ($N) and named (@name) parameters"`

### Decision 2: String Escaping

**What:** Escape strings using doubled single-quotes (SQL standard):
```go
func formatString(s string) string {
    escaped := strings.ReplaceAll(s, "'", "''")
    return "'" + escaped + "'"
}
```

**Examples:**
- `"hello"` → `'hello'`
- `"O'Brien"` → `'O''Brien'`
- `"it's \"quoted\""` → `'it''s "quoted"'`

**Why:** SQL standard escaping, works with DuckDB.

### Decision 3: BLOB Formatting

**What:** Use hex literal format X'...' with uppercase hex:
```go
func formatBlob(b []byte) string {
    return "X'" + strings.ToUpper(hex.EncodeToString(b)) + "'"
}
```

**Examples:**
- `[]byte("HELLO")` → `X'48454C4C4F'`
- `[]byte{}` → `X''`

**Why:** DuckDB-compatible hex literal format.

### Decision 4: Timestamp Formatting

**What:** Format as ISO8601 with microsecond precision:
```go
func formatTimestamp(t time.Time) string {
    return "'" + t.UTC().Format("2006-01-02 15:04:05.000000") + "'"
}
```

**Why:** DuckDB parses ISO8601 timestamps. UTC normalization ensures consistency.

### Decision 4b: Float Formatting

**What:** Format floats with full precision, reject Infinity/NaN:

```go
func formatFloat(f float64) (string, error) {
    if math.IsInf(f, 0) {
        return "", &Error{Type: ErrorTypeInvalid, Msg: "infinity values not supported as parameters"}
    }
    if math.IsNaN(f) {
        return "", &Error{Type: ErrorTypeInvalid, Msg: "NaN values not supported as parameters"}
    }
    // Use %g for shortest accurate representation
    // Max precision to avoid loss: 17 for float64, 9 for float32
    return strconv.FormatFloat(f, 'g', -1, 64), nil
}
```

**Why:** SQL literals don't support Infinity/NaN; these must be constructed via expressions like `1e309` or `0/0`.

### Decision 4c: Statement Closed Behavior

**What:** Executing a closed statement returns specific error:

```go
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    if s.closed {
        return nil, &Error{Type: ErrorTypeClosed, Msg: "statement is closed"}
    }
    // ... execution
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
    if s.closed {
        return nil, &Error{Type: ErrorTypeClosed, Msg: "statement is closed"}
    }
    // ... execution
}
```

**Why:** Clear error type for closed statement operations.

### Decision 5: Context Handling Strategy

**What:** Context is checked BEFORE execution; timeout controls backend behavior:

```go
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    // 1. Check if already cancelled
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    // 2. Calculate effective timeout
    timeout := s.conn.backend.DefaultTimeout()
    if deadline, ok := ctx.Deadline(); ok {
        remaining := time.Until(deadline)
        if remaining < timeout {
            timeout = remaining
        }
    }

    // 3. Execute with timeout
    result, err := s.conn.backend.ExecWithTimeout(s.query, timeout)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            return nil, context.DeadlineExceeded
        }
        return nil, err
    }
    return result, nil
}
```

**Why:** Subprocess backend cannot interrupt running queries. We enforce timeout via process management.

### Decision 6: Transaction Implementation

**What:** Transactions are implemented via SQL commands:

```go
// tx.go

type Tx struct {
    conn   *Conn
    done   bool
    mu     sync.Mutex
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
    // Validate options
    if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
        return nil, &Error{
            Type:    ErrorTypeSettings,
            Message: "only sql.LevelDefault isolation is supported",
        }
    }
    if opts.ReadOnly {
        return nil, &Error{
            Type:    ErrorTypeSettings,
            Message: "read-only transactions are not supported",
        }
    }

    // Start transaction
    _, err := c.backend.Exec(ctx, "BEGIN TRANSACTION", nil)
    if err != nil {
        return nil, err
    }

    return &Tx{conn: c}, nil
}

func (tx *Tx) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    if tx.done {
        return &Error{Type: ErrorTypeTransaction, Message: "transaction already completed"}
    }
    tx.done = true
    _, err := tx.conn.backend.Exec(context.Background(), "COMMIT", nil)
    return err
}

func (tx *Tx) Rollback() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    if tx.done {
        return &Error{Type: ErrorTypeTransaction, Message: "transaction already completed"}
    }
    tx.done = true
    _, err := tx.conn.backend.Exec(context.Background(), "ROLLBACK", nil)
    return err
}
```

**Why:** DuckDB CLI supports transactions via BEGIN/COMMIT/ROLLBACK commands.

### Decision 7: Error Type Classification

**What:** Map execution errors to specific ErrorTypes:

| Error Condition | ErrorType |
|-----------------|-----------|
| Parameter count mismatch | ErrorTypeInvalid |
| Unsupported parameter type | ErrorTypeInvalid |
| SQL syntax error | ErrorTypeParser |
| Table not found | ErrorTypeCatalog |
| Constraint violation | ErrorTypeConstraint |
| Unsupported isolation level | ErrorTypeSettings |
| Transaction already completed | ErrorTypeTransaction |
| Context deadline exceeded | (return context.DeadlineExceeded directly) |
| Context cancelled | (return context.Canceled directly) |

**Why:** Consistent error handling across driver.

### Decision 8: Multiple Statement Handling

**What:** Multiple statements in single query are executed sequentially:

```go
// Backend sends entire query to DuckDB CLI
// CLI executes all statements in order
// Result reflects last statement only
// If any statement fails, execution stops and error is returned
```

**Behavior:**
- `INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)` → RowsAffected = 1 (last statement)
- `INSERT INTO t VALUES (1); INVALID SQL` → Error for second statement, first already committed

**Why:** DuckDB CLI behavior. Use transactions if atomicity needed.

## Risks / Trade-offs

- **Risk:** Cannot cancel long-running queries
  - Mitigation: Backend kills process after timeout; document limitation

- **Trade-off:** Client-side parameter binding exposes SQL to potential issues
  - Mitigation: Comprehensive escaping, test suite for injection vectors

- **Risk:** Transaction state mismatch if backend crashes mid-transaction
  - Mitigation: Backend detects crash and resets connection state
