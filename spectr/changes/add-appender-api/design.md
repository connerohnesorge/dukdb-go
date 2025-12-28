# Design: Appender API

## Architecture Overview

The Appender provides efficient bulk data loading by buffering rows in memory and executing batched INSERT statements. It trades latency for throughput - individual AppendRow calls are fast, but data only reaches the database on Flush.

```
User ──→ AppendRow() ──→ Buffer ──→ Flush() ──→ Backend.Execute(INSERT)
             │                          │
             └── Auto-flush at          └── Build multi-row INSERT
                 threshold                   statement
```

## Design Decisions

### Decision 1: Table Metadata Query

**What:** Query `duckdb_columns()` to get column names and types.

**SQL:**
```sql
SELECT column_name, data_type
FROM duckdb_columns()
WHERE database_name = '<catalog>' AND schema_name = '<schema>' AND table_name = '<table>'
ORDER BY column_index
```

**Parameters:**
- `<catalog>`: If empty, use 'memory' for in-memory or database filename
- `<schema>`: If empty, use 'main'
- `<table>`: Table name (required, non-empty)

**Result parsing:**
- column_name: string - Go column name
- data_type: string - DuckDB type name (e.g., "INTEGER", "VARCHAR", "STRUCT(a INTEGER, b VARCHAR)")

**Table not found:** If query returns 0 rows, return:
```go
&Error{Type: ErrorTypeCatalog, Msg: "table '<schema>.<table>' not found"}
```

### Decision 2: FormatValue Dependency

**What:** Use FormatValue from add-query-execution to convert Go values to SQL literals.

**Dependency:** This change requires add-query-execution to be implemented first. FormatValue handles:
- nil → NULL
- Primitive types → SQL literals
- Complex types (UUID, Interval, Decimal) → Appropriate SQL syntax
- BLOB → X'...' hex literal
- Lists/structs → Array/struct literals

**Usage in Flush:**
```go
func (a *Appender) buildInsert() (string, error) {
    var sb strings.Builder
    sb.WriteString("INSERT INTO ")
    sb.WriteString(a.qualifiedTableName())
    sb.WriteString(" (")
    sb.WriteString(strings.Join(a.columns, ", "))
    sb.WriteString(") VALUES ")

    for i, row := range a.buffer {
        if i > 0 {
            sb.WriteString(", ")
        }
        sb.WriteString("(")
        for j, val := range row {
            if j > 0 {
                sb.WriteString(", ")
            }
            formatted, err := FormatValue(val)  // From add-query-execution
            if err != nil {
                return "", err
            }
            sb.WriteString(formatted)
        }
        sb.WriteString(")")
    }
    return sb.String(), nil
}
```

### Decision 3: NULL Handling

**What:** NULL values are allowed for any column. Type validation happens at Flush.

**AppendRow behavior:**
- nil value → stored as nil in buffer (no validation)
- Non-NULL value → stored as-is in buffer (no validation)

**Flush behavior:**
- Build INSERT with NULL for nil values
- DuckDB validates NULL-ability at execution time
- If column is NOT NULL and value is NULL, DuckDB returns error
- Error type: ErrorTypeConstraint with DuckDB's error message

**Rationale:** Eager validation would require tracking NOT NULL constraints, adding complexity. DuckDB's validation is authoritative and provides clear error messages.

### Decision 4: Buffer Preservation on Error

**What:** On Flush failure, the entire buffer is preserved for retry or inspection.

**Behavior:**
- Flush builds INSERT from all buffered rows
- If INSERT fails (any error), buffer is NOT cleared
- Caller can: inspect buffer, fix data, retry Flush, or Close without flush
- On next successful Flush, buffer is cleared

**Partial failure handling:**
- DuckDB INSERT is atomic - either all rows succeed or all fail
- No partial success possible within a single Flush
- If caller needs row-by-row error handling, flush after each row (defeats purpose of Appender)

**Implementation:**
```go
func (a *Appender) Flush() error {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.closed {
        return &Error{Type: ErrorTypeClosed, Msg: "appender is closed"}
    }
    if len(a.buffer) == 0 {
        return nil  // No-op for empty buffer
    }

    insert, err := a.buildInsert()
    if err != nil {
        return err  // Buffer preserved
    }

    _, err = a.conn.ExecContext(context.Background(), insert)
    if err != nil {
        return err  // Buffer preserved for retry
    }

    a.buffer = a.buffer[:0]  // Clear buffer only on success
    return nil
}
```

### Decision 5: Threshold Edge Cases

**What:** Define behavior for edge case threshold values.

| Threshold | Behavior |
|-----------|----------|
| 0 | Invalid - constructor returns ErrorTypeInvalid |
| 1 | Flush after every row (defeats buffering purpose, but valid) |
| 1-2^31 | Normal operation |
| Default | 1024 rows |

**Validation:**
```go
func NewAppenderWithThreshold(conn *Conn, schema, table string, threshold int) (*Appender, error) {
    if threshold < 1 {
        return nil, &Error{Type: ErrorTypeInvalid, Msg: "threshold must be >= 1"}
    }
    // ... rest of constructor
}
```

### Decision 6: Auto-Flush During Append

**What:** When buffer reaches threshold, auto-flush before adding new row.

**Algorithm:**
```go
func (a *Appender) AppendRow(values ...any) error {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.closed {
        return &Error{Type: ErrorTypeClosed, Msg: "appender is closed"}
    }
    if len(values) != len(a.columns) {
        return &Error{Type: ErrorTypeInvalid,
            Msg: fmt.Sprintf("expected %d columns, got %d", len(a.columns), len(values))}
    }

    // Auto-flush at threshold
    if len(a.buffer) >= a.threshold {
        if err := a.flushLocked(); err != nil {
            return err  // Return auto-flush error to caller
        }
    }

    a.buffer = append(a.buffer, values)
    return nil
}
```

**Auto-flush failure:** If auto-flush fails, AppendRow returns the flush error. The new row is NOT added to buffer. Caller should handle error (e.g., retry or abort).

### Decision 7: Concurrent Append/Flush

**What:** All operations are serialized via mutex.

**Serialization guarantees:**
- Only one goroutine can be in AppendRow at a time
- Only one goroutine can be in Flush at a time
- Auto-flush during AppendRow holds lock throughout
- Close is also serialized

**Deadlock prevention:** Appender does not call external code while holding lock (except conn.ExecContext, which is safe).

## Error Types

| Scenario | Error Type |
|----------|------------|
| Table not found | ErrorTypeCatalog |
| Column count mismatch | ErrorTypeInvalid |
| Threshold < 1 | ErrorTypeInvalid |
| Append after close | ErrorTypeClosed |
| Flush after close | ErrorTypeClosed |
| Double close | ErrorTypeClosed |
| NOT NULL violation | ErrorTypeConstraint |
| Type mismatch | ErrorTypeInvalid (from FormatValue) |
| Foreign key violation | ErrorTypeConstraint |

## Thread Safety

All Appender methods are thread-safe via internal mutex:
- AppendRow: acquires lock, may auto-flush, releases lock
- Flush: acquires lock, executes INSERT, releases lock
- Close: acquires lock, flushes, marks closed, releases lock
