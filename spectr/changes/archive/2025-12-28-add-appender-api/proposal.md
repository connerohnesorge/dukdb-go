# Change: Implement Appender API

## Why

The Appender API enables efficient bulk data loading, providing significant performance improvement over individual INSERT statements. This matches duckdb-go's Appender interface for API compatibility.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Create `appender.go` implementing Appender type
- Buffer rows in memory and batch-insert via backend
- Implement auto-flush at configurable row threshold (default 1024 rows)
- Support all DuckDB types in append operations
- Provide NewAppenderFromConn and NewAppender constructors

## Architecture

```go
type Appender struct {
    conn       *Conn
    catalog    string
    schema     string
    table      string
    columns    []string
    colTypes   []Type
    buffer     [][]any
    threshold  int        // Default 1024 rows
    closed     bool
    mu         sync.Mutex
}

func NewAppenderFromConn(conn *Conn, schema, table string) (*Appender, error)
func NewAppender(conn *Conn, catalog, schema, table string) (*Appender, error)
func (a *Appender) AppendRow(values ...any) error
func (a *Appender) Flush() error
func (a *Appender) Close() error
```

## Type Conversion Rules

| Go Type | Conversion to SQL |
|---------|-------------------|
| nil | NULL |
| bool | TRUE/FALSE |
| int/int8/.../int64 | Numeric literal |
| uint/uint8/.../uint64 | Numeric literal |
| float32/float64 | Numeric literal (error on Inf/NaN) |
| string | Single-quoted with escaping |
| []byte | X'...' hex literal |
| time.Time | ISO8601 timestamp string |
| UUID | Hyphenated UUID string |
| Decimal | Numeric string |
| Interval | INTERVAL literal |
| []any | Array literal |
| map[string]any | Struct literal |
| Map | Map literal |

Type mismatches are detected at Flush() time when the INSERT executes.

## Flush Behavior

- **Empty buffer:** No-op, returns nil
- **Successful flush:** Buffer cleared, nil returned
- **Flush error:** Buffer preserved for retry, error returned
- **Close:** Flushes remaining, marks closed
- **Double close:** Returns ErrorTypeClosed

## Dependencies (Explicit)

This change depends on:
1. `add-project-foundation` - Error types, Backend interface
2. `add-type-system` - Type definitions (UUID, Interval, Decimal, etc.)
3. `add-query-execution` - FormatValue function for SQL literal formatting
4. `add-result-handling` - Conn type with ExecContext method

**Key dependency:** FormatValue from add-query-execution is used to convert Go values to SQL literals during Flush.

## Table Metadata Query

Column information is retrieved using DuckDB's `duckdb_columns()` function:
```sql
SELECT column_name, data_type
FROM duckdb_columns()
WHERE database_name = '<catalog>' AND schema_name = '<schema>' AND table_name = '<table>'
ORDER BY column_index
```

If query returns 0 rows, constructor returns ErrorTypeCatalog with message "table '<schema>.<table>' not found".

## Impact

- Affected specs: `appender-api` (new capability)
- Affected code: NEW file `appender.go` in root package
- Dependencies: Requires all previous proposals (see explicit list above)
- Enables: Efficient bulk data loading matching duckdb-go patterns
