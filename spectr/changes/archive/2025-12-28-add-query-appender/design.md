## Context

Query Appender extends the existing Appender API to support custom SQL queries with batched data. While the standard Appender provides efficient bulk INSERT into tables, Query Appender enables complex ETL patterns like MERGE, conditional updates, and multi-table operations.

**Stakeholders**: ETL developers, data engineers, BI tool authors, migration tool builders

**Constraints**:
- Must reuse existing Appender infrastructure where possible
- Must support all DuckDB query types (INSERT, UPDATE, DELETE, MERGE INTO)
- Temporary table must be invisible to other connections
- Memory usage must be bounded by flush threshold

## Goals / Non-Goals

### Goals
- Execute custom queries with batched row data
- Support MERGE INTO for upsert patterns
- Support DELETE with batched criteria
- Support UPDATE with batched source data
- API compatibility with duckdb-go NewQueryAppender
- Thread-safe operation

### Non-Goals
- Streaming query results back through appender
- Cross-connection temporary table sharing
- Transaction boundary control (uses connection's transaction)
- Stored procedure execution

## Decisions

### Decision 1: Temporary Table Implementation

**What**: Use transaction-scoped temporary tables for batched data

**Why**:
- Avoids polluting the database namespace
- Automatic cleanup on connection close
- Isolation between concurrent appenders
- No need for manual cleanup on errors

**Implementation**:
```go
func (a *Appender) createTempTable() error {
    // Build CREATE TEMP TABLE statement from typeInfos
    var sb strings.Builder
    sb.WriteString("CREATE TEMP TABLE IF NOT EXISTS ")
    sb.WriteString(quoteIdentifier(a.tempTable))
    sb.WriteString(" (")

    for i, ti := range a.typeInfos {
        if i > 0 {
            sb.WriteString(", ")
        }
        colName := a.getColumnName(i)
        sb.WriteString(quoteIdentifier(colName))
        sb.WriteString(" ")
        sb.WriteString(ti.SQLType())
    }
    sb.WriteString(")")

    _, err := a.conn.ExecContext(context.Background(), sb.String(), nil)
    return err
}

func (a *Appender) getColumnName(idx int) string {
    if idx < len(a.colNames) && a.colNames[idx] != "" {
        return a.colNames[idx]
    }
    return fmt.Sprintf("col%d", idx+1)
}
```

### Decision 2: Flush Execution Strategy

**What**: Three-phase flush for query appenders

**Why**: Ensures atomicity and proper cleanup

**Implementation**:
```go
func (a *Appender) flushQueryAppender() error {
    // Phase 1: Insert batched data into temp table
    if err := a.createTempTable(); err != nil {
        return err
    }

    insertSQL := a.buildTempTableInsert()
    if _, err := a.conn.ExecContext(context.Background(), insertSQL, nil); err != nil {
        return err
    }

    // Phase 2: Execute user's query
    if _, err := a.conn.ExecContext(context.Background(), a.query, nil); err != nil {
        // Cleanup temp table on error
        a.dropTempTable()
        return err
    }

    // Phase 3: Truncate temp table for next batch
    if err := a.truncateTempTable(); err != nil {
        return err
    }

    a.buffer = a.buffer[:0]
    return nil
}

func (a *Appender) truncateTempTable() error {
    sql := "DELETE FROM " + quoteIdentifier(a.tempTable)
    _, err := a.conn.ExecContext(context.Background(), sql, nil)
    return err
}
```

### Decision 3: Validation at Creation Time

**What**: Validate query and column configuration at NewQueryAppender call

**Why**:
- Fail fast on configuration errors
- Avoid runtime surprises during Flush
- Better error messages with context

**Implementation**:
```go
func NewQueryAppender(
    driverConn driver.Conn,
    query, table string,
    colTypes []TypeInfo,
    colNames []string,
) (*Appender, error) {
    // Validate query is not empty
    if strings.TrimSpace(query) == "" {
        return nil, &Error{
            Type: ErrorTypeInvalid,
            Msg:  "query cannot be empty",
        }
    }

    // Validate column types provided
    if len(colTypes) == 0 {
        return nil, &Error{
            Type: ErrorTypeInvalid,
            Msg:  "at least one column type required",
        }
    }

    // Validate column names match types if provided
    if len(colNames) > 0 && len(colNames) != len(colTypes) {
        return nil, &Error{
            Type: ErrorTypeInvalid,
            Msg:  fmt.Sprintf("column names count (%d) must match column types count (%d)",
                len(colNames), len(colTypes)),
        }
    }

    // Default table name
    if table == "" {
        table = "appended_data"
    }

    // Extract connection from driver.Conn
    conn, ok := driverConn.(*Conn)
    if !ok {
        return nil, &Error{
            Type: ErrorTypeInvalid,
            Msg:  "driver connection must be *dukdb.Conn",
        }
    }

    return &Appender{
        conn:      conn,
        query:     query,
        tempTable: table,
        typeInfos: colTypes,
        colNames:  colNames,
        buffer:    make([][]any, 0, DefaultAppenderThreshold),
        threshold: DefaultAppenderThreshold,
    }, nil
}
```

### Decision 4: TypeInfo Integration

**What**: Use TypeInfo for column type specification

**Why**:
- Consistent with existing type system
- Supports all 45 DuckDB types including nested types
- Provides SQLType() method for DDL generation

**Implementation**:
```go
// TypeInfo.SQLType returns the SQL type declaration
func (ti TypeInfo) SQLType() string {
    switch ti.Type {
    case TYPE_INTEGER:
        return "INTEGER"
    case TYPE_VARCHAR:
        return "VARCHAR"
    case TYPE_LIST:
        return fmt.Sprintf("LIST(%s)", ti.ChildType.SQLType())
    case TYPE_STRUCT:
        var parts []string
        for name, childTI := range ti.StructFields {
            parts = append(parts, fmt.Sprintf("%s %s",
                quoteIdentifier(name), childTI.SQLType()))
        }
        return fmt.Sprintf("STRUCT(%s)", strings.Join(parts, ", "))
    // ... handle all types
    }
}
```

### Decision 5: Clock Injection for Timeout Handling

**What**: Use injected quartz.Clock for timeout checking during query execution

**Why**:
- Per deterministic-testing spec, all time-dependent code must use injected clock
- Query appender flush operations may have deadlines
- Enables deterministic testing of timeout scenarios

**Implementation**:
```go
type appenderContext struct {
    ctx   context.Context
    clock quartz.Clock
}

func (a *Appender) flushWithClock(actx appenderContext) error {
    // Check deadline before expensive operations
    if deadline, ok := actx.ctx.Deadline(); ok {
        if actx.clock.Until(deadline) <= 0 {
            return context.DeadlineExceeded
        }
    }

    // Proceed with flush
    if err := a.createTempTable(); err != nil {
        return err
    }
    // ... rest of flush logic
    return nil
}
```

## Risks / Trade-offs

### Risk 1: Temporary Table Collision
**Risk**: Multiple QueryAppenders with same table name on same connection
**Mitigation**:
- Document that table names must be unique per connection
- Consider adding unique suffix generation option
- Use TEMP table scope for isolation

### Risk 2: Memory Growth on Large Batches
**Risk**: Buffer can grow unbounded before flush
**Mitigation**:
- Honor threshold for auto-flush
- Document memory implications
- Consider streaming insert for very large batches

### Risk 3: Error State Cleanup
**Risk**: Failed query leaves temp table in unknown state
**Mitigation**:
- Always truncate on successful flush
- Document that errors may leave partial data in temp table
- Close() cleans up temp table completely

## Migration Plan

New capability with no migration required. Existing Appender users continue using NewAppender unchanged.
