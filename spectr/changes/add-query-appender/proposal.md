# Change: Add Query Appender API

## Why

The current Appender API only supports direct INSERT into existing tables. Users need the ability to:
- Batch rows and execute custom queries (INSERT, DELETE, UPDATE, MERGE INTO) treating the batched rows as a temporary table
- Perform complex ETL operations like conditional upserts, merge-insert patterns
- Execute parameterized bulk operations without materializing temporary tables manually
- Chain appended data with JOINs, subqueries, and other SQL operations

This is a key feature in duckdb-go v1.4.3 that enables advanced data manipulation patterns.

## What Changes

### Core API

```go
// NewQueryAppender creates an Appender that executes a custom query with batched rows.
// The batched rows are treated as a temporary table named by the `table` parameter.
// NOTE: The `table` parameter is passed directly to DuckDB - no default is applied in Go code.
// The query can be INSERT, DELETE, UPDATE, or MERGE INTO statements.
// colTypes define the schema of the temporary table.
// colNames are optional column names (default: col1, col2, ...).
func NewQueryAppender(
    driverConn driver.Conn,
    query, table string,
    colTypes []TypeInfo,
    colNames []string,
) (*Appender, error)
```

### Usage Example

```go
// Create a query appender for conditional upsert
appender, err := dukdb.NewQueryAppender(
    conn,
    `MERGE INTO target t
     USING appended_data s ON t.id = s.id
     WHEN MATCHED THEN UPDATE SET value = s.value
     WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)`,
    "appended_data",
    []dukdb.TypeInfo{
        dukdb.NewTypeInfo(dukdb.TYPE_INTEGER),
        dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR),
    },
    []string{"id", "value"},
)
if err != nil {
    return err
}
defer appender.Close()

// Batch rows
appender.AppendRow(1, "first")
appender.AppendRow(2, "second")

// Flush executes the MERGE query with batched data
err = appender.Flush()
```

### Implementation Note

The Appender struct does NOT contain additional Go fields for query appender functionality. Instead, the query appender state is managed through the underlying C binding (`mapping.Appender`) which handles:
- Query storage
- Temporary table name
- Column type metadata

The Go struct remains unchanged - all state is delegated to the C layer via `mapping.AppenderCreateQuery()`.

### Modified Methods

```go
// Flush behavior changes based on appender type:
// - Table appender: Direct INSERT into target table
// - Query appender: Creates temp table, inserts data, executes query, drops temp table
func (a *Appender) Flush() error

// Close ensures cleanup of temporary resources
func (a *Appender) Close() error
```

## Impact

- **Affected specs**: Extends appender capability
- **Affected code**: Modifications to `appender.go`
- **Dependencies**: Requires existing Appender infrastructure, TypeInfo system
- **Consumers**: ETL pipelines, data synchronization tools, batch processing applications

## Breaking Changes

None. This adds a new constructor function while preserving existing NewAppender behavior.
