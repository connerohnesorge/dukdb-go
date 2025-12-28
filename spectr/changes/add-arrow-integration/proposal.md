# Change: Add Apache Arrow Integration

## Why

Apache Arrow provides an efficient columnar memory format for data interchange. Arrow integration enables:
- Zero-copy data exchange with other Arrow-enabled systems
- High-performance analytics on large datasets
- Integration with data science tools (pandas, polars, etc.)
- Streaming large result sets efficiently

## What Changes

### Core Types

```go
// Arrow provides Arrow-format query results
type Arrow struct {
    conn *Conn
}
```

### Public API

```go
// NewArrowFromConn creates an Arrow interface from a driver connection
// Note: Uses driver.Conn, not *sql.Conn
func NewArrowFromConn(driverConn driver.Conn) (*Arrow, error)

// QueryContext executes query returning Arrow record reader
// Note: Returns array.RecordReader from arrow-go/v18/arrow/array package
func (a *Arrow) QueryContext(ctx context.Context, query string, args ...any) (array.RecordReader, error)
```

### Build Tags

- Requires build tag: `duckdb_arrow`
- Adds dependency on `apache/arrow-go` v18

## Impact

- **Affected specs**: Depends on data-chunk-api
- **Affected code**: New file `arrow.go` with build tags
- **Dependencies**: apache/arrow-go library (heavy dependency)
- **Consumers**: Data science applications, analytics pipelines

## Breaking Changes

None. This is an opt-in feature via build tag.

## Implementation Notes

Arrow connections:
- NOT safe for concurrent use
- Do NOT benefit from database/sql connection pooling
- Should be used with dedicated connections for streaming

### Build Tags

Requires TWO build constraints:
- `duckdb_arrow` - Main feature flag
- `duckdb_use_lib` OR `duckdb_use_static_lib` - Platform linking requirement

The pure Go implementation will:
1. Build Arrow schemas from DuckDB type info
2. Convert DataChunks to Arrow RecordBatches
3. Implement pull-based streaming (RecordReader.Next() pattern, not channels)
4. Support all type mappings between DuckDB and Arrow
5. Include reference counting via Retain/Release pattern
