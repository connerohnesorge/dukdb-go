# Change: Add Data Chunk Vectorized API

## Why

The current dukdb-go implementation lacks efficient vectorized data access patterns. The pure Go driver processes data row-by-row, which is inefficient for analytical workloads. DuckDB's native architecture processes data in columnar batches (chunks) of up to 2048 values, enabling better CPU cache utilization, SIMD optimization opportunities, and reduced per-row overhead.

This is a foundational component required by:
- Scalar User-Defined Functions (UDFs)
- Table-Valued User-Defined Functions
- Appender API optimizations
- Query result streaming
- Arrow integration

## What Changes

### Core Types

1. **DataChunk**: Container holding multiple Vector columns
   - Capacity of 2048 rows per chunk (matching DuckDB's VECTOR_SIZE)
   - Column projection support for sparse access patterns
   - Size management (get/set row count)
   - Value access by column and row index

2. **Vector**: Internal columnar data representation
   - Support for 32 DuckDB types (excluding INVALID, UHUGEINT, BIT, ANY, BIGNUM which are unsupported)
   - NULL bitmap management via validity mask
   - Type-specific getter/setter callbacks
   - Nested type support: LIST, STRUCT, UNION, MAP, ARRAY (all fully supported with getters and setters)
   - Memory-efficient storage layout

3. **Row**: Individual row accessor within a DataChunk
   - Projection-aware column access
   - Generic type-safe value setting via `SetRowValue[T]`
   - Reference to parent chunk for efficient batch operations

### Public API (matching duckdb-go exactly)

```go
// DataChunk operations
func GetDataChunkCapacity() int
func (c *DataChunk) GetSize() int
func (c *DataChunk) SetSize(size int) error
func (c *DataChunk) GetValue(colIdx, rowIdx int) (any, error)
func (c *DataChunk) SetValue(colIdx, rowIdx int, val any) error
func SetChunkValue[T any](chunk DataChunk, colIdx, rowIdx int, val T) error  // Note: pass-by-value for consistency with duckdb-go

// Row operations
func (r Row) IsProjected(colIdx int) bool
func (r Row) SetRowValue(colIdx int, val any) error
func SetRowValue[T any](row Row, colIdx int, val T) error
```

## Impact

- **Affected specs**: execution-engine, appender-api, type-system
- **Affected code**: New files `data_chunk.go`, `vector.go`, `row.go`; modifications to appender and result handling
- **Dependencies**: Requires existing TypeInfo system from type-system spec
- **Consumers**: Scalar UDFs, Table UDFs, Query Appender, Appender optimizations

## Breaking Changes

None. This adds new functionality without modifying existing APIs.

## Performance Considerations

- Batch operations should achieve O(n) with minimal per-row overhead
- Memory allocation amortized across chunk lifetime via buffer reuse
- NULL checks use efficient bitmap operations
- Type conversions use fast paths for primitive types (int64, float64, string, []byte)
- Unsafe pointer operations for high-performance memory access
