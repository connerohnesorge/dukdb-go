# Change: Complete Apache Arrow Integration with View Registration and Zero-Copy Optimization

## Why

The current `arrow.go` implementation (640 lines) provides solid Arrow query execution with:
- `NewArrowFromConn` factory ✅
- `QueryContext` returning streaming `RecordReader` ✅
- Complete type mapping for all 37 DuckDB types (`duckdbTypeToArrow`) ✅
- Quartz clock integration for deterministic testing ✅
- Memory allocator support ✅

However, it lacks two key features from the duckdb-go reference implementation:

1. **RegisterView** (`duckdb-go/arrow.go:130-168`) - Register external Arrow data as a queryable table
   - duckdb-go uses CGO with `arrowmapping.ArrowScan` to bind Arrow streams
   - Our pure Go implementation needs an alternative approach

2. **Bidirectional Type Mapping** - Arrow → DuckDB conversion
   - `duckdbTypeToArrow` exists (lines 311-444)
   - `arrowToDuckDBType` is missing (needed for RegisterView)

3. **Zero-Copy Optimization** - Current implementation uses row-by-row builders
   - `buildNextBatch` (lines 260-309) reads row-by-row via `rows.Next(dest)`
   - Appends to builders element-by-element
   - No buffer sharing between DuckDB vectors and Arrow arrays

The duckdb-go reference uses CGO for direct memory access:
```go
// duckdb-go/arrow.go:326-344
chunk := mapping.FetchChunk(r.res)
rec, ed := arrowmapping.DataChunkToArrowArray(r.opts, r.schema, chunk)
```

Our pure Go implementation needs efficient alternatives without CGO.

## What Changes

### 1. RegisterView Implementation (arrow.go lines 640+)

Add `RegisterView` using replacement scan callback (per existing `replacement_scan.go` infrastructure):

```go
// RegisterView creates a virtual table backed by Arrow data
func (a *Arrow) RegisterView(reader array.RecordReader, name string) (release func(), err error) {
    // Convert Arrow schema to DuckDB columns
    columns, err := arrowSchemaToDuckDB(reader.Schema())
    if err != nil {
        return nil, err
    }

    // Create table source with clock propagation for deterministic testing
    source := &arrowTableSource{
        reader:  reader,
        schema:  reader.Schema(),
        columns: columns,
        clock:   a.clock, // Propagate clock for deterministic testing
    }

    // Create ChunkTableFunction that binds to our source
    tableFunc := ChunkTableFunction{
        Config: TableFunctionConfig{},
        BindArguments: func(named map[string]any, args ...any) (ChunkTableSource, error) {
            return source, nil
        },
    }

    // Register via replacement scan callback for "FROM view_name" syntax
    callback := func(tableName string) (string, []any, error) {
        if tableName == name {
            return name, nil, nil // Return function name matching registered function
        }
        return "", nil, nil // No replacement
    }

    if err := RegisterReplacementScan(a.conn.connector, callback); err != nil {
        return nil, err
    }

    // Register the table function itself
    if err := RegisterTableUDF(a.conn, name, tableFunc); err != nil {
        return nil, err
    }

    return func() {
        // Cleanup: unregister and release
        UnregisterTableUDF(a.conn, name)
        reader.Release()
    }, nil
}
```

### 2. Bidirectional Type Mapping (arrow.go NEW section)

Add reverse type mapping for Arrow → DuckDB:

```go
// arrowToDuckDBType converts an Arrow DataType to DuckDB TypeInfo
func arrowToDuckDBType(dt arrow.DataType) (TypeInfo, error) {
    switch dt.ID() {
    case arrow.BOOL:
        return NewTypeInfo(TYPE_BOOLEAN)
    case arrow.INT8:
        return NewTypeInfo(TYPE_TINYINT)
    // ... all 37 types
    case arrow.LIST:
        listType := dt.(*arrow.ListType)
        child, err := arrowToDuckDBType(listType.Elem())
        if err != nil {
            return nil, err
        }
        return NewListInfo(child)
    // ... nested types
    }
}

// arrowSchemaToDuckDB converts Arrow schema to DuckDB column info
func arrowSchemaToDuckDB(schema *arrow.Schema) ([]ColumnInfo, error) {
    columns := make([]ColumnInfo, len(schema.Fields()))
    for i, field := range schema.Fields() {
        typeInfo, err := arrowToDuckDBType(field.Type)
        if err != nil {
            return nil, fmt.Errorf("field %s: %w", field.Name, err)
        }
        columns[i] = ColumnInfo{Name: field.Name, T: typeInfo}
    }
    return columns, nil
}
```

### 3. DataChunk Conversion (NEW: arrow_convert.go)

Add conversion utilities (copy-based for safety and correctness):

```go
// DataChunkToRecordBatch converts a DataChunk to Arrow RecordBatch
// Uses copy semantics for memory safety (Arrow and Go have different buffer management)
func DataChunkToRecordBatch(chunk DataChunk, schema *arrow.Schema, alloc memory.Allocator) (arrow.Record, error) {
    arrays := make([]arrow.Array, chunk.ColumnCount())

    for i := 0; i < chunk.ColumnCount(); i++ {
        vec, err := chunk.GetColumn(i)
        if err != nil {
            return nil, err
        }

        arr, err := vectorToArrowArray(vec, schema.Field(i).Type, alloc)
        if err != nil {
            return nil, err
        }
        arrays[i] = arr
    }

    return array.NewRecord(schema, arrays, int64(chunk.GetSize())), nil
}

// vectorToArrowArray converts a DuckDB Vector to Arrow Array (copy semantics)
// NOTE: Zero-copy is NOT used because:
// - BOOLEAN: Arrow uses bit-packing (1 bit/value), Go uses []bool (1 byte/value)
// - Strings: Arrow uses offset arrays, Go uses individual string allocations
// - Memory safety: Arrow buffer lifetimes differ from Go GC
func vectorToArrowArray(vec Vector, dt arrow.DataType, alloc memory.Allocator) (arrow.Array, error) {
    // Copy data element-by-element into Arrow builders
    // This is safe but slower than CGO-based zero-copy
}

// recordBatchToDataChunk converts Arrow RecordBatch to DataChunk (copy semantics)
func recordBatchToDataChunk(record arrow.Record, chunk *DataChunk) error {
    chunk.SetSize(int(record.NumRows()))
    for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
        arr := record.Column(colIdx)
        vec, _ := chunk.GetColumn(colIdx)
        if err := arrowArrayToVector(arr, vec); err != nil {
            return err
        }
    }
    return nil
}
```

### 4. ArrowTableSource for RegisterView

New internal type implementing `ChunkTableSource` interface (per `table_udf.go:101-108`):

```go
type arrowTableSource struct {
    reader  array.RecordReader
    schema  *arrow.Schema
    columns []ColumnInfo
    current arrow.Record
    clock   quartz.Clock // For deterministic testing
    ctx     context.Context
}

// sequentialTableSource interface methods
func (s *arrowTableSource) ColumnInfos() []ColumnInfo {
    return s.columns
}

func (s *arrowTableSource) Cardinality() *CardinalityInfo {
    return nil // Unknown cardinality for streaming Arrow data
}

func (s *arrowTableSource) Init() {
    // No initialization needed
}

// ChunkTableSource.FillChunk - returns error only, signals end by setting chunk size to 0
// This matches the actual interface: FillChunk(*DataChunk) error
func (s *arrowTableSource) FillChunk(chunk *DataChunk) error {
    // Check deadline using deterministic clock
    if s.ctx != nil {
        if deadline, ok := s.ctx.Deadline(); ok {
            if s.clock.Until(deadline) <= 0 {
                return context.DeadlineExceeded
            }
        }
    }

    // Tag for deterministic trapping
    _ = s.clock.Now() // Tagged as "Arrow", "view", "fillChunk" in test setup

    if !s.reader.Next() {
        if err := s.reader.Err(); err != nil {
            return err
        }
        // Signal end of data by setting chunk size to 0 (per table_udf.go:104)
        chunk.SetSize(0)
        return nil
    }

    s.current = s.reader.Record()

    // Convert Arrow Record to DataChunk (copy semantics for safety)
    return recordBatchToDataChunk(s.current, chunk)
}
```

## Impact

- **Affected specs**: arrow-integration (MODIFIED)
- **Affected code**:
  - `arrow.go` (add ~200 lines: RegisterView, arrowToDuckDBType, arrowSchemaToDuckDB)
  - NEW: `arrow_convert.go` (~400 lines: conversion utilities with copy semantics)
  - NEW: `arrow_view.go` (~150 lines: arrowTableSource implementation)
- **Dependencies**:
  - `table_udf.go` ✅ (ChunkTableSource interface)
  - `replacement_scan.go` ✅ (for `FROM view_name` syntax)
  - `data_chunk.go` ✅ (DataChunk for conversion)
  - `vector.go` ✅ (Vector access)
  - `type_info.go` ✅ (TypeInfo construction)
- **Build tag**: Maintain `//go:build duckdb_arrow`
- **Performance expectation**: Copy-based conversion is slower than CGO zero-copy, but correct and safe. Expected ~5-10x slower than duckdb-go for large datasets. Acceptable for interop use cases.

## Breaking Changes

None. All changes are additive:
- `RegisterView` is new functionality
- `arrowToDuckDBType` is new utility
- Existing APIs (`QueryContext`, `Query`, etc.) unchanged

## Reference Implementation Analysis

**duckdb-go/arrow.go RegisterView** (lines 130-168):
```go
func (a *Arrow) RegisterView(reader array.RecordReader, name string) (release func(), err error) {
    stream := C.calloc(1, C.sizeof_struct_ArrowArrayStream)
    cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(stream))

    arrowStream := arrowmapping.ArrowStream{Ptr: unsafe.Pointer(stream)}
    if arrowmapping.ArrowScan(a.conn.conn, name, arrowStream) == mapping.StateError {
        return nil, errArrowScan
    }
    return release, nil
}
```

Our pure Go approach replaces CGO ArrowScan with:
1. Convert Arrow schema to DuckDB TypeInfo (new `arrowToDuckDBType`)
2. Create ChunkTableSource that reads from RecordReader
3. Register as table function (existing infrastructure)
4. On query, convert Arrow Records to DataChunks on-the-fly

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`:

**Existing Support** (arrow.go lines 42-63):
- `WithClock(clock quartz.Clock)` for deterministic timing ✅
- Deadline checking in `QueryContext` uses clock ✅

**Additions Needed**:
- Tag RegisterView operations with clock: `mClock.Now("Arrow", "view", "register")`
- Test view registration with traps for concurrent access
- Test data streaming with deterministic timing

Test pattern:
```go
func TestArrowViewDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)

    arrow := NewArrowFromConn(conn).WithClock(mClock)

    // Create Arrow data with known values
    schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
    builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
    record := builder.NewRecord()

    reader := array.NewRecordReader(schema, []arrow.Record{record})

    release, err := arrow.RegisterView(reader, "test_view")
    require.NoError(t, err)
    defer release()

    // Query the view
    rows, err := db.Query("SELECT * FROM test_view")
    require.NoError(t, err)

    // Verify data matches
    var ids []int32
    for rows.Next() {
        var id int32
        rows.Scan(&id)
        ids = append(ids, id)
    }
    assert.Equal(t, []int32{1, 2, 3}, ids)
}
```
