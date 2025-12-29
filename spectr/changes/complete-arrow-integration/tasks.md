# Tasks: Complete Arrow Integration

## 1. Arrow → DuckDB Type Mapping

- [ ] 1.1 Create `arrowToDuckDBType(dt arrow.DataType) (TypeInfo, error)` in `arrow.go`
  - Implement switch on `dt.ID()` for all Arrow type IDs
  - Handle primitive types: BOOL, INT8-64, UINT8-64, FLOAT32/64
  - Handle string types: STRING, BINARY, LARGE_STRING, LARGE_BINARY
  - Handle temporal types: DATE32, TIME64, TIMESTAMP (all units)

- [ ] 1.2 Implement nested type mapping
  - LIST → ListInfo with recursive child mapping
  - FIXED_SIZE_LIST → ArrayInfo with size
  - STRUCT → StructInfo with field names and types
  - MAP → MapInfo with key/value types
  - DENSE_UNION → UnionInfo with members

- [ ] 1.3 Implement special type mapping
  - DECIMAL128 → DecimalDetails or HUGEINT (check scale=0)
  - FIXED_SIZE_BINARY(16) → UUID (check metadata)
  - DICTIONARY → EnumDetails (string dictionary only)
  - MONTH_DAY_NANO_INTERVAL → Interval

- [ ] 1.4 Create `arrowSchemaToDuckDB(schema *arrow.Schema) ([]ColumnInfo, error)`
  - Iterate schema fields, map each to TypeInfo
  - Preserve field names
  - Propagate errors with field context

- [ ] 1.5 Add unit tests for type mapping
  - Test all 37 DuckDB types round-trip
  - Test nested types: LIST(LIST(INT)), STRUCT(a INT, b VARCHAR)
  - Test error cases: unsupported types, invalid precision

## 2. Arrow → DataChunk Conversion

- [ ] 2.1 Create `arrow_convert.go` with build tag
  - Add `//go:build duckdb_arrow` header
  - Import Arrow packages

- [ ] 2.2 Implement `recordToChunk(record arrow.Record, chunk DataChunk) (bool, error)`
  - Set chunk size from record num rows
  - Iterate columns, convert each Arrow array to Vector

- [ ] 2.3 Implement `arrowArrayToVector(arr arrow.Array, vec Vector) error`
  - Switch on array type
  - Handle primitives with type assertion and copy
  - Handle NULL values via validity bitmap

- [ ] 2.4 Add validity mask conversion
  - `arrowValidityToDuckDB([]byte, int) []uint64`
  - Handle edge cases: all valid, all null, partial

- [ ] 2.5 Add nested array conversion
  - LIST: Convert offsets and child array
  - STRUCT: Convert each field array
  - MAP: Convert key and value arrays
  - UNION: Convert type codes and child arrays

- [ ] 2.6 Add unit tests for conversion
  - Test each primitive type with sample data
  - Test NULL handling
  - Test nested types
  - Test empty arrays

## 3. RegisterView Implementation

- [ ] 3.1 Create `arrow_view.go` with build tag

- [ ] 3.2 Implement `arrowTableSource` struct
  - Fields: reader, schema, columns, closed
  - Implement `ChunkTableSource` interface

- [ ] 3.3 Implement `ColumnInfos() []ColumnInfo`
  - Return pre-computed column info from Arrow schema

- [ ] 3.4 Implement `Cardinality() *CardinalityInfo`
  - Return nil (unknown cardinality)

- [ ] 3.5 Implement `Init()`
  - No-op for simple implementation

- [ ] 3.6 Implement `FillChunk(chunk DataChunk) (bool, error)`
  - Call `reader.Next()`
  - If false, check `reader.Err()` and return
  - Get `reader.Record()`
  - Call `recordToChunk(record, chunk)`
  - Return true for more data

- [ ] 3.7 Implement `RegisterView(reader, name) (release, error)` on Arrow
  - Convert Arrow schema to columns via `arrowSchemaToDuckDB`
  - Create `arrowTableSource`
  - Register via `conn.RegisterTableFunction(name, source)` or replacement scan
  - Return release function that unregisters and releases reader

- [ ] 3.8 Add unit tests for RegisterView
  - Test simple query: `SELECT * FROM view`
  - Test with parameters: `SELECT * FROM view WHERE id > $1`
  - Test multiple batches
  - Test empty data
  - Test after release (should error)

## 4. Conversion Implementation

- [ ] 4.1 Implement `arrowArrayToVector(arr arrow.Array, vec Vector) error`
  - Switch on Arrow type ID
  - Extract values using Arrow's typed accessors
  - Copy to DuckDB vector element-by-element
  - Handle NULL values via validity bitmap conversion

- [ ] 4.2 Implement validity bitmap conversion
  - `arrowValidityToDuckDB(arrowBitmap []byte, numRows int) []uint64`
  - Handle empty bitmap (all valid) case
  - Convert LSB-first byte bitmap to uint64 bitmap
  - Add unit tests for edge cases (boundary rows, all null, all valid)

- [ ] 4.3 Implement BOOLEAN unpacking
  - Arrow stores 8 bools per byte (bit-packed)
  - DuckDB uses Go `[]bool` (1 byte per value)
  - Must unpack bits to bytes

- [ ] 4.4 Add benchmarks
  - Measure throughput for 1M rows of int64
  - Measure memory allocation
  - Document copy overhead vs theoretical zero-copy
  - Compare to existing arrow.go QueryContext path

## 5. Deterministic Testing

- [ ] 5.1 Add clock integration to RegisterView path
  - Pass clock through `arrowTableSource`
  - Tag operations: `mClock.Now("Arrow", "view", "read")`

- [ ] 5.2 Add deterministic test for view registration
  - Use mock clock
  - Verify no time.Sleep or time.Now usage

- [ ] 5.3 Add trap-based concurrent test
  - Multiple goroutines querying same view
  - Trap on FillChunk to verify ordering
  - Release traps in deterministic order

- [ ] 5.4 Add timeout test
  - Query with deadline via context
  - Advance mock clock past deadline
  - Verify DeadlineExceeded error

- [ ] 5.5 Verify no flaky tests
  - Run test suite 100 times
  - Check for race conditions with `-race`

## 6. Documentation and Polish

- [ ] 6.1 Add godoc comments to all new public functions
  - RegisterView
  - arrowToDuckDBType (if exported)
  - DataChunkToRecordBatch (if exported)

- [ ] 6.2 Add usage examples in doc.go or arrow.go
  - Example: Query to Arrow
  - Example: Register external Arrow data

- [ ] 6.3 Document performance characteristics
  - Which types support zero-copy
  - Expected overhead vs duckdb-go

- [ ] 6.4 Update FEATURE_PARITY_ANALYSIS.md
  - Mark Arrow RegisterView as complete
  - Update percentage

## Validation

- [ ] Run `go test -v -race ./...` with duckdb_arrow tag
- [ ] Run `go test -v -race -count=100 ./arrow_test.go`
- [ ] Verify `spectr validate complete-arrow-integration`
- [ ] Memory profile shows no leaks after release()
