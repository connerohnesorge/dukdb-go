# Implementation Tasks: Vector/DataChunk Low-Level API

## Phase 1: Core Vector Infrastructure (Week 1)

### Task 1: Implement ValidityMask
- [ ] Create `internal/vector/validity.go`
- [ ] Implement NewValidityMask(capacity) function
- [ ] Implement IsValid(row) using bitmap operations
- [ ] Implement SetValid(row, valid) using bit manipulation
- [ ] Implement SetAllValid(count) for initialization
- [ ] Implement CountValid() using bits.OnesCount64
- [ ] **Validation**: Benchmark <50ns per IsValid/SetValid operation

### Task 2: Define Vector Interface
- [ ] Create `internal/vector/vector.go`
- [ ] Define Vector interface with Get(row), Set(row, value), Reset(), Close()
- [ ] Define base vector struct with typ, size, capacity, validity
- [ ] Implement NewVector(TypeInfo, capacity) factory function
- [ ] Add type validation helpers
- [ ] **Validation**: Interface compiles, factory creates correct types

### Task 3: Implement VectorPool
- [ ] Create `internal/vector/pool.go`
- [ ] Implement VectorPool struct with sync.Mutex
- [ ] Implement Get(typ) to retrieve from pool or create new
- [ ] Implement Put(vec) to return vector to pool after Reset()
- [ ] Add global pool instance
- [ ] **Validation**: Benchmark shows 90% allocation reduction vs no pooling

## Phase 2: Primitive Type Vectors (Week 1)

### Task 4: Implement Integer Vectors
- [ ] Create `internal/vector/primitive_vectors.go`
- [ ] Implement Int8Vector, Int16Vector, Int32Vector, Int64Vector
- [ ] Implement Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector
- [ ] Each vector has `values []T` + ValidityMask
- [ ] Implement Get(row) (T, bool) and Set(row, value T)
- [ ] Implement SetNull(row)
- [ ] **Validation**: All integer spec scenarios pass (lines 162-170)

### Task 5: Implement Float Vectors
- [ ] Implement Float32Vector and Float64Vector in `primitive_vectors.go`
- [ ] Handle special values (Inf, -Inf, NaN, -0.0)
- [ ] **Validation**: Float spec scenarios pass (lines 172-176), special values preserved

### Task 6: Implement Boolean Vector
- [ ] Implement BoolVector with `values []bool`
- [ ] **Validation**: Boolean spec scenario passes (lines 157-160)

### Task 7: Implement String and Binary Vectors
- [ ] Implement StringVector with `values []string`
- [ ] Implement BlobVector with `values [][]byte`
- [ ] **Validation**: VARCHAR and BLOB spec scenarios pass (lines 182-190)

### Task 8: Implement Temporal Vectors
- [ ] Implement DateVector (stores int32 days since epoch)
- [ ] Implement TimeVector (stores int64 micros since midnight)
- [ ] Implement TimestampVector (stores int64 micros since epoch)
- [ ] Implement IntervalVector with Interval struct {Months, Days, Micros}
- [ ] Handle timezone conversions for TIMESTAMP_TZ
- [ ] **Validation**: All temporal spec scenarios pass (lines 197-224)

### Task 9: Implement Complex Numeric Vectors
- [ ] Implement HugeIntVector with `values []*big.Int`
- [ ] Implement DecimalVector with Decimal struct {Value *big.Int, Width, Scale uint8}
- [ ] Implement UUIDVector with `values [][16]byte`
- [ ] **Validation**: HUGEINT, DECIMAL, UUID spec scenarios pass (lines 229-242)

## Phase 3: Complex Type Vectors (Week 2)

### Task 10: Implement LIST Vector
- [ ] Create `internal/vector/list_vector.go`
- [ ] Implement ListVector with child *Vector + offsets []uint64
- [ ] Implement Get(row) to extract slice from child using offsets
- [ ] Implement Append(row, elements) to add elements to child
- [ ] Handle empty lists (offsets[row] == offsets[row+1])
- [ ] **Validation**: LIST spec scenarios pass (lines 262-276), nested lists work

### Task 11: Implement STRUCT Vector
- [ ] Create `internal/vector/struct_vector.go`
- [ ] Implement StructVector with children map[string]*Vector
- [ ] Implement Get(row) to return map[string]any
- [ ] Implement Set(row, map[string]any) with field validation
- [ ] Handle nested STRUCT types recursively
- [ ] **Validation**: STRUCT spec scenarios pass (lines 281-290)

### Task 12: Implement ENUM Vector
- [ ] Create `internal/vector/enum_vector.go`
- [ ] Implement EnumVector with indices []uint32 + dict []string
- [ ] Initialize dict from EnumDetails (P0-1a)
- [ ] Implement Set(row, string) with dictionary lookup
- [ ] Return error for invalid enum values
- [ ] **Validation**: ENUM spec scenarios pass (lines 248-257)

### Task 13: Implement ARRAY Vector
- [ ] Create `internal/vector/array_vector.go`
- [ ] Implement ArrayVector with child *Vector + size uint32
- [ ] Implement Get(row) to extract fixed-size slice from child
- [ ] Implement Set(row, []any) with size validation
- [ ] **Validation**: ARRAY spec scenarios pass (lines 309-318), size validation works

### Task 14: Implement MAP Vector
- [ ] Create `internal/vector/map_vector.go`
- [ ] Implement MapVector as wrapper around ListVector
- [ ] Child is STRUCT<key, value> (uses StructVector from Task 11)
- [ ] Implement Get(row) to return Map (map[any]any)
- [ ] Implement Set(row, Map) to convert to LIST<STRUCT>
- [ ] **Validation**: MAP spec scenarios pass (lines 295-303)

### Task 15: Implement UNION Vector
- [ ] Create `internal/vector/union_vector.go`
- [ ] Implement UnionVector with tags []uint8 + children map[string]*Vector
- [ ] Implement Get(row) to return UnionValue {Tag, Value}
- [ ] Implement Set(row, UnionValue) with tag validation
- [ ] **Validation**: UNION spec scenarios pass (lines 323-332)

## Phase 4: DataChunk Implementation (Week 2)

### Task 16: Implement DataChunk Core
- [ ] Create `data_chunk.go` in root package (public API)
- [ ] Implement DataChunk struct with vectors []*Vector
- [ ] Define VectorSize constant = 2048
- [ ] Implement NewDataChunk(types []TypeInfo) *DataChunk
- [ ] Implement GetSize() and SetSize(size) with validation
- [ ] **Validation**: Capacity and size management spec scenarios pass (lines 7-38)

### Task 17: Implement DataChunk Value Access
- [ ] Implement GetValue(col, row) (any, error)
- [ ] Implement SetValue(col, row, value) error
- [ ] Add bounds checking (col < len(vectors), row < size)
- [ ] Handle NULL values correctly
- [ ] **Validation**: Value access spec scenarios pass (lines 44-78)

### Task 18: Implement Column Projection
- [ ] Add projection []int field to DataChunk
- [ ] Implement getPhysicalColumn(logicalCol) int helper
- [ ] Update GetValue/SetValue to apply projection
- [ ] Unprojected columns (projection[i] == -1) ignored on set
- [ ] **Validation**: Projection spec scenarios pass (lines 110-124)

### Task 19: Implement DataChunk Lifecycle
- [ ] Implement Reset() to clear size and reset vectors (preserves structure)
- [ ] Implement Close() to cleanup all vectors
- [ ] **Validation**: Reset and cleanup spec scenarios pass (lines 373-392)

### Task 20: Implement Row Accessor
- [ ] Create `row.go` in root package
- [ ] Implement Row struct with chunk *DataChunk + index uint64
- [ ] Implement IsProjected(col) bool
- [ ] Implement SetRowValue(col, value) error
- [ ] Implement GetRowValue(col) (any, error)
- [ ] **Validation**: Row accessor spec scenarios pass (lines 129-152)

### Task 21: Implement Type-Safe Generic Accessors
- [ ] Implement SetChunkValue[T](chunk, col, row, value T) error
- [ ] Implement SetRowValue[T](row, col, value T) error
- [ ] Add compile-time type safety tests
- [ ] **Validation**: Generic accessor spec scenarios pass (lines 95-105, 147-152)

## Phase 5: Appender API (Week 3)

### Task 22: Implement Appender Core
- [ ] Create `appender.go` in root package (public API)
- [ ] Implement Appender struct with table *Table + chunk *DataChunk + currentRow uint64
- [ ] Implement NewAppender(conn, tableName) (*Appender, error)
- [ ] Get table schema from catalog
- [ ] Initialize DataChunk with table column types
- [ ] **Validation**: Appender creation succeeds for valid table

### Task 23: Implement AppendRow
- [ ] Implement AppendRow(values ...any) error
- [ ] Validate column count matches table schema
- [ ] Set values in chunk at currentRow
- [ ] Increment currentRow
- [ ] Auto-flush when currentRow >= VectorSize
- [ ] **Validation**: Can append single row, values accessible

### Task 24: Implement Flush and Close
- [ ] Implement Flush() error to write chunk to storage
- [ ] Set chunk size to currentRow before flush
- [ ] Call table.AppendChunk(chunk)
- [ ] Reset chunk and currentRow after flush
- [ ] Implement Close() to flush remaining rows + cleanup
- [ ] **Validation**: Flush writes data, Close finalizes

### Task 25: Implement Type Coercion
- [ ] Add type coercion in SetValue (int → int64, etc.)
- [ ] Handle NULL values (nil input)
- [ ] Return error for incompatible types
- [ ] **Validation**: Type coercion spec scenarios pass (lines 80-90)

## Phase 6: Integration and Testing (Week 3)

### Task 26: Unit Tests for All Vector Types
- [ ] Test each primitive vector type (int8, int16, int32, ..., float32, float64, bool, string, blob)
- [ ] Test temporal vectors (date, time, timestamp, interval)
- [ ] Test complex numeric vectors (hugeint, decimal, uuid)
- [ ] Test all complex vectors (list, struct, enum, array, map, union)
- [ ] Test NULL handling for all types
- [ ] **Validation**: All 37 types tested, all 393 spec scenarios pass

### Task 27: DataChunk Integration Tests
- [ ] Test multi-column DataChunk with all types
- [ ] Test column projection with reordering
- [ ] Test Reset() and Close() lifecycle
- [ ] Test concurrent access (detect data races with -race)
- [ ] **Validation**: Integration tests pass, no data races

### Task 28: Appender Integration Tests
- [ ] Test appending 1M rows
- [ ] Test auto-flush at 2048 row boundary
- [ ] Test type validation errors
- [ ] Test NULL value handling
- [ ] **Validation**: 1M row append completes in <1 second

### Task 29: Performance Benchmarks
- [ ] Benchmark append throughput (rows/second)
- [ ] Benchmark scan throughput (rows/second)
- [ ] Benchmark validity bitmap operations (ns/op)
- [ ] Benchmark vector pool vs allocation
- [ ] Benchmark memory usage with pprof
- [ ] **Validation**: Performance within 2x of DuckDB C++ for basic ops

### Task 30: Memory Leak Detection
- [ ] Run pprof heap profiling on 10M row append
- [ ] Verify no memory leaks after Close()
- [ ] Test vector pool returns all vectors
- [ ] **Validation**: Zero leaks detected, stable memory usage

### Task 31: Spec Compliance Verification
- [ ] Run all 393 spec scenarios from data-chunk-api/spec.md
- [ ] Generate coverage report
- [ ] Fix any failing scenarios
- [ ] **Validation**: 100% spec scenario pass rate

## Dependencies

- **Sequential**: Task 1 (ValidityMask) required by all vector tasks
- **Sequential**: Task 2 (Vector Interface) required by all vector tasks
- **Parallel**: Tasks 4-9 (primitive vectors) can be done in parallel
- **Sequential**: Task 10 (LIST) required by Task 14 (MAP uses LIST)
- **Sequential**: Task 11 (STRUCT) required by Task 14 (MAP uses STRUCT)
- **Sequential**: Tasks 16-19 (DataChunk) require Tasks 4-15 (all vectors)
- **Sequential**: Task 22-25 (Appender) require Task 16 (DataChunk)
- **Sequential**: Phase 6 (testing) requires all implementation tasks

## Success Criteria

All tasks completed when:
- [ ] All 37 DuckDB types have working vector implementations
- [ ] DataChunk capacity is exactly 2048 (VectorSize)
- [ ] NULL values handled correctly via validity bitmaps for all types
- [ ] Nested types (LIST, STRUCT, MAP, ARRAY, UNION) work recursively
- [ ] Appender can insert 1M rows in <1 second (benchmark passes)
- [ ] Type-safe generics compile and validate at compile-time
- [ ] Column projection correctly filters unprojected columns
- [ ] Vector pooling reduces allocations by 90% (benchmark passes)
- [ ] All 393 spec scenarios pass (100% compliance)
- [ ] Zero memory leaks detected with pprof
- [ ] Performance within 2x of DuckDB C++ for basic operations
- [ ] No data races detected with `-race` flag
