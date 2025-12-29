# Design: Complete Arrow Integration

## Context

The Arrow integration provides interoperability with the Apache Arrow ecosystem (pandas, Polars, Apache Spark, etc.). The current implementation in `arrow.go` (640 lines) handles query execution returning Arrow RecordReaders. This design adds RegisterView (external Arrow data as tables) and optimizes conversions.

**Stakeholders**:
- Analytics users needing Arrow interop (data pipelines, ML workflows)
- Users with external Arrow data sources (Parquet files via PyArrow, etc.)

**Constraints**:
- Must remain pure Go (no CGO) - cannot use `arrowmapping.ArrowScan`
- Must maintain API compatibility with duckdb-go signatures
- Must integrate with quartz for deterministic testing
- Build tag `duckdb_arrow` must gate all Arrow functionality
- Arrow dependency: `github.com/apache/arrow-go/v18`

## Goals / Non-Goals

**Goals**:
1. Implement `RegisterView` to query external Arrow data
2. Add bidirectional type mapping (Arrow ↔ DuckDB)
3. Optimize DataChunk ↔ Arrow conversion with buffer sharing where possible
4. Maintain 100% API compatibility with duckdb-go Arrow interface
5. Full deterministic testing support via quartz

**Non-Goals**:
1. Match CGO performance exactly (pure Go will be slower)
2. Support Arrow Flight protocol (network streaming)
3. Add Arrow-native storage format (Arrow as persistence layer)
4. Support all Arrow extension types (only standard types)

## Decisions

### Decision 1: RegisterView Implementation Strategy

**Options**:
A. Replacement Scan + Table UDF - Combined approach for `FROM view_name` syntax
B. Table UDF only - Requires `FROM table_function()` syntax
C. Virtual Table - Create proper virtual table abstraction

**Choice**: A - Replacement Scan + Table UDF (combined approach)

**Rationale**:
- Replacement scan enables natural `FROM view_name` SQL syntax
- ChunkTableSource provides the actual data iteration
- Uses existing `replacement_scan.go` and `table_udf.go` infrastructure
- Virtual table is over-engineering for this use case

**Trade-offs**:
- ✅ Natural SQL syntax (`SELECT * FROM view_name`)
- ✅ Reuses existing tested infrastructure
- ⚠️ Requires both replacement scan callback AND table function registration
- ⚠️ Cleanup must unregister both
- ⚠️ **Known Limitation**: Replacement scan is per-connector (single callback), not per-view. Multiple simultaneous views require a view registry that the single replacement scan callback dispatches to.

**Multi-View Support**:
```go
// Internal view registry for multiple simultaneous views
var viewRegistry = struct {
    sync.RWMutex
    views map[string]*arrowTableSource
}{views: make(map[string]*arrowTableSource)}

// Single replacement scan callback that dispatches to registered views
func arrowViewReplacementScan(tableName string) (string, []any, error) {
    viewRegistry.RLock()
    defer viewRegistry.RUnlock()
    if _, exists := viewRegistry.views[tableName]; exists {
        return tableName, nil, nil // Redirect to table function
    }
    return "", nil, nil // No replacement
}
```

**Implementation** (matches actual `ChunkTableSource` interface from `table_udf.go:101-108`):
```go
type arrowTableSource struct {
    reader  array.RecordReader
    columns []ColumnInfo
    clock   quartz.Clock // For deterministic testing
    ctx     context.Context
}

func (s *arrowTableSource) ColumnInfos() []ColumnInfo { return s.columns }
func (s *arrowTableSource) Cardinality() *CardinalityInfo { return nil }
func (s *arrowTableSource) Init() {}

// FillChunk returns error only; signals end by setting chunk.SetSize(0)
func (s *arrowTableSource) FillChunk(chunk *DataChunk) error {
    if !s.reader.Next() {
        chunk.SetSize(0) // Signal end of data
        return s.reader.Err()
    }
    return recordBatchToDataChunk(s.reader.Record(), chunk)
}
```

### Decision 2: Arrow → DuckDB Type Mapping

**Options**:
A. Direct ID mapping - Switch on `arrow.Type.ID()`
B. Interface-based - Type-specific converters
C. Table-driven - Lookup table with conversion functions

**Choice**: A - Direct ID mapping (mirrors existing `duckdbTypeToArrow`)

**Rationale**:
- Consistent with existing code style (`duckdbTypeToArrow` uses switch)
- Simple and readable
- No allocation overhead of interface dispatch
- Easy to add new types

**Mapping Table**:

| Arrow Type | DuckDB Type | Notes |
|------------|-------------|-------|
| BOOL | BOOLEAN | Direct |
| INT8/16/32/64 | TINYINT/SMALLINT/INTEGER/BIGINT | Direct |
| UINT8/16/32/64 | UTINYINT/USMALLINT/UINTEGER/UBIGINT | Direct |
| FLOAT32/64 | FLOAT/DOUBLE | Direct |
| STRING | VARCHAR | Direct |
| BINARY | BLOB | Direct |
| DATE32 | DATE | Direct |
| TIME64us | TIME | Microsecond precision |
| TIMESTAMP(unit, tz) | TIMESTAMP_X | Map unit to S/MS/US/NS |
| MONTH_DAY_NANO_INTERVAL | INTERVAL | Direct |
| FIXED_SIZE_BINARY(16) | UUID | Check metadata |
| DECIMAL128 | DECIMAL/HUGEINT | Check scale=0 for HUGEINT |
| LIST | LIST | Recursive |
| FIXED_SIZE_LIST | ARRAY | With size |
| STRUCT | STRUCT | Recursive |
| MAP | MAP | Recursive |
| DENSE_UNION | UNION | With members |
| DICTIONARY | ENUM | String dictionary |

### Decision 3: Conversion Strategy (Copy-First, Optimize Later)

**Options**:
A. Full zero-copy - Share all buffers directly
B. Primitive zero-copy - Share only primitive type buffers (RISK: memory safety)
C. Copy-always - Never share buffers (safest, most maintainable)

**Choice**: C - Copy-always for initial implementation

**Rationale**:
- **Memory Safety**: Arrow buffer lifetimes are managed by Arrow's reference counting; sharing with Go slices risks dangling pointers when Arrow releases
- **BOOLEAN incompatibility**: Arrow packs bools as bits (1 bit/value), DuckDB uses Go `[]bool` (1 byte/value) - NOT layout-compatible
- **Validity bitmap conversion required anyway**: Even "zero-copy" primitives require copying validity
- **String/binary impossible**: Arrow uses int32 offsets into byte buffer; Go `[]string` uses individual allocations
- **Simplicity**: Copy-always is easier to implement, test, and maintain
- **Future optimization**: Can add zero-copy for numeric primitives later if benchmarks show need

**Copy Strategy for Primitives**:
```go
// Copy numeric arrays element-by-element (safe, correct)
var copyableNumericTypes = map[Type]bool{
    TYPE_TINYINT:   true,  // Copy []int8
    TYPE_SMALLINT:  true,  // Copy []int16
    TYPE_INTEGER:   true,  // Copy []int32
    TYPE_BIGINT:    true,  // Copy []int64
    TYPE_UTINYINT:  true,  // Copy []uint8
    TYPE_USMALLINT: true,  // Copy []uint16
    TYPE_UINTEGER:  true,  // Copy []uint32
    TYPE_UBIGINT:   true,  // Copy []uint64
    TYPE_FLOAT:     true,  // Copy []float32
    TYPE_DOUBLE:    true,  // Copy []float64
}

// BOOLEAN requires bit-unpacking (NOT zero-copy compatible)
// Arrow: 1 bit per value, packed into bytes
// DuckDB: 1 byte per value in Go []bool
```

**Validity Mask Conversion** (always required):
- Arrow: `[]byte` bitmap (8 values per byte, LSB-first within each byte)
- DuckDB: `[]uint64` bitmap (64 values per uint64, LSB-first)
- Both use 1=valid, 0=null convention

```go
// Correct validity bitmap conversion
func arrowValidityToDuckDB(arrowBitmap []byte, numRows int) []uint64 {
    if len(arrowBitmap) == 0 {
        return nil // All values valid (no bitmap = all valid in Arrow)
    }

    result := make([]uint64, (numRows+63)/64)
    for row := 0; row < numRows; row++ {
        byteIdx := row / 8
        bitIdx := row % 8
        // Arrow stores LSB-first within bytes
        if arrowBitmap[byteIdx]&(1<<bitIdx) != 0 {
            // Value is valid - set bit in DuckDB bitmap
            wordIdx := row / 64
            bitPos := row % 64
            result[wordIdx] |= 1 << bitPos
        }
    }
    return result
}
```

**Why NOT zero-copy for strings/binary**:
- Arrow STRING: `[offset0, offset1, ..., offsetN]` (int32) + contiguous byte buffer
- Go `[]string`: Each string is separate allocation with pointer + length
- No way to share Arrow's byte buffer as Go strings without copying

### Decision 4: Buffer Ownership Model

**Options**:
A. Arrow owns - DuckDB borrows, Arrow must outlive usage
B. DuckDB owns - Copy all data, Arrow can be released
C. Shared ownership - Reference counting between systems

**Choice**: A - Arrow owns for zero-copy, with explicit lifetime contract

**Rationale**:
- Simplest ownership model
- RegisterView already returns `release func()` for lifetime control
- Caller is responsible for not releasing Arrow data while queries run
- Copy-on-demand for safety when ownership unclear

**Lifetime Contract**:
```go
// User's responsibility:
release, _ := arrow.RegisterView(reader, "my_view")
defer release() // Must call AFTER all queries complete

// Internal tracking:
type arrowTableSource struct {
    reader array.RecordReader
    // Reader MUST outlive all FillChunk calls
}
```

### Decision 5: Error Handling for Type Mismatches

**Options**:
A. Fail fast - Return error on first unsupported type
B. Best effort - Skip unsupported columns with warning
C. Coerce - Convert to nearest supported type

**Choice**: A - Fail fast with descriptive errors

**Rationale**:
- Predictable behavior
- No silent data loss
- Clear error messages for debugging
- Consistent with duckdb-go behavior

**Error Examples**:
```go
errors.New("arrow type DURATION not supported by DuckDB")
errors.New("field 'data': expected STRUCT, got LIST")
errors.New("decimal precision 50 exceeds DuckDB maximum 38")
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance gap vs CGO | Medium | Benchmark, document expectations |
| Buffer lifetime bugs | High | Explicit ownership, tests |
| Type mapping gaps | Medium | Comprehensive test suite |
| Arrow API changes | Low | Pin to v18, document upgrade path |
| Memory leaks | High | Finalizers, strict release patterns |

## Migration Plan

### Phase 1: Type Mapping (Days 1-2)
1. Implement `arrowToDuckDBType` with all primitive types
2. Add unit tests for each type mapping
3. Implement `arrowSchemaToDuckDB` utility

### Phase 2: RegisterView Foundation (Days 3-4)
1. Create `arrowTableSource` implementing `ChunkTableSource`
2. Implement basic `FillChunk` with copy semantics
3. Add `RegisterView` to Arrow struct
4. Test with simple schemas (primitives only)

### Phase 3: Data Conversion (Days 5-7)
1. Implement `recordToChunk` for Arrow → DataChunk
2. Add validity mask conversion
3. Add nested type support (LIST, STRUCT, MAP)
4. Test round-trip: Arrow → Query → Arrow

### Phase 4: Zero-Copy Optimization (Days 8-10)
1. Identify zero-copy eligible paths
2. Add buffer sharing for primitive types
3. Benchmark vs copy path
4. Document performance characteristics

### Phase 5: Testing & Polish (Days 11-14)
1. Deterministic tests with quartz
2. Concurrent access tests
3. Memory leak tests with finalizers
4. Documentation and examples

### Rollback Plan
- Feature flag: `DUKDB_ARROW_VIEW=0` disables RegisterView
- Copy-always fallback if zero-copy causes issues
- All existing Arrow functionality unchanged

## Open Questions

1. **Table function vs replacement scan syntax?**
   - Answer: Use BOTH - replacement scan callback for `FROM view_name` syntax, table UDF for actual data iteration

2. **Arrow extension types?**
   - Support for custom extension types (e.g., PyArrow's `pandas` extensions)?
   - Answer: Not in scope, document as limitation

3. **Concurrent RegisterView?**
   - What if same name registered twice?
   - Answer: Error on duplicate, require explicit unregister first

4. **String/binary conversion?**
   - Arrow uses int32 offsets into byte buffer; Go uses individual string allocations
   - Answer: Always copy - fundamental incompatibility makes zero-copy impossible

5. **BOOLEAN conversion?**
   - Arrow packs 8 bools per byte; Go []bool uses 1 byte per value
   - Answer: Always unpack/copy - layouts are incompatible
