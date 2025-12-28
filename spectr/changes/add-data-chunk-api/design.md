## Context

DuckDB uses a vectorized execution model where data is processed in columnar chunks of up to 2048 values. This design provides significant performance advantages:

1. **Cache efficiency**: Columnar layout keeps related data contiguous in memory
2. **SIMD optimization**: Batch operations enable compiler vectorization
3. **Reduced overhead**: Per-chunk processing vs per-row processing
4. **NULL handling**: Bitmap validity masks are more space-efficient than sentinel values

The pure Go dukdb-go driver must implement this pattern without CGO dependencies while maintaining API compatibility with the reference duckdb-go implementation.

**Stakeholders**: Users implementing UDFs, high-performance data loading, Arrow integration

**Constraints**:
- No CGO allowed (pure Go requirement)
- Must match duckdb-go public API exactly
- Must work in constrained environments (TinyGo, WASM)
- Performance within 2x of CGO implementation

## Goals / Non-Goals

### Goals
- Provide vectorized data access matching DuckDB's DataChunk semantics
- Support all 45 DuckDB types including nested types
- Enable efficient NULL handling via bitmap operations
- Maintain thread-safety for concurrent read access
- API compatibility with duckdb-go for drop-in replacement

### Non-Goals
- SIMD intrinsics (Go lacks portable SIMD support)
- Memory-mapped storage (handled by storage layer)
- Parallel chunk processing (consumer responsibility)
- Chunk-level compression (out of scope)

## Decisions

### Decision 1: Memory Layout Strategy

**What**: Use Go slices with unsafe.Pointer for columnar data storage

**Why**:
- Native Go slices provide automatic memory management
- unsafe.Pointer enables efficient type reinterpretation without allocation
- Avoids CGO while maintaining performance

**Alternatives considered**:
- Pure reflect-based access: Too slow (10-100x overhead)
- Custom allocator: Complex and error-prone in Go
- Arena allocation: Go 1.20+ arena is experimental

**Implementation**:
```go
type vector struct {
    dataSlice any           // Concrete typed slice ([]int64, []float64, etc.)
    dataPtr   unsafe.Pointer // Raw pointer for generic access
    maskBits  []uint64       // Validity bitmap (1 bit per value)
    typ       Type           // DuckDB type
    getFn     fnGetValue     // Type-specific getter
    setFn     fnSetValue     // Type-specific setter
}
```

### Decision 2: Type-Specific Callbacks

**What**: Use function pointers for type-specific get/set operations

**Why**:
- Avoids switch statements in hot paths
- Enables compiler inlining for simple types
- Matches duckdb-go architecture for compatibility

**Implementation**:
```go
type fnGetValue func(vec *vector, rowIdx int) any
type fnSetValue func(vec *vector, rowIdx int, val any) error

// Type-specific initialization
func initNumeric[T numericType](vec *vector, t Type) {
    vec.getFn = func(vec *vector, rowIdx int) any {
        if vec.isNull(rowIdx) {
            return nil
        }
        return getPrimitive[T](vec, rowIdx)
    }
    vec.setFn = func(vec *vector, rowIdx int, val any) error {
        if val == nil {
            return vec.setNull(rowIdx)
        }
        return setNumeric[T](vec, rowIdx, val)
    }
}
```

### Decision 3: NULL Bitmap Implementation

**What**: Use []uint64 slice with bit manipulation for validity masks

**Why**:
- Space efficient: 1 bit per value vs 1 byte
- Batch NULL checks: Process 64 values per word
- Standard pattern in columnar databases

**Implementation**:
```go
const bitsPerWord = 64

func (vec *vector) isNull(rowIdx int) bool {
    wordIdx := rowIdx / bitsPerWord
    bitIdx := rowIdx % bitsPerWord
    return (vec.maskBits[wordIdx] & (1 << bitIdx)) == 0
}

func (vec *vector) setNull(rowIdx int) {
    wordIdx := rowIdx / bitsPerWord
    bitIdx := rowIdx % bitsPerWord
    vec.maskBits[wordIdx] &^= (1 << bitIdx)
}

func (vec *vector) setValid(rowIdx int) {
    wordIdx := rowIdx / bitsPerWord
    bitIdx := rowIdx % bitsPerWord
    vec.maskBits[wordIdx] |= (1 << bitIdx)
}
```

### Decision 4: Nested Type Handling

**What**: Recursive vector initialization for nested types (LIST, STRUCT, MAP, ARRAY, UNION)

**Why**:
- DuckDB's nested types are compositional
- Child vectors share the same lifecycle as parent
- Enables uniform access patterns

**Implementation**:
```go
type vector struct {
    // ... base fields
    childVectors []vector       // Child vectors for nested types
    structEntries []StructEntry // Field metadata for STRUCT
    arrayLength   int           // Fixed size for ARRAY type
    namesDict     map[string]uint32 // ENUM/UNION name mapping
}

func (vec *vector) initList(logicalType TypeInfo, colIdx int) error {
    childType := logicalType.ChildType()
    vec.childVectors = make([]vector, 1)
    if err := vec.childVectors[0].init(childType, colIdx); err != nil {
        return err
    }
    vec.getFn = func(v *vector, rowIdx int) any {
        return v.getList(rowIdx)
    }
    vec.setFn = func(v *vector, rowIdx int, val any) error {
        return setList(v, rowIdx, val)
    }
    return nil
}
```

### Decision 5: Column Projection Support

**What**: Optional projection mapping for sparse column access

**Why**:
- Table UDFs may only fill certain columns
- Avoids unnecessary computation for unused columns
- Matches duckdb-go behavior

**Implementation**:
```go
type DataChunk struct {
    columns    []vector
    projection []int  // nil = all columns, [-1] = unprojected
    size       int
}

func (c *DataChunk) SetValue(colIdx, rowIdx int, val any) error {
    // Rewrite column index through projection
    actualCol, err := c.resolveProjection(colIdx)
    if errors.Is(err, errUnprojectedColumn) {
        return nil // Silently ignore unprojected columns
    }
    if err != nil {
        return err
    }
    return c.columns[actualCol].setFn(&c.columns[actualCol], rowIdx, val)
}
```

## Risks / Trade-offs

### Risk 1: unsafe.Pointer Usage
**Risk**: Incorrect pointer arithmetic could cause memory corruption
**Mitigation**:
- Comprehensive test coverage with race detector
- Fuzzing for boundary conditions
- Clear documentation of unsafe usage patterns

### Risk 2: Performance Overhead
**Risk**: Pure Go implementation may be slower than CGO
**Mitigation**:
- Benchmark against CGO implementation
- Profile and optimize hot paths
- Document expected performance characteristics
**Acceptable**: Within 2x of CGO performance

### Risk 3: Memory Leaks in Nested Types
**Risk**: Recursive structures could leak if not properly cleaned up
**Mitigation**:
- Clear ownership model: parent owns children
- Explicit close() methods with recursive cleanup
- Integration tests verifying memory cleanup

### Risk 4: Type Conversion Errors
**Risk**: Incorrect type coercion could produce wrong values
**Mitigation**:
- Type-specific validation in setters
- Clear error messages with type information
- Unit tests for all 45 types with edge cases

## Migration Plan

This is a new capability with no migration required. The implementation adds new files without modifying existing code paths.

**Rollout steps**:
1. Implement core vector types (primitives, strings, blobs)
2. Add nested type support (LIST, STRUCT, MAP)
3. Implement DataChunk container
4. Add Row accessor type
5. Integrate with existing Appender (optional optimization)
6. Performance benchmarking and optimization

**Rollback**: Remove new files; no existing functionality affected.

### Decision 6: Clock Injection for Temporal Types

**What**: Inject `quartz.Clock` for all temporal type operations

**Why**: Per deterministic-testing spec, all time-dependent code must use injected clock for testability

**Implementation**:
```go
type DataChunk struct {
    columns []vector
    clock   quartz.Clock  // Default: quartz.NewReal()
}

func NewDataChunk(types []TypeInfo) *DataChunk {
    return &DataChunk{
        columns: initColumns(types),
        clock:   quartz.NewReal(),
    }
}

func (c *DataChunk) WithClock(clk quartz.Clock) *DataChunk {
    c.clock = clk
    // Propagate to temporal vectors
    for i := range c.columns {
        if c.columns[i].isTemporal() {
            c.columns[i].clock = clk
        }
    }
    return c
}

// Temporal vector uses injected clock
func (vec *vector) initTimestamp() {
    vec.getFn = func(v *vector, rowIdx int) any {
        // Uses v.clock for any time-relative operations
        return v.getTimestamp(rowIdx)
    }
}
```

**Test Pattern**:
```go
func TestTimestampVectorDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC))

    chunk := NewDataChunk([]TypeInfo{TimestampType}).WithClock(mClock)
    // All timestamp operations are now deterministic
}
```

## Open Questions

1. **Chunk pooling**: Should we implement chunk reuse for repeated operations?
   - Deferred: Implement basic version first, add pooling if benchmarks show allocation overhead

2. **Parallel filling**: Should DataChunk support concurrent writes to different columns?
   - Deferred: Current design assumes single-writer; parallel support can be added later

3. **Streaming interface**: Should we add iterator-based chunk access?
   - Deferred: Focus on random-access API first; streaming can layer on top
