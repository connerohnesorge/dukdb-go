# Change: Implement Vector/DataChunk Low-Level API

## Why

**Current State**: dukdb-go HAS working Vector/DataChunk/Appender implementations in:
- `vector.go` - Callback-based vector with validity bitmaps
- `data_chunk.go` - Multi-column chunk with projection support
- `appender.go` - Bulk insert with auto-flush
- `row.go` - Row accessor for row-oriented operations

**Problem**: The implementation has design issues:
1. **Type System Integration**: Uses `vectorTypeInfo` wrapper instead of P0-1a TypeInfo interface directly
2. **Missing Lifecycle Methods**: No Vector.Reset() or Vector.Close() for proper resource management
3. **Incorrect Reset Behavior**: DataChunk.reset() sets size=2048 instead of 0, breaking Appender flush
4. **No Vector Pooling**: Excessive allocations without reuse mechanism
5. **ValidityMask Not Abstracted**: Direct `[]uint64` instead of dedicated type (blocks future RLE compression)

**Without These Fixes**:
- Cannot integrate with P0-1a TypeInfo serialization (P0-1b needs TypeInfo.SQLType())
- Memory leaks in nested types (LIST, STRUCT, MAP) without Close()
- Performance degradation from repeated vector allocations
- Cannot implement P1 optimizations (constant vectors, dictionary encoding)

**This Proposal**: Refactor existing implementation to fix design issues while maintaining backward compatibility.

## What

Implement Vector and DataChunk APIs matching DuckDB's columnar architecture:

1. **Vector Implementation** - Columnar storage for single-type data
   - Flat vectors for primitive types
   - Validity bitmaps for NULL handling
   - Child vectors for nested types (LIST, STRUCT, MAP, ARRAY, UNION)
   - Type-specific accessors matching TypeInfo (P0-1a)

2. **DataChunk Implementation** - Collection of vectors with uniform length
   - Fixed capacity of 2048 rows (VECTOR_SIZE)
   - Multi-column storage
   - Size management and validation
   - Column projection support

3. **Appender API** - Efficient bulk data insertion
   - Row-by-row appending
   - Batch flushing
   - Type validation
   - NULL value handling

4. **Row Accessor** - Row-oriented view of columnar data
   - Get/Set values by column index within a row
   - Projection awareness
   - Type-safe generic accessors

5. **Memory Management** - Efficient allocation and cleanup
   - Vector pooling
   - Reset for reuse
   - Proper cleanup on close

**Scope Limitation**: This proposal implements the LOW-LEVEL data structures only. Query execution and operators are in P0-4 (SQL Execution Engine).

## Implementation Strategy

This is a **REFACTORING** proposal, not a greenfield implementation.

**Existing Implementation** (`vector.go`, `data_chunk.go`, `appender.go`, `row.go`):
- ✅ Working vector with validity bitmaps (`maskBits []uint64`)
- ✅ Working DataChunk with projection support
- ✅ Working Appender with auto-flush at 2048 rows
- ✅ Callback-based type dispatch (`getFn`, `setFn`)
- ❌ Uses `vectorTypeInfo` wrapper instead of TypeInfo interface
- ❌ No Vector.Reset() or Vector.Close() methods
- ❌ DataChunk.reset() bug: sets size=2048 instead of 0
- ❌ No vector pooling (excessive allocations)

**Refactoring Goals**:
1. **TypeInfo Integration**: Replace `vectorTypeInfo` with direct TypeInfo usage from P0-1a
   - Enables serialization (P0-1b needs TypeInfo.SQLType())
   - Simplifies type metadata management
2. **ValidityMask Abstraction**: Wrap `[]uint64` in ValidityMask type
   - Enables future RLE compression (P1)
   - Consistent API across all vector types
3. **Lifecycle Methods**: Add Vector.Reset() and Vector.Close()
   - Reset() enables vector pooling (90% allocation reduction)
   - Close() prevents memory leaks in nested types
4. **Fix Reset Behavior**: DataChunk.reset() should set size=0
   - Current bug breaks Appender flush cycle
5. **Add Vector Pooling**: VectorPool with type-specific pools
   - Reduces GC pressure
   - 90% fewer allocations (benchmark target)

**Migration Path** (backward compatible):
- Phase 1: Add ValidityMask type, keep `maskBits` as internal field
- Phase 2: Update vector initialization to use TypeInfo directly
- Phase 3: Add Reset()/Close() methods
- Phase 4: Implement VectorPool
- Phase 5: Fix DataChunk.reset() bug
- Phase 6: Update tests, verify no regressions

**Breaking Changes**: **NONE** (all internal refactoring, public APIs unchanged)

## Impact

### Users
- ✅ **Enables**: Efficient bulk inserts via Appender API
- ✅ **Unlocks**: Vectorized query execution (10-100x faster than row-by-row)
- ✅ **Performance**: Cache-efficient columnar storage
- ⚠️ **Breaking**: None (pure addition, new APIs)

### Codebase
- **Refactored Files** (existing → improved):
  - `vector.go` → Add TypeInfo integration, Reset(), Close()
  - `data_chunk.go` → Fix reset() bug (size=0), add Close()
  - `appender.go` → Update to use TypeInfo.SQLType()
  - `row.go` → Update generic accessors to use pointers
- **New Files**:
  - `internal/vector/validity.go` - ValidityMask type (wraps existing `maskBits`)
  - `internal/vector/pool.go` - VectorPool implementation
- **Dependencies**:
  - **Requires**: P0-1a Core TypeInfo (COMPLETED)
  - **Uses**: TypeInfo.InternalType(), TypeInfo.Details(), TypeInfo.SQLType()
- **Blocks**: P0-4 SQL Execution Engine, Vectorized operators

### Risks
- **Memory Management**: Vectors must be properly pooled/reused
- **Type Safety**: Generic accessors need careful validation
- **Performance**: Must match DuckDB's vectorization efficiency
- **Mitigation**: Comprehensive benchmarks, memory profiling, type validation tests

### Alternatives Considered
1. **Row-oriented storage** - Rejected: 10-100x slower than columnar
2. **Apache Arrow only** - Rejected: Need DuckDB-specific optimizations
3. **CGO to DuckDB vectors** - Rejected: Violates pure Go constraint

## Success Criteria

- [ ] Vector supports all 37 DuckDB types (primitives + 7 complex types)
- [ ] DataChunk capacity is exactly 2048 (VECTOR_SIZE)
- [ ] NULL values correctly handled via validity bitmaps
- [ ] Nested types (LIST, STRUCT, MAP, ARRAY, UNION) work correctly
- [ ] Appender can insert 1M rows in <1 second (benchmark)
- [ ] Type-safe generics (SetChunkValue[T], SetRowValue[T]) compile and validate
- [ ] Column projection correctly filters unprojected columns
- [ ] Memory pooling reduces allocations by 90% (benchmark)
- [ ] All 393 spec scenarios pass
- [ ] Zero memory leaks (verified with pprof)
- [ ] Performance within 2x of DuckDB C++ for basic operations

**Deferred to Future Work**:
- String dictionary compression (P1)
- Run-length encoding for constant vectors (P1)
- Advanced vector formats (P1)

## Dependencies

### Required Before
- ✅ P0-1a Core TypeInfo (TypeInfo interface, 8 constructors, 7 TypeDetails)
- ✅ Type enum (TYPE_INTEGER, TYPE_VARCHAR, etc.)

### Enables After
- P0-3 Statement Introspection (Stmt.ColumnTypeInfo uses vectors)
- P0-4 SQL Execution Engine (operators use DataChunk)
- Vectorized aggregations
- Efficient bulk loading
- Arrow integration (P1)

## Related Specs

- `data-chunk-api` - IMPLEMENTS (this change implements the existing spec)
- `type-system` - USES TypeInfo for vector type metadata
- `appender` - IMPLEMENTS Appender API

## Rollout Plan

### Phase 1: Core Vector Infrastructure (Week 1)
- Implement validity bitmap
- Implement flat vector for primitive types
- Type registration and validation

### Phase 2: Complex Type Vectors (Week 1-2)
- LIST vector with child vectors
- STRUCT vector with named fields
- MAP vector (uses LIST of STRUCT internally)
- ARRAY vector with fixed size
- UNION vector with tagged values

### Phase 3: DataChunk Implementation (Week 2)
- Multi-column storage
- Size management
- Column projection
- Reset and cleanup

### Phase 4: Appender API (Week 2-3)
- Row-by-row append
- Batch flushing
- Type coercion
- NULL handling

### Phase 5: Performance Optimization (Week 3)
- Vector pooling
- Memory profiling
- Benchmarking vs DuckDB
- Cache optimization

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec validated (spectr validate implement-vector-datachunk-api)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed (P0-1a complete)
- [ ] Testing strategy approved (all 393 spec scenarios + benchmarks)
