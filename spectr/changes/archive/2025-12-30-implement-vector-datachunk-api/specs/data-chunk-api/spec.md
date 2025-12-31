# Data Chunk API Specification Delta

## Summary

This change **IMPLEMENTS** the existing `data-chunk-api` specification without modifications. The spec is already comprehensive (393 lines, 31 requirements, 119 scenarios) and requires no changes.

**Implementation Scope**: This proposal implements ALL requirements from `spectr/specs/data-chunk-api/spec.md`:
- Vector support for all 37 DuckDB types
- DataChunk with 2048 capacity (VECTOR_SIZE)
- Appender API for bulk inserts
- Row accessor for row-oriented operations
- Column projection support
- NULL handling via validity bitmaps
- Type-safe generic accessors

**No Spec Modifications Required**: The existing specification is complete and well-designed. This change purely implements what's already specified.

## Integration with P0-1a Core TypeInfo

The implementation uses TypeInfo (from P0-1a) for:
- Vector type metadata (vector.typ TypeInfo)
- TypeDetails for complex types (EnumDetails, ListDetails, StructDetails, etc.)
- Type validation during Set operations

## ADDED Requirements

### Requirement: Vector Implementation Uses TypeInfo

Vectors MUST use TypeInfo from P0-1a Core TypeInfo for type metadata and validation.

**Context**: Integration with completed P0-1a TypeInfo system.

#### Scenario: Vector stores TypeInfo

```go
vec := NewVector(TypeInfoInteger(), 2048)
assert.Equal(t, TYPE_INTEGER, vec.Type().InternalType())
```

#### Scenario: Complex type vector uses TypeDetails

```go
listInfo := NewListInfo(TypeInfoVarchar())
vec := NewVector(listInfo, 2048)
details := vec.Type().TypeDetails()
assert.IsType(t, &ListDetails{}, details)
```

### Requirement: Vector Capacity Fixed at 2048

All vectors MUST have capacity exactly equal to VectorSize constant (2048).

**Context**: Matches DuckDB's STANDARD_VECTOR_SIZE for cache alignment.

#### Scenario: Vector created with 2048 capacity

```go
vec := NewVector(TypeInfoInteger(), 2048)
assert.Equal(t, uint64(2048), vec.Capacity())
```

#### Scenario: DataChunk vectors all have 2048 capacity

```go
chunk := NewDataChunk([]TypeInfo{TypeInfoInteger(), TypeInfoVarchar()})
for _, vec := range chunk.Vectors() {
    assert.Equal(t, uint64(2048), vec.Capacity())
}
```

### Requirement: ValidityMask for NULL Handling

All vectors MUST use ValidityMask bitmap for NULL value tracking.

**Context**: Efficient NULL storage (1 bit per row) matching DuckDB architecture.

#### Scenario: ValidityMask tracks NULL values

```go
vec := NewVector(TypeInfoInteger(), 2048)
vec.SetNull(5)
assert.False(t, vec.IsValid(5))
assert.True(t, vec.IsValid(0))
```

#### Scenario: ValidityMask initialized to all valid

```go
vec := NewVector(TypeInfoInteger(), 2048)
vec.SetSize(100)
for i := uint64(0); i < 100; i++ {
    assert.True(t, vec.IsValid(i))
}
```

### Requirement: Nested Types Use Child Vectors

Complex types (LIST, STRUCT, MAP, ARRAY, UNION) MUST use child Vector instances for recursive storage.

**Context**: Matches DuckDB's recursive columnar architecture.

#### Scenario: LIST vector has child vector

```go
listInfo := NewListInfo(TypeInfoInteger())
listVec := NewVector(listInfo, 2048)
assert.NotNil(t, listVec.Child())
assert.Equal(t, TYPE_INTEGER, listVec.Child().Type().InternalType())
```

#### Scenario: STRUCT vector has named children

```go
structInfo := NewStructInfo(
    StructEntry{Name: "a", Info: TypeInfoInteger()},
    StructEntry{Name: "b", Info: TypeInfoVarchar()},
)
structVec := NewVector(structInfo, 2048)
children := structVec.Children()
assert.Len(t, children, 2)
assert.NotNil(t, children["a"])
assert.NotNil(t, children["b"])
```

### Requirement: Vector Pooling for Memory Efficiency

Vector implementation MUST support pooling to reduce allocations by 90%.

**Context**: Performance requirement - minimize GC pressure.

#### Scenario: VectorPool reuses vectors

```go
pool := NewVectorPool()
vec1 := pool.Get(TypeInfoInteger(), 2048)
vec1.Close()
pool.Put(vec1)

vec2 := pool.Get(TypeInfoInteger(), 2048)
// vec2 should be reused vec1 (same pointer after Reset)
```

### Requirement: Appender Auto-Flush at Capacity

Appender MUST automatically flush when currentRow reaches VectorSize (2048).

**Context**: Batch processing requirement for bulk inserts.

#### Scenario: Appender flushes at 2048 rows

```go
app := NewAppender(conn, "test_table")
for i := 0; i < 2049; i++ {
    err := app.AppendRow(i, fmt.Sprintf("row%d", i))
    assert.NoError(t, err)
}
// First chunk (2048 rows) should be flushed automatically
// Second chunk has 1 row pending
```

**Note**: This is an implementation-only change. No requirements are modified or removed from the existing `data-chunk-api` spec. All 31 requirements and 119 scenarios from the base spec remain valid and must be implemented.
