# Complex Data Types Implementation Design

## Overview

The complex data types implementation extends dukdb-go's columnar storage from primitive-only to full support for JSON, MAP, STRUCT, and UNION types. The architecture maintains the existing Vector interface while adding specialized subclasses for each complex type.

## Core Architecture

### Vector Hierarchy

```
Vector (base/interface)
├── ListVector (future: Phase 2)
├── StructVector
│   ├── fields map[string]Vector
│   └── fieldIndices map[string]int
├── MapVector
│   ├── keyVector Vector
│   └── valueVector Vector
├── UnionVector
│   ├── activeIndices []int
│   ├── memberVectors []Vector
│   └── memberNames []string
└── JSONVector
    ├── stringData []string
    ├── parsed []any (cached)
    └── parsedValid []bool
```

### Validity Bitmap Propagation

**Parent-child relationship**:
- Top-level NULL marked in parent Vector's ValidityMask
- Child vectors' validity bitmasks track per-field/per-element NULLs
- Accessing child of NULL parent returns NULL (short-circuit)

**Example - STRUCT field access**:
```
parent NULL?
├─ YES → return NULL, don't access child
└─ NO → check field vector's validity at index
```

### Physical Storage Representation

#### Current (Phase 0 - Generic):
```
[]any slice containing interface{} values
- Type information lost at storage
- No structure, no compression
- Deserialization guesses at structure
```

#### New (Phase 1):

**JSON Vector**:
```
[]string (JSON strings)     ← Persisted
ValidityMask (NULL bits)    ← Persisted
[]any (cached parsed)       ← Runtime only
[]bool (parsed valid?)      ← Runtime only
```

**MAP Vector**:
```
KeyVector (child)           ← Child persistence
ValueVector (child)         ← Child persistence
ValidityMask (parent level) ← Track map-level NULLs
[]int (offsets per row)     ← Map boundaries per row
```

**STRUCT Vector**:
```
fieldVectors map[string]Vector  ← Each field persisted as column
fieldIndices map[string]int      ← Stable ordering
ValidityMask (NULL bits)         ← Top-level struct NULLs
```

**UNION Vector**:
```
memberVectors []Vector      ← One per union member
activeIndices []int         ← Which member is active per row
ValidityMask (NULL bits)    ← Track union-level NULLs
memberNames []string        ← Member names (metadata)
```

## DuckDB 1.4.3 Format Compliance

### Serialization Layers

**Layer 1 - Vector → DuckDBColumnSegment**:
```
type DuckDBColumnSegment struct {
    Metadata    DuckDBSegmentMetadata
    CompressionType CompressionAlgorithm
    Data        []byte      // Physical bytes
    Validity    []byte      // Validity bitmap bytes
    Children    []DuckDBColumnSegment (for nested types)
}
```

**Layer 2 - Complex Type Serialization**:
- JSON: Store as string with FSST compression
- MAP: Serialize as two children (keys, values) + offsets
- STRUCT: Serialize each field as separate column segment
- UNION: Serialize indices + all member types as children

**Layer 3 - Row Group Structure**:
```
DuckDBRowGroup {
    Columns: map[string]DuckDBColumnSegment
    ColumnOrder: []string (preserves field order for STRUCT)
    RowCount: int
}
```

### Compression Strategy

| Type | Algorithm | Rationale |
|------|-----------|-----------|
| JSON strings | FSST | Symbol table trained on JSON structure |
| MAP keys | Depends on key type (INT→BitPacking, VARCHAR→FSST) | Leverage key type compression |
| MAP values | Depends on value type | Leverage value type compression |
| STRUCT fields | Per-field (each field compressed independently) | Fields may have different characteristics |
| UNION indices | RLE (many same active type) | Run-length encode active member indices |

### Format Layout Example - STRUCT(id INT, name VARCHAR)

```
DuckDBColumnSegment (STRUCT)
├─ Children[0]: DuckDBColumnSegment (id, TYPE_INTEGER)
│  ├─ Data: [4-byte integers × row_count]
│  ├─ Validity: [bit-packed NULLs for id column]
│  └─ Compression: BitPacking
├─ Children[1]: DuckDBColumnSegment (name, TYPE_VARCHAR)
│  ├─ Data: [string data, FSST encoded]
│  ├─ Validity: [bit-packed NULLs for name column]
│  └─ Compression: FSST
└─ Parent Validity: [top-level struct NULLs]
```

## Memory Management

### Vector Pooling

Leverage existing `VectorPool` for complex vector recycling:
```go
// Acquire from pool
v := pool.Acquire(TYPE_STRUCT, capacity)
defer pool.Release(v)

// Pool handles child vector allocation/release
// Validity bitmasks cleared automatically
```

### Child Vector Lifecycle

**Allocation**:
- Complex vector constructor allocates child vectors recursively
- Depth-first allocation for nested structures

**Cleanup**:
- Pool release recursively releases children
- Validity bitmasks deallocated

**Resizing**:
- Complex vectors resize children proportionally
- Maintain parent-child invariants

## Scanning & Binding API

### User-Facing Scanning

**JSON Scanning**:
```go
var config struct {
    Enabled bool
    Timeout int
}
err := row.ScanJSON(&config, "settings")
```

**MAP Scanning**:
```go
var attrs map[string]string
err := row.ScanMap(&attrs, "tags")
```

**STRUCT Scanning**:
```go
type Person struct {
    Name string `duckdb:"name"`
    Age  int    `duckdb:"age"`
}
var p Person
err := row.ScanStruct(&p, "person_col")
```

**UNION Scanning**:
```go
var u UnionValue
err := row.ScanUnion(&u, "result")
if u.Tag == "error" {
    // handle error variant
}
```

### Parameter Binding

Leverage existing `StructValue[T]`, `MapValue[K,V]` wrappers:
```go
type Config struct {
    Enabled bool `duckdb:"enabled"`
}
err := stmt.QueryRow(StructValue[Config]{V: Config{Enabled: true}}).Scan(...)
```

## Type Safety & Error Handling

### Runtime Type Checking

**Type assertion with error**:
```go
// User asks to scan STRING into int field
err := row.ScanStruct(&person)  // Age field is int
// Error: "field Age: cannot convert VARCHAR to int"
```

**Nested error paths**:
```go
// Error: "list element 0: field Name: cannot convert..."
// Helps locate exact failing value
```

### Null Propagation

**Parent NULL prevents child access**:
```
row.ScanStruct(&person)  // person is NULL
// Result: person struct contains zero values
// No panic, no undefined behavior
```

## Performance Considerations

### Hot Path Optimization

1. **Validity check before data access**:
   - NULL check occurs in GetValue() before type assertion
   - Minimal overhead (single bit operation)

2. **String parsing laziness for JSON**:
   - Parse JSON on first access, cache result
   - Avoid parsing unused JSON columns

3. **Child vector access patterns**:
   - STRUCT field access: O(1) map lookup (precompiled)
   - MAP lookup: O(n) scan (acceptable for analytical workload)
   - UNION member check: O(1) index check

### Memory Footprint

**Minimal overhead**:
- Extra ValidityMask per child vector (8 bytes per 64 rows)
- Field name mappings for STRUCT (cached, amortized)
- Member indices for UNION (fixed, once per vector)

## Phase 2 - Future Extensions

**Scheduled for Phase 2** (separate proposal):
- LIST and ARRAY types with element vector references
- ENUM type support with value pool
- GEOMETRY and LAMBDA types
- VARIANT type (dynamic typing)
- Specialized operators for complex types (map access, path extraction, etc.)

## Decisions Made

1. **Dedicated vector classes**: Type-specific implementation for clarity and optimization
2. **Match DuckDB 1.4.3 exactly**: Ensure byte-level format compatibility
3. **Recursive nesting support**: Full support in Phase 1 (no flattening)
4. **Lazy JSON parsing**: Parse on demand to avoid unnecessary work
5. **Child vector pooling**: Reuse existing pool infrastructure

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Format incompatibility with DuckDB 1.4.3 | Reference implementation validation, test round-trip with DuckDB binary |
| Memory leaks in child vector lifecycle | Pool-based cleanup, tests with pprof memory profiling |
| Performance regression in primitive types | Vector routing via interface{} assertion, no changes to primitive path |
| Serialization bloat | Compression algorithms selected per field, baseline validation |
| User API confusion with nested nulls | Clear documentation, error messages with nested paths |

## Decisions Requiring User Input

- ✅ Completed - Vector storage approach (dedicated classes)
- ✅ Completed - DuckDB 1.4.3 format compliance
- ✅ Completed - Recursive nesting support
- ✅ Completed - Include scanning API
