# Vector/DataChunk Low-Level API Design

## Overview

This document specifies the implementation of DuckDB's Vector and DataChunk columnar data structures in pure Go. These are the foundational low-level APIs that enable efficient vectorized query execution.

## Architecture

### Columnar vs Row-Oriented Storage

**DuckDB uses columnar storage** for cache efficiency and SIMD operations:

```
Row-Oriented (traditional):
[id=1, name="Alice", age=30]
[id=2, name="Bob",   age=25]
[id=3, name="Carol", age=35]

Columnar (DuckDB):
Column 0 (id):    [1, 2, 3]
Column 1 (name):  ["Alice", "Bob", "Carol"]
Column 2 (age):   [30, 25, 35]
```

**Benefits**:
- Better CPU cache utilization (process one column at a time)
- SIMD operations on contiguous data
- Column-specific compression
- Skip columns not needed by query (projection pushdown)

## Vector: Single-Column Storage

### Vector Structure

```go
// Vector represents a single column of typed data
type Vector struct {
    typ      TypeInfo          // Type metadata from P0-1a
    size     uint64            // Current number of valid entries
    capacity uint64            // Maximum capacity (usually 2048)
    data     interface{}       // Type-specific backing store
    validity *ValidityMask     // NULL bitmap (1 bit per row)
    children []*Vector         // Child vectors for nested types
}

// ValidityMask tracks NULL values (1 bit per row)
type ValidityMask struct {
    bits []uint64  // Bitmap: 1 = valid, 0 = NULL
}
```

### Vector Types

**Flat Vector** (most common):
- Contiguous array of values
- One validity bit per value
- Used for primitives: INTEGER, VARCHAR, DOUBLE, etc.

**Constant Vector** (optimization):
- Single value repeated N times
- Memory: O(1) instead of O(N)
- Used for literal constants

**Dictionary Vector** (compression):
- Array of indices + small dictionary
- Saves memory for low-cardinality columns
- Example: ["red", "blue", "red", "red"] → indices=[0,1,0,0], dict=["red","blue"]

**For P0-2 scope**: We implement **Flat Vectors only**. Constant and Dictionary vectors deferred to P1 optimization.

### Primitive Type Vectors

```go
// Integer types use typed slices
type Int32Vector struct {
    values   []int32
    validity *ValidityMask
}

// Example: INTEGER vector with 3 values
vector := &Int32Vector{
    values:   []int32{10, 20, 0}, // 0 for NULL (value ignored)
    validity: &ValidityMask{bits: []uint64{0b110}}, // bits 0,1 valid; bit 2 NULL
}

// Type-specific accessors
func (v *Int32Vector) Get(row uint64) (int32, bool) {
    if !v.validity.IsValid(row) {
        return 0, false  // NULL
    }
    return v.values[row], true
}

func (v *Int32Vector) Set(row uint64, value int32) {
    v.values[row] = value
    v.validity.SetValid(row, true)
}

func (v *Int32Vector) SetNull(row uint64) {
    v.validity.SetValid(row, false)
}
```

**All Primitive Types**:
- `BOOLEAN` → `[]bool`
- `TINYINT` → `[]int8`, `UTINYINT` → `[]uint8`
- `SMALLINT` → `[]int16`, `USMALLINT` → `[]uint16`
- `INTEGER` → `[]int32`, `UINTEGER` → `[]uint32`
- `BIGINT` → `[]int64`, `UBIGINT` → `[]uint64`
- `FLOAT` → `[]float32`, `DOUBLE` → `[]float64`
- `HUGEINT` → `[]*big.Int`, `UHUGEINT` → `[]*big.Int`
- `DECIMAL` → `[]Decimal` (struct with value, width, scale)
- `UUID` → `[][16]byte`

### String Type Vectors

```go
// VARCHAR/BLOB use slice of strings/bytes
type StringVector struct {
    values   []string
    validity *ValidityMask
}

// Optimization: String dictionary (P1 future work)
// High-cardinality strings stored inline
// Low-cardinality strings use dictionary vector
```

### Temporal Type Vectors

```go
// DATE stored as days since epoch (int32)
type DateVector struct {
    values   []int32  // Days since 1970-01-01
    validity *ValidityMask
}

// TIMESTAMP stored as microseconds since epoch (int64)
type TimestampVector struct {
    values   []int64  // Microseconds since epoch
    validity *ValidityMask
}

// TIME stored as microseconds since midnight (int64)
type TimeVector struct {
    values   []int64  // Microseconds since 00:00:00
    validity *ValidityMask
}

// INTERVAL stored as struct
type IntervalVector struct {
    values   []Interval  // {Months, Days, Micros}
    validity *ValidityMask
}

type Interval struct {
    Months int32
    Days   int32
    Micros int64
}
```

### ENUM Type Vectors

```go
// ENUM uses integer indices into dictionary
type EnumVector struct {
    indices  []uint32        // Index into enum values
    validity *ValidityMask
    dict     []string        // Enum values from EnumDetails (P0-1a)
}

func (v *EnumVector) Set(row uint64, value string) error {
    // Find value in dictionary
    idx := slices.Index(v.dict, value)
    if idx == -1 {
        return fmt.Errorf("invalid enum value: %s", value)
    }
    v.indices[row] = uint32(idx)
    v.validity.SetValid(row, true)
    return nil
}

func (v *EnumVector) Get(row uint64) (string, bool) {
    if !v.validity.IsValid(row) {
        return "", false
    }
    return v.dict[v.indices[row]], true
}
```

### LIST Type Vectors

**LIST is variable-length** - each row can have different number of elements.

```go
// LIST uses child vector + offset array
type ListVector struct {
    child    *Vector         // Child vector (all list elements concatenated)
    offsets  []uint64        // Start offset for each list
    validity *ValidityMask
}

// Example: LIST(INTEGER) with [[1,2], [3], [4,5,6]]
// Concatenated child vector: [1, 2, 3, 4, 5, 6]
// Offsets: [0, 2, 3, 6]
//   Row 0: elements [0:2] = [1, 2]
//   Row 1: elements [2:3] = [3]
//   Row 2: elements [3:6] = [4, 5, 6]

func (v *ListVector) Get(row uint64) ([]any, bool) {
    if !v.validity.IsValid(row) {
        return nil, false
    }
    start := v.offsets[row]
    end := v.offsets[row+1]

    result := make([]any, end-start)
    for i := start; i < end; i++ {
        result[i-start] = v.child.Get(i)
    }
    return result, true
}

func (v *ListVector) Append(row uint64, elements []any) error {
    v.offsets[row] = uint64(len(v.child.values))
    for _, elem := range elements {
        v.child.Append(elem)
    }
    v.offsets[row+1] = uint64(len(v.child.values))
    v.validity.SetValid(row, true)
    return nil
}
```

### STRUCT Type Vectors

**STRUCT has fixed fields** with named columns.

```go
// STRUCT uses child vector per field
type StructVector struct {
    children map[string]*Vector  // Field name → child vector
    validity *ValidityMask
}

// Example: STRUCT(name VARCHAR, age INTEGER)
structVec := &StructVector{
    children: map[string]*Vector{
        "name": NewStringVector(capacity),
        "age":  NewInt32Vector(capacity),
    },
    validity: NewValidityMask(capacity),
}

func (v *StructVector) Get(row uint64) (map[string]any, bool) {
    if !v.validity.IsValid(row) {
        return nil, false
    }
    result := make(map[string]any)
    for name, child := range v.children {
        result[name] = child.Get(row)
    }
    return result, true
}

func (v *StructVector) Set(row uint64, value map[string]any) error {
    for name, val := range value {
        child, exists := v.children[name]
        if !exists {
            return fmt.Errorf("unknown struct field: %s", name)
        }
        child.Set(row, val)
    }
    v.validity.SetValid(row, true)
    return nil
}
```

### MAP Type Vectors

**MAP is internally LIST<STRUCT<key, value>>** (from P0-1b design).

```go
// MAP reuses LIST vector infrastructure
type MapVector struct {
    *ListVector  // Embed LIST vector
    // Child is STRUCT with "key" and "value" fields
}

func NewMapVector(keyType, valueType TypeInfo, capacity uint64) *MapVector {
    // Create STRUCT<key, value> type
    structInfo, _ := NewStructInfo(
        StructEntry{Name: "key", Info: keyType},
        StructEntry{Name: "value", Info: valueType},
    )

    // Create LIST of STRUCT
    listInfo, _ := NewListInfo(structInfo)

    return &MapVector{
        ListVector: NewListVector(listInfo, capacity),
    }
}

func (v *MapVector) Get(row uint64) (Map, bool) {
    // Get LIST of STRUCT from child
    list, ok := v.ListVector.Get(row)
    if !ok {
        return nil, false
    }

    // Convert to map
    result := make(Map)
    for _, entry := range list.([]any) {
        structVal := entry.(map[string]any)
        result[structVal["key"]] = structVal["value"]
    }
    return result, true
}

type Map map[any]any
```

### ARRAY Type Vectors

**ARRAY is fixed-size** - all rows have same number of elements.

```go
// ARRAY uses child vector with fixed stride
type ArrayVector struct {
    child    *Vector
    size     uint32          // Fixed array size (from ArrayDetails)
    validity *ValidityMask
}

// Example: ARRAY(INTEGER, 3) with [[1,2,3], [4,5,6]]
// Concatenated child vector: [1, 2, 3, 4, 5, 6]
// Row 0: elements [0:3]
// Row 1: elements [3:6]

func (v *ArrayVector) Get(row uint64) ([]any, bool) {
    if !v.validity.IsValid(row) {
        return nil, false
    }
    start := row * uint64(v.size)
    end := start + uint64(v.size)

    result := make([]any, v.size)
    for i := start; i < end; i++ {
        result[i-start] = v.child.Get(i)
    }
    return result, true
}

func (v *ArrayVector) Set(row uint64, values []any) error {
    if len(values) != int(v.size) {
        return fmt.Errorf("array size mismatch: expected %d, got %d", v.size, len(values))
    }
    start := row * uint64(v.size)
    for i, val := range values {
        v.child.Set(start+uint64(i), val)
    }
    v.validity.SetValid(row, true)
    return nil
}
```

### UNION Type Vectors

**UNION stores tag + value** (tagged union).

```go
// UNION uses tag array + child vector per member
type UnionVector struct {
    tags     []uint8              // Member tag (0, 1, 2, ...)
    children map[string]*Vector   // Member name → child vector
    validity *ValidityMask
}

type UnionValue struct {
    Tag   string  // Member name
    Value any
}

func (v *UnionVector) Get(row uint64) (UnionValue, bool) {
    if !v.validity.IsValid(row) {
        return UnionValue{}, false
    }
    tag := v.tags[row]
    memberName := v.getMemberName(tag)
    child := v.children[memberName]
    return UnionValue{
        Tag:   memberName,
        Value: child.Get(row),
    }, true
}

func (v *UnionVector) Set(row uint64, value UnionValue) error {
    child, exists := v.children[value.Tag]
    if !exists {
        return fmt.Errorf("invalid union tag: %s", value.Tag)
    }
    tag := v.getMemberTag(value.Tag)
    v.tags[row] = tag
    child.Set(row, value.Value)
    v.validity.SetValid(row, true)
    return nil
}
```

## ValidityMask: NULL Handling

```go
// ValidityMask uses bitmap (1 bit per row)
type ValidityMask struct {
    bits []uint64  // 64 bits per uint64
}

const BitsPerWord = 64

func NewValidityMask(capacity uint64) *ValidityMask {
    numWords := (capacity + BitsPerWord - 1) / BitsPerWord
    return &ValidityMask{
        bits: make([]uint64, numWords),
    }
}

func (v *ValidityMask) IsValid(row uint64) bool {
    wordIdx := row / BitsPerWord
    bitIdx := row % BitsPerWord
    return (v.bits[wordIdx] & (1 << bitIdx)) != 0
}

func (v *ValidityMask) SetValid(row uint64, valid bool) {
    wordIdx := row / BitsPerWord
    bitIdx := row % BitsPerWord
    if valid {
        v.bits[wordIdx] |= (1 << bitIdx)
    } else {
        v.bits[wordIdx] &^= (1 << bitIdx)
    }
}

func (v *ValidityMask) SetAllValid(count uint64) {
    // Set all bits to 1 (no NULLs)
    for i := uint64(0); i < count; i++ {
        v.SetValid(i, true)
    }
}

func (v *ValidityMask) CountValid() uint64 {
    // Use bits.OnesCount64 for efficiency
    count := uint64(0)
    for _, word := range v.bits {
        count += uint64(bits.OnesCount64(word))
    }
    return count
}
```

## DataChunk: Multi-Column Storage

```go
// DataChunk represents a batch of rows across multiple columns
type DataChunk struct {
    vectors    []*Vector       // One vector per column
    size       uint64          // Current number of valid rows
    capacity   uint64          // Maximum capacity (2048)
    projection []int           // Column projection (-1 for unprojected)
}

const VectorSize = 2048  // DuckDB's VECTOR_SIZE

func NewDataChunk(types []TypeInfo) *DataChunk {
    vectors := make([]*Vector, len(types))
    for i, typ := range types {
        vectors[i] = NewVector(typ, VectorSize)
    }
    return &DataChunk{
        vectors:  vectors,
        size:     0,
        capacity: VectorSize,
    }
}

func (c *DataChunk) GetSize() uint64 {
    return c.size
}

func (c *DataChunk) SetSize(size uint64) error {
    if size > c.capacity {
        return fmt.Errorf("size %d exceeds capacity %d", size, c.capacity)
    }
    c.size = size
    return nil
}

func (c *DataChunk) GetValue(col, row uint64) (any, error) {
    // Apply projection
    physicalCol := c.getPhysicalColumn(col)
    if physicalCol == -1 {
        return nil, fmt.Errorf("column %d is not projected", col)
    }

    if physicalCol >= len(c.vectors) {
        return nil, fmt.Errorf("column index %d out of range", col)
    }

    if row >= c.size {
        return nil, fmt.Errorf("row index %d out of range", row)
    }

    return c.vectors[physicalCol].Get(row), nil
}

func (c *DataChunk) SetValue(col, row uint64, value any) error {
    physicalCol := c.getPhysicalColumn(col)
    if physicalCol == -1 {
        // Unprojected column - silently ignore
        return nil
    }

    if physicalCol >= len(c.vectors) {
        return fmt.Errorf("column index %d out of range", col)
    }

    return c.vectors[physicalCol].Set(row, value)
}

func (c *DataChunk) Reset() {
    // Reset size but preserve vectors for reuse
    c.size = c.capacity
    for _, vec := range c.vectors {
        vec.Reset()
    }
}

func (c *DataChunk) Close() {
    // Cleanup all vectors
    for _, vec := range c.vectors {
        vec.Close()
    }
    c.vectors = nil
}

func (c *DataChunk) getPhysicalColumn(logicalCol uint64) int {
    if c.projection == nil {
        return int(logicalCol)
    }
    if logicalCol >= uint64(len(c.projection)) {
        return -1
    }
    return c.projection[logicalCol]
}
```

## Row Accessor

```go
// Row provides row-oriented view of DataChunk
type Row struct {
    chunk *DataChunk
    index uint64
}

func (r *Row) IsProjected(col uint64) bool {
    return r.chunk.getPhysicalColumn(col) != -1
}

func (r *Row) SetRowValue(col uint64, value any) error {
    return r.chunk.SetValue(col, r.index, value)
}

func (r *Row) GetRowValue(col uint64) (any, error) {
    return r.chunk.GetValue(col, r.index)
}

// Type-safe generic accessor
func SetRowValue[T any](row *Row, col uint64, value T) error {
    return row.SetRowValue(col, value)
}
```

## Appender API

```go
// Appender efficiently appends rows to a table
type Appender struct {
    table      *Table
    chunk      *DataChunk
    currentRow uint64
}

func NewAppender(conn *Conn, table string) (*Appender, error) {
    // Get table metadata
    tbl := conn.catalog.GetTable(table)
    if tbl == nil {
        return nil, fmt.Errorf("table not found: %s", table)
    }

    // Create DataChunk matching table schema
    types := make([]TypeInfo, len(tbl.Columns))
    for i, col := range tbl.Columns {
        types[i] = col.Type
    }

    return &Appender{
        table:      tbl,
        chunk:      NewDataChunk(types),
        currentRow: 0,
    }, nil
}

func (a *Appender) AppendRow(values ...any) error {
    if len(values) != len(a.chunk.vectors) {
        return fmt.Errorf("column count mismatch: expected %d, got %d", len(a.chunk.vectors), len(values))
    }

    // Set values in current row
    for col, value := range values {
        if err := a.chunk.SetValue(uint64(col), a.currentRow, value); err != nil {
            return fmt.Errorf("column %d: %w", col, err)
        }
    }

    a.currentRow++

    // Flush if chunk is full
    if a.currentRow >= VectorSize {
        return a.Flush()
    }

    return nil
}

func (a *Appender) Flush() error {
    if a.currentRow == 0 {
        return nil  // Nothing to flush
    }

    // Set chunk size to current row count
    a.chunk.SetSize(a.currentRow)

    // Write chunk to storage
    if err := a.table.AppendChunk(a.chunk); err != nil {
        return err
    }

    // Reset chunk for reuse
    a.chunk.Reset()
    a.currentRow = 0

    return nil
}

func (a *Appender) Close() error {
    // Flush remaining rows
    if err := a.Flush(); err != nil {
        return err
    }
    a.chunk.Close()
    return nil
}
```

## Type-Safe Generic Accessors

```go
// SetChunkValue provides type-safe setting without runtime type assertion
func SetChunkValue[T any](chunk *DataChunk, col, row uint64, value T) error {
    // Type validation at compile time
    return chunk.SetValue(col, row, value)
}

// Example usage:
// SetChunkValue[int32](chunk, 0, 0, 42)         ✅ Compiles
// SetChunkValue[string](chunk, 0, 0, "hello")   ✅ Compiles
// SetChunkValue[int32](chunk, 0, 0, "wrong")    ❌ Compile error
```

## Memory Management

### Vector Pooling

```go
// VectorPool reuses vectors to reduce allocations
type VectorPool struct {
    pools map[Type][]*Vector
    mu    sync.Mutex
}

var globalVectorPool = NewVectorPool()

func (p *VectorPool) Get(typ TypeInfo) *Vector {
    p.mu.Lock()
    defer p.mu.Unlock()

    pool := p.pools[typ.InternalType()]
    if len(pool) == 0 {
        return NewVector(typ, VectorSize)
    }

    vec := pool[len(pool)-1]
    p.pools[typ.InternalType()] = pool[:len(pool)-1]
    return vec
}

func (p *VectorPool) Put(vec *Vector) {
    p.mu.Lock()
    defer p.mu.Unlock()

    vec.Reset()
    p.pools[vec.typ.InternalType()] = append(p.pools[vec.typ.InternalType()], vec)
}
```

### Reset for Reuse

```go
func (v *Vector) Reset() {
    // Reset size but preserve capacity
    v.size = 0

    // Clear validity mask (all invalid)
    if v.validity != nil {
        for i := range v.validity.bits {
            v.validity.bits[i] = 0
        }
    }

    // Reset children recursively
    for _, child := range v.children {
        child.Reset()
    }

    // Don't deallocate backing arrays - reuse them
}
```

## Performance Considerations

**Cache Efficiency**:
- Columnar layout improves CPU cache utilization
- Process one column at a time (spatial locality)
- SIMD operations on contiguous arrays

**Memory Allocation**:
- Vector pooling reduces GC pressure
- Pre-allocate capacity (2048 rows)
- Reuse vectors across queries

**Batch Processing**:
- Process 2048 rows at a time
- Amortize function call overhead
- Enable loop unrolling and SIMD

**Benchmarks** (target performance):
- Append 1M int32 values: <1 second
- Scan 1M int32 values: <500ms
- NULL bitmap operations: <50ns per row
- Vector allocation from pool: <100ns

## File Structure

```
internal/vector/
├── vector.go              # Core Vector interface
├── flat_vector.go         # Flat vector implementation
├── primitive_vectors.go   # Int32Vector, StringVector, etc.
├── list_vector.go         # LIST type
├── struct_vector.go       # STRUCT type
├── map_vector.go          # MAP type (uses LIST + STRUCT)
├── array_vector.go        # ARRAY type
├── union_vector.go        # UNION type
├── enum_vector.go         # ENUM type
├── validity.go            # ValidityMask bitmap operations
├── pool.go                # VectorPool for memory reuse
└── vector_test.go         # Comprehensive tests

data_chunk.go              # DataChunk public API
appender.go                # Appender public API
row.go                     # Row accessor
```

## Testing Strategy

**Unit Tests** (per vector type):
- Set/Get primitive values
- NULL handling
- Nested type operations
- Validity bitmap operations

**Integration Tests**:
- Appender with 1M rows
- DataChunk with all 37 types
- Column projection
- Memory leak detection (pprof)

**Benchmarks**:
- Append throughput
- Scan throughput
- Memory allocations
- Pool efficiency

**Spec Compliance**:
- All 393 scenarios from data-chunk-api spec
- Type validation
- Error cases
