# Design: Type System Extensions

## Context

This design document outlines the technical approach for implementing extended type support in dukdb-go. The goal is to achieve full DuckDB API compatibility by implementing:

1. New type constants (JSON, GEOMETRY, VARIANT, LAMBDA)
2. First-class JSON type support
3. SQLNULL type implementation
4. BIGNUM type for variable-width decimals
5. GEOMETRY type for spatial data

**Stakeholders**:
- Application developers needing spatial data support
- Data engineers working with JSON documents
- Users migrating from official DuckDB

**Constraints**:
- Pure Go implementation (no CGO)
- Must maintain API compatibility with go-duckdb
- Backward compatible with existing type handling
- Performance should be competitive with VARCHAR for JSON

## Goals / Non-Goals

**Goals**:
1. Add TYPE_JSON, TYPE_GEOMETRY, TYPE_VARIANT, TYPE_LAMBDA constants
2. Implement fully functional initJSON() for JSON column storage
3. Fix initSQLNull() to work correctly
4. Add BignumDetails for variable-width decimal support
5. Implement geometry storage as WKB binary
6. Add JSON validation and extraction functions

**Non-Goals**:
1. R-tree spatial indexing (future work)
2. LAMBDA function execution (depends on expression evaluation)
3. JSON path indexing optimization
4. Native WKT parsing optimizations

## Decisions

### Decision 1: Type Constants Addition

**Options**:
A. Use placeholder IDs for new types
B. Match DuckDB type IDs exactly
C. Use sequential IDs after current max (36)

**Choice**: B - Match DuckDB type IDs exactly

**Rationale**:
- Required for DuckDB file format compatibility
- Enables reading files created by official DuckDB
- Clear mapping for debugging

```go
// In type_enum.go, add after TYPE_SQLNULL (36):

// TYPE_JSON (ID 37) - JSON data type
const TYPE_JSON Type = 37

// TYPE_GEOMETRY (ID 60) - Spatial geometry type
const TYPE_GEOMETRY Type = 60

// TYPE_LAMBDA (ID 106) - Lambda function type for higher-order functions
const TYPE_LAMBDA Type = 106

// TYPE_VARIANT (ID 109) - Dynamic/any JSON-like type
const TYPE_VARIANT Type = 109
```

**Type ID Verification**:
- TYPE_JSON = 37 (matches DuckDB 1.4.3)
- TYPE_GEOMETRY = 60 (matches DuckDB extension)
- TYPE_LAMBDA = 106 (matches DuckDB extension)
- TYPE_VARIANT = 109 (matches DuckDB extension)

### Decision 2: JSON Storage Strategy

**Options**:
A. Store as internal JSON structure (parsed on insert)
B. Store as VARCHAR with validation on access (lazy parsing)
C. Store as BLOB with JSON bytes

**Choice**: B - Store as VARCHAR with validation on access (lazy parsing)

**Rationale**:
- Compatible with existing VARCHAR infrastructure
- Minimal memory overhead (no duplication)
- Validation only when needed (lazy evaluation)
- Easy interoperability with string-based JSON sources

```go
// In vector.go, modify initJSON():

func (vec *vector) initJSON() {
    vec.dataSlice = make([]string, vec.capacity)
    vec.getFn = func(vec *vector, rowIdx int) any {
        if vec.getNull(rowIdx) {
            return nil
        }
        s := getPrimitive[string](vec, rowIdx)
        // Lazy parse only when accessed
        var result any
        if err := json.Unmarshal([]byte(s), &result); err != nil {
            return s // Return raw string on parse failure
        }
        return result
    }
    vec.setFn = func(vec *vector, rowIdx int, val any) error {
        if val == nil {
            vec.setNull(rowIdx)
            return nil
        }
        return setJSON(vec, rowIdx, val)
    }
    vec.Type = TYPE_JSON
}
```

**JSON Validation Function**:
```go
// In internal/io/json/json.go:

func IsValidJSON(s string) bool {
    return json.Valid([]byte(s))
}

func ParseJSON(s string) (any, error) {
    var result any
    err := json.Unmarshal([]byte(s), &result)
    return result, err
}

func ExtractJSONPath(s string, path string) (any, error) {
    // Parse path (e.g., "$.field[0]")
    // Use go-jsonpath or custom implementation
    // Return extracted value as JSON string
}
```

### Decision 3: SQLNULL Implementation

**Options**:
A. Store nothing (no dataSlice), only validity bitmap
B. Store as special NULL marker
C. Store as zero-length strings

**Choice**: A - Store nothing, only validity bitmap

**Rationale**:
- Maximum space efficiency (no data storage)
- All values are implicitly NULL
- Simple implementation

```go
// In vector.go, modify initSQLNull():

func (vec *vector) initSQLNull() {
    vec.dataSlice = nil // No data storage needed
    vec.getFn = func(vec *vector, rowIdx int) any {
        return nil // Always returns nil
    }
    vec.setFn = func(vec *vector, rowIdx int, val any) error {
        // Always mark as NULL in validity bitmap
        vec.setNull(rowIdx)
        return nil
    }
    vec.Type = TYPE_SQLNULL
}
```

### Decision 4: Geometry Storage

**Options**:
A. Store as internal geometry structure
B. Store as WKT (Well-Known Text) string
C. Store as WKB (Well-Known Binary) blob

**Choice**: C - Store as WKB (Well-Known Binary) blob

**Rationale**:
- Standard OGC format (Interoperable)
- Compact binary representation
- Compatible with GEOMETRY type serialization
- Fixed-width for simple types

```go
// In internal/io/geometry/geometry.go:

type Geometry struct {
    Type    GeometryType
    Data    []byte // WKB encoding
    Srid    int32  // Spatial reference ID
}

type GeometryType uint8

const (
    GeometryPoint      GeometryType = 1
    GeometryLineString GeometryType = 2
    GeometryPolygon    GeometryType = 3
    GeometryMultiPoint GeometryType = 4
    GeometryMultiLineString GeometryType = 5
    GeometryMultiPolygon GeometryType = 6
    GeometryCollection GeometryType = 7
)

// WKBReader reads Well-Known Binary format
type WKBReader struct {
    byteOrder binary.ByteOrder
}

func (r *WKBReader) Read(data []byte) (*Geometry, error) {
    // Parse WKB header (byte order, geometry type)
    // Parse coordinates based on type
    // Return Geometry struct
}

// WKTReader reads Well-Known Text format
type WKTReader struct{}

func (r *WKTReader) Read(s string) (*Geometry, error) {
    // Parse WKT string (e.g., "POINT(1 2)")
    // Convert to WKB for storage
}
```

**Geometry Vector Initialization**:
```go
// In vector.go, add initGeometry():

func (vec *vector) initGeometry() {
    vec.dataSlice = make([]Geometry, vec.capacity)
    vec.getFn = func(vec *vector, rowIdx int) any {
        if vec.getNull(rowIdx) {
            return nil
        }
        return getPrimitive[Geometry](vec, rowIdx)
    }
    vec.setFn = func(vec *vector, rowIdx int, val any) error {
        if val == nil {
            vec.setNull(rowIdx)
            return nil
        }
        return setGeometry(vec, rowIdx, val)
    }
    vec.Type = TYPE_GEOMETRY
}

func setGeometry(vec *vector, rowIdx int, val any) error {
    switch v := val.(type) {
    case Geometry:
        vec.setValid(rowIdx)
        setPrimitive(vec, rowIdx, v)
        return nil
    case string:
        // Parse as WKT
        g, err := ParseWKT(v)
        if err != nil {
            return err
        }
        vec.setValid(rowIdx)
        setPrimitive(vec, rowIdx, g)
        return nil
    case []byte:
        // Parse as WKB
        g, err := ParseWKB(v)
        if err != nil {
            return err
        }
        vec.setValid(rowIdx)
        setPrimitive(vec, rowIdx, g)
        return nil
    default:
        return fmt.Errorf("cannot convert %T to Geometry", val)
    }
}
```

### Decision 5: BIGNUM Type Implementation

**Options**:
A. Use variable-width integer storage
B. Use big.Int directly
C. Use string storage with decimal parsing

**Choice**: B - Use big.Int directly

**Rationale**:
- Already available in Go standard library
- Arbitrary precision
- Compatible with existing HUGEINT infrastructure

```go
// In type_info.go, add BignumDetails:

type BignumDetails struct {
    Scale uint8
}

func NewBignumInfo(scale uint8) (TypeInfo, error) {
    return &typeInfo{
        typ:   TYPE_BIGNUM,
        types: []TypeInfo{},
    }, nil
}
```

**Bignum Vector Initialization**:
```go
// In vector.go, add initBignum():

func (vec *vector) initBignum() {
    vec.dataSlice = make([]*big.Int, vec.capacity)
    vec.getFn = func(vec *vector, rowIdx int) any {
        if vec.getNull(rowIdx) {
            return nil
        }
        return getPrimitive[*big.Int](vec, rowIdx)
    }
    vec.setFn = func(vec *vector, rowIdx int, val any) error {
        if val == nil {
            vec.setNull(rowIdx)
            return nil
        }
        return setBignum(vec, rowIdx, val)
    }
    vec.Type = TYPE_BIGNUM
}

func setBignum(vec *vector, rowIdx int, val any) error {
    var b *big.Int
    switch v := val.(type) {
    case *big.Int:
        b = v
    case big.Int:
        b = &v
    case int64:
        b = big.NewInt(v)
    case string:
        var ok bool
        b, ok = new(big.Int).SetString(v, 10)
        if !ok {
            return fmt.Errorf("cannot parse %q as BIGNUM", v)
        }
    default:
        return fmt.Errorf("cannot convert %T to BIGNUM", val)
    }
    vec.setValid(rowIdx)
    setPrimitive(vec, rowIdx, b)
    return nil
}
```

### Decision 6: TypeInfo Updates

**Options**:
A. Add new Details types for each new type
B. Use existing Details types where possible
C. Generic Details type for all extended types

**Choice**: A - Add new Details types for each new type

**Rationale**:
- Type-safe access patterns
- Consistent with existing TypeInfo design
- Easy to extend

```go
// In type_info.go, add new Details types:

// JSONDetails provides JSON type information.
type JSONDetails struct{}

func (j *JSONDetails) isTypeDetails() {}

// GeometryDetails provides GEOMETRY type information.
type GeometryDetails struct {
    Srid int32
}

func (g *GeometryDetails) isTypeDetails() {}

// VariantDetails provides VARIANT type information.
type VariantDetails struct{}

func (v *VariantDetails) isTypeDetails() {}

// LambdaDetails provides LAMBDA type information.
type LambdaDetails struct {
    InputTypes  []TypeInfo
    ReturnType  TypeInfo
}

func (l *LambdaDetails) isTypeDetails() {}

// BignumDetails provides BIGNUM type information.
type BignumDetails struct {
    Scale uint8
}

func (b *BignumDetails) isTypeDetails() {}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| JSON lazy parsing performance | Medium | Add JSON caching; benchmark with realistic workloads |
| Geometry WKB parsing | Medium | Use existing Go geometry libraries (geos, go-geom) |
| BIGNUM arbitrary precision | Low | Limit precision in documentation; add overflow checks |
| Type ID conflicts | High | Verify against DuckDB source before implementation |

## Performance Considerations

1. **JSON columns**: Store as VARCHAR, lazy parse on getFn access
2. **Geometry columns**: Store as WKB bytes for compactness
3. **SQLNULL columns**: No data storage, only validity bitmap
4. **BIGNUM columns**: Store *big.Int pointers, may need pooling

## Migration Plan

### Phase 1: Type Constants (1 day)
1. Add TYPE_JSON, TYPE_GEOMETRY, TYPE_VARIANT, TYPE_LAMBDA to type_enum.go
2. Update typeToStringMap and unsupportedTypeToStringMap
3. Add type category for new types

### Phase 2: SQLNULL Fix (1 day)
1. Modify initSQLNull() to work correctly
2. Add SQLNULL to NewTypeInfo() validation
3. Add tests for SQLNULL behavior

### Phase 3: JSON Support (2 days)
1. Update initJSON() to use TYPE_JSON
2. Implement JSON validation functions
3. Add JSON path extraction support
4. Add tests for JSON operations

### Phase 4: Geometry Support (3 days)
1. Create internal/io/geometry/ package
2. Implement WKB/WKT parsing
3. Add initGeometry() to vector.go
4. Add geometry helper functions
5. Add tests for geometry operations

### Phase 5: BIGNUM Support (2 days)
1. Add BignumDetails to type_info.go
2. Implement initBignum() in vector.go
3. Add BIGNUM to NewTypeInfo() validation
4. Add tests for BIGNUM operations

### Phase 6: Variant/Lambda (2 days)
1. Add VariantDetails and LambdaDetails
2. Implement initVariant() and initLambda()
3. Add type validation for these types
4. Add basic tests

## Open Questions

1. **JSON Path Syntax**: Should we implement full JSONPath (RFC 6901) or DuckDB subset?
   - Current decision: DuckDB subset ($.field, $[0], .field)

2. **Geometry SRID**: How to handle spatial reference systems?
   - Current decision: Store SRID in GeometryDetails, validate on write

3. **Variant Internal Storage**: What should the internal storage be?
   - Current decision: Store as JSON string (similar to VARCHAR)

4. **Lambda Type Representation**: How to represent lambda functions?
   - Current decision: Store as string expression, compile on execution
