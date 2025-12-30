# TypeInfo Metadata System Design

## Architecture Overview

The TypeInfo system provides a two-level type representation:
1. **Type enum** (existing) - Primitive type identifier (TYPE_INTEGER, TYPE_VARCHAR, etc.)
2. **TypeInfo interface** (new) - Metadata container with type-specific details

```
┌──────────────────────────────────────────────────────────┐
│                      TypeInfo Interface                   │
│  InternalType() Type                                      │
│  Details() TypeDetails  // nil for primitives             │
└──────────────────────────────────────────────────────────┘
                            │
                            │
      ┌─────────────────────┴────────────────────────┬──────────────┐
      │                     │                         │              │
┌─────▼─────┐      ┌────────▼────────┐      ┌────────▼────────┐   ...
│ Primitive │      │  DecimalDetails │      │   EnumDetails    │
│  (nil)    │      │  Width uint8    │      │  Values []string │
└───────────┘      │  Scale uint8    │      └──────────────────┘
                   └─────────────────┘
```

## Core Interfaces

### TypeInfo Interface

```go
// TypeInfo provides metadata for DuckDB types
type TypeInfo interface {
    // InternalType returns the primitive type enum
    InternalType() Type

    // Details returns type-specific metadata (nil for primitives)
    Details() TypeDetails

    // logicalType returns the internal DuckDB logical type representation
    // (unexported method for backend integration)
    logicalType() mapping.LogicalType
}
```

**Design Decisions**:
- **Immutable**: TypeInfo instances never modified after creation (thread-safe)
- **Interface**: Enables multiple implementations (future extensibility)
- **Nil Details**: Primitives (INTEGER, VARCHAR, etc.) return nil for Details()
- **logicalType()**: Included for API compatibility with duckdb-go v1.4.3; stub implementation returns zero value until full LogicalType implementation completed

### TypeDetails Struct Hierarchy

**IMPORTANT**: TypeDetails types are STRUCTS with public fields, not interfaces with methods. This matches duckdb-go v1.4.3 exactly.

```go
// TypeDetails is a marker interface for type-specific metadata
type TypeDetails interface {
    isTypeDetails() // Unexported marker method (matches duckdb-go v1.4.3)
}

// DecimalDetails provides DECIMAL type metadata (STRUCT, not interface)
type DecimalDetails struct {
    Width uint8  // 1-38 (precision)
    Scale uint8  // 0-width (decimal places)
}

func (d *DecimalDetails) isTypeDetails() {}

// EnumDetails provides ENUM type metadata (STRUCT, not interface)
type EnumDetails struct {
    Values []string  // Ordered enum values (defensive copy in Details())
}

func (e *EnumDetails) isTypeDetails() {}

// ListDetails provides LIST type metadata (STRUCT, not interface)
type ListDetails struct {
    Child TypeInfo  // Element type (recursive)
}

func (l *ListDetails) isTypeDetails() {}

// ArrayDetails provides ARRAY type metadata (STRUCT, not interface)
type ArrayDetails struct {
    Child TypeInfo  // Element type
    Size  uint64    // Fixed array size
}

func (a *ArrayDetails) isTypeDetails() {}

// MapDetails provides MAP type metadata (STRUCT, not interface)
type MapDetails struct {
    Key   TypeInfo  // Key type
    Value TypeInfo  // Value type
}

func (m *MapDetails) isTypeDetails() {}

// StructDetails provides STRUCT type metadata (STRUCT, not interface)
type StructDetails struct {
    Entries []StructEntry  // Field list
}

func (s *StructDetails) isTypeDetails() {}

// UnionDetails provides UNION type metadata (STRUCT, not interface)
type UnionDetails struct {
    Members []UnionMember  // Tagged union members
}

func (u *UnionDetails) isTypeDetails() {}
```

**Design Decisions**:
- **Marker interface**: TypeDetails interface prevents external implementations
- **Public fields**: Direct field access (Width, Scale, Values, etc.) instead of getter methods
- **Defensive copying**: EnumDetails.Values and UnionDetails.Members copied in Details() to prevent modification
- **Recursive types**: Child field contains TypeInfo for nested types (LIST, ARRAY)

### Supporting Types

```go
// StructEntry represents a STRUCT field
type StructEntry interface {
    Name() string     // Field name
    Info() TypeInfo   // Field type
}

// UnionMember represents a UNION variant
type UnionMember struct {
    Name string       // Variant tag
    Type TypeInfo     // Variant type
}
```

## Construction Functions

### Design Pattern

All constructors follow this pattern:
1. **Validate parameters** (constraints, uniqueness, etc.)
2. **Create internal implementation** (unexported struct)
3. **Return TypeInfo interface**

```go
func NewDecimalInfo(width, scale uint8) (TypeInfo, error) {
    // 1. Validate
    if width < 1 || width > 38 {
        return nil, fmt.Errorf("%w: width must be 1-38, got %d", ErrInvalidDecimalParams, width)
    }
    if scale > width {
        return nil, fmt.Errorf("%w: scale %d exceeds width %d", ErrInvalidDecimalParams, scale, width)
    }

    // 2. Create internal struct
    return &decimalTypeInfo{
        width: width,
        scale: scale,
    }, nil
}
```

### Constructor List

```go
// Primitive types
func NewTypeInfo(t Type) (TypeInfo, error)

// Complex types with validation
func NewDecimalInfo(width, scale uint8) (TypeInfo, error)
func NewEnumInfo(first string, others ...string) (TypeInfo, error)
func NewListInfo(childInfo TypeInfo) (TypeInfo, error)
func NewStructInfo(firstEntry StructEntry, others ...StructEntry) (TypeInfo, error)
func NewMapInfo(keyInfo, valueInfo TypeInfo) (TypeInfo, error)
func NewArrayInfo(childInfo TypeInfo, size uint64) (TypeInfo, error)
func NewUnionInfo(memberTypes []TypeInfo, memberNames []string) (TypeInfo, error)
```

## Internal Implementation

### Struct Hierarchy

```go
// Internal implementations (unexported)

// primitiveTypeInfo - For simple types (INTEGER, VARCHAR, etc.)
type primitiveTypeInfo struct {
    typ Type
}

func (p *primitiveTypeInfo) InternalType() Type { return p.typ }
func (p *primitiveTypeInfo) Details() TypeDetails { return nil }
func (p *primitiveTypeInfo) logicalType() mapping.LogicalType {
    // Stub implementation - full conversion logic in future LogicalType work
    return mapping.LogicalType{}
}

// decimalTypeInfo - DECIMAL with precision/scale
type decimalTypeInfo struct {
    width uint8
    scale uint8
}

func (d *decimalTypeInfo) InternalType() Type { return TYPE_DECIMAL }
func (d *decimalTypeInfo) Details() TypeDetails {
    return &DecimalDetails{
        Width: d.width,
        Scale: d.scale,
    }
}
func (d *decimalTypeInfo) logicalType() mapping.LogicalType {
    return mapping.LogicalType{}  // Stub
}

// enumTypeInfo - ENUM with value list
type enumTypeInfo struct {
    values []string  // Internal storage (never modified)
}

func (e *enumTypeInfo) InternalType() Type { return TYPE_ENUM }
func (e *enumTypeInfo) Details() TypeDetails {
    // Defensive copy to prevent modification
    valuesCopy := make([]string, len(e.values))
    copy(valuesCopy, e.values)
    return &EnumDetails{
        Values: valuesCopy,
    }
}
func (e *enumTypeInfo) logicalType() mapping.LogicalType {
    return mapping.LogicalType{}  // Stub
}

// listTypeInfo - LIST with element type
type listTypeInfo struct {
    child TypeInfo
}

func (l *listTypeInfo) InternalType() Type { return TYPE_LIST }
func (l *listTypeInfo) Details() TypeDetails {
    return &ListDetails{
        Child: l.child,
    }
}
func (l *listTypeInfo) logicalType() mapping.LogicalType {
    return mapping.LogicalType{}  // Stub
}

// Similar pattern for: arrayTypeInfo, mapTypeInfo, structTypeInfo, unionTypeInfo
// All return TypeDetails structs from Details() method
// All implement logicalType() stub returning zero value
```

**Design Decisions**:
- **Unexported structs**: Hide implementation details
- **Interface return**: Enables future alternative implementations
- **Immutable fields**: Thread-safe without locking
- **Embedded validation**: Constructors ensure invariants

## Validation Rules

### DECIMAL Type
```go
// Constraints
- Width: 1-38 (DuckDB supports up to 38 digits)
- Scale: 0-width (decimal places cannot exceed width)

// Examples
NewDecimalInfo(18, 4)  // ✅ DECIMAL(18,4)
NewDecimalInfo(38, 38) // ✅ DECIMAL(38,38) - all decimal places
NewDecimalInfo(39, 2)  // ❌ Error: width exceeds 38
NewDecimalInfo(10, 11) // ❌ Error: scale exceeds width
```

### ENUM Type
```go
// Constraints
- At least 1 value required (first parameter)
- No duplicate values
- Values stored in order

// Examples
NewEnumInfo("RED", "GREEN", "BLUE")     // ✅
NewEnumInfo("A")                        // ✅ Single value OK
NewEnumInfo("X", "Y", "X")              // ❌ Error: duplicate "X"
NewEnumInfo()                           // ❌ Compile error: first required
```

### STRUCT Type
```go
// Constraints
- At least 1 field required
- No duplicate field names
- Field names support special characters (auto-escaped)

// Examples
NewStructInfo(
    structEntry{name: "id", info: intInfo},
    structEntry{name: "name", info: varcharInfo},
)  // ✅

NewStructInfo(
    structEntry{name: "x", info: intInfo},
    structEntry{name: "x", info: floatInfo},
)  // ❌ Error: duplicate field "x"
```

### MAP Type
```go
// Constraints (as implemented in duckdb-go v1.4.3)
- Key TypeInfo cannot be nil
- Value TypeInfo cannot be nil

// NOTE: The reference implementation does NOT validate key comparability.
// While DuckDB semantically requires comparable keys, the validation
// happens at query execution time, not at TypeInfo construction.

// Examples
NewMapInfo(intInfo, varcharInfo)      // ✅ MAP(INTEGER, VARCHAR)
NewMapInfo(varcharInfo, structInfo)   // ✅ MAP(VARCHAR, STRUCT)
NewMapInfo(listInfo, intInfo)         // ✅ Allowed (validation deferred to execution)
NewMapInfo(nil, intInfo)              // ❌ Error: keyInfo cannot be nil
```

### ARRAY Type
```go
// Constraints
- Size must be > 0
- Element type can be any type

// Examples
NewArrayInfo(intInfo, 10)   // ✅ INTEGER[10]
NewArrayInfo(listInfo, 5)   // ✅ LIST(INTEGER)[5]
NewArrayInfo(intInfo, 0)    // ❌ Error: size must be > 0
```

### UNION Type
```go
// Constraints
- At least 1 member required
- Member names and types must match in length
- No duplicate member names

// Examples
NewUnionInfo(
    []TypeInfo{intInfo, varcharInfo},
    []string{"num", "str"},
)  // ✅

NewUnionInfo(
    []TypeInfo{intInfo},
    []string{"a", "b"},
)  // ❌ Error: length mismatch

NewUnionInfo(
    []TypeInfo{intInfo, floatInfo},
    []string{"x", "x"},
)  // ❌ Error: duplicate member "x"
```

## Integration Points

### Statement Introspection

```go
// backend.go - Add to BackendStmtIntrospector
type BackendStmtIntrospector interface {
    // Existing methods...
    ColumnCount() (int, error)
    ColumnName(idx int) (string, error)
    ColumnType(idx int) (Type, error)

    // NEW: Type metadata
    GetColumnTypeInfo(idx int) (TypeInfo, error)
}

// prepared.go - Add to Stmt
func (s *Stmt) ColumnTypeInfo(n int) (TypeInfo, error) {
    // Delegate to backend
    if introspector, ok := s.backendStmt.(backend.BackendStmtIntrospector); ok {
        return introspector.GetColumnTypeInfo(n)
    }
    return nil, ErrNotSupported
}
```

### UDF Integration

```go
// scalar_udf.go - Update config
type ScalarFuncConfig struct {
    InputTypeInfos   []TypeInfo    // CHANGED: from []Type
    ResultTypeInfo   TypeInfo      // CHANGED: from Type
    VariadicTypeInfo TypeInfo      // CHANGED: from Type (optional)
    Volatile         bool
    SpecialNullHandling bool
}

// table_udf.go - Update config
type ColumnInfo struct {
    Name string
    T    TypeInfo    // CHANGED: from Type
}

// aggregate_udf.go - Update config
type AggregateFuncConfig struct {
    InputTypeInfos []TypeInfo    // CHANGED: from []Type
    StateTypeInfo  TypeInfo      // CHANGED: from Type
    ResultTypeInfo TypeInfo      // CHANGED: from Type
    // ... other fields
}
```

### Catalog Persistence

```go
// internal/catalog/column.go
type Column struct {
    Name     string
    TypeInfo TypeInfo    // CHANGED: from Type only
    NotNull  bool
}

// Serialization format
type SerializedColumn struct {
    Name         string
    Type         Type    // Base type
    TypeDetails  []byte  // JSON-encoded TypeDetails (nil for primitives)
    NotNull      bool
}
```

## Serialization Format

### JSON Encoding

```go
type SerializedTypeInfo struct {
    Type    Type    `json:"type"`              // Required: base type
    Details json.RawMessage `json:"details,omitempty"`  // Optional: type-specific data
}

// DECIMAL example
{
    "type": "DECIMAL",
    "details": {"width": 18, "scale": 4}
}

// ENUM example
{
    "type": "ENUM",
    "details": {"values": ["RED", "GREEN", "BLUE"]}
}

// LIST example (recursive)
{
    "type": "LIST",
    "details": {
        "child": {
            "type": "INTEGER",
            "details": null
        }
    }
}

// STRUCT example (nested)
{
    "type": "STRUCT",
    "details": {
        "entries": [
            {"name": "id", "info": {"type": "INTEGER"}},
            {"name": "name", "info": {"type": "VARCHAR"}}
        ]
    }
}
```

## Performance Considerations

### TypeInfo Caching

```go
// Cache common primitive TypeInfo instances
var (
    typeInfoCache sync.Map  // map[Type]TypeInfo
)

func NewTypeInfo(t Type) (TypeInfo, error) {
    // Check cache first
    if cached, ok := typeInfoCache.Load(t); ok {
        return cached.(TypeInfo), nil
    }

    // Create and cache
    info := &primitiveTypeInfo{typ: t}
    typeInfoCache.Store(t, info)
    return info, nil
}
```

**Benefits**:
- Reduce allocations for common types (INTEGER, VARCHAR, etc.)
- Safe due to immutability
- sync.Map for lock-free concurrent access

### Lazy Detail Construction

For complex types, delay expensive operations:

```go
// Example: ENUM value lookup
type enumTypeInfo struct {
    values     []string
    valueIndex map[string]int  // Lazy: built on first lookup
    indexMu    sync.Mutex
}

func (e *enumTypeInfo) IndexOf(value string) int {
    e.indexMu.Lock()
    defer e.indexMu.Unlock()

    if e.valueIndex == nil {
        // Build index on first access
        e.valueIndex = make(map[string]int, len(e.values))
        for i, v := range e.values {
            e.valueIndex[v] = i
        }
    }

    return e.valueIndex[value]
}
```

## Error Handling

### Error Variables Pattern (duckdb-go Compatible)

**Design Decision**: Use simple error variables with `getError()` wrapper to maintain strict API compatibility with duckdb-go v1.4.3.

**Pattern**: Define error constants in `errors.go` and wrap with `getError(errAPI, err)` in constructors.

```go
// errors.go - Add TypeInfo validation errors

const max_decimal_width = 38

var (
    // TypeInfo construction errors
    errEmptyName = errors.New("empty name")

    errInvalidDecimalWidth = fmt.Errorf(
        "the DECIMAL width must be between 1 and %d",
        max_decimal_width,
    )
    errInvalidDecimalScale = errors.New(
        "the DECIMAL scale must be less than or equal to the width",
    )

    errInvalidArraySize = errors.New(
        "invalid ARRAY size",
    )

    errDuplicateName = errors.New(
        "duplicate name",
    )
)

// Helper functions for dynamic error messages
func duplicateNameError(name string) error {
    return fmt.Errorf("duplicate name: %s", name)
}

func interfaceIsNilError(param string) error {
    return fmt.Errorf("%s is nil", param)
}
```

### Usage in TypeInfo Constructors

```go
// type_info.go

// DECIMAL validation
func NewDecimalInfo(width, scale uint8) (TypeInfo, error) {
    if width < 1 || width > max_decimal_width {
        return nil, getError(errAPI, errInvalidDecimalWidth)
    }
    if scale > width {
        return nil, getError(errAPI, errInvalidDecimalScale)
    }
    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:         TYPE_DECIMAL,
            decimalWidth: width,
            decimalScale: scale,
        },
    }, nil
}

// ENUM duplicate detection
func NewEnumInfo(first string, others ...string) (TypeInfo, error) {
    if first == "" {
        return nil, getError(errAPI, errEmptyName)
    }

    values := append([]string{first}, others...)
    seen := make(map[string]bool, len(values))

    for _, val := range values {
        if val == "" {
            return nil, getError(errAPI, errEmptyName)
        }
        if seen[val] {
            return nil, getError(errAPI, duplicateNameError(val))
        }
        seen[val] = true
    }

    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:       TYPE_ENUM,
            enumValues: values,
        },
    }, nil
}

// MAP nil checking
func NewMapInfo(keyInfo, valueInfo TypeInfo) (TypeInfo, error) {
    if keyInfo == nil {
        return nil, getError(errAPI, interfaceIsNilError("key"))
    }
    if valueInfo == nil {
        return nil, getError(errAPI, interfaceIsNilError("value"))
    }

    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:     TYPE_MAP,
            mapKey:   keyInfo,
            mapValue: valueInfo,
        },
    }, nil
}

// ARRAY size validation
func NewArrayInfo(childInfo TypeInfo, size uint64) (TypeInfo, error) {
    if childInfo == nil {
        return nil, getError(errAPI, interfaceIsNilError("child"))
    }
    if size == 0 {
        return nil, getError(errAPI, errInvalidArraySize)
    }

    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:       TYPE_ARRAY,
            arrayChild: childInfo,
            arraySize:  size,
        },
    }, nil
}

// STRUCT field validation
func NewStructInfo(firstEntry StructEntry, others ...StructEntry) (TypeInfo, error) {
    entries := append([]StructEntry{firstEntry}, others...)
    seen := make(map[string]bool, len(entries))

    for _, entry := range entries {
        name := entry.Name()
        if name == "" {
            return nil, getError(errAPI, errEmptyName)
        }
        if seen[name] {
            return nil, getError(errAPI, duplicateNameError(name))
        }
        seen[name] = true
    }

    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:          TYPE_STRUCT,
            structEntries: entries,
        },
    }, nil
}

// UNION member validation
func NewUnionInfo(memberTypes []TypeInfo, memberNames []string) (TypeInfo, error) {
    if len(memberTypes) == 0 {
        return nil, getError(errAPI, errors.New("at least one member required"))
    }
    if len(memberTypes) != len(memberNames) {
        return nil, getError(errAPI, errors.New(
            "memberTypes and memberNames must have the same length",
        ))
    }

    seen := make(map[string]bool, len(memberNames))
    for _, name := range memberNames {
        if name == "" {
            return nil, getError(errAPI, errEmptyName)
        }
        if seen[name] {
            return nil, getError(errAPI, duplicateNameError(name))
        }
        seen[name] = true
    }

    members := make([]UnionMember, len(memberTypes))
    for i := range memberTypes {
        members[i] = UnionMember{
            Name: memberNames[i],
            Type: memberTypes[i],
        }
    }

    return &typeInfo{
        baseTypeInfo: baseTypeInfo{
            Type:         TYPE_UNION,
            unionMembers: members,
        },
    }, nil
}
```

### Error Message Format

All TypeInfo errors follow the duckdb-go pattern:
```
API error: <specific error>
```

Examples:
- `API error: the DECIMAL width must be between 1 and 38`
- `API error: the DECIMAL scale must be less than or equal to the width`
- `API error: duplicate name: field_name`
- `API error: invalid ARRAY size`
- `API error: key is nil`

This matches the reference implementation's error testing pattern using string matching.

## Testing Strategy

### Unit Test Categories

1. **Constructor Tests** - Validate all 7 construction functions
2. **Validation Tests** - Test all constraint violations
3. **Details Tests** - Verify Details() extraction
4. **Recursion Tests** - Test nested types (LIST of STRUCT of MAP)
5. **Serialization Tests** - Round-trip encoding/decoding
6. **Equality Tests** - TypeInfo comparison

### Example Test Structure

```go
func TestNewDecimalInfo(t *testing.T) {
    tests := []struct {
        name      string
        width     uint8
        scale     uint8
        wantErr   bool
        errType   error
    }{
        {"valid 18,4", 18, 4, false, nil},
        {"valid 38,38", 38, 38, false, nil},
        {"width too high", 39, 2, true, ErrInvalidDecimalParams},
        {"width too low", 0, 0, true, ErrInvalidDecimalParams},
        {"scale exceeds width", 10, 11, true, ErrInvalidDecimalParams},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            info, err := NewDecimalInfo(tt.width, tt.scale)
            if tt.wantErr {
                require.Error(t, err)
                require.ErrorIs(t, err, tt.errType)
                require.Nil(t, info)
            } else {
                require.NoError(t, err)
                require.NotNil(t, info)
                require.Equal(t, TYPE_DECIMAL, info.InternalType())

                details := info.Details()
                require.NotNil(t, details)

                decDetails, ok := details.(DecimalDetails)
                require.True(t, ok)
                require.Equal(t, tt.width, decDetails.Width())
                require.Equal(t, tt.scale, decDetails.Scale())
            }
        })
    }
}
```

## File Organization

```
type_info.go                # TypeInfo interface, construction functions, public API
type_info_impl.go           # Internal TypeInfo implementations (unexported structs)
type_details.go             # TypeDetails interface hierarchy
type_info_serialize.go      # JSON serialization support
type_info_cache.go          # TypeInfo caching for primitives
type_info_test.go           # Unit tests for all constructors
type_info_validation_test.go  # Constraint validation tests
type_info_integration_test.go # Integration tests with SQL
```

## Migration Path

Since TypeInfo is a pure addition with no breaking changes:

**Phase 1**: Add TypeInfo system alongside existing Type enum
**Phase 2**: Update UDF configs to use TypeInfo (optional parameters)
**Phase 3**: Add Stmt.ColumnTypeInfo() method
**Phase 4**: Integrate with catalog persistence
**Phase 5**: Update executor to use TypeInfo for complex type operations

**Backward Compatibility**: All existing code continues to work. TypeInfo is opt-in for advanced features.

## Reference Implementation Compatibility

### Key Differences from duckdb-go

**Same**:
- API signatures match exactly
- Validation rules identical
- Error messages compatible

**Different** (Internal):
- No CGO - Pure Go implementation
- Simplified internal structs (no C pointers)
- JSON serialization (duckdb-go uses C serialization)

### Testing Against Reference

```go
// Compatibility test pattern
func TestTypeInfoCompatibility(t *testing.T) {
    // Create TypeInfo in both implementations
    dukInfo, err := dukdb.NewDecimalInfo(18, 4)
    require.NoError(t, err)

    duckInfo, err := duckdb.NewDecimalInfo(18, 4)
    require.NoError(t, err)

    // Verify same behavior
    require.Equal(t, duckInfo.InternalType(), dukInfo.InternalType())

    dukDetails := dukInfo.Details().(dukdb.DecimalDetails)
    duckDetails := duckInfo.Details().(duckdb.DecimalDetails)

    require.Equal(t, duckDetails.Width(), dukDetails.Width())
    require.Equal(t, duckDetails.Scale(), dukDetails.Scale())
}
```

## Summary

This design provides:
- ✅ **Full API compatibility** with duckdb-go v1.4.3
- ✅ **Pure Go implementation** (no CGO)
- ✅ **Type safety** with compile-time validation
- ✅ **Thread safety** through immutability
- ✅ **Extensibility** via interface-based design
- ✅ **Performance** through caching and lazy initialization
- ✅ **Testability** with clear validation rules

The implementation follows Go best practices while maintaining exact API parity with the reference implementation.
