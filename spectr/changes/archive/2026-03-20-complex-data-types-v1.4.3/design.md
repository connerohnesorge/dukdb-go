# Design: Complex Data Types Implementation

## Architecture Overview

The complex data types implementation integrates with dukdb-go's existing columnar storage architecture. Each complex type is stored as multiple physical vectors (one per field/representation) with a ValidityMask for NULL handling.

```
Logical Type (JSON, MAP, STRUCT, UNION)
    ↓
Physical Vectors (columnar storage)
    ↓
DataChunks with ValidityMasks
```

## Type System Integration

### 1. JSON Type

**Representation:**
```go
// JSON type stores data as string with validation metadata
type JSON struct {
    data     string
    validated bool
    parsed   interface{} // Cached parsed structure
}
```

**Storage:** Single VARCHAR vector
- Raw JSON string stored in string vector
- ValidityMask for NULL handling
- Optional parsed cache for repeated access

**Operators:**
- `json_col -> 'key'`: Extract JSON field (returns JSON)
- `json_col ->> 'key'`: Extract JSON field as text
- `json_col #> '{a,b}'`: Navigate to path
- `json_col #>> '{a,b}'`: Navigate to path as text

**Casting Rules:**
- ANY → JSON: Serialize to JSON string
- JSON → ANY: Parse JSON and cast to target type
- Validation: Ensure valid JSON on insert, fail on invalid

### 2. MAP Type

**Representation:**
```go
// Map physical storage uses separate vectors for keys and values
type MapData struct {
    keys   Vector   // Vector of key type
    values Vector   // Vector of value type
}
```

**Storage:** Two parallel vectors (keys and values)
- Keys stored in one vector
- Values stored in second vector
- ValidityMask per map entry
- Global ValidityMask for NULL maps

**Construction:**
```sql
-- Function-based
MAP(['key1', 'key2'], ['value1', 'value2'])

-- From existing arrays
map(zip(keys_array, values_array))
```

**Access:**
```sql
map_col['key'] -- Returns value or NULL if key not found
```

**Constraints:**
- Keys must be unique within map instance
- Key and value types are consistent across map
- Different rows can have different keys

### 3. STRUCT Type

**Representation:**
```go
// Struct stores fields as separate vectors (column-wise)
type StructData struct {
    fields map[string]Vector  // Field name → Vector
    order  []string           // Field order preservation
}
```

**Storage:** One vector per field (columnar)
- Each field stored as separate typed vector
- All field vectors same length
- Single ValidityMask for NULL structs
- Field order preserved for display

**Construction:**
```sql
-- DuckDB inline syntax
{'field1': value1, 'field2': value2}

-- Or function-based
struct_pack(field1 := value1, field2 := value2)
```

**Access Methods:**
```sql
-- Static field access (parsed at query compile time)
struct_col.field_name

-- Dynamic field access (field name as string)
struct_extract(struct_col, 'field_name')
```

**Type Safety:**
- All rows have same field names and types
- Field access validated at compile time when possible
- Runtime error for invalid field names

### 4. UNION Type

**Representation:**
```go
// Union stores discriminator and value for each row
type UnionData struct {
    discriminator []string  // Which member is active per row
    members      map[string]Vector  // One vector per member type
    activeValues Vector  // Contiguous storage of active values
}
```

**Storage:**
- Discriminator vector (string)
- One vector per member type
- Active values stored in compact representation

**Construction:**
```sql
-- Tag-based construction
union_value(member_name := value)

-- Alternative syntax
union_value(member_name = value)
```

**Inspection:**
```sql
-- Get which member is active for each row
union_tag(union_col)

-- Extract specific member (NULL if not active)
union_extract(union_col, 'member_name')
```

## Parser Integration

### Type Literal Syntax

**JSON:**
```
JSONLiteral :=
    StringLiteral  -- Must be valid JSON string
```

**MAP:**
```
MapConstructor :=
    'MAP' '(' ArrayLiteral ',' ArrayLiteral ')'
```

**STRUCT:**
```
StructLiteral :=
      '{' FieldList '}'
    | 'struct_pack' '(' FieldAssignments ')'

FieldList :=
    Field (',' Field)*

Field :=
    StringLiteral ':' Expression
```

**UNION:**
```
UnionConstructor :=
    'union_value' '(' MemberAssignment ')'

MemberAssignment :=
    Identifier ':=' Expression
    | Identifier '=' Expression
```

### Type Names in DDL

```sql
CREATE TABLE example (
    json_col JSON,
    map_col MAP(VARCHAR, INTEGER),
    struct_col STRUCT(field1 INTEGER, field2 VARCHAR),
    union_col UNION(num INTEGER, text VARCHAR)
);
```

## Storage Layer

### Vectorized Operations

Complex types leverage existing Vector interface:

```go
// JSON Vector
jsonVector := StringVector{...}  // Stores JSON strings

// MAP Vector
mapVector := &MapVector{
    keys:   Vector,   // Sub-vector for keys
    values: Vector,   // Sub-vector for values
}

// STRUCT Vector
structVector := &StructVector{
    fields: map[string]Vector{
        "field1": IntegerVector{},
        "field2": StringVector{},
    },
}

// UNION Vector
unionVector := &UnionVector{
    discriminator: StringVector{},
    members: map[string]Vector{
        "num":  IntegerVector{},
        "text": StringVector{},
    },
}
```

### Serialization Format

Data stored in DuckDB format follows these rules:

**JSON:**
- Type ID: 37
- Storage: Length-prefixed UTF-8 string
- Validation: On write, verify valid JSON

**MAP:**
- Type ID: 38
- Storage: Child vectors for keys and values
- List size: Variable per row

**STRUCT:**
- Type ID: 39
- Storage: One child vector per field
- Field count: Fixed per type definition

**UNION:**
- Type ID: 40
- Storage: Discriminator vector + member vectors
- Members: Fixed per type definition

## Execution Engine

### Operator Implementations

**JSON Navigation Operators:**
```go
func (o *JSONExtractOperator) Execute(chunk DataChunk) DataChunk {
    // For each row
    for i := 0; i < chunk.Size(); i++ {
        jsonData := chunk.GetValue(0, i)
        path := chunk.GetValue(1, i)

        // Parse JSON if needed
        parsed := parseJSON(jsonData.GetString())

        // Navigate path
        result := navigateJSON(parsed, path)

        output.SetValue(i, result)
    }
}
```

**MAP Element Access:**
```go
func (o *MapAccessOperator) Execute(chunk DataChunk) DataChunk {
    // Extract map and key
    mapData := chunk.GetVector(0)
    keyData := chunk.GetVector(1)

    // For each row, search keys vector for matching key
    // Return corresponding value or NULL
}
```

**STRUCT Field Access:**
```go
func (o *StructFieldAccessOperator) Execute(chunk DataChunk) DataChunk {
    structData := chunk.GetVector(0)

    // Field name determined at operator creation
    fieldVector := structData.GetField(o.fieldName)

    // Return field vector directly
    return fieldVector
}
```

### Function Implementations

**to_json()**:
```go
func toJSON(value interface{}) string {
    // Switch on input type
    switch v := value.(type) {
    case int64:
        return strconv.FormatInt(v, 10)
    case string:
        return `"` + escapeJSONString(v) + `"`
    case map[string]interface{}:
        return marshalJSONObject(v)
    // ... other types
    }
}
```

**map()**:
```go
func mapConstructor(keys Vector, values Vector) MapVector {
    // Validate same length
    if keys.Size() != values.Size() {
        return error
    }

    // Create MAP vector
    return &MapVector{keys: keys, values: values}
}
```

## Performance Considerations

### JSON Parsing
- Lazy parsing: Only parse JSON when needed
- Cache parsed structures for repeated access
- Use fast JSON library (e.g., jsoniter, gjson)

### MAP Lookup
- For large maps: Build hash index on first access
- Small maps: Linear search is acceptable
- Cache frequently accessed keys

### STRUCT Field Access
- Compile-time field access: Direct vector reference
- Dynamic field access: Hash map lookup
- Optimize common case (static access)

### UNION Operations
- Store values in compact format
- Extract member on demand
- Fast discriminator checking

## Memory Management

### Nested Structures
- Maximum nesting depth: 64 levels (configurable)
- Recursion limits prevent stack overflow
- Progressive allocation for deep nesting

### Large Objects
- JSON > 1MB: Store in separate allocation
- MAP/STRUCT with many fields: Use dictionary encoding
- Streaming for very large values

## Error Handling

### Validation Errors
```sql
-- Invalid JSON
CREATE TABLE t (j JSON);
INSERT INTO t VALUES ('{invalid json}');  -- Should error

-- Invalid MAP keys (duplicates)
SELECT MAP(['a', 'a'], [1, 2]);  -- Should error
```

### Type Errors
```sql
-- Invalid field access
SELECT struct_col.nonexistent_field;  -- Should error

-- Invalid union member access
SELECT union_extract(union_col, 'wrong_member');  -- Returns NULL
```

## Compatibility with DuckDB v1.4.3

### Type IDs
Use same type IDs as DuckDB for compatibility:
- JSON: 37
- MAP: 38
- STRUCT: 39
- UNION: 40

### Function Names
Match DuckDB function names exactly:
- to_json/from_json
- json_extract/json_keys/etc
- map_keys/map_values
- struct_pack/struct_extract
- union_tag/union_extract

### Behavior Parity
- JSON validation rules
- MAP key uniqueness enforcement
- STRUCT field ordering preservation
- UNION discriminator behavior

## Future Enhancements

### Phase 2 (Post v1.4.3 Compatibility)
- Partial updates for complex types
- Complex type indexes
- Specialized aggregate functions
- Vectorized JSON parsing

### Performance Optimizations
- SIMD-accelerated operations
- Compressed storage for nested structures
- Cached access paths

## Migration Path

For existing users:
1. New types are additive - no breaking changes
2. Map type extended with full functionality
3. Migration guide for type conversions
4. Compatibility mode for older syntax

## References

- DuckDB Complex Types: https://duckdb.org/docs/stable/sql/data_types/overview.html
- DuckDB JSON: https://duckdb.org/docs/stable/data/json/json_type.html
- DuckDB MAP: https://duckdb.org/docs/stable/sql/data_types/map.html
- DuckDB STRUCT: https://duckdb.org/docs/stable/sql/data_types/struct.html
