# Specification: Complex Type Functions and Operators

## Overview
This spec defines construction functions and operators for complex types (JSON, MAP, STRUCT, UNION) in dukdb-go for DuckDB v1.4.3 compatibility.

## ADDED Requirements

### Requirement 1: Complex Type Casting Functions

#### Scenario: Use to_json for any type
```sql
-- Given: Various data types
-- When: I execute:
SELECT
    to_json(42) as int_json,
    to_json('text') as str_json,
    to_json(true) as bool_json,
    to_json(ARRAY[1,2,3]) as arr_json,
    to_json({'a': 1, 'b': 2}) as struct_json,
    to_json(MAP(['x'], [10])) as map_json;
-- Then: All values should convert to JSON format
-- And: Preserve type information in JSON representation
```

#### Scenario: Use from_json to parse JSON
```sql
-- Given: JSON strings
-- When: I execute:
SELECT
    from_json('42', 'INTEGER') as int_val,
    from_json('"hello"', 'VARCHAR') as str_val,
    from_json('[1,2,3]', 'INTEGER[]') as arr_val;
-- Then: Should parse JSON and return specified type
-- And: Fail for type mismatches
```

#### Scenario: Use row_to_json
```sql
-- Given: Table rows
-- When: I execute:
SELECT row_to_json(users) as user_json
FROM users LIMIT 1;
-- Then: Should convert entire row to JSON object
-- And: Column names become JSON keys
```

### Requirement 2: MAP Construction Functions

#### Scenario: Use map() function
```sql
-- Given: Arrays of keys and values
-- When: I execute:
SELECT map(['a', 'b', 'c'], [1, 2, 3]) as m;
-- Then: Should return MAP with key-value pairs
-- And: Equivalent to MAP() constructor
```

#### Scenario: Use map_from_entries
```sql
-- Given: Array of structs with key/value fields
-- When: I execute:
SELECT map_from_entries([
    {'key': 'x', 'value': 10},
    {'key': 'y', 'value': 20}
]) as m;
-- Then: Should construct MAP from struct array
```

### Requirement 3: STRUCT Construction Functions

#### Scenario: Use struct_pack
```sql
-- Given: Field values
-- When: I execute:
SELECT struct_pack(
    name := 'Alice',
    age := 30,
    active := true
) as person;
-- Then: Should return STRUCT with named fields
```

#### Scenario: Use struct_insert
```sql
-- Given: Existing struct
-- When: I execute:
SELECT struct_insert(
    {'name': 'Alice', 'age': 30},
    email := 'alice@example.com'
) as extended;
-- Then: Should return new struct with added field
```

### Requirement 4: UNION Functions

#### Scenario: Use union_value for construction
```sql
-- Given: Member value
-- When: I execute:
SELECT union_value(num := 42) as u;
SELECT union_value(str := 'hello') as u;
SELECT union_value(flag := true) as u;
-- Then: Should create UNION with specified member active
```

#### Scenario: Use union_tag for inspection
```sql
-- Given: UNION values
-- When: I execute:
SELECT union_tag(union_value(num := 42));
-- Then: Should return 'num'
```

#### Scenario: Use union_extract for access
```sql
-- Given: UNION values
-- When: I execute:
SELECT union_extract(union_value(num := 42), 'num');
-- Then: Should return 42
-- And: Return NULL for non-active members
```

### Requirement 5: Complex Type Operators

#### Scenario: Use [] operator for MAP access
```sql
-- Given: MAP column
-- When: I execute:
SELECT settings['theme'] as theme
FROM user_prefs;
-- Then: Should access map element by key
-- And: Return NULL for missing keys
```

#### Scenario: Use . operator for STRUCT field access
```sql
-- Given: STRUCT column
-- When: I execute:
SELECT profile.name, profile.age
FROM users;
-- Then: Should access struct fields by name
```

#### Scenario: Use JSON operators
```sql
-- Given: JSON data
-- When: I execute:
SELECT data->'user'->>'name' as username
FROM events;
-- Then: Should navigate JSON structure
```

### Requirement 6: Array Functions on Complex Types

#### Scenario: Transform MAP keys to array
```sql
-- Given: MAP values
-- When: I execute:
SELECT map_keys(MAP(['a', 'b'], [1, 2])) as keys;
-- Then: Should return ['a', 'b'] as array
```

#### Scenario: Extract array of values
```sql
-- Given: MAP values
-- When: I execute:
SELECT map_values(MAP(['a', 'b'], [1, 2])) as values;
-- Then: Should return [1, 2] as array
```

#### Scenario: Check if key exists in MAP
```sql
-- Given: MAP values
-- When: I execute:
SELECT map_contains(MAP(['a', 'b'], [1, 2]), 'a') as has_a;
SELECT map_contains(MAP(['a', 'b'], [1, 2]), 'c') as has_c;
-- Then: First should return TRUE, second FALSE
```

### Requirement 7: Complex Type Inspection Functions

#### Scenario: Get JSON type information
```sql
-- Given: JSON values
-- When: I execute:
SELECT json_type('true') as t1,
       json_type('null') as t2,
       json_type('42') as t3,
       json_type('"hello"') as t4,
       json_type('[]') as t5,
       json_type('{}') as t6;
-- Then: Should return 'boolean', 'null', 'number', 'string', 'array', 'object'
```

#### Scenario: Get JSON keys
```sql
-- Given: JSON object
-- When: I execute:
SELECT json_keys('{"a": 1, "b": 2}') as keys;
-- Then: Should return ['a', 'b'] as array
```

#### Scenario: Check if field exists in STRUCT
```sql
-- Given: STRUCT values
-- When: I execute:
SELECT struct_extract(profile, 'email') IS NOT NULL as has_email
FROM users;
-- Then: Should indicate if field extraction succeeded
```

### Requirement 8: Complex Type Conversion Functions

#### Scenario: Convert MAP to JSON
```sql
-- Given: MAP data
-- When: I execute:
SELECT to_json(MAP(['a', 'b'], [1, 2])) as json_map;
-- Then: Should return '{"a": 1, "b": 2}' as JSON
```

#### Scenario: Convert STRUCT to JSON
```sql
-- Given: STRUCT data
-- When: I execute:
SELECT to_json({'name': 'Alice', 'age': 30}) as json_struct;
-- Then: Should return '{"name": "Alice", "age": 30}' as JSON
```

### Requirement 9: Nested Complex Type Functions

#### Scenario: Nested MAP access
```sql
-- Given: Nested MAP structure
-- When: I execute:
SELECT nested_map['outer']['inner'] as value
FROM table_with_nested_maps;
-- Then: Should navigate nested structure
```

#### Scenario: Nested STRUCT access
```sql
-- Given: Nested STRUCT
-- When: I execute:
SELECT outer.inner.field as value
FROM table_with_nested_structs;
-- Then: Should navigate nested structure
```

### Requirement 10: Complex Type Aggregations

#### Scenario: Aggregate STRUCT fields
```sql
-- Given: Table with STRUCT column having numeric fields
-- When: I execute:
SELECT AVG(profile.age) as avg_age
FROM users;
-- Then: Should aggregate across struct field
```

#### Scenario: Map with aggregate functions
```sql
-- Given: Table with MAP column
-- When: I execute:
SELECT
    map_keys(MAP_CONCAT_AGG(settings))
FROM user_metrics
GROUP BY category;
-- Then: Should aggregate maps across groups
```

## MODIFIED Requirements

### Function Registry
```
-- In: internal/functions/registry.go
-- Add: Registration for all complex type functions
-- Add: Type checking for complex type parameters
-- Add: Overload resolution for polymorphic functions
```

### Operator Implementation
```
-- In: internal/executor/operators.go
-- Add: Bracket operator [] for MAP access
-- Add: Dot operator . for STRUCT access
-- Add: Arrow operators -> and ->> for JSON navigation
-- Add: Path operators #> and #>> for JSON paths
```

## REMOVED Requirements
*None - purely additive*

## FUNCTION SIGNATURES

### Core Functions
- `to_json(any) -> JSON`
- `from_json(json_str VARCHAR, type VARCHAR) -> any`
- `row_to_json(record) -> JSON`
- `map(keys ARRAY, values ARRAY) -> MAP`
- `map_from_entries(ARRAY<STRUCT>) -> MAP`
- `struct_pack(...) -> STRUCT`
- `struct_insert(STRUCT, ...) -> STRUCT`
- `union_value(member := value) -> UNION`
- `union_tag(UNION) -> VARCHAR`
- `union_extract(UNION, member_name VARCHAR) -> any`

### Inspection Functions
- `map_keys(MAP) -> ARRAY`
- `map_values(MAP) -> ARRAY`
- `map_size(MAP) -> BIGINT`
- `map_contains(MAP, key) -> BOOLEAN`
- `json_valid(JSON) -> BOOLEAN`
- `json_type(JSON) -> VARCHAR`
- `json_keys(JSON) -> ARRAY`
- `json_array_length(JSON) -> BIGINT`

### Manipulation Functions
- `json_extract(JSON, path) -> JSON`
- `json_merge_patch(JSON, JSON) -> JSON`
- `struct_extract(STRUCT, field VARCHAR) -> any`

## OPERATOR PRECEDENCE

1. `.` (STRUCT field access) - Highest
2. `[]` (MAP/ARRAY access)
3. `->` `->>` (JSON field access)
4. `#>` `#>>` (JSON path access)

## TEST COVERAGE

### Unit Tests (40 scenarios)
- Each function with various input types
- Operator implementations
- Edge cases (NULL, empty structures, etc.)
- Type checking errors

### Integration Tests (20 scenarios)
- Function chains and compositions
- Complex type interactions
- COPY statement integration
- Query optimization paths

### Compatibility Tests (10 scenarios)
- DuckDB function names and signatures
- Operator precedence matching
- JSON function equivalency

## ACCEPTANCE CRITERIA

1. All scenarios pass with DuckDB v1.4.3 equivalent behavior
2. Function signatures match DuckDB exactly
3. Operator precedence follows SQL standard
4. Type checking provides clear error messages
5. NULL handling consistent throughout
6. Performance within 2x of equivalent DuckDB operations
7. Integration with existing function registry

## FUTURE ENHANCEMENTS

- Additional JSON functions (json_group_array, json_group_object)
- Specialized aggregate functions for complex types
- Lambda functions for complex type transformations
- Custom function extensions for complex types
- Vectorized implementations with SIMD

## REFERENCES

- DuckDB JSON Functions: https://duckdb.org/docs/stable/data/json/json_functions.html
- PostgreSQL JSON Operators: https://www.postgresql.org/docs/current/functions-json.html
- SQL Standard: Part 2 - Foundation (ISO/IEC 9075-2)
