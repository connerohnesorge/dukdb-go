# Complex Functions Specification

## Requirements

### Requirement: Complex Type Casting Functions

The system MUST provide functions to cast between complex types and JSON/TEXT.

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

### Requirement: MAP Construction Functions

The system MUST provide functions to construct MAP values.

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

### Requirement: STRUCT Construction Functions

The system MUST provide functions to construct STRUCT values.

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

### Requirement: UNION Functions

The system MUST provide functions to construct and inspect UNION values.

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

### Requirement: Complex Type Operators

The system MUST implement operators for accessing complex type elements.

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

### Requirement: Array Functions on Complex Types

The system MUST provide array functions that work with complex types.

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

### Requirement: Complex Type Inspection Functions

The system MUST provide functions to inspect the structure and type of complex values.

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

### Requirement: Complex Type Conversion Functions

The system MUST provide functions to convert between complex types.

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

### Requirement: Nested Complex Type Functions

The system MUST support function access on nested complex structures.

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

### Requirement: Complex Type Aggregations

The system MUST support aggregation functions on complex types.

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

### Requirement: Function Registry Updates

The system MUST register all complex type functions in the function registry.

#### Scenario: Register complex functions
- **Given** the function registry
- **When** the system starts
- **Then** all complex type functions are registered
- **And** parameter type checking is enabled
- **And** overload resolution supports polymorphic functions

### Requirement: Operator Implementation Updates

The system MUST implement operators for complex type access.

#### Scenario: Support complex type operators
- **Given** the executor operator system
- **When** expressions are evaluated
- **Then** bracket operator [] supports MAP access
- **And** dot operator . supports STRUCT access
- **And** arrow operators -> and ->> support JSON navigation
- **And** path operators #> and #>> support JSON paths

