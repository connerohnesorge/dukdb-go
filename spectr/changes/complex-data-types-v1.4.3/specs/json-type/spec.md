# Specification: JSON Type Implementation

## Overview
This spec adds the JSON data type and associated functions to dukdb-go for DuckDB v1.4.3 compatibility.

## ADDED Requirements

### Requirement 1: JSON Type Definition
#### Scenario: Create table with JSON column
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE events (
    id INTEGER,
    data JSON
);
-- Then: The table should be created successfully
-- And: The data column should have type JSON
```

#### Scenario: Insert valid JSON
```sql
-- Given: A table with JSON column
-- When: I execute:
INSERT INTO events VALUES
    (1, '{"type": "click", "x": 100, "y": 200}'),
    (2, '[1, 2, 3]'),
    (3, '"simple string"'),
    (4, '42');
-- Then: All rows should insert successfully
-- And: Data should be stored as valid JSON
```

#### Scenario: Reject invalid JSON
```sql
-- Given: A table with JSON column
-- When: I execute:
INSERT INTO events VALUES (5, '{invalid json}');
-- Then: The statement should fail with error "Invalid JSON"
```

### Requirement 2: JSON Casting
#### Scenario: Cast other types to JSON
```sql
-- Given: A table with various data types
-- When: I execute:
SELECT
    42::JSON as int_json,
    'hello'::JSON as str_json,
    ARRAY[1,2,3]::JSON as arr_json,
    MAP(['a','b'], [1,2])::JSON as map_json;
-- Then: All values should cast to valid JSON strings
-- And: Integers should serialize as numbers
-- And: Arrays and maps should serialize appropriately
```

#### Scenario: Extract values from JSON
```sql
-- Given: Table with JSON data
-- When: I execute:
SELECT
    data::INTEGER as int_val,
    data::VARCHAR as str_val,
    data::INTEGER[] as int_arr
FROM events;
-- Then: Values should extract correctly if compatible type
-- And: Return NULL for type mismatches
```

### Requirement 3: JSON Operators
#### Scenario: Extract JSON field with ->
```sql
-- Given: Table with JSON objects
-- When: I execute:
SELECT data->'type' as event_type
FROM events
WHERE id = 1;
-- Then: Should return JSON value `"click"`
-- And: Return NULL if key doesn't exist
```

#### Scenario: Extract field as text with ->>
```sql
-- Given: Table with JSON objects
-- When: I execute:
SELECT data->>'type' as event_type_text
FROM events
WHERE id = 1;
-- Then: Should return text value `click` (without quotes)
-- And: Return NULL if key doesn't exist
```

#### Scenario: Navigate JSON paths with #>
```sql
-- Given: Table with nested JSON
-- When: I execute:
SELECT data#>'{user,name}' as username
FROM events
WHERE data->>'type' = 'login';
-- Then: Should navigate nested structure
-- And: Return matched JSON value
```

### Requirement 4: JSON Construction Functions
#### Scenario: Use to_json function
```sql
-- Given: Various data values
-- When: I execute:
SELECT
    to_json(42) as json_int,
    to_json('hello') as json_str,
    to_json(ARRAY['a','b']) as json_array;
-- Then: All values should serialize to JSON format
```

#### Scenario: Use row_to_json
```sql
-- Given: A table with rows
-- When: I execute:
SELECT row_to_json(e) as row_json
FROM events e
WHERE id = 1;
-- Then: Should return JSON object representing entire row
```

### Requirement 5: JSON Validation Functions
#### Scenario: Use json_valid function
```sql
-- Given: Various string values
-- When: I execute:
SELECT
    json_valid('{"a":1}') as valid1,
    json_valid('{invalid}') as valid2,
    json_valid(NULL) as valid3;
-- Then: Should return TRUE for valid JSON
-- And: FALSE for invalid JSON
-- And: NULL for NULL input
```

#### Scenario: Use json_type function
```sql
-- Given: JSON data of different types
-- When: I execute:
SELECT
    json_type('42') as type1,
    json_type('"hello"') as type2,
    json_type('{"a":1}') as type3,
    json_type('[1,2,3]') as type4,
    json_type('true') as type5,
    json_type('null') as type6;
-- Then: Should return 'number', 'string', 'object', 'array', 'boolean', 'null'
```

#### Scenario: Use json_keys function
```sql
-- Given: JSON objects
-- When: I execute:
SELECT json_keys('{"a":1,"b":2,"c":3}') as keys;
-- Then: Should return ['a', 'b', 'c'] (as array of strings)
```

#### Scenario: Use json_quote function
```sql
-- Given: Text values
-- When: I execute:
SELECT json_quote('hello') as quoted;
-- Then: Should return '"hello"' (properly escaped JSON string)
```

### Requirement 6: JSON Existence Functions
#### Scenario: Use json_exists
```sql
-- Given: JSON data
-- When: I execute:
SELECT
    json_exists('{"a": {"b": 1}}', '$.a') as exists1,
    json_exists('{"a": {"b": 1}}', '$.c') as exists2;
-- Then: Should return TRUE for $.a, FALSE for $.c
```

#### Scenario: Use json_contains
```sql
-- Given: JSON objects and arrays
-- When: I execute:
SELECT json_contains('{"a":1,"b":2}', '{"a":1}');
SELECT json_contains('[1,2,3]', '[1]');
-- Then: Should return TRUE if right contains all elements of left
```

### Requirement 7: JSON Structure Functions
#### Scenario: Use json_structure
```sql
-- Given: JSON values
-- When: I execute:
SELECT json_structure('{"a":1,"b":{"c":2}}') as struct;
-- Then: Should return structure description
```

#### Scenario: Use json_group_structure
```sql
-- Given: Multiple JSON rows
-- When: I execute:
SELECT json_group_structure(data) as common_struct
FROM events;
-- Then: Should return common structure across all rows
```

### Requirement 8: JSON Transformation Functions
#### Scenario: Use from_json with structure
```sql
-- Given: JSON string and type specification
-- When: I execute:
SELECT from_json('{"x":1,"y":2}', 'STRUCT(x INTEGER, y INTEGER)');
-- Then: Should parse JSON and return structured type
```

#### Scenario: Use from_json_strict
```sql
-- Given: JSON and structure
-- When: I execute:
SELECT from_json_strict('[1,2,3]', 'INTEGER[]');
-- Then: Should strictly validate and parse JSON
```

### Requirement 9: JSON Array Functions
#### Scenario: Use json_array_length with optional path
```sql
-- Given: JSON arrays
-- When: I execute:
SELECT json_array_length('[1,2,3,4,5]') as len1,
       json_array_length('{"data": [1,2,3]}', '$.data') as len2;
-- Then: Should return array length or path navigated length
```

### Requirement 6: JSON Manipulation Functions
#### Scenario: Use json_merge_patch
```sql
-- Given: JSON objects
-- When: I execute:
SELECT json_merge_patch(
    '{"a":1,"b":2}',
    '{"b":3,"c":4}'
) as merged;
-- Then: Should return '{"a":1,"b":3,"c":4}'
```

### Requirement 7: COPY statement with JSON files
#### Scenario: Import JSON data from file
```sql
-- Given: A file with JSON data at '/tmp/data.json'
-- When: I execute:
COPY events FROM '/tmp/data.json';
-- Then: Should import JSON data into table
```

#### Scenario: Export query results as JSON
```sql
-- Given: Query results
-- When: I execute:
COPY (SELECT * FROM events) TO '/tmp/export.json';
-- Then: Should export data in JSON format
```

## MODIFIED Requirements

### Existing Type System
#### Scenario: Enhance type.go with JSON type
```
-- In: internal/storage/type.go
-- Add: TYPE_JSON = 37
-- Add: TypeName() method returns "JSON"
-- Add: Format() method for display
```

## REMOVED Requirements
*None - this is purely additive*

## Cross-Reference Dependencies

- `map-type` - Map type uses similar storage patterns
- `struct-type` - Both require compound type handling
- `complex-functions` - Construction functions build on type foundations

## Test Coverage Requirements

### Unit Tests
- [ ] JSON validation logic (10 test cases)
- [ ] Casting operations (20 test cases)
- [ ] Operator implementations (15 test cases)
- [ ] Function implementations (25 test cases)

### Integration Tests
- [ ] Full CREATE/INSERT/SELECT flow (5 test cases)
- [ ] COPY with JSON files (3 test cases)
- [ ] Complex type in queries (5 test cases)

### Performance Tests
- [ ] Large JSON parsing speed (1MB+ documents)
- [ ] Repeated operator usage (cached path)
- [ ] Nested structure depth (up to 64 levels)

## Acceptance Criteria

1. All scenarios above pass with correct behavior matching DuckDB v1.4.3
2. JSON validation rejects malformed input with clear error messages
3. Type casting follows DuckDB compatibility matrix
4. Performance within 2x of DuckDB for common operations
5. Memory usage reasonable for large JSON documents (streaming for >10MB)

## Future Enhancements

- JSON path expressions beyond basic navigation
- JSON aggregate functions (json_group_array, json_group_object)
- JSON schema validation
- Partial JSON updates
- Vectorized JSON parsing with SIMD
