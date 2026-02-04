# Specification: JSON Type Implementation

## ADDED Requirements

### Requirement: JSON Type Definition

The system MUST implement a JSON data type that stores and validates JSON data.

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

### Requirement: JSON Casting

The system MUST provide casting capabilities between JSON and other types.

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

### Requirement: JSON Operators

The system MUST implement standard JSON extraction and navigation operators.

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

### Requirement: JSON Construction Functions

The system MUST provide functions to construct JSON values.

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

### Requirement: JSON Validation Functions

The system MUST provide functions to validate and inspect JSON data.

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

### Requirement: JSON Existence Functions

The system MUST provide functions to check for the existence of JSON paths.

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

### Requirement: JSON Structure Functions

The system MUST provide functions to analyze JSON structure.

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

### Requirement: JSON Transformation Functions

The system MUST provide functions to transform JSON to structured types.

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

### Requirement: JSON Array Functions

The system MUST provide functions to manipulate JSON arrays.

#### Scenario: Use json_array_length with optional path
```sql
-- Given: JSON arrays
-- When: I execute:
SELECT json_array_length('[1,2,3,4,5]') as len1,
       json_array_length('{"data": [1,2,3]}', '$.data') as len2;
-- Then: Should return array length or path navigated length
```

### Requirement: JSON Manipulation Functions

The system MUST provide functions to manipulate JSON objects.

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

### Requirement: COPY statement with JSON files

The system MUST support importing and exporting JSON data via COPY.

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

### Requirement: Existing Type System Updates

The system MUST update the existing type system to include JSON support.

#### Scenario: Enhance type.go with JSON type
- **Given** the internal type system
- **When** types are initialized
- **Then** TYPE_JSON (37) is defined
- **And** TypeName() returns "JSON"
- **And** Format() handles JSON display