## ADDED Requirements

### Requirement: JSON_CONTAINS scalar function

The system SHALL provide a `JSON_CONTAINS(json, value)` scalar function that checks whether a JSON document contains a specified value. The function MUST accept two arguments: a JSON string and a candidate value. It SHALL return `true` if the candidate value is found anywhere within the JSON document (including nested objects and arrays), and `false` otherwise. If either argument is NULL, the function SHALL return NULL.

#### Scenario: Object contains a matching key-value pair

- WHEN `JSON_CONTAINS('{"a":1,"b":2}', '1')` is evaluated
- THEN the result SHALL be `true`

#### Scenario: Array contains a matching element

- WHEN `JSON_CONTAINS('[1, 2, 3]', '2')` is evaluated
- THEN the result SHALL be `true`

#### Scenario: Value not found in document

- WHEN `JSON_CONTAINS('{"a":1}', '5')` is evaluated
- THEN the result SHALL be `false`

#### Scenario: NULL input returns NULL

- WHEN `JSON_CONTAINS(NULL, '1')` is evaluated
- THEN the result SHALL be NULL

#### Scenario: Nested containment

- WHEN `JSON_CONTAINS('{"a":{"b":1}}', '{"b":1}')` is evaluated
- THEN the result SHALL be `true`

### Requirement: JSON_QUOTE scalar function

The system SHALL provide a `JSON_QUOTE(value)` scalar function that converts a value into its JSON string representation. Strings SHALL be wrapped in double quotes with proper escaping. Numbers and booleans SHALL be represented as their JSON equivalents. NULL input SHALL return the JSON literal string `"null"`.

#### Scenario: Quote a string value

- WHEN `JSON_QUOTE('hello')` is evaluated
- THEN the result SHALL be `"hello"` (with surrounding double quotes in the output)

#### Scenario: Quote a numeric value

- WHEN `JSON_QUOTE(42)` is evaluated
- THEN the result SHALL be `42`

#### Scenario: Quote a NULL value

- WHEN `JSON_QUOTE(NULL)` is evaluated
- THEN the result SHALL be `null`

#### Scenario: Quote a string with special characters

- WHEN `JSON_QUOTE('he said "hi"')` is evaluated
- THEN the result SHALL be `"he said \"hi\""` with properly escaped quotes

### Requirement: JSON_GROUP_ARRAY aggregate function

The system SHALL provide a `JSON_GROUP_ARRAY(expr)` aggregate function that collects all values of `expr` across the group into a JSON array string. NULL values within the group SHALL be included as JSON `null` entries. If the group is empty, the function SHALL return `[]`. The return type SHALL be VARCHAR containing valid JSON.

#### Scenario: Aggregate integers into JSON array

- WHEN a table contains rows with values 1, 2, 3 and `JSON_GROUP_ARRAY(value)` is evaluated
- THEN the result SHALL be `[1,2,3]`

#### Scenario: Aggregate with NULL values

- WHEN a table contains rows with values 1, NULL, 3 and `JSON_GROUP_ARRAY(value)` is evaluated
- THEN the result SHALL be `[1,null,3]`

#### Scenario: Aggregate over empty group

- WHEN `JSON_GROUP_ARRAY(value)` is evaluated over an empty group
- THEN the result SHALL be `[]`

#### Scenario: Aggregate with GROUP BY

- WHEN a table has rows (group=a, val=1), (group=a, val=2), (group=b, val=3) and `JSON_GROUP_ARRAY(val)` is evaluated with `GROUP BY group`
- THEN group `a` SHALL produce `[1,2]` and group `b` SHALL produce `[3]`

### Requirement: JSON_GROUP_OBJECT aggregate function

The system SHALL provide a `JSON_GROUP_OBJECT(key, value)` aggregate function that collects key-value pairs across the group into a JSON object string. The function MUST accept exactly two arguments. Keys MUST be coerced to strings. NULL keys SHALL be skipped. The return type SHALL be VARCHAR containing valid JSON. If the group is empty, the function SHALL return `{}`.

#### Scenario: Aggregate key-value pairs into JSON object

- WHEN a table contains rows (key='a', val=1), (key='b', val=2) and `JSON_GROUP_OBJECT(key, val)` is evaluated
- THEN the result SHALL be `{"a":1,"b":2}`

#### Scenario: Aggregate with NULL key skipped

- WHEN a table contains rows (key='a', val=1), (key=NULL, val=2) and `JSON_GROUP_OBJECT(key, val)` is evaluated
- THEN the result SHALL be `{"a":1}`

#### Scenario: Aggregate over empty group

- WHEN `JSON_GROUP_OBJECT(key, val)` is evaluated over an empty group
- THEN the result SHALL be `{}`

#### Scenario: Duplicate keys use last value

- WHEN a table contains rows (key='a', val=1), (key='a', val=2) and `JSON_GROUP_OBJECT(key, val)` is evaluated
- THEN the result SHALL contain key `a` with value `2` (last-write-wins)

### Requirement: JSON_EACH table function

The system SHALL provide a `JSON_EACH(json)` table function that expands a JSON value into a set of rows. For JSON objects, the function SHALL produce one row per key-value pair with columns `key` (VARCHAR) and `value` (VARCHAR, JSON-encoded). For JSON arrays, the function SHALL produce one row per element with `key` as the zero-based integer index cast to VARCHAR and `value` as the JSON-encoded element. If the input is NULL or not a valid JSON object or array, the function SHALL return zero rows.

#### Scenario: Expand a JSON object

- WHEN `SELECT * FROM JSON_EACH('{"a":1,"b":"hello"}')` is evaluated
- THEN the result SHALL contain rows (key='a', value='1') and (key='b', value='"hello"')

#### Scenario: Expand a JSON array

- WHEN `SELECT * FROM JSON_EACH('[10, 20, 30]')` is evaluated
- THEN the result SHALL contain rows (key='0', value='10'), (key='1', value='20'), (key='2', value='30')

#### Scenario: NULL input returns empty result

- WHEN `SELECT * FROM JSON_EACH(NULL)` is evaluated
- THEN the result SHALL contain zero rows

#### Scenario: Nested values are JSON-encoded

- WHEN `SELECT * FROM JSON_EACH('{"x":{"nested":true}}')` is evaluated
- THEN the result SHALL contain row (key='x', value='{"nested":true}')
