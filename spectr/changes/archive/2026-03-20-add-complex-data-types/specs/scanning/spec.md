# Scanning and Binding Specification

## ADDED Requirements

### Requirement: JSON Scanning

The system SHALL provide JSON scanning to typed Go structs and maps.

#### Scenario: Scan JSON to struct
- GIVEN query returning JSON column with '{"enabled": true, "timeout": 30}'
- AND Go struct Config with Enabled bool and Timeout int
- WHEN calling row.ScanJSON(&config, "settings")
- THEN config.Enabled is true
- AND config.Timeout is 30

#### Scenario: Scan JSON to map
- GIVEN query returning JSON '{"a": 1, "b": 2}'
- WHEN calling row.ScanJSON(&map[string]int{}, "data")
- THEN map contains keys "a" and "b" with values 1 and 2

#### Scenario: Scan NULL JSON
- GIVEN query returning NULL JSON
- WHEN calling row.ScanJSON(&config, "settings")
- THEN config unchanged
- AND no error (NULL is valid)

#### Scenario: JSON invalid syntax
- GIVEN query returning JSON with invalid syntax '{broken'
- WHEN calling row.ScanJSON(&config, "json_col")
- THEN error indicates JSON parse failure
- AND error message includes "json unmarshal"

#### Scenario: JSON type mismatch
- GIVEN JSON '{"name": "Alice"}'
- AND Go struct with Name string field expects integer
- WHEN scanning
- THEN error indicates type mismatch
- AND error includes "field Name"

#### Scenario: Nested JSON objects
- GIVEN JSON '{"user": {"name": "Alice", "age": 30}}'
- AND Go struct User with nested Address struct containing Name and Age
- WHEN scanning
- THEN User.Name is "Alice"
- AND User.Age is 30

#### Scenario: JSON array in struct field
- GIVEN JSON '{"items": [1, 2, 3]}'
- AND Go struct with Items []int field
- WHEN scanning
- THEN Items contains [1, 2, 3]

#### Scenario: Cached JSON parsing
- GIVEN JSONVector with 1000 rows
- WHEN scanning same column multiple times
- THEN JSON parsed once per row (cached)
- AND subsequent scans use cache (no re-parsing)

### Requirement: MAP Scanning

The system SHALL provide MAP scanning to typed Go maps.

#### Scenario: Scan string-to-int map
- GIVEN query returning MAP {'a': 1, 'b': 2}
- WHEN scanning with row.ScanMap(&map[string]int64{}, "tags")
- THEN result map has keys "a" and "b"
- AND values are 1 and 2

#### Scenario: Scan int-to-string map
- GIVEN query returning MAP {1: 'one', 2: 'two'}
- WHEN scanning with row.ScanMap(&map[int64]string{}, "values")
- THEN result map has keys 1 and 2
- AND values are "one" and "two"

#### Scenario: Scan NULL map
- GIVEN query returning NULL MAP
- WHEN scanning with row.ScanMap(&map[string]int64{}, "data")
- THEN result map is nil
- AND no error

#### Scenario: Scan map with NULL values
- GIVEN query returning MAP {'a': 1, 'b': NULL}
- WHEN scanning with row.ScanMap(&map[string]int64{}, "data")
- THEN key "b" has zero value (0)
- AND no panic

#### Scenario: Scan map with NULL key
- GIVEN query returning MAP with NULL key
- WHEN scanning with row.ScanMap(&map[string]int64{}, "data")
- THEN error indicates "map key cannot be NULL"
- AND map not populated

#### Scenario: Scan empty map
- GIVEN query returning empty MAP {}
- WHEN scanning with row.ScanMap(&map[string]int64{}, "data")
- THEN result map is empty (length 0)
- AND result map is not nil

#### Scenario: Scan map with type conversion
- GIVEN query returning MAP with int32 values
- WHEN scanning with map[string]int64
- THEN values converted from int32 to int64
- AND no precision loss

#### Scenario: Scan large map
- GIVEN query returning MAP with 100,000 entries
- WHEN scanning
- THEN all entries populated
- AND memory reasonable

### Requirement: STRUCT Scanning

The system SHALL provide STRUCT scanning to Go structs.

#### Scenario: Scan simple struct
- GIVEN query returning STRUCT(name VARCHAR, age INTEGER)
- AND Go struct Person with Name string and Age int fields
- WHEN scanning with row.ScanStruct(&person, "person")
- THEN Name field is "Alice"
- AND Age field is 30

#### Scenario: Scan struct with duckdb tags
- GIVEN query returning STRUCT(user_name VARCHAR, user_age INT)
- AND Go struct with Name string `duckdb:"user_name"` and Age int `duckdb:"user_age"`
- WHEN scanning with row.ScanStruct(&user, "person")
- THEN Name field matches user_name value
- AND Age field matches user_age value

#### Scenario: Scan struct with missing fields
- GIVEN query returning STRUCT(a INT, b INT, c INT)
- AND Go struct with only A and B fields
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN A and B are populated
- AND c value is ignored (no error)

#### Scenario: Scan NULL struct
- GIVEN query returning NULL STRUCT
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN struct fields contain zero values
- AND no panic or undefined behavior

#### Scenario: Scan struct with NULL fields
- GIVEN query returning STRUCT(name VARCHAR) with name NULL
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN Name field is empty string ""
- AND no special NULL indicator

#### Scenario: Scan struct with pointer field NULL
- GIVEN query returning STRUCT(name VARCHAR) with name NULL
- AND Go struct with Name *string pointer field
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN Name field is nil pointer
- AND allows distinguishing NULL from empty string

#### Scenario: Scan struct with type mismatch
- GIVEN query returning STRUCT(age VARCHAR) with age='hello'
- AND Go struct with Age int field
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN error indicates type mismatch
- AND error includes "field Age"
- AND error shows both types

#### Scenario: Scan nested struct
- GIVEN query returning STRUCT(city VARCHAR, person STRUCT(name VARCHAR, age INT))
- AND nested Go struct
- WHEN scanning with row.ScanStruct(&result, "data")
- THEN all nested fields populated correctly

#### Scenario: Scan struct with case-insensitive matching
- GIVEN query returning STRUCT(user_name VARCHAR)
- AND Go struct with UserName string field (no tag)
- WHEN scanning with row.ScanStruct(&s, "data")
- THEN UserName field is populated
- AND matching is case-insensitive

#### Scenario: Scan struct with embedded struct
- GIVEN query returning STRUCT(city VARCHAR, name VARCHAR)
- AND Go struct Person with embedded Address struct containing City
- WHEN scanning with row.ScanStruct(&person, "data")
- THEN City field in embedded Address is populated
- AND Name field is populated

### Requirement: UNION Scanning

The system SHALL provide UNION scanning with type-safe access.

#### Scenario: Scan union with integer active
- GIVEN query returning UNION(i INT, s VARCHAR) with i=42 active
- WHEN scanning with row.ScanUnion(&u, "result")
- THEN u.Tag is "i"
- AND u.Index is 0 (member index)
- AND u.Value is 42

#### Scenario: Scan union with string active
- GIVEN query returning UNION(i INT, s VARCHAR) with s='hello' active
- WHEN scanning with row.ScanUnion(&u, "result")
- THEN u.Tag is "s"
- AND u.Index is 1
- AND u.Value is "hello"

#### Scenario: Type-safe union access
- GIVEN scanned UnionValue with i=42 active
- WHEN calling u.As(&myInt)
- THEN myInt is 42
- AND error is nil

#### Scenario: Type-safe union access with wrong type
- GIVEN scanned UnionValue with i=42 active
- WHEN calling u.As(&myString)
- THEN error indicates type mismatch
- AND error message includes expected and actual types

#### Scenario: Scan NULL union
- GIVEN query returning NULL UNION
- WHEN scanning with row.ScanUnion(&u, "result")
- THEN u.Tag is ""
- AND u.Index is -1
- AND u.Value is nil

#### Scenario: Type-safe union access with type conversion
- GIVEN scanned UnionValue with i=int32(42) active
- WHEN calling u.As(&myInt64)
- THEN myInt64 is int64(42)
- AND conversion successful

#### Scenario: Enumerate all union members
- GIVEN UnionValue
- WHEN iterating members
- THEN all member tags and values accessible
- AND can identify active member

### Requirement: Complex Type Parameter Binding

The system SHALL provide wrappers for binding complex types as query parameters.

#### Scenario: Bind JSON parameter
- GIVEN prepared statement with JSON parameter
- WHEN binding with JSONValue(`{"enabled": true}`)
- THEN statement receives JSON value
- AND can be stored or processed

#### Scenario: Bind MAP parameter
- GIVEN prepared statement with MAP parameter
- WHEN binding with MapValue[string, int]{"a": 1, "b": 2}
- THEN statement receives MAP{'a': 1, 'b': 2}

#### Scenario: Bind STRUCT parameter
- GIVEN prepared statement with STRUCT parameter
- AND Go struct Person{Name: "Alice", Age: 30}
- WHEN binding with StructValue[Person]{V: person}
- THEN statement receives STRUCT(name: 'Alice', age: 30)

#### Scenario: Bind STRUCT with duckdb tags
- GIVEN Go struct with `duckdb:"user_name"` tag
- WHEN binding with StructValue
- THEN parameter uses "user_name" as field name

#### Scenario: Bind parameter with NULL elements
- GIVEN MAP parameter with NULL values
- WHEN binding
- THEN NULLs preserved in transmission
- AND query receives correct NULL structure

#### Scenario: Parameter binding round-trip
- GIVEN insert statement binding StructValue[Person]
- WHEN inserting into STRUCT column
- AND selecting the value back
- THEN result equals original struct
- AND NULL preservation verified

### Requirement: Complex Type Error Messages

The system SHALL provide descriptive error messages for scanning failures.

#### Scenario: JSON parse error
- GIVEN invalid JSON during scan
- WHEN error returned
- THEN message includes "json unmarshal"
- AND includes JSON content (truncated if large)

#### Scenario: Struct field mismatch
- GIVEN type mismatch in struct field
- WHEN error returned
- THEN message includes "field Name"
- AND shows expected vs actual types

#### Scenario: MAP key NULL error
- GIVEN NULL key in map
- WHEN error returned
- THEN message clearly states "map key cannot be NULL"

#### Scenario: Nested error paths
- GIVEN error deep in nested structure
- WHEN error returned
- THEN message includes full path
- AND message example: "list element 0: field Name: cannot convert VARCHAR to int"

#### Scenario: Type mismatch in nested context
- GIVEN nested struct with field type error
- WHEN error returned
- THEN error chain includes all levels
- AND helps locate exact failing value

### Requirement: Lazy Evaluation for Complex Types

The system SHALL defer expensive operations until necessary.

#### Scenario: JSON parsing deferred until first access
- GIVEN query returning 1000 JSON rows
- AND only selecting 10 of them
- WHEN scanning only selected rows
- THEN only 10 JSON strings parsed
- AND 990 remain as strings

#### Scenario: JSON cached after first parse
- GIVEN JSON column accessed multiple times
- WHEN scanning same row again
- THEN cached parsed value returned
- AND no re-parsing occurs

#### Scenario: Struct field access doesn't parse unused fields
- GIVEN STRUCT with 10 fields
- AND accessing only 2 fields
- WHEN scanning
- THEN only 2 fields converted to Go types
- AND 8 fields skipped

#### Scenario: MAP enumeration iterates actual entries
- GIVEN MAP with 1000 entries
- WHEN iterating
- THEN all entries enumerated
- AND efficient memory usage

### Requirement: Scanning API Consistency

The system SHALL provide consistent API across complex type scanners.

#### Scenario: All scanners follow Row interface
- GIVEN Row from database/sql
- WHEN calling ScanJSON, ScanMap, ScanStruct, ScanUnion
- THEN all follow standard Scan() semantics
- AND work with standard database/sql patterns

#### Scenario: Error propagation consistency
- GIVEN scanning failures
- WHEN returning errors
- THEN all errors wrapped with context
- AND all implement standard error interface

#### Scenario: NULL handling consistency
- GIVEN complex type columns
- WHEN values are NULL
- THEN all scanners handle NULL uniformly
- AND (generally) don't panic, return zero values or nil
