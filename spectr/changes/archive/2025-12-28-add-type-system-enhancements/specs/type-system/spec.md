# Type System Delta Spec

## ADDED Requirements

### Requirement: Typed List Scanning

The system SHALL provide generic list scanning to typed Go slices.

#### Scenario: Scan integer list
- GIVEN a query returning LIST column with [1, 2, 3]
- WHEN scanning with ScanList(&[]int64{})
- THEN result slice contains [1, 2, 3]
- AND slice length is 3

#### Scenario: Scan string list
- GIVEN a query returning LIST column with ['a', 'b']
- WHEN scanning with ScanList(&[]string{})
- THEN result slice contains ["a", "b"]

#### Scenario: Scan nested list
- GIVEN a query returning LIST of LIST with [[1,2], [3,4]]
- WHEN scanning with ScanList(&[][]int64{})
- THEN result contains nested slices [[1,2], [3,4]]

#### Scenario: Scan NULL list
- GIVEN a query returning NULL LIST
- WHEN scanning with ScanList(&[]int64{})
- THEN result slice is nil

#### Scenario: Scan list with NULL elements
- GIVEN a query returning LIST with [1, NULL, 3]
- WHEN scanning with ScanList(&[]int64{})
- THEN NULL element becomes zero value (0)

#### Scenario: Scan list with pointer elements
- GIVEN a query returning LIST with [1, NULL, 3]
- WHEN scanning with ScanList(&[]*int64{})
- THEN NULL element becomes nil pointer

#### Scenario: Scan empty list
- GIVEN a query returning LIST with []
- WHEN scanning with ScanList(&[]int64{})
- THEN result slice is empty (length 0)
- AND result slice is not nil

#### Scenario: Scan list with type mismatch
- GIVEN a query returning LIST with ['a', 'b']
- WHEN scanning with ScanList(&[]int64{})
- THEN error indicates type mismatch
- AND error includes "list element 0"

### Requirement: Typed Array Scanning

The system SHALL provide generic array scanning with size validation.

#### Scenario: Scan fixed-size array
- GIVEN a query returning ARRAY[3] with [1, 2, 3]
- WHEN scanning with ScanArray(&[]int64{}, 3)
- THEN result slice contains [1, 2, 3]

#### Scenario: Scan array with size mismatch
- GIVEN a query returning ARRAY[3] with [1, 2, 3]
- WHEN scanning with ScanArray(&[]int64{}, 5)
- THEN error indicates "array size mismatch: expected 5, got 3"

#### Scenario: Scan array without size validation
- GIVEN a query returning ARRAY[3] with [1, 2, 3]
- WHEN scanning with ScanArray(&[]int64{}, -1)
- THEN result slice contains [1, 2, 3]
- AND no size validation occurs

### Requirement: Typed Struct Scanning

The system SHALL provide generic struct scanning to Go structs.

#### Scenario: Scan struct to matching Go struct
- GIVEN a query returning STRUCT(name VARCHAR, age INTEGER)
- AND Go struct with Name string and Age int fields
- WHEN scanning with ScanStruct(&person)
- THEN Name field is populated
- AND Age field is populated

#### Scenario: Scan struct with duckdb tags
- GIVEN a query returning STRUCT(user_name VARCHAR)
- AND Go struct with Name string `duckdb:"user_name"`
- WHEN scanning with ScanStruct(&user)
- THEN Name field matches user_name value

#### Scenario: Scan struct with missing fields
- GIVEN a query returning STRUCT(a INT, b INT, c INT)
- AND Go struct with only A and B fields
- WHEN scanning with ScanStruct(&s)
- THEN A and B are populated
- AND c value is ignored (no error)

#### Scenario: Scan NULL struct
- GIVEN a query returning NULL STRUCT
- WHEN scanning with ScanStruct(&s)
- THEN no fields are modified

#### Scenario: Scan struct with NULL fields
- GIVEN a query returning STRUCT(name VARCHAR) with name NULL
- WHEN scanning with ScanStruct(&s)
- THEN Name field is zero value (empty string)

#### Scenario: Scan struct with pointer field NULL
- GIVEN a query returning STRUCT(name VARCHAR) with name NULL
- AND Go struct with Name *string pointer field
- WHEN scanning with ScanStruct(&s)
- THEN Name field is nil pointer

#### Scenario: Scan struct with type mismatch
- GIVEN a query returning STRUCT(age VARCHAR) with age='hello'
- AND Go struct with Age int field
- WHEN scanning with ScanStruct(&s)
- THEN error indicates type mismatch
- AND error includes "field Age"

#### Scenario: Scan struct with embedded struct
- GIVEN a query returning STRUCT(city VARCHAR, name VARCHAR)
- AND Go struct Person with embedded Address struct containing City
- WHEN scanning with ScanStruct(&person)
- THEN City field in embedded Address is populated
- AND Name field is populated

#### Scenario: Scan struct with case-insensitive matching
- GIVEN a query returning STRUCT(user_name VARCHAR)
- AND Go struct with UserName string field (no tag)
- WHEN scanning with ScanStruct(&s)
- THEN UserName field is populated via lowercase matching

### Requirement: Typed Map Scanning

The system SHALL provide generic map scanning to typed Go maps.

#### Scenario: Scan string-to-int map
- GIVEN a query returning MAP {'a': 1, 'b': 2}
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN result map has keys "a" and "b"
- AND values are 1 and 2

#### Scenario: Scan int-to-string map
- GIVEN a query returning MAP {1: 'one', 2: 'two'}
- WHEN scanning with ScanMap(&map[int64]string{})
- THEN result map has keys 1 and 2
- AND values are "one" and "two"

#### Scenario: Scan NULL map
- GIVEN a query returning NULL MAP
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN result map is nil

#### Scenario: Scan map with NULL values
- GIVEN a query returning MAP {'a': 1, 'b': NULL}
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN key "b" has zero value (0)

#### Scenario: Scan map with NULL key
- GIVEN a query returning MAP with NULL key
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN error indicates "map key cannot be NULL"

#### Scenario: Scan empty map
- GIVEN a query returning MAP with {}
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN result map is empty (length 0)
- AND result map is not nil

#### Scenario: Scan map with key type conversion
- GIVEN a query returning MAP with int32 keys
- WHEN scanning with ScanMap(&map[int64]string{})
- THEN keys are converted from int32 to int64

#### Scenario: Scan map with value type conversion
- GIVEN a query returning MAP with int32 values
- WHEN scanning with ScanMap(&map[string]int64{})
- THEN values are converted from int32 to int64

### Requirement: Union Scanning

The system SHALL provide union scanning with type-safe access.

#### Scenario: Scan union with integer active
- GIVEN a query returning UNION(i INT, s VARCHAR) with i=42 active
- WHEN scanning with ScanUnion(&u)
- THEN u.Tag is "i"
- AND u.Index is 0
- AND u.Value is 42

#### Scenario: Scan union with string active
- GIVEN a query returning UNION(i INT, s VARCHAR) with s='hello' active
- WHEN scanning with ScanUnion(&u)
- THEN u.Tag is "s"
- AND u.Index is 1
- AND u.Value is "hello"

#### Scenario: Type-safe union access
- GIVEN a scanned UnionValue with i=42 active
- WHEN calling u.As(&myInt)
- THEN myInt is 42
- AND error is nil

#### Scenario: Type-safe union access with wrong type
- GIVEN a scanned UnionValue with i=42 active
- WHEN calling u.As(&myString)
- THEN error indicates type mismatch

#### Scenario: Scan NULL union
- GIVEN a query returning NULL UNION
- WHEN scanning with ScanUnion(&u)
- THEN u.Tag is ""
- AND u.Index is -1
- AND u.Value is nil

#### Scenario: Type-safe union access with type conversion
- GIVEN a scanned UnionValue with i=int32(42) active
- WHEN calling u.As(&myInt64)
- THEN myInt64 is int64(42)
- AND conversion widened int32 to int64

### Requirement: Enum Scanning

The system SHALL provide enum scanning to custom Go string types.

#### Scenario: Scan enum to custom type
- GIVEN a query returning ENUM('active', 'inactive') with 'active'
- AND Go type Status string
- WHEN scanning with ScanEnum(&status)
- THEN status equals Status("active")

#### Scenario: Scan NULL enum
- GIVEN a query returning NULL ENUM
- WHEN scanning with ScanEnum(&status)
- THEN status is zero value (empty string)

### Requirement: JSON Scanning

The system SHALL provide JSON scanning to Go structs.

#### Scenario: Scan JSON to struct
- GIVEN a query returning JSON '{"enabled": true, "timeout": 30}'
- AND Go struct with Enabled bool and Timeout int
- WHEN scanning with ScanJSON(&config)
- THEN config.Enabled is true
- AND config.Timeout is 30

#### Scenario: Scan JSON to map
- GIVEN a query returning JSON '{"a": 1, "b": 2}'
- WHEN scanning with ScanJSON(&map[string]int{})
- THEN map has keys "a" and "b"

#### Scenario: Scan NULL JSON
- GIVEN a query returning NULL JSON
- WHEN scanning with ScanJSON(&config)
- THEN config is unchanged

#### Scenario: Scan invalid JSON
- GIVEN a query returning JSON with invalid syntax
- WHEN scanning with ScanJSON(&config)
- THEN error indicates JSON parse failure
- AND error includes "json unmarshal"

#### Scenario: Scan nested JSON
- GIVEN a query returning JSON '{"user": {"name": "Alice", "age": 30}}'
- AND Go struct with User struct field containing Name and Age
- WHEN scanning with ScanJSON(&obj)
- THEN User.Name is "Alice"
- AND User.Age is 30

### Requirement: Parameter Binding for Complex Types

The system SHALL provide wrappers for binding complex types as parameters.

#### Scenario: Bind list parameter
- GIVEN a prepared statement with LIST parameter
- WHEN binding with ListValue[int]{1, 2, 3}
- THEN statement receives [1, 2, 3]

#### Scenario: Bind struct parameter
- GIVEN a prepared statement with STRUCT parameter
- AND Go struct Person{Name: "Alice", Age: 30}
- WHEN binding with StructValue[Person]{V: person}
- THEN statement receives STRUCT(name: 'Alice', age: 30)

#### Scenario: Bind struct with duckdb tags
- GIVEN a Go struct with `duckdb:"user_name"` tag
- WHEN binding with StructValue
- THEN parameter uses "user_name" as field name

#### Scenario: Bind map parameter
- GIVEN a prepared statement with MAP parameter
- WHEN binding with MapValue[string, int]{"a": 1, "b": 2}
- THEN statement receives MAP{'a': 1, 'b': 2}

#### Scenario: Bind list parameter with NULL elements
- GIVEN a prepared statement with LIST parameter
- WHEN binding with ListValue[*int]{ptr(1), nil, ptr(3)}
- THEN statement receives [1, NULL, 3]

#### Scenario: Parameter binding round-trip
- GIVEN an insert statement binding ListValue[int]{1, 2, 3}
- WHEN inserting into LIST column
- AND selecting the value back
- AND scanning with ScanList(&[]int{})
- THEN result equals original [1, 2, 3]

### Requirement: Type Conversion

The system SHALL convert between compatible numeric types.

#### Scenario: Convert int8 to int64
- GIVEN source value int8(42)
- WHEN scanning into int64 destination
- THEN destination is 42

#### Scenario: Convert float32 to float64
- GIVEN source value float32(3.14)
- WHEN scanning into float64 destination
- THEN destination is approximately 3.14

#### Scenario: Reject incompatible types
- GIVEN source value string("hello")
- WHEN scanning into int64 destination
- THEN error indicates type mismatch
- AND error message includes source and destination types

### Requirement: Error Messages

The system SHALL provide descriptive error messages.

#### Scenario: List element conversion error
- GIVEN list conversion failing at element 3
- WHEN error is returned
- THEN message includes "list element 3"

#### Scenario: Struct field conversion error
- GIVEN struct field "Name" conversion failing
- WHEN error is returned
- THEN message includes "field Name"

#### Scenario: Map key conversion error
- GIVEN map key conversion failing
- WHEN error is returned
- THEN message includes "map key"

#### Scenario: Map value conversion error
- GIVEN map value conversion failing for key "foo"
- WHEN error is returned
- THEN message includes "map value"

### Requirement: Clock Integration for Timestamps

The system SHALL support clock injection for timestamp helpers.

#### Scenario: NowNS with mock clock
- GIVEN a mock clock set to 2024-01-01 12:00:00
- WHEN calling NowNS(mockClock)
- THEN result represents 12:00:00 as nanoseconds since midnight

#### Scenario: NowNS with nil clock
- GIVEN nil clock parameter
- WHEN calling NowNS(nil)
- THEN uses real clock (quartz.NewReal())
- AND returns current time

#### Scenario: CurrentTimeNS with mock clock
- GIVEN a mock clock set to specific timestamp
- WHEN calling CurrentTimeNS(mockClock)
- THEN returns mock timestamp in nanoseconds

#### Scenario: Deterministic time testing
- GIVEN test with mClock := quartz.NewMock(t)
- WHEN executing time-dependent operations
- THEN results are deterministic and reproducible

### Requirement: Nested Type Scanning

The system SHALL support scanning nested complex types.

#### Scenario: Scan LIST of STRUCT
- GIVEN a query returning LIST of STRUCT(name VARCHAR, age INT)
- AND Go type []Person where Person has Name and Age fields
- WHEN scanning with ScanList(&[]Person{})
- THEN result contains Person structs with populated fields

#### Scenario: Scan STRUCT with LIST field
- GIVEN a query returning STRUCT(name VARCHAR, scores LIST)
- AND Go struct with Name string and Scores []int fields
- WHEN scanning with ScanStruct(&s)
- THEN Name is populated
- AND Scores contains the list values

#### Scenario: Scan MAP with STRUCT value
- GIVEN a query returning MAP{'alice': STRUCT(age INT)}
- AND Go type map[string]Person where Person has Age field
- WHEN scanning with ScanMap(&map[string]Person{})
- THEN map contains Person structs with populated fields

#### Scenario: Scan deeply nested structure
- GIVEN a query returning LIST of LIST of INT
- WHEN scanning with ScanList(&[][]int64{})
- THEN result contains nested integer slices

#### Scenario: Nested error path
- GIVEN a query with LIST of STRUCT where inner field fails
- WHEN scanning with type mismatch in inner struct field
- THEN error includes "list element 0: field Name: cannot convert"

### Requirement: UUID Scanning

The system SHALL support UUID type scanning.

#### Scenario: Scan UUID to byte array
- GIVEN a query returning UUID value
- WHEN scanning with ScanUUID(&[16]byte{})
- THEN result contains 16 bytes of UUID

#### Scenario: Scan UUID from string
- GIVEN a query returning UUID as string format
- WHEN scanning with ScanUUID(&[16]byte{})
- THEN UUID string is parsed to bytes

#### Scenario: Scan NULL UUID
- GIVEN a query returning NULL UUID
- WHEN scanning with ScanUUID(&[16]byte{})
- THEN result is zero bytes [16]byte{}

#### Scenario: Scan invalid UUID string
- GIVEN a query returning invalid UUID format
- WHEN scanning with ScanUUID(&[16]byte{})
- THEN error indicates "invalid UUID string"
