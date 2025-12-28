## ADDED Requirements

### Requirement: UUID Type

The package SHALL export a UUID type as a 16-byte array implementing sql.Scanner, driver.Valuer, and fmt.Stringer interfaces.

#### Scenario: UUID from hyphenated string
- GIVEN a UUID type variable
- WHEN scanning the string "550e8400-e29b-41d4-a716-446655440000"
- THEN the UUID contains bytes [0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00]

#### Scenario: UUID to string
- GIVEN a UUID with known bytes
- WHEN calling String()
- THEN the result is lowercase hyphenated format "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

#### Scenario: UUID as driver value
- GIVEN a UUID type
- WHEN calling Value()
- THEN the result is (string, nil) with the hyphenated format

#### Scenario: UUID from 16-byte slice
- GIVEN a UUID variable and 16-byte []byte
- WHEN calling Scan with the []byte
- THEN the UUID contains those exact bytes

#### Scenario: UUID scan type error
- GIVEN a UUID variable and an int value
- WHEN calling Scan
- THEN error message is "cannot scan int into UUID"

#### Scenario: UUID scan nil
- GIVEN a UUID variable
- WHEN calling Scan(nil)
- THEN the UUID is zero value (all bytes 0)

### Requirement: Interval Type

The package SHALL export an Interval struct with Days (int32), Months (int32), and Micros (int64) fields with JSON tags.

#### Scenario: Interval JSON unmarshaling
- GIVEN JSON `{"days": 5, "months": 2, "micros": 1000000}`
- WHEN unmarshaling into Interval
- THEN Interval.Days equals 5, Interval.Months equals 2, Interval.Micros equals 1000000

#### Scenario: Interval zero value
- GIVEN a zero-value Interval
- THEN Days, Months, and Micros are all 0

#### Scenario: Interval as driver value
- GIVEN Interval{Months: 1, Days: 2, Micros: 3000000}
- WHEN calling Value()
- THEN the result is "INTERVAL '1 months 2 days 3000000 microseconds'"

#### Scenario: Interval scan from map
- GIVEN a map[string]any{"days": float64(5), "months": float64(2), "micros": float64(1000000)}
- WHEN calling Scan on Interval
- THEN Interval.Days equals 5, Interval.Months equals 2, Interval.Micros equals 1000000

### Requirement: Decimal Type

The package SHALL export a Decimal struct with Width (uint8), Scale (uint8), and Value (*big.Int) fields implementing Float64() and String() methods.

#### Scenario: Decimal to float64
- GIVEN Decimal{Width: 10, Scale: 2, Value: big.NewInt(12345)}
- WHEN calling Float64()
- THEN the result is 123.45

#### Scenario: Decimal to string with trailing zeros removed
- GIVEN Decimal{Width: 10, Scale: 4, Value: big.NewInt(123400)}
- WHEN calling String()
- THEN the result is "12.34" (not "12.3400")

#### Scenario: Decimal zero value
- GIVEN Decimal with Value = big.NewInt(0)
- WHEN calling String()
- THEN the result is "0"

#### Scenario: Decimal scan from string
- GIVEN string "123.45"
- WHEN calling Scan on Decimal
- THEN Width is inferred, Scale is 2, Value equals big.NewInt(12345)

#### Scenario: Decimal scan from float64
- GIVEN float64 value 123.45
- WHEN calling Scan on Decimal
- THEN Scale is 2, Value equals big.NewInt(12345)

#### Scenario: Decimal as driver value
- GIVEN Decimal{Scale: 2, Value: big.NewInt(12345)}
- WHEN calling Value()
- THEN the result is ("123.45", nil)

#### Scenario: Decimal scan nil
- GIVEN Decimal variable
- WHEN calling Scan(nil)
- THEN Decimal.Value is nil

### Requirement: Map Type

The package SHALL export a Map type as map[any]any implementing sql.Scanner and driver.Valuer interfaces.

#### Scenario: Map scanning from Map
- GIVEN a Map variable and a source Map value
- WHEN calling Scan with the source
- THEN the target Map contains the same key-value pairs

#### Scenario: Map scanning from key-value array
- GIVEN JSON array `[{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]`
- WHEN parsing as Map
- THEN Map contains {1: "a", 2: "b"}

#### Scenario: Map with invalid scan source
- GIVEN a Map variable and an int source value
- WHEN calling Scan
- THEN an error is returned with message "cannot scan int into Map"

#### Scenario: Map as driver value
- GIVEN Map{1: "a", 2: "b"}
- WHEN calling Value()
- THEN the result is JSON bytes of the map

### Requirement: Union Type

The package SHALL export a Union struct with Value (driver.Value) and Tag (string) fields with JSON tags.

#### Scenario: Union JSON unmarshaling
- GIVEN JSON `{"tag": "int", "value": 42}`
- WHEN unmarshaling into Union
- THEN Union.Tag equals "int" and Union.Value equals 42

#### Scenario: Union with string variant
- GIVEN JSON `{"tag": "varchar", "value": "hello"}`
- WHEN unmarshaling into Union
- THEN Union.Tag equals "varchar" and Union.Value equals "hello"

#### Scenario: Union with null value
- GIVEN JSON `{"tag": "int", "value": null}`
- WHEN unmarshaling into Union
- THEN Union.Tag equals "int" and Union.Value is nil

#### Scenario: Union as driver value
- GIVEN Union{Tag: "int", Value: 42}
- WHEN calling Value()
- THEN the result is JSON bytes `{"tag":"int","value":42}`

### Requirement: Composite Generic Type

The package SHALL export a Composite[T] generic type implementing sql.Scanner with Get() method returning T.

#### Scenario: Composite struct scanning
- GIVEN Composite[struct{Name string; Age int}]
- WHEN scanning map[string]any{"Name": "Alice", "Age": 30}
- THEN Get() returns struct with Name="Alice" and Age=30

#### Scenario: Composite list scanning
- GIVEN Composite[[]int]
- WHEN scanning []any{1, 2, 3}
- THEN Get() returns []int{1, 2, 3}

#### Scenario: Composite type mismatch
- GIVEN Composite[struct{Name string}]
- WHEN scanning int value 42
- THEN error message contains "cannot decode" or "type mismatch"

#### Scenario: Composite nested struct
- GIVEN Composite[struct{User struct{Name string}}]
- WHEN scanning map[string]any{"User": map[string]any{"Name": "Alice"}}
- THEN Get() returns correctly nested struct

### Requirement: Type Enumeration

The package SHALL define a Type enumeration with constants for all 54 DuckDB logical types with exact values matching DuckDB.

#### Scenario: Type constant values
- GIVEN the Type enumeration
- THEN TYPE_INVALID equals 0
- AND TYPE_BOOLEAN equals 1
- AND TYPE_TINYINT equals 2
- AND TYPE_VARCHAR equals 17
- AND TYPE_UUID equals 27
- AND TYPE_TIMESTAMP_TZ equals 31

#### Scenario: Type string representation
- GIVEN TYPE_VARCHAR
- WHEN converting to string via String()
- THEN the result is "VARCHAR"

#### Scenario: Type string with underscore
- GIVEN TYPE_TIMESTAMP_TZ
- WHEN converting to string via String()
- THEN the result is "TIMESTAMP_TZ"

#### Scenario: Type category primitive
- GIVEN TYPE_INTEGER
- WHEN calling Category()
- THEN the result is "primitive"

#### Scenario: Type category temporal
- GIVEN TYPE_TIMESTAMP
- WHEN calling Category()
- THEN the result is "temporal"

#### Scenario: Type category nested
- GIVEN TYPE_LIST
- WHEN calling Category()
- THEN the result is "nested"

### Requirement: HugeInt Conversion

The package SHALL provide functions to convert between *big.Int and DuckDB's 128-bit hugeint representation.

#### Scenario: Positive hugeint to big.Int
- GIVEN a hugeint with lower=1000, upper=0
- WHEN converting to *big.Int
- THEN the result equals big.NewInt(1000)

#### Scenario: Negative hugeint to big.Int
- GIVEN a hugeint representing -1 (lower=maxUint64, upper=-1)
- WHEN converting to *big.Int
- THEN the result equals big.NewInt(-1)

#### Scenario: Large big.Int to hugeint
- GIVEN a *big.Int larger than 2^64
- WHEN converting to hugeint
- THEN the upper component is non-zero

#### Scenario: Maximum positive hugeint
- GIVEN *big.Int equal to 2^127-1
- WHEN converting to hugeint
- THEN conversion succeeds without error

#### Scenario: Minimum negative hugeint
- GIVEN *big.Int equal to -2^127
- WHEN converting to hugeint
- THEN conversion succeeds without error

#### Scenario: Overflow detection positive
- GIVEN a *big.Int equal to 2^127
- WHEN converting to hugeint
- THEN error message is "value 170141183460469231731687303715884105728 overflows HUGEINT range"

#### Scenario: Overflow detection negative
- GIVEN a *big.Int equal to -2^127-1
- WHEN converting to hugeint
- THEN error message contains "overflows HUGEINT range"

### Requirement: Temporal Type Conversion

The package SHALL provide functions to convert Go time.Time to DuckDB temporal types with correct precision.

#### Scenario: Timestamp microsecond precision
- GIVEN time.Time representing 2024-01-15 10:30:45.123456
- WHEN converting to TIMESTAMP (microseconds since epoch)
- THEN the result preserves microsecond precision

#### Scenario: Timestamp_S second precision
- GIVEN time.Time with sub-second component .999999
- WHEN converting to TIMESTAMP_S
- THEN the result truncates to seconds (not rounds)

#### Scenario: Timestamp_MS millisecond precision
- GIVEN time.Time with microsecond component .123456
- WHEN converting to TIMESTAMP_MS
- THEN the result is .123 milliseconds (truncated)

#### Scenario: Timestamp_NS nanosecond precision
- GIVEN time.Time with nanosecond component
- WHEN converting to TIMESTAMP_NS
- THEN the result preserves nanosecond precision

#### Scenario: Timestamp_TZ with timezone
- GIVEN time.Time in UTC+5 timezone
- WHEN converting to TIMESTAMP_TZ
- THEN the offset is +300 minutes

#### Scenario: Date conversion
- GIVEN time.Time representing 2024-01-15 10:30:45
- WHEN converting to DATE (days since epoch)
- THEN the result is 19737 (ignores time component)

#### Scenario: Time conversion
- GIVEN time.Time representing any date with time 10:30:45.123456
- WHEN converting to TIME (microseconds since midnight)
- THEN the result is 37845123456

### Requirement: JSON Type Parsing

The package SHALL correctly parse all DuckDB types from JSON format as output by DuckDB CLI.

#### Scenario: NULL value parsing
- GIVEN JSON null
- WHEN parsing as any type
- THEN the Go value is nil and error is nil

#### Scenario: Boolean parsing
- GIVEN JSON true or false
- WHEN parsing as BOOLEAN
- THEN the Go value is bool true or false

#### Scenario: Integer parsing
- GIVEN JSON number 42
- WHEN parsing as INTEGER
- THEN the Go value is int32(42)

#### Scenario: Special float Infinity
- GIVEN JSON string "Infinity"
- WHEN parsing as DOUBLE
- THEN the Go value is math.Inf(1)

#### Scenario: Special float negative Infinity
- GIVEN JSON string "-Infinity"
- WHEN parsing as DOUBLE
- THEN the Go value is math.Inf(-1)

#### Scenario: Special float NaN
- GIVEN JSON string "NaN"
- WHEN parsing as DOUBLE
- THEN the Go value satisfies math.IsNaN()

#### Scenario: BLOB hex parsing
- GIVEN JSON string "\x48454C4C4F"
- WHEN parsing as BLOB
- THEN the Go value is []byte("HELLO")

#### Scenario: Nested LIST parsing
- GIVEN JSON [[1, 2], [3, 4]]
- WHEN parsing as LIST of LIST of INTEGER
- THEN the Go value is []any{[]any{int32(1), int32(2)}, []any{int32(3), int32(4)}}

#### Scenario: STRUCT parsing
- GIVEN JSON {"name": "Alice", "age": 30}
- WHEN parsing as STRUCT
- THEN the Go value is map[string]any{"name": "Alice", "age": int64(30)}

#### Scenario: MAP parsing
- GIVEN JSON [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]
- WHEN parsing as MAP
- THEN the Go value is Map{int64(1): "a", int64(2): "b"}

#### Scenario: Empty LIST parsing
- GIVEN JSON []
- WHEN parsing as LIST
- THEN the Go value is []any{} (empty slice, not nil)

#### Scenario: Empty STRUCT parsing
- GIVEN JSON {}
- WHEN parsing as STRUCT
- THEN the Go value is map[string]any{} (empty map, not nil)

#### Scenario: DATE parsing
- GIVEN JSON string "2024-01-15"
- WHEN parsing as DATE
- THEN the Go value is time.Time representing 2024-01-15 00:00:00 UTC

#### Scenario: TIMESTAMP parsing
- GIVEN JSON string "2024-01-15 10:30:45.123456"
- WHEN parsing as TIMESTAMP
- THEN the Go value is time.Time with microsecond precision
