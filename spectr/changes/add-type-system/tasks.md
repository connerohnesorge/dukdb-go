## 1. Type Constants (type_enum.go)

- [ ] 1.1 Create `type_enum.go` with Type enumeration
  - Define Type as uint8
  - Define all 54 type constants with exact DuckDB values (TYPE_INVALID=0 through TYPE_SQLNULL=36)
  - **Acceptance:** All constants match DuckDB duckdb_type enum values

- [ ] 1.2 Implement Type.String() method
  - Return uppercase type name: TYPE_VARCHAR → "VARCHAR", TYPE_TIMESTAMP_TZ → "TIMESTAMP_TZ"
  - Use array lookup for O(1) performance
  - **Acceptance:** All 54 types return correct string names

- [ ] 1.3 Implement Type.Category() method
  - Return "primitive" for BOOLEAN through BLOB
  - Return "temporal" for DATE, TIME, TIMESTAMP variants, INTERVAL
  - Return "nested" for LIST, STRUCT, MAP, ARRAY, UNION
  - Return "special" for INVALID, ANY, SQLNULL
  - **Acceptance:** All types correctly categorized

## 2. Exported Types (types.go)

- [ ] 2.1 Implement UUID type
  - Define as `type UUID [16]byte`
  - Implement Scan(src any) error - accepts string (hyphenated), []byte (16 bytes or string), nil (zero value)
  - Implement Value() (driver.Value, error) - returns hyphenated lowercase string
  - Implement String() string - returns hyphenated lowercase format
  - **Acceptance:** UUID round-trips through Scan/Value/String

- [ ] 2.2 Implement Interval struct
  - Fields: Days int32, Months int32, Micros int64 with JSON tags
  - Implement Scan(src any) error - accepts map[string]any or JSON
  - Implement Value() (driver.Value, error) - returns SQL INTERVAL literal
  - **Acceptance:** Interval parses from DuckDB JSON format

- [ ] 2.3 Implement Decimal struct
  - Fields: Width uint8, Scale uint8, Value *big.Int
  - Implement Scan(src any) error - parses string "123.45" or float64
  - Implement Value() (driver.Value, error) - returns string representation
  - Implement Float64() float64 - returns approximate float value
  - Implement String() string - returns decimal string without trailing zeros
  - **Acceptance:** Decimal preserves precision through round-trip

- [ ] 2.4 Implement Map type
  - Define as `type Map map[any]any`
  - Implement Scan(src any) error - accepts Map, []any (key-value pairs), map[string]any
  - Implement Value() (driver.Value, error) - returns JSON []byte
  - **Acceptance:** Map round-trips through JSON format

- [ ] 2.5 Implement Union struct
  - Fields: Tag string, Value driver.Value with JSON tags
  - Implement Scan(src any) error - parses {tag: string, value: any}
  - Implement Value() (driver.Value, error) - returns JSON
  - **Acceptance:** Union correctly identifies tag and value

- [ ] 2.6 Implement Composite[T] generic type
  - Private field: t T
  - Implement Scan(src any) error - uses mapstructure.Decode for struct, type assertion for primitives
  - Implement Get() T - returns scanned value
  - Error handling: Return typed error when T doesn't match src structure
  - **Acceptance:** Composite[struct] and Composite[[]T] both work

## 3. HugeInt Conversion Functions

- [ ] 3.1 Implement internal hugeInt struct
  - Fields: lower uint64, upper int64
  - **Acceptance:** Matches DuckDB hugeint_t layout

- [ ] 3.2 Implement hugeIntToBigInt(h hugeInt) *big.Int
  - Handle positive numbers (upper >= 0)
  - Handle negative numbers (upper < 0, two's complement)
  - **Acceptance:** Converts -2^127, -1, 0, 1, 2^127-1 correctly

- [ ] 3.3 Implement bigIntToHugeInt(b *big.Int) (hugeInt, error)
  - Check range: -2^127 ≤ b ≤ 2^127-1
  - Return error with message "value %s overflows HUGEINT range" for overflow
  - **Acceptance:** Overflow at 2^127 and -2^127-1 returns error

- [ ] 3.4 Implement timestamp inference functions
  - inferTimestamp(t time.Time) int64 - microseconds since epoch
  - inferTimestampS(t time.Time) int64 - seconds since epoch
  - inferTimestampMS(t time.Time) int64 - milliseconds since epoch
  - inferTimestampNS(t time.Time) int64 - nanoseconds since epoch (may overflow)
  - inferTimestampTZ(t time.Time) (int64, int32) - microseconds + UTC offset minutes
  - **Acceptance:** All 5 variants preserve correct precision

- [ ] 3.5 Implement date/time inference functions
  - inferDate(t time.Time) int32 - days since Unix epoch
  - inferTime(t time.Time) int64 - microseconds since midnight
  - **Acceptance:** Date ignores time component, Time ignores date component

## 4. JSON Parsing (type_json.go)

- [ ] 4.1 Implement ParseValue(data []byte, typ Type) (any, error)
  - Switch on Type to call appropriate parser
  - Return nil for JSON null
  - Return descriptive error for parse failures
  - **Acceptance:** All 54 types parse correctly from CLI JSON

- [ ] 4.2 Implement special float handling
  - Parse "Infinity" → math.Inf(1)
  - Parse "-Infinity" → math.Inf(-1)
  - Parse "NaN" → math.NaN()
  - Parse null → nil
  - Parse numeric JSON → float64
  - **Acceptance:** All IEEE 754 special values handled

- [ ] 4.3 Implement nested type parsing
  - LIST: Recursive []any with element parsing
  - STRUCT: map[string]any with value parsing
  - MAP: Parse [{key:K, value:V}, ...] into Map
  - ARRAY: Same as LIST
  - **Acceptance:** 3-level nested structures parse correctly

- [ ] 4.4 Implement UNION type parsing
  - Parse {"tag": "variant_name", "value": <value>}
  - Set Union.Tag to variant name
  - Parse Union.Value based on tag (requires type info)
  - **Acceptance:** Union with all variant types parses correctly

- [ ] 4.5 Implement BLOB parsing
  - Parse "\x48454C4C4F" hex format
  - Strip "\x" prefix, hex decode remaining
  - **Acceptance:** Binary data round-trips correctly

## 5. Scanner/Valuer Verification

- [ ] 5.1 Verify UUID implements sql.Scanner and driver.Valuer
  - Compile-time check: var _ sql.Scanner = (*UUID)(nil)
  - Compile-time check: var _ driver.Valuer = UUID{}
  - **Acceptance:** Compiles without error

- [ ] 5.2 Verify Interval, Decimal, Map, Union implement both interfaces
  - Same compile-time checks for each type
  - **Acceptance:** All compile without error

- [ ] 5.3 Document NULL handling behavior
  - Scan(nil) sets zero value for non-pointer fields
  - Scan(nil) for pointer types sets pointer to nil
  - Value() on zero value returns appropriate representation
  - **Acceptance:** NULL round-trip documented and tested

## 6. Testing

- [ ] 6.1 Unit tests for each type's Scan method
  - Test valid inputs, nil, type mismatches
  - Use table-driven tests with subtests
  - **Acceptance:** 100% branch coverage on Scan methods

- [ ] 6.2 Unit tests for each type's Value method
  - Test zero values, typical values, edge cases
  - **Acceptance:** 100% branch coverage on Value methods

- [ ] 6.3 JSON round-trip tests for all types
  - Parse JSON → Go value → JSON (if marshaling) or Value() output
  - Verify semantic equivalence
  - **Acceptance:** All 30+ types round-trip correctly

- [ ] 6.4 Edge case tests
  - HUGEINT: -2^127, 0, 2^127-1, overflow
  - DECIMAL: max precision (38 digits), negative, zero
  - TIMESTAMP_NS: near overflow (year 2262+)
  - FLOAT: Infinity, -Infinity, NaN
  - BLOB: empty, large (1MB)
  - UUID: all zeros, all ones
  - **Acceptance:** All edge cases pass

- [ ] 6.5 API compatibility tests against duckdb-go signatures
  - Verify type signatures match duckdb-go exports
  - Verify method signatures match
  - **Acceptance:** Can drop-in replace duckdb-go types
