## ADDED Requirements

### Requirement: Rows Interface

The package SHALL implement driver.Rows interface for iterating query results.

#### Scenario: Basic row iteration
- GIVEN query result with 3 rows
- WHEN calling Next() repeatedly
- THEN returns true 3 times, then false

#### Scenario: Columns returns column names
- GIVEN query "SELECT id, name FROM users"
- WHEN calling Columns()
- THEN returns ["id", "name"]

#### Scenario: Columns safe after close
- GIVEN Rows that has been closed
- WHEN calling Columns()
- THEN still returns correct column names

#### Scenario: Close prevents further iteration
- GIVEN open Rows
- WHEN Close() is called
- THEN subsequent Next() returns false

#### Scenario: Close is idempotent
- GIVEN Rows that has been closed
- WHEN Close() is called again
- THEN returns nil (no error)

#### Scenario: Empty result set
- GIVEN query that returns 0 rows
- WHEN calling Next()
- THEN returns false immediately
- AND Columns() still returns column names

### Requirement: Value Scanning

The package SHALL scan values from JSON backend format to Go types.

#### Scenario: Scan integer types
- GIVEN column of type INTEGER with value 42
- WHEN scanning into *int32
- THEN destination contains int32(42)

#### Scenario: Scan into wider integer
- GIVEN column of type INTEGER with value 42
- WHEN scanning into *int64
- THEN destination contains int64(42)

#### Scenario: Scan float types
- GIVEN column of type DOUBLE with value 3.14
- WHEN scanning into *float64
- THEN destination contains 3.14

#### Scenario: Scan string
- GIVEN column of type VARCHAR with value "hello"
- WHEN scanning into *string
- THEN destination contains "hello"

#### Scenario: Scan bytes from hex
- GIVEN column of type BLOB with JSON value "\x48454C4C4F"
- WHEN scanning into *[]byte
- THEN destination contains []byte("HELLO")

#### Scenario: Scan timestamp
- GIVEN column of type TIMESTAMP with value "2024-01-15 10:30:45.123456"
- WHEN scanning into *time.Time
- THEN destination contains time in UTC with microsecond precision

#### Scenario: Scan date
- GIVEN column of type DATE with value "2024-01-15"
- WHEN scanning into *time.Time
- THEN destination contains 2024-01-15 00:00:00 UTC

#### Scenario: Scan NULL into pointer
- GIVEN column with NULL value
- WHEN scanning into *int
- THEN destination pointer is nil

#### Scenario: Scan NULL into non-pointer
- GIVEN column with NULL value
- WHEN scanning into int (non-pointer)
- THEN destination is 0 (zero value)

#### Scenario: Scan NULL into string pointer
- GIVEN column with NULL value
- WHEN scanning into *string
- THEN destination pointer is nil

#### Scenario: Scan NULL into string non-pointer
- GIVEN column with NULL value
- WHEN scanning into string (non-pointer)
- THEN destination is "" (empty string)

#### Scenario: Scan UUID
- GIVEN column of type UUID with value "550e8400-e29b-41d4-a716-446655440000"
- WHEN scanning into *UUID
- THEN destination contains correct 16-byte UUID

#### Scenario: Scan Interval
- GIVEN column of type INTERVAL with JSON {"days": 5, "months": 2, "micros": 1000000}
- WHEN scanning into *Interval
- THEN Interval.Days=5, Interval.Months=2, Interval.Micros=1000000

#### Scenario: Scan Decimal
- GIVEN column of type DECIMAL(10,2) with value "123.45"
- WHEN scanning into *Decimal
- THEN Scale=2, Value equals big.NewInt(12345)

#### Scenario: Scan after close
- GIVEN Rows that has been closed
- WHEN calling Scan()
- THEN error of type ErrorTypeClosed is returned
- AND error message is "rows are closed"

#### Scenario: Scan type mismatch
- GIVEN column of type VARCHAR with value "hello"
- WHEN scanning into *int
- THEN error of type ErrorTypeInvalid is returned
- AND error message contains "cannot scan TYPE_VARCHAR into int"

#### Scenario: Scan wrong dest count (too few)
- GIVEN row with 3 columns
- WHEN calling Scan with 2 destinations
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 3 destinations, got 2"

#### Scenario: Scan wrong dest count (too many)
- GIVEN row with 2 columns
- WHEN calling Scan with 3 destinations
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 2 destinations, got 3"

#### Scenario: Scan before Next
- GIVEN Rows before first Next() call
- WHEN calling Scan()
- THEN error of type ErrorTypeBadState is returned
- AND error message is "no current row"

#### Scenario: Scan after exhausted
- GIVEN Rows after Next() returned false
- WHEN calling Scan()
- THEN error of type ErrorTypeBadState is returned
- AND error message is "no current row"

### Requirement: Nested Type Scanning

The package SHALL scan nested DuckDB types into Go structures.

#### Scenario: Scan LIST into slice
- GIVEN column of type LIST with JSON [1, 2, 3]
- WHEN scanning into *[]any
- THEN destination is []any{int64(1), int64(2), int64(3)}

#### Scenario: Scan empty LIST
- GIVEN column of type LIST with JSON []
- WHEN scanning into *[]any
- THEN destination is []any{} (empty slice, not nil)

#### Scenario: Scan nested LIST
- GIVEN column of type LIST with JSON [[1, 2], [3, 4]]
- WHEN scanning into *[]any
- THEN destination is []any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}}

#### Scenario: Scan STRUCT into map
- GIVEN column of type STRUCT with JSON {"a": 1, "b": "hello"}
- WHEN scanning into *map[string]any
- THEN destination is map[string]any{"a": int64(1), "b": "hello"}

#### Scenario: Scan empty STRUCT
- GIVEN column of type STRUCT with JSON {}
- WHEN scanning into *map[string]any
- THEN destination is map[string]any{} (empty map, not nil)

#### Scenario: Scan with Composite struct
- GIVEN column of type STRUCT with JSON {"name": "Alice", "age": 30}
- WHEN scanning into *Composite[Person] where Person has Name string and Age int
- THEN Composite.Get() returns Person{Name: "Alice", Age: 30}

#### Scenario: Scan MAP type
- GIVEN column of type MAP with JSON [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]
- WHEN scanning into *Map
- THEN Map contains {int64(1): "a", int64(2): "b"}

#### Scenario: Scan UNION type
- GIVEN column of type UNION with JSON {"tag": "int", "value": 42}
- WHEN scanning into *Union
- THEN Union.Tag="int" and Union.Value=int64(42)

### Requirement: Column Metadata

The package SHALL provide column type information.

#### Scenario: ColumnTypeDatabaseTypeName
- GIVEN column of type VARCHAR
- WHEN calling ColumnTypeDatabaseTypeName(index)
- THEN returns "VARCHAR"

#### Scenario: ColumnTypeDatabaseTypeName for timestamp
- GIVEN column of type TIMESTAMP_TZ
- WHEN calling ColumnTypeDatabaseTypeName(index)
- THEN returns "TIMESTAMP_TZ"

#### Scenario: ColumnTypeScanType for integer
- GIVEN column of type INTEGER
- WHEN calling ColumnTypeScanType(index)
- THEN returns reflect.TypeOf(int32(0))

#### Scenario: ColumnTypeScanType for varchar
- GIVEN column of type VARCHAR
- WHEN calling ColumnTypeScanType(index)
- THEN returns reflect.TypeOf("")

#### Scenario: ColumnTypeNullable
- GIVEN any column
- WHEN calling ColumnTypeNullable(index)
- THEN returns (true, true)

### Requirement: Scanner Interface

The package SHALL support sql.Scanner interface for custom scanning.

#### Scenario: Custom Scanner invoked
- GIVEN a type implementing sql.Scanner
- WHEN scanning column into that type
- THEN Scanner.Scan() method is called with the JSON-parsed value

#### Scenario: Scanner receives nil for NULL
- GIVEN a type implementing sql.Scanner
- WHEN scanning NULL column into that type
- THEN Scanner.Scan(nil) is called
