## 1. Rows Implementation (rows.go)

- [ ] 1.1 Implement Rows struct
  - Fields: columns []string, colTypes []Type, data [][]any, index int, closed bool
  - Constructor: NewRows(columns []string, colTypes []Type, data [][]any) *Rows
  - Initialize index to -1 (before first row)
  - **Acceptance:** Struct correctly initialized from backend response

- [ ] 1.2 Implement Rows.Columns() []string
  - Return column names in order (copy of slice to prevent mutation)
  - Safe to call after Close()
  - **Acceptance:** Returns correct column names in query order

- [ ] 1.3 Implement Rows.Next(dest []driver.Value) bool
  - Increment index
  - Return false if index >= len(data) or closed
  - Copy current row values to dest slice
  - **Acceptance:** Iterates through all rows exactly once

- [ ] 1.4 Implement Rows.Close() error
  - Set closed = true
  - Clear data slice to free memory: `data = nil`
  - Idempotent (safe to call multiple times, returns nil each time)
  - **Acceptance:** Subsequent Next() returns false, no memory leak

- [ ] 1.5 Implement Rows.Scan(dest ...any) error
  - If closed → return `&Error{Type: ErrorTypeClosed, Msg: "rows are closed"}`
  - If no current row (index < 0 or >= len) → return `&Error{Type: ErrorTypeBadState, Msg: "no current row"}`
  - If len(dest) != len(columns) → return `&Error{Type: ErrorTypeInvalid, Msg: "expected N destinations, got M"}`
  - Scan each value using scanValue()
  - **Acceptance:** Clear error message for each failure case

## 2. Value Scanning (scan.go)

- [ ] 2.1 Implement scanValue(src any, dest any) error
  - Check if dest implements sql.Scanner → call dest.Scan(src)
  - Check if src is nil → handle NULL (see 2.5)
  - Determine dest type via reflection
  - Call appropriate conversion function
  - Return `&Error{Type: ErrorTypeInvalid, Msg: "cannot scan TYPE into T"}` for incompatible types
  - **Acceptance:** All compatible type conversions work

- [ ] 2.2 Implement scanning for primitive types
  - JSON number (float64) → int8/int16/int32/int64 (truncate decimals)
  - JSON number (float64) → uint8/uint16/uint32/uint64 (truncate decimals)
  - JSON number (float64) → float32/float64
  - JSON string → string
  - JSON boolean → bool
  - Hex string "\x48454C4C4F" → []byte (parse hex after \x)
  - ISO date string "2024-01-15" → time.Time (DATE)
  - ISO time string "15:04:05.000000" → time.Time (TIME, set date to 0001-01-01)
  - ISO datetime string → time.Time (TIMESTAMP)
  - **Acceptance:** All primitive types scan correctly

- [ ] 2.3 Implement scanning for custom types
  - Hyphenated string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" → UUID
  - Object {"months": N, "days": N, "micros": N} → Interval
  - String representation → Decimal (parse precision and scale)
  - String representation → *big.Int (parse big integer)
  - **Acceptance:** Custom types match duckdb-go behavior

- [ ] 2.4 Implement scanning for nested types
  - JSON array → []any (recursive element scanning)
  - JSON object → map[string]any (recursive value scanning)
  - Array of {"key": K, "value": V} objects → Map
  - Object with "tag" field and value → Union
  - **Acceptance:** 3-level nested structures scan correctly

- [ ] 2.5 Implement NULL handling
  - If dest is pointer (*T): set pointer to nil
  - If dest is non-pointer (T): set to zero value of T
  - If dest implements sql.Scanner: call dest.Scan(nil)
  - **Zero values:** int→0, string→"", bool→false, []byte→nil, time.Time→time.Time{}
  - **Acceptance:** NULL handling matches database/sql behavior

- [ ] 2.6 Implement sql.Scanner detection
  - Check if dest implements sql.Scanner via type assertion
  - If so, call dest.Scan(src) and return its result
  - Works for both pointer and value receivers
  - **Acceptance:** Custom Scanner types receive raw value

## 3. Column Metadata

- [ ] 3.1 Implement Rows.ColumnTypeScanType(index int) reflect.Type
  - Complete mapping:
    - TYPE_BOOLEAN → reflect.TypeOf(false)
    - TYPE_TINYINT → reflect.TypeOf(int8(0))
    - TYPE_SMALLINT → reflect.TypeOf(int16(0))
    - TYPE_INTEGER → reflect.TypeOf(int32(0))
    - TYPE_BIGINT → reflect.TypeOf(int64(0))
    - TYPE_UTINYINT → reflect.TypeOf(uint8(0))
    - TYPE_USMALLINT → reflect.TypeOf(uint16(0))
    - TYPE_UINTEGER → reflect.TypeOf(uint32(0))
    - TYPE_UBIGINT → reflect.TypeOf(uint64(0))
    - TYPE_FLOAT → reflect.TypeOf(float32(0))
    - TYPE_DOUBLE → reflect.TypeOf(float64(0))
    - TYPE_VARCHAR → reflect.TypeOf("")
    - TYPE_BLOB → reflect.TypeOf([]byte{})
    - TYPE_DATE/TIME/TIMESTAMP → reflect.TypeOf(time.Time{})
    - TYPE_UUID → reflect.TypeOf(UUID{})
    - TYPE_INTERVAL → reflect.TypeOf(Interval{})
    - TYPE_DECIMAL → reflect.TypeOf(Decimal{})
    - TYPE_HUGEINT → reflect.TypeOf((*big.Int)(nil))
    - TYPE_LIST → reflect.TypeOf([]any{})
    - TYPE_STRUCT → reflect.TypeOf(map[string]any{})
    - TYPE_MAP → reflect.TypeOf(Map{})
    - TYPE_UNION → reflect.TypeOf(Union{})
    - Default → reflect.TypeOf((*any)(nil)).Elem()
  - **Acceptance:** Returns correct Go type for each column

- [ ] 3.2 Implement Rows.ColumnTypeDatabaseTypeName(index int) string
  - Return DuckDB type name: "VARCHAR", "INTEGER", "TIMESTAMP", etc.
  - Use colTypes[index].String() (Type.String() from type-system)
  - **Acceptance:** Returns correct DuckDB type names

- [ ] 3.3 Implement Rows.ColumnTypeNullable(index int) (nullable, ok bool)
  - Return (true, true) - all columns potentially nullable in DuckDB
  - We don't track NOT NULL constraint at this level
  - **Acceptance:** Always returns (true, true)

## 4. Empty and Edge Cases

- [ ] 4.1 Handle empty result set
  - Rows with len(data) == 0
  - First Next() returns false immediately
  - Columns() still returns column names
  - Close() works normally
  - **Acceptance:** Empty results don't panic, Columns() works

- [ ] 4.2 Handle zero-column result
  - Result with no columns (e.g., from DDL statement)
  - Columns() returns empty slice []string{}
  - Next() may return true if there are "rows" (execution count)
  - **Acceptance:** DDL results work

- [ ] 4.3 Handle large result sets
  - 100,000+ rows
  - Memory usage bounded by JSON response size
  - No streaming - all data buffered
  - **Acceptance:** Large results don't crash, complete within reasonable time

## 5. Testing

- [ ] 5.1 Scan tests for all primitive types
  - Test: JSON number 42 → int8(42), int16(42), int32(42), int64(42)
  - Test: JSON number 42 → uint8(42), uint16(42), uint32(42), uint64(42)
  - Test: JSON number 3.14 → float32(3.14), float64(3.14)
  - Test: JSON string "hello" → string("hello")
  - Test: JSON string "\x4849" → []byte{0x48, 0x49}
  - Test: JSON boolean true → bool(true)
  - Test: JSON string "2024-01-15" → time.Time (DATE)
  - Test: JSON string "15:04:05.123456" → time.Time (TIME)
  - Test: JSON string "2024-01-15 15:04:05" → time.Time (TIMESTAMP)
  - **Acceptance:** 100% type coverage

- [ ] 5.2 Scan tests for custom dukdb types
  - Test: "a1b2c3d4-e5f6-7890-abcd-ef1234567890" → UUID
  - Test: {"months": 1, "days": 2, "micros": 3000000} → Interval
  - Test: "123.45" → Decimal{Value: 12345, Scale: 2}
  - Test: "12345678901234567890" → *big.Int
  - **Acceptance:** All custom types scan correctly

- [ ] 5.3 Scan tests for nested types
  - Test: [1, 2, 3] → []any{1, 2, 3}
  - Test: [[1, 2], [3, 4]] → []any{[]any{1, 2}, []any{3, 4}}
  - Test: {"a": 1, "b": "two"} → map[string]any{"a": 1, "b": "two"}
  - Test: [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}] → Map
  - **Acceptance:** Nested structures scan correctly

- [ ] 5.4 NULL handling tests
  - Test: nil → *int → nil pointer
  - Test: nil → int → 0
  - Test: nil → *string → nil pointer
  - Test: nil → string → ""
  - Test: nil → *bool → nil pointer
  - Test: nil → bool → false
  - **Acceptance:** NULL behavior correct for all types

- [ ] 5.5 Error case tests
  - Test: Scan after Close() → ErrorTypeClosed "rows are closed"
  - Test: Scan with wrong dest count → ErrorTypeInvalid "expected N destinations, got M"
  - Test: Scan before Next() → ErrorTypeBadState "no current row"
  - Test: Scan string into int → ErrorTypeInvalid "cannot scan TYPE_VARCHAR into int64"
  - **Acceptance:** Clear error messages
