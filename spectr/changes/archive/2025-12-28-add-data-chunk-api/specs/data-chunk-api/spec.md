## ADDED Requirements

### Requirement: DataChunk Capacity

The package SHALL provide a constant chunk capacity of 2048 values matching DuckDB's VECTOR_SIZE.

#### Scenario: Get chunk capacity
- GIVEN the dukdb package
- WHEN calling GetDataChunkCapacity()
- THEN the result is 2048

### Requirement: DataChunk Size Management

The DataChunk SHALL track the current number of valid rows.

#### Scenario: Get initial size
- GIVEN a newly created DataChunk with 3 columns
- WHEN calling GetSize()
- THEN the result is 0

#### Scenario: Set size within capacity
- GIVEN a DataChunk with capacity 2048
- WHEN calling SetSize(1000)
- THEN no error is returned
- AND GetSize() returns 1000

#### Scenario: Set size exceeds capacity
- GIVEN a DataChunk with capacity 2048
- WHEN calling SetSize(3000)
- THEN error is returned with message containing "exceeds capacity"

#### Scenario: Set size to zero
- GIVEN a DataChunk with size 500
- WHEN calling SetSize(0)
- THEN no error is returned
- AND GetSize() returns 0

### Requirement: DataChunk Value Access

The DataChunk SHALL provide value access by column and row index.

#### Scenario: Get primitive value
- GIVEN DataChunk with INTEGER column containing [1, 2, 3]
- WHEN calling GetValue(0, 1)
- THEN the result is int32(2) and no error

#### Scenario: Get NULL value
- GIVEN DataChunk with nullable column where row 5 is NULL
- WHEN calling GetValue(0, 5)
- THEN the result is nil and no error

#### Scenario: Get value invalid column index
- GIVEN DataChunk with 3 columns
- WHEN calling GetValue(5, 0)
- THEN error is returned containing "column index 5 out of range"

#### Scenario: Get value negative column index
- GIVEN DataChunk with 3 columns
- WHEN calling GetValue(-1, 0)
- THEN error is returned containing "column index"

### Requirement: DataChunk Value Setting

The DataChunk SHALL support setting values by column and row index with type validation.

#### Scenario: Set primitive value
- GIVEN DataChunk with INTEGER column
- WHEN calling SetValue(0, 0, int32(42))
- THEN no error is returned
- AND GetValue(0, 0) returns int32(42)

#### Scenario: Set NULL value
- GIVEN DataChunk with nullable column
- WHEN calling SetValue(0, 0, nil)
- THEN no error is returned
- AND GetValue(0, 0) returns nil

#### Scenario: Set value with type coercion
- GIVEN DataChunk with BIGINT column
- WHEN calling SetValue(0, 0, int(100))
- THEN no error is returned
- AND value is converted to int64(100)

#### Scenario: Set value type mismatch
- GIVEN DataChunk with INTEGER column
- WHEN calling SetValue(0, 0, "not a number")
- THEN error is returned containing "cannot convert"

### Requirement: Generic Chunk Value Setting

The package SHALL provide generic SetChunkValue[T] for type-safe setting without runtime type assertion.

#### Scenario: Set chunk value generic
- GIVEN DataChunk with DOUBLE column
- WHEN calling SetChunkValue[float64](chunk, 0, 0, 3.14)
- THEN no error is returned
- AND GetValue(0, 0) returns float64(3.14)

#### Scenario: Set chunk value generic type mismatch
- GIVEN DataChunk with INTEGER column
- WHEN calling SetChunkValue[string](chunk, 0, 0, "hello")
- THEN error is returned

### Requirement: Column Projection

The DataChunk SHALL support column projection for sparse column access.

#### Scenario: Unprojected column ignored on set
- GIVEN DataChunk with projection [0, -1, 2] (column 1 unprojected)
- WHEN calling SetValue(1, 0, 42)
- THEN no error is returned (silently ignored)

#### Scenario: Projected column accessible
- GIVEN DataChunk with projection [0, -1, 2]
- WHEN calling SetValue(0, 0, 42)
- THEN value is set in physical column 0

#### Scenario: Get value from projected column
- GIVEN DataChunk with projection [2, 0] (reordered)
- WHEN calling GetValue(0, 0)
- THEN value from physical column 2 is returned

### Requirement: Row Accessor

The package SHALL provide a Row type for row-oriented access within a DataChunk.

#### Scenario: Row projection check
- GIVEN Row with chunk projection [0, -1, 2]
- WHEN calling IsProjected(1)
- THEN the result is false

#### Scenario: Row projection check positive
- GIVEN Row with chunk projection [0, 1, 2]
- WHEN calling IsProjected(1)
- THEN the result is true

#### Scenario: Row set value
- GIVEN Row at index 5 in chunk with INTEGER column
- WHEN calling SetRowValue(0, int32(100))
- THEN chunk.GetValue(0, 5) returns int32(100)

### Requirement: Generic Row Value Setting

The package SHALL provide generic SetRowValue[T] for type-safe row setting.

#### Scenario: Set row value generic
- GIVEN Row at index 3 in chunk with VARCHAR column
- WHEN calling SetRowValue[string](row, 0, "hello")
- THEN chunk.GetValue(0, 3) returns "hello"

### Requirement: Vector Primitive Types

The vector SHALL support all DuckDB primitive types.

#### Scenario: Boolean vector
- GIVEN vector of TYPE_BOOLEAN
- WHEN setting values true, false, nil
- THEN get returns bool(true), bool(false), nil

#### Scenario: Integer type vectors
- GIVEN vectors of TINYINT, SMALLINT, INTEGER, BIGINT
- WHEN setting minimum and maximum values for each
- THEN get returns correct typed values (int8, int16, int32, int64)

#### Scenario: Unsigned integer type vectors
- GIVEN vectors of UTINYINT, USMALLINT, UINTEGER, UBIGINT
- WHEN setting minimum (0) and maximum values for each
- THEN get returns correct typed values (uint8, uint16, uint32, uint64)

#### Scenario: Float type vectors
- GIVEN vectors of FLOAT, DOUBLE
- WHEN setting values including Inf, -Inf, NaN
- THEN get returns correct float32 or float64 values

### Requirement: Vector String Types

The vector SHALL support string and binary types.

#### Scenario: VARCHAR vector
- GIVEN vector of TYPE_VARCHAR
- WHEN setting "hello world" and ""
- THEN get returns string values

#### Scenario: BLOB vector
- GIVEN vector of TYPE_BLOB
- WHEN setting []byte{0x00, 0xFF}
- THEN get returns []byte with same content

#### Scenario: JSON alias
- GIVEN vector with JSON alias
- WHEN getting values
- THEN values are returned as parsed JSON (map[string]any or []any)

### Requirement: Vector Temporal Types

The vector SHALL support all DuckDB temporal types.

#### Scenario: TIMESTAMP vector precision variants
- GIVEN vectors of TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS
- WHEN setting time.Time value
- THEN get returns time.Time with appropriate precision

#### Scenario: TIMESTAMP_TZ with timezone
- GIVEN vector of TYPE_TIMESTAMP_TZ
- WHEN setting time.Time in non-UTC timezone
- THEN timezone offset is preserved

#### Scenario: DATE vector
- GIVEN vector of TYPE_DATE
- WHEN setting time.Time for 2024-01-15 10:30:00
- THEN get returns time.Time for 2024-01-15 00:00:00 UTC

#### Scenario: TIME vector
- GIVEN vector of TYPE_TIME
- WHEN setting time representing 10:30:45.123456
- THEN get returns time with same time-of-day

#### Scenario: INTERVAL vector
- GIVEN vector of TYPE_INTERVAL
- WHEN setting Interval{Months: 1, Days: 2, Micros: 3000000}
- THEN get returns same Interval value

### Requirement: Vector Complex Numeric Types

The vector SHALL support HUGEINT, DECIMAL, and UUID types.

#### Scenario: HUGEINT vector
- GIVEN vector of TYPE_HUGEINT
- WHEN setting *big.Int larger than int64 max
- THEN get returns equivalent *big.Int

#### Scenario: DECIMAL vector
- GIVEN vector of TYPE_DECIMAL with width=10, scale=2
- WHEN setting Decimal value 123.45
- THEN get returns Decimal with same width, scale, value

#### Scenario: UUID vector
- GIVEN vector of TYPE_UUID
- WHEN setting UUID bytes
- THEN get returns same UUID

### Requirement: Vector ENUM Type

The vector SHALL support ENUM types with dictionary lookup.

#### Scenario: ENUM vector set by name
- GIVEN vector of TYPE_ENUM with values ["red", "green", "blue"]
- WHEN setting "green"
- THEN get returns "green"

#### Scenario: ENUM vector invalid value
- GIVEN vector of TYPE_ENUM with values ["red", "green", "blue"]
- WHEN setting "purple"
- THEN error is returned containing "invalid enum value"

### Requirement: Vector LIST Type

The vector SHALL support LIST types with child vectors.

#### Scenario: LIST vector
- GIVEN vector of TYPE_LIST with child type INTEGER
- WHEN setting []any{int32(1), int32(2), int32(3)}
- THEN get returns []any with same elements

#### Scenario: Nested LIST vector
- GIVEN vector of LIST(LIST(INTEGER))
- WHEN setting nested lists
- THEN get returns correctly nested structure

#### Scenario: Empty LIST
- GIVEN vector of TYPE_LIST
- WHEN setting []any{}
- THEN get returns empty slice (not nil)

### Requirement: Vector STRUCT Type

The vector SHALL support STRUCT types with named fields.

#### Scenario: STRUCT vector
- GIVEN vector of STRUCT(name VARCHAR, age INTEGER)
- WHEN setting map[string]any{"name": "Alice", "age": int32(30)}
- THEN get returns map with same fields

#### Scenario: Nested STRUCT
- GIVEN vector of STRUCT containing another STRUCT
- WHEN setting nested map structure
- THEN get returns correctly nested maps

### Requirement: Vector MAP Type

The vector SHALL support MAP types with key-value pairs.

#### Scenario: MAP vector
- GIVEN vector of MAP(INTEGER, VARCHAR)
- WHEN setting Map{int32(1): "one", int32(2): "two"}
- THEN get returns Map with same entries

#### Scenario: MAP key type restriction
- GIVEN vector initialization for MAP with LIST key type
- WHEN initializing vector
- THEN error is returned containing "unsupported map key type"

### Requirement: Vector ARRAY Type

The vector SHALL support fixed-size ARRAY types.

#### Scenario: ARRAY vector
- GIVEN vector of ARRAY(INTEGER, 3)
- WHEN setting [3]int32{1, 2, 3}
- THEN get returns slice with 3 elements

#### Scenario: ARRAY size validation
- GIVEN vector of ARRAY(INTEGER, 3)
- WHEN setting slice with 4 elements
- THEN error is returned containing "array size mismatch"

### Requirement: Vector UNION Type

The vector SHALL support UNION types with tagged values.

#### Scenario: UNION vector
- GIVEN vector of UNION(i INTEGER, s VARCHAR)
- WHEN setting Union{Tag: "i", Value: int32(42)}
- THEN get returns Union with same tag and value

#### Scenario: UNION invalid tag
- GIVEN vector of UNION(i INTEGER, s VARCHAR)
- WHEN setting Union{Tag: "invalid", Value: 42}
- THEN error is returned containing "invalid union tag"

### Requirement: Vector NULL Handling

The vector SHALL handle NULL values via validity bitmap.

#### Scenario: Set NULL value
- GIVEN vector of any type
- WHEN setting nil
- THEN validity bitmap marks row as NULL
- AND get returns nil

#### Scenario: Overwrite NULL with value
- GIVEN vector with NULL at row 0
- WHEN setting non-nil value at row 0
- THEN validity bitmap marks row as valid
- AND get returns the new value

#### Scenario: Batch NULL check
- GIVEN vector with mixed NULL and non-NULL values
- WHEN checking validity for multiple rows
- THEN bitmap operations efficiently determine NULL status

### Requirement: DataChunk Initialization

The DataChunk SHALL initialize from type specifications.

#### Scenario: Initialize from types
- GIVEN array of logical types [INTEGER, VARCHAR, BOOLEAN]
- WHEN calling initFromTypes()
- THEN chunk has 3 columns with correct types
- AND all columns are writable

#### Scenario: Initialize with nested types
- GIVEN logical type LIST(STRUCT(a INTEGER, b VARCHAR))
- WHEN initializing chunk
- THEN nested child vectors are correctly created

### Requirement: DataChunk Reset

The DataChunk SHALL support reset for reuse.

#### Scenario: Reset clears size
- GIVEN DataChunk with size 500
- WHEN calling reset()
- THEN GetSize() returns capacity (for reuse pattern)

#### Scenario: Reset preserves column structure
- GIVEN DataChunk with 3 columns
- WHEN calling reset()
- THEN column count and types are preserved

### Requirement: DataChunk Cleanup

The DataChunk SHALL properly clean up resources on close.

#### Scenario: Close releases memory
- GIVEN DataChunk with nested vectors
- WHEN calling close()
- THEN all child vectors are cleaned up
- AND subsequent GetValue returns error
