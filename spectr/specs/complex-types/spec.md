# Complex Types Specification

## Requirements

### Requirement: JSONVector Type

The package SHALL provide a JSONVector type for storing JSON documents in columnar format with lazy parsing.

#### Scenario: Create JSONVector with capacity
- WHEN creating JSONVector with capacity 1024
- THEN JSONVector is initialized with empty string slice and validity bitmap
- AND JSONVector is ready to accept string values

#### Scenario: Set and get JSON string
- GIVEN JSONVector initialized
- WHEN setting value at index 0 to '{"name": "Alice"}'
- THEN GetValue(0) returns the JSON string
- AND internal validity mask marks index 0 as valid

#### Scenario: Set NULL JSON value
- GIVEN JSONVector initialized
- WHEN calling SetNull(0)
- THEN GetValue(0) returns nil
- AND internal validity mask marks index 0 as NULL

#### Scenario: JSONVector reports exact validity
- GIVEN JSONVector with mixed valid and NULL values
- WHEN calling IsValid(i) for each index
- THEN returns correct validity state

#### Scenario: JSONVector supports batch operations
- GIVEN JSONVector with capacity 2048
- WHEN setting 1000 JSON values
- THEN all values accessible at their indices
- AND memory footprint is reasonable (one string slice + validity bitmap)

### Requirement: MapVector Type

The package SHALL provide a MapVector type for storing key-value pairs with separate key and value vectors.

#### Scenario: Create MapVector with key and value types
- WHEN creating MapVector(TYPE_VARCHAR, TYPE_INTEGER)
- THEN MapVector initializes with key and value child vectors
- AND validity bitmap for map-level NULLs

#### Scenario: Access map key and value vectors
- GIVEN MapVector initialized
- WHEN accessing GetKeyVector() and GetValueVector()
- THEN returns child Vector instances for key and value columns
- AND child vectors have same row capacity as parent

#### Scenario: Set map value at index
- GIVEN MapVector with TYPE_VARCHAR keys and TYPE_BIGINT values
- WHEN setting key="region" and value=1000 at index 5
- THEN key and value vectors updated at corresponding indices

#### Scenario: MapVector maintains parent validity
- GIVEN MapVector with NULL at index 3
- WHEN calling IsValid(3)
- THEN returns false (parent-level NULL)
- AND child vector access for index 3 returns zero values

#### Scenario: MapVector offsets track key-value boundaries
- GIVEN MapVector with variable-length maps (some rows have 1 pair, some 3)
- WHEN storing offsets internally
- THEN each row knows its map boundaries
- AND GetKeyVector() contains all keys concatenated

### Requirement: StructVector Type

The package SHALL provide a StructVector type for storing named fields with per-field vectors.

#### Scenario: Create StructVector with fields
- WHEN creating StructVector with fields {"id" TYPE_INTEGER, "name" TYPE_VARCHAR, "age" TYPE_SMALLINT}
- THEN StructVector initializes field vectors for each field
- AND field order is preserved

#### Scenario: Add field to StructVector
- GIVEN StructVector with existing fields
- WHEN calling AddField("email", TYPE_VARCHAR)
- THEN new field vector is created
- AND field is accessible for all existing rows

#### Scenario: Access field vector by name
- GIVEN StructVector with fields
- WHEN calling GetField("age")
- THEN returns Vector for age field
- AND returned vector has same capacity as parent

#### Scenario: Set struct field value at index
- GIVEN StructVector with integer "id" field
- WHEN calling GetField("id").SetValue(5, int32(42))
- THEN id field contains 42 at index 5

#### Scenario: StructVector maintains field count
- GIVEN StructVector with 5 fields
- WHEN iterating fields
- THEN exactly 5 fields returned
- AND field order matches creation order

#### Scenario: StructVector parent validity
- GIVEN StructVector with struct NULL at index 2
- WHEN calling IsValid(2) on parent
- THEN returns false
- AND field vectors at index 2 contain zero values (or NULL if field allows)

#### Scenario: StructVector field names case-insensitive lookup
- GIVEN StructVector with field "UserName"
- WHEN calling GetField("username")
- THEN field is found (case-insensitive)

### Requirement: UnionVector Type

The package SHALL provide a UnionVector type for storing tagged variants with active member tracking.

#### Scenario: Create UnionVector with members
- WHEN creating UnionVector with members [("value" TYPE_INTEGER), ("error" TYPE_VARCHAR)]
- THEN UnionVector initializes member vectors for each variant
- AND member names preserved

#### Scenario: Set union to specific member
- GIVEN UnionVector with members value/error
- WHEN setting index 0 to member "value" with value 42
- THEN active member index is 0 (value)
- AND member vector contains 42

#### Scenario: Set different member per row
- GIVEN UnionVector
- WHEN setting rows alternately to value/error members
- THEN different active members tracked per row
- AND each row's active member accessible

#### Scenario: Access active member index
- GIVEN UnionVector with index 5 set to "error" member
- WHEN calling GetActiveIndex(5)
- THEN returns member index for "error"
- AND GetActiveMemberName(5) returns "error"

#### Scenario: UnionVector member NULL tracking
- GIVEN UnionVector with NULL at row 3
- WHEN calling IsValid(3)
- THEN returns false
- AND all member vectors at index 3 are marked NULL

#### Scenario: UnionVector counts members
- GIVEN UnionVector created with 4 members
- WHEN calling GetMemberCount()
- THEN returns 4

### Requirement: Complex Vector Validity Semantics

The system SHALL ensure consistent validity semantics for complex types with parent-child NULL propagation.

#### Scenario: Parent NULL blocks child access
- GIVEN StructVector with struct NULL at index 2
- WHEN accessing child field vector
- THEN child vector's validity at index 2 reflects parent NULL
- AND no exception on access

#### Scenario: Child NULL independent from parent
- GIVEN MapVector with valid map at index 3
- AND key vector has NULL at index 3 (invalid map structure)
- THEN getting value propagates error or handles gracefully

#### Scenario: Deep nesting validity propagation
- GIVEN StructVector containing MAP field
- AND struct is NULL at index 5
- WHEN accessing the MAP's child vectors
- THEN validity reflects struct-level NULL

#### Scenario: Batch validity check performance
- GIVEN complex vector with 10,000 rows
- WHEN calling CountValid()
- THEN returns count efficiently (bit-level operations)
- AND completes in milliseconds

### Requirement: Complex Vector Serialization Compatibility

The system SHALL serialize complex vectors to DuckDB 1.4.3 row group format.

#### Scenario: JSON vector serializes as string column
- GIVEN JSONVector with 100 rows
- WHEN converting to DuckDBColumnSegment
- THEN segment type is TYPE_VARCHAR/TYPE_JSON
- AND strings are compressed (FSST)
- AND validity bitmap included

#### Scenario: MAP vector serializes children
- GIVEN MapVector(VARCHAR keys, BIGINT values) with 50 rows
- WHEN converting to DuckDBColumnSegment
- THEN segment contains two child segments
- AND child 0 is VARCHAR (keys), child 1 is BIGINT (values)
- AND segment metadata marks it as MAP type

#### Scenario: STRUCT vector serializes fields in order
- GIVEN StructVector with fields [id INT, name VARCHAR, age INT]
- WHEN converting to DuckDBColumnSegment
- THEN segment contains 3 children in same field order
- AND metadata preserves field names

#### Scenario: UNION vector serializes all members
- GIVEN UnionVector with members [value INT, error VARCHAR]
- WHEN converting to DuckDBColumnSegment
- THEN segment contains indices column + 2 member columns
- AND metadata lists member names and types

#### Scenario: Complex vector deserializes from segment
- GIVEN DuckDBColumnSegment for STRUCT
- WHEN converting to StructVector
- THEN field vectors reconstructed
- AND field names accessible
- AND validity bitmaps restored

### Requirement: Complex Vector Capacity Management

The system SHALL manage capacity for complex vectors and their children consistently.

#### Scenario: Complex vector resize
- GIVEN JSONVector with capacity 1024
- WHEN calling SetCapacity(2048)
- THEN capacity increases
- AND existing values preserved
- AND new slots available

#### Scenario: Child vector resize with parent
- GIVEN StructVector with 3 field vectors
- WHEN resizing parent to capacity 2000
- THEN all child vectors resized to 2000
- AND child data preserved

#### Scenario: Capacity shrink cleans up
- GIVEN MapVector with capacity 5000
- WHEN calling SetCapacity(1000)
- THEN capacity decreases
- AND excess data cleaned
- AND remaining data preserved

### Requirement: Complex Vector Type Introspection

The system SHALL provide type metadata for complex vectors.

#### Scenario: Get type of complex vector
- GIVEN JSONVector
- WHEN calling Type()
- THEN returns TYPE_JSON

#### Scenario: Get child type from MapVector
- GIVEN MapVector(VARCHAR → INTEGER)
- WHEN calling GetKeyType() and GetValueType()
- THEN returns TYPE_VARCHAR and TYPE_INTEGER

#### Scenario: Get field types from StructVector
- GIVEN StructVector with fields
- WHEN calling GetFieldType(name)
- THEN returns type of named field

#### Scenario: Get member types from UnionVector
- GIVEN UnionVector
- WHEN calling GetMemberTypes()
- THEN returns slice of member types in order

