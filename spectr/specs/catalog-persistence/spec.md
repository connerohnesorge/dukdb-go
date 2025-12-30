# Catalog Persistence Specification

## Requirements

### Requirement: Database File Creation

The system SHALL create a database file when opening with a non-memory path.

#### Scenario: New database file created
- GIVEN a path "/tmp/test.duckdb" that does not exist
- WHEN calling sql.Open("dukdb", path)
- AND creating a table
- AND closing the database
- THEN a file exists at the specified path

#### Scenario: Memory database creates no file
- GIVEN the path ":memory:"
- WHEN calling sql.Open("dukdb", ":memory:")
- AND creating a table
- AND closing the database
- THEN no file is created

#### Scenario: Existing file is loaded
- GIVEN a valid database file at "/tmp/test.duckdb"
- WHEN calling sql.Open("dukdb", path)
- THEN the existing schemas and data are available

#### Scenario: Empty database persists
- GIVEN a new database with no tables created
- WHEN the database is closed and reopened
- THEN no errors occur
- AND the database is ready for table creation

### Requirement: Catalog Persistence

The system SHALL persist catalog metadata (schemas, tables, columns) to the database file.

#### Scenario: Table schema persists
- GIVEN a database at path "/tmp/test.duckdb"
- AND a table created with `CREATE TABLE users (id INTEGER, name VARCHAR)`
- WHEN the database is closed and reopened
- THEN the table "users" exists with columns "id" (INTEGER) and "name" (VARCHAR)

#### Scenario: Multiple tables persist
- GIVEN a database with tables "users" and "orders"
- WHEN the database is closed and reopened
- THEN both tables exist with their original schemas

#### Scenario: Column nullability persists
- GIVEN a table with column "id INTEGER NOT NULL"
- WHEN the database is closed and reopened
- THEN the column "id" is marked as NOT NULL

#### Scenario: Primary key persists
- GIVEN a table with `PRIMARY KEY (id)`
- WHEN the database is closed and reopened
- THEN the primary key constraint is preserved

#### Scenario: Default values persist
- GIVEN a table with column "status VARCHAR DEFAULT 'active'"
- WHEN the database is closed and reopened
- THEN the column "status" has default value 'active'

#### Scenario: Unicode table and column names persist
- GIVEN a table named "用户表" with column "名前"
- WHEN the database is closed and reopened
- THEN the table "用户表" exists with column "名前"

#### Scenario: Decimal precision and scale persist
- GIVEN a table with column "price DECIMAL(18,4)"
- WHEN the database is closed and reopened
- THEN the column "price" has precision 18 and scale 4

#### Scenario: STRUCT field names persist
- GIVEN a table with column "data STRUCT(x INTEGER, y VARCHAR)"
- WHEN the database is closed and reopened
- THEN the field names "x" and "y" are preserved exactly

### Requirement: Data Persistence

The system SHALL persist table row data to the database file.

#### Scenario: Integer data persists
- GIVEN a table with INTEGER column containing values [1, 2, 3]
- WHEN the database is closed and reopened
- THEN querying the table returns [1, 2, 3]

#### Scenario: String data persists
- GIVEN a table with VARCHAR column containing ["Alice", "Bob", "Charlie"]
- WHEN the database is closed and reopened
- THEN querying the table returns ["Alice", "Bob", "Charlie"]

#### Scenario: Unicode string data persists
- GIVEN a table with VARCHAR column containing ["Hello", "世界", "🌍🔥"]
- WHEN the database is closed and reopened
- THEN querying the table returns ["Hello", "世界", "🌍🔥"]

#### Scenario: NULL values persist
- GIVEN a table with rows containing NULL values
- WHEN the database is closed and reopened
- THEN the NULL values are preserved

#### Scenario: NULL in INTEGER column persists
- GIVEN an INTEGER column with [1, NULL, 3]
- WHEN the database is closed and reopened
- THEN values are [1, NULL, 3]

#### Scenario: NULL in DECIMAL column persists
- GIVEN a DECIMAL column with [1.5, NULL, 3.5]
- WHEN the database is closed and reopened
- THEN values are [1.5, NULL, 3.5]

#### Scenario: NULL in DATE column persists
- GIVEN a DATE column with [2024-01-01, NULL, 2024-12-31]
- WHEN the database is closed and reopened
- THEN values are [2024-01-01, NULL, 2024-12-31]

#### Scenario: NULL in UUID column persists
- GIVEN a UUID column with [valid-uuid, NULL]
- WHEN the database is closed and reopened
- THEN values are [valid-uuid, NULL]

#### Scenario: NULL in LIST column persists
- GIVEN a LIST(INTEGER) column with [[1,2], NULL, [3]]
- WHEN the database is closed and reopened
- THEN values are [[1,2], NULL, [3]]

#### Scenario: All-NULL column persists
- GIVEN a column where every row is NULL
- WHEN the database is closed and reopened
- THEN all values remain NULL

#### Scenario: Large dataset persists
- GIVEN a table with 100,000 rows
- WHEN the database is closed and reopened
- THEN all 100,000 rows are present and correct

#### Scenario: Empty table persists
- GIVEN an empty table
- WHEN the database is closed and reopened
- THEN the table exists but contains no rows

### Requirement: All Types Persistence

The system SHALL persist all supported DuckDB types correctly.

#### Scenario: Boolean type persists
- GIVEN a BOOLEAN column with [true, false, NULL]
- WHEN the database is closed and reopened
- THEN values are [true, false, NULL]

#### Scenario: Numeric types persist
- GIVEN columns TINYINT, SMALLINT, INTEGER, BIGINT
- WHEN the database is closed and reopened
- THEN all numeric values are preserved exactly

#### Scenario: Floating point types persist
- GIVEN columns FLOAT, DOUBLE with values including infinity and NaN
- WHEN the database is closed and reopened
- THEN special floating point values are preserved

#### Scenario: Positive infinity persists
- GIVEN a DOUBLE column with value +Infinity
- WHEN the database is closed and reopened
- THEN the value is +Infinity

#### Scenario: Negative infinity persists
- GIVEN a DOUBLE column with value -Infinity
- WHEN the database is closed and reopened
- THEN the value is -Infinity

#### Scenario: NaN persists
- GIVEN a DOUBLE column with value NaN
- WHEN the database is closed and reopened
- THEN the value is NaN (NaN != NaN is true)

#### Scenario: Negative zero persists
- GIVEN a DOUBLE column with value -0.0
- WHEN the database is closed and reopened
- THEN the value is -0.0 (1/-0.0 = -Infinity)

#### Scenario: HUGEINT type persists
- GIVEN a HUGEINT column with max value 170141183460469231731687303715884105727
- WHEN the database is closed and reopened
- THEN the value is preserved exactly

#### Scenario: UHUGEINT type persists
- GIVEN a UHUGEINT column with max value 340282366920938463463374607431768211455
- WHEN the database is closed and reopened
- THEN the value is preserved exactly

#### Scenario: ARRAY type persists
- GIVEN an ARRAY(INTEGER, 3) column with [1, 2, 3]
- WHEN the database is closed and reopened
- THEN the array value [1, 2, 3] is preserved

#### Scenario: ENUM type persists
- GIVEN an ENUM('small', 'medium', 'large') column with value 'medium'
- WHEN the database is closed and reopened
- THEN the value is 'medium'
- AND the enum definition is preserved

#### Scenario: Decimal type persists
- GIVEN a DECIMAL(18,4) column with value 12345.6789
- WHEN the database is closed and reopened
- THEN the decimal value is preserved exactly

#### Scenario: Date/Time types persist
- GIVEN DATE, TIME, TIMESTAMP columns
- WHEN the database is closed and reopened
- THEN all temporal values are preserved

#### Scenario: BLOB type persists
- GIVEN a BLOB column with binary data
- WHEN the database is closed and reopened
- THEN the binary data is identical

#### Scenario: UUID type persists
- GIVEN a UUID column with value
- WHEN the database is closed and reopened
- THEN the UUID is preserved

#### Scenario: Interval type persists
- GIVEN an INTERVAL column
- WHEN the database is closed and reopened
- THEN the interval value is preserved

### Requirement: Nested Types Persistence

The system SHALL persist nested types (LIST, STRUCT, MAP) correctly.

#### Scenario: LIST type persists
- GIVEN a LIST(INTEGER) column with [[1,2,3], [4,5], NULL]
- WHEN the database is closed and reopened
- THEN the list values are preserved

#### Scenario: STRUCT type persists
- GIVEN a STRUCT(x INTEGER, y VARCHAR) column
- WHEN the database is closed and reopened
- THEN the struct values are preserved

#### Scenario: MAP type persists
- GIVEN a MAP(VARCHAR, INTEGER) column
- WHEN the database is closed and reopened
- THEN the map values are preserved

#### Scenario: Nested structures persist
- GIVEN a LIST(STRUCT(a INTEGER, b VARCHAR)) column
- WHEN the database is closed and reopened
- THEN the nested values are preserved

### Requirement: File Format Validation

The system SHALL validate database files on open.

#### Scenario: Magic number validation
- GIVEN a file without valid magic number
- WHEN attempting to open as database
- THEN an error is returned indicating invalid file format

#### Scenario: Version validation
- GIVEN a file with unsupported version
- WHEN attempting to open as database
- THEN an error is returned indicating version incompatibility

#### Scenario: Checksum validation
- GIVEN a file with corrupted checksum
- WHEN attempting to open as database
- THEN an error is returned indicating file corruption

#### Scenario: Truncated file detection
- GIVEN a file that is truncated mid-block
- WHEN attempting to open as database
- THEN an error is returned indicating incomplete file

#### Scenario: Corrupted data block detection
- GIVEN a file with modified bytes in a data block
- WHEN attempting to open as database
- THEN an error is returned indicating block checksum mismatch

#### Scenario: Corrupted catalog detection
- GIVEN a file with corrupted catalog JSON
- WHEN attempting to open as database
- THEN an error is returned indicating catalog parse error

#### Scenario: Corrupted block index detection
- GIVEN a file with invalid block index
- WHEN attempting to open as database
- THEN an error is returned indicating index corruption

### Requirement: Atomic Save

The system SHALL save database files atomically to prevent corruption.

#### Scenario: Temp file used for save
- GIVEN a database being saved
- WHEN the save operation is in progress
- THEN a temporary file is created
- AND the original file is not modified until completion

#### Scenario: Failed save preserves original
- GIVEN a database at path "/tmp/test.duckdb"
- AND a save operation that fails (e.g., disk full simulation)
- WHEN the save fails
- THEN the original file is unchanged
- AND no partial file remains

#### Scenario: Successful save replaces original
- GIVEN a database with changes
- WHEN the save completes successfully
- THEN the original file is replaced atomically
- AND no temporary file remains

### Requirement: Multiple Schema Support

The system SHALL persist databases with multiple schemas.

#### Scenario: Custom schema persists
- GIVEN a schema "analytics" with tables
- WHEN the database is closed and reopened
- THEN the "analytics" schema exists with its tables

#### Scenario: Multiple schemas persist
- GIVEN schemas "main" and "staging" each with tables
- WHEN the database is closed and reopened
- THEN both schemas exist with their tables

### Requirement: Database Reopening

The system SHALL allow reopening a database multiple times.

#### Scenario: Reopen after close
- GIVEN a database that was opened and closed
- WHEN reopening the same file
- THEN all data is accessible

#### Scenario: Multiple open-close cycles
- GIVEN a database file
- WHEN opened, modified, closed 10 times
- THEN all modifications persist

#### Scenario: Insert after reopen
- GIVEN a database that was closed and reopened
- WHEN inserting new data
- THEN the new data is added to existing data

#### Scenario: Modify after reopen
- GIVEN a database that was closed and reopened
- WHEN updating existing data
- THEN the modifications are reflected

### Requirement: Error Handling

The system SHALL handle persistence errors gracefully.

#### Scenario: Read-only filesystem
- GIVEN a database path on read-only filesystem
- WHEN attempting to save
- THEN an appropriate error is returned
- AND the database remains usable in memory

#### Scenario: Missing parent directory
- GIVEN a path with non-existent parent directory
- WHEN attempting to create database
- THEN an appropriate error is returned

#### Scenario: Permission denied
- GIVEN a path where write is not permitted
- WHEN attempting to save
- THEN an appropriate error is returned

### Requirement: Memory Database Unchanged

The system SHALL not change behavior for in-memory databases.

#### Scenario: Memory database no persistence
- GIVEN a database opened with ":memory:"
- WHEN the database is closed
- THEN no file operations occur

#### Scenario: Memory database performance
- GIVEN a database opened with ":memory:"
- WHEN performing operations
- THEN performance is equivalent to previous implementation

### Requirement: Single-Process Constraint

The system SHALL support single-process access only.

#### Scenario: Concurrent multi-process access undefined
- GIVEN a database file at "/tmp/test.duckdb"
- AND process A has the database open
- WHEN process B attempts to open the same file
- THEN behavior is undefined (no file locking)
- AND documentation warns against concurrent process access

#### Scenario: Sequential process access works
- GIVEN process A opens, modifies, and closes the database
- WHEN process B opens the same file afterward
- THEN process B sees all changes from process A

### Requirement: Deterministic Testing

The system SHALL support deterministic testing.

#### Scenario: Tests use t.TempDir
- GIVEN a test function for persistence
- WHEN the test creates database files
- THEN it MUST use t.TempDir() for isolation
- AND no hardcoded paths are used

#### Scenario: Tests are parallelizable
- GIVEN multiple persistence tests
- WHEN run with `go test -parallel`
- THEN all tests pass without interference

#### Scenario: No time dependencies
- GIVEN persistence operations
- WHEN executed
- THEN no time.Now() or time.Sleep() calls affect behavior

### Requirement: DuckDB Binary Format Structure

The system SHALL use DuckDB's v64 binary format for catalog persistence to enable cross-implementation compatibility.

#### Scenario: File header with magic number
- GIVEN a database saved to "/tmp/test.duckdb"
- WHEN reading the first 4 bytes
- THEN the bytes equal 0x4455434B ("DUCK" in ASCII)

#### Scenario: File header with version
- GIVEN a database saved to "/tmp/test.duckdb"
- WHEN reading bytes 4-11 (8 bytes after magic)
- THEN the little-endian uint64 value equals 64 (format v64)

#### Scenario: File uses little-endian byte order
- GIVEN any multi-byte value in the file
- WHEN reading the bytes
- THEN values are encoded using binary.LittleEndian

#### Scenario: File ends with CRC64 checksum
- GIVEN a database saved to "/tmp/test.duckdb"
- WHEN reading the last 8 bytes
- THEN the bytes contain a CRC64-ECMA checksum of the file contents

### Requirement: Property-Based Serialization

The system SHALL use property-based serialization matching DuckDB's auto-generated serialize_types.cpp format.

#### Scenario: Properties written in sorted ID order
- GIVEN a TypeInfo being serialized
- WHEN properties are written to the file
- THEN property IDs appear in ascending numeric order (100, 101, 200, 201, ...)

#### Scenario: Base property IDs (100-199)
- GIVEN any TypeInfo serialization
- WHEN writing base properties
- THEN property 100 is type discriminator (ExtraTypeInfoType enum)
- AND property 101 is optional alias string
- AND property 103 is optional extension info

#### Scenario: Type-specific property IDs (200-299)
- GIVEN a DECIMAL TypeInfo
- WHEN writing type-specific properties
- THEN property 200 is width (uint8)
- AND property 201 is scale (uint8)

#### Scenario: Required properties always written
- GIVEN any TypeInfo with required properties
- WHEN serializing
- THEN all required properties (e.g., property 100 type discriminator) are written

#### Scenario: Optional properties with defaults skipped
- GIVEN a TypeInfo with optional property matching default value
- WHEN serializing
- THEN the property is not written (uses WritePropertyWithDefault)

### Requirement: DECIMAL TypeInfo Binary Serialization

The system SHALL serialize DECIMAL TypeInfo to DuckDB's ExtraTypeInfo binary format.

#### Scenario: DECIMAL(18,4) binary format
- GIVEN NewDecimalInfo(18, 4)
- WHEN serializing to binary format
- THEN property 100 = DECIMAL_TYPE_INFO enum value
- AND property 200 = uint8(18) (width)
- AND property 201 = uint8(4) (scale)

#### Scenario: DECIMAL(38,38) maximum values
- GIVEN NewDecimalInfo(38, 38)
- WHEN serializing to binary format
- THEN property 200 = uint8(38)
- AND property 201 = uint8(38)

#### Scenario: DECIMAL round-trip preserves metadata
- GIVEN NewDecimalInfo(18, 4) serialized to file
- WHEN deserializing from the file
- THEN width equals 18
- AND scale equals 4

### Requirement: ENUM TypeInfo Binary Serialization

The system SHALL serialize ENUM TypeInfo with value vector and optional dictionary size hint.

#### Scenario: ENUM binary format
- GIVEN NewEnumInfo("RED", "GREEN", "BLUE")
- WHEN serializing to binary format
- THEN property 100 = ENUM_TYPE_INFO enum value (6)
- AND property 200 = values_count (uint64 = 3)
- AND property 201 = values list ["RED", "GREEN", "BLUE"] using WriteList

#### Scenario: ENUM with 100 values
- GIVEN NewEnumInfo with 100 distinct values
- WHEN serializing to binary format
- THEN property 200 = values_count (uint64 = 100)
- AND property 201 contains all 100 values in order using WriteList

#### Scenario: ENUM round-trip preserves value order
- GIVEN NewEnumInfo("Z", "A", "M") serialized to file
- WHEN deserializing from the file
- THEN values equal ["Z", "A", "M"] in exact order

#### Scenario: ENUM values immutable after deserialization
- GIVEN ENUM TypeInfo deserialized from file
- WHEN modifying the returned Values slice
- THEN subsequent reads return original values (defensive copy)

### Requirement: LIST TypeInfo Binary Serialization

The system SHALL serialize LIST TypeInfo with recursive child type support.

#### Scenario: LIST(INTEGER) binary format
- GIVEN NewListInfo(NewTypeInfo(TYPE_INTEGER))
- WHEN serializing to binary format
- THEN property 100 = LIST_TYPE_INFO enum value
- AND property 200 = serialized child TypeInfo (TYPE_INTEGER)

#### Scenario: Nested LIST(LIST(VARCHAR))
- GIVEN NewListInfo(NewListInfo(NewTypeInfo(TYPE_VARCHAR)))
- WHEN serializing to binary format
- THEN property 200 contains LIST TypeInfo
- AND that LIST's property 200 contains VARCHAR TypeInfo

#### Scenario: LIST of STRUCT
- GIVEN NewListInfo(structInfo) where structInfo is STRUCT(x INTEGER, y VARCHAR)
- WHEN serializing to binary format
- THEN property 200 contains serialized STRUCT TypeInfo
- AND STRUCT's field metadata is preserved

### Requirement: ARRAY TypeInfo Binary Serialization

The system SHALL serialize ARRAY TypeInfo with child type and fixed size.

#### Scenario: ARRAY(INTEGER, 10) binary format
- GIVEN NewArrayInfo(NewTypeInfo(TYPE_INTEGER), 10)
- WHEN serializing to binary format
- THEN property 100 = ARRAY_TYPE_INFO enum value
- AND property 200 = serialized child TypeInfo (TYPE_INTEGER)
- AND property 201 = uint32(10) (size)

#### Scenario: ARRAY round-trip preserves size
- GIVEN NewArrayInfo(intInfo, 1000) serialized to file
- WHEN deserializing from the file
- THEN size equals 1000

#### Scenario: ARRAY of complex type
- GIVEN NewArrayInfo(structInfo, 5)
- WHEN serializing to binary format
- THEN property 200 contains serialized STRUCT TypeInfo
- AND size property 201 = uint32(5)

### Requirement: STRUCT TypeInfo Binary Serialization

The system SHALL serialize STRUCT TypeInfo with field list preserving names and types.

#### Scenario: STRUCT binary format
- GIVEN NewStructInfo(entry1 "id" INTEGER, entry2 "name" VARCHAR)
- WHEN serializing to binary format
- THEN property 100 = STRUCT_TYPE_INFO enum value
- AND property 200 = field list with count 2
- AND field list contains [(name="id", type=INTEGER), (name="name", type=VARCHAR)]

#### Scenario: STRUCT with 20 fields
- GIVEN NewStructInfo with 20 fields
- WHEN serializing to binary format
- THEN property 200 field list count equals 20
- AND all field names and types are preserved in order

#### Scenario: Nested STRUCT
- GIVEN STRUCT(outer INTEGER, inner STRUCT(x INTEGER, y VARCHAR))
- WHEN serializing to binary format
- THEN outer STRUCT property 200 contains 2 fields
- AND second field type is serialized inner STRUCT
- AND inner STRUCT field names "x", "y" are preserved

#### Scenario: STRUCT round-trip preserves field names
- GIVEN STRUCT(field_name VARCHAR) serialized to file
- WHEN deserializing from the file
- THEN field name equals "field_name" exactly

### Requirement: MAP TypeInfo Binary Serialization

The system SHALL serialize MAP TypeInfo with key and value types.

#### Scenario: MAP(VARCHAR, INTEGER) binary format
- GIVEN NewMapInfo(varcharInfo, intInfo)
- WHEN serializing to binary format
- THEN property 100 = MAP_TYPE_INFO enum value
- AND property 200 = serialized key TypeInfo (VARCHAR)
- AND property 201 = serialized value TypeInfo (INTEGER)

#### Scenario: MAP with complex value type
- GIVEN NewMapInfo(intInfo, structInfo)
- WHEN serializing to binary format
- THEN property 201 contains serialized STRUCT TypeInfo
- AND STRUCT field metadata is preserved

#### Scenario: MAP with complex key type
- GIVEN NewMapInfo(structInfo, varcharInfo)
- WHEN serializing to binary format
- THEN property 200 contains serialized STRUCT TypeInfo

### Requirement: Cross-Implementation Compatibility

The system SHALL read and write .duckdb files compatible with DuckDB C++ v1.1.3 and duckdb-go v1.4.3.

#### Scenario: Read file created by duckdb-go v1.4.3
- GIVEN a .duckdb file created by duckdb-go v1.4.3
- AND the file contains table with DECIMAL(18,4) column
- WHEN opening the file with dukdb-go
- THEN the table is readable
- AND DECIMAL width equals 18
- AND DECIMAL scale equals 4

#### Scenario: Write file readable by DuckDB C++
- GIVEN a dukdb-go database with table containing STRUCT(x INTEGER, y VARCHAR)
- WHEN saving to "/tmp/test.duckdb"
- AND opening the file with DuckDB CLI (v1.1.3)
- THEN `DESCRIBE table` shows STRUCT with fields x, y
- AND field types are INTEGER, VARCHAR

#### Scenario: Round-trip with duckdb-go v1.4.3
- GIVEN a .duckdb file created by duckdb-go v1.4.3 with all 37 types
- WHEN dukdb-go reads and writes the file
- AND duckdb-go v1.4.3 reads the new file
- THEN all TypeInfo metadata is preserved exactly

#### Scenario: ENUM values preserved across implementations
- GIVEN ENUM("small", "medium", "large") created in duckdb-go v1.4.3
- WHEN dukdb-go reads the file
- THEN enum values equal ["small", "medium", "large"] in order

#### Scenario: Nested types compatible
- GIVEN LIST(STRUCT(MAP(VARCHAR, INTEGER))) in DuckDB C++
- WHEN dukdb-go reads the file
- THEN nested type structure is preserved
- AND all type metadata is accessible

### Requirement: Binary Format Error Detection

The system SHALL detect and report binary format errors with descriptive messages.

#### Scenario: Invalid magic number detected
- GIVEN a file with first 4 bytes != 0x4455434B
- WHEN attempting to open
- THEN error is ErrInvalidMagicNumber
- AND error message contains "invalid magic number"

#### Scenario: Unsupported version detected
- GIVEN a file with version != 64
- WHEN attempting to open
- THEN error is ErrUnsupportedVersion
- AND error message contains "unsupported version" AND version number

#### Scenario: Checksum mismatch detected
- GIVEN a file with modified bytes but valid header
- WHEN attempting to open
- THEN error is ErrChecksumMismatch
- AND error message contains "checksum mismatch"

#### Scenario: Truncated file detected
- GIVEN a file missing final bytes
- WHEN attempting to open
- THEN error indicates file is truncated or incomplete

#### Scenario: Missing required property detected
- GIVEN serialized TypeInfo missing property 100 (type discriminator)
- WHEN deserializing
- THEN error message contains "missing required property 100"

#### Scenario: Corrupted property data detected
- GIVEN serialized TypeInfo with invalid property value
- WHEN deserializing
- THEN error describes the property ID and issue

### Requirement: Atomic File Operations

The system SHALL write database files atomically using temporary file and rename.

#### Scenario: Temporary file created during save
- GIVEN a database being saved to "/tmp/test.duckdb"
- WHEN save is in progress
- THEN a temporary file exists at "/tmp/test.duckdb.tmp" or similar
- AND original file is not modified

#### Scenario: Successful save renames temp file
- GIVEN a save operation that completes successfully
- WHEN the save is done
- THEN temporary file is renamed to final path
- AND no temporary file remains

#### Scenario: Failed save removes temp file
- GIVEN a save operation that fails
- WHEN the error is returned
- THEN temporary file is cleaned up
- AND original file is unchanged

### Requirement: Performance Characteristics

The system SHALL serialize and deserialize TypeInfo efficiently.

#### Scenario: Serialization performance acceptable
- GIVEN 1000 TypeInfo instances to serialize
- WHEN serializing all instances
- THEN operation completes in reasonable time (< 1 second)

#### Scenario: Deserialization performance acceptable
- GIVEN a .duckdb file with 100 tables
- WHEN deserializing all table schemas
- THEN operation completes in reasonable time (< 1 second)

#### Scenario: Memory allocations minimized
- GIVEN TypeInfo deserialization
- WHEN profiling memory allocations
- THEN allocations are proportional to TypeInfo complexity
- AND no excessive temporary allocations occur

### Requirement: Hex Dump Verification

The system SHALL produce byte-for-byte compatible output with DuckDB reference implementation.

#### Scenario: DECIMAL(18,4) hex dump matches
- GIVEN DECIMAL(18,4) serialized by dukdb-go
- AND DECIMAL(18,4) serialized by DuckDB C++ v1.1.3
- WHEN comparing hex dumps
- THEN bytes are identical

#### Scenario: ENUM("A","B","C") hex dump matches
- GIVEN ENUM("A","B","C") serialized by dukdb-go
- AND ENUM("A","B","C") serialized by DuckDB C++ v1.1.3
- WHEN comparing hex dumps
- THEN bytes are identical

#### Scenario: Complex nested type hex dump matches
- GIVEN LIST(STRUCT(x INTEGER, y VARCHAR)) serialized by both implementations
- WHEN comparing hex dumps
- THEN bytes are identical

### Requirement: Robustness Testing

The system SHALL handle invalid binary format data without panics.

#### Scenario: Fuzz testing with random bytes
- GIVEN a binary reader fed with random bytes
- WHEN deserializing TypeInfo
- THEN function returns error (not panic)

#### Scenario: Invalid property combinations handled
- GIVEN serialized data with contradictory properties
- WHEN deserializing
- THEN function returns descriptive error

#### Scenario: Malformed catalog data handled
- GIVEN corrupted catalog binary data
- WHEN deserializing
- THEN function returns error with context
- AND no panic occurs
