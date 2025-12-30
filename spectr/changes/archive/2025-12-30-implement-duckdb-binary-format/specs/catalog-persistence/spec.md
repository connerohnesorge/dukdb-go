# Catalog Persistence Specification Delta

## ADDED Requirements

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

### DEFERRED: UNION TypeInfo Binary Serialization (Future Work)

**STATUS**: UNION serialization is NOT in DuckDB v64 format and is deferred to future work.

The system SHALL NOT serialize UNION TypeInfo in v64 format:
- UNION type is not included in DuckDB v1.1.3 ExtraTypeInfoType enum
- Attempting to serialize UNION SHALL return ErrUnsupportedTypeForSerialization
- UNION TypeInfo CAN still be constructed in-memory (per P0-1a Core TypeInfo)
- UNION serialization will be added when DuckDB format v65+ includes it

#### Scenario: UNION serialization returns error
- GIVEN NewUnionInfo([intInfo, varcharInfo], ["num", "str"])
- WHEN attempting to serialize to binary format
- THEN error is ErrUnsupportedTypeForSerialization
- AND error message contains "UNION not supported in format v64"

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
