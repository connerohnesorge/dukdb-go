// Package format provides DuckDB binary file format (v64) compatibility for catalog persistence.
//
// This package enables cross-platform database file interchange between dukdb-go, DuckDB C++,
// and duckdb-go v1.4.3 implementations. It implements property-based serialization matching
// DuckDB's binary format specification.
//
//nolint:revive // Constant names use underscores to match DuckDB C++ source exactly for binary compatibility
package format

import (
	"encoding/binary"
	"errors"
)

// DuckDB Binary Format Constants
//
// These constants define the DuckDB v64 binary file format structure.
// All values MUST match DuckDB C++ v1.1.3 exactly for compatibility.

const (
	// DuckDBMagicNumber is the 4-byte magic number at the start of all DuckDB files.
	// ASCII representation: "DUCK"
	DuckDBMagicNumber = 0x4455434B

	// DuckDBFormatVersion is the binary format version number.
	// Version 64 corresponds to DuckDB v1.1.3.
	DuckDBFormatVersion = 64
)

// ByteOrder specifies the endianness for all multi-byte values in DuckDB files.
// DuckDB uses little-endian byte order throughout the binary format.
var ByteOrder = binary.LittleEndian

// Property ID Constants
//
// DuckDB uses property-based serialization with numeric property IDs.
// Properties are grouped into base properties (100-199) and type-specific properties (200+).

// Base Properties (100-199)
//
// These properties are common across all types and provide metadata about the type system.
const (
	// PropertyTypeDiscriminator identifies the ExtraTypeInfoType enum value.
	// This property determines which type-specific serialization to use.
	PropertyTypeDiscriminator = 100

	// PropertyAlias is an optional string alias for the type.
	PropertyAlias = 101

	// PropertyModifiers is a deleted property retained for backward compatibility.
	// This property ID is reserved but not used in v64 format.
	PropertyModifiers = 102

	// PropertyExtensionInfo contains extension-specific type information.
	PropertyExtensionInfo = 103
)

// Type-Specific Properties (200-299)
//
// These properties are specific to particular type categories.
// The same property ID may be reused across different type categories.

// DECIMAL Properties
const (
	// PropertyDecimalWidth is the total number of digits (1-38) for DECIMAL types.
	PropertyDecimalWidth = 200

	// PropertyDecimalScale is the number of digits after the decimal point for DECIMAL types.
	PropertyDecimalScale = 201
)

// ENUM Properties
const (
	// PropertyEnumCount is the number of enum values (uint64/idx_t).
	// Note: This reuses property ID 200 within the ENUM type context.
	PropertyEnumCount = 200

	// PropertyEnumValues is the list of enum value strings.
	// Note: This reuses property ID 201 within the ENUM type context.
	PropertyEnumValues = 201
)

// LIST/ARRAY Properties
const (
	// PropertyChildType is the TypeInfo of the child element for LIST and ARRAY types.
	// Note: This reuses property ID 200 within the LIST/ARRAY type context.
	PropertyChildType = 200

	// PropertyArraySize is the fixed size for ARRAY types (not used for LIST).
	// Note: This reuses property ID 201 within the ARRAY type context.
	PropertyArraySize = 201
)

// STRUCT Properties
const (
	// PropertyStructFields is the child_list_t<LogicalType> for STRUCT field definitions.
	// Each field contains a name and its corresponding TypeInfo.
	// Note: This reuses property ID 200 within the STRUCT type context.
	PropertyStructFields = 200
)

// MAP Properties
const (
	// PropertyMapKeyType is the TypeInfo for MAP key types.
	// Note: This reuses property ID 200 within the MAP type context.
	PropertyMapKeyType = 200

	// PropertyMapValueType is the TypeInfo for MAP value types.
	// Note: This reuses property ID 201 within the MAP type context.
	PropertyMapValueType = 201
)

// UNION Properties
const (
	// PropertyUnionMembers is the tagged union type member definitions.
	// Note: UNION serialization is NOT supported in v64 format (deferred to v65+).
	// Note: This reuses property ID 200 within the UNION type context.
	PropertyUnionMembers = 200
)

// ExtraTypeInfoType Enum
//
// These constants define the type discriminator values used in property 100.
// Values MUST match DuckDB C++ v1.1.3 exactly (duckdb/src/include/duckdb/common/extra_type_info.hpp).
//
// Types In Scope for Binary Serialization (6 types):
//   - DECIMAL (2): Fixed-precision decimal numbers
//   - LIST (4): Variable-length lists
//   - STRUCT (5): Named field structures
//   - ENUM (6): Enumerated string values
//   - ARRAY (9): Fixed-size arrays
//   - MAP: Uses LIST_TYPE_INFO (4) with child STRUCT<key, value> (no separate enum value)
//
// Types Deferred to Future Work (7 types):
//   - STRING (3): Collation info
//   - USER (7): User-defined types
//   - AGGREGATE_STATE (8): Aggregate function state
//   - ANY (10): Any type
//   - INTEGER_LITERAL (11): Constant values
//   - TEMPLATE (12): Template types
//   - GEO (13): Geographic types
const (
	// ExtraTypeInfoType_INVALID represents an invalid or uninitialized type.
	ExtraTypeInfoType_INVALID = 0

	// ExtraTypeInfoType_GENERIC represents a generic type with no extra info.
	ExtraTypeInfoType_GENERIC = 1

	// ExtraTypeInfoType_DECIMAL represents fixed-precision decimal numbers.
	// Uses PropertyDecimalWidth and PropertyDecimalScale.
	ExtraTypeInfoType_DECIMAL = 2

	// ExtraTypeInfoType_STRING represents string types with collation info.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_STRING = 3

	// ExtraTypeInfoType_LIST represents variable-length list types.
	// Uses PropertyChildType for the element type.
	// Also used for MAP types (MAP is LIST<STRUCT<key, value>>).
	ExtraTypeInfoType_LIST = 4

	// ExtraTypeInfoType_STRUCT represents structure types with named fields.
	// Uses PropertyStructFields for field definitions.
	ExtraTypeInfoType_STRUCT = 5

	// ExtraTypeInfoType_ENUM represents enumerated string value types.
	// Uses PropertyEnumCount and PropertyEnumValues.
	ExtraTypeInfoType_ENUM = 6

	// ExtraTypeInfoType_USER represents user-defined types.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_USER = 7

	// ExtraTypeInfoType_AGGREGATE_STATE represents aggregate function state.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_AGGREGATE_STATE = 8

	// ExtraTypeInfoType_ARRAY represents fixed-size array types.
	// Uses PropertyChildType and PropertyArraySize.
	ExtraTypeInfoType_ARRAY = 9

	// ExtraTypeInfoType_ANY represents the any type.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_ANY = 10

	// ExtraTypeInfoType_INTEGER_LITERAL represents integer literal constant types.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_INTEGER_LITERAL = 11

	// ExtraTypeInfoType_TEMPLATE represents template types.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_TEMPLATE = 12

	// ExtraTypeInfoType_GEO represents geographic/spatial types.
	// DEFERRED: Not supported for serialization in v64 format.
	ExtraTypeInfoType_GEO = 13
)

// CatalogEntryType Enum
//
// These constants identify different catalog entry types in the DuckDB catalog.
const (
	// CatalogEntryType_INVALID represents an invalid catalog entry.
	CatalogEntryType_INVALID = 0

	// CatalogEntryType_SCHEMA represents a database schema.
	CatalogEntryType_SCHEMA = 1

	// CatalogEntryType_TABLE represents a table definition.
	CatalogEntryType_TABLE = 2

	// CatalogEntryType_VIEW represents a view definition.
	CatalogEntryType_VIEW = 3

	// CatalogEntryType_INDEX represents an index definition.
	CatalogEntryType_INDEX = 4

	// CatalogEntryType_SEQUENCE represents a sequence generator.
	CatalogEntryType_SEQUENCE = 5

	// CatalogEntryType_MACRO represents a macro function.
	CatalogEntryType_MACRO = 6

	// CatalogEntryType_TABLE_MACRO represents a table-valued macro.
	CatalogEntryType_TABLE_MACRO = 7

	// CatalogEntryType_TYPE represents a user-defined type.
	CatalogEntryType_TYPE = 8
)

// Errors
//
// These errors are returned by format serialization and deserialization functions.
var (
	// ErrInvalidMagicNumber indicates the file does not start with the DuckDB magic number.
	ErrInvalidMagicNumber = errors.New("invalid DuckDB magic number")

	// ErrUnsupportedVersion indicates the file uses an unsupported format version.
	ErrUnsupportedVersion = errors.New("unsupported DuckDB format version")

	// ErrChecksumMismatch indicates data corruption was detected via checksum verification.
	ErrChecksumMismatch = errors.New("checksum verification failed")

	// ErrRequiredProperty indicates a required property is missing from the binary data.
	ErrRequiredProperty = errors.New("required property missing")

	// ErrInvalidPropertyType indicates a property value has an incorrect type.
	ErrInvalidPropertyType = errors.New("property type mismatch")

	// ErrUnsupportedTypeForSerialization indicates the type cannot be serialized in this format version.
	// This applies to UNION types and other types deferred to future format versions.
	ErrUnsupportedTypeForSerialization = errors.New("type not supported for serialization in this format version")
)
