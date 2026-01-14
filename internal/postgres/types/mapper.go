package types

import (
	"strings"
)

// TypeMapper provides bidirectional mapping between PostgreSQL and DuckDB types.
type TypeMapper interface {
	// PostgreSQLToDuckDB resolves a PostgreSQL type name to its DuckDB equivalent.
	// Returns nil if the type is not recognized.
	PostgreSQLToDuckDB(pgType string) *TypeMapping

	// DuckDBToPostgresOID returns the PostgreSQL OID for a DuckDB type name.
	// Returns OID_UNKNOWN (705) if the type is not recognized.
	DuckDBToPostgresOID(duckDBType string) uint32

	// GetTypeSize returns the type size in bytes for wire protocol RowDescription.
	// Returns -1 for variable-length types.
	GetTypeSize(oid uint32) int16

	// GetTypeName returns the PostgreSQL type name for an OID.
	GetTypeName(oid uint32) string

	// IsArrayType returns true if the OID represents an array type.
	IsArrayType(oid uint32) bool
}

// DefaultTypeMapper is the default implementation of TypeMapper.
type DefaultTypeMapper struct{}

// NewTypeMapper creates a new TypeMapper instance.
func NewTypeMapper() TypeMapper {
	return &DefaultTypeMapper{}
}

// PostgreSQLToDuckDB implements TypeMapper.
func (*DefaultTypeMapper) PostgreSQLToDuckDB(pgType string) *TypeMapping {
	return GetTypeAlias(pgType)
}

// duckDBToOIDMap maps normalized DuckDB type names to PostgreSQL OIDs.
var duckDBToOIDMap = map[string]uint32{
	// Boolean
	"BOOLEAN": OID_BOOL,
	"BOOL":    OID_BOOL,

	// Integer types (signed)
	"TINYINT":  OID_INT2, // PostgreSQL doesn't have 1-byte int, use smallint
	"INT1":     OID_INT2,
	"SMALLINT": OID_INT2,
	"INT2":     OID_INT2,
	"INTEGER":  OID_INT4,
	"INT":      OID_INT4,
	"INT4":     OID_INT4,
	"BIGINT":   OID_INT8,
	"INT8":     OID_INT8,
	"HUGEINT":  OID_NUMERIC, // Use numeric for huge integers
	"INT128":   OID_NUMERIC,

	// Integer types (unsigned) - map to next larger signed type
	"UTINYINT":  OID_INT2,
	"UINT1":     OID_INT2,
	"USMALLINT": OID_INT4,
	"UINT2":     OID_INT4,
	"UINTEGER":  OID_INT8,
	"UINT4":     OID_INT8,
	"UBIGINT":   OID_NUMERIC,
	"UINT8":     OID_NUMERIC,

	// Floating point
	"FLOAT":  OID_FLOAT4,
	"FLOAT4": OID_FLOAT4,
	"REAL":   OID_FLOAT4,
	"DOUBLE": OID_FLOAT8,
	"FLOAT8": OID_FLOAT8,

	// Decimal/Numeric
	"DECIMAL": OID_NUMERIC,
	"NUMERIC": OID_NUMERIC,

	// String types
	"VARCHAR": OID_TEXT,
	"STRING":  OID_TEXT,
	"TEXT":    OID_TEXT,
	"CHAR":    OID_BPCHAR,
	"BPCHAR":  OID_BPCHAR,

	// Binary
	"BLOB":  OID_BYTEA,
	"BYTEA": OID_BYTEA,

	// Date/Time
	"DATE":                     OID_DATE,
	"TIME":                     OID_TIME,
	"TIMETZ":                   OID_TIMETZ,
	"TIMESTAMP":                OID_TIMESTAMP,
	"TIMESTAMPTZ":              OID_TIMESTAMPTZ,
	"TIMESTAMP WITH TIME ZONE": OID_TIMESTAMPTZ,
	"INTERVAL":                 OID_INTERVAL,

	// JSON
	"JSON": OID_JSON,

	// UUID
	"UUID": OID_UUID,

	// Bit strings
	"BIT":    OID_BIT,
	"VARBIT": OID_VARBIT,

	// Array types
	"INTEGER[]":  OID_INT4_ARRAY,
	"INT[]":      OID_INT4_ARRAY,
	"INT4[]":     OID_INT4_ARRAY,
	"BIGINT[]":   OID_INT8_ARRAY,
	"INT8[]":     OID_INT8_ARRAY,
	"SMALLINT[]": OID_INT2_ARRAY,
	"INT2[]":     OID_INT2_ARRAY,
	"TEXT[]":     OID_TEXT_ARRAY,
	"VARCHAR[]":  OID_TEXT_ARRAY,
	"STRING[]":   OID_TEXT_ARRAY,
	"BOOLEAN[]":  OID_BOOL_ARRAY,
	"BOOL[]":     OID_BOOL_ARRAY,
	"FLOAT[]":    OID_FLOAT4_ARRAY,
	"FLOAT4[]":   OID_FLOAT4_ARRAY,
	"REAL[]":     OID_FLOAT4_ARRAY,
	"DOUBLE[]":   OID_FLOAT8_ARRAY,
	"FLOAT8[]":   OID_FLOAT8_ARRAY,

	// LIST type (DuckDB's array type) - default to text array
	"LIST": OID_TEXT_ARRAY,
}

// DuckDBToPostgresOID implements TypeMapper.
func (*DefaultTypeMapper) DuckDBToPostgresOID(duckDBType string) uint32 {
	// Normalize the DuckDB type name
	normalized := strings.ToUpper(strings.TrimSpace(duckDBType))

	// Handle parameterized types - extract base type
	if idx := strings.Index(normalized, "("); idx != -1 {
		normalized = strings.TrimSpace(normalized[:idx])
	}

	// Look up in the map
	if oid, ok := duckDBToOIDMap[normalized]; ok {
		return oid
	}

	return OID_UNKNOWN
}

// typeSizes maps PostgreSQL OIDs to their fixed sizes.
// -1 indicates variable length, -2 indicates null-terminated string.
var typeSizes = map[uint32]int16{
	OID_BOOL:        1,
	OID_INT2:        2,
	OID_INT4:        4,
	OID_INT8:        8,
	OID_FLOAT4:      4,
	OID_FLOAT8:      8,
	OID_DATE:        4,
	OID_TIME:        8,
	OID_TIMETZ:      12,
	OID_TIMESTAMP:   8,
	OID_TIMESTAMPTZ: 8,
	OID_INTERVAL:    16,
	OID_OID:         4,
	OID_UUID:        16,
	// Variable length types
	OID_TEXT:    -1,
	OID_VARCHAR: -1,
	OID_CHAR:    -1,
	OID_BPCHAR:  -1,
	OID_NAME:    -1,
	OID_BYTEA:   -1,
	OID_JSON:    -1,
	OID_JSONB:   -1,
	OID_NUMERIC: -1,
	OID_XML:     -1,
	OID_BIT:     -1,
	OID_VARBIT:  -1,
	OID_MONEY:   8,
	// Array types are variable length
	OID_BOOL_ARRAY:    -1,
	OID_INT2_ARRAY:    -1,
	OID_INT4_ARRAY:    -1,
	OID_INT8_ARRAY:    -1,
	OID_TEXT_ARRAY:    -1,
	OID_VARCHAR_ARRAY: -1,
	OID_FLOAT4_ARRAY:  -1,
	OID_FLOAT8_ARRAY:  -1,
}

// GetTypeSize implements TypeMapper.
func (*DefaultTypeMapper) GetTypeSize(oid uint32) int16 {
	if size, ok := typeSizes[oid]; ok {
		return size
	}

	return -1 // Default to variable length for unknown types
}

// GetTypeName implements TypeMapper.
func (*DefaultTypeMapper) GetTypeName(oid uint32) string {
	if name, ok := OIDToName[oid]; ok {
		return name
	}

	return "unknown"
}

// IsArrayType implements TypeMapper.
func (*DefaultTypeMapper) IsArrayType(oid uint32) bool {
	return IsArrayOID(oid)
}

// Global default mapper instance.
var defaultMapper = NewTypeMapper()

// GetDefaultMapper returns the global default TypeMapper instance.
func GetDefaultMapper() TypeMapper {
	return defaultMapper
}

// Convenience functions that use the default mapper.

// MapPostgreSQLToDuckDB maps a PostgreSQL type to DuckDB using the default mapper.
func MapPostgreSQLToDuckDB(pgType string) *TypeMapping {
	return defaultMapper.PostgreSQLToDuckDB(pgType)
}

// MapDuckDBToPostgresOID maps a DuckDB type to PostgreSQL OID using the default mapper.
func MapDuckDBToPostgresOID(duckDBType string) uint32 {
	return defaultMapper.DuckDBToPostgresOID(duckDBType)
}
