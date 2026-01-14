package types

import (
	"strings"
)

// TypeMapping represents the mapping from a PostgreSQL type to DuckDB.
// This structure contains all metadata needed for type conversion
// during DDL parsing and wire protocol communication.
type TypeMapping struct {
	// PostgreSQLName is the canonical PostgreSQL type name (e.g., "serial", "text").
	PostgreSQLName string

	// DuckDBType is the corresponding DuckDB type name (e.g., "INTEGER", "VARCHAR").
	DuckDBType string

	// PostgresOID is the PostgreSQL type OID for wire protocol communication.
	PostgresOID uint32

	// IsSerial indicates if this is an auto-increment type (serial, bigserial, smallserial).
	// Serial types require special handling: they expand to an integer type with
	// a sequence-backed default value.
	IsSerial bool

	// HasModifiers indicates if this type accepts length or precision modifiers.
	// Examples: varchar(n), numeric(p,s), char(n).
	HasModifiers bool

	// FixedLength indicates if this is a fixed-length character type (CHAR).
	// Fixed-length types pad with spaces to the specified length.
	FixedLength int
}

// PostgreSQLTypeAliases maps PostgreSQL type names (lowercase) to their DuckDB mappings.
// This map is used during DDL parsing to resolve PostgreSQL type names to DuckDB types.
var PostgreSQLTypeAliases = map[string]*TypeMapping{
	// Serial types (auto-increment)
	// Serial types do not have their own OIDs; they use the underlying integer type OID.
	// The auto-increment behavior is implemented via sequence + default constraint.
	"serial": {
		PostgreSQLName: "serial",
		DuckDBType:     "INTEGER",
		PostgresOID:    OID_INT4,
		IsSerial:       true,
	},
	"bigserial": {
		PostgreSQLName: "bigserial",
		DuckDBType:     "BIGINT",
		PostgresOID:    OID_INT8,
		IsSerial:       true,
	},
	"smallserial": {
		PostgreSQLName: "smallserial",
		DuckDBType:     "SMALLINT",
		PostgresOID:    OID_INT2,
		IsSerial:       true,
	},
	"serial4": {
		PostgreSQLName: "serial4",
		DuckDBType:     "INTEGER",
		PostgresOID:    OID_INT4,
		IsSerial:       true,
	},
	"serial8": {
		PostgreSQLName: "serial8",
		DuckDBType:     "BIGINT",
		PostgresOID:    OID_INT8,
		IsSerial:       true,
	},
	"serial2": {
		PostgreSQLName: "serial2",
		DuckDBType:     "SMALLINT",
		PostgresOID:    OID_INT2,
		IsSerial:       true,
	},

	// Integer types
	"integer": {
		PostgreSQLName: "integer",
		DuckDBType:     "INTEGER",
		PostgresOID:    OID_INT4,
	},
	"int": {
		PostgreSQLName: "int",
		DuckDBType:     "INTEGER",
		PostgresOID:    OID_INT4,
	},
	"int4": {
		PostgreSQLName: "int4",
		DuckDBType:     "INTEGER",
		PostgresOID:    OID_INT4,
	},
	"bigint": {
		PostgreSQLName: "bigint",
		DuckDBType:     "BIGINT",
		PostgresOID:    OID_INT8,
	},
	"int8": {
		PostgreSQLName: "int8",
		DuckDBType:     "BIGINT",
		PostgresOID:    OID_INT8,
	},
	"smallint": {
		PostgreSQLName: "smallint",
		DuckDBType:     "SMALLINT",
		PostgresOID:    OID_INT2,
	},
	"int2": {
		PostgreSQLName: "int2",
		DuckDBType:     "SMALLINT",
		PostgresOID:    OID_INT2,
	},

	// Floating point types
	"real": {
		PostgreSQLName: "real",
		DuckDBType:     "FLOAT",
		PostgresOID:    OID_FLOAT4,
	},
	"float4": {
		PostgreSQLName: "float4",
		DuckDBType:     "FLOAT",
		PostgresOID:    OID_FLOAT4,
	},
	"double precision": {
		PostgreSQLName: "double precision",
		DuckDBType:     "DOUBLE",
		PostgresOID:    OID_FLOAT8,
	},
	"float8": {
		PostgreSQLName: "float8",
		DuckDBType:     "DOUBLE",
		PostgresOID:    OID_FLOAT8,
	},
	"float": {
		PostgreSQLName: "float",
		DuckDBType:     "DOUBLE",
		PostgresOID:    OID_FLOAT8,
	},

	// Numeric/decimal types
	"numeric": {
		PostgreSQLName: "numeric",
		DuckDBType:     "DECIMAL",
		PostgresOID:    OID_NUMERIC,
		HasModifiers:   true,
	},
	"decimal": {
		PostgreSQLName: "decimal",
		DuckDBType:     "DECIMAL",
		PostgresOID:    OID_NUMERIC,
		HasModifiers:   true,
	},

	// String types
	"text": {
		PostgreSQLName: "text",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_TEXT,
	},
	"varchar": {
		PostgreSQLName: "varchar",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_VARCHAR,
		HasModifiers:   true,
	},
	"character varying": {
		PostgreSQLName: "character varying",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_VARCHAR,
		HasModifiers:   true,
	},
	"char": {
		PostgreSQLName: "char",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_BPCHAR,
		HasModifiers:   true,
		FixedLength:    1, // Default length for CHAR without modifier
	},
	"character": {
		PostgreSQLName: "character",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_BPCHAR,
		HasModifiers:   true,
		FixedLength:    1, // Default length for CHARACTER without modifier
	},
	"bpchar": {
		PostgreSQLName: "bpchar",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_BPCHAR,
	},
	"name": {
		PostgreSQLName: "name",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_NAME,
	},

	// Boolean type
	"boolean": {
		PostgreSQLName: "boolean",
		DuckDBType:     "BOOLEAN",
		PostgresOID:    OID_BOOL,
	},
	"bool": {
		PostgreSQLName: "bool",
		DuckDBType:     "BOOLEAN",
		PostgresOID:    OID_BOOL,
	},

	// Binary type
	"bytea": {
		PostgreSQLName: "bytea",
		DuckDBType:     "BLOB",
		PostgresOID:    OID_BYTEA,
	},

	// Date/Time types
	"date": {
		PostgreSQLName: "date",
		DuckDBType:     "DATE",
		PostgresOID:    OID_DATE,
	},
	"time": {
		PostgreSQLName: "time",
		DuckDBType:     "TIME",
		PostgresOID:    OID_TIME,
	},
	"time without time zone": {
		PostgreSQLName: "time without time zone",
		DuckDBType:     "TIME",
		PostgresOID:    OID_TIME,
	},
	"time with time zone": {
		PostgreSQLName: "time with time zone",
		DuckDBType:     "TIMETZ",
		PostgresOID:    OID_TIMETZ,
	},
	"timetz": {
		PostgreSQLName: "timetz",
		DuckDBType:     "TIMETZ",
		PostgresOID:    OID_TIMETZ,
	},
	"timestamp": {
		PostgreSQLName: "timestamp",
		DuckDBType:     "TIMESTAMP",
		PostgresOID:    OID_TIMESTAMP,
	},
	"timestamp without time zone": {
		PostgreSQLName: "timestamp without time zone",
		DuckDBType:     "TIMESTAMP",
		PostgresOID:    OID_TIMESTAMP,
	},
	"timestamp with time zone": {
		PostgreSQLName: "timestamp with time zone",
		DuckDBType:     "TIMESTAMPTZ",
		PostgresOID:    OID_TIMESTAMPTZ,
	},
	"timestamptz": {
		PostgreSQLName: "timestamptz",
		DuckDBType:     "TIMESTAMPTZ",
		PostgresOID:    OID_TIMESTAMPTZ,
	},
	"interval": {
		PostgreSQLName: "interval",
		DuckDBType:     "INTERVAL",
		PostgresOID:    OID_INTERVAL,
	},

	// JSON types
	"json": {
		PostgreSQLName: "json",
		DuckDBType:     "JSON",
		PostgresOID:    OID_JSON,
	},
	"jsonb": {
		PostgreSQLName: "jsonb",
		DuckDBType:     "JSON",
		PostgresOID:    OID_JSONB,
	},

	// UUID type
	"uuid": {
		PostgreSQLName: "uuid",
		DuckDBType:     "UUID",
		PostgresOID:    OID_UUID,
	},

	// OID type (PostgreSQL internal)
	"oid": {
		PostgreSQLName: "oid",
		DuckDBType:     "UINTEGER",
		PostgresOID:    OID_OID,
	},

	// Bit string types
	"bit": {
		PostgreSQLName: "bit",
		DuckDBType:     "BIT",
		PostgresOID:    OID_BIT,
		HasModifiers:   true,
	},
	"bit varying": {
		PostgreSQLName: "bit varying",
		DuckDBType:     "BIT",
		PostgresOID:    OID_VARBIT,
		HasModifiers:   true,
	},
	"varbit": {
		PostgreSQLName: "varbit",
		DuckDBType:     "BIT",
		PostgresOID:    OID_VARBIT,
		HasModifiers:   true,
	},

	// Money type (maps to DECIMAL for compatibility)
	"money": {
		PostgreSQLName: "money",
		DuckDBType:     "DECIMAL",
		PostgresOID:    OID_MONEY,
	},

	// XML type (maps to VARCHAR)
	"xml": {
		PostgreSQLName: "xml",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_XML,
	},

	// Network types (stored as VARCHAR for compatibility)
	"cidr": {
		PostgreSQLName: "cidr",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_CIDR,
	},
	"inet": {
		PostgreSQLName: "inet",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_INET,
	},
	"macaddr": {
		PostgreSQLName: "macaddr",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_MACADDR,
	},
	"macaddr8": {
		PostgreSQLName: "macaddr8",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_MACADDR8,
	},

	// Geometric types (stored as VARCHAR for compatibility)
	"point": {
		PostgreSQLName: "point",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_POINT,
	},
	"line": {
		PostgreSQLName: "line",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_LINE,
	},
	"lseg": {
		PostgreSQLName: "lseg",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_LSEG,
	},
	"box": {
		PostgreSQLName: "box",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_BOX,
	},
	"path": {
		PostgreSQLName: "path",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_PATH,
	},
	"polygon": {
		PostgreSQLName: "polygon",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_POLYGON,
	},
	"circle": {
		PostgreSQLName: "circle",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_CIRCLE,
	},

	// Text search types (stored as VARCHAR for compatibility)
	"tsvector": {
		PostgreSQLName: "tsvector",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_TSVECTOR,
	},
	"tsquery": {
		PostgreSQLName: "tsquery",
		DuckDBType:     "VARCHAR",
		PostgresOID:    OID_TSQUERY,
	},
}

// GetTypeAlias looks up a PostgreSQL type alias (case-insensitive).
// Returns nil if the type is not found.
func GetTypeAlias(pgType string) *TypeMapping {
	// Normalize to lowercase for lookup
	normalized := strings.ToLower(strings.TrimSpace(pgType))
	return PostgreSQLTypeAliases[normalized]
}

// IsSerialType returns true if the type is a serial (auto-increment) type.
// Serial types include: serial, bigserial, smallserial, serial2, serial4, serial8.
func IsSerialType(pgType string) bool {
	mapping := GetTypeAlias(pgType)
	return mapping != nil && mapping.IsSerial
}

// HasModifiers returns true if the type accepts length or precision modifiers.
// Examples: varchar(n), numeric(p,s), char(n).
func HasModifiers(pgType string) bool {
	mapping := GetTypeAlias(pgType)
	return mapping != nil && mapping.HasModifiers
}

// GetDuckDBType returns the DuckDB type name for a PostgreSQL type.
// Returns an empty string if the type is not found.
func GetDuckDBType(pgType string) string {
	mapping := GetTypeAlias(pgType)
	if mapping == nil {
		return ""
	}
	return mapping.DuckDBType
}

// GetPostgresOID returns the PostgreSQL OID for a type name.
// Returns OID_UNKNOWN if the type is not found.
func GetPostgresOID(pgType string) uint32 {
	mapping := GetTypeAlias(pgType)
	if mapping == nil {
		return OID_UNKNOWN
	}
	return mapping.PostgresOID
}

// AllTypeAliases returns a list of all supported PostgreSQL type names.
func AllTypeAliases() []string {
	result := make([]string, 0, len(PostgreSQLTypeAliases))
	for name := range PostgreSQLTypeAliases {
		result = append(result, name)
	}
	return result
}

// SerialTypes returns a list of all serial (auto-increment) type names.
func SerialTypes() []string {
	return []string{
		"serial",
		"bigserial",
		"smallserial",
		"serial2",
		"serial4",
		"serial8",
	}
}
