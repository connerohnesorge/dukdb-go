// Package types provides PostgreSQL type mapping and OID constants for
// wire protocol compatibility. This package enables dukdb-go to present
// PostgreSQL-compatible type information to clients.
package types

// PostgreSQL type OIDs (from pg_type.oid in PostgreSQL system catalog).
// These constants are used for wire protocol communication and type mapping.
// Reference: https://www.postgresql.org/docs/current/datatype-oid.html
const (
	// Boolean type
	OID_BOOL uint32 = 16 // boolean

	// Binary types
	OID_BYTEA uint32 = 17 // bytea (binary data)

	// Character types (internal)
	OID_CHAR uint32 = 18 // "char" (single byte internal type)
	OID_NAME uint32 = 19 // name (63-byte internal type for identifiers)

	// Integer types
	OID_INT8 uint32 = 20 // bigint (int8)
	OID_INT2 uint32 = 21 // smallint (int2)
	OID_INT4 uint32 = 23 // integer (int4)

	// Text type
	OID_TEXT uint32 = 25 // text (unlimited length)

	// Object identifier type
	OID_OID uint32 = 26 // oid (object identifier)

	// JSON type
	OID_JSON uint32 = 114 // json

	// XML type
	OID_XML uint32 = 142 // xml

	// Geometric types
	OID_POINT   uint32 = 600 // point
	OID_LSEG    uint32 = 601 // lseg (line segment)
	OID_PATH    uint32 = 602 // path
	OID_BOX     uint32 = 603 // box
	OID_POLYGON uint32 = 604 // polygon
	OID_LINE    uint32 = 628 // line

	// Network types
	OID_CIDR uint32 = 650 // cidr (network address)

	// Floating point types
	OID_FLOAT4 uint32 = 700 // real (float4)
	OID_FLOAT8 uint32 = 701 // double precision (float8)

	// Geometric type (circle)
	OID_CIRCLE uint32 = 718 // circle

	// Additional special types
	OID_UNKNOWN  uint32 = 705 // unknown type (unresolved string literal)
	OID_MACADDR8 uint32 = 774 // macaddr8 (EUI-64 MAC address)
	OID_MONEY    uint32 = 790 // money (currency)
	OID_MACADDR  uint32 = 829 // macaddr (MAC address)
	OID_INET     uint32 = 869 // inet (IP address)

	// Array types (internal representation)
	OID_BOOL_ARRAY    uint32 = 1000 // boolean[]
	OID_INT2_ARRAY    uint32 = 1005 // smallint[]
	OID_INT4_ARRAY    uint32 = 1007 // integer[]
	OID_TEXT_ARRAY    uint32 = 1009 // text[]
	OID_VARCHAR_ARRAY uint32 = 1015 // varchar[]
	OID_INT8_ARRAY    uint32 = 1016 // bigint[]
	OID_FLOAT4_ARRAY  uint32 = 1021 // real[]
	OID_FLOAT8_ARRAY  uint32 = 1022 // double precision[]

	// Character types with length
	OID_BPCHAR  uint32 = 1042 // char(n) / character(n) (blank-padded)
	OID_VARCHAR uint32 = 1043 // varchar(n) / character varying(n)

	// Date/Time types
	OID_DATE        uint32 = 1082 // date
	OID_TIME        uint32 = 1083 // time without time zone
	OID_TIMESTAMP   uint32 = 1114 // timestamp without time zone
	OID_TIMESTAMPTZ uint32 = 1184 // timestamp with time zone
	OID_INTERVAL    uint32 = 1186 // interval
	OID_TIMETZ      uint32 = 1266 // time with time zone

	// Bit string types
	OID_BIT    uint32 = 1560 // bit(n)
	OID_VARBIT uint32 = 1562 // bit varying(n)

	// Numeric type
	OID_NUMERIC uint32 = 1700 // numeric(p,s) / decimal(p,s)

	// Special OIDs
	OID_VOID   uint32 = 2278 // void (for functions with no return)
	OID_RECORD uint32 = 2249 // generic record type

	// UUID type
	OID_UUID uint32 = 2950 // uuid

	// Additional array types
	OID_JSON_ARRAY  uint32 = 199  // json[]
	OID_UUID_ARRAY  uint32 = 2951 // uuid[]
	OID_JSONB_ARRAY uint32 = 3807 // jsonb[]

	// Text search types
	OID_TSVECTOR uint32 = 3614 // tsvector
	OID_TSQUERY  uint32 = 3615 // tsquery

	// JSONB type
	OID_JSONB uint32 = 3802 // jsonb (binary JSON)
)

// OIDToName maps PostgreSQL OIDs to their canonical type names.
// This is useful for displaying type names in query results.
var OIDToName = map[uint32]string{
	OID_BOOL:        "boolean",
	OID_BYTEA:       "bytea",
	OID_CHAR:        "\"char\"",
	OID_NAME:        "name",
	OID_INT8:        "bigint",
	OID_INT2:        "smallint",
	OID_INT4:        "integer",
	OID_TEXT:        "text",
	OID_OID:         "oid",
	OID_JSON:        "json",
	OID_XML:         "xml",
	OID_POINT:       "point",
	OID_LSEG:        "lseg",
	OID_PATH:        "path",
	OID_BOX:         "box",
	OID_POLYGON:     "polygon",
	OID_LINE:        "line",
	OID_CIDR:        "cidr",
	OID_FLOAT4:      "real",
	OID_FLOAT8:      "double precision",
	OID_CIRCLE:      "circle",
	OID_UNKNOWN:     "unknown",
	OID_MACADDR8:    "macaddr8",
	OID_MONEY:       "money",
	OID_MACADDR:     "macaddr",
	OID_INET:        "inet",
	OID_BPCHAR:      "character",
	OID_VARCHAR:     "character varying",
	OID_DATE:        "date",
	OID_TIME:        "time without time zone",
	OID_TIMESTAMP:   "timestamp without time zone",
	OID_TIMESTAMPTZ: "timestamp with time zone",
	OID_INTERVAL:    "interval",
	OID_TIMETZ:      "time with time zone",
	OID_BIT:         "bit",
	OID_VARBIT:      "bit varying",
	OID_NUMERIC:     "numeric",
	OID_VOID:        "void",
	OID_RECORD:      "record",
	OID_UUID:        "uuid",
	OID_TSVECTOR:    "tsvector",
	OID_TSQUERY:     "tsquery",
	OID_JSONB:       "jsonb",
	// Array types
	OID_BOOL_ARRAY:    "boolean[]",
	OID_INT2_ARRAY:    "smallint[]",
	OID_INT4_ARRAY:    "integer[]",
	OID_TEXT_ARRAY:    "text[]",
	OID_VARCHAR_ARRAY: "character varying[]",
	OID_INT8_ARRAY:    "bigint[]",
	OID_FLOAT4_ARRAY:  "real[]",
	OID_FLOAT8_ARRAY:  "double precision[]",
	OID_JSON_ARRAY:    "json[]",
	OID_UUID_ARRAY:    "uuid[]",
	OID_JSONB_ARRAY:   "jsonb[]",
}

// ArrayElementOID maps array type OIDs to their element type OIDs.
var ArrayElementOID = map[uint32]uint32{
	OID_BOOL_ARRAY:    OID_BOOL,
	OID_INT2_ARRAY:    OID_INT2,
	OID_INT4_ARRAY:    OID_INT4,
	OID_TEXT_ARRAY:    OID_TEXT,
	OID_VARCHAR_ARRAY: OID_VARCHAR,
	OID_INT8_ARRAY:    OID_INT8,
	OID_FLOAT4_ARRAY:  OID_FLOAT4,
	OID_FLOAT8_ARRAY:  OID_FLOAT8,
	OID_JSON_ARRAY:    OID_JSON,
	OID_UUID_ARRAY:    OID_UUID,
	OID_JSONB_ARRAY:   OID_JSONB,
}

// ElementToArrayOID maps element type OIDs to their array type OIDs.
var ElementToArrayOID = map[uint32]uint32{
	OID_BOOL:    OID_BOOL_ARRAY,
	OID_INT2:    OID_INT2_ARRAY,
	OID_INT4:    OID_INT4_ARRAY,
	OID_TEXT:    OID_TEXT_ARRAY,
	OID_VARCHAR: OID_VARCHAR_ARRAY,
	OID_INT8:    OID_INT8_ARRAY,
	OID_FLOAT4:  OID_FLOAT4_ARRAY,
	OID_FLOAT8:  OID_FLOAT8_ARRAY,
	OID_JSON:    OID_JSON_ARRAY,
	OID_UUID:    OID_UUID_ARRAY,
	OID_JSONB:   OID_JSONB_ARRAY,
}

// IsArrayOID returns true if the given OID represents an array type.
func IsArrayOID(oid uint32) bool {
	_, ok := ArrayElementOID[oid]
	return ok
}

// GetArrayElementOID returns the element OID for an array type OID.
// Returns OID_UNKNOWN if the OID is not an array type.
func GetArrayElementOID(arrayOID uint32) uint32 {
	if elemOID, ok := ArrayElementOID[arrayOID]; ok {
		return elemOID
	}
	return OID_UNKNOWN
}

// GetArrayOID returns the array OID for an element type OID.
// Returns OID_UNKNOWN if no array type exists for the element.
func GetArrayOID(elementOID uint32) uint32 {
	if arrayOID, ok := ElementToArrayOID[elementOID]; ok {
		return arrayOID
	}
	return OID_UNKNOWN
}
