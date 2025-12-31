package dukdb

// Type represents a DuckDB type enumeration.
// This is a pure Go implementation that mirrors the DuckDB type system.
type Type uint8

// DuckDB type constants.
// These values match the DuckDB internal type enumeration (0-36).
const (
	TYPE_INVALID      Type = 0  // Invalid type
	TYPE_BOOLEAN      Type = 1  // Boolean (true/false)
	TYPE_TINYINT      Type = 2  // 8-bit signed integer
	TYPE_SMALLINT     Type = 3  // 16-bit signed integer
	TYPE_INTEGER      Type = 4  // 32-bit signed integer
	TYPE_BIGINT       Type = 5  // 64-bit signed integer
	TYPE_UTINYINT     Type = 6  // 8-bit unsigned integer
	TYPE_USMALLINT    Type = 7  // 16-bit unsigned integer
	TYPE_UINTEGER     Type = 8  // 32-bit unsigned integer
	TYPE_UBIGINT      Type = 9  // 64-bit unsigned integer
	TYPE_FLOAT        Type = 10 // 32-bit floating point
	TYPE_DOUBLE       Type = 11 // 64-bit floating point
	TYPE_TIMESTAMP    Type = 12 // Timestamp (microseconds since epoch)
	TYPE_DATE         Type = 13 // Date (days since epoch)
	TYPE_TIME         Type = 14 // Time (microseconds since midnight)
	TYPE_INTERVAL     Type = 15 // Interval (months, days, microseconds)
	TYPE_HUGEINT      Type = 16 // 128-bit signed integer
	TYPE_UHUGEINT     Type = 17 // 128-bit unsigned integer
	TYPE_VARCHAR      Type = 18 // Variable-length string
	TYPE_BLOB         Type = 19 // Binary large object
	TYPE_DECIMAL      Type = 20 // Fixed-point decimal
	TYPE_TIMESTAMP_S  Type = 21 // Timestamp (seconds precision)
	TYPE_TIMESTAMP_MS Type = 22 // Timestamp (milliseconds precision)
	TYPE_TIMESTAMP_NS Type = 23 // Timestamp (nanoseconds precision)
	TYPE_ENUM         Type = 24 // Enumeration type
	TYPE_LIST         Type = 25 // Variable-length list
	TYPE_STRUCT       Type = 26 // Struct (named fields)
	TYPE_MAP          Type = 27 // Map (key-value pairs)
	TYPE_ARRAY        Type = 28 // Fixed-length array
	TYPE_UUID         Type = 29 // UUID (128-bit)
	TYPE_UNION        Type = 30 // Union type
	TYPE_BIT          Type = 31 // Bit string
	TYPE_TIME_TZ      Type = 32 // Time with time zone
	TYPE_TIMESTAMP_TZ Type = 33 // Timestamp with time zone
	TYPE_ANY          Type = 34 // Any type (for UDFs)
	TYPE_BIGNUM       Type = 35 // Variable-width decimal (internal)
	TYPE_SQLNULL      Type = 36 // SQL NULL type
)

// unsupportedTypeToStringMap lists types not yet fully supported.
// These types cannot be used with NewTypeInfo() except for TYPE_ANY.
var unsupportedTypeToStringMap = map[Type]string{
	TYPE_INVALID: "INVALID",
	TYPE_BIT:     "BIT",
	TYPE_ANY:     "ANY",
	TYPE_BIGNUM:  "BIGNUM",
}

// typeToStringMap maps Type values to their string representations.
// Note: TIME_TZ -> "TIMETZ" and TIMESTAMP_TZ -> "TIMESTAMPTZ" (no underscore)
var typeToStringMap = map[Type]string{
	TYPE_INVALID:      "INVALID",
	TYPE_BOOLEAN:      "BOOLEAN",
	TYPE_TINYINT:      "TINYINT",
	TYPE_SMALLINT:     "SMALLINT",
	TYPE_INTEGER:      "INTEGER",
	TYPE_BIGINT:       "BIGINT",
	TYPE_UTINYINT:     "UTINYINT",
	TYPE_USMALLINT:    "USMALLINT",
	TYPE_UINTEGER:     "UINTEGER",
	TYPE_UBIGINT:      "UBIGINT",
	TYPE_FLOAT:        "FLOAT",
	TYPE_DOUBLE:       "DOUBLE",
	TYPE_TIMESTAMP:    "TIMESTAMP",
	TYPE_DATE:         "DATE",
	TYPE_TIME:         "TIME",
	TYPE_INTERVAL:     "INTERVAL",
	TYPE_HUGEINT:      "HUGEINT",
	TYPE_UHUGEINT:     "UHUGEINT",
	TYPE_VARCHAR:      "VARCHAR",
	TYPE_BLOB:         "BLOB",
	TYPE_DECIMAL:      "DECIMAL",
	TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	TYPE_ENUM:         "ENUM",
	TYPE_LIST:         "LIST",
	TYPE_STRUCT:       "STRUCT",
	TYPE_MAP:          "MAP",
	TYPE_ARRAY:        "ARRAY",
	TYPE_UUID:         "UUID",
	TYPE_UNION:        "UNION",
	TYPE_BIT:          "BIT",
	TYPE_TIME_TZ:      "TIMETZ",
	TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
	TYPE_ANY:          "ANY",
	TYPE_BIGNUM:       "BIGNUM",
	TYPE_SQLNULL:      "SQLNULL",
}

// String returns the string representation of the type.
// Implements fmt.Stringer interface.
func (t Type) String() string {
	if s, ok := typeToStringMap[t]; ok {
		return s
	}
	return "UNKNOWN"
}

// Category returns the category of the type.
// Categories: "numeric", "temporal", "string", "nested", "other"
func (t Type) Category() string {
	switch t {
	// Numeric types
	case TYPE_BOOLEAN,
		TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT,
		TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT,
		TYPE_FLOAT, TYPE_DOUBLE,
		TYPE_HUGEINT, TYPE_UHUGEINT,
		TYPE_DECIMAL, TYPE_BIGNUM:
		return "numeric"

	// Temporal types
	case TYPE_TIMESTAMP,
		TYPE_TIMESTAMP_S,
		TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS,
		TYPE_TIMESTAMP_TZ,
		TYPE_DATE,
		TYPE_TIME,
		TYPE_TIME_TZ,
		TYPE_INTERVAL:
		return "temporal"

	// String types
	case TYPE_VARCHAR, TYPE_BLOB, TYPE_BIT:
		return "string"

	// Nested types
	case TYPE_LIST,
		TYPE_STRUCT,
		TYPE_MAP,
		TYPE_ARRAY,
		TYPE_UNION:
		return "nested"

	// Other types
	default:
		return "other"
	}
}
