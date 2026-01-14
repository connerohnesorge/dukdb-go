# Type Mapping System for PostgreSQL Compatibility

## Overview

This specification defines the bidirectional type mapping system for PostgreSQL compatibility mode in dukdb-go. The type mapping system serves two critical purposes:

1. **DDL/Query Parsing**: Map PostgreSQL type names (e.g., `serial`, `text`, `bytea`) to DuckDB internal types during SQL parsing
2. **Wire Protocol Response**: Map DuckDB types back to PostgreSQL OIDs when sending RowDescription messages over the wire protocol

## Design Goals

- Zero ambiguity in type resolution
- Complete coverage of common PostgreSQL types
- Efficient bidirectional lookup (O(1) both directions)
- Integration with psql-wire library expectations
- Support for type modifiers (precision, scale, length)

---

## Type Mapping Table

### Core Type Mappings

| PostgreSQL Type | PostgreSQL OID | DuckDB Type | DuckDB Type ID | Notes |
|-----------------|----------------|-------------|----------------|-------|
| `boolean` / `bool` | 16 | BOOLEAN | TYPE_BOOLEAN (1) | |
| `smallint` / `int2` | 21 | SMALLINT | TYPE_SMALLINT (3) | |
| `integer` / `int` / `int4` | 23 | INTEGER | TYPE_INTEGER (4) | |
| `bigint` / `int8` | 20 | BIGINT | TYPE_BIGINT (5) | |
| `real` / `float4` | 700 | FLOAT | TYPE_FLOAT (10) | |
| `double precision` / `float8` | 701 | DOUBLE | TYPE_DOUBLE (11) | |
| `numeric` / `decimal` | 1700 | DECIMAL | TYPE_DECIMAL (20) | With precision/scale |
| `text` | 25 | VARCHAR | TYPE_VARCHAR (18) | Unlimited length |
| `varchar` / `character varying` | 1043 | VARCHAR | TYPE_VARCHAR (18) | With optional length |
| `char` / `character` | 1042 | VARCHAR | TYPE_VARCHAR (18) | Fixed length (pad with spaces) |
| `bytea` | 17 | BLOB | TYPE_BLOB (19) | |
| `date` | 1082 | DATE | TYPE_DATE (13) | |
| `time` / `time without time zone` | 1083 | TIME | TYPE_TIME (14) | |
| `timetz` / `time with time zone` | 1266 | TIMETZ | TYPE_TIME_TZ (32) | |
| `timestamp` / `timestamp without time zone` | 1114 | TIMESTAMP | TYPE_TIMESTAMP (12) | |
| `timestamptz` / `timestamp with time zone` | 1184 | TIMESTAMPTZ | TYPE_TIMESTAMP_TZ (33) | Store as UTC |
| `interval` | 1186 | INTERVAL | TYPE_INTERVAL (15) | |
| `uuid` | 2950 | UUID | TYPE_UUID (29) | |
| `json` | 114 | JSON | TYPE_JSON (37) | |
| `jsonb` | 3802 | JSON | TYPE_JSON (37) | Binary JSON stored as text |

### Auto-Increment Types (Serial)

Serial types require special handling during DDL parsing. They expand to an integer type with a sequence-backed default value.

| PostgreSQL Type | PostgreSQL OID | DuckDB Type | DuckDB Type ID | Auto-Increment |
|-----------------|----------------|-------------|----------------|----------------|
| `smallserial` / `serial2` | 21 | SMALLINT | TYPE_SMALLINT (3) | Yes |
| `serial` / `serial4` | 23 | INTEGER | TYPE_INTEGER (4) | Yes |
| `bigserial` / `serial8` | 20 | BIGINT | TYPE_BIGINT (5) | Yes |

**Implementation Note**: Serial types do not have their own OIDs. The column appears as the underlying integer type in metadata. The auto-increment behavior is implemented via sequence + default constraint.

### Array Types

| PostgreSQL Type | PostgreSQL OID | DuckDB Type | DuckDB Type ID | Notes |
|-----------------|----------------|-------------|----------------|-------|
| `_bool` / `boolean[]` | 1000 | LIST(BOOLEAN) | TYPE_LIST (25) | |
| `_int2` / `smallint[]` | 1005 | LIST(SMALLINT) | TYPE_LIST (25) | |
| `_int4` / `integer[]` | 1007 | LIST(INTEGER) | TYPE_LIST (25) | |
| `_int8` / `bigint[]` | 1016 | LIST(BIGINT) | TYPE_LIST (25) | |
| `_float4` / `real[]` | 1021 | LIST(FLOAT) | TYPE_LIST (25) | |
| `_float8` / `double precision[]` | 1022 | LIST(DOUBLE) | TYPE_LIST (25) | |
| `_text` / `text[]` | 1009 | LIST(VARCHAR) | TYPE_LIST (25) | |
| `_varchar` / `varchar[]` | 1015 | LIST(VARCHAR) | TYPE_LIST (25) | |
| `_uuid` / `uuid[]` | 2951 | LIST(UUID) | TYPE_LIST (25) | |
| `_json` / `json[]` | 199 | LIST(JSON) | TYPE_LIST (25) | |
| `_jsonb` / `jsonb[]` | 3807 | LIST(JSON) | TYPE_LIST (25) | |

**Note**: PostgreSQL arrays map to DuckDB LIST type. DuckDB does not distinguish between arrays and lists at the type level.

### Geometric Types (Limited Support)

| PostgreSQL Type | PostgreSQL OID | DuckDB Type | DuckDB Type ID | Notes |
|-----------------|----------------|-------------|----------------|-------|
| `point` | 600 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |
| `line` | 628 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |
| `polygon` | 604 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |
| `box` | 603 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |
| `path` | 602 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |
| `circle` | 718 | VARCHAR | TYPE_VARCHAR (18) | Stored as text representation |

**Note**: Full geometry support would use TYPE_GEOMETRY, but for PostgreSQL compatibility we store as text.

### Special PostgreSQL Types (Not Directly Supported)

| PostgreSQL Type | PostgreSQL OID | DuckDB Fallback | Notes |
|-----------------|----------------|-----------------|-------|
| `money` | 790 | DECIMAL(19,2) | Currency type |
| `xml` | 142 | VARCHAR | XML document |
| `cidr` | 650 | VARCHAR | Network address |
| `inet` | 869 | VARCHAR | Network address |
| `macaddr` | 829 | VARCHAR | MAC address |
| `macaddr8` | 774 | VARCHAR | MAC address (EUI-64) |
| `bit` | 1560 | BIT | Fixed-length bit string |
| `varbit` / `bit varying` | 1562 | BIT | Variable-length bit string |
| `tsvector` | 3614 | VARCHAR | Text search vector |
| `tsquery` | 3615 | VARCHAR | Text search query |

---

## PostgreSQL OID Constants

The following OID constants are required for wire protocol communication.

```go
// PostgreSQL base type OIDs
const (
    OID_BOOL        uint32 = 16    // boolean
    OID_BYTEA       uint32 = 17    // bytea
    OID_CHAR        uint32 = 18    // "char" (single byte internal type)
    OID_INT8        uint32 = 20    // bigint
    OID_INT2        uint32 = 21    // smallint
    OID_INT4        uint32 = 23    // integer
    OID_TEXT        uint32 = 25    // text
    OID_OID         uint32 = 26    // oid
    OID_JSON        uint32 = 114   // json
    OID_XML         uint32 = 142   // xml
    OID_POINT       uint32 = 600   // point
    OID_LSEG        uint32 = 601   // lseg
    OID_PATH        uint32 = 602   // path
    OID_BOX         uint32 = 603   // box
    OID_POLYGON     uint32 = 604   // polygon
    OID_LINE        uint32 = 628   // line
    OID_CIDR        uint32 = 650   // cidr
    OID_FLOAT4      uint32 = 700   // real
    OID_FLOAT8      uint32 = 701   // double precision
    OID_CIRCLE      uint32 = 718   // circle
    OID_MACADDR8    uint32 = 774   // macaddr8
    OID_MONEY       uint32 = 790   // money
    OID_MACADDR     uint32 = 829   // macaddr
    OID_INET        uint32 = 869   // inet
    OID_BPCHAR      uint32 = 1042  // char(n) / character(n)
    OID_VARCHAR     uint32 = 1043  // varchar(n) / character varying(n)
    OID_DATE        uint32 = 1082  // date
    OID_TIME        uint32 = 1083  // time without time zone
    OID_TIMESTAMP   uint32 = 1114  // timestamp without time zone
    OID_TIMESTAMPTZ uint32 = 1184  // timestamp with time zone
    OID_INTERVAL    uint32 = 1186  // interval
    OID_TIMETZ      uint32 = 1266  // time with time zone
    OID_BIT         uint32 = 1560  // bit(n)
    OID_VARBIT      uint32 = 1562  // bit varying(n)
    OID_NUMERIC     uint32 = 1700  // numeric(p,s)
    OID_UUID        uint32 = 2950  // uuid
    OID_JSONB       uint32 = 3802  // jsonb

    // Array type OIDs
    OID_BOOL_ARRAY    uint32 = 1000  // boolean[]
    OID_INT2_ARRAY    uint32 = 1005  // smallint[]
    OID_INT4_ARRAY    uint32 = 1007  // integer[]
    OID_TEXT_ARRAY    uint32 = 1009  // text[]
    OID_VARCHAR_ARRAY uint32 = 1015  // varchar[]
    OID_INT8_ARRAY    uint32 = 1016  // bigint[]
    OID_FLOAT4_ARRAY  uint32 = 1021  // real[]
    OID_FLOAT8_ARRAY  uint32 = 1022  // double precision[]
    OID_JSON_ARRAY    uint32 = 199   // json[]
    OID_UUID_ARRAY    uint32 = 2951  // uuid[]
    OID_JSONB_ARRAY   uint32 = 3807  // jsonb[]

    // Special OIDs
    OID_UNKNOWN uint32 = 705  // unknown type (unresolved string literal)
    OID_VOID    uint32 = 2278 // void (for functions with no return)
    OID_RECORD  uint32 = 2249 // generic record type
)
```

---

## Parser Integration Design

### Type Alias Resolution

During SQL parsing, PostgreSQL type names must be resolved to DuckDB types. This happens in the parser's type resolution phase.

```go
// TypeAliasMap maps PostgreSQL type names (lowercase) to DuckDB types.
// Used during DDL parsing (CREATE TABLE, etc.)
var TypeAliasMap = map[string]TypeMapping{
    // Boolean
    "boolean":                  {DuckDBType: TYPE_BOOLEAN, IsAlias: false},
    "bool":                     {DuckDBType: TYPE_BOOLEAN, IsAlias: true},

    // Integers
    "smallint":                 {DuckDBType: TYPE_SMALLINT, IsAlias: false},
    "int2":                     {DuckDBType: TYPE_SMALLINT, IsAlias: true},
    "integer":                  {DuckDBType: TYPE_INTEGER, IsAlias: false},
    "int":                      {DuckDBType: TYPE_INTEGER, IsAlias: true},
    "int4":                     {DuckDBType: TYPE_INTEGER, IsAlias: true},
    "bigint":                   {DuckDBType: TYPE_BIGINT, IsAlias: false},
    "int8":                     {DuckDBType: TYPE_BIGINT, IsAlias: true},

    // Serial (auto-increment)
    "smallserial":              {DuckDBType: TYPE_SMALLINT, IsAlias: true, IsSerial: true},
    "serial2":                  {DuckDBType: TYPE_SMALLINT, IsAlias: true, IsSerial: true},
    "serial":                   {DuckDBType: TYPE_INTEGER, IsAlias: true, IsSerial: true},
    "serial4":                  {DuckDBType: TYPE_INTEGER, IsAlias: true, IsSerial: true},
    "bigserial":                {DuckDBType: TYPE_BIGINT, IsAlias: true, IsSerial: true},
    "serial8":                  {DuckDBType: TYPE_BIGINT, IsAlias: true, IsSerial: true},

    // Floating point
    "real":                     {DuckDBType: TYPE_FLOAT, IsAlias: false},
    "float4":                   {DuckDBType: TYPE_FLOAT, IsAlias: true},
    "double precision":         {DuckDBType: TYPE_DOUBLE, IsAlias: false},
    "float8":                   {DuckDBType: TYPE_DOUBLE, IsAlias: true},

    // Numeric
    "numeric":                  {DuckDBType: TYPE_DECIMAL, IsAlias: false, HasModifiers: true},
    "decimal":                  {DuckDBType: TYPE_DECIMAL, IsAlias: true, HasModifiers: true},

    // Character types
    "text":                     {DuckDBType: TYPE_VARCHAR, IsAlias: true},
    "varchar":                  {DuckDBType: TYPE_VARCHAR, IsAlias: true, HasModifiers: true},
    "character varying":        {DuckDBType: TYPE_VARCHAR, IsAlias: false, HasModifiers: true},
    "char":                     {DuckDBType: TYPE_VARCHAR, IsAlias: true, HasModifiers: true, FixedLength: true},
    "character":                {DuckDBType: TYPE_VARCHAR, IsAlias: false, HasModifiers: true, FixedLength: true},
    "bpchar":                   {DuckDBType: TYPE_VARCHAR, IsAlias: true, HasModifiers: true, FixedLength: true},

    // Binary
    "bytea":                    {DuckDBType: TYPE_BLOB, IsAlias: true},

    // Date/Time
    "date":                     {DuckDBType: TYPE_DATE, IsAlias: false},
    "time":                     {DuckDBType: TYPE_TIME, IsAlias: false},
    "time without time zone":   {DuckDBType: TYPE_TIME, IsAlias: false},
    "timetz":                   {DuckDBType: TYPE_TIME_TZ, IsAlias: true},
    "time with time zone":      {DuckDBType: TYPE_TIME_TZ, IsAlias: false},
    "timestamp":                {DuckDBType: TYPE_TIMESTAMP, IsAlias: false},
    "timestamp without time zone": {DuckDBType: TYPE_TIMESTAMP, IsAlias: false},
    "timestamptz":              {DuckDBType: TYPE_TIMESTAMP_TZ, IsAlias: true},
    "timestamp with time zone": {DuckDBType: TYPE_TIMESTAMP_TZ, IsAlias: false},
    "interval":                 {DuckDBType: TYPE_INTERVAL, IsAlias: false},

    // UUID
    "uuid":                     {DuckDBType: TYPE_UUID, IsAlias: false},

    // JSON
    "json":                     {DuckDBType: TYPE_JSON, IsAlias: false},
    "jsonb":                    {DuckDBType: TYPE_JSON, IsAlias: true}, // JSONB -> JSON (no binary storage optimization)
}
```

### TypeMapping Structure

```go
// TypeMapping represents the mapping from a PostgreSQL type name to DuckDB.
type TypeMapping struct {
    DuckDBType   Type   // The DuckDB type
    IsAlias      bool   // True if this is an alias (e.g., "int4" for "integer")
    IsSerial     bool   // True for auto-increment types (serial, bigserial)
    HasModifiers bool   // True if type accepts modifiers like (n) or (p,s)
    FixedLength  bool   // True for CHAR (pad with spaces)
}
```

### Serial Type Handling

When a serial type is encountered during CREATE TABLE parsing:

1. Replace type with underlying integer type (SMALLINT, INTEGER, BIGINT)
2. Create implicit sequence: `<table>_<column>_seq`
3. Add DEFAULT constraint: `nextval('<table>_<column>_seq'::regclass)`
4. Add NOT NULL constraint (serial columns are NOT NULL by default)

```sql
-- PostgreSQL input:
CREATE TABLE users (id serial PRIMARY KEY, name text);

-- Transformed to:
CREATE SEQUENCE users_id_seq;
CREATE TABLE users (
    id INTEGER NOT NULL DEFAULT nextval('users_id_seq'),
    name VARCHAR,
    PRIMARY KEY (id)
);
```

---

## Wire Protocol Integration

### RowDescription Message

When sending query results over the wire protocol, each column requires type information in the RowDescription message:

```go
// FieldDescription for wire protocol (matches psql-wire structure)
type FieldDescription struct {
    Name         string // Column name
    TableOID     uint32 // OID of source table (0 if not from table)
    ColumnIndex  uint16 // Column number in source table (0 if not from table)
    TypeOID      uint32 // PostgreSQL type OID
    TypeSize     int16  // Type size (-1 for variable)
    TypeModifier int32  // Type-specific modifier (e.g., varchar length)
    Format       int16  // 0 = text, 1 = binary
}
```

### DuckDB to PostgreSQL OID Mapping

```go
// DuckDBTypeToPostgresOID maps DuckDB types to PostgreSQL OIDs for wire protocol.
var DuckDBTypeToPostgresOID = map[Type]uint32{
    TYPE_BOOLEAN:      OID_BOOL,
    TYPE_TINYINT:      OID_INT2,      // No int1 in PostgreSQL, use int2
    TYPE_SMALLINT:     OID_INT2,
    TYPE_INTEGER:      OID_INT4,
    TYPE_BIGINT:       OID_INT8,
    TYPE_UTINYINT:     OID_INT2,      // Unsigned not supported, use signed
    TYPE_USMALLINT:    OID_INT4,      // Promote to avoid overflow
    TYPE_UINTEGER:     OID_INT8,      // Promote to avoid overflow
    TYPE_UBIGINT:      OID_NUMERIC,   // Use numeric for uint64 range
    TYPE_FLOAT:        OID_FLOAT4,
    TYPE_DOUBLE:       OID_FLOAT8,
    TYPE_TIMESTAMP:    OID_TIMESTAMP,
    TYPE_TIMESTAMP_TZ: OID_TIMESTAMPTZ,
    TYPE_TIMESTAMP_S:  OID_TIMESTAMP,
    TYPE_TIMESTAMP_MS: OID_TIMESTAMP,
    TYPE_TIMESTAMP_NS: OID_TIMESTAMP,
    TYPE_DATE:         OID_DATE,
    TYPE_TIME:         OID_TIME,
    TYPE_TIME_TZ:      OID_TIMETZ,
    TYPE_INTERVAL:     OID_INTERVAL,
    TYPE_HUGEINT:      OID_NUMERIC,   // 128-bit int as numeric
    TYPE_UHUGEINT:     OID_NUMERIC,   // 128-bit uint as numeric
    TYPE_VARCHAR:      OID_TEXT,      // Use TEXT for unbounded, VARCHAR for bounded
    TYPE_BLOB:         OID_BYTEA,
    TYPE_DECIMAL:      OID_NUMERIC,
    TYPE_UUID:         OID_UUID,
    TYPE_JSON:         OID_JSONB,     // Prefer JSONB OID for better client support
    TYPE_LIST:         OID_UNKNOWN,   // Complex: need element type
    TYPE_STRUCT:       OID_RECORD,
    TYPE_MAP:          OID_JSONB,     // Map serialized as JSONB
    TYPE_ARRAY:        OID_UNKNOWN,   // Complex: need element type
    TYPE_UNION:        OID_RECORD,
    TYPE_BIT:          OID_VARBIT,
    TYPE_ENUM:         OID_TEXT,      // Enum as text for compatibility
    TYPE_GEOMETRY:     OID_TEXT,      // Geometry as WKT text
    TYPE_ANY:          OID_UNKNOWN,
    TYPE_SQLNULL:      OID_UNKNOWN,
    TYPE_BIGNUM:       OID_NUMERIC,
    TYPE_INVALID:      OID_UNKNOWN,
}
```

### Type Size Information

```go
// PostgresTypeSize returns the type size for wire protocol.
// Returns -1 for variable-length types, fixed size otherwise.
func PostgresTypeSize(oid uint32) int16 {
    switch oid {
    case OID_BOOL:
        return 1
    case OID_INT2:
        return 2
    case OID_INT4:
        return 4
    case OID_INT8:
        return 8
    case OID_FLOAT4:
        return 4
    case OID_FLOAT8:
        return 8
    case OID_DATE:
        return 4
    case OID_TIME, OID_TIMETZ:
        return 8
    case OID_TIMESTAMP, OID_TIMESTAMPTZ:
        return 8
    case OID_UUID:
        return 16
    case OID_INTERVAL:
        return 16
    default:
        return -1 // Variable length
    }
}
```

---

## Type Conversion

### Text Format Conversion

PostgreSQL wire protocol primarily uses text format for data exchange. Each DuckDB value must be converted to/from PostgreSQL text representation.

```go
// Encode functions: DuckDB value -> PostgreSQL text format
func EncodeBool(v bool) []byte
func EncodeInt16(v int16) []byte
func EncodeInt32(v int32) []byte
func EncodeInt64(v int64) []byte
func EncodeFloat32(v float32) []byte
func EncodeFloat64(v float64) []byte
func EncodeString(v string) []byte
func EncodeBytes(v []byte) []byte        // Hex encoding with \x prefix
func EncodeDate(v time.Time) []byte      // YYYY-MM-DD
func EncodeTime(v time.Duration) []byte  // HH:MM:SS.ffffff
func EncodeTimestamp(v time.Time) []byte // YYYY-MM-DD HH:MM:SS.ffffff
func EncodeInterval(v Interval) []byte   // PostgreSQL interval format
func EncodeUUID(v [16]byte) []byte       // Hyphenated format
func EncodeDecimal(v Decimal) []byte     // Numeric string
func EncodeJSON(v any) []byte            // JSON string

// Decode functions: PostgreSQL text format -> DuckDB value
func DecodeBool(b []byte) (bool, error)
func DecodeInt16(b []byte) (int16, error)
func DecodeInt32(b []byte) (int32, error)
func DecodeInt64(b []byte) (int64, error)
func DecodeFloat32(b []byte) (float32, error)
func DecodeFloat64(b []byte) (float64, error)
func DecodeString(b []byte) (string, error)
func DecodeBytes(b []byte) ([]byte, error)        // Hex or escape format
func DecodeDate(b []byte) (time.Time, error)
func DecodeTime(b []byte) (time.Duration, error)
func DecodeTimestamp(b []byte) (time.Time, error)
func DecodeInterval(b []byte) (Interval, error)
func DecodeUUID(b []byte) ([16]byte, error)
func DecodeDecimal(b []byte) (Decimal, error)
func DecodeJSON(b []byte) (any, error)
```

### Binary Format (Optional)

Binary format is more efficient but requires careful handling. Initially, we will support text format only. Binary format can be added later for performance optimization with specific clients.

### NULL Handling

NULL values are handled at the protocol level, not in type conversion:
- In RowData messages, NULL is indicated by a -1 length prefix
- Type conversion functions do not receive NULL values

---

## Implementation Plan

### File Structure

```
internal/postgres/
    types/
        oid.go          # PostgreSQL OID constants
        mapping.go      # Type mapping tables and lookup functions
        conversion.go   # Text/binary format conversion
        serial.go       # Serial type handling
    wire/
        field.go        # FieldDescription for wire protocol
        encode.go       # Value encoding for wire protocol
        decode.go       # Value decoding for wire protocol
```

### Key Interfaces

```go
// TypeMapper provides bidirectional type mapping.
type TypeMapper interface {
    // PostgreSQLToDuckDB resolves a PostgreSQL type name to DuckDB type.
    // Returns the DuckDB type, modifiers, and whether it's a serial type.
    PostgreSQLToDuckDB(pgType string) (dukdb.Type, *TypeModifiers, bool, error)

    // DuckDBToPostgreSQL returns the PostgreSQL OID for a DuckDB type.
    DuckDBToPostgreSQL(duckType dukdb.Type) uint32

    // GetTypeSize returns the PostgreSQL type size for wire protocol.
    GetTypeSize(oid uint32) int16

    // IsArrayType returns true if the OID represents an array type.
    IsArrayType(oid uint32) bool

    // GetArrayElementOID returns the element OID for an array type.
    GetArrayElementOID(arrayOID uint32) uint32
}

// TypeModifiers holds type-specific parameters.
type TypeModifiers struct {
    Length    int32 // For VARCHAR(n), CHAR(n)
    Precision int32 // For NUMERIC(p,s)
    Scale     int32 // For NUMERIC(p,s)
}

// TypeConverter handles value conversion between PostgreSQL and DuckDB formats.
type TypeConverter interface {
    // EncodeText converts a DuckDB value to PostgreSQL text format.
    EncodeText(value any, duckType dukdb.Type) ([]byte, error)

    // DecodeText converts PostgreSQL text format to a DuckDB value.
    DecodeText(data []byte, pgOID uint32) (any, error)

    // EncodeBinary converts a DuckDB value to PostgreSQL binary format.
    EncodeBinary(value any, duckType dukdb.Type) ([]byte, error)

    // DecodeBinary converts PostgreSQL binary format to a DuckDB value.
    DecodeBinary(data []byte, pgOID uint32) (any, error)
}
```

### Implementation Order

1. **Phase 1: OID Constants and Basic Mapping**
   - Define all PostgreSQL OID constants
   - Implement TypeAliasMap for parser integration
   - Implement DuckDBTypeToPostgresOID for wire protocol
   - Unit tests for mapping accuracy

2. **Phase 2: Parser Integration**
   - Integrate type alias resolution into parser
   - Implement serial type expansion
   - Handle type modifiers (length, precision, scale)
   - Parser tests with PostgreSQL DDL

3. **Phase 3: Wire Protocol Integration**
   - Implement FieldDescription builder
   - Integrate with psql-wire RowDescription
   - Handle NULL values correctly
   - Integration tests with psql client

4. **Phase 4: Value Conversion**
   - Implement text format encode/decode for all types
   - Handle edge cases (infinity, NaN, special dates)
   - Implement bytea hex encoding
   - Add interval format support
   - Comprehensive conversion tests

5. **Phase 5: Advanced Features (Optional)**
   - Binary format support for performance
   - Array type handling
   - Composite type (STRUCT) support
   - Custom type registration

---

## Testing Strategy

### Unit Tests

1. **Type Mapping Tests**
   - Every PostgreSQL type name resolves correctly
   - Every DuckDB type maps to correct OID
   - Bidirectional mapping consistency

2. **Conversion Tests**
   - Round-trip: DuckDB -> PostgreSQL text -> DuckDB
   - Edge cases: NULL, infinity, empty strings, max values
   - Bytea encoding: hex format, escape format

3. **Serial Tests**
   - Serial type expansion generates correct sequence
   - NOT NULL constraint added automatically
   - DEFAULT nextval constraint added

### Integration Tests

1. **psql Client**
   - Connect with psql and verify type display
   - Run DESCRIBE/\d commands
   - Insert and select all data types

2. **pgx Driver**
   - Use Go pgx to connect and query
   - Verify type scanning works correctly
   - Test prepared statements with typed parameters

3. **ORM Tests**
   - Basic GORM model mapping
   - SQLAlchemy schema reflection
   - TypeORM entity definition

---

## References

- [PostgreSQL Type OIDs](https://www.postgresql.org/docs/current/datatype-oid.html)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [PostgreSQL Wire Protocol Types](https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES)
- [psql-wire Type Handling](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire)
- [pgproto3 Types](https://pkg.go.dev/github.com/jackc/pgx/v5/pgproto3)
