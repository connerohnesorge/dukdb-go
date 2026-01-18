package catalog

import (
	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

// pg_type columns - PostgreSQL data type catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-type.html
var pgTypeColumns = []string{
	"oid",            // Row identifier
	"typname",        // Type name
	"typnamespace",   // OID of namespace containing this type
	"typowner",       // Owner of the type
	"typlen",         // For fixed-size types, number of bytes; -1 for varlena
	"typbyval",       // Passed by value
	"typtype",        // b = base, c = composite, d = domain, e = enum, p = pseudo, r = range, m = multirange
	"typcategory",    // Type category (A = array, B = boolean, etc.)
	"typispreferred", // Preferred type in its category
	"typisdefined",   // True if type is defined
	"typdelim",       // Character that separates values when parsing array
	"typrelid",       // For composite types, the pg_class OID
	"typsubscript",   // Subscript handler function OID
	"typelem",        // Array element type OID (0 if not an array)
	"typarray",       // Array type OID for this type (0 if no array type)
	"typinput",       // Input function OID
	"typoutput",      // Output function OID
	"typreceive",     // Receive function OID
	"typsend",        // Send function OID
	"typmodin",       // Type modifier input function OID
	"typmodout",      // Type modifier output function OID
	"typanalyze",     // Custom ANALYZE function OID
	"typalign",       // Alignment requirement
	"typstorage",     // Storage strategy
	"typnotnull",     // For domains, NOT NULL constraint
	"typbasetype",    // For domains, the base type OID
	"typtypmod",      // For domains, typmod of the domain
	"typndims",       // For domains, number of dimensions (0 for non-array)
	"typcollation",   // Collation OID
	"typdefaultbin",  // Default expression (nodeToString representation)
	"typdefault",     // Default expression (pg_get_expr representation)
	"typacl",         // Access privileges
}

// builtinTypes contains the PostgreSQL built-in types to expose.
var builtinTypes = []struct {
	oid         uint32
	typname     string
	typlen      int64
	typbyval    bool
	typtype     string
	typcategory string
	typarray    uint32
	typalign    string
	typstorage  string
}{
	// Boolean
	{types.OID_BOOL, "bool", 1, true, "b", "B", types.OID_BOOL_ARRAY, "c", "p"},
	// Binary
	{types.OID_BYTEA, "bytea", -1, false, "b", "U", 0, "i", "x"},
	// Character types
	{types.OID_CHAR, "char", 1, true, "b", "S", 0, "c", "p"},
	{types.OID_NAME, "name", 64, false, "b", "S", 0, "c", "p"},
	{types.OID_TEXT, "text", -1, false, "b", "S", types.OID_TEXT_ARRAY, "i", "x"},
	{types.OID_VARCHAR, "varchar", -1, false, "b", "S", types.OID_VARCHAR_ARRAY, "i", "x"},
	{types.OID_BPCHAR, "bpchar", -1, false, "b", "S", 0, "i", "x"},
	// Integer types
	{types.OID_INT2, "int2", 2, true, "b", "N", types.OID_INT2_ARRAY, "s", "p"},
	{types.OID_INT4, "int4", 4, true, "b", "N", types.OID_INT4_ARRAY, "i", "p"},
	{types.OID_INT8, "int8", 8, true, "b", "N", types.OID_INT8_ARRAY, "d", "p"},
	// OID type
	{types.OID_OID, "oid", 4, true, "b", "N", 0, "i", "p"},
	// Floating point types
	{types.OID_FLOAT4, "float4", 4, true, "b", "N", types.OID_FLOAT4_ARRAY, "i", "p"},
	{types.OID_FLOAT8, "float8", 8, true, "b", "N", types.OID_FLOAT8_ARRAY, "d", "p"},
	// Numeric
	{types.OID_NUMERIC, "numeric", -1, false, "b", "N", 0, "i", "m"},
	// Date/Time types
	{types.OID_DATE, "date", 4, true, "b", "D", 0, "i", "p"},
	{types.OID_TIME, "time", 8, true, "b", "D", 0, "d", "p"},
	{types.OID_TIMETZ, "timetz", 12, false, "b", "D", 0, "d", "p"},
	{types.OID_TIMESTAMP, "timestamp", 8, true, "b", "D", 0, "d", "p"},
	{types.OID_TIMESTAMPTZ, "timestamptz", 8, true, "b", "D", 0, "d", "p"},
	{types.OID_INTERVAL, "interval", 16, false, "b", "T", 0, "d", "p"},
	// UUID
	{types.OID_UUID, "uuid", 16, false, "b", "U", types.OID_UUID_ARRAY, "c", "p"},
	// JSON types
	{types.OID_JSON, "json", -1, false, "b", "U", types.OID_JSON_ARRAY, "i", "x"},
	{types.OID_JSONB, "jsonb", -1, false, "b", "U", types.OID_JSONB_ARRAY, "i", "x"},
	// Unknown type
	{types.OID_UNKNOWN, "unknown", -2, false, "p", "X", 0, "c", "p"},
	// Void type
	{types.OID_VOID, "void", 4, true, "p", "P", 0, "i", "p"},
	// Record type
	{types.OID_RECORD, "record", -1, false, "p", "P", 0, "d", "x"},
	// Array types
	{types.OID_BOOL_ARRAY, "_bool", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_INT2_ARRAY, "_int2", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_INT4_ARRAY, "_int4", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_INT8_ARRAY, "_int8", -1, false, "b", "A", 0, "d", "x"},
	{types.OID_TEXT_ARRAY, "_text", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_VARCHAR_ARRAY, "_varchar", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_FLOAT4_ARRAY, "_float4", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_FLOAT8_ARRAY, "_float8", -1, false, "b", "A", 0, "d", "x"},
	{types.OID_JSON_ARRAY, "_json", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_UUID_ARRAY, "_uuid", -1, false, "b", "A", 0, "i", "x"},
	{types.OID_JSONB_ARRAY, "_jsonb", -1, false, "b", "A", 0, "i", "x"},
}

// queryPgType returns data for pg_catalog.pg_type.
func (pg *PgCatalog) queryPgType(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgTypeColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, t := range builtinTypes {
		// Determine element type for arrays
		var typelem int64
		if t.typcategory == "A" {
			// For array types, get the element type
			if elem, ok := types.ArrayElementOID[t.oid]; ok {
				typelem = int64(elem)
			}
		}

		row := map[string]any{
			"oid":          int64(t.oid),
			"typname":      t.typname,
			"typnamespace": pgCatalogNamespaceOID,
			"typowner":     int64(10), // Superuser
			"typlen":       t.typlen,
			"typbyval":     t.typbyval,
			"typtype":      t.typtype,
			"typcategory":  t.typcategory,
			"typispreferred": t.typcategory == "S" &&
				t.typname == "text", // text is preferred string
			"typisdefined":  true,
			"typdelim":      ",",
			"typrelid":      int64(0),
			"typsubscript":  int64(0),
			"typelem":       typelem,
			"typarray":      int64(t.typarray),
			"typinput":      int64(0), // Would need function OIDs
			"typoutput":     int64(0),
			"typreceive":    int64(0),
			"typsend":       int64(0),
			"typmodin":      int64(0),
			"typmodout":     int64(0),
			"typanalyze":    int64(0),
			"typalign":      t.typalign,
			"typstorage":    t.typstorage,
			"typnotnull":    false,
			"typbasetype":   int64(0),
			"typtypmod":     int64(-1),
			"typndims":      int64(0),
			"typcollation":  int64(0),
			"typdefaultbin": nil,
			"typdefault":    nil,
			"typacl":        nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
