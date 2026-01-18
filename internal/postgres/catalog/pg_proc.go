package catalog

// pg_proc columns - PostgreSQL procedure/function catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-proc.html
var pgProcColumns = []string{
	"oid",             // Row identifier
	"proname",         // Name of the function
	"pronamespace",    // OID of namespace containing this function
	"proowner",        // Owner of the function
	"prolang",         // Language of the function
	"procost",         // Estimated execution cost
	"prorows",         // Estimated number of result rows (0 for non-SRF)
	"provariadic",     // Data type of variadic array parameter
	"prosupport",      // Planner support function
	"prokind",         // Function kind: f = ordinary function, p = procedure, a = aggregate, w = window
	"prosecdef",       // Function is a security definer
	"proleakproof",    // Function has no side effects
	"proisstrict",     // Function returns null on null input
	"proretset",       // Function returns a set
	"provolatile",     // Volatility (i = immutable, s = stable, v = volatile)
	"proparallel",     // Parallel safety (s = safe, r = restricted, u = unsafe)
	"pronargs",        // Number of input arguments
	"pronargdefaults", // Number of arguments with defaults
	"prorettype",      // Return data type OID
	"proargtypes",     // Argument types (as an OID vector)
	"proallargtypes",  // All argument types (including OUT and INOUT)
	"proargmodes",     // Modes of arguments (i = IN, o = OUT, b = INOUT, v = VARIADIC, t = TABLE)
	"proargnames",     // Names of arguments
	"proargdefaults",  // Default expressions
	"protrftypes",     // Types for which to apply transforms
	"prosrc",          // Function source code or info
	"probin",          // Additional info about function
	"prosqlbody",      // SQL body definition
	"proconfig",       // Configuration parameter settings
	"proacl",          // Access privileges
}

// builtinFunctions contains commonly used PostgreSQL functions.
var builtinFunctions = []struct {
	oid        int64
	proname    string
	prokind    string
	pronargs   int64
	prorettype int64
	prosrc     string
}{
	// String functions
	{1, "length", "f", 1, 23, "length"}, // int4
	{2, "lower", "f", 1, 25, "lower"},   // text
	{3, "upper", "f", 1, 25, "upper"},
	{4, "concat", "f", -1, 25, "concat"}, // variadic
	{5, "substring", "f", 3, 25, "substring"},
	{6, "replace", "f", 3, 25, "replace"},
	{7, "trim", "f", 1, 25, "trim"},
	{8, "ltrim", "f", 1, 25, "ltrim"},
	{9, "rtrim", "f", 1, 25, "rtrim"},
	{10, "split_part", "f", 3, 25, "split_part"},

	// Date/time functions
	{100, "now", "f", 0, 1184, "now"}, // timestamptz
	{101, "current_timestamp", "f", 0, 1184, "current_timestamp"},
	{102, "current_date", "f", 0, 1082, "current_date"}, // date
	{103, "current_time", "f", 0, 1266, "current_time"}, // timetz
	{104, "date_part", "f", 2, 701, "date_part"},        // float8
	{105, "extract", "f", 2, 701, "extract"},
	{106, "age", "f", 2, 1186, "age"}, // interval
	{107, "date_trunc", "f", 2, 1184, "date_trunc"},

	// Math functions
	{200, "abs", "f", 1, 701, "abs"},
	{201, "round", "f", 1, 1700, "round"}, // numeric
	{202, "ceil", "f", 1, 701, "ceil"},
	{203, "floor", "f", 1, 701, "floor"},
	{204, "sqrt", "f", 1, 701, "sqrt"},
	{205, "power", "f", 2, 701, "power"},
	{206, "mod", "f", 2, 23, "mod"},
	{207, "random", "f", 0, 701, "random"},

	// Aggregate functions
	{300, "count", "a", 1, 20, "count"}, // int8
	{301, "sum", "a", 1, 1700, "sum"},   // numeric
	{302, "avg", "a", 1, 1700, "avg"},
	{303, "min", "a", 1, 0, "min"}, // any type
	{304, "max", "a", 1, 0, "max"},
	{305, "array_agg", "a", 1, 0, "array_agg"},
	{306, "string_agg", "a", 2, 25, "string_agg"},

	// Type casting/conversion
	{400, "to_char", "f", 2, 25, "to_char"},
	{401, "to_number", "f", 2, 1700, "to_number"},
	{402, "to_date", "f", 2, 1082, "to_date"},
	{403, "to_timestamp", "f", 2, 1184, "to_timestamp"},
	{404, "cast", "f", 1, 0, "cast"},

	// JSON functions
	{500, "json_build_object", "f", -1, 114, "json_build_object"},
	{501, "json_agg", "a", 1, 114, "json_agg"},
	{502, "jsonb_build_object", "f", -1, 3802, "jsonb_build_object"},

	// Utility functions
	{600, "coalesce", "f", -1, 0, "coalesce"},
	{601, "nullif", "f", 2, 0, "nullif"},
	{602, "greatest", "f", -1, 0, "greatest"},
	{603, "least", "f", -1, 0, "least"},

	// System information
	{700, "current_user", "f", 0, 19, "current_user"}, // name
	{701, "current_database", "f", 0, 19, "current_database"},
	{702, "current_schema", "f", 0, 19, "current_schema"},
	{703, "version", "f", 0, 25, "version"},
	{704, "pg_typeof", "f", 1, 2206, "pg_typeof"}, // regtype
}

// queryPgProc returns data for pg_catalog.pg_proc.
func (pg *PgCatalog) queryPgProc(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgProcColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, f := range builtinFunctions {
		row := map[string]any{
			"oid":             f.oid,
			"proname":         f.proname,
			"pronamespace":    pgCatalogNamespaceOID,
			"proowner":        int64(10), // Superuser
			"prolang":         int64(12), // internal language
			"procost":         float64(1),
			"prorows":         float64(0),
			"provariadic":     int64(0),
			"prosupport":      int64(0),
			"prokind":         f.prokind,
			"prosecdef":       false,
			"proleakproof":    false,
			"proisstrict":     false,
			"proretset":       false,
			"provolatile":     "v", // volatile by default
			"proparallel":     "u", // unsafe by default
			"pronargs":        f.pronargs,
			"pronargdefaults": int64(0),
			"prorettype":      f.prorettype,
			"proargtypes":     nil,
			"proallargtypes":  nil,
			"proargmodes":     nil,
			"proargnames":     nil,
			"proargdefaults":  nil,
			"protrftypes":     nil,
			"prosrc":          f.prosrc,
			"probin":          nil,
			"prosqlbody":      nil,
			"proconfig":       nil,
			"proacl":          nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
