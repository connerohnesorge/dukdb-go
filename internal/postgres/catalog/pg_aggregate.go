package catalog

// pg_aggregate columns - PostgreSQL aggregate function catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-aggregate.html
var pgAggregateColumns = []string{
	"aggfnoid",         // OID of the aggregate function (pg_proc.oid)
	"aggkind",          // n = normal, o = ordered-set, h = hypothetical-set
	"aggnumdirectargs", // Number of direct (non-aggregated) arguments
	"aggtransfn",       // Transition function OID
	"aggfinalfn",       // Final function OID (0 if none)
	"aggcombinefn",     // Combine function OID (for parallel aggregation)
	"aggserialfn",      // Serialization function OID
	"aggdeserialfn",    // Deserialization function OID
	"aggmtransfn",      // Forward transition function for moving-aggregate mode
	"aggminvtransfn",   // Inverse transition function for moving-aggregate mode
	"aggmfinalfn",      // Final function for moving-aggregate mode
	"aggfinalextra",    // True if final function takes extra dummy arguments
	"aggmfinalextra",   // True if moving-aggregate final function takes extra args
	"aggfinalmodify",   // Whether final function modifies transition state
	"aggmfinalmodify",  // Whether moving-agg final function modifies state
	"aggsortop",        // Associated sort operator OID (0 if none)
	"aggtranstype",     // Data type of transition state
	"aggtransspace",    // Approximate average size of transition state
	"aggmtranstype",    // Data type of moving-aggregate transition state
	"aggmtransspace",   // Approximate average size of moving-aggregate state
	"agginitval",       // Initial value of transition state
	"aggminitval",      // Initial value for moving-aggregate mode
}

// builtinAggregates contains commonly used PostgreSQL aggregate functions.
// These link to pg_proc entries with prokind='a'.
var builtinAggregates = []struct {
	aggfnoid     int64  // OID (matches pg_proc.oid)
	proname      string // Function name (for reference)
	aggkind      string // n = normal, o = ordered-set, h = hypothetical-set
	aggtranstype int64  // Transition state type OID
	agginitval   string // Initial value
}{
	// Basic aggregates
	{300, "count", "n", 20, "0"},     // count -> int8, initial 0
	{301, "sum", "n", 1700, ""},      // sum -> numeric
	{302, "avg", "n", 1700, ""},      // avg -> numeric (internal state)
	{303, "min", "n", 0, ""},         // min -> any (uses input type)
	{304, "max", "n", 0, ""},         // max -> any (uses input type)
	{305, "array_agg", "n", 0, ""},   // array_agg -> any array
	{306, "string_agg", "n", 25, ""}, // string_agg -> text

	// Statistical aggregates
	{310, "stddev", "n", 1700, ""},      // stddev -> numeric
	{311, "stddev_pop", "n", 1700, ""},  // stddev_pop -> numeric
	{312, "stddev_samp", "n", 1700, ""}, // stddev_samp -> numeric
	{313, "variance", "n", 1700, ""},    // variance -> numeric
	{314, "var_pop", "n", 1700, ""},     // var_pop -> numeric
	{315, "var_samp", "n", 1700, ""},    // var_samp -> numeric

	// Boolean aggregates
	{320, "bool_and", "n", 16, ""}, // bool_and -> bool
	{321, "bool_or", "n", 16, ""},  // bool_or -> bool
	{322, "every", "n", 16, ""},    // every -> bool (synonym for bool_and)

	// Bit aggregates
	{330, "bit_and", "n", 1560, ""}, // bit_and -> bit
	{331, "bit_or", "n", 1560, ""},  // bit_or -> bit

	// JSON aggregates
	{340, "json_agg", "n", 114, ""},          // json_agg -> json
	{341, "jsonb_agg", "n", 3802, ""},        // jsonb_agg -> jsonb
	{342, "json_object_agg", "n", 114, ""},   // json_object_agg -> json
	{343, "jsonb_object_agg", "n", 3802, ""}, // jsonb_object_agg -> jsonb

	// Ordered-set aggregates
	{350, "percentile_cont", "o", 701, ""}, // percentile_cont -> float8
	{351, "percentile_disc", "o", 0, ""},   // percentile_disc -> any
	{352, "mode", "o", 0, ""},              // mode -> any

	// Hypothetical-set aggregates
	{360, "rank", "h", 0, ""},         // rank -> bigint
	{361, "dense_rank", "h", 0, ""},   // dense_rank -> bigint
	{362, "percent_rank", "h", 0, ""}, // percent_rank -> float8
	{363, "cume_dist", "h", 0, ""},    // cume_dist -> float8

	// Regression aggregates
	{370, "corr", "n", 1700, ""},           // corr -> float8
	{371, "covar_pop", "n", 1700, ""},      // covar_pop -> float8
	{372, "covar_samp", "n", 1700, ""},     // covar_samp -> float8
	{373, "regr_avgx", "n", 1700, ""},      // regr_avgx -> float8
	{374, "regr_avgy", "n", 1700, ""},      // regr_avgy -> float8
	{375, "regr_count", "n", 20, "0"},      // regr_count -> bigint
	{376, "regr_intercept", "n", 1700, ""}, // regr_intercept -> float8
	{377, "regr_r2", "n", 1700, ""},        // regr_r2 -> float8
	{378, "regr_slope", "n", 1700, ""},     // regr_slope -> float8
	{379, "regr_sxx", "n", 1700, ""},       // regr_sxx -> float8
	{380, "regr_sxy", "n", 1700, ""},       // regr_sxy -> float8
	{381, "regr_syy", "n", 1700, ""},       // regr_syy -> float8
}

// queryPgAggregate returns data for pg_catalog.pg_aggregate.
func (pg *PgCatalog) queryPgAggregate(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgAggregateColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, agg := range builtinAggregates {
		// Determine numdirectargs based on aggkind
		var numdirectargs int64
		if agg.aggkind == "o" || agg.aggkind == "h" {
			numdirectargs = 1 // ordered-set and hypothetical aggregates have direct args
		}

		row := map[string]any{
			"aggfnoid":         agg.aggfnoid,
			"aggkind":          agg.aggkind,
			"aggnumdirectargs": numdirectargs,
			"aggtransfn":       int64(0), // Transition function OID
			"aggfinalfn":       int64(0), // Final function OID
			"aggcombinefn":     int64(0), // Combine function OID
			"aggserialfn":      int64(0), // Serialization function OID
			"aggdeserialfn":    int64(0), // Deserialization function OID
			"aggmtransfn":      int64(0), // Moving-aggregate transition function
			"aggminvtransfn":   int64(0), // Moving-aggregate inverse transition
			"aggmfinalfn":      int64(0), // Moving-aggregate final function
			"aggfinalextra":    false,
			"aggmfinalextra":   false,
			"aggfinalmodify":   "r", // read-only (default)
			"aggmfinalmodify":  "r", // read-only (default)
			"aggsortop":        int64(0),
			"aggtranstype":     agg.aggtranstype,
			"aggtransspace":    int64(0),
			"aggmtranstype":    int64(0),
			"aggmtransspace":   int64(0),
			"agginitval":       nilIfEmpty(agg.agginitval),
			"aggminitval":      nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}

// nilIfEmpty is defined in pg_settings.go
