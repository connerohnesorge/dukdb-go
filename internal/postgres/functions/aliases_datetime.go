package functions

// Common DuckDB function name constants for date/time functions.
const (
	funcCurrentTimestamp = "current_timestamp"
	funcCurrentDate      = "current_date"
	funcCurrentTime      = "current_time"
)

// registerDateTimeFuncs registers date/time function aliases.
func (r *FunctionAliasRegistry) registerDateTimeFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "now", DuckDBName: funcCurrentTimestamp, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current date and time"},
		{
			PostgreSQLName: "current_timestamp",
			DuckDBName:     funcCurrentTimestamp,
			Category:       DirectAlias,
			MinArgs:        0,
			MaxArgs:        0,
			Description:    "Returns current date and time",
		},
		{PostgreSQLName: "current_date", DuckDBName: funcCurrentDate, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current date"},
		{PostgreSQLName: "current_time", DuckDBName: funcCurrentTime, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current time"},
		{PostgreSQLName: "localtime", DuckDBName: funcCurrentTime, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current local time"},
		{PostgreSQLName: "localtimestamp", DuckDBName: funcCurrentTimestamp, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current local timestamp"},
		{PostgreSQLName: "timeofday", DuckDBName: funcCurrentTimestamp, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current time as text"},
		{
			PostgreSQLName: "transaction_timestamp",
			DuckDBName:     funcCurrentTimestamp,
			Category:       DirectAlias,
			MinArgs:        0,
			MaxArgs:        0,
			Description:    "Returns timestamp of current transaction",
		},
		{
			PostgreSQLName: "statement_timestamp",
			DuckDBName:     funcCurrentTimestamp,
			Category:       DirectAlias,
			MinArgs:        0,
			MaxArgs:        0,
			Description:    "Returns timestamp of current statement",
		},
		{PostgreSQLName: "clock_timestamp", DuckDBName: funcCurrentTimestamp, Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current clock time"},
		{PostgreSQLName: "date_part", DuckDBName: "date_part", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Extracts date/time part"},
		{PostgreSQLName: "date_trunc", DuckDBName: "date_trunc", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Truncates timestamp to specified precision"},
		{PostgreSQLName: "extract", DuckDBName: "extract", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Extracts date/time field"},
		{PostgreSQLName: "age", DuckDBName: "age", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Calculates age between timestamps"},
		{PostgreSQLName: "to_timestamp", DuckDBName: "to_timestamp", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Converts to timestamp"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
