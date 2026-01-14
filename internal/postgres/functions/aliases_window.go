package functions

// Argument count constants for window functions.
const argsMaxWindowThree = 3

// registerWindowFuncs registers window function aliases.
func (r *FunctionAliasRegistry) registerWindowFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "row_number", DuckDBName: "row_number", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Sequential row number"},
		{PostgreSQLName: "rank", DuckDBName: "rank", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Rank with gaps"},
		{PostgreSQLName: "dense_rank", DuckDBName: "dense_rank", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Rank without gaps"},
		{PostgreSQLName: "percent_rank", DuckDBName: "percent_rank", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Relative rank"},
		{PostgreSQLName: "cume_dist", DuckDBName: "cume_dist", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Cumulative distribution"},
		{PostgreSQLName: "ntile", DuckDBName: "ntile", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Divides into N buckets"},
		{PostgreSQLName: "lag", DuckDBName: "lag", Category: DirectAlias,
			MinArgs: 1, MaxArgs: argsMaxWindowThree, Description: "Previous row value"},
		{PostgreSQLName: "lead", DuckDBName: "lead", Category: DirectAlias,
			MinArgs: 1, MaxArgs: argsMaxWindowThree, Description: "Next row value"},
		{PostgreSQLName: "first_value", DuckDBName: "first_value", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "First value in window"},
		{PostgreSQLName: "last_value", DuckDBName: "last_value", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Last value in window"},
		{PostgreSQLName: "nth_value", DuckDBName: "nth_value", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Nth value in window"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
