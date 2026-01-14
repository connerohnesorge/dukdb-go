package functions

// registerAggregateFuncs registers aggregate function aliases.
func (r *FunctionAliasRegistry) registerAggregateFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "array_agg", DuckDBName: "list", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Aggregates values into array"},
		{PostgreSQLName: "string_agg", DuckDBName: "string_agg", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Concatenates strings with delimiter"},
		{PostgreSQLName: "count", DuckDBName: "count", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 1, Description: "Count rows"},
		{PostgreSQLName: "sum", DuckDBName: "sum", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Sum of values"},
		{PostgreSQLName: "avg", DuckDBName: "avg", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Average of values"},
		{PostgreSQLName: "min", DuckDBName: "min", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Minimum value"},
		{PostgreSQLName: "max", DuckDBName: "max", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Maximum value"},
		{PostgreSQLName: "bool_and", DuckDBName: "bool_and", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Logical AND aggregate"},
		{PostgreSQLName: "bool_or", DuckDBName: "bool_or", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Logical OR aggregate"},
		{PostgreSQLName: "every", DuckDBName: "bool_and", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Logical AND aggregate (alias)"},
		{PostgreSQLName: "bit_and", DuckDBName: "bit_and", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Bitwise AND aggregate"},
		{PostgreSQLName: "bit_or", DuckDBName: "bit_or", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Bitwise OR aggregate"},
		{PostgreSQLName: "variance", DuckDBName: "var_samp", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Sample variance"},
		{PostgreSQLName: "var_samp", DuckDBName: "var_samp", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Sample variance"},
		{PostgreSQLName: "var_pop", DuckDBName: "var_pop", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Population variance"},
		{PostgreSQLName: "stddev", DuckDBName: "stddev_samp", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Sample standard deviation"},
		{PostgreSQLName: "stddev_samp", DuckDBName: "stddev_samp", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Sample standard deviation"},
		{PostgreSQLName: "stddev_pop", DuckDBName: "stddev_pop", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Population standard deviation"},
		{PostgreSQLName: "corr", DuckDBName: "corr", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Correlation coefficient"},
		{PostgreSQLName: "covar_pop", DuckDBName: "covar_pop", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Population covariance"},
		{PostgreSQLName: "covar_samp", DuckDBName: "covar_samp", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Sample covariance"},
		{PostgreSQLName: "mode", DuckDBName: "mode", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Mode (most frequent value)"},
		{PostgreSQLName: "percentile_cont", DuckDBName: "quantile_cont", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Continuous percentile"},
		{PostgreSQLName: "percentile_disc", DuckDBName: "quantile_disc", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Discrete percentile"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
