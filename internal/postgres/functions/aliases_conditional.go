package functions

// registerConditionalFuncs registers conditional function aliases.
func (r *FunctionAliasRegistry) registerConditionalFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "coalesce", DuckDBName: "coalesce", Category: DirectAlias,
			MinArgs: 1, MaxArgs: -1, Description: "Returns first non-null value"},
		{PostgreSQLName: "nullif", DuckDBName: "nullif", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Returns null if arguments are equal"},
		{PostgreSQLName: "greatest", DuckDBName: "greatest", Category: DirectAlias,
			MinArgs: 1, MaxArgs: -1, Description: "Returns largest value"},
		{PostgreSQLName: "least", DuckDBName: "least", Category: DirectAlias,
			MinArgs: 1, MaxArgs: -1, Description: "Returns smallest value"},
		{PostgreSQLName: "ifnull", DuckDBName: "ifnull", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Returns second arg if first is null"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
