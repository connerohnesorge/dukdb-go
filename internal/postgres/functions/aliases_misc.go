package functions

// Argument count constants for misc functions.
const argsMaxMiscThree = 3

// registerMiscFuncs registers miscellaneous function aliases.
func (r *FunctionAliasRegistry) registerMiscFuncs() {
	aliases := []*FunctionAlias{
		// UUID functions
		{PostgreSQLName: "gen_random_uuid", DuckDBName: "uuid", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Generates random UUID"},
		{PostgreSQLName: "uuid_generate_v4", DuckDBName: "uuid", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Generates random UUID (v4)"},

		// Sequence functions
		{PostgreSQLName: "nextval", DuckDBName: "nextval", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Gets next value from sequence"},
		{PostgreSQLName: "currval", DuckDBName: "currval", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Gets current sequence value"},
		{PostgreSQLName: "setval", DuckDBName: "setval", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxMiscThree, Description: "Sets sequence value"},

		// Session info functions
		{PostgreSQLName: "version", DuckDBName: "version", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns database version"},
		{PostgreSQLName: "current_user", DuckDBName: "current_user", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current user name"},
		{PostgreSQLName: "session_user", DuckDBName: "session_user", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns session user name"},
		{PostgreSQLName: "current_database", DuckDBName: "current_database", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current database name"},
		{PostgreSQLName: "current_catalog", DuckDBName: "current_database", Category: DirectAlias,
			MinArgs: 0, MaxArgs: 0, Description: "Returns current catalog name"},

		// Type conversion
		{PostgreSQLName: "cast", DuckDBName: "cast", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Type cast"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
