package functions

// registerJSONFuncs registers JSON function aliases.
func (r *FunctionAliasRegistry) registerJSONFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "json_array_length", DuckDBName: "json_array_length", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns JSON array length"},
		{PostgreSQLName: "jsonb_array_length", DuckDBName: "json_array_length", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns JSONB array length"},
		{PostgreSQLName: "json_extract_path", DuckDBName: "json_extract", Category: DirectAlias,
			MinArgs: 2, MaxArgs: -1, Description: "Extracts JSON value at path"},
		{PostgreSQLName: "jsonb_extract_path", DuckDBName: "json_extract", Category: DirectAlias,
			MinArgs: 2, MaxArgs: -1, Description: "Extracts JSONB value at path"},
		{PostgreSQLName: "json_extract_path_text", DuckDBName: "json_extract_string", Category: DirectAlias,
			MinArgs: 2, MaxArgs: -1, Description: "Extracts JSON value as text"},
		{PostgreSQLName: "jsonb_extract_path_text", DuckDBName: "json_extract_string", Category: DirectAlias,
			MinArgs: 2, MaxArgs: -1, Description: "Extracts JSONB value as text"},
		{PostgreSQLName: "json_typeof", DuckDBName: "json_type", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns JSON value type"},
		{PostgreSQLName: "jsonb_typeof", DuckDBName: "json_type", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns JSONB value type"},
		{PostgreSQLName: "to_json", DuckDBName: "to_json", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Converts value to JSON"},
		{PostgreSQLName: "to_jsonb", DuckDBName: "to_json", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Converts value to JSONB"},
		{PostgreSQLName: "json_agg", DuckDBName: "json_group_array", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Aggregates values into JSON array"},
		{PostgreSQLName: "jsonb_agg", DuckDBName: "json_group_array", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Aggregates values into JSONB array"},
		{PostgreSQLName: "json_object_agg", DuckDBName: "json_group_object", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Aggregates key-value pairs into JSON object"},
		{PostgreSQLName: "jsonb_object_agg", DuckDBName: "json_group_object", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Aggregates key-value pairs into JSONB object"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
