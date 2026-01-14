package functions

// Argument count constants for system functions.
const (
	argsMaxSysThree = 3
	argsMaxSysFour  = 4
)

// registerSystemFunctions registers PostgreSQL system functions.
func (r *FunctionAliasRegistry) registerSystemFunctions() {
	r.registerSystemIntrospection()
	r.registerSystemCatalog()
	r.registerSystemPrivilege()
	r.registerSystemSession()
}

// registerSystemIntrospection registers introspection system functions.
func (r *FunctionAliasRegistry) registerSystemIntrospection() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "pg_typeof", DuckDBName: "typeof", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Returns type name of argument"},
		{PostgreSQLName: "pg_column_size", DuckDBName: "pg_column_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Estimated storage size of value"},
		{PostgreSQLName: "pg_database_size", DuckDBName: "pg_database_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Database size in bytes"},
		{PostgreSQLName: "pg_table_size", DuckDBName: "pg_table_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Table size excluding indexes"},
		{PostgreSQLName: "pg_relation_size", DuckDBName: "pg_relation_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 2, Description: "Relation size in bytes"},
		{PostgreSQLName: "pg_total_relation_size", DuckDBName: "pg_total_relation_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Total size including indexes"},
		{PostgreSQLName: "pg_indexes_size", DuckDBName: "pg_indexes_size", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Total size of indexes"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}

// registerSystemCatalog registers catalog system functions.
func (r *FunctionAliasRegistry) registerSystemCatalog() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "pg_get_constraintdef", DuckDBName: "pg_get_constraintdef", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 2, Description: "Constraint definition"},
		{PostgreSQLName: "pg_get_indexdef", DuckDBName: "pg_get_indexdef", Category: SystemFunction,
			MinArgs: 1, MaxArgs: argsMaxSysThree, Description: "Index definition"},
		{PostgreSQLName: "pg_get_viewdef", DuckDBName: "pg_get_viewdef", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 2, Description: "View definition"},
		{PostgreSQLName: "pg_get_function_arguments", DuckDBName: "pg_get_function_arguments", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Function arguments"},
		{PostgreSQLName: "pg_get_function_result", DuckDBName: "pg_get_function_result", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Function return type"},
		{PostgreSQLName: "pg_get_expr", DuckDBName: "pg_get_expr", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Deparse expression"},

		// Comment functions (return empty - comments not supported)
		{PostgreSQLName: "obj_description", DuckDBName: "obj_description", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 2, Description: "Object comment (returns empty)"},
		{PostgreSQLName: "col_description", DuckDBName: "col_description", Category: SystemFunction,
			MinArgs: 2, MaxArgs: 2, Description: "Column comment (returns empty)"},
		{PostgreSQLName: "shobj_description", DuckDBName: "shobj_description", Category: SystemFunction,
			MinArgs: 2, MaxArgs: 2, Description: "Shared object comment (returns empty)"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}

// registerSystemPrivilege registers privilege check system functions.
func (r *FunctionAliasRegistry) registerSystemPrivilege() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "has_table_privilege", DuckDBName: "has_table_privilege", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check table privilege (always true)"},
		{PostgreSQLName: "has_column_privilege", DuckDBName: "has_column_privilege", Category: SystemFunction,
			MinArgs: argsMaxSysThree, MaxArgs: argsMaxSysFour, Description: "Check column privilege (always true)"},
		{PostgreSQLName: "has_schema_privilege", DuckDBName: "has_schema_privilege", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check schema privilege (always true)"},
		{PostgreSQLName: "has_database_privilege", DuckDBName: "has_database_privilege", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check database privilege (always true)"},
		{PostgreSQLName: "has_function_privilege", DuckDBName: "has_function_privilege", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check function privilege (always true)"},
		{PostgreSQLName: "has_sequence_privilege", DuckDBName: "has_sequence_privilege", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check sequence privilege (always true)"},
		{PostgreSQLName: "pg_has_role", DuckDBName: "pg_has_role", Category: SystemFunction,
			MinArgs: 2, MaxArgs: argsMaxSysThree, Description: "Check role membership (always true)"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}

// registerSystemSession registers session/backend system functions.
func (r *FunctionAliasRegistry) registerSystemSession() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "current_schema", DuckDBName: "current_schema", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Current schema name (returns 'main')"},
		{PostgreSQLName: "current_schemas", DuckDBName: "current_schemas", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 1, Description: "Search path schemas"},
		{PostgreSQLName: "pg_backend_pid", DuckDBName: "pg_backend_pid", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Backend process ID"},
		{PostgreSQLName: "pg_postmaster_start_time", DuckDBName: "pg_postmaster_start_time", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Server start time"},
		{PostgreSQLName: "current_setting", DuckDBName: "current_setting", Category: SystemFunction,
			MinArgs: 1, MaxArgs: 2, Description: "Get setting value"},
		{PostgreSQLName: "set_config", DuckDBName: "set_config", Category: SystemFunction,
			MinArgs: argsMaxSysThree, MaxArgs: argsMaxSysThree, Description: "Set configuration"},
		{PostgreSQLName: "pg_is_in_recovery", DuckDBName: "pg_is_in_recovery", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Recovery mode check (always false)"},
		{PostgreSQLName: "pg_is_wal_replay_paused", DuckDBName: "pg_is_wal_replay_paused", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "WAL replay paused (always false)"},
		{PostgreSQLName: "pg_client_encoding", DuckDBName: "pg_client_encoding", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Client encoding (returns UTF8)"},
		{PostgreSQLName: "txid_current", DuckDBName: "txid_current", Category: SystemFunction,
			MinArgs: 0, MaxArgs: 0, Description: "Current transaction ID"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
