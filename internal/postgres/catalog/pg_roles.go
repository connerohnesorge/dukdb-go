package catalog

// pg_roles columns - PostgreSQL roles view
// Reference: https://www.postgresql.org/docs/current/view-pg-roles.html
// Note: pg_roles is actually a view on pg_authid, but we implement it directly
// since we don't have pg_authid.
var pgRolesColumns = []string{
	"oid",            // Row identifier (from pg_authid)
	"rolname",        // Role name
	"rolsuper",       // Role has superuser privileges
	"rolinherit",     // Role automatically inherits privileges of roles it is a member of
	"rolcreaterole",  // Role can create more roles
	"rolcreatedb",    // Role can create databases
	"rolcanlogin",    // Role can log in (is a user)
	"rolreplication", // Role is a replication role
	"rolconnlimit",   // For roles that can log in, max concurrent connections (-1 = no limit)
	"rolpassword",    // Not the password (always shows as ********)
	"rolvaliduntil",  // Password expiry time (null = never expires)
	"rolbypassrls",   // Role bypasses row-level security policies
	"rolconfig",      // Role-specific defaults for configuration variables
}

// pg_user columns - PostgreSQL users view
// Reference: https://www.postgresql.org/docs/current/view-pg-user.html
// Note: pg_user shows only roles that have rolcanlogin = true
var pgUserColumns = []string{
	"usename",      // User name
	"usesysid",     // ID of user
	"usecreatedb",  // User can create databases
	"usesuper",     // User is a superuser
	"userepl",      // User can initiate streaming replication
	"usebypassrls", // User bypasses row-level security policies
	"passwd",       // Not the password (always shows as ********)
	"valuntil",     // Password expiry time (null = never expires)
	"useconfig",    // Session defaults for configuration variables
}

// builtinRoles contains the default roles for DukDB.
// DukDB currently runs without authentication by default, but provides
// these role entries for compatibility with PostgreSQL clients.
var builtinRoles = []struct {
	oid            int64
	rolname        string
	rolsuper       bool
	rolinherit     bool
	rolcreaterole  bool
	rolcreatedb    bool
	rolcanlogin    bool
	rolreplication bool
	rolconnlimit   int64
	rolbypassrls   bool
}{
	// Default superuser role (like postgres)
	{10, "dukdb", true, true, true, true, true, true, -1, true},
	// Additional standard roles that PostgreSQL clients might expect
	{11, "pg_monitor", false, true, false, false, false, false, -1, false},
	{12, "pg_read_all_settings", false, true, false, false, false, false, -1, false},
	{13, "pg_read_all_stats", false, true, false, false, false, false, -1, false},
	{14, "pg_stat_scan_tables", false, true, false, false, false, false, -1, false},
	{15, "pg_read_server_files", false, true, false, false, false, false, -1, false},
	{16, "pg_write_server_files", false, true, false, false, false, false, -1, false},
	{17, "pg_execute_server_program", false, true, false, false, false, false, -1, false},
	{18, "pg_signal_backend", false, true, false, false, false, false, -1, false},
}

// queryPgRoles returns data for pg_catalog.pg_roles.
func (pg *PgCatalog) queryPgRoles(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgRolesColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, role := range builtinRoles {
		row := map[string]any{
			"oid":            role.oid,
			"rolname":        role.rolname,
			"rolsuper":       role.rolsuper,
			"rolinherit":     role.rolinherit,
			"rolcreaterole":  role.rolcreaterole,
			"rolcreatedb":    role.rolcreatedb,
			"rolcanlogin":    role.rolcanlogin,
			"rolreplication": role.rolreplication,
			"rolconnlimit":   role.rolconnlimit,
			"rolpassword":    "********",
			"rolvaliduntil":  nil,
			"rolbypassrls":   role.rolbypassrls,
			"rolconfig":      nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}

// queryPgUser returns data for pg_catalog.pg_user.
// This returns only roles that can log in (rolcanlogin = true).
func (pg *PgCatalog) queryPgUser(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgUserColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, role := range builtinRoles {
		// pg_user only shows roles that can log in
		if !role.rolcanlogin {
			continue
		}

		row := map[string]any{
			"usename":      role.rolname,
			"usesysid":     role.oid,
			"usecreatedb":  role.rolcreatedb,
			"usesuper":     role.rolsuper,
			"userepl":      role.rolreplication,
			"usebypassrls": role.rolbypassrls,
			"passwd":       "********",
			"valuntil":     nil,
			"useconfig":    nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
