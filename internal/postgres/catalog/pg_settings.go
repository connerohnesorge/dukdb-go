package catalog

// pg_settings columns - PostgreSQL server configuration
// Reference: https://www.postgresql.org/docs/current/view-pg-settings.html
var pgSettingsColumns = []string{
	"name",            // Parameter name
	"setting",         // Current value
	"unit",            // Implicit unit of the parameter
	"category",        // Logical group of the parameter
	"short_desc",      // Brief description
	"extra_desc",      // Additional, more detailed description
	"context",         // Context required to set the parameter
	"vartype",         // Parameter type (bool, enum, integer, real, string)
	"source",          // Source of current value
	"min_val",         // Minimum allowed value (null for non-numeric)
	"max_val",         // Maximum allowed value (null for non-numeric)
	"enumvals",        // Allowed values for enum parameters
	"boot_val",        // Parameter value at server start
	"reset_val",       // Value that RESET would restore
	"sourcefile",      // Configuration file containing current value
	"sourceline",      // Line in configuration file
	"pending_restart", // True if value changed but requires restart
}

// serverSettings contains the dukdb server settings to expose.
var serverSettings = []struct {
	name      string
	setting   string
	unit      string
	category  string
	shortDesc string
	context   string
	vartype   string
	source    string
	minVal    string
	maxVal    string
	enumvals  string
	bootVal   string
}{
	// Version and identification
	{
		name:      "server_version",
		setting:   "14.0 (DukDB 1.0)",
		category:  "Preset Options",
		shortDesc: "Shows the server version",
		context:   "internal",
		vartype:   "string",
		source:    "default",
		bootVal:   "14.0 (DukDB 1.0)",
	},
	{
		name:      "server_version_num",
		setting:   "140000",
		category:  "Preset Options",
		shortDesc: "Shows the server version as an integer",
		context:   "internal",
		vartype:   "integer",
		source:    "default",
		bootVal:   "140000",
	},
	// Connection settings
	{
		name:      "max_connections",
		setting:   "100",
		category:  "Connections and Authentication / Connection Settings",
		shortDesc: "Sets the maximum number of concurrent connections",
		context:   "postmaster",
		vartype:   "integer",
		source:    "default",
		minVal:    "1",
		maxVal:    "262143",
		bootVal:   "100",
	},
	{
		name:      "superuser_reserved_connections",
		setting:   "3",
		category:  "Connections and Authentication / Connection Settings",
		shortDesc: "Sets the number of connection slots reserved for superusers",
		context:   "postmaster",
		vartype:   "integer",
		source:    "default",
		minVal:    "0",
		maxVal:    "262143",
		bootVal:   "3",
	},
	// Memory settings
	{
		name:      "shared_buffers",
		setting:   "128MB",
		unit:      "8kB",
		category:  "Resource Usage / Memory",
		shortDesc: "Sets the number of shared memory buffers",
		context:   "postmaster",
		vartype:   "integer",
		source:    "default",
		minVal:    "16",
		bootVal:   "128MB",
	},
	{
		name:      "work_mem",
		setting:   "4MB",
		unit:      "kB",
		category:  "Resource Usage / Memory",
		shortDesc: "Sets the maximum memory to be used for query workspaces",
		context:   "user",
		vartype:   "integer",
		source:    "default",
		minVal:    "64",
		bootVal:   "4MB",
	},
	// Transaction settings
	{
		name:      "default_transaction_isolation",
		setting:   "serializable",
		category:  "Client Connection Defaults / Statement Behavior",
		shortDesc: "Sets the transaction isolation level of each new transaction",
		context:   "user",
		vartype:   "enum",
		source:    "default",
		enumvals:  "{serializable,\"repeatable read\",\"read committed\",\"read uncommitted\"}",
		bootVal:   "serializable",
	},
	{
		name:      "default_transaction_read_only",
		setting:   "off",
		category:  "Client Connection Defaults / Statement Behavior",
		shortDesc: "Sets the default read-only status of new transactions",
		context:   "user",
		vartype:   "bool",
		source:    "default",
		bootVal:   "off",
	},
	// Client settings
	{
		name:      "client_encoding",
		setting:   "UTF8",
		category:  "Client Connection Defaults / Locale and Formatting",
		shortDesc: "Sets the client's character set encoding",
		context:   "user",
		vartype:   "string",
		source:    "default",
		bootVal:   "UTF8",
	},
	{
		name:      "DateStyle",
		setting:   "ISO, MDY",
		category:  "Client Connection Defaults / Locale and Formatting",
		shortDesc: "Sets the display format for date and time values",
		context:   "user",
		vartype:   "string",
		source:    "default",
		bootVal:   "ISO, MDY",
	},
	{
		name:      "TimeZone",
		setting:   "UTC",
		category:  "Client Connection Defaults / Locale and Formatting",
		shortDesc: "Sets the time zone for displaying and interpreting time stamps",
		context:   "user",
		vartype:   "string",
		source:    "default",
		bootVal:   "UTC",
	},
	{
		name:      "standard_conforming_strings",
		setting:   "on",
		category:  "Client Connection Defaults / Statement Behavior",
		shortDesc: "Causes '...' strings to treat backslashes literally",
		context:   "user",
		vartype:   "bool",
		source:    "default",
		bootVal:   "on",
	},
	// Logging
	{
		name:      "log_statement",
		setting:   "none",
		category:  "Reporting and Logging / What to Log",
		shortDesc: "Sets the type of statements logged",
		context:   "superuser",
		vartype:   "enum",
		source:    "default",
		enumvals:  "{none,ddl,mod,all}",
		bootVal:   "none",
	},
	// Query planning
	{
		name:      "enable_seqscan",
		setting:   "on",
		category:  "Query Tuning / Planner Method Configuration",
		shortDesc: "Enables the planner's use of sequential-scan plans",
		context:   "user",
		vartype:   "bool",
		source:    "default",
		bootVal:   "on",
	},
	{
		name:      "enable_indexscan",
		setting:   "on",
		category:  "Query Tuning / Planner Method Configuration",
		shortDesc: "Enables the planner's use of index-scan plans",
		context:   "user",
		vartype:   "bool",
		source:    "default",
		bootVal:   "on",
	},
	{
		name:      "enable_hashjoin",
		setting:   "on",
		category:  "Query Tuning / Planner Method Configuration",
		shortDesc: "Enables the planner's use of hash join plans",
		context:   "user",
		vartype:   "bool",
		source:    "default",
		bootVal:   "on",
	},
	// SSL
	{
		name:      "ssl",
		setting:   "off",
		category:  "Connections and Authentication / SSL",
		shortDesc: "Enables SSL connections",
		context:   "sighup",
		vartype:   "bool",
		source:    "default",
		bootVal:   "off",
	},
	// Search path
	{
		name:      "search_path",
		setting:   "\"$user\", public",
		category:  "Client Connection Defaults / Statement Behavior",
		shortDesc: "Sets the schema search order for names that are not schema-qualified",
		context:   "user",
		vartype:   "string",
		source:    "default",
		bootVal:   "\"$user\", public",
	},
	// Application name
	{
		name:      "application_name",
		setting:   "",
		category:  "Reporting and Logging / What to Log",
		shortDesc: "Sets the application name to be reported in statistics and logs",
		context:   "user",
		vartype:   "string",
		source:    "default",
		bootVal:   "",
	},
	// Integer datetime
	{
		name:      "integer_datetimes",
		setting:   "on",
		category:  "Preset Options",
		shortDesc: "Datetimes are integer based",
		context:   "internal",
		vartype:   "bool",
		source:    "default",
		bootVal:   "on",
	},
	// Interval style
	{
		name:      "IntervalStyle",
		setting:   "postgres",
		category:  "Client Connection Defaults / Locale and Formatting",
		shortDesc: "Sets the display format for interval values",
		context:   "user",
		vartype:   "enum",
		source:    "default",
		enumvals:  "{postgres,postgres_verbose,sql_standard,iso_8601}",
		bootVal:   "postgres",
	},
}

// queryPgSettings returns data for pg_catalog.pg_settings.
func (pg *PgCatalog) queryPgSettings(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgSettingsColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, s := range serverSettings {
		var minVal, maxVal, enumvals any
		if s.minVal != "" {
			minVal = s.minVal
		}
		if s.maxVal != "" {
			maxVal = s.maxVal
		}
		if s.enumvals != "" {
			enumvals = s.enumvals
		}

		row := map[string]any{
			"name":            s.name,
			"setting":         s.setting,
			"unit":            nilIfEmpty(s.unit),
			"category":        s.category,
			"short_desc":      s.shortDesc,
			"extra_desc":      nil,
			"context":         s.context,
			"vartype":         s.vartype,
			"source":          s.source,
			"min_val":         minVal,
			"max_val":         maxVal,
			"enumvals":        enumvals,
			"boot_val":        s.bootVal,
			"reset_val":       s.setting, // Same as current setting
			"sourcefile":      nil,
			"sourceline":      nil,
			"pending_restart": false,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}

// nilIfEmpty returns nil if the string is empty, otherwise the string.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
