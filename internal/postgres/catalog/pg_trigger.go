package catalog

// pg_trigger columns - PostgreSQL trigger catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-trigger.html
// Note: DukDB does not currently support triggers, so this view returns an empty
// result set. It is provided for compatibility with PostgreSQL clients that
// query pg_trigger to check for trigger existence.
var pgTriggerColumns = []string{
	"oid",             // Row identifier
	"tgrelid",         // OID of table trigger is on
	"tgparentid",      // OID of parent trigger
	"tgname",          // Trigger name
	"tgfoid",          // OID of function to be called
	"tgtype",          // Bit mask for trigger type
	"tgenabled",       // Controls whether trigger fires (O = origin, D = disabled, R = replica, A = always)
	"tgisinternal",    // True if trigger is internally generated
	"tgconstrrelid",   // Referenced table if constraint trigger
	"tgconstrindid",   // OID of index supporting constraint
	"tgconstraint",    // OID of pg_constraint entry, if constraint trigger
	"tgdeferrable",    // True if constraint trigger is deferrable
	"tginitdeferred",  // True if constraint trigger is initially deferred
	"tgnargs",         // Number of arguments to trigger function
	"tgattr",          // Column numbers the trigger fires on, or empty
	"tgargs",          // Argument strings passed to trigger function
	"tgqual",          // WHEN condition, or null if none
	"tgoldtable",      // Name of OLD TABLE transition relation, or null
	"tgnewtable",      // Name of NEW TABLE transition relation, or null
}

// queryPgTrigger returns data for pg_catalog.pg_trigger.
// DukDB does not support triggers, so this always returns an empty result set.
// This is provided for compatibility with PostgreSQL clients and ORMs that
// query pg_trigger to discover triggers on tables.
func (pg *PgCatalog) queryPgTrigger(filters []Filter) *QueryResult {
	// Return empty result - DukDB does not support triggers
	return &QueryResult{
		Columns: pgTriggerColumns,
		Rows:    make([]map[string]any, 0),
	}
}
