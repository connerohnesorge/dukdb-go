package catalog

// pg_tables columns - Simplified PostgreSQL tables view
// Reference: https://www.postgresql.org/docs/current/view-pg-tables.html
var pgTablesColumns = []string{
	"schemaname",  // Name of schema containing table
	"tablename",   // Name of the table
	"tableowner",  // Name of table's owner
	"tablespace",  // Name of tablespace containing table (null if default)
	"hasindexes",  // True if table has (or recently had) any indexes
	"hasrules",    // True if table has (or ever had) rules
	"hastriggers", // True if table has (or ever had) triggers
	"rowsecurity", // True if table has row level security enabled
}

// queryPgTables returns data for pg_catalog.pg_tables.
func (pg *PgCatalog) queryPgTables(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgTablesColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		// Map "main" to "public" for PostgreSQL compatibility
		schemaName := schema.Name()
		if schemaName == "main" {
			schemaName = "public"
		}

		// Get tables in this schema
		tables := pg.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			hasIndexes := len(pg.catalog.GetIndexesForTable(schema.Name(), table.Name)) > 0

			row := map[string]any{
				"schemaname":  schemaName,
				"tablename":   table.Name,
				"tableowner":  "dukdb",
				"tablespace":  nil, // Default tablespace
				"hasindexes":  hasIndexes,
				"hasrules":    false,
				"hastriggers": false,
				"rowsecurity": false,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}

// pg_views columns - Simplified PostgreSQL views view
// Reference: https://www.postgresql.org/docs/current/view-pg-views.html
var pgViewsColumns = []string{
	"schemaname", // Name of schema containing view
	"viewname",   // Name of the view
	"viewowner",  // Name of view's owner
	"definition", // View definition (a reconstructed SELECT command)
}

// queryPgViews returns data for pg_catalog.pg_views.
func (pg *PgCatalog) queryPgViews(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgViewsColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		// Map "main" to "public" for PostgreSQL compatibility
		schemaName := schema.Name()
		if schemaName == "main" {
			schemaName = "public"
		}

		// Get views in this schema
		views := schema.ListViews()
		for _, view := range views {
			row := map[string]any{
				"schemaname": schemaName,
				"viewname":   view.Name,
				"viewowner":  "dukdb",
				"definition": view.Query,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}
