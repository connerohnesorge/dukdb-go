package catalog

// pg_extension columns - PostgreSQL extension catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-extension.html
// Note: DukDB does not use the PostgreSQL extension system, but provides this
// view for compatibility with PostgreSQL clients that query pg_extension.
var pgExtensionColumns = []string{
	"oid",             // Row identifier
	"extname",         // Extension name
	"extowner",        // Owner of the extension
	"extnamespace",    // Schema containing the extension's exported objects
	"extrelocatable",  // True if extension can be relocated to another schema
	"extversion",      // Version string
	"extconfig",       // Array of regclass OIDs for extension's configuration table(s)
	"extcondition",    // Array of WHERE-clause filter conditions for config tables
}

// builtinExtensions contains extensions that DukDB reports for compatibility.
// This includes plpgsql (which many ORMs expect to exist) and a dukdb extension
// for identification purposes.
var builtinExtensions = []struct {
	oid            int64
	extname        string
	extnamespace   int64
	extrelocatable bool
	extversion     string
}{
	// plpgsql is commonly queried by ORMs, report it as installed for compatibility
	{13824, "plpgsql", pgCatalogNamespaceOID, false, "1.0"},
	// DukDB "extension" for identification
	{16384, "dukdb", publicNamespaceOID, false, "1.0.0"},
}

// queryPgExtension returns data for pg_catalog.pg_extension.
// Returns a minimal set of extensions for compatibility with PostgreSQL clients.
func (pg *PgCatalog) queryPgExtension(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgExtensionColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, ext := range builtinExtensions {
		row := map[string]any{
			"oid":            ext.oid,
			"extname":        ext.extname,
			"extowner":       int64(10), // Superuser
			"extnamespace":   ext.extnamespace,
			"extrelocatable": ext.extrelocatable,
			"extversion":     ext.extversion,
			"extconfig":      nil,
			"extcondition":   nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
