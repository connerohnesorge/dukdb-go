package catalog

// pg_database columns - PostgreSQL database catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-database.html
var pgDatabaseColumns = []string{
	"oid",             // Row identifier
	"datname",         // Database name
	"datdba",          // Owner of the database
	"encoding",        // Character encoding for this database
	"datlocprovider",  // Locale provider (c, i, or empty)
	"datistemplate",   // If true, then this database can be cloned
	"datallowconn",    // If false then no one can connect
	"datconnlimit",    // Maximum concurrent connections (-1 = no limit)
	"datfrozenxid",    // All rows with xmax < this are frozen
	"datminmxid",      // All rows with xmax < this are frozen
	"dattablespace",   // Default tablespace OID
	"datcollate",      // LC_COLLATE for this database
	"datctype",        // LC_CTYPE for this database
	"daticulocale",    // ICU locale if datlocprovider is i
	"daticurules",     // ICU collation rules
	"datcollversion",  // Collation version
	"datacl",          // Access privileges
}

// queryPgDatabase returns data for pg_catalog.pg_database.
func (pg *PgCatalog) queryPgDatabase(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgDatabaseColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Add the current database
	dbOID := generateOID("database:" + pg.databaseName)
	row := map[string]any{
		"oid":            dbOID,
		"datname":        pg.databaseName,
		"datdba":         int64(10), // Superuser OID
		"encoding":       int64(6),  // UTF8 encoding
		"datlocprovider": "c",       // libc
		"datistemplate":  false,
		"datallowconn":   true,
		"datconnlimit":   int64(-1), // No limit
		"datfrozenxid":   int64(0),
		"datminmxid":     int64(1),
		"dattablespace":  int64(1663), // pg_default
		"datcollate":     "en_US.UTF-8",
		"datctype":       "en_US.UTF-8",
		"daticulocale":   nil,
		"daticurules":    nil,
		"datcollversion": nil,
		"datacl":         nil,
	}

	if matchesFilters(row, filters) {
		result.Rows = append(result.Rows, row)
	}

	// Add template databases for completeness
	templateDbs := []struct {
		name       string
		oid        int64
		isTemplate bool
		allowConn  bool
	}{
		{"template0", 1, true, false},
		{"template1", 4, true, true},
	}

	for _, tdb := range templateDbs {
		row := map[string]any{
			"oid":            tdb.oid,
			"datname":        tdb.name,
			"datdba":         int64(10),
			"encoding":       int64(6),
			"datlocprovider": "c",
			"datistemplate":  tdb.isTemplate,
			"datallowconn":   tdb.allowConn,
			"datconnlimit":   int64(-1),
			"datfrozenxid":   int64(0),
			"datminmxid":     int64(1),
			"dattablespace":  int64(1663),
			"datcollate":     "en_US.UTF-8",
			"datctype":       "en_US.UTF-8",
			"daticulocale":   nil,
			"daticurules":    nil,
			"datcollversion": nil,
			"datacl":         nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
