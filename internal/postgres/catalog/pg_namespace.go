package catalog

// pg_namespace columns - PostgreSQL namespace (schema) catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-namespace.html
var pgNamespaceColumns = []string{
	"oid",        // Row identifier
	"nspname",    // Name of the namespace
	"nspowner",   // Owner of the namespace
	"nspacl",     // Access privileges; see Section 5.7 for details
	"nspcomment", // Comment
}

// queryPgNamespace returns data for pg_catalog.pg_namespace.
func (pg *PgCatalog) queryPgNamespace(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgNamespaceColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Add system schemas first
	systemSchemas := []struct {
		oid     int64
		nspname string
	}{
		{pgCatalogNamespaceOID, "pg_catalog"},
		{informationSchemaNamespaceOID, "information_schema"},
	}

	for _, sys := range systemSchemas {
		row := map[string]any{
			"oid":        sys.oid,
			"nspname":    sys.nspname,
			"nspowner":   int64(10), // Superuser OID
			"nspacl":     nil,
			"nspcomment": nil,
		}
		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	// Iterate over all user schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		// Map "main" to "public" for PostgreSQL compatibility
		schemaName := schema.Name()
		if schemaName == "main" {
			schemaName = "public"
		}

		row := map[string]any{
			"oid":        getNamespaceOID(schema.Name()),
			"nspname":    schemaName,
			"nspowner":   int64(10), // Superuser OID
			"nspacl":     nil,
			"nspcomment": nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
