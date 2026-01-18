package catalog

// pg_class columns - PostgreSQL relation (table/view/index/sequence) catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-class.html
var pgClassColumns = []string{
	"oid",                 // Row identifier
	"relname",             // Name of the table, index, view, etc.
	"relnamespace",        // OID of namespace containing this relation
	"reltype",             // OID of data type for this table's row type (0 for index)
	"reloftype",           // OID of a composite type if typed table, else 0
	"relowner",            // Owner of the relation
	"relam",               // Access method; 0 for sequences and views
	"relfilenode",         // Name of on-disk file; 0 means relation is a mapped relation
	"reltablespace",       // Tablespace; 0 means default tablespace
	"relpages",            // Number of disk pages
	"reltuples",           // Number of live rows in table
	"relallvisible",       // Number of pages marked all-visible
	"reltoastrelid",       // OID of the TOAST table, 0 if none
	"relhasindex",         // True if has (or recently had) any indexes
	"relisshared",         // True if shared across all databases
	"relpersistence",      // p = permanent, u = unlogged, t = temporary
	"relkind",             // r = table, i = index, S = sequence, v = view, etc.
	"relnatts",            // Number of user columns
	"relchecks",           // Number of CHECK constraints
	"relhasrules",         // True if has any rules
	"relhastriggers",      // True if has any triggers
	"relhassubclass",      // True if has any inheritance children
	"relrowsecurity",      // True if row security enabled
	"relforcerowsecurity", // True if row security forced for owner
	"relispopulated",      // True if populated (non-MATERIALIZED VIEW)
	"relreplident",        // Columns used to form replica identity
	"relispartition",      // True if is a partition
	"relrewrite",          // OID used during table rewrite, else 0
	"relfrozenxid",        // All rows with xmax < this are frozen
	"relminmxid",          // All rows with xmax < this are frozen
	"relacl",              // Access privileges
	"reloptions",          // Options
	"relpartbound",        // Partition bound definition
}

// relkind values
const (
	relKindTable       = "r" // ordinary table
	relKindIndex       = "i" // index
	relKindSequence    = "S" // sequence
	relKindView        = "v" // view
	relKindMatView     = "m" // materialized view
	relKindComposite   = "c" // composite type
	relKindForeign     = "f" // foreign table
	relKindPartitioned = "p" // partitioned table
)

// queryPgClass returns data for pg_catalog.pg_class.
func (pg *PgCatalog) queryPgClass(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgClassColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		namespaceOID := getNamespaceOID(schema.Name())

		// Add tables
		tables := pg.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			tableOID := generateOID("table:" + schema.Name() + "." + table.Name)
			hasIndex := len(pg.catalog.GetIndexesForTable(schema.Name(), table.Name)) > 0

			row := map[string]any{
				"oid":                 tableOID,
				"relname":             table.Name,
				"relnamespace":        namespaceOID,
				"reltype":             generateOID("type:" + table.Name),
				"reloftype":           int64(0),
				"relowner":            int64(10), // Superuser OID
				"relam":               int64(2),  // heap access method
				"relfilenode":         tableOID,
				"reltablespace":       int64(0),
				"relpages":            int64(0),    // Unknown
				"reltuples":           float64(-1), // Unknown
				"relallvisible":       int64(0),
				"reltoastrelid":       int64(0),
				"relhasindex":         hasIndex,
				"relisshared":         false,
				"relpersistence":      "p", // permanent
				"relkind":             relKindTable,
				"relnatts":            int64(len(table.Columns)),
				"relchecks":           int64(0),
				"relhasrules":         false,
				"relhastriggers":      false,
				"relhassubclass":      false,
				"relrowsecurity":      false,
				"relforcerowsecurity": false,
				"relispopulated":      true,
				"relreplident":        "d", // default
				"relispartition":      false,
				"relrewrite":          int64(0),
				"relfrozenxid":        int64(0),
				"relminmxid":          int64(1),
				"relacl":              nil,
				"reloptions":          nil,
				"relpartbound":        nil,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}

		// Add views
		views := schema.ListViews()
		for _, view := range views {
			viewOID := generateOID("view:" + schema.Name() + "." + view.Name)

			row := map[string]any{
				"oid":                 viewOID,
				"relname":             view.Name,
				"relnamespace":        namespaceOID,
				"reltype":             generateOID("type:" + view.Name),
				"reloftype":           int64(0),
				"relowner":            int64(10),
				"relam":               int64(0), // No access method for views
				"relfilenode":         int64(0),
				"reltablespace":       int64(0),
				"relpages":            int64(0),
				"reltuples":           float64(-1),
				"relallvisible":       int64(0),
				"reltoastrelid":       int64(0),
				"relhasindex":         false,
				"relisshared":         false,
				"relpersistence":      "p",
				"relkind":             relKindView,
				"relnatts":            int64(0), // Unknown without parsing query
				"relchecks":           int64(0),
				"relhasrules":         true, // Views have rules
				"relhastriggers":      false,
				"relhassubclass":      false,
				"relrowsecurity":      false,
				"relforcerowsecurity": false,
				"relispopulated":      true,
				"relreplident":        "n", // nothing
				"relispartition":      false,
				"relrewrite":          int64(0),
				"relfrozenxid":        int64(0),
				"relminmxid":          int64(1),
				"relacl":              nil,
				"reloptions":          nil,
				"relpartbound":        nil,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}

		// Add indexes
		for _, table := range tables {
			indexes := pg.catalog.GetIndexesForTable(schema.Name(), table.Name)
			for _, idx := range indexes {
				indexOID := generateOID("index:" + schema.Name() + "." + idx.Name)

				row := map[string]any{
					"oid":                 indexOID,
					"relname":             idx.Name,
					"relnamespace":        namespaceOID,
					"reltype":             int64(0), // Indexes don't have row types
					"reloftype":           int64(0),
					"relowner":            int64(10),
					"relam":               int64(403), // btree access method
					"relfilenode":         indexOID,
					"reltablespace":       int64(0),
					"relpages":            int64(0),
					"reltuples":           float64(-1),
					"relallvisible":       int64(0),
					"reltoastrelid":       int64(0),
					"relhasindex":         false,
					"relisshared":         false,
					"relpersistence":      "p",
					"relkind":             relKindIndex,
					"relnatts":            int64(len(idx.Columns)),
					"relchecks":           int64(0),
					"relhasrules":         false,
					"relhastriggers":      false,
					"relhassubclass":      false,
					"relrowsecurity":      false,
					"relforcerowsecurity": false,
					"relispopulated":      true,
					"relreplident":        "n",
					"relispartition":      false,
					"relrewrite":          int64(0),
					"relfrozenxid":        int64(0),
					"relminmxid":          int64(1),
					"relacl":              nil,
					"reloptions":          nil,
					"relpartbound":        nil,
				}

				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}
		}

		// Add sequences
		sequences := schema.ListSequences()
		for _, seq := range sequences {
			seqOID := generateOID("sequence:" + schema.Name() + "." + seq.Name)

			row := map[string]any{
				"oid":                 seqOID,
				"relname":             seq.Name,
				"relnamespace":        namespaceOID,
				"reltype":             generateOID("type:sequence"),
				"reloftype":           int64(0),
				"relowner":            int64(10),
				"relam":               int64(0),
				"relfilenode":         seqOID,
				"reltablespace":       int64(0),
				"relpages":            int64(1),
				"reltuples":           float64(1),
				"relallvisible":       int64(0),
				"reltoastrelid":       int64(0),
				"relhasindex":         false,
				"relisshared":         false,
				"relpersistence":      "p",
				"relkind":             relKindSequence,
				"relnatts":            int64(3), // Sequences have 3 system columns
				"relchecks":           int64(0),
				"relhasrules":         false,
				"relhastriggers":      false,
				"relhassubclass":      false,
				"relrowsecurity":      false,
				"relforcerowsecurity": false,
				"relispopulated":      true,
				"relreplident":        "n",
				"relispartition":      false,
				"relrewrite":          int64(0),
				"relfrozenxid":        int64(0),
				"relminmxid":          int64(1),
				"relacl":              nil,
				"reloptions":          nil,
				"relpartbound":        nil,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}
