package catalog

// pg_constraint columns - PostgreSQL constraint catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-constraint.html
var pgConstraintColumns = []string{
	"oid",           // Row identifier
	"conname",       // Constraint name
	"connamespace",  // OID of namespace containing this constraint
	"contype",       // c = check, f = foreign key, p = primary key, u = unique, t = trigger, x = exclusion
	"condeferrable", // Is constraint deferrable
	"condeferred",   // Is constraint deferred by default
	"convalidated",  // Has constraint been validated
	"conrelid",      // Table this constraint is on; 0 if not a table constraint
	"contypid",      // Domain this constraint is on; 0 if not a domain constraint
	"conindid",      // Index supporting this constraint, if any
	"conpfeqop",     // Equality operators for comparison (for FK)
	"conppeqop",     // Equality operators for comparison (for FK)
	"conffeqop",     // Equality operators for comparison (for FK)
	"conexclop",     // Exclusion operator OIDs
	"conparentid",   // Parent constraint of partitioned constraint
	"confupdtype",   // FK update action code
	"confdeltype",   // FK delete action code
	"confmatchtype", // FK match type
	"conperiod",     // Constraint includes period
	"conislocal",    // Constraint is local
	"coninhcount",   // Number of inheritance ancestors
	"connoinherit",  // Constraint is non-inheritable
	"conkey",        // Constrained columns (for table constraints)
	"confkey",       // Referenced columns (for foreign keys)
	"conbin",        // If check constraint, pg_node_tree representation
	"consrc",        // Constraint definition
	"conpfeqop_v",   // Equality operators for comparison (vector)
	"conppeqop_v",   // Equality operators for comparison (vector)
	"conffeqop_v",   // Equality operators for comparison (vector)
	"conexclop_v",   // Exclusion operator OIDs (vector)
}

// queryPgConstraint returns data for pg_catalog.pg_constraint.
func (pg *PgCatalog) queryPgConstraint(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgConstraintColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		namespaceOID := getNamespaceOID(schema.Name())

		// Get tables to check for primary keys
		tables := pg.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			tableOID := generateOID("table:" + schema.Name() + "." + table.Name)

			// Check if table has a primary key
			if table.HasPrimaryKey() {
				constraintOID := generateOID("constraint:pk:" + schema.Name() + "." + table.Name)
				indexOID := generateOID("index:pk:" + schema.Name() + "." + table.Name)

				// Build conkey array (column numbers)
				conkey := buildConKey(table.PrimaryKey)

				row := map[string]any{
					"oid":           constraintOID,
					"conname":       table.Name + "_pkey",
					"connamespace":  namespaceOID,
					"contype":       "p", // primary key
					"condeferrable": false,
					"condeferred":   false,
					"convalidated":  true,
					"conrelid":      tableOID,
					"contypid":      int64(0),
					"conindid":      indexOID,
					"conpfeqop":     nil,
					"conppeqop":     nil,
					"conffeqop":     nil,
					"conexclop":     nil,
					"conparentid":   int64(0),
					"confupdtype":   " ",
					"confdeltype":   " ",
					"confmatchtype": " ",
					"conperiod":     false,
					"conislocal":    true,
					"coninhcount":   int64(0),
					"connoinherit":  false,
					"conkey":        conkey,
					"confkey":       nil,
					"conbin":        nil,
					"consrc":        nil,
					"conpfeqop_v":   nil,
					"conppeqop_v":   nil,
					"conffeqop_v":   nil,
					"conexclop_v":   nil,
				}

				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}

			// Get indexes for unique constraints
			indexes := pg.catalog.GetIndexesForTable(schema.Name(), table.Name)
			for _, idx := range indexes {
				if idx.IsUnique && !idx.IsPrimary {
					constraintOID := generateOID("constraint:uq:" + schema.Name() + "." + idx.Name)
					indexOID := generateOID("index:" + schema.Name() + "." + idx.Name)

					// Build conkey array from column names
					colIndices := make([]int, len(idx.Columns))
					for i, colName := range idx.Columns {
						for j, col := range table.Columns {
							if col.Name == colName {
								colIndices[i] = j
								break
							}
						}
					}
					conkey := buildConKey(colIndices)

					row := map[string]any{
						"oid":           constraintOID,
						"conname":       idx.Name,
						"connamespace":  namespaceOID,
						"contype":       "u", // unique
						"condeferrable": false,
						"condeferred":   false,
						"convalidated":  true,
						"conrelid":      tableOID,
						"contypid":      int64(0),
						"conindid":      indexOID,
						"conpfeqop":     nil,
						"conppeqop":     nil,
						"conffeqop":     nil,
						"conexclop":     nil,
						"conparentid":   int64(0),
						"confupdtype":   " ",
						"confdeltype":   " ",
						"confmatchtype": " ",
						"conperiod":     false,
						"conislocal":    true,
						"coninhcount":   int64(0),
						"connoinherit":  false,
						"conkey":        conkey,
						"confkey":       nil,
						"conbin":        nil,
						"consrc":        nil,
						"conpfeqop_v":   nil,
						"conppeqop_v":   nil,
						"conffeqop_v":   nil,
						"conexclop_v":   nil,
					}

					if matchesFilters(row, filters) {
						result.Rows = append(result.Rows, row)
					}
				}
			}
		}
	}

	return result
}

// buildConKey builds the conkey array (1-indexed column numbers).
func buildConKey(colIndices []int) string {
	// Returns a space-separated string of column positions (1-indexed)
	// In PostgreSQL this is actually an int2vector type
	result := ""
	for i, idx := range colIndices {
		if i > 0 {
			result += " "
		}
		result += intToString(int64(idx + 1)) // 1-indexed
	}
	return result
}
