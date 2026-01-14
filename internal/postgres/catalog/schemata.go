package catalog

// schemataColumns defines the columns for information_schema.schemata.
// Reference: https://www.postgresql.org/docs/current/infoschema-schemata.html
var schemataColumns = []string{
	"catalog_name",
	"schema_name",
	"schema_owner",
	"default_character_set_catalog",
	"default_character_set_schema",
	"default_character_set_name",
	"sql_path",
}

// querySchemata returns data for information_schema.schemata.
func (is *InformationSchema) querySchemata(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: schemataColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		row := map[string]any{
			"catalog_name":                  is.databaseName,
			"schema_name":                   schema.Name(),
			"schema_owner":                  "dukdb", // Default owner
			"default_character_set_catalog": nil,
			"default_character_set_schema":  nil,
			"default_character_set_name":    nil,
			"sql_path":                      nil,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	// Add the information_schema itself (virtual schema)
	infoSchemaRow := map[string]any{
		"catalog_name":                  is.databaseName,
		"schema_name":                   "information_schema",
		"schema_owner":                  "dukdb",
		"default_character_set_catalog": nil,
		"default_character_set_schema":  nil,
		"default_character_set_name":    nil,
		"sql_path":                      nil,
	}
	if matchesFilters(infoSchemaRow, filters) {
		result.Rows = append(result.Rows, infoSchemaRow)
	}

	// Add pg_catalog (virtual schema for PostgreSQL system catalog)
	pgCatalogRow := map[string]any{
		"catalog_name":                  is.databaseName,
		"schema_name":                   "pg_catalog",
		"schema_owner":                  "dukdb",
		"default_character_set_catalog": nil,
		"default_character_set_schema":  nil,
		"default_character_set_name":    nil,
		"sql_path":                      nil,
	}
	if matchesFilters(pgCatalogRow, filters) {
		result.Rows = append(result.Rows, pgCatalogRow)
	}

	return result
}

// tableConstraintsColumns defines the columns for information_schema.table_constraints.
// Reference: https://www.postgresql.org/docs/current/infoschema-table-constraints.html
var tableConstraintsColumns = []string{
	"constraint_catalog",
	"constraint_schema",
	"constraint_name",
	"table_catalog",
	"table_schema",
	"table_name",
	"constraint_type",
	"is_deferrable",
	"initially_deferred",
	"enforced",
}

// queryTableConstraints returns data for information_schema.table_constraints.
// This includes primary key and unique constraints.
func (is *InformationSchema) queryTableConstraints(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: tableConstraintsColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get tables to check for primary keys
		tables := is.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			// Check if table has a primary key
			if table.HasPrimaryKey() {
				row := map[string]any{
					"constraint_catalog": is.databaseName,
					"constraint_schema":  schema.Name(),
					"constraint_name":    table.Name + "_pkey",
					"table_catalog":      is.databaseName,
					"table_schema":       schema.Name(),
					"table_name":         table.Name,
					"constraint_type":    "PRIMARY KEY",
					"is_deferrable":      "NO",
					"initially_deferred": "NO",
					"enforced":           "YES",
				}
				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}

			// Get indexes for unique constraints
			indexes := is.catalog.GetIndexesForTable(schema.Name(), table.Name)
			for _, idx := range indexes {
				if idx.IsUnique && !idx.IsPrimary {
					row := map[string]any{
						"constraint_catalog": is.databaseName,
						"constraint_schema":  schema.Name(),
						"constraint_name":    idx.Name,
						"table_catalog":      is.databaseName,
						"table_schema":       schema.Name(),
						"table_name":         table.Name,
						"constraint_type":    "UNIQUE",
						"is_deferrable":      "NO",
						"initially_deferred": "NO",
						"enforced":           "YES",
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

// keyColumnUsageColumns defines the columns for information_schema.key_column_usage.
// Reference: https://www.postgresql.org/docs/current/infoschema-key-column-usage.html
var keyColumnUsageColumns = []string{
	"constraint_catalog",
	"constraint_schema",
	"constraint_name",
	"table_catalog",
	"table_schema",
	"table_name",
	"column_name",
	"ordinal_position",
	"position_in_unique_constraint",
}

// queryKeyColumnUsage returns data for information_schema.key_column_usage.
func (is *InformationSchema) queryKeyColumnUsage(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: keyColumnUsageColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get tables to check for primary keys
		tables := is.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			// Add primary key columns
			if table.HasPrimaryKey() {
				for i, pkIdx := range table.PrimaryKey {
					if pkIdx < len(table.Columns) {
						col := table.Columns[pkIdx]
						row := map[string]any{
							"constraint_catalog":            is.databaseName,
							"constraint_schema":             schema.Name(),
							"constraint_name":               table.Name + "_pkey",
							"table_catalog":                 is.databaseName,
							"table_schema":                  schema.Name(),
							"table_name":                    table.Name,
							"column_name":                   col.Name,
							"ordinal_position":              i + 1,
							"position_in_unique_constraint": nil, // Not a foreign key
						}
						if matchesFilters(row, filters) {
							result.Rows = append(result.Rows, row)
						}
					}
				}
			}

			// Add unique index columns
			indexes := is.catalog.GetIndexesForTable(schema.Name(), table.Name)
			for _, idx := range indexes {
				if idx.IsUnique && !idx.IsPrimary {
					for i, colName := range idx.Columns {
						row := map[string]any{
							"constraint_catalog":            is.databaseName,
							"constraint_schema":             schema.Name(),
							"constraint_name":               idx.Name,
							"table_catalog":                 is.databaseName,
							"table_schema":                  schema.Name(),
							"table_name":                    table.Name,
							"column_name":                   colName,
							"ordinal_position":              i + 1,
							"position_in_unique_constraint": nil,
						}
						if matchesFilters(row, filters) {
							result.Rows = append(result.Rows, row)
						}
					}
				}
			}
		}
	}

	return result
}

// sequencesColumns defines the columns for information_schema.sequences.
// Reference: https://www.postgresql.org/docs/current/infoschema-sequences.html
var sequencesColumns = []string{
	"sequence_catalog",
	"sequence_schema",
	"sequence_name",
	"data_type",
	"numeric_precision",
	"numeric_precision_radix",
	"numeric_scale",
	"start_value",
	"minimum_value",
	"maximum_value",
	"increment",
	"cycle_option",
}

// querySequences returns data for information_schema.sequences.
func (is *InformationSchema) querySequences(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: sequencesColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get sequences in this schema
		sequences := schema.ListSequences()
		for _, seq := range sequences {
			cycleOption := "NO"
			if seq.IsCycle {
				cycleOption = "YES"
			}

			row := map[string]any{
				"sequence_catalog":       is.databaseName,
				"sequence_schema":        schema.Name(),
				"sequence_name":          seq.Name,
				"data_type":              "bigint",
				"numeric_precision":      64,
				"numeric_precision_radix": 2,
				"numeric_scale":          0,
				"start_value":            intToString(seq.StartWith),
				"minimum_value":          intToString(seq.MinValue),
				"maximum_value":          intToString(seq.MaxValue),
				"increment":              intToString(seq.IncrementBy),
				"cycle_option":           cycleOption,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}
