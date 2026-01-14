package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// tablesColumns defines the columns for information_schema.tables.
// These are the columns that PostgreSQL returns for this view.
// Reference: https://www.postgresql.org/docs/current/infoschema-tables.html
var tablesColumns = []string{
	"table_catalog",
	"table_schema",
	"table_name",
	"table_type",
	"self_referencing_column_name",
	"reference_generation",
	"user_defined_type_catalog",
	"user_defined_type_schema",
	"user_defined_type_name",
	"is_insertable_into",
	"is_typed",
	"commit_action",
}

// queryTables returns data for information_schema.tables.
// This includes both tables and views.
func (is *InformationSchema) queryTables(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: tablesColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get tables in this schema
		tables := is.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			row := map[string]any{
				"table_catalog":                is.databaseName,
				"table_schema":                 schema.Name(),
				"table_name":                   table.Name,
				"table_type":                   "BASE TABLE",
				"self_referencing_column_name": nil,
				"reference_generation":         nil,
				"user_defined_type_catalog":    nil,
				"user_defined_type_schema":     nil,
				"user_defined_type_name":       nil,
				"is_insertable_into":           "YES",
				"is_typed":                     "NO",
				"commit_action":                nil,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}

		// Get views in this schema
		views := schema.ListViews()
		for _, view := range views {
			row := map[string]any{
				"table_catalog":                is.databaseName,
				"table_schema":                 schema.Name(),
				"table_name":                   view.Name,
				"table_type":                   "VIEW",
				"self_referencing_column_name": nil,
				"reference_generation":         nil,
				"user_defined_type_catalog":    nil,
				"user_defined_type_schema":     nil,
				"user_defined_type_name":       nil,
				"is_insertable_into":           "NO",
				"is_typed":                     "NO",
				"commit_action":                nil,
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}

// queryViews returns data for information_schema.views.
// This view contains metadata about views only.
// Reference: https://www.postgresql.org/docs/current/infoschema-views.html
var viewsColumns = []string{
	"table_catalog",
	"table_schema",
	"table_name",
	"view_definition",
	"check_option",
	"is_updatable",
	"is_insertable_into",
	"is_trigger_updatable",
	"is_trigger_deletable",
	"is_trigger_insertable_into",
}

func (is *InformationSchema) queryViews(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: viewsColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get views in this schema
		views := schema.ListViews()
		for _, view := range views {
			row := map[string]any{
				"table_catalog":              is.databaseName,
				"table_schema":               schema.Name(),
				"table_name":                 view.Name,
				"view_definition":            view.Query,
				"check_option":               "NONE",
				"is_updatable":               "NO",
				"is_insertable_into":         "NO",
				"is_trigger_updatable":       "NO",
				"is_trigger_deletable":       "NO",
				"is_trigger_insertable_into": "NO",
			}

			if matchesFilters(row, filters) {
				result.Rows = append(result.Rows, row)
			}
		}
	}

	return result
}

// dukdbTypeToSQLType converts a dukdb.Type to a PostgreSQL-compatible SQL type name.
func dukdbTypeToSQLType(typ dukdb.Type) string {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return "boolean"
	case dukdb.TYPE_TINYINT:
		return "smallint" // PostgreSQL doesn't have tinyint
	case dukdb.TYPE_SMALLINT:
		return "smallint"
	case dukdb.TYPE_INTEGER:
		return "integer"
	case dukdb.TYPE_BIGINT:
		return "bigint"
	case dukdb.TYPE_UTINYINT:
		return "smallint"
	case dukdb.TYPE_USMALLINT:
		return "integer"
	case dukdb.TYPE_UINTEGER:
		return "bigint"
	case dukdb.TYPE_UBIGINT:
		return "numeric"
	case dukdb.TYPE_FLOAT:
		return "real"
	case dukdb.TYPE_DOUBLE:
		return "double precision"
	case dukdb.TYPE_VARCHAR:
		return "character varying"
	case dukdb.TYPE_BLOB:
		return "bytea"
	case dukdb.TYPE_DATE:
		return "date"
	case dukdb.TYPE_TIME:
		return "time without time zone"
	case dukdb.TYPE_TIMESTAMP:
		return "timestamp without time zone"
	case dukdb.TYPE_TIMESTAMP_TZ:
		return "timestamp with time zone"
	case dukdb.TYPE_INTERVAL:
		return "interval"
	case dukdb.TYPE_DECIMAL:
		return "numeric"
	case dukdb.TYPE_UUID:
		return "uuid"
	case dukdb.TYPE_JSON:
		return "json"
	case dukdb.TYPE_HUGEINT:
		return "numeric"
	default:
		return "text" // Fallback for unknown types
	}
}
