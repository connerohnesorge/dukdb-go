package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// columnsColumns defines the columns for information_schema.columns.
// These are the columns that PostgreSQL returns for this view.
// Reference: https://www.postgresql.org/docs/current/infoschema-columns.html
var columnsColumns = []string{
	"table_catalog",
	"table_schema",
	"table_name",
	"column_name",
	"ordinal_position",
	"column_default",
	"is_nullable",
	"data_type",
	"character_maximum_length",
	"character_octet_length",
	"numeric_precision",
	"numeric_precision_radix",
	"numeric_scale",
	"datetime_precision",
	"interval_type",
	"interval_precision",
	"character_set_catalog",
	"character_set_schema",
	"character_set_name",
	"collation_catalog",
	"collation_schema",
	"collation_name",
	"domain_catalog",
	"domain_schema",
	"domain_name",
	"udt_catalog",
	"udt_schema",
	"udt_name",
	"scope_catalog",
	"scope_schema",
	"scope_name",
	"maximum_cardinality",
	"dtd_identifier",
	"is_self_referencing",
	"is_identity",
	"identity_generation",
	"identity_start",
	"identity_increment",
	"identity_maximum",
	"identity_minimum",
	"identity_cycle",
	"is_generated",
	"generation_expression",
	"is_updatable",
}

// queryColumns returns data for information_schema.columns.
// This includes columns from both tables and views.
func (is *InformationSchema) queryColumns(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: columnsColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := is.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get columns from tables
		tables := is.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			for i, col := range table.Columns {
				row := buildColumnRow(
					is.databaseName,
					schema.Name(),
					table.Name,
					col.Name,
					i+1, // ordinal_position is 1-based
					col.Type,
					col.Nullable,
					col.DefaultValue,
					col.HasDefault,
				)

				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}
		}

		// Note: Views don't have direct column definitions in our catalog.
		// In a full implementation, we would parse the view query to determine
		// output columns. For now, views are excluded from columns view.
	}

	return result
}

// buildColumnRow constructs a row for information_schema.columns.
func buildColumnRow(
	catalog, schema, table, column string,
	ordinal int,
	typ dukdb.Type,
	nullable bool,
	defaultValue any,
	hasDefault bool,
) map[string]any {
	// Determine nullability string
	isNullable := "NO"
	if nullable {
		isNullable = "YES"
	}

	// Determine default value string
	var columnDefault any
	if hasDefault && defaultValue != nil {
		columnDefault = toString(defaultValue)
	}

	// Get type information
	dataType := dukdbTypeToSQLType(typ)
	numericPrecision, numericScale, numericRadix := getNumericInfo(typ)
	charMaxLen, charOctetLen := getCharacterInfo(typ)
	datetimePrecision := getDatetimePrecision(typ)

	// UDT (User Defined Type) name - for PostgreSQL, this is the base type name
	udtName := getUDTName(typ)

	return map[string]any{
		"table_catalog":            catalog,
		"table_schema":             schema,
		"table_name":               table,
		"column_name":              column,
		"ordinal_position":         ordinal,
		"column_default":           columnDefault,
		"is_nullable":              isNullable,
		"data_type":                dataType,
		"character_maximum_length": charMaxLen,
		"character_octet_length":   charOctetLen,
		"numeric_precision":        numericPrecision,
		"numeric_precision_radix":  numericRadix,
		"numeric_scale":            numericScale,
		"datetime_precision":       datetimePrecision,
		"interval_type":            nil,
		"interval_precision":       nil,
		"character_set_catalog":    nil,
		"character_set_schema":     nil,
		"character_set_name":       nil,
		"collation_catalog":        nil,
		"collation_schema":         nil,
		"collation_name":           nil,
		"domain_catalog":           nil,
		"domain_schema":            nil,
		"domain_name":              nil,
		"udt_catalog":              catalog,
		"udt_schema":               "pg_catalog",
		"udt_name":                 udtName,
		"scope_catalog":            nil,
		"scope_schema":             nil,
		"scope_name":               nil,
		"maximum_cardinality":      nil,
		"dtd_identifier":           intToString(int64(ordinal)),
		"is_self_referencing":      "NO",
		"is_identity":              "NO",
		"identity_generation":      nil,
		"identity_start":           nil,
		"identity_increment":       nil,
		"identity_maximum":         nil,
		"identity_minimum":         nil,
		"identity_cycle":           "NO",
		"is_generated":             "NEVER",
		"generation_expression":    nil,
		"is_updatable":             "YES",
	}
}

// getNumericInfo returns precision, scale, and radix for numeric types.
func getNumericInfo(typ dukdb.Type) (precision any, scale any, radix any) {
	switch typ {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return 8, 0, 2
	case dukdb.TYPE_SMALLINT, dukdb.TYPE_USMALLINT:
		return 16, 0, 2
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER:
		return 32, 0, 2
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT:
		return 64, 0, 2
	case dukdb.TYPE_HUGEINT:
		return 128, 0, 2
	case dukdb.TYPE_FLOAT:
		return 24, nil, 2
	case dukdb.TYPE_DOUBLE:
		return 53, nil, 2
	case dukdb.TYPE_DECIMAL:
		// Default decimal precision - in a full implementation, we would get
		// the actual precision from TypeInfo
		return 18, 3, 10
	default:
		return nil, nil, nil
	}
}

// getCharacterInfo returns character_maximum_length and character_octet_length.
func getCharacterInfo(typ dukdb.Type) (maxLen any, octetLen any) {
	switch typ {
	case dukdb.TYPE_VARCHAR:
		// Variable length - no maximum
		return nil, nil
	case dukdb.TYPE_UUID:
		// UUID is 36 characters when formatted
		return 36, 36
	default:
		return nil, nil
	}
}

// getDatetimePrecision returns the datetime precision for temporal types.
func getDatetimePrecision(typ dukdb.Type) any {
	switch typ {
	case dukdb.TYPE_DATE:
		return 0
	case dukdb.TYPE_TIME, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ:
		return 6 // microsecond precision
	case dukdb.TYPE_INTERVAL:
		return 6
	default:
		return nil
	}
}

// getUDTName returns the PostgreSQL UDT (User Defined Type) name for a type.
// This is used in the udt_name column.
func getUDTName(typ dukdb.Type) string {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return "bool"
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_UTINYINT:
		return "int2"
	case dukdb.TYPE_INTEGER, dukdb.TYPE_USMALLINT:
		return "int4"
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UINTEGER:
		return "int8"
	case dukdb.TYPE_UBIGINT, dukdb.TYPE_DECIMAL, dukdb.TYPE_HUGEINT:
		return "numeric"
	case dukdb.TYPE_FLOAT:
		return "float4"
	case dukdb.TYPE_DOUBLE:
		return "float8"
	case dukdb.TYPE_VARCHAR:
		return "varchar"
	case dukdb.TYPE_BLOB:
		return "bytea"
	case dukdb.TYPE_DATE:
		return "date"
	case dukdb.TYPE_TIME:
		return "time"
	case dukdb.TYPE_TIMESTAMP:
		return "timestamp"
	case dukdb.TYPE_TIMESTAMP_TZ:
		return "timestamptz"
	case dukdb.TYPE_INTERVAL:
		return "interval"
	case dukdb.TYPE_UUID:
		return "uuid"
	case dukdb.TYPE_JSON:
		return "json"
	default:
		return "text"
	}
}
