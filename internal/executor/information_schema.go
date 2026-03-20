package executor

import (
	"fmt"
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// dukdbTypeToInfoSchemaType converts a dukdb type to a SQL standard type name
// suitable for information_schema.columns data_type column.
func dukdbTypeToInfoSchemaType(col *catalog.ColumnDef) string {
	return strings.ToUpper(col.Type.String())
}

func (e *Executor) executeInformationSchemaTables(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"table_catalog", "table_schema", "table_name", "table_type",
		"self_referencing_column_name", "reference_generation",
		"user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name",
		"is_insertable_into", "is_typed", "commit_action",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		schemaName := schema.Name()

		// Tables
		for _, table := range schema.ListTables() {
			rows = append(rows, map[string]any{
				"table_catalog":                "memory",
				"table_schema":                 schemaName,
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
			})
		}

		// Views
		for _, view := range schema.ListViews() {
			rows = append(rows, map[string]any{
				"table_catalog":                "memory",
				"table_schema":                 schemaName,
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
			})
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeInformationSchemaColumns(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"table_catalog", "table_schema", "table_name", "column_name",
		"ordinal_position", "column_default", "is_nullable", "data_type",
		"character_maximum_length", "numeric_precision", "numeric_scale",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		schemaName := schema.Name()

		for _, table := range schema.ListTables() {
			for i, col := range table.Columns {
				isNullable := "YES"
				if !col.Nullable {
					isNullable = "NO"
				}

				rows = append(rows, map[string]any{
					"table_catalog":            "memory",
					"table_schema":             schemaName,
					"table_name":               table.Name,
					"column_name":              col.Name,
					"ordinal_position":         int64(i + 1),
					"column_default":           nil,
					"is_nullable":              isNullable,
					"data_type":                dukdbTypeToInfoSchemaType(col),
					"character_maximum_length": nil,
					"numeric_precision":        nil,
					"numeric_scale":            nil,
				})
			}
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeInformationSchemaSchemata(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"catalog_name", "schema_name", "schema_owner",
		"default_character_set_catalog", "default_character_set_schema",
		"default_character_set_name", "sql_path",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		rows = append(rows, map[string]any{
			"catalog_name":                  "memory",
			"schema_name":                   schema.Name(),
			"schema_owner":                  nil,
			"default_character_set_catalog": nil,
			"default_character_set_schema":  nil,
			"default_character_set_name":    nil,
			"sql_path":                      nil,
		})
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeInformationSchemaViews(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"table_catalog", "table_schema", "table_name",
		"view_definition", "check_option", "is_updatable",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		schemaName := schema.Name()

		for _, view := range schema.ListViews() {
			rows = append(rows, map[string]any{
				"table_catalog":   "memory",
				"table_schema":    schemaName,
				"table_name":      view.Name,
				"view_definition": view.Query,
				"check_option":    "NONE",
				"is_updatable":    "NO",
			})
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeInformationSchemaTableConstraints(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"constraint_catalog", "constraint_schema", "constraint_name",
		"table_catalog", "table_schema", "table_name", "constraint_type",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		schemaName := schema.Name()

		for _, table := range schema.ListTables() {
			// Primary key constraint
			if table.HasPrimaryKey() {
				constraintName := table.Name + "_pkey"
				rows = append(rows, map[string]any{
					"constraint_catalog": "memory",
					"constraint_schema":  schemaName,
					"constraint_name":    constraintName,
					"table_catalog":      "memory",
					"table_schema":       schemaName,
					"table_name":         table.Name,
					"constraint_type":    "PRIMARY KEY",
				})
			}

			// Other constraints (UNIQUE, CHECK)
			for i, c := range table.Constraints {
				switch ct := c.(type) {
				case *catalog.UniqueConstraintDef:
					constraintName := ct.Name
					if constraintName == "" {
						constraintName = fmt.Sprintf("%s_unique_%d", table.Name, i)
					}
					rows = append(rows, map[string]any{
						"constraint_catalog": "memory",
						"constraint_schema":  schemaName,
						"constraint_name":    constraintName,
						"table_catalog":      "memory",
						"table_schema":       schemaName,
						"table_name":         table.Name,
						"constraint_type":    "UNIQUE",
					})
				case *catalog.CheckConstraintDef:
					constraintName := ct.Name
					if constraintName == "" {
						constraintName = fmt.Sprintf("%s_check_%d", table.Name, i)
					}
					rows = append(rows, map[string]any{
						"constraint_catalog": "memory",
						"constraint_schema":  schemaName,
						"constraint_name":    constraintName,
						"table_catalog":      "memory",
						"table_schema":       schemaName,
						"table_name":         table.Name,
						"constraint_type":    "CHECK",
					})
				}
			}
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeInformationSchemaKeyColumnUsage(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{
		"constraint_catalog", "constraint_schema", "constraint_name",
		"table_catalog", "table_schema", "table_name",
		"column_name", "ordinal_position",
	}

	rows := make([]map[string]any, 0)

	schemas := e.catalog.ListSchemas()
	for _, schema := range schemas {
		schemaName := schema.Name()

		for _, table := range schema.ListTables() {
			// Primary key columns
			if table.HasPrimaryKey() {
				constraintName := table.Name + "_pkey"
				for ordinal, colIdx := range table.PrimaryKey {
					if colIdx < len(table.Columns) {
						rows = append(rows, map[string]any{
							"constraint_catalog": "memory",
							"constraint_schema":  schemaName,
							"constraint_name":    constraintName,
							"table_catalog":      "memory",
							"table_schema":       schemaName,
							"table_name":         table.Name,
							"column_name":        table.Columns[colIdx].Name,
							"ordinal_position":   int64(ordinal + 1),
						})
					}
				}
			}

			// Unique constraint columns
			for i, c := range table.Constraints {
				if ct, ok := c.(*catalog.UniqueConstraintDef); ok {
					constraintName := ct.Name
					if constraintName == "" {
						constraintName = fmt.Sprintf("%s_unique_%d", table.Name, i)
					}
					for ordinal, colName := range ct.Columns {
						rows = append(rows, map[string]any{
							"constraint_catalog": "memory",
							"constraint_schema":  schemaName,
							"constraint_name":    constraintName,
							"table_catalog":      "memory",
							"table_schema":       schemaName,
							"table_name":         table.Name,
							"column_name":        colName,
							"ordinal_position":   int64(ordinal + 1),
						})
					}
				}
			}
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}
