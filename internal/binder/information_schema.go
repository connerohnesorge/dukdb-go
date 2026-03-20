package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// bindInformationSchemaTable handles references to information_schema.* virtual views.
// These views provide SQL-standard metadata access about the database catalog.
func (b *Binder) bindInformationSchemaTable(ref parser.TableRef) (*BoundTableRef, error) {
	tableName := strings.ToLower(ref.TableName)
	alias := ref.Alias
	if alias == "" {
		alias = tableName
	}

	var columns []*catalog.ColumnDef

	switch tableName {
	case "tables":
		columns = informationSchemaTables()
	case "columns":
		columns = informationSchemaColumns()
	case "schemata":
		columns = informationSchemaSchemata()
	case "views":
		columns = informationSchemaViews()
	case "table_constraints":
		columns = informationSchemaTableConstraints()
	case "key_column_usage":
		columns = informationSchemaKeyColumnUsage()
	default:
		return nil, b.errorf("unknown information_schema view: %s", tableName)
	}

	// Create as a system table function so the executor can dispatch it.
	boundFunc := &BoundTableFunctionRef{
		Name:    "information_schema_" + tableName,
		Options: make(map[string]any),
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     tableName,
		Alias:         alias,
		TableFunction: boundFunc,
	}

	for i, col := range columns {
		boundRef.Columns = append(boundRef.Columns, &BoundColumn{
			Table:      alias,
			Column:     col.Name,
			ColumnIdx:  i,
			Type:       col.Type,
			SourceType: "table_function",
		})
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = tableName

	return boundRef, nil
}

func informationSchemaTables() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("table_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("self_referencing_column_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("reference_generation", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("user_defined_type_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("user_defined_type_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("user_defined_type_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_insertable_into", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_typed", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("commit_action", dukdb.TYPE_VARCHAR),
	}
}

func informationSchemaColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("table_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("column_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("ordinal_position", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("column_default", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_nullable", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("data_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("character_maximum_length", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("numeric_precision", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("numeric_scale", dukdb.TYPE_INTEGER),
	}
}

func informationSchemaSchemata() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("catalog_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_owner", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("default_character_set_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("default_character_set_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("default_character_set_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("sql_path", dukdb.TYPE_VARCHAR),
	}
}

func informationSchemaViews() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("table_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("view_definition", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("check_option", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_updatable", dukdb.TYPE_VARCHAR),
	}
}

func informationSchemaTableConstraints() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("constraint_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_type", dukdb.TYPE_VARCHAR),
	}
}

func informationSchemaKeyColumnUsage() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("constraint_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_catalog", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_schema", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("column_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("ordinal_position", dukdb.TYPE_INTEGER),
	}
}
