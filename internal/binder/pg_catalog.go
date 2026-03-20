package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// bindPgCatalogTable handles references to pg_catalog.* virtual views.
// These views provide PostgreSQL-compatible metadata access about the database catalog.
func (b *Binder) bindPgCatalogTable(ref parser.TableRef) (*BoundTableRef, error) {
	tableName := strings.ToLower(ref.TableName)
	alias := ref.Alias
	if alias == "" {
		alias = tableName
	}

	var columns []*catalog.ColumnDef

	switch tableName {
	case "pg_namespace":
		columns = pgCatalogNamespace()
	case "pg_class":
		columns = pgCatalogClass()
	case "pg_attribute":
		columns = pgCatalogAttribute()
	case "pg_type":
		columns = pgCatalogType()
	case "pg_tables":
		columns = pgCatalogTables()
	case "pg_views":
		columns = pgCatalogViews()
	case "pg_index":
		columns = pgCatalogIndex()
	case "pg_constraint":
		columns = pgCatalogConstraint()
	case "pg_database":
		columns = pgCatalogDatabase()
	case "pg_settings":
		columns = pgCatalogSettings()
	case "pg_roles", "pg_user":
		columns = pgCatalogRoles()
	default:
		return nil, b.errorf("unknown pg_catalog view: %s", tableName)
	}

	// Create as a system table function so the executor can dispatch it.
	boundFunc := &BoundTableFunctionRef{
		Name:    "pg_catalog_" + tableName,
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

func pgCatalogNamespace() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("nspname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("nspowner", dukdb.TYPE_INTEGER),
	}
}

func pgCatalogClass() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("relname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("relnamespace", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("relkind", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("relowner", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("reltuples", dukdb.TYPE_FLOAT),
	}
}

func pgCatalogAttribute() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("attrelid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("attname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("atttypid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("attnum", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("attnotnull", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("attisdropped", dukdb.TYPE_BOOLEAN),
	}
}

func pgCatalogType() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("typname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("typnamespace", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("typlen", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("typtype", dukdb.TYPE_VARCHAR),
	}
}

func pgCatalogTables() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("schemaname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("tablename", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("tableowner", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("hasindexes", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("hasrules", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("hastriggers", dukdb.TYPE_BOOLEAN),
	}
}

func pgCatalogViews() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("schemaname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("viewname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("viewowner", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("definition", dukdb.TYPE_VARCHAR),
	}
}

func pgCatalogIndex() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("indexrelid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("indrelid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("indnatts", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("indisunique", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("indisprimary", dukdb.TYPE_BOOLEAN),
	}
}

func pgCatalogConstraint() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("conname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("connamespace", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("contype", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("conrelid", dukdb.TYPE_INTEGER),
	}
}

func pgCatalogDatabase() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("datname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("datdba", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("encoding", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("datcollate", dukdb.TYPE_VARCHAR),
	}
}

func pgCatalogSettings() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("setting", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("description", dukdb.TYPE_VARCHAR),
	}
}

func pgCatalogRoles() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("oid", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("rolname", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("rolsuper", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("rolcreatedb", dukdb.TYPE_BOOLEAN),
	}
}
