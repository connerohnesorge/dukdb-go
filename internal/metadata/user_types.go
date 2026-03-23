package metadata

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TypeMetadata represents a row in duckdb_types().
type TypeMetadata struct {
	DatabaseName string
	SchemaName   string
	TypeName     string
	TypeSize     int64
	TypeCategory string
	Internal     bool
	SQL          string
}

// GetTypes returns type metadata for all built-in and user-defined types.
func GetTypes(cat *catalog.Catalog, _ *storage.Storage, databaseName string) []TypeMetadata {
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	result := make([]TypeMetadata, 0)

	// Built-in types
	builtinTypes := []struct {
		name     string
		size     int64
		category string
	}{
		{"BOOLEAN", 1, "BOOLEAN"},
		{"TINYINT", 1, "NUMERIC"},
		{"SMALLINT", 2, "NUMERIC"},
		{"INTEGER", 4, "NUMERIC"},
		{"BIGINT", 8, "NUMERIC"},
		{"HUGEINT", 16, "NUMERIC"},
		{"UTINYINT", 1, "NUMERIC"},
		{"USMALLINT", 2, "NUMERIC"},
		{"UINTEGER", 4, "NUMERIC"},
		{"UBIGINT", 8, "NUMERIC"},
		{"FLOAT", 4, "NUMERIC"},
		{"DOUBLE", 8, "NUMERIC"},
		{"DECIMAL", -1, "NUMERIC"},
		{"VARCHAR", -1, "STRING"},
		{"BLOB", -1, "STRING"},
		{"DATE", 4, "DATETIME"},
		{"TIME", 8, "DATETIME"},
		{"TIMESTAMP", 8, "DATETIME"},
		{"TIMESTAMP WITH TIME ZONE", 8, "DATETIME"},
		{"INTERVAL", 16, "DATETIME"},
		{"UUID", 16, "STRING"},
		{"JSON", -1, "STRING"},
		{"LIST", -1, "NESTED"},
		{"MAP", -1, "NESTED"},
		{"STRUCT", -1, "NESTED"},
		{"UNION", -1, "NESTED"},
		{"ARRAY", -1, "NESTED"},
		{"BIT", -1, "STRING"},
	}

	for _, t := range builtinTypes {
		result = append(result, TypeMetadata{
			DatabaseName: dbName,
			SchemaName:   "main",
			TypeName:     t.name,
			TypeSize:     t.size,
			TypeCategory: t.category,
			Internal:     true,
			SQL:          "",
		})
	}

	// User-created types (ENUMs)
	if cat != nil {
		for _, entry := range cat.ListTypes() {
			sql := ""
			if entry.TypeKind == "ENUM" && len(entry.EnumValues) > 0 {
				quoted := make([]string, len(entry.EnumValues))
				for i, v := range entry.EnumValues {
					quoted[i] = fmt.Sprintf("'%s'", v)
				}
				sql = fmt.Sprintf("CREATE TYPE %s AS ENUM(%s);", entry.Name, strings.Join(quoted, ", "))
			}
			result = append(result, TypeMetadata{
				DatabaseName: dbName,
				SchemaName:   "main",
				TypeName:     entry.Name,
				TypeSize:     -1,
				TypeCategory: "COMPOSITE",
				Internal:     false,
				SQL:          sql,
			})
		}
	}

	return result
}

// DuckDBTypesColumns returns column definitions for duckdb_types().
func DuckDBTypesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("type_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("type_size", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("type_category", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("internal", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("sql", dukdb.TYPE_VARCHAR),
	}
}
