package metadata

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// SchemaMetadata represents a row in duckdb_schemas().
type SchemaMetadata struct {
	DatabaseName string
	SchemaName   string
	SchemaOID    int64
	Internal     bool
	SQL          string
}

// GetSchemas returns schema metadata.
func GetSchemas(cat *catalog.Catalog, _ *storage.Storage, databaseName string) []SchemaMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	result := make([]SchemaMetadata, 0)

	// Add built-in schemas
	builtins := []struct {
		name string
		oid  int64
	}{
		{"main", 1},
		{"information_schema", 2},
		{"pg_catalog", 3},
	}
	for _, b := range builtins {
		result = append(result, SchemaMetadata{
			DatabaseName: dbName,
			SchemaName:   b.name,
			SchemaOID:    b.oid,
			Internal:     true,
			SQL:          "",
		})
	}

	// Add user-created schemas
	oid := int64(100)
	schemas := cat.ListSchemas()
	for _, s := range schemas {
		name := s.Name()
		if name == "main" || name == "information_schema" || name == "pg_catalog" || name == "temp" {
			continue
		}
		result = append(result, SchemaMetadata{
			DatabaseName: dbName,
			SchemaName:   name,
			SchemaOID:    oid,
			Internal:     false,
			SQL:          fmt.Sprintf("CREATE SCHEMA %s;", name),
		})
		oid++
	}
	return result
}

// DuckDBSchemasColumns returns column definitions for duckdb_schemas().
func DuckDBSchemasColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_oid", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("internal", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("sql", dukdb.TYPE_VARCHAR),
	}
}
