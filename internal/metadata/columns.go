package metadata

import (
	"fmt"

	"github.com/dukdb/dukdb-go/internal/catalog"
)

// GetColumns returns column metadata for all tables.
func GetColumns(cat *catalog.Catalog, databaseName string) []ColumnMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]ColumnMetadata, 0)
	for _, schema := range schemas {
		tables := schema.ListTables()
		for _, table := range tables {
			for idx, col := range table.Columns {
				defaultValue := ""
				if col.HasDefault {
					defaultValue = fmt.Sprint(col.DefaultValue)
				}

				result = append(result, ColumnMetadata{
					DatabaseName:  dbName,
					SchemaName:    schema.Name(),
					TableName:     table.Name,
					ColumnName:    col.Name,
					ColumnIndex:   idx,
					DataType:      FormatTypeInfo(col.GetTypeInfo()),
					IsNullable:    col.Nullable,
					ColumnDefault: defaultValue,
				})
			}
		}
	}

	return result
}
