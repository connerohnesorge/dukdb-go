package metadata

import (
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// GetTables returns table metadata for all schemas.
func GetTables(cat *catalog.Catalog, stor *storage.Storage, databaseName string) []TableMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]TableMetadata, 0)
	for _, schema := range schemas {
		tables := schema.ListTables()
		for _, table := range tables {
			rowCount := int64(0)
			estimatedSize := int64(0)

			if table.Statistics != nil {
				if table.Statistics.RowCount > 0 {
					rowCount = table.Statistics.RowCount
				}
				if table.Statistics.DataSizeBytes > 0 {
					estimatedSize = table.Statistics.DataSizeBytes
				}
			}

			if stor != nil && strings.EqualFold(schema.Name(), DefaultSchemaName) {
				if dataTable, ok := stor.GetTable(table.Name); ok {
					rowCount = dataTable.RowCount()
					if estimatedSize == 0 {
						estimatedSize = rowCount * 100
					}
				}
			}

			result = append(result, TableMetadata{
				DatabaseName:  dbName,
				SchemaName:    schema.Name(),
				TableName:     table.Name,
				TableType:     "BASE TABLE",
				RowCount:      rowCount,
				EstimatedSize: estimatedSize,
				ColumnCount:   len(table.Columns),
				HasPrimaryKey: table.HasPrimaryKey(),
			})
		}
	}

	return result
}
