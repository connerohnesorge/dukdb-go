package metadata

import "github.com/dukdb/dukdb-go/internal/catalog"

// GetIndexes returns index metadata for all schemas.
func GetIndexes(cat *catalog.Catalog, databaseName string) []IndexMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]IndexMetadata, 0)
	for _, schema := range schemas {
		indexes := schema.ListIndexes()
		for _, idx := range indexes {
			result = append(result, IndexMetadata{
				DatabaseName: dbName,
				SchemaName:   schema.Name(),
				TableName:    idx.Table,
				IndexName:    idx.Name,
				IsUnique:     idx.IsUnique,
				IsPrimary:    idx.IsPrimary,
				IndexColumns: JoinColumns(idx.Columns),
			})
		}
	}

	return result
}
