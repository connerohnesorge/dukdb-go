package metadata

import "github.com/dukdb/dukdb-go/internal/catalog"

// GetViews returns view metadata for all schemas.
func GetViews(cat *catalog.Catalog, databaseName string) []ViewMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]ViewMetadata, 0)
	for _, schema := range schemas {
		views := schema.ListViews()
		for _, view := range views {
			result = append(result, ViewMetadata{
				DatabaseName:   dbName,
				SchemaName:     schema.Name(),
				ViewName:       view.Name,
				ViewDefinition: view.Query,
			})
		}
	}

	return result
}
