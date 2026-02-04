package metadata

import "github.com/dukdb/dukdb-go/internal/catalog"

// GetDependencies returns dependency metadata for views.
func GetDependencies(cat *catalog.Catalog, databaseName string) []DependencyMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]DependencyMetadata, 0)
	for _, schema := range schemas {
		views := schema.ListViews()
		for _, view := range views {
			for _, dep := range view.TableDependencies {
				result = append(result, DependencyMetadata{
					DatabaseName:   dbName,
					SchemaName:     schema.Name(),
					ObjectName:     view.Name,
					ObjectType:     "view",
					DependencyName: dep,
					DependencyType: "table",
				})
			}
		}
	}

	return result
}
