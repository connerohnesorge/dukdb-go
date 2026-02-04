package metadata

import (
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// GetConstraints returns constraint metadata for tables.
func GetConstraints(cat *catalog.Catalog, databaseName string) []ConstraintMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]ConstraintMetadata, 0)
	for _, schema := range schemas {
		tables := schema.ListTables()
		for _, table := range tables {
			if len(table.PrimaryKey) > 0 {
				columnNames := make([]string, 0, len(table.PrimaryKey))
				for _, idx := range table.PrimaryKey {
					if idx >= 0 && idx < len(table.Columns) {
						columnNames = append(columnNames, table.Columns[idx].Name)
					}
				}
				result = append(result, ConstraintMetadata{
					DatabaseName:     dbName,
					SchemaName:       schema.Name(),
					TableName:        table.Name,
					ConstraintName:   table.Name + "_pkey",
					ConstraintType:   "PRIMARY KEY",
					ConstraintColumn: JoinColumns(columnNames),
				})
			}
		}

		indexes := schema.ListIndexes()
		for _, idx := range indexes {
			if !idx.IsUnique || idx.IsPrimary {
				continue
			}
			result = append(result, ConstraintMetadata{
				DatabaseName:     dbName,
				SchemaName:       schema.Name(),
				TableName:        idx.Table,
				ConstraintName:   idx.Name,
				ConstraintType:   "UNIQUE",
				ConstraintColumn: JoinColumns(idx.Columns),
			})
		}
	}

	return result
}
