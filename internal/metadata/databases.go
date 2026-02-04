package metadata

import "github.com/dukdb/dukdb-go/internal/storage"

// GetDatabases returns database metadata.
func GetDatabases(stor *storage.Storage, databaseName string) []DatabaseMetadata {
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	totalSize := int64(0)
	if stor != nil {
		tables := stor.Tables()
		for _, table := range tables {
			totalSize += table.RowCount() * 100
		}
	}

	return []DatabaseMetadata{
		{
			DatabaseName: dbName,
			DatabaseOID:  1,
			DatabaseSize: totalSize,
			DatabaseType: "memory",
		},
	}
}
