package metadata

import "github.com/dukdb/dukdb-go/internal/catalog"

// GetSequences returns sequence metadata for all schemas.
func GetSequences(cat *catalog.Catalog, databaseName string) []SequenceMetadata {
	if cat == nil {
		return nil
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	schemas := cat.ListSchemas()
	result := make([]SequenceMetadata, 0)
	for _, schema := range schemas {
		sequences := schema.ListSequences()
		for _, seq := range sequences {
			result = append(result, SequenceMetadata{
				DatabaseName: dbName,
				SchemaName:   schema.Name(),
				SequenceName: seq.Name,
				StartValue:   seq.StartWith,
				IncrementBy:  seq.IncrementBy,
				MinValue:     seq.MinValue,
				MaxValue:     seq.MaxValue,
				IsCycle:      seq.IsCycle,
				CurrentValue: seq.GetCurrentVal(),
			})
		}
	}

	return result
}
