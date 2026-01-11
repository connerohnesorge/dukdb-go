package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
)

func TestSchemaMapper_MapType_Primitives(t *testing.T) {
	mapper := NewSchemaMapper()

	tests := []struct {
		name        string
		icebergType iceberg.Type
		expected    dukdb.Type
	}{
		{"boolean", iceberg.BooleanType{}, dukdb.TYPE_BOOLEAN},
		{"int32", iceberg.Int32Type{}, dukdb.TYPE_INTEGER},
		{"int64", iceberg.Int64Type{}, dukdb.TYPE_BIGINT},
		{"float32", iceberg.Float32Type{}, dukdb.TYPE_FLOAT},
		{"float64", iceberg.Float64Type{}, dukdb.TYPE_DOUBLE},
		{"string", iceberg.StringType{}, dukdb.TYPE_VARCHAR},
		{"binary", iceberg.BinaryType{}, dukdb.TYPE_BLOB},
		{"date", iceberg.DateType{}, dukdb.TYPE_DATE},
		{"time", iceberg.TimeType{}, dukdb.TYPE_TIME},
		{"timestamp", iceberg.TimestampType{}, dukdb.TYPE_TIMESTAMP},
		{"timestamptz", iceberg.TimestampTzType{}, dukdb.TYPE_TIMESTAMP_TZ},
		{"uuid", iceberg.UUIDType{}, dukdb.TYPE_UUID},
		{"fixed", iceberg.FixedType{}, dukdb.TYPE_BLOB},
		{"decimal", iceberg.DecimalType{}, dukdb.TYPE_DECIMAL},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := mapper.MapType(tc.icebergType)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSchemaMapper_MapType_Nested(t *testing.T) {
	mapper := NewSchemaMapper()

	tests := []struct {
		name        string
		icebergType iceberg.Type
		expected    dukdb.Type
	}{
		{"struct", &iceberg.StructType{}, dukdb.TYPE_STRUCT},
		{"list", &iceberg.ListType{}, dukdb.TYPE_LIST},
		{"map", &iceberg.MapType{}, dukdb.TYPE_MAP},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := mapper.MapType(tc.icebergType)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSchemaMapper_MapType_Nil(t *testing.T) {
	mapper := NewSchemaMapper()

	_, err := mapper.MapType(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

func TestSchemaMapper_MapSchema(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	mapper := NewSchemaMapper()
	names, types, err := mapper.MapSchema(metadata.CurrentSchema())
	require.NoError(t, err)

	assert.Len(t, names, 3)
	assert.Len(t, types, 3)

	assert.Equal(t, "id", names[0])
	assert.Equal(t, dukdb.TYPE_BIGINT, types[0])

	assert.Equal(t, "name", names[1])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])

	assert.Equal(t, "created_at", names[2])
	assert.Equal(t, dukdb.TYPE_TIMESTAMP, types[2])
}

func TestSchemaMapper_MapSchemaToColumnInfo(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	mapper := NewSchemaMapper()
	columns, err := mapper.MapSchemaToColumnInfo(metadata.CurrentSchema())
	require.NoError(t, err)

	assert.Len(t, columns, 3)

	// Check first column
	assert.Equal(t, 1, columns[0].ID)
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, dukdb.TYPE_BIGINT, columns[0].Type)
	assert.True(t, columns[0].Required)

	// Check second column
	assert.Equal(t, 2, columns[1].ID)
	assert.Equal(t, "name", columns[1].Name)
	assert.Equal(t, dukdb.TYPE_VARCHAR, columns[1].Type)
	assert.False(t, columns[1].Required)
}

func TestSchemaMapper_FindColumnByID(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	mapper := NewSchemaMapper()

	// Find existing column
	col, err := mapper.FindColumnByID(metadata.CurrentSchema(), 2)
	require.NoError(t, err)
	assert.Equal(t, "name", col.Name)

	// Find non-existing column
	_, err = mapper.FindColumnByID(metadata.CurrentSchema(), 999)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSchemaNotFound)
}

func TestSchemaMapper_FindColumnByName(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	mapper := NewSchemaMapper()

	// Find existing column
	col, err := mapper.FindColumnByName(metadata.CurrentSchema(), "name")
	require.NoError(t, err)
	assert.Equal(t, 2, col.ID)

	// Find non-existing column
	_, err = mapper.FindColumnByName(metadata.CurrentSchema(), "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSchemaNotFound)
}

func TestSchemaMapper_ProjectSchema(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	mapper := NewSchemaMapper()

	// Project subset of columns
	projected, err := mapper.ProjectSchema(metadata.CurrentSchema(), []string{"id", "name"})
	require.NoError(t, err)
	assert.Len(t, projected, 2)
	assert.Equal(t, "id", projected[0].Name)
	assert.Equal(t, "name", projected[1].Name)

	// Project non-existing column
	_, err = mapper.ProjectSchema(metadata.CurrentSchema(), []string{"id", "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSchemaNotFound)
}

// Schema evolution test metadata
const schemaEvolutionOldJSON = `{
  "format-version": 2,
  "table-uuid": "11111111-1111-1111-1111-111111111111",
  "location": "s3://bucket/table",
  "last-sequence-number": 1,
  "last-updated-ms": 1672531200000,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "old_name", "required": false, "type": "string"},
        {"id": 3, "name": "dropped_col", "required": false, "type": "int"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": []
}`

const schemaEvolutionNewJSON = `{
  "format-version": 2,
  "table-uuid": "22222222-2222-2222-2222-222222222222",
  "location": "s3://bucket/table",
  "last-sequence-number": 2,
  "last-updated-ms": 1672617600000,
  "last-column-id": 4,
  "current-schema-id": 1,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "new_name", "required": false, "type": "string"},
        {"id": 4, "name": "added_col", "required": false, "type": "double"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": []
}`

func TestSchemaEvolutionChecker_GetAddedColumns(t *testing.T) {
	oldMeta, err := ParseMetadataBytes([]byte(schemaEvolutionOldJSON))
	require.NoError(t, err)

	newMeta, err := ParseMetadataBytes([]byte(schemaEvolutionNewJSON))
	require.NoError(t, err)

	checker := NewSchemaEvolutionChecker()
	added, err := checker.GetAddedColumns(oldMeta.CurrentSchema(), newMeta.CurrentSchema())
	require.NoError(t, err)

	assert.Len(t, added, 1)
	assert.Equal(t, "added_col", added[0].Name)
	assert.Equal(t, 4, added[0].ID)
}

func TestSchemaEvolutionChecker_GetDroppedColumns(t *testing.T) {
	oldMeta, err := ParseMetadataBytes([]byte(schemaEvolutionOldJSON))
	require.NoError(t, err)

	newMeta, err := ParseMetadataBytes([]byte(schemaEvolutionNewJSON))
	require.NoError(t, err)

	checker := NewSchemaEvolutionChecker()
	dropped, err := checker.GetDroppedColumns(oldMeta.CurrentSchema(), newMeta.CurrentSchema())
	require.NoError(t, err)

	assert.Len(t, dropped, 1)
	assert.Equal(t, "dropped_col", dropped[0].Name)
	assert.Equal(t, 3, dropped[0].ID)
}

func TestSchemaEvolutionChecker_GetRenamedColumns(t *testing.T) {
	oldMeta, err := ParseMetadataBytes([]byte(schemaEvolutionOldJSON))
	require.NoError(t, err)

	newMeta, err := ParseMetadataBytes([]byte(schemaEvolutionNewJSON))
	require.NoError(t, err)

	checker := NewSchemaEvolutionChecker()
	renamed, err := checker.GetRenamedColumns(oldMeta.CurrentSchema(), newMeta.CurrentSchema())
	require.NoError(t, err)

	assert.Len(t, renamed, 1)
	assert.Equal(t, "new_name", renamed["old_name"])
}

func TestSchemaEvolutionChecker_IsCompatible(t *testing.T) {
	oldMeta, err := ParseMetadataBytes([]byte(schemaEvolutionOldJSON))
	require.NoError(t, err)

	newMeta, err := ParseMetadataBytes([]byte(schemaEvolutionNewJSON))
	require.NoError(t, err)

	checker := NewSchemaEvolutionChecker()

	// Old to new should be compatible (column types unchanged or column dropped)
	assert.True(t, checker.IsCompatible(oldMeta.CurrentSchema(), newMeta.CurrentSchema()))
}
