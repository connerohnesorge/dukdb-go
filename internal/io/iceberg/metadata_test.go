package iceberg

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Sample Iceberg metadata.json content for testing (v2 format)
const sampleMetadataV2JSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-sequence-number": 10,
  "last-updated-ms": 1672531200000,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "name", "required": false, "type": "string"},
        {"id": 3, "name": "created_at", "required": false, "type": "timestamp"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 3, "field-id": 1000, "name": "created_at_day", "transform": "day"}
      ]
    }
  ],
  "last-partition-id": 1000,
  "default-sort-order-id": 0,
  "sort-orders": [
    {"order-id": 0, "fields": []}
  ],
  "properties": {
    "write.format.default": "parquet"
  },
  "current-snapshot-id": 3051729675574597004,
  "refs": {
    "main": {
      "snapshot-id": 3051729675574597004,
      "type": "branch"
    }
  },
  "snapshots": [
    {
      "snapshot-id": 3051729675574597004,
      "parent-snapshot-id": null,
      "sequence-number": 1,
      "timestamp-ms": 1672531200000,
      "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-3051729675574597004-1-uuid.avro",
      "summary": {
        "operation": "append",
        "added-data-files": "1",
        "added-records": "100"
      },
      "schema-id": 0
    }
  ],
  "snapshot-log": [
    {"snapshot-id": 3051729675574597004, "timestamp-ms": 1672531200000}
  ],
  "metadata-log": []
}`

// Sample Iceberg metadata.json content for testing (v1 format)
const sampleMetadataV1JSON = `{
  "format-version": 1,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-updated-ms": 1672531200000,
  "last-column-id": 2,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": true, "type": "long"},
      {"id": 2, "name": "data", "required": false, "type": "string"}
    ]
  },
  "partition-spec": {"spec-id": 0, "fields": []},
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": []
}`

func TestParseMetadataBytes_V2(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)
	require.NotNil(t, metadata)

	// Check format version
	assert.Equal(t, FormatVersionV2, metadata.Version)

	// Check UUID
	assert.Equal(t, "9c12d441-03fe-4693-9a96-a0705ddf69c1", metadata.TableUUID.String())

	// Check location
	assert.Equal(t, "s3://bucket/warehouse/db/table", metadata.Location)

	// Check last updated
	assert.Equal(t, int64(1672531200000), metadata.LastUpdatedMs)

	// Check schema
	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))

	// Check snapshot
	require.NotNil(t, metadata.CurrentSnapshotID)
	assert.Equal(t, int64(3051729675574597004), *metadata.CurrentSnapshotID)

	// Check properties
	assert.Equal(t, "parquet", metadata.Properties["write.format.default"])
}

func TestParseMetadataBytes_V1(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV1JSON))
	require.NoError(t, err)
	require.NotNil(t, metadata)

	// Check format version
	assert.Equal(t, FormatVersionV1, metadata.Version)

	// Check UUID
	assert.Equal(t, "9c12d441-03fe-4693-9a96-a0705ddf69c1", metadata.TableUUID.String())

	// Check location
	assert.Equal(t, "s3://bucket/warehouse/db/table", metadata.Location)

	// Check schema
	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)
	assert.Equal(t, 2, len(schema.Fields()))
}

func TestParseMetadataBytes_InvalidJSON(t *testing.T) {
	_, err := ParseMetadataBytes([]byte("not valid json"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestParseMetadataBytes_UnsupportedVersion(t *testing.T) {
	invalidVersion := `{"format-version": 99, "table-uuid": "test"}`
	_, err := ParseMetadataBytes([]byte(invalidVersion))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedVersion)
}

func TestMetadataReader_parseMetadataVersion(t *testing.T) {
	reader := NewMetadataReader(nil)

	tests := []struct {
		filename string
		expected int
	}{
		{"v1.metadata.json", 1},
		{"v2.metadata.json", 2},
		{"v10.metadata.json", 10},
		{"00001-uuid.metadata.json", 1},
		{"00005-uuid.metadata.json", 5},
		{"invalid.metadata.json", 0},
	}

	for _, tc := range tests {
		t.Run(tc.filename, func(t *testing.T) {
			version := reader.parseMetadataVersion(tc.filename)
			assert.Equal(t, tc.expected, version)
		})
	}
}

func TestMetadataReader_ReadMetadataFromPath(t *testing.T) {
	// Create a temp directory with a metadata file
	tmpDir := t.TempDir()
	metadataPath := filepath.Join(tmpDir, "v1.metadata.json")

	err := os.WriteFile(metadataPath, []byte(sampleMetadataV2JSON), 0o644)
	require.NoError(t, err)

	reader := NewMetadataReader(nil) // uses local filesystem by default
	ctx := context.Background()

	metadata, err := reader.ReadMetadataFromPath(ctx, metadataPath)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	assert.Equal(t, FormatVersionV2, metadata.Version)
	assert.Equal(t, "s3://bucket/warehouse/db/table", metadata.Location)
}

func TestMetadataReader_ReadMetadataFromPath_NotFound(t *testing.T) {
	reader := NewMetadataReader(nil)
	ctx := context.Background()

	_, err := reader.ReadMetadataFromPath(ctx, "/nonexistent/path/metadata.json")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTableNotFound)
}

func TestTableMetadata_Schemas(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	schemas := metadata.Schemas()
	assert.Len(t, schemas, 1)
	assert.Equal(t, 0, schemas[0].ID)
}

func TestTableMetadata_PartitionSpecs(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	specs := metadata.PartitionSpecs()
	assert.Len(t, specs, 1)
	assert.Equal(t, 0, specs[0].ID())
}

func TestTableMetadata_CurrentSchema(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)

	fields := schema.Fields()
	assert.Len(t, fields, 3)
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, "created_at", fields[2].Name)
}

func TestTableMetadata_CurrentSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	snap := metadata.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, int64(3051729675574597004), snap.SnapshotID)
}

func TestTableMetadata_SnapshotByID(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	// Existing snapshot
	snap := metadata.SnapshotByID(3051729675574597004)
	require.NotNil(t, snap)

	// Non-existing snapshot
	snap = metadata.SnapshotByID(999)
	assert.Nil(t, snap)
}

func TestTableMetadata_SnapshotByName(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	// main reference should return current snapshot
	snap := metadata.SnapshotByName("main")
	require.NotNil(t, snap)
	assert.Equal(t, int64(3051729675574597004), snap.SnapshotID)

	// Unknown reference
	snap = metadata.SnapshotByName("unknown")
	assert.Nil(t, snap)
}

func TestTableMetadata_SnapshotLogs(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(sampleMetadataV2JSON))
	require.NoError(t, err)

	logs := metadata.SnapshotLogs()
	assert.Len(t, logs, 1)
	assert.Equal(t, int64(3051729675574597004), logs[0].SnapshotID)
}

// Test metadata with no current snapshot
const metadataNoSnapshotJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-sequence-number": 0,
  "last-updated-ms": 1672531200000,
  "last-column-id": 2,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": null,
  "snapshots": [],
  "snapshot-log": []
}`

func TestTableMetadata_NoCurrentSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(metadataNoSnapshotJSON))
	require.NoError(t, err)

	snap := metadata.CurrentSnapshot()
	assert.Nil(t, snap)
}

// Test parsing nested types
const metadataNestedTypesJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-sequence-number": 1,
  "last-updated-ms": 1672531200000,
  "last-column-id": 5,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "tags", "required": false, "type": {"type": "list", "element-id": 3, "element-required": false, "element": "string"}},
        {"id": 4, "name": "props", "required": false, "type": {"type": "map", "key-id": 5, "key": "string", "value-id": 6, "value": "string", "value-required": false}}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": null,
  "snapshots": []
}`

func TestParseMetadataBytes_NestedTypes(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(metadataNestedTypesJSON))
	require.NoError(t, err)

	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)

	fields := schema.Fields()
	assert.Len(t, fields, 3)
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, "tags", fields[1].Name)
	assert.Equal(t, "props", fields[2].Name)
}

// Test parsing parameterized types
const metadataParamTypesJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-sequence-number": 1,
  "last-updated-ms": 1672531200000,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "uuid_col", "required": false, "type": "fixed[16]"},
        {"id": 2, "name": "price", "required": false, "type": "decimal(10,2)"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": null,
  "snapshots": []
}`

func TestParseMetadataBytes_ParameterizedTypes(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(metadataParamTypesJSON))
	require.NoError(t, err)

	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)

	fields := schema.Fields()
	assert.Len(t, fields, 2)
	assert.Equal(t, "uuid_col", fields[0].Name)
	assert.Equal(t, "price", fields[1].Name)
}

// Test parsing partition transforms
const metadataPartitionTransformsJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
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
        {"id": 2, "name": "name", "required": false, "type": "string"},
        {"id": 3, "name": "created_at", "required": false, "type": "timestamp"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"},
        {"source-id": 2, "field-id": 1001, "name": "name_truncate", "transform": "truncate[10]"},
        {"source-id": 3, "field-id": 1002, "name": "year", "transform": "year"},
        {"source-id": 3, "field-id": 1003, "name": "month", "transform": "month"},
        {"source-id": 3, "field-id": 1004, "name": "day", "transform": "day"},
        {"source-id": 3, "field-id": 1005, "name": "hour", "transform": "hour"},
        {"source-id": 1, "field-id": 1006, "name": "id_identity", "transform": "identity"},
        {"source-id": 1, "field-id": 1007, "name": "void_col", "transform": "void"}
      ]
    }
  ],
  "last-partition-id": 1007,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": null,
  "snapshots": []
}`

func TestParseMetadataBytes_PartitionTransforms(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(metadataPartitionTransformsJSON))
	require.NoError(t, err)

	specs := metadata.PartitionSpecs()
	require.Len(t, specs, 1)

	spec := specs[0]
	numFields := 0
	for range spec.Fields() {
		numFields++
	}
	assert.Equal(t, 8, numFields)
}

func TestParseMetadataFromReader(t *testing.T) {
	reader := &stringReader{s: sampleMetadataV2JSON}
	metadata, err := ParseMetadataFromReader(reader)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, FormatVersionV2, metadata.Version)
}

// Helper type to implement io.Reader
type stringReader struct {
	s   string
	pos int
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}

func TestNewMetadataReader(t *testing.T) {
	// Test with nil filesystem - should use default
	reader := NewMetadataReader(nil)
	require.NotNil(t, reader)
}
