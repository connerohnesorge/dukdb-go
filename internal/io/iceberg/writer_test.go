// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains tests for the Iceberg writer.
package iceberg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterOptions_Defaults(t *testing.T) {
	opts := DefaultWriterOptions()

	assert.Equal(t, "snappy", opts.CompressionCodec)
	assert.Equal(t, 100000, opts.RowGroupSize)
	assert.Equal(t, FormatVersionV2, opts.FormatVersion)
	assert.NotNil(t, opts.Properties)
}

func TestNewWriter_RequiresTableLocation(t *testing.T) {
	ctx := context.Background()
	opts := DefaultWriterOptions()

	_, err := NewWriter(ctx, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TableLocation is required")
}

func TestNewWriter_CreatesDirectories(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "test_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Verify directories were created
	metadataDir := filepath.Join(tableLocation, "metadata")
	dataDir := filepath.Join(tableLocation, "data")

	assert.DirExists(t, metadataDir)
	assert.DirExists(t, dataDir)
}

func TestWriter_WriteChunk(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "test_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	// Create a test DataChunk
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	// Add some test data
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i), "test"})
	}

	// Set schema and types
	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	// Write the chunk
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	// Close to finalize
	err = writer.Close()
	require.NoError(t, err)

	// Verify metadata files were created
	metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
	versionHintPath := filepath.Join(tableLocation, "metadata", "version-hint.text")

	assert.FileExists(t, metadataPath)
	assert.FileExists(t, versionHintPath)

	// Verify version hint content
	versionHintContent, err := os.ReadFile(versionHintPath)
	require.NoError(t, err)
	assert.Equal(t, "1", string(versionHintContent))

	// Verify metadata.json structure
	metadataContent, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var metadata IcebergTableMetadata
	err = json.Unmarshal(metadataContent, &metadata)
	require.NoError(t, err)

	assert.Equal(t, 2, metadata.FormatVersion)
	assert.Equal(t, tableLocation, metadata.Location)
	assert.NotEmpty(t, metadata.TableUUID)
	assert.Equal(t, 1, len(metadata.Schemas))
	assert.Equal(t, 2, len(metadata.Schemas[0].Fields))
}

func TestWriter_EmptyTable(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "empty_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	// Close without writing any data
	err = writer.Close()
	require.NoError(t, err)

	// Verify metadata files exist even for empty table
	metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
	assert.FileExists(t, metadataPath)
}

func TestWriter_MultipleChunks(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "multi_chunk_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	err = writer.SetSchema([]string{"value"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	// Write multiple chunks
	for i := 0; i < 3; i++ {
		chunk := storage.NewDataChunkWithCapacity(types, 100)
		for j := 0; j < 10; j++ {
			chunk.AppendRow([]any{int64(i*10 + j)})
		}
		err = writer.WriteChunk(chunk)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Verify data files were created
	dataDir := filepath.Join(tableLocation, "data")
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)

	// Should have at least one parquet file
	parquetFiles := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".parquet" {
			parquetFiles++
		}
	}
	assert.GreaterOrEqual(t, parquetFiles, 1)
}

func TestWriter_SetSchemaAfterClose(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "closed_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Try to set schema after close
	err = writer.SetSchema([]string{"id"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestWriter_WriteChunkAfterClose(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "closed_table2")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Try to write after close
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.AppendRow([]any{int32(1)})

	err = writer.WriteChunk(chunk)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestWriter_DoubleClose(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "double_close")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_CompressionOptions(t *testing.T) {
	testCases := []struct {
		name  string
		codec string
	}{
		{"snappy", "snappy"},
		{"gzip", "gzip"},
		{"zstd", "zstd"},
		{"none", "none"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			tempDir := t.TempDir()
			tableLocation := filepath.Join(tempDir, "compression_"+tc.name)

			opts := DefaultWriterOptions()
			opts.TableLocation = tableLocation
			opts.CompressionCodec = tc.codec

			writer, err := NewWriter(ctx, opts)
			require.NoError(t, err)

			types := []dukdb.Type{dukdb.TYPE_INTEGER}
			err = writer.SetSchema([]string{"id"})
			require.NoError(t, err)
			err = writer.SetTypes(types)
			require.NoError(t, err)

			chunk := storage.NewDataChunkWithCapacity(types, 10)
			chunk.AppendRow([]any{int32(1)})
			err = writer.WriteChunk(chunk)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify metadata file exists
			metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
			assert.FileExists(t, metadataPath)
		})
	}
}

func TestWriter_AllDataTypes(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "all_types")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}

	columns := []string{
		"bool_col",
		"tinyint_col",
		"smallint_col",
		"int_col",
		"bigint_col",
		"float_col",
		"double_col",
		"varchar_col",
	}

	err = writer.SetSchema(columns)
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.AppendRow([]any{
		true,
		int8(1),
		int16(2),
		int32(3),
		int64(4),
		float32(5.5),
		float64(6.6),
		"test",
	})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify schema in metadata
	metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
	metadataContent, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var metadata IcebergTableMetadata
	err = json.Unmarshal(metadataContent, &metadata)
	require.NoError(t, err)

	assert.Equal(t, len(types), len(metadata.Schemas[0].Fields))
}

func TestWriter_NullValues(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "null_values")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.AppendRow([]any{int32(1), nil})
	chunk.AppendRow([]any{nil, "test"})
	chunk.AppendRow([]any{nil, nil})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify table was created
	metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
	assert.FileExists(t, metadataPath)
}

func TestWriter_TableCanBeReadBack(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "readable_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	chunk := storage.NewDataChunkWithCapacity(types, 10)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i), "test"})
	}

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Try to open the table with the reader
	table, err := OpenTable(ctx, tableLocation, nil)
	require.NoError(t, err)
	defer func() { _ = table.Close() }()

	// Verify table properties
	assert.Equal(t, tableLocation, table.Location())
	assert.NotNil(t, table.Schema())

	// Note: Full round-trip reading of data files through manifest parsing
	// requires compatible AVRO schema handling. For now, we verify the table
	// metadata can be read back. The data files themselves are readable as
	// standard Parquet files.
}

func TestWriter_FormatVersion(t *testing.T) {
	testCases := []struct {
		name    string
		version FormatVersion
	}{
		{"v1", FormatVersionV1},
		{"v2", FormatVersionV2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			tempDir := t.TempDir()
			tableLocation := filepath.Join(tempDir, "format_"+tc.name)

			opts := DefaultWriterOptions()
			opts.TableLocation = tableLocation
			opts.FormatVersion = tc.version

			writer, err := NewWriter(ctx, opts)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify format version in metadata
			metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
			metadataContent, err := os.ReadFile(metadataPath)
			require.NoError(t, err)

			var metadata IcebergTableMetadata
			err = json.Unmarshal(metadataContent, &metadata)
			require.NoError(t, err)

			assert.Equal(t, int(tc.version), metadata.FormatVersion)
		})
	}
}

func TestWriter_Properties(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tableLocation := filepath.Join(tempDir, "properties_table")

	opts := DefaultWriterOptions()
	opts.TableLocation = tableLocation
	opts.Properties = map[string]string{
		"custom.key":   "custom.value",
		"another.prop": "another.value",
	}

	writer, err := NewWriter(ctx, opts)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify properties in metadata
	metadataPath := filepath.Join(tableLocation, "metadata", "v1.metadata.json")
	metadataContent, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var metadata IcebergTableMetadata
	err = json.Unmarshal(metadataContent, &metadata)
	require.NoError(t, err)

	assert.Equal(t, "custom.value", metadata.Properties["custom.key"])
	assert.Equal(t, "another.value", metadata.Properties["another.prop"])
}
