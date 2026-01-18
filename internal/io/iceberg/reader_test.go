// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains tests for the Iceberg reader.
package iceberg

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReaderOptions tests the default reader options.
func TestReaderOptions(t *testing.T) {
	opts := DefaultReaderOptions()

	assert.NotNil(t, opts)
	assert.Equal(t, 2048, opts.MaxRowsPerChunk)
	assert.Nil(t, opts.SelectedColumns)
	assert.Nil(t, opts.SnapshotID)
	assert.Nil(t, opts.Timestamp)
	assert.Equal(t, int64(0), opts.Limit)
}

// TestScanOptions tests the default scan options.
func TestScanOptions(t *testing.T) {
	opts := DefaultScanOptions()

	assert.NotNil(t, opts)
	assert.Equal(t, 2048, opts.MaxRowsPerChunk)
	assert.Nil(t, opts.SelectedColumns)
	assert.Equal(t, int64(0), opts.Limit)
}

// TestScanPlanCreation tests creating a scan plan.
func TestScanPlanCreation(t *testing.T) {
	// Create test metadata
	metadata := createTestMetadata(t)

	// Create scan planner
	planner := NewScanPlanner(metadata, NewManifestReader(nil))

	// Create scan plan with default options
	ctx := context.Background()
	plan, err := planner.CreateScanPlan(ctx, nil)

	// Plan creation succeeds even without snapshots
	require.NoError(t, err)
	assert.NotNil(t, plan)
}

// TestTimeTravelBySnapshotID tests selecting a snapshot by ID.
func TestTimeTravelBySnapshotID(t *testing.T) {
	// Create test metadata with snapshots
	metadata := createTestMetadataWithSnapshots(t)

	selector := NewTimeTravelSelector(metadata)

	// Select by snapshot ID
	snapshotID := int64(1001)
	result, err := selector.SelectSnapshot(&TimeTravelOptions{SnapshotID: &snapshotID})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Snapshot)
	assert.Equal(t, snapshotID, result.Snapshot.SnapshotID)
	assert.Equal(t, "snapshot_id=1001", result.SelectedBy)
}

// TestTimeTravelByTimestamp tests selecting a snapshot by timestamp.
func TestTimeTravelByTimestamp(t *testing.T) {
	metadata := createTestMetadataWithSnapshots(t)

	selector := NewTimeTravelSelector(metadata)

	// Select by timestamp (should get snapshot at or before this time)
	// Our test snapshot has timestamp 1700000000000 (ms)
	ts := time.UnixMilli(1700000001000) // 1 second after first snapshot
	result, err := selector.SelectSnapshot(&TimeTravelOptions{Timestamp: &ts})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Snapshot)
}

// TestTimeTravelCurrentSnapshot tests selecting the current snapshot.
func TestTimeTravelCurrentSnapshot(t *testing.T) {
	metadata := createTestMetadataWithSnapshots(t)

	selector := NewTimeTravelSelector(metadata)

	// Select current (no options)
	result, err := selector.SelectSnapshot(nil)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "current", result.SelectedBy)
}

// TestTimeTravelSnapshotNotFound tests error when snapshot is not found.
func TestTimeTravelSnapshotNotFound(t *testing.T) {
	metadata := createTestMetadataWithSnapshots(t)

	selector := NewTimeTravelSelector(metadata)

	// Try to select non-existent snapshot
	snapshotID := int64(9999)
	_, err := selector.SelectSnapshot(&TimeTravelOptions{SnapshotID: &snapshotID})

	assert.Error(t, err)
}

// TestColumnProjection tests column projection in scan planning.
func TestColumnProjection(t *testing.T) {
	metadata := createTestMetadataWithSchema(t)

	planner := NewScanPlanner(metadata, NewManifestReader(nil))

	ctx := context.Background()
	plan, err := planner.CreateScanPlan(ctx, &ScanOptions{
		SelectedColumns: []string{"id", "name"},
	})

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.ColumnProjection, 2)
	assert.Equal(t, "id", plan.ColumnProjection[0].Name)
	assert.Equal(t, "name", plan.ColumnProjection[1].Name)
}

// TestColumnProjectionAllColumns tests selecting all columns.
func TestColumnProjectionAllColumns(t *testing.T) {
	metadata := createTestMetadataWithSchema(t)

	planner := NewScanPlanner(metadata, NewManifestReader(nil))

	ctx := context.Background()
	plan, err := planner.CreateScanPlan(ctx, &ScanOptions{
		SelectedColumns: []string{"*"},
	})

	require.NoError(t, err)
	assert.NotNil(t, plan)
	// Should have all 3 columns from the test schema
	assert.Len(t, plan.ColumnProjection, 3)
}

// TestPartitionFilter tests partition filter evaluation.
func TestPartitionFilter(t *testing.T) {
	tests := []struct {
		name      string
		filter    PartitionFilterExpr
		partData  map[string]any
		wantMatch bool
	}{
		{
			name:      "equality match",
			filter:    PartitionFilterExpr{FieldName: "region", Operator: "=", Value: "US"},
			partData:  map[string]any{"region": "US"},
			wantMatch: true,
		},
		{
			name:      "equality no match",
			filter:    PartitionFilterExpr{FieldName: "region", Operator: "=", Value: "US"},
			partData:  map[string]any{"region": "EU"},
			wantMatch: false,
		},
		{
			name:      "less than match",
			filter:    PartitionFilterExpr{FieldName: "year", Operator: "<", Value: 2024},
			partData:  map[string]any{"year": 2023},
			wantMatch: true,
		},
		{
			name:      "less than no match",
			filter:    PartitionFilterExpr{FieldName: "year", Operator: "<", Value: 2024},
			partData:  map[string]any{"year": 2024},
			wantMatch: false,
		},
		{
			name:      "greater than match",
			filter:    PartitionFilterExpr{FieldName: "year", Operator: ">", Value: 2020},
			partData:  map[string]any{"year": 2023},
			wantMatch: true,
		},
		{
			name: "IN list match",
			filter: PartitionFilterExpr{
				FieldName: "region",
				Operator:  "IN",
				Value:     []any{"US", "EU"},
			},
			partData:  map[string]any{"region": "US"},
			wantMatch: true,
		},
		{
			name: "IN list no match",
			filter: PartitionFilterExpr{
				FieldName: "region",
				Operator:  "IN",
				Value:     []any{"US", "EU"},
			},
			partData:  map[string]any{"region": "APAC"},
			wantMatch: false,
		},
		{
			name:      "nil partition data",
			filter:    PartitionFilterExpr{FieldName: "region", Operator: "=", Value: "US"},
			partData:  nil,
			wantMatch: true, // Can't prune without partition data
		},
		{
			name:      "missing partition field",
			filter:    PartitionFilterExpr{FieldName: "region", Operator: "=", Value: "US"},
			partData:  map[string]any{"year": 2024},
			wantMatch: true, // Can't prune if field not in partition
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := BuildPartitionFilter([]PartitionFilterExpr{tt.filter})
			file := &DataFile{PartitionData: tt.partData}
			got := filter.fileMatches(file)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

// TestDeleteApplierNoOpLegacy tests the no-op delete applier.
func TestDeleteApplierNoOpLegacy(t *testing.T) {
	applier := &NoOpDeleteApplier{}

	assert.False(t, applier.HasDeletes())
	assert.NoError(t, applier.LoadDeleteFiles(nil, nil))
	assert.NoError(t, applier.Close())
}

// TestCreateDeleteApplierLegacy tests creating the appropriate delete applier.
func TestCreateDeleteApplierLegacy(t *testing.T) {
	ctx := context.Background()

	// With no delete files, should return NoOp
	applier := CreateDeleteApplier(ctx, []*DataFile{}, nil, "/test", []string{})
	assert.IsType(t, &NoOpDeleteApplier{}, applier)

	// With positional delete files, should return PositionalDeleteApplier
	applier = CreateDeleteApplier(
		ctx,
		[]*DataFile{{Path: "delete.parquet", Format: FileFormatParquet}},
		nil,
		"/test",
		[]string{},
	)
	assert.IsType(t, &PositionalDeleteApplier{}, applier)
}

// TestSchemaEvolutionChecker tests schema evolution detection.
func TestSchemaEvolutionChecker(t *testing.T) {
	checker := NewSchemaEvolutionChecker()

	// TODO: Add tests when we have proper test schemas
	assert.NotNil(t, checker)
}

// TestFileScanTasks tests creating file scan tasks.
func TestFileScanTasks(t *testing.T) {
	plan := &ScanPlan{
		DataFiles: []*DataFile{
			{Path: "file1.parquet", RecordCount: 1000},
			{Path: "file2.parquet", RecordCount: 2000},
		},
		ColumnProjection: []ColumnInfo{
			{ID: 1, Name: "id"},
			{ID: 2, Name: "name"},
		},
	}

	tasks := plan.CreateFileTasks()

	assert.Len(t, tasks, 2)
	assert.Equal(t, "file1.parquet", tasks[0].DataFile.Path)
	assert.Equal(t, "file2.parquet", tasks[1].DataFile.Path)
	assert.Len(t, tasks[0].Columns, 2)
}

// TestSplitFileTasks tests splitting file tasks.
func TestSplitFileTasks(t *testing.T) {
	plan := &ScanPlan{
		DataFiles: []*DataFile{
			{
				Path:         "file1.parquet",
				RecordCount:  10000,
				SplitOffsets: []int64{0, 2500, 5000, 7500},
			},
			{
				Path:        "file2.parquet",
				RecordCount: 500,
			},
		},
		ColumnProjection: []ColumnInfo{
			{ID: 1, Name: "id"},
		},
	}

	// Split large files into 1000-row tasks
	tasks := plan.SplitFileTasks(1000)

	// file1 should be split based on SplitOffsets, file2 should be single task
	assert.GreaterOrEqual(t, len(tasks), 2)
}

// TestSnapshotHistoryEntry tests snapshot history generation.
func TestSnapshotHistoryEntry(t *testing.T) {
	metadata := createTestMetadataWithSnapshots(t)

	history := GetSnapshotHistory(metadata)

	assert.NotEmpty(t, history)
	assert.Equal(t, int64(1001), history[0].SnapshotID)
	assert.NotZero(t, history[0].TimestampMs)
}

// TestValidateTimeTravelOptions tests time travel option validation.
func TestValidateTimeTravelOptions(t *testing.T) {
	// Nil options is valid
	assert.NoError(t, ValidateTimeTravelOptions(nil))

	// Single option is valid
	snapshotID := int64(1001)
	assert.NoError(t, ValidateTimeTravelOptions(&TimeTravelOptions{SnapshotID: &snapshotID}))

	// Multiple options is invalid
	ts := time.Now()
	err := ValidateTimeTravelOptions(&TimeTravelOptions{
		SnapshotID: &snapshotID,
		Timestamp:  &ts,
	})
	assert.Error(t, err)
}

// TestBinaryBoundDecoder tests binary bound decoding.
func TestBinaryBoundDecoder(t *testing.T) {
	decoder := &BinaryBoundDecoder{}

	// Test int32 decoding
	int32Val, err := decoder.DecodeInt32([]byte{0x01, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	assert.Equal(t, int32(1), int32Val)

	// Test int64 decoding
	int64Val, err := decoder.DecodeInt64([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	assert.Equal(t, int64(1), int64Val)

	// Test string decoding
	str := decoder.DecodeString([]byte("hello"))
	assert.Equal(t, "hello", str)
}

// TestCompareValues tests value comparison.
func TestCompareValues(t *testing.T) {
	tests := []struct {
		a, b any
		want int
	}{
		{1, 2, -1},
		{2, 1, 1},
		{1, 1, 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{"a", "a", 0},
		{int32(1), int32(2), -1},
		{int64(1), int64(2), -1},
		{float64(1.0), float64(2.0), -1},
	}

	for _, tt := range tests {
		got := compareValues(tt.a, tt.b)
		assert.Equal(t, tt.want, got, "compareValues(%v, %v)", tt.a, tt.b)
	}
}

// Helper functions for creating test data

func createTestMetadata(t *testing.T) *TableMetadata {
	t.Helper()

	// Create minimal metadata
	metadataJSON := `{
		"format-version": 2,
		"table-uuid": "12345678-1234-1234-1234-123456789012",
		"location": "/test/table",
		"last-updated-ms": 1700000000000,
		"last-column-id": 3,
		"current-schema-id": 0,
		"schemas": [
			{
				"type": "struct",
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "name", "required": false, "type": "string"},
					{"id": 3, "name": "value", "required": false, "type": "double"}
				]
			}
		],
		"default-spec-id": 0,
		"partition-specs": [
			{"spec-id": 0, "fields": []}
		],
		"snapshots": [],
		"snapshot-log": []
	}`

	metadata, err := ParseMetadataBytes([]byte(metadataJSON))
	require.NoError(t, err)

	return metadata
}

func createTestMetadataWithSnapshots(t *testing.T) *TableMetadata {
	t.Helper()

	metadataJSON := `{
		"format-version": 2,
		"table-uuid": "12345678-1234-1234-1234-123456789012",
		"location": "/test/table",
		"last-updated-ms": 1700000000000,
		"last-column-id": 3,
		"current-schema-id": 0,
		"current-snapshot-id": 1002,
		"schemas": [
			{
				"type": "struct",
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "name", "required": false, "type": "string"},
					{"id": 3, "name": "value", "required": false, "type": "double"}
				]
			}
		],
		"default-spec-id": 0,
		"partition-specs": [
			{"spec-id": 0, "fields": []}
		],
		"snapshots": [
			{
				"snapshot-id": 1001,
				"timestamp-ms": 1700000000000,
				"manifest-list": "/test/table/metadata/snap-1001.avro",
				"summary": {"operation": "append"}
			},
			{
				"snapshot-id": 1002,
				"parent-snapshot-id": 1001,
				"timestamp-ms": 1700001000000,
				"manifest-list": "/test/table/metadata/snap-1002.avro",
				"summary": {"operation": "append"}
			}
		],
		"snapshot-log": [
			{"snapshot-id": 1001, "timestamp-ms": 1700000000000},
			{"snapshot-id": 1002, "timestamp-ms": 1700001000000}
		]
	}`

	metadata, err := ParseMetadataBytes([]byte(metadataJSON))
	require.NoError(t, err)

	return metadata
}

func createTestMetadataWithSchema(t *testing.T) *TableMetadata {
	t.Helper()

	// Same as createTestMetadata but with current snapshot set
	metadataJSON := `{
		"format-version": 2,
		"table-uuid": "12345678-1234-1234-1234-123456789012",
		"location": "/test/table",
		"last-updated-ms": 1700000000000,
		"last-column-id": 3,
		"current-schema-id": 0,
		"schemas": [
			{
				"type": "struct",
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "name", "required": false, "type": "string"},
					{"id": 3, "name": "value", "required": false, "type": "double"}
				]
			}
		],
		"default-spec-id": 0,
		"partition-specs": [
			{"spec-id": 0, "fields": []}
		],
		"snapshots": [],
		"snapshot-log": []
	}`

	metadata, err := ParseMetadataBytes([]byte(metadataJSON))
	require.NoError(t, err)

	return metadata
}

// TestIntegrationWithRealTable tests reading a real Iceberg table.
// This test is skipped unless a test table is available.
func TestIntegrationWithRealTable(t *testing.T) {
	// Skip if test table doesn't exist
	testTablePath := os.Getenv("ICEBERG_TEST_TABLE")
	if testTablePath == "" {
		t.Skip("ICEBERG_TEST_TABLE environment variable not set")
	}

	ctx := context.Background()

	reader, err := NewReader(ctx, testTablePath, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Read schema
	schema, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("Schema columns: %v", schema)

	// Read first chunk
	chunk, err := reader.ReadChunk()
	if err != nil {
		t.Logf("Error or EOF reading chunk: %v", err)
		return
	}

	t.Logf("Read %d rows", chunk.Count())
	assert.Greater(t, chunk.Count(), 0)
}

// TestIntegrationColumnProjection tests column projection with a real table.
func TestIntegrationColumnProjection(t *testing.T) {
	testTablePath := os.Getenv("ICEBERG_TEST_TABLE")
	if testTablePath == "" {
		t.Skip("ICEBERG_TEST_TABLE environment variable not set")
	}

	ctx := context.Background()

	reader, err := NewReader(ctx, testTablePath, &ReaderOptions{
		SelectedColumns: []string{"id"}, // Only read id column
	})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)

	// Should only have the projected column
	assert.Len(t, schema, 1)
	assert.Equal(t, "id", schema[0])
}

// TestIntegrationTimeTravel tests time travel queries.
func TestIntegrationTimeTravel(t *testing.T) {
	testTablePath := os.Getenv("ICEBERG_TEST_TABLE")
	if testTablePath == "" {
		t.Skip("ICEBERG_TEST_TABLE environment variable not set")
	}

	ctx := context.Background()

	// First, open table to get a snapshot ID
	table, err := OpenTable(ctx, testTablePath, nil)
	require.NoError(t, err)

	currentSnapshot := table.CurrentSnapshot()
	if currentSnapshot == nil {
		t.Skip("Table has no snapshots")
	}

	snapshotID := currentSnapshot.SnapshotID
	_ = table.Close()

	// Now read using time travel
	reader, err := NewReader(ctx, testTablePath, &ReaderOptions{
		SnapshotID: &snapshotID,
	})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Verify we got the right snapshot
	plan := reader.ScanPlan()
	if plan != nil && plan.Snapshot != nil {
		assert.Equal(t, snapshotID, plan.Snapshot.SnapshotID)
	}
}

// createTestIcebergTable creates a minimal Iceberg table for testing.
// Returns the table path.
func createTestIcebergTable(t *testing.T) string {
	t.Helper()

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "iceberg_test_*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	// Create metadata directory
	metadataDir := filepath.Join(tmpDir, "metadata")
	require.NoError(t, os.MkdirAll(metadataDir, 0o755))

	// Create metadata.json
	metadataJSON := map[string]any{
		"format-version":      2,
		"table-uuid":          "12345678-1234-1234-1234-123456789012",
		"location":            tmpDir,
		"last-updated-ms":     1700000000000,
		"last-column-id":      3,
		"current-schema-id":   0,
		"current-snapshot-id": nil,
		"schemas": []map[string]any{
			{
				"type":      "struct",
				"schema-id": 0,
				"fields": []map[string]any{
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "name", "required": false, "type": "string"},
					{"id": 3, "name": "value", "required": false, "type": "double"},
				},
			},
		},
		"default-spec-id": 0,
		"partition-specs": []map[string]any{
			{"spec-id": 0, "fields": []any{}},
		},
		"snapshots":    []any{},
		"snapshot-log": []any{},
	}

	metadataBytes, err := json.MarshalIndent(metadataJSON, "", "  ")
	require.NoError(t, err)

	metadataPath := filepath.Join(metadataDir, "v1.metadata.json")
	require.NoError(t, os.WriteFile(metadataPath, metadataBytes, 0o644))

	// Create version-hint.text
	versionHintPath := filepath.Join(metadataDir, "version-hint.text")
	require.NoError(t, os.WriteFile(versionHintPath, []byte("1"), 0o644))

	return tmpDir
}

// TestReaderWithEmptyTable tests reading an empty Iceberg table.
func TestReaderWithEmptyTable(t *testing.T) {
	tablePath := createTestIcebergTable(t)

	ctx := context.Background()
	reader, err := NewReader(ctx, tablePath, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Should be able to get schema even from empty table
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Len(t, schema, 3)
	assert.Equal(t, "id", schema[0])
	assert.Equal(t, "name", schema[1])
	assert.Equal(t, "value", schema[2])

	// Reading should return EOF immediately (no data files)
	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF) // EOF because there are no data files
}

// TODO: Cloud storage tests
// These tests require S3/GCS test setup and are skipped by default.

// TestIntegrationS3 tests reading from S3.
func TestIntegrationS3(t *testing.T) {
	// TODO: requires S3 test setup
	t.Skip("S3 integration tests require test environment setup")
}

// TestIntegrationGCS tests reading from GCS.
func TestIntegrationGCS(t *testing.T) {
	// TODO: requires GCS test setup
	t.Skip("GCS integration tests require test environment setup")
}
