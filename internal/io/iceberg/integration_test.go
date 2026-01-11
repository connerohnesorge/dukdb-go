// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains integration tests for the Iceberg reader using generated test fixtures.
package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestdataDir returns the absolute path to the testdata directory.
func getTestdataDir(t *testing.T) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}

	testdataDir := filepath.Join(filepath.Dir(currentFile), "testdata")

	// Check if testdata exists
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skipf("testdata directory not found at %s - run generate_fixtures.py to create", testdataDir)
	}

	return testdataDir
}

// getSimpleTablePath returns the absolute path to the simple_table test fixture.
func getSimpleTablePath(t *testing.T) string {
	t.Helper()
	tablePath := filepath.Join(getTestdataDir(t), "simple_table")

	// Check for metadata
	metadataPath := filepath.Join(tablePath, "metadata", "v1.metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Skipf("simple_table not found - run generate_fixtures.py to create")
	}

	return tablePath
}

// getTimeTravelTablePath returns the absolute path to the time_travel_table test fixture.
func getTimeTravelTablePath(t *testing.T) string {
	t.Helper()
	tablePath := filepath.Join(getTestdataDir(t), "time_travel_table")

	// Check for metadata
	metadataPath := filepath.Join(tablePath, "metadata", "v3.metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Skipf("time_travel_table not found - run generate_fixtures.py to create")
	}

	return tablePath
}

// updateMetadataLocations updates the location field in metadata.json to use
// the actual test path. This is needed because the fixtures are generated with
// absolute paths that differ from the test environment.
func updateMetadataLocations(t *testing.T, tablePath string) {
	t.Helper()

	metadataDir := filepath.Join(tablePath, "metadata")

	// Find all metadata.json files
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		t.Fatalf("failed to read metadata directory: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}

		path := filepath.Join(metadataDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read %s: %v", path, err)
		}

		var metadata map[string]any
		if err := json.Unmarshal(data, &metadata); err != nil {
			t.Fatalf("failed to parse %s: %v", path, err)
		}

		// Update location to actual table path
		metadata["location"] = tablePath

		// Update snapshot manifest-list paths
		if snapshots, ok := metadata["snapshots"].([]any); ok {
			for _, snap := range snapshots {
				if s, ok := snap.(map[string]any); ok {
					if manifestList, ok := s["manifest-list"].(string); ok {
						// Extract filename and rebuild path
						manifestListName := filepath.Base(manifestList)
						s["manifest-list"] = filepath.Join(metadataDir, manifestListName)
					}
				}
			}
		}

		// Write updated metadata
		updated, err := json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			t.Fatalf("failed to marshal updated metadata: %v", err)
		}

		if err := os.WriteFile(path, updated, 0o644); err != nil {
			t.Fatalf("failed to write updated metadata: %v", err)
		}
	}
}

// TestIntegrationSimpleTableRead tests reading a simple unpartitioned Iceberg table.
func TestIntegrationSimpleTableRead(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Open the table
	reader, err := NewReader(ctx, tablePath, nil)
	require.NoError(t, err, "failed to create reader")
	defer func() { _ = reader.Close() }()

	// Check schema
	schema, err := reader.Schema()
	require.NoError(t, err, "failed to get schema")
	assert.Len(t, schema, 3, "expected 3 columns")
	assert.Equal(t, "id", schema[0])
	assert.Equal(t, "name", schema[1])
	assert.Equal(t, "value", schema[2])

	// Read all data
	var totalRows int
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "failed to read chunk")
		totalRows += chunk.Count()
	}

	assert.Equal(t, 100, totalRows, "expected 100 rows total")
}

// TestIntegrationSimpleTableColumnProjection tests reading specific columns.
func TestIntegrationSimpleTableColumnProjection(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Read only id column
	reader, err := NewReader(ctx, tablePath, &ReaderOptions{
		SelectedColumns: []string{"id", "name"},
	})
	require.NoError(t, err, "failed to create reader")
	defer func() { _ = reader.Close() }()

	// Check schema reflects projection
	schema, err := reader.Schema()
	require.NoError(t, err, "failed to get schema")
	assert.Len(t, schema, 2, "expected 2 columns")
	assert.Equal(t, "id", schema[0])
	assert.Equal(t, "name", schema[1])
}

// TestIntegrationSimpleTableMetadata tests reading table metadata.
func TestIntegrationSimpleTableMetadata(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Open table
	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	// Check metadata
	metadata := table.Metadata()
	assert.NotNil(t, metadata)
	assert.Equal(t, FormatVersionV2, metadata.Version)
	assert.NotEmpty(t, metadata.TableUUID.String())

	// Check schema
	schema := table.Schema()
	assert.NotNil(t, schema)
	fields := schema.Fields()
	assert.Len(t, fields, 3)

	// Check snapshot
	snapshot := table.CurrentSnapshot()
	assert.NotNil(t, snapshot, "expected a current snapshot")
	assert.Equal(t, int64(1000000001), snapshot.SnapshotID)
}

// TestIntegrationTimeTravelTableSnapshots tests reading snapshot history.
func TestIntegrationTimeTravelTableSnapshots(t *testing.T) {
	tablePath := getTimeTravelTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Open table
	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	// Check snapshots
	snapshots := table.Snapshots()
	assert.Len(t, snapshots, 3, "expected 3 snapshots")

	// Verify snapshot IDs
	expectedIDs := []int64{1000000001, 1000000002, 1000000003}
	for i, snap := range snapshots {
		assert.Equal(t, expectedIDs[i], snap.SnapshotID, "snapshot %d ID mismatch", i)
	}

	// Current snapshot should be the latest
	current := table.CurrentSnapshot()
	assert.Equal(t, int64(1000000003), current.SnapshotID)
}

// TestIntegrationTimeTravelBySnapshotID tests time travel to a specific snapshot.
func TestIntegrationTimeTravelBySnapshotID(t *testing.T) {
	tablePath := getTimeTravelTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Test data expectations per snapshot:
	// Snapshot 1 (1000000001): 50 rows (id 1-50)
	// Snapshot 2 (1000000002): 80 rows (id 1-80)
	// Snapshot 3 (1000000003): 100 rows (id 1-100)

	testCases := []struct {
		snapshotID   int64
		expectedRows int
	}{
		{1000000001, 50},
		{1000000002, 80},
		{1000000003, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("snapshot_%d", tc.snapshotID), func(t *testing.T) {
			snapshotID := tc.snapshotID
			reader, err := NewReader(ctx, tablePath, &ReaderOptions{
				SnapshotID: &snapshotID,
			})
			require.NoError(t, err, "failed to create reader for snapshot %d", snapshotID)
			defer func() { _ = reader.Close() }()

			// Trigger initialization by calling Schema()
			_, err = reader.Schema()
			require.NoError(t, err, "failed to get schema for snapshot %d", snapshotID)

			// Verify we got the right snapshot
			plan := reader.ScanPlan()
			require.NotNil(t, plan, "scan plan should not be nil")
			require.NotNil(t, plan.Snapshot, "snapshot should not be nil")
			assert.Equal(t, snapshotID, plan.Snapshot.SnapshotID)

			// Count rows
			var totalRows int
			for {
				chunk, err := reader.ReadChunk()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, "failed to read chunk")
				totalRows += chunk.Count()
			}

			assert.Equal(t, tc.expectedRows, totalRows,
				"expected %d rows for snapshot %d, got %d",
				tc.expectedRows, snapshotID, totalRows)
		})
	}
}

// TestIntegrationTimeTravelByTimestamp tests time travel using timestamps.
func TestIntegrationTimeTravelByTimestamp(t *testing.T) {
	tablePath := getTimeTravelTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	// Timestamps from fixture:
	// ts1 = 1700000000000 (Snapshot 1: 50 rows)
	// ts2 = 1700003600000 (Snapshot 2: 80 rows)
	// ts3 = 1700007200000 (Snapshot 3: 100 rows)

	// Query at time between snapshot 1 and 2 should return snapshot 1
	ts := time.UnixMilli(1700001800000) // 30 minutes after snapshot 1
	tsMs := ts.UnixMilli()

	reader, err := NewReader(ctx, tablePath, &ReaderOptions{
		Timestamp: &tsMs,
	})
	require.NoError(t, err, "failed to create reader with timestamp")
	defer func() { _ = reader.Close() }()

	// Trigger initialization by calling Schema()
	_, err = reader.Schema()
	require.NoError(t, err, "failed to get schema")

	// Verify we got snapshot 1
	plan := reader.ScanPlan()
	require.NotNil(t, plan, "scan plan should not be nil")
	require.NotNil(t, plan.Snapshot, "snapshot should not be nil")
	assert.Equal(t, int64(1000000001), plan.Snapshot.SnapshotID,
		"expected snapshot 1 for timestamp between snapshots 1 and 2")
}

// TestIntegrationSnapshotNotFound tests error handling for non-existent snapshots.
func TestIntegrationSnapshotNotFound(t *testing.T) {
	tablePath := getTimeTravelTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	nonExistentID := int64(9999999999)
	reader, err := NewReader(ctx, tablePath, &ReaderOptions{
		SnapshotID: &nonExistentID,
	})

	// Should fail during initialization
	if err == nil {
		// Might succeed in creating but fail during schema retrieval
		_, schemaErr := reader.Schema()
		assert.Error(t, schemaErr, "expected error for non-existent snapshot")
		_ = reader.Close()
	} else {
		assert.Error(t, err, "expected error for non-existent snapshot")
	}
}

// TestIntegrationReadWithLimit tests reading with a row limit.
func TestIntegrationReadWithLimit(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	reader, err := NewReader(ctx, tablePath, &ReaderOptions{
		Limit: 25,
	})
	require.NoError(t, err, "failed to create reader with limit")
	defer func() { _ = reader.Close() }()

	var totalRows int
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "failed to read chunk")
		totalRows += chunk.Count()
	}

	assert.Equal(t, 25, totalRows, "expected 25 rows with limit")
}

// TestIntegrationDataFiles tests reading data file metadata.
func TestIntegrationDataFiles(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	dataFiles, err := table.DataFiles(ctx)
	require.NoError(t, err, "failed to get data files")

	assert.Len(t, dataFiles, 1, "expected 1 data file")

	df := dataFiles[0]
	assert.Contains(t, df.Path, "00000-0-data.parquet")
	assert.Equal(t, FileFormatParquet, df.Format)
	assert.Equal(t, int64(100), df.RecordCount)
	assert.Greater(t, df.FileSizeBytes, int64(0))
}

// TestIntegrationManifests tests reading manifest metadata.
func TestIntegrationManifests(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	manifests, err := table.Manifests(ctx)
	require.NoError(t, err, "failed to get manifests")

	assert.Len(t, manifests, 1, "expected 1 manifest")

	mf := manifests[0]
	assert.Contains(t, mf.Path, "snap-1000000001-1-manifest.avro")
	assert.True(t, mf.IsDataManifest())
	assert.False(t, mf.IsDeleteManifest())
}

// TestIntegrationRowCount tests the row count estimation.
func TestIntegrationRowCount(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	rowCount, err := table.RowCount(ctx)
	require.NoError(t, err, "failed to get row count")

	assert.Equal(t, int64(100), rowCount, "expected 100 rows")
}

// TestIntegrationFileCount tests the file count.
func TestIntegrationFileCount(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	fileCount, err := table.FileCount(ctx)
	require.NoError(t, err, "failed to get file count")

	assert.Equal(t, int64(1), fileCount, "expected 1 data file")
}

// TestIntegrationSnapshotHistory tests snapshot history listing.
func TestIntegrationSnapshotHistory(t *testing.T) {
	tablePath := getTimeTravelTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	history := table.SnapshotHistory()
	assert.Len(t, history, 3, "expected 3 snapshot log entries")

	// Verify chronological order
	for i := 1; i < len(history); i++ {
		assert.Greater(t, history[i].TimestampMs, history[i-1].TimestampMs,
			"snapshot log should be in chronological order")
	}
}

// TestIntegrationTableFromMetadataPath tests opening table from specific metadata file.
func TestIntegrationTableFromMetadataPath(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()
	metadataPath := filepath.Join(tablePath, "metadata", "v1.metadata.json")

	table, err := OpenTableFromMetadata(ctx, metadataPath, nil)
	require.NoError(t, err, "failed to open table from metadata path")
	defer func() { _ = table.Close() }()

	// Verify table is valid
	assert.NotNil(t, table.Schema())
	assert.NotNil(t, table.CurrentSnapshot())
}

// TestIntegrationErrorInvalidPath tests error handling for invalid paths.
func TestIntegrationErrorInvalidPath(t *testing.T) {
	ctx := context.Background()

	_, err := NewReader(ctx, "/nonexistent/path/to/iceberg/table", nil)
	assert.Error(t, err, "expected error for non-existent path")
}

// TestIntegrationSchemaEvolution tests that schema is correctly retrieved.
func TestIntegrationSchemaEvolution(t *testing.T) {
	tablePath := getSimpleTablePath(t)
	updateMetadataLocations(t, tablePath)

	ctx := context.Background()

	table, err := OpenTable(ctx, tablePath, nil)
	require.NoError(t, err, "failed to open table")
	defer func() { _ = table.Close() }()

	columns, err := table.SchemaColumns()
	require.NoError(t, err, "failed to get schema columns")

	assert.Len(t, columns, 3)

	// Verify column properties
	assert.Equal(t, 1, columns[0].ID)
	assert.Equal(t, "id", columns[0].Name)

	assert.Equal(t, 2, columns[1].ID)
	assert.Equal(t, "name", columns[1].Name)

	assert.Equal(t, 3, columns[2].ID)
	assert.Equal(t, "value", columns[2].Name)
}
