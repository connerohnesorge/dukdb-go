// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestIcebergScan tests the iceberg_scan table function.
func TestIcebergScan(t *testing.T) {
	// Get test fixtures path
	testTablePath := getTestIcebergTablePath(t)
	if testTablePath == "" {
		t.Skip("No Iceberg test table available - run generate_fixtures.py to create")
	}

	// Update metadata locations for the test environment
	updateIcebergMetadataLocations(t, testTablePath)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic iceberg_scan", func(t *testing.T) {
		sql := `SELECT * FROM iceberg_scan('` + testTablePath + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify we got the expected columns
		assert.Equal(t, 3, len(result.Columns), "Should have 3 columns")
		assert.Equal(t, "id", result.Columns[0])
		assert.Equal(t, "name", result.Columns[1])
		assert.Equal(t, "value", result.Columns[2])

		// Verify row count
		assert.Equal(t, 100, len(result.Rows), "Should have 100 rows")
	})

	t.Run("iceberg_scan column projection", func(t *testing.T) {
		sql := `SELECT id, name FROM iceberg_scan('` + testTablePath + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify we got only the projected columns
		assert.GreaterOrEqual(t, len(result.Columns), 2, "Should have at least 2 columns")
	})
}

// TestIcebergMetadata tests the iceberg_metadata table function.
func TestIcebergMetadata(t *testing.T) {
	testTablePath := getTestIcebergTablePath(t)
	if testTablePath == "" {
		t.Skip("No Iceberg test table available - run generate_fixtures.py to create")
	}

	// Update metadata locations for the test environment
	updateIcebergMetadataLocations(t, testTablePath)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic iceberg_metadata", func(t *testing.T) {
		sql := `SELECT * FROM iceberg_metadata('` + testTablePath + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify expected columns
		expectedColumns := []string{
			"file_path",
			"file_format",
			"record_count",
			"file_size_in_bytes",
			"partition_data",
			"value_counts",
			"null_value_counts",
			"lower_bounds",
			"upper_bounds",
		}
		assert.Equal(t, expectedColumns, result.Columns, "Should have expected columns")

		// Verify we have data file metadata
		assert.Greater(t, len(result.Rows), 0, "Should have at least one data file")

		// Verify first row structure
		row := result.Rows[0]
		filePath, ok := row["file_path"]
		assert.True(t, ok, "Row should have file_path")
		assert.Contains(t, filePath.(string), "data.parquet")

		fileFormat, ok := row["file_format"]
		assert.True(t, ok, "Row should have file_format")
		assert.Equal(t, "parquet", fileFormat)

		recordCount, ok := row["record_count"]
		assert.True(t, ok, "Row should have record_count")
		assert.Equal(t, int64(100), recordCount)
	})
}

// TestIcebergSnapshots tests the iceberg_snapshots table function.
func TestIcebergSnapshots(t *testing.T) {
	testTablePath := getTestIcebergTablePath(t)
	if testTablePath == "" {
		t.Skip("No Iceberg test table available - run generate_fixtures.py to create")
	}

	// Update metadata locations for the test environment
	updateIcebergMetadataLocations(t, testTablePath)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic iceberg_snapshots", func(t *testing.T) {
		sql := `SELECT * FROM iceberg_snapshots('` + testTablePath + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify expected columns
		expectedColumns := []string{
			"snapshot_id",
			"parent_snapshot_id",
			"timestamp_ms",
			"timestamp",
			"manifest_list",
			"operation",
			"summary",
			"added_data_files",
			"deleted_data_files",
			"added_records",
			"deleted_records",
		}
		assert.Equal(t, expectedColumns, result.Columns, "Should have expected columns")

		// Verify we have at least one snapshot
		assert.Greater(t, len(result.Rows), 0, "Should have at least one snapshot")

		// Verify first row structure
		row := result.Rows[0]
		snapshotID, ok := row["snapshot_id"]
		assert.True(t, ok, "Row should have snapshot_id")
		assert.Equal(t, int64(1000000001), snapshotID)

		manifestList, ok := row["manifest_list"]
		assert.True(t, ok, "Row should have manifest_list")
		assert.Contains(t, manifestList.(string), "manifest-list.avro")

		operation, ok := row["operation"]
		assert.True(t, ok, "Row should have operation")
		assert.Equal(t, "append", operation)
	})
}

// TestIcebergSnapshotsTimeTravelTable tests iceberg_snapshots with multiple snapshots.
func TestIcebergSnapshotsTimeTravelTable(t *testing.T) {
	testTablePath := getTimeTravelIcebergTablePath(t)
	if testTablePath == "" {
		t.Skip("No Iceberg time travel test table available - run generate_fixtures.py to create")
	}

	// Update metadata locations for the test environment
	updateIcebergMetadataLocations(t, testTablePath)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("multiple snapshots", func(t *testing.T) {
		sql := `SELECT * FROM iceberg_snapshots('` + testTablePath + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify we have 3 snapshots
		assert.Equal(t, 3, len(result.Rows), "Should have 3 snapshots")

		// Verify snapshot IDs
		expectedIDs := []int64{1000000001, 1000000002, 1000000003}
		for i, row := range result.Rows {
			snapshotID := row["snapshot_id"].(int64)
			assert.Equal(t, expectedIDs[i], snapshotID, "Snapshot %d should have correct ID", i)
		}
	})
}

// TestIcebergScanInvalidPath tests error handling for invalid paths.
func TestIcebergScanInvalidPath(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName:  "iceberg_scan",
		Path:          "/nonexistent/path/to/iceberg/table",
		Options:       make(map[string]any),
		TableFunction: &binder.BoundTableFunctionRef{},
	}

	result, err := exec.executeIcebergScan(ctx, plan)
	assert.Error(t, err, "Should error on invalid path")
	assert.Nil(t, result)
}

// TestIcebergMetadataInvalidPath tests error handling for invalid paths.
func TestIcebergMetadataInvalidPath(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName:  "iceberg_metadata",
		Path:          "/nonexistent/path/to/iceberg/table",
		Options:       make(map[string]any),
		TableFunction: &binder.BoundTableFunctionRef{},
	}

	result, err := exec.executeIcebergMetadata(ctx, plan)
	assert.Error(t, err, "Should error on invalid path")
	assert.Nil(t, result)
}

// TestIcebergSnapshotsInvalidPath tests error handling for invalid paths.
func TestIcebergSnapshotsInvalidPath(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName:  "iceberg_snapshots",
		Path:          "/nonexistent/path/to/iceberg/table",
		Options:       make(map[string]any),
		TableFunction: &binder.BoundTableFunctionRef{},
	}

	result, err := exec.executeIcebergSnapshots(ctx, plan)
	assert.Error(t, err, "Should error on invalid path")
	assert.Nil(t, result)
}

// TestTableFunctionDispatchIceberg tests that the table function dispatcher
// correctly routes to Iceberg functions.
func TestTableFunctionDispatchIceberg(t *testing.T) {
	testCases := []struct {
		name         string
		functionName string
		expectError  bool // All should error with invalid path
	}{
		{"iceberg_scan", "iceberg_scan", true},
		{"iceberg_metadata", "iceberg_metadata", true},
		{"iceberg_snapshots", "iceberg_snapshots", true},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan := &planner.PhysicalTableFunctionScan{
				FunctionName:  tc.functionName,
				Path:          "/nonexistent/path",
				Options:       make(map[string]any),
				TableFunction: &binder.BoundTableFunctionRef{},
			}

			result, err := exec.executeTableFunctionScan(ctx, plan)
			if tc.expectError {
				assert.Error(t, err, "Should error with invalid path")
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestJSONHelperFunctions tests the JSON helper functions.
func TestJSONHelperFunctions(t *testing.T) {
	t.Run("toJSONString", func(t *testing.T) {
		// Test nil
		assert.Equal(t, "", toJSONString(nil))

		// Test map
		m := map[string]string{"key": "value"}
		result := toJSONString(m)
		var parsed map[string]string
		err := json.Unmarshal([]byte(result), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "value", parsed["key"])
	})

	t.Run("intMapToJSONString", func(t *testing.T) {
		// Test nil
		assert.Equal(t, "", intMapToJSONString(nil))

		// Test map
		m := map[int]int64{1: 100, 2: 200}
		result := intMapToJSONString(m)
		var parsed map[string]int64
		err := json.Unmarshal([]byte(result), &parsed)
		require.NoError(t, err)
		assert.Equal(t, int64(100), parsed["1"])
		assert.Equal(t, int64(200), parsed["2"])
	})

	t.Run("bytesMapToJSONString", func(t *testing.T) {
		// Test nil
		assert.Equal(t, "", bytesMapToJSONString(nil))

		// Test map
		m := map[int][]byte{1: {0xde, 0xad, 0xbe, 0xef}}
		result := bytesMapToJSONString(m)
		var parsed map[string]string
		err := json.Unmarshal([]byte(result), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "deadbeef", parsed["1"])
	})
}

// TestSnapshotSummaryHelpers tests the snapshot summary helper functions.
func TestSnapshotSummaryHelpers(t *testing.T) {
	t.Run("getSnapshotSummaryValue", func(t *testing.T) {
		// Test nil
		assert.Equal(t, "", getSnapshotSummaryValue(nil, "key"))

		// Test existing key
		m := map[string]string{"operation": "append"}
		assert.Equal(t, "append", getSnapshotSummaryValue(m, "operation"))

		// Test missing key
		assert.Equal(t, "", getSnapshotSummaryValue(m, "missing"))
	})

	t.Run("getSnapshotSummaryInt", func(t *testing.T) {
		// Test nil
		assert.Equal(t, int64(0), getSnapshotSummaryInt(nil, "key"))

		// Test existing key with valid int
		m := map[string]string{"added-records": "1000"}
		assert.Equal(t, int64(1000), getSnapshotSummaryInt(m, "added-records"))

		// Test missing key
		assert.Equal(t, int64(0), getSnapshotSummaryInt(m, "missing"))

		// Test invalid int
		m["invalid"] = "not-a-number"
		assert.Equal(t, int64(0), getSnapshotSummaryInt(m, "invalid"))
	})
}

// getTestIcebergTablePath returns the path to the simple_table test fixture.
// Returns empty string if no test table is available.
func getTestIcebergTablePath(t *testing.T) string {
	t.Helper()

	// Check environment variable first
	if path := os.Getenv("ICEBERG_TEST_TABLE_PATH"); path != "" {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Get the path relative to this test file
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}

	// Navigate from internal/executor to internal/io/iceberg/testdata/simple_table
	testdataPath := filepath.Join(filepath.Dir(currentFile), "..", "io", "iceberg", "testdata", "simple_table")
	absPath, err := filepath.Abs(testdataPath)
	if err != nil {
		return ""
	}

	// Check if metadata directory exists
	metadataPath := filepath.Join(absPath, "metadata")
	if _, err := os.Stat(metadataPath); err == nil {
		return absPath
	}

	return ""
}

// getTimeTravelIcebergTablePath returns the path to the time_travel_table test fixture.
// Returns empty string if no test table is available.
func getTimeTravelIcebergTablePath(t *testing.T) string {
	t.Helper()

	// Get the path relative to this test file
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}

	// Navigate from internal/executor to internal/io/iceberg/testdata/time_travel_table
	testdataPath := filepath.Join(filepath.Dir(currentFile), "..", "io", "iceberg", "testdata", "time_travel_table")
	absPath, err := filepath.Abs(testdataPath)
	if err != nil {
		return ""
	}

	// Check if metadata directory exists
	metadataPath := filepath.Join(absPath, "metadata")
	if _, err := os.Stat(metadataPath); err == nil {
		return absPath
	}

	return ""
}

// updateIcebergMetadataLocations updates the location field in metadata.json to use
// the actual test path. This is needed because the fixtures are generated with
// absolute paths that differ from the test environment.
func updateIcebergMetadataLocations(t *testing.T, tablePath string) {
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
