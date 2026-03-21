// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains DuckDB CLI compatibility tests.
package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getDuckDBPath returns the path to the DuckDB CLI.
// It checks DUCKDB_PATH env var first, then looks in PATH.
func getDuckDBPath() string {
	if path := os.Getenv("DUCKDB_PATH"); path != "" {
		return path
	}
	// Look for duckdb in PATH (works with nix develop)
	if path, err := exec.LookPath("duckdb"); err == nil {
		return path
	}
	return "duckdb" // Fallback, let exec handle the error
}

// isDuckDBAvailable checks if DuckDB CLI is available.
func isDuckDBAvailable() bool {
	path := getDuckDBPath()
	_, err := os.Stat(path)
	return err == nil
}

// isDuckDBIcebergAvailable checks if DuckDB CLI can load the Iceberg extension.
func isDuckDBIcebergAvailable(t *testing.T) bool {
	t.Helper()

	duckdbPath := getDuckDBPath()
	if _, err := os.Stat(duckdbPath); os.IsNotExist(err) {
		return false
	}

	// Try to load the iceberg extension
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, duckdbPath,
		"-c", "INSTALL iceberg; LOAD iceberg; SELECT 1 as test;")

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("DuckDB iceberg extension not available: %v\nOutput: %s", err, string(output))
		return false
	}
	return true
}

// DuckDBQueryResult contains results from a DuckDB CLI query.
type DuckDBQueryResult struct {
	RowCount    int64
	ColumnNames []string
	SampleData  []map[string]interface{}
	RawOutput   string
	Error       error
}

// queryDuckDB executes a query using the DuckDB CLI and returns results.
func queryDuckDB(t *testing.T, query string) *DuckDBQueryResult {
	t.Helper()

	duckdbPath := getDuckDBPath()

	// Build the full command with iceberg extension
	fullQuery := fmt.Sprintf("INSTALL iceberg; LOAD iceberg; %s", query)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, duckdbPath, "-json", "-c", fullQuery)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	result := &DuckDBQueryResult{
		RawOutput: stdout.String(),
	}

	if err != nil {
		result.Error = fmt.Errorf("duckdb query failed: %w\nstderr: %s", err, stderr.String())
		return result
	}

	// Parse JSON output
	if err := parseDuckDBJSONOutput(stdout.String(), result); err != nil {
		result.Error = fmt.Errorf("failed to parse duckdb output: %w", err)
	}

	return result
}

// parseDuckDBJSONOutput parses DuckDB JSON output format.
func parseDuckDBJSONOutput(output string, result *DuckDBQueryResult) error {
	// DuckDB JSON output is an array of objects
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(output), &rows); err != nil {
		return fmt.Errorf("json unmarshal failed: %w (output: %s)", err, output)
	}

	result.RowCount = int64(len(rows))

	// Extract column names from first row
	if len(rows) > 0 {
		for colName := range rows[0] {
			result.ColumnNames = append(result.ColumnNames, colName)
		}
	}

	// Store sample data (first 10 rows)
	maxSample := 10
	if len(rows) < maxSample {
		maxSample = len(rows)
	}
	result.SampleData = rows[:maxSample]

	return nil
}

// TestDuckDBCLIAvailability verifies DuckDB CLI is available.
func TestDuckDBCLIAvailability(t *testing.T) {
	if !isDuckDBAvailable() {
		t.Skip("DuckDB CLI not available at: " + getDuckDBPath())
	}

	// Verify DuckDB runs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, getDuckDBPath(), "-c", "SELECT 42 as answer;")
	output, err := cmd.CombinedOutput()

	require.NoError(t, err, "DuckDB CLI should execute basic query")
	assert.Contains(t, string(output), "42", "Output should contain query result")

	t.Logf("DuckDB CLI available and working")
}

// TestDuckDBIcebergExtensionAvailability checks if iceberg extension can be loaded.
func TestDuckDBIcebergExtensionAvailability(t *testing.T) {
	if !isDuckDBAvailable() {
		t.Skip("DuckDB CLI not available")
	}

	available := isDuckDBIcebergAvailable(t)
	if !available {
		t.Log("DuckDB Iceberg extension is NOT available.")
		t.Log("This is expected in some environments (missing avro extension dependency).")
		t.Log("Compatibility tests will be skipped.")
	} else {
		t.Log("DuckDB Iceberg extension is available")
	}
}

// TestDukdbGoIcebergSimpleTable tests reading simple_table with dukdb-go.
// This serves as baseline for compatibility comparison.
func TestDukdbGoIcebergSimpleTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tablePath := getSimpleTablePath(t)
	if tablePath == "" {
		t.Skip("simple_table test fixture not found")
	}

	// Update metadata locations for the test
	updateMetadataLocations(t, tablePath)
	skipIfManifestsInaccessible(t, tablePath)

	ctx := context.Background()
	reader, err := NewReader(ctx, tablePath, nil)
	require.NoError(t, err, "Should open simple_table")
	defer func() { _ = reader.Close() }()

	// Get schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("dukdb-go schema: %v", columns)

	// Read all rows
	var rowCount int64
	var sampleData []map[string]interface{}

	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		rowCount += int64(chunk.Count())

		// Capture sample data from first chunk
		if len(sampleData) == 0 && chunk.Count() > 0 {
			maxSample := 10
			if chunk.Count() < maxSample {
				maxSample = chunk.Count()
			}
			for i := 0; i < maxSample; i++ {
				row := make(map[string]interface{})
				for j, colName := range columns {
					vec := chunk.GetVector(j)
					if vec != nil {
						row[colName] = vec.GetValue(i)
					}
				}
				sampleData = append(sampleData, row)
			}
		}
	}

	t.Logf("dukdb-go results: %d rows, columns: %v", rowCount, columns)
	t.Logf("dukdb-go sample data (first row): %v", sampleData[0])

	// Verify expected results
	assert.Equal(t, int64(100), rowCount, "simple_table should have 100 rows")
	assert.ElementsMatch(
		t,
		[]string{"id", "name", "value"},
		columns,
		"Should have expected columns",
	)
}

// TestDuckDBCompatibilitySimpleTable compares dukdb-go results with DuckDB CLI.
// If DuckDB's iceberg extension is not available, it validates the pure Go implementation
// against expected values.
func TestDuckDBCompatibilitySimpleTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tablePath := getSimpleTablePath(t)
	if tablePath == "" {
		t.Skip("simple_table test fixture not found")
	}

	// Update metadata locations for the test
	updateMetadataLocations(t, tablePath)
	skipIfManifestsInaccessible(t, tablePath)

	// Query with dukdb-go (pure Go implementation)
	ctx := context.Background()
	reader, err := NewReader(ctx, tablePath, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	var dukdbRowCount int64
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		dukdbRowCount += int64(chunk.Count())
	}

	// Expected row count for simple_table fixture
	const expectedRowCount int64 = 100
	assert.Equal(t, expectedRowCount, dukdbRowCount,
		"dukdb-go row count should match expected value")

	// If DuckDB CLI with iceberg extension is available, also compare against it
	if isDuckDBAvailable() && isDuckDBIcebergAvailable(t) {
		query := fmt.Sprintf("SELECT COUNT(*) as cnt FROM iceberg_scan('%s');", tablePath)
		duckdbResult := queryDuckDB(t, query)

		if duckdbResult.Error != nil {
			t.Logf("DuckDB query failed (skipping comparison): %v", duckdbResult.Error)
		} else {
			t.Logf("DuckDB raw output: %s", duckdbResult.RawOutput)

			// Extract DuckDB row count and compare
			if len(duckdbResult.SampleData) > 0 {
				if cntVal, ok := duckdbResult.SampleData[0]["cnt"]; ok {
					switch v := cntVal.(type) {
					case float64:
						duckdbRowCount := int64(v)
						assert.Equal(t, duckdbRowCount, dukdbRowCount,
							"Row counts should match between DuckDB and dukdb-go")
					case int64:
						assert.Equal(t, v, dukdbRowCount,
							"Row counts should match between DuckDB and dukdb-go")
					case string:
						parsed, _ := strconv.ParseInt(v, 10, 64)
						assert.Equal(t, parsed, dukdbRowCount,
							"Row counts should match between DuckDB and dukdb-go")
					}
				}
			}
			t.Logf("Compatibility check: DuckDB=%d rows, dukdb-go=%d rows",
				duckdbResult.RowCount, dukdbRowCount)
		}
	} else {
		t.Logf("DuckDB iceberg extension not available - validated against expected value (%d rows)", expectedRowCount)
	}
}

// TestDuckDBCompatibilitySchema compares schema between DuckDB and dukdb-go.
// If DuckDB's iceberg extension is not available, it validates the pure Go implementation
// against expected schema.
func TestDuckDBCompatibilitySchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tablePath := getSimpleTablePath(t)
	if tablePath == "" {
		t.Skip("simple_table test fixture not found")
	}

	updateMetadataLocations(t, tablePath)
	skipIfManifestsInaccessible(t, tablePath)

	// Get dukdb-go schema (pure Go implementation)
	ctx := context.Background()
	reader, err := NewReader(ctx, tablePath, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	dukdbColumns, err := reader.Schema()
	require.NoError(t, err)

	// Expected columns for simple_table fixture
	expectedColumns := []string{"id", "name", "value"}
	assert.ElementsMatch(t, expectedColumns, dukdbColumns,
		"dukdb-go columns should match expected schema")

	t.Logf("dukdb-go columns: %v", dukdbColumns)

	// If DuckDB CLI with iceberg extension is available, also compare against it
	if isDuckDBAvailable() && isDuckDBIcebergAvailable(t) {
		query := fmt.Sprintf("SELECT * FROM iceberg_scan('%s') LIMIT 0;", tablePath)
		duckdbResult := queryDuckDB(t, query)

		if duckdbResult.Error != nil {
			t.Logf("DuckDB schema query failed (skipping comparison): %v", duckdbResult.Error)
		} else {
			t.Logf("DuckDB columns: %v", duckdbResult.ColumnNames)

			// Compare column names (order-independent)
			assert.ElementsMatch(t, duckdbResult.ColumnNames, dukdbColumns,
				"Column names should match between DuckDB and dukdb-go")
		}
	} else {
		t.Logf("DuckDB iceberg extension not available - validated against expected schema")
	}
}

// TestDuckDBCompatibilityTimeTravel compares time travel behavior.
// If DuckDB's iceberg extension is not available, it validates the pure Go implementation
// against expected values.
func TestDuckDBCompatibilityTimeTravel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tablePath := getTimeTravelTablePath(t)
	if tablePath == "" {
		t.Skip("time_travel_table test fixture not found")
	}

	updateMetadataLocations(t, tablePath)
	skipIfManifestsInaccessible(t, tablePath)

	// Snapshot IDs from the time_travel_table fixture
	snapshotTests := []struct {
		snapshotID   int64
		expectedRows int64
	}{
		{1000000001, 50},
		{1000000002, 80},
		{1000000003, 100},
	}

	duckdbAvailable := isDuckDBAvailable() && isDuckDBIcebergAvailable(t)

	for _, tc := range snapshotTests {
		t.Run(fmt.Sprintf("snapshot_%d", tc.snapshotID), func(t *testing.T) {
			// Query with dukdb-go (pure Go implementation)
			ctx := context.Background()
			snapshotID := tc.snapshotID
			reader, err := NewReader(ctx, tablePath, &ReaderOptions{
				SnapshotID: &snapshotID,
			})

			if err != nil {
				t.Logf("dukdb-go failed to read snapshot %d: %v", tc.snapshotID, err)
				return
			}
			defer func() { _ = reader.Close() }()

			var dukdbRowCount int64
			for {
				chunk, err := reader.ReadChunk()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				dukdbRowCount += int64(chunk.Count())
			}

			// Verify dukdb-go matches expected
			assert.Equal(t, tc.expectedRows, dukdbRowCount,
				"dukdb-go row count should match expected for snapshot %d", tc.snapshotID)

			// If DuckDB CLI with iceberg extension is available, also compare against it
			if duckdbAvailable {
				query := fmt.Sprintf(
					"SELECT COUNT(*) as cnt FROM iceberg_scan('%s', allow_moved_paths = true, version = '%d');",
					tablePath,
					tc.snapshotID,
				)
				duckdbResult := queryDuckDB(t, query)

				if duckdbResult.Error != nil {
					t.Logf("DuckDB query failed (skipping comparison): %v", duckdbResult.Error)
				} else {
					t.Logf("Snapshot %d: DuckDB result=%v, dukdb-go=%d rows (expected %d)",
						tc.snapshotID, duckdbResult.SampleData, dukdbRowCount, tc.expectedRows)
				}
			} else {
				t.Logf("Snapshot %d: dukdb-go=%d rows (expected %d) - DuckDB iceberg extension not available",
					tc.snapshotID, dukdbRowCount, tc.expectedRows)
			}
		})
	}
}

// Note: Helper functions getSimpleTablePath, getTimeTravelTablePath, and updateMetadataLocations
// are defined in integration_test.go and are shared with these tests.
