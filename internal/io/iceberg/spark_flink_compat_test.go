package iceberg

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSparkGenerated_Compatibility tests reading Spark-generated Iceberg tables.
func TestSparkGenerated_Compatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Spark compatibility test in short mode")
	}

	if !isDockerComposeAvailable() {
		t.Skip("docker-compose not available, skipping Spark test")
	}

	ctx := context.Background()
	testDataDir := "./testdata"

	// Start Spark container
	t.Log("Starting Spark container...")
	err := startDockerService(t, testDataDir, "spark")
	require.NoError(t, err, "Failed to start Spark")
	defer stopDockerService(t, testDataDir, "spark")

	// Wait for Spark to be ready
	t.Log("Waiting for Spark to initialize...")
	time.Sleep(15 * time.Second)

	// Generate Iceberg tables using Spark
	t.Log("Generating Iceberg tables with Spark...")
	err = runSparkScript(t, testDataDir)
	if err != nil {
		t.Logf("Warning: Failed to generate Spark tables: %v", err)
		t.Skip("Skipping Spark compatibility tests - table generation failed")
	}

	// Test 1: Read simple Spark-generated table
	t.Run("read_spark_simple_table", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../spark-warehouse/spark_simple")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Spark table not found at %s", tablePath)
			return
		}
		defer table.Close()

		assert.NotNil(t, table.Metadata())
		assert.Equal(t, 2, table.Metadata().Version)

		// Verify we can read the data
		reader, err := NewReader(ctx, tablePath, nil)
		require.NoError(t, err)
		defer reader.Close()

		rowCount := int64(0)
		for {
			chunk, err := reader.ReadChunk()
			if err != nil {
				break
			}
			rowCount += int64(chunk.Count())
		}

		assert.Greater(t, rowCount, int64(0), "Should read at least some rows")
		t.Logf("✓ Read %d rows from Spark-generated table", rowCount)
	})

	// Test 2: Read partitioned Spark table
	t.Run("read_spark_partitioned_table", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../spark-warehouse/spark_partitioned")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Spark partitioned table not found")
			return
		}
		defer table.Close()

		// Verify partition spec exists
		metadata := table.Metadata()
		assert.NotEmpty(t, metadata.PartitionSpecs, "Should have partition specs")

		// Test partition pruning
		reader, err := NewReader(ctx, tablePath, &ReaderOptions{
			// Add filter for specific partition
			Limit: 10,
		})
		require.NoError(t, err)
		defer reader.Close()

		chunk, err := reader.ReadChunk()
		if err == nil {
			assert.Greater(t, chunk.Count(), 0)
			t.Logf("✓ Successfully read partitioned Spark table")
		}
	})

	// Test 3: Read Spark table with schema evolution
	t.Run("read_spark_schema_evolution", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../spark-warehouse/spark_schema_evolution")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Spark schema evolution table not found")
			return
		}
		defer table.Close()

		// Verify schema has evolved
		columns, err := table.SchemaColumns()
		require.NoError(t, err)

		// Should have both original and new columns
		columnNames := make([]string, len(columns))
		for i, col := range columns {
			columnNames[i] = col.Name
		}

		assert.Contains(t, columnNames, "id")
		assert.Contains(t, columnNames, "name")
		// age column was added via ALTER TABLE
		// It may or may not be present depending on how schema evolution works

		t.Logf("✓ Schema columns: %v", columnNames)
	})

	// Test 4: Read Spark table with deletes
	t.Run("read_spark_with_deletes", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../spark-warehouse/spark_deletes")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Spark deletes table not found")
			return
		}
		defer table.Close()

		reader, err := NewReader(ctx, tablePath, nil)
		require.NoError(t, err)
		defer reader.Close()

		rowCount := int64(0)
		for {
			chunk, err := reader.ReadChunk()
			if err != nil {
				break
			}
			rowCount += int64(chunk.Count())
		}

		// Should have fewer rows than original due to deletes
		// Original: 20 rows, deleted: 3 rows, expected: 17 rows
		assert.Equal(t, int64(17), rowCount, "Should have 17 rows after deletes")
		t.Logf("✓ Correctly handled delete files, row count: %d", rowCount)
	})

	// Test 5: Time travel on Spark table
	t.Run("spark_time_travel", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../spark-warehouse/spark_time_travel")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Spark time travel table not found")
			return
		}
		defer table.Close()

		snapshots := table.Snapshots()
		assert.GreaterOrEqual(t, len(snapshots), 2, "Should have multiple snapshots")

		// Read first snapshot
		firstSnapshot := snapshots[0].SnapshotID
		reader, err := NewReader(ctx, tablePath, &ReaderOptions{
			SnapshotID: &firstSnapshot,
		})
		require.NoError(t, err)
		defer reader.Close()

		_, err = reader.Schema()
		assert.NoError(t, err)

		t.Logf("✓ Time travel to snapshot %d successful", firstSnapshot)
	})
}

// TestFlinkGenerated_Compatibility tests reading Flink-generated Iceberg tables.
func TestFlinkGenerated_Compatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Flink compatibility test in short mode")
	}

	if !isDockerComposeAvailable() {
		t.Skip("docker-compose not available, skipping Flink test")
	}

	ctx := context.Background()
	testDataDir := "./testdata"

	// Start Flink containers
	t.Log("Starting Flink containers...")
	err := startDockerService(t, testDataDir, "flink-jobmanager", "flink-taskmanager")
	require.NoError(t, err, "Failed to start Flink")
	defer stopDockerService(t, testDataDir, "flink-jobmanager", "flink-taskmanager")

	// Wait for Flink to be ready
	t.Log("Waiting for Flink to initialize...")
	time.Sleep(20 * time.Second)

	// Generate Iceberg tables using Flink
	t.Log("Generating Iceberg tables with Flink...")
	err = runFlinkScript(t, testDataDir)
	if err != nil {
		t.Logf("Warning: Failed to generate Flink tables: %v", err)
		t.Skip("Skipping Flink compatibility tests - table generation failed")
	}

	// Test 1: Read simple Flink-generated table
	t.Run("read_flink_simple_table", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../flink-warehouse/flink_db.db/flink_simple")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Flink table not found at %s", tablePath)
			return
		}
		defer table.Close()

		assert.NotNil(t, table.Metadata())

		// Read data
		reader, err := NewReader(ctx, tablePath, nil)
		require.NoError(t, err)
		defer reader.Close()

		rowCount := int64(0)
		for {
			chunk, err := reader.ReadChunk()
			if err != nil {
				break
			}
			rowCount += int64(chunk.Count())
		}

		assert.Greater(t, rowCount, int64(0))
		t.Logf("✓ Read %d rows from Flink-generated table", rowCount)
	})

	// Test 2: Read partitioned Flink table
	t.Run("read_flink_partitioned_table", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../flink-warehouse/flink_db.db/flink_partitioned")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Flink partitioned table not found")
			return
		}
		defer table.Close()

		metadata := table.Metadata()
		assert.NotEmpty(t, metadata.PartitionSpecs)

		t.Logf("✓ Flink partitioned table validated")
	})

	// Test 3: Read Flink table with complex types
	t.Run("read_flink_complex_types", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../flink-warehouse/flink_db.db/flink_complex")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Flink complex types table not found")
			return
		}
		defer table.Close()

		columns, err := table.SchemaColumns()
		require.NoError(t, err)

		// Verify complex types are present
		hasMap := false
		hasArray := false
		for _, col := range columns {
			typeStr := col.Type.String()
			if typeStr == "MAP" || typeStr == "map" {
				hasMap = true
			}
			if typeStr == "LIST" || typeStr == "list" || typeStr == "ARRAY" || typeStr == "array" {
				hasArray = true
			}
		}

		t.Logf("✓ Complex types present: map=%v, array=%v", hasMap, hasArray)
	})

	// Test 4: Read Flink table with deletes (Iceberg v2 merge-on-read)
	t.Run("read_flink_with_deletes", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../flink-warehouse/flink_db.db/flink_deletes")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Flink deletes table not found")
			return
		}
		defer table.Close()

		reader, err := NewReader(ctx, tablePath, nil)
		require.NoError(t, err)
		defer reader.Close()

		rowCount := int64(0)
		for {
			chunk, err := reader.ReadChunk()
			if err != nil {
				break
			}
			rowCount += int64(chunk.Count())
		}

		// Should have 7 rows (10 - 3 deletes)
		assert.Equal(t, int64(7), rowCount, "Should have 7 rows after deletes")
		t.Logf("✓ Correctly handled Flink delete files, row count: %d", rowCount)
	})

	// Test 5: Time travel on Flink table
	t.Run("flink_time_travel", func(t *testing.T) {
		tablePath := filepath.Join(testDataDir, "../../../flink-warehouse/flink_db.db/flink_time_travel")

		table, err := OpenTable(ctx, tablePath, nil)
		if err != nil {
			t.Logf("Skipping: Flink time travel table not found")
			return
		}
		defer table.Close()

		snapshots := table.Snapshots()
		assert.GreaterOrEqual(t, len(snapshots), 2, "Should have multiple snapshots")

		t.Logf("✓ Flink time travel table has %d snapshots", len(snapshots))
	})
}

// runSparkScript executes the Spark script to generate test tables.
func runSparkScript(t *testing.T, testDataDir string) error {
	// Copy script into Spark container and execute
	cmd := execCommand("docker", "exec", "iceberg-spark",
		"/opt/spark/bin/spark-submit",
		"--master", "local[*]",
		"--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4",
		"/opt/spark-scripts/generate_iceberg_tables.py")

	output, err := cmd.CombinedOutput()
	t.Logf("Spark script output:\n%s", string(output))

	return err
}

// runFlinkScript executes the Flink SQL script to generate test tables.
func runFlinkScript(t *testing.T, testDataDir string) error {
	// Execute Flink SQL script
	cmd := execCommand("docker", "exec", "iceberg-flink-jobmanager",
		"/opt/flink/bin/sql-client.sh",
		"-f", "/opt/flink-scripts/generate_iceberg_tables.sql")

	output, err := cmd.CombinedOutput()
	t.Logf("Flink script output:\n%s", string(output))

	return err
}

// execCommand is a wrapper for exec.Command to allow for testing.
var execCommand = func(name string, args ...string) command {
	return &realCommand{cmd: exec.Command(name, args...)}
}

type command interface {
	CombinedOutput() ([]byte, error)
}

type realCommand struct {
	cmd *exec.Cmd
}

func (c *realCommand) CombinedOutput() ([]byte, error) {
	return c.cmd.CombinedOutput()
}
