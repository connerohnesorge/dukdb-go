package duckdb

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugWithLogging(t *testing.T) {
	duckdbPath, err := exec.LookPath("duckdb")
	if err != nil {
		t.Skip("duckdb CLI not found")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "allnull_varchar.duckdb")

	sql := `CREATE TABLE all_null_test (int_col INTEGER, varchar_col VARCHAR);
		INSERT INTO all_null_test VALUES (NULL, NULL), (NULL, NULL), (NULL, NULL); CHECKPOINT;`

	cmd := exec.Command(duckdbPath, dbPath, "-c", sql)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Error creating DB: %v", err)
	}

	storage, err := OpenDuckDBStorage(dbPath, nil)
	if err != nil {
		t.Fatalf("Error opening storage: %v", err)
	}
	defer storage.Close()

	table := storage.catalog.GetTable("all_null_test")
	if table == nil {
		t.Fatal("Table not found")
	}

	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		table.StorageMetadata.TablePointer,
		table.StorageMetadata.TotalRows,
		len(table.Columns),
	)
	if err != nil {
		t.Fatalf("Error reading row groups: %v", err)
	}

	if len(rowGroups) > 0 {
		mbp := rowGroups[0].DataPointers[1]
		
		// Use the actual ReadColumnDataPointer
		dp, err := ReadColumnDataPointer(storage.blockManager, mbp)
		if err != nil {
			t.Fatalf("ReadColumnDataPointer failed: %v", err)
		}
		
		t.Logf("DataPointer result:")
		t.Logf("  TupleCount: %d", dp.TupleCount)
		t.Logf("  Block.BlockID: %d", dp.Block.BlockID)
		t.Logf("  Compression: %s", dp.Compression.String())
		t.Logf("  Statistics.HasStats: %v", dp.Statistics.HasStats)
		t.Logf("  Statistics.HasNull: %v", dp.Statistics.HasNull)
		t.Logf("  ValidityPointer: %v", dp.ValidityPointer != nil)
		if dp.ValidityPointer != nil {
			t.Logf("    VP.Block.BlockID: %d", dp.ValidityPointer.Block.BlockID)
			t.Logf("    VP.Compression: %s", dp.ValidityPointer.Compression.String())
			t.Logf("    VP.Statistics.HasStats: %v", dp.ValidityPointer.Statistics.HasStats)
		}
	}
}
