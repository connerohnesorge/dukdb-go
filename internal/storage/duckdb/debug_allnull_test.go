package duckdb

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugAllNullVarcharParsing(t *testing.T) {
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

	t.Logf("Table: %s", table.Name)
	t.Logf("TotalRows: %d", table.StorageMetadata.TotalRows)

	// Read row groups
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
		t.Logf("RowGroup 0:")
		t.Logf("  TupleCount: %d", rowGroups[0].TupleCount)
		t.Logf("  DataPointers: %d columns", len(rowGroups[0].DataPointers))
		
		// Log the MetaBlockPointers
		for i, mbp := range rowGroups[0].DataPointers {
			t.Logf("  Column %d MBP: BlockID=%d, BlockIndex=%d, Offset=%d", 
				i, mbp.BlockID, mbp.BlockIndex, mbp.Offset)
		}
		
		// Read DataPointer for column 0 (INTEGER)
		dp0, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[0])
		if err != nil {
			t.Fatalf("Error reading column 0 DataPointer: %v", err)
		}
		t.Logf("\nColumn 0 (INTEGER) DataPointer:")
		t.Logf("  TupleCount: %d", dp0.TupleCount)
		t.Logf("  Block: ID=%d, Offset=%d", dp0.Block.BlockID, dp0.Block.Offset)
		t.Logf("  Compression: %s", dp0.Compression.String())
		t.Logf("  Statistics.HasStats: %v", dp0.Statistics.HasStats)
		t.Logf("  Statistics.HasNull: %v", dp0.Statistics.HasNull)
		t.Logf("  Statistics.StatData len: %d", len(dp0.Statistics.StatData))
		if dp0.ValidityPointer != nil {
			t.Logf("  ValidityPointer: EXISTS")
			t.Logf("    Block: ID=%d, Offset=%d", dp0.ValidityPointer.Block.BlockID, dp0.ValidityPointer.Block.Offset)
			t.Logf("    Compression: %s", dp0.ValidityPointer.Compression.String())
			t.Logf("    Statistics.HasStats: %v", dp0.ValidityPointer.Statistics.HasStats)
			t.Logf("    Statistics.HasNull: %v", dp0.ValidityPointer.Statistics.HasNull)
			t.Logf("    Statistics.StatData: %v", dp0.ValidityPointer.Statistics.StatData)
		} else {
			t.Logf("  ValidityPointer: nil")
		}
		
		// Read DataPointer for column 1 (VARCHAR)
		dp1, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[1])
		if err != nil {
			t.Fatalf("Error reading column 1 DataPointer: %v", err)
		}
		t.Logf("\nColumn 1 (VARCHAR) DataPointer:")
		t.Logf("  TupleCount: %d", dp1.TupleCount)
		t.Logf("  Block: ID=%d, Offset=%d", dp1.Block.BlockID, dp1.Block.Offset)
		t.Logf("  Compression: %s", dp1.Compression.String())
		t.Logf("  Statistics.HasStats: %v", dp1.Statistics.HasStats)
		t.Logf("  Statistics.HasNull: %v", dp1.Statistics.HasNull)
		t.Logf("  Statistics.StatData len: %d", len(dp1.Statistics.StatData))
		if dp1.ValidityPointer != nil {
			t.Logf("  ValidityPointer: EXISTS")
			t.Logf("    Block: ID=%d, Offset=%d", dp1.ValidityPointer.Block.BlockID, dp1.ValidityPointer.Block.Offset)
			t.Logf("    Compression: %s", dp1.ValidityPointer.Compression.String())
			t.Logf("    Statistics.HasStats: %v", dp1.ValidityPointer.Statistics.HasStats)
			t.Logf("    Statistics.HasNull: %v", dp1.ValidityPointer.Statistics.HasNull)
			t.Logf("    Statistics.StatData: %v", dp1.ValidityPointer.Statistics.StatData)
		} else {
			t.Logf("  ValidityPointer: nil")
		}
		
		// Now read the actual data
		rgReader := NewRowGroupReader(
			storage.blockManager,
			rowGroups[0],
			[]LogicalTypeID{TypeInteger, TypeVarchar},
		)

		colData0, err := rgReader.ReadColumn(0)
		if err != nil {
			t.Fatalf("Error reading column 0: %v", err)
		}
		t.Logf("\nColumn 0 data:")
		for i := uint64(0); i < 3; i++ {
			val, valid := colData0.GetValue(i)
			t.Logf("  Row %d: value=%v, valid=%v", i, val, valid)
		}

		colData1, err := rgReader.ReadColumn(1)
		if err != nil {
			t.Fatalf("Error reading column 1: %v", err)
		}
		t.Logf("\nColumn 1 data:")
		if colData1.Validity != nil {
			t.Logf("  Has validity mask: allValid=%v", colData1.Validity.AllValid())
		} else {
			t.Logf("  No validity mask")
		}
		for i := uint64(0); i < 3; i++ {
			val, valid := colData1.GetValue(i)
			t.Logf("  Row %d: value=%v, valid=%v", i, val, valid)
		}
	}
}
