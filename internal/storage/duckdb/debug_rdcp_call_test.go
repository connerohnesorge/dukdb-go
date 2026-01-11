package duckdb

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugReadColumnDataPointer(t *testing.T) {
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
		// Let me trace through ReadColumnDataPointer manually to see where parsing fails
		mbp := rowGroups[0].DataPointers[1] // VARCHAR column
		t.Logf("VARCHAR column MBP: BlockID=%d, BlockIndex=%d, Offset=%d", mbp.BlockID, mbp.BlockIndex, mbp.Offset)
		
		// Create reader like ReadColumnDataPointer does
		encodedPointer := mbp.Encode()
		reader, err := NewMetadataReaderWithOffset(storage.blockManager, encodedPointer, mbp.Offset)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		
		// Read Field 100
		err = reader.OnPropertyBegin(100)
		if err != nil {
			t.Fatalf("OnPropertyBegin(100) failed: %v", err)
		}
		
		count, err := reader.ReadVarint()
		if err != nil {
			t.Fatalf("ReadVarint count failed: %v", err)
		}
		t.Logf("data_pointers count: %d", count)
		
		// Read DataPointer
		dp, err := readDataPointer(reader)
		if err != nil {
			t.Fatalf("readDataPointer failed: %v", err)
		}
		t.Logf("DataPointer: TupleCount=%d, BlockID=%d, Compression=%s", 
			dp.TupleCount, dp.Block.BlockID, dp.Compression.String())
		t.Logf("Statistics: HasStats=%v, HasNull=%v, StatData len=%d",
			dp.Statistics.HasStats, dp.Statistics.HasNull, len(dp.Statistics.StatData))
		
		// Now check what field comes next
		nextField, err := reader.PeekField()
		if err != nil {
			t.Logf("PeekField after DataPointer error: %v", err)
		} else {
			t.Logf("Next field after DataPointer: %d (0x%04X)", nextField, nextField)
			if nextField == 101 {
				t.Log("-> Field 101 found! This is validity child!")
			} else if nextField == 0xFFFF {
				t.Log("-> FFFF terminator (no validity child)")
			}
		}
	}
}
