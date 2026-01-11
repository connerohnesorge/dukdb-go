package duckdb

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugStatsFix(t *testing.T) {
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
		mbp := rowGroups[0].DataPointers[1] // VARCHAR column
		t.Logf("VARCHAR column MBP: BlockID=%d, BlockIndex=%d, Offset=%d", mbp.BlockID, mbp.BlockIndex, mbp.Offset)
		
		// Create reader like the code does
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
		
		// Field 101: tuple_count
		err = reader.OnPropertyBegin(101)
		if err != nil {
			t.Fatalf("Field 101 failed: %v", err)
		}
		tc, _ := reader.ReadVarint()
		t.Logf("tuple_count: %d", tc)
		
		// Field 102: block_pointer
		err = reader.OnPropertyBegin(102)
		if err != nil {
			t.Fatalf("Field 102 failed: %v", err)
		}
		err = reader.OnPropertyBegin(100)
		if err != nil {
			t.Fatalf("BlockPointer.100 failed: %v", err)
		}
		bid, _ := reader.ReadVarint()
		t.Logf("block_id: %d", bid)
		
		// BlockPointer terminator
		f, _ := reader.PeekField()
		if f == ddbFieldTerminator {
			reader.ConsumeField()
		}
		
		// Field 103: compression
		err = reader.OnPropertyBegin(103)
		if err != nil {
			t.Fatalf("Field 103 failed: %v", err)
		}
		comp, _ := reader.ReadVarint()
		t.Logf("compression: %d", comp)
		
		// Field 104: statistics
		err = reader.OnPropertyBegin(104)
		if err != nil {
			t.Fatalf("Field 104 failed: %v", err)
		}
		t.Log("Entered statistics")
		
		// Field 100: has_stats
		f, _ = reader.PeekField()
		t.Logf("First stats field: %d (0x%04X)", f, f)
		reader.ConsumeField()
		hs, _ := reader.ReadVarint()
		t.Logf("has_stats: %d", hs)
		
		// Field 101: has_null
		f, _ = reader.PeekField()
		t.Logf("Second stats field: %d", f)
		reader.ConsumeField()
		hn, _ := reader.ReadVarint()
		t.Logf("has_null: %d", hn)
		
		// Field 102: stats_type
		f, _ = reader.PeekField()
		t.Logf("Third stats field: %d", f)
		reader.ConsumeField()
		st, _ := reader.ReadVarint()
		t.Logf("stats_type: %d", st)
		
		// Field 103: type_specific_stats
		f, _ = reader.PeekField()
		t.Logf("Fourth stats field: %d (should be 103)", f)
		reader.ConsumeField()
		
		// Inside type_specific_stats, peek Field 200
		f, _ = reader.PeekField()
		t.Logf("Inside type_specific_stats, first field: %d (should be 200)", f)
		reader.ConsumeField()
		
		// Now peek what readNumericValueUnion would see
		firstInner, _ := reader.PeekField()
		t.Logf("Inside Field 200 (min_value), peek: %d (0x%04X)", firstInner, firstInner)
		t.Logf("Is this 100, 101, or FFFF? %v", firstInner == 100 || firstInner == 101 || firstInner == 0xFFFF)
		
		// If not 100/101/FFFF, my fix should read as string length
		if firstInner != 100 && firstInner != 101 && firstInner != ddbFieldTerminator {
			t.Log("Not a numeric format - should read as string")
			length, err := reader.ReadVarint()
			if err != nil {
				t.Logf("Error reading length: %v", err)
			} else {
				t.Logf("String length: %d", length)
				for i := uint64(0); i < length && i < 20; i++ {
					b, err := reader.ReadByte()
					if err != nil {
						t.Logf("Error reading byte %d: %v", i, err)
						break
					}
					t.Logf("  byte[%d]: 0x%02X", i, b)
				}
			}
			
			// After skipping min string, peek next
			nextF, _ := reader.PeekField()
			t.Logf("After min_value, peek: %d (0x%04X)", nextF, nextF)
		}
	}
}
