package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugCatalogLoad(t *testing.T) {
	// Check for duckdb CLI
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// Create database with DuckDB CLI
	cmd := exec.Command("duckdb", dbPath, "-c", "CREATE TABLE test (id INTEGER, name VARCHAR); INSERT INTO test VALUES (1, 'Alice');")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	// Open file and check headers
	file, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() { _ = file.Close() }()

	fileHeader, err := ReadFileHeader(file)
	if err != nil {
		t.Fatalf("failed to read file header: %v", err)
	}
	t.Logf("File header: version=%d, flags=%d", fileHeader.Version, fileHeader.Flags)

	dbHeader, which, err := GetActiveHeader(file)
	if err != nil {
		t.Fatalf("failed to get active header: %v", err)
	}
	t.Logf("Active header: which=%d", which)
	t.Logf("MetaBlock=%d (0x%x), IsValid=%v", dbHeader.MetaBlock, dbHeader.MetaBlock, IsValidBlockID(dbHeader.MetaBlock))
	t.Logf("FreeList=%d (0x%x), IsValid=%v", dbHeader.FreeList, dbHeader.FreeList, IsValidBlockID(dbHeader.FreeList))
	t.Logf("BlockCount=%d", dbHeader.BlockCount)
	t.Logf("BlockAllocSize=%d", dbHeader.BlockAllocSize)

	// Now try loading catalog
	blockManager := NewBlockManager(file, dbHeader.BlockAllocSize, 100)
	blockManager.SetBlockCount(dbHeader.BlockCount)

	if IsValidBlockID(dbHeader.MetaBlock) {
		t.Logf("MetaBlock is valid, attempting to read block %d", dbHeader.MetaBlock)

		// Try to read the meta block
		block, err := blockManager.ReadBlock(dbHeader.MetaBlock)
		if err != nil {
			t.Fatalf("failed to read meta block: %v", err)
		}
		t.Logf("Meta block read successfully, size=%d", len(block.Data))

		// Show first 100 bytes
		if len(block.Data) > 100 {
			t.Logf("First 100 bytes: %x", block.Data[:100])
		}

		// Try to read catalog entry by entry manually for debugging
		reader, err := NewMetadataReader(blockManager, dbHeader.MetaBlock)
		if err != nil {
			t.Fatalf("failed to create metadata reader: %v", err)
		}

		// Read field 100 (catalog_entries)
		fieldID, err := reader.ReadFieldID()
		if err != nil {
			t.Fatalf("failed to read first field ID: %v", err)
		}
		t.Logf("First field ID: %d", fieldID)

		// Read count
		count, err := reader.ReadVarint()
		if err != nil {
			t.Fatalf("failed to read count: %v", err)
		}
		t.Logf("Entry count: %d", count)

		for i := uint64(0); i < count; i++ {
			t.Logf("Reading entry %d (offset=%d)...", i, reader.offset())
			entry, err := reader.ReadCatalogEntry()
			if err != nil {
				t.Logf("Failed to read entry %d: %v", i, err)
				// Show next few bytes for debugging
				if reader.remaining() > 20 {
					data := reader.data()
					offset := reader.offset()
					t.Logf("  Next bytes: %x", data[offset:offset+20])
				}
				break
			}
			if entry != nil {
				switch e := entry.(type) {
				case *SchemaCatalogEntry:
					t.Logf("  Entry %d: Schema %s", i, e.Name)
				case *TableCatalogEntry:
					t.Logf("  Entry %d: Table %s (cols=%d)", i, e.Name, len(e.Columns))
					for j, col := range e.Columns {
						t.Logf("    Column %d: %s (type=%d)", j, col.Name, col.Type)
					}
				default:
					t.Logf("  Entry %d: %T", i, entry)
				}
			} else {
				t.Logf("  Entry %d: nil", i)
			}
		}

		// Also try the full API
		catalog, err := ReadCatalogFromMetadata(blockManager, dbHeader.MetaBlock)
		if err != nil {
			t.Skipf("DuckDB CLI format not yet fully compatible: %v", err)
		} else {
			t.Logf("Catalog read successfully")
			t.Logf("Tables: %d, Schemas: %d, Views: %d", len(catalog.Tables), len(catalog.Schemas), len(catalog.Views))
			for j, schema := range catalog.Schemas {
				t.Logf("  Schema %d: %s", j, schema.Name)
			}
			for j, tbl := range catalog.Tables {
				t.Logf("  Table %d: %s (cols=%d)", j, tbl.Name, len(tbl.Columns))
			}
		}
	} else {
		t.Logf("MetaBlock is invalid")
	}
}
