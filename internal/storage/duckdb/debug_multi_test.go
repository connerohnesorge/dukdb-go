package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugMultiTable(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "multi.duckdb")

	// Test with two tables - create users first, then orders (same as interop test)
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DOUBLE);
		INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO orders VALUES (100, 1, 99.99), (101, 2, 149.50);
	`)
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	// Read raw block data
	data, _ := os.ReadFile(dbPath)
	blockStart := 4096 + 4096 + 4096 + 8 // Skip headers + checksum
	block := data[blockStart:]

	// Show hex dump starting from offset 8 (where catalog data starts)
	t.Logf("Catalog data (from offset 8):\n%s", hex.Dump(block[8:350]))

	// Read headers manually
	file, _ := os.Open(dbPath)
	defer file.Close()

	dbHeader, _, _ := GetActiveHeader(file)
	bm := NewBlockManager(file, dbHeader.BlockAllocSize, 100)
	bm.SetBlockCount(dbHeader.BlockCount)

	// Try reading entries manually to see the error
	reader, err := NewMetadataReader(bm, dbHeader.MetaBlock)
	if err != nil {
		t.Fatalf("NewMetadataReader: %v", err)
	}

	fieldID, _ := reader.ReadFieldID()
	count, _ := reader.ReadVarint()
	t.Logf("Field %d, count %d", fieldID, count)

	for i := uint64(0); i < count; i++ {
		startOffset := reader.offset
		t.Logf("Entry %d starting at offset %d, next bytes: %x", i, startOffset, reader.data[startOffset:min(startOffset+20, len(reader.data))])
		entry, err := reader.ReadCatalogEntry()
		endOffset := reader.offset
		t.Logf("Entry %d ended at offset %d", i, endOffset)
		if err != nil {
			t.Logf("Entry %d failed: %v", i, err)
			if reader.remaining() > 20 {
				t.Logf("  Next bytes: %x", reader.data[reader.offset:reader.offset+20])
			}
			break
		}
		switch e := entry.(type) {
		case *SchemaCatalogEntry:
			t.Logf("Entry %d: Schema %s", i, e.Name)
		case *TableCatalogEntry:
			t.Logf("Entry %d: Table %s (cols=%d, constraints=%d)", i, e.Name, len(e.Columns), len(e.Constraints))
		case *IndexCatalogEntry:
			t.Logf("Entry %d: Index %s on %s", i, e.Name, e.TableName)
		default:
			t.Logf("Entry %d: %T", i, entry)
		}
	}

	catalog, err := ReadCatalogFromMetadata(bm, dbHeader.MetaBlock)
	if err != nil {
		t.Logf("ReadCatalogFromMetadata error: %v", err)
	} else {
		t.Logf("Catalog: Tables=%d, Schemas=%d", len(catalog.Tables), len(catalog.Schemas))
		for i, tbl := range catalog.Tables {
			t.Logf("  Table %d: %s (cols=%d, constraints=%d)", i, tbl.Name, len(tbl.Columns), len(tbl.Constraints))
		}
	}

	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	if err != nil {
		t.Logf("Open error: %v", err)
	} else {
		defer storage.Close()
		t.Logf("DuckDB catalog tables: %d", len(storage.catalog.Tables))
	}
}
