package duckdb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSerializeDebug(t *testing.T) {
	// Create a minimal catalog with a schema and table
	var buf bytes.Buffer
	serializer := NewBinarySerializer(&buf)

	// Write property 100 with list of 2 entries
	serializer.OnPropertyBegin(100, "catalog_entries")
	serializer.OnListBegin(2)

	// Entry 1: Schema
	t.Log("=== Writing Schema Entry ===")
	serializer.OnObjectBegin()

	// Write catalog_type = SCHEMA_ENTRY (2)
	serializer.WriteProperty(99, "catalog_type", uint8(2))

	// Write entry data (property 100, nullable pointer)
	serializer.OnPropertyBegin(100, "entry")
	serializer.WriteBool(true) // nullable: present

	// Now serialize the CreateSchemaInfo content
	serializer.WriteProperty(100, "type", uint8(2)) // CatalogSchemaEntry
	serializer.WriteProperty(101, "catalog", "")
	serializer.WriteProperty(102, "schema", "main")
	serializer.WriteProperty(105, "on_conflict", uint8(0))
	serializer.OnObjectEnd() // End CreateSchemaInfo
	serializer.OnObjectEnd() // End entry object

	// Entry 2: Table
	t.Log("=== Writing Table Entry ===")
	serializer.OnObjectBegin()

	// Write catalog_type = TABLE_ENTRY (1)
	serializer.WriteProperty(99, "catalog_type", uint8(1))

	// Write entry data (property 100, nullable pointer)
	serializer.OnPropertyBegin(100, "entry")
	serializer.WriteBool(true) // nullable: present

	// Now serialize the CreateTableInfo content
	serializer.WriteProperty(100, "type", uint8(1)) // CatalogTableEntry
	serializer.WriteProperty(101, "catalog", "")
	serializer.WriteProperty(102, "schema", "main")
	serializer.WriteProperty(105, "on_conflict", uint8(0))
	serializer.WriteProperty(200, "table", "test")

	// Property 201: columns (ColumnList object)
	serializer.OnPropertyBegin(201, "columns")
	serializer.OnObjectBegin() // Begin ColumnList object
	// Field 100 within ColumnList: physical_columns vector
	serializer.OnPropertyBegin(100, "physical_columns")
	serializer.OnListBegin(1)

	// ColumnDefinition
	serializer.WriteProperty(100, "name", "id")
	serializer.OnPropertyBegin(101, "type")
	serializer.WriteProperty(100, "type_id", uint8(4)) // INTEGER
	serializer.OnObjectEnd()                           // LogicalType
	serializer.WriteProperty(103, "category", uint8(0))
	serializer.WriteProperty(104, "compression_type", uint8(0))
	serializer.OnObjectEnd() // ColumnDefinition

	serializer.OnObjectEnd() // End ColumnList object

	// Property 202: constraints (empty list)
	serializer.OnPropertyBegin(202, "constraints")
	serializer.OnListBegin(0)

	serializer.OnObjectEnd() // End CreateTableInfo
	serializer.OnObjectEnd() // End entry object

	// Root terminator
	serializer.OnObjectEnd()

	t.Logf("Total bytes: %d", buf.Len())
	t.Log("Hex dump:")
	fmt.Println(hex.Dump(buf.Bytes()))

	// Now write to actual file and test with DuckDB
	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	storage, err := CreateDuckDBStorage(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	storage.catalog.AddSchema(NewSchemaCatalogEntry("main"))

	tableEntry := NewTableCatalogEntry("test")
	tableEntry.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{
		Name: "id",
		Type: TypeInteger,
	})
	storage.catalog.AddTable(tableEntry)
	storage.modified = true

	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	t.Logf("Created database at %s", dbPath)

	// Read the metadata block to see what was actually written
	file, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer file.Close()

	fileHeader, err := ReadFileHeader(file)
	if err != nil {
		t.Fatalf("Failed to read file header: %v", err)
	}
	t.Logf("File version: %d", fileHeader.Version)

	dbHeader, which, err := GetActiveHeader(file)
	if err != nil {
		t.Fatalf("Failed to get active header: %v", err)
	}
	t.Logf("Active header: slot %d, MetaBlock=0x%x", which, dbHeader.MetaBlock)

	if !IsValidBlockID(dbHeader.MetaBlock) {
		t.Fatal("MetaBlock is invalid - catalog not written")
	}

	// Decode MetaBlockPointer
	ptr := DecodeMetaBlockPointer(dbHeader.MetaBlock)
	t.Logf("MetaBlockPointer: BlockID=%d, BlockIndex=%d", ptr.BlockID, ptr.BlockIndex)

	// Read the metadata block manually
	blockManager := NewBlockManager(file, dbHeader.BlockAllocSize, 100)
	blockManager.SetBlockCount(dbHeader.BlockCount)

	block, err := blockManager.ReadBlock(ptr.BlockID)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

	t.Log("Actual metadata block content:")
	if len(block.Data) > 300 {
		fmt.Println(hex.Dump(block.Data[:300]))
	} else {
		fmt.Println(hex.Dump(block.Data))
	}

	// Try to open with DuckDB CLI
	cmd := exec.Command("duckdb", dbPath, "-c", "SHOW TABLES;")
	output, err := cmd.CombinedOutput()
	t.Logf("DuckDB CLI output:\n%s", string(output))
	if err != nil {
		t.Logf("DuckDB CLI error: %v", err)
	}
}
