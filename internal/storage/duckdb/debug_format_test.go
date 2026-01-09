package duckdb

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugFormatComparison(t *testing.T) {
	tmpDir := t.TempDir()

	// Create our file with a simple table (matching interop test)
	ourPath := filepath.Join(tmpDir, "ours.duckdb")
	storage, err := CreateDuckDBStorage(ourPath, nil)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create a table with INTEGER and VARCHAR columns (like interop test)
	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{
		Name: "id",
		Type: TypeInteger,
	})
	tableEntry.AddColumn(ColumnDefinition{
		Name: "name",
		Type: TypeVarchar,
	})

	storage.catalog.AddTable(tableEntry)
	storage.modified = true

	// Debug: check catalog state
	t.Logf("Catalog tables before close: %d", len(storage.catalog.Tables))
	t.Logf("Catalog is empty: %v", storage.catalog.IsEmpty())

	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Read the file and dump the metadata block
	file, _ := os.Open(ourPath)
	defer file.Close()

	// Read both database headers to see which is active
	// Database header structure:
	// - Bytes 0-7: checksum
	// - Bytes 8-15: iteration
	// - Bytes 16-23: meta_block
	// - Bytes 24-31: free_list
	header1 := make([]byte, 64)
	file.ReadAt(header1, 0x1000)

	header2 := make([]byte, 64)
	file.ReadAt(header2, 0x2000)

	// Parse both headers
	iter1 := uint64(header1[8]) | uint64(header1[9])<<8 | uint64(header1[10])<<16 | uint64(header1[11])<<24 |
		uint64(header1[12])<<32 | uint64(header1[13])<<40 | uint64(header1[14])<<48 | uint64(header1[15])<<56
	meta1 := uint64(header1[16]) | uint64(header1[17])<<8 | uint64(header1[18])<<16 | uint64(header1[19])<<24 |
		uint64(header1[20])<<32 | uint64(header1[21])<<40 | uint64(header1[22])<<48 | uint64(header1[23])<<56

	iter2 := uint64(header2[8]) | uint64(header2[9])<<8 | uint64(header2[10])<<16 | uint64(header2[11])<<24 |
		uint64(header2[12])<<32 | uint64(header2[13])<<40 | uint64(header2[14])<<48 | uint64(header2[15])<<56
	meta2 := uint64(header2[16]) | uint64(header2[17])<<8 | uint64(header2[18])<<16 | uint64(header2[19])<<24 |
		uint64(header2[20])<<32 | uint64(header2[21])<<40 | uint64(header2[22])<<48 | uint64(header2[23])<<56

	t.Logf("Header 1: iteration=%d, meta_block=0x%x", iter1, meta1)
	t.Logf("Header 2: iteration=%d, meta_block=0x%x", iter2, meta2)

	// Use header with higher iteration
	var metaBlock uint64
	if iter2 > iter1 {
		metaBlock = meta2
		t.Log("Using header 2")
	} else {
		metaBlock = meta1
		t.Log("Using header 1")
	}
	blockID := metaBlock & 0x00FFFFFFFFFFFFFF

	t.Logf("MetaBlock pointer: 0x%x, blockID: %d", metaBlock, blockID)

	// Read metadata block
	blockOffset := int64(0x3000) + int64(blockID)*262144
	data := make([]byte, 500)
	file.ReadAt(data, blockOffset)

	t.Log("=== OUR METADATA BLOCK ===")
	fmt.Println(hex.Dump(data[:300]))

	// Create native DuckDB file with same structure (INTEGER and VARCHAR)
	nativePath := filepath.Join(tmpDir, "native.duckdb")
	exec.Command("duckdb", nativePath, "-c", "CREATE TABLE test(id INTEGER, name VARCHAR); CHECKPOINT;").Run()

	// Read native file
	nativeFile, _ := os.Open(nativePath)
	defer nativeFile.Close()

	nativeHeader := make([]byte, 64)
	nativeFile.ReadAt(nativeHeader, 0x1000)

	// MetaBlock is at offset 16 (after checksum + iteration)
	nativeMetaBlock := uint64(nativeHeader[16]) | uint64(nativeHeader[17])<<8 | uint64(nativeHeader[18])<<16 | uint64(nativeHeader[19])<<24 |
		uint64(nativeHeader[20])<<32 | uint64(nativeHeader[21])<<40 | uint64(nativeHeader[22])<<48 | uint64(nativeHeader[23])<<56
	nativeBlockID := nativeMetaBlock & 0x00FFFFFFFFFFFFFF

	t.Logf("Native MetaBlock pointer: 0x%x, blockID: %d", nativeMetaBlock, nativeBlockID)

	nativeBlockOffset := int64(0x3000) + int64(nativeBlockID)*262144
	nativeData := make([]byte, 500)
	nativeFile.ReadAt(nativeData, nativeBlockOffset)

	t.Log("=== NATIVE METADATA BLOCK ===")
	fmt.Println(hex.Dump(nativeData[:300]))

	// Compare byte by byte starting at position 8 (after checksum)
	t.Log("=== BYTE-BY-BYTE COMPARISON (starting at byte 8) ===")
	diffCount := 0
	for i := 8; i < 200; i++ {
		if data[i] != nativeData[i] {
			t.Logf("DIFF at byte %d: ours=0x%02x native=0x%02x", i, data[i], nativeData[i])
			diffCount++
			if diffCount > 30 {
				t.Log("... (truncated, too many differences)")
				break
			}
		}
	}

	// Also compare file headers
	t.Log("=== FILE HEADERS (offset 0x0) ===")
	ourFileHeader := make([]byte, 64)
	nativeFileHeader := make([]byte, 64)
	file.ReadAt(ourFileHeader, 0)
	nativeFile.ReadAt(nativeFileHeader, 0)
	t.Log("Ours:")
	fmt.Println(hex.Dump(ourFileHeader))
	t.Log("Native:")
	fmt.Println(hex.Dump(nativeFileHeader))

	// Compare file headers
	t.Log("=== FILE HEADER DIFFS ===")
	for i := 0; i < 64; i++ {
		if ourFileHeader[i] != nativeFileHeader[i] {
			t.Logf("DIFF at byte %d: ours=0x%02x native=0x%02x", i, ourFileHeader[i], nativeFileHeader[i])
		}
	}

	// Compare DB headers more carefully
	t.Log("=== DB HEADER 1 (offset 0x1000) ===")
	ourDBHeader1 := make([]byte, 80)
	nativeDBHeader1 := make([]byte, 80)
	file.ReadAt(ourDBHeader1, 0x1000)
	nativeFile.ReadAt(nativeDBHeader1, 0x1000)
	t.Log("Ours:")
	fmt.Println(hex.Dump(ourDBHeader1))
	t.Log("Native:")
	fmt.Println(hex.Dump(nativeDBHeader1))

	t.Log("=== DB HEADER 2 (offset 0x2000) ===")
	ourDBHeader2 := make([]byte, 80)
	nativeDBHeader2 := make([]byte, 80)
	file.ReadAt(ourDBHeader2, 0x2000)
	nativeFile.ReadAt(nativeDBHeader2, 0x2000)
	t.Log("Ours:")
	fmt.Println(hex.Dump(ourDBHeader2))
	t.Log("Native:")
	fmt.Println(hex.Dump(nativeDBHeader2))

	// Dump free list blocks (pointed by FreeList in DB headers)
	// Both ours and native should have FreeList at block 0, sub-block 3
	t.Log("=== FREE LIST BLOCKS ===")

	// Parse our free list pointer from DB header 2
	ourFreeListPtr := uint64(ourDBHeader2[24]) | uint64(ourDBHeader2[25])<<8 |
		uint64(ourDBHeader2[26])<<16 | uint64(ourDBHeader2[27])<<24 |
		uint64(ourDBHeader2[28])<<32 | uint64(ourDBHeader2[29])<<40 |
		uint64(ourDBHeader2[30])<<48 | uint64(ourDBHeader2[31])<<56
	ourFreeListBlock := ourFreeListPtr & 0x00FFFFFFFFFFFFFF
	ourFreeListIndex := uint8(ourFreeListPtr >> 56)
	t.Logf("Our FreeList pointer: 0x%x, blockID=%d, index=%d", ourFreeListPtr, ourFreeListBlock, ourFreeListIndex)

	// Our free list should be at block 0, sub-block 3 (like native)
	ourFreeListOffset := int64(0x3000) + int64(ourFreeListBlock)*262144 + int64(ourFreeListIndex)*4096
	ourFreeListData := make([]byte, 200)
	file.ReadAt(ourFreeListData, ourFreeListOffset)
	t.Logf("Ours (block %d, index %d at offset 0x%x):", ourFreeListBlock, ourFreeListIndex, ourFreeListOffset)
	fmt.Println(hex.Dump(ourFreeListData[:128]))

	// Native free list: block 0, but with index 3 (bits 56-63)
	// Actually the native header 1 says FreeList = 0x0300000000000000
	// That's: block_id = 0, block_index = 3
	// So it's in block 0 at some sub-block offset
	nativeFreeListBlockID := uint64(nativeDBHeader1[24]) | uint64(nativeDBHeader1[25])<<8 |
		uint64(nativeDBHeader1[26])<<16 | uint64(nativeDBHeader1[27])<<24 |
		uint64(nativeDBHeader1[28])<<32 | uint64(nativeDBHeader1[29])<<40 |
		uint64(nativeDBHeader1[30])<<48 | uint64(nativeDBHeader1[31])<<56
	nativeFreeListBlock := nativeFreeListBlockID & 0x00FFFFFFFFFFFFFF
	nativeFreeListIndex := uint8(nativeFreeListBlockID >> 56)
	t.Logf("Native FreeList pointer: 0x%x, blockID=%d, index=%d", nativeFreeListBlockID, nativeFreeListBlock, nativeFreeListIndex)

	// Native free list is in the same block as metadata (block 0), but at a different sub-block offset
	// Sub-block size is typically BlockSize / 64 = 262144 / 64 = 4096 bytes
	// So free list starts at offset 0x3000 + block * 262144 + index * 4096
	nativeFreeListOffset := int64(0x3000) + int64(nativeFreeListBlock)*262144 + int64(nativeFreeListIndex)*4096
	nativeFreeListData := make([]byte, 200)
	nativeFile.ReadAt(nativeFreeListData, nativeFreeListOffset)
	t.Logf("Native (block %d, index %d at offset 0x%x):", nativeFreeListBlock, nativeFreeListIndex, nativeFreeListOffset)
	fmt.Println(hex.Dump(nativeFreeListData[:128]))

	// Debug: dump sub-blocks 1 and 2
	t.Log("=== SUB-BLOCK 1 AND 2 DEBUG ===")
	sb1Data := make([]byte, 128)
	sb2Data := make([]byte, 128)
	file.ReadAt(sb1Data, 0x4000) // Sub-block 1 at offset 0x4000
	file.ReadAt(sb2Data, 0x5000) // Sub-block 2 at offset 0x5000

	sb1End := 0
	for i := 127; i >= 0; i-- {
		if sb1Data[i] != 0 {
			sb1End = i + 1
			break
		}
	}
	sb2End := 0
	for i := 127; i >= 0; i-- {
		if sb2Data[i] != 0 {
			sb2End = i + 1
			break
		}
	}
	t.Logf("Our sub-block 1: %d non-zero bytes in first 128", sb1End)
	if sb1End > 0 {
		fmt.Printf("Sub-block 1:\n%s", hex.Dump(sb1Data[:sb1End]))
	}
	t.Logf("Our sub-block 2: %d non-zero bytes in first 128", sb2End)
	if sb2End > 0 {
		fmt.Printf("Sub-block 2:\n%s", hex.Dump(sb2Data[:sb2End]))
	}

	// Also show native sub-blocks for comparison
	nativeSb1Data := make([]byte, 128)
	nativeSb2Data := make([]byte, 128)
	nativeFile.ReadAt(nativeSb1Data, 0x4000)
	nativeFile.ReadAt(nativeSb2Data, 0x5000)
	t.Log("Native sub-block 1 (first 80 bytes):")
	fmt.Printf("%s", hex.Dump(nativeSb1Data[:80]))

	// Try to open our file with DuckDB CLI
	t.Log("=== DUCKDB CLI TEST ===")
	cmd := exec.Command("duckdb", ourPath, "-c", "SELECT 1;")
	output, _ := cmd.CombinedOutput()
	t.Logf("Output: %s", output)
}
