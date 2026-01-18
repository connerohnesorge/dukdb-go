package duckdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSubBlockComparison creates identical tables with CLI and dukdb-go,
// then compares sub-block 1 data byte-by-byte to find format differences.
func TestSubBlockComparison(t *testing.T) {
	// Create temp files for both databases
	cliFile := filepath.Join(t.TempDir(), "cli_empty.db")
	goFile := filepath.Join(t.TempDir(), "go_empty.db")

	// Create table with DuckDB CLI
	createTableWithCLI(t, cliFile, "CREATE TABLE test (id INTEGER, name VARCHAR)")

	// Create table with dukdb-go
	createTableWithGo(
		t,
		goFile,
		"test",
		[]string{"id", "name"},
		[]LogicalTypeID{TypeInteger, TypeVarchar},
	)

	// Read sub-block 1 from both files
	cliSB1 := readSubBlock1FromFile(t, cliFile)
	goSB1 := readSubBlock1FromFile(t, goFile)

	// Read the catalog MetaBlockPointer from both files to see what offset they use
	cliOffset := readCatalogTablePointerOffset(t, cliFile)
	goOffset := readCatalogTablePointerOffset(t, goFile)

	t.Logf("CLI catalog table_pointer offset: %d", cliOffset)
	t.Logf("Go catalog table_pointer offset: %d", goOffset)

	// Print the first 100 bytes at the offset for both
	t.Logf("\nCLI bytes at offset %d:", cliOffset)
	cliEnd := int(cliOffset) + 100
	if cliEnd > len(cliSB1) {
		cliEnd = len(cliSB1)
	}
	printHex(t, cliSB1[cliOffset:cliEnd])

	t.Logf("\nGo bytes at offset %d:", goOffset)
	goEnd := int(goOffset) + 100
	if goEnd > len(goSB1) {
		goEnd = len(goSB1)
	}
	printHex(t, goSB1[goOffset:goEnd])

	// Try reading field 100 at the CLI offset from both files
	t.Logf("\nTrying to read field 100 at CLI offset %d from CLI file:", cliOffset)
	fieldID, value := tryReadFieldAtOffset(cliSB1, cliOffset)
	t.Logf("  Field ID: %d (0x%04x), Value: %d (0x%x)", fieldID, fieldID, value, value)

	t.Logf("\nTrying to read field 100 at Go offset %d from Go file:", goOffset)
	fieldID, value = tryReadFieldAtOffset(goSB1, goOffset)
	t.Logf("  Field ID: %d (0x%04x), Value: %d (0x%x)", fieldID, fieldID, value, value)

	// Compare full sub-block 1 data
	t.Logf("\nComparing %d bytes of sub-block 1 data...", min(len(cliSB1), len(goSB1)))
	differences := compareBytes(cliSB1, goSB1)

	if len(differences) == 0 {
		t.Log("Sub-block 1 data is identical!")
	} else {
		t.Logf("Found %d byte differences:", len(differences))
		for i, diff := range differences {
			if i >= 20 {
				t.Logf("... and %d more differences", len(differences)-20)
				break
			}
			t.Logf("  Offset 0x%04x: CLI=0x%02x, Go=0x%02x", diff.Offset, diff.CLIByte, diff.GoByte)
		}
	}

	// Compare specific regions
	t.Logf("\nComparing specific regions:")

	// Full header including next_ptr (first 40 bytes starting from sub-block 1 byte 0)
	t.Log("\nFull header (SB1 bytes 0-39):")
	printRegionComparison(t, cliSB1, goSB1, 0, 40)

	// Header region (first 32 bytes from table_pointer.Offset)
	t.Log("\nHeader from table_pointer.Offset (0x00-0x1f):")
	printRegionComparison(t, cliSB1, goSB1, int(cliOffset), 32)

	// Field 100 region (around offset 8)
	t.Log("\nField 100 region (0x08-0x20):")
	printRegionComparison(t, cliSB1, goSB1, 8, 32)

	// Field statistics region
	t.Log("\nStatistics region (0x0c57-0x0c80):")
	printRegionComparison(t, cliSB1, goSB1, 0xc57, 0xc80-0xc57)
}

// ByteDiff represents a difference between two byte arrays
type ByteDiff struct {
	Offset  int
	CLIByte byte
	GoByte  byte
}

// compareBytes compares two byte arrays and returns all differences
func compareBytes(cli, go_ []byte) []ByteDiff {
	var diffs []ByteDiff
	maxLen := len(cli)
	if len(go_) < maxLen {
		maxLen = len(go_)
	}

	for i := 0; i < maxLen; i++ {
		if cli[i] != go_[i] {
			diffs = append(diffs, ByteDiff{
				Offset:  i,
				CLIByte: cli[i],
				GoByte:  go_[i],
			})
		}
	}

	// Handle length differences
	if len(cli) != len(go_) {
		for i := maxLen; i < len(cli); i++ {
			diffs = append(diffs, ByteDiff{
				Offset:  i,
				CLIByte: cli[i],
				GoByte:  0, // padding
			})
		}
		for i := maxLen; i < len(go_); i++ {
			diffs = append(diffs, ByteDiff{
				Offset:  i,
				CLIByte: 0, // padding
				GoByte:  go_[i],
			})
		}
	}

	return diffs
}

// printRegionComparison prints a side-by-side comparison of a region
func printRegionComparison(t *testing.T, cli, go_ []byte, offset, length int) {
	if offset+length > len(cli) || offset+length > len(go_) {
		maxLen := min(len(cli), len(go_))
		if offset >= maxLen {
			t.Logf("  Region out of bounds (file size: %d)", maxLen)
			return
		}
		length = maxLen - offset
	}

	cliRegion := cli[offset : offset+length]
	goRegion := go_[offset : offset+length]

	t.Logf("  CLI: %s", formatHexLine(cliRegion))
	t.Logf("  Go:  %s", formatHexLine(goRegion))

	if bytes.Equal(cliRegion, goRegion) {
		t.Log("  (identical)")
	} else {
		t.Log("  (DIFFERENT)")
	}
}

// formatHexLine formats bytes as hex string
func formatHexLine(data []byte) string {
	var buf bytes.Buffer
	for i, b := range data {
		if i > 0 && i%16 == 0 {
			buf.WriteString("\n       ")
		} else if i > 0 && i%8 == 0 {
			buf.WriteString(" ")
		}
		if i > 0 {
			buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%02x", b)
	}
	return buf.String()
}

// printHex prints bytes in hex format
func printHex(t *testing.T, data []byte) {
	for i := 0; i < len(data); i += 16 {
		end := min(i+16, len(data))
		line := data[i:end]

		// Offset
		fmt.Printf("  %04x: ", i)

		// Hex bytes
		for j, b := range line {
			if j == 8 {
				fmt.Print(" ")
			}
			fmt.Printf("%02x ", b)
		}

		// Padding
		for j := len(line); j < 16; j++ {
			if j == 8 {
				fmt.Print(" ")
			}
			fmt.Print("   ")
		}

		// ASCII
		fmt.Print(" |")
		for _, b := range line {
			if b >= 32 && b < 127 {
				fmt.Printf("%c", b)
			} else {
				fmt.Print(".")
			}
		}
		fmt.Print("|")
		fmt.Println()
	}
}

// tryReadFieldAtOffset attempts to read a BinarySerializer field at the given offset
func tryReadFieldAtOffset(data []byte, offset uint64) (fieldID uint16, value uint64) {
	if int(offset)+2 > len(data) {
		return 0, 0
	}

	// Read field ID (2 bytes little-endian)
	fieldID = binary.LittleEndian.Uint16(data[offset:])

	// Try to read value as varint
	if int(offset)+2 < len(data) {
		value, _ = readVarintFromBytes(data[offset+2:])
	}

	return fieldID, value
}

// readVarintFromBytes reads a varint from bytes
func readVarintFromBytes(data []byte) (uint64, int) {
	var result uint64
	var shift uint
	for i, b := range data {
		result |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return result, i + 1
		}
		shift += 7
		if i >= 9 {
			break
		}
	}
	return result, 0
}

// readSubBlock1FromFile reads sub-block 1 (4096 bytes starting at file offset 16392)
func readSubBlock1FromFile(t *testing.T, path string) []byte {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	// Sub-block 1 starts at file offset:
	// - DataBlocksOffset (12288) - file header and database headers
	// - 8 bytes checksum (block 0)
	// - 4096 bytes sub-block 0
	// = 12288 + 8 + 4096 = 16392 bytes
	const DataBlocksOffset = 12288
	sb1Offset := int64(DataBlocksOffset + 8 + 4096)
	sb1Size := 4096

	sb1Data := make([]byte, sb1Size)
	n, err := f.ReadAt(sb1Data, sb1Offset)
	require.NoError(t, err)
	require.Equal(t, sb1Size, n)

	return sb1Data
}

// readCatalogTablePointerOffset reads the table_pointer offset from the catalog
func readCatalogTablePointerOffset(t *testing.T, path string) uint64 {
	// Open file and read metadata
	config := DefaultConfig()
	config.ReadOnly = true
	storage, err := OpenDuckDBStorage(path, config)
	require.NoError(t, err)
	defer storage.Close()

	// Get the first table's storage metadata
	t.Logf("  File %s has %d tables", filepath.Base(path), len(storage.catalog.Tables))
	if len(storage.catalog.Tables) == 0 {
		t.Log("  WARNING: No tables found in catalog")
		return 0
	}

	// Iterate through all tables
	for i, table := range storage.catalog.Tables {
		t.Logf("  Table %d: %s.%s", i, table.GetSchema(), table.Name)
		if table.StorageMetadata != nil {
			t.Logf("    Storage: BlockID=%d, BlockIndex=%d, Offset=%d",
				table.StorageMetadata.TablePointer.BlockID,
				table.StorageMetadata.TablePointer.BlockIndex,
				table.StorageMetadata.TablePointer.Offset)
		} else {
			t.Log("    Storage: NONE")
		}
	}

	table := storage.catalog.Tables[0]
	if table.StorageMetadata == nil {
		t.Log("  WARNING: First table has no storage metadata")
		return 0
	}

	return table.StorageMetadata.TablePointer.Offset
}

// createTableWithCLI creates a table using DuckDB CLI
func createTableWithCLI(t *testing.T, dbPath, createSQL string) {
	// Remove existing file
	_ = os.Remove(dbPath)

	// Run DuckDB CLI
	cmd := exec.Command("duckdb", dbPath, createSQL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB CLI failed: %v\nOutput: %s", err, output)
	}
}

// createTableWithGo creates a table using dukdb-go
func createTableWithGo(
	t *testing.T,
	dbPath, tableName string,
	colNames []string,
	colTypes []LogicalTypeID,
) {
	// Remove existing file
	_ = os.Remove(dbPath)

	// Create storage
	config := DefaultConfig()
	storage, err := CreateDuckDBStorage(dbPath, config)
	require.NoError(t, err)
	defer storage.Close()

	// Create table in catalog
	columns := make([]ColumnDefinition, len(colNames))
	for i := range colNames {
		columns[i] = ColumnDefinition{
			Name:     colNames[i],
			Type:     colTypes[i],
			Nullable: true,
		}
	}

	table := &TableCatalogEntry{
		CreateInfo: CreateInfo{
			Catalog:    "cli",
			Schema:     "main",
			OnConflict: OnCreateConflictError,
		},
		Name:    tableName,
		Columns: columns,
	}

	storage.catalog.AddTable(table)
	storage.modified = true

	// Checkpoint to write catalog
	err = storage.Checkpoint()
	require.NoError(t, err)
}
