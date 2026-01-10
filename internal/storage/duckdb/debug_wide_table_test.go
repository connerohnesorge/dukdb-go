package duckdb

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugWideTableCompare compares wide table (20 columns) database bytes with CLI
func TestDebugWideTableCompare(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI wide table database
	cliPath := filepath.Join(tmpDir, "cli.duckdb")

	// Build column definitions
	var cols []string
	for i := 0; i < 20; i++ {
		colName := "col_" + string(rune('a'+(i/26))) + string(rune('a'+(i%26)))
		cols = append(cols, colName+" INTEGER")
	}

	cmd := exec.Command("duckdb", cliPath, "CREATE TABLE wide_table ("+strings.Join(cols, ", ")+");")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI failed: %s", string(output))

	// Create Go wide table database
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	require.NoError(t, err)

	tableEntry := NewTableCatalogEntry("wide_table")
	tableEntry.CreateInfo.Schema = "main"

	// Add 20 columns
	for i := 0; i < 20; i++ {
		colName := "col_" + string(rune('a'+(i/26))) + string(rune('a'+(i%26)))
		tableEntry.AddColumn(ColumnDefinition{
			Name: colName,
			Type: TypeInteger,
		})
	}

	storage.catalog.Tables = append(storage.catalog.Tables, tableEntry)
	storage.modified = true

	err = storage.Close()
	require.NoError(t, err)

	// Read both files
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	goData, err := os.ReadFile(goPath)
	require.NoError(t, err)

	t.Logf("File sizes: CLI=%d Go=%d", len(cliData), len(goData))

	// Dump first 1024 bytes of SB0 for comparison
	sbOffset := int64(DataBlocksOffset)
	t.Logf("\n=== CLI SB0 first 512 bytes ===\n%s", hex.Dump(cliData[sbOffset:sbOffset+512]))
	t.Logf("\n=== Go SB0 first 512 bytes ===\n%s", hex.Dump(goData[sbOffset:sbOffset+512]))
	t.Logf("\n=== CLI SB0 512-1024 bytes ===\n%s", hex.Dump(cliData[sbOffset+512:sbOffset+1024]))
	t.Logf("\n=== Go SB0 512-1024 bytes ===\n%s", hex.Dump(goData[sbOffset+512:sbOffset+1024]))

	// Dump SB1 regions where differences occur
	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)
	t.Logf("\n=== CLI SB1 0xc50-0xd00 ===\n%s", hex.Dump(cliData[sb1Offset+0xc50:sb1Offset+0xd00]))
	t.Logf("\n=== Go SB1 0xc50-0xd00 ===\n%s", hex.Dump(goData[sb1Offset+0xc50:sb1Offset+0xd00]))
	t.Logf("\n=== CLI SB1 last 48 bytes ===\n%s", hex.Dump(cliData[sb1Offset+0xfd0:sb1Offset+0x1000]))

	// Dump SB3 region where differences occur
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB3 0x500-0x600 ===\n%s", hex.Dump(cliData[sb3Offset+0x500:sb3Offset+0x600]))
	t.Logf("\n=== Go SB3 0x500-0x600 ===\n%s", hex.Dump(goData[sb3Offset+0x500:sb3Offset+0x600]))
	// Dump SB3 end region
	t.Logf("\n=== CLI SB3 0xfe0-0x1000 ===\n%s", hex.Dump(cliData[sb3Offset+0xfe0:sb3Offset+0x1000]))
	t.Logf("\n=== Go SB3 0xfe0-0x1000 ===\n%s", hex.Dump(goData[sb3Offset+0xfe0:sb3Offset+0x1000]))

	// Dump SB4 region where differences occur
	sb4Offset := int64(DataBlocksOffset + 4*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB4 0x150-0x250 ===\n%s", hex.Dump(cliData[sb4Offset+0x150:sb4Offset+0x250]))
	t.Logf("\n=== Go SB4 0x150-0x250 ===\n%s", hex.Dump(goData[sb4Offset+0x150:sb4Offset+0x250]))
	t.Logf("\n=== CLI SB4 0xda0-0xe00 ===\n%s", hex.Dump(cliData[sb4Offset+0xda0:sb4Offset+0xe00]))
	t.Logf("\n=== Go SB4 0xda0-0xe00 ===\n%s", hex.Dump(goData[sb4Offset+0xda0:sb4Offset+0xe00]))

	// Dump SB5 region where differences occur
	sb5Offset := int64(DataBlocksOffset + 5*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB5 0xa00-0xb00 ===\n%s", hex.Dump(cliData[sb5Offset+0xa00:sb5Offset+0xb00]))
	t.Logf("\n=== Go SB5 0xa00-0xb00 ===\n%s", hex.Dump(goData[sb5Offset+0xa00:sb5Offset+0xb00]))

	// Dump SB6 region where differences occur
	sb6Offset := int64(DataBlocksOffset + 6*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB6 0x650-0x750 ===\n%s", hex.Dump(cliData[sb6Offset+0x650:sb6Offset+0x750]))
	t.Logf("\n=== Go SB6 0x650-0x750 ===\n%s", hex.Dump(goData[sb6Offset+0x650:sb6Offset+0x750]))

	// Dump SB7 region where differences occur
	sb7Offset := int64(DataBlocksOffset + 7*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB7 0x2b0-0x350 ===\n%s", hex.Dump(cliData[sb7Offset+0x2b0:sb7Offset+0x350]))
	t.Logf("\n=== Go SB7 0x2b0-0x350 ===\n%s", hex.Dump(goData[sb7Offset+0x2b0:sb7Offset+0x350]))
	t.Logf("\n=== CLI SB7 0xf00-0x1000 ===\n%s", hex.Dump(cliData[sb7Offset+0xf00:sb7Offset+0x1000]))
	t.Logf("\n=== Go SB7 0xf00-0x1000 ===\n%s", hex.Dump(goData[sb7Offset+0xf00:sb7Offset+0x1000]))

	// Verify checksum calculation
	// CLI checksum is in the first 8 bytes of SB0
	cliChecksum := binary.LittleEndian.Uint64(cliData[sbOffset : sbOffset+8])
	goChecksum := binary.LittleEndian.Uint64(goData[sbOffset : sbOffset+8])

	// Compute expected checksums based on data after checksum
	cliDataForChecksum := cliData[sbOffset+8 : sbOffset+MetadataSubBlockSize]
	goDataForChecksum := goData[sbOffset+8 : sbOffset+MetadataSubBlockSize]

	t.Logf("CLI SB0 checksum: 0x%016x", cliChecksum)
	t.Logf("Go SB0 checksum: 0x%016x", goChecksum)
	t.Logf("CLI data bytes 8-16: % x", cliData[sbOffset+8:sbOffset+16])
	t.Logf("Go data bytes 8-16: % x", goData[sbOffset+8:sbOffset+16])
	// Show if data after checksum matches
	dataMatches := true
	for i := range cliDataForChecksum {
		if cliDataForChecksum[i] != goDataForChecksum[i] {
			dataMatches = false
			t.Logf("SB0 data mismatch at offset %d: CLI=0x%02x Go=0x%02x", i+8, cliDataForChecksum[i], goDataForChecksum[i])
			break
		}
	}
	t.Logf("SB0 data after checksum matches: %v", dataMatches)

	// Now test: what checksum does our checksumBlock produce for CLI's data?
	// Note: In DuckDB, checksum is computed over the ENTIRE block (256KB), not just SB0 (4KB)
	// CLI file: offset DataBlocksOffset is start of first 256KB block
	cliBlockData := cliData[DataBlocksOffset+BlockChecksumSize : DataBlocksOffset+DefaultBlockSize]
	goBlockData := goData[DataBlocksOffset+BlockChecksumSize : DataBlocksOffset+DefaultBlockSize]
	t.Logf("CLI block data length: %d", len(cliBlockData))
	t.Logf("Go block data length: %d", len(goBlockData))

	// Compare entire block data
	blockDataMatches := true
	firstMismatch := -1
	for i := range cliBlockData {
		if cliBlockData[i] != goBlockData[i] {
			if blockDataMatches {
				firstMismatch = i
			}
			blockDataMatches = false
		}
	}
	t.Logf("Entire block data matches: %v (first mismatch at %d)", blockDataMatches, firstMismatch)
	if firstMismatch >= 0 {
		// Show the sub-block containing the mismatch
		subBlockIdx := firstMismatch / MetadataSubBlockSize
		offsetInSubBlock := firstMismatch % MetadataSubBlockSize
		t.Logf("Mismatch in sub-block %d at offset 0x%x", subBlockIdx, offsetInSubBlock)
		// Show context around mismatch
		start := firstMismatch - 16
		if start < 0 {
			start = 0
		}
		end := firstMismatch + 16
		if end > len(cliBlockData) {
			end = len(cliBlockData)
		}
		t.Logf("CLI block data around mismatch: % x", cliBlockData[start:end])
		t.Logf("Go block data around mismatch: % x", goBlockData[start:end])
	}

	// Dump SB8 region
	sb8Offset := int64(DataBlocksOffset + 8*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB8 0xb50-0xc00 ===\n%s", hex.Dump(cliData[sb8Offset+0xb50:sb8Offset+0xc00]))
	t.Logf("\n=== Go SB8 0xb50-0xc00 ===\n%s", hex.Dump(goData[sb8Offset+0xb50:sb8Offset+0xc00]))
	t.Logf("\n=== CLI SB8 0xfc0-0x1000 ===\n%s", hex.Dump(cliData[sb8Offset+0xfc0:sb8Offset+0x1000]))
	t.Logf("\n=== Go SB8 0xfc0-0x1000 ===\n%s", hex.Dump(goData[sb8Offset+0xfc0:sb8Offset+0x1000]))

	// Dump SB9 region
	sb9Offset := int64(DataBlocksOffset + 9*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB9 0x7b0-0x850 ===\n%s", hex.Dump(cliData[sb9Offset+0x7b0:sb9Offset+0x850]))
	t.Logf("\n=== Go SB9 0x7b0-0x850 ===\n%s", hex.Dump(goData[sb9Offset+0x7b0:sb9Offset+0x850]))
	t.Logf("\n=== CLI SB9 0xfb0-0x1000 ===\n%s", hex.Dump(cliData[sb9Offset+0xfb0:sb9Offset+0x1000]))
	t.Logf("\n=== Go SB9 0xfb0-0x1000 ===\n%s", hex.Dump(goData[sb9Offset+0xfb0:sb9Offset+0x1000]))

	// Dump SB10 region
	sb10Offset := int64(DataBlocksOffset + 10*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB10 first 256 bytes ===\n%s", hex.Dump(cliData[sb10Offset:sb10Offset+256]))
	t.Logf("\n=== Go SB10 first 256 bytes ===\n%s", hex.Dump(goData[sb10Offset:sb10Offset+256]))
	t.Logf("\n=== CLI SB10 0x3e0-0x4a0 ===\n%s", hex.Dump(cliData[sb10Offset+0x3e0:sb10Offset+0x4a0]))
	t.Logf("\n=== Go SB10 0x3e0-0x4a0 ===\n%s", hex.Dump(goData[sb10Offset+0x3e0:sb10Offset+0x4a0]))
	t.Logf("\n=== CLI SB10 0xfa0-0x1000 ===\n%s", hex.Dump(cliData[sb10Offset+0xfa0:sb10Offset+0x1000]))
	t.Logf("\n=== Go SB10 0xfa0-0x1000 ===\n%s", hex.Dump(goData[sb10Offset+0xfa0:sb10Offset+0x1000]))

	// Dump SB11 region
	sb11Offset := int64(DataBlocksOffset + 11*MetadataSubBlockSize)
	t.Logf("\n=== CLI SB11 first 256 bytes ===\n%s", hex.Dump(cliData[sb11Offset:sb11Offset+256]))
	t.Logf("\n=== Go SB11 first 256 bytes ===\n%s", hex.Dump(goData[sb11Offset:sb11Offset+256]))
	t.Logf("\n=== CLI SB11 0xfa0-0x1000 ===\n%s", hex.Dump(cliData[sb11Offset+0xfa0:sb11Offset+0x1000]))
	t.Logf("\n=== Go SB11 0xfa0-0x1000 ===\n%s", hex.Dump(goData[sb11Offset+0xfa0:sb11Offset+0x1000]))

	// Dump SB12-SB19 end regions to find chain terminator
	for sbIdx := 12; sbIdx <= 19; sbIdx++ {
		sbOff := int64(DataBlocksOffset + sbIdx*MetadataSubBlockSize)
		t.Logf("\n=== CLI SB%d 0xfa0-0x1000 ===\n%s", sbIdx, hex.Dump(cliData[sbOff+0xfa0:sbOff+0x1000]))
	}

	// Compare each sub-block
	for sbIdx := 0; sbIdx <= 11; sbIdx++ {
		sbOffset := int64(DataBlocksOffset + sbIdx*MetadataSubBlockSize)

		if sbOffset+int64(MetadataSubBlockSize) > int64(len(cliData)) ||
			sbOffset+int64(MetadataSubBlockSize) > int64(len(goData)) {
			break
		}

		diffCount := 0
		for i := 0; i < MetadataSubBlockSize; i++ {
			if cliData[sbOffset+int64(i)] != goData[sbOffset+int64(i)] {
				diffCount++
			}
		}

		if diffCount > 0 {
			t.Logf("SB%d: %d differences", sbIdx, diffCount)

			// Show first 20 differences
			shown := 0
			for i := 0; i < MetadataSubBlockSize && shown < 20; i++ {
				if cliData[sbOffset+int64(i)] != goData[sbOffset+int64(i)] {
					t.Logf("  SB%d[0x%03x]: CLI=0x%02x Go=0x%02x", sbIdx, i, cliData[sbOffset+int64(i)], goData[sbOffset+int64(i)])
					shown++
				}
			}
		} else {
			t.Logf("SB%d: IDENTICAL", sbIdx)
		}
	}
}
