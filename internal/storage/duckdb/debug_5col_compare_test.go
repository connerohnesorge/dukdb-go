package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColCompare compares Go output with CLI output for 5-column table
func TestDebug5ColCompare(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI 5-column table
	cliPath := filepath.Join(tmpDir, "cli.duckdb")
	cmd := exec.Command("duckdb", cliPath, `
		CREATE TABLE test (
			col_int INTEGER,
			col_varchar VARCHAR,
			col_bigint BIGINT,
			col_bool BOOLEAN,
			col_double DOUBLE
		);
	`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI failed: %s", string(output))

	// Create Go 5-column table
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	require.NoError(t, err)

	// Create table using the same pattern as write_verify_test.go
	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{Name: "col_int", Type: TypeInteger})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_varchar", Type: TypeVarchar})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_bigint", Type: TypeBigInt})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_bool", Type: TypeBoolean})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_double", Type: TypeDouble})

	storage.catalog.Tables = append(storage.catalog.Tables, tableEntry)
	storage.modified = true

	err = storage.Close()
	require.NoError(t, err)

	// Read both files
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	goData, err := os.ReadFile(goPath)
	require.NoError(t, err)

	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)

	// Compare SB1 header
	t.Log("=== SB1 header comparison (0x00-0x80) ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset:sb1Offset+0x80]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb1Offset:sb1Offset+0x80]))

	// Compare SB1 second column area (0xc50-0xca0)
	t.Log("\n=== SB1 second column area (0xc50-0xca0) ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xc50:sb1Offset+0xca0]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb1Offset+0xc50:sb1Offset+0xca0]))

	// Compare SB1 end (0xff8-0x1000)
	t.Log("\n=== SB1 end (0xff8-0x1000) ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xff8:sb1Offset+0x1000]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb1Offset+0xff8:sb1Offset+0x1000]))

	// Compare SB2 third column area (0x8a0-0x910)
	t.Log("\n=== SB2 third column area (0x8a0-0x910) ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0x8a0:sb2Offset+0x910]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb2Offset+0x8a0:sb2Offset+0x910]))

	// Compare SB3 end (0xfe0-0x1000)
	t.Log("\n=== SB3 end (0xfe0-0x1000) ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb3Offset+0xfe0:sb3Offset+0x1000]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb3Offset+0xfe0:sb3Offset+0x1000]))

	// Find first difference
	t.Log("\n=== First differences ===")
	diffCount := 0
	for i := int64(0); i < int64(len(cliData)) && i < int64(len(goData)); i++ {
		if cliData[i] != goData[i] {
			if diffCount < 30 {
				blockType := "header"
				blockOffset := i
				if i >= DataBlocksOffset {
					blockNum := (i - DataBlocksOffset) / MetadataSubBlockSize
					blockType = "SB" + string('0'+byte(blockNum))
					blockOffset = (i - DataBlocksOffset) % MetadataSubBlockSize
				}
				t.Logf("  Diff at 0x%04x (%s+0x%03x): CLI=0x%02x, Go=0x%02x",
					i, blockType, blockOffset, cliData[i], goData[i])
			}
			diffCount++
		}
	}
	t.Logf("Total differences: %d bytes", diffCount)
}
