package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColDetailed compares CLI 5-column table storage layout
func TestDebug5ColDetailed(t *testing.T) {
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

	// Read CLI file
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	// Show entire sub-block 1 structure
	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)
	t.Log("=== CLI 5-column SB1 (Full table storage header) ===")

	// First 128 bytes of SB1
	t.Log("\n=== SB1 @ 0x0000-0x007f (128 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset:sb1Offset+128]))

	// Table storage info area (where column metadata starts)
	t.Log("\n=== SB1 @ 0x0c00-0x0cff (256 bytes) - Table storage area ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xc00:sb1Offset+0xd00]))

	// End of SB1 area
	t.Log("\n=== SB1 @ 0x0f00-0x0fff (256 bytes) - End of SB1 ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xf00:sb1Offset+0x1000]))

	// Sub-block 2
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)
	t.Log("\n=== SB2 @ 0x0000-0x00ff (256 bytes) - Start of SB2 ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset:sb2Offset+256]))

	// SB2 continuation of table storage
	t.Log("\n=== SB2 @ 0x0800-0x08ff (256 bytes) - Table storage continuation ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0x800:sb2Offset+0x900]))

	// SB2 more data
	t.Log("\n=== SB2 @ 0x0900-0x09ff (256 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0x900:sb2Offset+0xa00]))

	// Compare with 2-column table structure
	t.Log("\n\n====== NOW CREATING 2-COLUMN TABLE FOR COMPARISON ======")

	cli2Path := filepath.Join(tmpDir, "cli2.duckdb")
	cmd = exec.Command("duckdb", cli2Path, `
		CREATE TABLE test (col_int INTEGER, col_varchar VARCHAR);
	`)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "CLI 2-col failed: %s", string(output))

	cli2Data, err := os.ReadFile(cli2Path)
	require.NoError(t, err)

	sb1Offset2 := int64(DataBlocksOffset + MetadataSubBlockSize)
	t.Log("\n=== 2-col SB1 @ 0x0000-0x007f (128 bytes) ===")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset2:sb1Offset2+128]))

	t.Log("\n=== 2-col SB1 @ 0x0c00-0x0cff (256 bytes) - Table storage area ===")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset2+0xc00:sb1Offset2+0xd00]))

	t.Log("\n=== 2-col SB1 @ 0x0f00-0x0fff (256 bytes) - End of SB1 ===")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset2+0xf00:sb1Offset2+0x1000]))
}
