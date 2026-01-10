package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug3ColTable compares CLI 3-column table with 2-column table
func TestDebug3ColTable(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI 3-column table
	cli3Path := filepath.Join(tmpDir, "cli3.duckdb")
	cmd := exec.Command("duckdb", cli3Path, `
		CREATE TABLE test (
			col_int INTEGER,
			col_varchar VARCHAR,
			col_bigint BIGINT
		);
	`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI 3-col failed: %s", string(output))

	// Create CLI 2-column table
	cli2Path := filepath.Join(tmpDir, "cli2.duckdb")
	cmd = exec.Command("duckdb", cli2Path, `
		CREATE TABLE test (col_int INTEGER, col_varchar VARCHAR);
	`)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "CLI 2-col failed: %s", string(output))

	// Read both files
	cli3Data, err := os.ReadFile(cli3Path)
	require.NoError(t, err)

	cli2Data, err := os.ReadFile(cli2Path)
	require.NoError(t, err)

	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)

	// Compare first 128 bytes
	t.Log("=== SB1 @ 0x0000-0x007f - 3-col vs 2-col ===")
	t.Log("\n3-col:")
	t.Logf("\n%s", hex.Dump(cli3Data[sb1Offset:sb1Offset+128]))
	t.Log("\n2-col:")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset:sb1Offset+128]))

	// Show the table storage area at 0xc50
	t.Log("\n=== SB1 @ 0x0c50-0x0d20 - 3-col ===")
	t.Logf("\n%s", hex.Dump(cli3Data[sb1Offset+0xc50:sb1Offset+0xd20]))

	t.Log("\n=== SB1 @ 0x0c50-0x0d20 - 2-col ===")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset+0xc50:sb1Offset+0xd20]))

	// Show SB1 end
	t.Log("\n=== SB1 @ 0x0ff0-0x1000 - 3-col ===")
	t.Logf("\n%s", hex.Dump(cli3Data[sb1Offset+0xff0:sb1Offset+0x1000]))

	t.Log("\n=== SB1 @ 0x0ff0-0x1000 - 2-col ===")
	t.Logf("\n%s", hex.Dump(cli2Data[sb1Offset+0xff0:sb1Offset+0x1000]))

	// Show SB2 for 3-col (where the third column data continues)
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)
	t.Log("\n=== SB2 @ 0x0870-0x0930 - 3-col ===")
	t.Logf("\n%s", hex.Dump(cli3Data[sb2Offset+0x870:sb2Offset+0x930]))

	// Show SB3 for 3-col (continuation)
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)
	t.Log("\n=== SB3 @ 0x0000-0x0080 - 3-col ===")
	t.Logf("\n%s", hex.Dump(cli3Data[sb3Offset:sb3Offset+0x80]))
}
