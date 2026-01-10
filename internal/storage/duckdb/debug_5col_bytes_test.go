package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColBytes captures all relevant bytes for 5-column table
func TestDebug5ColBytes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI 5-column table matching TestWriteMultipleDataTypes
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

	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)

	// Dump the key areas for 5-column table
	t.Log("=== SB1 header (0x00-0x80) for column count and first column metadata ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset:sb1Offset+0x80]))

	// Second column (VARCHAR) metadata area
	t.Log("\n=== SB1 0xc50-0xca0 - Second column (VARCHAR) metadata ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xc50:sb1Offset+0xca0]))

	// End of SB1 - next_ptr
	t.Log("\n=== SB1 0xff8-0x1000 - End of SB1 (next_ptr) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xff8:sb1Offset+0x1000]))

	// SB2 - third column (BIGINT) and fourth column (BOOLEAN) metadata
	t.Log("\n=== SB2 0x8a0-0x950 - Third & Fourth column metadata ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0x8a0:sb2Offset+0x950]))

	// End of SB2
	t.Log("\n=== SB2 0xff8-0x1000 - End of SB2 (next_ptr) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0xff8:sb2Offset+0x1000]))

	// SB3 - fifth column (DOUBLE) metadata continuation
	t.Log("\n=== SB3 0x000-0x100 - Fifth column (DOUBLE) metadata ===")
	t.Logf("\n%s", hex.Dump(cliData[sb3Offset:sb3Offset+0x100]))

	// End of SB3
	t.Log("\n=== SB3 0xff8-0x1000 - End of SB3 ===")
	t.Logf("\n%s", hex.Dump(cliData[sb3Offset+0xff8:sb3Offset+0x1000]))

	// Print offsets where data differs from zeros
	t.Log("\n=== Non-zero byte ranges in SB2 (0x800-0xa00) ===")
	for i := int64(0x800); i < 0xa00; i++ {
		if cliData[sb2Offset+i] != 0 {
			t.Logf("  SB2[0x%03x] = 0x%02x", i, cliData[sb2Offset+i])
		}
	}

	t.Log("\n=== Non-zero byte ranges in SB3 (0x000-0x200) ===")
	for i := int64(0); i < 0x200; i++ {
		if cliData[sb3Offset+i] != 0 {
			t.Logf("  SB3[0x%03x] = 0x%02x", i, cliData[sb3Offset+i])
		}
	}

	t.Log("\n=== Non-zero byte ranges in SB3 (0xf00-0x1000) ===")
	for i := int64(0xf00); i < 0x1000; i++ {
		if cliData[sb3Offset+i] != 0 {
			t.Logf("  SB3[0x%03x] = 0x%02x", i, cliData[sb3Offset+i])
		}
	}
}
