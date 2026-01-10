package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColTable compares CLI 5-column table with our current output
func TestDebug5ColTable(t *testing.T) {
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

	// Verify CLI can read it
	cmd = exec.Command("duckdb", cliPath, "DESCRIBE test;")
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "CLI DESCRIBE failed: %s", string(output))
	t.Logf("CLI DESCRIBE output:\n%s", string(output))

	// Read CLI file
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	// Show catalog block (sub-block 0)
	sb0Offset := int64(DataBlocksOffset)
	t.Log("=== CLI 5-column Catalog (sub-block 0) at 0x30 (256 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb0Offset+0x30:sb0Offset+0x30+256]))

	// Show table storage sub-block 1
	sb1Offset := int64(DataBlocksOffset + MetadataSubBlockSize)
	t.Log("\n=== CLI 5-column SB1 at 0x00 (64 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset:sb1Offset+64]))

	t.Log("\n=== CLI 5-column SB1 at 0xc50 (192 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb1Offset+0xc50:sb1Offset+0xc50+192]))

	// Show table storage sub-block 2
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)
	t.Log("\n=== CLI 5-column SB2 at 0x00 (64 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset:sb2Offset+64]))

	t.Log("\n=== CLI 5-column SB2 at 0x880 (128 bytes) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb2Offset+0x880:sb2Offset+0x880+128]))
}
