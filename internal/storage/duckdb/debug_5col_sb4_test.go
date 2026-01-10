package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColSB4 shows CLI SB4 and SB5 data for 5-column table
func TestDebug5ColSB4(t *testing.T) {
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

	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	sb4Offset := int64(DataBlocksOffset + 4*MetadataSubBlockSize)
	sb5Offset := int64(DataBlocksOffset + 5*MetadataSubBlockSize)

	// Show non-zero bytes in SB4
	t.Log("=== CLI SB4 non-zero bytes ===")
	for i := int64(0); i < int64(MetadataSubBlockSize); i++ {
		if cliData[sb4Offset+i] != 0 {
			t.Logf("  SB4[0x%03x] = 0x%02x", i, cliData[sb4Offset+i])
		}
	}

	// Show non-zero bytes in SB5
	t.Log("\n=== CLI SB5 non-zero bytes ===")
	for i := int64(0); i < int64(MetadataSubBlockSize); i++ {
		if cliData[sb5Offset+i] != 0 {
			t.Logf("  SB5[0x%03x] = 0x%02x", i, cliData[sb5Offset+i])
		}
	}
}
