package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColSB3 shows CLI SB3 data for 5-column table
func TestDebug5ColSB3(t *testing.T) {
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

	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)

	// Show non-zero bytes in SB3
	t.Log("=== CLI SB3 non-zero bytes ===")
	for i := int64(0); i < int64(MetadataSubBlockSize); i++ {
		if cliData[sb3Offset+i] != 0 {
			t.Logf("  SB3[0x%03x] = 0x%02x", i, cliData[sb3Offset+i])
		}
	}
}
