package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug2ColFreeList shows CLI free_list bytes for 2-column table
func TestDebug2ColFreeList(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI 2-column table
	cliPath := filepath.Join(tmpDir, "cli.duckdb")
	cmd := exec.Command("duckdb", cliPath, `
		CREATE TABLE test (
			id INTEGER,
			name VARCHAR
		);
	`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI failed: %s", string(output))

	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	// Free_list for 2-column table should be at SB3 (sub-block 3)
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)

	t.Log("=== CLI SB3 (free_list) non-zero bytes (0x000-0x030) ===")
	for i := int64(0x000); i < int64(0x030); i++ {
		if cliData[sb3Offset+i] != 0 {
			t.Logf("  SB3[0x%03x] = 0x%02x", i, cliData[sb3Offset+i])
		}
	}
}
