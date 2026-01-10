package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugMultiTableSubBlocks shows CLI sub-block structure for multi-table database
func TestDebugMultiTableSubBlocks(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI multi-table database
	cliPath := filepath.Join(tmpDir, "cli.duckdb")
	cmd := exec.Command("duckdb", cliPath, `
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			email VARCHAR
		);
		CREATE TABLE orders (
			id INTEGER,
			user_id INTEGER,
			amount DOUBLE
		);
	`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI failed: %s", string(output))

	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	t.Logf("File size: %d bytes", len(cliData))

	// Show first 64 bytes of each sub-block
	for sbIdx := 0; sbIdx <= 7; sbIdx++ {
		sbOffset := int64(DataBlocksOffset + sbIdx*MetadataSubBlockSize)
		if sbOffset+64 > int64(len(cliData)) {
			break
		}

		t.Logf("\n=== CLI SB%d first 64 bytes ===", sbIdx)
		t.Logf("%s", hex.Dump(cliData[sbOffset:sbOffset+64]))

		// Also show non-zero regions
		t.Logf("=== CLI SB%d non-zero regions ===", sbIdx)
		lastWasZero := true
		for i := 0; i < MetadataSubBlockSize && sbOffset+int64(i) < int64(len(cliData)); i++ {
			isZero := cliData[sbOffset+int64(i)] == 0
			if !isZero {
				if lastWasZero {
					t.Logf("  Non-zero starting at 0x%03x:", i)
				}
				lastWasZero = false
			} else if !lastWasZero {
				t.Logf("  ... to 0x%03x", i-1)
				lastWasZero = true
			}
		}
	}
}
