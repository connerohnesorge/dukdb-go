package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugMultiTableHexDump shows CLI hex dump for multi-table database
func TestDebugMultiTableHexDump(t *testing.T) {
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

	// Dump relevant portions of each sub-block
	for sbIdx := 1; sbIdx <= 6; sbIdx++ {
		sbOffset := int64(DataBlocksOffset + sbIdx*MetadataSubBlockSize)
		if sbOffset+int64(MetadataSubBlockSize) > int64(len(cliData)) {
			break
		}

		t.Logf("\n=== CLI SB%d ===", sbIdx)

		// First 128 bytes
		t.Logf("First 128 bytes:\n%s", hex.Dump(cliData[sbOffset:sbOffset+128]))

		// 0x500-0x600 region (for SB3 table data)
		if sbIdx == 3 {
			t.Logf("0x500-0x600 region:\n%s", hex.Dump(cliData[sbOffset+0x500:sbOffset+0x600]))
		}

		// 0x8b0-0x900 region (for SB2 continuation)
		if sbIdx == 2 {
			t.Logf("0x8b0-0x900 region:\n%s", hex.Dump(cliData[sbOffset+0x8b0:sbOffset+0x900]))
		}

		// 0xa40-0xab0 region (for SB5)
		if sbIdx == 5 {
			t.Logf("0xa40-0xab0 region:\n%s", hex.Dump(cliData[sbOffset+0xa40:sbOffset+0xab0]))
		}

		// 0xc50-0xd00 region (for SB1)
		if sbIdx == 1 {
			t.Logf("0xc50-0xd00 region:\n%s", hex.Dump(cliData[sbOffset+0xc50:sbOffset+0xd00]))
		}

		// 0x190-0x200 region (for SB4)
		if sbIdx == 4 {
			t.Logf("0x190-0x200 region:\n%s", hex.Dump(cliData[sbOffset+0x190:sbOffset+0x200]))
			t.Logf("0xde0-0xe50 region:\n%s", hex.Dump(cliData[sbOffset+0xde0:sbOffset+0xe50]))
		}

		// 0xfd0-0x1000 region (last 48 bytes)
		t.Logf("Last 48 bytes (0xfd0-0xfff):\n%s", hex.Dump(cliData[sbOffset+0xfd0:sbOffset+0x1000]))
	}
}
