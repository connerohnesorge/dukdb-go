package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug3ColVarcharBytes shows CLI bytes for 3-column VARCHAR table
func TestDebug3ColVarcharBytes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()

	// Create CLI 3-column table with VARCHAR columns
	cliPath := filepath.Join(tmpDir, "cli.duckdb")
	cmd := exec.Command("duckdb", cliPath, `
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			email VARCHAR
		);
	`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI failed: %s", string(output))

	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	// Show non-zero bytes in SB1 around 0xc57
	sb1Offset := int64(DataBlocksOffset + 1*MetadataSubBlockSize)

	t.Log("=== CLI SB1 non-zero bytes (0xc50-0xcd0) ===")
	for i := int64(0xc50); i < int64(0xcd0); i++ {
		if cliData[sb1Offset+i] != 0 {
			t.Logf("  SB1[0x%03x] = 0x%02x", i, cliData[sb1Offset+i])
		}
	}

	// Show non-zero bytes in SB2 around 0x8b4
	sb2Offset := int64(DataBlocksOffset + 2*MetadataSubBlockSize)

	t.Log("\n=== CLI SB2 non-zero bytes (0x8a0-0x900) ===")
	for i := int64(0x8a0); i < int64(0x900); i++ {
		if cliData[sb2Offset+i] != 0 {
			t.Logf("  SB2[0x%03x] = 0x%02x", i, cliData[sb2Offset+i])
		}
	}

	// Show non-zero bytes in SB3
	sb3Offset := int64(DataBlocksOffset + 3*MetadataSubBlockSize)

	t.Log("\n=== CLI SB3 non-zero bytes (0x500-0x600) ===")
	for i := int64(0x500); i < int64(0x600); i++ {
		if cliData[sb3Offset+i] != 0 {
			t.Logf("  SB3[0x%03x] = 0x%02x", i, cliData[sb3Offset+i])
		}
	}

	// Show non-zero bytes in SB4 (free_list)
	sb4Offset := int64(DataBlocksOffset + 4*MetadataSubBlockSize)

	t.Log("\n=== CLI SB4 (free_list) non-zero bytes (0x000-0x030) ===")
	for i := int64(0x000); i < int64(0x030); i++ {
		if cliData[sb4Offset+i] != 0 {
			t.Logf("  SB4[0x%03x] = 0x%02x", i, cliData[sb4Offset+i])
		}
	}
}
