package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColTablePtr compares table storage pointer for 5-column table
func TestDebug5ColTablePtr(t *testing.T) {
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

	// Create Go 5-column table
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	require.NoError(t, err)

	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{Name: "col_int", Type: TypeInteger})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_varchar", Type: TypeVarchar})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_bigint", Type: TypeBigInt})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_bool", Type: TypeBoolean})
	tableEntry.AddColumn(ColumnDefinition{Name: "col_double", Type: TypeDouble})

	storage.catalog.Tables = append(storage.catalog.Tables, tableEntry)
	storage.modified = true

	err = storage.Close()
	require.NoError(t, err)

	// Read both files
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	goData, err := os.ReadFile(goPath)
	require.NoError(t, err)

	// Catalog is in sub-block 0
	sb0Offset := int64(DataBlocksOffset)

	// Show full catalog for analysis
	t.Log("=== CLI Full Catalog (0xd0-0x100) - contains table pointer area ===")
	t.Logf("\n%s", hex.Dump(cliData[sb0Offset+0xd0:sb0Offset+0x100]))

	t.Log("\n=== Go Full Catalog (0xd0-0x100) ===")
	t.Logf("\n%s", hex.Dump(goData[sb0Offset+0xd0:sb0Offset+0x100]))

	// Find where table storage pointer is (after catalog entry, field 101)
	// CLI has shorter catalog so offsets differ
	t.Log("\n=== CLI catalog area with table pointer (0x0d0-0x0f8) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb0Offset+0xd0:sb0Offset+0xf8]))

	// For Go, need to find the corresponding offset (shifted by 3 bytes due to longer catalog name)
	t.Log("\n=== Go catalog area with table pointer (0x0d3-0x0fb) ===")
	t.Logf("\n%s", hex.Dump(goData[sb0Offset+0xd3:sb0Offset+0xfb]))

	// Dump the area around 0xe0-0xf8 which should contain table_pointer field
	t.Log("\n=== CLI around table_pointer (0xe0-0x100) ===")
	t.Logf("\n%s", hex.Dump(cliData[sb0Offset+0xe0:sb0Offset+0x100]))

	t.Log("\n=== Go around table_pointer (0xe3-0x103) - shifted ===")
	if sb0Offset+0x103 <= int64(len(goData)) {
		t.Logf("\n%s", hex.Dump(goData[sb0Offset+0xe3:sb0Offset+0x103]))
	}

	// Check non-zero bytes in the table storage metadata area for CLI
	t.Log("\n=== CLI Catalog non-zero bytes (0x0d0-0x120) ===")
	for i := int64(0xd0); i < 0x120; i++ {
		if cliData[sb0Offset+i] != 0 {
			t.Logf("  CLI[0x%03x] = 0x%02x", i, cliData[sb0Offset+i])
		}
	}

	t.Log("\n=== Go Catalog non-zero bytes (0x0d0-0x120) ===")
	for i := int64(0xd0); i < 0x120; i++ {
		if goData[sb0Offset+i] != 0 {
			t.Logf("  Go[0x%03x] = 0x%02x", i, goData[sb0Offset+i])
		}
	}
}
