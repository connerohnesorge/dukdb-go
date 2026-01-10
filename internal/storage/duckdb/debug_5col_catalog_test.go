package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColCatalog compares catalog entries for 5-column table
func TestDebug5ColCatalog(t *testing.T) {
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

	// Compare catalog area
	t.Log("=== SB0 (Catalog) 0x00-0x100 ===")
	t.Log("\nCLI:")
	t.Logf("\n%s", hex.Dump(cliData[sb0Offset:sb0Offset+0x100]))
	t.Log("\nGo:")
	t.Logf("\n%s", hex.Dump(goData[sb0Offset:sb0Offset+0x100]))

	// Find catalog differences
	t.Log("\n=== Catalog differences ===")
	diffCount := 0
	for i := int64(0); i < int64(0x200); i++ {
		if cliData[sb0Offset+i] != goData[sb0Offset+i] {
			if diffCount < 50 {
				t.Logf("  SB0[0x%03x]: CLI=0x%02x, Go=0x%02x", i, cliData[sb0Offset+i], goData[sb0Offset+i])
			}
			diffCount++
		}
	}
	t.Logf("Catalog differences: %d bytes", diffCount)
}
