package duckdb

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugMultiTableCompare compares multi-table database bytes with CLI
func TestDebugMultiTableCompare(t *testing.T) {
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

	// Create Go multi-table database
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	require.NoError(t, err)

	// Create first table: orders (alphabetically first, to match CLI order)
	orders := NewTableCatalogEntry("orders")
	orders.CreateInfo.Schema = "main"
	orders.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	orders.AddColumn(ColumnDefinition{Name: "user_id", Type: TypeInteger})
	orders.AddColumn(ColumnDefinition{Name: "amount", Type: TypeDouble})

	// Create second table: users
	users := NewTableCatalogEntry("users")
	users.CreateInfo.Schema = "main"
	users.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	users.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	users.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})

	storage.catalog.Tables = append(storage.catalog.Tables, orders, users)
	storage.modified = true

	err = storage.Close()
	require.NoError(t, err)

	// Read both files
	cliData, err := os.ReadFile(cliPath)
	require.NoError(t, err)

	goData, err := os.ReadFile(goPath)
	require.NoError(t, err)

	t.Logf("File sizes: CLI=%d Go=%d", len(cliData), len(goData))

	// Dump first 512 bytes of SB0 for detailed comparison
	sbOffset := int64(DataBlocksOffset)
	t.Logf("\n=== CLI SB0 first 256 bytes ===\n%s", hex.Dump(cliData[sbOffset:sbOffset+256]))
	t.Logf("\n=== Go SB0 first 256 bytes ===\n%s", hex.Dump(goData[sbOffset:sbOffset+256]))
	t.Logf("\n=== CLI SB0 0x100-0x180 ===\n%s", hex.Dump(cliData[sbOffset+0x100:sbOffset+0x180]))
	t.Logf("\n=== Go SB0 0x100-0x180 ===\n%s", hex.Dump(goData[sbOffset+0x100:sbOffset+0x180]))

	// Compare each sub-block
	for sbIdx := 0; sbIdx <= 7; sbIdx++ {
		sbOffset := int64(DataBlocksOffset + sbIdx*MetadataSubBlockSize)

		if sbOffset+int64(MetadataSubBlockSize) > int64(len(cliData)) ||
			sbOffset+int64(MetadataSubBlockSize) > int64(len(goData)) {
			break
		}

		diffCount := 0
		for i := 0; i < MetadataSubBlockSize; i++ {
			if cliData[sbOffset+int64(i)] != goData[sbOffset+int64(i)] {
				diffCount++
			}
		}

		if diffCount > 0 {
			t.Logf("SB%d: %d differences", sbIdx, diffCount)

			// Show first 20 differences
			shown := 0
			for i := 0; i < MetadataSubBlockSize && shown < 20; i++ {
				if cliData[sbOffset+int64(i)] != goData[sbOffset+int64(i)] {
					t.Logf("  SB%d[0x%03x]: CLI=0x%02x Go=0x%02x", sbIdx, i, cliData[sbOffset+int64(i)], goData[sbOffset+int64(i)])
					shown++
				}
			}
		} else {
			t.Logf("SB%d: IDENTICAL", sbIdx)
		}
	}
}
