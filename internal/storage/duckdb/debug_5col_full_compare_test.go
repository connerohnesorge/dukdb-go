package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebug5ColFullCompare does byte-by-byte comparison for 5-column table
func TestDebug5ColFullCompare(t *testing.T) {
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

	// Compare each sub-block
	for sbIdx := 0; sbIdx <= 5; sbIdx++ {
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

			// Show first 10 differences
			shown := 0
			for i := 0; i < MetadataSubBlockSize && shown < 10; i++ {
				if cliData[sbOffset+int64(i)] != goData[sbOffset+int64(i)] {
					t.Logf("  SB%d[0x%03x]: CLI=0x%02x Go=0x%02x", sbIdx, i, cliData[sbOffset+int64(i)], goData[sbOffset+int64(i)])
					shown++
				}
			}
		} else {
			t.Logf("SB%d: IDENTICAL", sbIdx)
		}
	}

	// Check if files are same size
	t.Logf("\nFile sizes: CLI=%d Go=%d", len(cliData), len(goData))
}
