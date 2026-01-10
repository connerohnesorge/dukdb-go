package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	
	"github.com/stretchr/testify/require"
)

func TestQuickBigint(t *testing.T) {
	skipIfNoDuckDBCLI(t)
	
	tmpDir := t.TempDir()
	
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	require.NoError(t, err)

	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	tableEntry.AddColumn(ColumnDefinition{Name: "b", Type: TypeBigInt})
	storage.catalog.Tables = append(storage.catalog.Tables, tableEntry)
	storage.modified = true
	
	err = storage.Close()
	require.NoError(t, err)

	// Test with CLI
	cmd := exec.Command("duckdb", goPath, "SHOW TABLES;")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("DuckDB CLI FAILED: %s", string(output))
	} else {
		t.Logf("DuckDB CLI SUCCESS: %s", string(output))
	}
	
	// Also test with -readonly flag
	cmd = exec.Command("duckdb", "-readonly", goPath, "SHOW TABLES;")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("DuckDB CLI (-readonly) FAILED: %s", string(output))
	} else {
		t.Logf("DuckDB CLI (-readonly) SUCCESS: %s", string(output))
	}
	
	// Check file exists and size
	info, _ := os.Stat(goPath)
	t.Logf("File size: %d bytes", info.Size())
}
