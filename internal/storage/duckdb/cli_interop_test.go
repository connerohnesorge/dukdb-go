package duckdb

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDuckDBCLIInterop(t *testing.T) {
	tmpDir := t.TempDir()
	ourPath := filepath.Join(tmpDir, "ours.duckdb")

	// Create our file with a simple table
	storage, err := CreateDuckDBStorage(ourPath, nil)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create a table with INTEGER and VARCHAR columns
	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{
		Name: "id",
		Type: TypeInteger,
	})
	tableEntry.AddColumn(ColumnDefinition{
		Name: "name",
		Type: TypeVarchar,
	})

	storage.catalog.AddTable(tableEntry)
	storage.modified = true

	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Test 1: SELECT 1 (basic connectivity)
	t.Run("SELECT_1", func(t *testing.T) {
		cmd := exec.Command("duckdb", ourPath, "-c", "SELECT 1;")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("SELECT 1 failed: %v\nOutput: %s", err, output)
		} else {
			t.Logf("SELECT 1 output:\n%s", output)
		}
	})

	// Test 2: SHOW TABLES
	t.Run("SHOW_TABLES", func(t *testing.T) {
		cmd := exec.Command("duckdb", ourPath, "-c", "SHOW TABLES;")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("SHOW TABLES failed: %v\nOutput: %s", err, output)
		} else {
			t.Logf("SHOW TABLES output:\n%s", output)
			if !strings.Contains(string(output), "test") {
				t.Error("Expected 'test' table in output")
			}
		}
	})

	// Test 3: DESCRIBE test
	t.Run("DESCRIBE", func(t *testing.T) {
		cmd := exec.Command("duckdb", ourPath, "-c", "DESCRIBE test;")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("DESCRIBE failed: %v\nOutput: %s", err, output)
		} else {
			t.Logf("DESCRIBE output:\n%s", output)
			if !strings.Contains(string(output), "id") {
				t.Error("Expected 'id' column in output")
			}
			if !strings.Contains(string(output), "name") {
				t.Error("Expected 'name' column in output")
			}
		}
	})

	// Test 4: SELECT * FROM test
	t.Run("SELECT_ALL", func(t *testing.T) {
		cmd := exec.Command("duckdb", ourPath, "-c", "SELECT * FROM test;")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("SELECT * failed: %v\nOutput: %s", err, output)
		} else {
			t.Logf("SELECT * output:\n%s", output)
		}
	})
}
