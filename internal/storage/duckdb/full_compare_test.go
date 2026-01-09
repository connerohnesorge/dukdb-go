package duckdb

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestFullFileCompare(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create Go-generated file
	goPath := filepath.Join(tmpDir, "go.duckdb")
	storage, err := CreateDuckDBStorage(goPath, nil)
	if err != nil {
		t.Fatalf("Failed to create Go storage: %v", err)
	}
	
	table := NewTableCatalogEntry("test")
	table.CreateInfo.Schema = "main"
	table.AddColumn(ColumnDefinition{Name: "col1", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "col2", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "col3", Type: TypeInteger})
	
	storage.catalog.AddTable(table)
	storage.modified = true
	storage.Close()
	
	// Create native DuckDB file
	nativePath := filepath.Join(tmpDir, "native.duckdb")
	cmd := exec.Command("duckdb", nativePath, "-c", "CREATE TABLE test(col1 INTEGER, col2 INTEGER, col3 INTEGER);")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create native DB: %v\n%s", err, out)
	}
	
	// Read and compare both files byte by byte starting from 0x3000 (metadata block)
	goFile, _ := os.Open(goPath)
	nativeFile, _ := os.Open(nativePath)
	defer goFile.Close()
	defer nativeFile.Close()
	
	goData := make([]byte, 400)
	nativeData := make([]byte, 400)
	goFile.ReadAt(goData, 0x3000)
	nativeFile.ReadAt(nativeData, 0x3000)
	
	fmt.Println("=== GO-GENERATED file metadata ===")
	fmt.Println(hex.Dump(goData[:200]))
	
	fmt.Println("=== NATIVE DuckDB file metadata ===")
	fmt.Println(hex.Dump(nativeData[:200]))
	
	// Find first byte difference
	for i := 0; i < len(goData) && i < len(nativeData); i++ {
		if goData[i] != nativeData[i] {
			fmt.Printf("First difference at offset 0x%x: go=0x%02x, native=0x%02x\n", i, goData[i], nativeData[i])
			
			// Show surrounding context
			start := i - 10
			if start < 0 {
				start = 0
			}
			end := i + 20
			if end > len(goData) {
				end = len(goData)
			}
			fmt.Printf("Go around diff:\n%s", hex.Dump(goData[start:end]))
			fmt.Printf("Native around diff:\n%s", hex.Dump(nativeData[start:end]))
			break
		}
	}
	
	// Test with DuckDB CLI
	fmt.Println("\n=== Testing Go file with DuckDB CLI ===")
	cmd = exec.Command("duckdb", goPath, "-c", "DESCRIBE test;")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("Go file FAILED: %v\n%s", err, out)
	} else {
		fmt.Printf("SUCCESS: %s", out)
	}
}
