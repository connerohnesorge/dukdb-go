package tests

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func TestDiag2MultiSchema(t *testing.T) {
	db, _ := sql.Open("dukdb", ":memory:")
	defer db.Close()

	stmts := []string{
		"CREATE SCHEMA s2",
		"CREATE TABLE data(id INT)",
		"INSERT INTO data VALUES (1)",
		"CREATE TABLE s2.other_data(id INT)",
		"INSERT INTO s2.other_data VALUES (2)",
	}
	for _, s := range stmts {
		_, err := db.Exec(s)
		fmt.Printf("%s => %v\n", s, err)
	}

	tmpDir := t.TempDir()
	_, err := db.Exec(fmt.Sprintf("EXPORT DATABASE '%s/exp'", tmpDir))
	fmt.Println("EXPORT:", err)
	data, _ := os.ReadFile(filepath.Join(tmpDir, "exp", "schema.sql"))
	fmt.Printf("schema.sql:\n%s\n", string(data))
	
	// List files
	entries, _ := os.ReadDir(filepath.Join(tmpDir, "exp"))
	for _, e := range entries {
		fmt.Println("file:", e.Name())
	}
}
