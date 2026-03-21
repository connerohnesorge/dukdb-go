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

func TestDiagSchema(t *testing.T) {
	db, _ := sql.Open("dukdb", ":memory:")
	defer db.Close()

	stmts := []string{
		"CREATE SCHEMA s2",
		"CREATE TABLE s2.t1(id INT)",
		"CREATE TABLE main_data(id INT)",
		"CREATE TABLE t1(id INT)",
	}
	for _, s := range stmts {
		_, err := db.Exec(s)
		fmt.Printf("%s => %v\n", s, err)
	}
}

func TestDiagDefaults(t *testing.T) {
	db, _ := sql.Open("dukdb", ":memory:")
	defer db.Close()

	_, err := db.Exec("CREATE TABLE def_test(id INT DEFAULT 42, name VARCHAR DEFAULT 'hello')")
	fmt.Println("CREATE TABLE:", err)

	tmpDir := t.TempDir()
	_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s/exp'", tmpDir))
	fmt.Println("EXPORT:", err)

	data, _ := os.ReadFile(filepath.Join(tmpDir, "exp", "schema.sql"))
	fmt.Printf("schema.sql: %s\n", string(data))
}

func TestDiagSchemaSeqView(t *testing.T) {
	db, _ := sql.Open("dukdb", ":memory:")
	defer db.Close()

	stmts := []string{
		"CREATE SEQUENCE my_seq",
		"CREATE TABLE t1(id INT)",
		"CREATE VIEW v1 AS SELECT id FROM t1",
		"CREATE INDEX idx1 ON t1(id)",
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
}
