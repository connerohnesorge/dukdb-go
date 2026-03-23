package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/require"
)

// TestExportDatabaseBasic tests basic EXPORT DATABASE functionality.
func TestExportDatabaseBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)

	dir := t.TempDir()
	exportDir := filepath.Join(dir, "export")

	_, err = db.Exec("EXPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Verify files exist
	_, err = os.Stat(filepath.Join(exportDir, "schema.sql"))
	require.NoError(t, err, "schema.sql should exist")
	_, err = os.Stat(filepath.Join(exportDir, "load.sql"))
	require.NoError(t, err, "load.sql should exist")
	_, err = os.Stat(filepath.Join(exportDir, "users.csv"))
	require.NoError(t, err, "users.csv should exist")

	// Verify schema.sql content
	schemaBytes, err := os.ReadFile(filepath.Join(exportDir, "schema.sql"))
	require.NoError(t, err)
	schema := string(schemaBytes)
	require.Contains(t, schema, "CREATE TABLE")
	require.Contains(t, schema, "users")
}

// TestExportImportDatabaseRoundtrip tests export followed by import into a fresh database.
func TestExportImportDatabaseRoundtrip(t *testing.T) {
	// Create and populate source database
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db1.Close()

	_, err = db1.Exec("CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)")
	require.NoError(t, err)
	_, err = db1.Exec("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
	require.NoError(t, err)

	dir := t.TempDir()
	exportDir := filepath.Join(dir, "export")

	_, err = db1.Exec("EXPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Import into a fresh database
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db2.Close()

	_, err = db2.Exec("IMPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Verify data
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	var name string
	var price float64
	err = db2.QueryRow("SELECT name, price FROM products WHERE id = 1").Scan(&name, &price)
	require.NoError(t, err)
	require.Equal(t, "Widget", name)
	require.Equal(t, 9.99, price)
}

// TestImportNonExistentDirectory tests that IMPORT DATABASE returns an error for non-existent paths.
func TestImportNonExistentDirectory(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("IMPORT DATABASE '/nonexistent/path'")
	require.Error(t, err)
}

// TestExportMultipleTables tests EXPORT DATABASE with multiple tables.
func TestExportMultipleTables(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1 (a INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE t2 (b VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t1 VALUES (1), (2)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t2 VALUES ('x'), ('y')")
	require.NoError(t, err)

	dir := t.TempDir()
	exportDir := filepath.Join(dir, "export")

	_, err = db.Exec("EXPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Verify both data files exist
	_, err = os.Stat(filepath.Join(exportDir, "t1.csv"))
	require.NoError(t, err, "t1.csv should exist")
	_, err = os.Stat(filepath.Join(exportDir, "t2.csv"))
	require.NoError(t, err, "t2.csv should exist")
}

// TestExportImportDatabaseMultipleTablesRoundtrip tests full roundtrip with multiple tables.
func TestExportImportDatabaseMultipleTablesRoundtrip(t *testing.T) {
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db1.Close()

	_, err = db1.Exec("CREATE TABLE t1 (a INTEGER)")
	require.NoError(t, err)
	_, err = db1.Exec("CREATE TABLE t2 (b VARCHAR)")
	require.NoError(t, err)
	_, err = db1.Exec("INSERT INTO t1 VALUES (1), (2), (3)")
	require.NoError(t, err)
	_, err = db1.Exec("INSERT INTO t2 VALUES ('hello'), ('world')")
	require.NoError(t, err)

	dir := t.TempDir()
	exportDir := filepath.Join(dir, "export")

	_, err = db1.Exec("EXPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Import into fresh database
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db2.Close()

	_, err = db2.Exec("IMPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Verify t1 data
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Verify t2 data
	err = db2.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

// TestExportImportDatabaseEmptyTable tests export/import with a table that has no data.
func TestExportImportDatabaseEmptyTable(t *testing.T) {
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db1.Close()

	_, err = db1.Exec("CREATE TABLE empty_table (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	dir := t.TempDir()
	exportDir := filepath.Join(dir, "export")

	_, err = db1.Exec("EXPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Import into fresh database
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db2.Close()

	_, err = db2.Exec("IMPORT DATABASE '" + exportDir + "'")
	require.NoError(t, err)

	// Verify table exists but is empty
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM empty_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
