package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExportImportRoundtrip tests that exporting a database and importing it
// into a fresh connection produces identical data.
func TestExportImportRoundtrip(t *testing.T) {
	// Set up the source database
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db1.Close() }()

	// Create tables with data
	_, err = db1.Exec("CREATE TABLE t1(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)
	_, err = db1.Exec("INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	_, err = db1.Exec("CREATE TABLE t2(x INTEGER, y INTEGER)")
	require.NoError(t, err)
	_, err = db1.Exec("INSERT INTO t2 VALUES (10, 20), (30, 40)")
	require.NoError(t, err)

	// Create a temp directory for export
	exportDir := t.TempDir()
	exportPath := filepath.Join(exportDir, "test_export")

	// Export
	_, err = db1.Exec("EXPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	// Verify files exist
	_, err = os.Stat(filepath.Join(exportPath, "schema.sql"))
	require.NoError(t, err, "schema.sql should exist")
	_, err = os.Stat(filepath.Join(exportPath, "load.sql"))
	require.NoError(t, err, "load.sql should exist")
	_, err = os.Stat(filepath.Join(exportPath, "t1.csv"))
	require.NoError(t, err, "t1.csv should exist")
	_, err = os.Stat(filepath.Join(exportPath, "t2.csv"))
	require.NoError(t, err, "t2.csv should exist")

	// Import into a fresh database
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	_, err = db2.Exec("IMPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	// Verify data in t1
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "t1 should have 2 rows after import")

	// Verify specific values in t1
	var name string
	err = db2.QueryRow("SELECT name FROM t1 WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name)

	err = db2.QueryRow("SELECT name FROM t1 WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "bob", name)

	// Verify data in t2
	err = db2.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "t2 should have 2 rows after import")

	var x, y int
	err = db2.QueryRow("SELECT x, y FROM t2 WHERE x = 10").Scan(&x, &y)
	require.NoError(t, err)
	assert.Equal(t, 10, x)
	assert.Equal(t, 20, y)
}

// TestExportDatabaseCreatesDirectory tests that EXPORT DATABASE creates
// the output directory if it does not exist.
func TestExportDatabaseCreatesDirectory(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE empty_table(id INTEGER)")
	require.NoError(t, err)

	exportDir := t.TempDir()
	exportPath := filepath.Join(exportDir, "nested", "export")

	_, err = db.Exec("EXPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(exportPath, "schema.sql"))
	require.NoError(t, err, "schema.sql should exist in nested directory")
}

// TestExportEmptyDatabase tests that exporting a database with no tables works.
func TestExportEmptyDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	exportDir := t.TempDir()
	exportPath := filepath.Join(exportDir, "empty_export")

	_, err = db.Exec("EXPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	// schema.sql should exist but be empty (or contain no CREATE TABLE)
	schemaContent, err := os.ReadFile(filepath.Join(exportPath, "schema.sql"))
	require.NoError(t, err)
	assert.NotContains(t, string(schemaContent), "CREATE TABLE")
}

// TestExportImportEmptyTable tests that exporting and importing
// an empty table preserves the schema.
func TestExportImportEmptyTable(t *testing.T) {
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db1.Close() }()

	_, err = db1.Exec("CREATE TABLE empty_tbl(id INTEGER, name VARCHAR NOT NULL)")
	require.NoError(t, err)

	exportDir := t.TempDir()
	exportPath := filepath.Join(exportDir, "empty_tbl_export")

	_, err = db1.Exec("EXPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	_, err = db2.Exec("IMPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)

	// Verify the table exists and is empty
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM empty_tbl").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestParseExportDatabase tests parsing of EXPORT DATABASE syntax.
func TestParseExportDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	exportDir := t.TempDir()
	exportPath := filepath.Join(exportDir, "parse_test")

	// Basic syntax should work
	_, err = db.Exec("EXPORT DATABASE '" + exportPath + "'")
	require.NoError(t, err)
}

// TestParseImportDatabaseMissingDir tests that IMPORT DATABASE fails
// with a meaningful error when the directory does not exist.
func TestParseImportDatabaseMissingDir(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("IMPORT DATABASE '/nonexistent/path/12345'")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema.sql")
}
