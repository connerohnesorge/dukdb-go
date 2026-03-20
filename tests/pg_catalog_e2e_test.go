package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPgCatalogNamespace verifies that pg_catalog.pg_namespace lists schemas.
func TestPgCatalogNamespace(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT nspname FROM pg_catalog.pg_namespace")
	require.NoError(t, err)
	defer rows.Close()

	var schemaNames []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		schemaNames = append(schemaNames, name)
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, schemaNames, "main")
}

// TestPgCatalogTables verifies that pg_catalog.pg_tables lists created tables.
func TestPgCatalogTables(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pg_test(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'pg_test'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var tableName string
	require.NoError(t, rows.Scan(&tableName))
	assert.Equal(t, "pg_test", tableName)
	assert.False(t, rows.Next())
}

// TestPgCatalogClass verifies that pg_catalog.pg_class shows tables with correct relkind.
func TestPgCatalogClass(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pg_class_test(id INTEGER)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'pg_class_test'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var relName, relKind string
	require.NoError(t, rows.Scan(&relName, &relKind))
	assert.Equal(t, "pg_class_test", relName)
	assert.Equal(t, "r", relKind) // 'r' for regular table
	assert.False(t, rows.Next())
}

// TestPgCatalogClassView verifies that views appear in pg_class with relkind 'v'.
func TestPgCatalogClassView(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pg_vbase(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE VIEW pg_vtest AS SELECT x FROM pg_vbase")
	require.NoError(t, err)

	rows, err := db.Query("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'pg_vtest'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var relName, relKind string
	require.NoError(t, rows.Scan(&relName, &relKind))
	assert.Equal(t, "pg_vtest", relName)
	assert.Equal(t, "v", relKind)
}

// TestPgCatalogAttribute verifies that pg_catalog.pg_attribute lists column info.
func TestPgCatalogAttribute(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pg_attr_test(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT attname, attnum FROM pg_catalog.pg_attribute WHERE attnum > 0 ORDER BY attnum")
	require.NoError(t, err)
	defer rows.Close()

	type colInfo struct {
		name   string
		attnum int64
	}
	var results []colInfo
	for rows.Next() {
		var ci colInfo
		require.NoError(t, rows.Scan(&ci.name, &ci.attnum))
		results = append(results, ci)
	}
	require.NoError(t, rows.Err())

	// Should have at least 2 columns for our table
	require.GreaterOrEqual(t, len(results), 2)

	// Find our table's columns (they should be present)
	found := 0
	for _, r := range results {
		if r.name == "id" || r.name == "name" {
			found++
		}
	}
	assert.Equal(t, 2, found, "should find both columns from pg_attr_test")
}

// TestPgCatalogDatabase verifies that pg_catalog.pg_database returns the default database.
func TestPgCatalogDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT datname FROM pg_catalog.pg_database")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var datName string
	require.NoError(t, rows.Scan(&datName))
	assert.Equal(t, "memory", datName)
}

// TestPgCatalogType verifies that pg_catalog.pg_type returns built-in types.
func TestPgCatalogType(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT typname FROM pg_catalog.pg_type")
	require.NoError(t, err)
	defer rows.Close()

	var typeNames []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		typeNames = append(typeNames, name)
	}
	require.NoError(t, rows.Err())

	assert.Contains(t, typeNames, "int4")
	assert.Contains(t, typeNames, "varchar")
	assert.Contains(t, typeNames, "bool")
}

// TestPgCatalogSettings verifies that pg_catalog.pg_settings returns settings.
func TestPgCatalogSettings(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name, setting string
	require.NoError(t, rows.Scan(&name, &setting))
	assert.Equal(t, "server_version", name)
	assert.NotEmpty(t, setting)
}

// TestPgCatalogRoles verifies that pg_catalog.pg_roles returns the default user.
func TestPgCatalogRoles(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT rolname, rolsuper FROM pg_catalog.pg_roles")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var rolName string
	var rolSuper bool
	require.NoError(t, rows.Scan(&rolName, &rolSuper))
	assert.Equal(t, "dukdb", rolName)
	assert.True(t, rolSuper)
}

// TestPgCatalogViews verifies that pg_catalog.pg_views lists created views.
func TestPgCatalogViews(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pg_view_base(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE VIEW pg_view_test AS SELECT x FROM pg_view_base")
	require.NoError(t, err)

	rows, err := db.Query("SELECT viewname FROM pg_catalog.pg_views WHERE viewname = 'pg_view_test'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var viewName string
	require.NoError(t, rows.Scan(&viewName))
	assert.Equal(t, "pg_view_test", viewName)
}

// TestPgCatalogSelectStar verifies SELECT * works on pg_catalog tables.
func TestPgCatalogSelectStar(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM pg_catalog.pg_database")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Len(t, cols, 5) // pg_database has 5 columns

	require.True(t, rows.Next())
}
