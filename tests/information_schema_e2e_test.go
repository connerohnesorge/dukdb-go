package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInformationSchemaTables verifies that information_schema.tables lists tables and views.
func TestInformationSchemaTables(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE is_test(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'is_test'")
	require.NoError(t, err)
	defer rows.Close()

	var tableName, tableType string
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&tableName, &tableType))
	assert.Equal(t, "is_test", tableName)
	assert.Equal(t, "BASE TABLE", tableType)
	assert.False(t, rows.Next())
}

// TestInformationSchemaTablesWithViews verifies views appear in information_schema.tables.
func TestInformationSchemaTablesWithViews(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE base_t(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE VIEW v_test AS SELECT x FROM base_t")
	require.NoError(t, err)

	rows, err := db.Query("SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'v_test'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var tableName, tableType string
	require.NoError(t, rows.Scan(&tableName, &tableType))
	assert.Equal(t, "v_test", tableName)
	assert.Equal(t, "VIEW", tableType)
}

// TestInformationSchemaColumns verifies column metadata is reported correctly.
func TestInformationSchemaColumns(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE is_col_test(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'is_col_test' ORDER BY ordinal_position")
	require.NoError(t, err)
	defer rows.Close()

	type colInfo struct {
		name     string
		dataType string
		ordinal  int64
	}
	var results []colInfo
	for rows.Next() {
		var ci colInfo
		require.NoError(t, rows.Scan(&ci.name, &ci.dataType, &ci.ordinal))
		results = append(results, ci)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "id", results[0].name)
	assert.Equal(t, "INTEGER", results[0].dataType)
	assert.Equal(t, int64(1), results[0].ordinal)
	assert.Equal(t, "name", results[1].name)
	assert.Equal(t, "VARCHAR", results[1].dataType)
	assert.Equal(t, int64(2), results[1].ordinal)
}

// TestInformationSchemaSchemata verifies schema listing includes "main".
func TestInformationSchemaSchemata(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT schema_name FROM information_schema.schemata")
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

// TestInformationSchemaViews verifies the views metadata view.
func TestInformationSchemaViews(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE vt(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE VIEW is_view AS SELECT x FROM vt")
	require.NoError(t, err)

	rows, err := db.Query("SELECT table_name FROM information_schema.views WHERE table_name = 'is_view'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var viewName string
	require.NoError(t, rows.Scan(&viewName))
	assert.Equal(t, "is_view", viewName)
	assert.False(t, rows.Next())
}

// TestInformationSchemaSelectStar verifies SELECT * works on information_schema tables.
func TestInformationSchemaSelectStar(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE star_test(a INTEGER)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT * FROM information_schema.tables WHERE table_name = 'star_test'")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Len(t, cols, 12) // information_schema.tables has 12 columns

	require.True(t, rows.Next())
}

// TestInformationSchemaTableConstraints verifies constraint listing.
func TestInformationSchemaTableConstraints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE pk_test(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT constraint_type, table_name FROM information_schema.table_constraints WHERE table_name = 'pk_test'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var constraintType, tableName string
	require.NoError(t, rows.Scan(&constraintType, &tableName))
	assert.Equal(t, "PRIMARY KEY", constraintType)
	assert.Equal(t, "pk_test", tableName)
}
