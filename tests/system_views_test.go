package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDBSchemas(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("lists built-in schemas", func(t *testing.T) {
		rows, err := db.Query("SELECT schema_name, internal FROM duckdb_schemas() ORDER BY schema_name")
		require.NoError(t, err)
		defer rows.Close()

		var schemas []string
		for rows.Next() {
			var name string
			var internal bool
			require.NoError(t, rows.Scan(&name, &internal))
			schemas = append(schemas, name)
			assert.True(t, internal)
		}
		assert.Contains(t, schemas, "main")
		assert.Contains(t, schemas, "information_schema")
		assert.Contains(t, schemas, "pg_catalog")
	})

	t.Run("has required columns", func(t *testing.T) {
		rows, err := db.Query("SELECT database_name, schema_name, schema_oid, internal, sql FROM duckdb_schemas() LIMIT 1")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var dbName, schemaName, sqlStr string
		var oid int64
		var internal bool
		require.NoError(t, rows.Scan(&dbName, &schemaName, &oid, &internal, &sqlStr))
		assert.NotEmpty(t, dbName)
		assert.NotEmpty(t, schemaName)
	})

	t.Run("user-created schema shows up", func(t *testing.T) {
		_, err := db.Exec("CREATE SCHEMA test_schema")
		require.NoError(t, err)

		var name string
		var internal bool
		err = db.QueryRow("SELECT schema_name, internal FROM duckdb_schemas() WHERE schema_name = 'test_schema'").Scan(&name, &internal)
		require.NoError(t, err)
		assert.Equal(t, "test_schema", name)
		assert.False(t, internal)
	})
}

func TestDuckDBTypes(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("lists built-in types", func(t *testing.T) {
		rows, err := db.Query("SELECT type_name FROM duckdb_types() WHERE internal = true")
		require.NoError(t, err)
		defer rows.Close()

		var types []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			types = append(types, name)
		}
		assert.Contains(t, types, "INTEGER")
		assert.Contains(t, types, "VARCHAR")
		assert.Contains(t, types, "BOOLEAN")
		assert.Contains(t, types, "DOUBLE")
		assert.Contains(t, types, "TIMESTAMP")
	})

	t.Run("INTEGER has correct attributes", func(t *testing.T) {
		var size int64
		var category string
		err := db.QueryRow("SELECT type_size, type_category FROM duckdb_types() WHERE type_name = 'INTEGER'").Scan(&size, &category)
		require.NoError(t, err)
		assert.Equal(t, int64(4), size)
		assert.Equal(t, "NUMERIC", category)
	})

	t.Run("numeric types have NUMERIC category", func(t *testing.T) {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM duckdb_types() WHERE type_category = 'NUMERIC'").Scan(&count)
		require.NoError(t, err)
		assert.True(t, count >= 10) // at least 10 numeric types
	})

	t.Run("has required columns", func(t *testing.T) {
		rows, err := db.Query("SELECT database_name, schema_name, type_name, type_size, type_category, internal, sql FROM duckdb_types() LIMIT 1")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var dbName, schemaName, typeName, category, sqlStr string
		var size int64
		var internal bool
		require.NoError(t, rows.Scan(&dbName, &schemaName, &typeName, &size, &category, &internal, &sqlStr))
		assert.NotEmpty(t, dbName)
	})
}
