package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstallExtension(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// INSTALL is a no-op for compiled-in extensions
	_, err = db.Exec("INSTALL json")
	require.NoError(t, err)

	// INSTALL of unknown extension is also a no-op (no error)
	_, err = db.Exec("INSTALL nonexistent")
	require.NoError(t, err)
}

func TestLoadExtension(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// LOAD activates a registered extension
	_, err = db.Exec("LOAD json")
	require.NoError(t, err)

	// LOAD again is a no-op (idempotent)
	_, err = db.Exec("LOAD json")
	require.NoError(t, err)
}

func TestLoadUnknownExtension(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// LOAD of unknown extension should error
	_, err = db.Exec("LOAD nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDuckDBExtensions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Query the duckdb_extensions() table function
	rows, err := db.Query(
		"SELECT extension_name, installed, loaded, description FROM duckdb_extensions() ORDER BY extension_name",
	)
	require.NoError(t, err)
	defer rows.Close()

	type extRow struct {
		Name        string
		Installed   bool
		Loaded      bool
		Description string
	}

	var extensions []extRow
	for rows.Next() {
		var ext extRow
		err := rows.Scan(&ext.Name, &ext.Installed, &ext.Loaded, &ext.Description)
		require.NoError(t, err)
		extensions = append(extensions, ext)
	}
	require.NoError(t, rows.Err())

	// Should have csv, icu, json, parquet (sorted by name)
	require.Len(t, extensions, 4)

	assert.Equal(t, "csv", extensions[0].Name)
	assert.True(t, extensions[0].Installed)
	assert.False(t, extensions[0].Loaded)

	assert.Equal(t, "icu", extensions[1].Name)
	assert.True(t, extensions[1].Installed)
	assert.False(t, extensions[1].Loaded)

	assert.Equal(t, "json", extensions[2].Name)
	assert.True(t, extensions[2].Installed)
	assert.False(t, extensions[2].Loaded)

	assert.Equal(t, "parquet", extensions[3].Name)
	assert.True(t, extensions[3].Installed)
	assert.False(t, extensions[3].Loaded)
}

func TestDuckDBExtensionsAfterLoad(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Load the json extension
	_, err = db.Exec("LOAD json")
	require.NoError(t, err)

	// Query extensions - json should now show as loaded
	var loaded bool
	err = db.QueryRow(
		"SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'json'",
	).Scan(&loaded)
	require.NoError(t, err)
	assert.True(t, loaded)

	// csv should still be not loaded
	err = db.QueryRow(
		"SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'csv'",
	).Scan(&loaded)
	require.NoError(t, err)
	assert.False(t, loaded)
}

func TestInstallLoadWithStringLiteral(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// INSTALL and LOAD should also work with string literals
	_, err = db.Exec("INSTALL 'json'")
	require.NoError(t, err)

	_, err = db.Exec("LOAD 'parquet'")
	require.NoError(t, err)
}
