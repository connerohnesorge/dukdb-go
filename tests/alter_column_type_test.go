package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterColumnType(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table with integer column
	_, err = db.Exec("CREATE TABLE test_alter_col_type (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Insert data
	_, err = db.Exec("INSERT INTO test_alter_col_type VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	// Alter column type from INTEGER to BIGINT
	_, err = db.Exec("ALTER TABLE test_alter_col_type ALTER COLUMN id TYPE BIGINT")
	require.NoError(t, err)

	// Verify data is preserved
	rows, err := db.Query("SELECT id, name FROM test_alter_col_type ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var results []struct {
		id   int64
		name string
	}
	for rows.Next() {
		var id int64
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		results = append(results, struct {
			id   int64
			name string
		}{id, name})
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "alice", results[0].name)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "bob", results[1].name)
}

func TestAlterColumnTypeSetDataType(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_sdt (val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_sdt VALUES (42)")
	require.NoError(t, err)

	// Test SET DATA TYPE syntax
	_, err = db.Exec("ALTER TABLE test_sdt ALTER COLUMN val SET DATA TYPE BIGINT")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT val FROM test_sdt").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

func TestAlterColumnTypeWithoutColumnKeyword(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_nock (val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_nock VALUES (10)")
	require.NoError(t, err)

	// Test without COLUMN keyword
	_, err = db.Exec("ALTER TABLE test_nock ALTER val TYPE BIGINT")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT val FROM test_nock").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(10), result)
}

func TestAlterColumnTypeNonexistentColumn(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_noexist (id INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE test_noexist ALTER COLUMN nonexistent TYPE BIGINT")
	require.Error(t, err)
}

func TestAlterColumnTypeIntToVarchar(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_int_to_vc (val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_int_to_vc VALUES (123)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE test_int_to_vc ALTER COLUMN val TYPE VARCHAR")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT val FROM test_int_to_vc").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "123", result)
}
