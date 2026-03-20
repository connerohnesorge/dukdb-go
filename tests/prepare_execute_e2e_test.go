package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrepareExecuteSelect verifies PREPARE with a parameter and EXECUTE with
// different values returns the correct rows.
func TestPrepareExecuteSelect(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_prep(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_prep VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	_, err = db.Exec("PREPARE q1 AS SELECT name FROM test_prep WHERE id = $1")
	require.NoError(t, err)

	// EXECUTE with id=1 should return 'alice'
	row := db.QueryRow("EXECUTE q1(1)")
	var name string
	err = row.Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name)

	// EXECUTE with id=2 should return 'bob'
	row = db.QueryRow("EXECUTE q1(2)")
	err = row.Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "bob", name)
}

// TestPrepareExecuteInsert verifies PREPARE of an INSERT statement and
// EXECUTE with multiple sets of parameters.
func TestPrepareExecuteInsert(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_ins(id INTEGER, val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("PREPARE ins AS INSERT INTO test_ins VALUES ($1, $2)")
	require.NoError(t, err)

	_, err = db.Exec("EXECUTE ins(1, 'one')")
	require.NoError(t, err)

	_, err = db.Exec("EXECUTE ins(2, 'two')")
	require.NoError(t, err)

	// Verify both rows were inserted
	rows, err := db.Query("SELECT id, val FROM test_ins ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id  int
		val string
	}
	for rows.Next() {
		var id int
		var val string
		err = rows.Scan(&id, &val)
		require.NoError(t, err)
		results = append(results, struct {
			id  int
			val string
		}{id, val})
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)
	assert.Equal(t, 1, results[0].id)
	assert.Equal(t, "one", results[0].val)
	assert.Equal(t, 2, results[1].id)
	assert.Equal(t, "two", results[1].val)
}

// TestDeallocateRemoves verifies that DEALLOCATE removes a prepared statement
// and subsequent EXECUTE fails.
func TestDeallocateRemoves(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("PREPARE q2 AS SELECT 1")
	require.NoError(t, err)

	_, err = db.Exec("DEALLOCATE q2")
	require.NoError(t, err)

	// EXECUTE after DEALLOCATE should fail
	row := db.QueryRow("EXECUTE q2")
	var result int
	err = row.Scan(&result)
	assert.Error(t, err, "EXECUTE after DEALLOCATE should return an error")
}

// TestDeallocateAll verifies that DEALLOCATE ALL removes all prepared statements.
func TestDeallocateAll(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("PREPARE a AS SELECT 1")
	require.NoError(t, err)

	_, err = db.Exec("PREPARE b AS SELECT 2")
	require.NoError(t, err)

	_, err = db.Exec("DEALLOCATE ALL")
	require.NoError(t, err)

	// Both should fail after DEALLOCATE ALL
	row := db.QueryRow("EXECUTE a")
	var result int
	err = row.Scan(&result)
	assert.Error(t, err, "EXECUTE a after DEALLOCATE ALL should return an error")

	row = db.QueryRow("EXECUTE b")
	err = row.Scan(&result)
	assert.Error(t, err, "EXECUTE b after DEALLOCATE ALL should return an error")
}

// TestDuplicatePrepareName verifies that preparing with a duplicate name
// returns an error.
func TestDuplicatePrepareName(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("PREPARE dup AS SELECT 1")
	require.NoError(t, err)

	_, err = db.Exec("PREPARE dup AS SELECT 2")
	assert.Error(t, err, "PREPARE with duplicate name should return an error")
}

// TestExecuteWrongParamCount verifies that EXECUTE with the wrong number
// of parameters returns an error.
func TestExecuteWrongParamCount(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("PREPARE q3 AS SELECT $1 + $2")
	require.NoError(t, err)

	// Only 1 param when 2 are expected
	row := db.QueryRow("EXECUTE q3(1)")
	var result int
	err = row.Scan(&result)
	assert.Error(t, err, "EXECUTE with wrong param count should return an error")
}

// TestConnectionScopedIsolation verifies that prepared statements are scoped
// to individual connections/databases and do not leak across them.
func TestConnectionScopedIsolation(t *testing.T) {
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db1.Close()

	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db2.Close()

	_, err = db1.Exec("PREPARE isolated_q AS SELECT 99")
	require.NoError(t, err)

	// Verify it works on db1
	row := db1.QueryRow("EXECUTE isolated_q")
	var result int
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 99, result)

	// Should fail on db2 since the prepared statement doesn't exist there
	row = db2.QueryRow("EXECUTE isolated_q")
	err = row.Scan(&result)
	assert.Error(t, err, "EXECUTE on different database should fail")
}

// TestPrepareExecuteNoParams verifies PREPARE and EXECUTE with zero parameters.
func TestPrepareExecuteNoParams(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("PREPARE simple AS SELECT 42")
	require.NoError(t, err)

	row := db.QueryRow("EXECUTE simple")
	var result int
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}
