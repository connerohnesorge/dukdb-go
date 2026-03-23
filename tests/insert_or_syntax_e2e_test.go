package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInsertOrIgnoreSkipsConflict verifies that INSERT OR IGNORE silently
// skips rows that conflict on the primary key.
func TestInsertOrIgnoreSkipsConflict(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_ignore(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_ignore VALUES (1, 'alice')")
	require.NoError(t, err)

	// Conflicting row should be silently ignored
	_, err = db.Exec("INSERT OR IGNORE INTO ior_ignore VALUES (1, 'bob')")
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM ior_ignore WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name, "INSERT OR IGNORE should keep the original row")
}

// TestInsertOrIgnoreInsertsNonConflicting verifies that non-conflicting rows
// are inserted normally with INSERT OR IGNORE.
func TestInsertOrIgnoreInsertsNonConflicting(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_ignore2(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_ignore2 VALUES (1, 'alice')")
	require.NoError(t, err)

	// Non-conflicting row should be inserted
	_, err = db.Exec("INSERT OR IGNORE INTO ior_ignore2 VALUES (2, 'bob')")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM ior_ignore2").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestInsertOrReplaceUpdatesConflicting verifies that INSERT OR REPLACE
// replaces conflicting rows with the new values.
func TestInsertOrReplaceUpdatesConflicting(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_replace(id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_replace VALUES (1, 'alice', 30)")
	require.NoError(t, err)

	// Conflicting row should be replaced
	_, err = db.Exec("INSERT OR REPLACE INTO ior_replace VALUES (1, 'bob', 25)")
	require.NoError(t, err)

	var name string
	var age int
	err = db.QueryRow("SELECT name, age FROM ior_replace WHERE id = 1").Scan(&name, &age)
	require.NoError(t, err)
	assert.Equal(t, "bob", name, "INSERT OR REPLACE should update the name")
	assert.Equal(t, 25, age, "INSERT OR REPLACE should update the age")
}

// TestInsertOrReplaceInsertsNonConflicting verifies that non-conflicting rows
// are inserted normally with INSERT OR REPLACE.
func TestInsertOrReplaceInsertsNonConflicting(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_replace2(id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_replace2 VALUES (1, 10)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT OR REPLACE INTO ior_replace2 VALUES (2, 20)")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM ior_replace2").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestInsertOrReplaceWithMultipleRows verifies INSERT OR REPLACE handles
// a mix of conflicting and non-conflicting rows in a single statement.
func TestInsertOrReplaceWithMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_multi(id INTEGER PRIMARY KEY, val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_multi VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)

	// Row 1 conflicts, row 3 is new
	_, err = db.Exec("INSERT OR REPLACE INTO ior_multi VALUES (1, 'x'), (3, 'c')")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM ior_multi").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	var val string
	err = db.QueryRow("SELECT val FROM ior_multi WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "x", val, "row 1 should be replaced")
}

// TestInsertOrWithOnConflictErrors verifies that combining INSERT OR REPLACE
// with an explicit ON CONFLICT clause produces a parse error.
func TestInsertOrWithOnConflictErrors(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_err(id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT OR REPLACE INTO ior_err VALUES (1, 10) ON CONFLICT DO NOTHING")
	assert.Error(t, err, "INSERT OR REPLACE with ON CONFLICT should error")
}

// TestExistingOnConflictStillWorks verifies that the existing ON CONFLICT
// syntax continues to work correctly after adding INSERT OR support.
func TestExistingOnConflictStillWorks(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE oc_compat(id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO oc_compat VALUES (1, 10)")
	require.NoError(t, err)

	// Existing DO NOTHING syntax
	_, err = db.Exec("INSERT INTO oc_compat VALUES (1, 20) ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	var val int
	err = db.QueryRow("SELECT val FROM oc_compat WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 10, val)

	// Existing DO UPDATE SET syntax
	_, err = db.Exec("INSERT INTO oc_compat VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val")
	require.NoError(t, err)

	err = db.QueryRow("SELECT val FROM oc_compat WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 30, val)
}

// TestInsertOrIgnoreWithUniqueConstraint verifies INSERT OR IGNORE works
// with UNIQUE constraints (not just PRIMARY KEY).
func TestInsertOrIgnoreWithUniqueConstraint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_uniq(id INTEGER PRIMARY KEY, email VARCHAR UNIQUE)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ior_uniq VALUES (1, 'a@b.com')")
	require.NoError(t, err)

	// Conflict on primary key
	_, err = db.Exec("INSERT OR IGNORE INTO ior_uniq VALUES (1, 'c@d.com')")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM ior_uniq").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "conflicting row should be ignored")
}

// TestInsertOrInvalidAction verifies that INSERT OR with an invalid action
// (not REPLACE or IGNORE) produces a parse error.
func TestInsertOrInvalidAction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ior_bad(id INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT OR ABORT INTO ior_bad VALUES (1)")
	assert.Error(t, err, "INSERT OR ABORT should produce a parse error")
}
