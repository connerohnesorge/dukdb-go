package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUniqueConstraintColumnLevel verifies that a column-level UNIQUE constraint
// prevents inserting duplicate values.
func TestUniqueConstraintColumnLevel(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE uc_test(id INTEGER, email VARCHAR UNIQUE)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO uc_test VALUES (1, 'a@b.com')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO uc_test VALUES (2, 'a@b.com')")
	assert.Error(t, err, "inserting duplicate UNIQUE value should produce an error")
}

// TestUniqueConstraintTableLevel verifies that a table-level UNIQUE constraint
// on a composite key prevents inserting rows with identical key combinations.
func TestUniqueConstraintTableLevel(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE uc_tbl(a INTEGER, b INTEGER, UNIQUE(a, b))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO uc_tbl VALUES (1, 2)")
	require.NoError(t, err)

	// Different b value should be allowed.
	_, err = db.Exec("INSERT INTO uc_tbl VALUES (1, 3)")
	require.NoError(t, err)

	// Duplicate (a, b) pair should fail.
	_, err = db.Exec("INSERT INTO uc_tbl VALUES (1, 2)")
	assert.Error(t, err, "inserting duplicate composite UNIQUE values should produce an error")
}

// TestUniqueConstraintNullAllowed verifies that NULL values do not violate
// a UNIQUE constraint, per the SQL standard.
func TestUniqueConstraintNullAllowed(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE uc_null(id INTEGER, val VARCHAR UNIQUE)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO uc_null VALUES (1, NULL)")
	require.NoError(t, err)

	// Second NULL should also be allowed.
	_, err = db.Exec("INSERT INTO uc_null VALUES (2, NULL)")
	require.NoError(t, err)

	row := db.QueryRow("SELECT COUNT(*) FROM uc_null")
	var count int64
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "both rows with NULL should be present")
}

// TestCheckConstraintColumnLevel verifies that a column-level CHECK constraint
// rejects rows that violate the condition.
func TestCheckConstraintColumnLevel(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ck_test(id INTEGER, age INTEGER CHECK (age >= 0))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ck_test VALUES (1, 25)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ck_test VALUES (2, -1)")
	assert.Error(t, err, "inserting a value violating the CHECK constraint should produce an error")
}

// TestCheckConstraintTableLevel verifies that a table-level CHECK constraint
// referencing multiple columns rejects invalid rows.
func TestCheckConstraintTableLevel(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ck_tbl(start_date INTEGER, end_date INTEGER, CHECK (end_date > start_date))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ck_tbl VALUES (1, 5)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ck_tbl VALUES (5, 1)")
	assert.Error(t, err, "inserting a row violating the table-level CHECK constraint should produce an error")
}

// TestCheckConstraintNullPasses verifies that NULL values pass CHECK constraints,
// as required by the SQL standard (CHECK evaluates to UNKNOWN for NULL).
func TestCheckConstraintNullPasses(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ck_null(id INTEGER, val INTEGER CHECK (val > 0))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO ck_null VALUES (1, NULL)")
	require.NoError(t, err)

	row := db.QueryRow("SELECT COUNT(*) FROM ck_null")
	var count int64
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "row with NULL should pass CHECK and be present")
}

// TestCombinedConstraints verifies that PRIMARY KEY, UNIQUE, and CHECK constraints
// can coexist on the same table and all enforce their rules.
func TestCombinedConstraints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE combined(id INTEGER PRIMARY KEY, email VARCHAR UNIQUE, age INTEGER CHECK (age >= 0))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO combined VALUES (1, 'a@b.com', 25)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO combined VALUES (2, 'b@b.com', 30)")
	require.NoError(t, err)

	row := db.QueryRow("SELECT COUNT(*) FROM combined")
	var count int64
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "both valid rows should be present")
}
