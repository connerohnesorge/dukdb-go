package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScalarMacroBasic tests basic scalar macro creation and invocation.
func TestScalarMacroBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO add(a, b) AS a + b")
	require.NoError(t, err)

	row := db.QueryRow("SELECT add(2, 3)")
	var result int64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(5), result)
}

// TestScalarMacroInWhere tests using a scalar macro in a WHERE clause.
func TestScalarMacroInWhere(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE macro_test(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO macro_test VALUES (1), (2), (3), (4), (5)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE MACRO double(n) AS n * 2")
	require.NoError(t, err)

	rows, err := db.Query("SELECT x FROM macro_test WHERE double(x) > 6 ORDER BY x")
	require.NoError(t, err)
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var x int64
		require.NoError(t, rows.Scan(&x))
		results = append(results, x)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{4, 5}, results)
}

// TestMacroWithDefault tests macros with default parameter values.
func TestMacroWithDefault(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO greet(name, greeting := 'Hello') AS greeting || ', ' || name || '!'")
	require.NoError(t, err)

	// Test with default
	row := db.QueryRow("SELECT greet('World')")
	var result string
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World!", result)

	// Test with explicit value
	row = db.QueryRow("SELECT greet('World', 'Hi')")
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hi, World!", result)
}

// TestCreateOrReplaceMacro tests CREATE OR REPLACE MACRO to overwrite an existing macro.
func TestCreateOrReplaceMacro(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO inc(x) AS x + 1")
	require.NoError(t, err)

	row := db.QueryRow("SELECT inc(5)")
	var result int64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(6), result)

	_, err = db.Exec("CREATE OR REPLACE MACRO inc(x) AS x + 10")
	require.NoError(t, err)

	row = db.QueryRow("SELECT inc(5)")
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

// TestDropMacro tests dropping a macro and verifying it is removed.
func TestDropMacro(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO tmp(x) AS x")
	require.NoError(t, err)

	// Verify the macro works
	row := db.QueryRow("SELECT tmp(42)")
	var result int64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result)

	// Drop the macro
	_, err = db.Exec("DROP MACRO tmp")
	require.NoError(t, err)

	// Verify the macro no longer works (should error)
	_, err = db.Query("SELECT tmp(42)")
	assert.Error(t, err)
}

// TestDropMacroIfExists tests that DROP MACRO IF EXISTS does not error
// when the macro does not exist.
func TestDropMacroIfExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("DROP MACRO IF EXISTS nonexistent")
	assert.NoError(t, err)
}

// TestMacroWrongArgCount tests that calling a macro with the wrong number
// of arguments produces an error.
func TestMacroWrongArgCount(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO pair(a, b) AS a + b")
	require.NoError(t, err)

	// Too few arguments
	_, err = db.Query("SELECT pair(1)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "requires at least 2")

	// Too many arguments
	_, err = db.Query("SELECT pair(1, 2, 3)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "accepts at most 2")
}

// TestTableMacroBasic tests basic table macro creation and invocation.
func TestTableMacroBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE employees(id INTEGER, name VARCHAR, dept VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'eng'), (3, 'Carol', 'sales')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE MACRO dept_employees(d) AS TABLE SELECT id, name FROM employees WHERE dept = d")
	require.NoError(t, err)

	rows, err := db.Query("SELECT * FROM dept_employees('eng') ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id   int64
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []row{{1, "Alice"}, {2, "Bob"}}, results)
}

// TestScalarMacroNested tests a macro calling another macro.
func TestScalarMacroNested(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO add1(x) AS x + 1")
	require.NoError(t, err)
	_, err = db.Exec("CREATE MACRO add2(x) AS add1(add1(x))")
	require.NoError(t, err)

	row := db.QueryRow("SELECT add2(10)")
	var result int64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(12), result)
}

// TestScalarMacroWithCaseExpr tests a macro containing a CASE expression.
func TestScalarMacroWithCaseExpr(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE MACRO sign(x) AS CASE WHEN x > 0 THEN 1 WHEN x < 0 THEN -1 ELSE 0 END")
	require.NoError(t, err)

	row := db.QueryRow("SELECT sign(5)")
	var result int64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)

	row = db.QueryRow("SELECT sign(-3)")
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), result)

	row = db.QueryRow("SELECT sign(0)")
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result)
}
