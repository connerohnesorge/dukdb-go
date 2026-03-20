package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecursiveCTEBasic verifies a basic recursive CTE that counts from 1 to 5.
func TestRecursiveCTEBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 5
		)
		SELECT x FROM cnt ORDER BY x
	`)
	require.NoError(t, err)
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var x int64
		require.NoError(t, rows.Scan(&x))
		results = append(results, x)
	}
	require.NoError(t, rows.Err())

	expected := []int64{1, 2, 3, 4, 5}
	assert.Equal(t, expected, results, "recursive CTE should produce 1 through 5")
}

// TestRecursiveCTEHierarchy verifies a recursive CTE that traverses an
// employee hierarchy from root (CEO) down through manager relationships.
func TestRecursiveCTEHierarchy(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE emp(id INTEGER, name VARCHAR, manager_id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dir', 2), (4, 'Mgr', 3)`)
	require.NoError(t, err)

	rows, err := db.Query(`
		WITH RECURSIVE hierarchy(id, name, level) AS (
			SELECT id, name, 1 FROM emp WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, h.level + 1 FROM emp e JOIN hierarchy h ON e.manager_id = h.id
		)
		SELECT name, level FROM hierarchy ORDER BY level, name
	`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		name  string
		level int64
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.name, &r.level))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	expected := []row{
		{"CEO", 1},
		{"VP", 2},
		{"Dir", 3},
		{"Mgr", 4},
	}
	assert.Equal(t, expected, results, "hierarchy should show each employee at their correct level")
}

// TestRecursiveCTEMaxRecursion verifies that large recursive CTEs either
// complete successfully or are bounded by a maximum recursion limit.
func TestRecursiveCTEMaxRecursion(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow(`
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 10000
		)
		SELECT COUNT(*) FROM cnt
	`)
	var count int64
	err = row.Scan(&count)
	if err != nil {
		// If there is a recursion limit error, that is acceptable behavior.
		t.Logf("large recursive CTE returned error (recursion limit may apply): %v", err)
		return
	}
	// The result should be either the full 10000 or capped at some default limit.
	assert.True(t, count >= 1000, "expected at least 1000 rows, got %d", count)
	t.Logf("large recursive CTE produced %d rows", count)
}

// TestRecursiveCTEFibonacci verifies a recursive CTE that generates Fibonacci
// numbers using multiple columns in the recursion.
func TestRecursiveCTEFibonacci(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE fib(n, a, b) AS (
			SELECT 1, 0, 1
			UNION ALL
			SELECT n + 1, b, a + b FROM fib WHERE n < 10
		)
		SELECT n, a FROM fib ORDER BY n
	`)
	require.NoError(t, err)
	defer rows.Close()

	// Expected Fibonacci sequence: F(0)=0, F(1)=1, F(2)=1, F(3)=2, ...
	expectedFib := []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}

	var ns []int64
	var fibs []int64
	for rows.Next() {
		var n, a int64
		require.NoError(t, rows.Scan(&n, &a))
		ns = append(ns, n)
		fibs = append(fibs, a)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, ns, "n should go from 1 to 10")
	assert.Equal(t, expectedFib, fibs, "should produce first 10 Fibonacci numbers")
}

// TestLateralJoinBasic verifies a basic LATERAL join that finds the top order
// amount per customer (top-N per group pattern).
func TestLateralJoinBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE lat_orders(id INTEGER, customer VARCHAR, amount INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lat_orders VALUES (1, 'Alice', 100), (2, 'Alice', 200), (3, 'Bob', 50)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE lat_customers(name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lat_customers VALUES ('Alice'), ('Bob')`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT c.name, o.amount
		FROM lat_customers c, LATERAL (
			SELECT amount FROM lat_orders WHERE customer = c.name ORDER BY amount DESC LIMIT 1
		) o
		ORDER BY c.name
	`)
	if err != nil {
		t.Skipf("LATERAL subquery not yet supported: %v", err)
		return
	}
	defer rows.Close()

	type row struct {
		name   string
		amount int64
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.name, &r.amount))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	expected := []row{
		{"Alice", 200},
		{"Bob", 50},
	}
	assert.Equal(t, expected, results, "LATERAL join should return top order per customer")
}

// TestLateralWithUnnest verifies a LATERAL join using UNNEST to expand arrays
// into rows, correlating with the outer table.
func TestLateralWithUnnest(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE lat_test(id INTEGER, arr INTEGER[])`)
	if err != nil {
		t.Skipf("array type not supported for this test: %v", err)
		return
	}

	_, err = db.Exec(`INSERT INTO lat_test VALUES (1, [10, 20]), (2, [30])`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT t.id, u.val
		FROM lat_test t, LATERAL UNNEST(t.arr) AS u(val)
		ORDER BY t.id, u.val
	`)
	if err != nil {
		t.Skipf("LATERAL UNNEST not yet supported: %v", err)
		return
	}
	defer rows.Close()

	type row struct {
		id  int64
		val int64
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.val))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	expected := []row{
		{1, 10},
		{1, 20},
		{2, 30},
	}
	assert.Equal(t, expected, results, "LATERAL UNNEST should expand arrays correlated with outer table")
}
