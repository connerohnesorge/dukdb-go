package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestGeneratedColumnBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE orders (
		price DOUBLE,
		quantity INTEGER,
		total DOUBLE GENERATED ALWAYS AS (price * quantity) STORED
	)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO orders (price, quantity) VALUES (10.0, 3)")
	require.NoError(t, err)

	var total float64
	err = db.QueryRow("SELECT total FROM orders").Scan(&total)
	require.NoError(t, err)
	require.Equal(t, 30.0, total)
}

func TestGeneratedColumnConcat(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE people (
		first_name VARCHAR,
		last_name VARCHAR,
		full_name VARCHAR GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
	)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO people (first_name, last_name) VALUES ('John', 'Doe')")
	require.NoError(t, err)

	var fullName string
	err = db.QueryRow("SELECT full_name FROM people").Scan(&fullName)
	require.NoError(t, err)
	require.Equal(t, "John Doe", fullName)
}

func TestGeneratedColumnSelectStar(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t1 (a, b) VALUES (10, 20)")
	require.NoError(t, err)

	var a, b, c int
	err = db.QueryRow("SELECT * FROM t1").Scan(&a, &b, &c)
	require.NoError(t, err)
	require.Equal(t, 10, a)
	require.Equal(t, 20, b)
	require.Equal(t, 30, c)
}

func TestGeneratedColumnRejectInsert(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t2 (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	require.NoError(t, err)

	// Explicit value for generated column should fail
	_, err = db.Exec("INSERT INTO t2 (a, b) VALUES (5, 99)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "generated")
}

func TestGeneratedColumnUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t3 (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t3 (a) VALUES (5)")
	require.NoError(t, err)

	var b int
	err = db.QueryRow("SELECT b FROM t3").Scan(&b)
	require.NoError(t, err)
	require.Equal(t, 10, b)

	// Update base column should recompute generated column
	_, err = db.Exec("UPDATE t3 SET a = 7")
	require.NoError(t, err)

	err = db.QueryRow("SELECT b FROM t3").Scan(&b)
	require.NoError(t, err)
	require.Equal(t, 14, b)
}

func TestGeneratedColumnRejectUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t4 (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t4 (a) VALUES (5)")
	require.NoError(t, err)

	// Direct update of generated column should fail
	_, err = db.Exec("UPDATE t4 SET b = 99")
	require.Error(t, err)
	require.Contains(t, err.Error(), "generated")
}

func TestGeneratedColumnWithFunction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t5 (name VARCHAR, upper_name VARCHAR GENERATED ALWAYS AS (UPPER(name)) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t5 (name) VALUES ('hello')")
	require.NoError(t, err)

	var upper string
	err = db.QueryRow("SELECT upper_name FROM t5").Scan(&upper)
	require.NoError(t, err)
	require.Equal(t, "HELLO", upper)
}

func TestGeneratedColumnWhereClause(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t6 (a INTEGER, doubled INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t6 (a) VALUES (1), (2), (3), (4), (5)")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE doubled > 6").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count) // a=4 (doubled=8) and a=5 (doubled=10)
}

func TestGeneratedColumnShorthandSyntax(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t7 (a INTEGER, b INTEGER AS (a + 1) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t7 (a) VALUES (10)")
	require.NoError(t, err)

	var b int
	err = db.QueryRow("SELECT b FROM t7").Scan(&b)
	require.NoError(t, err)
	require.Equal(t, 11, b)
}

func TestGeneratedColumnMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t8 (x INTEGER, y INTEGER, sum_xy INTEGER GENERATED ALWAYS AS (x + y) STORED)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t8 (x, y) VALUES (1, 2), (3, 4), (5, 6)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT sum_xy FROM t8 ORDER BY x")
	require.NoError(t, err)
	defer rows.Close()

	expected := []int{3, 7, 11}
	i := 0
	for rows.Next() {
		var s int
		require.NoError(t, rows.Scan(&s))
		require.Equal(t, expected[i], s)
		i++
	}
	require.Equal(t, 3, i)
}
