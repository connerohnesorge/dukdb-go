package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestGroupByAllSingle(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (a TEXT, b INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t VALUES ('x', 1), ('x', 2), ('y', 3)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT a, SUM(b) FROM t GROUP BY ALL ORDER BY a")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		a string
		s int
	}
	for rows.Next() {
		var a string
		var s int
		require.NoError(t, rows.Scan(&a, &s))
		results = append(results, struct {
			a string
			s int
		}{a, s})
	}
	require.Len(t, results, 2)
	require.Equal(t, "x", results[0].a)
	require.Equal(t, 3, results[0].s)
	require.Equal(t, "y", results[1].a)
	require.Equal(t, 3, results[1].s)
}

func TestGroupByAllMultiple(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t2 (a TEXT, b TEXT, c INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t2 VALUES ('x', 'p', 1), ('x', 'p', 2), ('x', 'q', 3), ('y', 'p', 4)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT a, b, SUM(c) FROM t2 GROUP BY ALL ORDER BY a, b")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		a, b string
		s    int
	}
	for rows.Next() {
		var a, b string
		var s int
		require.NoError(t, rows.Scan(&a, &b, &s))
		results = append(results, struct {
			a, b string
			s    int
		}{a, b, s})
	}
	require.Len(t, results, 3)
	require.Equal(t, "x", results[0].a)
	require.Equal(t, "p", results[0].b)
	require.Equal(t, 3, results[0].s)
}

func TestGroupByAllOnlyAggregates(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t3 (a INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t3 VALUES (1), (2), (3)")
	require.NoError(t, err)

	// All columns are aggregates — GROUP BY ALL = no grouping columns
	var total int
	err = db.QueryRow("SELECT SUM(a) FROM t3 GROUP BY ALL").Scan(&total)
	require.NoError(t, err)
	require.Equal(t, 6, total)
}

func TestGroupByAllWithHaving(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t4 (a TEXT, b INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t4 VALUES ('x', 1), ('x', 2), ('y', 10)")
	require.NoError(t, err)

	// GROUP BY ALL with HAVING should parse and execute without error,
	// and produce the same result as explicit GROUP BY with HAVING.
	rows1, err := db.Query("SELECT a, SUM(b) FROM t4 GROUP BY ALL HAVING SUM(b) > 5 ORDER BY a")
	require.NoError(t, err)
	var results1 []string
	for rows1.Next() {
		var a string
		var s int
		require.NoError(t, rows1.Scan(&a, &s))
		results1 = append(results1, a)
	}
	require.NoError(t, rows1.Close())

	rows2, err := db.Query("SELECT a, SUM(b) FROM t4 GROUP BY a HAVING SUM(b) > 5 ORDER BY a")
	require.NoError(t, err)
	var results2 []string
	for rows2.Next() {
		var a string
		var s int
		require.NoError(t, rows2.Scan(&a, &s))
		results2 = append(results2, a)
	}
	require.NoError(t, rows2.Close())

	// Both should produce identical results
	require.Equal(t, results1, results2)
}

func TestGroupByAllEquivalent(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t5 (a TEXT, b INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t5 VALUES ('x', 1), ('x', 2), ('y', 3)")
	require.NoError(t, err)

	// GROUP BY ALL should produce same results as explicit GROUP BY
	var a1 string
	var s1 int
	err = db.QueryRow("SELECT a, SUM(b) FROM t5 GROUP BY ALL ORDER BY a LIMIT 1").Scan(&a1, &s1)
	require.NoError(t, err)

	var a2 string
	var s2 int
	err = db.QueryRow("SELECT a, SUM(b) FROM t5 GROUP BY a ORDER BY a LIMIT 1").Scan(&a2, &s2)
	require.NoError(t, err)

	require.Equal(t, a1, a2)
	require.Equal(t, s1, s2)
}

func TestGroupByAllWithCount(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t6 (category TEXT, value INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t6 VALUES ('a', 1), ('a', 2), ('b', 3)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT category, COUNT(*) FROM t6 GROUP BY ALL ORDER BY category")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		cat string
		cnt int
	}
	for rows.Next() {
		var cat string
		var cnt int
		require.NoError(t, rows.Scan(&cat, &cnt))
		results = append(results, struct {
			cat string
			cnt int
		}{cat, cnt})
	}
	require.Len(t, results, 2)
	require.Equal(t, "a", results[0].cat)
	require.Equal(t, 2, results[0].cnt)
	require.Equal(t, "b", results[1].cat)
	require.Equal(t, 1, results[1].cnt)
}
