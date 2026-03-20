package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNaturalJoin verifies NATURAL JOIN auto-matches on common column names.
func TestNaturalJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE j_left(id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE j_right(id INTEGER, dept VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO j_left VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO j_right VALUES (1, 'Eng'), (2, 'Sales')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT j_left.id, j_left.name, j_right.dept FROM j_left NATURAL JOIN j_right")
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		id   int
		name string
		dept string
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.id, &r.name, &r.dept)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// Results should match on id column
	found := map[int]result{}
	for _, r := range results {
		found[r.id] = r
	}
	assert.Equal(t, "Alice", found[1].name)
	assert.Equal(t, "Eng", found[1].dept)
	assert.Equal(t, "Bob", found[2].name)
	assert.Equal(t, "Sales", found[2].dept)
}

// TestNaturalLeftJoin verifies NATURAL LEFT JOIN with non-matching rows.
func TestNaturalLeftJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nl_left(id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE nl_right(id INTEGER, dept VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO nl_left VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO nl_right VALUES (1, 'Eng'), (2, 'Sales')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT nl_left.id, nl_left.name, nl_right.dept FROM nl_left NATURAL LEFT JOIN nl_right")
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		id   int
		name string
		dept sql.NullString
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.id, &r.name, &r.dept)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 3)

	found := map[int]result{}
	for _, r := range results {
		found[r.id] = r
	}
	assert.Equal(t, "Alice", found[1].name)
	assert.True(t, found[1].dept.Valid)
	assert.Equal(t, "Eng", found[1].dept.String)
	assert.Equal(t, "Bob", found[2].name)
	assert.True(t, found[2].dept.Valid)
	assert.Equal(t, "Sales", found[2].dept.String)
	assert.Equal(t, "Charlie", found[3].name)
	assert.False(t, found[3].dept.Valid) // NULL for unmatched
}

// TestPositionalJoin verifies POSITIONAL JOIN matches rows by position.
func TestPositionalJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE p_left(a INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE p_right(b VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO p_left VALUES (1), (2), (3)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO p_right VALUES ('x'), ('y')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT p_left.a, p_right.b FROM p_left POSITIONAL JOIN p_right")
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		a int
		b sql.NullString
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.a, &r.b)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 3)

	assert.Equal(t, 1, results[0].a)
	assert.True(t, results[0].b.Valid)
	assert.Equal(t, "x", results[0].b.String)

	assert.Equal(t, 2, results[1].a)
	assert.True(t, results[1].b.Valid)
	assert.Equal(t, "y", results[1].b.String)

	assert.Equal(t, 3, results[2].a)
	assert.False(t, results[2].b.Valid) // NULL for missing right row
}

// TestAsOfJoin verifies ASOF JOIN finds the nearest match.
func TestAsOfJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE trades(ticker VARCHAR, ts INTEGER, price INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE quotes(ticker VARCHAR, ts INTEGER, bid INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO trades VALUES ('AAPL', 10, 100), ('AAPL', 20, 102)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO quotes VALUES ('AAPL', 8, 99), ('AAPL', 12, 101), ('AAPL', 18, 103)")
	require.NoError(t, err)

	rows, err := db.Query(
		"SELECT t.ts, t.price, q.bid FROM trades t ASOF JOIN quotes q ON t.ticker = q.ticker AND t.ts >= q.ts",
	)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		ts    int
		price int
		bid   int
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.ts, &r.price, &r.bid)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// ts=10: quotes at 8 (99) satisfies 10 >= 8, and 12 does not (10 >= 12 is false)
	// So best match is quote at ts=8 with bid=99
	// BUT with ASOF semantics, we pick the LAST matching row.
	// If quotes are in insertion order (8, 12, 18):
	// For ts=10: 8 matches (10>=8), 12 doesn't (10>=12 false), 18 doesn't -> best = 8, bid=99
	assert.Equal(t, 10, results[0].ts)
	assert.Equal(t, 100, results[0].price)
	assert.Equal(t, 99, results[0].bid)

	// ts=20: 8 matches (20>=8), 12 matches (20>=12), 18 matches (20>=18) -> best = 18, bid=103
	assert.Equal(t, 20, results[1].ts)
	assert.Equal(t, 102, results[1].price)
	assert.Equal(t, 103, results[1].bid)
}

// TestAsOfLeftJoin verifies ASOF LEFT JOIN with non-matching left rows.
func TestAsOfLeftJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE tl_trades(ticker VARCHAR, ts INTEGER, price INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE tl_quotes(ticker VARCHAR, ts INTEGER, bid INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO tl_trades VALUES ('AAPL', 10, 100), ('MSFT', 10, 200)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO tl_quotes VALUES ('AAPL', 8, 99)")
	require.NoError(t, err)

	rows, err := db.Query(
		"SELECT t.ticker, t.ts, t.price, q.bid FROM tl_trades t ASOF LEFT JOIN tl_quotes q ON t.ticker = q.ticker AND t.ts >= q.ts",
	)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		ticker string
		ts     int
		price  int
		bid    sql.NullInt64
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.ticker, &r.ts, &r.price, &r.bid)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	found := map[string]result{}
	for _, r := range results {
		found[r.ticker] = r
	}

	// AAPL has a matching quote
	assert.True(t, found["AAPL"].bid.Valid)
	assert.Equal(t, int64(99), found["AAPL"].bid.Int64)

	// MSFT has no matching quote -> NULL
	assert.False(t, found["MSFT"].bid.Valid)
}
