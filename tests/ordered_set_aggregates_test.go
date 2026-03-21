package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithinGroupPercentileCont(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE vals (val INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO vals VALUES (1), (2), (3), (4)")
	require.NoError(t, err)

	var result float64
	err = db.QueryRow("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM vals").Scan(&result)
	require.NoError(t, err)
	assert.InDelta(t, 2.5, result, 0.01)
}

func TestWithinGroupMode(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE vals (val INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO vals VALUES (1), (1), (2), (3)")
	require.NoError(t, err)

	var result int
	err = db.QueryRow("SELECT MODE() WITHIN GROUP (ORDER BY val) FROM vals").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestListaggWithDelimiter(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE names (name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO names VALUES ('Charlie'), ('Alice'), ('Bob')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM names").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Alice, Bob, Charlie", result)
}

func TestListaggDefaultDelimiter(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE vals (value VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO vals VALUES ('a'), ('b'), ('c')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT LISTAGG(value) WITHIN GROUP (ORDER BY value) FROM vals").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "abc", result)
}

func TestListaggNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE vals (value VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO vals VALUES ('a'), (NULL), ('c')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT LISTAGG(value, ',') WITHIN GROUP (ORDER BY value) FROM vals").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "a,c", result)
}

func TestListaggEmpty(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE names (name VARCHAR)")
	require.NoError(t, err)

	var result sql.NullString
	err = db.QueryRow("SELECT LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM names").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid, "expected NULL for empty table LISTAGG")
}

func TestListaggWithGroupBy(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE employees (dept VARCHAR, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO employees VALUES ('eng', 'Charlie'), ('eng', 'Alice'), ('sales', 'Bob'), ('sales', 'Diana')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT dept, LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) AS names FROM employees GROUP BY dept ORDER BY dept")
	require.NoError(t, err)
	defer rows.Close()

	results := make(map[string]string)
	for rows.Next() {
		var dept, names string
		require.NoError(t, rows.Scan(&dept, &names))
		results[dept] = names
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, "Alice, Charlie", results["eng"])
	assert.Equal(t, "Bob, Diana", results["sales"])
}

func TestWithinGroupConflictError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE names (id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO names VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)

	// Using both internal ORDER BY and WITHIN GROUP should error
	var result string
	err = db.QueryRow("SELECT STRING_AGG(name, ',' ORDER BY name) WITHIN GROUP (ORDER BY id) FROM names").Scan(&result)
	require.Error(t, err, "expected error when using both internal ORDER BY and WITHIN GROUP")
	assert.Contains(t, err.Error(), "cannot use both internal ORDER BY and WITHIN GROUP")
}
