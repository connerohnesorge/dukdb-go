package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SHA1
// ---------------------------------------------------------------------------

func TestSHA1KnownValue(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SHA1('hello')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", result)
}

func TestSHA1Null(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result *string
	err = db.QueryRow("SELECT SHA1(NULL)").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result, "SHA1(NULL) should return NULL")
}

func TestSHA1EmptyString(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SHA1('')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "da39a3ee5e6b4b0d3255bfef95601890afd80709", result)
}

// ---------------------------------------------------------------------------
// SETSEED
// ---------------------------------------------------------------------------

func TestSetseedDeterministic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// First: set seed and get a random value.
	_, err = db.Exec("SELECT SETSEED(0.5)")
	require.NoError(t, err)

	var r1 float64
	err = db.QueryRow("SELECT RANDOM()").Scan(&r1)
	require.NoError(t, err)

	// Second: reset the same seed and get another random value.
	_, err = db.Exec("SELECT SETSEED(0.5)")
	require.NoError(t, err)

	var r2 float64
	err = db.QueryRow("SELECT RANDOM()").Scan(&r2)
	require.NoError(t, err)

	assert.Equal(t, r1, r2, "SETSEED(0.5) followed by RANDOM() should be deterministic")
}

func TestSetseedRangeError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("SELECT SETSEED(2.0)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "between 0 and 1")
}

// ---------------------------------------------------------------------------
// LIST_VALUE / LIST_PACK
// ---------------------------------------------------------------------------

func TestListValue(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT LIST_VALUE(1, 2, 3)").Scan(&result)
	require.NoError(t, err)

	list, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T", result)
	assert.Len(t, list, 3)
}

func TestListValueEmpty(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT LIST_VALUE()").Scan(&result)
	require.NoError(t, err)

	list, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T", result)
	assert.Empty(t, list)
}

func TestListPack(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT LIST_PACK(10, 20)").Scan(&result)
	require.NoError(t, err)

	list, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T", result)
	assert.Len(t, list, 2)
}

// ---------------------------------------------------------------------------
// ANY_VALUE
// ---------------------------------------------------------------------------

func TestAnyValue(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_any(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_any VALUES (1), (2), (3)")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT ANY_VALUE(x) FROM t_any").Scan(&result)
	require.NoError(t, err)
	assert.Contains(t, []int64{1, 2, 3}, result, "ANY_VALUE should return one of the values in the table")
}

func TestAnyValueAllNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_any_null(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_any_null VALUES (NULL), (NULL), (NULL)")
	require.NoError(t, err)

	var result *int64
	err = db.QueryRow("SELECT ANY_VALUE(x) FROM t_any_null").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result, "ANY_VALUE over all NULLs should return NULL")
}

// ---------------------------------------------------------------------------
// HISTOGRAM
// ---------------------------------------------------------------------------

func TestHistogram(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_hist(x VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_hist VALUES ('a'), ('a'), ('b'), ('c')")
	require.NoError(t, err)

	var result any
	err = db.QueryRow("SELECT HISTOGRAM(x) FROM t_hist").Scan(&result)
	require.NoError(t, err)

	m, ok := result.(map[string]any)
	require.True(t, ok, "expected map[string]any, got %T: %v", result, result)

	assert.Equal(t, int64(2), m["a"], "histogram['a'] should be 2")
	assert.Equal(t, int64(1), m["b"], "histogram['b'] should be 1")
	assert.Equal(t, int64(1), m["c"], "histogram['c'] should be 1")
}

func TestHistogramEmpty(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_hist_empty(x VARCHAR)")
	require.NoError(t, err)

	var result any
	err = db.QueryRow("SELECT HISTOGRAM(x) FROM t_hist_empty").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result, "HISTOGRAM over empty set should return NULL")
}

// ---------------------------------------------------------------------------
// ARG_MIN / ARG_MAX aliases
// ---------------------------------------------------------------------------

func TestArgMinAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_argmin(name VARCHAR, age INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_argmin VALUES ('Alice', 30), ('Bob', 20), ('Carol', 25)")
	require.NoError(t, err)

	var r1 string
	err = db.QueryRow("SELECT ARG_MIN(name, age) FROM t_argmin").Scan(&r1)
	require.NoError(t, err)

	var r2 string
	err = db.QueryRow("SELECT ARGMIN(name, age) FROM t_argmin").Scan(&r2)
	require.NoError(t, err)

	assert.Equal(t, r1, r2, "ARG_MIN and ARGMIN should return the same result")
	assert.Equal(t, "Bob", r1, "ARG_MIN(name, age) should return 'Bob' (youngest)")
}

func TestArgMaxAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t_argmax(name VARCHAR, age INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_argmax VALUES ('Alice', 30), ('Bob', 20), ('Carol', 25)")
	require.NoError(t, err)

	var r1 string
	err = db.QueryRow("SELECT ARG_MAX(name, age) FROM t_argmax").Scan(&r1)
	require.NoError(t, err)

	var r2 string
	err = db.QueryRow("SELECT ARGMAX(name, age) FROM t_argmax").Scan(&r2)
	require.NoError(t, err)

	assert.Equal(t, r1, r2, "ARG_MAX and ARGMAX should return the same result")
	assert.Equal(t, "Alice", r1, "ARG_MAX(name, age) should return 'Alice' (oldest)")
}
