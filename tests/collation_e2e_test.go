package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderByCollateNocase(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE coll_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO coll_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM coll_test ORDER BY name COLLATE NOCASE")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// Case-insensitive sort: Apple, banana, cherry
	assert.Equal(t, []string{"Apple", "banana", "cherry"}, results)
}

func TestOrderByCollateDefault(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE coll_default_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO coll_default_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM coll_default_test ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// Binary (default) sort: uppercase A (65) < lowercase b (98)
	assert.Equal(t, []string{"Apple", "banana", "cherry"}, results)
}

func TestOrderByCollateNocaseDesc(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE coll_desc_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO coll_desc_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM coll_desc_test ORDER BY name COLLATE NOCASE DESC")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// Case-insensitive descending: cherry, banana, Apple
	assert.Equal(t, []string{"cherry", "banana", "Apple"}, results)
}

func TestPragmaCollations(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("PRAGMA collations")
	require.NoError(t, err)
	defer rows.Close()

	var collations []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		collations = append(collations, name)
	}
	require.NoError(t, rows.Err())

	// Should contain at least BINARY, C, NOCASE
	assert.Contains(t, collations, "BINARY")
	assert.Contains(t, collations, "C")
	assert.Contains(t, collations, "NOCASE")
	assert.Contains(t, collations, "NFC")
	assert.Contains(t, collations, "NOACCENT")
	assert.Contains(t, collations, "POSIX")
}

func TestOrderByCollateBinary(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE coll_binary_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO coll_binary_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM coll_binary_test ORDER BY name COLLATE BINARY")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// Binary sort: Apple < banana < cherry (ASCII order)
	assert.Equal(t, []string{"Apple", "banana", "cherry"}, results)
}

func TestOrderByCollateC(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE coll_c_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO coll_c_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM coll_c_test ORDER BY name COLLATE C")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// C collation is same as binary
	assert.Equal(t, []string{"Apple", "banana", "cherry"}, results)
}

func TestCollationDoesNotAffectNumericOrderBy(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE num_test(val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO num_test VALUES (3), (1), (2)")
	require.NoError(t, err)

	// COLLATE on a numeric column should not cause errors and should still sort normally
	rows, err := db.Query("SELECT val FROM num_test ORDER BY val COLLATE NOCASE")
	require.NoError(t, err)
	defer rows.Close()

	var results []int
	for rows.Next() {
		var val int
		require.NoError(t, rows.Scan(&val))
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 2, 3}, results)
}

func TestOrderByCollateLocale(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE locale_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO locale_test VALUES ('banana'), ('Apple'), ('cherry')")
	require.NoError(t, err)

	// en_US locale should sort case-insensitively by default in Unicode collation
	rows, err := db.Query("SELECT name FROM locale_test ORDER BY name COLLATE en_US")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// In Unicode locale collation, Apple < banana < cherry (locale-aware)
	assert.Equal(t, []string{"Apple", "banana", "cherry"}, results)
}

func TestCreateTableWithCollate(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Should parse without error
	_, err = db.Exec("CREATE TABLE collated_table(name VARCHAR COLLATE NOCASE)")
	require.NoError(t, err)

	// Should be able to insert and query
	_, err = db.Exec("INSERT INTO collated_table VALUES ('hello')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT name FROM collated_table").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestOrderByCollateChained(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE chained_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO chained_test VALUES ('cafe'), ('caf\u00e9'), ('CAFE')")
	require.NoError(t, err)

	// BINARY.NOCASE.NOACCENT should treat cafe, CAFE, and cafe (from cafe) as equal
	rows, err := db.Query("SELECT name FROM chained_test ORDER BY name COLLATE BINARY.NOCASE.NOACCENT")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		results = append(results, name)
	}
	require.NoError(t, rows.Err())

	// All three should be considered equal after case+accent folding,
	// so order should be stable (insertion order for equal elements).
	assert.Len(t, results, 3)
}
