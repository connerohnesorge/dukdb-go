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
// SIMILAR TO tests
// ---------------------------------------------------------------------------

// TestSimilarToBasic verifies basic exact matching with SIMILAR TO.
func TestSimilarToBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result bool

	err = db.QueryRow("SELECT 'hello' SIMILAR TO 'hello'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'hello' SIMILAR TO 'hello' should be true")

	err = db.QueryRow("SELECT 'hello' SIMILAR TO 'world'").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result, "'hello' SIMILAR TO 'world' should be false")
}

// TestSimilarToWildcards verifies % and _ wildcard support.
func TestSimilarToWildcards(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result bool

	err = db.QueryRow("SELECT 'hello world' SIMILAR TO '%world'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'hello world' SIMILAR TO '%%world' should be true")

	err = db.QueryRow("SELECT 'abc' SIMILAR TO '_bc'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'abc' SIMILAR TO '_bc' should be true")

	err = db.QueryRow("SELECT 'abc' SIMILAR TO '__'").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result, "'abc' SIMILAR TO '__' should be false (too short)")
}

// TestSimilarToAlternation verifies pipe alternation syntax.
func TestSimilarToAlternation(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result bool

	err = db.QueryRow("SELECT 'cat' SIMILAR TO '(cat|dog)'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'cat' SIMILAR TO '(cat|dog)' should be true")

	err = db.QueryRow("SELECT 'bird' SIMILAR TO '(cat|dog)'").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result, "'bird' SIMILAR TO '(cat|dog)' should be false")
}

// TestSimilarToCharClass verifies character class patterns.
func TestSimilarToCharClass(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result bool

	err = db.QueryRow("SELECT 'a1' SIMILAR TO '[a-z][0-9]'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'a1' SIMILAR TO '[a-z][0-9]' should be true")

	err = db.QueryRow("SELECT 'A1' SIMILAR TO '[a-z][0-9]'").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result, "'A1' SIMILAR TO '[a-z][0-9]' should be false")
}

// TestNotSimilarTo verifies the NOT SIMILAR TO operator.
func TestNotSimilarTo(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result bool

	err = db.QueryRow("SELECT 'hello' NOT SIMILAR TO 'world'").Scan(&result)
	require.NoError(t, err)
	assert.True(t, result, "'hello' NOT SIMILAR TO 'world' should be true")

	err = db.QueryRow("SELECT 'hello' NOT SIMILAR TO 'hello'").Scan(&result)
	require.NoError(t, err)
	assert.False(t, result, "'hello' NOT SIMILAR TO 'hello' should be false")
}

// TestSimilarToInWhere verifies SIMILAR TO within a WHERE clause with table data.
func TestSimilarToInWhere(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE sim_test(name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO sim_test VALUES ('John'), ('Jane'), ('Bob'), ('Janet')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name FROM sim_test WHERE name SIMILAR TO 'J(ohn|ane)' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"Jane", "John"}, names)
}

// ---------------------------------------------------------------------------
// CREATE TYPE AS ENUM tests
// ---------------------------------------------------------------------------

// TestCreateTypeEnum verifies the full workflow of creating an enum type,
// using it in a table, inserting values, and querying them.
func TestCreateTypeEnum(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE people(name VARCHAR, feeling mood)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO people VALUES ('Alice', 'happy')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO people VALUES ('Bob', 'sad')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT name, feeling FROM people ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	type person struct {
		name    string
		feeling string
	}
	var people []person
	for rows.Next() {
		var p person
		err = rows.Scan(&p.name, &p.feeling)
		require.NoError(t, err)
		people = append(people, p)
	}
	require.NoError(t, rows.Err())

	require.Len(t, people, 2)
	assert.Equal(t, "Alice", people[0].name)
	assert.Equal(t, "happy", people[0].feeling)
	assert.Equal(t, "Bob", people[1].name)
	assert.Equal(t, "sad", people[1].feeling)
}

// TestDropType verifies that DROP TYPE removes a user-defined enum type.
func TestDropType(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
	require.NoError(t, err)

	_, err = db.Exec("DROP TYPE color")
	require.NoError(t, err)
}

// TestDropTypeIfExists verifies that DROP TYPE IF EXISTS does not error
// when the type does not exist.
func TestDropTypeIfExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("DROP TYPE IF EXISTS nonexistent")
	require.NoError(t, err)
}

// TestCreateTypeIfNotExists verifies that CREATE TYPE IF NOT EXISTS does not
// error when the type already exists.
func TestCreateTypeIfNotExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE status AS ENUM ('active', 'inactive')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TYPE IF NOT EXISTS status AS ENUM ('active', 'inactive')")
	require.NoError(t, err)
}
