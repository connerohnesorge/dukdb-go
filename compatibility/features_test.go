package compatibility

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FeatureCompatibilityTests verifies advanced features work correctly.
var FeatureCompatibilityTests = []CompatibilityTest{
	// Advanced SQL features
	{
		Name:     "Coalesce",
		Category: "feature",
		Test:     testCoalesce,
	},
	{
		Name:     "NullIf",
		Category: "feature",
		Test:     testNullIf,
	},
	{
		Name:     "Case",
		Category: "feature",
		Test:     testCase,
	},

	// String functions
	{
		Name:     "StringConcat",
		Category: "feature",
		Test:     testStringConcat,
	},
	{
		Name:     "StringLength",
		Category: "feature",
		Test:     testStringLength,
	},
	{
		Name:     "StringUpper",
		Category: "feature",
		Test:     testStringUpper,
	},
	{
		Name:     "StringLower",
		Category: "feature",
		Test:     testStringLower,
	},
	{
		Name:     "StringTrim",
		Category: "feature",
		Test:     testStringTrim,
	},
	{
		Name:     "StringSubstring",
		Category: "feature",
		Test:     testStringSubstring,
	},
	{
		Name:     "StringReplace",
		Category: "feature",
		Test:     testStringReplace,
	},

	// Numeric functions
	{
		Name:     "MathAbs",
		Category: "feature",
		Test:     testMathAbs,
	},
}

// Advanced SQL features

func testCoalesce(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT COALESCE(NULL, NULL, 'default', 'other')`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "default", result)
}

func testNullIf(t *testing.T, db *sql.DB) {
	// NULLIF returns NULL if both arguments are equal
	var result sql.NullInt64
	err := db.QueryRow(`SELECT NULLIF(5, 5)`).
		Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid)

	// NULLIF returns first argument if they differ
	err = db.QueryRow(`SELECT NULLIF(5, 10)`).
		Scan(&result)
	require.NoError(t, err)
	assert.True(t, result.Valid)
	assert.Equal(t, int64(5), result.Int64)
}

func testCase(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE case_test (val INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO case_test VALUES (1), (2), (3)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT val, CASE
			WHEN val = 1 THEN 'one'
			WHEN val = 2 THEN 'two'
			ELSE 'other'
		END as text
		FROM case_test ORDER BY val
	`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []string{"one", "two", "other"}
	var idx int
	for rows.Next() {
		var val int
		var text string
		require.NoError(t, rows.Scan(&val, &text))
		assert.Equal(t, expected[idx], text)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// String functions

func testStringConcat(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT 'Hello' || ' ' || 'World'`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", result)
}

func testStringLength(t *testing.T, db *sql.DB) {
	var length int
	err := db.QueryRow(`SELECT LENGTH('Hello')`).
		Scan(&length)
	require.NoError(t, err)
	assert.Equal(t, 5, length)
}

func testStringUpper(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT UPPER('hello')`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "HELLO", result)
}

func testStringLower(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT LOWER('HELLO')`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func testStringTrim(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT TRIM('  hello  ')`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func testStringSubstring(
	t *testing.T,
	db *sql.DB,
) {
	var result string
	err := db.QueryRow(`SELECT SUBSTRING('Hello World', 1, 5)`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello", result)
}

func testStringReplace(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT REPLACE('Hello World', 'World', 'DuckDB')`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello DuckDB", result)
}

// Numeric functions

func testMathAbs(t *testing.T, db *sql.DB) {
	var result int
	err := db.QueryRow(`SELECT ABS(-42)`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}
