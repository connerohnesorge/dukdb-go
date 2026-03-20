package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTryCastInvalidReturnsNull verifies that TRY_CAST of an invalid string
// to INTEGER returns NULL instead of raising an error.
func TestTryCastInvalidReturnsNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT TRY_CAST('abc' AS INTEGER)")
	var result sql.NullInt64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid, "TRY_CAST('abc' AS INTEGER) should return NULL")
}

// TestTryCastValidReturnsValue verifies that TRY_CAST of a valid string
// to INTEGER returns the correct integer value.
func TestTryCastValidReturnsValue(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT TRY_CAST('42' AS INTEGER)")
	var result sql.NullInt64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.True(t, result.Valid, "TRY_CAST('42' AS INTEGER) should return a valid value")
	assert.Equal(t, int64(42), result.Int64)
}

// TestDoubleColonCast verifies that the :: operator works as a shorthand
// for CAST, converting an integer to a VARCHAR string.
func TestDoubleColonCast(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT 42::VARCHAR")
	var result sql.NullString
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.True(t, result.Valid, "42::VARCHAR should return a valid value")
	assert.Equal(t, "42", result.String)
}

// TestTryCastNullInput verifies that TRY_CAST of a NULL input returns NULL.
func TestTryCastNullInput(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT TRY_CAST(NULL AS INTEGER)")
	var result sql.NullInt64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid, "TRY_CAST(NULL AS INTEGER) should return NULL")
}

// TestTryCastNested verifies that nested TRY_CAST correctly propagates NULL
// when the inner cast fails.
func TestTryCastNested(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT TRY_CAST(TRY_CAST('abc' AS INTEGER) AS VARCHAR)")
	var result sql.NullString
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid, "TRY_CAST(TRY_CAST('abc' AS INTEGER) AS VARCHAR) should return NULL")
}

// TestDoubleColonInExpression verifies that the :: cast operator works
// within arithmetic expressions.
func TestDoubleColonInExpression(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT '123'::INTEGER + 1")
	var result sql.NullInt64
	err = row.Scan(&result)
	require.NoError(t, err)
	assert.True(t, result.Valid, "'123'::INTEGER + 1 should return a valid value")
	assert.Equal(t, int64(124), result.Int64)
}
