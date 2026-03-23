package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringFunctionsRound4(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("LCASE", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT LCASE('HELLO')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("UCASE", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT UCASE('hello')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "HELLO", result)
	})

	t.Run("OCTET_LENGTH ascii", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT OCTET_LENGTH('hello')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(5), result)
	})

	t.Run("OCTET_LENGTH multibyte", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT OCTET_LENGTH('héllo')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(6), result) // é is 2 bytes in UTF-8
	})

	t.Run("OCTET_LENGTH NULL", func(t *testing.T) {
		var result sql.NullInt64
		err := db.QueryRow("SELECT OCTET_LENGTH(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("INITCAP basic", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT INITCAP('hello world')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "Hello World", result)
	})

	t.Run("INITCAP mixed case", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT INITCAP('hELLO wORLD')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "Hello World", result)
	})

	t.Run("INITCAP NULL", func(t *testing.T) {
		var result sql.NullString
		err := db.QueryRow("SELECT INITCAP(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("SOUNDEX Robert", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT SOUNDEX('Robert')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "R163", result)
	})

	t.Run("SOUNDEX Rupert matches Robert", func(t *testing.T) {
		var r1, r2 string
		err := db.QueryRow("SELECT SOUNDEX('Robert')").Scan(&r1)
		require.NoError(t, err)
		err = db.QueryRow("SELECT SOUNDEX('Rupert')").Scan(&r2)
		require.NoError(t, err)
		assert.Equal(t, r1, r2)
	})

	t.Run("SOUNDEX NULL", func(t *testing.T) {
		var result sql.NullString
		err := db.QueryRow("SELECT SOUNDEX(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("LIKE_ESCAPE basic", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT LIKE_ESCAPE('10%', '10#%', '#')").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("LIKE_ESCAPE no match", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT LIKE_ESCAPE('10x', '10#%', '#')").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("LIKE_ESCAPE with wildcard", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT LIKE_ESCAPE('hello world', 'hello%', '#')").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})
}
