package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUtilityFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// System functions
	t.Run("CURRENT_DATABASE", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "memory", result)
	})

	t.Run("CURRENT_SCHEMA", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT CURRENT_SCHEMA()").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "main", result)
	})

	t.Run("VERSION", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT VERSION()").Scan(&result)
		require.NoError(t, err)
		assert.Contains(t, result, "v1.4.3")
	})

	// Date/time functions
	t.Run("DAYNAME", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT DAYNAME(CAST('2024-01-15' AS DATE))").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "Monday", result)
	})

	t.Run("DAYNAME NULL", func(t *testing.T) {
		var result *string
		err := db.QueryRow("SELECT DAYNAME(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MONTHNAME", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT MONTHNAME(CAST('2024-03-15' AS DATE))").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "March", result)
	})

	t.Run("MONTHNAME NULL", func(t *testing.T) {
		var result *string
		err := db.QueryRow("SELECT MONTHNAME(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("YEARWEEK", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT YEARWEEK(CAST('2024-01-15' AS DATE))").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(202403), result)
	})

	t.Run("YEARWEEK NULL", func(t *testing.T) {
		var result *int64
		err := db.QueryRow("SELECT YEARWEEK(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("EPOCH_US", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT EPOCH_US(CAST('1970-01-01 00:00:01' AS TIMESTAMP))").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(1000000), result)
	})

	t.Run("EPOCH_US NULL", func(t *testing.T) {
		var result *int64
		err := db.QueryRow("SELECT EPOCH_US(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// String functions
	t.Run("TRANSLATE basic", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT TRANSLATE('hello', 'el', 'ip')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "hippo", result)
	})

	t.Run("TRANSLATE deletion", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT TRANSLATE('hello', 'elo', 'a')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "ha", result)
	})

	t.Run("TRANSLATE NULL", func(t *testing.T) {
		var result *string
		err := db.QueryRow("SELECT TRANSLATE(NULL, 'a', 'b')").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRIP_ACCENTS", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT STRIP_ACCENTS('cafe\u0301')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "cafe", result)
	})

	t.Run("STRIP_ACCENTS multiple", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT STRIP_ACCENTS('re\u0301sume\u0301')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "resume", result)
	})

	t.Run("STRIP_ACCENTS no accents", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT STRIP_ACCENTS('hello')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("STRIP_ACCENTS NULL", func(t *testing.T) {
		var result *string
		err := db.QueryRow("SELECT STRIP_ACCENTS(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}
