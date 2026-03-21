package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionAliases(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db.Close())
	}()

	// --- DATETRUNC / DATEADD aliases ---

	t.Run("DATETRUNC alias", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT DATETRUNC('month', CAST('2024-03-15 10:30:00' AS TIMESTAMP))").Scan(&result)
		require.NoError(t, err)

		var expected string
		err = db.QueryRow("SELECT DATE_TRUNC('month', CAST('2024-03-15 10:30:00' AS TIMESTAMP))").Scan(&expected)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("DATEADD alias", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT DATEADD(CAST('2024-03-15' AS DATE), 1)").Scan(&result)
		require.NoError(t, err)

		var expected string
		err = db.QueryRow("SELECT DATE_ADD(CAST('2024-03-15' AS DATE), 1)").Scan(&expected)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	// --- ORD alias for ASCII ---

	t.Run("ORD alias for ASCII", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT ORD('A')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(65), result)

		var asciiResult int64
		err = db.QueryRow("SELECT ASCII('A')").Scan(&asciiResult)
		require.NoError(t, err)
		assert.Equal(t, result, asciiResult)
	})

	t.Run("ORD empty string", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT ORD('')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	// --- IFNULL / NVL ---

	t.Run("IFNULL returns replacement for NULL", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT IFNULL(NULL, 42)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)
	})

	t.Run("IFNULL returns first when non-NULL", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT IFNULL(1, 99)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)
	})

	t.Run("IFNULL both NULL", func(t *testing.T) {
		var result sql.NullInt64
		err := db.QueryRow("SELECT IFNULL(NULL, NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("IFNULL wrong arg count", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT IFNULL(1, 2, 3)").Scan(&result)
		assert.Error(t, err)
	})

	t.Run("NVL alias for IFNULL", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT NVL(NULL, 'default')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "default", result)
	})

	t.Run("NVL returns first when non-NULL", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT NVL(1, 99)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)
	})

	// --- BIT_LENGTH ---

	t.Run("BIT_LENGTH of string", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT BIT_LENGTH('hello')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(40), result)
	})

	t.Run("BIT_LENGTH of empty string", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT BIT_LENGTH('')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	t.Run("BIT_LENGTH of NULL", func(t *testing.T) {
		var result sql.NullInt64
		err := db.QueryRow("SELECT BIT_LENGTH(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	// --- GET_BIT / SET_BIT (using table with BLOB column) ---

	t.Run("GET_BIT via BLOB column", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE test_getbit (data BLOB)")
		require.NoError(t, err)

		defer func() {
			_, _ = db.Exec("DROP TABLE test_getbit") // cleanup after test
		}()

		_, err = db.Exec("INSERT INTO test_getbit VALUES ($1)", []byte{0x80})
		require.NoError(t, err)

		var bit0 int64
		err = db.QueryRow("SELECT GET_BIT(data, 0) FROM test_getbit").Scan(&bit0)
		require.NoError(t, err)
		assert.Equal(t, int64(1), bit0)

		var bit1 int64
		err = db.QueryRow("SELECT GET_BIT(data, 1) FROM test_getbit").Scan(&bit1)
		require.NoError(t, err)
		assert.Equal(t, int64(0), bit1)
	})

	t.Run("SET_BIT via BLOB column", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE test_setbit (data BLOB)")
		require.NoError(t, err)

		defer func() {
			_, _ = db.Exec("DROP TABLE test_setbit") // cleanup after test
		}()

		_, err = db.Exec("INSERT INTO test_setbit VALUES ($1)", []byte{0x00})
		require.NoError(t, err)

		var result []byte
		err = db.QueryRow("SELECT SET_BIT(data, 0, 1) FROM test_setbit").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, []byte{0x80}, result)
	})

	t.Run("GET_BIT NULL propagation", func(t *testing.T) {
		var result sql.NullInt64
		err := db.QueryRow("SELECT GET_BIT(NULL, 0)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("SET_BIT invalid bit value", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE test_setbit_err (data BLOB)")
		require.NoError(t, err)

		defer func() {
			_, _ = db.Exec("DROP TABLE test_setbit_err") // cleanup after test
		}()

		_, err = db.Exec("INSERT INTO test_setbit_err VALUES ($1)", []byte{0x00})
		require.NoError(t, err)

		var result []byte
		err = db.QueryRow("SELECT SET_BIT(data, 0, 2) FROM test_setbit_err").Scan(&result)
		assert.Error(t, err)
	})

	// --- ENCODE / DECODE ---

	t.Run("ENCODE UTF-8", func(t *testing.T) {
		var result []byte
		err := db.QueryRow("SELECT ENCODE('hello', 'UTF-8')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), result)
	})

	t.Run("DECODE UTF-8", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT DECODE(ENCODE('hello', 'UTF-8'), 'UTF-8')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("ENCODE unsupported encoding", func(t *testing.T) {
		var result []byte
		err := db.QueryRow("SELECT ENCODE('hello', 'EBCDIC')").Scan(&result)
		assert.Error(t, err)
	})

	t.Run("DECODE NULL input", func(t *testing.T) {
		var result sql.NullString
		err := db.QueryRow("SELECT DECODE(NULL, 'UTF-8')").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("ENCODE default encoding", func(t *testing.T) {
		var result []byte
		err := db.QueryRow("SELECT ENCODE('test')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, []byte("test"), result)
	})

	t.Run("ENCODE LATIN1", func(t *testing.T) {
		var result []byte
		err := db.QueryRow("SELECT ENCODE('abc', 'LATIN1')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, []byte("abc"), result)
	})

	t.Run("DECODE LATIN1", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT DECODE(ENCODE('abc', 'LATIN1'), 'LATIN1')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "abc", result)
	})

	t.Run("ENCODE NULL input", func(t *testing.T) {
		var result sql.NullString
		err := db.QueryRow("SELECT ENCODE(NULL, 'UTF-8')").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})
}
