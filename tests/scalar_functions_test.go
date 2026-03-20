package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScalarFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("IF_IFF", func(t *testing.T) {
		t.Run("IF_true_returns_true_branch", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT IF(true, 'yes', 'no')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "yes", result)
		})

		t.Run("IF_false_returns_false_branch", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT IF(false, 'yes', 'no')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "no", result)
		})

		t.Run("IF_NULL_condition_returns_false_branch", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT IF(NULL, 'yes', 'no')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "no", result)
		})

		t.Run("IFF_with_comparison", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT IFF(1 > 0, 1, 2)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(1), result)
		})

		t.Run("IF_true_with_NULL_true_branch", func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow("SELECT IF(true, NULL, 'fallback')").Scan(&result)
			require.NoError(t, err)
			assert.False(t, result.Valid, "IF(true, NULL, 'fallback') should return NULL")
		})
	})

	t.Run("FORMAT_PRINTF", func(t *testing.T) {
		t.Run("FORMAT_string_and_int", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT FORMAT('%s has %d items', 'cart', 5)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "cart has 5 items", result)
		})

		t.Run("FORMAT_float_precision", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT FORMAT('%.2f', 3.14159)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "3.14", result)
		})

		t.Run("PRINTF_zero_padded", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT PRINTF('%05d', 42)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "00042", result)
		})

		t.Run("FORMAT_escaped_percent", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT FORMAT('100%%')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "100%", result)
		})

		t.Run("FORMAT_NULL_argument", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT FORMAT('%s', NULL)").Scan(&result)
			require.NoError(t, err)
			assert.Contains(t, result, "NULL")
		})
	})

	t.Run("TYPEOF_PG_TYPEOF", func(t *testing.T) {
		t.Run("TYPEOF_integer", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF(42)").Scan(&result)
			require.NoError(t, err)
			// Integer literals are inferred as BIGINT by the engine
			assert.Equal(t, "BIGINT", result)
		})

		t.Run("TYPEOF_varchar", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF('hello')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "VARCHAR", result)
		})

		t.Run("TYPEOF_boolean", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF(true)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "BOOLEAN", result)
		})

		t.Run("TYPEOF_double", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF(3.14)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "DOUBLE", result)
		})

		t.Run("PG_TYPEOF_integer", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT PG_TYPEOF(42)").Scan(&result)
			require.NoError(t, err)
			// Integer literals are inferred as BIGINT by the engine
			assert.Equal(t, "bigint", result)
		})

		t.Run("PG_TYPEOF_varchar", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT PG_TYPEOF('hello')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "character varying", result)
		})

		t.Run("TYPEOF_null", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF(NULL)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "NULL", result)
		})
	})

	t.Run("BASE64", func(t *testing.T) {
		t.Run("BASE64_ENCODE", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT BASE64_ENCODE('Hello')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "SGVsbG8=", result)
		})

		t.Run("BASE64_alias", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT BASE64('World')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "V29ybGQ=", result)
		})

		t.Run("TO_BASE64_alias", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TO_BASE64('Test')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "VGVzdA==", result)
		})

		t.Run("BASE64_DECODE", func(t *testing.T) {
			var result []byte
			err := db.QueryRow("SELECT BASE64_DECODE('SGVsbG8=')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "Hello", string(result))
		})

		t.Run("FROM_BASE64_alias", func(t *testing.T) {
			var result []byte
			err := db.QueryRow("SELECT FROM_BASE64('V29ybGQ=')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "World", string(result))
		})

		t.Run("BASE64_ENCODE_NULL_propagation", func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow("SELECT BASE64_ENCODE(NULL)").Scan(&result)
			require.NoError(t, err)
			assert.False(t, result.Valid, "BASE64_ENCODE(NULL) should return NULL")
		})

		t.Run("BASE64_ENCODE_empty_string", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT BASE64_ENCODE('')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "", result)
		})
	})

	t.Run("URL_ENCODE_DECODE", func(t *testing.T) {
		t.Run("URL_ENCODE_spaces", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT URL_ENCODE('hello world')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "hello+world", result)
		})

		t.Run("URL_ENCODE_special_chars", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT URL_ENCODE('a=1&b=2')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "a%3D1%26b%3D2", result)
		})

		t.Run("URL_DECODE_spaces", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT URL_DECODE('hello+world')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "hello world", result)
		})

		t.Run("URL_DECODE_special_chars", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT URL_DECODE('a%3D1%26b%3D2')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "a=1&b=2", result)
		})

		t.Run("URL_ENCODE_NULL_propagation", func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow("SELECT URL_ENCODE(NULL)").Scan(&result)
			require.NoError(t, err)
			assert.False(t, result.Valid, "URL_ENCODE(NULL) should return NULL")
		})
	})
}
