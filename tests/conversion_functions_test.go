package tests

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConversionFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db.Close())
	}()

	// dateFromResult extracts a time.Time from a TO_DATE result.
	// The driver returns int32 (days since epoch) which database/sql may
	// surface as int32 or int64 depending on the scan target.
	dateFromResult := func(t *testing.T, val any) time.Time {
		t.Helper()

		switch v := val.(type) {
		case time.Time:
			return v
		case int64:
			return time.Unix(v*86400, 0).UTC()
		case int32:
			return time.Unix(int64(v)*86400, 0).UTC()
		default:
			t.Fatalf("unexpected type %T for date result: %v", val, val)

			return time.Time{}
		}
	}

	t.Run("TO_DATE", func(t *testing.T) {
		t.Run("ToDateWithFormat", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT TO_DATE('2024-01-15', '%Y-%m-%d')").Scan(&result)
			require.NoError(t, err)
			require.NotNil(t, result)
			tm := dateFromResult(t, result)
			assert.Equal(t, 2024, tm.Year())
			assert.Equal(t, time.January, tm.Month())
			assert.Equal(t, 15, tm.Day())
		})

		t.Run("ToDateAutoDetect", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT TO_DATE('2024-06-30')").Scan(&result)
			require.NoError(t, err)
			require.NotNil(t, result)
			tm := dateFromResult(t, result)
			assert.Equal(t, 2024, tm.Year())
			assert.Equal(t, time.June, tm.Month())
			assert.Equal(t, 30, tm.Day())
		})

		t.Run("ToDateNull", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT TO_DATE(NULL)").Scan(&result)
			require.NoError(t, err)
			assert.Nil(t, result)
		})

		t.Run("ToDateInvalidFormat", func(t *testing.T) {
			// STRPTIME returns NULL for unparseable strings, so TO_DATE
			// with a 2-arg form returns NULL rather than an error.
			var result any
			err := db.QueryRow("SELECT TO_DATE('not-a-date', '%Y-%m-%d')").Scan(&result)
			require.NoError(t, err)
			assert.Nil(t, result, "TO_DATE with unparseable input should return NULL")
		})
	})

	t.Run("TO_CHAR", func(t *testing.T) {
		// TO_CHAR is aliased to STRFTIME and uses the same argument order:
		// TO_CHAR(format_string, date_or_timestamp)
		t.Run("ToCharDate", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TO_CHAR('%Y/%m/%d', CAST('2024-01-15' AS DATE))").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "2024/01/15", result)
		})

		t.Run("ToCharTimestamp", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TO_CHAR('%Y-%m-%d %H:%M:%S', CAST('2024-01-15 10:30:00' AS TIMESTAMP))").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "2024-01-15 10:30:00", result)
		})
	})

	t.Run("GENERATE_SUBSCRIPTS", func(t *testing.T) {
		t.Run("GenerateSubscriptsBasic", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT GENERATE_SUBSCRIPTS([10, 20, 30], 1)").Scan(&result)
			require.NoError(t, err)
			require.NotNil(t, result)

			switch v := result.(type) {
			case []any:
				require.Len(t, v, 3)

				for i, expected := range []int{1, 2, 3} {
					switch num := v[i].(type) {
					case int32:
						assert.Equal(t, int32(expected), num)
					case int64:
						assert.Equal(t, int64(expected), num)
					default:
						t.Fatalf("unexpected element type %T at index %d: %v", v[i], i, v[i])
					}
				}
			case string:
				assert.Equal(t, "[1, 2, 3]", v)
			default:
				t.Fatalf("unexpected type %T for GENERATE_SUBSCRIPTS result: %v", result, result)
			}
		})

		t.Run("GenerateSubscriptsEmpty", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT GENERATE_SUBSCRIPTS([], 1)").Scan(&result)
			require.NoError(t, err)

			switch v := result.(type) {
			case []any:
				assert.Empty(t, v)
			case string:
				assert.Equal(t, "[]", v)
			case nil:
				// empty list might come back as nil
			default:
				t.Fatalf("unexpected type %T for GENERATE_SUBSCRIPTS empty result: %v", result, result)
			}
		})

		t.Run("GenerateSubscriptsNull", func(t *testing.T) {
			var result any
			err := db.QueryRow("SELECT GENERATE_SUBSCRIPTS(NULL, 1)").Scan(&result)
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	})
}
