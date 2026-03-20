package tests

import (
	"database/sql"
	"encoding/json"
	"sort"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONScalarAndAggregateFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	t.Run("JSON_CONTAINS", func(t *testing.T) {
		t.Run("array contains integer", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('[1,2,3]', 2)`).Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("object contains value", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('{"a":1,"b":2}', 1)`).Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("array does not contain value", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('[1,2,3]', 5)`).Scan(&result)
			require.NoError(t, err)
			assert.False(t, result)
		})

		t.Run("null JSON returns null", func(t *testing.T) {
			var result sql.NullBool
			err := db.QueryRow(`SELECT JSON_CONTAINS(NULL, 1)`).Scan(&result)
			require.NoError(t, err)
			assert.False(t, result.Valid, "JSON_CONTAINS(NULL, ...) should return SQL NULL")
		})

		t.Run("nested object recursive search", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('{"nested":{"x":1}}', 1)`).Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("nested object value not found", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('{"nested":{"x":1}}', 99)`).Scan(&result)
			require.NoError(t, err)
			assert.False(t, result)
		})

		t.Run("array contains string", func(t *testing.T) {
			var result bool
			err := db.QueryRow(`SELECT JSON_CONTAINS('["a","b","c"]', 'b')`).Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})
	})

	t.Run("JSON_QUOTE", func(t *testing.T) {
		t.Run("quote string", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_QUOTE('hello')`).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, `"hello"`, result)
		})

		t.Run("quote integer", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_QUOTE(42)`).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "42", result)
		})

		t.Run("quote boolean true", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_QUOTE(true)`).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "true", result)
		})

		t.Run("quote null returns string null", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_QUOTE(NULL)`).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "null", result)
		})
	})

	t.Run("JSON_GROUP_ARRAY", func(t *testing.T) {
		// Create and populate test table
		_, err := db.Exec(`CREATE TABLE jga_test (id INTEGER, name VARCHAR, category VARCHAR)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO jga_test VALUES
			(1, 'Alice', 'A'),
			(2, 'Bob', 'A'),
			(3, 'Charlie', 'B')`)
		require.NoError(t, err)

		t.Run("basic aggregation", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_GROUP_ARRAY(name) FROM jga_test`).Scan(&result)
			require.NoError(t, err)

			// Parse result to verify it is valid JSON
			var parsed []any
			err = json.Unmarshal([]byte(result), &parsed)
			require.NoError(t, err, "result should be valid JSON array")
			assert.Len(t, parsed, 3)

			// Collect string values and sort for deterministic comparison
			names := make([]string, 0, len(parsed))
			for _, v := range parsed {
				s, ok := v.(string)
				require.True(t, ok, "each element should be a string")
				names = append(names, s)
			}
			sort.Strings(names)
			assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)
		})

		t.Run("with GROUP BY", func(t *testing.T) {
			rows, err := db.Query(
				`SELECT category, JSON_GROUP_ARRAY(name) FROM jga_test GROUP BY category ORDER BY category`,
			)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()

			type groupResult struct {
				category string
				names    []string
			}
			var results []groupResult

			for rows.Next() {
				var cat, jsonArr string
				require.NoError(t, rows.Scan(&cat, &jsonArr))

				var parsed []any
				err := json.Unmarshal([]byte(jsonArr), &parsed)
				require.NoError(t, err, "result should be valid JSON array for category %s", cat)

				names := make([]string, 0, len(parsed))
				for _, v := range parsed {
					s, ok := v.(string)
					require.True(t, ok)
					names = append(names, s)
				}
				sort.Strings(names)
				results = append(results, groupResult{category: cat, names: names})
			}
			require.NoError(t, rows.Err())
			require.Len(t, results, 2)

			assert.Equal(t, "A", results[0].category)
			assert.Equal(t, []string{"Alice", "Bob"}, results[0].names)

			assert.Equal(t, "B", results[1].category)
			assert.Equal(t, []string{"Charlie"}, results[1].names)
		})

		// Clean up
		_, err = db.Exec(`DROP TABLE jga_test`)
		require.NoError(t, err)
	})

	t.Run("JSON_GROUP_OBJECT", func(t *testing.T) {
		// Create and populate test table
		_, err := db.Exec(
			`CREATE TABLE jgo_test (id INTEGER, k VARCHAR, v VARCHAR, category VARCHAR)`,
		)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO jgo_test VALUES
			(1, 'name', 'Alice', 'profile'),
			(2, 'age', '30', 'profile'),
			(3, 'city', 'NYC', 'location')`)
		require.NoError(t, err)

		t.Run("basic aggregation", func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT JSON_GROUP_OBJECT(k, v) FROM jgo_test`).Scan(&result)
			require.NoError(t, err)

			// Parse result to verify it is valid JSON
			var parsed map[string]any
			err = json.Unmarshal([]byte(result), &parsed)
			require.NoError(t, err, "result should be valid JSON object")

			assert.Equal(t, "Alice", parsed["name"])
			assert.Equal(t, "30", parsed["age"])
			assert.Equal(t, "NYC", parsed["city"])
		})

		t.Run("with GROUP BY", func(t *testing.T) {
			rows, err := db.Query(
				`SELECT category, JSON_GROUP_OBJECT(k, v) FROM jgo_test GROUP BY category ORDER BY category`,
			)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()

			type groupResult struct {
				category string
				obj      map[string]any
			}
			var results []groupResult

			for rows.Next() {
				var cat, jsonObj string
				require.NoError(t, rows.Scan(&cat, &jsonObj))

				var parsed map[string]any
				err := json.Unmarshal([]byte(jsonObj), &parsed)
				require.NoError(t, err, "result should be valid JSON object for category %s", cat)
				results = append(results, groupResult{category: cat, obj: parsed})
			}
			require.NoError(t, rows.Err())
			require.Len(t, results, 2)

			assert.Equal(t, "location", results[0].category)
			assert.Equal(t, "NYC", results[0].obj["city"])

			assert.Equal(t, "profile", results[1].category)
			assert.Equal(t, "Alice", results[1].obj["name"])
			assert.Equal(t, "30", results[1].obj["age"])
		})

		// Clean up
		_, err = db.Exec(`DROP TABLE jgo_test`)
		require.NoError(t, err)
	})

	t.Run("JSON_EACH", func(t *testing.T) {
		t.Run("object expansion", func(t *testing.T) {
			rows, err := db.Query(`SELECT * FROM json_each('{"a":1,"b":2}')`)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()

			// Verify column names
			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"key", "value"}, cols)

			type kvPair struct {
				key   string
				value string
			}
			var results []kvPair

			for rows.Next() {
				var k, v string
				require.NoError(t, rows.Scan(&k, &v))
				results = append(results, kvPair{key: k, value: v})
			}
			require.NoError(t, rows.Err())
			require.Len(t, results, 2)

			// Sort by key for deterministic comparison (map iteration order is undefined)
			sort.Slice(results, func(i, j int) bool {
				return results[i].key < results[j].key
			})

			assert.Equal(t, "a", results[0].key)
			assert.Equal(t, "1", results[0].value)
			assert.Equal(t, "b", results[1].key)
			assert.Equal(t, "2", results[1].value)
		})

		t.Run("array expansion", func(t *testing.T) {
			rows, err := db.Query(`SELECT * FROM json_each('[10,20,30]')`)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()

			type kvPair struct {
				key   string
				value string
			}
			var results []kvPair

			for rows.Next() {
				var k, v string
				require.NoError(t, rows.Scan(&k, &v))
				results = append(results, kvPair{key: k, value: v})
			}
			require.NoError(t, rows.Err())
			require.Len(t, results, 3)

			assert.Equal(t, "0", results[0].key)
			assert.Equal(t, "10", results[0].value)
			assert.Equal(t, "1", results[1].key)
			assert.Equal(t, "20", results[1].value)
			assert.Equal(t, "2", results[2].key)
			assert.Equal(t, "30", results[2].value)
		})

		t.Run("object with mixed value types", func(t *testing.T) {
			rows, err := db.Query(
				`SELECT * FROM json_each('{"name":"Alice","age":30,"active":true}')`,
			)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()

			results := make(map[string]string)
			for rows.Next() {
				var k, v string
				require.NoError(t, rows.Scan(&k, &v))
				results[k] = v
			}
			require.NoError(t, rows.Err())
			require.Len(t, results, 3)

			assert.Equal(t, `"Alice"`, results["name"])
			assert.Equal(t, "30", results["age"])
			assert.Equal(t, "true", results["active"])
		})
	})
}
