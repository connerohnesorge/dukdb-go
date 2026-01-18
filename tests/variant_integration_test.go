package tests

import (
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVariantColumnIntegration tests VARIANT column operations end-to-end with database/sql.
func TestVariantColumnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Insert and select string value", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_test VALUES (1, $1)`, `"hello world"`)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 1`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 1, id)

		// Verify the value can be parsed back
		var parsed string
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "hello world", parsed)
	})

	t.Run("Insert and select integer value", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_test VALUES (2, $1)`, `42`)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 2`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 2, id)

		// Verify the value can be parsed back
		var parsed float64
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, float64(42), parsed)
	})

	t.Run("Insert and select float value", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_test VALUES (3, $1)`, `3.14159`)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 3`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 3, id)

		// Verify the value can be parsed back
		var parsed float64
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.InDelta(t, 3.14159, parsed, 0.00001)
	})

	t.Run("Insert and select boolean true", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_test VALUES (4, $1)`, `true`)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 4`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 4, id)

		// Verify the value can be parsed back
		var parsed bool
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.True(t, parsed)
	})

	t.Run("Insert and select boolean false", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_test VALUES (5, $1)`, `false`)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 5`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 5, id)

		// Verify the value can be parsed back
		var parsed bool
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.False(t, parsed)
	})

	t.Run("Insert and select JSON object", func(t *testing.T) {
		jsonData := `{"name":"Alice","age":30,"active":true}`
		_, err := db.Exec(`INSERT INTO variant_test VALUES (6, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 6`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 6, id)

		// Verify the JSON can be parsed back
		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parsed["name"])
		assert.Equal(t, float64(30), parsed["age"])
		assert.Equal(t, true, parsed["active"])
	})

	t.Run("Insert and select JSON array", func(t *testing.T) {
		jsonData := `[1,2,3,"four",null,true]`
		_, err := db.Exec(`INSERT INTO variant_test VALUES (7, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 7`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 7, id)

		// Verify the JSON can be parsed back
		var parsed []any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		require.Len(t, parsed, 6)
		assert.Equal(t, float64(1), parsed[0])
		assert.Equal(t, float64(2), parsed[1])
		assert.Equal(t, float64(3), parsed[2])
		assert.Equal(t, "four", parsed[3])
		assert.Nil(t, parsed[4])
		assert.Equal(t, true, parsed[5])
	})

	t.Run("Insert and select JSON null primitive", func(t *testing.T) {
		jsonData := `null`
		_, err := db.Exec(`INSERT INTO variant_test VALUES (8, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_test WHERE id = 8`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 8, id)

		// Verify it is JSON null (not SQL NULL)
		var parsed any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Nil(t, parsed)
	})
}

// TestVariantColumnNestedStructures tests VARIANT column with complex nested structures.
func TestVariantColumnNestedStructures(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_nested_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Insert and select deeply nested object", func(t *testing.T) {
		jsonData := `{"level1":{"level2":{"level3":{"level4":{"value":"deep"}}}}}`
		_, err := db.Exec(`INSERT INTO variant_nested_test VALUES (1, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_nested_test WHERE id = 1`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 1, id)

		// Verify the nested structure
		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)

		// Navigate to the deep value
		level1, ok := parsed["level1"].(map[string]any)
		require.True(t, ok)
		level2, ok := level1["level2"].(map[string]any)
		require.True(t, ok)
		level3, ok := level2["level3"].(map[string]any)
		require.True(t, ok)
		level4, ok := level3["level4"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "deep", level4["value"])
	})

	t.Run("Insert and select array of objects", func(t *testing.T) {
		jsonData := `[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"},{"id":3,"name":"Charlie"}]`
		_, err := db.Exec(`INSERT INTO variant_nested_test VALUES (2, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_nested_test WHERE id = 2`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 2, id)

		// Verify the array of objects
		var parsed []any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		require.Len(t, parsed, 3)

		user1, ok := parsed[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(1), user1["id"])
		assert.Equal(t, "Alice", user1["name"])

		user2, ok := parsed[1].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(2), user2["id"])
		assert.Equal(t, "Bob", user2["name"])
	})

	t.Run("Insert and select complex nested structure", func(t *testing.T) {
		jsonData := `{"users":[{"id":1,"name":"Alice","tags":["admin","active"]},{"id":2,"name":"Bob","tags":["user"]}],"metadata":{"count":2,"version":"1.0","nested":{"a":{"b":{"c":"value"}}}}}`
		_, err := db.Exec(`INSERT INTO variant_nested_test VALUES (3, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_nested_test WHERE id = 3`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 3, id)

		// Verify the complex structure
		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)

		users, ok := parsed["users"].([]any)
		require.True(t, ok)
		require.Len(t, users, 2)

		user1, ok := users[0].(map[string]any)
		require.True(t, ok)
		tags, ok := user1["tags"].([]any)
		require.True(t, ok)
		assert.Equal(t, "admin", tags[0])
		assert.Equal(t, "active", tags[1])

		metadata, ok := parsed["metadata"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(2), metadata["count"])

		nested, ok := metadata["nested"].(map[string]any)
		require.True(t, ok)
		a, ok := nested["a"].(map[string]any)
		require.True(t, ok)
		b, ok := a["b"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", b["c"])
	})

	t.Run("Insert and select mixed array types", func(t *testing.T) {
		jsonData := `[1,"two",3.0,true,null,{"key":"value"},[1,2,3]]`
		_, err := db.Exec(`INSERT INTO variant_nested_test VALUES (4, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM variant_nested_test WHERE id = 4`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 4, id)

		// Verify the mixed array
		var parsed []any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		require.Len(t, parsed, 7)

		assert.Equal(t, float64(1), parsed[0])
		assert.Equal(t, "two", parsed[1])
		assert.Equal(t, float64(3.0), parsed[2])
		assert.Equal(t, true, parsed[3])
		assert.Nil(t, parsed[4])

		obj, ok := parsed[5].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", obj["key"])

		arr, ok := parsed[6].([]any)
		require.True(t, ok)
		assert.Len(t, arr, 3)
	})
}

// TestVariantColumnNullHandling tests NULL handling in VARIANT columns.
func TestVariantColumnNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_null_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Insert SQL NULL into VARIANT column", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_null_test VALUES (1, NULL)`)
		require.NoError(t, err)

		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM variant_null_test WHERE id = 1`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.False(t, data.Valid, "SQL NULL should result in NullString.Valid = false")
	})

	t.Run("Insert SQL NULL using parameter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_null_test VALUES (2, $1)`, nil)
		require.NoError(t, err)

		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM variant_null_test WHERE id = 2`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.False(
			t,
			data.Valid,
			"SQL NULL via parameter should result in NullString.Valid = false",
		)
	})

	t.Run("Distinguish SQL NULL from JSON null", func(t *testing.T) {
		// Insert JSON null (the literal "null" string which is valid JSON)
		_, err := db.Exec(`INSERT INTO variant_null_test VALUES (3, $1)`, `null`)
		require.NoError(t, err)

		// Query and check - JSON null is a valid string value
		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM variant_null_test WHERE id = 3`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.True(t, data.Valid, "JSON null should be a valid string value, not SQL NULL")

		// Verify the JSON null parses correctly
		var parsed any
		err = json.Unmarshal([]byte(data.String), &parsed)
		require.NoError(t, err)
		assert.Nil(t, parsed, "JSON null should parse to nil")
	})

	t.Run("Query NULL with IS NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM variant_null_test WHERE data IS NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		// IDs 1 and 2 have SQL NULL, ID 3 has JSON null (which is not SQL NULL)
		assert.Equal(t, []int{1, 2}, ids)
	})

	t.Run("Query non-NULL with IS NOT NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM variant_null_test WHERE data IS NOT NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		// ID 3 has JSON null (which is a valid value, not SQL NULL)
		assert.Equal(t, []int{3}, ids)
	})
}

// TestVariantColumnMultipleRows tests VARIANT column with multiple rows.
func TestVariantColumnMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_multi_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	// Insert multiple rows with different value types
	testData := []struct {
		id    int
		value string
	}{
		{1, `{"type":"object"}`},
		{2, `[1,2,3]`},
		{3, `"string value"`},
		{4, `42`},
		{5, `true`},
		{6, `null`},
		{7, `3.14159`},
		{8, `false`},
	}

	for _, td := range testData {
		_, err := db.Exec(`INSERT INTO variant_multi_test VALUES ($1, $2)`, td.id, td.value)
		require.NoError(t, err)
	}

	// Query all rows
	rows, err := db.Query(`SELECT id, data FROM variant_multi_test ORDER BY id`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var results []struct {
		id   int
		data string
	}
	for rows.Next() {
		var id int
		var data string
		require.NoError(t, rows.Scan(&id, &data))
		results = append(results, struct {
			id   int
			data string
		}{id, data})
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 8)

	// Verify each row
	for i, td := range testData {
		assert.Equal(t, td.id, results[i].id)
		// Parse both to compare as JSON values (to handle formatting differences)
		var expected, actual any
		err := json.Unmarshal([]byte(td.value), &expected)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(results[i].data), &actual)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

// TestVariantColumnPreparedStatement tests VARIANT column with prepared statements.
func TestVariantColumnPreparedStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_prep_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	// Prepare insert statement
	insertStmt, err := db.Prepare(`INSERT INTO variant_prep_test VALUES ($1, $2)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, insertStmt.Close())
	}()

	// Insert multiple rows using prepared statement
	testData := []struct {
		id    int
		value string
	}{
		{1, `{"key":"value1"}`},
		{2, `[1,2,3]`},
		{3, `"string"`},
		{4, `42`},
		{5, `true`},
	}

	for _, td := range testData {
		_, err := insertStmt.Exec(td.id, td.value)
		require.NoError(t, err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare(`SELECT data FROM variant_prep_test WHERE id = $1`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, selectStmt.Close())
	}()

	// Query using prepared statement
	for _, td := range testData {
		var data string
		err := selectStmt.QueryRow(td.id).Scan(&data)
		require.NoError(t, err)

		// Parse and verify
		var expected, actual any
		err = json.Unmarshal([]byte(td.value), &expected)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(data), &actual)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

// TestVariantColumnTransaction tests VARIANT column operations within a transaction.
func TestVariantColumnTransaction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_tx_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO variant_tx_test VALUES (1, $1)`, `{"status":"committed"}`)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify data persisted
		var data string
		err = db.QueryRow(`SELECT data FROM variant_tx_test WHERE id = 1`).Scan(&data)
		require.NoError(t, err)

		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "committed", parsed["status"])
	})

	t.Run("Rollback transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(
			`INSERT INTO variant_tx_test VALUES (2, $1)`,
			`{"status":"should_be_rolled_back"}`,
		)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify data was not persisted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM variant_tx_test WHERE id = 2`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestVariantColumnUnicodeAndEscaping tests VARIANT column with special characters.
func TestVariantColumnUnicodeAndEscaping(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_unicode_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	testCases := []struct {
		id    int
		name  string
		value string
	}{
		{1, "Unicode characters", `{"message":"Hello World"}`},
		{2, "Escaped quotes", `{"text":"He said \"hello\""}`},
		{3, "Newlines and tabs", `{"text":"line1\nline2\ttabbed"}`},
		{4, "Backslash", `{"path":"C:\\Users\\test"}`},
		{5, "Unicode escape", `{"emoji":"\u0048\u0065\u006c\u006c\u006f"}`},
		{6, "Chinese characters", `{"greeting":"Ni Hao"}`},
		{7, "Japanese characters", `{"greeting":"Kon'nichiwa"}`},
		{8, "Empty string", `""`},
		{9, "Empty object", `{}`},
		{10, "Empty array", `[]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(`INSERT INTO variant_unicode_test VALUES ($1, $2)`, tc.id, tc.value)
			require.NoError(t, err)

			var data string
			err = db.QueryRow(`SELECT data FROM variant_unicode_test WHERE id = $1`, tc.id).
				Scan(&data)
			require.NoError(t, err)

			// Verify the value can be parsed
			var expected, actual any
			err = json.Unmarshal([]byte(tc.value), &expected)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(data), &actual)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})
	}
}

// TestVariantColumnUpdate tests updating VARIANT column values.
func TestVariantColumnUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert initial data
	_, err = db.Exec(`CREATE TABLE variant_update_test (id INTEGER PRIMARY KEY, data VARIANT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO variant_update_test VALUES (1, $1)`, `{"version":1}`)
	require.NoError(t, err)

	// Update the VARIANT data
	result, err := db.Exec(
		`UPDATE variant_update_test SET data = $1 WHERE id = 1`,
		`{"version":2,"updated":true}`,
	)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify the update
	var data string
	err = db.QueryRow(`SELECT data FROM variant_update_test WHERE id = 1`).Scan(&data)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(data), &parsed)
	require.NoError(t, err)
	assert.Equal(t, float64(2), parsed["version"])
	assert.Equal(t, true, parsed["updated"])
}

// TestVariantColumnDelete tests deleting rows with VARIANT columns.
func TestVariantColumnDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert data
	_, err = db.Exec(`CREATE TABLE variant_delete_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO variant_delete_test VALUES (1, $1)`, `{"keep":false}`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO variant_delete_test VALUES (2, $1)`, `{"keep":true}`)
	require.NoError(t, err)

	// Delete one row
	result, err := db.Exec(`DELETE FROM variant_delete_test WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only one row remains
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM variant_delete_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the correct row remains
	var data string
	err = db.QueryRow(`SELECT data FROM variant_delete_test WHERE id = 2`).Scan(&data)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(data), &parsed)
	require.NoError(t, err)
	assert.Equal(t, true, parsed["keep"])
}

// TestVariantColumnMixedWithOtherColumns tests VARIANT column alongside other column types.
func TestVariantColumnMixedWithOtherColumns(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with mixed column types
	_, err = db.Exec(`CREATE TABLE variant_mixed_test (
		id INTEGER PRIMARY KEY,
		name VARCHAR,
		score DOUBLE,
		active BOOLEAN,
		metadata VARIANT
	)`)
	require.NoError(t, err)

	// Insert data
	_, err = db.Exec(`INSERT INTO variant_mixed_test VALUES (1, 'Alice', 95.5, true, $1)`,
		`{"level":"advanced","achievements":["gold","silver"]}`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO variant_mixed_test VALUES (2, 'Bob', 87.3, false, $1)`,
		`{"level":"intermediate","achievements":["bronze"]}`)
	require.NoError(t, err)

	// Query and verify
	rows, err := db.Query(
		`SELECT id, name, score, active, metadata FROM variant_mixed_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	type result struct {
		id       int
		name     string
		score    float64
		active   bool
		metadata string
	}

	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.id, &r.name, &r.score, &r.active, &r.metadata))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)

	// Verify first row
	assert.Equal(t, 1, results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.InDelta(t, 95.5, results[0].score, 0.01)
	assert.True(t, results[0].active)

	var meta1 map[string]any
	err = json.Unmarshal([]byte(results[0].metadata), &meta1)
	require.NoError(t, err)
	assert.Equal(t, "advanced", meta1["level"])

	// Verify second row
	assert.Equal(t, 2, results[1].id)
	assert.Equal(t, "Bob", results[1].name)
	assert.InDelta(t, 87.3, results[1].score, 0.01)
	assert.False(t, results[1].active)

	var meta2 map[string]any
	err = json.Unmarshal([]byte(results[1].metadata), &meta2)
	require.NoError(t, err)
	assert.Equal(t, "intermediate", meta2["level"])
}

// TestVariantColumnLargeValues tests VARIANT column with large values.
func TestVariantColumnLargeValues(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_large_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Large array", func(t *testing.T) {
		// Create a large array
		arr := make([]int, 1000)
		for i := range arr {
			arr[i] = i
		}
		jsonBytes, err := json.Marshal(arr)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO variant_large_test VALUES (1, $1)`, string(jsonBytes))
		require.NoError(t, err)

		var data string
		err = db.QueryRow(`SELECT data FROM variant_large_test WHERE id = 1`).Scan(&data)
		require.NoError(t, err)

		var parsed []any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Len(t, parsed, 1000)
		assert.Equal(t, float64(0), parsed[0])
		assert.Equal(t, float64(999), parsed[999])
	})

	t.Run("Large nested object", func(t *testing.T) {
		// Create a large nested object
		obj := make(map[string]any)
		for i := range 100 {
			key := "key_" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			obj[key] = map[string]any{
				"id":    i,
				"value": "value_" + string(rune('a'+i%26)),
				"nested": map[string]any{
					"a": i * 2,
					"b": i * 3,
				},
			}
		}
		jsonBytes, err := json.Marshal(obj)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO variant_large_test VALUES (2, $1)`, string(jsonBytes))
		require.NoError(t, err)

		var data string
		err = db.QueryRow(`SELECT data FROM variant_large_test WHERE id = 2`).Scan(&data)
		require.NoError(t, err)

		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Len(t, parsed, 100)
	})
}
