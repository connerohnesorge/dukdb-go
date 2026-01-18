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

// TestJSONColumnIntegration tests JSON column operations end-to-end with database/sql.
func TestJSONColumnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	t.Run("Insert and select JSON object", func(t *testing.T) {
		jsonData := `{"name":"Alice","age":30,"active":true}`
		_, err := db.Exec(`INSERT INTO json_test VALUES (1, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 1`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 1, id)

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
		_, err := db.Exec(`INSERT INTO json_test VALUES (2, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 2`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 2, id)

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

	t.Run("Insert and select JSON string primitive", func(t *testing.T) {
		jsonData := `"hello world"`
		_, err := db.Exec(`INSERT INTO json_test VALUES (3, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 3`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 3, id)

		// Verify the JSON can be parsed back
		var parsed string
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "hello world", parsed)
	})

	t.Run("Insert and select JSON number primitive", func(t *testing.T) {
		jsonData := `42.5`
		_, err := db.Exec(`INSERT INTO json_test VALUES (4, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 4`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 4, id)

		// Verify the JSON can be parsed back
		var parsed float64
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, 42.5, parsed)
	})

	t.Run("Insert and select JSON boolean primitives", func(t *testing.T) {
		// Insert true
		_, err := db.Exec(`INSERT INTO json_test VALUES (5, $1)`, `true`)
		require.NoError(t, err)

		// Insert false
		_, err = db.Exec(`INSERT INTO json_test VALUES (6, $1)`, `false`)
		require.NoError(t, err)

		// Verify true
		var data string
		err = db.QueryRow(`SELECT data FROM json_test WHERE id = 5`).Scan(&data)
		require.NoError(t, err)
		var parsedTrue bool
		err = json.Unmarshal([]byte(data), &parsedTrue)
		require.NoError(t, err)
		assert.True(t, parsedTrue)

		// Verify false
		err = db.QueryRow(`SELECT data FROM json_test WHERE id = 6`).Scan(&data)
		require.NoError(t, err)
		var parsedFalse bool
		err = json.Unmarshal([]byte(data), &parsedFalse)
		require.NoError(t, err)
		assert.False(t, parsedFalse)
	})

	t.Run("Insert and select JSON null primitive", func(t *testing.T) {
		jsonData := `null`
		_, err := db.Exec(`INSERT INTO json_test VALUES (7, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 7`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 7, id)

		// Verify it is JSON null (not SQL NULL)
		var parsed any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Nil(t, parsed)
	})

	t.Run("Insert and select nested JSON", func(t *testing.T) {
		jsonData := `{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}],"count":2,"metadata":{"version":"1.0"}}`
		_, err := db.Exec(`INSERT INTO json_test VALUES (8, $1)`, jsonData)
		require.NoError(t, err)

		var id int
		var data string
		err = db.QueryRow(`SELECT id, data FROM json_test WHERE id = 8`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 8, id)

		// Verify the nested JSON structure
		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)

		users, ok := parsed["users"].([]any)
		require.True(t, ok)
		require.Len(t, users, 2)

		user1, ok := users[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(1), user1["id"])
		assert.Equal(t, "Alice", user1["name"])

		metadata, ok := parsed["metadata"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "1.0", metadata["version"])
	})
}

// TestJSONColumnNullHandling tests NULL handling in JSON columns.
func TestJSONColumnNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_null_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	t.Run("Insert SQL NULL into JSON column", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO json_null_test VALUES (1, NULL)`)
		require.NoError(t, err)

		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM json_null_test WHERE id = 1`).Scan(&id, &data)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.False(t, data.Valid, "SQL NULL should result in NullString.Valid = false")
	})

	t.Run("Insert SQL NULL using parameter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO json_null_test VALUES (2, $1)`, nil)
		require.NoError(t, err)

		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM json_null_test WHERE id = 2`).Scan(&id, &data)
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
		_, err := db.Exec(`INSERT INTO json_null_test VALUES (3, $1)`, `null`)
		require.NoError(t, err)

		// Query and check - JSON null is a valid string value
		var id int
		var data sql.NullString
		err = db.QueryRow(`SELECT id, data FROM json_null_test WHERE id = 3`).Scan(&id, &data)
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
		rows, err := db.Query(`SELECT id FROM json_null_test WHERE data IS NULL ORDER BY id`)
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
		rows, err := db.Query(`SELECT id FROM json_null_test WHERE data IS NOT NULL ORDER BY id`)
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

// TestJSONColumnMultipleRows tests JSON column with multiple rows.
func TestJSONColumnMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_multi_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	// Insert multiple rows with different JSON types
	testData := []struct {
		id   int
		json string
	}{
		{1, `{"type":"object"}`},
		{2, `[1,2,3]`},
		{3, `"string value"`},
		{4, `42`},
		{5, `true`},
		{6, `null`},
	}

	for _, td := range testData {
		_, err := db.Exec(`INSERT INTO json_multi_test VALUES ($1, $2)`, td.id, td.json)
		require.NoError(t, err)
	}

	// Query all rows
	rows, err := db.Query(`SELECT id, data FROM json_multi_test ORDER BY id`)
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

	require.Len(t, results, 6)

	// Verify each row
	for i, td := range testData {
		assert.Equal(t, td.id, results[i].id)
		// Parse both to compare as JSON values (to handle formatting differences)
		var expected, actual any
		err := json.Unmarshal([]byte(td.json), &expected)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(results[i].data), &actual)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

// TestJSONColumnPreparedStatement tests JSON column with prepared statements.
func TestJSONColumnPreparedStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_prep_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	// Prepare insert statement
	insertStmt, err := db.Prepare(`INSERT INTO json_prep_test VALUES ($1, $2)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, insertStmt.Close())
	}()

	// Insert multiple rows using prepared statement
	testData := []struct {
		id   int
		json string
	}{
		{1, `{"key":"value1"}`},
		{2, `{"key":"value2"}`},
		{3, `{"key":"value3"}`},
	}

	for _, td := range testData {
		_, err := insertStmt.Exec(td.id, td.json)
		require.NoError(t, err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare(`SELECT data FROM json_prep_test WHERE id = $1`)
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
		var expected, actual map[string]any
		err = json.Unmarshal([]byte(td.json), &expected)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(data), &actual)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

// TestJSONColumnTransaction tests JSON column operations within a transaction.
func TestJSONColumnTransaction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_tx_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	t.Run("Commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO json_tx_test VALUES (1, $1)`, `{"status":"committed"}`)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify data persisted
		var data string
		err = db.QueryRow(`SELECT data FROM json_tx_test WHERE id = 1`).Scan(&data)
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
			`INSERT INTO json_tx_test VALUES (2, $1)`,
			`{"status":"should_be_rolled_back"}`,
		)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify data was not persisted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM json_tx_test WHERE id = 2`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestJSONColumnUnicodeAndEscaping tests JSON column with special characters.
func TestJSONColumnUnicodeAndEscaping(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_unicode_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	testCases := []struct {
		id   int
		name string
		json string
	}{
		{1, "Unicode characters", `{"message":"Hello World"}`},
		{2, "Escaped quotes", `{"text":"He said \"hello\""}`},
		{3, "Newlines and tabs", `{"text":"line1\nline2\ttabbed"}`},
		{4, "Backslash", `{"path":"C:\\Users\\test"}`},
		{5, "Unicode escape", `{"emoji":"\u0048\u0065\u006c\u006c\u006f"}`},
		{6, "Chinese characters", `{"greeting":"Ni Hao"}`},
		{7, "Japanese characters", `{"greeting":"Kon'nichiwa"}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(`INSERT INTO json_unicode_test VALUES ($1, $2)`, tc.id, tc.json)
			require.NoError(t, err)

			var data string
			err = db.QueryRow(`SELECT data FROM json_unicode_test WHERE id = $1`, tc.id).Scan(&data)
			require.NoError(t, err)

			// Verify the JSON can be parsed
			var expected, actual any
			err = json.Unmarshal([]byte(tc.json), &expected)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(data), &actual)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})
	}
}

// TestJSONColumnUpdate tests updating JSON column values.
func TestJSONColumnUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert initial data
	_, err = db.Exec(`CREATE TABLE json_update_test (id INTEGER PRIMARY KEY, data JSON)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_update_test VALUES (1, $1)`, `{"version":1}`)
	require.NoError(t, err)

	// Update the JSON data
	result, err := db.Exec(
		`UPDATE json_update_test SET data = $1 WHERE id = 1`,
		`{"version":2,"updated":true}`,
	)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify the update
	var data string
	err = db.QueryRow(`SELECT data FROM json_update_test WHERE id = 1`).Scan(&data)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(data), &parsed)
	require.NoError(t, err)
	assert.Equal(t, float64(2), parsed["version"])
	assert.Equal(t, true, parsed["updated"])
}

// TestJSONColumnDelete tests deleting rows with JSON columns.
func TestJSONColumnDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert data
	_, err = db.Exec(`CREATE TABLE json_delete_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_delete_test VALUES (1, $1)`, `{"keep":false}`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO json_delete_test VALUES (2, $1)`, `{"keep":true}`)
	require.NoError(t, err)

	// Delete one row
	result, err := db.Exec(`DELETE FROM json_delete_test WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only one row remains
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM json_delete_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the correct row remains
	var data string
	err = db.QueryRow(`SELECT data FROM json_delete_test WHERE id = 2`).Scan(&data)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(data), &parsed)
	require.NoError(t, err)
	assert.Equal(t, true, parsed["keep"])
}
