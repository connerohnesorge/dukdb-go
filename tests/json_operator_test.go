package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/require"
)

func TestJSONOperatorExtract(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a table with JSON data
	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec(`INSERT INTO json_test VALUES
		(1, '{"name": "Alice", "age": 30}'),
		(2, '{"name": "Bob", "nested": {"city": "NYC"}}'),
		(3, '[1, 2, 3]')`)
	require.NoError(t, err)

	// Test -> operator (returns JSON)
	t.Run("extract JSON by key", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'name' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"Alice"`, result) // JSON string with quotes
	})

	// Test ->> operator (returns text)
	t.Run("extract text by key", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->>'name' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"Alice"`, result) // Returns the JSON string representation
	})

	// Test chained operators
	t.Run("chained JSON extraction", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'nested'->'city' FROM json_test WHERE id = 2`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"NYC"`, result)
	})

	// Test array index access
	t.Run("array index access", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->0 FROM json_test WHERE id = 3`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "1", result)
	})

	// Test NULL handling - key doesn't exist
	t.Run("missing key returns NULL", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'nonexistent' FROM json_test WHERE id = 1`)
		var result sql.NullString
		err := row.Scan(&result)
		require.NoError(t, err)
		require.False(t, result.Valid) // Should be NULL
	})

	// Test NULL data
	t.Run("NULL data returns NULL", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO json_test VALUES (4, NULL)`)
		require.NoError(t, err)

		row := db.QueryRow(`SELECT data->'name' FROM json_test WHERE id = 4`)
		var result sql.NullString
		err = row.Scan(&result)
		require.NoError(t, err)
		require.False(t, result.Valid)
	})
}

func TestJSONFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a table with JSON data
	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES
		(1, '{"name": "Alice", "address": {"city": "NYC"}}')`)
	require.NoError(t, err)

	// Test json_extract function
	t.Run("json_extract function", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_extract(data, 'name') FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"Alice"`, result)
	})

	// Test json_extract_string function
	t.Run("json_extract_string function", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_extract_string(data, 'name') FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"Alice"`, result)
	})

	// Test json_valid function - valid JSON
	t.Run("json_valid with valid JSON", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_valid('{"a": 1}')`)
		var result bool
		err := row.Scan(&result)
		require.NoError(t, err)
		require.True(t, result)
	})

	// Test json_valid function - invalid JSON
	t.Run("json_valid with invalid JSON", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_valid('not json')`)
		var result bool
		err := row.Scan(&result)
		require.NoError(t, err)
		require.False(t, result)
	})

	// Test json_valid function - NULL
	t.Run("json_valid with NULL", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_valid(NULL)`)
		var result sql.NullBool
		err := row.Scan(&result)
		require.NoError(t, err)
		require.False(t, result.Bool)
	})
}

func TestJSONPathExtraction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES
		(1, '{"users": [{"name": "Alice"}, {"name": "Bob"}]}')`)
	require.NoError(t, err)

	// Test path extraction with json_extract
	t.Run("nested path extraction", func(t *testing.T) {
		row := db.QueryRow(`SELECT json_extract(data, 'users') FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Contains(t, result, "Alice")
		require.Contains(t, result, "Bob")
	})
}

func TestJSONOperatorWithInvalidJSON(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES (1, 'not valid json')`)
	require.NoError(t, err)

	// Invalid JSON should return NULL, not error
	t.Run("invalid JSON returns NULL", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'key' FROM json_test WHERE id = 1`)
		var result sql.NullString
		err := row.Scan(&result)
		require.NoError(t, err)
		require.False(t, result.Valid)
	})
}

func TestJSONOperatorNumbers(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES (1, '{"count": 42, "price": 19.99}')`)
	require.NoError(t, err)

	// Test extracting integer
	t.Run("extract integer", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'count' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "42", result)
	})

	// Test extracting float
	t.Run("extract float", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'price' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "19.99", result)
	})
}

func TestJSONOperatorBoolean(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES (1, '{"active": true, "deleted": false}')`)
	require.NoError(t, err)

	t.Run("extract boolean true", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'active' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "true", result)
	})

	t.Run("extract boolean false", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'deleted' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "false", result)
	})
}

func TestJSONOperatorNestedObjects(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER, data VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO json_test VALUES (1, '{"user": {"profile": {"name": "Test"}}}')`)
	require.NoError(t, err)

	t.Run("extract nested object", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'user'->'profile' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `{"name":"Test"}`, result)
	})

	t.Run("deeply nested extraction", func(t *testing.T) {
		row := db.QueryRow(`SELECT data->'user'->'profile'->'name' FROM json_test WHERE id = 1`)
		var result string
		err := row.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, `"Test"`, result)
	})
}
