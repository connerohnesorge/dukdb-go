package tests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStructPack tests creating a struct using struct_pack.
func TestStructPack(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT struct_pack(name := 'Alice', age := 30)").Scan(&result)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The result should be a string representation of the struct (map)
	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "Alice")
	assert.Contains(t, resultStr, "30")
}

// TestStructExtract tests extracting a field from a struct.
func TestStructExtract(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var name any
	err = db.QueryRow("SELECT struct_extract(struct_pack(name := 'Alice', age := 30), 'name')").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
}

// TestStructExtractAge tests extracting an integer field from a struct.
func TestStructExtractAge(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var age any
	err = db.QueryRow("SELECT struct_extract(struct_pack(name := 'Alice', age := 30), 'age')").Scan(&age)
	require.NoError(t, err)
	assert.Equal(t, int64(30), age)
}

// TestStructExtractMissing tests extracting a non-existent field from a struct.
func TestStructExtractMissing(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT struct_extract(struct_pack(name := 'Alice'), 'missing')").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result)
}

// TestMapCreation tests creating a map using the MAP function.
func TestMapCreation(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT map(['a', 'b'], [1, 2])").Scan(&result)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "a")
	assert.Contains(t, resultStr, "b")
}

// TestMapKeys tests extracting keys from a map.
func TestMapKeys(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT map_keys(map(['x', 'y'], [10, 20]))").Scan(&result)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "x")
	assert.Contains(t, resultStr, "y")
}

// TestMapValues tests extracting values from a map.
func TestMapValues(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT map_values(map(['x', 'y'], [10, 20]))").Scan(&result)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "10")
	assert.Contains(t, resultStr, "20")
}

// TestElementAt tests accessing a map element by key.
func TestElementAt(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT element_at(map(['name', 'city'], ['Alice', 'NYC']), 'city')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "NYC", result)
}

// TestElementAtMissing tests accessing a non-existent map key.
func TestElementAtMissing(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT element_at(map(['name'], ['Alice']), 'missing')").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result)
}

// TestElementAtName tests accessing the 'name' key from a map.
func TestElementAtName(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT element_at(map(['name', 'city'], ['Alice', 'NYC']), 'name')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result)
}

// TestStructPackWithExpressions tests struct_pack with expression values, not just literals.
func TestStructPackWithExpressions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT struct_extract(struct_pack(sum := 1 + 2, product := 3 * 4), 'sum')").Scan(&result)
	require.NoError(t, err)
	// The expression 1+2 may evaluate to int64 or float64 depending on the engine
	assert.Contains(t, []any{int64(3), float64(3)}, result)
}

// TestMapWithIntegerValues tests MAP with integer values.
func TestMapWithIntegerValues(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT element_at(map(['a', 'b', 'c'], [100, 200, 300]), 'b')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(200), result)
}

// TestMapKeysOrder tests that map_keys returns keys in sorted order.
func TestMapKeysOrder(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT map_keys(map(['c', 'a', 'b'], [3, 1, 2]))").Scan(&result)
	require.NoError(t, err)

	resultStr := fmt.Sprintf("%v", result)
	// Keys should be sorted: [a b c]
	assert.Equal(t, "[a b c]", resultStr)
}

// TestNestedStructExtract tests nested struct_pack and struct_extract calls.
func TestNestedStructExtract(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT struct_extract(struct_pack(x := 'hello', y := 'world'), 'y')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "world", result)
}
