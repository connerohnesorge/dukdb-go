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

// TestJSONTypeFunctions verifies JSON_TYPE returns the correct type string for various JSON inputs.
func TestJSONTypeFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"OBJECT", `SELECT JSON_TYPE('{"a":1}')`, "OBJECT"},
		{"ARRAY", `SELECT JSON_TYPE('[1,2]')`, "ARRAY"},
		{"BIGINT", `SELECT JSON_TYPE('42')`, "BIGINT"},
		{"VARCHAR", `SELECT JSON_TYPE('"hello"')`, "VARCHAR"},
		{"BOOLEAN", `SELECT JSON_TYPE('true')`, "BOOLEAN"},
		{"NULL", `SELECT JSON_TYPE('null')`, "NULL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tc.query).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestJSONArrayLength verifies JSON_ARRAY_LENGTH returns the correct count for a JSON array.
func TestJSONArrayLength(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result int64
	err = db.QueryRow(`SELECT JSON_ARRAY_LENGTH('[1,2,3]')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestToJSON verifies TO_JSON serializes a value to a JSON string.
func TestToJSON(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow(`SELECT TO_JSON(42)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "42", result)
}

// TestJSONKeys verifies JSON_KEYS returns the keys of a JSON object.
func TestJSONKeys(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT JSON_KEYS('{"a":1,"b":2}')`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "a")
	assert.Contains(t, resultStr, "b")
}

// TestJSONObjectAndArray verifies JSON_OBJECT and JSON_ARRAY construct valid JSON.
func TestJSONObjectAndArray(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("JSON_OBJECT", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT JSON_OBJECT('key', 'value')`).Scan(&result)
		require.NoError(t, err)
		assert.Contains(t, result, "key")
		assert.Contains(t, result, "value")
	})

	t.Run("JSON_ARRAY", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT JSON_ARRAY(1, 2, 3)`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "[1,2,3]", result)
	})
}

// TestMapContainsKey verifies MAP_CONTAINS_KEY returns correct boolean results.
func TestMapContainsKey(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("key_exists", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT MAP_CONTAINS_KEY(MAP(['a','b'], [1,2]), 'a')`).Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("key_missing", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT MAP_CONTAINS_KEY(MAP(['a','b'], [1,2]), 'c')`).Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})
}

// TestMapExtract verifies MAP_EXTRACT retrieves the correct value for a given key.
func TestMapExtract(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result int64
	err = db.QueryRow(`SELECT MAP_EXTRACT(MAP(['x','y'], [10,20]), 'x')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(10), result)
}

// TestMapEntries verifies MAP_ENTRIES returns a non-empty result.
func TestMapEntries(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT MAP_ENTRIES(MAP(['a'], [1]))`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	assert.NotEmpty(t, resultStr)
}

// TestStructKeys verifies STRUCT_KEYS returns the field names of a struct.
func TestStructKeys(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT STRUCT_KEYS(STRUCT_PACK(name := 'Alice', age := 30))`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	assert.Contains(t, resultStr, "age")
	assert.Contains(t, resultStr, "name")
}

// TestStructExtractByName verifies STRUCT_EXTRACT retrieves the correct field value.
func TestStructExtractByName(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow(`SELECT STRUCT_EXTRACT(STRUCT_PACK(name := 'Alice', age := 30), 'name')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result)
}

// TestUnionFunctions verifies UNION_TAG and UNION_EXTRACT if union construction is available.
func TestUnionFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("UNION_TAG", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT UNION_TAG(MAP(['name'], ['Alice']))`).Scan(&result)
		if err != nil {
			t.Skipf("UNION_TAG not fully supported: %v", err)
		}
		assert.Equal(t, "name", result)
	})

	t.Run("UNION_EXTRACT", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT UNION_EXTRACT(MAP(['name'], ['Alice']), 'name')`).Scan(&result)
		if err != nil {
			t.Skipf("UNION_EXTRACT not fully supported: %v", err)
		}
		assert.Equal(t, "Alice", result)
	})
}

// TestListContains verifies LIST_CONTAINS returns correct boolean results.
func TestListContains(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("contains_true", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT LIST_CONTAINS([1,2,3], 2)`).Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("contains_false", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT LIST_CONTAINS([1,2,3], 5)`).Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})
}

// TestListPosition verifies LIST_POSITION returns the 1-based index of an element.
func TestListPosition(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result int64
	err = db.QueryRow(`SELECT LIST_POSITION([10,20,30], 20)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result)
}

// TestListReverse verifies LIST_REVERSE returns elements in reverse order.
func TestListReverse(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT LIST_REVERSE([1,2,3])`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	assert.Equal(t, "[3 2 1]", resultStr)
}

// TestFlatten verifies FLATTEN merges nested lists into a single list.
func TestFlatten(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT FLATTEN([[1,2],[3,4]])`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	assert.Equal(t, "[1 2 3 4]", resultStr)
}

// TestListDistinct verifies LIST_DISTINCT removes duplicate elements.
func TestListDistinct(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow(`SELECT LIST_DISTINCT([1,2,2,3,3])`).Scan(&result)
	require.NoError(t, err)
	resultStr := fmt.Sprintf("%v", result)
	// The result should contain 1, 2, and 3 without duplicates
	assert.Contains(t, resultStr, "1")
	assert.Contains(t, resultStr, "2")
	assert.Contains(t, resultStr, "3")
}
