package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJSONTypeInfo tests the NewJSONInfo() function and TypeInfo interface.
func TestJSONTypeInfo(t *testing.T) {
	t.Run("NewJSONInfo returns correct TypeInfo", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)
		require.NotNil(t, info)
	})

	t.Run("InternalType returns TYPE_JSON", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)
		assert.Equal(t, TYPE_JSON, info.InternalType())
	})

	t.Run("SQLType returns JSON", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)
		assert.Equal(t, "JSON", info.SQLType())
	})

	t.Run("Details returns JSONDetails", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		jsonDetails, ok := details.(*JSONDetails)
		require.True(t, ok, "Details should return *JSONDetails")
		require.NotNil(t, jsonDetails)
	})
}

// TestJSONNewTypeInfo tests that NewTypeInfo(TYPE_JSON) works correctly.
func TestJSONNewTypeInfo(t *testing.T) {
	t.Run("NewTypeInfo with TYPE_JSON works", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_JSON)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, TYPE_JSON, info.InternalType())
	})

	t.Run("NewTypeInfo TYPE_JSON returns correct SQLType", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_JSON)
		require.NoError(t, err)
		assert.Equal(t, "JSON", info.SQLType())
	})
}

// TestJSONTypeCategory tests that TYPE_JSON.Category() returns "string".
func TestJSONTypeCategory(t *testing.T) {
	t.Run("TYPE_JSON category is string", func(t *testing.T) {
		category := TYPE_JSON.Category()
		assert.Equal(t, "string", category)
	})
}

// TestJSONTypeString tests that TYPE_JSON.String() returns "JSON".
func TestJSONTypeString(t *testing.T) {
	t.Run("TYPE_JSON string representation is JSON", func(t *testing.T) {
		s := TYPE_JSON.String()
		assert.Equal(t, "JSON", s)
	})
}

// TestJSONVector tests vector operations for the JSON type.
func TestJSONVector(t *testing.T) {
	t.Run("Create vector with JSON type", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		require.NotNil(t, vec)

		err = vec.init(info, 0)
		require.NoError(t, err)
		assert.Equal(t, TYPE_JSON, vec.Type)
	})

	t.Run("Set and get JSON object string", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		jsonObj := `{"name":"Alice","age":30}`
		err = vec.setFn(vec, 0, jsonObj)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		// getFn parses JSON and returns Go types
		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "Alice", m["name"])
		assert.Equal(t, float64(30), m["age"])
	})

	t.Run("Set and get JSON array string", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		jsonArr := `[1,2,3,"four",null]`
		err = vec.setFn(vec, 0, jsonArr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		// getFn parses JSON and returns Go types
		arr, ok := result.([]any)
		require.True(t, ok, "expected []any, got %T", result)
		require.Len(t, arr, 5)
		assert.Equal(t, float64(1), arr[0])
		assert.Equal(t, float64(2), arr[1])
		assert.Equal(t, float64(3), arr[2])
		assert.Equal(t, "four", arr[3])
		assert.Nil(t, arr[4])
	})

	t.Run("Set and get JSON primitive string value", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		jsonStr := `"hello world"`
		err = vec.setFn(vec, 0, jsonStr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, "hello world", result)
	})

	t.Run("Set and get JSON primitive number value", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		jsonNum := `42.5`
		err = vec.setFn(vec, 0, jsonNum)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, float64(42.5), result)
	})

	t.Run("Set and get JSON primitive boolean value", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Test true
		err = vec.setFn(vec, 0, `true`)
		require.NoError(t, err)
		result := vec.getFn(vec, 0)
		assert.Equal(t, true, result)

		// Test false
		err = vec.setFn(vec, 1, `false`)
		require.NoError(t, err)
		result = vec.getFn(vec, 1)
		assert.Equal(t, false, result)
	})

	t.Run("Set and get NULL values", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a value first to ensure it works
		err = vec.setFn(vec, 0, `{"key":"value"}`)
		require.NoError(t, err)

		// Now set NULL
		err = vec.setFn(vec, 0, nil)
		require.NoError(t, err)

		// Check that value is now NULL
		assert.True(t, vec.isNull(0), "value should be NULL after setting nil")
		result := vec.getFn(vec, 0)
		assert.Nil(t, result)
	})

	t.Run("getFn parses JSON and returns map for objects", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		complexJSON := `{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}],"count":2}`
		err = vec.setFn(vec, 0, complexJSON)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)

		users, ok := m["users"].([]any)
		require.True(t, ok, "expected []any for users")
		assert.Len(t, users, 2)

		count, ok := m["count"].(float64)
		require.True(t, ok, "expected float64 for count")
		assert.Equal(t, float64(2), count)
	})

	t.Run("setFn with Go map marshals to JSON", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set using a Go map
		goMap := map[string]any{
			"name": "Charlie",
			"age":  25,
		}
		err = vec.setFn(vec, 0, goMap)
		require.NoError(t, err)

		// Get back the value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "Charlie", m["name"])
		assert.Equal(t, float64(25), m["age"])
	})

	t.Run("setFn with Go slice marshals to JSON", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set using a Go slice
		goSlice := []any{1, 2, 3, "four"}
		err = vec.setFn(vec, 0, goSlice)
		require.NoError(t, err)

		// Get back the value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		arr, ok := result.([]any)
		require.True(t, ok, "expected []any, got %T", result)
		assert.Len(t, arr, 4)
		assert.Equal(t, float64(1), arr[0])
		assert.Equal(t, "four", arr[3])
	})

	t.Run("setFn with bytes marshals correctly", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set using bytes
		jsonBytes := []byte(`{"status":"ok"}`)
		err = vec.setFn(vec, 0, jsonBytes)
		require.NoError(t, err)

		// Get back the value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "ok", m["status"])
	})

	t.Run("Reset works on JSON vector", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set some values
		err = vec.setFn(vec, 0, `{"a":1}`)
		require.NoError(t, err)
		err = vec.setFn(vec, 1, `{"b":2}`)
		require.NoError(t, err)

		// Set one as NULL
		err = vec.setFn(vec, 2, nil)
		require.NoError(t, err)

		// Verify values are set
		assert.False(t, vec.isNull(0))
		assert.False(t, vec.isNull(1))
		assert.True(t, vec.isNull(2))

		// Reset the vector
		vec.Reset()

		// After reset, all values should be valid (not NULL) and empty
		assert.False(t, vec.isNull(0), "after Reset, value should be valid")
		assert.False(t, vec.isNull(1), "after Reset, value should be valid")
		assert.False(t, vec.isNull(2), "after Reset, value should be valid")

		// The underlying data should be empty strings after reset
		// When we get the value, it will try to parse empty string which will fail
		// and return the raw string
		result := vec.getFn(vec, 0)
		// Empty string JSON parse fails, so it returns the raw string
		assert.Equal(t, "", result)
	})

	t.Run("Multiple values in vector", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set multiple JSON values
		testCases := []struct {
			idx      int
			json     string
			expected any
		}{
			{0, `{"type":"object"}`, map[string]any{"type": "object"}},
			{1, `[1,2,3]`, []any{float64(1), float64(2), float64(3)}},
			{2, `"just a string"`, "just a string"},
			{3, `123`, float64(123)},
			{4, `true`, true},
			{5, `null`, nil},
		}

		for _, tc := range testCases {
			err = vec.setFn(vec, tc.idx, tc.json)
			require.NoError(t, err, "setting value at index %d", tc.idx)
		}

		for _, tc := range testCases {
			result := vec.getFn(vec, tc.idx)
			assert.Equal(t, tc.expected, result, "value at index %d", tc.idx)
		}
	})
}

// TestJSONVectorInvalidJSON tests behavior with invalid JSON.
func TestJSONVectorInvalidJSON(t *testing.T) {
	t.Run("Invalid JSON returns raw string on get", func(t *testing.T) {
		info, err := NewJSONInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set invalid JSON as a raw string
		invalidJSON := `{not valid json}`
		err = vec.setFn(vec, 0, invalidJSON)
		require.NoError(t, err)

		// Get should return the raw string since JSON parsing fails
		result := vec.getFn(vec, 0)
		assert.Equal(t, invalidJSON, result)
	})
}

// TestJSONTypeDetailsInterface tests that JSONDetails implements TypeDetails.
func TestJSONTypeDetailsInterface(t *testing.T) {
	t.Run("JSONDetails implements TypeDetails", func(t *testing.T) {
		details := &JSONDetails{}

		// This call should not panic - it's the marker method for the interface
		details.isTypeDetails()

		// Verify it satisfies the interface
		var _ TypeDetails = details
	})
}
