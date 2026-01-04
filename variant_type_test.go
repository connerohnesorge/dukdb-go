package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVariantTypeConstant tests TYPE_VARIANT constant and string representation.
func TestVariantTypeConstant(t *testing.T) {
	t.Run("TYPE_VARIANT value is 109", func(t *testing.T) {
		assert.Equal(t, Type(109), TYPE_VARIANT)
	})

	t.Run("TYPE_VARIANT.String() returns VARIANT", func(t *testing.T) {
		s := TYPE_VARIANT.String()
		assert.Equal(t, "VARIANT", s)
	})

	t.Run("TYPE_VARIANT.Category() returns other", func(t *testing.T) {
		category := TYPE_VARIANT.Category()
		assert.Equal(t, "other", category)
	})
}

// TestVariantTypeInfo tests the NewVariantInfo() function and TypeInfo interface.
func TestVariantTypeInfo(t *testing.T) {
	t.Run("NewVariantInfo returns correct TypeInfo", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)
		require.NotNil(t, info)
	})

	t.Run("InternalType returns TYPE_VARIANT", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)
		assert.Equal(t, TYPE_VARIANT, info.InternalType())
	})

	t.Run("SQLType returns VARIANT", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)
		assert.Equal(t, "VARIANT", info.SQLType())
	})

	t.Run("Details returns VariantDetails", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		variantDetails, ok := details.(*VariantDetails)
		require.True(t, ok, "Details should return *VariantDetails")
		require.NotNil(t, variantDetails)
	})
}

// TestVariantNewTypeInfo tests that NewTypeInfo(TYPE_VARIANT) works correctly.
func TestVariantNewTypeInfo(t *testing.T) {
	t.Run("NewTypeInfo with TYPE_VARIANT works", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_VARIANT)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, TYPE_VARIANT, info.InternalType())
	})

	t.Run("NewTypeInfo TYPE_VARIANT returns correct SQLType", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_VARIANT)
		require.NoError(t, err)
		assert.Equal(t, "VARIANT", info.SQLType())
	})
}

// TestVariantVector tests vector operations for the VARIANT type.
func TestVariantVector(t *testing.T) {
	t.Run("Create vector with VARIANT type", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		require.NotNil(t, vec)

		err = vec.init(info, 0)
		require.NoError(t, err)
		assert.Equal(t, TYPE_VARIANT, vec.Type)
	})

	t.Run("Set and get integer values (stored as JSON, returned as float64)", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set integer value - will be marshaled to JSON
		err = vec.setFn(vec, 0, 42)
		require.NoError(t, err)

		// Get value - JSON numbers are unmarshaled as float64
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, float64(42), result)
	})

	t.Run("Set and get string values", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set raw string value that is valid JSON
		err = vec.setFn(vec, 0, `"hello world"`)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, "hello world", result)
	})

	t.Run("Set and get boolean values", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set true
		err = vec.setFn(vec, 0, true)
		require.NoError(t, err)
		result := vec.getFn(vec, 0)
		assert.Equal(t, true, result)

		// Set false
		err = vec.setFn(vec, 1, false)
		require.NoError(t, err)
		result = vec.getFn(vec, 1)
		assert.Equal(t, false, result)
	})

	t.Run("Set and get array/slice values", func(t *testing.T) {
		info, err := NewVariantInfo()
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
		assert.Equal(t, float64(2), arr[1])
		assert.Equal(t, float64(3), arr[2])
		assert.Equal(t, "four", arr[3])
	})

	t.Run("Set and get map/object values", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set using a Go map
		goMap := map[string]any{
			"name": "Alice",
			"age":  30,
		}
		err = vec.setFn(vec, 0, goMap)
		require.NoError(t, err)

		// Get back the value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "Alice", m["name"])
		assert.Equal(t, float64(30), m["age"])
	})

	t.Run("Set and get complex nested structures", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a complex nested structure
		complexData := map[string]any{
			"users": []any{
				map[string]any{"id": 1, "name": "Alice"},
				map[string]any{"id": 2, "name": "Bob"},
			},
			"metadata": map[string]any{
				"count":   2,
				"version": "1.0",
			},
		}
		err = vec.setFn(vec, 0, complexData)
		require.NoError(t, err)

		// Get back the value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)

		users, ok := m["users"].([]any)
		require.True(t, ok, "expected []any for users")
		assert.Len(t, users, 2)

		user1, ok := users[0].(map[string]any)
		require.True(t, ok, "expected map[string]any for user")
		assert.Equal(t, float64(1), user1["id"])
		assert.Equal(t, "Alice", user1["name"])

		metadata, ok := m["metadata"].(map[string]any)
		require.True(t, ok, "expected map[string]any for metadata")
		assert.Equal(t, float64(2), metadata["count"])
		assert.Equal(t, "1.0", metadata["version"])
	})

	t.Run("Test NULL values", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a value first
		err = vec.setFn(vec, 0, 42)
		require.NoError(t, err)
		assert.False(t, vec.isNull(0))

		// Now set NULL
		err = vec.setFn(vec, 0, nil)
		require.NoError(t, err)

		// Check that value is now NULL
		assert.True(t, vec.isNull(0), "value should be NULL after setting nil")
		result := vec.getFn(vec, 0)
		assert.Nil(t, result)
	})

	t.Run("Test JSON string input (already valid JSON)", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a JSON object string directly
		jsonObj := `{"status":"ok","code":200}`
		err = vec.setFn(vec, 0, jsonObj)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "ok", m["status"])
		assert.Equal(t, float64(200), m["code"])
	})

	t.Run("Test Reset works on VARIANT vector", func(t *testing.T) {
		info, err := NewVariantInfo()
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
}

// TestVariantEdgeCases tests edge cases for VARIANT type.
func TestVariantEdgeCases(t *testing.T) {
	t.Run("Setting a pre-formatted JSON string", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set JSON array string
		jsonArr := `[1,2,3]`
		err = vec.setFn(vec, 0, jsonArr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		arr, ok := result.([]any)
		require.True(t, ok, "expected []any, got %T", result)
		assert.Len(t, arr, 3)
		assert.Equal(t, float64(1), arr[0])
		assert.Equal(t, float64(2), arr[1])
		assert.Equal(t, float64(3), arr[2])
	})

	t.Run("Setting []byte that is valid JSON", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set JSON bytes
		jsonBytes := []byte(`{"key":"value"}`)
		err = vec.setFn(vec, 0, jsonBytes)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		m, ok := result.(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", result)
		assert.Equal(t, "value", m["key"])
	})

	t.Run("Setting []byte that is not valid JSON", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set non-JSON bytes
		plainBytes := []byte(`hello world`)
		err = vec.setFn(vec, 0, plainBytes)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		// The bytes will be marshaled as JSON string
		assert.Equal(t, "hello world", result)
	})

	t.Run("Multiple values in vector", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set multiple values of different types
		testCases := []struct {
			idx      int
			input    any
			expected any
		}{
			{0, 42, float64(42)},
			{1, "hello", "hello"},
			{2, true, true},
			{3, false, false},
			{4, []any{1, 2}, []any{float64(1), float64(2)}},
			{5, map[string]any{"a": "b"}, map[string]any{"a": "b"}},
		}

		for _, tc := range testCases {
			err = vec.setFn(vec, tc.idx, tc.input)
			require.NoError(t, err, "setting value at index %d", tc.idx)
		}

		for _, tc := range testCases {
			result := vec.getFn(vec, tc.idx)
			assert.Equal(t, tc.expected, result, "value at index %d", tc.idx)
		}
	})

	t.Run("Non-string non-JSON values marshal correctly", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a float64
		err = vec.setFn(vec, 0, float64(3.14159))
		require.NoError(t, err)
		result := vec.getFn(vec, 0)
		assert.Equal(t, float64(3.14159), result)

		// Set an int64
		err = vec.setFn(vec, 1, int64(9223372036854775807))
		require.NoError(t, err)
		result = vec.getFn(vec, 1)
		// Large int64 may lose precision when unmarshaled as float64
		assert.NotNil(t, result)
	})
}

// TestVariantTypeDetailsInterface tests that VariantDetails implements TypeDetails.
func TestVariantTypeDetailsInterface(t *testing.T) {
	t.Run("VariantDetails implements TypeDetails", func(t *testing.T) {
		details := &VariantDetails{}

		// This call should not panic - it's the marker method for the interface
		details.isTypeDetails()

		// Verify it satisfies the interface
		var _ TypeDetails = details
	})
}

// TestVariantVectorInvalidJSON tests behavior with invalid JSON.
func TestVariantVectorInvalidJSON(t *testing.T) {
	t.Run("Invalid JSON returns raw string on get", func(t *testing.T) {
		info, err := NewVariantInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a string that looks like invalid JSON but is valid JSON string content
		// Since setVariant checks if string is valid JSON first
		invalidJSON := `{not valid json}`
		err = vec.setFn(vec, 0, invalidJSON)
		require.NoError(t, err)

		// Get should return the raw string since JSON parsing fails
		result := vec.getFn(vec, 0)
		// The invalid JSON string is marshaled as a JSON string, then unmarshaled back
		// Since "{not valid json}" is not valid JSON, it will be marshaled as `"{not valid json}"`
		// Then when we get it, it will unmarshal to "{not valid json}"
		assert.Equal(t, "{not valid json}", result)
	})
}
