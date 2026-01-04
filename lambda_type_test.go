package dukdb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambdaTypeConstant tests TYPE_LAMBDA constant and string representation.
func TestLambdaTypeConstant(t *testing.T) {
	t.Run("TYPE_LAMBDA value is 106", func(t *testing.T) {
		assert.Equal(t, Type(106), TYPE_LAMBDA)
	})

	t.Run("TYPE_LAMBDA.String() returns LAMBDA", func(t *testing.T) {
		s := TYPE_LAMBDA.String()
		assert.Equal(t, "LAMBDA", s)
	})

	t.Run("TYPE_LAMBDA.Category() returns other", func(t *testing.T) {
		category := TYPE_LAMBDA.Category()
		assert.Equal(t, "other", category)
	})
}

// TestLambdaTypeInfo tests the NewLambdaInfo() function and TypeInfo interface.
func TestLambdaTypeInfo(t *testing.T) {
	t.Run("NewLambdaInfo returns correct TypeInfo", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)
		require.NotNil(t, info)
	})

	t.Run("InternalType returns TYPE_LAMBDA", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)
		assert.Equal(t, TYPE_LAMBDA, info.InternalType())
	})

	t.Run("SQLType returns LAMBDA", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)
		assert.Equal(t, "LAMBDA", info.SQLType())
	})

	t.Run("Details returns LambdaDetails", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		lambdaDetails, ok := details.(*LambdaDetails)
		require.True(t, ok, "Details should return *LambdaDetails")
		require.NotNil(t, lambdaDetails)
	})

	t.Run("LambdaDetails has InputTypes and ReturnType fields", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		details := info.Details()
		lambdaDetails, ok := details.(*LambdaDetails)
		require.True(t, ok)

		// Check that fields exist (they may be nil/empty by default)
		// InputTypes should be a slice of TypeInfo
		assert.NotNil(t, lambdaDetails) // Just verify the struct exists
		// The fields InputTypes and ReturnType are part of the struct definition
		_ = lambdaDetails.InputTypes
		_ = lambdaDetails.ReturnType
	})
}

// TestLambdaNewTypeInfo tests that NewTypeInfo(TYPE_LAMBDA) works correctly.
func TestLambdaNewTypeInfo(t *testing.T) {
	t.Run("NewTypeInfo with TYPE_LAMBDA works", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_LAMBDA)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, TYPE_LAMBDA, info.InternalType())
	})

	t.Run("NewTypeInfo TYPE_LAMBDA returns correct SQLType", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_LAMBDA)
		require.NoError(t, err)
		assert.Equal(t, "LAMBDA", info.SQLType())
	})
}

// TestLambdaVector tests vector operations for the LAMBDA type.
func TestLambdaVector(t *testing.T) {
	t.Run("Create vector with LAMBDA type", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		require.NotNil(t, vec)

		err = vec.init(info, 0)
		require.NoError(t, err)
		assert.Equal(t, TYPE_LAMBDA, vec.Type)
	})

	t.Run("Set and get simple lambda expression x -> x + 1", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set lambda expression string
		lambdaExpr := "x -> x + 1"
		err = vec.setFn(vec, 0, lambdaExpr)
		require.NoError(t, err)

		// Get value back
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, "x -> x + 1", result)
	})

	t.Run("Set and get complex lambda expression (x, y) -> x * y", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set more complex lambda expression
		lambdaExpr := "(x, y) -> x * y"
		err = vec.setFn(vec, 0, lambdaExpr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)
		assert.Equal(t, "(x, y) -> x * y", result)
	})

	t.Run("Test NULL values", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set a value first
		err = vec.setFn(vec, 0, "x -> x")
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

	t.Run("Test Reset works on LAMBDA vector", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set some values
		err = vec.setFn(vec, 0, "x -> x + 1")
		require.NoError(t, err)
		err = vec.setFn(vec, 1, "y -> y * 2")
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
		result := vec.getFn(vec, 0)
		assert.Equal(t, "", result)
	})
}

// TestLambdaVectorErrorCases tests error cases for LAMBDA vector operations.
func TestLambdaVectorErrorCases(t *testing.T) {
	t.Run("Setting non-string value (int) returns error", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Attempt to set an integer value
		err = vec.setFn(vec, 0, 42)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})

	t.Run("Setting non-string value (map) returns error", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Attempt to set a map value
		err = vec.setFn(vec, 0, map[string]any{"key": "value"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})

	t.Run("Setting non-string value (slice) returns error", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Attempt to set a slice value
		err = vec.setFn(vec, 0, []string{"x", "y"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})

	t.Run("Setting non-string value (bool) returns error", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Attempt to set a boolean value
		err = vec.setFn(vec, 0, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})

	t.Run("Setting non-string value (float) returns error", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Attempt to set a float value
		err = vec.setFn(vec, 0, 3.14)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})
}

// TestLambdaEdgeCases tests edge cases for LAMBDA type.
func TestLambdaEdgeCases(t *testing.T) {
	t.Run("Empty string expression", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set empty string
		err = vec.setFn(vec, 0, "")
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		assert.Equal(t, "", result)
	})

	t.Run("Multiple values in vector", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set multiple lambda expressions
		expressions := []string{
			"x -> x",
			"x -> x + 1",
			"(x, y) -> x + y",
			"(a, b, c) -> a * b + c",
			"n -> n * n",
		}

		for i, expr := range expressions {
			err = vec.setFn(vec, i, expr)
			require.NoError(t, err, "setting value at index %d", i)
		}

		// Verify all values
		for i, expr := range expressions {
			result := vec.getFn(vec, i)
			assert.Equal(t, expr, result, "value at index %d", i)
		}
	})

	t.Run("Very long lambda expression", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Create a very long lambda expression
		var sb strings.Builder
		sb.WriteString("(")
		for i := 0; i < 100; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("x")
			sb.WriteString(string(rune('0' + i%10)))
		}
		sb.WriteString(") -> x0")
		for i := 1; i < 100; i++ {
			sb.WriteString(" + x")
			sb.WriteString(string(rune('0' + i%10)))
		}

		longExpr := sb.String()
		err = vec.setFn(vec, 0, longExpr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		assert.Equal(t, longExpr, result)
	})

	t.Run("Lambda expression with special characters", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Lambda with special characters
		specialExpr := "(x, y) -> x >= y AND x <= 100"
		err = vec.setFn(vec, 0, specialExpr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		assert.Equal(t, specialExpr, result)
	})

	t.Run("Lambda expression with nested function calls", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Lambda with nested function calls
		nestedExpr := "x -> upper(trim(x))"
		err = vec.setFn(vec, 0, nestedExpr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		assert.Equal(t, nestedExpr, result)
	})

	t.Run("Lambda expression with Unicode", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Lambda with Unicode characters
		unicodeExpr := "x -> x + 'hello'"
		err = vec.setFn(vec, 0, unicodeExpr)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		assert.Equal(t, unicodeExpr, result)
	})
}

// TestLambdaTypeDetailsInterface tests that LambdaDetails implements TypeDetails.
func TestLambdaTypeDetailsInterface(t *testing.T) {
	t.Run("LambdaDetails implements TypeDetails", func(t *testing.T) {
		details := &LambdaDetails{}

		// This call should not panic - it's the marker method for the interface
		details.isTypeDetails()

		// Verify it satisfies the interface
		var _ TypeDetails = details
	})

	t.Run("LambdaDetails fields are accessible", func(t *testing.T) {
		details := &LambdaDetails{
			InputTypes: nil,
			ReturnType: nil,
		}

		// Verify fields can be set and read
		assert.Nil(t, details.InputTypes)
		assert.Nil(t, details.ReturnType)
	})

	t.Run("LambdaDetails with populated fields", func(t *testing.T) {
		// Create some TypeInfo for testing
		intInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		stringInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		details := &LambdaDetails{
			InputTypes: []TypeInfo{intInfo, stringInfo},
			ReturnType: intInfo,
		}

		assert.Len(t, details.InputTypes, 2)
		assert.NotNil(t, details.ReturnType)
		assert.Equal(t, TYPE_INTEGER, details.InputTypes[0].InternalType())
		assert.Equal(t, TYPE_VARCHAR, details.InputTypes[1].InternalType())
		assert.Equal(t, TYPE_INTEGER, details.ReturnType.InternalType())
	})
}

// TestLambdaVectorCapacity tests LAMBDA vector with various capacities.
func TestLambdaVectorCapacity(t *testing.T) {
	t.Run("Small capacity vector", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(10)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Fill all slots
		for i := 0; i < 10; i++ {
			expr := "x -> x + " + string(rune('0'+i))
			err = vec.setFn(vec, i, expr)
			require.NoError(t, err)
		}

		// Verify all slots
		for i := 0; i < 10; i++ {
			result := vec.getFn(vec, i)
			expected := "x -> x + " + string(rune('0'+i))
			assert.Equal(t, expected, result, "index %d", i)
		}
	})

	t.Run("Default VectorSize capacity", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set values at boundaries
		err = vec.setFn(vec, 0, "first -> first")
		require.NoError(t, err)
		err = vec.setFn(vec, VectorSize-1, "last -> last")
		require.NoError(t, err)

		assert.Equal(t, "first -> first", vec.getFn(vec, 0))
		assert.Equal(t, "last -> last", vec.getFn(vec, VectorSize-1))
	})
}

// TestLambdaVectorNullHandling tests comprehensive NULL handling.
func TestLambdaVectorNullHandling(t *testing.T) {
	t.Run("Alternating NULL and non-NULL values", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set alternating NULL and non-NULL values
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				err = vec.setFn(vec, i, "x -> x")
				require.NoError(t, err)
			} else {
				err = vec.setFn(vec, i, nil)
				require.NoError(t, err)
			}
		}

		// Verify the pattern
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				assert.False(t, vec.isNull(i), "index %d should not be NULL", i)
				assert.Equal(t, "x -> x", vec.getFn(vec, i))
			} else {
				assert.True(t, vec.isNull(i), "index %d should be NULL", i)
				assert.Nil(t, vec.getFn(vec, i))
			}
		}
	})

	t.Run("Setting value then NULL then value again", func(t *testing.T) {
		info, err := NewLambdaInfo()
		require.NoError(t, err)

		vec := newVector(VectorSize)
		err = vec.init(info, 0)
		require.NoError(t, err)

		// Set initial value
		err = vec.setFn(vec, 0, "original -> original")
		require.NoError(t, err)
		assert.Equal(t, "original -> original", vec.getFn(vec, 0))

		// Set to NULL
		err = vec.setFn(vec, 0, nil)
		require.NoError(t, err)
		assert.Nil(t, vec.getFn(vec, 0))
		assert.True(t, vec.isNull(0))

		// Set new value
		err = vec.setFn(vec, 0, "new -> new")
		require.NoError(t, err)
		assert.Equal(t, "new -> new", vec.getFn(vec, 0))
		assert.False(t, vec.isNull(0))
	})
}
