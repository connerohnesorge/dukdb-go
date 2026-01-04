package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLNullTypeConstant tests that TYPE_SQLNULL has the correct value.
func TestSQLNullTypeConstant(t *testing.T) {
	t.Run("TYPE_SQLNULL has value 36", func(t *testing.T) {
		assert.Equal(t, Type(36), TYPE_SQLNULL)
	})
}

// TestSQLNullTypeString tests that TYPE_SQLNULL.String() returns "SQLNULL".
func TestSQLNullTypeString(t *testing.T) {
	t.Run("TYPE_SQLNULL string representation is SQLNULL", func(t *testing.T) {
		s := TYPE_SQLNULL.String()
		assert.Equal(t, "SQLNULL", s)
	})
}

// TestSQLNullTypeCategory tests that TYPE_SQLNULL.Category() returns "other".
func TestSQLNullTypeCategory(t *testing.T) {
	t.Run("TYPE_SQLNULL category is other", func(t *testing.T) {
		category := TYPE_SQLNULL.Category()
		assert.Equal(t, "other", category)
	})
}

// TestSQLNullVector tests vector operations for the SQLNULL type.
func TestSQLNullVector(t *testing.T) {
	// Helper to create a SQLNULL vector using the internal typeInfo
	createSQLNullVector := func(capacity int) *vector {
		vec := newVector(capacity)
		// Manually create typeInfo for SQLNULL since NewTypeInfo returns an error
		info := &typeInfo{typ: TYPE_SQLNULL}
		err := vec.init(info, 0)
		require.NoError(t, err)
		return vec
	}

	t.Run("Create vector with SQLNULL type", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)
		require.NotNil(t, vec)
		assert.Equal(t, TYPE_SQLNULL, vec.Type)
	})

	t.Run("dataSlice is nil for SQLNULL", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)
		assert.Nil(t, vec.dataSlice, "SQLNULL type should have nil dataSlice")
	})

	t.Run("getFn always returns nil", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)

		// All values should return nil regardless of index
		for i := range 10 {
			result := vec.getFn(vec, i)
			assert.Nil(t, result, "getFn should always return nil for SQLNULL at index %d", i)
		}
	})

	t.Run("setFn with nil marks as NULL", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)

		err := vec.setFn(vec, 0, nil)
		require.NoError(t, err)
		assert.True(t, vec.isNull(0), "value should be marked as NULL after setting nil")
	})

	t.Run("setFn with string marks as NULL", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)

		err := vec.setFn(vec, 0, "hello")
		require.NoError(t, err)
		assert.True(t, vec.isNull(0), "value should be marked as NULL after setting string")
		assert.Nil(t, vec.getFn(vec, 0), "getFn should return nil after setting string")
	})

	t.Run("setFn with int marks as NULL", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)

		err := vec.setFn(vec, 0, 42)
		require.NoError(t, err)
		assert.True(t, vec.isNull(0), "value should be marked as NULL after setting int")
		assert.Nil(t, vec.getFn(vec, 0), "getFn should return nil after setting int")
	})

	t.Run("setFn with various types all mark as NULL", func(t *testing.T) {
		vec := createSQLNullVector(VectorSize)

		testValues := []any{
			nil,
			"string",
			42,
			3.14,
			true,
			[]byte("bytes"),
			map[string]any{"key": "value"},
		}

		for i, val := range testValues {
			err := vec.setFn(vec, i, val)
			require.NoError(t, err, "setFn should not error for value %v at index %d", val, i)
			assert.True(t, vec.isNull(i), "value should be marked as NULL at index %d", i)
		}
	})

	t.Run("isNull returns true for all rows after setting", func(t *testing.T) {
		vec := createSQLNullVector(10)

		// Set values at various indices
		_ = vec.setFn(vec, 0, "value0")
		_ = vec.setFn(vec, 3, 123)
		_ = vec.setFn(vec, 7, nil)

		// All set values should be NULL
		assert.True(t, vec.isNull(0))
		assert.True(t, vec.isNull(3))
		assert.True(t, vec.isNull(7))
	})

	t.Run("Vector Reset works correctly", func(t *testing.T) {
		vec := createSQLNullVector(10)

		// Set some values
		_ = vec.setFn(vec, 0, "value0")
		_ = vec.setFn(vec, 1, 42)
		_ = vec.setFn(vec, 2, nil)

		// Verify they are NULL
		assert.True(t, vec.isNull(0))
		assert.True(t, vec.isNull(1))
		assert.True(t, vec.isNull(2))

		// Reset the vector
		vec.Reset()

		// After reset, dataSlice should still be nil
		assert.Nil(t, vec.dataSlice, "dataSlice should remain nil after Reset")

		// After reset, values should be valid (Reset fills the mask)
		assert.False(t, vec.isNull(0), "after Reset, value should be valid (not NULL)")
		assert.False(t, vec.isNull(1), "after Reset, value should be valid (not NULL)")
		assert.False(t, vec.isNull(2), "after Reset, value should be valid (not NULL)")

		// getFn should still return nil for SQLNULL type
		assert.Nil(t, vec.getFn(vec, 0), "getFn should return nil even after Reset")
	})
}

// TestSQLNullTypeInfoError tests that NewTypeInfo(TYPE_SQLNULL) returns an error.
func TestSQLNullTypeInfoError(t *testing.T) {
	t.Run("NewTypeInfo with TYPE_SQLNULL returns error", func(t *testing.T) {
		info, err := NewTypeInfo(TYPE_SQLNULL)
		require.Error(t, err, "NewTypeInfo(TYPE_SQLNULL) should return an error")
		assert.Nil(t, info, "TypeInfo should be nil when error is returned")
	})

	t.Run("Error message indicates unsupported type", func(t *testing.T) {
		_, err := NewTypeInfo(TYPE_SQLNULL)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQLNULL", "error should mention SQLNULL")
	})
}

// TestSQLNullTypeSQLType tests that SQLType() returns "NULL" for TYPE_SQLNULL.
func TestSQLNullTypeSQLType(t *testing.T) {
	t.Run("SQLType returns NULL for TYPE_SQLNULL", func(t *testing.T) {
		// Create a typeInfo directly since NewTypeInfo returns error
		info := &typeInfo{typ: TYPE_SQLNULL}
		sqlType := info.SQLType()
		assert.Equal(t, "NULL", sqlType)
	})
}

// TestSQLNullTypeDetails tests that Details() returns nil for TYPE_SQLNULL.
func TestSQLNullTypeDetails(t *testing.T) {
	t.Run("Details returns nil for TYPE_SQLNULL", func(t *testing.T) {
		// Create a typeInfo directly since NewTypeInfo returns error
		info := &typeInfo{typ: TYPE_SQLNULL}
		details := info.Details()
		assert.Nil(t, details, "Details should return nil for SQLNULL")
	})
}

// TestSQLNullInternalType tests that InternalType() returns TYPE_SQLNULL.
func TestSQLNullInternalType(t *testing.T) {
	t.Run("InternalType returns TYPE_SQLNULL", func(t *testing.T) {
		// Create a typeInfo directly since NewTypeInfo returns error
		info := &typeInfo{typ: TYPE_SQLNULL}
		assert.Equal(t, TYPE_SQLNULL, info.InternalType())
	})
}

// TestSQLNullVectorClose tests that Close works correctly for SQLNULL vectors.
func TestSQLNullVectorClose(t *testing.T) {
	t.Run("Close works correctly for SQLNULL vector", func(t *testing.T) {
		vec := newVector(10)
		info := &typeInfo{typ: TYPE_SQLNULL}
		err := vec.init(info, 0)
		require.NoError(t, err)

		// Verify initial state
		assert.Nil(t, vec.dataSlice)
		assert.NotNil(t, vec.maskBits)

		// Close the vector
		vec.Close()

		// Verify closed state
		assert.Nil(t, vec.dataSlice)
		assert.Nil(t, vec.maskBits)
		assert.Equal(t, 0, vec.capacity)
	})
}
