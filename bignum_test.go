package dukdb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBignumTypeInfo(t *testing.T) {
	// Test NewBignumInfo
	info, err := NewBignumInfo()
	require.NoError(t, err)
	assert.Equal(t, TYPE_BIGNUM, info.InternalType())
	assert.Equal(t, "BIGNUM", info.SQLType())

	// Test Details returns BignumDetails
	details := info.Details()
	_, ok := details.(*BignumDetails)
	assert.True(t, ok, "Details should return *BignumDetails")

	// Test NewTypeInfo(TYPE_BIGNUM) works
	info2, err := NewTypeInfo(TYPE_BIGNUM)
	require.NoError(t, err)
	assert.Equal(t, TYPE_BIGNUM, info2.InternalType())
}

func TestBignumVector(t *testing.T) {
	// Create a BIGNUM vector
	info, err := NewBignumInfo()
	require.NoError(t, err)

	vec := newVector(10)
	err = vec.init(info, 0)
	require.NoError(t, err)

	// Test setting values with *big.Int
	bigVal := new(big.Int)
	bigVal.SetString("12345678901234567890", 10)
	err = vec.setFn(vec, 0, bigVal)
	require.NoError(t, err)

	// Test getting values
	result := vec.getFn(vec, 0)
	resultBig, ok := result.(*big.Int)
	require.True(t, ok)
	assert.Equal(t, bigVal, resultBig)

	// Test NULL handling
	err = vec.setFn(vec, 1, nil)
	require.NoError(t, err)
	assert.Nil(t, vec.getFn(vec, 1))

	// Test string conversion
	err = vec.setFn(vec, 2, "99999999999999999999")
	require.NoError(t, err)
	result2 := vec.getFn(vec, 2).(*big.Int)
	expected, _ := new(big.Int).SetString("99999999999999999999", 10)
	assert.Equal(t, expected, result2)

	// Test int64 conversion
	err = vec.setFn(vec, 3, int64(42))
	require.NoError(t, err)
	result3 := vec.getFn(vec, 3).(*big.Int)
	assert.Equal(t, big.NewInt(42), result3)

	// Test int conversion
	err = vec.setFn(vec, 4, 100)
	require.NoError(t, err)
	result4 := vec.getFn(vec, 4).(*big.Int)
	assert.Equal(t, big.NewInt(100), result4)

	// Test big.Int (non-pointer) conversion
	err = vec.setFn(vec, 5, *big.NewInt(200))
	require.NoError(t, err)
	result5 := vec.getFn(vec, 5).(*big.Int)
	assert.Equal(t, big.NewInt(200), result5)
}

func TestBignumVectorReset(t *testing.T) {
	// Create a BIGNUM vector
	info, err := NewBignumInfo()
	require.NoError(t, err)

	vec := newVector(10)
	err = vec.init(info, 0)
	require.NoError(t, err)

	// Set some values
	err = vec.setFn(vec, 0, big.NewInt(123))
	require.NoError(t, err)
	err = vec.setFn(vec, 1, big.NewInt(456))
	require.NoError(t, err)

	// Reset the vector
	vec.Reset()

	// Verify data is cleared
	data := vec.dataSlice.([]*big.Int)
	for i := range data {
		assert.Nil(t, data[i], "data[%d] should be nil after reset", i)
	}
}

func TestBignumInvalidInput(t *testing.T) {
	// Create a BIGNUM vector
	info, err := NewBignumInfo()
	require.NoError(t, err)

	vec := newVector(10)
	err = vec.init(info, 0)
	require.NoError(t, err)

	// Test invalid string
	err = vec.setFn(vec, 0, "not a number")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot parse")

	// Test unsupported type
	err = vec.setFn(vec, 0, 3.14)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot convert")
}
