package iceberg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityTransform(t *testing.T) {
	transform := IdentityTransform{}

	assert.Equal(t, "identity", transform.Name())

	tests := []struct {
		input    any
		expected any
	}{
		{42, 42},
		{"hello", "hello"},
		{nil, nil},
		{int64(100), int64(100)},
		{3.14, 3.14},
	}

	for _, tc := range tests {
		result, err := transform.Apply(tc.input)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, result)
	}
}

func TestBucketTransform(t *testing.T) {
	transform := BucketTransform{NumBuckets: 16}

	assert.Equal(t, "bucket[16]", transform.Name())

	// Test nil input
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Test integer input
	result, err = transform.Apply(42)
	require.NoError(t, err)
	bucket, ok := result.(int)
	require.True(t, ok)
	assert.GreaterOrEqual(t, bucket, 0)
	assert.Less(t, bucket, 16)

	// Test string input
	result, err = transform.Apply("hello")
	require.NoError(t, err)
	bucket, ok = result.(int)
	require.True(t, ok)
	assert.GreaterOrEqual(t, bucket, 0)
	assert.Less(t, bucket, 16)

	// Test consistent hashing - same input should give same bucket
	result1, _ := transform.Apply("test")
	result2, _ := transform.Apply("test")
	assert.Equal(t, result1, result2)
}

func TestTruncateTransform_Integer(t *testing.T) {
	transform := TruncateTransform{Width: 10}

	assert.Equal(t, "truncate[10]", transform.Name())

	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{5, 0},
		{10, 10},
		{15, 10},
		{99, 90},
		{100, 100},
	}

	for _, tc := range tests {
		result, err := transform.Apply(tc.input)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, result)
	}
}

func TestTruncateTransform_Int64(t *testing.T) {
	transform := TruncateTransform{Width: 100}

	tests := []struct {
		input    int64
		expected int64
	}{
		{0, 0},
		{50, 0},
		{100, 100},
		{150, 100},
		{999, 900},
	}

	for _, tc := range tests {
		result, err := transform.Apply(tc.input)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, result)
	}
}

func TestTruncateTransform_String(t *testing.T) {
	transform := TruncateTransform{Width: 5}

	tests := []struct {
		input    string
		expected string
	}{
		{"hello world", "hello"},
		{"hi", "hi"},
		{"test", "test"},
		{"abcdef", "abcde"},
	}

	for _, tc := range tests {
		result, err := transform.Apply(tc.input)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, result)
	}
}

func TestTruncateTransform_Nil(t *testing.T) {
	transform := TruncateTransform{Width: 10}
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestYearTransform(t *testing.T) {
	transform := YearTransform{}

	assert.Equal(t, "year", transform.Name())

	// Test nil input
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Test time.Time input
	tm := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(tm)
	require.NoError(t, err)
	assert.Equal(t, 53, result) // 2023 - 1970 = 53

	// Test epoch year
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(epoch)
	require.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestMonthTransform(t *testing.T) {
	transform := MonthTransform{}

	assert.Equal(t, "month", transform.Name())

	// Test nil input
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Test time.Time input - June 2023
	tm := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(tm)
	require.NoError(t, err)
	// 53 years * 12 months + 5 (June is month 6, 0-indexed is 5)
	assert.Equal(t, 53*12+5, result)

	// Test epoch month
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(epoch)
	require.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestDayTransform(t *testing.T) {
	transform := DayTransform{}

	assert.Equal(t, "day", transform.Name())

	// Test nil input
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Test time.Time input
	// January 2, 1970 should be day 1
	day2 := time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(day2)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	// Test epoch day
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(epoch)
	require.NoError(t, err)
	assert.Equal(t, 0, result)

	// Test int32 input (days since epoch)
	result, err = transform.Apply(int32(100))
	require.NoError(t, err)
	assert.Equal(t, 100, result)
}

func TestHourTransform(t *testing.T) {
	transform := HourTransform{}

	assert.Equal(t, "hour", transform.Name())

	// Test nil input
	result, err := transform.Apply(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Test time.Time input
	// 1 hour after epoch
	hour1 := time.Date(1970, 1, 1, 1, 0, 0, 0, time.UTC)
	result, err = transform.Apply(hour1)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	// Test epoch hour
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err = transform.Apply(epoch)
	require.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestVoidTransform(t *testing.T) {
	transform := VoidTransform{}

	assert.Equal(t, "void", transform.Name())

	// Test various inputs - all should return nil
	inputs := []any{nil, 42, "hello", 3.14}
	for _, input := range inputs {
		result, err := transform.Apply(input)
		require.NoError(t, err)
		assert.Nil(t, result)
	}
}

func TestPartitionSpec_ComputePartitionValues(t *testing.T) {
	// Create a partition spec with identity transform
	spec := &PartitionSpec{
		ID: 0,
		Fields: []PartitionField{
			{
				FieldID:   1000,
				SourceID:  1,
				Name:      "category",
				Transform: IdentityTransform{},
			},
			{
				FieldID:   1001,
				SourceID:  2,
				Name:      "year",
				Transform: YearTransform{},
			},
		},
	}

	values := map[int]any{
		1: "electronics",
		2: time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC),
	}

	partValues, err := spec.ComputePartitionValues(values)
	require.NoError(t, err)

	assert.Equal(t, "electronics", partValues["category"])
	assert.Equal(t, 53, partValues["year"]) // 2023 - 1970
}

func TestPartitionSpec_IsUnpartitioned(t *testing.T) {
	// Unpartitioned spec
	unpartitioned := &PartitionSpec{
		ID:     0,
		Fields: []PartitionField{},
	}
	assert.True(t, unpartitioned.IsUnpartitioned())

	// Partitioned spec
	partitioned := &PartitionSpec{
		ID: 0,
		Fields: []PartitionField{
			{FieldID: 1000, SourceID: 1, Name: "col", Transform: IdentityTransform{}},
		},
	}
	assert.False(t, partitioned.IsUnpartitioned())
}

func TestPartitionEvaluator_EvaluateEquality(t *testing.T) {
	spec := &PartitionSpec{
		ID: 0,
		Fields: []PartitionField{
			{
				FieldID:   1000,
				SourceID:  1,
				Name:      "category",
				Transform: IdentityTransform{},
			},
		},
	}

	evaluator := NewPartitionEvaluator(spec)

	// Test with partition column
	result, err := evaluator.EvaluateEquality("category", "electronics")
	require.NoError(t, err)
	assert.True(t, result) // For now, always returns true (no pruning implemented yet)

	// Test with non-partition column
	result, err = evaluator.EvaluateEquality("other_column", "value")
	require.NoError(t, err)
	assert.True(t, result) // Non-partition column cannot be pruned
}

func TestInt32ToBytes(t *testing.T) {
	bytes := int32ToBytes(0x12345678)
	expected := []byte{0x78, 0x56, 0x34, 0x12} // Little endian
	assert.Equal(t, expected, bytes)
}

func TestInt64ToBytes(t *testing.T) {
	bytes := int64ToBytes(0x123456789ABCDEF0)
	expected := []byte{0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12} // Little endian
	assert.Equal(t, expected, bytes)
}
