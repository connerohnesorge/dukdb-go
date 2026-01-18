// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains tests for delete file handling (positional and equality deletes).
package iceberg

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoOpDeleteApplier tests the no-op delete applier.
func TestNoOpDeleteApplier(t *testing.T) {
	applier := &NoOpDeleteApplier{}

	// Test HasDeletes
	assert.False(t, applier.HasDeletes())

	// Test LoadDeleteFiles
	assert.NoError(t, applier.LoadDeleteFiles(nil, nil))

	// Test ApplyDeletes with a test chunk
	chunk := createTestChunk(10)
	result, err := applier.ApplyDeletes(chunk, "test.parquet", 0)
	require.NoError(t, err)
	assert.Equal(t, 10, result.Count())

	// Test Close
	assert.NoError(t, applier.Close())
}

// TestPositionalDeleteApplier tests the positional delete applier.
func TestPositionalDeleteApplier(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")

	// Initially should not have deletes
	assert.False(t, applier.HasDeletes())

	// Manually add some deleted positions for testing
	applier.deletedPositions["/test/table/data/file1.parquet"] = []int64{2, 5, 8}
	applier.loaded = true

	// Now should have deletes
	assert.True(t, applier.HasDeletes())

	// Create a test chunk with 10 rows
	chunk := createTestChunk(10)

	// Apply deletes - rows 2, 5, 8 should be removed
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// Should have 7 rows remaining (10 - 3 deleted)
	assert.Equal(t, 7, result.Count())

	// Test Close
	assert.NoError(t, applier.Close())
	assert.False(t, applier.HasDeletes())
}

// TestPositionalDeleteApplierWithOffset tests deletes with row offset.
func TestPositionalDeleteApplierWithOffset(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")

	// Set up deletes at positions 100, 105, 110
	applier.deletedPositions["/test/table/data/file1.parquet"] = []int64{100, 105, 110}
	applier.loaded = true

	// Create a test chunk starting at position 100 with 15 rows
	chunk := createTestChunk(15)

	// Apply deletes - positions 100, 105, 110 map to local indices 0, 5, 10
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 100)
	require.NoError(t, err)

	// Should have 12 rows remaining (15 - 3 deleted)
	assert.Equal(t, 12, result.Count())
}

// TestPositionalDeleteApplierNoMatchingFile tests deletes for non-matching file.
func TestPositionalDeleteApplierNoMatchingFile(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")

	// Set up deletes for different file
	applier.deletedPositions["/test/table/data/other_file.parquet"] = []int64{0, 1, 2}
	applier.loaded = true

	// Create a test chunk
	chunk := createTestChunk(10)

	// Apply deletes for different file - should not affect chunk
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// All rows should remain
	assert.Equal(t, 10, result.Count())
}

// TestPositionalDeleteApplierAllRowsDeleted tests when all rows are deleted.
func TestPositionalDeleteApplierAllRowsDeleted(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")

	// Set up deletes for all rows
	applier.deletedPositions["/test/table/data/file1.parquet"] = []int64{0, 1, 2, 3, 4}
	applier.loaded = true

	// Create a test chunk with 5 rows
	chunk := createTestChunk(5)

	// Apply deletes - all rows should be removed
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// Should have 0 rows
	assert.Equal(t, 0, result.Count())
}

// TestEqualityDeleteApplier tests the equality delete applier.
func TestEqualityDeleteApplier(t *testing.T) {
	columnNames := []string{"id", "name", "value"}
	applier := NewEqualityDeleteApplier(nil, "/test/table", columnNames)

	// Initially should not have deletes
	assert.False(t, applier.HasDeletes())

	// Add delete records
	applier.deleteRecords = []EqualityDeleteRecord{
		{Values: map[string]any{"id": int64(2)}},
		{Values: map[string]any{"id": int64(5)}},
	}
	applier.loaded = true

	// Now should have deletes
	assert.True(t, applier.HasDeletes())

	// Create a test chunk with 10 rows where id = row index
	chunk := createTestChunkWithIDs(10)

	// Apply deletes - rows where id=2 and id=5 should be removed
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// Should have 8 rows remaining (10 - 2 deleted)
	assert.Equal(t, 8, result.Count())

	// Test Close
	assert.NoError(t, applier.Close())
	assert.False(t, applier.HasDeletes())
}

// TestEqualityDeleteApplierMultipleColumns tests equality deletes with multiple columns.
func TestEqualityDeleteApplierMultipleColumns(t *testing.T) {
	columnNames := []string{"id", "name", "value"}
	applier := NewEqualityDeleteApplier(nil, "/test/table", columnNames)

	// Add delete record requiring both id and name to match
	applier.deleteRecords = []EqualityDeleteRecord{
		{Values: map[string]any{"id": int64(2), "name": "name_2"}},
	}
	applier.loaded = true

	// Create a test chunk where row 2 has id=2 and name="name_2"
	chunk := createTestChunkWithIDs(10)

	// Apply deletes
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// Should have 9 rows remaining (row 2 matches and is deleted)
	assert.Equal(t, 9, result.Count())
}

// TestCompositeDeleteApplier tests the composite delete applier.
func TestCompositeDeleteApplier(t *testing.T) {
	columnNames := []string{"id", "name", "value"}
	applier := NewCompositeDeleteApplier(nil, "/test/table", columnNames)

	// Set up positional deletes
	applier.positional.deletedPositions["/test/table/data/file1.parquet"] = []int64{0}
	applier.positional.loaded = true

	// Set up equality deletes
	applier.equality.deleteRecords = []EqualityDeleteRecord{
		{Values: map[string]any{"id": int64(5)}},
	}
	applier.equality.loaded = true

	// Both should have deletes
	assert.True(t, applier.HasDeletes())

	// Create a test chunk
	chunk := createTestChunkWithIDs(10)

	// Apply deletes - row 0 (positional) and row 5 (equality) should be removed
	result, err := applier.ApplyDeletes(chunk, "/test/table/data/file1.parquet", 0)
	require.NoError(t, err)

	// Should have 8 rows remaining
	assert.Equal(t, 8, result.Count())

	// Test Close
	assert.NoError(t, applier.Close())
	assert.False(t, applier.HasDeletes())
}

// TestCreateDeleteApplier tests the CreateDeleteApplier factory function.
func TestCreateDeleteApplier(t *testing.T) {
	ctx := context.Background()

	// With no delete files, should return NoOp
	applier := CreateDeleteApplier(ctx, []*DataFile{}, nil, "/test/table", []string{})
	_, ok := applier.(*NoOpDeleteApplier)
	assert.True(t, ok, "Expected NoOpDeleteApplier for empty delete files")

	// With positional delete files, should return PositionalDeleteApplier
	positionalFiles := []*DataFile{
		{Path: "delete.parquet", Format: FileFormatParquet},
	}
	applier = CreateDeleteApplier(ctx, positionalFiles, nil, "/test/table", []string{})
	_, ok = applier.(*PositionalDeleteApplier)
	assert.True(t, ok, "Expected PositionalDeleteApplier for positional delete files")

	// With equality delete files, should return EqualityDeleteApplier
	equalityFiles := []*DataFile{
		{Path: "delete.parquet", Format: FileFormatParquet, EqualityFieldIDs: []int{1}},
	}
	applier = CreateDeleteApplier(ctx, equalityFiles, nil, "/test/table", []string{"id"})
	_, ok = applier.(*EqualityDeleteApplier)
	assert.True(t, ok, "Expected EqualityDeleteApplier for equality delete files")

	// With both types, should return CompositeDeleteApplier
	mixedFiles := []*DataFile{
		{Path: "pos_delete.parquet", Format: FileFormatParquet},
		{Path: "eq_delete.parquet", Format: FileFormatParquet, EqualityFieldIDs: []int{1}},
	}
	applier = CreateDeleteApplier(ctx, mixedFiles, nil, "/test/table", []string{"id"})
	_, ok = applier.(*CompositeDeleteApplier)
	assert.True(t, ok, "Expected CompositeDeleteApplier for mixed delete files")
}

// TestIsDeleteSupported tests that delete support is now enabled.
func TestIsDeleteSupported(t *testing.T) {
	assert.True(t, IsDeleteSupported())
}

// TestSummarizeDeleteFiles tests the delete file summary function.
func TestSummarizeDeleteFiles(t *testing.T) {
	deleteFiles := []*DataFile{
		{Path: "pos1.parquet", RecordCount: 100},
		{Path: "pos2.parquet", RecordCount: 50},
		{Path: "eq1.parquet", RecordCount: 25, EqualityFieldIDs: []int{1}},
	}

	summary := SummarizeDeleteFiles(deleteFiles)

	assert.Equal(t, 3, summary.TotalDeleteFiles)
	assert.Equal(t, 2, summary.PositionalDeleteFiles)
	assert.Equal(t, 1, summary.EqualityDeleteFiles)
	assert.Equal(t, int64(175), summary.TotalDeleteRecords)
}

// TestNormalizeFilePath tests file path normalization.
func TestNormalizeFilePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"file:///path/to/file.parquet", "/path/to/file.parquet"},
		{"/path/to/file.parquet", "/path/to/file.parquet"},
		{"/path/to/../to/file.parquet", "/path/to/file.parquet"},
		{"./file.parquet", "file.parquet"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeFilePath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestValuesEqual tests value equality comparison with type coercion.
func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected bool
	}{
		{"nil_nil", nil, nil, true},
		{"nil_value", nil, 1, false},
		{"value_nil", 1, nil, false},
		{"same_int", 42, 42, true},
		{"different_int", 42, 43, false},
		{"int_int64", int(42), int64(42), true},
		{"int64_int32", int64(42), int32(42), true},
		{"float_int", float64(42.0), int(42), true},
		{"string_equal", "hello", "hello", true},
		{"string_different", "hello", "world", false},
		{"bytes_equal", []byte("hello"), []byte("hello"), true},
		{"bytes_different", []byte("hello"), []byte("world"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valuesEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFilterChunkWithDeletes tests chunk filtering.
func TestFilterChunkWithDeletes(t *testing.T) {
	// Create a chunk with 10 rows
	chunk := createTestChunk(10)

	// Delete rows 0, 5, 9
	deletedIndices := map[int]bool{0: true, 5: true, 9: true}

	result, err := filterChunkWithDeletes(chunk, deletedIndices)
	require.NoError(t, err)

	// Should have 7 rows remaining
	assert.Equal(t, 7, result.Count())
}

// TestFilterChunkAllDeleted tests filtering when all rows are deleted.
func TestFilterChunkAllDeleted(t *testing.T) {
	chunk := createTestChunk(5)

	deletedIndices := map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true}

	result, err := filterChunkWithDeletes(chunk, deletedIndices)
	require.NoError(t, err)

	assert.Equal(t, 0, result.Count())
}

// TestDeleteApplierWithTracking tests the tracking wrapper.
func TestDeleteApplierWithTracking(t *testing.T) {
	inner := NewPositionalDeleteApplier(nil, "/test/table")
	inner.deletedPositions["/test/table/data/file1.parquet"] = []int64{
		5,
		2055,
	} // Position in first and second chunk
	inner.loaded = true

	tracking := NewDeleteApplierWithTracking(inner)
	tracking.SetCurrentFile("/test/table/data/file1.parquet")

	// First chunk (positions 0-2047)
	chunk1 := createTestChunk(2048)
	result1, err := tracking.ApplyDeletes(chunk1)
	require.NoError(t, err)
	assert.Equal(t, 2047, result1.Count()) // One delete at position 5

	// Second chunk (positions 2048-4095)
	chunk2 := createTestChunk(2048)
	result2, err := tracking.ApplyDeletes(chunk2)
	require.NoError(t, err)
	assert.Equal(t, 2047, result2.Count()) // One delete at position 2055 (local index 7)
}

// TestLegacyDeleteFileApplier tests the legacy adapter.
func TestLegacyDeleteFileApplier(t *testing.T) {
	inner := NewPositionalDeleteApplier(nil, "/test/table")
	inner.deletedPositions["/test/table/data/file1.parquet"] = []int64{3}
	inner.loaded = true

	legacy := NewLegacyDeleteFileApplier(inner)
	legacy.SetCurrentDataFile("/test/table/data/file1.parquet")

	chunk := createTestChunk(10)
	result, err := legacy.ApplyDeletes(chunk)
	require.NoError(t, err)

	assert.Equal(t, 9, result.Count()) // One row deleted

	// Check that it tracks position correctly
	assert.Equal(t, int64(10), legacy.currentOffset)
}

// TestGetDataFileContentType tests content type detection.
func TestGetDataFileContentType(t *testing.T) {
	tests := []struct {
		name     string
		df       *DataFile
		expected DataFileContentType
	}{
		{
			name:     "data_file",
			df:       &DataFile{Path: "data/file1.parquet"},
			expected: DataFileContentData,
		},
		{
			name:     "positional_delete_by_name",
			df:       &DataFile{Path: "data/delete-file.parquet"},
			expected: DataFileContentPositionDeletes,
		},
		{
			name:     "equality_delete_by_field_ids",
			df:       &DataFile{Path: "data/file.parquet", EqualityFieldIDs: []int{1, 2}},
			expected: DataFileContentEqualityDeletes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetDataFileContentType(tt.df)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper functions for tests

// createTestChunk creates a test DataChunk with the given number of rows.
func createTestChunk(rowCount int) *storage.DataChunk {
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(types, rowCount)

	for i := 0; i < rowCount; i++ {
		idVec := chunk.GetVector(0)
		nameVec := chunk.GetVector(1)
		valueVec := chunk.GetVector(2)

		idVec.SetValue(i, int64(i))
		nameVec.SetValue(i, "name_"+string(rune('0'+i%10)))
		valueVec.SetValue(i, float64(i)*1.5)
	}

	chunk.SetCount(rowCount)
	return chunk
}

// createTestChunkWithIDs creates a test DataChunk where id equals row index.
func createTestChunkWithIDs(rowCount int) *storage.DataChunk {
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(types, rowCount)

	for i := 0; i < rowCount; i++ {
		idVec := chunk.GetVector(0)
		nameVec := chunk.GetVector(1)
		valueVec := chunk.GetVector(2)

		idVec.SetValue(i, int64(i))
		nameVec.SetValue(i, "name_"+string(rune('0'+i%10)))
		valueVec.SetValue(i, float64(i)*1.5)
	}

	chunk.SetCount(rowCount)
	return chunk
}

// TestPositionalDeleteBinarySearch tests that binary search works correctly.
func TestPositionalDeleteBinarySearch(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")

	// Create a large set of deleted positions
	positions := make([]int64, 1000)
	for i := range positions {
		positions[i] = int64(i * 10) // Every 10th position: 0, 10, 20, ...
	}
	applier.deletedPositions["file.parquet"] = positions
	applier.loaded = true

	// Create a chunk starting at position 500 with 100 rows
	chunk := createTestChunk(100)

	// Should delete positions 500, 510, 520, 530, 540, 550, 560, 570, 580, 590
	// That's 10 deletes within the range [500, 600)
	result, err := applier.ApplyDeletes(chunk, "file.parquet", 500)
	require.NoError(t, err)

	// 100 - 10 = 90 rows remaining
	assert.Equal(t, 90, result.Count())
}

// TestPositionalDeleteEmptyChunk tests handling of empty chunks.
func TestPositionalDeleteEmptyChunk(t *testing.T) {
	applier := NewPositionalDeleteApplier(nil, "/test/table")
	applier.deletedPositions["file.parquet"] = []int64{0, 1, 2}
	applier.loaded = true

	// Create an empty chunk
	chunk := createTestChunk(0)

	result, err := applier.ApplyDeletes(chunk, "file.parquet", 0)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Count())
}

// TestEqualityDeleteNullHandling tests NULL value handling in equality deletes.
func TestEqualityDeleteNullHandling(t *testing.T) {
	columnNames := []string{"id", "name"}
	applier := NewEqualityDeleteApplier(nil, "/test/table", columnNames)

	// Delete where name is NULL
	applier.deleteRecords = []EqualityDeleteRecord{
		{Values: map[string]any{"name": nil}},
	}
	applier.loaded = true

	// Create a chunk with some NULL names
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 5)

	for i := 0; i < 5; i++ {
		chunk.GetVector(0).SetValue(i, int64(i))
		if i == 2 || i == 4 {
			chunk.GetVector(1).Validity().SetInvalid(i) // NULL
		} else {
			chunk.GetVector(1).SetValue(i, "name")
		}
	}
	chunk.SetCount(5)

	result, err := applier.ApplyDeletes(chunk, "file.parquet", 0)
	require.NoError(t, err)

	// Rows 2 and 4 have NULL names and should be deleted
	assert.Equal(t, 3, result.Count())
}
