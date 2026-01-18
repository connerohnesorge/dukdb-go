package storage

import (
	"runtime"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

// TestRowIDGeneration tests that RowIDs are generated monotonically
func TestRowIDGeneration(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert 5 rows
	for i := 0; i < 5; i++ {
		err := table.AppendRow([]any{int32(i), "test"})
		if err != nil {
			t.Fatalf("Failed to append row: %v", err)
		}
	}

	// Check that nextRowID is now 5
	if table.NextRowID() != 5 {
		t.Errorf("Expected nextRowID to be 5, got %d", table.NextRowID())
	}

	// Check that RowIDs were assigned sequentially
	for i := 0; i < 5; i++ {
		rowID := RowID(i)
		if !table.ContainsRow(rowID) {
			t.Errorf("Row with RowID %d should exist", rowID)
		}

		row := table.GetRow(rowID)
		if row == nil {
			t.Errorf("Failed to get row with RowID %d", rowID)
			continue
		}

		if row[0].(int32) != int32(i) {
			t.Errorf("Expected row %d to have value %d, got %v", i, i, row[0])
		}
	}
}

// TestRowIDGenerationWithChunk tests RowID generation when using AppendChunk
func TestRowIDGenerationWithChunk(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Create a chunk with 3 rows
	chunk := NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})
	chunk.AppendRow([]any{int32(1), "row1"})
	chunk.AppendRow([]any{int32(2), "row2"})
	chunk.AppendRow([]any{int32(3), "row3"})

	err := table.AppendChunk(chunk)
	if err != nil {
		t.Fatalf("Failed to append chunk: %v", err)
	}

	// Check that nextRowID is now 3
	if table.NextRowID() != 3 {
		t.Errorf("Expected nextRowID to be 3, got %d", table.NextRowID())
	}

	// Verify all rows exist
	for i := 0; i < 3; i++ {
		rowID := RowID(i)
		if !table.ContainsRow(rowID) {
			t.Errorf("Row with RowID %d should exist", rowID)
		}
	}
}

// TestTombstoneMarking tests that DeleteRows marks rows as deleted
func TestTombstoneMarking(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert 5 rows
	for i := 0; i < 5; i++ {
		err := table.AppendRow([]any{int32(i), "test"})
		if err != nil {
			t.Fatalf("Failed to append row: %v", err)
		}
	}

	// Delete rows 1 and 3
	err := table.DeleteRows([]RowID{1, 3})
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Check that rows 1 and 3 are marked as deleted
	if table.ContainsRow(1) {
		t.Error("Row 1 should be marked as deleted")
	}
	if table.ContainsRow(3) {
		t.Error("Row 3 should be marked as deleted")
	}

	// Check that other rows still exist
	if !table.ContainsRow(0) {
		t.Error("Row 0 should still exist")
	}
	if !table.ContainsRow(2) {
		t.Error("Row 2 should still exist")
	}
	if !table.ContainsRow(4) {
		t.Error("Row 4 should still exist")
	}

	// Verify GetRow returns nil for deleted rows
	if table.GetRow(1) != nil {
		t.Error("GetRow should return nil for deleted row 1")
	}
	if table.GetRow(3) != nil {
		t.Error("GetRow should return nil for deleted row 3")
	}

	// Verify GetRow returns data for non-deleted rows
	row0 := table.GetRow(0)
	if row0 == nil || row0[0].(int32) != 0 {
		t.Error("GetRow should return data for non-deleted row 0")
	}
}

// TestScannerSkipsTombstones tests that the TableScanner skips deleted rows
func TestScannerSkipsTombstones(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert 10 rows
	for i := 0; i < 10; i++ {
		err := table.AppendRow([]any{int32(i), "test"})
		if err != nil {
			t.Fatalf("Failed to append row: %v", err)
		}
	}

	// Delete rows 2, 5, and 7
	err := table.DeleteRows([]RowID{2, 5, 7})
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Scan the table and count rows
	scanner := table.Scan()
	rowCount := 0
	seenValues := make(map[int32]bool)

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0).(int32)
			seenValues[val] = true
			rowCount++
		}
	}

	// Should see 7 rows (10 - 3 deleted)
	if rowCount != 7 {
		t.Errorf("Expected to scan 7 rows, got %d", rowCount)
	}

	// Verify we didn't see deleted rows
	deletedValues := []int32{2, 5, 7}
	for _, val := range deletedValues {
		if seenValues[val] {
			t.Errorf("Scanner should not return deleted row with value %d", val)
		}
	}

	// Verify we saw all non-deleted rows
	expectedValues := []int32{0, 1, 3, 4, 6, 8, 9}
	for _, val := range expectedValues {
		if !seenValues[val] {
			t.Errorf("Scanner should return non-deleted row with value %d", val)
		}
	}
}

// TestUpdateRows tests in-place row updates
func TestUpdateRows(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert 5 rows
	for i := 0; i < 5; i++ {
		err := table.AppendRow([]any{int32(i), "original"})
		if err != nil {
			t.Fatalf("Failed to append row: %v", err)
		}
	}

	// Update rows 1 and 3 - change both columns
	updates := map[int]any{
		0: int32(100), // First column (integer)
		1: "updated",  // Second column (varchar)
	}
	err := table.UpdateRows([]RowID{1, 3}, updates)
	if err != nil {
		t.Fatalf("Failed to update rows: %v", err)
	}

	// Verify row 1 was updated
	row1 := table.GetRow(1)
	if row1 == nil {
		t.Fatal("Row 1 should exist")
	}
	if row1[0].(int32) != 100 {
		t.Errorf("Row 1 column 0 should be 100, got %v", row1[0])
	}
	if row1[1].(string) != "updated" {
		t.Errorf("Row 1 column 1 should be 'updated', got %v", row1[1])
	}

	// Verify row 3 was updated
	row3 := table.GetRow(3)
	if row3 == nil {
		t.Fatal("Row 3 should exist")
	}
	if row3[0].(int32) != 100 {
		t.Errorf("Row 3 column 0 should be 100, got %v", row3[0])
	}
	if row3[1].(string) != "updated" {
		t.Errorf("Row 3 column 1 should be 'updated', got %v", row3[1])
	}

	// Verify other rows were not updated
	row0 := table.GetRow(0)
	if row0 == nil || row0[0].(int32) != 0 || row0[1].(string) != "original" {
		t.Error("Row 0 should not be updated")
	}
}

// TestUpdateDeletedRow tests that updating a deleted row returns an error
func TestUpdateDeletedRow(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Insert and delete a row
	err := table.AppendRow([]any{int32(1)})
	if err != nil {
		t.Fatalf("Failed to append row: %v", err)
	}
	err = table.DeleteRows([]RowID{0})
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	// Try to update the deleted row
	err = table.UpdateRows([]RowID{0}, map[int]any{0: int32(100)})
	if err == nil {
		t.Error("Updating a deleted row should return an error")
	}
}

// TestGetRowByRowID tests O(1) row lookup by RowID
func TestGetRowByRowID(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert 100 rows
	for i := 0; i < 100; i++ {
		err := table.AppendRow([]any{int32(i), "test"})
		if err != nil {
			t.Fatalf("Failed to append row: %v", err)
		}
	}

	// Test random access by RowID
	testCases := []RowID{0, 25, 50, 75, 99}
	for _, rowID := range testCases {
		row := table.GetRow(rowID)
		if row == nil {
			t.Errorf("Failed to get row with RowID %d", rowID)
			continue
		}

		expectedValue := int32(rowID)
		if row[0].(int32) != expectedValue {
			t.Errorf("Row %d should have value %d, got %v", rowID, expectedValue, row[0])
		}
	}

	// Test non-existent RowID
	row := table.GetRow(1000)
	if row != nil {
		t.Error("GetRow should return nil for non-existent RowID")
	}
}

// TestBitmapGrow tests that the bitmap expands correctly
func TestBitmapGrow(t *testing.T) {
	bitmap := NewBitmap(10)

	// Set a bit beyond the initial size
	bitmap.Set(100, true)

	// Verify the bit is set
	if !bitmap.Get(100) {
		t.Error("Bit 100 should be set")
	}

	// Verify bits within original range still work
	bitmap.Set(5, true)
	if !bitmap.Get(5) {
		t.Error("Bit 5 should be set")
	}
}

// TestBitmapSetGet tests basic bitmap operations
func TestBitmapSetGet(t *testing.T) {
	bitmap := NewBitmap(100)

	// Initially all bits should be false
	for i := 0; i < 100; i++ {
		if bitmap.Get(RowID(i)) {
			t.Errorf("Bit %d should initially be false", i)
		}
	}

	// Set some bits
	bitmap.Set(0, true)
	bitmap.Set(10, true)
	bitmap.Set(99, true)

	// Verify set bits
	if !bitmap.Get(0) {
		t.Error("Bit 0 should be set")
	}
	if !bitmap.Get(10) {
		t.Error("Bit 10 should be set")
	}
	if !bitmap.Get(99) {
		t.Error("Bit 99 should be set")
	}

	// Verify unset bits
	if bitmap.Get(5) {
		t.Error("Bit 5 should not be set")
	}

	// Unset a bit
	bitmap.Set(10, false)
	if bitmap.Get(10) {
		t.Error("Bit 10 should be unset")
	}
}

// TestDeleteNonExistentRow tests that deleting a non-existent row returns an error
func TestDeleteNonExistentRow(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	err := table.DeleteRows([]RowID{100})
	if err == nil {
		t.Error("Deleting a non-existent row should return an error")
	}
}

// TestDeleteAlreadyDeletedRow tests that deleting an already deleted row is idempotent
func TestDeleteAlreadyDeletedRow(t *testing.T) {
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Insert and delete a row
	err := table.AppendRow([]any{int32(1)})
	require.NoError(t, err)
	err = table.DeleteRows([]RowID{0})
	if err != nil {
		t.Fatalf("First delete failed: %v", err)
	}

	// Delete again - should not error (idempotent)
	err = table.DeleteRows([]RowID{0})
	if err != nil {
		t.Errorf("Second delete should not error: %v", err)
	}
}

// Task 2.3: Test InsertChunk with 2048-row DataChunk completes in <5ms
func TestInsertChunk_Performance_2048Rows(t *testing.T) {
	table := NewTable("perf_test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Create a 2048-row DataChunk (StandardVectorSize)
	chunk := NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})
	for i := 0; i < StandardVectorSize; i++ {
		chunk.AppendRow([]any{int32(i), "test"})
	}

	// Measure insertion time
	start := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset table for each iteration
			table = NewTable("perf_test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})
			_, err := table.InsertChunk(chunk)
			if err != nil {
				t.Fatalf("InsertChunk failed: %v", err)
			}
		}
	})

	// Verify performance: 2048 rows should complete in <5ms
	avgNs := start.NsPerOp()
	avgMs := float64(avgNs) / 1000000.0

	if avgMs > 5.0 {
		t.Errorf("InsertChunk too slow: %.2fms > 5ms target", avgMs)
	}

	// Verify all rows were inserted
	if table.RowCount() != StandardVectorSize {
		t.Errorf("Expected %d rows, got %d", StandardVectorSize, table.RowCount())
	}
}

// Task 2.4: Test InsertChunk with multi-column DataChunk preserves all values
func TestInsertChunk_MultiColumn_PreservesValues(t *testing.T) {
	// Create table with multiple column types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_BIGINT,
	}
	table := NewTable("multi_col_test", columnTypes)

	// Create DataChunk with test data
	chunk := NewDataChunk(columnTypes)
	testRows := [][]any{
		{int32(1), "row1", float64(1.1), true, int64(100)},
		{int32(2), "row2", float64(2.2), false, int64(200)},
		{int32(3), "row3", float64(3.3), true, int64(300)},
		{int32(4), "row4", float64(4.4), false, int64(400)},
		{int32(5), "row5", float64(5.5), true, int64(500)},
	}

	for _, row := range testRows {
		chunk.AppendRow(row)
	}

	// Insert chunk
	rowsInserted, err := table.InsertChunk(chunk)
	if err != nil {
		t.Fatalf("InsertChunk failed: %v", err)
	}

	if rowsInserted != len(testRows) {
		t.Errorf("Expected %d rows inserted, got %d", len(testRows), rowsInserted)
	}

	// Verify all values were preserved
	for i, expectedRow := range testRows {
		rowID := RowID(i)
		actualRow := table.GetRow(rowID)
		if actualRow == nil {
			t.Fatalf("Row %d not found", i)
		}

		// Verify each column
		if actualRow[0].(int32) != expectedRow[0].(int32) {
			t.Errorf("Row %d col 0: expected %v, got %v", i, expectedRow[0], actualRow[0])
		}
		if actualRow[1].(string) != expectedRow[1].(string) {
			t.Errorf("Row %d col 1: expected %v, got %v", i, expectedRow[1], actualRow[1])
		}
		if actualRow[2].(float64) != expectedRow[2].(float64) {
			t.Errorf("Row %d col 2: expected %v, got %v", i, expectedRow[2], actualRow[2])
		}
		if actualRow[3].(bool) != expectedRow[3].(bool) {
			t.Errorf("Row %d col 3: expected %v, got %v", i, expectedRow[3], actualRow[3])
		}
		if actualRow[4].(int64) != expectedRow[4].(int64) {
			t.Errorf("Row %d col 4: expected %v, got %v", i, expectedRow[4], actualRow[4])
		}
	}
}

// Task 2.5: Test InsertChunk with NULL values correctly sets validity masks
func TestInsertChunk_NullValues_ValidityMasks(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	table := NewTable("null_test", columnTypes)

	// Create chunk with NULL values
	chunk := NewDataChunk(columnTypes)
	chunk.AppendRow([]any{int32(1), "test", float64(1.1)}) // No NULLs
	chunk.AppendRow([]any{nil, "test2", float64(2.2)})     // NULL in col 0
	chunk.AppendRow([]any{int32(3), nil, float64(3.3)})    // NULL in col 1
	chunk.AppendRow([]any{int32(4), "test4", nil})         // NULL in col 2
	chunk.AppendRow([]any{nil, nil, nil})                  // All NULLs

	// Insert chunk
	rowsInserted, err := table.InsertChunk(chunk)
	if err != nil {
		t.Fatalf("InsertChunk failed: %v", err)
	}

	if rowsInserted != 5 {
		t.Errorf("Expected 5 rows inserted, got %d", rowsInserted)
	}

	// Verify NULL values are preserved
	testCases := []struct {
		rowID       RowID
		col         int
		shouldBeNil bool
	}{
		{0, 0, false}, {0, 1, false}, {0, 2, false},
		{1, 0, true}, {1, 1, false}, {1, 2, false},
		{2, 0, false}, {2, 1, true}, {2, 2, false},
		{3, 0, false}, {3, 1, false}, {3, 2, true},
		{4, 0, true}, {4, 1, true}, {4, 2, true},
	}

	for _, tc := range testCases {
		row := table.GetRow(tc.rowID)
		if row == nil {
			t.Fatalf("Row %d not found", tc.rowID)
		}

		isNil := row[tc.col] == nil
		if isNil != tc.shouldBeNil {
			t.Errorf("Row %d col %d: expected nil=%v, got nil=%v (value=%v)",
				tc.rowID, tc.col, tc.shouldBeNil, isNil, row[tc.col])
		}
	}
}

// Task 2.6: Verify InsertChunk maintains row IDs correctly
func TestInsertChunk_RowID_Assignment(t *testing.T) {
	table := NewTable("rowid_test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Insert first chunk
	chunk1 := NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
	for i := 0; i < 10; i++ {
		chunk1.AppendRow([]any{int32(i)})
	}

	rowsInserted1, err := table.InsertChunk(chunk1)
	if err != nil {
		t.Fatalf("First InsertChunk failed: %v", err)
	}
	if rowsInserted1 != 10 {
		t.Errorf("Expected 10 rows inserted, got %d", rowsInserted1)
	}

	// Verify RowIDs 0-9 were assigned
	for i := 0; i < 10; i++ {
		if !table.ContainsRow(RowID(i)) {
			t.Errorf("RowID %d should exist", i)
		}
	}

	// Insert second chunk
	chunk2 := NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
	for i := 10; i < 20; i++ {
		chunk2.AppendRow([]any{int32(i)})
	}

	rowsInserted2, err := table.InsertChunk(chunk2)
	if err != nil {
		t.Fatalf("Second InsertChunk failed: %v", err)
	}
	if rowsInserted2 != 10 {
		t.Errorf("Expected 10 rows inserted, got %d", rowsInserted2)
	}

	// Verify RowIDs 10-19 were assigned (monotonic continuation)
	for i := 10; i < 20; i++ {
		if !table.ContainsRow(RowID(i)) {
			t.Errorf("RowID %d should exist", i)
		}
		row := table.GetRow(RowID(i))
		if row == nil {
			t.Fatalf("Failed to get row %d", i)
		}
		if row[0].(int32) != int32(i) {
			t.Errorf("Row %d has wrong value: expected %d, got %v", i, i, row[0])
		}
	}

	// Verify nextRowID is correct
	if table.NextRowID() != 20 {
		t.Errorf("Expected nextRowID=20, got %d", table.NextRowID())
	}
}

// Task 2.7: Verify memory usage bounded (<10MB for 2048-row chunk)
func TestInsertChunk_MemoryUsage_Bounded(t *testing.T) {
	// Use runtime.MemStats to track memory usage
	var memBefore, memAfter runtime.MemStats
	runtime.GC() // Force GC before measurement

	runtime.ReadMemStats(&memBefore)

	// Create table and insert 2048-row chunk with multiple columns
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BIGINT,
	}
	table := NewTable("mem_test", columnTypes)

	chunk := NewDataChunk(columnTypes)
	for i := 0; i < StandardVectorSize; i++ {
		chunk.AppendRow([]any{
			int32(i),
			"test_string_value_with_some_length",
			float64(i) * 1.5,
			int64(i) * 1000,
		})
	}

	_, err := table.InsertChunk(chunk)
	if err != nil {
		t.Fatalf("InsertChunk failed: %v", err)
	}

	runtime.ReadMemStats(&memAfter)

	// Calculate memory increase in MB
	memIncrease := float64(memAfter.Alloc-memBefore.Alloc) / (1024 * 1024)

	// Verify memory usage is bounded (<10MB for 2048 rows)
	if memIncrease > 10.0 {
		t.Errorf("Memory usage too high: %.2fMB > 10MB target", memIncrease)
	}

	// Log memory usage for visibility
	t.Logf("Memory usage for 2048-row chunk: %.2fMB", memIncrease)
}
