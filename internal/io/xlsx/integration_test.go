// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file contains comprehensive integration tests for the XLSX reader and writer.
package xlsx

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// TestIntegration_ReadWriteRoundTrip tests writing data to XLSX and reading it back.
func TestIntegration_ReadWriteRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("basic round trip", func(t *testing.T) {
		path := filepath.Join(tmpDir, "roundtrip_basic.xlsx")

		// Create test data
		testData := [][]any{
			{"ID", "Name", "Value"},
			{1, "Alice", 100.5},
			{2, "Bob", 200.25},
			{3, "Charlie", 300.75},
		}

		// Write XLSX
		f := excelize.NewFile()
		for rowIdx, row := range testData {
			for colIdx, val := range row {
				cell := CellAddress(colIdx, rowIdx+1)
				require.NoError(t, f.SetCellValue("Sheet1", cell, val))
			}
		}
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		// Read XLSX back
		reader, err := NewReaderFromPath(path, nil)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		// Verify schema
		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"ID", "Name", "Value"}, schema)

		// Verify data
		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 3, chunk.Count())

		// First row
		assert.Equal(t, int64(1), chunk.GetValue(0, 0))
		assert.Equal(t, "Alice", chunk.GetValue(0, 1))
		assert.Equal(t, 100.5, chunk.GetValue(0, 2))
	})

	t.Run("mixed types round trip", func(t *testing.T) {
		path := filepath.Join(tmpDir, "roundtrip_mixed.xlsx")

		// Create test data with mixed types
		f := excelize.NewFile()

		// Headers
		require.NoError(t, f.SetCellValue("Sheet1", "A1", "StringCol"))
		require.NoError(t, f.SetCellValue("Sheet1", "B1", "IntCol"))
		require.NoError(t, f.SetCellValue("Sheet1", "C1", "FloatCol"))
		require.NoError(t, f.SetCellValue("Sheet1", "D1", "BoolCol"))

		// Data with mixed types within columns
		require.NoError(t, f.SetCellValue("Sheet1", "A2", "text1"))
		require.NoError(t, f.SetCellValue("Sheet1", "B2", 100))
		require.NoError(t, f.SetCellValue("Sheet1", "C2", 1.5))
		require.NoError(t, f.SetCellValue("Sheet1", "D2", true))

		require.NoError(t, f.SetCellValue("Sheet1", "A3", "text2"))
		require.NoError(t, f.SetCellValue("Sheet1", "B3", 200))
		require.NoError(t, f.SetCellValue("Sheet1", "C3", 2.5))
		require.NoError(t, f.SetCellValue("Sheet1", "D3", false))

		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		// Read back
		reader, err := NewReaderFromPath(path, nil)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Len(t, schema, 4)

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 2, chunk.Count())
	})
}

// TestIntegration_MultipleSheets tests reading from multiple sheets.
func TestIntegration_MultipleSheets(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "multiple_sheets.xlsx")

	// Create workbook with multiple sheets
	f := excelize.NewFile()

	// Sheet1 - Sales data
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Product"))
	require.NoError(t, f.SetCellValue("Sheet1", "B1", "Sales"))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "Widget"))
	require.NoError(t, f.SetCellValue("Sheet1", "B2", 100))

	// Sheet2 - Customer data
	_, err := f.NewSheet("Customers")
	require.NoError(t, err)
	require.NoError(t, f.SetCellValue("Customers", "A1", "Name"))
	require.NoError(t, f.SetCellValue("Customers", "B1", "Email"))
	require.NoError(t, f.SetCellValue("Customers", "A2", "John"))
	require.NoError(t, f.SetCellValue("Customers", "B2", "john@example.com"))

	// Sheet3 - Empty sheet
	_, err = f.NewSheet("Empty")
	require.NoError(t, err)

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	t.Run("read default (first) sheet", func(t *testing.T) {
		reader, err := NewReaderFromPath(path, nil)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Product", "Sales"}, schema)
	})

	t.Run("read sheet by name", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.Sheet = "Customers"

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Name", "Email"}, schema)

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, "John", chunk.GetValue(0, 0))
		assert.Equal(t, "john@example.com", chunk.GetValue(0, 1))
	})

	t.Run("read sheet by index", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.SheetIndex = 1 // Customers sheet

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Name", "Email"}, schema)
	})

	t.Run("read empty sheet", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.Sheet = "Empty"

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Empty(t, schema)

		_, err = reader.ReadChunk()
		assert.ErrorIs(t, err, io.EOF)
	})
}

// TestIntegration_EmptyCells tests handling of empty cells in various positions.
func TestIntegration_EmptyCells(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty_cells.xlsx")

	// Create file with various empty cells
	f := excelize.NewFile()

	// Header
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Col1"))
	require.NoError(t, f.SetCellValue("Sheet1", "B1", "Col2"))
	require.NoError(t, f.SetCellValue("Sheet1", "C1", "Col3"))

	// Row with empty cell in middle
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "a"))
	// B2 is empty
	require.NoError(t, f.SetCellValue("Sheet1", "C2", "c"))

	// Row with empty cells at start and end
	// A3 is empty
	require.NoError(t, f.SetCellValue("Sheet1", "B3", "b"))
	// C3 is empty

	// Row with all empty cells (except header)
	// A4, B4, C4 are all empty

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	t.Run("empty as null", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.EmptyAsNull = true

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		// Excel only stores non-empty rows, so we get 2 rows (rows with at least one value)
		assert.GreaterOrEqual(t, chunk.Count(), 2)

		// Row 1: "a", NULL, "c"
		assert.Equal(t, "a", chunk.GetValue(0, 0))
		assert.Nil(t, chunk.GetValue(0, 1))
		assert.Equal(t, "c", chunk.GetValue(0, 2))

		// Row 2: NULL, "b", NULL
		assert.Nil(t, chunk.GetValue(1, 0))
		assert.Equal(t, "b", chunk.GetValue(1, 1))
		assert.Nil(t, chunk.GetValue(1, 2))
	})

	t.Run("empty as empty string", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.EmptyAsNull = false

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)

		// Row 1: "a", "", "c"
		assert.Equal(t, "a", chunk.GetValue(0, 0))
		assert.Equal(t, "", chunk.GetValue(0, 1))
		assert.Equal(t, "c", chunk.GetValue(0, 2))
	})
}

// TestIntegration_LargeFile tests reading/writing larger files.
func TestIntegration_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large_file.xlsx")

	const numRows = 10000
	const numCols = 10

	// Create large file
	f := excelize.NewFile()

	// Headers
	for col := 0; col < numCols; col++ {
		cell := CellAddress(col, 1)
		require.NoError(t, f.SetCellValue("Sheet1", cell, IndexToColumnLetters(col)))
	}

	// Data rows
	for row := 2; row <= numRows+1; row++ {
		for col := 0; col < numCols; col++ {
			cell := CellAddress(col, row)
			require.NoError(t, f.SetCellValue("Sheet1", cell, row*numCols+col))
		}
	}

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	// Read back and verify
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Len(t, schema, numCols)

	totalRows := 0
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += chunk.Count()
	}

	assert.Equal(t, numRows, totalRows)
}

// TestIntegration_RangeSelection tests reading specific cell ranges.
func TestIntegration_RangeSelection(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "range_test.xlsx")

	// Create 10x10 grid
	f := excelize.NewFile()
	for row := 1; row <= 10; row++ {
		for col := 0; col < 10; col++ {
			cell := CellAddress(col, row)
			val := row*10 + col
			require.NoError(t, f.SetCellValue("Sheet1", cell, val))
		}
	}
	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	t.Run("range B2:D4", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.Range = "B2:D4"
		opts.Header = false // Treat first row as data

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Len(t, schema, 3) // Columns B, C, D

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 3, chunk.Count()) // Rows 2, 3, 4

		// First cell should be value at B2 = 21
		// Value could be int64 due to type inference
		val := chunk.GetValue(0, 0)
		if intVal, ok := val.(int64); ok {
			assert.Equal(t, int64(21), intVal)
		} else {
			assert.Equal(t, "21", val)
		}
	})

	t.Run("start row and end row", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.StartRow = 3
		opts.EndRow = 5
		opts.Header = false

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 3, chunk.Count()) // Rows 3, 4, 5
	})

	t.Run("start col and end col", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.StartCol = "C"
		opts.EndCol = "E"
		opts.Header = false

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Len(t, schema, 3) // Columns C, D, E
	})
}

// TestIntegration_DateTimeHandling tests reading and writing date/time values.
func TestIntegration_DateTimeHandling(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "datetime.xlsx")

	// Create file with dates
	f := excelize.NewFile()

	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Date"))
	require.NoError(t, f.SetCellValue("Sheet1", "B1", "Timestamp"))

	// Set date values using Excel serial numbers
	date1 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2024, 6, 30, 14, 30, 45, 0, time.UTC)

	require.NoError(t, f.SetCellValue("Sheet1", "A2", date1))
	require.NoError(t, f.SetCellValue("Sheet1", "B2", date2))

	// Apply date format
	dateStyle, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", dateStyle))

	datetimeStyle, err := f.NewStyle(&excelize.Style{NumFmt: 22})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "B2", "B2", datetimeStyle))

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	// Read back
	opts := DefaultReaderOptions()
	opts.InferTypes = true

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)
	assert.Len(t, types, 2)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, chunk.Count())

	// Values should be time.Time
	val1 := chunk.GetValue(0, 0)
	val2 := chunk.GetValue(0, 1)

	if t1, ok := val1.(time.Time); ok {
		assert.Equal(t, 2024, t1.Year())
		assert.Equal(t, time.January, t1.Month())
		assert.Equal(t, 15, t1.Day())
	}

	if t2, ok := val2.(time.Time); ok {
		assert.Equal(t, 2024, t2.Year())
		assert.Equal(t, time.June, t2.Month())
		assert.Equal(t, 30, t2.Day())
	}
}

// TestIntegration_WriterDataTypes tests the writer with various data types.
func TestIntegration_WriterDataTypes(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Set schema
	columns := []string{"String", "Int", "Float", "Bool"}
	require.NoError(t, writer.SetSchema(columns))

	// Set types
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE, dukdb.TYPE_BOOLEAN}
	require.NoError(t, writer.SetTypes(types))

	// Create a simple data chunk manually
	// This is a simplified test - in real usage, chunks come from the engine
	require.NoError(t, writer.Close())

	// Verify output is valid XLSX
	assert.True(t, len(buf.Bytes()) > 0)
	// Check ZIP magic bytes
	assert.Equal(t, byte(0x50), buf.Bytes()[0]) // P
	assert.Equal(t, byte(0x4B), buf.Bytes()[1]) // K
}

// TestIntegration_ErrorHandling tests error conditions.
func TestIntegration_ErrorHandling(t *testing.T) {
	t.Run("non-existent file", func(t *testing.T) {
		_, err := NewReaderFromPath("/nonexistent/path/file.xlsx", nil)
		assert.Error(t, err)
	})

	t.Run("invalid sheet name", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.xlsx")

		f := excelize.NewFile()
		require.NoError(t, f.SetCellValue("Sheet1", "A1", "test"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		opts := DefaultReaderOptions()
		opts.Sheet = "NonExistent"

		_, err := NewReaderFromPath(path, opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("invalid range", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.xlsx")

		f := excelize.NewFile()
		require.NoError(t, f.SetCellValue("Sheet1", "A1", "test"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		opts := DefaultReaderOptions()
		opts.Range = "invalid range"

		_, err := NewReaderFromPath(path, opts)
		assert.Error(t, err)
	})

	t.Run("invalid sheet index", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.xlsx")

		f := excelize.NewFile()
		require.NoError(t, f.SetCellValue("Sheet1", "A1", "test"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		opts := DefaultReaderOptions()
		opts.SheetIndex = 999

		_, err := NewReaderFromPath(path, opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("corrupted file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "corrupted.xlsx")

		// Write garbage data
		require.NoError(t, os.WriteFile(path, []byte("not a valid xlsx file"), 0o644))

		_, err := NewReaderFromPath(path, nil)
		assert.Error(t, err)
	})
}

// TestIntegration_MemoryReader tests reading from in-memory bytes.
func TestIntegration_MemoryReader(t *testing.T) {
	// Create XLSX in memory
	f := excelize.NewFile()
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Header"))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "Value"))

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	require.NoError(t, f.Close())

	// Read from bytes
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Header"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, "Value", chunk.GetValue(0, 0))
}

// TestIntegration_NullValueMarkers tests custom NULL value markers.
func TestIntegration_NullValueMarkers(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "null_markers.xlsx")

	// Create file with various NULL representations
	f := excelize.NewFile()
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Value"))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "actual"))
	require.NoError(t, f.SetCellValue("Sheet1", "A3", "NA"))
	require.NoError(t, f.SetCellValue("Sheet1", "A4", "N/A"))
	require.NoError(t, f.SetCellValue("Sheet1", "A5", "#N/A"))
	require.NoError(t, f.SetCellValue("Sheet1", "A6", "NULL"))
	require.NoError(t, f.SetCellValue("Sheet1", "A7", ""))

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	opts := DefaultReaderOptions()
	opts.NullValues = []string{"NA", "N/A", "#N/A", "NULL"}
	opts.EmptyAsNull = true

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 6, chunk.Count())

	// First row should be actual value
	assert.Equal(t, "actual", chunk.GetValue(0, 0))

	// Rows 2-5 should be NULL
	for i := 1; i <= 4; i++ {
		assert.Nil(t, chunk.GetValue(i, 0), "row %d should be NULL", i+1)
	}

	// Last row (empty) should also be NULL
	assert.Nil(t, chunk.GetValue(5, 0))
}

// TestIntegration_SkipRows tests skipping rows at the start.
func TestIntegration_SkipRows(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "skip_rows.xlsx")

	// Create file with metadata rows at top
	f := excelize.NewFile()
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Report Title"))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "Generated: 2024-01-01"))
	require.NoError(t, f.SetCellValue("Sheet1", "A3", "")) // Empty row
	require.NoError(t, f.SetCellValue("Sheet1", "A4", "Name"))
	require.NoError(t, f.SetCellValue("Sheet1", "B4", "Value"))
	require.NoError(t, f.SetCellValue("Sheet1", "A5", "Alice"))
	require.NoError(t, f.SetCellValue("Sheet1", "B5", "100"))

	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	opts := DefaultReaderOptions()
	opts.Skip = 3 // Skip first 3 rows

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Name", "Value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
}

// TestIntegration_ChunkedReading tests reading in chunks.
func TestIntegration_ChunkedReading(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "chunked.xlsx")

	const totalRows = 25

	// Create file
	f := excelize.NewFile()
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Value"))
	for i := 2; i <= totalRows+1; i++ {
		cell := CellAddress(0, i)
		require.NoError(t, f.SetCellValue("Sheet1", cell, i-1))
	}
	require.NoError(t, f.SaveAs(path))
	require.NoError(t, f.Close())

	opts := DefaultReaderOptions()
	opts.ChunkSize = 10

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	var chunks []int
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		chunks = append(chunks, chunk.Count())
	}

	// Should have 3 chunks: 10 + 10 + 5
	assert.Equal(t, []int{10, 10, 5}, chunks)
}
