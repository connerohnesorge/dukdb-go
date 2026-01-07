package xlsx

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// createTestXLSX creates a test XLSX file with the given data.
// Returns the file path. Caller is responsible for cleanup.
func createTestXLSX(t *testing.T, sheetName string, data [][]any) string {
	t.Helper()

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			t.Logf("failed to close excelize file: %v", err)
		}
	}()

	// Rename default sheet or create new one
	defaultSheet := f.GetSheetName(0)
	if sheetName != "" && defaultSheet != sheetName {
		if err := f.SetSheetName(defaultSheet, sheetName); err != nil {
			t.Fatalf("failed to set sheet name: %v", err)
		}
	}

	sheet := sheetName
	if sheet == "" {
		sheet = defaultSheet
	}

	// Write data
	for rowIdx, row := range data {
		for colIdx, val := range row {
			cell := CellAddress(colIdx, rowIdx+1)
			if cell == "" {
				continue
			}
			if err := f.SetCellValue(sheet, cell, val); err != nil {
				t.Fatalf("failed to set cell value: %v", err)
			}
		}
	}

	// Save to temp file
	tmpFile, err := os.CreateTemp("", "xlsx_test_*.xlsx")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()

	if err := tmpFile.Close(); err != nil {
		t.Logf("failed to close temp file: %v", err)
	}

	err = f.SaveAs(tmpPath)
	require.NoError(t, err)

	return tmpPath
}

func TestNewReaderFromPath_Simple(t *testing.T) {
	// Create test data with header
	data := [][]any{
		{"Name", "Age", "City"},
		{"Alice", 30, "New York"},
		{"Bob", 25, "Los Angeles"},
		{"Charlie", 35, "Chicago"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Check schema
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Name", "Age", "City"}, schema)

	// Check types (with type inference: Name=VARCHAR, Age=BIGINT, City=VARCHAR)
	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}, types)

	// Read first chunk
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, 3, chunk.ColumnCount())

	// Verify data (Age is now int64 due to type inference)
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
	assert.Equal(t, int64(30), chunk.GetValue(0, 1))
	assert.Equal(t, "New York", chunk.GetValue(0, 2))

	assert.Equal(t, "Bob", chunk.GetValue(1, 0))
	assert.Equal(t, int64(25), chunk.GetValue(1, 1))
	assert.Equal(t, "Los Angeles", chunk.GetValue(1, 2))

	// Second read should return EOF
	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF)
}

func TestNewReader_FromReader(t *testing.T) {
	// Create test data
	data := [][]any{
		{"Col1", "Col2"},
		{"A", "B"},
		{"C", "D"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	// Read file into memory
	fileData, err := os.ReadFile(path)
	require.NoError(t, err)

	reader, err := NewReader(bytes.NewReader(fileData), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Col1", "Col2"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
}

func TestReader_NoHeader(t *testing.T) {
	// Create test data without header
	data := [][]any{
		{"Alice", 30, "New York"},
		{"Bob", 25, "Los Angeles"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.Header = false

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Schema should be generated column names
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"column0", "column1", "column2"}, schema)

	// Should read all rows including the first one
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
}

func TestReader_EmptyAsNull(t *testing.T) {
	// Create test data with empty cells
	data := [][]any{
		{"Name", "Value"},
		{"Alice", ""},
		{"", "10"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	t.Run("empty as null (default)", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.EmptyAsNull = true

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)

		// Empty cells should be NULL
		assert.Nil(t, chunk.GetValue(0, 1)) // Second column of first row
		assert.Nil(t, chunk.GetValue(1, 0)) // First column of second row
	})

	t.Run("empty as empty string", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.EmptyAsNull = false

		reader, err := NewReaderFromPath(path, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		chunk, err := reader.ReadChunk()
		require.NoError(t, err)

		// Empty cells should be empty strings
		assert.Equal(t, "", chunk.GetValue(0, 1))
		assert.Equal(t, "", chunk.GetValue(1, 0))
	})
}

func TestReader_NullValues(t *testing.T) {
	// Create test data with NULL markers
	data := [][]any{
		{"Name", "Value"},
		{"Alice", "NA"},
		{"Bob", "N/A"},
		{"Charlie", "10"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.NullValues = []string{"NA", "N/A"}

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)

	// "NA" and "N/A" should be NULL
	assert.Nil(t, chunk.GetValue(0, 1))
	assert.Nil(t, chunk.GetValue(1, 1))
	// "10" should be kept
	assert.Equal(t, "10", chunk.GetValue(2, 1))
}

func TestReader_SheetSelection(t *testing.T) {
	// Create workbook with multiple sheets
	f := excelize.NewFile()

	// Create Sheet1
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Sheet1Header"))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", "Sheet1Data"))

	// Create Sheet2
	_, err := f.NewSheet("Sheet2")
	require.NoError(t, err)
	require.NoError(t, f.SetCellValue("Sheet2", "A1", "Sheet2Header"))
	require.NoError(t, f.SetCellValue("Sheet2", "A2", "Sheet2Data"))

	tmpFile, err := os.CreateTemp("", "xlsx_multi_sheet_*.xlsx")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()

	if err := tmpFile.Close(); err != nil {
		t.Logf("failed to close temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpPath) }()

	err = f.SaveAs(tmpPath)
	require.NoError(t, err)

	if err := f.Close(); err != nil {
		t.Logf("failed to close excelize file: %v", err)
	}

	t.Run("select by name", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.Sheet = "Sheet2"

		reader, err := NewReaderFromPath(tmpPath, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Sheet2Header"}, schema)
	})

	t.Run("select by index", func(t *testing.T) {
		opts := DefaultReaderOptions()
		opts.SheetIndex = 1

		reader, err := NewReaderFromPath(tmpPath, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Sheet2Header"}, schema)
	})

	t.Run("default to first sheet", func(t *testing.T) {
		reader, err := NewReaderFromPath(tmpPath, nil)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		schema, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"Sheet1Header"}, schema)
	})
}

func TestReader_SkipRows(t *testing.T) {
	// Create test data with some rows to skip
	data := [][]any{
		{"Title Row"},
		{"Subtitle Row"},
		{"Name", "Value"},
		{"Alice", "10"},
		{"Bob", "20"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.Skip = 2 // Skip first two rows

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Header should be "Name", "Value"
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Name", "Value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
}

func TestReader_Range(t *testing.T) {
	// Create test data
	data := [][]any{
		{"A", "B", "C", "D"},
		{"1", "2", "3", "4"},
		{"5", "6", "7", "8"},
		{"9", "10", "11", "12"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.Range = "B1:C3" // Columns B-C, Rows 1-3

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Schema should only include B and C columns
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"B", "C"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	// Should have rows 2-3 (row 1 is header)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, "2", chunk.GetValue(0, 0))
	assert.Equal(t, "3", chunk.GetValue(0, 1))
}

func TestReader_ChunkSize(t *testing.T) {
	// Create test data with many rows
	data := make([][]any, 11) // 1 header + 10 data rows
	data[0] = []any{"Value"}
	for i := 1; i <= 10; i++ {
		data[i] = []any{i}
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.ChunkSize = 3

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// First chunk: 3 rows
	chunk1, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk1.Count())

	// Second chunk: 3 rows
	chunk2, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk2.Count())

	// Third chunk: 3 rows
	chunk3, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk3.Count())

	// Fourth chunk: 1 remaining row
	chunk4, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, chunk4.Count())

	// Fifth read should be EOF
	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF)
}

func TestReader_EmptyFile(t *testing.T) {
	f := excelize.NewFile()

	tmpFile, err := os.CreateTemp("", "xlsx_empty_*.xlsx")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()

	if err := tmpFile.Close(); err != nil {
		t.Logf("failed to close temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpPath) }()

	err = f.SaveAs(tmpPath)
	require.NoError(t, err)

	if err := f.Close(); err != nil {
		t.Logf("failed to close excelize file: %v", err)
	}

	reader, err := NewReaderFromPath(tmpPath, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF)
}

func TestReader_InvalidPath(t *testing.T) {
	_, err := NewReaderFromPath(filepath.Join(os.TempDir(), "nonexistent_file.xlsx"), nil)
	assert.Error(t, err)
}

func TestReader_SheetNotFound(t *testing.T) {
	data := [][]any{{"Header"}, {"Data"}}
	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.Sheet = "NonexistentSheet"

	_, err := NewReaderFromPath(path, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestReader_InvalidRange(t *testing.T) {
	data := [][]any{{"Header"}, {"Data"}}
	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.Range = "invalid"

	_, err := NewReaderFromPath(path, opts)
	assert.Error(t, err)
}

func TestReader_EmptyHeaderCell(t *testing.T) {
	// Create test data with empty header cell
	data := [][]any{
		{"Name", "", "City"},
		{"Alice", "30", "NY"},
	}

	path := createTestXLSX(t, "Sheet1", data)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	// Empty header should be replaced with "column1"
	assert.Equal(t, []string{"Name", "column1", "City"}, schema)
}
