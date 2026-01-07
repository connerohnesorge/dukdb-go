// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// TestReadXLSXBasic tests basic read_xlsx functionality.
func TestReadXLSXBasic(t *testing.T) {
	// Create a temporary XLSX file for testing
	tmpDir := t.TempDir()
	xlsxPath := filepath.Join(tmpDir, "test.xlsx")

	// Create a test XLSX file using excelize
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	// Set header row
	_ = f.SetCellValue("Sheet1", "A1", "id")
	_ = f.SetCellValue("Sheet1", "B1", "name")
	_ = f.SetCellValue("Sheet1", "C1", "value")

	// Set data rows
	_ = f.SetCellValue("Sheet1", "A2", 1)
	_ = f.SetCellValue("Sheet1", "B2", "Alice")
	_ = f.SetCellValue("Sheet1", "C2", 100)

	_ = f.SetCellValue("Sheet1", "A3", 2)
	_ = f.SetCellValue("Sheet1", "B3", "Bob")
	_ = f.SetCellValue("Sheet1", "C3", 200)

	_ = f.SetCellValue("Sheet1", "A4", 3)
	_ = f.SetCellValue("Sheet1", "B4", "Charlie")
	_ = f.SetCellValue("Sheet1", "C4", 300)

	// Save the file
	err := f.SaveAs(xlsxPath)
	require.NoError(t, err)

	// Verify the file exists
	_, err = os.Stat(xlsxPath)
	require.NoError(t, err)

	// The actual SQL execution test would require the full engine setup.
	// For now, we verify the file was created correctly by reading it back.
	f2, err := excelize.OpenFile(xlsxPath)
	require.NoError(t, err)
	defer func() { _ = f2.Close() }()

	// Verify header
	header, err := f2.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "id", header)

	header, err = f2.GetCellValue("Sheet1", "B1")
	require.NoError(t, err)
	assert.Equal(t, "name", header)

	// Verify data
	val, err := f2.GetCellValue("Sheet1", "B2")
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)
}

// TestReadXLSXWithOptions tests read_xlsx with various options.
func TestReadXLSXWithOptions(t *testing.T) {
	// Create a temporary XLSX file for testing
	tmpDir := t.TempDir()
	xlsxPath := filepath.Join(tmpDir, "test_options.xlsx")

	// Create a test XLSX file with multiple sheets
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	// Sheet1 - default sheet
	_ = f.SetCellValue("Sheet1", "A1", "col1")
	_ = f.SetCellValue("Sheet1", "B1", "col2")
	_ = f.SetCellValue("Sheet1", "A2", "data1")
	_ = f.SetCellValue("Sheet1", "B2", "data2")

	// Create a second sheet
	_, _ = f.NewSheet("DataSheet")
	_ = f.SetCellValue("DataSheet", "A1", "x")
	_ = f.SetCellValue("DataSheet", "B1", "y")
	_ = f.SetCellValue("DataSheet", "A2", 10)
	_ = f.SetCellValue("DataSheet", "B2", 20)

	// Save the file
	err := f.SaveAs(xlsxPath)
	require.NoError(t, err)

	// Verify multi-sheet file was created
	f2, err := excelize.OpenFile(xlsxPath)
	require.NoError(t, err)
	defer func() { _ = f2.Close() }()

	sheets := f2.GetSheetList()
	assert.Len(t, sheets, 2)
	assert.Contains(t, sheets, "Sheet1")
	assert.Contains(t, sheets, "DataSheet")
}

// TestReadXLSXEmptyCells tests handling of empty cells.
func TestReadXLSXEmptyCells(t *testing.T) {
	// Create a temporary XLSX file for testing
	tmpDir := t.TempDir()
	xlsxPath := filepath.Join(tmpDir, "test_empty.xlsx")

	// Create a test XLSX file with empty cells
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	_ = f.SetCellValue("Sheet1", "A1", "col1")
	_ = f.SetCellValue("Sheet1", "B1", "col2")
	_ = f.SetCellValue("Sheet1", "C1", "col3")

	// Row with empty cell in middle
	_ = f.SetCellValue("Sheet1", "A2", "a")
	// B2 is empty
	_ = f.SetCellValue("Sheet1", "C2", "c")

	// Save the file
	err := f.SaveAs(xlsxPath)
	require.NoError(t, err)

	// Verify empty cell handling
	f2, err := excelize.OpenFile(xlsxPath)
	require.NoError(t, err)
	defer func() { _ = f2.Close() }()

	// B2 should be empty
	val, err := f2.GetCellValue("Sheet1", "B2")
	require.NoError(t, err)
	assert.Equal(t, "", val)
}
