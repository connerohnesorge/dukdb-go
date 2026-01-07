package xlsx

import (
	"math"
	"os"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestExcelDateToTime(t *testing.T) {
	// Test values based on excelize.ExcelDateToTime behavior
	tests := []struct {
		name     string
		serial   float64
		expected time.Time
	}{
		{
			name:     "Serial 1 (Dec 31, 1899)",
			serial:   1,
			expected: time.Date(1899, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 2 (Jan 1, 1900)",
			serial:   2,
			expected: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 59 (Feb 27, 1900)",
			serial:   59,
			expected: time.Date(1900, 2, 27, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 60 (Feb 28, 1900 - leap year bug boundary)",
			serial:   60,
			expected: time.Date(1900, 2, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 61 (Mar 1, 1900)",
			serial:   61,
			expected: time.Date(1900, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 36526 (Jan 1, 2000)",
			serial:   36526,
			expected: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 36526.5 (Jan 1, 2000 at noon)",
			serial:   36526.5,
			expected: time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "Serial 45292 (Jan 1, 2024)",
			serial:   45292,
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := excelDateToTime(tt.serial)
			// Allow 1 second tolerance for time calculations
			diff := result.Sub(tt.expected)
			assert.LessOrEqual(t, math.Abs(diff.Seconds()), 1.0,
				"Expected %v, got %v (diff: %v)", tt.expected, result, diff)
		})
	}
}

func TestTimeToExcelDate(t *testing.T) {
	// Test that timeToExcelDate produces values that round-trip correctly with excelDateToTime
	// The important thing is consistency: time -> serial -> time should give the same result
	tests := []struct {
		name string
		time time.Time
	}{
		{
			name: "Jan 1, 1900",
			time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Feb 28, 1900",
			time: time.Date(1900, 2, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Mar 1, 1900",
			time: time.Date(1900, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Jan 1, 2000",
			time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Jan 1, 2024",
			time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Jan 1, 2000 at noon",
			time: time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serial := timeToExcelDate(tt.time)
			result := excelDateToTime(serial)
			diff := result.Sub(tt.time)
			assert.LessOrEqual(t, math.Abs(diff.Seconds()), 1.0,
				"Expected round-trip to return %v, got %v (serial: %v)", tt.time, result, serial)
		})
	}
}

func TestExcelDateRoundTrip(t *testing.T) {
	// Test that converting time -> excel -> time gives the same result
	testTimes := []time.Time{
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
		time.Date(1950, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2050, 12, 31, 23, 59, 59, 0, time.UTC),
	}

	for _, original := range testTimes {
		t.Run(original.Format("2006-01-02"), func(t *testing.T) {
			serial := timeToExcelDate(original)
			result := excelDateToTime(serial)
			diff := result.Sub(original)
			assert.LessOrEqual(t, math.Abs(diff.Seconds()), 1.0,
				"Expected %v, got %v", original, result)
		})
	}
}

func TestInferTypeFromString(t *testing.T) {
	tests := []struct {
		value    string
		expected dukdb.Type
	}{
		{"", dukdb.TYPE_VARCHAR},
		{"hello", dukdb.TYPE_VARCHAR},
		{"true", dukdb.TYPE_BOOLEAN},
		{"false", dukdb.TYPE_BOOLEAN},
		{"TRUE", dukdb.TYPE_BOOLEAN},
		{"FALSE", dukdb.TYPE_BOOLEAN},
		{"123", dukdb.TYPE_BIGINT},
		{"-456", dukdb.TYPE_BIGINT},
		{"0", dukdb.TYPE_BIGINT},
		{"123.45", dukdb.TYPE_DOUBLE},
		{"-456.789", dukdb.TYPE_DOUBLE},
		{"1.23e10", dukdb.TYPE_DOUBLE},
		{"not a number", dukdb.TYPE_VARCHAR},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			result := inferTypeFromString(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseTypeFromString(t *testing.T) {
	tests := []struct {
		typeName string
		expected dukdb.Type
		valid    bool
	}{
		{"VARCHAR", dukdb.TYPE_VARCHAR, true},
		{"varchar", dukdb.TYPE_VARCHAR, true},
		{"TEXT", dukdb.TYPE_VARCHAR, true},
		{"STRING", dukdb.TYPE_VARCHAR, true},
		{"INTEGER", dukdb.TYPE_INTEGER, true},
		{"INT", dukdb.TYPE_INTEGER, true},
		{"BIGINT", dukdb.TYPE_BIGINT, true},
		{"INT64", dukdb.TYPE_BIGINT, true},
		{"DOUBLE", dukdb.TYPE_DOUBLE, true},
		{"FLOAT64", dukdb.TYPE_DOUBLE, true},
		{"FLOAT", dukdb.TYPE_FLOAT, true},
		{"BOOLEAN", dukdb.TYPE_BOOLEAN, true},
		{"BOOL", dukdb.TYPE_BOOLEAN, true},
		{"TIMESTAMP", dukdb.TYPE_TIMESTAMP, true},
		{"DATETIME", dukdb.TYPE_TIMESTAMP, true},
		{"DATE", dukdb.TYPE_DATE, true},
		{"TIME", dukdb.TYPE_TIME, true},
		{"UNKNOWN_TYPE", dukdb.TYPE_INVALID, false},
		{"", dukdb.TYPE_INVALID, false},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result, valid := parseTypeFromString(tt.typeName)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.valid, valid)
		})
	}
}

func TestConvertValueToType(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		typ         dukdb.Type
		emptyAsNull bool
		expected    any
	}{
		// VARCHAR
		{"varchar string", "hello", dukdb.TYPE_VARCHAR, true, "hello"},
		{"varchar empty null", "", dukdb.TYPE_VARCHAR, true, nil},
		{"varchar empty string", "", dukdb.TYPE_VARCHAR, false, ""},

		// BOOLEAN
		{"bool true", "true", dukdb.TYPE_BOOLEAN, true, true},
		{"bool false", "false", dukdb.TYPE_BOOLEAN, true, false},
		{"bool 1", "1", dukdb.TYPE_BOOLEAN, true, true},
		{"bool yes", "yes", dukdb.TYPE_BOOLEAN, true, true},
		{"bool no", "no", dukdb.TYPE_BOOLEAN, true, false},

		// INTEGER types
		{"bigint", "123", dukdb.TYPE_BIGINT, true, int64(123)},
		{"bigint negative", "-456", dukdb.TYPE_BIGINT, true, int64(-456)},
		{"integer", "123", dukdb.TYPE_INTEGER, true, int32(123)},
		{"smallint", "123", dukdb.TYPE_SMALLINT, true, int16(123)},
		{"tinyint", "123", dukdb.TYPE_TINYINT, true, int8(123)},

		// FLOAT types
		{"double", "123.45", dukdb.TYPE_DOUBLE, true, float64(123.45)},
		{"float", "123.45", dukdb.TYPE_FLOAT, true, float32(123.45)},

		// Invalid conversions fall back to string
		{"invalid bigint", "not a number", dukdb.TYPE_BIGINT, true, "not a number"},
		{"invalid double", "not a number", dukdb.TYPE_DOUBLE, true, "not a number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValueToType(tt.value, tt.typ, tt.emptyAsNull)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimestampFromSerial(t *testing.T) {
	// Test converting Excel serial number to timestamp
	result := convertValueToType("45292", dukdb.TYPE_TIMESTAMP, true)
	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if ts, ok := result.(time.Time); ok {
		diff := ts.Sub(expected)
		assert.LessOrEqual(t, math.Abs(diff.Seconds()), 1.0)
	} else {
		t.Errorf("Expected time.Time, got %T", result)
	}
}

func TestConvertDateFromSerial(t *testing.T) {
	// Test converting Excel serial number to date
	result := convertValueToType("45292", dukdb.TYPE_DATE, true)
	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if ts, ok := result.(time.Time); ok {
		assert.Equal(t, expected.Year(), ts.Year())
		assert.Equal(t, expected.Month(), ts.Month())
		assert.Equal(t, expected.Day(), ts.Day())
	} else {
		t.Errorf("Expected time.Time, got %T", result)
	}
}

// createTypedTestXLSX creates a test XLSX file with typed data
func createTypedTestXLSX(t *testing.T, sheetName string, data [][]any, setTypes bool) string {
	t.Helper()

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			t.Logf("failed to close excelize file: %v", err)
		}
	}()

	// Rename default sheet
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

	// Write data with proper types
	for rowIdx, row := range data {
		for colIdx, val := range row {
			cell := CellAddress(colIdx, rowIdx+1)
			if cell == "" {
				continue
			}

			// Set cell value with proper type
			if err := f.SetCellValue(sheet, cell, val); err != nil {
				t.Fatalf("failed to set cell value: %v", err)
			}
		}
	}

	// Save to temp file
	tmpFile, err := os.CreateTemp("", "xlsx_typed_test_*.xlsx")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()

	if err := tmpFile.Close(); err != nil {
		t.Logf("failed to close temp file: %v", err)
	}

	err = f.SaveAs(tmpPath)
	require.NoError(t, err)

	return tmpPath
}

func TestReader_TypeInference_Numbers(t *testing.T) {
	// Create test data with numbers
	data := [][]any{
		{"IntCol", "FloatCol", "MixedCol"},
		{100, 1.5, "text"},
		{200, 2.5, 123},
		{300, 3.5, "more text"},
	}

	path := createTypedTestXLSX(t, "Sheet1", data, true)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.InferTypes = true

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)

	// IntCol should be BIGINT (all integers)
	assert.Equal(t, dukdb.TYPE_BIGINT, types[0], "IntCol should be BIGINT")

	// FloatCol should be DOUBLE (all floats)
	assert.Equal(t, dukdb.TYPE_DOUBLE, types[1], "FloatCol should be DOUBLE")

	// MixedCol should be VARCHAR (mixed types, no majority)
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[2], "MixedCol should be VARCHAR")
}

func TestReader_TypeInference_Booleans(t *testing.T) {
	// Create test data with booleans
	data := [][]any{
		{"BoolCol", "TextCol"},
		{true, "hello"},
		{false, "world"},
		{true, "foo"},
	}

	path := createTypedTestXLSX(t, "Sheet1", data, true)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.InferTypes = true

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)

	// BoolCol should be BOOLEAN
	assert.Equal(t, dukdb.TYPE_BOOLEAN, types[0], "BoolCol should be BOOLEAN")

	// TextCol should be VARCHAR
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1], "TextCol should be VARCHAR")
}

func TestReader_TypeInference_Disabled(t *testing.T) {
	// Create test data with numbers
	data := [][]any{
		{"NumCol", "TextCol"},
		{100, "hello"},
		{200, "world"},
	}

	path := createTypedTestXLSX(t, "Sheet1", data, true)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.InferTypes = false // Disable type inference

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)

	// All columns should be VARCHAR when inference is disabled
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[0], "NumCol should be VARCHAR when inference disabled")
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1], "TextCol should be VARCHAR when inference disabled")
}

func TestReader_ExplicitColumnTypes(t *testing.T) {
	// Create test data
	data := [][]any{
		{"Col1", "Col2", "Col3"},
		{"100", "hello", "true"},
		{"200", "world", "false"},
	}

	path := createTypedTestXLSX(t, "Sheet1", data, true)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.InferTypes = true
	opts.Columns = map[string]string{
		"Col1": "INTEGER",
		"Col3": "BOOLEAN",
	}

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)

	// Col1 should be INTEGER (explicit override)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0], "Col1 should be INTEGER from explicit type")

	// Col2 should be VARCHAR (inferred)
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1], "Col2 should be VARCHAR from inference")

	// Col3 should be BOOLEAN (explicit override)
	assert.Equal(t, dukdb.TYPE_BOOLEAN, types[2], "Col3 should be BOOLEAN from explicit type")
}

func TestReader_TypedDataChunk(t *testing.T) {
	// Create test data with integers
	data := [][]any{
		{"IntCol"},
		{100},
		{200},
		{300},
	}

	path := createTypedTestXLSX(t, "Sheet1", data, true)
	defer func() { _ = os.Remove(path) }()

	opts := DefaultReaderOptions()
	opts.InferTypes = true

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)

	// Verify values are converted to int64
	for i := 0; i < chunk.Count(); i++ {
		val := chunk.GetValue(i, 0)
		if val != nil {
			// The value should be int64 after conversion
			_, ok := val.(int64)
			assert.True(t, ok, "Expected int64, got %T", val)
		}
	}
}

func TestIsDateFormat(t *testing.T) {
	// This test verifies that our date format detection works
	// We create a file with date-formatted cells and verify detection

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	// Create a date style
	style, err := f.NewStyle(&excelize.Style{
		NumFmt: 14, // Built-in date format m/d/yy
	})
	require.NoError(t, err)

	// Apply style to cell
	require.NoError(t, f.SetCellStyle("Sheet1", "A1", "A1", style))
	require.NoError(t, f.SetCellValue("Sheet1", "A1", 45292)) // Jan 1, 2024

	// Test the isDateFormat function
	assert.True(t, isDateFormat(f, style))
	assert.False(t, isDateFormat(f, 0))
	assert.False(t, isDateFormat(f, -1))
}
