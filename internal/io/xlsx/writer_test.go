package xlsx

import (
	"bytes"
	"os"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestNewWriter_Basic(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	err = writer.Close()
	require.NoError(t, err)

	// Verify we can open the resulting XLSX
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Should have default sheet name
	sheets := f.GetSheetList()
	assert.Contains(t, sheets, "Sheet1")
}

func TestNewWriter_CustomSheetName(t *testing.T) {
	var buf bytes.Buffer

	opts := DefaultWriterOptions()
	opts.SheetName = "MyData"

	writer, err := NewWriter(&buf, opts)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify sheet name
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	sheets := f.GetSheetList()
	assert.Contains(t, sheets, "MyData")
}

func TestWriter_WriteChunk_Simple(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Set schema
	err = writer.SetSchema([]string{"Name", "Age", "City"})
	require.NoError(t, err)

	// Create a DataChunk
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)

	chunk.AppendRow([]any{"Alice", int64(30), "New York"})
	chunk.AppendRow([]any{"Bob", int64(25), "Los Angeles"})
	chunk.AppendRow([]any{"Charlie", int64(35), "Chicago"})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back and verify
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Verify header
	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "Name", val)

	val, err = f.GetCellValue("Sheet1", "B1")
	require.NoError(t, err)
	assert.Equal(t, "Age", val)

	val, err = f.GetCellValue("Sheet1", "C1")
	require.NoError(t, err)
	assert.Equal(t, "City", val)

	// Verify data row 1
	val, err = f.GetCellValue("Sheet1", "A2")
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	val, err = f.GetCellValue("Sheet1", "B2")
	require.NoError(t, err)
	assert.Equal(t, "30", val)

	val, err = f.GetCellValue("Sheet1", "C2")
	require.NoError(t, err)
	assert.Equal(t, "New York", val)
}

func TestWriter_NoHeader(t *testing.T) {
	var buf bytes.Buffer

	opts := DefaultWriterOptions()
	opts.Header = false

	writer, err := NewWriter(&buf, opts)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Name", "Age"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	chunk.AppendRow([]any{"Alice", int64(30)})
	chunk.AppendRow([]any{"Bob", int64(25)})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back - A1 should be data, not header
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)
}

func TestWriter_NullValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Name", "Value"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	chunk.AppendRow([]any{"Alice", nil})      // NULL value
	chunk.AppendRow([]any{nil, int64(100)}) // NULL name

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back - NULL cells should be empty
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "B2")
	require.NoError(t, err)
	assert.Equal(t, "", val) // NULL -> empty

	val, err = f.GetCellValue("Sheet1", "A3")
	require.NoError(t, err)
	assert.Equal(t, "", val) // NULL -> empty
}

func TestWriter_BooleanValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Active"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BOOLEAN}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	chunk.AppendRow([]any{true})
	chunk.AppendRow([]any{false})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A2")
	require.NoError(t, err)
	assert.Equal(t, "TRUE", val)

	val, err = f.GetCellValue("Sheet1", "A3")
	require.NoError(t, err)
	assert.Equal(t, "FALSE", val)
}

func TestWriter_FloatValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Price"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	chunk.AppendRow([]any{float64(123.45)})
	chunk.AppendRow([]any{float64(-99.99)})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A2")
	require.NoError(t, err)
	assert.Equal(t, "123.45", val)
}

func TestWriter_DateValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Date"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_DATE}
	chunk := storage.NewDataChunkWithCapacity(types, 1)

	testDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	chunk.AppendRow([]any{testDate})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back - date should be stored as serial number
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Get the raw numeric value
	val, err := f.GetCellValue("Sheet1", "A2", excelize.Options{RawCellValue: true})
	require.NoError(t, err)

	// The raw value should be the Excel serial number for the date
	// 2024-06-15 should be a serial number around 45458
	assert.NotEmpty(t, val)
}

func TestWriter_TimestampValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Timestamp"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_TIMESTAMP}
	chunk := storage.NewDataChunkWithCapacity(types, 1)

	testTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
	chunk.AppendRow([]any{testTime})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Should have a non-empty value (serial number with decimal)
	val, err := f.GetCellValue("Sheet1", "A2", excelize.Options{RawCellValue: true})
	require.NoError(t, err)
	assert.NotEmpty(t, val)
}

func TestWriter_TimeValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Time"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_TIME}
	chunk := storage.NewDataChunkWithCapacity(types, 1)

	// Create a time-only value (14:30:45)
	testTime := time.Date(1970, 1, 1, 14, 30, 45, 0, time.UTC)
	chunk.AppendRow([]any{testTime})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Time should be stored as a fraction (0.0 - 1.0)
	val, err := f.GetCellValue("Sheet1", "A2", excelize.Options{RawCellValue: true})
	require.NoError(t, err)
	assert.NotEmpty(t, val)
}

func TestWriter_MultipleChunks(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Value"})
	require.NoError(t, err)

	// Write first chunk
	types := []dukdb.Type{dukdb.TYPE_BIGINT}
	chunk1 := storage.NewDataChunkWithCapacity(types, 2)
	chunk1.AppendRow([]any{int64(1)})
	chunk1.AppendRow([]any{int64(2)})

	err = writer.WriteChunk(chunk1)
	require.NoError(t, err)

	// Write second chunk
	chunk2 := storage.NewDataChunkWithCapacity(types, 2)
	chunk2.AppendRow([]any{int64(3)})
	chunk2.AppendRow([]any{int64(4)})

	err = writer.WriteChunk(chunk2)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back and verify all 4 rows
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A2")
	require.NoError(t, err)
	assert.Equal(t, "1", val)

	val, err = f.GetCellValue("Sheet1", "A3")
	require.NoError(t, err)
	assert.Equal(t, "2", val)

	val, err = f.GetCellValue("Sheet1", "A4")
	require.NoError(t, err)
	assert.Equal(t, "3", val)

	val, err = f.GetCellValue("Sheet1", "A5")
	require.NoError(t, err)
	assert.Equal(t, "4", val)
}

func TestWriter_AutoWidth(t *testing.T) {
	var buf bytes.Buffer

	opts := DefaultWriterOptions()
	opts.AutoWidth = true

	writer, err := NewWriter(&buf, opts)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"ShortName", "VeryLongColumnNameForTesting"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{"A", "SomeVeryLongValueThatShouldExpandTheColumn"})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back - verify file is valid (auto-width was applied)
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Column widths should be different
	colAWidth, err := f.GetColWidth("Sheet1", "A")
	require.NoError(t, err)

	colBWidth, err := f.GetColWidth("Sheet1", "B")
	require.NoError(t, err)

	// Column B should be wider than column A due to longer header/values
	assert.Greater(t, colBWidth, colAWidth)
}

func TestWriter_NoAutoWidth(t *testing.T) {
	var buf bytes.Buffer

	opts := DefaultWriterOptions()
	opts.AutoWidth = false

	writer, err := NewWriter(&buf, opts)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Column"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{"Value"})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// File should be valid
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "Column", val)
}

func TestNewWriterToPath(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "xlsx_writer_test_*.xlsx")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpPath) }()

	writer, err := NewWriterToPath(tmpPath, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Name", "Value"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{"Test", int64(123)})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back from file
	f, err := excelize.OpenFile(tmpPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "Name", val)

	val, err = f.GetCellValue("Sheet1", "B2")
	require.NoError(t, err)
	assert.Equal(t, "123", val)
}

func TestWriter_GenerateColumnNames(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Don't set schema - it should be generated from chunk

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{"Val1", "Val2"})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Header should be generated names
	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "column0", val)

	val, err = f.GetCellValue("Sheet1", "B1")
	require.NoError(t, err)
	assert.Equal(t, "column1", val)
}

func TestWriter_RoundTrip(t *testing.T) {
	// Write data with the writer
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Name", "Age", "City"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)
	chunk.AppendRow([]any{"Alice", int64(30), "New York"})
	chunk.AppendRow([]any{"Bob", int64(25), "Los Angeles"})
	chunk.AppendRow([]any{"Charlie", int64(35), "Chicago"})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read data back with the reader
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Verify schema
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"Name", "Age", "City"}, schema)

	// Verify data
	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, readChunk.Count())

	assert.Equal(t, "Alice", readChunk.GetValue(0, 0))
	assert.Equal(t, int64(30), readChunk.GetValue(0, 1))
	assert.Equal(t, "New York", readChunk.GetValue(0, 2))

	assert.Equal(t, "Bob", readChunk.GetValue(1, 0))
	assert.Equal(t, int64(25), readChunk.GetValue(1, 1))
	assert.Equal(t, "Los Angeles", readChunk.GetValue(1, 2))

	assert.Equal(t, "Charlie", readChunk.GetValue(2, 0))
	assert.Equal(t, int64(35), readChunk.GetValue(2, 1))
	assert.Equal(t, "Chicago", readChunk.GetValue(2, 2))
}

func TestWriter_EmptyChunk(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Name"})
	require.NoError(t, err)

	// Write nil chunk - should be no-op
	err = writer.WriteChunk(nil)
	require.NoError(t, err)

	// Write empty chunk
	types := []dukdb.Type{dukdb.TYPE_VARCHAR}
	emptyChunk := storage.NewDataChunkWithCapacity(types, 0)

	err = writer.WriteChunk(emptyChunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// File should be valid with just header
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A1")
	require.NoError(t, err)
	assert.Equal(t, "Name", val)
}

func TestWriter_SetTypes(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"Value"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_DOUBLE})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{float64(3.14159)})

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	val, err := f.GetCellValue("Sheet1", "A2")
	require.NoError(t, err)
	assert.Contains(t, val, "3.14")
}
