// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file implements the FileWriter interface for XLSX format.
package xlsx

import (
	"fmt"
	"io"
	"os"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/xuri/excelize/v2"
)

// Writer implements the FileWriter interface for XLSX files.
// It writes DataChunks to Excel XLSX format with configurable options.
type Writer struct {
	// file is the underlying excelize file handle.
	file *excelize.File
	// output is the destination io.Writer for streaming output.
	output io.Writer
	// closer handles cleanup of file handles.
	closer io.Closer
	// opts contains user-specified options.
	opts *WriterOptions
	// sheet is the name of the sheet being written to.
	sheet string
	// row is the current row position (1-based, as Excel uses 1-based rows).
	row int
	// columns holds column names (set via SetSchema).
	columns []string
	// columnTypes holds the type for each column (derived from first chunk).
	columnTypes []dukdb.Type
	// headerWritten tracks whether the header row has been written.
	headerWritten bool
	// maxWidths tracks the maximum width of each column for auto-width calculation.
	maxWidths []float64
}

// NewWriter creates a new XLSX writer to an io.Writer.
// If opts is nil, default options are used.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	return createWriter(w, nil, writerOpts)
}

// NewWriterToPath creates a new XLSX writer to a file path.
func NewWriterToPath(path string, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("xlsx: failed to create file: %w", err)
	}

	return createWriter(file, file, writerOpts)
}

// createWriter creates a Writer with the given configuration.
func createWriter(w io.Writer, closer io.Closer, opts *WriterOptions) (*Writer, error) {
	// Create a new Excel file
	f := excelize.NewFile()

	// Get the default sheet name and rename it if needed
	sheetName := opts.SheetName
	if sheetName == "" {
		sheetName = "Sheet1"
	}

	// Get default sheet and rename it
	defaultSheet := f.GetSheetName(0)
	if defaultSheet != sheetName {
		if err := f.SetSheetName(defaultSheet, sheetName); err != nil {
			return nil, fmt.Errorf("xlsx: failed to rename sheet: %w", err)
		}
	}

	return &Writer{
		file:          f,
		output:        w,
		closer:        closer,
		opts:          opts,
		sheet:         sheetName,
		row:           1,
		columns:       nil,
		columnTypes:   nil,
		headerWritten: false,
		maxWidths:     nil,
	}, nil
}

// SetSchema sets the column names for the output file.
// Must be called before WriteChunk if Header option is enabled.
func (w *Writer) SetSchema(columns []string) error {
	w.columns = make([]string, len(columns))
	copy(w.columns, columns)

	// Initialize max widths tracking for auto-width
	w.maxWidths = make([]float64, len(columns))
	for i, col := range columns {
		// Initialize with header width
		w.maxWidths[i] = float64(len(col))
	}

	return nil
}

// SetTypes sets the column types for the output file.
// This helps with proper value formatting when writing cells.
func (w *Writer) SetTypes(types []dukdb.Type) error {
	w.columnTypes = make([]dukdb.Type, len(types))
	copy(w.columnTypes, types)

	return nil
}

// WriteChunk writes a DataChunk to the XLSX file.
// On the first call, column types are inferred from the chunk if not already set.
// If Header is enabled and a schema was set, the header row is written first.
func (w *Writer) WriteChunk(chunk *storage.DataChunk) error {
	if chunk == nil {
		return nil
	}

	// Infer column types from the first chunk if not explicitly set
	if w.columnTypes == nil {
		w.columnTypes = chunk.Types()
	}

	// Generate default column names if not set via SetSchema
	if w.columns == nil {
		w.columns = generateColumnNames(chunk.ColumnCount())
		// Initialize max widths tracking
		w.maxWidths = make([]float64, len(w.columns))
		for i, col := range w.columns {
			w.maxWidths[i] = float64(len(col))
		}
	}

	// Write header on first chunk if enabled
	if !w.headerWritten && w.opts.Header {
		if err := w.writeHeader(); err != nil {
			return err
		}
	}

	// Write each row
	for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
		if err := w.writeRow(chunk, rowIdx); err != nil {
			return err
		}
	}

	return nil
}

// writeHeader writes the header row to the XLSX file.
func (w *Writer) writeHeader() error {
	for colIdx, colName := range w.columns {
		cell := CellAddress(colIdx, w.row)
		if cell == "" {
			return fmt.Errorf("xlsx: failed to generate cell address for column %d", colIdx)
		}

		if err := w.file.SetCellValue(w.sheet, cell, colName); err != nil {
			return fmt.Errorf("xlsx: failed to write header cell: %w", err)
		}
	}

	w.row++
	w.headerWritten = true

	return nil
}

// writeRow writes a single row from the DataChunk.
func (w *Writer) writeRow(chunk *storage.DataChunk, rowIdx int) error {
	for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
		cell := CellAddress(colIdx, w.row)
		if cell == "" {
			return fmt.Errorf(
				"xlsx: failed to generate cell address for row %d, column %d",
				w.row,
				colIdx,
			)
		}

		vec := chunk.GetVector(colIdx)
		if vec == nil {
			// Leave cell empty for missing vector
			continue
		}

		// Check if value is NULL
		if !vec.Validity().IsValid(rowIdx) {
			// Leave cell empty for NULL
			continue
		}

		value := chunk.GetValue(rowIdx, colIdx)
		if value == nil {
			// Leave cell empty for nil value
			continue
		}

		// Get column type for proper conversion
		var colType dukdb.Type
		if colIdx < len(w.columnTypes) {
			colType = w.columnTypes[colIdx]
		}

		// Convert and write the value
		if err := w.writeCellValue(cell, value, colType); err != nil {
			return err
		}

		// Update max width for auto-width calculation
		if w.opts.AutoWidth && colIdx < len(w.maxWidths) {
			width := w.estimateCellWidth(value, colType)
			if width > w.maxWidths[colIdx] {
				w.maxWidths[colIdx] = width
			}
		}
	}

	w.row++

	return nil
}

// writeCellValue writes a value to a cell with appropriate type handling.
//
//nolint:exhaustive // We handle common types; others fall through to default.
func (w *Writer) writeCellValue(cell string, value any, typ dukdb.Type) error {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		if b, ok := value.(bool); ok {
			return w.file.SetCellValue(w.sheet, cell, b)
		}

	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT:
		// Integers are written as numbers
		return w.file.SetCellValue(w.sheet, cell, value)

	case dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE:
		// Floats are written as numbers
		return w.file.SetCellValue(w.sheet, cell, value)

	case dukdb.TYPE_DATE:
		if t, ok := value.(time.Time); ok {
			// Write as Excel date serial number
			serial := timeToExcelDate(t)
			if err := w.file.SetCellValue(w.sheet, cell, serial); err != nil {
				return err
			}
			// Apply date format
			return w.applyDateFormat(cell)
		}

	case dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS,
		dukdb.TYPE_TIMESTAMP_TZ:
		if t, ok := value.(time.Time); ok {
			// Write as Excel date serial number (includes time component)
			serial := timeToExcelDate(t)
			if err := w.file.SetCellValue(w.sheet, cell, serial); err != nil {
				return err
			}
			// Apply datetime format
			return w.applyDateTimeFormat(cell)
		}

	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		if t, ok := value.(time.Time); ok {
			// Write as Excel time fraction (0.0 - 1.0 representing time of day)
			// Extract just the time portion
			hour, min, sec := t.Clock()
			nsec := t.Nanosecond()
			timeFraction := (float64(hour)*3600 + float64(min)*60 + float64(sec) + float64(nsec)/1e9) / 86400.0
			if err := w.file.SetCellValue(w.sheet, cell, timeFraction); err != nil {
				return err
			}
			// Apply time format
			return w.applyTimeFormat(cell)
		}

	case dukdb.TYPE_DECIMAL:
		if decimal, ok := value.(dukdb.Decimal); ok {
			// Convert decimal to string for precision
			return w.file.SetCellValue(w.sheet, cell, decimal.String())
		}

	case dukdb.TYPE_UUID:
		if uuid, ok := value.(dukdb.UUID); ok {
			return w.file.SetCellValue(w.sheet, cell, uuid.String())
		}

	case dukdb.TYPE_INTERVAL:
		if interval, ok := value.(dukdb.Interval); ok {
			return w.file.SetCellValue(w.sheet, cell, formatInterval(interval))
		}
	}

	// Default: write as string or native value
	return w.file.SetCellValue(w.sheet, cell, value)
}

// applyDateFormat applies a date format to a cell.
func (w *Writer) applyDateFormat(cell string) error {
	format := w.opts.DateFormat
	if format == "" {
		format = "yyyy-mm-dd"
	}

	style, err := w.file.NewStyle(&excelize.Style{
		NumFmt: w.getNumFmtID(format),
	})
	if err != nil {
		return fmt.Errorf("xlsx: failed to create date style: %w", err)
	}

	return w.file.SetCellStyle(w.sheet, cell, cell, style)
}

// applyDateTimeFormat applies a datetime format to a cell.
func (w *Writer) applyDateTimeFormat(cell string) error {
	format := w.opts.DateFormat
	if format == "" {
		format = "yyyy-mm-dd hh:mm:ss"
	}

	style, err := w.file.NewStyle(&excelize.Style{
		NumFmt: w.getNumFmtID(format),
	})
	if err != nil {
		return fmt.Errorf("xlsx: failed to create datetime style: %w", err)
	}

	return w.file.SetCellStyle(w.sheet, cell, cell, style)
}

// applyTimeFormat applies a time format to a cell.
func (w *Writer) applyTimeFormat(cell string) error {
	format := w.opts.TimeFormat
	if format == "" {
		format = "hh:mm:ss"
	}

	style, err := w.file.NewStyle(&excelize.Style{
		NumFmt: w.getNumFmtID(format),
	})
	if err != nil {
		return fmt.Errorf("xlsx: failed to create time style: %w", err)
	}

	return w.file.SetCellStyle(w.sheet, cell, cell, style)
}

// getNumFmtID returns the number format ID for a format string.
// Excel has built-in format IDs for common formats.
func (w *Writer) getNumFmtID(format string) int {
	// Map common formats to Excel built-in format IDs
	switch format {
	case "yyyy-mm-dd":
		return 14 // Date: m/d/yy or m/d/yyyy depending on locale
	case "hh:mm:ss":
		return 21 // Time: h:mm:ss
	case "yyyy-mm-dd hh:mm:ss":
		return 22 // Date + Time: m/d/yy h:mm
	default:
		// Use custom format - Excel will handle it
		// Return 0 for now; excelize will handle custom formats
		return 0
	}
}

// estimateCellWidth estimates the display width of a value.
func (w *Writer) estimateCellWidth(value any, typ dukdb.Type) float64 {
	// Get string representation for width estimation
	var str string

	switch v := value.(type) {
	case string:
		str = v
	case time.Time:
		// Use appropriate format length based on type
		//nolint:exhaustive // We handle common date/time types; others fall through to default.
		switch typ {
		case dukdb.TYPE_DATE:
			str = v.Format("2006-01-02")
		case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
			str = v.Format("15:04:05")
		default:
			str = v.Format("2006-01-02 15:04:05")
		}
	case bool:
		if v {
			str = "TRUE"
		} else {
			str = "FALSE"
		}
	default:
		str = fmt.Sprint(value)
	}

	// Add some padding for Excel rendering
	return float64(len(str)) * 1.1
}

// applyColumnWidths sets the column widths based on calculated max widths.
func (w *Writer) applyColumnWidths() error {
	if !w.opts.AutoWidth || w.maxWidths == nil {
		return nil
	}

	for colIdx, width := range w.maxWidths {
		colLetter := IndexToColumnLetters(colIdx)
		if colLetter == "" {
			continue
		}

		// Set minimum width of 8 and maximum of 100
		adjustedWidth := width
		if adjustedWidth < 8 {
			adjustedWidth = 8
		}
		if adjustedWidth > 100 {
			adjustedWidth = 100
		}

		if err := w.file.SetColWidth(w.sheet, colLetter, colLetter, adjustedWidth); err != nil {
			return fmt.Errorf("xlsx: failed to set column width: %w", err)
		}
	}

	return nil
}

// Close flushes any buffered data and releases resources.
func (w *Writer) Close() error {
	// Apply column widths if auto-width is enabled
	if err := w.applyColumnWidths(); err != nil {
		if w.closer != nil {
			_ = w.closer.Close()
		}
		return err
	}

	// Write the Excel file to the output writer
	if w.output != nil {
		if err := w.file.Write(w.output); err != nil {
			if w.closer != nil {
				_ = w.closer.Close()
			}
			return fmt.Errorf("xlsx: failed to write file: %w", err)
		}
	}

	// Close the excelize file
	if err := w.file.Close(); err != nil {
		if w.closer != nil {
			_ = w.closer.Close()
		}
		return fmt.Errorf("xlsx: failed to close excelize file: %w", err)
	}

	// Close underlying resources
	if w.closer != nil {
		return w.closer.Close()
	}

	return nil
}

// formatInterval formats an Interval as a string.
func formatInterval(interval dukdb.Interval) string {
	var parts []string

	if interval.Months != 0 {
		years := interval.Months / 12
		months := interval.Months % 12

		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}

		if months != 0 {
			parts = append(parts, fmt.Sprintf("%d months", months))
		}
	}

	if interval.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", interval.Days))
	}

	if interval.Micros != 0 {
		hours := interval.Micros / (3600 * 1000000)
		remaining := interval.Micros % (3600 * 1000000)
		minutes := remaining / (60 * 1000000)
		remaining = remaining % (60 * 1000000)
		seconds := remaining / 1000000
		micros := remaining % 1000000

		if hours != 0 || minutes != 0 || seconds != 0 || micros != 0 {
			if micros != 0 {
				parts = append(
					parts,
					fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros),
				)
			} else {
				parts = append(parts, fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds))
			}
		}
	}

	if len(parts) == 0 {
		return "00:00:00"
	}

	return fmt.Sprintf("%s", joinStrings(parts, " "))
}

// joinStrings joins strings with a separator.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}

	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}

	return result
}

// Verify Writer implements FileWriter interface.
var _ fileio.FileWriter = (*Writer)(nil)
