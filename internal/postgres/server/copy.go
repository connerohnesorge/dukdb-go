// Package server implements PostgreSQL wire protocol COPY support.
// This file provides handlers for COPY TO STDOUT and COPY FROM STDIN commands.
package server

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
)

// CopyFormat represents the format of COPY data.
type CopyFormat int

const (
	// CopyFormatText is the default tab-separated text format.
	CopyFormatText CopyFormat = iota
	// CopyFormatCSV is comma-separated values format.
	CopyFormatCSV
	// CopyFormatBinary is PostgreSQL binary COPY format.
	CopyFormatBinary
)

// CopyOptions holds options for COPY commands.
type CopyOptions struct {
	Format    CopyFormat
	Header    bool     // Include header row (CSV only)
	Delimiter byte     // Field delimiter (default: tab for text, comma for CSV)
	Null      string   // NULL value string (default: empty for CSV, \N for text)
	Quote     byte     // Quote character for CSV (default: ")
	Escape    byte     // Escape character for CSV (default: ")
	Columns   []string // Specific columns to copy
	Encoding  string   // Character encoding
}

// DefaultCopyOptions returns default options for COPY commands.
func DefaultCopyOptions() *CopyOptions {
	return &CopyOptions{
		Format:    CopyFormatText,
		Header:    false,
		Delimiter: '\t',
		Null:      "\\N",
		Quote:     '"',
		Escape:    '"',
		Encoding:  "UTF8",
	}
}

// ServerCopyOutResponse is the PostgreSQL COPY OUT response message type.
const ServerCopyOutResponse byte = 'H'

// ServerCopyData is the PostgreSQL COPY data message type.
const ServerCopyData byte = 'd'

// ServerCopyDone is the PostgreSQL COPY done message type.
const ServerCopyDone byte = 'c'

// CopyCommand represents a parsed COPY command.
type CopyCommand struct {
	IsFrom      bool   // true for FROM, false for TO
	Table       string // Table name for COPY table
	Query       string // Query for COPY (SELECT ...)
	Destination string // STDOUT, STDIN, or file path
	Options     *CopyOptions
}

// ParseCopyCommand parses a COPY SQL command and returns a CopyCommand struct.
func ParseCopyCommand(query string) (*CopyCommand, error) {
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	if !strings.HasPrefix(upperQuery, "COPY ") {
		return nil, fmt.Errorf("not a COPY command")
	}

	cmd := &CopyCommand{
		Options: DefaultCopyOptions(),
	}

	// Remove the "COPY " prefix
	rest := strings.TrimSpace(query[5:])
	upperRest := strings.ToUpper(rest)

	// Check for COPY (SELECT ...) syntax
	if strings.HasPrefix(rest, "(") {
		// Find the matching closing paren
		parenCount := 0
		endIdx := -1
		for i, c := range rest {
			if c == '(' {
				parenCount++
			} else if c == ')' {
				parenCount--
				if parenCount == 0 {
					endIdx = i
					break
				}
			}
		}
		if endIdx == -1 {
			return nil, fmt.Errorf("mismatched parentheses in COPY command")
		}
		cmd.Query = strings.TrimSpace(rest[1:endIdx])
		rest = strings.TrimSpace(rest[endIdx+1:])
		upperRest = strings.ToUpper(rest)
	} else {
		// Extract table name
		parts := strings.Fields(rest)
		if len(parts) == 0 {
			return nil, fmt.Errorf("missing table name in COPY command")
		}

		// Check if there's a column list
		tablePart := parts[0]
		if idx := strings.Index(tablePart, "("); idx != -1 {
			// Table name followed by column list without space
			cmd.Table = tablePart[:idx]
			// Find the closing paren
			endIdx := strings.Index(rest, ")")
			if endIdx == -1 {
				return nil, fmt.Errorf("mismatched parentheses in column list")
			}
			colList := rest[idx+1 : endIdx]
			cmd.Options.Columns = parseColumnList(colList)
			rest = strings.TrimSpace(rest[endIdx+1:])
		} else {
			cmd.Table = tablePart
			rest = strings.TrimSpace(rest[len(tablePart):])
		}
		upperRest = strings.ToUpper(rest)

		// Check for column list after table name
		if strings.HasPrefix(rest, "(") {
			endIdx := strings.Index(rest, ")")
			if endIdx == -1 {
				return nil, fmt.Errorf("mismatched parentheses in column list")
			}
			colList := rest[1:endIdx]
			cmd.Options.Columns = parseColumnList(colList)
			rest = strings.TrimSpace(rest[endIdx+1:])
			upperRest = strings.ToUpper(rest)
		}
	}

	// Parse FROM or TO
	if strings.HasPrefix(upperRest, "FROM ") {
		cmd.IsFrom = true
		rest = strings.TrimSpace(rest[5:])
	} else if strings.HasPrefix(upperRest, "TO ") {
		cmd.IsFrom = false
		rest = strings.TrimSpace(rest[3:])
	} else {
		return nil, fmt.Errorf("expected FROM or TO in COPY command")
	}
	upperRest = strings.ToUpper(rest)

	// Parse destination (STDOUT, STDIN, or file path)
	if strings.HasPrefix(upperRest, "STDOUT") {
		cmd.Destination = "STDOUT"
		rest = strings.TrimSpace(rest[6:])
	} else if strings.HasPrefix(upperRest, "STDIN") {
		cmd.Destination = "STDIN"
		rest = strings.TrimSpace(rest[5:])
	} else {
		// File path - for now we only support STDOUT/STDIN via wire protocol
		return nil, fmt.Errorf("COPY to/from file paths not supported via wire protocol, use STDOUT or STDIN")
	}

	// Parse options
	if err := parseCopyOptions(rest, cmd.Options); err != nil {
		return nil, err
	}

	return cmd, nil
}

// parseColumnList parses a comma-separated list of column names.
func parseColumnList(s string) []string {
	parts := strings.Split(s, ",")
	columns := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

// parseCopyOptions parses COPY options in various formats.
func parseCopyOptions(rest string, opts *CopyOptions) error {
	upperRest := strings.ToUpper(rest)

	// Parse WITH ( ... ) syntax
	if strings.HasPrefix(upperRest, "WITH ") {
		rest = strings.TrimSpace(rest[5:])
		upperRest = strings.ToUpper(rest)
	}

	if strings.HasPrefix(rest, "(") {
		endIdx := strings.LastIndex(rest, ")")
		if endIdx == -1 {
			return fmt.Errorf("mismatched parentheses in COPY options")
		}
		optStr := rest[1:endIdx]
		return parseOptionsList(optStr, opts)
	}

	// Parse old-style options (FORMAT csv, HEADER, etc.)
	return parseOldStyleOptions(rest, opts)
}

// parseOptionsList parses options within parentheses.
func parseOptionsList(optStr string, opts *CopyOptions) error {
	// Split by comma, but respect quoted strings
	parts := splitOptionsRespectQuotes(optStr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split into key and value
		kv := strings.SplitN(part, " ", 2)
		key := strings.ToUpper(strings.TrimSpace(kv[0]))
		var value string
		if len(kv) > 1 {
			value = strings.TrimSpace(kv[1])
		}

		if err := applyOption(key, value, opts); err != nil {
			return err
		}
	}

	return nil
}

// parseOldStyleOptions parses old-style COPY options.
func parseOldStyleOptions(rest string, opts *CopyOptions) error {
	// Look for FORMAT, HEADER, DELIMITER keywords
	re := regexp.MustCompile(
		`(?i)(FORMAT|HEADER|DELIMITER|NULL|QUOTE|ESCAPE)\s*(['"]?[^,\s]+['"]?)?`,
	)
	matches := re.FindAllStringSubmatch(rest, -1)

	for _, match := range matches {
		if len(match) >= 2 {
			key := strings.ToUpper(match[1])
			value := ""
			if len(match) >= 3 {
				value = match[2]
			}
			if err := applyOption(key, value, opts); err != nil {
				return err
			}
		}
	}

	return nil
}

// applyOption applies a single COPY option.
func applyOption(key, value string, opts *CopyOptions) error {
	// Strip quotes from value
	value = strings.Trim(value, "'\"")

	switch key {
	case "FORMAT":
		switch strings.ToUpper(value) {
		case "TEXT":
			opts.Format = CopyFormatText
			opts.Delimiter = '\t'
			opts.Null = "\\N"
		case "CSV":
			opts.Format = CopyFormatCSV
			opts.Delimiter = ','
			opts.Null = ""
		case "BINARY":
			opts.Format = CopyFormatBinary
		default:
			return fmt.Errorf("unknown COPY format: %s", value)
		}

	case "HEADER":
		// HEADER with no value or TRUE means include header
		if value == "" || strings.ToUpper(value) == "TRUE" || strings.ToUpper(value) == "ON" ||
			value == "1" {
			opts.Header = true
		} else if strings.ToUpper(value) == "FALSE" || strings.ToUpper(value) == "OFF" || value == "0" {
			opts.Header = false
		} else {
			opts.Header = true
		}

	case "DELIMITER":
		if len(value) > 0 {
			// Handle escape sequences
			if value == "\\t" {
				opts.Delimiter = '\t'
			} else {
				opts.Delimiter = value[0]
			}
		}

	case "NULL":
		opts.Null = value

	case "QUOTE":
		if len(value) > 0 {
			opts.Quote = value[0]
		}

	case "ESCAPE":
		if len(value) > 0 {
			opts.Escape = value[0]
		}

	case "ENCODING":
		opts.Encoding = value
	}

	return nil
}

// splitOptionsRespectQuotes splits options by comma while respecting quoted strings.
func splitOptionsRespectQuotes(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]

		if !inQuote && (c == '\'' || c == '"') {
			inQuote = true
			quoteChar = c
			current.WriteByte(c)
		} else if inQuote && c == quoteChar {
			inQuote = false
			current.WriteByte(c)
		} else if !inQuote && c == ',' {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// handleCopyCommand handles COPY TO STDOUT and COPY FROM STDIN commands.
func (h *Handler) handleCopyCommand(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	session *Session,
) error {
	cmd, err := ParseCopyCommand(query)
	if err != nil {
		return NewPgError(CodeSyntaxError, err.Error())
	}

	if cmd.IsFrom && cmd.Destination == "STDIN" {
		return h.handleCopyFromStdin(ctx, cmd, writer, session)
	} else if !cmd.IsFrom && cmd.Destination == "STDOUT" {
		return h.handleCopyToStdout(ctx, cmd, writer)
	}

	return NewPgError(
		CodeFeatureNotSupported,
		"only COPY TO STDOUT and COPY FROM STDIN are supported via wire protocol",
	)
}

// handleCopyToStdout handles COPY table TO STDOUT or COPY (SELECT ...) TO STDOUT.
func (h *Handler) handleCopyToStdout(
	ctx context.Context,
	cmd *CopyCommand,
	writer wire.DataWriter,
) error {
	// Build the SELECT query
	var selectQuery string
	if cmd.Query != "" {
		selectQuery = cmd.Query
	} else {
		if len(cmd.Options.Columns) > 0 {
			selectQuery = fmt.Sprintf("SELECT %s FROM %s", strings.Join(cmd.Options.Columns, ", "), cmd.Table)
		} else {
			selectQuery = fmt.Sprintf("SELECT * FROM %s", cmd.Table)
		}
	}

	// Execute the query to get data
	args := []driver.NamedValue{}
	rows, columns, err := h.server.conn.Query(ctx, selectQuery, args)
	if err != nil {
		return ToPgError(err)
	}

	// Create a CopyOut writer
	copyWriter := NewCopyOutWriter(writer, cmd.Options)

	// Write header if requested (CSV format only)
	if cmd.Options.Header && cmd.Options.Format == CopyFormatCSV {
		if err := copyWriter.WriteHeader(columns); err != nil {
			return ToPgError(err)
		}
	}

	// Write data rows
	rowCount := 0
	for _, row := range rows {
		values := make([]any, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}
		if err := copyWriter.WriteRow(values); err != nil {
			return ToPgError(err)
		}
		rowCount++
	}

	// Complete the COPY operation
	if err := copyWriter.Complete(); err != nil {
		return ToPgError(err)
	}

	// Send command complete
	return writer.Complete(fmt.Sprintf("COPY %d", rowCount))
}

// handleCopyFromStdin handles COPY table FROM STDIN.
func (h *Handler) handleCopyFromStdin(
	ctx context.Context,
	cmd *CopyCommand,
	writer wire.DataWriter,
	session *Session,
) error {
	if cmd.Table == "" {
		return NewPgError(CodeSyntaxError, "COPY FROM STDIN requires a table name")
	}

	// Determine columns - if not specified, get all columns from the table
	columns := cmd.Options.Columns
	if len(columns) == 0 {
		// Query table columns
		tableColumns, err := h.getTableColumns(ctx, cmd.Table)
		if err != nil {
			return ToPgError(err)
		}
		columns = tableColumns
	}

	// Build wire.Columns for CopyIn (for column type mapping)
	_ = buildColumnsForCopy(columns)

	// Determine format code
	formatCode := wire.TextFormat
	if cmd.Options.Format == CopyFormatBinary {
		formatCode = wire.BinaryFormat
	}

	// Initiate COPY IN
	copyReader, err := writer.CopyIn(formatCode)
	if err != nil {
		return ToPgError(err)
	}

	// Read and insert data
	rowCount := 0

	if cmd.Options.Format == CopyFormatBinary {
		// Binary format
		binaryReader, err := wire.NewBinaryColumnReader(ctx, copyReader)
		if err != nil {
			return ToPgError(err)
		}

		// Skip header if present
		if cmd.Options.Header {
			_, _ = binaryReader.Read(ctx)
		}

		for {
			values, err := binaryReader.Read(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return ToPgError(err)
			}

			if err := h.insertRow(ctx, cmd.Table, columns, values); err != nil {
				return ToPgError(err)
			}
			rowCount++
		}
	} else {
		// Text or CSV format
		csvBuffer := &bytes.Buffer{}
		csvReader := csv.NewReader(csvBuffer)
		csvReader.Comma = rune(cmd.Options.Delimiter)
		csvReader.LazyQuotes = true

		textReader, err := wire.NewTextColumnReader(ctx, copyReader, csvReader, csvBuffer, cmd.Options.Null)
		if err != nil {
			return ToPgError(err)
		}

		// Skip header if present
		if cmd.Options.Header {
			_, _ = textReader.Read(ctx)
		}

		for {
			values, err := textReader.Read(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return ToPgError(err)
			}

			if err := h.insertRow(ctx, cmd.Table, columns, values); err != nil {
				return ToPgError(err)
			}
			rowCount++
		}
	}

	return writer.Complete(fmt.Sprintf("COPY %d", rowCount))
}

// getTableColumns gets the column names for a table.
func (h *Handler) getTableColumns(ctx context.Context, table string) ([]string, error) {
	// Query to get column names
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", table)
	args := []driver.NamedValue{}
	_, columns, err := h.server.conn.Query(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// buildColumnsForCopy builds wire.Columns for COPY operations.
func buildColumnsForCopy(columnNames []string) wire.Columns {
	columns := make(wire.Columns, len(columnNames))
	for i, name := range columnNames {
		columns[i] = wire.Column{
			Name:  name,
			Oid:   OidText, // Default to text for COPY
			Width: -1,
		}
	}
	return columns
}

// insertRow inserts a single row into the table.
func (h *Handler) insertRow(
	ctx context.Context,
	table string,
	columns []string,
	values []any,
) error {
	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	args := make([]driver.NamedValue, len(values))
	for i, v := range values {
		args[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}

	_, err := h.server.conn.Execute(ctx, insertQuery, args)
	return err
}

// CopyOutWriter writes data in PostgreSQL COPY OUT protocol format.
type CopyOutWriter struct {
	writer  wire.DataWriter
	options *CopyOptions
	buffer  *bytes.Buffer
	started bool
}

// NewCopyOutWriter creates a new CopyOutWriter.
func NewCopyOutWriter(writer wire.DataWriter, options *CopyOptions) *CopyOutWriter {
	return &CopyOutWriter{
		writer:  writer,
		options: options,
		buffer:  &bytes.Buffer{},
	}
}

// WriteHeader writes the column header row (for CSV format with HEADER option).
func (w *CopyOutWriter) WriteHeader(columns []string) error {
	if w.options.Format != CopyFormatCSV {
		return nil
	}

	if w.options.Format == CopyFormatCSV {
		csvWriter := csv.NewWriter(w.buffer)
		csvWriter.Comma = rune(w.options.Delimiter)
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
		csvWriter.Flush()
	}

	return nil
}

// WriteRow writes a single data row in the configured format.
func (w *CopyOutWriter) WriteRow(values []any) error {
	switch w.options.Format {
	case CopyFormatText:
		return w.writeTextRow(values)
	case CopyFormatCSV:
		return w.writeCSVRow(values)
	case CopyFormatBinary:
		return w.writeBinaryRow(values)
	default:
		return fmt.Errorf("unknown copy format: %d", w.options.Format)
	}
}

// writeTextRow writes a row in text format (tab-separated).
func (w *CopyOutWriter) writeTextRow(values []any) error {
	for i, v := range values {
		if i > 0 {
			w.buffer.WriteByte(w.options.Delimiter)
		}
		if v == nil {
			w.buffer.WriteString(w.options.Null)
		} else {
			w.buffer.WriteString(formatValueForCopy(v))
		}
	}
	w.buffer.WriteByte('\n')
	return nil
}

// writeCSVRow writes a row in CSV format.
func (w *CopyOutWriter) writeCSVRow(values []any) error {
	csvWriter := csv.NewWriter(w.buffer)
	csvWriter.Comma = rune(w.options.Delimiter)

	record := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			record[i] = w.options.Null
		} else {
			record[i] = formatValueForCopy(v)
		}
	}

	if err := csvWriter.Write(record); err != nil {
		return err
	}
	csvWriter.Flush()
	return nil
}

// writeBinaryRow writes a row in PostgreSQL binary format.
func (w *CopyOutWriter) writeBinaryRow(values []any) error {
	// Write tuple header (number of columns as int16)
	if err := binary.Write(w.buffer, binary.BigEndian, int16(len(values))); err != nil {
		return err
	}

	// Write each field
	for _, v := range values {
		if v == nil {
			// NULL is represented as -1 (int32)
			if err := binary.Write(w.buffer, binary.BigEndian, int32(-1)); err != nil {
				return err
			}
		} else {
			// Write field length and data
			data := formatBinaryValue(v)
			if err := binary.Write(w.buffer, binary.BigEndian, int32(len(data))); err != nil {
				return err
			}
			w.buffer.Write(data)
		}
	}

	return nil
}

// Complete finishes the COPY OUT operation.
func (w *CopyOutWriter) Complete() error {
	// For binary format, write the trailer (-1 as int16)
	if w.options.Format == CopyFormatBinary {
		if err := binary.Write(w.buffer, binary.BigEndian, int16(-1)); err != nil {
			return err
		}
	}

	// The buffered data will be sent through the normal Row() mechanism
	// since psql-wire doesn't have direct CopyOut support.
	// We'll return the data as a single row with the raw bytes.
	return nil
}

// GetData returns the accumulated COPY data.
func (w *CopyOutWriter) GetData() []byte {
	return w.buffer.Bytes()
}

// formatValueForCopy formats a value for COPY text or CSV output.
func formatValueForCopy(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "t"
		}
		return "f"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatBinaryValue formats a value for PostgreSQL binary COPY format.
func formatBinaryValue(v any) []byte {
	switch val := v.(type) {
	case bool:
		if val {
			return []byte{1}
		}
		return []byte{0}
	case int16:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(val))
		return buf
	case int32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(val))
		return buf
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return buf
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return buf
	case float32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(val))
		return buf
	case float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return buf
	case string:
		return []byte(val)
	case []byte:
		return val
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// IsCopyCommand checks if a query is a COPY command.
func IsCopyCommand(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upperQuery, "COPY ")
}

// IsCopyToStdout checks if a query is a COPY TO STDOUT command.
func IsCopyToStdout(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upperQuery, "COPY ") && strings.Contains(upperQuery, "TO STDOUT")
}

// IsCopyFromStdin checks if a query is a COPY FROM STDIN command.
func IsCopyFromStdin(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upperQuery, "COPY ") && strings.Contains(upperQuery, "FROM STDIN")
}
