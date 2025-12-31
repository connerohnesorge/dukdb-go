package executor

import (
	"database/sql/driver"
	"errors"
	"io"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ResultSet wraps DataChunks to provide row-by-row iteration for database/sql driver.
// It implements the database/sql/driver.Rows interface, allowing DataChunk-based
// query results to be consumed by the database/sql package.
type ResultSet struct {
	// chunks holds the data chunks produced by query execution
	chunks []*storage.DataChunk

	// types holds the TypeInfo for each column
	types []dukdb.TypeInfo

	// columnNames holds the names of all columns in the result set
	columnNames []string

	// currentChunk is the index of the chunk currently being iterated
	currentChunk int

	// currentRow is the row index within the current chunk
	currentRow int

	// closed indicates whether the result set has been closed
	closed bool
}

// Compile-time check that ResultSet implements driver.Rows
var _ driver.Rows = (*ResultSet)(nil)

// Compile-time check that ResultSet implements driver.RowsColumnTypeDatabaseTypeName
var _ driver.RowsColumnTypeDatabaseTypeName = (*ResultSet)(nil)

// NewResultSet creates a new ResultSet from operator output.
// chunks contains the data chunks from query execution,
// types contains the TypeInfo for each column,
// columnNames contains the names of all columns.
func NewResultSet(
	chunks []*storage.DataChunk,
	types []dukdb.TypeInfo,
	columnNames []string,
) *ResultSet {
	return &ResultSet{
		chunks:       chunks,
		types:        types,
		columnNames:  columnNames,
		currentChunk: 0,
		currentRow:   0,
		closed:       false,
	}
}

// Columns returns the names of the columns in the result set.
// This implements the driver.Rows interface.
func (rs *ResultSet) Columns() []string {
	return rs.columnNames
}

// Close marks the result set as closed and releases resources.
// This implements the driver.Rows interface.
func (rs *ResultSet) Close() error {
	rs.closed = true

	return nil
}

// Next advances to the next row and populates dest with the row's values.
// Returns io.EOF when there are no more rows.
// This implements the driver.Rows interface.
func (rs *ResultSet) Next(dest []driver.Value) error {
	// Check if the result set is closed
	if rs.closed {
		return errors.New("result set is closed")
	}

	// Find the next non-empty chunk with available rows
	for {
		// Check if we've exhausted all chunks
		if rs.currentChunk >= len(rs.chunks) {
			return io.EOF
		}

		// Get the current chunk
		chunk := rs.chunks[rs.currentChunk]

		// Check if we've exhausted rows in the current chunk
		if rs.currentRow >= chunk.Count() {
			// Move to the next chunk
			rs.currentChunk++
			rs.currentRow = 0

			continue // Try the next chunk
		}

		// We have a valid row in the current chunk
		// Extract values from the current row
		numCols := chunk.ColumnCount()
		if len(dest) < numCols {
			return errors.New("destination slice too small")
		}

		for col := range numCols {
			value := chunk.GetValue(rs.currentRow, col)
			dest[col] = toDriverValue(value)
		}

		// Advance to the next row
		rs.currentRow++

		return nil
	}
}

// toDriverValue converts a value from the DataChunk to a driver.Value.
// driver.Value can be: nil, int64, float64, bool, []byte, string, time.Time
func toDriverValue(value any) driver.Value {
	if value == nil {
		return nil
	}

	// Convert to driver-compatible types
	switch v := value.(type) {
	case bool:
		return v
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		// Note: This may overflow for very large uint64 values
		return int64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		return v
	case []byte:
		return v
	default:
		// For other types, return as-is and let database/sql handle it
		return v
	}
}

// ColumnTypeDatabaseTypeName returns the database type name for the column at the given index.
// This implements the driver.RowsColumnTypeDatabaseTypeName interface.
// It returns the SQL type string (e.g., "INTEGER", "VARCHAR", "DECIMAL(10,2)") for the column.
// If the index is out of bounds, it returns an empty string.
func (rs *ResultSet) ColumnTypeDatabaseTypeName(index int) string {
	// Validate index bounds
	if index < 0 || index >= len(rs.types) {
		return ""
	}

	// Return the SQL type name from TypeInfo
	return rs.types[index].SQLType()
}

// ColumnTypeInfo returns the full TypeInfo for the column at the given index.
// This is a dukdb-specific extension that provides access to complete type metadata
// including InternalType, Details, and SQLType.
// If the index is out of bounds, it returns nil.
func (rs *ResultSet) ColumnTypeInfo(index int) dukdb.TypeInfo {
	// Validate index bounds
	if index < 0 || index >= len(rs.types) {
		return nil
	}

	// Return the TypeInfo for this column
	return rs.types[index]
}
