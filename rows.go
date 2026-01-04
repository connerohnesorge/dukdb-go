package dukdb

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"
)

// Rows implements driver.Rows for DuckDB query results.
// It stores pre-parsed row data from backend JSON responses.
// Rows is not thread-safe and should be used from a single goroutine.
type Rows struct {
	columns  []string // Column names in order
	colTypes []Type   // DuckDB type for each column
	data     [][]any  // Pre-parsed row data
	index    int      // Current row index (-1 before first Next)
	closed   bool     // Closed flag
}

// NewRows creates a new Rows instance from backend response data.
// columns contains the column names in order.
// colTypes contains the DuckDB type for each column.
// data contains the pre-parsed row data.
func NewRows(
	columns []string,
	colTypes []Type,
	data [][]any,
) *Rows {
	return &Rows{
		columns:  columns,
		colTypes: colTypes,
		data:     data,
		index:    -1,
		closed:   false,
	}
}

// Columns returns the column names of the result set.
// Implements driver.Rows.
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the Rows, preventing further iteration.
// Implements driver.Rows.
func (r *Rows) Close() error {
	if r.closed {
		return nil // Already closed, idempotent
	}
	r.closed = true
	r.data = nil // Release memory

	return nil
}

// Next advances to the next row and populates dest with row values.
// Returns io.EOF when no more rows are available.
// Implements driver.Rows.
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	r.index++

	if r.index >= len(r.data) {
		return io.EOF
	}

	row := r.data[r.index]
	for i, val := range row {
		dest[i] = val
	}

	return nil
}

// Scan scans the current row into the provided destination pointers.
// Each dest must be a pointer to a value that the column can be scanned into.
// Returns an error if the Rows is closed, no current row exists, or
// the destination count doesn't match the column count.
func (r *Rows) Scan(dest ...any) error {
	if r.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "rows are closed",
		}
	}

	if r.index < 0 || r.index >= len(r.data) {
		return &Error{
			Type: ErrorTypeBadState,
			Msg:  "no current row",
		}
	}

	if len(dest) != len(r.columns) {
		return &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"expected %d destinations, got %d",
				len(r.columns),
				len(dest),
			),
		}
	}

	row := r.data[r.index]
	for i, val := range row {
		if err := scanValue(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// ColumnTypeScanType returns the Go type suitable for scanning into
// for the column at the given index.
// Implements driver.RowsColumnTypeScanType.
func (r *Rows) ColumnTypeScanType(
	index int,
) reflect.Type {
	if index < 0 || index >= len(r.colTypes) {
		return reflectTypeAny
	}

	switch r.colTypes[index] {
	case TYPE_BOOLEAN:
		return reflectTypeBool
	case TYPE_TINYINT:
		return reflectTypeInt8
	case TYPE_SMALLINT:
		return reflectTypeInt16
	case TYPE_INTEGER:
		return reflectTypeInt32
	case TYPE_BIGINT:
		return reflectTypeInt64
	case TYPE_UTINYINT:
		return reflectTypeUint8
	case TYPE_USMALLINT:
		return reflectTypeUint16
	case TYPE_UINTEGER:
		return reflectTypeUint32
	case TYPE_UBIGINT:
		return reflectTypeUint64
	case TYPE_FLOAT:
		return reflectTypeFloat32
	case TYPE_DOUBLE:
		return reflectTypeFloat64
	case TYPE_VARCHAR, TYPE_ENUM:
		return reflectTypeString
	case TYPE_BLOB:
		return reflectTypeBytes
	case TYPE_DATE,
		TYPE_TIME,
		TYPE_TIME_TZ,
		TYPE_TIMESTAMP,
		TYPE_TIMESTAMP_S,
		TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS,
		TYPE_TIMESTAMP_TZ:
		return reflectTypeTime
	case TYPE_UUID:
		return reflectTypeUUID
	case TYPE_INTERVAL:
		return reflectTypeInterval
	case TYPE_DECIMAL:
		return reflectTypeDecimal
	case TYPE_HUGEINT, TYPE_UHUGEINT:
		return reflectTypeBigInt
	case TYPE_LIST, TYPE_ARRAY:
		return reflectTypeSliceAny
	case TYPE_STRUCT:
		return reflectTypeMapStringAny
	case TYPE_MAP:
		return reflectTypeMap
	case TYPE_UNION:
		return reflectTypeUnion
	case TYPE_BIGNUM:
		return reflectTypeBigInt
	case TYPE_JSON, TYPE_VARIANT:
		return reflectTypeAny
	case TYPE_GEOMETRY:
		return reflectTypeBytes
	case TYPE_LAMBDA:
		return reflectTypeString
	case TYPE_INVALID, TYPE_BIT, TYPE_ANY, TYPE_SQLNULL:
		return reflectTypeAny
	}
	return reflectTypeAny
}

// ColumnTypeDatabaseTypeName returns the DuckDB type name for the
// column at the given index.
// Implements driver.RowsColumnTypeDatabaseTypeName.
func (r *Rows) ColumnTypeDatabaseTypeName(
	index int,
) string {
	if index < 0 || index >= len(r.colTypes) {
		return "UNKNOWN"
	}

	return r.colTypes[index].String()
}

// ColumnTypeNullable returns whether the column at the given index is nullable.
// All DuckDB columns are nullable, so this always returns (true, true).
// Implements driver.RowsColumnTypeNullable.
func (r *Rows) ColumnTypeNullable(
	index int,
) (nullable, ok bool) {
	return true, true
}

// Cached reflect.Type values for efficiency
var (
	reflectTypeAny = reflect.TypeOf((*any)(nil)).
			Elem()
	reflectTypeBool = reflect.TypeOf(
		false,
	)
	reflectTypeInt8 = reflect.TypeOf(
		int8(0),
	)
	reflectTypeInt16 = reflect.TypeOf(
		int16(0),
	)
	reflectTypeInt32 = reflect.TypeOf(
		int32(0),
	)
	reflectTypeInt64 = reflect.TypeOf(
		int64(0),
	)
	reflectTypeUint8 = reflect.TypeOf(
		uint8(0),
	)
	reflectTypeUint16 = reflect.TypeOf(
		uint16(0),
	)
	reflectTypeUint32 = reflect.TypeOf(
		uint32(0),
	)
	reflectTypeUint64 = reflect.TypeOf(
		uint64(0),
	)
	reflectTypeFloat32 = reflect.TypeOf(
		float32(0),
	)
	reflectTypeFloat64 = reflect.TypeOf(
		float64(0),
	)
	reflectTypeString = reflect.TypeOf("")
	reflectTypeBytes  = reflect.TypeOf(
		[]byte{},
	)
	reflectTypeTime = reflect.TypeOf(
		time.Time{},
	)
	reflectTypeUUID = reflect.TypeOf(
		UUID{},
	)
	reflectTypeInterval = reflect.TypeOf(
		Interval{},
	)
	reflectTypeDecimal = reflect.TypeOf(
		Decimal{},
	)
	reflectTypeBigInt = reflect.TypeOf(
		(*big.Int)(nil),
	)
	reflectTypeSliceAny = reflect.TypeOf(
		[]any{},
	)
	reflectTypeMapStringAny = reflect.TypeOf(
		map[string]any{},
	)
	reflectTypeMap = reflect.TypeOf(
		Map{},
	)
	reflectTypeUnion = reflect.TypeOf(
		Union{},
	)
)

// columnTypes returns TypeInfo for each column.
// For simple types, this creates a basic TypeInfo wrapper.
// For complex types (LIST, STRUCT, etc.), full type info would need
// to be stored separately during query execution.
func (r *Rows) columnTypes() []TypeInfo {
	result := make([]TypeInfo, len(r.colTypes))
	for i, t := range r.colTypes {
		info, err := NewTypeInfo(t)
		if err != nil {
			// For complex types, create a minimal wrapper
			result[i] = &typeInfo{typ: t}
		} else {
			result[i] = info
		}
	}

	return result
}

// Interface assertions to verify Rows implements the expected interfaces.
var (
	_ driver.Rows = (*Rows)(
		nil,
	)
	_ driver.RowsColumnTypeScanType = (*Rows)(
		nil,
	)
	_ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(
		nil,
	)
	_ driver.RowsColumnTypeNullable = (*Rows)(
		nil,
	)
)
