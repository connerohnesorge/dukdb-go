package dukdb

import (
	"database/sql/driver"
	"errors"
	"io"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowsColumns(t *testing.T) {
	columns := []string{"id", "name", "value"}
	colTypes := []Type{
		TYPE_INTEGER,
		TYPE_VARCHAR,
		TYPE_DOUBLE,
	}
	data := [][]any{
		{int32(1), "test", 3.14},
	}

	rows := NewRows(columns, colTypes, data)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	assert.Equal(t, columns, rows.Columns())
}

func TestRowsNext(t *testing.T) {
	columns := []string{"id", "name"}
	colTypes := []Type{TYPE_INTEGER, TYPE_VARCHAR}
	data := [][]any{
		{int32(1), "first"},
		{int32(2), "second"},
		{int32(3), "third"},
	}

	rows := NewRows(columns, colTypes, data)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	dest := make([]driver.Value, 2)

	// First row
	err := rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int32(1), dest[0])
	assert.Equal(t, "first", dest[1])

	// Second row
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int32(2), dest[0])
	assert.Equal(t, "second", dest[1])

	// Third row
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int32(3), dest[0])
	assert.Equal(t, "third", dest[1])

	// No more rows
	err = rows.Next(dest)
	assert.Equal(t, io.EOF, err)
}

func TestRowsClose(t *testing.T) {
	rows := NewRows(
		[]string{"id"},
		[]Type{TYPE_INTEGER},
		[][]any{{int32(1)}},
	)

	err := rows.Close()
	require.NoError(t, err)
	assert.True(t, rows.closed)
	assert.Nil(t, rows.data)

	// Close is idempotent
	err = rows.Close()
	require.NoError(t, err)
}

func TestRowsScan(t *testing.T) {
	columns := []string{"id", "name", "value"}
	colTypes := []Type{
		TYPE_INTEGER,
		TYPE_VARCHAR,
		TYPE_DOUBLE,
	}
	data := [][]any{
		{int32(42), "hello", 3.14},
	}

	rows := NewRows(columns, colTypes, data)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	dest := make([]driver.Value, 3)
	err := rows.Next(dest)
	require.NoError(t, err)

	var id int32
	var name string
	var value float64

	err = rows.Scan(&id, &name, &value)
	require.NoError(t, err)
	assert.Equal(t, int32(42), id)
	assert.Equal(t, "hello", name)
	assert.Equal(t, 3.14, value)
}

func TestRowsScanAfterClose(t *testing.T) {
	rows := NewRows(
		[]string{"id"},
		[]Type{TYPE_INTEGER},
		[][]any{{int32(1)}},
	)
	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	require.NoError(t, err)
	err = rows.Close()
	require.NoError(t, err)

	var id int
	err = rows.Scan(&id)
	require.Error(t, err)

	var dukErr *Error
	require.True(t, errors.As(err, &dukErr))
	assert.Equal(t, ErrorTypeClosed, dukErr.Type)
	assert.Equal(t, "rows are closed", dukErr.Msg)
}

func TestRowsScanNoCurrentRow(t *testing.T) {
	rows := NewRows(
		[]string{"id"},
		[]Type{TYPE_INTEGER},
		[][]any{{int32(1)}},
	)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	// Scan before Next
	var id int
	err := rows.Scan(&id)
	require.Error(t, err)

	var dukErr *Error
	require.True(t, errors.As(err, &dukErr))
	assert.Equal(
		t,
		ErrorTypeBadState,
		dukErr.Type,
	)
	assert.Equal(t, "no current row", dukErr.Msg)
}

func TestRowsScanWrongDestCount(t *testing.T) {
	columns := []string{"id", "name"}
	colTypes := []Type{TYPE_INTEGER, TYPE_VARCHAR}
	data := [][]any{
		{int32(1), "test"},
	}

	rows := NewRows(columns, colTypes, data)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	dest := make([]driver.Value, 2)
	err := rows.Next(dest)
	require.NoError(t, err)

	// Try to scan with wrong number of destinations
	var id int
	err = rows.Scan(&id)
	require.Error(t, err)

	var dukErr *Error
	require.True(t, errors.As(err, &dukErr))
	assert.Equal(t, ErrorTypeInvalid, dukErr.Type)
	assert.Contains(
		t,
		dukErr.Msg,
		"expected 2 destinations, got 1",
	)
}

func TestRowsColumnTypeScanType(t *testing.T) {
	tests := []struct {
		colType  Type
		expected reflect.Type
	}{
		{TYPE_BOOLEAN, reflect.TypeOf(false)},
		{TYPE_TINYINT, reflect.TypeOf(int8(0))},
		{TYPE_SMALLINT, reflect.TypeOf(int16(0))},
		{TYPE_INTEGER, reflect.TypeOf(int32(0))},
		{TYPE_BIGINT, reflect.TypeOf(int64(0))},
		{TYPE_UTINYINT, reflect.TypeOf(uint8(0))},
		{
			TYPE_USMALLINT,
			reflect.TypeOf(uint16(0)),
		},
		{
			TYPE_UINTEGER,
			reflect.TypeOf(uint32(0)),
		},
		{TYPE_UBIGINT, reflect.TypeOf(uint64(0))},
		{TYPE_FLOAT, reflect.TypeOf(float32(0))},
		{TYPE_DOUBLE, reflect.TypeOf(float64(0))},
		{TYPE_VARCHAR, reflect.TypeOf("")},
		{TYPE_BLOB, reflect.TypeOf([]byte{})},
		{TYPE_DATE, reflect.TypeOf(time.Time{})},
		{TYPE_TIME, reflect.TypeOf(time.Time{})},
		{
			TYPE_TIMESTAMP,
			reflect.TypeOf(time.Time{}),
		},
		{
			TYPE_TIMESTAMP_TZ,
			reflect.TypeOf(time.Time{}),
		},
		{TYPE_UUID, reflect.TypeOf(UUID{})},
		{
			TYPE_INTERVAL,
			reflect.TypeOf(Interval{}),
		},
		{TYPE_DECIMAL, reflect.TypeOf(Decimal{})},
		{
			TYPE_HUGEINT,
			reflect.TypeOf((*big.Int)(nil)),
		},
		{TYPE_LIST, reflect.TypeOf([]any{})},
		{
			TYPE_STRUCT,
			reflect.TypeOf(map[string]any{}),
		},
		{TYPE_MAP, reflect.TypeOf(Map{})},
		{TYPE_UNION, reflect.TypeOf(Union{})},
	}

	for _, tt := range tests {
		t.Run(
			tt.colType.String(),
			func(t *testing.T) {
				rows := NewRows(
					[]string{"col"},
					[]Type{tt.colType},
					nil,
				)
				t.Cleanup(func() {
					assert.NoError(t, rows.Close())
				})

				result := rows.ColumnTypeScanType(
					0,
				)
				assert.Equal(
					t,
					tt.expected,
					result,
				)
			},
		)
	}
}

func TestRowsColumnTypeDatabaseTypeName(
	t *testing.T,
) {
	tests := []struct {
		colType  Type
		expected string
	}{
		{TYPE_BOOLEAN, "BOOLEAN"},
		{TYPE_INTEGER, "INTEGER"},
		{TYPE_VARCHAR, "VARCHAR"},
		{TYPE_TIMESTAMP, "TIMESTAMP"},
		{TYPE_UUID, "UUID"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			rows := NewRows(
				[]string{"col"},
				[]Type{tt.colType},
				nil,
			)
			t.Cleanup(func() {
				assert.NoError(t, rows.Close())
			})

			result := rows.ColumnTypeDatabaseTypeName(
				0,
			)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRowsColumnTypeNullable(t *testing.T) {
	rows := NewRows(
		[]string{"col"},
		[]Type{TYPE_INTEGER},
		nil,
	)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	nullable, ok := rows.ColumnTypeNullable(0)
	assert.True(t, nullable)
	assert.True(t, ok)
}

func TestRowsEmptyResultSet(t *testing.T) {
	rows := NewRows(
		[]string{"id", "name"},
		[]Type{TYPE_INTEGER, TYPE_VARCHAR},
		[][]any{},
	)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	dest := make([]driver.Value, 2)
	err := rows.Next(dest)
	assert.Equal(t, io.EOF, err)

	assert.Equal(
		t,
		[]string{"id", "name"},
		rows.Columns(),
	)
}

func TestRowsZeroColumnResult(t *testing.T) {
	rows := NewRows(
		[]string{},
		[]Type{},
		[][]any{},
	)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	assert.Empty(t, rows.Columns())
}

func TestRowsLargeResultSet(t *testing.T) {
	const numRows = 10000
	data := make([][]any, numRows)
	for i := range numRows {
		data[i] = []any{int32(i), "row"}
	}

	rows := NewRows(
		[]string{"id", "name"},
		[]Type{TYPE_INTEGER, TYPE_VARCHAR},
		data,
	)
	t.Cleanup(func() {
		assert.NoError(t, rows.Close())
	})

	dest := make([]driver.Value, 2)
	count := 0
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}

	assert.Equal(t, numRows, count)
}

// Test driver.Rows interface compliance
func TestRowsDriverInterface(t *testing.T) {
	var _ driver.Rows = (*Rows)(nil)
	var _ driver.RowsColumnTypeScanType = (*Rows)(nil)
	var _ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
	var _ driver.RowsColumnTypeNullable = (*Rows)(nil)
}
