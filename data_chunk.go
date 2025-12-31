package dukdb

import (
	"errors"
	"fmt"
)

// DataChunk storage of a DuckDB table.
// A DataChunk contains multiple column vectors and supports vectorized operations.
type DataChunk struct {
	// columns is a helper slice providing direct access to all columns.
	columns []vector

	// columnNames holds the column names, if known.
	columnNames []string

	// size is the current number of valid rows.
	size int

	// projection mapping of projected columns, when known (otherwise nil).
	// If projection[i] == -1, the column is not projected.
	projection []int

	// closed indicates if the chunk has been closed.
	closed bool
}

// GetDataChunkCapacity returns the capacity of a data chunk.
// This matches DuckDB's VECTOR_SIZE of 2048.
func GetDataChunkCapacity() int {
	return VectorSize
}

// GetSize returns the internal size of the data chunk (number of valid rows).
func (chunk *DataChunk) GetSize() int {
	return chunk.size
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetDataChunkCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if chunk.closed {
		return errors.New("data chunk is closed")
	}
	if size < 0 {
		return errors.New(
			"size cannot be negative",
		)
	}
	if size > GetDataChunkCapacity() {
		return fmt.Errorf(
			"size %d exceeds capacity %d",
			size,
			GetDataChunkCapacity(),
		)
	}
	chunk.size = size

	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(
	colIdx, rowIdx int,
) (any, error) {
	if chunk.closed {
		return nil, errors.New(
			"data chunk is closed",
		)
	}

	actualCol, err := chunk.verifyAndRewriteColIdx(
		colIdx,
	)
	if err != nil {
		return nil, err
	}

	if rowIdx < 0 || rowIdx >= chunk.size {
		return nil, fmt.Errorf(
			"row index %d out of range [0, %d)",
			rowIdx,
			chunk.size,
		)
	}

	column := &chunk.columns[actualCol]

	return column.getFn(column, rowIdx), nil
}

// SetValue writes a single value to a column in a data chunk.
// Note that this requires casting the type for each invocation.
// If the column is not projected, the value is ignored.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(
	colIdx, rowIdx int,
	val any,
) error {
	if chunk.closed {
		return errors.New("data chunk is closed")
	}

	actualCol, err := chunk.verifyAndRewriteColIdx(
		colIdx,
	)
	if err != nil {
		if errors.Is(err, errUnprojectedColumn) {
			return nil // Silently ignore unprojected columns.
		}

		return err
	}

	if rowIdx < 0 ||
		rowIdx >= GetDataChunkCapacity() {
		return fmt.Errorf(
			"row index %d out of range [0, %d)",
			rowIdx,
			GetDataChunkCapacity(),
		)
	}

	column := &chunk.columns[actualCol]
	if err := column.setFn(column, rowIdx, val); err != nil {
		return fmt.Errorf(
			"failed to set value at row %d, col %d: %w",
			rowIdx,
			colIdx,
			err,
		)
	}

	return nil
}

// SetChunkValue writes a single value to a column in a data chunk.
// The difference with `chunk.SetValue` is that `SetChunkValue` does not
// require casting the value to `any` (implicitly).
// If the column is not projected, the value is ignored.
// NOTE: Custom ENUM types must be passed as string.
func SetChunkValue[T any](
	chunk DataChunk,
	colIdx, rowIdx int,
	val T,
) error {
	if chunk.closed {
		return errors.New("data chunk is closed")
	}

	actualCol, err := chunk.verifyAndRewriteColIdx(
		colIdx,
	)
	if err != nil {
		if errors.Is(err, errUnprojectedColumn) {
			return nil
		}

		return err
	}

	return setVectorVal(
		&chunk.columns[actualCol],
		rowIdx,
		val,
	)
}

// GetColumnCount returns the number of columns in the chunk.
func (chunk *DataChunk) GetColumnCount() int {
	if chunk.projection != nil {
		return len(chunk.projection)
	}

	return len(chunk.columns)
}

// errUnprojectedColumn is returned when accessing an unprojected column.
var errUnprojectedColumn = errors.New(
	"unprojected column",
)

// verifyAndRewriteColIdx checks whether the provided column index is valid
// and rewrites it through the projection if one exists.
func (chunk *DataChunk) verifyAndRewriteColIdx(
	colIdx int,
) (int, error) {
	if chunk.projection == nil {
		if colIdx < 0 ||
			colIdx >= len(chunk.columns) {
			return colIdx, fmt.Errorf(
				"column index %d out of range [0, %d)",
				colIdx,
				len(chunk.columns),
			)
		}

		return colIdx, nil
	}

	if colIdx < 0 ||
		colIdx >= len(chunk.projection) {
		return colIdx, fmt.Errorf(
			"column index %d out of range [0, %d)",
			colIdx,
			len(chunk.projection),
		)
	}

	actualCol := chunk.projection[colIdx]
	if actualCol < 0 ||
		actualCol >= len(chunk.columns) {
		return colIdx, errUnprojectedColumn
	}

	return actualCol, nil
}

// initFromTypes initializes the chunk from type specifications.
func (chunk *DataChunk) initFromTypes(
	types []TypeInfo,
) error {
	columnCount := len(types)

	chunk.columns = make([]vector, columnCount)
	for i := range columnCount {
		chunk.columns[i] = *newVector(GetDataChunkCapacity())
		if err := chunk.columns[i].init(types[i], i); err != nil {
			return err
		}
	}

	chunk.size = 0
	chunk.closed = false

	return nil
}

// reset resets the chunk for reuse, preserving the column structure.
func (chunk *DataChunk) reset() {
	chunk.size = 0
	for i := range chunk.columns {
		chunk.columns[i].Reset()
	}
}

// close releases resources held by the chunk.
func (chunk *DataChunk) close() {
	chunk.columns = nil
	chunk.columnNames = nil
	chunk.projection = nil
	chunk.closed = true
}

// Close closes all column vectors and the chunk itself.
func (chunk *DataChunk) Close() {
	for i := range chunk.columns {
		chunk.columns[i].Close()
	}
	chunk.close()
}

// NewDataChunk creates a new DataChunk from type specifications.
func NewDataChunk(
	types []TypeInfo,
) (*DataChunk, error) {
	chunk := &DataChunk{}
	if err := chunk.initFromTypes(types); err != nil {
		return nil, err
	}

	return chunk, nil
}

// NewDataChunkWithProjection creates a new DataChunk with column projection.
// projection maps logical column indices to physical column indices.
// A value of -1 in projection indicates an unprojected column.
func NewDataChunkWithProjection(
	types []TypeInfo,
	projection []int,
) (*DataChunk, error) {
	chunk, err := NewDataChunk(types)
	if err != nil {
		return nil, err
	}
	chunk.projection = projection

	return chunk, nil
}
