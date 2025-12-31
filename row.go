package dukdb

import "errors"

// Row provides row-oriented access within a DataChunk.
// It maintains a reference to the parent chunk and the row index.
type Row struct {
	// chunk is a reference to the parent DataChunk.
	chunk *DataChunk

	// rowIdx is the index of this row within the chunk.
	rowIdx int
}

// NewRow creates a new Row accessor for a specific row in a DataChunk.
func NewRow(chunk *DataChunk, rowIdx int) Row {
	return Row{
		chunk:  chunk,
		rowIdx: rowIdx,
	}
}

// IsProjected returns whether the column at colIdx is projected.
// Returns true if the column is projected (accessible), false otherwise.
// If the chunk has no projection, all columns are considered projected.
func (r Row) IsProjected(colIdx int) bool {
	if r.chunk == nil {
		return false
	}
	if r.chunk.projection == nil {
		return colIdx >= 0 && colIdx < len(r.chunk.columns)
	}

	if colIdx < 0 || colIdx >= len(r.chunk.projection) {
		return false
	}

	actualCol := r.chunk.projection[colIdx]

	return actualCol >= 0 && actualCol < len(r.chunk.columns)
}

// SetRowValue sets a value at the specified column index for this row.
// If the column is not projected, the value is silently ignored.
func (r Row) SetRowValue(colIdx int, val any) error {
	if r.chunk == nil {
		return errors.New("row has no associated chunk")
	}

	return r.chunk.SetValue(colIdx, r.rowIdx, val)
}

// GetRowValue gets a value at the specified column index for this row.
func (r Row) GetRowValue(colIdx int) (any, error) {
	if r.chunk == nil {
		return nil, errors.New("row has no associated chunk")
	}

	return r.chunk.GetValue(colIdx, r.rowIdx)
}

// RowIndex returns the row index within the parent chunk.
func (r Row) RowIndex() int {
	return r.rowIdx
}

// Chunk returns the parent DataChunk.
func (r Row) Chunk() *DataChunk {
	return r.chunk
}

// SetRowValue is a generic function for type-safe row value setting.
// If the column is not projected, the value is silently ignored.
func SetRowValue[T any](row Row, colIdx int, val T) error {
	if row.chunk == nil {
		return errors.New("row has no associated chunk")
	}

	return SetChunkValue(*row.chunk, colIdx, row.rowIdx, val)
}
