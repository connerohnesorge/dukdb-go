package storage

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// DataChunk represents a collection of vectors, forming a column-oriented data chunk.
type DataChunk struct {
	vectors   []*Vector
	count     int
	capacity  int
	selection *SelectionVector
}

// NewDataChunk creates a new DataChunk with the given column types.
func NewDataChunk(types []dukdb.Type) *DataChunk {
	return NewDataChunkWithCapacity(
		types,
		StandardVectorSize,
	)
}

// NewDataChunkWithCapacity creates a new DataChunk with the given column types and capacity.
func NewDataChunkWithCapacity(
	types []dukdb.Type,
	capacity int,
) *DataChunk {
	vectors := make([]*Vector, len(types))
	for i, t := range types {
		vectors[i] = NewVector(t, capacity)
	}

	return &DataChunk{
		vectors:  vectors,
		count:    0,
		capacity: capacity,
	}
}

// ColumnCount returns the number of columns.
func (dc *DataChunk) ColumnCount() int {
	return len(dc.vectors)
}

// Count returns the number of rows.
func (dc *DataChunk) Count() int {
	return dc.count
}

// SetCount sets the number of rows.
func (dc *DataChunk) SetCount(count int) {
	dc.count = count
	for _, v := range dc.vectors {
		v.SetCount(count)
	}
}

// Capacity returns the maximum number of rows.
func (dc *DataChunk) Capacity() int {
	return dc.capacity
}

// GetVector returns the vector at the given index.
func (dc *DataChunk) GetVector(idx int) *Vector {
	if idx < 0 || idx >= len(dc.vectors) {
		return nil
	}

	return dc.vectors[idx]
}

// SetVector sets the vector at the given index.
func (dc *DataChunk) SetVector(
	idx int,
	vec *Vector,
) {
	if idx >= 0 && idx < len(dc.vectors) {
		dc.vectors[idx] = vec
	}
}

// Selection returns the selection vector (if any).
func (dc *DataChunk) Selection() *SelectionVector {
	return dc.selection
}

// SetSelection sets the selection vector.
func (dc *DataChunk) SetSelection(
	sel *SelectionVector,
) {
	dc.selection = sel
}

// ClearSelection clears the selection vector.
func (dc *DataChunk) ClearSelection() {
	dc.selection = nil
}

// GetValue returns the value at the given row and column.
func (dc *DataChunk) GetValue(row, col int) any {
	if col < 0 || col >= len(dc.vectors) {
		return nil
	}

	actualRow := row
	if dc.selection != nil &&
		row < dc.selection.Count() {
		actualRow = int(dc.selection.Get(row))
	}

	return dc.vectors[col].GetValue(actualRow)
}

// SetValue sets the value at the given row and column.
func (dc *DataChunk) SetValue(
	row, col int,
	val any,
) {
	if col < 0 || col >= len(dc.vectors) {
		return
	}
	dc.vectors[col].SetValue(row, val)
}

// AppendRow appends a row of values to the chunk.
func (dc *DataChunk) AppendRow(
	values []any,
) bool {
	if dc.count >= dc.capacity {
		return false
	}
	if len(values) != len(dc.vectors) {
		return false
	}

	for i, val := range values {
		dc.vectors[i].SetValue(dc.count, val)
	}
	dc.count++

	return true
}

// Reset clears all data from the chunk.
func (dc *DataChunk) Reset() {
	dc.count = 0
	dc.selection = nil
	for _, v := range dc.vectors {
		v.count = 0
	}
}

// Clone creates a deep copy of the DataChunk.
func (dc *DataChunk) Clone() *DataChunk {
	vectors := make([]*Vector, len(dc.vectors))
	for i, v := range dc.vectors {
		vectors[i] = v.Clone()
	}

	newChunk := &DataChunk{
		vectors:  vectors,
		count:    dc.count,
		capacity: dc.capacity,
	}

	if dc.selection != nil {
		newChunk.selection = dc.selection.Clone()
	}

	return newChunk
}

// Flatten applies the selection vector and creates a new chunk with only selected rows.
func (dc *DataChunk) Flatten() *DataChunk {
	if dc.selection == nil {
		return dc.Clone()
	}

	types := make([]dukdb.Type, len(dc.vectors))
	for i, v := range dc.vectors {
		types[i] = v.Type()
	}

	newChunk := NewDataChunkWithCapacity(
		types,
		dc.selection.Count(),
	)

	for i := 0; i < dc.selection.Count(); i++ {
		srcRow := int(dc.selection.Get(i))
		values := make([]any, len(dc.vectors))
		for j, v := range dc.vectors {
			values[j] = v.GetValue(srcRow)
		}
		newChunk.AppendRow(values)
	}

	return newChunk
}

// Types returns the types of all columns.
func (dc *DataChunk) Types() []dukdb.Type {
	types := make([]dukdb.Type, len(dc.vectors))
	for i, v := range dc.vectors {
		types[i] = v.Type()
	}

	return types
}
