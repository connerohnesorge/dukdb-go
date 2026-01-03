package storage

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/compression"
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

// ToDuckDBRowGroup converts a DataChunk to a DuckDB RowGroup format.
// This is used for persisting data chunks in the DuckDB file format.
func (dc *DataChunk) ToDuckDBRowGroup() *DuckDBRowGroup {
	rg := NewDuckDBRowGroup(uint16(len(dc.vectors)))
	rg.SetRowCount(uint64(dc.count))

	for i, vec := range dc.vectors {
		segment := dc.vectorToDuckDBSegment(vec)
		rg.AddColumn(i, segment)
	}

	return rg
}

// vectorToDuckDBSegment converts a Vector to a DuckDBColumnSegment.
func (dc *DataChunk) vectorToDuckDBSegment(vec *Vector) *DuckDBColumnSegment {
	// Map dukdb.Type to compression.LogicalTypeID
	typeID := mapTypeToLogicalTypeID(vec.Type())

	segment := NewDuckDBColumnSegment(typeID)

	// Select compression based on column type
	segment.Compression = compression.SelectCompression(typeID)

	// Copy validity bitmap from vector
	if vec.validity != nil {
		validityData := make([]uint64, len(vec.validity.mask))
		copy(validityData, vec.validity.mask)
		segment.SetValidity(validityData)
	}

	// Note: Data serialization is deferred to SerializeRowGroup
	// This method just sets up the structure

	return segment
}

// FromDuckDBRowGroup converts a DuckDB RowGroup to a DataChunk.
// This is used for reading persisted data in the DuckDB file format.
func FromDuckDBRowGroup(rg *DuckDBRowGroup) *DataChunk {
	types := make([]dukdb.Type, rg.MetaData.ColumnCount)
	for i := uint16(0); i < rg.MetaData.ColumnCount; i++ {
		segment := rg.GetColumn(int(i))
		if segment != nil {
			types[i] = mapLogicalTypeIDToType(segment.MetaData.Type)
		} else {
			types[i] = dukdb.TYPE_INVALID
		}
	}

	chunk := NewDataChunkWithCapacity(types, int(rg.MetaData.RowCount))
	chunk.SetCount(int(rg.MetaData.RowCount))

	for i := uint16(0); i < rg.MetaData.ColumnCount; i++ {
		segment := rg.GetColumn(int(i))
		if segment != nil {
			vec := chunk.GetVector(int(i))
			if vec != nil {
				// Copy validity bitmap
				if segment.Validity != nil {
					validityMask := make([]uint64, len(segment.Validity))
					copy(validityMask, segment.Validity)
					vec.validity.mask = validityMask
				}
				// Note: Data deserialization is deferred to DeserializeRowGroup
			}
		}
	}

	return chunk
}

// mapTypeToLogicalTypeID maps a dukdb.Type to a compression.LogicalTypeID.
func mapTypeToLogicalTypeID(t dukdb.Type) compression.LogicalTypeID {
	switch t {
	case dukdb.TYPE_BOOLEAN:
		return compression.LogicalTypeBoolean
	case dukdb.TYPE_TINYINT:
		return compression.LogicalTypeTinyInt
	case dukdb.TYPE_SMALLINT:
		return compression.LogicalTypeSmallInt
	case dukdb.TYPE_INTEGER:
		return compression.LogicalTypeInteger
	case dukdb.TYPE_BIGINT:
		return compression.LogicalTypeBigInt
	case dukdb.TYPE_UTINYINT:
		return compression.LogicalTypeUTinyInt
	case dukdb.TYPE_USMALLINT:
		return compression.LogicalTypeUSmallInt
	case dukdb.TYPE_UINTEGER:
		return compression.LogicalTypeUInteger
	case dukdb.TYPE_UBIGINT:
		return compression.LogicalTypeUBigInt
	case dukdb.TYPE_FLOAT:
		return compression.LogicalTypeFloat
	case dukdb.TYPE_DOUBLE:
		return compression.LogicalTypeDouble
	case dukdb.TYPE_TIMESTAMP:
		return compression.LogicalTypeTimestamp
	case dukdb.TYPE_DATE:
		return compression.LogicalTypeDate
	case dukdb.TYPE_TIME:
		return compression.LogicalTypeTime
	case dukdb.TYPE_INTERVAL:
		return compression.LogicalTypeInterval
	case dukdb.TYPE_VARCHAR:
		return compression.LogicalTypeVarchar
	case dukdb.TYPE_BLOB:
		return compression.LogicalTypeBlob
	case dukdb.TYPE_DECIMAL:
		return compression.LogicalTypeDecimal
	default:
		return compression.LogicalTypeInvalid
	}
}

// mapLogicalTypeIDToType maps a compression.LogicalTypeID to a dukdb.Type.
func mapLogicalTypeIDToType(typeID compression.LogicalTypeID) dukdb.Type {
	switch typeID {
	case compression.LogicalTypeBoolean:
		return dukdb.TYPE_BOOLEAN
	case compression.LogicalTypeTinyInt:
		return dukdb.TYPE_TINYINT
	case compression.LogicalTypeSmallInt:
		return dukdb.TYPE_SMALLINT
	case compression.LogicalTypeInteger:
		return dukdb.TYPE_INTEGER
	case compression.LogicalTypeBigInt:
		return dukdb.TYPE_BIGINT
	case compression.LogicalTypeUTinyInt:
		return dukdb.TYPE_UTINYINT
	case compression.LogicalTypeUSmallInt:
		return dukdb.TYPE_USMALLINT
	case compression.LogicalTypeUInteger:
		return dukdb.TYPE_UINTEGER
	case compression.LogicalTypeUBigInt:
		return dukdb.TYPE_UBIGINT
	case compression.LogicalTypeFloat:
		return dukdb.TYPE_FLOAT
	case compression.LogicalTypeDouble:
		return dukdb.TYPE_DOUBLE
	case compression.LogicalTypeTimestamp, compression.LogicalTypeTimestampSec,
		compression.LogicalTypeTimestampMS, compression.LogicalTypeTimestampNS:
		return dukdb.TYPE_TIMESTAMP
	case compression.LogicalTypeDate:
		return dukdb.TYPE_DATE
	case compression.LogicalTypeTime, compression.LogicalTypeTimeNS:
		return dukdb.TYPE_TIME
	case compression.LogicalTypeInterval:
		return dukdb.TYPE_INTERVAL
	case compression.LogicalTypeVarchar, compression.LogicalTypeChar:
		return dukdb.TYPE_VARCHAR
	case compression.LogicalTypeBlob:
		return dukdb.TYPE_BLOB
	case compression.LogicalTypeDecimal:
		return dukdb.TYPE_DECIMAL
	default:
		return dukdb.TYPE_INVALID
	}
}
