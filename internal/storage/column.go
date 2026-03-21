package storage

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// StandardVectorSize is the standard size for vectors in the columnar storage.
const StandardVectorSize = 2048

// ValidityMask tracks NULL values using a bit array.
// A bit value of 1 means the value is valid (not NULL).
// A bit value of 0 means the value is NULL.
type ValidityMask struct {
	mask  []uint64
	count int
}

// NewValidityMask creates a new ValidityMask for the given count of elements.
func NewValidityMask(count int) *ValidityMask {
	// Calculate the number of uint64 entries needed
	entries := (count + 63) / 64
	mask := make([]uint64, entries)

	// Initialize all bits to 1 (all values valid by default)
	for i := range mask {
		mask[i] = ^uint64(0)
	}

	return &ValidityMask{
		mask:  mask,
		count: count,
	}
}

// IsValid returns whether the value at the given index is valid (not NULL).
func (v *ValidityMask) IsValid(idx int) bool {
	if idx < 0 || idx >= v.count {
		return false
	}
	entry := idx / 64
	bit := uint64(1) << (idx % 64)

	return v.mask[entry]&bit != 0
}

// SetValid sets the value at the given index as valid.
func (v *ValidityMask) SetValid(idx int) {
	if idx < 0 || idx >= v.count {
		return
	}
	entry := idx / 64
	bit := uint64(1) << (idx % 64)
	v.mask[entry] |= bit
}

// SetInvalid sets the value at the given index as invalid (NULL).
func (v *ValidityMask) SetInvalid(idx int) {
	if idx < 0 || idx >= v.count {
		return
	}
	entry := idx / 64
	bit := uint64(1) << (idx % 64)
	v.mask[entry] &^= bit
}

// AllValid returns whether all values are valid.
func (v *ValidityMask) AllValid() bool {
	for i := range len(v.mask) {
		if i == len(v.mask)-1 {
			// Check only the bits up to count for the last entry
			lastBits := v.count % 64
			if lastBits == 0 {
				lastBits = 64
			}
			expectedMask := (uint64(1) << lastBits) - 1
			if v.mask[i]&expectedMask != expectedMask {
				return false
			}
		} else {
			if v.mask[i] != ^uint64(0) {
				return false
			}
		}
	}

	return true
}

// CountValid returns the number of valid entries.
func (v *ValidityMask) CountValid() int {
	count := 0
	for i := range v.count {
		if v.IsValid(i) {
			count++
		}
	}

	return count
}

// Clone creates a deep copy of the ValidityMask.
func (v *ValidityMask) Clone() *ValidityMask {
	mask := make([]uint64, len(v.mask))
	copy(mask, v.mask)

	return &ValidityMask{
		mask:  mask,
		count: v.count,
	}
}

// SelectionVector represents a selection of indices for filtered operations.
type SelectionVector struct {
	indices []uint32
	count   int
}

// NewSelectionVector creates a new SelectionVector with the given capacity.
func NewSelectionVector(
	capacity int,
) *SelectionVector {
	return &SelectionVector{
		indices: make([]uint32, capacity),
		count:   0,
	}
}

// NewSelectionVectorFromRange creates a SelectionVector with sequential indices from 0 to count-1.
func NewSelectionVectorFromRange(
	count int,
) *SelectionVector {
	sv := &SelectionVector{
		indices: make([]uint32, count),
		count:   count,
	}
	for i := range count {
		sv.indices[i] = uint32(i)
	}

	return sv
}

// Get returns the index at the given position.
func (sv *SelectionVector) Get(pos int) uint32 {
	return sv.indices[pos]
}

// Set sets the index at the given position.
func (sv *SelectionVector) Set(
	pos int,
	idx uint32,
) {
	sv.indices[pos] = idx
}

// SetCount sets the count of valid indices.
func (sv *SelectionVector) SetCount(count int) {
	sv.count = count
}

// Count returns the number of selected indices.
func (sv *SelectionVector) Count() int {
	return sv.count
}

// Append adds an index to the selection.
func (sv *SelectionVector) Append(idx uint32) {
	if sv.count < len(sv.indices) {
		sv.indices[sv.count] = idx
		sv.count++
	}
}

// Reset clears the selection vector.
func (sv *SelectionVector) Reset() {
	sv.count = 0
}

// Clone creates a deep copy of the SelectionVector.
func (sv *SelectionVector) Clone() *SelectionVector {
	indices := make([]uint32, len(sv.indices))
	copy(indices, sv.indices)

	return &SelectionVector{
		indices: indices,
		count:   sv.count,
	}
}

// Vector represents a columnar vector of values with validity tracking.
type Vector struct {
	typ      dukdb.Type
	data     any
	validity *ValidityMask
	count    int
}

// NewVector creates a new Vector with the given type and capacity.
func NewVector(
	typ dukdb.Type,
	capacity int,
) *Vector {
	var data any

	switch typ {
	case dukdb.TYPE_BOOLEAN:
		data = make([]bool, capacity)
	case dukdb.TYPE_TINYINT:
		data = make([]int8, capacity)
	case dukdb.TYPE_SMALLINT:
		data = make([]int16, capacity)
	case dukdb.TYPE_INTEGER:
		data = make([]int32, capacity)
	case dukdb.TYPE_BIGINT:
		data = make([]int64, capacity)
	case dukdb.TYPE_UTINYINT:
		data = make([]uint8, capacity)
	case dukdb.TYPE_USMALLINT:
		data = make([]uint16, capacity)
	case dukdb.TYPE_UINTEGER:
		data = make([]uint32, capacity)
	case dukdb.TYPE_UBIGINT:
		data = make([]uint64, capacity)
	case dukdb.TYPE_FLOAT:
		data = make([]float32, capacity)
	case dukdb.TYPE_DOUBLE:
		data = make([]float64, capacity)
	case dukdb.TYPE_VARCHAR:
		data = make([]string, capacity)
	case dukdb.TYPE_BLOB:
		data = make([][]byte, capacity)
	case dukdb.TYPE_INVALID, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_DATE, dukdb.TYPE_TIME,
		dukdb.TYPE_INTERVAL, dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT, dukdb.TYPE_DECIMAL,
		dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS,
		dukdb.TYPE_ENUM, dukdb.TYPE_LIST, dukdb.TYPE_STRUCT, dukdb.TYPE_MAP,
		dukdb.TYPE_ARRAY, dukdb.TYPE_UUID, dukdb.TYPE_UNION, dukdb.TYPE_BIT,
		dukdb.TYPE_TIME_TZ, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY, dukdb.TYPE_BIGNUM,
		dukdb.TYPE_SQLNULL, dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY, dukdb.TYPE_LAMBDA,
		dukdb.TYPE_VARIANT:
		// For other types, use interface{} slice
		data = make([]any, capacity)
	}

	return &Vector{
		typ:      typ,
		data:     data,
		validity: NewValidityMask(capacity),
		count:    0,
	}
}

// Type returns the vector's data type.
func (v *Vector) Type() dukdb.Type {
	return v.typ
}

// SetType updates the type of the vector.
func (v *Vector) SetType(typ dukdb.Type) {
	v.typ = typ
}

// Data returns the underlying data.
func (v *Vector) Data() any {
	return v.data
}

// Validity returns the validity mask.
func (v *Vector) Validity() *ValidityMask {
	return v.validity
}

// Count returns the number of elements in the vector.
func (v *Vector) Count() int {
	return v.count
}

// SetCount sets the number of elements in the vector.
func (v *Vector) SetCount(count int) {
	v.count = count
}

// GetValue returns the value at the given index.
func (v *Vector) GetValue(idx int) any {
	if idx < 0 || idx >= v.count {
		return nil
	}
	if !v.validity.IsValid(idx) {
		return nil
	}

	switch data := v.data.(type) {
	case []bool:
		return data[idx]
	case []int8:
		return data[idx]
	case []int16:
		return data[idx]
	case []int32:
		return data[idx]
	case []int64:
		return data[idx]
	case []uint8:
		return data[idx]
	case []uint16:
		return data[idx]
	case []uint32:
		return data[idx]
	case []uint64:
		return data[idx]
	case []float32:
		return data[idx]
	case []float64:
		return data[idx]
	case []string:
		return data[idx]
	case [][]byte:
		return data[idx]
	case []any:
		return data[idx]
	default:
		return nil
	}
}

// SetValue sets the value at the given index.
func (v *Vector) SetValue(idx int, val any) {
	if idx < 0 {
		return
	}

	if val == nil {
		v.validity.SetInvalid(idx)

		return
	}

	v.validity.SetValid(idx)

	switch data := v.data.(type) {
	case []bool:
		if b, ok := val.(bool); ok {
			data[idx] = b
		}
	case []int8:
		data[idx] = toInt8(val)
	case []int16:
		data[idx] = toInt16(val)
	case []int32:
		data[idx] = toInt32(val)
	case []int64:
		data[idx] = toInt64(val)
	case []uint8:
		data[idx] = toUint8(val)
	case []uint16:
		data[idx] = toUint16(val)
	case []uint32:
		data[idx] = toUint32(val)
	case []uint64:
		data[idx] = toUint64(val)
	case []float32:
		data[idx] = toFloat32(val)
	case []float64:
		data[idx] = toFloat64(val)
	case []string:
		if s, ok := val.(string); ok {
			data[idx] = s
		}
	case [][]byte:
		if b, ok := val.([]byte); ok {
			data[idx] = b
		}
	case []any:
		data[idx] = val
	}

	if idx >= v.count {
		v.count = idx + 1
	}
}

// Clone creates a deep copy of the Vector.
func (v *Vector) Clone() *Vector {
	newVec := NewVector(v.typ, v.count)
	newVec.count = v.count
	newVec.validity = v.validity.Clone()

	switch data := v.data.(type) {
	case []bool:
		copy(newVec.data.([]bool), data[:v.count])
	case []int8:
		copy(newVec.data.([]int8), data[:v.count])
	case []int16:
		copy(newVec.data.([]int16), data[:v.count])
	case []int32:
		copy(newVec.data.([]int32), data[:v.count])
	case []int64:
		copy(newVec.data.([]int64), data[:v.count])
	case []uint8:
		copy(newVec.data.([]uint8), data[:v.count])
	case []uint16:
		copy(newVec.data.([]uint16), data[:v.count])
	case []uint32:
		copy(newVec.data.([]uint32), data[:v.count])
	case []uint64:
		copy(newVec.data.([]uint64), data[:v.count])
	case []float32:
		copy(newVec.data.([]float32), data[:v.count])
	case []float64:
		copy(newVec.data.([]float64), data[:v.count])
	case []string:
		copy(newVec.data.([]string), data[:v.count])
	case [][]byte:
		newData, ok := newVec.data.([][]byte)
		if !ok {
			return newVec
		}
		for i := range v.count {
			if data[i] != nil {
				newData[i] = make([]byte, len(data[i]))
				copy(newData[i], data[i])
			}
		}
	case []any:
		copy(newVec.data.([]any), data[:v.count])
	}

	return newVec
}

// Helper functions for type conversion

func toInt8(val any) int8 {
	switch v := val.(type) {
	case int8:
		return v
	case int:
		return int8(v)
	case int16:
		return int8(v)
	case int32:
		return int8(v)
	case int64:
		return int8(v)
	case float64:
		return int8(v)
	case float32:
		return int8(v)
	default:
		return 0
	}
}

func toInt16(val any) int16 {
	switch v := val.(type) {
	case int16:
		return v
	case int:
		return int16(v)
	case int8:
		return int16(v)
	case int32:
		return int16(v)
	case int64:
		return int16(v)
	case float64:
		return int16(v)
	case float32:
		return int16(v)
	default:
		return 0
	}
}

func toInt32(val any) int32 {
	switch v := val.(type) {
	case int32:
		return v
	case int:
		return int32(v)
	case int8:
		return int32(v)
	case int16:
		return int32(v)
	case int64:
		return int32(v)
	case float64:
		return int32(v)
	case float32:
		return int32(v)
	default:
		return 0
	}
}

func toInt64(val any) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func toUint8(val any) uint8 {
	switch v := val.(type) {
	case uint8:
		return v
	case uint:
		return uint8(v)
	case uint16:
		return uint8(v)
	case uint32:
		return uint8(v)
	case uint64:
		return uint8(v)
	case int:
		return uint8(v)
	default:
		return 0
	}
}

func toUint16(val any) uint16 {
	switch v := val.(type) {
	case uint16:
		return v
	case uint:
		return uint16(v)
	case uint8:
		return uint16(v)
	case uint32:
		return uint16(v)
	case uint64:
		return uint16(v)
	case int:
		return uint16(v)
	default:
		return 0
	}
}

func toUint32(val any) uint32 {
	switch v := val.(type) {
	case uint32:
		return v
	case uint:
		return uint32(v)
	case uint8:
		return uint32(v)
	case uint16:
		return uint32(v)
	case uint64:
		return uint32(v)
	case int:
		return uint32(v)
	default:
		return 0
	}
}

func toUint64(val any) uint64 {
	switch v := val.(type) {
	case uint64:
		return v
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case int:
		return uint64(v)
	default:
		return 0
	}
}

func toFloat32(val any) float32 {
	switch v := val.(type) {
	case float32:
		return v
	case float64:
		return float32(v)
	case int:
		return float32(v)
	case int64:
		return float32(v)
	default:
		return 0
	}
}

func toFloat64(val any) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}
