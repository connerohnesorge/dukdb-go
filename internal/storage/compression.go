package storage

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// SegmentCompressionType identifies the columnar compression scheme.
type SegmentCompressionType uint8

const (
	SegmentCompressionNone       SegmentCompressionType = 0
	SegmentCompressionConstant   SegmentCompressionType = 1
	SegmentCompressionDictionary SegmentCompressionType = 2
	SegmentCompressionRLE        SegmentCompressionType = 3
)

func (t SegmentCompressionType) String() string {
	switch t {
	case SegmentCompressionNone:
		return "None"
	case SegmentCompressionConstant:
		return "Constant"
	case SegmentCompressionDictionary:
		return "Dictionary"
	case SegmentCompressionRLE:
		return "RLE"
	default:
		return "Unknown"
	}
}

// CompressedSegment holds the compressed representation of a column segment.
type CompressedSegment struct {
	Type      SegmentCompressionType
	DataType  dukdb.Type
	Count     int           // number of logical rows
	NullCount int           // number of NULL values
	Validity  *ValidityMask // NULL bitmap
	Data      any           // scheme-specific payload
}

// ConstantPayload stores a single value for all non-NULL rows.
type ConstantPayload struct {
	Value any
}

// DictionaryPayload stores unique values and per-row indices.
type DictionaryPayload struct {
	Dictionary []any    // unique non-NULL values
	Indices    []uint16 // per-row index into Dictionary (nullDictIndex = NULL)
}

// RLEPayload stores (value, run-length) pairs.
type RLEPayload struct {
	Values     []any    // one per run (nil for NULL runs)
	RunLengths []uint32 // one per run
}

// Compression thresholds and constants.
const (
	DefaultDictionaryThreshold = 256
	DefaultRLEMinRunLength     = 2.0
	nullDictIndex              = 0xFFFF
)

// CompressConstant attempts to compress a Vector using constant compression.
// Returns nil if not all non-NULL values are identical.
func CompressConstant(vec *Vector) *CompressedSegment {
	count := vec.Count()
	if count == 0 {
		return &CompressedSegment{
			Type:      SegmentCompressionConstant,
			DataType:  vec.Type(),
			Count:     0,
			NullCount: 0,
			Validity:  NewValidityMask(0),
			Data:      &ConstantPayload{Value: nil},
		}
	}

	var constantVal any
	allNull := true
	nullCount := 0
	validity := NewValidityMask(count)

	for i := range count {
		val := vec.GetValue(i)
		if val == nil {
			validity.SetInvalid(i)
			nullCount++

			continue
		}

		if allNull {
			constantVal = val
			allNull = false
		} else if !valuesEqual(constantVal, val) {
			return nil
		}
	}

	return &CompressedSegment{
		Type:      SegmentCompressionConstant,
		DataType:  vec.Type(),
		Count:     count,
		NullCount: nullCount,
		Validity:  validity,
		Data:      &ConstantPayload{Value: constantVal},
	}
}

// DecompressConstant decompresses a Constant-compressed segment.
func DecompressConstant(seg *CompressedSegment, startRow, rowCount int) *Vector {
	if rowCount <= 0 || startRow >= seg.Count {
		return NewVector(seg.DataType, 0)
	}

	n := rowCount
	if startRow+n > seg.Count {
		n = seg.Count - startRow
	}

	vec := NewVector(seg.DataType, n)
	payload, _ := seg.Data.(*ConstantPayload)

	for i := range n {
		srcIdx := startRow + i
		if seg.Validity != nil && !seg.Validity.IsValid(srcIdx) {
			vec.SetValue(i, nil)
		} else {
			vec.SetValue(i, payload.Value)
		}
	}

	vec.SetCount(n)

	return vec
}

// CompressDictionary attempts to compress a Vector using dictionary compression.
// Returns nil if the number of distinct values exceeds the threshold.
func CompressDictionary(vec *Vector, threshold int) *CompressedSegment {
	count := vec.Count()
	if count == 0 {
		return nil
	}

	dictMap := make(map[any]uint16)
	var dictionary []any
	indices := make([]uint16, count)
	validity := NewValidityMask(count)
	nullCount := 0

	for i := range count {
		val := vec.GetValue(i)
		if val == nil {
			validity.SetInvalid(i)
			indices[i] = nullDictIndex
			nullCount++

			continue
		}

		key := normalizeForMap(val)
		idx, exists := dictMap[key]
		if !exists {
			if len(dictionary) >= threshold {
				return nil
			}

			idx = uint16(len(dictionary))
			dictMap[key] = idx
			dictionary = append(dictionary, val)
		}

		indices[i] = idx
	}

	return &CompressedSegment{
		Type:      SegmentCompressionDictionary,
		DataType:  vec.Type(),
		Count:     count,
		NullCount: nullCount,
		Validity:  validity,
		Data: &DictionaryPayload{
			Dictionary: dictionary,
			Indices:    indices,
		},
	}
}

// DecompressDictionary decompresses a Dictionary-compressed segment.
func DecompressDictionary(seg *CompressedSegment, startRow, rowCount int) *Vector {
	if rowCount <= 0 || startRow >= seg.Count {
		return NewVector(seg.DataType, 0)
	}

	n := rowCount
	if startRow+n > seg.Count {
		n = seg.Count - startRow
	}

	vec := NewVector(seg.DataType, n)
	payload, _ := seg.Data.(*DictionaryPayload)

	for i := range n {
		srcIdx := startRow + i
		if seg.Validity != nil && !seg.Validity.IsValid(srcIdx) {
			vec.SetValue(i, nil)
		} else {
			dictIdx := payload.Indices[srcIdx]
			vec.SetValue(i, payload.Dictionary[dictIdx])
		}
	}

	vec.SetCount(n)

	return vec
}
