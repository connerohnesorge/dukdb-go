package storage

import (
	"fmt"
)

// CompressRLE attempts to compress a Vector using run-length encoding.
// Returns nil if the average run length is below the threshold.
func CompressRLE(vec *Vector, minAvgRunLength float64) *CompressedSegment {
	count := vec.Count()
	if count == 0 {
		return nil
	}

	validity := NewValidityMask(count)
	nullCount := 0

	runs := buildRLERuns(vec, count, validity, &nullCount)

	avgRunLen := float64(count) / float64(len(runs))
	if avgRunLen < minAvgRunLength {
		return nil
	}

	values := make([]any, len(runs))
	runLengths := make([]uint32, len(runs))

	for i, r := range runs {
		values[i] = r.value
		runLengths[i] = r.length
	}

	return &CompressedSegment{
		Type:      SegmentCompressionRLE,
		DataType:  vec.Type(),
		Count:     count,
		NullCount: nullCount,
		Validity:  validity,
		Data: &RLEPayload{
			Values:     values,
			RunLengths: runLengths,
		},
	}
}

type rleRun struct {
	value  any
	isNull bool
	length uint32
}

func buildRLERuns(
	vec *Vector,
	count int,
	validity *ValidityMask,
	nullCount *int,
) []rleRun {
	var runs []rleRun

	var current rleRun

	for i := range count {
		val := vec.GetValue(i)
		isNull := val == nil

		if isNull {
			validity.SetInvalid(i)
			*nullCount++
		}

		if i == 0 {
			current = rleRun{value: val, isNull: isNull, length: 1}

			continue
		}

		sameRun := (isNull && current.isNull) ||
			(!isNull && !current.isNull && valuesEqual(val, current.value))

		if sameRun {
			current.length++
		} else {
			runs = append(runs, current)
			current = rleRun{value: val, isNull: isNull, length: 1}
		}
	}

	if current.length > 0 {
		runs = append(runs, current)
	}

	return runs
}

// DecompressRLE decompresses an RLE-compressed segment.
func DecompressRLE(seg *CompressedSegment, startRow, rowCount int) *Vector {
	if rowCount <= 0 || startRow >= seg.Count {
		return NewVector(seg.DataType, 0)
	}

	n := rowCount
	if startRow+n > seg.Count {
		n = seg.Count - startRow
	}

	vec := NewVector(seg.DataType, n)
	payload, _ := seg.Data.(*RLEPayload)

	pos := 0
	runIdx := 0

	for runIdx < len(payload.RunLengths) && pos+int(payload.RunLengths[runIdx]) <= startRow {
		pos += int(payload.RunLengths[runIdx])
		runIdx++
	}

	outIdx := 0
	offsetInRun := startRow - pos

	for outIdx < n && runIdx < len(payload.RunLengths) {
		remaining := int(payload.RunLengths[runIdx]) - offsetInRun
		toWrite := remaining
		if toWrite > n-outIdx {
			toWrite = n - outIdx
		}

		val := payload.Values[runIdx]
		for j := range toWrite {
			vec.SetValue(outIdx+j, val)
		}

		outIdx += toWrite
		runIdx++
		offsetInRun = 0
	}

	vec.SetCount(n)

	return vec
}

// AnalyzeAndCompress analyzes a Vector and compresses it using the best scheme.
// Selection waterfall: Constant > Dictionary > RLE > None
func AnalyzeAndCompress(vec *Vector) *CompressedSegment {
	if vec.Count() == 0 {
		return nil
	}

	if seg := CompressConstant(vec); seg != nil {
		return seg
	}

	if seg := CompressDictionary(vec, DefaultDictionaryThreshold); seg != nil {
		return seg
	}

	return CompressRLE(vec, DefaultRLEMinRunLength)
}

// DecompressSegment decompresses a compressed segment for the given row range.
func DecompressSegment(seg *CompressedSegment, startRow, count int) *Vector {
	switch seg.Type {
	case SegmentCompressionConstant:
		return DecompressConstant(seg, startRow, count)
	case SegmentCompressionDictionary:
		return DecompressDictionary(seg, startRow, count)
	case SegmentCompressionRLE:
		return DecompressRLE(seg, startRow, count)
	case SegmentCompressionNone:
		return nil
	}

	return nil
}

// valuesEqual compares two values for equality.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// normalizeForMap normalizes a value for use as a map key.
func normalizeForMap(val any) any {
	switch val.(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64,
		float32, float64, bool, string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
