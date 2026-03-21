package storage

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func makeIntVector(values []any) *Vector {
	vec := NewVector(dukdb.TYPE_BIGINT, len(values))
	for i, v := range values {
		vec.SetValue(i, v)
	}
	vec.SetCount(len(values))
	return vec
}

func makeStringVector(values []any) *Vector {
	vec := NewVector(dukdb.TYPE_VARCHAR, len(values))
	for i, v := range values {
		vec.SetValue(i, v)
	}
	vec.SetCount(len(values))
	return vec
}

func makeBoolVector(values []any) *Vector {
	vec := NewVector(dukdb.TYPE_BOOLEAN, len(values))
	for i, v := range values {
		vec.SetValue(i, v)
	}
	vec.SetCount(len(values))
	return vec
}

func makeFloat64Vector(values []any) *Vector {
	vec := NewVector(dukdb.TYPE_DOUBLE, len(values))
	for i, v := range values {
		vec.SetValue(i, v)
	}
	vec.SetCount(len(values))
	return vec
}

func assertVectorEquals(t *testing.T, expected []any, vec *Vector) {
	t.Helper()
	require.Equal(t, len(expected), vec.Count(), "vector count mismatch")
	for i, exp := range expected {
		got := vec.GetValue(i)
		if exp == nil {
			assert.Nil(t, got, "row %d: expected nil", i)
		} else {
			assert.Equal(t, exp, got, "row %d: value mismatch", i)
		}
	}
}

// --- Type tests (Task 1.4) ---

func TestSegmentCompressionType_String(t *testing.T) {
	tests := []struct {
		typ  SegmentCompressionType
		want string
	}{
		{SegmentCompressionNone, "None"},
		{SegmentCompressionConstant, "Constant"},
		{SegmentCompressionDictionary, "Dictionary"},
		{SegmentCompressionRLE, "RLE"},
		{SegmentCompressionType(99), "Unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.typ.String())
	}
}

// --- Constant compression tests (Task 2.4) ---

func TestCompressConstant_AllSameInt64(t *testing.T) {
	vec := makeIntVector([]any{int64(42), int64(42), int64(42), int64(42), int64(42)})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionConstant, seg.Type)
	assert.Equal(t, 5, seg.Count)
	assert.Equal(t, 0, seg.NullCount)

	payload := seg.Data.(*ConstantPayload)
	assert.Equal(t, int64(42), payload.Value)

	// Decompress and verify round-trip
	result := DecompressConstant(seg, 0, 5)
	assertVectorEquals(t, []any{int64(42), int64(42), int64(42), int64(42), int64(42)}, result)
}

func TestCompressConstant_AllSameString(t *testing.T) {
	vec := makeStringVector([]any{"hello", "hello", "hello"})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionConstant, seg.Type)

	result := DecompressConstant(seg, 0, 3)
	assertVectorEquals(t, []any{"hello", "hello", "hello"}, result)
}

func TestCompressConstant_AllSameBool(t *testing.T) {
	vec := makeBoolVector([]any{true, true, true})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)

	result := DecompressConstant(seg, 0, 3)
	assertVectorEquals(t, []any{true, true, true}, result)
}

func TestCompressConstant_AllSameFloat64(t *testing.T) {
	vec := makeFloat64Vector([]any{3.14, 3.14, 3.14})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)

	result := DecompressConstant(seg, 0, 3)
	assertVectorEquals(t, []any{3.14, 3.14, 3.14}, result)
}

func TestCompressConstant_MixedValues(t *testing.T) {
	vec := makeIntVector([]any{int64(1), int64(2), int64(3)})
	seg := CompressConstant(vec)
	assert.Nil(t, seg, "mixed values should not compress as constant")
}

func TestCompressConstant_AllNull(t *testing.T) {
	vec := makeIntVector([]any{nil, nil, nil})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionConstant, seg.Type)
	assert.Equal(t, 3, seg.Count)
	assert.Equal(t, 3, seg.NullCount)

	payload := seg.Data.(*ConstantPayload)
	assert.Nil(t, payload.Value)

	result := DecompressConstant(seg, 0, 3)
	assertVectorEquals(t, []any{nil, nil, nil}, result)
}

func TestCompressConstant_SingleRow(t *testing.T) {
	vec := makeIntVector([]any{int64(7)})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, 1, seg.Count)

	result := DecompressConstant(seg, 0, 1)
	assertVectorEquals(t, []any{int64(7)}, result)
}

func TestCompressConstant_Empty(t *testing.T) {
	vec := makeIntVector([]any{})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, 0, seg.Count)
}

func TestCompressConstant_WithNulls(t *testing.T) {
	vec := makeIntVector([]any{int64(5), nil, int64(5), nil})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)
	assert.Equal(t, 4, seg.Count)
	assert.Equal(t, 2, seg.NullCount)

	result := DecompressConstant(seg, 0, 4)
	assertVectorEquals(t, []any{int64(5), nil, int64(5), nil}, result)
}

func TestDecompressConstant_PartialRange(t *testing.T) {
	vec := makeIntVector([]any{int64(9), int64(9), int64(9), int64(9), int64(9)})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)

	result := DecompressConstant(seg, 2, 2)
	assertVectorEquals(t, []any{int64(9), int64(9)}, result)
}

func TestDecompressConstant_OutOfBounds(t *testing.T) {
	vec := makeIntVector([]any{int64(1), int64(1)})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)

	// startRow beyond count
	result := DecompressConstant(seg, 10, 5)
	assert.Equal(t, 0, result.Count())

	// count of 0
	result = DecompressConstant(seg, 0, 0)
	assert.Equal(t, 0, result.Count())

	// negative count
	result = DecompressConstant(seg, 0, -1)
	assert.Equal(t, 0, result.Count())
}

// --- Dictionary compression tests (Task 3.4) ---

func TestCompressDictionary_LowCardinality(t *testing.T) {
	// 100 rows with 5 distinct values
	vals := make([]any, 100)
	for i := range vals {
		vals[i] = int64(i % 5)
	}
	vec := makeIntVector(vals)

	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionDictionary, seg.Type)
	assert.Equal(t, 100, seg.Count)
	assert.Equal(t, 0, seg.NullCount)

	payload := seg.Data.(*DictionaryPayload)
	assert.Equal(t, 5, len(payload.Dictionary))

	// Decompress and verify round-trip
	result := DecompressDictionary(seg, 0, 100)
	assertVectorEquals(t, vals, result)
}

func TestCompressDictionary_HighCardinality(t *testing.T) {
	// More distinct values than threshold
	threshold := 5
	vals := make([]any, 10)
	for i := range vals {
		vals[i] = int64(i) // 10 distinct values
	}
	vec := makeIntVector(vals)

	seg := CompressDictionary(vec, threshold)
	assert.Nil(t, seg, "should fail when distinct count exceeds threshold")
}

func TestCompressDictionary_WithNulls(t *testing.T) {
	vals := []any{int64(1), nil, int64(2), nil, int64(1)}
	vec := makeIntVector(vals)

	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	require.NotNil(t, seg)
	assert.Equal(t, 5, seg.Count)
	assert.Equal(t, 2, seg.NullCount)

	payload := seg.Data.(*DictionaryPayload)
	// Only non-NULL values in dictionary
	assert.Equal(t, 2, len(payload.Dictionary))
	// NULL indices should be 0xFFFF
	assert.Equal(t, uint16(0xFFFF), payload.Indices[1])
	assert.Equal(t, uint16(0xFFFF), payload.Indices[3])

	// Round-trip
	result := DecompressDictionary(seg, 0, 5)
	assertVectorEquals(t, vals, result)
}

func TestCompressDictionary_Strings(t *testing.T) {
	vals := []any{"apple", "banana", "apple", "cherry", "banana", "apple"}
	vec := makeStringVector(vals)

	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	require.NotNil(t, seg)

	payload := seg.Data.(*DictionaryPayload)
	assert.Equal(t, 3, len(payload.Dictionary))

	result := DecompressDictionary(seg, 0, 6)
	assertVectorEquals(t, vals, result)
}

func TestCompressDictionary_Empty(t *testing.T) {
	vec := makeIntVector([]any{})
	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	assert.Nil(t, seg)
}

func TestCompressDictionary_PartialDecompress(t *testing.T) {
	vals := []any{"a", "b", "c", "a", "b"}
	vec := makeStringVector(vals)
	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	require.NotNil(t, seg)

	result := DecompressDictionary(seg, 1, 3)
	assertVectorEquals(t, []any{"b", "c", "a"}, result)
}

// --- RLE compression tests (Task 4.4) ---

func TestCompressRLE_RepeatedRuns(t *testing.T) {
	vals := []any{int64(1), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(3), int64(3)}
	vec := makeIntVector(vals)

	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionRLE, seg.Type)
	assert.Equal(t, 9, seg.Count)

	payload := seg.Data.(*RLEPayload)
	assert.Equal(t, 3, len(payload.Values))
	assert.Equal(t, []uint32{3, 2, 4}, payload.RunLengths)

	// Round-trip
	result := DecompressRLE(seg, 0, 9)
	assertVectorEquals(t, vals, result)
}

func TestCompressRLE_NoRuns(t *testing.T) {
	vals := []any{int64(1), int64(2), int64(3), int64(4), int64(5)}
	vec := makeIntVector(vals)

	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	assert.Nil(t, seg, "no runs should fail RLE threshold check")
}

func TestCompressRLE_WithNulls(t *testing.T) {
	// NULLs form their own runs
	vals := []any{int64(1), int64(1), nil, nil, int64(1), int64(1)}
	vec := makeIntVector(vals)

	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	require.NotNil(t, seg)
	assert.Equal(t, 6, seg.Count)
	assert.Equal(t, 2, seg.NullCount)

	payload := seg.Data.(*RLEPayload)
	assert.Equal(t, 3, len(payload.Values))
	assert.Equal(t, []uint32{2, 2, 2}, payload.RunLengths)

	// Round-trip
	result := DecompressRLE(seg, 0, 6)
	assertVectorEquals(t, vals, result)
}

func TestCompressRLE_PartialDecompress(t *testing.T) {
	vals := []any{int64(1), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(3), int64(3)}
	vec := makeIntVector(vals)

	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	require.NotNil(t, seg)

	// Decompress rows 2..5 (indices 2,3,4,5)
	result := DecompressRLE(seg, 2, 4)
	assertVectorEquals(t, []any{int64(1), int64(2), int64(2), int64(3)}, result)
}

func TestCompressRLE_PartialDecompressFromMiddleOfRun(t *testing.T) {
	vals := []any{int64(5), int64(5), int64(5), int64(5), int64(5), int64(7), int64(7)}
	vec := makeIntVector(vals)

	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	require.NotNil(t, seg)

	// Start from row 3
	result := DecompressRLE(seg, 3, 3)
	assertVectorEquals(t, []any{int64(5), int64(5), int64(7)}, result)
}

func TestCompressRLE_Empty(t *testing.T) {
	vec := makeIntVector([]any{})
	seg := CompressRLE(vec, DefaultRLEMinRunLength)
	assert.Nil(t, seg)
}

func TestDecompressRLE_OutOfBounds(t *testing.T) {
	vals := []any{int64(1), int64(1), int64(1)}
	vec := makeIntVector(vals)
	seg := CompressRLE(vec, 1.0)
	require.NotNil(t, seg)

	result := DecompressRLE(seg, 10, 5)
	assert.Equal(t, 0, result.Count())

	result = DecompressRLE(seg, 0, 0)
	assert.Equal(t, 0, result.Count())
}

// --- AnalyzeAndCompress tests (Task 5.4) ---

func TestAnalyzeAndCompress_SelectsConstant(t *testing.T) {
	vec := makeIntVector([]any{int64(42), int64(42), int64(42), int64(42)})
	seg := AnalyzeAndCompress(vec)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionConstant, seg.Type)
}

func TestAnalyzeAndCompress_SelectsDictionary(t *testing.T) {
	// Low cardinality but not constant
	vals := make([]any, 100)
	for i := range vals {
		vals[i] = int64(i % 5)
	}
	vec := makeIntVector(vals)

	seg := AnalyzeAndCompress(vec)
	require.NotNil(t, seg)
	assert.Equal(t, SegmentCompressionDictionary, seg.Type)
}

func TestAnalyzeAndCompress_SelectsRLE(t *testing.T) {
	// Many distinct values in sorted order with runs, exceeding dictionary threshold
	vals := make([]any, 2000)
	for i := range vals {
		// 500 distinct values, each repeated 4 times => avg run = 4
		vals[i] = int64(i / 4)
	}
	vec := makeIntVector(vals)

	seg := AnalyzeAndCompress(vec)
	require.NotNil(t, seg)
	// Dictionary should succeed here since 500 < 256 is false, so dictionary fails
	// and RLE should be selected
	assert.Equal(t, SegmentCompressionRLE, seg.Type)
}

func TestAnalyzeAndCompress_ReturnsNil(t *testing.T) {
	// Random high cardinality with no runs
	vals := make([]any, 1000)
	for i := range vals {
		vals[i] = int64(i)
	}
	// Shuffle to prevent runs (alternate pattern)
	for i := 0; i < len(vals)-1; i += 2 {
		vals[i], vals[i+1] = vals[i+1], vals[i]
	}
	vec := makeIntVector(vals)

	seg := AnalyzeAndCompress(vec)
	assert.Nil(t, seg, "random high cardinality should not compress")
}

func TestAnalyzeAndCompress_Empty(t *testing.T) {
	vec := makeIntVector([]any{})
	seg := AnalyzeAndCompress(vec)
	assert.Nil(t, seg)
}

// --- DecompressSegment dispatcher tests ---

func TestDecompressSegment_Constant(t *testing.T) {
	vec := makeIntVector([]any{int64(10), int64(10), int64(10)})
	seg := CompressConstant(vec)
	require.NotNil(t, seg)

	result := DecompressSegment(seg, 0, 3)
	require.NotNil(t, result)
	assertVectorEquals(t, []any{int64(10), int64(10), int64(10)}, result)
}

func TestDecompressSegment_Dictionary(t *testing.T) {
	vals := []any{"x", "y", "x", "z"}
	vec := makeStringVector(vals)
	seg := CompressDictionary(vec, DefaultDictionaryThreshold)
	require.NotNil(t, seg)

	result := DecompressSegment(seg, 0, 4)
	require.NotNil(t, result)
	assertVectorEquals(t, vals, result)
}

func TestDecompressSegment_RLE(t *testing.T) {
	vals := []any{int64(1), int64(1), int64(1), int64(2), int64(2)}
	vec := makeIntVector(vals)
	seg := CompressRLE(vec, 1.0)
	require.NotNil(t, seg)

	result := DecompressSegment(seg, 0, 5)
	require.NotNil(t, result)
	assertVectorEquals(t, vals, result)
}

func TestDecompressSegment_None(t *testing.T) {
	seg := &CompressedSegment{
		Type: SegmentCompressionNone,
	}
	result := DecompressSegment(seg, 0, 10)
	assert.Nil(t, result)
}

// --- Helper function tests ---

func TestValuesEqual(t *testing.T) {
	assert.True(t, valuesEqual(nil, nil))
	assert.False(t, valuesEqual(nil, int64(1)))
	assert.False(t, valuesEqual(int64(1), nil))
	assert.True(t, valuesEqual(int64(42), int64(42)))
	assert.False(t, valuesEqual(int64(1), int64(2)))
	assert.True(t, valuesEqual("hello", "hello"))
	assert.False(t, valuesEqual("hello", "world"))
	assert.True(t, valuesEqual(3.14, 3.14))
	assert.True(t, valuesEqual(true, true))
	assert.False(t, valuesEqual(true, false))
}

func TestNormalizeForMap(t *testing.T) {
	// Primitive types returned as-is
	assert.Equal(t, int64(42), normalizeForMap(int64(42)))
	assert.Equal(t, "hello", normalizeForMap("hello"))
	assert.Equal(t, true, normalizeForMap(true))
	assert.Equal(t, float64(3.14), normalizeForMap(float64(3.14)))

	// Complex types use fmt.Sprintf
	slice := []int{1, 2, 3}
	result := normalizeForMap(slice)
	assert.IsType(t, "", result)
}
