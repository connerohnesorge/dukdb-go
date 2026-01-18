package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectCompression_EmptyData(t *testing.T) {
	result := SelectCompression(nil, TypeInteger)
	assert.Equal(t, CompressionUncompressed, result, "empty data should return UNCOMPRESSED")

	result = SelectCompression([]any{}, TypeInteger)
	assert.Equal(t, CompressionUncompressed, result, "empty slice should return UNCOMPRESSED")
}

func TestSelectCompression_ConstantData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name:   "all same integers",
			data:   []any{int64(42), int64(42), int64(42), int64(42), int64(42)},
			typeID: TypeBigInt,
		},
		{
			name:   "all same strings",
			data:   []any{"hello", "hello", "hello", "hello"},
			typeID: TypeVarchar,
		},
		{
			name:   "all same with many values",
			data:   makeConstantData(1000, int64(100)),
			typeID: TypeBigInt,
		},
		{
			name:   "single value",
			data:   []any{int64(1)},
			typeID: TypeBigInt,
		},
		// Note: all nulls case is handled in a separate test since it has special behavior
		// (unique count is 0 for all nulls, so it selects RLE not CONSTANT)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(t, CompressionConstant, result, "constant data should return CONSTANT")
		})
	}
}

func TestSelectCompression_RLEData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name: "repeated runs",
			data: []any{
				int64(1), int64(1), int64(1), int64(1), int64(1), // run of 5
				int64(2), int64(2), int64(2), int64(2), int64(2), // run of 5
				int64(3), int64(3), int64(3), int64(3), int64(3), // run of 5
			}, // 3 runs / 15 values = 0.2 < 0.3
			typeID: TypeBigInt,
		},
		{
			name: "long runs with few unique values",
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 50; i++ {
					d[i] = int64(1)
				}
				for i := 50; i < 100; i++ {
					d[i] = int64(2)
				}
				return d
			}(), // 2 runs / 100 values = 0.02 < 0.3
			typeID: TypeBigInt,
		},
		{
			name: "string runs",
			data: []any{
				"aaa", "aaa", "aaa", "aaa", "aaa",
				"bbb", "bbb", "bbb", "bbb", "bbb",
			}, // 2 runs / 10 values = 0.2 < 0.3
			typeID: TypeVarchar,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(t, CompressionRLE, result, "data with few runs should return RLE")
		})
	}
}

func TestSelectCompression_DictionaryData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name: "few unique values alternating",
			// 4 unique values, 16 total = 0.25 < 0.5
			// But many runs, so RLE ratio = 16/16 = 1.0 > 0.3
			data: []any{
				int64(1), int64(2), int64(3), int64(4),
				int64(1), int64(2), int64(3), int64(4),
				int64(1), int64(2), int64(3), int64(4),
				int64(1), int64(2), int64(3), int64(4),
			},
			typeID: TypeBigInt,
		},
		{
			name: "low cardinality strings",
			// 3 unique values, 12 total = 0.25 < 0.5
			// Runs = 12, so RLE ratio = 12/12 = 1.0 > 0.3
			data: []any{
				"red", "green", "blue",
				"red", "green", "blue",
				"red", "green", "blue",
				"red", "green", "blue",
			},
			typeID: TypeVarchar,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(
				t,
				CompressionDictionary,
				result,
				"data with few unique values should return DICTIONARY",
			)
		})
	}
}

func TestSelectCompression_PFORDeltaData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name: "sorted ascending integers",
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					d[i] = int64(i * 10) // 0, 10, 20, ..., 990
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
		{
			name: "sorted descending integers",
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					d[i] = int64(1000 - i*10) // 1000, 990, 980, ..., 10
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
		{
			name: "small range sorted",
			// Values 0-255 fit in 8 bits, 64-bit type = 8/64 = 0.125 < 0.7
			// Also monotonic
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					d[i] = int64(i)
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(
				t,
				CompressionPFORDelta,
				result,
				"sorted integers should return PFOR_DELTA",
			)
		})
	}
}

func TestSelectCompression_BitPackingData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name: "random small integers",
			// Values 0-255 fit in 8 bits, int64 is 64 bits
			// 8/64 = 0.125 < 0.7
			// Not monotonic (random order), not few unique values
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					// Pseudo-random but deterministic pattern
					d[i] = int64((i*37 + 13) % 256)
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
		{
			name: "random medium range integers",
			// Values 0-4095 fit in 12 bits, int64 is 64 bits
			// 12/64 = 0.1875 < 0.7
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					d[i] = int64((i*137 + 29) % 4096)
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(
				t,
				CompressionBitPacking,
				result,
				"random small integers should return BITPACKING",
			)
		})
	}
}

func TestSelectCompression_UncompressedData(t *testing.T) {
	tests := []struct {
		name   string
		data   []any
		typeID LogicalTypeID
	}{
		{
			name: "random large integers",
			// Full range values that need all 64 bits
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					// Large values that need many bits
					d[i] = int64(1<<62 + int64(i)*1000000000)
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
		{
			name: "high cardinality strings",
			// Many unique values relative to total
			data: func() []any {
				d := make([]any, 10)
				for i := 0; i < 10; i++ {
					d[i] = string([]byte{byte('a' + i)})
				}
				return d
			}(), // 10 unique / 10 total = 1.0 > 0.5
			typeID: TypeVarchar,
		},
		{
			name: "alternating many unique large values",
			// Need many unique values AND large bit width to get UNCOMPRESSED
			// 100 unique values / 100 total = 1.0 > 0.5 (no dictionary)
			// Not sorted (no PFOR_DELTA)
			// Full 64-bit range (no bitpacking)
			data: func() []any {
				d := make([]any, 100)
				for i := 0; i < 100; i++ {
					// Each value is unique and uses many bits
					d[i] = int64(1<<62) + int64(i)*int64(1<<40)
				}
				return d
			}(),
			typeID: TypeBigInt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCompression(tt.data, tt.typeID)
			assert.Equal(
				t,
				CompressionUncompressed,
				result,
				"random high-entropy data should return UNCOMPRESSED",
			)
		})
	}
}

func TestAnalyzeData(t *testing.T) {
	t.Run("basic statistics", func(t *testing.T) {
		data := []any{int64(1), int64(2), int64(2), int64(3), int64(3), int64(3)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.Equal(t, 6, stats.TotalCount)
		assert.Equal(t, 3, stats.UniqueCount) // 1, 2, 3
		assert.Equal(t, 0, stats.NullCount)
		assert.Equal(t, 3, stats.RunCount) // [1], [2,2], [3,3,3]
	})

	t.Run("with nulls", func(t *testing.T) {
		data := []any{int64(1), nil, int64(2), nil, int64(3)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.Equal(t, 5, stats.TotalCount)
		assert.Equal(t, 3, stats.UniqueCount) // 1, 2, 3
		assert.Equal(t, 2, stats.NullCount)
	})

	t.Run("monotonic increasing", func(t *testing.T) {
		data := []any{int64(1), int64(2), int64(3), int64(4), int64(5)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.True(t, stats.IsMonotonic)
		assert.True(t, stats.ConstantDelta)
		assert.InDelta(t, 1.0, stats.AvgDelta, 0.001)
	})

	t.Run("monotonic decreasing", func(t *testing.T) {
		data := []any{int64(5), int64(4), int64(3), int64(2), int64(1)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.True(t, stats.IsMonotonic)
		assert.True(t, stats.ConstantDelta)
		assert.InDelta(t, -1.0, stats.AvgDelta, 0.001)
	})

	t.Run("non-monotonic", func(t *testing.T) {
		data := []any{int64(1), int64(3), int64(2), int64(4), int64(3)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.False(t, stats.IsMonotonic)
	})

	t.Run("bit width calculation", func(t *testing.T) {
		// Values 0-15 need 4 bits
		data := []any{int64(0), int64(7), int64(15)}
		stats := AnalyzeData(data, TypeBigInt)

		assert.Equal(t, uint8(4), stats.BitWidth)
	})
}

func TestIsConstant(t *testing.T) {
	tests := []struct {
		name     string
		data     []any
		expected bool
	}{
		{"empty", []any{}, true},
		{"single", []any{1}, true},
		{"all same", []any{1, 1, 1, 1}, true},
		{"different", []any{1, 2, 1}, false},
		{"all nil", []any{nil, nil}, true},
		{"mixed nil", []any{1, nil}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConstant(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCountRuns(t *testing.T) {
	tests := []struct {
		name     string
		data     []any
		expected int
	}{
		{"empty", []any{}, 0},
		{"single", []any{1}, 1},
		{"all same", []any{1, 1, 1, 1}, 1},
		{"alternating", []any{1, 2, 1, 2}, 4},
		{"three runs", []any{1, 1, 2, 2, 3, 3}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countRuns(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCountUnique(t *testing.T) {
	tests := []struct {
		name     string
		data     []any
		expected int
	}{
		{"empty", []any{}, 0},
		{"single", []any{1}, 1},
		{"duplicates", []any{1, 1, 2, 2}, 2},
		{"with nil", []any{1, nil, 2, nil}, 3}, // 1, 2, nil
		{"all same", []any{1, 1, 1}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countUnique(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRequiredBitWidth(t *testing.T) {
	tests := []struct {
		name     string
		data     []any
		expected uint8
	}{
		{"empty", []any{}, 0},
		{"all nil", []any{nil, nil}, 0},
		{"zero only", []any{int64(0)}, 0},
		{"one bit", []any{int64(0), int64(1)}, 1},
		{"four bits", []any{int64(0), int64(15)}, 4},
		{"eight bits", []any{int64(0), int64(255)}, 8},
		{"negative", []any{int64(-1), int64(0)}, 2}, // sign bit + magnitude
		// For -128 to 127, we need bits to represent both extremes
		// -128 has magnitude 128, which needs 8 bits, plus sign bit = 9 bits
		{"negative range", []any{int64(-128), int64(127)}, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := requiredBitWidth(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAnalyzeDeltas(t *testing.T) {
	tests := []struct {
		name          string
		data          []any
		isMonotonic   bool
		constantDelta bool
		avgDelta      float64
	}{
		{
			name:          "single value",
			data:          []any{int64(1)},
			isMonotonic:   true,
			constantDelta: true,
			avgDelta:      0,
		},
		{
			name:          "constant delta ascending",
			data:          []any{int64(1), int64(3), int64(5), int64(7)},
			isMonotonic:   true,
			constantDelta: true,
			avgDelta:      2,
		},
		{
			name:          "constant delta descending",
			data:          []any{int64(10), int64(8), int64(6), int64(4)},
			isMonotonic:   true,
			constantDelta: true,
			avgDelta:      -2,
		},
		{
			name:          "non-monotonic",
			data:          []any{int64(1), int64(3), int64(2), int64(4)},
			isMonotonic:   false,
			constantDelta: false,
			avgDelta:      1, // (2 + -1 + 2) / 3 = 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMonotonic, constantDelta, avgDelta := analyzeDeltas(tt.data)
			assert.Equal(t, tt.isMonotonic, isMonotonic)
			assert.Equal(t, tt.constantDelta, constantDelta)
			assert.InDelta(t, tt.avgDelta, avgDelta, 0.001)
		})
	}
}

func TestSelectCompressionForStrings(t *testing.T) {
	t.Run("constant strings", func(t *testing.T) {
		data := []string{"hello", "hello", "hello", "hello"}
		result := SelectCompressionForStrings(data)
		assert.Equal(t, CompressionConstant, result)
	})

	t.Run("RLE strings", func(t *testing.T) {
		data := make([]string, 100)
		for i := 0; i < 50; i++ {
			data[i] = "aaaa"
		}
		for i := 50; i < 100; i++ {
			data[i] = "bbbb"
		}
		result := SelectCompressionForStrings(data)
		assert.Equal(t, CompressionRLE, result)
	})

	t.Run("dictionary strings", func(t *testing.T) {
		// Few unique values but alternating
		data := make([]string, 100)
		values := []string{"red", "green", "blue"}
		for i := 0; i < 100; i++ {
			data[i] = values[i%3]
		}
		result := SelectCompressionForStrings(data)
		assert.Equal(t, CompressionDictionary, result)
	})
}

func TestSelectCompressionForIntegers(t *testing.T) {
	t.Run("constant integers", func(t *testing.T) {
		data := []int64{42, 42, 42, 42, 42}
		result := SelectCompressionForIntegers(data)
		assert.Equal(t, CompressionConstant, result)
	})

	t.Run("RLE integers", func(t *testing.T) {
		data := make([]int64, 100)
		for i := 0; i < 50; i++ {
			data[i] = 100
		}
		for i := 50; i < 100; i++ {
			data[i] = 200
		}
		result := SelectCompressionForIntegers(data)
		assert.Equal(t, CompressionRLE, result)
	})

	t.Run("sorted integers", func(t *testing.T) {
		data := make([]int64, 100)
		for i := 0; i < 100; i++ {
			data[i] = int64(i * 10)
		}
		result := SelectCompressionForIntegers(data)
		assert.Equal(t, CompressionPFORDelta, result)
	})

	t.Run("small random integers", func(t *testing.T) {
		data := make([]int64, 100)
		for i := 0; i < 100; i++ {
			data[i] = int64((i*37 + 13) % 256)
		}
		result := SelectCompressionForIntegers(data)
		assert.Equal(t, CompressionBitPacking, result)
	})
}

func TestEstimateCompressionSizes(t *testing.T) {
	t.Run("constant data", func(t *testing.T) {
		data := makeConstantData(100, int64(42))
		estimates := EstimateCompressionSizes(data, TypeBigInt, 8)

		require.NotEmpty(t, estimates)
		// CONSTANT should be first (smallest)
		assert.Equal(t, CompressionConstant, estimates[0].Algorithm)
		assert.Equal(t, 8, estimates[0].EstimatedSize) // Single value
	})

	t.Run("rle data", func(t *testing.T) {
		data := make([]any, 100)
		for i := 0; i < 50; i++ {
			data[i] = int64(1)
		}
		for i := 50; i < 100; i++ {
			data[i] = int64(2)
		}
		estimates := EstimateCompressionSizes(data, TypeBigInt, 8)

		require.NotEmpty(t, estimates)
		// RLE should have a good estimate
		var hasRLE bool
		for _, e := range estimates {
			if e.Algorithm == CompressionRLE {
				hasRLE = true
				assert.Less(t, e.EstimatedSize, 800) // Much less than 100*8=800
			}
		}
		assert.True(t, hasRLE)
	})

	t.Run("bitpacking data", func(t *testing.T) {
		data := make([]any, 100)
		for i := 0; i < 100; i++ {
			data[i] = int64(i % 16) // 4 bits needed
		}
		estimates := EstimateCompressionSizes(data, TypeBigInt, 8)

		require.NotEmpty(t, estimates)
		// Should have bitpacking estimate
		var hasBitpacking bool
		for _, e := range estimates {
			if e.Algorithm == CompressionBitPacking {
				hasBitpacking = true
				// 9 bytes header + 100 * 4 bits / 8 = 9 + 50 = 59 bytes
				assert.Less(t, e.EstimatedSize, 800)
			}
		}
		assert.True(t, hasBitpacking)
	})
}

func TestSelectBestCompression(t *testing.T) {
	t.Run("constant data", func(t *testing.T) {
		data := makeConstantData(100, int64(42))
		result := SelectBestCompression(data, TypeBigInt, 8)
		assert.Equal(t, CompressionConstant, result)
	})

	t.Run("empty data", func(t *testing.T) {
		result := SelectBestCompression(nil, TypeBigInt, 8)
		assert.Equal(t, CompressionUncompressed, result)
	})
}

func TestIsIntegerType(t *testing.T) {
	intTypes := []LogicalTypeID{
		TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt,
		TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt,
		TypeHugeInt, TypeUHugeInt,
	}
	for _, typeID := range intTypes {
		assert.True(t, isIntegerType(typeID), "%s should be integer type", typeID)
	}

	nonIntTypes := []LogicalTypeID{
		TypeFloat, TypeDouble, TypeVarchar, TypeBoolean, TypeDate,
	}
	for _, typeID := range nonIntTypes {
		assert.False(t, isIntegerType(typeID), "%s should not be integer type", typeID)
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []LogicalTypeID{
		TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt,
		TypeFloat, TypeDouble, TypeDecimal,
	}
	for _, typeID := range numericTypes {
		assert.True(t, isNumericType(typeID), "%s should be numeric type", typeID)
	}

	nonNumericTypes := []LogicalTypeID{
		TypeVarchar, TypeBoolean, TypeDate, TypeTimestamp,
	}
	for _, typeID := range nonNumericTypes {
		assert.False(t, isNumericType(typeID), "%s should not be numeric type", typeID)
	}
}

func TestGetTypeWidth(t *testing.T) {
	tests := []struct {
		typeID   LogicalTypeID
		expected uint8
	}{
		{TypeTinyInt, 1},
		{TypeUTinyInt, 1},
		{TypeSmallInt, 2},
		{TypeUSmallInt, 2},
		{TypeInteger, 4},
		{TypeUInteger, 4},
		{TypeBigInt, 8},
		{TypeUBigInt, 8},
		{TypeHugeInt, 16},
		{TypeUHugeInt, 16},
		{TypeVarchar, 0},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			result := getTypeWidth(tt.typeID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"one nil", nil, 1, false},
		{"same int", 42, 42, true},
		{"different int", 42, 43, false},
		{"same string", "hello", "hello", true},
		{"same bytes", []byte{1, 2, 3}, []byte{1, 2, 3}, true},
		{"different bytes", []byte{1, 2, 3}, []byte{1, 2, 4}, false},
		{"different length bytes", []byte{1, 2}, []byte{1, 2, 3}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valuesEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBitsNeeded(t *testing.T) {
	tests := []struct {
		value    uint64
		expected uint8
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{7, 3},
		{8, 4},
		{15, 4},
		{16, 5},
		{255, 8},
		{256, 9},
		{65535, 16},
		{1 << 31, 32},
		{1 << 63, 64},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := bitsNeeded(tt.value)
			assert.Equal(t, tt.expected, result, "bitsNeeded(%d)", tt.value)
		})
	}
}

func TestMinMaxValue(t *testing.T) {
	t.Run("minValue int64", func(t *testing.T) {
		result := minValue(int64(10), int64(5), TypeBigInt)
		assert.Equal(t, int64(5), result)

		result = minValue(int64(5), int64(10), TypeBigInt)
		assert.Equal(t, int64(5), result)
	})

	t.Run("maxValue int64", func(t *testing.T) {
		result := maxValue(int64(10), int64(5), TypeBigInt)
		assert.Equal(t, int64(10), result)

		result = maxValue(int64(5), int64(10), TypeBigInt)
		assert.Equal(t, int64(10), result)
	})

	t.Run("with nil", func(t *testing.T) {
		result := minValue(nil, int64(5), TypeBigInt)
		assert.Equal(t, int64(5), result)

		result = maxValue(int64(10), nil, TypeBigInt)
		assert.Equal(t, int64(10), result)
	})
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		input    any
		expected int64
	}{
		{int8(42), 42},
		{int16(42), 42},
		{int32(42), 42},
		{int64(42), 42},
		{int(42), 42},
		{uint8(42), 42},
		{uint16(42), 42},
		{uint32(42), 42},
		{uint64(42), 42},
		{uint(42), 42},
		{"not a number", 0},
	}

	for _, tt := range tests {
		result := toInt64(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    any
		expected float64
	}{
		{float32(3.14), float64(float32(3.14))},
		{float64(3.14), 3.14},
		{int64(42), 42.0},
		{uint64(42), 42.0},
		{"not a number", 0},
	}

	for _, tt := range tests {
		result := toFloat64(tt.input)
		assert.InDelta(t, tt.expected, result, 0.0001)
	}
}

// Helper function to create constant data
func makeConstantData(count int, value any) []any {
	data := make([]any, count)
	for i := 0; i < count; i++ {
		data[i] = value
	}
	return data
}

// Benchmark tests
func BenchmarkSelectCompression_Small(b *testing.B) {
	data := make([]any, 100)
	for i := 0; i < 100; i++ {
		data[i] = int64(i * 10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectCompression(data, TypeBigInt)
	}
}

func BenchmarkSelectCompression_Large(b *testing.B) {
	data := make([]any, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = int64(i * 10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectCompression(data, TypeBigInt)
	}
}

func BenchmarkAnalyzeData(b *testing.B) {
	data := make([]any, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = int64((i * 37) % 1000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnalyzeData(data, TypeBigInt)
	}
}
