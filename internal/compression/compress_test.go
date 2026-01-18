package compression

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		name string
		ct   CompressionType
		want string
	}{
		{"None", CompressionNone, "None"},
		{"RLE", CompressionRLE, "RLE"},
		{"BitPack", CompressionBitPack, "BitPack"},
		{"FSST", CompressionFSST, "FSST"},
		{"Chimp", CompressionChimp, "Chimp"},
		{"Zstd", CompressionZstd, "Zstd"},
		{"Unknown", CompressionType(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ct.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNoneCodec_Type(t *testing.T) {
	codec := NewNoneCodec()
	assert.Equal(t, CompressionNone, codec.Type())
}

func TestNoneCodec_Compress(t *testing.T) {
	codec := NewNoneCodec()

	tests := []struct {
		name  string
		input []byte
	}{
		{"Empty", nil},
		{"Small", []byte{1, 2, 3, 4, 5}},
		{"Large", make([]byte, 10000)},
		{"Binary", []byte{0x00, 0xFF, 0xAA, 0x55}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := codec.Compress(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.input, compressed, "NoneCodec should return identical data")

			// Verify independence - modifying compressed shouldn't affect input
			if len(compressed) == 0 {
				return
			}

			original := make([]byte, len(tt.input))
			copy(original, tt.input)
			compressed[0] ^= 0xFF
			assert.Equal(t, original, tt.input, "Input should be independent of compressed output")
		})
	}
}

func TestNoneCodec_Decompress(t *testing.T) {
	codec := NewNoneCodec()

	tests := []struct {
		name     string
		input    []byte
		destSize int
		wantErr  bool
	}{
		{"Empty", nil, 0, false},
		{"Small", []byte{1, 2, 3, 4, 5}, 5, false},
		{"Large", make([]byte, 10000), 10000, false},
		{"Binary", []byte{0x00, 0xFF, 0xAA, 0x55}, 4, false},
		{"SizeMismatch", []byte{1, 2, 3}, 5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decompressed, err := codec.Decompress(tt.input, tt.destSize)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid data size")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.input, decompressed, "NoneCodec should return identical data")

			// Verify independence - modifying decompressed shouldn't affect input
			if len(decompressed) == 0 {
				return
			}

			original := make([]byte, len(tt.input))
			copy(original, tt.input)
			decompressed[0] ^= 0xFF
			assert.Equal(
				t,
				original,
				tt.input,
				"Input should be independent of decompressed output",
			)
		})
	}
}

func TestNoneCodec_RoundTrip(t *testing.T) {
	codec := NewNoneCodec()

	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", nil},
		{"SingleByte", []byte{42}},
		{"Sequence", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"Repeated", []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{
			"Binary",
			[]byte{
				0x00,
				0x11,
				0x22,
				0x33,
				0x44,
				0x55,
				0x66,
				0x77,
				0x88,
				0x99,
				0xAA,
				0xBB,
				0xCC,
				0xDD,
				0xEE,
				0xFF,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Compress
			compressed, err := codec.Compress(tt.data)
			require.NoError(t, err)

			// Decompress
			decompressed, err := codec.Decompress(compressed, len(tt.data))
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, tt.data, decompressed, "Round-trip should preserve data")
		})
	}
}

func TestGetCodec(t *testing.T) {
	tests := []struct {
		name    string
		ct      CompressionType
		wantErr bool
		errMsg  string
	}{
		{"None", CompressionNone, false, ""},
		{"RLE", CompressionRLE, false, ""},
		{"BitPack", CompressionBitPack, false, ""},
		{"FSST", CompressionFSST, false, ""},
		{"Chimp", CompressionChimp, false, ""},
		{"Zstd", CompressionZstd, false, ""},
		{"Unknown", CompressionType(99), true, "unsupported compression type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec, err := GetCodec(tt.ct)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, codec)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, codec)
			assert.Equal(t, tt.ct, codec.Type())
		})
	}
}

func TestNoneCodec_Interface(_ *testing.T) {
	var _ Codec = (*NoneCodec)(nil)
	var _ Compressor = (*NoneCodec)(nil)
	var _ Decompressor = (*NoneCodec)(nil)
}

func BenchmarkNoneCodec_Compress(b *testing.B) {
	codec := NewNoneCodec()
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.Run(formatSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				_, err := codec.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNoneCodec_Decompress(b *testing.B) {
	codec := NewNoneCodec()
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.Run(formatSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				_, err := codec.Decompress(data, size)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNoneCodec_RoundTrip(b *testing.B) {
	codec := NewNoneCodec()
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.Run(formatSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				compressed, err := codec.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
				_, err = codec.Decompress(compressed, size)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func formatSize(size int) string {
	if size >= 1000000 {
		return string(rune('0'+(size/1000000))) + "MB"
	}
	if size >= 1000 {
		return string(rune('0'+(size/1000))) + "KB"
	}

	return string(rune('0'+size)) + "B"
}

func TestSelectCompression_IntegerTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Boolean", LogicalTypeBoolean, CompressionBitPack},
		{"TinyInt", LogicalTypeTinyInt, CompressionBitPack},
		{"SmallInt", LogicalTypeSmallInt, CompressionBitPack},
		{"Integer", LogicalTypeInteger, CompressionBitPack},
		{"BigInt", LogicalTypeBigInt, CompressionBitPack},
		{"UTinyInt", LogicalTypeUTinyInt, CompressionBitPack},
		{"USmallInt", LogicalTypeUSmallInt, CompressionBitPack},
		{"UInteger", LogicalTypeUInteger, CompressionBitPack},
		{"UBigInt", LogicalTypeUBigInt, CompressionBitPack},
		{"HugeInt", LogicalTypeHugeInt, CompressionBitPack},
		{"UHugeInt", LogicalTypeUHugeInt, CompressionBitPack},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Integer type %s should use BitPacking", tt.name)
		})
	}
}

func TestSelectCompression_FloatingPointTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Float", LogicalTypeFloat, CompressionChimp},
		{"Double", LogicalTypeDouble, CompressionChimp},
		{"Decimal", LogicalTypeDecimal, CompressionChimp},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Floating-point type %s should use Chimp", tt.name)
		})
	}
}

func TestSelectCompression_StringTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Varchar", LogicalTypeVarchar, CompressionFSST},
		{"Char", LogicalTypeChar, CompressionFSST},
		{"Blob", LogicalTypeBlob, CompressionFSST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "String type %s should use FSST", tt.name)
		})
	}
}

func TestSelectCompression_DateTimeTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Date", LogicalTypeDate, CompressionRLE},
		{"Time", LogicalTypeTime, CompressionRLE},
		{"TimeNS", LogicalTypeTimeNS, CompressionRLE},
		{"TimeTZ", LogicalTypeTimeTZ, CompressionRLE},
		{"Timestamp", LogicalTypeTimestamp, CompressionRLE},
		{"TimestampSec", LogicalTypeTimestampSec, CompressionRLE},
		{"TimestampMS", LogicalTypeTimestampMS, CompressionRLE},
		{"TimestampNS", LogicalTypeTimestampNS, CompressionRLE},
		{"TimestampTZ", LogicalTypeTimestampTZ, CompressionRLE},
		{"Interval", LogicalTypeInterval, CompressionRLE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Date/Time type %s should use RLE", tt.name)
		})
	}
}

func TestSelectCompression_OtherTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"UUID", LogicalTypeUUID, CompressionRLE},
		{"Bit", LogicalTypeBit, CompressionRLE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Type %s should use RLE", tt.name)
		})
	}
}

func TestSelectCompression_ComplexTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Struct", LogicalTypeStruct, CompressionNone},
		{"List", LogicalTypeList, CompressionNone},
		{"Map", LogicalTypeMap, CompressionNone},
		{"Union", LogicalTypeUnion, CompressionNone},
		{"Array", LogicalTypeArray, CompressionNone},
		{"Enum", LogicalTypeEnum, CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Complex type %s should use no compression", tt.name)
		})
	}
}

func TestSelectCompression_SpecialTypes(t *testing.T) {
	tests := []struct {
		name   string
		typeID LogicalTypeID
		want   CompressionType
	}{
		{"Invalid", LogicalTypeInvalid, CompressionNone},
		{"SQLNull", LogicalTypeSQLNull, CompressionNone},
		{"Unknown", LogicalTypeUnknown, CompressionNone},
		{"Any", LogicalTypeAny, CompressionNone},
		{"Undefined", LogicalTypeID(255), CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectCompression(tt.typeID)
			assert.Equal(t, tt.want, got, "Special type %s should use no compression", tt.name)
		})
	}
}

func TestSelectCompressionWithData_SmallData(t *testing.T) {
	// Small data blocks (<64 bytes) should skip compression overhead
	tests := []struct {
		name     string
		typeID   LogicalTypeID
		dataSize int
		want     CompressionType
	}{
		{"Integer-1byte", LogicalTypeInteger, 1, CompressionNone},
		{"Integer-32bytes", LogicalTypeInteger, 32, CompressionNone},
		{"Integer-63bytes", LogicalTypeInteger, 63, CompressionNone},
		{"Float-16bytes", LogicalTypeFloat, 16, CompressionNone},
		{"Varchar-8bytes", LogicalTypeVarchar, 8, CompressionNone},
		{"Date-4bytes", LogicalTypeDate, 4, CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataSize)
			got := SelectCompressionWithData(tt.typeID, data)
			assert.Equal(
				t,
				tt.want,
				got,
				"Small data (%d bytes) should not be compressed",
				tt.dataSize,
			)
		})
	}
}

func TestSelectCompressionWithData_LargeData(t *testing.T) {
	// Large data blocks (>=64 bytes) should use type-based selection
	tests := []struct {
		name     string
		typeID   LogicalTypeID
		dataSize int
		want     CompressionType
	}{
		{"Integer-64bytes", LogicalTypeInteger, 64, CompressionBitPack},
		{"Integer-1KB", LogicalTypeInteger, 1024, CompressionBitPack},
		{"Float-128bytes", LogicalTypeFloat, 128, CompressionChimp},
		{"Varchar-256bytes", LogicalTypeVarchar, 256, CompressionFSST},
		{"Date-512bytes", LogicalTypeDate, 512, CompressionRLE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataSize)
			got := SelectCompressionWithData(tt.typeID, data)
			assert.Equal(
				t,
				tt.want,
				got,
				"Large data (%d bytes) should use type-based compression",
				tt.dataSize,
			)
		})
	}
}

func TestSelectCompressionWithData_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name     string
		typeID   LogicalTypeID
		dataSize int
		want     CompressionType
	}{
		{"Empty-Integer", LogicalTypeInteger, 0, CompressionNone},
		{"Boundary-63", LogicalTypeInteger, 63, CompressionNone},
		{"Boundary-64", LogicalTypeInteger, 64, CompressionBitPack},
		{"Boundary-65", LogicalTypeInteger, 65, CompressionBitPack},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataSize)
			got := SelectCompressionWithData(tt.typeID, data)
			assert.Equal(t, tt.want, got, "Data size %d should return %s", tt.dataSize, tt.want)
		})
	}
}

func TestSelectCompressionWithData_ComplexTypesAlwaysNone(t *testing.T) {
	// Complex types should always return None, regardless of data size
	complexTypes := []LogicalTypeID{
		LogicalTypeStruct,
		LogicalTypeList,
		LogicalTypeMap,
		LogicalTypeUnion,
		LogicalTypeArray,
	}

	dataSizes := []int{0, 32, 64, 128, 1024}

	for _, typeID := range complexTypes {
		for _, size := range dataSizes {
			t.Run(fmt.Sprintf("%d-size%d", typeID, size), func(t *testing.T) {
				data := make([]byte, size)
				got := SelectCompressionWithData(typeID, data)
				assert.Equal(
					t,
					CompressionNone,
					got,
					"Complex types should always use no compression",
				)
			})
		}
	}
}

func TestSelectCompression_AllTypeCategories(t *testing.T) {
	// Comprehensive test covering all compression strategies
	categories := map[string]struct {
		types       []LogicalTypeID
		compression CompressionType
	}{
		"BitPack": {
			types: []LogicalTypeID{
				LogicalTypeBoolean,
				LogicalTypeTinyInt, LogicalTypeSmallInt, LogicalTypeInteger, LogicalTypeBigInt,
				LogicalTypeUTinyInt, LogicalTypeUSmallInt, LogicalTypeUInteger, LogicalTypeUBigInt,
				LogicalTypeHugeInt, LogicalTypeUHugeInt,
			},
			compression: CompressionBitPack,
		},
		"Chimp": {
			types:       []LogicalTypeID{LogicalTypeFloat, LogicalTypeDouble, LogicalTypeDecimal},
			compression: CompressionChimp,
		},
		"FSST": {
			types:       []LogicalTypeID{LogicalTypeVarchar, LogicalTypeChar, LogicalTypeBlob},
			compression: CompressionFSST,
		},
		"RLE": {
			types: []LogicalTypeID{
				LogicalTypeDate, LogicalTypeTime, LogicalTypeTimeNS, LogicalTypeTimeTZ,
				LogicalTypeTimestamp, LogicalTypeTimestampSec, LogicalTypeTimestampMS,
				LogicalTypeTimestampNS, LogicalTypeTimestampTZ,
				LogicalTypeInterval, LogicalTypeUUID, LogicalTypeBit,
			},
			compression: CompressionRLE,
		},
		"None": {
			types: []LogicalTypeID{
				LogicalTypeInvalid, LogicalTypeSQLNull, LogicalTypeUnknown, LogicalTypeAny,
				LogicalTypeStruct, LogicalTypeList, LogicalTypeMap, LogicalTypeUnion,
				LogicalTypeArray, LogicalTypeEnum,
			},
			compression: CompressionNone,
		},
	}

	for category, testCase := range categories {
		for _, typeID := range testCase.types {
			t.Run(fmt.Sprintf("%s-Type%d", category, typeID), func(t *testing.T) {
				got := SelectCompression(typeID)
				assert.Equal(t, testCase.compression, got,
					"Type %d should use %s compression", typeID, testCase.compression)
			})
		}
	}
}

func BenchmarkSelectCompression(b *testing.B) {
	types := []LogicalTypeID{
		LogicalTypeInteger,
		LogicalTypeFloat,
		LogicalTypeVarchar,
		LogicalTypeTimestamp,
		LogicalTypeStruct,
	}

	b.ResetTimer()
	for range b.N {
		for _, typeID := range types {
			_ = SelectCompression(typeID)
		}
	}
}

func BenchmarkSelectCompressionWithData(b *testing.B) {
	types := []LogicalTypeID{
		LogicalTypeInteger,
		LogicalTypeFloat,
		LogicalTypeVarchar,
		LogicalTypeTimestamp,
		LogicalTypeStruct,
	}

	dataSizes := []int{32, 64, 256, 1024}

	for _, size := range dataSizes {
		data := make([]byte, size)
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				for _, typeID := range types {
					_ = SelectCompressionWithData(typeID, data)
				}
			}
		})
	}
}
