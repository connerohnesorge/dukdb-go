package duckdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Boolean tests
// ============================================================================

func TestDecodeBool(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
		wantErr  bool
	}{
		{"true", []byte{1}, true, false},
		{"false", []byte{0}, false, false},
		{"non-zero is true", []byte{255}, true, false},
		{"empty data", []byte{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeValue(tt.data, TypeBoolean, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeBool(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected []byte
		wantErr  bool
	}{
		{"true", true, []byte{1}, false},
		{"false", false, []byte{0}, false},
		{"invalid type", "not a bool", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EncodeValue(tt.value, TypeBoolean, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// Signed integer tests
// ============================================================================

func TestDecodeTinyInt(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int8
		wantErr  bool
	}{
		{"zero", []byte{0}, 0, false},
		{"positive", []byte{127}, 127, false},
		{"negative", []byte{0x80}, -128, false},
		{"minus one", []byte{0xFF}, -1, false},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeValue(tt.data, TypeTinyInt, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeSmallInt(t *testing.T) {
	tests := []struct {
		name    string
		value   int16
		wantErr bool
	}{
		{"zero", 0, false},
		{"positive max", 32767, false},
		{"negative min", -32768, false},
		{"positive", 12345, false},
		{"negative", -12345, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 2)
			binary.LittleEndian.PutUint16(data, uint16(tt.value))

			result, err := DecodeValue(data, TypeSmallInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeInteger(t *testing.T) {
	tests := []struct {
		name  string
		value int32
	}{
		{"zero", 0},
		{"positive max", math.MaxInt32},
		{"negative min", math.MinInt32},
		{"positive", 123456789},
		{"negative", -123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(tt.value))

			result, err := DecodeValue(data, TypeInteger, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeBigInt(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"positive max", math.MaxInt64},
		{"negative min", math.MinInt64},
		{"positive", 1234567890123456789},
		{"negative", -1234567890123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(tt.value))

			result, err := DecodeValue(data, TypeBigInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeHugeInt(t *testing.T) {
	tests := []struct {
		name     string
		lower    uint64
		upper    int64
		expected HugeInt
	}{
		{"zero", 0, 0, HugeInt{0, 0}},
		{"positive small", 12345, 0, HugeInt{12345, 0}},
		{"positive large", math.MaxUint64, 100, HugeInt{math.MaxUint64, 100}},
		{"negative", 0, -1, HugeInt{0, -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 16)
			binary.LittleEndian.PutUint64(data[0:8], tt.lower)
			binary.LittleEndian.PutUint64(data[8:16], uint64(tt.upper))

			result, err := DecodeValue(data, TypeHugeInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// Unsigned integer tests
// ============================================================================

func TestDecodeUTinyInt(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint8
	}{
		{"zero", []byte{0}, 0},
		{"max", []byte{255}, 255},
		{"mid", []byte{128}, 128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeValue(tt.data, TypeUTinyInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeUSmallInt(t *testing.T) {
	tests := []struct {
		name  string
		value uint16
	}{
		{"zero", 0},
		{"max", 65535},
		{"mid", 32768},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 2)
			binary.LittleEndian.PutUint16(data, tt.value)

			result, err := DecodeValue(data, TypeUSmallInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeUInteger(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
	}{
		{"zero", 0},
		{"max", math.MaxUint32},
		{"mid", math.MaxUint32 / 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, tt.value)

			result, err := DecodeValue(data, TypeUInteger, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeUBigInt(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"max", math.MaxUint64},
		{"mid", math.MaxUint64 / 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, tt.value)

			result, err := DecodeValue(data, TypeUBigInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodeUHugeInt(t *testing.T) {
	tests := []struct {
		name     string
		lower    uint64
		upper    uint64
		expected UHugeInt
	}{
		{"zero", 0, 0, UHugeInt{0, 0}},
		{"max lower only", math.MaxUint64, 0, UHugeInt{math.MaxUint64, 0}},
		{"max both", math.MaxUint64, math.MaxUint64, UHugeInt{math.MaxUint64, math.MaxUint64}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 16)
			binary.LittleEndian.PutUint64(data[0:8], tt.lower)
			binary.LittleEndian.PutUint64(data[8:16], tt.upper)

			result, err := DecodeValue(data, TypeUHugeInt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// Floating point tests
// ============================================================================

func TestDecodeFloat(t *testing.T) {
	tests := []struct {
		name  string
		value float32
	}{
		{"zero", 0.0},
		{"positive", 3.14159},
		{"negative", -3.14159},
		{"max", math.MaxFloat32},
		{"min positive", math.SmallestNonzeroFloat32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, math.Float32bits(tt.value))

			result, err := DecodeValue(data, TypeFloat, nil)
			require.NoError(t, err)
			assert.InDelta(t, tt.value, result, 0.0001)
		})
	}
}

func TestDecodeDouble(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"zero", 0.0},
		{"positive", 3.141592653589793},
		{"negative", -3.141592653589793},
		{"max", math.MaxFloat64},
		{"min positive", math.SmallestNonzeroFloat64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, math.Float64bits(tt.value))

			result, err := DecodeValue(data, TypeDouble, nil)
			require.NoError(t, err)
			assert.InDelta(t, tt.value, result, 0.0000001)
		})
	}
}

// ============================================================================
// Decimal tests
// ============================================================================

func TestDecodeDecimal(t *testing.T) {
	tests := []struct {
		name     string
		width    uint8
		scale    uint8
		rawValue int64
		expected float64 // For comparison
	}{
		// Width 4 uses 1 byte storage (int8), max value is 127
		{"small precision int8", 4, 2, 12, 0.12},
		// Width 9 uses 2 byte storage (int16), max value is 32767
		{"medium precision int16", 9, 2, 12345, 123.45},
		// Width 18 uses 4 byte storage (int32)
		{"large precision int32", 18, 4, 123456789, 12345.6789},
		// Width > 18 uses 8 byte storage (int64)
		{"xlarge precision int64", 38, 4, 123456789012345678, 12345678901234.5678},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mods := &TypeModifiers{Width: tt.width, Scale: tt.scale}
			size := DecimalStorageSize(tt.width)
			data := make([]byte, size)

			switch size {
			case 1:
				data[0] = byte(tt.rawValue)
			case 2:
				binary.LittleEndian.PutUint16(data, uint16(tt.rawValue))
			case 4:
				binary.LittleEndian.PutUint32(data, uint32(tt.rawValue))
			case 8:
				binary.LittleEndian.PutUint64(data, uint64(tt.rawValue))
			}

			result, err := DecodeValue(data, TypeDecimal, mods)
			require.NoError(t, err)

			dec, ok := result.(Decimal)
			require.True(t, ok)
			assert.Equal(t, tt.width, dec.Width)
			assert.Equal(t, tt.scale, dec.Scale)
			assert.InDelta(t, tt.expected, dec.ToFloat64(), 0.0001)
		})
	}
}

func TestDecimalRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		width uint8
		scale uint8
		value int64
	}{
		// Width 4 uses 1 byte storage (int8), so value must fit in int8
		{"small int8", 4, 2, 12},
		// Width 9 uses 2 byte storage (int16), so value must fit in int16
		{"medium int16", 9, 2, 12345},
		// Width 18 uses 4 byte storage (int32)
		{"large int32", 18, 4, 123456789},
		// Width > 18 uses 8 byte storage (int64)
		{"xlarge int64", 38, 4, 123456789012345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mods := &TypeModifiers{Width: tt.width, Scale: tt.scale}
			dec := Decimal{Value: big.NewInt(tt.value), Width: tt.width, Scale: tt.scale}

			encoded, err := EncodeValue(dec, TypeDecimal, mods)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeDecimal, mods)
			require.NoError(t, err)

			result := decoded.(Decimal)
			assert.Equal(t, tt.value, result.Value.Int64())
		})
	}
}

// ============================================================================
// String type tests
// ============================================================================

func TestDecodeVarchar(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"empty", "", false},
		{"hello", "hello", false},
		{"unicode", "Hello, 世界! 🌍", false},
		{
			"long string",
			"This is a longer string with more characters for testing purposes.",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			binary.Write(&buf, binary.LittleEndian, uint32(len(tt.value)))
			buf.WriteString(tt.value)
			data := buf.Bytes()

			result, err := DecodeValue(data, TypeVarchar, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestVarcharRoundTrip(t *testing.T) {
	tests := []string{
		"",
		"hello",
		"Hello, 世界! 🌍",
		"A longer string with many characters for testing purposes.",
	}

	for _, s := range tests {
		t.Run(s[:min(10, len(s))], func(t *testing.T) {
			encoded, err := EncodeValue(s, TypeVarchar, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeVarchar, nil)
			require.NoError(t, err)
			assert.Equal(t, s, decoded)
		})
	}
}

func TestDecodeChar(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		length   uint32
		expected string
	}{
		{"exact length", "hello", 5, "hello"},
		{"padded", "hi   ", 5, "hi   "},
		{"single char", "X", 1, "X"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mods := &TypeModifiers{Length: tt.length}
			data := []byte(tt.data)

			result, err := DecodeValue(data, TypeChar, mods)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCharRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		length uint32
	}{
		{"exact", "hello", 5},
		{"shorter", "hi", 5},
		{"single", "X", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mods := &TypeModifiers{Length: tt.length}

			encoded, err := EncodeValue(tt.value, TypeChar, mods)
			require.NoError(t, err)
			assert.Len(t, encoded, int(tt.length))

			decoded, err := DecodeValue(encoded, TypeChar, mods)
			require.NoError(t, err)

			// The decoded value may have trailing spaces
			result := decoded.(string)
			assert.Len(t, result, int(tt.length))
		})
	}
}

func TestDecodeBlob(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"empty", []byte{}},
		{"binary", []byte{0x00, 0xFF, 0x01, 0xFE}},
		{"text as bytes", []byte("hello world")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			binary.Write(&buf, binary.LittleEndian, uint32(len(tt.value)))
			buf.Write(tt.value)
			data := buf.Bytes()

			result, err := DecodeValue(data, TypeBlob, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestBlobRoundTrip(t *testing.T) {
	tests := [][]byte{
		{},
		{0x00, 0xFF, 0x01, 0xFE},
		[]byte("hello world"),
		make([]byte, 256), // Large blob
	}

	for i, b := range tests {
		t.Run("", func(t *testing.T) {
			_ = i // Silence unused variable warning

			encoded, err := EncodeValue(b, TypeBlob, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeBlob, nil)
			require.NoError(t, err)
			assert.Equal(t, b, decoded)
		})
	}
}

func TestDecodeBit(t *testing.T) {
	tests := []struct {
		name     string
		bits     uint64
		data     []byte
		expected BitString
	}{
		{"8 bits", 8, []byte{0xFF}, BitString{[]byte{0xFF}, 8}},
		{"16 bits", 16, []byte{0xAA, 0x55}, BitString{[]byte{0xAA, 0x55}, 16}},
		{"partial byte", 5, []byte{0x1F}, BitString{[]byte{0x1F}, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			binary.Write(&buf, binary.LittleEndian, tt.bits)
			buf.Write(tt.data)
			data := buf.Bytes()

			result, err := DecodeValue(data, TypeBit, nil)
			require.NoError(t, err)

			bs := result.(BitString)
			assert.Equal(t, tt.expected.Length, bs.Length)
			assert.Equal(t, tt.expected.Data, bs.Data)
		})
	}
}

// ============================================================================
// Date/Time tests
// ============================================================================

func TestDecodeDate(t *testing.T) {
	tests := []struct {
		name     string
		days     int32
		expected time.Time
	}{
		{"epoch", 0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"2024-01-01", 19723, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"before epoch", -365, time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(tt.days))

			result, err := DecodeValue(data, TypeDate, nil)
			require.NoError(t, err)

			// Compare dates only (ignoring time component)
			resultDate := result.(time.Time)
			assert.Equal(t, tt.expected.Year(), resultDate.Year())
			assert.Equal(t, tt.expected.Month(), resultDate.Month())
			assert.Equal(t, tt.expected.Day(), resultDate.Day())
		})
	}
}

func TestDateRoundTrip(t *testing.T) {
	dates := []time.Time{
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	for _, d := range dates {
		t.Run(d.Format("2006-01-02"), func(t *testing.T) {
			encoded, err := EncodeValue(d, TypeDate, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeDate, nil)
			require.NoError(t, err)

			result := decoded.(time.Time)
			assert.Equal(t, d.Year(), result.Year())
			assert.Equal(t, d.Month(), result.Month())
			assert.Equal(t, d.Day(), result.Day())
		})
	}
}

func TestDecodeTime(t *testing.T) {
	tests := []struct {
		name     string
		micros   int64
		expected time.Duration
	}{
		{"midnight", 0, 0},
		{"1 hour", 3600 * 1000000, time.Hour},
		{"12:30:45", 45045 * 1000000, 12*time.Hour + 30*time.Minute + 45*time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(tt.micros))

			result, err := DecodeValue(data, TypeTime, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeTimeNS(t *testing.T) {
	tests := []struct {
		name     string
		nanos    int64
		expected TimeNS
	}{
		{"midnight", 0, TimeNS{0}},
		{"1 second", 1000000000, TimeNS{1000000000}},
		{"12:30:45.123456789", 45045123456789, TimeNS{45045123456789}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(tt.nanos))

			result, err := DecodeValue(data, TypeTimeNS, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeTimeTZ(t *testing.T) {
	tests := []struct {
		name     string
		micros   int64
		offset   int32
		expected TimeTZ
	}{
		{"UTC midnight", 0, 0, TimeTZ{0, 0}},
		{"EST noon", 43200000000, -18000, TimeTZ{43200000000, -18000}},
		{"JST evening", 72000000000, 32400, TimeTZ{72000000000, 32400}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 12)
			binary.LittleEndian.PutUint64(data[0:8], uint64(tt.micros))
			binary.LittleEndian.PutUint32(data[8:12], uint32(tt.offset))

			result, err := DecodeValue(data, TypeTimeTZ, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		micros   int64
		expected time.Time
	}{
		{"epoch", 0, time.Unix(0, 0).UTC()},
		{"2024-01-01 12:00:00", 1704110400000000, time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(tt.micros))

			result, err := DecodeValue(data, TypeTimestamp, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	timestamps := []time.Time{
		time.Unix(0, 0).UTC(),
		time.Date(2024, 6, 15, 12, 30, 45, 123456000, time.UTC),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	for _, ts := range timestamps {
		t.Run(ts.Format(time.RFC3339), func(t *testing.T) {
			encoded, err := EncodeValue(ts, TypeTimestamp, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeTimestamp, nil)
			require.NoError(t, err)

			result := decoded.(time.Time)
			// Timestamps are stored with microsecond precision
			assert.Equal(t, ts.UnixMicro(), result.UnixMicro())
		})
	}
}

func TestDecodeTimestampVariants(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 30, 45, 123456789, time.UTC)

	t.Run("TIMESTAMP_S", func(t *testing.T) {
		secs := ts.Unix()
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(secs))

		result, err := DecodeValue(data, TypeTimestampS, nil)
		require.NoError(t, err)
		assert.Equal(t, secs, result.(time.Time).Unix())
	})

	t.Run("TIMESTAMP_MS", func(t *testing.T) {
		millis := ts.UnixMilli()
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(millis))

		result, err := DecodeValue(data, TypeTimestampMS, nil)
		require.NoError(t, err)
		assert.Equal(t, millis, result.(time.Time).UnixMilli())
	})

	t.Run("TIMESTAMP_NS", func(t *testing.T) {
		nanos := ts.UnixNano()
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(nanos))

		result, err := DecodeValue(data, TypeTimestampNS, nil)
		require.NoError(t, err)
		assert.Equal(t, nanos, result.(time.Time).UnixNano())
	})
}

func TestDecodeInterval(t *testing.T) {
	tests := []struct {
		name     string
		months   int32
		days     int32
		micros   int64
		expected Interval
	}{
		{"zero", 0, 0, 0, Interval{0, 0, 0}},
		{"1 year", 12, 0, 0, Interval{12, 0, 0}},
		{"1 day", 0, 1, 0, Interval{0, 1, 0}},
		{"1 hour", 0, 0, 3600000000, Interval{0, 0, 3600000000}},
		{"mixed", 2, 15, 43200000000, Interval{2, 15, 43200000000}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, 16)
			binary.LittleEndian.PutUint32(data[0:4], uint32(tt.months))
			binary.LittleEndian.PutUint32(data[4:8], uint32(tt.days))
			binary.LittleEndian.PutUint64(data[8:16], uint64(tt.micros))

			result, err := DecodeValue(data, TypeInterval, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIntervalRoundTrip(t *testing.T) {
	intervals := []Interval{
		{0, 0, 0},
		{12, 0, 0},
		{0, 30, 0},
		{0, 0, 3600000000},
		{6, 15, 43200000000},
	}

	for _, iv := range intervals {
		t.Run("", func(t *testing.T) {
			encoded, err := EncodeValue(iv, TypeInterval, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeInterval, nil)
			require.NoError(t, err)
			assert.Equal(t, iv, decoded)
		})
	}
}

// ============================================================================
// UUID tests
// ============================================================================

func TestDecodeUUID(t *testing.T) {
	tests := []struct {
		name     string
		data     [16]byte
		expected string
	}{
		{
			"zero",
			[16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			"00000000-0000-0000-0000-000000000000",
		},
		{
			"typical",
			[16]byte{
				0x12,
				0x34,
				0x56,
				0x78,
				0x9a,
				0xbc,
				0xde,
				0xf0,
				0x12,
				0x34,
				0x56,
				0x78,
				0x9a,
				0xbc,
				0xde,
				0xf0,
			},
			"12345678-9abc-def0-1234-56789abcdef0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeValue(tt.data[:], TypeUUID, nil)
			require.NoError(t, err)

			uuid := result.(UUID)
			assert.Equal(t, tt.expected, uuid.String())
		})
	}
}

func TestUUIDRoundTrip(t *testing.T) {
	uuids := []string{
		"00000000-0000-0000-0000-000000000000",
		"12345678-9abc-def0-1234-56789abcdef0",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}

	for _, s := range uuids {
		t.Run(s, func(t *testing.T) {
			encoded, err := EncodeValue(s, TypeUUID, nil)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeUUID, nil)
			require.NoError(t, err)

			uuid := decoded.(UUID)
			assert.Equal(t, s, uuid.String())
		})
	}
}

// ============================================================================
// Complex type tests
// ============================================================================

func TestDecodeEnum(t *testing.T) {
	tests := []struct {
		name        string
		values      []string
		index       uint32
		expectedStr string
	}{
		{"small enum uint8", []string{"a", "b", "c"}, 1, "b"},
		{"medium enum uint16", make([]string, 300), 150, ""},
		{"first value", []string{"first", "second", "third"}, 0, "first"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mods := &TypeModifiers{EnumValues: tt.values}
			size := EnumStorageSize(len(tt.values))
			data := make([]byte, size)

			switch size {
			case 1:
				data[0] = byte(tt.index)
			case 2:
				binary.LittleEndian.PutUint16(data, uint16(tt.index))
			case 4:
				binary.LittleEndian.PutUint32(data, tt.index)
			}

			result, err := DecodeValue(data, TypeEnum, mods)
			require.NoError(t, err)

			ev := result.(EnumValue)
			assert.Equal(t, tt.index, ev.Index)
			if tt.expectedStr != "" {
				assert.Equal(t, tt.expectedStr, ev.Value)
			}
		})
	}
}

func TestEnumRoundTrip(t *testing.T) {
	values := []string{"small", "medium", "large"}
	mods := &TypeModifiers{EnumValues: values}

	for i, v := range values {
		t.Run(v, func(t *testing.T) {
			ev := EnumValue{Index: uint32(i), Value: v}

			encoded, err := EncodeValue(ev, TypeEnum, mods)
			require.NoError(t, err)

			decoded, err := DecodeValue(encoded, TypeEnum, mods)
			require.NoError(t, err)

			result := decoded.(EnumValue)
			assert.Equal(t, ev.Index, result.Index)
			assert.Equal(t, ev.Value, result.Value)
		})
	}
}

// ============================================================================
// Helper function tests
// ============================================================================

func TestGetValueSize(t *testing.T) {
	tests := []struct {
		typeID   LogicalTypeID
		mods     *TypeModifiers
		expected int
	}{
		{TypeBoolean, nil, 1},
		{TypeTinyInt, nil, 1},
		{TypeSmallInt, nil, 2},
		{TypeInteger, nil, 4},
		{TypeBigInt, nil, 8},
		{TypeFloat, nil, 4},
		{TypeDouble, nil, 8},
		{TypeDate, nil, 4},
		{TypeTime, nil, 8},
		{TypeTimestamp, nil, 8},
		{TypeHugeInt, nil, 16},
		{TypeUUID, nil, 16},
		{TypeInterval, nil, 16},
		{TypeVarchar, nil, 0},
		{TypeBlob, nil, 0},
		{TypeList, nil, 0},
		{TypeDecimal, &TypeModifiers{Width: 4}, 1},
		{TypeDecimal, &TypeModifiers{Width: 9}, 2},
		{TypeDecimal, &TypeModifiers{Width: 18}, 4},
		{TypeDecimal, &TypeModifiers{Width: 38}, 8},
		{TypeDecimal, &TypeModifiers{Width: 39}, 16},
		{TypeChar, &TypeModifiers{Length: 10}, 10},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			result := GetValueSize(tt.typeID, tt.mods)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsFixedSize(t *testing.T) {
	fixedTypes := []LogicalTypeID{
		TypeBoolean, TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt,
		TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt,
		TypeHugeInt, TypeUHugeInt,
		TypeFloat, TypeDouble,
		TypeDate, TypeTime, TypeTimeNS, TypeTimeTZ,
		TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ,
		TypeInterval, TypeUUID,
	}

	for _, typeID := range fixedTypes {
		t.Run(typeID.String(), func(t *testing.T) {
			assert.True(t, IsFixedSize(typeID))
		})
	}

	variableTypes := []LogicalTypeID{
		TypeVarchar, TypeBlob, TypeList, TypeStruct, TypeMap,
	}

	for _, typeID := range variableTypes {
		t.Run(typeID.String(), func(t *testing.T) {
			assert.False(t, IsFixedSize(typeID))
		})
	}
}

func TestIsVariableSize(t *testing.T) {
	variableTypes := []LogicalTypeID{
		TypeVarchar, TypeBlob, TypeBit, TypeStringLiteral,
		TypeList, TypeStruct, TypeMap, TypeUnion, TypeArray,
	}

	for _, typeID := range variableTypes {
		t.Run(typeID.String(), func(t *testing.T) {
			assert.True(t, IsVariableSize(typeID))
		})
	}

	fixedTypes := []LogicalTypeID{
		TypeBoolean, TypeInteger, TypeDouble, TypeDate, TypeTimestamp,
	}

	for _, typeID := range fixedTypes {
		t.Run(typeID.String(), func(t *testing.T) {
			assert.False(t, IsVariableSize(typeID))
		})
	}
}

func TestDecimalStorageSize(t *testing.T) {
	tests := []struct {
		width    uint8
		expected int
	}{
		{1, 1},
		{4, 1},
		{5, 2},
		{9, 2},
		{10, 4},
		{18, 4},
		{19, 8},
		{38, 8},
		{39, 16},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := DecimalStorageSize(tt.width)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnumStorageSize(t *testing.T) {
	tests := []struct {
		valueCount int
		expected   int
	}{
		{1, 1},
		{255, 1},
		{256, 1},
		{257, 2},
		{65535, 2},
		{65536, 2},
		{65537, 4},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := EnumStorageSize(tt.valueCount)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// HugeInt/UHugeInt conversion tests
// ============================================================================

func TestHugeIntToBigInt(t *testing.T) {
	tests := []struct {
		name     string
		h        HugeInt
		expected string // String representation of expected big.Int
	}{
		{"zero", HugeInt{0, 0}, "0"},
		{"small positive", HugeInt{12345, 0}, "12345"},
		{"max uint64 lower", HugeInt{math.MaxUint64, 0}, "18446744073709551615"},
		{"with upper bits", HugeInt{0, 1}, "18446744073709551616"}, // 2^64
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.h.ToBigInt()
			assert.Equal(t, tt.expected, result.String())
		})
	}
}

func TestUHugeIntToBigInt(t *testing.T) {
	tests := []struct {
		name     string
		h        UHugeInt
		expected string
	}{
		{"zero", UHugeInt{0, 0}, "0"},
		{"small positive", UHugeInt{12345, 0}, "12345"},
		{"max uint64 lower", UHugeInt{math.MaxUint64, 0}, "18446744073709551615"},
		{"with upper bits", UHugeInt{0, 1}, "18446744073709551616"},
		{
			"max value",
			UHugeInt{math.MaxUint64, math.MaxUint64},
			"340282366920938463463374607431768211455",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.h.ToBigInt()
			assert.Equal(t, tt.expected, result.String())
		})
	}
}

// ============================================================================
// Error handling tests
// ============================================================================

func TestDecodeValueErrors(t *testing.T) {
	t.Run("invalid type", func(t *testing.T) {
		_, err := DecodeValue([]byte{0}, TypeInvalid, nil)
		assert.Error(t, err)
	})

	t.Run("insufficient data for integer", func(t *testing.T) {
		_, err := DecodeValue([]byte{0}, TypeInteger, nil)
		assert.Error(t, err)
	})

	t.Run("insufficient data for bigint", func(t *testing.T) {
		_, err := DecodeValue([]byte{0, 1, 2, 3}, TypeBigInt, nil)
		assert.Error(t, err)
	})

	t.Run("insufficient data for hugeint", func(t *testing.T) {
		_, err := DecodeValue([]byte{0, 1, 2, 3, 4, 5, 6, 7}, TypeHugeInt, nil)
		assert.Error(t, err)
	})

	t.Run("insufficient data for varchar", func(t *testing.T) {
		_, err := DecodeValue([]byte{0, 0, 0}, TypeVarchar, nil)
		assert.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		_, err := DecodeValue([]byte{0}, TypeGeometry, nil)
		assert.Error(t, err)
	})
}

func TestEncodeValueErrors(t *testing.T) {
	t.Run("invalid bool type", func(t *testing.T) {
		_, err := EncodeValue("not a bool", TypeBoolean, nil)
		assert.Error(t, err)
	})

	t.Run("invalid integer type", func(t *testing.T) {
		_, err := EncodeValue("not an int", TypeInteger, nil)
		assert.Error(t, err)
	})

	t.Run("invalid uuid length", func(t *testing.T) {
		_, err := EncodeValue([]byte{0, 1, 2}, TypeUUID, nil)
		assert.Error(t, err)
	})
}

// ============================================================================
// Interval helper tests
// ============================================================================

func TestIntervalToDuration(t *testing.T) {
	tests := []struct {
		name     string
		interval Interval
		expected time.Duration
	}{
		{"zero", Interval{0, 0, 0}, 0},
		{"1 day", Interval{0, 1, 0}, 24 * time.Hour},
		{"1 hour micros", Interval{0, 0, 3600 * 1000000}, time.Hour},
		{"1 month approx", Interval{1, 0, 0}, 30 * 24 * time.Hour}, // 30 days approximation
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.ToDuration()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// Decimal helper tests
// ============================================================================

func TestDecimalToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		scale    uint8
		expected float64
	}{
		{"zero", 0, 0, 0.0},
		{"whole number", 12345, 0, 12345.0},
		{"with decimals", 12345, 2, 123.45},
		{"negative", -12345, 2, -123.45},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := Decimal{Value: big.NewInt(tt.value), Width: 18, Scale: tt.scale}
			result := dec.ToFloat64()
			assert.InDelta(t, tt.expected, result, 0.0001)
		})
	}
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
