package duckdb

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarIntEncodeDecodeRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"max single byte", 127},
		{"min two bytes", 128},
		{"two bytes", 255},
		{"max two bytes", 16383},
		{"min three bytes", 16384},
		{"three bytes", 1234567},
		{"large value", 0x123456789ABCDEF},
		{"max uint32", math.MaxUint32},
		{"max uint64", math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Encode
			err := VarIntEncode(&buf, tc.value)
			require.NoError(t, err)

			// Decode
			decoded, err := VarIntDecode(&buf)
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, tc.value, decoded)
		})
	}
}

func TestVarIntSpecificEncodings(t *testing.T) {
	testCases := []struct {
		name     string
		value    uint64
		expected []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"127", 127, []byte{0x7F}},
		{"128", 128, []byte{0x80, 0x01}},
		{"255", 255, []byte{0xFF, 0x01}},
		{"300", 300, []byte{0xAC, 0x02}},
		{"16383", 16383, []byte{0xFF, 0x7F}},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Encode
			err := VarIntEncode(&buf, tc.value)
			require.NoError(t, err)

			// Verify encoding matches expected bytes
			assert.Equal(t, tc.expected, buf.Bytes())

			// Also verify decoding
			decoded, err := VarIntDecode(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			assert.Equal(t, tc.value, decoded)
		})
	}
}

func TestZigZagEncodeDecodeRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"positive one", 1},
		{"negative one", -1},
		{"positive two", 2},
		{"negative two", -2},
		{"small positive", 127},
		{"small negative", -127},
		{"large positive", 1234567890},
		{"large negative", -1234567890},
		{"max int64", math.MaxInt64},
		{"min int64", math.MinInt64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode
			encoded := ZigZagEncode(tc.value)

			// Decode
			decoded := ZigZagDecode(encoded)

			// Verify round-trip
			assert.Equal(t, tc.value, decoded)
		})
	}
}

func TestZigZagSpecificEncodings(t *testing.T) {
	testCases := []struct {
		name     string
		value    int64
		expected uint64
	}{
		{"zero", 0, 0},
		{"negative one", -1, 1},
		{"positive one", 1, 2},
		{"negative two", -2, 3},
		{"positive two", 2, 4},
		{"negative three", -3, 5},
		{"positive three", 3, 6},
		{"negative 64", -64, 127},
		{"positive 64", 64, 128},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := ZigZagEncode(tc.value)
			assert.Equal(t, tc.expected, encoded)

			// Also verify decoding
			decoded := ZigZagDecode(encoded)
			assert.Equal(t, tc.value, decoded)
		})
	}
}

func TestVarIntDecodeError(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := VarIntDecode(&buf)
		assert.Error(t, err)
	})

	t.Run("incomplete encoding", func(t *testing.T) {
		// Write a byte with continuation bit but no following bytes
		buf := bytes.NewBuffer([]byte{0x80})
		_, err := VarIntDecode(buf)
		assert.Error(t, err)
	})
}

func TestCombinedZigZagAndVarInt(t *testing.T) {
	// Test combining zigzag encoding with varint for signed integers
	testCases := []int64{0, 1, -1, 127, -127, 1234567890, -1234567890}

	for _, value := range testCases {
		t.Run("", func(t *testing.T) {
			var buf bytes.Buffer

			// Encode: signed -> zigzag -> varint
			zigzag := ZigZagEncode(value)
			err := VarIntEncode(&buf, zigzag)
			require.NoError(t, err)

			// Decode: varint -> zigzag -> signed
			decoded, err := VarIntDecode(&buf)
			require.NoError(t, err)
			result := ZigZagDecode(decoded)

			assert.Equal(t, value, result)
		})
	}
}

func BenchmarkVarIntEncode(b *testing.B) {
	var buf bytes.Buffer
	b.ResetTimer()

	for range b.N {
		buf.Reset()
		_ = VarIntEncode(&buf, uint64(b.N))
	}
}

func BenchmarkVarIntDecode(b *testing.B) {
	var buf bytes.Buffer
	_ = VarIntEncode(&buf, 1234567890)
	data := buf.Bytes()

	b.ResetTimer()

	for range b.N {
		_, _ = VarIntDecode(bytes.NewReader(data))
	}
}

func BenchmarkZigZagEncode(b *testing.B) {
	value := int64(-1234567890)
	b.ResetTimer()

	for range b.N {
		_ = ZigZagEncode(value)
	}
}

func BenchmarkZigZagDecode(b *testing.B) {
	encoded := ZigZagEncode(-1234567890)
	b.ResetTimer()

	for range b.N {
		_ = ZigZagDecode(encoded)
	}
}
