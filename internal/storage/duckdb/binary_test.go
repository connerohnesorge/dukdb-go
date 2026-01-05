package duckdb

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBinaryReaderWriter tests round-trip serialization of all types.
func TestBinaryReaderWriter(t *testing.T) {
	t.Run("uint8 round trip", func(t *testing.T) {
		testValues := []uint8{0, 1, 127, 128, 255}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUint8(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUint8()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uint8 round trip failed for %d", v)
		}
	})

	t.Run("int8 round trip", func(t *testing.T) {
		testValues := []int8{-128, -1, 0, 1, 127}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteInt8(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadInt8()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "int8 round trip failed for %d", v)
		}
	})

	t.Run("uint16 round trip", func(t *testing.T) {
		testValues := []uint16{0, 1, 255, 256, 32767, 32768, 65535}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUint16(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(2), writer.BytesWritten())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUint16()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uint16 round trip failed for %d", v)
		}
	})

	t.Run("int16 round trip", func(t *testing.T) {
		testValues := []int16{-32768, -1, 0, 1, 32767}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteInt16(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadInt16()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "int16 round trip failed for %d", v)
		}
	})

	t.Run("uint32 round trip", func(t *testing.T) {
		testValues := []uint32{0, 1, 255, 65535, 2147483647, 4294967295}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUint32(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(4), writer.BytesWritten())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUint32()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uint32 round trip failed for %d", v)
		}
	})

	t.Run("int32 round trip", func(t *testing.T) {
		testValues := []int32{-2147483648, -1, 0, 1, 2147483647}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteInt32(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadInt32()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "int32 round trip failed for %d", v)
		}
	})

	t.Run("uint64 round trip", func(t *testing.T) {
		testValues := []uint64{
			0, 1, 255, 65535,
			4294967295,
			9223372036854775807,
			18446744073709551615,
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUint64(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(8), writer.BytesWritten())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUint64()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uint64 round trip failed for %d", v)
		}
	})

	t.Run("int64 round trip", func(t *testing.T) {
		testValues := []int64{
			-9223372036854775808,
			-1, 0, 1,
			9223372036854775807,
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteInt64(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadInt64()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "int64 round trip failed for %d", v)
		}
	})

	t.Run("float32 round trip", func(t *testing.T) {
		testValues := []float32{
			0.0, 1.0, -1.0,
			math.MaxFloat32, math.SmallestNonzeroFloat32,
			float32(math.Pi), float32(-math.E),
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteFloat32(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(4), writer.BytesWritten())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadFloat32()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "float32 round trip failed for %v", v)
		}
	})

	t.Run("float32 special values", func(t *testing.T) {
		// Test NaN and Inf separately since they need special comparison
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteFloat32(float32(math.NaN()))
		writer.WriteFloat32(float32(math.Inf(1)))
		writer.WriteFloat32(float32(math.Inf(-1)))
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		nan := reader.ReadFloat32()
		posInf := reader.ReadFloat32()
		negInf := reader.ReadFloat32()
		require.NoError(t, reader.Err())

		assert.True(t, math.IsNaN(float64(nan)), "NaN round trip failed")
		assert.True(t, math.IsInf(float64(posInf), 1), "+Inf round trip failed")
		assert.True(t, math.IsInf(float64(negInf), -1), "-Inf round trip failed")
	})

	t.Run("float64 round trip", func(t *testing.T) {
		testValues := []float64{
			0.0, 1.0, -1.0,
			math.MaxFloat64, math.SmallestNonzeroFloat64,
			math.Pi, -math.E,
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteFloat64(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(8), writer.BytesWritten())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadFloat64()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "float64 round trip failed for %v", v)
		}
	})

	t.Run("float64 special values", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteFloat64(math.NaN())
		writer.WriteFloat64(math.Inf(1))
		writer.WriteFloat64(math.Inf(-1))
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		nan := reader.ReadFloat64()
		posInf := reader.ReadFloat64()
		negInf := reader.ReadFloat64()
		require.NoError(t, reader.Err())

		assert.True(t, math.IsNaN(nan), "NaN round trip failed")
		assert.True(t, math.IsInf(posInf, 1), "+Inf round trip failed")
		assert.True(t, math.IsInf(negInf, -1), "-Inf round trip failed")
	})

	t.Run("bool round trip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteBool(true)
		writer.WriteBool(false)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(2), writer.BytesWritten())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		trueVal := reader.ReadBool()
		falseVal := reader.ReadBool()
		require.NoError(t, reader.Err())

		assert.True(t, trueVal, "true round trip failed")
		assert.False(t, falseVal, "false round trip failed")
	})

	t.Run("bool non-zero is true", func(t *testing.T) {
		// Any non-zero byte should read as true
		data := []byte{0, 1, 2, 127, 128, 255}
		reader := NewBinaryReader(bytes.NewReader(data))

		assert.False(t, reader.ReadBool(), "0 should be false")
		assert.True(t, reader.ReadBool(), "1 should be true")
		assert.True(t, reader.ReadBool(), "2 should be true")
		assert.True(t, reader.ReadBool(), "127 should be true")
		assert.True(t, reader.ReadBool(), "128 should be true")
		assert.True(t, reader.ReadBool(), "255 should be true")
	})
}

// TestBinaryBytes tests byte slice read/write.
func TestBinaryBytes(t *testing.T) {
	t.Run("empty bytes", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteBytes(make([]byte, 0))
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(0), writer.BytesWritten())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadBytes(0)
		require.NoError(t, reader.Err())
		assert.Equal(t, make([]byte, 0), result)
	})

	t.Run("non-empty bytes", func(t *testing.T) {
		testData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteBytes(testData)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(5), writer.BytesWritten())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadBytes(5)
		require.NoError(t, reader.Err())
		assert.Equal(t, testData, result)
	})

	t.Run("large bytes", func(t *testing.T) {
		testData := make([]byte, 10000)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteBytes(testData)
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadBytes(10000)
		require.NoError(t, reader.Err())
		assert.Equal(t, testData, result)
	})
}

// TestBinaryString tests string read/write.
func TestBinaryString(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteString("")
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(4), writer.BytesWritten()) // Just the length prefix

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadString()
		require.NoError(t, reader.Err())
		assert.Equal(t, "", result)
	})

	t.Run("simple string", func(t *testing.T) {
		testStr := "Hello, DuckDB!"

		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteString(testStr)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(4+len(testStr)), writer.BytesWritten())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadString()
		require.NoError(t, reader.Err())
		assert.Equal(t, testStr, result)
	})

	t.Run("unicode string", func(t *testing.T) {
		testStr := "Hello, World!"

		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteString(testStr)
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadString()
		require.NoError(t, reader.Err())
		assert.Equal(t, testStr, result)
	})

	t.Run("string with null bytes", func(t *testing.T) {
		testStr := "Hello\x00World"

		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteString(testStr)
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadString()
		require.NoError(t, reader.Err())
		assert.Equal(t, testStr, result)
	})
}

// TestBinaryVarint tests variable-length integer encoding.
func TestBinaryVarint(t *testing.T) {
	t.Run("uvarint small values", func(t *testing.T) {
		testValues := []uint64{0, 1, 127}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUvarint(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(1), writer.BytesWritten(), "small values should use 1 byte")

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUvarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uvarint round trip failed for %d", v)
		}
	})

	t.Run("uvarint medium values", func(t *testing.T) {
		testValues := []uint64{128, 255, 16383}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUvarint(v)
			require.NoError(t, writer.Err())
			assert.Equal(t, int64(2), writer.BytesWritten(), "medium values should use 2 bytes")

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUvarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uvarint round trip failed for %d", v)
		}
	})

	t.Run("uvarint large values", func(t *testing.T) {
		testValues := []uint64{
			16384,      // 3 bytes
			2097151,    // 3 bytes max
			2097152,    // 4 bytes
			268435455,  // 4 bytes max
			268435456,  // 5 bytes
			1 << 32,    // 5 bytes
			1 << 56,    // 8 bytes
			1<<63 - 1,  // near max
			1<<64 - 1,  // max uint64
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteUvarint(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadUvarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "uvarint round trip failed for %d", v)
		}
	})

	t.Run("varint positive values", func(t *testing.T) {
		testValues := []int64{0, 1, 63, 64, 8191, 8192, 1048575, 1048576}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteVarint(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadVarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "varint round trip failed for %d", v)
		}
	})

	t.Run("varint negative values", func(t *testing.T) {
		testValues := []int64{-1, -64, -65, -8192, -8193, -1048576, -1048577}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteVarint(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadVarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "varint round trip failed for %d", v)
		}
	})

	t.Run("varint extreme values", func(t *testing.T) {
		testValues := []int64{
			math.MinInt64,
			math.MaxInt64,
		}
		for _, v := range testValues {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)
			writer.WriteVarint(v)
			require.NoError(t, writer.Err())

			reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			result := reader.ReadVarint()
			require.NoError(t, reader.Err())
			assert.Equal(t, v, result, "varint round trip failed for %d", v)
		}
	})
}

// TestBinaryReaderErrorHandling tests error propagation.
func TestBinaryReaderErrorHandling(t *testing.T) {
	t.Run("read past end uint8", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader(nil))
		result := reader.ReadUint8()
		assert.Error(t, reader.Err())
		assert.Equal(t, io.EOF, reader.Err())
		assert.Equal(t, uint8(0), result)
	})

	t.Run("read past end uint16", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader([]byte{0x01})) // Only 1 byte
		result := reader.ReadUint16()
		assert.Error(t, reader.Err())
		assert.Equal(t, uint16(0), result)
	})

	t.Run("read past end uint32", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader([]byte{0x01, 0x02})) // Only 2 bytes
		result := reader.ReadUint32()
		assert.Error(t, reader.Err())
		assert.Equal(t, uint32(0), result)
	})

	t.Run("read past end uint64", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04})) // Only 4 bytes
		result := reader.ReadUint64()
		assert.Error(t, reader.Err())
		assert.Equal(t, uint64(0), result)
	})

	t.Run("error persists across reads", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader(nil))
		_ = reader.ReadUint8() // Triggers error
		require.Error(t, reader.Err())

		// Subsequent reads should return zero and maintain error
		assert.Equal(t, uint16(0), reader.ReadUint16())
		assert.Equal(t, uint32(0), reader.ReadUint32())
		assert.Equal(t, uint64(0), reader.ReadUint64())
		assert.Equal(t, int64(0), reader.ReadInt64())
		assert.Equal(t, float32(0), reader.ReadFloat32())
		assert.Equal(t, float64(0), reader.ReadFloat64())
		assert.False(t, reader.ReadBool())
		assert.Nil(t, reader.ReadBytes(10))
		assert.Equal(t, "", reader.ReadString())
		assert.Equal(t, uint64(0), reader.ReadUvarint())
		assert.Equal(t, int64(0), reader.ReadVarint())

		// Error should still be set
		assert.Error(t, reader.Err())
	})

	t.Run("read bytes with insufficient data", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader([]byte{0x01, 0x02}))
		result := reader.ReadBytes(10) // Request more than available
		assert.Error(t, reader.Err())
		assert.Nil(t, result)
	})

	t.Run("read string with truncated data", func(t *testing.T) {
		// Write a length prefix indicating 100 bytes but only provide 5
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteUint32(100)
		writer.WriteBytes([]byte("hello"))

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadString()
		assert.Error(t, reader.Err())
		assert.Equal(t, "", result)
	})

	t.Run("reset clears error", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader(nil))
		_ = reader.ReadUint8()
		require.Error(t, reader.Err())

		reader.Reset()
		assert.NoError(t, reader.Err())
	})
}

// TestBinaryWriterErrorHandling tests error propagation for writer.
func TestBinaryWriterErrorHandling(t *testing.T) {
	t.Run("error persists across writes", func(t *testing.T) {
		writer := NewBinaryWriter(&errorWriter{})
		writer.WriteUint8(1)
		require.Error(t, writer.Err())

		// Subsequent writes should be no-ops
		writer.WriteUint16(1)
		writer.WriteUint32(1)
		writer.WriteUint64(1)
		writer.WriteFloat32(1.0)
		writer.WriteFloat64(1.0)
		writer.WriteBool(true)
		writer.WriteBytes([]byte{1, 2, 3})
		writer.WriteString("test")
		writer.WriteUvarint(1)
		writer.WriteVarint(1)
		writer.WritePadding(10)

		// Error should still be from first write
		assert.Error(t, writer.Err())
	})

	t.Run("reset clears error", func(t *testing.T) {
		writer := NewBinaryWriter(&errorWriter{})
		writer.WriteUint8(1)
		require.Error(t, writer.Err())

		writer.Reset()
		assert.NoError(t, writer.Err())
	})
}

// errorWriter is a writer that always returns an error.
type errorWriter struct{}

func (*errorWriter) Write(_ []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}

// TestBinarySkip tests the Skip method.
func TestBinarySkip(t *testing.T) {
	t.Run("skip with seeker", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		reader := NewBinaryReader(bytes.NewReader(data))

		reader.Skip(4)
		require.NoError(t, reader.Err())
		assert.Equal(t, int64(4), reader.BytesRead())

		result := reader.ReadUint32()
		require.NoError(t, reader.Err())
		assert.Equal(t, uint32(0x08070605), result) // Little-endian
	})

	t.Run("skip without seeker", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		reader := NewBinaryReader(&nonSeekingReader{bytes.NewReader(data)})

		reader.Skip(4)
		require.NoError(t, reader.Err())
		assert.Equal(t, int64(4), reader.BytesRead())

		result := reader.ReadUint32()
		require.NoError(t, reader.Err())
		assert.Equal(t, uint32(0x08070605), result)
	})

	t.Run("skip past end without seeker", func(t *testing.T) {
		data := []byte{0x01, 0x02}
		reader := NewBinaryReader(&nonSeekingReader{bytes.NewReader(data)})

		reader.Skip(10)
		assert.Error(t, reader.Err(), "skip past end should fail without seeker")
	})

	t.Run("skip then read past end with seeker", func(t *testing.T) {
		// With seeker, Skip doesn't validate bounds - error happens on next read
		data := []byte{0x01, 0x02}
		reader := NewBinaryReader(bytes.NewReader(data))

		reader.Skip(10)
		// With bytes.Reader, seek past end doesn't error until read
		_ = reader.ReadUint8()
		assert.Error(t, reader.Err(), "read after skip past end should fail")
	})

	t.Run("skip with error already set", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader(nil))
		_ = reader.ReadUint8() // Trigger error
		require.Error(t, reader.Err())

		originalErr := reader.Err()
		reader.Skip(10)
		assert.Equal(t, originalErr, reader.Err()) // Error unchanged
	})
}

// nonSeekingReader wraps a reader without Seek capability.
type nonSeekingReader struct {
	r io.Reader
}

func (n *nonSeekingReader) Read(p []byte) (int, error) {
	return n.r.Read(p)
}

// TestBinaryRemaining tests the Remaining method.
func TestBinaryRemaining(t *testing.T) {
	t.Run("remaining with seeker", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		reader := NewBinaryReader(bytes.NewReader(data))

		assert.Equal(t, int64(8), reader.Remaining())

		reader.ReadUint32()
		assert.Equal(t, int64(4), reader.Remaining())

		reader.ReadUint32()
		assert.Equal(t, int64(0), reader.Remaining())
	})

	t.Run("remaining without seeker", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04}
		reader := NewBinaryReader(&nonSeekingReader{bytes.NewReader(data)})

		assert.Equal(t, int64(-1), reader.Remaining())
	})

	t.Run("remaining with error set", func(t *testing.T) {
		reader := NewBinaryReader(bytes.NewReader(nil))
		_ = reader.ReadUint8() // Trigger error
		require.Error(t, reader.Err())

		assert.Equal(t, int64(0), reader.Remaining())
	})
}

// TestBinaryPadding tests the WritePadding method.
func TestBinaryPadding(t *testing.T) {
	t.Run("write padding", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WritePadding(10)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(10), writer.BytesWritten())

		// All bytes should be zero
		for i, b := range buf.Bytes() {
			assert.Equal(t, byte(0), b, "padding byte %d should be zero", i)
		}
	})

	t.Run("write zero padding", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WritePadding(0)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(0), writer.BytesWritten())
	})

	t.Run("write negative padding", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WritePadding(-5)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(0), writer.BytesWritten())
	})
}

// TestBinaryPropertyID tests the WritePropertyID method.
func TestBinaryPropertyID(t *testing.T) {
	t.Run("write property ID", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WritePropertyID(0x12345678)
		require.NoError(t, writer.Err())
		assert.Equal(t, int64(4), writer.BytesWritten())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		result := reader.ReadUint32()
		require.NoError(t, reader.Err())
		assert.Equal(t, uint32(0x12345678), result)
	})
}

// TestBinaryBytesTracking tests that BytesRead and BytesWritten are tracked correctly.
func TestBinaryBytesTracking(t *testing.T) {
	t.Run("writer bytes tracking", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		assert.Equal(t, int64(0), writer.BytesWritten())

		writer.WriteUint8(1)
		assert.Equal(t, int64(1), writer.BytesWritten())

		writer.WriteUint16(1)
		assert.Equal(t, int64(3), writer.BytesWritten())

		writer.WriteUint32(1)
		assert.Equal(t, int64(7), writer.BytesWritten())

		writer.WriteUint64(1)
		assert.Equal(t, int64(15), writer.BytesWritten())

		writer.WriteFloat32(1.0)
		assert.Equal(t, int64(19), writer.BytesWritten())

		writer.WriteFloat64(1.0)
		assert.Equal(t, int64(27), writer.BytesWritten())

		writer.WriteBool(true)
		assert.Equal(t, int64(28), writer.BytesWritten())

		writer.WriteBytes([]byte{1, 2, 3})
		assert.Equal(t, int64(31), writer.BytesWritten())

		writer.WriteString("hello") // 4 + 5 = 9
		assert.Equal(t, int64(40), writer.BytesWritten())
	})

	t.Run("reader bytes tracking", func(t *testing.T) {
		data := make([]byte, 100)
		reader := NewBinaryReader(bytes.NewReader(data))
		assert.Equal(t, int64(0), reader.BytesRead())

		reader.ReadUint8()
		assert.Equal(t, int64(1), reader.BytesRead())

		reader.ReadUint16()
		assert.Equal(t, int64(3), reader.BytesRead())

		reader.ReadUint32()
		assert.Equal(t, int64(7), reader.BytesRead())

		reader.ReadUint64()
		assert.Equal(t, int64(15), reader.BytesRead())

		reader.ReadFloat32()
		assert.Equal(t, int64(19), reader.BytesRead())

		reader.ReadFloat64()
		assert.Equal(t, int64(27), reader.BytesRead())

		reader.ReadBool()
		assert.Equal(t, int64(28), reader.BytesRead())

		reader.ReadBytes(3)
		assert.Equal(t, int64(31), reader.BytesRead())

		reader.Skip(5)
		assert.Equal(t, int64(36), reader.BytesRead())
	})
}

// TestBinaryLittleEndian verifies little-endian byte order.
func TestBinaryLittleEndian(t *testing.T) {
	t.Run("uint16 little endian", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteUint16(0x0102)
		require.NoError(t, writer.Err())

		// In little-endian, LSB comes first
		assert.Equal(t, []byte{0x02, 0x01}, buf.Bytes())
	})

	t.Run("uint32 little endian", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteUint32(0x01020304)
		require.NoError(t, writer.Err())

		assert.Equal(t, []byte{0x04, 0x03, 0x02, 0x01}, buf.Bytes())
	})

	t.Run("uint64 little endian", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteUint64(0x0102030405060708)
		require.NoError(t, writer.Err())

		assert.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, buf.Bytes())
	})
}

// TestBinaryMultipleValues tests reading/writing multiple values in sequence.
func TestBinaryMultipleValues(t *testing.T) {
	t.Run("write then read multiple values", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write various types
		writer.WriteUint8(0xAA)
		writer.WriteInt16(-1234)
		writer.WriteUint32(0xDEADBEEF)
		writer.WriteInt64(-9876543210)
		writer.WriteFloat32(3.14159)
		writer.WriteFloat64(2.71828)
		writer.WriteBool(true)
		writer.WriteString("test string")
		writer.WriteUvarint(123456789)
		writer.WriteVarint(-987654321)
		require.NoError(t, writer.Err())

		// Read back and verify
		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))

		assert.Equal(t, uint8(0xAA), reader.ReadUint8())
		assert.Equal(t, int16(-1234), reader.ReadInt16())
		assert.Equal(t, uint32(0xDEADBEEF), reader.ReadUint32())
		assert.Equal(t, int64(-9876543210), reader.ReadInt64())
		assert.InDelta(t, float32(3.14159), reader.ReadFloat32(), 0.00001)
		assert.InDelta(t, float64(2.71828), reader.ReadFloat64(), 0.00001)
		assert.True(t, reader.ReadBool())
		assert.Equal(t, "test string", reader.ReadString())
		assert.Equal(t, uint64(123456789), reader.ReadUvarint())
		assert.Equal(t, int64(-987654321), reader.ReadVarint())

		require.NoError(t, reader.Err())
	})
}

// TestBinaryWriteTrue and WriteFalse tests for explicit bool writing.
func TestBinaryWriteTrueFalse(t *testing.T) {
	t.Run("WriteTrue and WriteFalse", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		writer.WriteTrue()
		writer.WriteFalse()
		require.NoError(t, writer.Err())

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))
		assert.True(t, reader.ReadBool())
		assert.False(t, reader.ReadBool())
		require.NoError(t, reader.Err())
	})
}

// BenchmarkBinaryReadWrite benchmarks read/write performance.
func BenchmarkBinaryReadWrite(b *testing.B) {
	b.Run("WriteUint64", func(b *testing.B) {
		buf := &bytes.Buffer{}
		buf.Grow(8 * b.N)
		writer := NewBinaryWriter(buf)

		b.ResetTimer()
		for i := range b.N {
			writer.WriteUint64(uint64(i))
		}
	})

	b.Run("ReadUint64", func(b *testing.B) {
		data := make([]byte, 8*b.N)
		reader := NewBinaryReader(bytes.NewReader(data))

		b.ResetTimer()
		for range b.N {
			reader.ReadUint64()
		}
	})

	b.Run("WriteString", func(b *testing.B) {
		buf := &bytes.Buffer{}
		buf.Grow(100 * b.N)
		writer := NewBinaryWriter(buf)
		testStr := "Hello, World! This is a test string."

		b.ResetTimer()
		for range b.N {
			writer.WriteString(testStr)
		}
	})

	b.Run("WriteUvarint", func(b *testing.B) {
		buf := &bytes.Buffer{}
		buf.Grow(10 * b.N)
		writer := NewBinaryWriter(buf)

		b.ResetTimer()
		for i := range b.N {
			writer.WriteUvarint(uint64(i))
		}
	})

	b.Run("ReadUvarint", func(b *testing.B) {
		// Prepare data with varint-encoded values
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)
		for i := range b.N {
			writer.WriteUvarint(uint64(i % 10000000))
		}

		reader := NewBinaryReader(bytes.NewReader(buf.Bytes()))

		b.ResetTimer()
		for range b.N {
			reader.ReadUvarint()
		}
	})
}
