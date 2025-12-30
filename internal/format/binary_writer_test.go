package format

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBinaryWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)
	assert.NotNil(t, w)
	assert.NotNil(t, w.properties)
	assert.Equal(t, 0, len(w.properties))
}

func TestWritePropertyUint8(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, uint8(42))
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))
	assert.Equal(t, []byte{42}, w.properties[100])
}

func TestWritePropertyUint32(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, uint32(0x12345678))
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify little-endian encoding
	expected := []byte{0x78, 0x56, 0x34, 0x12}
	assert.Equal(t, expected, w.properties[100])
}

func TestWritePropertyUint64(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, uint64(0x123456789ABCDEF0))
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify little-endian encoding
	expected := []byte{0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12}
	assert.Equal(t, expected, w.properties[100])
}

func TestWritePropertyString(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, "hello")
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify format: length (uint64) + data
	reader := bytes.NewReader(w.properties[100])
	var length uint64
	err = binary.Read(reader, ByteOrder, &length)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), length)

	data := make([]byte, length)
	_, err = reader.Read(data)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestWritePropertyBytes(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	data := []byte{0x01, 0x02, 0x03, 0x04}
	err := w.WriteProperty(100, data)
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))
	assert.Equal(t, data, w.properties[100])
}

func TestWritePropertyStringSlice(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	items := []string{"apple", "banana", "cherry"}
	err := w.WriteProperty(100, items)
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify format: count + (length + data) for each item
	reader := bytes.NewReader(w.properties[100])

	var count uint64
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), count)

	for _, expected := range items {
		var length uint64
		err = binary.Read(reader, ByteOrder, &length)
		require.NoError(t, err)

		data := make([]byte, length)
		_, err = reader.Read(data)
		require.NoError(t, err)
		assert.Equal(t, expected, string(data))
	}
}

func TestWritePropertyUnsupportedType(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, 3.14) // float64 not supported
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported value type")
}

func TestWritePropertyWithDefaultSkipsDefault(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WritePropertyWithDefault(100, uint32(0), uint32(0))
	require.NoError(t, err)
	assert.Equal(t, 0, len(w.properties)) // Property should not be written
}

func TestWritePropertyWithDefaultWritesNonDefault(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WritePropertyWithDefault(100, uint32(42), uint32(0))
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))
	assert.NotNil(t, w.properties[100])
}

func TestWritePropertyWithDefaultStringSkipsEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WritePropertyWithDefault(100, "", "")
	require.NoError(t, err)
	assert.Equal(t, 0, len(w.properties))
}

func TestWritePropertyWithDefaultStringWritesNonEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WritePropertyWithDefault(100, "hello", "")
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))
}

func TestWriteList(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	items := []string{"red", "green", "blue"}
	err := w.WriteList(100, items)
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify format matches WriteProperty([]string)
	reader := bytes.NewReader(w.properties[100])

	var count uint64
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), count)

	for _, expected := range items {
		var length uint64
		err = binary.Read(reader, ByteOrder, &length)
		require.NoError(t, err)

		data := make([]byte, length)
		_, err = reader.Read(data)
		require.NoError(t, err)
		assert.Equal(t, expected, string(data))
	}
}

func TestWriteListEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteList(100, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.properties))

	// Verify count is 0
	reader := bytes.NewReader(w.properties[100])
	var count uint64
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestFlushEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.Flush()
	require.NoError(t, err)

	// Should write count of 0
	assert.Equal(t, 4, buf.Len()) // uint32 = 4 bytes
	var count uint32
	err = binary.Read(buf, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestFlushSingleProperty(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	err := w.WriteProperty(100, uint8(42))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Verify output format
	reader := bytes.NewReader(buf.Bytes())

	// Read property count
	var count uint32
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), count)

	// Read property ID
	var id uint32
	err = binary.Read(reader, ByteOrder, &id)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), id)

	// Read data length
	var length uint64
	err = binary.Read(reader, ByteOrder, &length)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), length)

	// Read data
	data := make([]byte, length)
	_, err = reader.Read(data)
	require.NoError(t, err)
	assert.Equal(t, []byte{42}, data)
}

func TestFlushMultiplePropertiesSorted(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	// Write properties in non-sorted order
	err := w.WriteProperty(300, uint8(3))
	require.NoError(t, err)
	err = w.WriteProperty(100, uint8(1))
	require.NoError(t, err)
	err = w.WriteProperty(200, uint8(2))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Verify properties are written in sorted order
	reader := bytes.NewReader(buf.Bytes())

	var count uint32
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), count)

	expectedIDs := []uint32{100, 200, 300}
	expectedValues := []uint8{1, 2, 3}

	for i := 0; i < 3; i++ {
		var id uint32
		err = binary.Read(reader, ByteOrder, &id)
		require.NoError(t, err)
		assert.Equal(t, expectedIDs[i], id, "Property %d ID mismatch", i)

		var length uint64
		err = binary.Read(reader, ByteOrder, &length)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), length)

		var value uint8
		err = binary.Read(reader, ByteOrder, &value)
		require.NoError(t, err)
		assert.Equal(t, expectedValues[i], value, "Property %d value mismatch", i)
	}
}

func TestFlushComplexProperties(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	// Write various property types
	err := w.WriteProperty(100, uint32(ExtraTypeInfoType_DECIMAL))
	require.NoError(t, err)
	err = w.WriteProperty(200, uint8(18)) // width
	require.NoError(t, err)
	err = w.WriteProperty(201, uint8(4)) // scale
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Verify all properties written correctly
	reader := bytes.NewReader(buf.Bytes())

	var count uint32
	err = binary.Read(reader, ByteOrder, &count)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), count)

	// Property 100 (uint32)
	var id100 uint32
	err = binary.Read(reader, ByteOrder, &id100)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), id100)

	var len100 uint64
	err = binary.Read(reader, ByteOrder, &len100)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), len100)

	var val100 uint32
	err = binary.Read(reader, ByteOrder, &val100)
	require.NoError(t, err)
	assert.Equal(t, uint32(ExtraTypeInfoType_DECIMAL), val100)

	// Property 200 (uint8)
	var id200 uint32
	err = binary.Read(reader, ByteOrder, &id200)
	require.NoError(t, err)
	assert.Equal(t, uint32(200), id200)

	var len200 uint64
	err = binary.Read(reader, ByteOrder, &len200)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), len200)

	var val200 uint8
	err = binary.Read(reader, ByteOrder, &val200)
	require.NoError(t, err)
	assert.Equal(t, uint8(18), val200)

	// Property 201 (uint8)
	var id201 uint32
	err = binary.Read(reader, ByteOrder, &id201)
	require.NoError(t, err)
	assert.Equal(t, uint32(201), id201)

	var len201 uint64
	err = binary.Read(reader, ByteOrder, &len201)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), len201)

	var val201 uint8
	err = binary.Read(reader, ByteOrder, &val201)
	require.NoError(t, err)
	assert.Equal(t, uint8(4), val201)
}

func TestFlushLittleEndian(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewBinaryWriter(buf)

	// Write a value with known byte pattern
	err := w.WriteProperty(100, uint32(0x01020304))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Verify little-endian encoding
	bytes := buf.Bytes()

	// Skip property count (4 bytes), ID (4 bytes), and length (8 bytes)
	offset := 4 + 4 + 8
	value := bytes[offset : offset+4]

	// Little-endian: least significant byte first
	assert.Equal(t, []byte{0x04, 0x03, 0x02, 0x01}, value)
}
