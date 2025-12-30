package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewBinaryReader tests BinaryReader construction.
func TestNewBinaryReader(t *testing.T) {
	buf := &bytes.Buffer{}
	reader := NewBinaryReader(buf)

	require.NotNil(t, reader)
	assert.NotNil(t, reader.properties)
	assert.Equal(t, 0, len(reader.properties))
}

// TestBinaryReaderLoad tests loading properties from a binary stream.
func TestBinaryReaderLoad(t *testing.T) {
	tests := []struct {
		name       string
		properties map[uint32][]byte
		wantErr    bool
	}{
		{
			name:       "empty properties",
			properties: map[uint32][]byte{},
			wantErr:    false,
		},
		{
			name: "single property",
			properties: map[uint32][]byte{
				100: {0x01, 0x02, 0x03, 0x04},
			},
			wantErr: false,
		},
		{
			name: "multiple properties",
			properties: map[uint32][]byte{
				100: {0x01, 0x02, 0x03, 0x04},
				101: {0x05, 0x06},
				200: {0x07, 0x08, 0x09},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write properties using BinaryWriter
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)

			for id, data := range tt.properties {
				err := writer.WriteProperty(id, data)
				require.NoError(t, err)
			}

			err := writer.Flush()
			require.NoError(t, err)

			// Load properties using BinaryReader
			reader := NewBinaryReader(buf)
			err = reader.Load()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(tt.properties), len(reader.properties))

				// Verify each property was loaded correctly
				for id, expectedData := range tt.properties {
					actualData, ok := reader.properties[id]
					require.True(t, ok, "property %d not found", id)
					assert.Equal(t, expectedData, actualData)
				}
			}
		})
	}
}

// TestBinaryReaderReadPropertyUint8 tests reading uint8 properties.
func TestBinaryReaderReadPropertyUint8(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write a uint8 property
	err := writer.WriteProperty(200, uint8(42))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read the property
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value uint8
	err = reader.ReadProperty(200, &value)
	require.NoError(t, err)
	assert.Equal(t, uint8(42), value)
}

// TestBinaryReaderReadPropertyUint32 tests reading uint32 properties.
func TestBinaryReaderReadPropertyUint32(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write a uint32 property
	err := writer.WriteProperty(100, uint32(0x12345678))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read the property
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value uint32
	err = reader.ReadProperty(100, &value)
	require.NoError(t, err)
	assert.Equal(t, uint32(0x12345678), value)
}

// TestBinaryReaderReadPropertyUint64 tests reading uint64 properties.
func TestBinaryReaderReadPropertyUint64(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write a uint64 property
	err := writer.WriteProperty(200, uint64(0x123456789ABCDEF0))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read the property
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value uint64
	err = reader.ReadProperty(200, &value)
	require.NoError(t, err)
	assert.Equal(t, uint64(0x123456789ABCDEF0), value)
}

// TestBinaryReaderReadPropertyString tests reading string properties.
func TestBinaryReaderReadPropertyString(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"empty string", ""},
		{"simple string", "hello"},
		{"string with spaces", "hello world"},
		{"unicode string", "hello 世界"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)

			// Write a string property
			err := writer.WriteProperty(101, tt.value)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Read the property
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)

			var value string
			err = reader.ReadProperty(101, &value)
			require.NoError(t, err)
			assert.Equal(t, tt.value, value)
		})
	}
}

// TestBinaryReaderReadPropertyBytes tests reading []byte properties.
func TestBinaryReaderReadPropertyBytes(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write a []byte property
	expectedBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	err := writer.WriteProperty(103, expectedBytes)
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read the property
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value []byte
	err = reader.ReadProperty(103, &value)
	require.NoError(t, err)
	assert.Equal(t, expectedBytes, value)

	// Verify defensive copy
	value[0] = 0xFF
	assert.NotEqual(t, expectedBytes[0], value[0])
}

// TestBinaryReaderReadPropertyMissing tests reading a missing required property.
func TestBinaryReaderReadPropertyMissing(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write property 100 only
	err := writer.WriteProperty(100, uint32(42))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Try to read missing property 200
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value uint32
	err = reader.ReadProperty(200, &value)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRequiredProperty))
}

// TestBinaryReaderReadPropertyWithDefault tests reading optional properties with defaults.
func TestBinaryReaderReadPropertyWithDefault(t *testing.T) {
	t.Run("property present", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write a property
		err := writer.WriteProperty(200, uint8(42))
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		// Read the property with a default
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var value uint8
		err = reader.ReadPropertyWithDefault(200, &value, uint8(99))
		require.NoError(t, err)
		assert.Equal(t, uint8(42), value, "should use actual value, not default")
	})

	t.Run("property missing - uint8", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write no properties
		err := writer.Flush()
		require.NoError(t, err)

		// Read missing property with a default
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var value uint8
		err = reader.ReadPropertyWithDefault(200, &value, uint8(99))
		require.NoError(t, err)
		assert.Equal(t, uint8(99), value, "should use default value")
	})

	t.Run("property missing - string", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write no properties
		err := writer.Flush()
		require.NoError(t, err)

		// Read missing property with a default
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var value string
		err = reader.ReadPropertyWithDefault(101, &value, "default")
		require.NoError(t, err)
		assert.Equal(t, "default", value, "should use default value")
	})

	t.Run("property missing - uint32", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write no properties
		err := writer.Flush()
		require.NoError(t, err)

		// Read missing property with a default
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var value uint32
		err = reader.ReadPropertyWithDefault(201, &value, uint32(0))
		require.NoError(t, err)
		assert.Equal(t, uint32(0), value, "should use default value")
	})
}

// TestBinaryReaderReadList tests reading string lists.
func TestBinaryReaderReadList(t *testing.T) {
	tests := []struct {
		name  string
		items []string
	}{
		{"empty list", []string{}},
		{"single item", []string{"RED"}},
		{"multiple items", []string{"RED", "GREEN", "BLUE"}},
		{"items with spaces", []string{"hello world", "foo bar"}},
		{"unicode items", []string{"hello", "世界", "🚀"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writer := NewBinaryWriter(buf)

			// Write a list property
			err := writer.WriteList(201, tt.items)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Read the list
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)

			items, err := reader.ReadList(201)
			require.NoError(t, err)
			assert.Equal(t, tt.items, items)

			// Verify defensive copy
			if len(items) > 0 {
				items[0] = "MODIFIED"
				newItems, err := reader.ReadList(201)
				require.NoError(t, err)
				assert.NotEqual(t, items[0], newItems[0], "should return defensive copy")
			}
		})
	}
}

// TestBinaryReaderReadListMissing tests reading a missing list property.
func TestBinaryReaderReadListMissing(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write no properties
	err := writer.Flush()
	require.NoError(t, err)

	// Try to read missing list
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	items, err := reader.ReadList(201)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRequiredProperty))
	assert.Nil(t, items)
}

// TestBinaryReaderRoundTrip tests complete write/read round-trip.
func TestBinaryReaderRoundTrip(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write various property types
	err := writer.WriteProperty(100, uint32(ExtraTypeInfoType_ENUM))
	require.NoError(t, err)
	err = writer.WriteProperty(200, uint64(3))
	require.NoError(t, err)
	err = writer.WriteList(201, []string{"RED", "GREEN", "BLUE"})
	require.NoError(t, err)
	err = writer.WritePropertyWithDefault(202, uint8(10), uint8(0))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read properties back
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	// Verify property 100 (type discriminator)
	var typeInfo uint32
	err = reader.ReadProperty(100, &typeInfo)
	require.NoError(t, err)
	assert.Equal(t, uint32(ExtraTypeInfoType_ENUM), typeInfo)

	// Verify property 200 (count)
	var count uint64
	err = reader.ReadProperty(200, &count)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), count)

	// Verify property 201 (enum values)
	values, err := reader.ReadList(201)
	require.NoError(t, err)
	assert.Equal(t, []string{"RED", "GREEN", "BLUE"}, values)

	// Verify property 202 (optional with value)
	var scale uint8
	err = reader.ReadPropertyWithDefault(202, &scale, uint8(0))
	require.NoError(t, err)
	assert.Equal(t, uint8(10), scale)

	// Verify missing optional property uses default
	var missing uint8
	err = reader.ReadPropertyWithDefault(999, &missing, uint8(77))
	require.NoError(t, err)
	assert.Equal(t, uint8(77), missing)
}

// TestBinaryReaderInvalidDestination tests error handling for invalid destination types.
func TestBinaryReaderInvalidDestination(t *testing.T) {
	t.Run("unsupported type pointer", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write a uint32 property
		err := writer.WriteProperty(100, uint32(42))
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		// Try to read into unsupported type
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var unsupported float64
		err = reader.ReadProperty(100, &unsupported)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidPropertyType))
	})

	t.Run("non-pointer destination with missing property", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write no properties
		err := writer.Flush()
		require.NoError(t, err)

		// Try to read missing property with non-pointer destination
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var value uint32
		err = reader.ReadPropertyWithDefault(100, value, uint32(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a pointer")
	})

	t.Run("unsupported type with ReadPropertyWithDefault", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := NewBinaryWriter(buf)

		// Write a uint32 property
		err := writer.WriteProperty(100, uint32(42))
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		// Try to read into unsupported type
		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)

		var unsupported float64
		err = reader.ReadPropertyWithDefault(100, &unsupported, float64(0))
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidPropertyType))
	})
}

// TestBinaryReaderEndianness tests that multi-byte values are read in little-endian order.
func TestBinaryReaderEndianness(t *testing.T) {
	buf := &bytes.Buffer{}

	// Manually write a uint32 in little-endian: 0x12345678
	// Little-endian bytes: 0x78 0x56 0x34 0x12
	propCount := uint32(1)
	err := binary.Write(buf, binary.LittleEndian, propCount)
	require.NoError(t, err)

	propID := uint32(100)
	err = binary.Write(buf, binary.LittleEndian, propID)
	require.NoError(t, err)

	dataLen := uint64(4)
	err = binary.Write(buf, binary.LittleEndian, dataLen)
	require.NoError(t, err)

	value := uint32(0x12345678)
	err = binary.Write(buf, binary.LittleEndian, value)
	require.NoError(t, err)

	// Read using BinaryReader
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var readValue uint32
	err = reader.ReadProperty(100, &readValue)
	require.NoError(t, err)
	assert.Equal(t, uint32(0x12345678), readValue)
}

// TestBinaryReaderPropertyOrder tests that properties can be read in any order.
func TestBinaryReaderPropertyOrder(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write properties in order: 100, 101, 200
	err := writer.WriteProperty(100, uint32(1))
	require.NoError(t, err)
	err = writer.WriteProperty(101, uint32(2))
	require.NoError(t, err)
	err = writer.WriteProperty(200, uint32(3))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read properties in different order: 200, 100, 101
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var val200, val100, val101 uint32

	err = reader.ReadProperty(200, &val200)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), val200)

	err = reader.ReadProperty(100, &val100)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), val100)

	err = reader.ReadProperty(101, &val101)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), val101)
}

// TestBinaryReaderMultipleReads tests that the same property can be read multiple times.
func TestBinaryReaderMultipleReads(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewBinaryWriter(buf)

	// Write a property
	err := writer.WriteProperty(100, uint32(42))
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	// Read the same property multiple times
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var value1, value2, value3 uint32

	err = reader.ReadProperty(100, &value1)
	require.NoError(t, err)
	assert.Equal(t, uint32(42), value1)

	err = reader.ReadProperty(100, &value2)
	require.NoError(t, err)
	assert.Equal(t, uint32(42), value2)

	err = reader.ReadProperty(100, &value3)
	require.NoError(t, err)
	assert.Equal(t, uint32(42), value3)
}
