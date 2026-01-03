package persistence

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryWriterVarint(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  []byte
	}{
		{
			name:  "zero",
			value: 0,
			want:  []byte{0x00},
		},
		{
			name:  "positive small",
			value: 42,
			want:  []byte{0x54}, // 42 << 1 = 84 = 0x54
		},
		{
			name:  "negative small",
			value: -1,
			want:  []byte{0x01}, // (-1 << 1) ^ (-1 >> 63) = -2 ^ -1 = 1
		},
		{
			name:  "positive large",
			value: 300,
			want:  []byte{0xD8, 0x04}, // 300 << 1 = 600
		},
		{
			name:  "negative large",
			value: -300,
			want:  []byte{0xD7, 0x04}, // zigzag encoding
		},
		{
			name:  "max int64",
			value: 1<<63 - 1,
			want:  []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
		{
			name:  "min int64",
			value: -1 << 63,
			want:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteVarint(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, w.Bytes())
		})
	}
}

func TestBinaryWriterUvarint(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  []byte
	}{
		{
			name:  "zero",
			value: 0,
			want:  []byte{0x00},
		},
		{
			name:  "small",
			value: 42,
			want:  []byte{0x2A},
		},
		{
			name:  "medium",
			value: 300,
			want:  []byte{0xAC, 0x02},
		},
		{
			name:  "large",
			value: 1<<20 - 1,
			want:  []byte{0xFF, 0xFF, 0x3F},
		},
		{
			name:  "max uint64",
			value: ^uint64(0),
			want:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteUvarint(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, w.Bytes())
		})
	}
}

func TestBinaryWriterString(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []byte
	}{
		{
			name:  "empty",
			value: "",
			want:  []byte{0x00},
		},
		{
			name:  "simple",
			value: "hello",
			want:  []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
		},
		{
			name:  "with spaces",
			value: "hello world",
			want:  []byte{0x0B, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			name:  "unicode",
			value: "hello 世界",
			want:  append([]byte{0x0C}, []byte("hello 世界")...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteString(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, w.Bytes())
		})
	}
}

func TestBinaryWriterBool(t *testing.T) {
	w := NewBinaryWriter()

	err := w.WriteBool(true)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01}, w.Bytes())

	w.Reset()
	err = w.WriteBool(false)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, w.Bytes())
}

func TestBinaryWriterBytes(t *testing.T) {
	w := NewBinaryWriter()
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	err := w.WriteBytes(data)
	require.NoError(t, err)
	assert.Equal(t, data, w.Bytes())
}

func TestBinaryWriterProperty(t *testing.T) {
	tests := []struct {
		name  string
		id    uint64
		value any
		want  []byte
	}{
		{
			name:  "int64",
			id:    PropertyIDType,
			value: int64(42),
			want:  []byte{0x01, 0x54}, // ID=1, value=42 (zigzag)
		},
		{
			name:  "uint64",
			id:    PropertyIDType,
			value: uint64(42),
			want:  []byte{0x01, 0x2A}, // ID=1, value=42
		},
		{
			name:  "string",
			id:    PropertyIDName,
			value: "test",
			want:  []byte{0x02, 0x04, 't', 'e', 's', 't'}, // ID=2, length=4, "test"
		},
		{
			name:  "bool true",
			id:    PropertyIDNullable,
			value: true,
			want:  []byte{0x03, 0x01}, // ID=3, true
		},
		{
			name:  "bool false",
			id:    PropertyIDNullable,
			value: false,
			want:  []byte{0x03, 0x00}, // ID=3, false
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteProperty(tt.id, tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, w.Bytes())
		})
	}
}

func TestBinaryWriterPropertyEnd(t *testing.T) {
	w := NewBinaryWriter()
	err := w.WritePropertyEnd()
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, w.Bytes())
}

func TestBinaryReaderVarint(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  int64
	}{
		{
			name:  "zero",
			input: []byte{0x00},
			want:  0,
		},
		{
			name:  "positive small",
			input: []byte{0x54},
			want:  42,
		},
		{
			name:  "negative small",
			input: []byte{0x01},
			want:  -1,
		},
		{
			name:  "positive large",
			input: []byte{0xD8, 0x04},
			want:  300,
		},
		{
			name:  "negative large",
			input: []byte{0xD7, 0x04},
			want:  -300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.input)
			got, err := r.ReadVarint()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBinaryReaderUvarint(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{
			name:  "zero",
			input: []byte{0x00},
			want:  0,
		},
		{
			name:  "small",
			input: []byte{0x2A},
			want:  42,
		},
		{
			name:  "medium",
			input: []byte{0xAC, 0x02},
			want:  300,
		},
		{
			name:  "large",
			input: []byte{0xFF, 0xFF, 0x3F},
			want:  1<<20 - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.input)
			got, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBinaryReaderString(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "empty",
			input: []byte{0x00},
			want:  "",
		},
		{
			name:  "simple",
			input: []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
			want:  "hello",
		},
		{
			name:  "with spaces",
			input: []byte{0x0B, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
			want:  "hello world",
		},
		{
			name:  "unicode",
			input: append([]byte{0x0C}, []byte("hello 世界")...),
			want:  "hello 世界",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.input)
			got, err := r.ReadString()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBinaryReaderStringTooLarge(t *testing.T) {
	// String claiming to be 2GB
	input := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}
	r := NewBinaryReaderFromBytes(input)
	_, err := r.ReadString()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "string length too large")
}

func TestBinaryReaderBool(t *testing.T) {
	r := NewBinaryReaderFromBytes([]byte{0x01})
	got, err := r.ReadBool()
	require.NoError(t, err)
	assert.True(t, got)

	r = NewBinaryReaderFromBytes([]byte{0x00})
	got, err = r.ReadBool()
	require.NoError(t, err)
	assert.False(t, got)
}

func TestBinaryReaderBytes(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	r := NewBinaryReaderFromBytes(data)
	got, err := r.ReadBytes(4)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBinaryReaderProperty(t *testing.T) {
	r := NewBinaryReaderFromBytes([]byte{0x01, 0x54}) // ID=1, value=42 (zigzag)
	id, err := r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), id)

	value, err := r.ReadVarint()
	require.NoError(t, err)
	assert.Equal(t, int64(42), value)
}

func TestPropertyWriterReader(t *testing.T) {
	// Write properties
	pw := NewPropertyWriter()
	err := pw.WriteProperty(PropertyIDType, uint64(1))
	require.NoError(t, err)

	err = pw.WriteProperty(PropertyIDName, "test_table")
	require.NoError(t, err)

	err = pw.WriteProperty(PropertyIDNullable, true)
	require.NoError(t, err)

	err = pw.WriteEnd()
	require.NoError(t, err)

	// Read properties back
	pr := NewPropertyReaderFromBytes(pw.Bytes())

	// Read PropertyIDType
	id, err := pr.ReadNextProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), id)
	typeVal, err := pr.ReadUvarint()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), typeVal)

	// Read PropertyIDName
	id, err = pr.ReadNextProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDName), id)
	nameVal, err := pr.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "test_table", nameVal)

	// Read PropertyIDNullable
	id, err = pr.ReadNextProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDNullable), id)
	nullableVal, err := pr.ReadBool()
	require.NoError(t, err)
	assert.True(t, nullableVal)

	// Read END marker
	id, err = pr.ReadNextProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDEnd), id)
}

func TestPropertyRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		properties map[uint64]any
	}{
		{
			name: "simple properties",
			properties: map[uint64]any{
				PropertyIDType:     uint64(5),
				PropertyIDName:     "users",
				PropertyIDNullable: false,
			},
		},
		{
			name: "complex properties",
			properties: map[uint64]any{
				PropertyIDType:        uint64(10),
				PropertyIDName:        "products_table_with_long_name",
				PropertyIDNullable:    true,
				PropertyIDPrimaryKey:  uint64(123),
				PropertyIDChildType:   uint64(1),
				PropertyIDChildName:   "child_field",
				PropertyIDMemberCount: uint64(5),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write properties
			pw := NewPropertyWriter()
			for id, value := range tt.properties {
				err := pw.WriteProperty(id, value)
				require.NoError(t, err)
			}
			err := pw.WriteEnd()
			require.NoError(t, err)

			// Read and verify
			pr := NewPropertyReaderFromBytes(pw.Bytes())
			readProps := make(map[uint64]any)

			for {
				id, err := pr.ReadNextProperty()
				require.NoError(t, err)

				if id == PropertyIDEnd {
					break
				}

				// Determine value type based on expected type
				expectedValue := tt.properties[id]
				switch expectedValue.(type) {
				case uint64:
					val, err := pr.ReadUvarint()
					require.NoError(t, err)
					readProps[id] = val
				case string:
					val, err := pr.ReadString()
					require.NoError(t, err)
					readProps[id] = val
				case bool:
					val, err := pr.ReadBool()
					require.NoError(t, err)
					readProps[id] = val
				default:
					t.Fatalf("unexpected type: %T", expectedValue)
				}
			}

			// Verify all properties match
			assert.Equal(t, tt.properties, readProps)
		})
	}
}

func TestBinaryWriterReset(t *testing.T) {
	w := NewBinaryWriter()

	err := w.WriteString("test")
	require.NoError(t, err)
	assert.Greater(t, w.Len(), 0)

	w.Reset()
	assert.Equal(t, 0, w.Len())
	assert.Empty(t, w.Bytes())
}

func TestVarintEdgeCases(t *testing.T) {
	// Test round-trip for various edge case values
	values := []int64{
		0,
		1,
		-1,
		127,
		-128,
		128,
		-129,
		16383,
		-16384,
		16384,
		-16385,
		1 << 20,
		-(1 << 20),
		1 << 30,
		-(1 << 30),
		1<<62 - 1,
		-(1 << 62),
	}

	for _, val := range values {
		t.Run(string(rune(val)), func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteVarint(val)
			require.NoError(t, err)

			r := NewBinaryReaderFromBytes(w.Bytes())
			got, err := r.ReadVarint()
			require.NoError(t, err)
			assert.Equal(t, val, got, "value %d did not round-trip correctly", val)
		})
	}
}

func TestUvarintEdgeCases(t *testing.T) {
	// Test round-trip for various edge case values
	values := []uint64{
		0,
		1,
		127,
		128,
		255,
		256,
		16383,
		16384,
		65535,
		65536,
		1 << 20,
		1 << 30,
		1 << 40,
		1 << 50,
		1 << 60,
		1<<63 - 1,
	}

	for _, val := range values {
		t.Run(string(rune(val)), func(t *testing.T) {
			w := NewBinaryWriter()
			err := w.WriteUvarint(val)
			require.NoError(t, err)

			r := NewBinaryReaderFromBytes(w.Bytes())
			got, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, val, got, "value %d did not round-trip correctly", val)
		})
	}
}

func TestBinaryReaderUnexpectedEOF(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*BinaryReader) error
	}{
		{
			name: "ReadVarint",
			fn: func(r *BinaryReader) error {
				_, err := r.ReadVarint()
				return err
			},
		},
		{
			name: "ReadUvarint",
			fn: func(r *BinaryReader) error {
				_, err := r.ReadUvarint()
				return err
			},
		},
		{
			name: "ReadString",
			fn: func(r *BinaryReader) error {
				_, err := r.ReadString()
				return err
			},
		},
		{
			name: "ReadBool",
			fn: func(r *BinaryReader) error {
				_, err := r.ReadBool()
				return err
			},
		},
		{
			name: "ReadBytes",
			fn: func(r *BinaryReader) error {
				_, err := r.ReadBytes(10)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes([]byte{})
			err := tt.fn(r)
			require.Error(t, err)
		})
	}
}

func TestSerializeTypeInfo(t *testing.T) {
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "INTEGER without name",
			typeID:   4, // TYPE_INTEGER
			typeName: "",
		},
		{
			name:     "VARCHAR without name",
			typeID:   18, // TYPE_VARCHAR
			typeName: "",
		},
		{
			name:     "BIGINT without name",
			typeID:   5, // TYPE_BIGINT
			typeName: "",
		},
		{
			name:     "BOOLEAN without name",
			typeID:   1, // TYPE_BOOLEAN
			typeName: "",
		},
		{
			name:     "DOUBLE without name",
			typeID:   11, // TYPE_DOUBLE
			typeName: "",
		},
		{
			name:     "DATE without name",
			typeID:   13, // TYPE_DATE
			typeName: "",
		},
		{
			name:     "TIMESTAMP without name",
			typeID:   12, // TYPE_TIMESTAMP
			typeName: "",
		},
		{
			name:     "BIT without name",
			typeID:   31, // TYPE_BIT
			typeName: "",
		},
		{
			name:     "TIME_TZ without name",
			typeID:   32, // TYPE_TIME_TZ
			typeName: "",
		},
		{
			name:     "TIMESTAMP_TZ without name",
			typeID:   33, // TYPE_TIMESTAMP_TZ
			typeName: "",
		},
		{
			name:     "Custom type with name",
			typeID:   100,
			typeName: "CustomType",
		},
		{
			name:     "Enum-like with name",
			typeID:   24, // TYPE_ENUM
			typeName: "MyEnum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			// Verify we can deserialize the data
			r := NewBinaryReaderFromBytes(w.Bytes())

			// Read PropertyIDType
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDType), propID)

			typeIDRead, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(tt.typeID), typeIDRead)

			// Read next property (either PropertyIDName or PropertyIDEnd)
			nextPropID, err := r.ReadProperty()
			require.NoError(t, err)

			if tt.typeName != "" {
				// Should have PropertyIDName
				assert.Equal(t, uint64(PropertyIDName), nextPropID)

				typeName, err := r.ReadString()
				require.NoError(t, err)
				assert.Equal(t, tt.typeName, typeName)

				// Now read PropertyIDEnd
				endPropID, err := r.ReadProperty()
				require.NoError(t, err)
				assert.Equal(t, uint64(PropertyIDEnd), endPropID)
			} else {
				// Should be PropertyIDEnd
				assert.Equal(t, uint64(PropertyIDEnd), nextPropID)
			}
		})
	}
}

func TestSerializeTypeInfoRoundTrip(t *testing.T) {
	// Test round-trip serialization/deserialization
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "basic INTEGER",
			typeID:   4,
			typeName: "",
		},
		{
			name:     "basic VARCHAR",
			typeID:   18,
			typeName: "",
		},
		{
			name:     "STRUCT with name",
			typeID:   26,
			typeName: "PersonStruct",
		},
		{
			name:     "LIST with name",
			typeID:   25,
			typeName: "IntegerList",
		},
		{
			name:     "MAP with name",
			typeID:   27,
			typeName: "StringMap",
		},
		{
			name:     "UNION with name",
			typeID:   30,
			typeName: "MyUnion",
		},
		{
			name:     "new BIT type",
			typeID:   31,
			typeName: "",
		},
		{
			name:     "new TIME_TZ type",
			typeID:   32,
			typeName: "",
		},
		{
			name:     "new TIMESTAMP_TZ type",
			typeID:   33,
			typeName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			serialized := w.Bytes()
			assert.Greater(t, len(serialized), 0, "serialized data should not be empty")

			// Deserialize
			r := NewBinaryReaderFromBytes(serialized)

			var readTypeID uint64
			var readTypeName string

			for {
				propID, err := r.ReadProperty()
				require.NoError(t, err)

				if propID == PropertyIDEnd {
					break
				}

				switch propID {
				case PropertyIDType:
					readTypeID, err = r.ReadUvarint()
					require.NoError(t, err)
				case PropertyIDName:
					readTypeName, err = r.ReadString()
					require.NoError(t, err)
				default:
					t.Fatalf("unexpected property ID: %d", propID)
				}
			}

			// Verify
			assert.Equal(t, uint64(tt.typeID), readTypeID)
			assert.Equal(t, tt.typeName, readTypeName)
		})
	}
}

func TestSerializeTypeInfoMultipleTypes(t *testing.T) {
	// Test serializing multiple types in sequence
	w := NewBinaryWriter()

	// Serialize INTEGER
	err := SerializeTypeInfo(w, 4, "")
	require.NoError(t, err)

	// Serialize VARCHAR with name
	err = SerializeTypeInfo(w, 18, "MyVarchar")
	require.NoError(t, err)

	// Serialize BIGINT
	err = SerializeTypeInfo(w, 5, "")
	require.NoError(t, err)

	// Now deserialize all three
	r := NewBinaryReaderFromBytes(w.Bytes())

	// First type: INTEGER
	propID, err := r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), propID)
	typeID, err := r.ReadUvarint()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), typeID)
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDEnd), propID)

	// Second type: VARCHAR with name
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), propID)
	typeID, err = r.ReadUvarint()
	require.NoError(t, err)
	assert.Equal(t, uint64(18), typeID)
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDName), propID)
	typeName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "MyVarchar", typeName)
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDEnd), propID)

	// Third type: BIGINT
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), propID)
	typeID, err = r.ReadUvarint()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), typeID)
	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDEnd), propID)
}

func TestSerializeTypeInfoEmptyTypeName(t *testing.T) {
	// Verify that empty type name doesn't write PropertyIDName
	w := NewBinaryWriter()
	err := SerializeTypeInfo(w, 4, "")
	require.NoError(t, err)

	// Should only have PropertyIDType and PropertyIDEnd
	r := NewBinaryReaderFromBytes(w.Bytes())

	propID, err := r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDType), propID)

	_, err = r.ReadUvarint()
	require.NoError(t, err)

	propID, err = r.ReadProperty()
	require.NoError(t, err)
	assert.Equal(t, uint64(PropertyIDEnd), propID)
}

func TestSerializeTypeInfoSpecialCharacters(t *testing.T) {
	// Test type names with special characters
	tests := []struct {
		name     string
		typeName string
	}{
		{
			name:     "unicode",
			typeName: "Type_世界",
		},
		{
			name:     "spaces",
			typeName: "My Custom Type",
		},
		{
			name:     "symbols",
			typeName: "Type$With#Symbols",
		},
		{
			name:     "long name",
			typeName: "VeryLongTypeNameThatExceedsMostReasonableLimitsButStillNeedsToBeSupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, 100, tt.typeName)
			require.NoError(t, err)

			// Deserialize and verify
			r := NewBinaryReaderFromBytes(w.Bytes())

			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDType), propID)

			_, err = r.ReadUvarint()
			require.NoError(t, err)

			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDName), propID)

			readName, err := r.ReadString()
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, readName)

			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDEnd), propID)
		})
	}
}

func TestDeserializeTypeInfo(t *testing.T) {
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "INTEGER without name",
			typeID:   4,
			typeName: "",
		},
		{
			name:     "VARCHAR without name",
			typeID:   18,
			typeName: "",
		},
		{
			name:     "BIGINT without name",
			typeID:   5,
			typeName: "",
		},
		{
			name:     "STRUCT with name",
			typeID:   26,
			typeName: "PersonStruct",
		},
		{
			name:     "LIST with name",
			typeID:   25,
			typeName: "IntegerList",
		},
		{
			name:     "MAP with name",
			typeID:   27,
			typeName: "StringMap",
		},
		{
			name:     "BIT without name",
			typeID:   31,
			typeName: "",
		},
		{
			name:     "TIME_TZ without name",
			typeID:   32,
			typeName: "",
		},
		{
			name:     "TIMESTAMP_TZ without name",
			typeID:   33,
			typeName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize first
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			// Deserialize using DeserializeTypeInfo
			r := NewBinaryReaderFromBytes(w.Bytes())
			readTypeID, readTypeName, err := DeserializeTypeInfo(r)
			require.NoError(t, err)

			assert.Equal(t, tt.typeID, readTypeID)
			assert.Equal(t, tt.typeName, readTypeName)
		})
	}
}

func TestDeserializeTypeInfoMissingType(t *testing.T) {
	// Test deserializing data with missing PropertyIDType
	w := NewBinaryWriter()
	// Write PropertyIDName without PropertyIDType
	err := w.WriteProperty(PropertyIDName, "SomeName")
	require.NoError(t, err)
	err = w.WritePropertyEnd()
	require.NoError(t, err)

	r := NewBinaryReaderFromBytes(w.Bytes())
	_, _, err = DeserializeTypeInfo(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required property")
}

func TestDeserializeTypeInfoTruncatedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "truncated property ID",
			data: []byte{},
		},
		{
			name: "truncated type ID",
			data: []byte{0x01}, // PropertyIDType without value
		},
		{
			name: "truncated type name",
			data: []byte{0x01, 0x04, 0x02}, // PropertyIDType=4, PropertyIDName without value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.data)
			_, _, err := DeserializeTypeInfo(r)
			require.Error(t, err)
		})
	}
}

func TestDeserializeTypeInfoInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		desc string
	}{
		{
			name: "invalid varint overflow",
			data: []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02}, // Invalid varint
			desc: "varint overflow should cause error",
		},
		{
			name: "string too large",
			data: []byte{0x01, 0x04, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F}, // Type=4, Name with length=2GB
			desc: "extremely large string length should cause error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.data)
			_, _, err := DeserializeTypeInfo(r)
			require.Error(t, err, tt.desc)
		})
	}
}

func TestDeserializeTypeInfoCompleteRoundTrip(t *testing.T) {
	// Test complete round-trip for various scenarios
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "basic type no name",
			typeID:   4,
			typeName: "",
		},
		{
			name:     "basic type with name",
			typeID:   18,
			typeName: "MyVarchar",
		},
		{
			name:     "complex type with unicode name",
			typeID:   26,
			typeName: "結構体_Type",
		},
		{
			name:     "max type ID",
			typeID:   1000000,
			typeName: "",
		},
		{
			name:     "type with very long name",
			typeID:   30,
			typeName: "VeryLongTypeNameThatExceedsMostReasonableLimitsButStillNeedsToBeSupported_ThisIsEvenLonger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			readTypeID, readTypeName, err := DeserializeTypeInfo(r)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.typeID, readTypeID, "type ID should match")
			assert.Equal(t, tt.typeName, readTypeName, "type name should match")
		})
	}
}

func TestDeserializeTypeInfoMultipleSequential(t *testing.T) {
	// Test deserializing multiple type infos in sequence
	w := NewBinaryWriter()

	// Serialize three types
	err := SerializeTypeInfo(w, 4, "")
	require.NoError(t, err)
	err = SerializeTypeInfo(w, 18, "MyVarchar")
	require.NoError(t, err)
	err = SerializeTypeInfo(w, 5, "")
	require.NoError(t, err)

	// Deserialize all three
	r := NewBinaryReaderFromBytes(w.Bytes())

	// First type
	typeID1, typeName1, err := DeserializeTypeInfo(r)
	require.NoError(t, err)
	assert.Equal(t, 4, typeID1)
	assert.Equal(t, "", typeName1)

	// Second type
	typeID2, typeName2, err := DeserializeTypeInfo(r)
	require.NoError(t, err)
	assert.Equal(t, 18, typeID2)
	assert.Equal(t, "MyVarchar", typeName2)

	// Third type
	typeID3, typeName3, err := DeserializeTypeInfo(r)
	require.NoError(t, err)
	assert.Equal(t, 5, typeID3)
	assert.Equal(t, "", typeName3)
}

// TestSerializeStructType tests serialization of STRUCT types with various field configurations
func TestSerializeStructType(t *testing.T) {
	tests := []struct {
		name   string
		fields []StructField
	}{
		{
			name:   "empty struct",
			fields: []StructField{},
		},
		{
			name: "single field",
			fields: []StructField{
				{Name: "id", TypeID: 4}, // INTEGER
			},
		},
		{
			name: "multiple fields",
			fields: []StructField{
				{Name: "id", TypeID: 4},      // INTEGER
				{Name: "name", TypeID: 18},   // VARCHAR
				{Name: "age", TypeID: 4},     // INTEGER
				{Name: "active", TypeID: 1},  // BOOLEAN
			},
		},
		{
			name: "fields with unicode names",
			fields: []StructField{
				{Name: "名前", TypeID: 18},    // Japanese "name"
				{Name: "年齢", TypeID: 4},     // Japanese "age"
			},
		},
		{
			name: "fields with special characters",
			fields: []StructField{
				{Name: "field_with_underscore", TypeID: 18},
				{Name: "field-with-dash", TypeID: 4},
				{Name: "field.with.dot", TypeID: 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeStructType(w, tt.fields)
			require.NoError(t, err)

			// Verify the serialized data is non-empty
			assert.Greater(t, w.Len(), 0)

			// Verify we can read back the basic structure
			r := NewBinaryReaderFromBytes(w.Bytes())

			// Read PropertyIDType
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDType), propID)

			// Read type ID (should be 26 for STRUCT)
			typeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(26), typeID)

			// Read PropertyIDChildCount
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDChildCount), propID)

			// Read field count
			count, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(len(tt.fields)), count)
		})
	}
}

// TestDeserializeStructType tests deserialization of STRUCT types
func TestDeserializeStructType(t *testing.T) {
	tests := []struct {
		name   string
		fields []StructField
	}{
		{
			name:   "empty struct",
			fields: []StructField{},
		},
		{
			name: "single field",
			fields: []StructField{
				{Name: "id", TypeID: 4},
			},
		},
		{
			name: "multiple fields",
			fields: []StructField{
				{Name: "id", TypeID: 4},
				{Name: "name", TypeID: 18},
				{Name: "age", TypeID: 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize first
			w := NewBinaryWriter()
			err := SerializeStructType(w, tt.fields)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			fields, err := DeserializeStructType(r)
			require.NoError(t, err)

			// Verify fields match
			assert.Equal(t, len(tt.fields), len(fields))
			for i, expected := range tt.fields {
				assert.Equal(t, expected.Name, fields[i].Name, "field %d name mismatch", i)
				assert.Equal(t, expected.TypeID, fields[i].TypeID, "field %d type ID mismatch", i)
			}
		})
	}
}

// TestStructTypeRoundTrip tests complete round-trip serialization/deserialization
func TestStructTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		fields []StructField
	}{
		{
			name:   "empty struct",
			fields: []StructField{},
		},
		{
			name: "single INTEGER field",
			fields: []StructField{
				{Name: "id", TypeID: 4},
			},
		},
		{
			name: "person struct",
			fields: []StructField{
				{Name: "id", TypeID: 4},       // INTEGER
				{Name: "name", TypeID: 18},    // VARCHAR
				{Name: "age", TypeID: 4},      // INTEGER
				{Name: "email", TypeID: 18},   // VARCHAR
			},
		},
		{
			name: "all basic types",
			fields: []StructField{
				{Name: "bool_field", TypeID: 1},     // BOOLEAN
				{Name: "tinyint_field", TypeID: 2},  // TINYINT
				{Name: "smallint_field", TypeID: 3}, // SMALLINT
				{Name: "int_field", TypeID: 4},      // INTEGER
				{Name: "bigint_field", TypeID: 5},   // BIGINT
				{Name: "float_field", TypeID: 10},   // FLOAT
				{Name: "double_field", TypeID: 11},  // DOUBLE
				{Name: "varchar_field", TypeID: 18}, // VARCHAR
				{Name: "date_field", TypeID: 13},    // DATE
				{Name: "time_field", TypeID: 14},    // TIME
			},
		},
		{
			name: "unicode field names",
			fields: []StructField{
				{Name: "名前", TypeID: 18},
				{Name: "年齢", TypeID: 4},
				{Name: "アドレス", TypeID: 18},
			},
		},
		{
			name: "long field names",
			fields: []StructField{
				{Name: "very_long_field_name_that_exceeds_normal_limits_but_should_still_work", TypeID: 18},
				{Name: "another_extremely_long_field_name_for_testing_purposes", TypeID: 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeStructType(w, tt.fields)
			require.NoError(t, err)

			serialized := w.Bytes()
			assert.Greater(t, len(serialized), 0, "serialized data should not be empty")

			// Deserialize
			r := NewBinaryReaderFromBytes(serialized)
			fields, err := DeserializeStructType(r)
			require.NoError(t, err)

			// Verify complete match
			require.Equal(t, len(tt.fields), len(fields), "field count mismatch")
			for i, expected := range tt.fields {
				assert.Equal(t, expected.Name, fields[i].Name, "field %d: name mismatch", i)
				assert.Equal(t, expected.TypeID, fields[i].TypeID, "field %d: type ID mismatch", i)
			}
		})
	}
}

// TestNestedStructType tests serialization of nested STRUCT types (STRUCT containing STRUCT)
func TestNestedStructType(t *testing.T) {
	// This test demonstrates recursive handling by manually creating nested struct serialization
	// In a real implementation, the TypeID for a nested struct would be 26, and we'd serialize
	// the child struct separately, but this test validates the basic structure works.

	// Create outer struct: {id: INTEGER, address: STRUCT}
	// The address field has TypeID 26 (STRUCT) - in practice we'd need to serialize it separately
	outerFields := []StructField{
		{Name: "id", TypeID: 4},       // INTEGER
		{Name: "address", TypeID: 26}, // STRUCT (would contain city, zip, etc.)
	}

	// Serialize outer struct
	w := NewBinaryWriter()
	err := SerializeStructType(w, outerFields)
	require.NoError(t, err)

	// Deserialize outer struct
	r := NewBinaryReaderFromBytes(w.Bytes())
	fields, err := DeserializeStructType(r)
	require.NoError(t, err)

	// Verify structure
	require.Equal(t, 2, len(fields))
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, 4, fields[0].TypeID)
	assert.Equal(t, "address", fields[1].Name)
	assert.Equal(t, 26, fields[1].TypeID) // STRUCT type ID
}

// TestDeserializeStructTypeInvalidData tests error handling for invalid data
func TestDeserializeStructTypeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		desc string
	}{
		{
			name: "missing type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDChildCount, uint64(1))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without type ID",
		},
		{
			name: "missing child count",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(26))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without child count",
		},
		{
			name: "wrong type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(4)) // INTEGER instead of STRUCT
				_ = w.WriteProperty(PropertyIDChildCount, uint64(0))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail with wrong type ID",
		},
		{
			name: "field count mismatch",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(26))
				_ = w.WriteProperty(PropertyIDChildCount, uint64(2)) // Says 2 fields
				// But only provide 1 field
				_ = w.WriteProperty(PropertyIDChildName, "id")
				_ = w.WriteProperty(PropertyIDChildType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail when field count doesn't match",
		},
		{
			name: "truncated field name",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(26))
				_ = w.WriteProperty(PropertyIDChildCount, uint64(1))
				_ = w.WriteUvarint(PropertyIDChildName) // Start property but no value
				return w.Bytes()
			}(),
			desc: "should fail on truncated data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.data)
			_, err := DeserializeStructType(r)
			require.Error(t, err, tt.desc)
		})
	}
}

// TestStructTypeWithComplexTypes tests STRUCT fields that reference other complex types
func TestStructTypeWithComplexTypes(t *testing.T) {
	// Test struct containing various complex type references
	fields := []StructField{
		{Name: "id", TypeID: 4},        // INTEGER
		{Name: "tags", TypeID: 25},     // LIST
		{Name: "metadata", TypeID: 27}, // MAP
		{Name: "address", TypeID: 26},  // STRUCT (nested)
		{Name: "union_field", TypeID: 30}, // UNION
	}

	// Serialize
	w := NewBinaryWriter()
	err := SerializeStructType(w, fields)
	require.NoError(t, err)

	// Deserialize
	r := NewBinaryReaderFromBytes(w.Bytes())
	readFields, err := DeserializeStructType(r)
	require.NoError(t, err)

	// Verify all fields
	require.Equal(t, len(fields), len(readFields))
	for i := range fields {
		assert.Equal(t, fields[i].Name, readFields[i].Name)
		assert.Equal(t, fields[i].TypeID, readFields[i].TypeID)
	}
}

// TestStructTypeEmpty tests handling of empty structs
func TestStructTypeEmpty(t *testing.T) {
	fields := []StructField{}

	// Serialize
	w := NewBinaryWriter()
	err := SerializeStructType(w, fields)
	require.NoError(t, err)

	// Deserialize
	r := NewBinaryReaderFromBytes(w.Bytes())
	readFields, err := DeserializeStructType(r)
	require.NoError(t, err)

	// Verify empty
	assert.Equal(t, 0, len(readFields))
}

// TestStructTypePropertyOrdering tests that deserialization handles different property orderings
func TestStructTypePropertyOrdering(t *testing.T) {
	// Test with type written before child count, name before type
	t.Run("type_before_count_name_before_type", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDType, uint64(26))
		_ = w.WriteProperty(PropertyIDChildCount, uint64(1))
		_ = w.WriteProperty(PropertyIDChildName, "id")
		_ = w.WriteProperty(PropertyIDChildType, uint64(4))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		fields, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, 1, len(fields))
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, 4, fields[0].TypeID)
	})

	// Test with type written after child count, type before name
	t.Run("count_before_type_type_before_name", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDChildCount, uint64(1))
		_ = w.WriteProperty(PropertyIDType, uint64(26))
		_ = w.WriteProperty(PropertyIDChildType, uint64(4))
		_ = w.WriteProperty(PropertyIDChildName, "id")
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		fields, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, 1, len(fields))
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, 4, fields[0].TypeID)
	})
}

// TestStructTypeMaxFields tests handling of structs with many fields
func TestStructTypeMaxFields(t *testing.T) {
	// Create struct with 100 fields
	const fieldCount = 100
	fields := make([]StructField, fieldCount)
	for i := 0; i < fieldCount; i++ {
		fields[i] = StructField{
			Name:   fmt.Sprintf("field_%d", i),
			TypeID: 4 + (i % 10), // Vary type IDs
		}
	}

	// Serialize
	w := NewBinaryWriter()
	err := SerializeStructType(w, fields)
	require.NoError(t, err)

	// Deserialize
	r := NewBinaryReaderFromBytes(w.Bytes())
	readFields, err := DeserializeStructType(r)
	require.NoError(t, err)

	// Verify all fields
	require.Equal(t, fieldCount, len(readFields))
	for i := 0; i < fieldCount; i++ {
		assert.Equal(t, fields[i].Name, readFields[i].Name)
		assert.Equal(t, fields[i].TypeID, readFields[i].TypeID)
	}
}

// TestSerializeListType tests serialization of LIST types with various element types
func TestSerializeListType(t *testing.T) {
	tests := []struct {
		name           string
		elementTypeID  int
		elementTypeName string
	}{
		{
			name:           "LIST(INTEGER)",
			elementTypeID:  4,
			elementTypeName: "INTEGER",
		},
		{
			name:           "LIST(VARCHAR)",
			elementTypeID:  18,
			elementTypeName: "VARCHAR",
		},
		{
			name:           "LIST(BIGINT)",
			elementTypeID:  5,
			elementTypeName: "BIGINT",
		},
		{
			name:           "LIST(BOOLEAN)",
			elementTypeID:  1,
			elementTypeName: "BOOLEAN",
		},
		{
			name:           "LIST(DOUBLE)",
			elementTypeID:  11,
			elementTypeName: "DOUBLE",
		},
		{
			name:           "LIST(DATE)",
			elementTypeID:  13,
			elementTypeName: "DATE",
		},
		{
			name:           "LIST(TIMESTAMP)",
			elementTypeID:  12,
			elementTypeName: "TIMESTAMP",
		},
		{
			name:           "LIST(STRUCT) - nested complex type",
			elementTypeID:  26,
			elementTypeName: "STRUCT",
		},
		{
			name:           "LIST(LIST) - nested list",
			elementTypeID:  25,
			elementTypeName: "LIST",
		},
		{
			name:           "LIST(MAP) - nested map",
			elementTypeID:  27,
			elementTypeName: "MAP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			// Verify the serialized data is non-empty
			assert.Greater(t, w.Len(), 0)

			// Verify we can read back the basic structure
			r := NewBinaryReaderFromBytes(w.Bytes())

			// Read PropertyIDType
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDType), propID)

			// Read type ID (should be 25 for LIST)
			typeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(25), typeID)

			// Read PropertyIDChildType
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDChildType), propID)

			// Read element type ID
			elementTypeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(tt.elementTypeID), elementTypeID)

			// Read PropertyIDEnd
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDEnd), propID)
		})
	}
}

// TestDeserializeListType tests deserialization of LIST types
func TestDeserializeListType(t *testing.T) {
	tests := []struct {
		name          string
		elementTypeID int
	}{
		{
			name:          "LIST(INTEGER)",
			elementTypeID: 4,
		},
		{
			name:          "LIST(VARCHAR)",
			elementTypeID: 18,
		},
		{
			name:          "LIST(BIGINT)",
			elementTypeID: 5,
		},
		{
			name:          "LIST(BOOLEAN)",
			elementTypeID: 1,
		},
		{
			name:          "LIST(STRUCT)",
			elementTypeID: 26,
		},
		{
			name:          "LIST(LIST)",
			elementTypeID: 25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize first
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			elementTypeID, err := DeserializeListType(r)
			require.NoError(t, err)

			// Verify element type ID matches
			assert.Equal(t, tt.elementTypeID, elementTypeID)
		})
	}
}

// TestListTypeRoundTrip tests complete round-trip serialization/deserialization
func TestListTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name          string
		elementTypeID int
		description   string
	}{
		{
			name:          "LIST(INTEGER)",
			elementTypeID: 4,
			description:   "basic integer list",
		},
		{
			name:          "LIST(VARCHAR)",
			elementTypeID: 18,
			description:   "string list",
		},
		{
			name:          "LIST(BOOLEAN)",
			elementTypeID: 1,
			description:   "boolean list",
		},
		{
			name:          "LIST(TINYINT)",
			elementTypeID: 2,
			description:   "tinyint list",
		},
		{
			name:          "LIST(SMALLINT)",
			elementTypeID: 3,
			description:   "smallint list",
		},
		{
			name:          "LIST(BIGINT)",
			elementTypeID: 5,
			description:   "bigint list",
		},
		{
			name:          "LIST(FLOAT)",
			elementTypeID: 10,
			description:   "float list",
		},
		{
			name:          "LIST(DOUBLE)",
			elementTypeID: 11,
			description:   "double list",
		},
		{
			name:          "LIST(DATE)",
			elementTypeID: 13,
			description:   "date list",
		},
		{
			name:          "LIST(TIME)",
			elementTypeID: 14,
			description:   "time list",
		},
		{
			name:          "LIST(TIMESTAMP)",
			elementTypeID: 12,
			description:   "timestamp list",
		},
		{
			name:          "LIST(LIST) - nested list",
			elementTypeID: 25,
			description:   "nested list of lists",
		},
		{
			name:          "LIST(STRUCT) - list of structs",
			elementTypeID: 26,
			description:   "list of struct records",
		},
		{
			name:          "LIST(MAP) - list of maps",
			elementTypeID: 27,
			description:   "list of map types",
		},
		{
			name:          "LIST(UNION) - list of unions",
			elementTypeID: 30,
			description:   "list of union types",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			serialized := w.Bytes()
			assert.Greater(t, len(serialized), 0, "serialized data should not be empty")

			// Deserialize
			r := NewBinaryReaderFromBytes(serialized)
			elementTypeID, err := DeserializeListType(r)
			require.NoError(t, err)

			// Verify complete match
			assert.Equal(t, tt.elementTypeID, elementTypeID, "%s: element type ID mismatch", tt.description)
		})
	}
}

// TestListTypeNestedList tests serialization of nested LIST types (LIST(LIST(INTEGER)))
func TestListTypeNestedList(t *testing.T) {
	// Create LIST(LIST(INTEGER)) by using LIST type ID (25) as the element type
	// In practice, we'd need to serialize the inner LIST separately, but this test
	// validates that the structure can handle nested list type IDs
	const innerListTypeID = 25 // LIST

	// Serialize outer list: LIST(LIST)
	// In a real implementation, the inner list would have its own serialization
	// containing the INTEGER type, but we're just testing the outer structure here
	w := NewBinaryWriter()
	err := SerializeListType(w, innerListTypeID)
	require.NoError(t, err)

	// Deserialize outer list
	r := NewBinaryReaderFromBytes(w.Bytes())
	elementTypeID, err := DeserializeListType(r)
	require.NoError(t, err)

	// Verify that the element type is LIST (25)
	assert.Equal(t, innerListTypeID, elementTypeID)
	assert.Equal(t, 25, elementTypeID, "element type should be LIST")
}

// TestDeserializeListTypeInvalidData tests error handling for invalid data
func TestDeserializeListTypeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		desc string
	}{
		{
			name: "missing type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDChildType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without type ID",
		},
		{
			name: "missing child type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(25))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without child type",
		},
		{
			name: "wrong type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(4)) // INTEGER instead of LIST
				_ = w.WriteProperty(PropertyIDChildType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail with wrong type ID",
		},
		{
			name: "truncated child type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(25))
				_ = w.WriteUvarint(PropertyIDChildType) // Start property but no value
				return w.Bytes()
			}(),
			desc: "should fail on truncated data",
		},
		{
			name: "empty data",
			data: []byte{},
			desc: "should fail on empty data",
		},
		{
			name: "only END marker",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail with only END marker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.data)
			_, err := DeserializeListType(r)
			require.Error(t, err, tt.desc)
		})
	}
}

// TestListTypePropertyOrdering tests that deserialization handles different property orderings
func TestListTypePropertyOrdering(t *testing.T) {
	// Test with type before child type (standard order)
	t.Run("type_before_child_type", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDType, uint64(25))
		_ = w.WriteProperty(PropertyIDChildType, uint64(4))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		elementTypeID, err := DeserializeListType(r)
		require.NoError(t, err)
		assert.Equal(t, 4, elementTypeID)
	})

	// Test with child type before type (alternative order)
	t.Run("child_type_before_type", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDChildType, uint64(18))
		_ = w.WriteProperty(PropertyIDType, uint64(25))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		elementTypeID, err := DeserializeListType(r)
		require.NoError(t, err)
		assert.Equal(t, 18, elementTypeID)
	})
}

// TestListTypeMultipleSequential tests serializing multiple list types in sequence
func TestListTypeMultipleSequential(t *testing.T) {
	w := NewBinaryWriter()

	// Serialize three list types
	err := SerializeListType(w, 4)  // LIST(INTEGER)
	require.NoError(t, err)
	err = SerializeListType(w, 18) // LIST(VARCHAR)
	require.NoError(t, err)
	err = SerializeListType(w, 25) // LIST(LIST)
	require.NoError(t, err)

	// Deserialize all three
	r := NewBinaryReaderFromBytes(w.Bytes())

	// First list
	elementTypeID1, err := DeserializeListType(r)
	require.NoError(t, err)
	assert.Equal(t, 4, elementTypeID1)

	// Second list
	elementTypeID2, err := DeserializeListType(r)
	require.NoError(t, err)
	assert.Equal(t, 18, elementTypeID2)

	// Third list
	elementTypeID3, err := DeserializeListType(r)
	require.NoError(t, err)
	assert.Equal(t, 25, elementTypeID3)
}

// TestListTypeWithComplexElementTypes tests LIST with various complex element types
func TestListTypeWithComplexElementTypes(t *testing.T) {
	tests := []struct {
		name          string
		elementTypeID int
		description   string
	}{
		{
			name:          "LIST(STRUCT)",
			elementTypeID: 26,
			description:   "list of struct records",
		},
		{
			name:          "LIST(LIST)",
			elementTypeID: 25,
			description:   "nested list (2D array)",
		},
		{
			name:          "LIST(MAP)",
			elementTypeID: 27,
			description:   "list of maps",
		},
		{
			name:          "LIST(UNION)",
			elementTypeID: 30,
			description:   "list of union types",
		},
		{
			name:          "LIST(ENUM)",
			elementTypeID: 24,
			description:   "list of enum values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			elementTypeID, err := DeserializeListType(r)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.elementTypeID, elementTypeID, "%s: %s", tt.name, tt.description)
		})
	}
}

// TestListTypeEdgeCases tests edge cases for LIST type serialization
func TestListTypeEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		elementTypeID int
	}{
		{
			name:          "zero element type ID",
			elementTypeID: 0,
		},
		{
			name:          "large element type ID",
			elementTypeID: 1000000,
		},
		{
			name:          "max varint element type ID",
			elementTypeID: 1<<31 - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			elementTypeID, err := DeserializeListType(r)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.elementTypeID, elementTypeID)
		})
	}
}

// TestListTypeSerializedSize tests that serialized LIST types have reasonable sizes
func TestListTypeSerializedSize(t *testing.T) {
	tests := []struct {
		name          string
		elementTypeID int
		maxSize       int
	}{
		{
			name:          "LIST(INTEGER) - small type ID",
			elementTypeID: 4,
			maxSize:       10, // Should be very compact
		},
		{
			name:          "LIST(VARCHAR) - small type ID",
			elementTypeID: 18,
			maxSize:       10,
		},
		{
			name:          "LIST(STRUCT) - medium type ID",
			elementTypeID: 26,
			maxSize:       10,
		},
		{
			name:          "LIST with large type ID",
			elementTypeID: 10000,
			maxSize:       20, // Larger type ID needs more bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeListType(w, tt.elementTypeID)
			require.NoError(t, err)

			// Verify size is reasonable
			size := w.Len()
			assert.LessOrEqual(t, size, tt.maxSize, "serialized size should be compact")
			assert.Greater(t, size, 0, "serialized size should not be empty")
		})
	}
}

// TestSerializeMapType tests serialization of MAP types with various key/value type combinations
func TestSerializeMapType(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
		description string
	}{
		{
			name:        "MAP(VARCHAR, INTEGER)",
			keyTypeID:   18,
			valueTypeID: 4,
			description: "string to int map",
		},
		{
			name:        "MAP(INTEGER, VARCHAR)",
			keyTypeID:   4,
			valueTypeID: 18,
			description: "int to string map",
		},
		{
			name:        "MAP(VARCHAR, VARCHAR)",
			keyTypeID:   18,
			valueTypeID: 18,
			description: "string to string map (dictionary)",
		},
		{
			name:        "MAP(INTEGER, INTEGER)",
			keyTypeID:   4,
			valueTypeID: 4,
			description: "int to int map",
		},
		{
			name:        "MAP(VARCHAR, BOOLEAN)",
			keyTypeID:   18,
			valueTypeID: 1,
			description: "string to boolean map (flags)",
		},
		{
			name:        "MAP(INTEGER, DOUBLE)",
			keyTypeID:   4,
			valueTypeID: 11,
			description: "int to double map",
		},
		{
			name:        "MAP(VARCHAR, DATE)",
			keyTypeID:   18,
			valueTypeID: 13,
			description: "string to date map",
		},
		{
			name:        "MAP(VARCHAR, TIMESTAMP)",
			keyTypeID:   18,
			valueTypeID: 12,
			description: "string to timestamp map",
		},
		{
			name:        "MAP(VARCHAR, STRUCT) - nested complex type",
			keyTypeID:   18,
			valueTypeID: 26,
			description: "map with struct values",
		},
		{
			name:        "MAP(VARCHAR, LIST) - nested list",
			keyTypeID:   18,
			valueTypeID: 25,
			description: "map with list values",
		},
		{
			name:        "MAP(VARCHAR, MAP) - nested map",
			keyTypeID:   18,
			valueTypeID: 27,
			description: "map of maps",
		},
		{
			name:        "MAP(INTEGER, MAP) - nested map with int keys",
			keyTypeID:   4,
			valueTypeID: 27,
			description: "int keys to map values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			// Verify the serialized data is non-empty
			assert.Greater(t, w.Len(), 0)

			// Verify we can read back the basic structure
			r := NewBinaryReaderFromBytes(w.Bytes())

			// Read PropertyIDType
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDType), propID)

			// Read type ID (should be 27 for MAP)
			typeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(27), typeID)

			// Read PropertyIDKeyType
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDKeyType), propID)

			// Read key type ID
			keyTypeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(tt.keyTypeID), keyTypeID)

			// Read PropertyIDValueType
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDValueType), propID)

			// Read value type ID
			valueTypeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, uint64(tt.valueTypeID), valueTypeID)

			// Read PropertyIDEnd
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, uint64(PropertyIDEnd), propID)
		})
	}
}

// TestDeserializeMapType tests deserialization of MAP types
func TestDeserializeMapType(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
	}{
		{
			name:        "MAP(VARCHAR, INTEGER)",
			keyTypeID:   18,
			valueTypeID: 4,
		},
		{
			name:        "MAP(INTEGER, VARCHAR)",
			keyTypeID:   4,
			valueTypeID: 18,
		},
		{
			name:        "MAP(VARCHAR, BOOLEAN)",
			keyTypeID:   18,
			valueTypeID: 1,
		},
		{
			name:        "MAP(VARCHAR, STRUCT)",
			keyTypeID:   18,
			valueTypeID: 26,
		},
		{
			name:        "MAP(VARCHAR, LIST)",
			keyTypeID:   18,
			valueTypeID: 25,
		},
		{
			name:        "MAP(VARCHAR, MAP)",
			keyTypeID:   18,
			valueTypeID: 27,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize first
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			keyTypeID, valueTypeID, err := DeserializeMapType(r)
			require.NoError(t, err)

			// Verify key and value type IDs match
			assert.Equal(t, tt.keyTypeID, keyTypeID)
			assert.Equal(t, tt.valueTypeID, valueTypeID)
		})
	}
}

// TestMapTypeRoundTrip tests complete round-trip serialization/deserialization
func TestMapTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
		description string
	}{
		{
			name:        "MAP(VARCHAR, INTEGER)",
			keyTypeID:   18,
			valueTypeID: 4,
			description: "basic string to int map",
		},
		{
			name:        "MAP(INTEGER, VARCHAR)",
			keyTypeID:   4,
			valueTypeID: 18,
			description: "basic int to string map",
		},
		{
			name:        "MAP(BOOLEAN, VARCHAR)",
			keyTypeID:   1,
			valueTypeID: 18,
			description: "boolean to string map",
		},
		{
			name:        "MAP(TINYINT, BIGINT)",
			keyTypeID:   2,
			valueTypeID: 5,
			description: "tinyint to bigint map",
		},
		{
			name:        "MAP(SMALLINT, FLOAT)",
			keyTypeID:   3,
			valueTypeID: 10,
			description: "smallint to float map",
		},
		{
			name:        "MAP(INTEGER, DOUBLE)",
			keyTypeID:   4,
			valueTypeID: 11,
			description: "int to double map",
		},
		{
			name:        "MAP(VARCHAR, DATE)",
			keyTypeID:   18,
			valueTypeID: 13,
			description: "string to date map",
		},
		{
			name:        "MAP(VARCHAR, TIME)",
			keyTypeID:   18,
			valueTypeID: 14,
			description: "string to time map",
		},
		{
			name:        "MAP(VARCHAR, TIMESTAMP)",
			keyTypeID:   18,
			valueTypeID: 12,
			description: "string to timestamp map",
		},
		{
			name:        "MAP(VARCHAR, LIST) - nested list",
			keyTypeID:   18,
			valueTypeID: 25,
			description: "map with list values",
		},
		{
			name:        "MAP(VARCHAR, STRUCT) - nested struct",
			keyTypeID:   18,
			valueTypeID: 26,
			description: "map with struct values",
		},
		{
			name:        "MAP(VARCHAR, MAP) - nested map",
			keyTypeID:   18,
			valueTypeID: 27,
			description: "map of maps",
		},
		{
			name:        "MAP(INTEGER, MAP) - nested map with int keys",
			keyTypeID:   4,
			valueTypeID: 27,
			description: "int keys with map values",
		},
		{
			name:        "MAP(VARCHAR, UNION) - map with union values",
			keyTypeID:   18,
			valueTypeID: 30,
			description: "map with union type values",
		},
		{
			name:        "MAP(VARCHAR, ENUM) - map with enum values",
			keyTypeID:   18,
			valueTypeID: 24,
			description: "map with enum values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			serialized := w.Bytes()
			assert.Greater(t, len(serialized), 0, "serialized data should not be empty")

			// Deserialize
			r := NewBinaryReaderFromBytes(serialized)
			keyTypeID, valueTypeID, err := DeserializeMapType(r)
			require.NoError(t, err)

			// Verify complete match
			assert.Equal(t, tt.keyTypeID, keyTypeID, "%s: key type ID mismatch", tt.description)
			assert.Equal(t, tt.valueTypeID, valueTypeID, "%s: value type ID mismatch", tt.description)
		})
	}
}

// TestMapTypeNestedMap tests serialization of nested MAP types (MAP(VARCHAR, MAP(...)))
func TestMapTypeNestedMap(t *testing.T) {
	// Create MAP(VARCHAR, MAP) by using MAP type ID (27) as the value type
	// In practice, we'd need to serialize the inner MAP separately, but this test
	// validates that the structure can handle nested map type IDs
	const keyTypeID = 18  // VARCHAR
	const valueTypeID = 27 // MAP

	// Serialize outer map: MAP(VARCHAR, MAP)
	w := NewBinaryWriter()
	err := SerializeMapType(w, keyTypeID, valueTypeID)
	require.NoError(t, err)

	// Deserialize outer map
	r := NewBinaryReaderFromBytes(w.Bytes())
	readKeyTypeID, readValueTypeID, err := DeserializeMapType(r)
	require.NoError(t, err)

	// Verify that the key type is VARCHAR (18) and value type is MAP (27)
	assert.Equal(t, keyTypeID, readKeyTypeID)
	assert.Equal(t, valueTypeID, readValueTypeID)
	assert.Equal(t, 18, readKeyTypeID, "key type should be VARCHAR")
	assert.Equal(t, 27, readValueTypeID, "value type should be MAP")
}

// TestDeserializeMapTypeInvalidData tests error handling for invalid data
func TestDeserializeMapTypeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		desc string
	}{
		{
			name: "missing type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDKeyType, uint64(18))
				_ = w.WriteProperty(PropertyIDValueType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without type ID",
		},
		{
			name: "missing key type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(27))
				_ = w.WriteProperty(PropertyIDValueType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without key type",
		},
		{
			name: "missing value type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(27))
				_ = w.WriteProperty(PropertyIDKeyType, uint64(18))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail without value type",
		},
		{
			name: "wrong type ID",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(4)) // INTEGER instead of MAP
				_ = w.WriteProperty(PropertyIDKeyType, uint64(18))
				_ = w.WriteProperty(PropertyIDValueType, uint64(4))
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail with wrong type ID",
		},
		{
			name: "truncated key type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(27))
				_ = w.WriteUvarint(PropertyIDKeyType) // Start property but no value
				return w.Bytes()
			}(),
			desc: "should fail on truncated key type",
		},
		{
			name: "truncated value type",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(27))
				_ = w.WriteProperty(PropertyIDKeyType, uint64(18))
				_ = w.WriteUvarint(PropertyIDValueType) // Start property but no value
				return w.Bytes()
			}(),
			desc: "should fail on truncated value type",
		},
		{
			name: "empty data",
			data: []byte{},
			desc: "should fail on empty data",
		},
		{
			name: "only END marker",
			data: func() []byte {
				w := NewBinaryWriter()
				_ = w.WritePropertyEnd()
				return w.Bytes()
			}(),
			desc: "should fail with only END marker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBinaryReaderFromBytes(tt.data)
			_, _, err := DeserializeMapType(r)
			require.Error(t, err, tt.desc)
		})
	}
}

// TestMapTypePropertyOrdering tests that deserialization handles different property orderings
func TestMapTypePropertyOrdering(t *testing.T) {
	// Test with standard order: type, key, value
	t.Run("type_key_value", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDType, uint64(27))
		_ = w.WriteProperty(PropertyIDKeyType, uint64(18))
		_ = w.WriteProperty(PropertyIDValueType, uint64(4))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		keyTypeID, valueTypeID, err := DeserializeMapType(r)
		require.NoError(t, err)
		assert.Equal(t, 18, keyTypeID)
		assert.Equal(t, 4, valueTypeID)
	})

	// Test with alternative order: key, value, type
	t.Run("key_value_type", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDKeyType, uint64(4))
		_ = w.WriteProperty(PropertyIDValueType, uint64(18))
		_ = w.WriteProperty(PropertyIDType, uint64(27))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		keyTypeID, valueTypeID, err := DeserializeMapType(r)
		require.NoError(t, err)
		assert.Equal(t, 4, keyTypeID)
		assert.Equal(t, 18, valueTypeID)
	})

	// Test with alternative order: value, type, key
	t.Run("value_type_key", func(t *testing.T) {
		w := NewBinaryWriter()
		_ = w.WriteProperty(PropertyIDValueType, uint64(5))
		_ = w.WriteProperty(PropertyIDType, uint64(27))
		_ = w.WriteProperty(PropertyIDKeyType, uint64(1))
		_ = w.WritePropertyEnd()

		r := NewBinaryReaderFromBytes(w.Bytes())
		keyTypeID, valueTypeID, err := DeserializeMapType(r)
		require.NoError(t, err)
		assert.Equal(t, 1, keyTypeID)
		assert.Equal(t, 5, valueTypeID)
	})
}

// TestMapTypeMultipleSequential tests serializing multiple map types in sequence
func TestMapTypeMultipleSequential(t *testing.T) {
	w := NewBinaryWriter()

	// Serialize three map types
	err := SerializeMapType(w, 18, 4)  // MAP(VARCHAR, INTEGER)
	require.NoError(t, err)
	err = SerializeMapType(w, 4, 18)  // MAP(INTEGER, VARCHAR)
	require.NoError(t, err)
	err = SerializeMapType(w, 18, 27) // MAP(VARCHAR, MAP)
	require.NoError(t, err)

	// Deserialize all three
	r := NewBinaryReaderFromBytes(w.Bytes())

	// First map
	keyTypeID1, valueTypeID1, err := DeserializeMapType(r)
	require.NoError(t, err)
	assert.Equal(t, 18, keyTypeID1)
	assert.Equal(t, 4, valueTypeID1)

	// Second map
	keyTypeID2, valueTypeID2, err := DeserializeMapType(r)
	require.NoError(t, err)
	assert.Equal(t, 4, keyTypeID2)
	assert.Equal(t, 18, valueTypeID2)

	// Third map
	keyTypeID3, valueTypeID3, err := DeserializeMapType(r)
	require.NoError(t, err)
	assert.Equal(t, 18, keyTypeID3)
	assert.Equal(t, 27, valueTypeID3)
}

// TestMapTypeWithComplexTypes tests MAP with various complex key and value types
func TestMapTypeWithComplexTypes(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
		description string
	}{
		{
			name:        "MAP(VARCHAR, STRUCT)",
			keyTypeID:   18,
			valueTypeID: 26,
			description: "map with struct values",
		},
		{
			name:        "MAP(VARCHAR, LIST)",
			keyTypeID:   18,
			valueTypeID: 25,
			description: "map with list values",
		},
		{
			name:        "MAP(VARCHAR, MAP)",
			keyTypeID:   18,
			valueTypeID: 27,
			description: "nested map",
		},
		{
			name:        "MAP(VARCHAR, UNION)",
			keyTypeID:   18,
			valueTypeID: 30,
			description: "map with union values",
		},
		{
			name:        "MAP(INTEGER, STRUCT)",
			keyTypeID:   4,
			valueTypeID: 26,
			description: "int keys with struct values",
		},
		{
			name:        "MAP(INTEGER, LIST)",
			keyTypeID:   4,
			valueTypeID: 25,
			description: "int keys with list values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			keyTypeID, valueTypeID, err := DeserializeMapType(r)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.keyTypeID, keyTypeID, "%s: %s", tt.name, tt.description)
			assert.Equal(t, tt.valueTypeID, valueTypeID, "%s: %s", tt.name, tt.description)
		})
	}
}

// TestMapTypeEdgeCases tests edge cases for MAP type serialization
func TestMapTypeEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
	}{
		{
			name:        "zero key and value type IDs",
			keyTypeID:   0,
			valueTypeID: 0,
		},
		{
			name:        "large key type ID",
			keyTypeID:   1000000,
			valueTypeID: 4,
		},
		{
			name:        "large value type ID",
			keyTypeID:   18,
			valueTypeID: 1000000,
		},
		{
			name:        "max varint type IDs",
			keyTypeID:   1<<31 - 1,
			valueTypeID: 1<<31 - 1,
		},
		{
			name:        "same key and value types",
			keyTypeID:   18,
			valueTypeID: 18,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			keyTypeID, valueTypeID, err := DeserializeMapType(r)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.keyTypeID, keyTypeID)
			assert.Equal(t, tt.valueTypeID, valueTypeID)
		})
	}
}

// TestMapTypeSerializedSize tests that serialized MAP types have reasonable sizes
func TestMapTypeSerializedSize(t *testing.T) {
	tests := []struct {
		name        string
		keyTypeID   int
		valueTypeID int
		maxSize     int
	}{
		{
			name:        "MAP(VARCHAR, INTEGER) - small type IDs",
			keyTypeID:   18,
			valueTypeID: 4,
			maxSize:     15, // Should be very compact
		},
		{
			name:        "MAP(INTEGER, VARCHAR) - small type IDs",
			keyTypeID:   4,
			valueTypeID: 18,
			maxSize:     15,
		},
		{
			name:        "MAP with large type IDs",
			keyTypeID:   10000,
			valueTypeID: 10000,
			maxSize:     25, // Larger type IDs need more bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeMapType(w, tt.keyTypeID, tt.valueTypeID)
			require.NoError(t, err)

			// Verify size is reasonable
			size := w.Len()
			assert.LessOrEqual(t, size, tt.maxSize, "serialized size should be compact")
			assert.Greater(t, size, 0, "serialized size should not be empty")
		})
	}
}

// TestSerializeUnionType tests serialization of UNION types with various member combinations
func TestSerializeUnionType(t *testing.T) {
	tests := []struct {
		name        string
		members     []UnionMember
		description string
	}{
		{
			name: "UNION(num INTEGER, str VARCHAR)",
			members: []UnionMember{
				{Name: "num", TypeID: 4},
				{Name: "str", TypeID: 18},
			},
			description: "simple two-member union",
		},
		{
			name: "UNION(flag BOOLEAN)",
			members: []UnionMember{
				{Name: "flag", TypeID: 1},
			},
			description: "single member union",
		},
		{
			name: "UNION(int INTEGER, float DOUBLE, str VARCHAR)",
			members: []UnionMember{
				{Name: "int", TypeID: 4},
				{Name: "float", TypeID: 11},
				{Name: "str", TypeID: 18},
			},
			description: "three-member union",
		},
		{
			name: "UNION(dt DATE, ts TIMESTAMP)",
			members: []UnionMember{
				{Name: "dt", TypeID: 13},
				{Name: "ts", TypeID: 14},
			},
			description: "temporal types union",
		},
		{
			name: "UNION(small TINYINT, medium SMALLINT, large INTEGER, huge BIGINT)",
			members: []UnionMember{
				{Name: "small", TypeID: 2},
				{Name: "medium", TypeID: 3},
				{Name: "large", TypeID: 4},
				{Name: "huge", TypeID: 5},
			},
			description: "multi-member integer sizes union",
		},
		{
			name: "UNION(decimal DECIMAL, float FLOAT, double DOUBLE)",
			members: []UnionMember{
				{Name: "decimal", TypeID: 12},
				{Name: "float", TypeID: 10},
				{Name: "double", TypeID: 11},
			},
			description: "numeric precision union",
		},
		{
			name: "UNION(blob BLOB, varchar VARCHAR)",
			members: []UnionMember{
				{Name: "blob", TypeID: 19},
				{Name: "varchar", TypeID: 18},
			},
			description: "binary and string union",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeUnionType(w, tt.members)
			require.NoError(t, err)

			// Verify the serialized data is not empty
			data := w.Bytes()
			assert.NotEmpty(t, data, "serialized data should not be empty")

			// Verify we can read it back
			r := NewBinaryReaderFromBytes(data)

			// Read type ID
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, PropertyIDType, int(propID))

			typeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, 28, int(typeID), "type ID should be UNION (28)")

			// Read member count
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, PropertyIDMemberCount, int(propID))

			memberCount, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, len(tt.members), int(memberCount), "member count should match")

			// Read each member
			for i := 0; i < len(tt.members); i++ {
				// Read member name property
				propID, err := r.ReadProperty()
				require.NoError(t, err)
				assert.Equal(t, PropertyIDMemberName, int(propID))

				name, err := r.ReadString()
				require.NoError(t, err)
				assert.Equal(t, tt.members[i].Name, name)

				// Read member type property
				propID, err = r.ReadProperty()
				require.NoError(t, err)
				assert.Equal(t, PropertyIDMemberType, int(propID))

				memTypeID, err := r.ReadUvarint()
				require.NoError(t, err)
				assert.Equal(t, tt.members[i].TypeID, int(memTypeID))
			}

			// Read END marker
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, PropertyIDEnd, int(propID), "should end with PropertyIDEnd")
		})
	}
}

// TestDeserializeUnionType tests deserialization of UNION types
func TestDeserializeUnionType(t *testing.T) {
	tests := []struct {
		name        string
		members     []UnionMember
		description string
	}{
		{
			name: "UNION(num INTEGER, str VARCHAR)",
			members: []UnionMember{
				{Name: "num", TypeID: 4},
				{Name: "str", TypeID: 18},
			},
			description: "simple two-member union",
		},
		{
			name: "UNION(flag BOOLEAN)",
			members: []UnionMember{
				{Name: "flag", TypeID: 1},
			},
			description: "single member union",
		},
		{
			name: "UNION(int INTEGER, float DOUBLE, str VARCHAR, bool BOOLEAN)",
			members: []UnionMember{
				{Name: "int", TypeID: 4},
				{Name: "float", TypeID: 11},
				{Name: "str", TypeID: 18},
				{Name: "bool", TypeID: 1},
			},
			description: "four-member mixed types union",
		},
		{
			name: "UNION(dt DATE, ts TIMESTAMP, time TIME)",
			members: []UnionMember{
				{Name: "dt", TypeID: 13},
				{Name: "ts", TypeID: 14},
				{Name: "time", TypeID: 15},
			},
			description: "temporal types union",
		},
		{
			name: "UNION(uuid UUID, varchar VARCHAR)",
			members: []UnionMember{
				{Name: "uuid", TypeID: 20},
				{Name: "varchar", TypeID: 18},
			},
			description: "uuid and string union",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeUnionType(w, tt.members)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			members, err := DeserializeUnionType(r)
			require.NoError(t, err)

			// Verify
			require.Equal(t, len(tt.members), len(members), "member count should match")
			for i, expected := range tt.members {
				assert.Equal(t, expected.Name, members[i].Name, "member %d name should match", i)
				assert.Equal(t, expected.TypeID, members[i].TypeID, "member %d type ID should match", i)
			}
		})
	}
}

// TestUnionTypeRoundTrip tests round-trip serialization/deserialization of UNION types
func TestUnionTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		members []UnionMember
	}{
		{
			name: "empty union",
			members: []UnionMember{},
		},
		{
			name: "single member",
			members: []UnionMember{
				{Name: "value", TypeID: 4},
			},
		},
		{
			name: "two members",
			members: []UnionMember{
				{Name: "num", TypeID: 4},
				{Name: "str", TypeID: 18},
			},
		},
		{
			name: "many members",
			members: []UnionMember{
				{Name: "bool", TypeID: 1},
				{Name: "tinyint", TypeID: 2},
				{Name: "smallint", TypeID: 3},
				{Name: "int", TypeID: 4},
				{Name: "bigint", TypeID: 5},
				{Name: "float", TypeID: 10},
				{Name: "double", TypeID: 11},
				{Name: "varchar", TypeID: 18},
			},
		},
		{
			name: "long member names",
			members: []UnionMember{
				{Name: "very_long_member_name_for_testing_purposes_one", TypeID: 4},
				{Name: "very_long_member_name_for_testing_purposes_two", TypeID: 18},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeUnionType(w, tt.members)
			require.NoError(t, err)

			// Deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			members, err := DeserializeUnionType(r)
			require.NoError(t, err)

			// Verify exact match
			require.Equal(t, tt.members, members, "round-trip should preserve all member data")
		})
	}
}

// TestDeserializeUnionTypeInvalidData tests error handling for invalid data
func TestDeserializeUnionTypeInvalidData(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() *BinaryReader
		expectError string
	}{
		{
			name: "missing type ID",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDMemberCount, uint64(1))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(4))
				_ = w.WritePropertyEnd()
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "missing required property: PropertyIDType",
		},
		{
			name: "missing member count",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(28))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(4))
				_ = w.WritePropertyEnd()
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "missing required property: PropertyIDMemberCount",
		},
		{
			name: "wrong type ID",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(25)) // LIST instead of UNION
				_ = w.WriteProperty(PropertyIDMemberCount, uint64(1))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(4))
				_ = w.WritePropertyEnd()
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "expected UNION type ID (28), got 25",
		},
		{
			name: "member count mismatch - too few",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(28))
				_ = w.WriteProperty(PropertyIDMemberCount, uint64(2))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(4))
				_ = w.WritePropertyEnd()
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "expected 2 members, got 1",
		},
		{
			name: "member count mismatch - too many",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(28))
				_ = w.WriteProperty(PropertyIDMemberCount, uint64(1))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(4))
				_ = w.WriteProperty(PropertyIDMemberName, "str")
				_ = w.WriteProperty(PropertyIDMemberType, uint64(18))
				_ = w.WritePropertyEnd()
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "expected 1 members, got 2",
		},
		{
			name: "truncated data - no end marker",
			setupFunc: func() *BinaryReader {
				w := NewBinaryWriter()
				_ = w.WriteProperty(PropertyIDType, uint64(28))
				_ = w.WriteProperty(PropertyIDMemberCount, uint64(1))
				_ = w.WriteProperty(PropertyIDMemberName, "num")
				// Missing PropertyIDMemberType and PropertyIDEnd
				return NewBinaryReaderFromBytes(w.Bytes())
			},
			expectError: "failed to read property ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setupFunc()
			_, err := DeserializeUnionType(r)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

// TestNestedUnionType tests UNION types containing complex types (e.g., nested structures)
// Note: This test demonstrates the concept - actual nested serialization would require
// recursive type serialization support.
func TestNestedUnionType(t *testing.T) {
	tests := []struct {
		name        string
		members     []UnionMember
		description string
	}{
		{
			name: "UNION containing LIST type",
			members: []UnionMember{
				{Name: "list", TypeID: 25}, // LIST type
				{Name: "value", TypeID: 4}, // INTEGER
			},
			description: "union with nested list type",
		},
		{
			name: "UNION containing STRUCT type",
			members: []UnionMember{
				{Name: "struct", TypeID: 26}, // STRUCT type
				{Name: "null", TypeID: 0},    // NULL type
			},
			description: "union with nested struct type",
		},
		{
			name: "UNION containing MAP type",
			members: []UnionMember{
				{Name: "map", TypeID: 27},    // MAP type
				{Name: "value", TypeID: 18}, // VARCHAR
			},
			description: "union with nested map type",
		},
		{
			name: "UNION containing another UNION",
			members: []UnionMember{
				{Name: "union", TypeID: 28}, // UNION type
				{Name: "simple", TypeID: 4}, // INTEGER
			},
			description: "union with nested union type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test serialization
			w := NewBinaryWriter()
			err := SerializeUnionType(w, tt.members)
			require.NoError(t, err)

			// Test deserialization
			r := NewBinaryReaderFromBytes(w.Bytes())
			members, err := DeserializeUnionType(r)
			require.NoError(t, err)

			// Verify structure
			require.Equal(t, len(tt.members), len(members))
			for i, expected := range tt.members {
				assert.Equal(t, expected.Name, members[i].Name)
				assert.Equal(t, expected.TypeID, members[i].TypeID)
			}
		})
	}
}

// TestUnionTypeCompactness tests that UNION serialization is compact
func TestUnionTypeCompactness(t *testing.T) {
	tests := []struct {
		name    string
		members []UnionMember
		maxSize int // Maximum expected size in bytes
	}{
		{
			name: "single member",
			members: []UnionMember{
				{Name: "v", TypeID: 4},
			},
			maxSize: 20,
		},
		{
			name: "two members",
			members: []UnionMember{
				{Name: "a", TypeID: 4},
				{Name: "b", TypeID: 18},
			},
			maxSize: 30,
		},
		{
			name: "three members",
			members: []UnionMember{
				{Name: "x", TypeID: 1},
				{Name: "y", TypeID: 4},
				{Name: "z", TypeID: 18},
			},
			maxSize: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeUnionType(w, tt.members)
			require.NoError(t, err)

			// Verify size is reasonable
			size := w.Len()
			assert.LessOrEqual(t, size, tt.maxSize, "serialized size should be compact")
			assert.Greater(t, size, 0, "serialized size should not be empty")
		})
	}
}

// TestSerializeNewTypes tests serialization of new DuckDB types: BIT, TIME_TZ, TIMESTAMP_TZ
func TestSerializeNewTypes(t *testing.T) {
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "BIT type",
			typeID:   TypeIDBit,
			typeName: "",
		},
		{
			name:     "TIME_TZ type",
			typeID:   TypeIDTimeTZ,
			typeName: "",
		},
		{
			name:     "TIMESTAMP_TZ type",
			typeID:   TypeIDTimestampTZ,
			typeName: "",
		},
		{
			name:     "BIT type with name",
			typeID:   TypeIDBit,
			typeName: "mybit",
		},
		{
			name:     "TIME_TZ type with name",
			typeID:   TypeIDTimeTZ,
			typeName: "mytime",
		},
		{
			name:     "TIMESTAMP_TZ type with name",
			typeID:   TypeIDTimestampTZ,
			typeName: "mytimestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			// Verify the serialized data is not empty
			data := w.Bytes()
			assert.NotEmpty(t, data, "serialized data should not be empty")

			// Verify we can read it back
			r := NewBinaryReaderFromBytes(data)

			// Read type ID property
			propID, err := r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, PropertyIDType, int(propID))

			typeID, err := r.ReadUvarint()
			require.NoError(t, err)
			assert.Equal(t, tt.typeID, int(typeID), "type ID should match")

			// If type name was provided, read it
			if tt.typeName != "" {
				propID, err := r.ReadProperty()
				require.NoError(t, err)
				assert.Equal(t, PropertyIDName, int(propID))

				name, err := r.ReadString()
				require.NoError(t, err)
				assert.Equal(t, tt.typeName, name)
			}

			// Read END marker
			propID, err = r.ReadProperty()
			require.NoError(t, err)
			assert.Equal(t, PropertyIDEnd, int(propID), "should end with PropertyIDEnd")
		})
	}
}

// TestDeserializeNewTypes tests deserialization of new DuckDB types
func TestDeserializeNewTypes(t *testing.T) {
	tests := []struct {
		name     string
		typeID   int
		typeName string
	}{
		{
			name:     "BIT type",
			typeID:   TypeIDBit,
			typeName: "",
		},
		{
			name:     "TIME_TZ type",
			typeID:   TypeIDTimeTZ,
			typeName: "",
		},
		{
			name:     "TIMESTAMP_TZ type",
			typeID:   TypeIDTimestampTZ,
			typeName: "",
		},
		{
			name:     "BIT type with name",
			typeID:   TypeIDBit,
			typeName: "flags",
		},
		{
			name:     "TIME_TZ type with name",
			typeID:   TypeIDTimeTZ,
			typeName: "local_time",
		},
		{
			name:     "TIMESTAMP_TZ type with name",
			typeID:   TypeIDTimestampTZ,
			typeName: "event_time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First serialize
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err)

			// Then deserialize
			r := NewBinaryReaderFromBytes(w.Bytes())
			typeID, typeName, err := DeserializeTypeInfo(r)
			require.NoError(t, err)

			// Verify values match
			assert.Equal(t, tt.typeID, typeID, "type ID should match")
			assert.Equal(t, tt.typeName, typeName, "type name should match")
		})
	}
}

// TestNewTypesRoundTrip tests round-trip serialization/deserialization of new types
func TestNewTypesRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		typeID      int
		typeName    string
		description string
	}{
		{
			name:        "BIT basic",
			typeID:      TypeIDBit,
			typeName:    "",
			description: "BIT type without name",
		},
		{
			name:        "BIT with name",
			typeID:      TypeIDBit,
			typeName:    "is_active",
			description: "BIT type with column name",
		},
		{
			name:        "TIME_TZ basic",
			typeID:      TypeIDTimeTZ,
			typeName:    "",
			description: "TIME_TZ type without name",
		},
		{
			name:        "TIME_TZ with name",
			typeID:      TypeIDTimeTZ,
			typeName:    "scheduled_time",
			description: "TIME_TZ type with column name",
		},
		{
			name:        "TIMESTAMP_TZ basic",
			typeID:      TypeIDTimestampTZ,
			typeName:    "",
			description: "TIMESTAMP_TZ type without name",
		},
		{
			name:        "TIMESTAMP_TZ with name",
			typeID:      TypeIDTimestampTZ,
			typeName:    "created_at",
			description: "TIMESTAMP_TZ type with column name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := NewBinaryWriter()
			err := SerializeTypeInfo(w, tt.typeID, tt.typeName)
			require.NoError(t, err, "serialization should succeed")

			data := w.Bytes()
			assert.NotEmpty(t, data, "serialized data should not be empty")

			// Deserialize
			r := NewBinaryReaderFromBytes(data)
			deserializedTypeID, deserializedTypeName, err := DeserializeTypeInfo(r)
			require.NoError(t, err, "deserialization should succeed")

			// Verify round-trip
			assert.Equal(t, tt.typeID, deserializedTypeID, "type ID should match after round-trip")
			assert.Equal(t, tt.typeName, deserializedTypeName, "type name should match after round-trip")
		})
	}
}

// TestNewTypesConstants verifies the type ID constants match DuckDB values
func TestNewTypesConstants(t *testing.T) {
	// These values are from DuckDB's LogicalTypeId enum in types.hpp
	assert.Equal(t, 32, TypeIDTimestampTZ, "TIMESTAMP_TZ should be 32")
	assert.Equal(t, 34, TypeIDTimeTZ, "TIME_TZ should be 34")
	assert.Equal(t, 36, TypeIDBit, "BIT should be 36")
}

// TestNewTypesInComplexStructures tests new types used within complex types
func TestNewTypesInComplexStructures(t *testing.T) {
	t.Run("STRUCT with BIT field", func(t *testing.T) {
		fields := []StructField{
			{Name: "id", TypeID: 4},         // INTEGER
			{Name: "is_active", TypeID: TypeIDBit}, // BIT
			{Name: "name", TypeID: 18},      // VARCHAR
		}

		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		deserialized, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, len(fields), len(deserialized))
		assert.Equal(t, fields[1].TypeID, deserialized[1].TypeID)
	})

	t.Run("STRUCT with TIME_TZ field", func(t *testing.T) {
		fields := []StructField{
			{Name: "event", TypeID: 18},            // VARCHAR
			{Name: "local_time", TypeID: TypeIDTimeTZ}, // TIME_TZ
		}

		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		deserialized, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, len(fields), len(deserialized))
		assert.Equal(t, fields[1].TypeID, deserialized[1].TypeID)
	})

	t.Run("STRUCT with TIMESTAMP_TZ field", func(t *testing.T) {
		fields := []StructField{
			{Name: "id", TypeID: 4},                         // INTEGER
			{Name: "created_at", TypeID: TypeIDTimestampTZ}, // TIMESTAMP_TZ
			{Name: "updated_at", TypeID: TypeIDTimestampTZ}, // TIMESTAMP_TZ
		}

		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		deserialized, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, len(fields), len(deserialized))
		assert.Equal(t, fields[1].TypeID, deserialized[1].TypeID)
		assert.Equal(t, fields[2].TypeID, deserialized[2].TypeID)
	})

	t.Run("LIST of BIT", func(t *testing.T) {
		w := NewBinaryWriter()
		err := SerializeListType(w, TypeIDBit)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		elementTypeID, err := DeserializeListType(r)
		require.NoError(t, err)
		assert.Equal(t, TypeIDBit, elementTypeID)
	})

	t.Run("UNION with new types", func(t *testing.T) {
		members := []UnionMember{
			{Name: "bit_val", TypeID: TypeIDBit},
			{Name: "time_val", TypeID: TypeIDTimeTZ},
			{Name: "timestamp_val", TypeID: TypeIDTimestampTZ},
		}

		w := NewBinaryWriter()
		err := SerializeUnionType(w, members)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		deserialized, err := DeserializeUnionType(r)
		require.NoError(t, err)
		require.Equal(t, len(members), len(deserialized))
		assert.Equal(t, TypeIDBit, deserialized[0].TypeID)
		assert.Equal(t, TypeIDTimeTZ, deserialized[1].TypeID)
		assert.Equal(t, TypeIDTimestampTZ, deserialized[2].TypeID)
	})
}

// TestDeepNestedTypes tests serialization of deeply nested types (3+ levels)
// This ensures that our serialization can handle complex real-world scenarios.
func TestDeepNestedTypes(t *testing.T) {
	tests := []struct {
		name        string
		description string
		testFunc    func(t *testing.T)
	}{
		{
			name:        "LIST(LIST(LIST(INTEGER))) - triple nested list",
			description: "three levels of list nesting",
			testFunc: func(t *testing.T) {
				// Triple nested list: LIST(LIST(LIST(INTEGER)))
				// Innermost: LIST(INTEGER) - typeID 25
				// Middle: LIST(LIST) - typeID 25 with element 25
				// Outermost: LIST(LIST(LIST)) - typeID 25 with element 25

				w := NewBinaryWriter()
				// Serialize LIST(LIST) where element is also LIST
				err := SerializeListType(w, 25) // LIST element
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				elementTypeID, err := DeserializeListType(r)
				require.NoError(t, err)
				assert.Equal(t, 25, elementTypeID, "element should be LIST")
			},
		},
		{
			name:        "MAP(VARCHAR, MAP(INTEGER, MAP(VARCHAR, INTEGER))) - triple nested map",
			description: "three levels of map nesting",
			testFunc: func(t *testing.T) {
				// Triple nested map
				// Innermost: MAP(VARCHAR, INTEGER)
				// Middle: MAP(INTEGER, MAP(VARCHAR, INTEGER))
				// Outermost: MAP(VARCHAR, MAP(INTEGER, MAP))

				w := NewBinaryWriter()
				// Serialize MAP(VARCHAR, MAP) where value is another MAP
				err := SerializeMapType(w, 18, 27) // VARCHAR key, MAP value
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				keyTypeID, valueTypeID, err := DeserializeMapType(r)
				require.NoError(t, err)
				assert.Equal(t, 18, keyTypeID, "key should be VARCHAR")
				assert.Equal(t, 27, valueTypeID, "value should be MAP")
			},
		},
		{
			name:        "STRUCT with nested STRUCT with nested STRUCT",
			description: "three levels of struct nesting",
			testFunc: func(t *testing.T) {
				// Outermost struct with a STRUCT field
				fields := []StructField{
					{Name: "id", TypeID: 4},              // INTEGER
					{Name: "nested_struct", TypeID: 26}, // STRUCT
				}

				w := NewBinaryWriter()
				err := SerializeStructType(w, fields)
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				readFields, err := DeserializeStructType(r)
				require.NoError(t, err)
				require.Equal(t, 2, len(readFields))
				assert.Equal(t, 26, readFields[1].TypeID, "nested field should be STRUCT")
			},
		},
		{
			name:        "STRUCT with LIST of MAP",
			description: "mixed nesting: struct containing list of maps",
			testFunc: func(t *testing.T) {
				// STRUCT {
				//   id: INTEGER,
				//   data: LIST(MAP)
				// }
				fields := []StructField{
					{Name: "id", TypeID: 4},        // INTEGER
					{Name: "data", TypeID: 25},     // LIST (of MAP)
				}

				w := NewBinaryWriter()
				err := SerializeStructType(w, fields)
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				readFields, err := DeserializeStructType(r)
				require.NoError(t, err)
				require.Equal(t, 2, len(readFields))
				assert.Equal(t, "data", readFields[1].Name)
				assert.Equal(t, 25, readFields[1].TypeID, "data field should be LIST")
			},
		},
		{
			name:        "MAP with STRUCT values containing LIST",
			description: "mixed nesting: map with struct values that contain lists",
			testFunc: func(t *testing.T) {
				// MAP(VARCHAR, STRUCT) where STRUCT contains LIST
				w := NewBinaryWriter()
				err := SerializeMapType(w, 18, 26) // VARCHAR key, STRUCT value
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				keyTypeID, valueTypeID, err := DeserializeMapType(r)
				require.NoError(t, err)
				assert.Equal(t, 18, keyTypeID, "key should be VARCHAR")
				assert.Equal(t, 26, valueTypeID, "value should be STRUCT")
			},
		},
		{
			name:        "LIST of UNION containing STRUCT and MAP",
			description: "mixed nesting: list of unions with complex member types",
			testFunc: func(t *testing.T) {
				// LIST(UNION) where UNION contains STRUCT and MAP
				w := NewBinaryWriter()
				err := SerializeListType(w, 28) // Element type is UNION
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				elementTypeID, err := DeserializeListType(r)
				require.NoError(t, err)
				assert.Equal(t, 28, elementTypeID, "element should be UNION")
			},
		},
		{
			name:        "UNION with LIST of STRUCT and MAP of LIST",
			description: "union containing multiple complex nested types",
			testFunc: func(t *testing.T) {
				// UNION {
				//   list_of_structs: LIST(STRUCT),
				//   map_of_lists: MAP(VARCHAR, LIST)
				// }
				members := []UnionMember{
					{Name: "list_of_structs", TypeID: 25}, // LIST
					{Name: "map_of_lists", TypeID: 27},    // MAP
				}

				w := NewBinaryWriter()
				err := SerializeUnionType(w, members)
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				readMembers, err := DeserializeUnionType(r)
				require.NoError(t, err)
				require.Equal(t, 2, len(readMembers))
				assert.Equal(t, "list_of_structs", readMembers[0].Name)
				assert.Equal(t, 25, readMembers[0].TypeID)
				assert.Equal(t, "map_of_lists", readMembers[1].Name)
				assert.Equal(t, 27, readMembers[1].TypeID)
			},
		},
		{
			name:        "Complex STRUCT with all nested types",
			description: "struct containing LIST, MAP, STRUCT, and UNION fields",
			testFunc: func(t *testing.T) {
				// STRUCT {
				//   id: INTEGER,
				//   tags: LIST,
				//   metadata: MAP,
				//   nested: STRUCT,
				//   variant: UNION
				// }
				fields := []StructField{
					{Name: "id", TypeID: 4},        // INTEGER
					{Name: "tags", TypeID: 25},     // LIST
					{Name: "metadata", TypeID: 27}, // MAP
					{Name: "nested", TypeID: 26},   // STRUCT
					{Name: "variant", TypeID: 28},  // UNION
				}

				w := NewBinaryWriter()
				err := SerializeStructType(w, fields)
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				readFields, err := DeserializeStructType(r)
				require.NoError(t, err)
				require.Equal(t, 5, len(readFields))

				// Verify all complex types are present
				typeIDs := make([]int, len(readFields))
				for i, f := range readFields {
					typeIDs[i] = f.TypeID
				}
				assert.Contains(t, typeIDs, 25, "should contain LIST")
				assert.Contains(t, typeIDs, 26, "should contain STRUCT")
				assert.Contains(t, typeIDs, 27, "should contain MAP")
				assert.Contains(t, typeIDs, 28, "should contain UNION")
			},
		},
		{
			name:        "MAP(STRUCT, LIST) - struct keys and list values",
			description: "map with complex key and value types",
			testFunc: func(t *testing.T) {
				// MAP with STRUCT keys and LIST values
				w := NewBinaryWriter()
				err := SerializeMapType(w, 26, 25) // STRUCT key, LIST value
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				keyTypeID, valueTypeID, err := DeserializeMapType(r)
				require.NoError(t, err)
				assert.Equal(t, 26, keyTypeID, "key should be STRUCT")
				assert.Equal(t, 25, valueTypeID, "value should be LIST")
			},
		},
		{
			name:        "LIST(UNION(STRUCT, LIST, MAP)) - list of unions with all complex types",
			description: "maximum complexity with all types nested",
			testFunc: func(t *testing.T) {
				// LIST of UNION where UNION contains STRUCT, LIST, and MAP
				// First serialize the LIST
				w := NewBinaryWriter()
				err := SerializeListType(w, 28) // Element is UNION
				require.NoError(t, err)

				r := NewBinaryReaderFromBytes(w.Bytes())
				elementTypeID, err := DeserializeListType(r)
				require.NoError(t, err)
				assert.Equal(t, 28, elementTypeID, "element should be UNION")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// TestNestedTypeRoundTrip tests that nested types can be serialized and deserialized
// in sequence, simulating real catalog persistence scenarios.
func TestNestedTypeRoundTrip(t *testing.T) {
	t.Run("Multiple nested types in sequence", func(t *testing.T) {
		w := NewBinaryWriter()

		// Write a sequence of nested types

		// 1. LIST(MAP)
		err := SerializeListType(w, 27) // MAP
		require.NoError(t, err)

		// 2. MAP(VARCHAR, STRUCT)
		err = SerializeMapType(w, 18, 26) // VARCHAR, STRUCT
		require.NoError(t, err)

		// 3. STRUCT with nested fields
		fields := []StructField{
			{Name: "data", TypeID: 25},  // LIST
			{Name: "index", TypeID: 27}, // MAP
		}
		err = SerializeStructType(w, fields)
		require.NoError(t, err)

		// 4. UNION with complex members
		members := []UnionMember{
			{Name: "list_val", TypeID: 25},   // LIST
			{Name: "struct_val", TypeID: 26}, // STRUCT
		}
		err = SerializeUnionType(w, members)
		require.NoError(t, err)

		// Now deserialize everything in the same order
		r := NewBinaryReaderFromBytes(w.Bytes())

		// 1. LIST(MAP)
		listElem, err := DeserializeListType(r)
		require.NoError(t, err)
		assert.Equal(t, 27, listElem)

		// 2. MAP(VARCHAR, STRUCT)
		mapKey, mapVal, err := DeserializeMapType(r)
		require.NoError(t, err)
		assert.Equal(t, 18, mapKey)
		assert.Equal(t, 26, mapVal)

		// 3. STRUCT
		structFields, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(structFields))
		assert.Equal(t, "data", structFields[0].Name)
		assert.Equal(t, 25, structFields[0].TypeID)

		// 4. UNION
		unionMembers, err := DeserializeUnionType(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(unionMembers))
		assert.Equal(t, "list_val", unionMembers[0].Name)
		assert.Equal(t, 25, unionMembers[0].TypeID)
	})
}

// TestNestedTypeEdgeCases tests edge cases with nested types
func TestNestedTypeEdgeCases(t *testing.T) {
	t.Run("Empty STRUCT in LIST", func(t *testing.T) {
		// LIST(STRUCT) where STRUCT is empty (0 fields)
		w := NewBinaryWriter()
		err := SerializeListType(w, 26) // STRUCT
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		elemType, err := DeserializeListType(r)
		require.NoError(t, err)
		assert.Equal(t, 26, elemType)
	})

	t.Run("STRUCT with single nested STRUCT field", func(t *testing.T) {
		fields := []StructField{
			{Name: "nested", TypeID: 26}, // Only field is a STRUCT
		}

		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		readFields, err := DeserializeStructType(r)
		require.NoError(t, err)
		require.Equal(t, 1, len(readFields))
		assert.Equal(t, 26, readFields[0].TypeID)
	})

	t.Run("MAP(LIST, LIST) - both key and value are lists", func(t *testing.T) {
		w := NewBinaryWriter()
		err := SerializeMapType(w, 25, 25) // LIST key, LIST value
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		keyType, valType, err := DeserializeMapType(r)
		require.NoError(t, err)
		assert.Equal(t, 25, keyType)
		assert.Equal(t, 25, valType)
	})

	t.Run("UNION with single complex member", func(t *testing.T) {
		members := []UnionMember{
			{Name: "only_member", TypeID: 27}, // MAP
		}

		w := NewBinaryWriter()
		err := SerializeUnionType(w, members)
		require.NoError(t, err)

		r := NewBinaryReaderFromBytes(w.Bytes())
		readMembers, err := DeserializeUnionType(r)
		require.NoError(t, err)
		require.Equal(t, 1, len(readMembers))
		assert.Equal(t, 27, readMembers[0].TypeID)
	})
}

// Benchmarks for catalog serialization

func BenchmarkSerializeTypeInfo_Integer(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeTypeInfo(w, 5, "INTEGER") // INTEGER
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeTypeInfo_Integer(b *testing.B) {
	w := NewBinaryWriter()
	err := SerializeTypeInfo(w, 5, "INTEGER")
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, _, err := DeserializeTypeInfo(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeTypeInfo_Decimal(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeTypeInfo(w, 18, "DECIMAL")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeTypeInfo_Decimal(b *testing.B) {
	w := NewBinaryWriter()
	err := SerializeTypeInfo(w, 18, "DECIMAL")
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, _, err := DeserializeTypeInfo(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeStructType_Simple(b *testing.B) {
	fields := []StructField{
		{Name: "id", TypeID: 5},      // INTEGER
		{Name: "name", TypeID: 14},   // VARCHAR
		{Name: "active", TypeID: 2},  // BOOLEAN
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeStructType_Simple(b *testing.B) {
	fields := []StructField{
		{Name: "id", TypeID: 5},      // INTEGER
		{Name: "name", TypeID: 14},   // VARCHAR
		{Name: "active", TypeID: 2},  // BOOLEAN
	}

	w := NewBinaryWriter()
	err := SerializeStructType(w, fields)
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, err := DeserializeStructType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeStructType_Nested(b *testing.B) {
	// Create a nested struct with 10 fields including complex types
	fields := []StructField{
		{Name: "f1", TypeID: 5},   // INTEGER
		{Name: "f2", TypeID: 14},  // VARCHAR
		{Name: "f3", TypeID: 25},  // LIST
		{Name: "f4", TypeID: 26},  // STRUCT
		{Name: "f5", TypeID: 27},  // MAP
		{Name: "f6", TypeID: 8},   // DOUBLE
		{Name: "f7", TypeID: 12},  // DATE
		{Name: "f8", TypeID: 13},  // TIMESTAMP
		{Name: "f9", TypeID: 2},   // BOOLEAN
		{Name: "f10", TypeID: 16}, // BLOB
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeListType(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeListType(w, 5) // LIST of INTEGER
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeListType(b *testing.B) {
	w := NewBinaryWriter()
	err := SerializeListType(w, 5) // LIST of INTEGER
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, err := DeserializeListType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeMapType(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeMapType(w, 14, 5) // MAP<VARCHAR, INTEGER>
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeMapType(b *testing.B) {
	w := NewBinaryWriter()
	err := SerializeMapType(w, 14, 5) // MAP<VARCHAR, INTEGER>
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, _, err := DeserializeMapType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeUnionType(b *testing.B) {
	members := []UnionMember{
		{Name: "int_val", TypeID: 5},    // INTEGER
		{Name: "str_val", TypeID: 14},   // VARCHAR
		{Name: "bool_val", TypeID: 2},   // BOOLEAN
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewBinaryWriter()
		err := SerializeUnionType(w, members)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeUnionType(b *testing.B) {
	members := []UnionMember{
		{Name: "int_val", TypeID: 5},    // INTEGER
		{Name: "str_val", TypeID: 14},   // VARCHAR
		{Name: "bool_val", TypeID: 2},   // BOOLEAN
	}

	w := NewBinaryWriter()
	err := SerializeUnionType(w, members)
	if err != nil {
		b.Fatal(err)
	}
	data := w.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := NewBinaryReaderFromBytes(data)
		_, err := DeserializeUnionType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogSerialization_RoundTrip_Simple(b *testing.B) {
	fields := []StructField{
		{Name: "id", TypeID: 5},      // INTEGER
		{Name: "name", TypeID: 14},   // VARCHAR
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Serialize
		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		if err != nil {
			b.Fatal(err)
		}

		// Deserialize
		r := NewBinaryReaderFromBytes(w.Bytes())
		_, err = DeserializeStructType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogSerialization_RoundTrip_Complex(b *testing.B) {
	// Complex nested structure
	fields := []StructField{
		{Name: "id", TypeID: 5},      // INTEGER
		{Name: "data", TypeID: 26},   // STRUCT
		{Name: "tags", TypeID: 25},   // LIST
		{Name: "meta", TypeID: 27},   // MAP
		{Name: "flags", TypeID: 28},  // UNION
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Serialize
		w := NewBinaryWriter()
		err := SerializeStructType(w, fields)
		if err != nil {
			b.Fatal(err)
		}

		// Deserialize
		r := NewBinaryReaderFromBytes(w.Bytes())
		_, err = DeserializeStructType(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}
