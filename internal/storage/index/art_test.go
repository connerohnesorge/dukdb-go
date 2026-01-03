package index

import (
	"bytes"
	"encoding/binary"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewART(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)
	assert.NotNil(t, art)
	assert.Nil(t, art.root)
	assert.Equal(t, dukdb.TYPE_INTEGER, art.keyType)
}

func TestNewNode(t *testing.T) {
	tests := []struct {
		name         string
		nodeType     NodeType
		expectedKeys int
		expectedCap  int
	}{
		{
			name:         "Leaf",
			nodeType:     NodeTypeLeaf,
			expectedKeys: -1, // nil
			expectedCap:  -1, // nil
		},
		{
			name:         "Node4",
			nodeType:     NodeType4,
			expectedKeys: 0,
			expectedCap:  4,
		},
		{
			name:         "Node16",
			nodeType:     NodeType16,
			expectedKeys: 0,
			expectedCap:  16,
		},
		{
			name:         "Node48",
			nodeType:     NodeType48,
			expectedKeys: 256,
			expectedCap:  48,
		},
		{
			name:         "Node256",
			nodeType:     NodeType256,
			expectedKeys: -1, // nil
			expectedCap:  256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newNode(tt.nodeType)
			assert.NotNil(t, node)
			assert.Equal(t, tt.nodeType, node.nodeType)

			if tt.expectedKeys == -1 {
				assert.Nil(t, node.keys)
			} else {
				assert.Equal(t, tt.expectedKeys, len(node.keys))
			}

			if tt.expectedCap == -1 {
				assert.Nil(t, node.children)
			} else {
				assert.Equal(t, tt.expectedCap, cap(node.children))
			}
		})
	}
}

func TestARTInsertAndLookup(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Create a simple integer key
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)

	// Insert a value
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)
	assert.NotNil(t, art.root)

	// Lookup the value
	value, found := art.Lookup(key)
	assert.True(t, found)
	assert.Equal(t, uint64(100), value)

	// Lookup non-existent key
	key2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(key2, 43)
	_, found = art.Lookup(key2)
	assert.False(t, found)
}

func TestARTSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name    string
		keyType dukdb.Type
		setup   func(*ART)
	}{
		{
			name:    "Empty ART",
			keyType: dukdb.TYPE_INTEGER,
			setup:   func(_ *ART) {},
		},
		{
			name:    "ART with root leaf",
			keyType: dukdb.TYPE_BIGINT,
			setup: func(art *ART) {
				key := make([]byte, 8)
				binary.LittleEndian.PutUint64(key, 12345)
				_ = art.Insert(key, uint64(99999))
			},
		},
		{
			name:    "String key type",
			keyType: dukdb.TYPE_VARCHAR,
			setup: func(art *ART) {
				_ = art.Insert([]byte("test"), uint64(42))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and setup ART
			art := NewART(tt.keyType)
			tt.setup(art)

			// Serialize
			data, err := art.Serialize()
			require.NoError(t, err)
			assert.NotEmpty(t, data)

			// Deserialize
			art2, err := DeserializeART(data)
			require.NoError(t, err)
			assert.NotNil(t, art2)
			assert.Equal(t, art.keyType, art2.keyType)

			// Verify root state
			if art.root == nil {
				assert.Nil(t, art2.root)
			} else {
				assert.NotNil(t, art2.root)
				assert.Equal(t, art.root.nodeType, art2.root.nodeType)
			}
		})
	}
}

func TestEncodeDecodeKey(t *testing.T) {
	tests := []struct {
		name    string
		keyType dukdb.Type
		input   []byte
	}{
		{
			name:    "Boolean true",
			keyType: dukdb.TYPE_BOOLEAN,
			input:   []byte{0x01},
		},
		{
			name:    "Boolean false",
			keyType: dukdb.TYPE_BOOLEAN,
			input:   []byte{0x00},
		},
		{
			name:    "TinyInt positive",
			keyType: dukdb.TYPE_TINYINT,
			input:   []byte{0x7F},
		},
		{
			name:    "TinyInt negative",
			keyType: dukdb.TYPE_TINYINT,
			input:   []byte{0x80},
		},
		{
			name:    "SmallInt",
			keyType: dukdb.TYPE_SMALLINT,
			input:   []byte{0x34, 0x12},
		},
		{
			name:    "Integer",
			keyType: dukdb.TYPE_INTEGER,
			input:   []byte{0x78, 0x56, 0x34, 0x12},
		},
		{
			name:    "BigInt",
			keyType: dukdb.TYPE_BIGINT,
			input:   []byte{0xEF, 0xCD, 0xAB, 0x90, 0x78, 0x56, 0x34, 0x12},
		},
		{
			name:    "UTinyInt",
			keyType: dukdb.TYPE_UTINYINT,
			input:   []byte{0xFF},
		},
		{
			name:    "USmallInt",
			keyType: dukdb.TYPE_USMALLINT,
			input:   []byte{0xFF, 0xFF},
		},
		{
			name:    "UInteger",
			keyType: dukdb.TYPE_UINTEGER,
			input:   []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:    "UBigInt",
			keyType: dukdb.TYPE_UBIGINT,
			input:   []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:    "Float",
			keyType: dukdb.TYPE_FLOAT,
			input:   []byte{0x00, 0x00, 0x80, 0x3F}, // 1.0
		},
		{
			name:    "Double",
			keyType: dukdb.TYPE_DOUBLE,
			input:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, // 1.0
		},
		{
			name:    "String",
			keyType: dukdb.TYPE_VARCHAR,
			input:   []byte("hello"),
		},
		{
			name:    "Blob",
			keyType: dukdb.TYPE_BLOB,
			input:   []byte{0x01, 0x02, 0x03, 0x04},
		},
		{
			name:    "Date",
			keyType: dukdb.TYPE_DATE,
			input:   []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:    "Time",
			keyType: dukdb.TYPE_TIME,
			input:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:    "Timestamp",
			keyType: dukdb.TYPE_TIMESTAMP,
			input:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := encodeKey(tt.input, tt.keyType)
			require.NoError(t, err)
			assert.NotNil(t, encoded)

			// Decode
			decoded, err := decodeKey(encoded, tt.keyType)
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, tt.input, decoded, "Round-trip encoding/decoding should preserve data")
		})
	}
}

func TestEncodeKeySortOrder(t *testing.T) {
	// Test that encoded keys preserve sort order for integers
	t.Run("Integer sort order", func(t *testing.T) {
		values := []int32{-100, -1, 0, 1, 100}
		var encodedKeys [][]byte

		for _, v := range values {
			key := make([]byte, 4)
			// Use LittleEndian (as DuckDB uses little-endian internally)
			binary.LittleEndian.PutUint32(key, uint32(v))
			encoded, err := encodeKey(key, dukdb.TYPE_INTEGER)
			require.NoError(t, err)
			encodedKeys = append(encodedKeys, encoded)
		}

		// Verify encoded keys are in ascending order
		for i := 1; i < len(encodedKeys); i++ {
			cmp := bytes.Compare(encodedKeys[i-1], encodedKeys[i])
			assert.LessOrEqual(t, cmp, 0, "Encoded keys should maintain sort order")
		}
	})

	// Test that encoded keys preserve sort order for strings
	t.Run("String sort order", func(t *testing.T) {
		values := []string{"apple", "banana", "cherry"}
		var encodedKeys [][]byte

		for _, v := range values {
			encoded, err := encodeKey([]byte(v), dukdb.TYPE_VARCHAR)
			require.NoError(t, err)
			encodedKeys = append(encodedKeys, encoded)
		}

		// Verify encoded keys are in ascending order
		for i := 1; i < len(encodedKeys); i++ {
			cmp := bytes.Compare(encodedKeys[i-1], encodedKeys[i])
			assert.Less(t, cmp, 0, "Encoded keys should maintain sort order")
		}
	})
}

func TestSerializeNodeTypes(t *testing.T) {
	tests := []struct {
		name     string
		nodeType NodeType
		setup    func(*ARTNode)
	}{
		{
			name:     "Leaf node",
			nodeType: NodeTypeLeaf,
			setup: func(node *ARTNode) {
				node.prefix = []byte("test")
				node.value = uint64(42)
			},
		},
		{
			name:     "Node4",
			nodeType: NodeType4,
			setup: func(node *ARTNode) {
				node.keys = []byte{0x01, 0x02}
				node.children = []*ARTNode{
					newNode(NodeTypeLeaf),
					newNode(NodeTypeLeaf),
				}
			},
		},
		{
			name:     "Node16",
			nodeType: NodeType16,
			setup: func(node *ARTNode) {
				node.keys = []byte{0x01}
				node.children = []*ARTNode{newNode(NodeTypeLeaf)}
			},
		},
		{
			name:     "Node48",
			nodeType: NodeType48,
			setup: func(node *ARTNode) {
				node.keys = make([]byte, 256)
				for i := range node.keys {
					node.keys[i] = 0xFF
				}
				node.keys[0] = 0x00 // First child at index 0
				node.children = []*ARTNode{newNode(NodeTypeLeaf)}
			},
		},
		{
			name:     "Node256",
			nodeType: NodeType256,
			setup: func(node *ARTNode) {
				node.children = make([]*ARTNode, 256)
				node.children[0] = newNode(NodeTypeLeaf)
				node.children[255] = newNode(NodeTypeLeaf)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and setup node
			node := newNode(tt.nodeType)
			tt.setup(node)

			// Serialize
			buf := new(bytes.Buffer)
			err := serializeNode(buf, node)
			require.NoError(t, err)

			// Deserialize
			r := bytes.NewReader(buf.Bytes())
			node2, err := deserializeNode(r)
			require.NoError(t, err)

			// Verify node type
			assert.Equal(t, node.nodeType, node2.nodeType)
		})
	}
}

func TestEncodeIntKey(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{"Negative", -1000},
		{"Zero", 0},
		{"Positive", 1000},
		{"Min", -9223372036854775808},
		{"Max", 9223372036854775807},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeIntKey(tt.value)
			assert.Equal(t, 8, len(encoded))

			// Verify that keys maintain sort order
			if tt.value < 0 {
				// Negative values should have high bit cleared after flip
				v := binary.LittleEndian.Uint64(encoded)
				assert.Less(t, v, uint64(1<<63))
			}
		})
	}
}

func TestEncodeFloatKey(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"Negative", -1.5},
		{"Zero", 0.0},
		{"Positive", 1.5},
		{"Large", 1e10},
		{"Small", 1e-10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeFloatKey(tt.value)
			assert.Equal(t, 8, len(encoded))
		})
	}
}

func TestEncodeStringKey(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"Empty", ""},
		{"Short", "a"},
		{"Medium", "hello world"},
		{"Long", "this is a longer string for testing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeStringKey(tt.value)
			assert.Equal(t, len(tt.value)+1, len(encoded))
			assert.Equal(t, byte(0x00), encoded[len(encoded)-1])
		})
	}
}

func TestARTSerializeEmptyTree(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	data, err := art.Serialize()
	require.NoError(t, err)
	assert.Equal(t, 2, len(data)) // keyType + null marker

	art2, err := DeserializeART(data)
	require.NoError(t, err)
	assert.Nil(t, art2.root)
	assert.Equal(t, dukdb.TYPE_INTEGER, art2.keyType)
}

func TestARTSerializeWithMultipleTypes(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, keyType := range types {
		t.Run(keyType.String(), func(t *testing.T) {
			art := NewART(keyType)

			// Serialize
			data, err := art.Serialize()
			require.NoError(t, err)

			// Deserialize
			art2, err := DeserializeART(data)
			require.NoError(t, err)
			assert.Equal(t, keyType, art2.keyType)
		})
	}
}

func TestEncodeKeyErrors(t *testing.T) {
	tests := []struct {
		name    string
		keyType dukdb.Type
		input   []byte
	}{
		{
			name:    "Invalid boolean length",
			keyType: dukdb.TYPE_BOOLEAN,
			input:   []byte{0x01, 0x02},
		},
		{
			name:    "Invalid tinyint length",
			keyType: dukdb.TYPE_TINYINT,
			input:   []byte{0x01, 0x02},
		},
		{
			name:    "Invalid smallint length",
			keyType: dukdb.TYPE_SMALLINT,
			input:   []byte{0x01},
		},
		{
			name:    "Invalid integer length",
			keyType: dukdb.TYPE_INTEGER,
			input:   []byte{0x01, 0x02},
		},
		{
			name:    "Invalid bigint length",
			keyType: dukdb.TYPE_BIGINT,
			input:   []byte{0x01, 0x02},
		},
		{
			name:    "Invalid float length",
			keyType: dukdb.TYPE_FLOAT,
			input:   []byte{0x01, 0x02},
		},
		{
			name:    "Invalid double length",
			keyType: dukdb.TYPE_DOUBLE,
			input:   []byte{0x01, 0x02},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encodeKey(tt.input, tt.keyType)
			assert.Error(t, err)
		})
	}
}

func TestDeserializeARTErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty data",
			data: nil,
		},
		{
			name: "Only keyType",
			data: []byte{byte(dukdb.TYPE_INTEGER)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeART(tt.data)
			assert.Error(t, err)
		})
	}
}
