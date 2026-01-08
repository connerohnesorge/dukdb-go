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

// =============================================================================
// RangeScan Tests
// =============================================================================

func TestRangeScanEmptyART(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// RangeScan on empty ART should return exhausted iterator
	it := art.RangeScan(nil, nil, DefaultRangeScanOptions())
	assert.NotNil(t, it)
	assert.True(t, it.IsExhausted())

	// Calling Close on exhausted iterator should be safe
	it.Close()
	assert.True(t, it.IsExhausted())
}

func TestRangeScanWithRoot(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value to create a root
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	// RangeScan on non-empty ART should return non-exhausted iterator
	it := art.RangeScan(nil, nil, DefaultRangeScanOptions())
	assert.NotNil(t, it)
	assert.False(t, it.IsExhausted())
	assert.False(t, it.initialized)

	it.Close()
}

func TestRangeScanInvalidRange(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value to create non-empty ART
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("lower > upper returns exhausted iterator", func(t *testing.T) {
		lower := []byte{0x10, 0x00, 0x00, 0x00} // 16
		upper := []byte{0x05, 0x00, 0x00, 0x00} // 5
		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		assert.True(t, it.IsExhausted())
	})

	t.Run("equal bounds, lower exclusive returns exhausted", func(t *testing.T) {
		bound := []byte{0x10, 0x00, 0x00, 0x00} // 16
		it := art.RangeScan(bound, bound, RangeScanOptions{
			LowerInclusive: false,
			UpperInclusive: true,
		})
		assert.True(t, it.IsExhausted())
	})

	t.Run("equal bounds, upper exclusive returns exhausted", func(t *testing.T) {
		bound := []byte{0x10, 0x00, 0x00, 0x00} // 16
		it := art.RangeScan(bound, bound, RangeScanOptions{
			LowerInclusive: true,
			UpperInclusive: false,
		})
		assert.True(t, it.IsExhausted())
	})

	t.Run("equal bounds, both inclusive returns valid iterator", func(t *testing.T) {
		bound := []byte{0x10, 0x00, 0x00, 0x00} // 16
		it := art.RangeScan(bound, bound, RangeScanOptions{
			LowerInclusive: true,
			UpperInclusive: true,
		})
		assert.False(t, it.IsExhausted())
		it.Close()
	})
}

func TestRangeScanWithBounds(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("with lower bound only", func(t *testing.T) {
		lower := []byte{0x01, 0x00, 0x00, 0x00}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())
		assert.False(t, it.IsExhausted())
		assert.Equal(t, lower, it.lowerBound)
		assert.Nil(t, it.upperBound)
		assert.True(t, it.lowerInclusive)
		it.Close()
	})

	t.Run("with upper bound only", func(t *testing.T) {
		upper := []byte{0xFF, 0x00, 0x00, 0x00}
		it := art.RangeScan(nil, upper, DefaultRangeScanOptions())
		assert.False(t, it.IsExhausted())
		assert.Nil(t, it.lowerBound)
		assert.Equal(t, upper, it.upperBound)
		assert.False(t, it.upperInclusive) // Default is exclusive
		it.Close()
	})

	t.Run("with both bounds", func(t *testing.T) {
		lower := []byte{0x01, 0x00, 0x00, 0x00}
		upper := []byte{0xFF, 0x00, 0x00, 0x00}
		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		assert.False(t, it.IsExhausted())
		assert.Equal(t, lower, it.lowerBound)
		assert.Equal(t, upper, it.upperBound)
		it.Close()
	})
}

func TestRangeScanOptions(t *testing.T) {
	t.Run("DefaultRangeScanOptions", func(t *testing.T) {
		opts := DefaultRangeScanOptions()
		assert.True(t, opts.LowerInclusive)
		assert.False(t, opts.UpperInclusive)
		assert.False(t, opts.Reverse)
		assert.Equal(t, 0, opts.MaxResults)
	})

	t.Run("Custom options are preserved", func(t *testing.T) {
		art := NewART(dukdb.TYPE_INTEGER)
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, 42)
		_ = art.Insert(key, uint64(100))

		lower := []byte{0x01, 0x00, 0x00, 0x00}
		upper := []byte{0xFF, 0x00, 0x00, 0x00}
		it := art.RangeScan(lower, upper, RangeScanOptions{
			LowerInclusive: false,
			UpperInclusive: true,
		})

		assert.False(t, it.lowerInclusive)
		assert.True(t, it.upperInclusive)
		it.Close()
	})
}

func TestScanFrom(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	_ = art.Insert(key, uint64(100))

	t.Run("inclusive lower bound", func(t *testing.T) {
		lower := []byte{0x10, 0x00, 0x00, 0x00}
		it := art.ScanFrom(lower, true)
		assert.NotNil(t, it)
		assert.False(t, it.IsExhausted())
		assert.Equal(t, lower, it.lowerBound)
		assert.Nil(t, it.upperBound)
		assert.True(t, it.lowerInclusive)
		it.Close()
	})

	t.Run("exclusive lower bound", func(t *testing.T) {
		lower := []byte{0x10, 0x00, 0x00, 0x00}
		it := art.ScanFrom(lower, false)
		assert.NotNil(t, it)
		assert.False(t, it.IsExhausted())
		assert.Equal(t, lower, it.lowerBound)
		assert.Nil(t, it.upperBound)
		assert.False(t, it.lowerInclusive)
		it.Close()
	})
}

func TestScanTo(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	_ = art.Insert(key, uint64(100))

	t.Run("inclusive upper bound", func(t *testing.T) {
		upper := []byte{0xFF, 0x00, 0x00, 0x00}
		it := art.ScanTo(upper, true)
		assert.NotNil(t, it)
		assert.False(t, it.IsExhausted())
		assert.Nil(t, it.lowerBound)
		assert.Equal(t, upper, it.upperBound)
		assert.True(t, it.upperInclusive)
		it.Close()
	})

	t.Run("exclusive upper bound", func(t *testing.T) {
		upper := []byte{0xFF, 0x00, 0x00, 0x00}
		it := art.ScanTo(upper, false)
		assert.NotNil(t, it)
		assert.False(t, it.IsExhausted())
		assert.Nil(t, it.lowerBound)
		assert.Equal(t, upper, it.upperBound)
		assert.False(t, it.upperInclusive)
		it.Close()
	})
}

func TestScanAll(t *testing.T) {
	t.Run("empty ART", func(t *testing.T) {
		art := NewART(dukdb.TYPE_INTEGER)
		it := art.ScanAll()
		assert.NotNil(t, it)
		assert.True(t, it.IsExhausted())
	})

	t.Run("non-empty ART", func(t *testing.T) {
		art := NewART(dukdb.TYPE_INTEGER)
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, 42)
		_ = art.Insert(key, uint64(100))

		it := art.ScanAll()
		assert.NotNil(t, it)
		assert.False(t, it.IsExhausted())
		assert.Nil(t, it.lowerBound)
		assert.Nil(t, it.upperBound)
		assert.True(t, it.lowerInclusive)
		assert.False(t, it.upperInclusive)
		it.Close()
	})
}

func TestIteratorClose(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	_ = art.Insert(key, uint64(100))

	it := art.RangeScan(nil, nil, DefaultRangeScanOptions())
	assert.False(t, it.IsExhausted())

	// Close should mark as exhausted and clear resources
	it.Close()
	assert.True(t, it.IsExhausted())
	assert.Nil(t, it.stack)
	assert.Nil(t, it.currentKey)
	assert.Nil(t, it.lowerBound)
	assert.Nil(t, it.upperBound)
	assert.Nil(t, it.lastKey)
	assert.Nil(t, it.lastValue)

	// Multiple Close calls should be safe
	it.Close()
	assert.True(t, it.IsExhausted())
}

func TestCompareKeys(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected int
	}{
		{
			name:     "equal keys",
			a:        []byte{0x01, 0x02, 0x03},
			b:        []byte{0x01, 0x02, 0x03},
			expected: 0,
		},
		{
			name:     "a < b (different byte)",
			a:        []byte{0x01, 0x02, 0x03},
			b:        []byte{0x01, 0x02, 0x04},
			expected: -1,
		},
		{
			name:     "a > b (different byte)",
			a:        []byte{0x01, 0x02, 0x04},
			b:        []byte{0x01, 0x02, 0x03},
			expected: 1,
		},
		{
			name:     "a < b (shorter)",
			a:        []byte{0x01, 0x02},
			b:        []byte{0x01, 0x02, 0x03},
			expected: -1,
		},
		{
			name:     "a > b (longer)",
			a:        []byte{0x01, 0x02, 0x03},
			b:        []byte{0x01, 0x02},
			expected: 1,
		},
		{
			name:     "both empty",
			a:        []byte{},
			b:        []byte{},
			expected: 0,
		},
		{
			name:     "a empty, b not",
			a:        []byte{},
			b:        []byte{0x01},
			expected: -1,
		},
		{
			name:     "a not, b empty",
			a:        []byte{0x01},
			b:        []byte{},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareKeys(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIteratorPreAllocation(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	_ = art.Insert(key, uint64(100))

	it := art.RangeScan(nil, nil, DefaultRangeScanOptions())

	// Verify pre-allocation sizes
	assert.Equal(t, defaultStackCapacity, cap(it.stack))
	assert.Equal(t, defaultKeyCapacity, cap(it.currentKey))

	// Stack and currentKey should be empty initially
	assert.Equal(t, 0, len(it.stack))
	assert.Equal(t, 0, len(it.currentKey))

	it.Close()
}

// =============================================================================
// Iterator Next() Tests
// =============================================================================

func TestNextOnEmptyART(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	it := art.ScanAll()
	assert.True(t, it.IsExhausted())

	// Calling Next on exhausted iterator should return false
	key, value, ok := it.Next()
	assert.False(t, ok)
	assert.Nil(t, key)
	assert.Nil(t, value)
}

func TestNextOnSingleLeafART(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a single value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	// Scan all should return the single value
	it := art.ScanAll()
	assert.False(t, it.IsExhausted())

	// First Next() should return the value
	returnedKey, value, ok := it.Next()
	assert.True(t, ok)
	assert.NotNil(t, returnedKey)
	assert.Equal(t, uint64(100), value)

	// Second Next() should exhaust the iterator
	returnedKey, value, ok = it.Next()
	assert.False(t, ok)
	assert.Nil(t, returnedKey)
	assert.Nil(t, value)
	assert.True(t, it.IsExhausted())

	it.Close()
}

// encodeIntegerKeyForTest encodes an integer value the same way Insert() does for TYPE_INTEGER.
// This helper is needed because bounds must be encoded the same way as keys in the tree.
func encodeIntegerKeyForTest(val uint32) []byte {
	// Create little-endian bytes (as DuckDB uses internally)
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, val)
	// Encode it the same way as art.Insert() does
	encoded, _ := encodeKey(key, dukdb.TYPE_INTEGER)
	return encoded
}

func TestNextWithLowerBound(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("lower bound less than key - should find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(10)
		it := art.ScanFrom(lower, true)

		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("lower bound equal to key inclusive - should find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(42)
		it := art.ScanFrom(lower, true)

		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("lower bound greater than key - should not find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(50)
		it := art.ScanFrom(lower, true)

		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})
}

func TestNextWithUpperBound(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("upper bound greater than key - should find key", func(t *testing.T) {
		upper := encodeIntegerKeyForTest(100)
		it := art.ScanTo(upper, false)

		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("upper bound equal to key inclusive - should find key", func(t *testing.T) {
		upper := encodeIntegerKeyForTest(42)
		it := art.ScanTo(upper, true)

		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("upper bound equal to key exclusive - should not find key", func(t *testing.T) {
		upper := encodeIntegerKeyForTest(42)
		it := art.ScanTo(upper, false)

		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})

	t.Run("upper bound less than key - should not find key", func(t *testing.T) {
		upper := encodeIntegerKeyForTest(10)
		it := art.ScanTo(upper, false)

		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})
}

func TestNextWithBothBounds(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("key in range - should find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(10)
		upper := encodeIntegerKeyForTest(100)

		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("key below lower bound - should not find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(50)
		upper := encodeIntegerKeyForTest(100)

		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})

	t.Run("key above upper bound - should not find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(10)
		upper := encodeIntegerKeyForTest(30)

		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})
}

func TestNextExclusiveLowerBound(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	t.Run("exclusive lower bound equal to key - should not find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(42)
		it := art.ScanFrom(lower, false) // exclusive

		returnedKey, value, ok := it.Next()
		assert.False(t, ok)
		assert.Nil(t, returnedKey)
		assert.Nil(t, value)
		it.Close()
	})

	t.Run("exclusive lower bound less than key - should find key", func(t *testing.T) {
		lower := encodeIntegerKeyForTest(10)
		it := art.ScanFrom(lower, false) // exclusive

		returnedKey, value, ok := it.Next()
		assert.True(t, ok)
		assert.NotNil(t, returnedKey)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})
}

func TestIteratorKeyAndValueMethods(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	it := art.ScanAll()

	// Before first Next(), Key() and Value() should return nil
	assert.Nil(t, it.Key())
	assert.Nil(t, it.Value())

	// After Next(), Key() and Value() should return the current values
	returnedKey, value, ok := it.Next()
	assert.True(t, ok)
	assert.Equal(t, returnedKey, it.Key())
	assert.Equal(t, value, it.Value())

	// After exhaustion, Key() and Value() should still return last values
	// (iterator state shows initialized but exhausted)
	it.Next() // Exhaust the iterator
	// After exhausted, Key() and Value() return nil because exhausted is checked first
	assert.Nil(t, it.Key())
	assert.Nil(t, it.Value())

	it.Close()
}

func TestNextMultipleCalls(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	it := art.ScanAll()

	// Multiple calls to Next after exhaustion should all return false
	it.Next()  // Returns the value
	it.Next()  // Exhausts
	_, _, ok := it.Next()
	assert.False(t, ok)
	_, _, ok = it.Next()
	assert.False(t, ok)
	_, _, ok = it.Next()
	assert.False(t, ok)

	it.Close()
}

func TestNextAfterClose(t *testing.T) {
	art := NewART(dukdb.TYPE_INTEGER)

	// Insert a value
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, 42)
	err := art.Insert(key, uint64(100))
	require.NoError(t, err)

	it := art.ScanAll()
	it.Close()

	// Next after Close should return false
	returnedKey, value, ok := it.Next()
	assert.False(t, ok)
	assert.Nil(t, returnedKey)
	assert.Nil(t, value)
}

// =============================================================================
// ART with Multiple Nodes Tests
// =============================================================================

// buildARTWithNode4 creates an ART with a Node4 root and multiple leaf children
func buildARTWithNode4() *ART {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node4 as root with 3 children
	root := newNode(NodeType4)
	root.prefix = []byte{0x80} // Some prefix

	// Add three children (leaf nodes)
	leaf1 := newNode(NodeTypeLeaf)
	leaf1.prefix = []byte{0x01}
	leaf1.value = uint64(100)

	leaf2 := newNode(NodeTypeLeaf)
	leaf2.prefix = []byte{0x02}
	leaf2.value = uint64(200)

	leaf3 := newNode(NodeTypeLeaf)
	leaf3.prefix = []byte{0x03}
	leaf3.value = uint64(300)

	root.keys = []byte{0x10, 0x20, 0x30}
	root.children = []*ARTNode{leaf1, leaf2, leaf3}

	art.root = root
	return art
}

func TestNextOnNode4(t *testing.T) {
	art := buildARTWithNode4()

	t.Run("scan all returns all keys in order", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64

		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{100, 200, 300}, values)
		it.Close()
	})

	t.Run("scan with lower bound skips smaller keys", func(t *testing.T) {
		// Lower bound that should skip first child
		lower := []byte{0x80, 0x15} // After 0x80, 0x10 child
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300}, values)
		it.Close()
	})

	t.Run("scan with upper bound stops at larger keys", func(t *testing.T) {
		// Upper bound that should stop after second child
		upper := []byte{0x80, 0x25}
		it := art.RangeScan(nil, upper, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{100, 200}, values)
		it.Close()
	})
}

// buildARTWithNode16 creates an ART with a Node16 root
func buildARTWithNode16() *ART {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node16 as root with 10 children
	root := newNode(NodeType16)
	root.prefix = nil // No prefix

	// Add 10 children (leaf nodes)
	keys := make([]byte, 10)
	children := make([]*ARTNode, 10)
	for i := 0; i < 10; i++ {
		keys[i] = byte(i * 10) // 0, 10, 20, 30, ...
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64((i + 1) * 100)
		children[i] = leaf
	}

	root.keys = keys
	root.children = children
	art.root = root
	return art
}

func TestNextOnNode16(t *testing.T) {
	art := buildARTWithNode16()

	t.Run("scan all returns all keys in order", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64

		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		expected := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		assert.Equal(t, expected, values)
		it.Close()
	})
}

// buildARTWithNode48 creates an ART with a Node48 root
func buildARTWithNode48() *ART {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node48 as root with sparse children
	root := newNode(NodeType48)
	root.prefix = nil

	// Initialize keys array (all 0xFF for empty)
	// Node48 uses keys[keyByte] = childIndex mapping
	// newNode already initializes keys to 0xFF

	// Add 5 children at specific key bytes
	keyBytes := []byte{0x05, 0x20, 0x80, 0xAA, 0xFF}
	children := make([]*ARTNode, 5)

	for i, kb := range keyBytes {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64((i + 1) * 100)
		children[i] = leaf
		root.keys[kb] = byte(i) // Map key byte to child index
	}
	root.children = children
	art.root = root
	return art
}

func TestNextOnNode48(t *testing.T) {
	art := buildARTWithNode48()

	t.Run("scan all returns all keys in order", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64

		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return values in key byte order: 0x05, 0x20, 0x80, 0xAA, 0xFF
		expected := []uint64{100, 200, 300, 400, 500}
		assert.Equal(t, expected, values)
		it.Close()
	})

	t.Run("scan with lower bound in middle", func(t *testing.T) {
		// Lower bound at 0x50 should skip 0x05 and 0x20
		lower := []byte{0x50}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return values for 0x80, 0xAA, 0xFF
		expected := []uint64{300, 400, 500}
		assert.Equal(t, expected, values)
		it.Close()
	})
}

// buildARTWithNode256 creates an ART with a Node256 root
func buildARTWithNode256() *ART {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node256 as root with sparse children
	root := newNode(NodeType256)
	root.prefix = nil

	// Add children at specific positions
	positions := []int{0, 50, 100, 150, 200, 255}
	for i, pos := range positions {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64((i + 1) * 100)
		root.children[pos] = leaf
	}

	art.root = root
	return art
}

func TestNextOnNode256(t *testing.T) {
	art := buildARTWithNode256()

	t.Run("scan all returns all keys in order", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64

		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return values in index order: 0, 50, 100, 150, 200, 255
		expected := []uint64{100, 200, 300, 400, 500, 600}
		assert.Equal(t, expected, values)
		it.Close()
	})

	t.Run("scan with range in middle", func(t *testing.T) {
		// Range from 60 to 180 should include keys 100 and 150
		lower := []byte{60}
		upper := []byte{180}
		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return values for 100 and 150
		expected := []uint64{300, 400}
		assert.Equal(t, expected, values)
		it.Close()
	})
}

// =============================================================================
// Deep Tree Tests
// =============================================================================

// buildDeepART creates a deeper ART with multiple levels
func buildDeepART() *ART {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a two-level tree:
	// Root (Node4) -> [0x10 -> Node4 -> leaves, 0x20 -> leaf]

	root := newNode(NodeType4)
	root.prefix = []byte{0x00} // Prefix at root

	// First child is an inner node
	inner := newNode(NodeType4)
	inner.prefix = []byte{0xAA} // Prefix for inner node

	// Inner node has 2 leaf children
	leafA := newNode(NodeTypeLeaf)
	leafA.prefix = []byte{0x01}
	leafA.value = uint64(100)

	leafB := newNode(NodeTypeLeaf)
	leafB.prefix = []byte{0x02}
	leafB.value = uint64(200)

	inner.keys = []byte{0x01, 0x02}
	inner.children = []*ARTNode{leafA, leafB}

	// Second child is a direct leaf
	leafC := newNode(NodeTypeLeaf)
	leafC.prefix = []byte{0xBB, 0x03}
	leafC.value = uint64(300)

	root.keys = []byte{0x10, 0x20}
	root.children = []*ARTNode{inner, leafC}

	art.root = root
	return art
}

func TestNextOnDeepART(t *testing.T) {
	art := buildDeepART()

	t.Run("scan all traverses depth first", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64

		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Path order:
		// 1. Root prefix 0x00 -> child 0x10 -> inner prefix 0xAA -> child 0x01 -> leaf prefix 0x01 = value 100
		// 2. Root prefix 0x00 -> child 0x10 -> inner prefix 0xAA -> child 0x02 -> leaf prefix 0x02 = value 200
		// 3. Root prefix 0x00 -> child 0x20 -> leaf prefix 0xBB,0x03 = value 300
		expected := []uint64{100, 200, 300}
		assert.Equal(t, expected, values)
		it.Close()
	})
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestNextWithEmptyPrefix(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a tree with empty prefixes
	root := newNode(NodeType4)
	root.prefix = nil // Empty prefix

	leaf := newNode(NodeTypeLeaf)
	leaf.prefix = nil // Empty prefix
	leaf.value = uint64(42)

	root.keys = []byte{0x00}
	root.children = []*ARTNode{leaf}
	art.root = root

	it := art.ScanAll()
	_, value, ok := it.Next()
	assert.True(t, ok)
	assert.Equal(t, uint64(42), value)

	_, _, ok = it.Next()
	assert.False(t, ok)
	it.Close()
}

func TestNextWithLongPrefix(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a tree with a long prefix
	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}
	root.value = uint64(999)

	art.root = root

	it := art.ScanAll()
	key, value, ok := it.Next()
	assert.True(t, ok)
	assert.Equal(t, root.prefix, key)
	assert.Equal(t, uint64(999), value)

	it.Close()
}

func TestIteratorRangeBoundsPrefixComparison(t *testing.T) {
	// Test that prefix comparison works correctly for lower bound search
	art := NewART(dukdb.TYPE_BIGINT)

	// Create tree with prefix that is greater than lower bound
	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0xFF, 0xFF}
	root.value = uint64(100)
	art.root = root

	t.Run("lower bound less than prefix - should find", func(t *testing.T) {
		lower := []byte{0x00, 0x00}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(100), value)
		it.Close()
	})

	t.Run("lower bound greater than prefix - should not find", func(t *testing.T) {
		lower := []byte{0xFF, 0xFF, 0x01}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})
}

// =============================================================================
// Inclusive/Exclusive Bounds Edge Case Tests (Task 3.4)
// =============================================================================

// TestBoundsInclusiveExclusiveEdgeCases tests edge cases for inclusive/exclusive bounds
// with a focus on boundary conditions.
func TestBoundsInclusiveExclusiveEdgeCases(t *testing.T) {
	// Build a tree with multiple keys for comprehensive testing
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node4 with 4 children representing keys: 0x10, 0x20, 0x30, 0x40
	root := newNode(NodeType4)
	root.prefix = nil

	leaf1 := newNode(NodeTypeLeaf)
	leaf1.prefix = nil
	leaf1.value = uint64(100)

	leaf2 := newNode(NodeTypeLeaf)
	leaf2.prefix = nil
	leaf2.value = uint64(200)

	leaf3 := newNode(NodeTypeLeaf)
	leaf3.prefix = nil
	leaf3.value = uint64(300)

	leaf4 := newNode(NodeTypeLeaf)
	leaf4.prefix = nil
	leaf4.value = uint64(400)

	root.keys = []byte{0x10, 0x20, 0x30, 0x40}
	root.children = []*ARTNode{leaf1, leaf2, leaf3, leaf4}
	art.root = root

	t.Run("inclusive lower bound at exact key", func(t *testing.T) {
		// >= 0x20 should include key 0x20
		lower := []byte{0x20}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300, 400}, values)
		it.Close()
	})

	t.Run("exclusive lower bound at exact key", func(t *testing.T) {
		// > 0x20 should exclude key 0x20
		lower := []byte{0x20}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: false})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{300, 400}, values)
		it.Close()
	})

	t.Run("inclusive upper bound at exact key", func(t *testing.T) {
		// <= 0x30 should include key 0x30
		upper := []byte{0x30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{100, 200, 300}, values)
		it.Close()
	})

	t.Run("exclusive upper bound at exact key", func(t *testing.T) {
		// < 0x30 should exclude key 0x30
		upper := []byte{0x30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{100, 200}, values)
		it.Close()
	})

	t.Run("both bounds inclusive at different keys", func(t *testing.T) {
		// >= 0x20 AND <= 0x30
		lower := []byte{0x20}
		upper := []byte{0x30}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300}, values)
		it.Close()
	})

	t.Run("both bounds exclusive at different keys", func(t *testing.T) {
		// > 0x10 AND < 0x40
		lower := []byte{0x10}
		upper := []byte{0x40}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: false, UpperInclusive: false})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300}, values)
		it.Close()
	})

	t.Run("lower exclusive upper inclusive", func(t *testing.T) {
		// > 0x10 AND <= 0x30
		lower := []byte{0x10}
		upper := []byte{0x30}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: false, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300}, values)
		it.Close()
	})

	t.Run("lower inclusive upper exclusive", func(t *testing.T) {
		// >= 0x20 AND < 0x40
		lower := []byte{0x20}
		upper := []byte{0x40}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		assert.Equal(t, []uint64{200, 300}, values)
		it.Close()
	})
}

// TestPointLookupBounds tests the edge case where lower and upper bounds are the same key.
// This simulates a point lookup via range scan (WHERE x = value).
func TestPointLookupBounds(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a simple leaf node
	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0x42}
	root.value = uint64(999)
	art.root = root

	t.Run("point lookup with both inclusive - single result", func(t *testing.T) {
		// WHERE x = 0x42 (both inclusive)
		bound := []byte{0x42}
		it := art.RangeScan(bound, bound, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		key, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x42}, key)
		assert.Equal(t, uint64(999), value)

		// No more results
		_, _, ok = it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("point lookup with lower exclusive - empty result", func(t *testing.T) {
		// x > 0x42 AND x <= 0x42 - impossible, empty range
		bound := []byte{0x42}
		it := art.RangeScan(bound, bound, RangeScanOptions{LowerInclusive: false, UpperInclusive: true})

		// RangeScan should detect this and return exhausted iterator
		assert.True(t, it.IsExhausted())

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("point lookup with upper exclusive - empty result", func(t *testing.T) {
		// x >= 0x42 AND x < 0x42 - impossible, empty range
		bound := []byte{0x42}
		it := art.RangeScan(bound, bound, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		// RangeScan should detect this and return exhausted iterator
		assert.True(t, it.IsExhausted())

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("point lookup with both exclusive - empty result", func(t *testing.T) {
		// x > 0x42 AND x < 0x42 - impossible, empty range
		bound := []byte{0x42}
		it := art.RangeScan(bound, bound, RangeScanOptions{LowerInclusive: false, UpperInclusive: false})

		// RangeScan should detect this and return exhausted iterator
		assert.True(t, it.IsExhausted())

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("point lookup for non-existent key - no result", func(t *testing.T) {
		// WHERE x = 0x50 (key doesn't exist)
		bound := []byte{0x50}
		it := art.RangeScan(bound, bound, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		// Iterator not exhausted (range is valid), but no key found
		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})
}

// TestBETWEENPredicates tests BETWEEN a AND b which translates to >= a AND <= b.
// This is a very common SQL pattern and must work correctly.
func TestBETWEENPredicates(t *testing.T) {
	// Build a tree with multiple keys
	art := NewART(dukdb.TYPE_BIGINT)

	root := newNode(NodeType4)
	root.prefix = nil

	// Create leaves at key bytes: 5, 10, 15, 20, 25
	keys := []byte{5, 10, 15, 20, 25}
	children := make([]*ARTNode, len(keys))
	for i, k := range keys {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(k * 10) // Values: 50, 100, 150, 200, 250
		children[i] = leaf
	}
	root.keys = keys
	root.children = children
	art.root = root

	t.Run("BETWEEN 10 AND 20", func(t *testing.T) {
		// WHERE x BETWEEN 10 AND 20 means >= 10 AND <= 20
		lower := []byte{10}
		upper := []byte{20}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should include keys 10, 15, 20 with values 100, 150, 200
		assert.Equal(t, []uint64{100, 150, 200}, values)
		it.Close()
	})

	t.Run("BETWEEN 7 AND 12", func(t *testing.T) {
		// WHERE x BETWEEN 7 AND 12 means >= 7 AND <= 12
		// Keys in range: 10
		lower := []byte{7}
		upper := []byte{12}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should include only key 10 with value 100
		assert.Equal(t, []uint64{100}, values)
		it.Close()
	})

	t.Run("BETWEEN 5 AND 25 - full range", func(t *testing.T) {
		// WHERE x BETWEEN 5 AND 25 - includes all keys
		lower := []byte{5}
		upper := []byte{25}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should include all keys: 5, 10, 15, 20, 25
		assert.Equal(t, []uint64{50, 100, 150, 200, 250}, values)
		it.Close()
	})

	t.Run("BETWEEN 0 AND 3 - no keys in range", func(t *testing.T) {
		// WHERE x BETWEEN 0 AND 3 - no keys exist in this range
		lower := []byte{0}
		upper := []byte{3}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return empty
		assert.Empty(t, values)
		it.Close()
	})

	t.Run("BETWEEN 30 AND 40 - beyond all keys", func(t *testing.T) {
		// WHERE x BETWEEN 30 AND 40 - beyond all keys
		lower := []byte{30}
		upper := []byte{40}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}

		// Should return empty
		assert.Empty(t, values)
		it.Close()
	})

	t.Run("NOT BETWEEN simulation with two scans", func(t *testing.T) {
		// NOT BETWEEN can be simulated with: < lower OR > upper
		// This test demonstrates that separate scans can achieve NOT BETWEEN
		lower := []byte{10}
		upper := []byte{20}

		// Scan 1: < 10
		it1 := art.RangeScan(nil, lower, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})
		var values1 []uint64
		for {
			_, value, ok := it1.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values1 = append(values1, v)
			}
		}
		it1.Close()

		// Scan 2: > 20
		it2 := art.RangeScan(upper, nil, RangeScanOptions{LowerInclusive: false, UpperInclusive: true})
		var values2 []uint64
		for {
			_, value, ok := it2.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values2 = append(values2, v)
			}
		}
		it2.Close()

		// Combined results: keys 5 (< 10) and 25 (> 20)
		assert.Equal(t, []uint64{50}, values1)
		assert.Equal(t, []uint64{250}, values2)
	})
}

// TestBoundaryConditionsAtKeyBoundaries tests boundary conditions at the exact key boundaries.
func TestBoundaryConditionsAtKeyBoundaries(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Single leaf at key 0x80
	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0x80}
	root.value = uint64(128)
	art.root = root

	t.Run("exclusive lower just below key", func(t *testing.T) {
		// > 0x7F should include 0x80
		lower := []byte{0x7F}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: false})

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(128), value)
		it.Close()
	})

	t.Run("inclusive lower at key", func(t *testing.T) {
		// >= 0x80 should include 0x80
		lower := []byte{0x80}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: true})

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(128), value)
		it.Close()
	})

	t.Run("exclusive lower at key", func(t *testing.T) {
		// > 0x80 should NOT include 0x80
		lower := []byte{0x80}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: false})

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("exclusive upper just above key", func(t *testing.T) {
		// < 0x81 should include 0x80
		upper := []byte{0x81}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(128), value)
		it.Close()
	})

	t.Run("inclusive upper at key", func(t *testing.T) {
		// <= 0x80 should include 0x80
		upper := []byte{0x80}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(128), value)
		it.Close()
	})

	t.Run("exclusive upper at key", func(t *testing.T) {
		// < 0x80 should NOT include 0x80
		upper := []byte{0x80}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})
}

// TestRangePredicateOperators tests the specific SQL operators: <, >, <=, >=
func TestRangePredicateOperators(t *testing.T) {
	// Build tree with keys: 10, 20, 30, 40, 50
	art := NewART(dukdb.TYPE_BIGINT)

	root := newNode(NodeType4)
	root.prefix = nil

	keys := []byte{10, 20, 30, 40}
	children := make([]*ARTNode, len(keys))
	for i, k := range keys {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(k)
		children[i] = leaf
	}
	root.keys = keys
	root.children = children
	art.root = root

	collectValues := func(it *ARTIterator) []uint64 {
		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		it.Close()
		return values
	}

	t.Run("WHERE x > 20", func(t *testing.T) {
		// > 20 means lowerInclusive=false, lower=20, no upper
		lower := []byte{20}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: false})
		values := collectValues(it)
		assert.Equal(t, []uint64{30, 40}, values)
	})

	t.Run("WHERE x >= 20", func(t *testing.T) {
		// >= 20 means lowerInclusive=true, lower=20, no upper
		lower := []byte{20}
		it := art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: true})
		values := collectValues(it)
		assert.Equal(t, []uint64{20, 30, 40}, values)
	})

	t.Run("WHERE x < 30", func(t *testing.T) {
		// < 30 means upperInclusive=false, upper=30, no lower
		upper := []byte{30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})
		values := collectValues(it)
		assert.Equal(t, []uint64{10, 20}, values)
	})

	t.Run("WHERE x <= 30", func(t *testing.T) {
		// <= 30 means upperInclusive=true, upper=30, no lower
		upper := []byte{30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})
		values := collectValues(it)
		assert.Equal(t, []uint64{10, 20, 30}, values)
	})

	t.Run("WHERE x > 10 AND x < 40", func(t *testing.T) {
		// Combined: lowerInclusive=false, upperInclusive=false
		lower := []byte{10}
		upper := []byte{40}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: false, UpperInclusive: false})
		values := collectValues(it)
		assert.Equal(t, []uint64{20, 30}, values)
	})

	t.Run("WHERE x >= 10 AND x <= 40", func(t *testing.T) {
		// Combined: both inclusive (like BETWEEN)
		lower := []byte{10}
		upper := []byte{40}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})
		values := collectValues(it)
		assert.Equal(t, []uint64{10, 20, 30, 40}, values)
	})

	t.Run("WHERE x > 15 AND x < 35", func(t *testing.T) {
		// Bounds between keys: > 15 and < 35 should get 20, 30
		lower := []byte{15}
		upper := []byte{35}
		it := art.RangeScan(lower, upper, RangeScanOptions{LowerInclusive: false, UpperInclusive: false})
		values := collectValues(it)
		assert.Equal(t, []uint64{20, 30}, values)
	})
}

// =============================================================================
// Composite Key Encoding Tests (Task 3.5)
// =============================================================================

func TestEncodeCompositeKey(t *testing.T) {
	t.Run("single integer value", func(t *testing.T) {
		key, err := EncodeCompositeKey(
			[]any{int32(42)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
		)
		require.NoError(t, err)
		assert.Equal(t, 4, len(key)) // Integer is 4 bytes
	})

	t.Run("two integers", func(t *testing.T) {
		key, err := EncodeCompositeKey(
			[]any{int32(5), int32(10)},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		)
		require.NoError(t, err)
		assert.Equal(t, 8, len(key)) // 4 + 4 bytes
	})

	t.Run("integer and string", func(t *testing.T) {
		key, err := EncodeCompositeKey(
			[]any{int32(5), "hello"},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
		)
		require.NoError(t, err)
		// Integer (4) + string "hello" (5) + null terminator (1) = 10 bytes
		assert.Equal(t, 10, len(key))
	})

	t.Run("three column composite", func(t *testing.T) {
		key, err := EncodeCompositeKey(
			[]any{int32(1), "abc", int64(100)},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT},
		)
		require.NoError(t, err)
		// Integer (4) + string "abc" (3) + null (1) + bigint (8) = 16 bytes
		assert.Equal(t, 16, len(key))
	})

	t.Run("mismatched lengths returns error", func(t *testing.T) {
		_, err := EncodeCompositeKey(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not match")
	})

	t.Run("wrong type returns error", func(t *testing.T) {
		_, err := EncodeCompositeKey(
			[]any{"not an int"},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
		)
		assert.Error(t, err)
	})
}

func TestDecodeCompositeKey(t *testing.T) {
	t.Run("single integer round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		original := []any{int32(42)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, int32(42), decoded[0])
	})

	t.Run("two integers round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		original := []any{int32(5), int32(10)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 2, len(decoded))
		assert.Equal(t, int32(5), decoded[0])
		assert.Equal(t, int32(10), decoded[1])
	})

	t.Run("integer and string round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		original := []any{int32(5), "hello"}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 2, len(decoded))
		assert.Equal(t, int32(5), decoded[0])
		assert.Equal(t, "hello", decoded[1])
	})

	t.Run("three column composite round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}
		original := []any{int32(1), "abc", int64(100)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 3, len(decoded))
		assert.Equal(t, int32(1), decoded[0])
		assert.Equal(t, "abc", decoded[1])
		assert.Equal(t, int64(100), decoded[2])
	})

	t.Run("key too short returns error", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		shortKey := make([]byte, 4) // Only enough for one integer

		_, err := DecodeCompositeKey(shortKey, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})
}

func TestCompositeKeyEncodingSortOrder(t *testing.T) {
	t.Run("composite keys sort correctly by first column", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}

		key1, err := EncodeCompositeKey([]any{int32(1), int32(100)}, types)
		require.NoError(t, err)
		key2, err := EncodeCompositeKey([]any{int32(2), int32(50)}, types)
		require.NoError(t, err)
		key3, err := EncodeCompositeKey([]any{int32(3), int32(1)}, types)
		require.NoError(t, err)

		// key1 < key2 < key3 (regardless of second column)
		assert.True(t, bytes.Compare(key1, key2) < 0)
		assert.True(t, bytes.Compare(key2, key3) < 0)
	})

	t.Run("composite keys sort correctly by second column when first is equal", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}

		key1, err := EncodeCompositeKey([]any{int32(5), int32(10)}, types)
		require.NoError(t, err)
		key2, err := EncodeCompositeKey([]any{int32(5), int32(20)}, types)
		require.NoError(t, err)
		key3, err := EncodeCompositeKey([]any{int32(5), int32(30)}, types)
		require.NoError(t, err)

		// Same first column: sort by second column
		assert.True(t, bytes.Compare(key1, key2) < 0)
		assert.True(t, bytes.Compare(key2, key3) < 0)
	})

	t.Run("integer and string composite preserves sort order", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		key1, err := EncodeCompositeKey([]any{int32(1), "zebra"}, types)
		require.NoError(t, err)
		key2, err := EncodeCompositeKey([]any{int32(2), "apple"}, types)
		require.NoError(t, err)

		// First column takes precedence
		assert.True(t, bytes.Compare(key1, key2) < 0)
	})

	t.Run("same first column string sort by second", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		key1, err := EncodeCompositeKey([]any{int32(5), "apple"}, types)
		require.NoError(t, err)
		key2, err := EncodeCompositeKey([]any{int32(5), "banana"}, types)
		require.NoError(t, err)
		key3, err := EncodeCompositeKey([]any{int32(5), "cherry"}, types)
		require.NoError(t, err)

		assert.True(t, bytes.Compare(key1, key2) < 0)
		assert.True(t, bytes.Compare(key2, key3) < 0)
	})

	t.Run("negative integers sort correctly in composite", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}

		keyNeg, err := EncodeCompositeKey([]any{int32(-10), int32(5)}, types)
		require.NoError(t, err)
		keyZero, err := EncodeCompositeKey([]any{int32(0), int32(5)}, types)
		require.NoError(t, err)
		keyPos, err := EncodeCompositeKey([]any{int32(10), int32(5)}, types)
		require.NoError(t, err)

		// -10 < 0 < 10
		assert.True(t, bytes.Compare(keyNeg, keyZero) < 0)
		assert.True(t, bytes.Compare(keyZero, keyPos) < 0)
	})
}

func TestComputePrefixUpperBound(t *testing.T) {
	t.Run("simple increment", func(t *testing.T) {
		prefix := []byte{0x80, 0x00, 0x05}
		upper := ComputePrefixUpperBound(prefix)
		assert.Equal(t, []byte{0x80, 0x00, 0x06}, upper)
	})

	t.Run("carry over from 0xFF", func(t *testing.T) {
		prefix := []byte{0x80, 0xFF}
		upper := ComputePrefixUpperBound(prefix)
		assert.Equal(t, []byte{0x81}, upper)
	})

	t.Run("multiple 0xFF bytes carry", func(t *testing.T) {
		prefix := []byte{0x01, 0xFF, 0xFF}
		upper := ComputePrefixUpperBound(prefix)
		assert.Equal(t, []byte{0x02}, upper)
	})

	t.Run("all 0xFF returns nil", func(t *testing.T) {
		prefix := []byte{0xFF, 0xFF, 0xFF}
		upper := ComputePrefixUpperBound(prefix)
		assert.Nil(t, upper)
	})

	t.Run("empty prefix returns nil", func(t *testing.T) {
		prefix := []byte{}
		upper := ComputePrefixUpperBound(prefix)
		assert.Nil(t, upper)
	})

	t.Run("single byte", func(t *testing.T) {
		prefix := []byte{0x42}
		upper := ComputePrefixUpperBound(prefix)
		assert.Equal(t, []byte{0x43}, upper)
	})
}

func TestCompositeRangeBounds(t *testing.T) {
	t.Run("prefix with range on second column", func(t *testing.T) {
		// Index on (a INT, b INT)
		// Query: WHERE a = 5 AND b BETWEEN 10 AND 20
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
			int32(10), int32(20),
			dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		// Lower should be encode(5) + encode(10)
		expectedLower, _ := EncodeCompositeKey([]any{int32(5), int32(10)}, []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER})
		assert.Equal(t, expectedLower, lower)

		// Upper should be encode(5) + encode(20)
		// Note: CompositeRangeBounds encodes rangeLower (10) and rangeUpper (20) directly,
		// so upper = prefix + encode(20)
		expectedUpper, _ := EncodeCompositeKey([]any{int32(5), int32(20)}, []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER})
		// The actual value will be the range upper (20) appended to prefix
		assert.Equal(t, expectedUpper, upper)
	})

	t.Run("prefix only (any value for second column)", func(t *testing.T) {
		// Index on (a INT, b INT)
		// Query: WHERE a = 5 (match any b value)
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
			nil, nil,
			dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		// Lower should be encode(5)
		expectedLower, _ := EncodeCompositeKey([]any{int32(5)}, []dukdb.Type{dukdb.TYPE_INTEGER})
		assert.Equal(t, expectedLower, lower)

		// Upper should be prefix upper bound of encode(5)
		expectedUpper := ComputePrefixUpperBound(expectedLower)
		assert.Equal(t, expectedUpper, upper)
	})

	t.Run("unbounded lower on range column", func(t *testing.T) {
		// Index on (a INT, b INT)
		// Query: WHERE a = 5 AND b <= 20
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
			nil, int32(20),
			dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		// Lower should just be encode(5) (prefix)
		expectedLower, _ := EncodeCompositeKey([]any{int32(5)}, []dukdb.Type{dukdb.TYPE_INTEGER})
		assert.Equal(t, expectedLower, lower)

		// Upper should be encode(5) + encode(20)
		expectedUpper, _ := EncodeCompositeKey([]any{int32(5), int32(20)}, []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER})
		assert.Equal(t, expectedUpper, upper)
	})

	t.Run("unbounded upper on range column", func(t *testing.T) {
		// Index on (a INT, b INT)
		// Query: WHERE a = 5 AND b >= 10
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
			int32(10), nil,
			dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		// Lower should be encode(5) + encode(10)
		expectedLower, _ := EncodeCompositeKey([]any{int32(5), int32(10)}, []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER})
		assert.Equal(t, expectedLower, lower)

		// Upper should be prefix upper bound of encode(5)
		prefix, _ := EncodeCompositeKey([]any{int32(5)}, []dukdb.Type{dukdb.TYPE_INTEGER})
		expectedUpper := ComputePrefixUpperBound(prefix)
		assert.Equal(t, expectedUpper, upper)
	})
}

// =============================================================================
// Composite Key Range Scan Integration Tests (Task 3.5)
// =============================================================================

func TestCompositeKeyRangeScan(t *testing.T) {
	// Build a simpler tree for testing composite key iteration
	// We'll manually construct an ART that represents composite keys

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}

	t.Run("full composite key range scan", func(t *testing.T) {
		// Test that encoded composite keys work with basic iteration
		key1, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key2, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key3, _ := EncodeCompositeKey([]any{int32(2), int32(10)}, types)

		// Verify lexicographic ordering
		assert.True(t, bytes.Compare(key1, key2) < 0, "key(1,10) should be < key(1,20)")
		assert.True(t, bytes.Compare(key2, key3) < 0, "key(1,20) should be < key(2,10)")
	})

	t.Run("prefix range for a=1 should include (1,10), (1,20), (1,30)", func(t *testing.T) {
		// Encode prefix for a=1
		prefixTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
		prefix, _ := EncodeCompositeKey([]any{int32(1)}, prefixTypes)

		// Encode some keys
		key1_10, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key1_20, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key1_30, _ := EncodeCompositeKey([]any{int32(1), int32(30)}, types)
		key2_10, _ := EncodeCompositeKey([]any{int32(2), int32(10)}, types)

		upper := ComputePrefixUpperBound(prefix)

		// All keys with a=1 should be >= prefix and < upper
		assert.True(t, bytes.Compare(key1_10, prefix) >= 0)
		assert.True(t, bytes.Compare(key1_20, prefix) >= 0)
		assert.True(t, bytes.Compare(key1_30, prefix) >= 0)

		if upper != nil {
			assert.True(t, bytes.Compare(key1_10, upper) < 0)
			assert.True(t, bytes.Compare(key1_20, upper) < 0)
			assert.True(t, bytes.Compare(key1_30, upper) < 0)
			// key2_10 should be >= upper (a=2 > a=1)
			assert.True(t, bytes.Compare(key2_10, upper) >= 0)
		}
	})

	t.Run("range on second column: a=1 AND b BETWEEN 15 AND 25", func(t *testing.T) {
		// Lower: (1, 15), Upper: (1, 25)
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(1)}, []dukdb.Type{dukdb.TYPE_INTEGER},
			int32(15), int32(25), dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		key1_10, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key1_20, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key1_30, _ := EncodeCompositeKey([]any{int32(1), int32(30)}, types)

		// key1_10 (1, 10) should be < lower (1, 15)
		assert.True(t, bytes.Compare(key1_10, lower) < 0, "key(1,10) should be before range [15,25]")

		// key1_20 (1, 20) should be in range
		assert.True(t, bytes.Compare(key1_20, lower) >= 0, "key(1,20) should be >= lower")
		assert.True(t, bytes.Compare(key1_20, upper) <= 0, "key(1,20) should be <= upper")

		// key1_30 (1, 30) should be > upper (1, 25)
		assert.True(t, bytes.Compare(key1_30, upper) > 0, "key(1,30) should be after range [15,25]")
	})
}

// TestCompositeKeyIteratorIntegration tests that the ARTIterator correctly handles
// composite encoded keys during range scans.
func TestCompositeKeyIteratorIntegration(t *testing.T) {
	// Create an ART with a single leaf containing a composite key
	art := NewART(dukdb.TYPE_BIGINT)

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
	compositeKey, _ := EncodeCompositeKey([]any{int32(5), int32(100)}, types)

	// Insert as a single leaf
	leaf := newNode(NodeTypeLeaf)
	leaf.prefix = compositeKey
	leaf.value = uint64(500)
	art.root = leaf

	t.Run("scan all finds composite key", func(t *testing.T) {
		it := art.ScanAll()
		key, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, compositeKey, key)
		assert.Equal(t, uint64(500), value)

		_, _, ok = it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("lower bound at composite key inclusive finds it", func(t *testing.T) {
		it := art.RangeScan(compositeKey, nil, RangeScanOptions{LowerInclusive: true})
		key, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, compositeKey, key)
		assert.Equal(t, uint64(500), value)
		it.Close()
	})

	t.Run("lower bound at composite key exclusive skips it", func(t *testing.T) {
		it := art.RangeScan(compositeKey, nil, RangeScanOptions{LowerInclusive: false})
		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("upper bound at composite key inclusive finds it", func(t *testing.T) {
		it := art.RangeScan(nil, compositeKey, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})
		key, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, compositeKey, key)
		assert.Equal(t, uint64(500), value)
		it.Close()
	})

	t.Run("upper bound at composite key exclusive skips it", func(t *testing.T) {
		it := art.RangeScan(nil, compositeKey, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})
		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("prefix scan with encoded prefix", func(t *testing.T) {
		// Prefix for a=5
		prefixTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
		prefix, _ := EncodeCompositeKey([]any{int32(5)}, prefixTypes)
		upper := ComputePrefixUpperBound(prefix)

		// The composite key (5, 100) should be in range [prefix, upper)
		it := art.RangeScan(prefix, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})
		key, value, ok := it.Next()
		assert.True(t, ok, "Should find key (5, 100) in prefix range for a=5")
		assert.Equal(t, compositeKey, key)
		assert.Equal(t, uint64(500), value)
		it.Close()
	})

	t.Run("different prefix finds nothing", func(t *testing.T) {
		// Prefix for a=6
		prefixTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
		prefix, _ := EncodeCompositeKey([]any{int32(6)}, prefixTypes)
		upper := ComputePrefixUpperBound(prefix)

		// The composite key (5, 100) should NOT be in range for a=6
		it := art.RangeScan(prefix, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})
		_, _, ok := it.Next()
		assert.False(t, ok, "Should not find key (5, 100) in prefix range for a=6")
		it.Close()
	})
}

// TestMultipleCompositeKeysRangeScan tests range scans across multiple composite keys.
//
// NOTE: The current ART.Insert() is a stub that only stores the last value.
// A full ART implementation with proper node splitting is beyond the scope
// of this task (task 3.5: composite key encoding). This test validates:
// 1. Composite keys encode correctly with proper sort order
// 2. The iterator works with the encoding format
// 3. Bounds calculation for composite range scans is correct
//
// For proper multi-key iteration tests, see TestCompositeKeyIteratorIntegration
// which uses manually constructed trees or single-key trees.
func TestMultipleCompositeKeysRangeScan(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}

	t.Run("composite key encoding produces correct sort order for multi-key scenarios", func(t *testing.T) {
		// Verify that encoded composite keys maintain proper lexicographic order
		// This is critical for range scans to work correctly
		key1_10, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key1_20, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key2_10, _ := EncodeCompositeKey([]any{int32(2), int32(10)}, types)
		key2_20, _ := EncodeCompositeKey([]any{int32(2), int32(20)}, types)

		// Verify sort order: (1,10) < (1,20) < (2,10) < (2,20)
		assert.True(t, bytes.Compare(key1_10, key1_20) < 0, "key(1,10) should be < key(1,20)")
		assert.True(t, bytes.Compare(key1_20, key2_10) < 0, "key(1,20) should be < key(2,10)")
		assert.True(t, bytes.Compare(key2_10, key2_20) < 0, "key(2,10) should be < key(2,20)")
	})

	t.Run("prefix bounds correctly isolate first column", func(t *testing.T) {
		prefixTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
		prefix1, _ := EncodeCompositeKey([]any{int32(1)}, prefixTypes)
		upper1 := ComputePrefixUpperBound(prefix1)

		key1_10, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key1_20, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key2_10, _ := EncodeCompositeKey([]any{int32(2), int32(10)}, types)

		// Keys with a=1 should be in range [prefix1, upper1)
		assert.True(t, bytes.Compare(key1_10, prefix1) >= 0, "key(1,10) should be >= prefix(1)")
		assert.True(t, bytes.Compare(key1_20, prefix1) >= 0, "key(1,20) should be >= prefix(1)")
		if upper1 != nil {
			assert.True(t, bytes.Compare(key1_10, upper1) < 0, "key(1,10) should be < upper(1)")
			assert.True(t, bytes.Compare(key1_20, upper1) < 0, "key(1,20) should be < upper(1)")
			assert.True(t, bytes.Compare(key2_10, upper1) >= 0, "key(2,10) should be >= upper(1)")
		}
	})

	t.Run("composite range bounds for second column work correctly", func(t *testing.T) {
		// Simulate WHERE a = 1 AND b >= 15 AND b <= 25
		lower, upper, err := CompositeRangeBounds(
			[]any{int32(1)}, []dukdb.Type{dukdb.TYPE_INTEGER},
			int32(15), int32(25), dukdb.TYPE_INTEGER,
			true, true,
		)
		require.NoError(t, err)

		key1_10, _ := EncodeCompositeKey([]any{int32(1), int32(10)}, types)
		key1_15, _ := EncodeCompositeKey([]any{int32(1), int32(15)}, types)
		key1_20, _ := EncodeCompositeKey([]any{int32(1), int32(20)}, types)
		key1_25, _ := EncodeCompositeKey([]any{int32(1), int32(25)}, types)
		key1_30, _ := EncodeCompositeKey([]any{int32(1), int32(30)}, types)

		// key(1,10) should be < lower(1,15)
		assert.True(t, bytes.Compare(key1_10, lower) < 0, "key(1,10) should be < lower(1,15)")
		// key(1,15) should be == lower
		assert.Equal(t, lower, key1_15, "lower bound should equal key(1,15)")
		// key(1,20) should be in range
		assert.True(t, bytes.Compare(key1_20, lower) >= 0, "key(1,20) should be >= lower")
		assert.True(t, bytes.Compare(key1_20, upper) <= 0, "key(1,20) should be <= upper")
		// key(1,25) should be == upper
		assert.Equal(t, upper, key1_25, "upper bound should equal key(1,25)")
		// key(1,30) should be > upper
		assert.True(t, bytes.Compare(key1_30, upper) > 0, "key(1,30) should be > upper(1,25)")
	})

	// NOTE: Tests for composite keys with ART.Insert() are not included here because
	// the current ART.Insert() applies additional type-specific encoding (encodeKey)
	// which double-encodes already-encoded composite keys. This is a limitation of
	// the current ART implementation, not the composite key encoding.
	//
	// For integration tests with proper tree construction, see TestCompositeKeyIteratorIntegration
	// which manually constructs trees with pre-encoded composite keys.
}

// TestEncodeValueTypes tests the encodeValue function with various types.
func TestEncodeValueTypes(t *testing.T) {
	t.Run("boolean true", func(t *testing.T) {
		encoded, err := encodeValue(true, dukdb.TYPE_BOOLEAN)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded))
	})

	t.Run("boolean false", func(t *testing.T) {
		encoded, err := encodeValue(false, dukdb.TYPE_BOOLEAN)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded))
	})

	t.Run("tinyint", func(t *testing.T) {
		encoded, err := encodeValue(int8(42), dukdb.TYPE_TINYINT)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded))
	})

	t.Run("smallint", func(t *testing.T) {
		encoded, err := encodeValue(int16(1000), dukdb.TYPE_SMALLINT)
		require.NoError(t, err)
		assert.Equal(t, 2, len(encoded))
	})

	t.Run("integer", func(t *testing.T) {
		encoded, err := encodeValue(int32(100000), dukdb.TYPE_INTEGER)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded))
	})

	t.Run("bigint", func(t *testing.T) {
		encoded, err := encodeValue(int64(10000000000), dukdb.TYPE_BIGINT)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})

	t.Run("varchar", func(t *testing.T) {
		encoded, err := encodeValue("hello", dukdb.TYPE_VARCHAR)
		require.NoError(t, err)
		assert.Equal(t, 6, len(encoded)) // 5 chars + null terminator
	})

	t.Run("empty varchar", func(t *testing.T) {
		encoded, err := encodeValue("", dukdb.TYPE_VARCHAR)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded)) // Just null terminator
	})

	t.Run("integer type coercion from int", func(t *testing.T) {
		encoded, err := encodeValue(int(42), dukdb.TYPE_INTEGER)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded))
	})

	t.Run("nil value", func(t *testing.T) {
		encoded, err := encodeValue(nil, dukdb.TYPE_INTEGER)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded)) // NULL marker
		assert.Equal(t, byte(0x00), encoded[0])
	})

	t.Run("float", func(t *testing.T) {
		encoded, err := encodeValue(float32(3.14), dukdb.TYPE_FLOAT)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded))
	})

	t.Run("double", func(t *testing.T) {
		encoded, err := encodeValue(float64(3.14159265359), dukdb.TYPE_DOUBLE)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})
}

// =============================================================================
// Additional Test Coverage for Task 3.6
// =============================================================================

// TestEncodeCompositeKeyPrefix tests the EncodeCompositeKeyPrefix function.
func TestEncodeCompositeKeyPrefix(t *testing.T) {
	t.Run("single column prefix", func(t *testing.T) {
		prefix, err := EncodeCompositeKeyPrefix(
			[]any{int32(5)},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
		)
		require.NoError(t, err)
		assert.Equal(t, 4, len(prefix))
	})

	t.Run("two column prefix", func(t *testing.T) {
		prefix, err := EncodeCompositeKeyPrefix(
			[]any{int32(5), "hello"},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
		)
		require.NoError(t, err)
		// 4 bytes integer + 5 chars + null terminator = 10 bytes
		assert.Equal(t, 10, len(prefix))
	})

	t.Run("empty prefix", func(t *testing.T) {
		prefix, err := EncodeCompositeKeyPrefix(
			[]any{},
			[]dukdb.Type{},
		)
		require.NoError(t, err)
		assert.Equal(t, 0, len(prefix))
	})
}

// TestEncodeValueAdditionalTypes tests more type encodings for encodeValue.
func TestEncodeValueAdditionalTypes(t *testing.T) {
	t.Run("utinyint", func(t *testing.T) {
		encoded, err := encodeValue(uint8(255), dukdb.TYPE_UTINYINT)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded))
	})

	t.Run("usmallint", func(t *testing.T) {
		encoded, err := encodeValue(uint16(65535), dukdb.TYPE_USMALLINT)
		require.NoError(t, err)
		assert.Equal(t, 2, len(encoded))
	})

	t.Run("uinteger", func(t *testing.T) {
		encoded, err := encodeValue(uint32(4294967295), dukdb.TYPE_UINTEGER)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded))
	})

	t.Run("ubigint", func(t *testing.T) {
		encoded, err := encodeValue(uint64(18446744073709551615), dukdb.TYPE_UBIGINT)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})

	t.Run("blob", func(t *testing.T) {
		encoded, err := encodeValue([]byte{0x01, 0x02, 0x03}, dukdb.TYPE_BLOB)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded)) // 3 bytes + null terminator
	})

	t.Run("date", func(t *testing.T) {
		encoded, err := encodeValue(int32(19000), dukdb.TYPE_DATE)
		require.NoError(t, err)
		assert.Equal(t, 4, len(encoded))
	})

	t.Run("time", func(t *testing.T) {
		encoded, err := encodeValue(int64(123456789), dukdb.TYPE_TIME)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})

	t.Run("timestamp", func(t *testing.T) {
		encoded, err := encodeValue(int64(1234567890000), dukdb.TYPE_TIMESTAMP)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})

	t.Run("varchar from bytes", func(t *testing.T) {
		encoded, err := encodeValue([]byte("hello"), dukdb.TYPE_VARCHAR)
		require.NoError(t, err)
		assert.Equal(t, 6, len(encoded)) // 5 chars + null
	})

	t.Run("tinyint coercion", func(t *testing.T) {
		encoded, err := encodeValue(int16(42), dukdb.TYPE_TINYINT)
		require.NoError(t, err)
		assert.Equal(t, 1, len(encoded))
	})

	t.Run("smallint coercion", func(t *testing.T) {
		encoded, err := encodeValue(int32(1000), dukdb.TYPE_SMALLINT)
		require.NoError(t, err)
		assert.Equal(t, 2, len(encoded))
	})

	t.Run("bigint coercion from int32", func(t *testing.T) {
		encoded, err := encodeValue(int32(1000), dukdb.TYPE_BIGINT)
		require.NoError(t, err)
		assert.Equal(t, 8, len(encoded))
	})
}

// TestEncodeValueErrors tests error cases for encodeValue.
func TestEncodeValueErrors(t *testing.T) {
	t.Run("wrong type for boolean", func(t *testing.T) {
		_, err := encodeValue("not a bool", dukdb.TYPE_BOOLEAN)
		assert.Error(t, err)
	})

	t.Run("wrong type for integer", func(t *testing.T) {
		_, err := encodeValue("not an int", dukdb.TYPE_INTEGER)
		assert.Error(t, err)
	})

	t.Run("wrong type for float", func(t *testing.T) {
		_, err := encodeValue("not a float", dukdb.TYPE_FLOAT)
		assert.Error(t, err)
	})

	t.Run("wrong type for double", func(t *testing.T) {
		_, err := encodeValue("not a double", dukdb.TYPE_DOUBLE)
		assert.Error(t, err)
	})

	t.Run("wrong type for utinyint", func(t *testing.T) {
		_, err := encodeValue("not a uint8", dukdb.TYPE_UTINYINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for usmallint", func(t *testing.T) {
		_, err := encodeValue("not a uint16", dukdb.TYPE_USMALLINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for uinteger", func(t *testing.T) {
		_, err := encodeValue("not a uint32", dukdb.TYPE_UINTEGER)
		assert.Error(t, err)
	})

	t.Run("wrong type for ubigint", func(t *testing.T) {
		_, err := encodeValue("not a uint64", dukdb.TYPE_UBIGINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for blob", func(t *testing.T) {
		_, err := encodeValue("not a blob", dukdb.TYPE_BLOB)
		assert.Error(t, err)
	})

	t.Run("wrong type for date", func(t *testing.T) {
		_, err := encodeValue("not a date", dukdb.TYPE_DATE)
		assert.Error(t, err)
	})

	t.Run("wrong type for time", func(t *testing.T) {
		_, err := encodeValue("not a time", dukdb.TYPE_TIME)
		assert.Error(t, err)
	})

	t.Run("wrong type for tinyint", func(t *testing.T) {
		_, err := encodeValue([]byte{}, dukdb.TYPE_TINYINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for smallint", func(t *testing.T) {
		_, err := encodeValue([]byte{}, dukdb.TYPE_SMALLINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for bigint", func(t *testing.T) {
		_, err := encodeValue([]byte{}, dukdb.TYPE_BIGINT)
		assert.Error(t, err)
	})

	t.Run("wrong type for varchar", func(t *testing.T) {
		_, err := encodeValue(123, dukdb.TYPE_VARCHAR)
		assert.Error(t, err)
	})
}

// TestDecodeCompositeKeyAdditional tests additional decode scenarios.
func TestDecodeCompositeKeyAdditional(t *testing.T) {
	t.Run("boolean round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_BOOLEAN}
		original := []any{true}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, true, decoded[0])
	})

	t.Run("tinyint round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_TINYINT}
		original := []any{int8(42)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, int8(42), decoded[0])
	})

	t.Run("smallint round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_SMALLINT}
		original := []any{int16(1000)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, int16(1000), decoded[0])
	})

	t.Run("float round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_FLOAT}
		original := []any{float32(3.14)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.InDelta(t, float32(3.14), decoded[0].(float32), 0.001)
	})

	t.Run("double round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_DOUBLE}
		original := []any{float64(3.14159)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.InDelta(t, float64(3.14159), decoded[0].(float64), 0.00001)
	})

	t.Run("utinyint round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_UTINYINT}
		original := []any{uint8(200)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, uint8(200), decoded[0])
	})

	t.Run("usmallint round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_USMALLINT}
		original := []any{uint16(50000)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, uint16(50000), decoded[0])
	})

	t.Run("uinteger round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_UINTEGER}
		original := []any{uint32(3000000000)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, uint32(3000000000), decoded[0])
	})

	t.Run("ubigint round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_UBIGINT}
		original := []any{uint64(10000000000000000000)}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, uint64(10000000000000000000), decoded[0])
	})

	t.Run("blob round-trip", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_BLOB}
		original := []any{[]byte{0x01, 0x02, 0x03}}

		encoded, err := EncodeCompositeKey(original, types)
		require.NoError(t, err)

		decoded, err := DecodeCompositeKey(encoded, types)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded))
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded[0])
	})
}

// TestIteratorDeepTreeWithMixedNodes tests iterating through a complex tree structure.
func TestIteratorDeepTreeWithMixedNodes(t *testing.T) {
	// Build a 3-level tree with Node4 -> Node16 -> leaves
	art := NewART(dukdb.TYPE_BIGINT)

	// Level 1: Node4 root
	root := newNode(NodeType4)
	root.prefix = []byte{0x00}

	// Level 2: Two Node16 children
	inner1 := newNode(NodeType16)
	inner1.prefix = []byte{0xAA}

	inner2 := newNode(NodeType16)
	inner2.prefix = []byte{0xBB}

	// Level 3: Add leaves to inner1
	leaves1 := make([]*ARTNode, 5)
	keys1 := make([]byte, 5)
	for i := 0; i < 5; i++ {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(i + 1)
		leaves1[i] = leaf
		keys1[i] = byte(i * 10)
	}
	inner1.keys = keys1
	inner1.children = leaves1

	// Level 3: Add leaves to inner2
	leaves2 := make([]*ARTNode, 3)
	keys2 := make([]byte, 3)
	for i := 0; i < 3; i++ {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(100 + i)
		leaves2[i] = leaf
		keys2[i] = byte(i * 20)
	}
	inner2.keys = keys2
	inner2.children = leaves2

	// Connect inner nodes to root
	root.keys = []byte{0x10, 0x20}
	root.children = []*ARTNode{inner1, inner2}
	art.root = root

	t.Run("scan all traverses entire tree", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		// Should find all 8 leaves: 5 from inner1, 3 from inner2
		assert.Equal(t, 8, len(values))
		// First 5 from inner1
		assert.Equal(t, []uint64{1, 2, 3, 4, 5, 100, 101, 102}, values)
		it.Close()
	})

	t.Run("range scan with lower bound skips first subtree", func(t *testing.T) {
		// Lower bound that skips inner1 (0x10) but includes inner2 (0x20)
		lower := []byte{0x00, 0x15} // After 0x10 child
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		// Should find only leaves from inner2
		assert.Equal(t, []uint64{100, 101, 102}, values)
		it.Close()
	})
}

// TestIteratorNode48Sparse tests iteration through a sparse Node48.
func TestIteratorNode48Sparse(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node48 with widely spaced children
	root := newNode(NodeType48)
	root.prefix = nil

	// Add children at sparse positions: 0, 100, 200, 255
	positions := []int{0, 100, 200, 255}
	children := make([]*ARTNode, len(positions))

	for i, pos := range positions {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(pos)
		children[i] = leaf
		root.keys[pos] = byte(i)
	}
	root.children = children
	art.root = root

	t.Run("scan all finds all sparse keys", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		assert.Equal(t, []uint64{0, 100, 200, 255}, values)
		it.Close()
	})

	t.Run("lower bound between sparse keys", func(t *testing.T) {
		// Start from 50, should skip 0 and find 100, 200, 255
		lower := []byte{50}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		assert.Equal(t, []uint64{100, 200, 255}, values)
		it.Close()
	})
}

// TestIteratorNode256Full tests iteration through a Node256 with many children.
func TestIteratorNode256Full(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node256 with every 10th child populated
	root := newNode(NodeType256)
	root.prefix = nil

	for i := 0; i < 256; i += 10 {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(i)
		root.children[i] = leaf
	}
	art.root = root

	t.Run("scan all finds all children", func(t *testing.T) {
		it := art.ScanAll()
		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		// Should find 26 leaves (0, 10, 20, ..., 250)
		expected := make([]uint64, 26)
		for i := 0; i < 26; i++ {
			expected[i] = uint64(i * 10)
		}
		assert.Equal(t, expected, values)
		it.Close()
	})

	t.Run("range scan in middle", func(t *testing.T) {
		// Range from 55 to 145 should include 60, 70, 80, 90, 100, 110, 120, 130, 140
		lower := []byte{55}
		upper := []byte{145}
		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())

		var values []uint64
		for {
			_, value, ok := it.Next()
			if !ok {
				break
			}
			if v, ok := value.(uint64); ok {
				values = append(values, v)
			}
		}
		expected := []uint64{60, 70, 80, 90, 100, 110, 120, 130, 140}
		assert.Equal(t, expected, values)
		it.Close()
	})
}

// TestIteratorEmptyInnerNode tests handling of an inner node with no valid children.
func TestIteratorEmptyInnerNode(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a Node4 with nil children (invalid but should handle gracefully)
	root := newNode(NodeType4)
	root.prefix = nil
	root.keys = []byte{}
	root.children = []*ARTNode{}
	art.root = root

	it := art.ScanAll()
	_, _, ok := it.Next()
	assert.False(t, ok, "Should not find any keys in empty inner node")
	it.Close()
}

// TestIteratorPrefixComparison tests edge cases in prefix comparison.
func TestIteratorPrefixComparison(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a leaf with a multi-byte prefix
	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0x10, 0x20, 0x30}
	root.value = uint64(123)
	art.root = root

	t.Run("lower bound shorter than prefix - should find", func(t *testing.T) {
		lower := []byte{0x10}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(123), value)
		it.Close()
	})

	t.Run("lower bound longer but less than prefix - should find", func(t *testing.T) {
		lower := []byte{0x10, 0x20, 0x29}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(123), value)
		it.Close()
	})

	t.Run("lower bound longer but greater than prefix - should not find", func(t *testing.T) {
		lower := []byte{0x10, 0x20, 0x31}
		it := art.RangeScan(lower, nil, DefaultRangeScanOptions())

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("upper bound at exact prefix - exclusive should not find", func(t *testing.T) {
		upper := []byte{0x10, 0x20, 0x30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: false})

		_, _, ok := it.Next()
		assert.False(t, ok)
		it.Close()
	})

	t.Run("upper bound at exact prefix - inclusive should find", func(t *testing.T) {
		upper := []byte{0x10, 0x20, 0x30}
		it := art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: true})

		_, value, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, uint64(123), value)
		it.Close()
	})
}

// TestGetChildKeyEdgeCases tests edge cases for getChildKey function.
func TestGetChildKeyEdgeCases(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Test Node4 with specific keys
	root := newNode(NodeType4)
	root.prefix = nil
	leaf := newNode(NodeTypeLeaf)
	leaf.prefix = nil
	leaf.value = uint64(42)

	root.keys = []byte{0xFF} // Maximum byte value
	root.children = []*ARTNode{leaf}
	art.root = root

	it := art.ScanAll()
	key, value, ok := it.Next()
	assert.True(t, ok)
	assert.Equal(t, []byte{0xFF}, key)
	assert.Equal(t, uint64(42), value)
	it.Close()
}

// TestRangeScanMaxResults tests the MaxResults option (even though not fully implemented).
func TestRangeScanMaxResultsOption(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	// Create a tree with multiple leaves
	root := newNode(NodeType4)
	root.prefix = nil

	keys := []byte{0x10, 0x20, 0x30}
	children := make([]*ARTNode, 3)
	for i, k := range keys {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(k)
		children[i] = leaf
	}
	root.keys = keys
	root.children = children
	art.root = root

	// Test that MaxResults option is preserved (even if not enforced yet)
	it := art.RangeScan(nil, nil, RangeScanOptions{
		LowerInclusive: true,
		UpperInclusive: false,
		MaxResults:     2,
	})
	assert.NotNil(t, it)
	it.Close()
}

// TestRangeScanReverseOption tests the Reverse option (even though not fully implemented).
func TestRangeScanReverseOption(t *testing.T) {
	art := NewART(dukdb.TYPE_BIGINT)

	root := newNode(NodeTypeLeaf)
	root.prefix = []byte{0x42}
	root.value = uint64(66)
	art.root = root

	// Test that Reverse option is preserved (even if not enforced yet)
	it := art.RangeScan(nil, nil, RangeScanOptions{
		LowerInclusive: true,
		UpperInclusive: false,
		Reverse:        true,
	})
	assert.NotNil(t, it)
	it.Close()
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkRangeScanAllLeaves(b *testing.B) {
	// Build an ART with 100 leaves
	art := NewART(dukdb.TYPE_BIGINT)

	root := newNode(NodeType256)
	root.prefix = nil

	for i := 0; i < 100; i++ {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(i)
		root.children[i] = leaf
	}
	art.root = root

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := art.ScanAll()
		for {
			_, _, ok := it.Next()
			if !ok {
				break
			}
		}
		it.Close()
	}
}

func BenchmarkRangeScanWithBounds(b *testing.B) {
	// Build an ART with 100 leaves
	art := NewART(dukdb.TYPE_BIGINT)

	root := newNode(NodeType256)
	root.prefix = nil

	for i := 0; i < 100; i++ {
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = nil
		leaf.value = uint64(i)
		root.children[i] = leaf
	}
	art.root = root

	lower := []byte{25}
	upper := []byte{75}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := art.RangeScan(lower, upper, DefaultRangeScanOptions())
		for {
			_, _, ok := it.Next()
			if !ok {
				break
			}
		}
		it.Close()
	}
}

func BenchmarkEncodeCompositeKey(b *testing.B) {
	values := []any{int32(42), "hello", int64(1000000)}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeCompositeKey(values, types)
	}
}

func BenchmarkDecodeCompositeKey(b *testing.B) {
	values := []any{int32(42), "hello", int64(1000000)}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}
	encoded, _ := EncodeCompositeKey(values, types)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeCompositeKey(encoded, types)
	}
}
