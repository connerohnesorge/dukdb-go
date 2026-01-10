package duckdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetadataBlockReader_BlockIndexHandling verifies that NewMetadataBlockReader
// correctly calculates sub-block offsets based on the block_index field.
func TestMetadataBlockReader_BlockIndexHandling(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test_blockindex_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write file header
	fileHeader := NewFileHeader()
	err = WriteFileHeader(tmpFile, fileHeader)
	require.NoError(t, err)

	// Write database headers
	dbHeader := NewDatabaseHeader()
	dbHeader.BlockAllocSize = DefaultBlockSize
	err = WriteDatabaseHeader(tmpFile, dbHeader, DatabaseHeader1Offset)
	require.NoError(t, err)
	err = WriteDatabaseHeader(tmpFile, dbHeader, DatabaseHeader2Offset)
	require.NoError(t, err)

	// Create a block manager
	bm := NewBlockManager(tmpFile, DefaultBlockSize, 0)
	defer bm.Close()

	// Allocate a block for testing
	blockID, err := bm.AllocateBlock()
	require.NoError(t, err)

	// Create a new block with data
	block := &Block{
		ID:   blockID,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}

	// Set up test data in different sub-blocks
	// Sub-block 0: starts at offset 0 in Block.Data
	// Sub-block 1: starts at offset 4088 in Block.Data (4096 - 8)
	// Sub-block 2: starts at offset 8184 in Block.Data (2*4096 - 8)

	// Sub-block 0: next_ptr (0xFFFFFFFFFFFFFFFF) + test data "BLOCK0"
	copy(block.Data[0:8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	copy(block.Data[8:14], []byte("BLOCK0"))

	// Sub-block 1: next_ptr (0xFFFFFFFFFFFFFFFF) + test data "BLOCK1"
	sb1Offset := 4088
	copy(block.Data[sb1Offset:sb1Offset+8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	copy(block.Data[sb1Offset+8:sb1Offset+14], []byte("BLOCK1"))

	// Sub-block 2: next_ptr (0xFFFFFFFFFFFFFFFF) + test data "BLOCK2"
	sb2Offset := 8184
	copy(block.Data[sb2Offset:sb2Offset+8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	copy(block.Data[sb2Offset+8:sb2Offset+14], []byte("BLOCK2"))

	// Write the block back
	err = bm.WriteBlock(block)
	require.NoError(t, err)

	// Test reading from sub-block 0
	t.Run("sub-block 0", func(t *testing.T) {
		ptr := MetaBlockPointer{BlockID: blockID, BlockIndex: 0}
		reader, err := NewMetadataBlockReader(bm, ptr.Encode())
		require.NoError(t, err)

		data := make([]byte, 6)
		n, err := reader.Read(data)
		require.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, "BLOCK0", string(data))
	})

	// Test reading from sub-block 1
	t.Run("sub-block 1", func(t *testing.T) {
		ptr := MetaBlockPointer{BlockID: blockID, BlockIndex: 1}
		reader, err := NewMetadataBlockReader(bm, ptr.Encode())
		require.NoError(t, err)

		data := make([]byte, 6)
		n, err := reader.Read(data)
		require.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, "BLOCK1", string(data))
	})

	// Test reading from sub-block 2
	t.Run("sub-block 2", func(t *testing.T) {
		ptr := MetaBlockPointer{BlockID: blockID, BlockIndex: 2}
		reader, err := NewMetadataBlockReader(bm, ptr.Encode())
		require.NoError(t, err)

		data := make([]byte, 6)
		n, err := reader.Read(data)
		require.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, "BLOCK2", string(data))
	})
}

// TestMetaBlockPointer_EncodeDecodeWithBlockIndex verifies that MetaBlockPointer
// correctly encodes and decodes block_index values.
func TestMetaBlockPointer_EncodeDecodeWithBlockIndex(t *testing.T) {
	testCases := []struct {
		name       string
		blockID    uint64
		blockIndex uint8
	}{
		{"block 0, index 0", 0, 0},
		{"block 0, index 1", 0, 1},
		{"block 0, index 2", 0, 2},
		{"block 5, index 1", 5, 1},
		{"block 100, index 63", 100, 63},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ptr := MetaBlockPointer{BlockID: tc.blockID, BlockIndex: tc.blockIndex}
			encoded := ptr.Encode()
			decoded := DecodeMetaBlockPointer(encoded)

			assert.Equal(t, tc.blockID, decoded.BlockID, "BlockID mismatch")
			assert.Equal(t, tc.blockIndex, decoded.BlockIndex, "BlockIndex mismatch")
		})
	}
}
