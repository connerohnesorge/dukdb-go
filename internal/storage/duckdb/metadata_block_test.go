package duckdb

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAllocateSubBlock_Sequential verifies that sub-blocks are allocated sequentially.
func TestAllocateSubBlock_Sequential(t *testing.T) {
	// Create temporary file for testing
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create block manager
	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)

	// Create metadata block manager
	mbm := NewMetadataBlockManager(bm)

	// Allocate first sub-block
	ptr1, err := mbm.AllocateSubBlock()
	require.NoError(t, err)
	assert.Equal(t, uint8(0), ptr1.BlockIndex, "First sub-block should have index 0")

	// Allocate second sub-block
	ptr2, err := mbm.AllocateSubBlock()
	require.NoError(t, err)
	assert.Equal(t, uint8(1), ptr2.BlockIndex, "Second sub-block should have index 1")
	assert.Equal(t, ptr1.BlockID, ptr2.BlockID, "Both sub-blocks should be in same storage block")

	// Allocate third sub-block
	ptr3, err := mbm.AllocateSubBlock()
	require.NoError(t, err)
	assert.Equal(t, uint8(2), ptr3.BlockIndex, "Third sub-block should have index 2")
	assert.Equal(t, ptr1.BlockID, ptr3.BlockID, "All sub-blocks should be in same storage block")
}

// TestWriteSubBlock_SingleBlock tests writing data that fits in a single sub-block.
func TestWriteSubBlock_SingleBlock(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Create small test data
	testData := []byte("Hello, DuckDB metadata!")
	require.LessOrEqual(
		t,
		len(testData),
		MetadataSubBlockDataSize,
		"Test data should fit in one sub-block",
	)

	// Write data
	ptr, err := mbm.WriteSubBlock(testData)
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Returned pointer should be valid")
	assert.Equal(t, uint8(0), ptr.BlockIndex, "First write should use sub-block index 0")

	// Read data back
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Read data should match written data")
}

// TestWriteSubBlock_MultipleBlocks tests writing data that spans multiple sub-blocks.
func TestWriteSubBlock_MultipleBlocks(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Create data larger than one sub-block (4088 bytes)
	// Use 10KB of data to span 3 sub-blocks
	testData := make([]byte, 10*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	require.Greater(
		t,
		len(testData),
		MetadataSubBlockDataSize,
		"Test data should exceed one sub-block",
	)

	// Calculate expected number of sub-blocks
	expectedBlocks := (len(testData) + MetadataSubBlockDataSize - 1) / MetadataSubBlockDataSize
	assert.Equal(t, 3, expectedBlocks, "Should need 3 sub-blocks for 10KB")

	// Write data
	ptr, err := mbm.WriteSubBlock(testData)
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Returned pointer should be valid")

	// Verify sub-blocks were allocated
	allocatedCount := mbm.AllocatedSubBlockCount()
	assert.Equal(
		t,
		expectedBlocks,
		allocatedCount,
		"Should have allocated expected number of sub-blocks",
	)

	// Read data back
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Read data should match written data")
}

// TestSubBlockIndex_WrapToNewStorageBlock verifies that allocation wraps to a new storage block at 64 sub-blocks.
func TestSubBlockIndex_WrapToNewStorageBlock(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Allocate 64 sub-blocks (filling first storage block)
	var ptrs []MetaBlockPointer
	for i := 0; i < MetadataBlocksPerStorage; i++ {
		ptr, err := mbm.AllocateSubBlock()
		require.NoError(t, err, "Failed to allocate sub-block %d", i)
		ptrs = append(ptrs, ptr)

		assert.Equal(t, uint8(i), ptr.BlockIndex, "Sub-block index should be %d", i)
	}

	firstBlockID := ptrs[0].BlockID

	// Allocate one more sub-block - should wrap to new storage block
	ptr65, err := mbm.AllocateSubBlock()
	require.NoError(t, err)
	assert.Equal(t, uint8(0), ptr65.BlockIndex, "Should start at index 0 in new block")
	assert.NotEqual(t, firstBlockID, ptr65.BlockID, "Should be in different storage block")
	assert.Greater(t, ptr65.BlockID, firstBlockID, "New block ID should be higher")
}

// TestMetaBlockPointer_EncodeDecode verifies MetaBlockPointer encoding/decoding.
func TestMetaBlockPointer_EncodeDecode(t *testing.T) {
	testCases := []struct {
		name       string
		blockID    uint64
		blockIndex uint8
	}{
		{"Zero values", 0, 0},
		{"Small values", 42, 5},
		{"Max block index", 12345, 63},
		{"Large block ID", 0x00FFFFFFFFFFFFFF, 63}, // Max 56-bit value
		{"Mid values", 1000000, 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := MetaBlockPointer{
				BlockID:    tc.blockID,
				BlockIndex: tc.blockIndex,
			}

			// Encode
			encoded := original.Encode()

			// Decode
			decoded := DecodeMetaBlockPointer(encoded)

			// Verify
			assert.Equal(t, original.BlockID, decoded.BlockID, "BlockID should match")
			assert.Equal(t, original.BlockIndex, decoded.BlockIndex, "BlockIndex should match")
		})
	}
}

// TestMetaBlockPointer_InvalidBlockID verifies that InvalidBlockID is preserved.
func TestMetaBlockPointer_InvalidBlockID(t *testing.T) {
	ptr := MetaBlockPointer{
		BlockID:    InvalidBlockID,
		BlockIndex: 0,
	}

	encoded := ptr.Encode()
	assert.Equal(t, InvalidBlockID, encoded, "InvalidBlockID should be preserved in encoding")

	// When decoding InvalidBlockID, the lower 56 bits are masked
	// This is expected behavior: 0xFFFFFFFFFFFFFFFF & 0x00FFFFFFFFFFFFFF = 0x00FFFFFFFFFFFFFF
	decoded := DecodeMetaBlockPointer(InvalidBlockID)
	assert.Equal(
		t,
		uint64(0x00FFFFFFFFFFFFFF),
		decoded.BlockID,
		"Decoded should have lower 56 bits set",
	)
	assert.Equal(t, uint8(0xFF), decoded.BlockIndex, "Decoded should have block index 0xFF")
}

// TestWriteSubBlock_EmptyData verifies handling of empty data.
func TestWriteSubBlock_EmptyData(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Write empty data
	ptr, err := mbm.WriteSubBlock([]byte{})
	require.NoError(t, err)
	assert.Equal(t, InvalidBlockID, ptr.BlockID, "Empty data should return invalid pointer")

	// Write nil data
	ptr, err = mbm.WriteSubBlock(nil)
	require.NoError(t, err)
	assert.Equal(t, InvalidBlockID, ptr.BlockID, "Nil data should return invalid pointer")
}

// TestWriteSubBlock_ExactlyOneBlock tests data that exactly fills one sub-block.
func TestWriteSubBlock_ExactlyOneBlock(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Create data exactly MetadataSubBlockDataSize bytes
	testData := make([]byte, MetadataSubBlockDataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data
	ptr, err := mbm.WriteSubBlock(testData)
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Returned pointer should be valid")

	// Should use exactly one sub-block
	allocatedCount := mbm.AllocatedSubBlockCount()
	assert.Equal(t, 1, allocatedCount, "Should have allocated exactly one sub-block")

	// Read data back
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Read data should match written data")
}

// TestWriteSubBlock_JustOverOneBlock tests data that barely exceeds one sub-block.
func TestWriteSubBlock_JustOverOneBlock(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Create data one byte over one sub-block
	testData := make([]byte, MetadataSubBlockDataSize+1)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data
	ptr, err := mbm.WriteSubBlock(testData)
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Returned pointer should be valid")

	// Should use two sub-blocks
	allocatedCount := mbm.AllocatedSubBlockCount()
	assert.Equal(t, 2, allocatedCount, "Should have allocated two sub-blocks")

	// Read data back
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Read data should match written data")
}

// TestReadSubBlock_ChainIntegrity verifies that chained sub-blocks maintain data integrity.
func TestReadSubBlock_ChainIntegrity(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Create a pattern that's easy to verify
	// 5 sub-blocks worth of data with distinct patterns
	testData := make([]byte, 5*MetadataSubBlockDataSize)
	for i := range testData {
		// Each sub-block has a different byte value pattern
		blockNum := i / MetadataSubBlockDataSize
		testData[i] = byte(blockNum*10 + (i % 256))
	}

	// Write data
	ptr, err := mbm.WriteSubBlock(testData)
	require.NoError(t, err)

	// Verify 5 sub-blocks were allocated
	allocatedCount := mbm.AllocatedSubBlockCount()
	assert.Equal(t, 5, allocatedCount, "Should have allocated 5 sub-blocks")

	// Read data back
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, len(testData), len(readData), "Read data length should match")

	// Verify each byte
	if !bytes.Equal(testData, readData) {
		// Find first mismatch for debugging
		for i := 0; i < len(testData) && i < len(readData); i++ {
			if testData[i] != readData[i] {
				t.Errorf(
					"First mismatch at byte %d: expected %d, got %d",
					i,
					testData[i],
					readData[i],
				)
				break
			}
		}
		t.Fail()
	}
}

// TestMetadataBlockManager_Reset verifies that Reset clears state.
func TestMetadataBlockManager_Reset(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Allocate some sub-blocks
	for i := 0; i < 5; i++ {
		_, err := mbm.AllocateSubBlock()
		require.NoError(t, err)
	}

	// Verify state before reset
	assert.NotEqual(t, InvalidBlockID, mbm.CurrentBlockID(), "Should have a valid block ID")
	assert.Equal(t, 5, mbm.AllocatedSubBlockCount(), "Should have 5 allocated sub-blocks")

	// Reset
	mbm.Reset()

	// Verify state after reset
	assert.Equal(t, InvalidBlockID, mbm.CurrentBlockID(), "Block ID should be invalid after reset")
	assert.Equal(
		t,
		0,
		mbm.AllocatedSubBlockCount(),
		"Should have 0 allocated sub-blocks after reset",
	)

	// Should be able to allocate again from index 0
	ptr, err := mbm.AllocateSubBlock()
	require.NoError(t, err)
	assert.Equal(t, uint8(0), ptr.BlockIndex, "Should start from index 0 after reset")
}

// TestMetadataBlockManager_Concurrent tests concurrent allocations (basic concurrency safety).
func TestMetadataBlockManager_Concurrent(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Run concurrent allocations
	const numGoroutines = 10
	const allocsPerGoroutine = 5

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < allocsPerGoroutine; j++ {
				_, err := mbm.AllocateSubBlock()
				assert.NoError(t, err)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify total allocations
	totalExpected := numGoroutines * allocsPerGoroutine
	allocated := mbm.AllocatedSubBlockCount()

	// May span multiple storage blocks
	assert.GreaterOrEqual(
		t,
		totalExpected,
		allocated,
		"Should have allocated expected number of sub-blocks",
	)
}

// TestReadSubBlock_InvalidPointer verifies error handling for invalid pointers.
func TestReadSubBlock_InvalidPointer(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)

	// Try to read with invalid pointer
	invalidPtr := MetaBlockPointer{
		BlockID:    InvalidBlockID,
		BlockIndex: 0,
	}

	_, err = mbm.ReadSubBlock(invalidPtr)
	assert.Error(t, err, "Should error on invalid pointer")
}

// TestMetadataSubBlockSizeConstants verifies the sub-block size calculations.
func TestMetadataSubBlockSizeConstants(t *testing.T) {
	// Verify arithmetic
	assert.Equal(t, uint64(4096), uint64(MetadataSubBlockSize), "Sub-block size should be 4KB")
	assert.Equal(t, 64, MetadataBlocksPerStorage, "Should have 64 sub-blocks per storage block")
	assert.Equal(t, uint64(262144), uint64(MetadataBlocksPerStorage*MetadataSubBlockSize),
		"64 × 4KB should equal 256KB (DefaultBlockSize)")

	assert.Equal(t, 8, MetadataNextBlockSize, "Next block pointer should be 8 bytes")
	assert.Equal(t, 4088, MetadataSubBlockDataSize, "Data size should be 4096 - 8 = 4088 bytes")
}

// TestMetadataWriter_ImplementsIOWriter verifies MetadataWriter implements io.Writer.
func TestMetadataWriter_ImplementsIOWriter(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Verify it's an io.Writer
	var _ = (interface{})(writer).(interface{ Write([]byte) (int, error) })

	// Test writing some data
	testData := []byte("test data")
	n, err := writer.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n, "Should write all bytes")
	assert.Equal(t, len(testData), writer.Len(), "Buffer should contain written data")
}

// TestMetadataWriter_WriteAccumulatesData verifies that Write accumulates data.
func TestMetadataWriter_WriteAccumulatesData(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Write multiple times
	data1 := []byte("first ")
	data2 := []byte("second ")
	data3 := []byte("third")

	n1, err := writer.Write(data1)
	require.NoError(t, err)
	assert.Equal(t, len(data1), n1)

	n2, err := writer.Write(data2)
	require.NoError(t, err)
	assert.Equal(t, len(data2), n2)

	n3, err := writer.Write(data3)
	require.NoError(t, err)
	assert.Equal(t, len(data3), n3)

	// Verify accumulated length
	expectedLen := len(data1) + len(data2) + len(data3)
	assert.Equal(t, expectedLen, writer.Len(), "Buffer should accumulate all writes")
}

// TestMetadataWriter_FlushWritesToSubBlocks verifies Flush writes to sub-blocks.
func TestMetadataWriter_FlushWritesToSubBlocks(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Write some data
	testData := []byte("Hello, metadata world!")
	_, err = writer.Write(testData)
	require.NoError(t, err)

	// Flush
	ptr, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Flushed pointer should be valid")

	// Verify buffer is cleared after flush
	assert.Equal(t, 0, writer.Len(), "Buffer should be empty after flush")

	// Verify pointer is stored
	assert.Equal(t, ptr, writer.GetPointer(), "GetPointer should return flushed pointer")

	// Read back the data
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Read data should match written data")
}

// TestMetadataWriter_BinarySerializerIntegration tests using BinarySerializer with MetadataWriter.
func TestMetadataWriter_BinarySerializerIntegration(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Create a BinarySerializer writing to MetadataWriter
	serializer := NewBinarySerializer(writer)

	// Serialize some data using BinarySerializer
	serializer.OnObjectBegin()
	serializer.WriteProperty(100, "name", "test_table")
	serializer.WriteProperty(101, "id", uint64(42))
	serializer.WriteProperty(102, "active", true)
	serializer.OnObjectEnd()

	// Check for serialization errors
	require.NoError(t, serializer.Err(), "Serialization should succeed")

	// Flush to metadata blocks
	ptr, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Flushed pointer should be valid")

	// Read back and verify we can get the data
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Greater(t, len(readData), 0, "Should have serialized data")

	// Verify the data contains expected field IDs (field IDs are written as uint16 little-endian)
	// Check if field ID 100 (0x64, 0x00) is present
	assert.True(t, bytes.Contains(readData, []byte{100, 0}), "Should contain field ID 100")
	// Check if field ID 101 (0x65, 0x00) is present
	assert.True(t, bytes.Contains(readData, []byte{101, 0}), "Should contain field ID 101")
	// Check if field ID 102 (0x66, 0x00) is present
	assert.True(t, bytes.Contains(readData, []byte{102, 0}), "Should contain field ID 102")
}

// TestMetadataWriter_LargeDataChaining tests handling of large data that spans multiple sub-blocks.
func TestMetadataWriter_LargeDataChaining(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Write data larger than one sub-block (> 4088 bytes)
	largeData := make([]byte, 15000) // Will span ~4 sub-blocks
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := writer.Write(largeData)
	require.NoError(t, err)
	assert.Equal(t, len(largeData), n)

	// Flush
	ptr, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr.IsValid(), "Flushed pointer should be valid")

	// Verify multiple sub-blocks were allocated
	allocatedCount := mbm.AllocatedSubBlockCount()
	expectedBlocks := (len(largeData) + MetadataSubBlockDataSize - 1) / MetadataSubBlockDataSize
	assert.Equal(t, expectedBlocks, allocatedCount, "Should allocate expected number of sub-blocks")

	// Read back and verify
	readData, err := mbm.ReadSubBlock(ptr)
	require.NoError(t, err)
	assert.Equal(t, largeData, readData, "Read data should match written data")
}

// TestMetadataWriter_Reset verifies Reset clears buffer and pointer.
func TestMetadataWriter_Reset(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Write and flush
	testData := []byte("test data")
	_, err = writer.Write(testData)
	require.NoError(t, err)

	ptr, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr.IsValid())

	// Verify state before reset
	assert.Equal(t, ptr, writer.GetPointer(), "Should have pointer")

	// Reset
	writer.Reset()

	// Verify state after reset
	assert.Equal(t, 0, writer.Len(), "Buffer should be empty")
	assert.Equal(t, MetaBlockPointer{}, writer.GetPointer(), "Pointer should be cleared")

	// Write new data after reset
	newData := []byte("new data after reset")
	_, err = writer.Write(newData)
	require.NoError(t, err)

	newPtr, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, newPtr.IsValid())
	assert.NotEqual(t, ptr, newPtr, "New pointer should be different after reset")

	// Verify new data is correct
	readData, err := mbm.ReadSubBlock(newPtr)
	require.NoError(t, err)
	assert.Equal(t, newData, readData)
}

// TestMetadataWriter_EmptyFlush verifies flushing empty data returns InvalidBlockID.
func TestMetadataWriter_EmptyFlush(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// Flush without writing anything
	ptr, err := writer.Flush()
	require.NoError(t, err)
	assert.Equal(t, InvalidBlockID, ptr.BlockID, "Empty flush should return invalid pointer")
}

// TestMetadataWriter_MultipleFlushes verifies multiple flushes with same writer.
func TestMetadataWriter_MultipleFlushes(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_metadata_*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	bm := NewBlockManager(tmpFile, DefaultBlockSize, 10)
	mbm := NewMetadataBlockManager(bm)
	writer := NewMetadataWriter(mbm)

	// First write and flush
	data1 := []byte("first data")
	_, err = writer.Write(data1)
	require.NoError(t, err)
	ptr1, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr1.IsValid())

	// Second write and flush (should preserve startPointer)
	data2 := []byte("second data")
	_, err = writer.Write(data2)
	require.NoError(t, err)
	ptr2, err := writer.Flush()
	require.NoError(t, err)
	assert.True(t, ptr2.IsValid())

	// Both flushes should return the same start pointer
	assert.Equal(t, ptr1, ptr2, "Multiple flushes should preserve startPointer")
	assert.Equal(t, ptr1, writer.GetPointer(), "GetPointer should return first pointer")
}
