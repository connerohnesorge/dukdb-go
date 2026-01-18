package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockCache tests the LRU block cache implementation.
func TestBlockCache(t *testing.T) {
	t.Run("NewBlockCache", func(t *testing.T) {
		cache := NewBlockCache(10)
		require.NotNil(t, cache)
		assert.Equal(t, 10, cache.capacity)
		assert.Equal(t, 0, cache.Size())
	})

	t.Run("NewBlockCache default capacity", func(t *testing.T) {
		cache := NewBlockCache(0)
		require.NotNil(t, cache)
		assert.Equal(t, DefaultCacheCapacity, cache.capacity)
	})

	t.Run("NewBlockCache negative capacity", func(t *testing.T) {
		cache := NewBlockCache(-5)
		require.NotNil(t, cache)
		assert.Equal(t, DefaultCacheCapacity, cache.capacity)
	})

	t.Run("Put and Get", func(t *testing.T) {
		cache := NewBlockCache(10)

		block := &Block{
			ID:       1,
			Type:     BlockMetaData,
			Checksum: 0x12345678,
			Data:     []byte("test data"),
		}

		cache.Put(block)
		assert.Equal(t, 1, cache.Size())

		retrieved := cache.Get(1)
		require.NotNil(t, retrieved)
		assert.Equal(t, block.ID, retrieved.ID)
		assert.Equal(t, block.Checksum, retrieved.Checksum)
		assert.Equal(t, block.Data, retrieved.Data)
	})

	t.Run("Get non-existent block", func(t *testing.T) {
		cache := NewBlockCache(10)

		result := cache.Get(999)
		assert.Nil(t, result)
	})

	t.Run("Put updates existing block", func(t *testing.T) {
		cache := NewBlockCache(10)

		block1 := &Block{ID: 1, Data: []byte("first")}
		block2 := &Block{ID: 1, Data: []byte("second")}

		cache.Put(block1)
		cache.Put(block2)

		assert.Equal(t, 1, cache.Size())

		retrieved := cache.Get(1)
		assert.Equal(t, []byte("second"), retrieved.Data)
	})

	t.Run("LRU eviction", func(t *testing.T) {
		cache := NewBlockCache(3)

		// Add 3 blocks
		for i := uint64(0); i < 3; i++ {
			cache.Put(&Block{ID: i, Data: []byte{byte(i)}})
		}

		assert.Equal(t, 3, cache.Size())

		// Access block 0 to make it recently used
		cache.Get(0)

		// Add block 3 - should evict block 1 (least recently used)
		cache.Put(&Block{ID: 3, Data: []byte{3}})

		assert.Equal(t, 3, cache.Size())
		assert.Nil(t, cache.Get(1)) // Block 1 should be evicted
		assert.NotNil(t, cache.Get(0))
		assert.NotNil(t, cache.Get(2))
		assert.NotNil(t, cache.Get(3))
	})

	t.Run("Invalidate", func(t *testing.T) {
		cache := NewBlockCache(10)

		cache.Put(&Block{ID: 1, Data: []byte("test")})
		assert.Equal(t, 1, cache.Size())

		cache.Invalidate(1)
		assert.Equal(t, 0, cache.Size())
		assert.Nil(t, cache.Get(1))
	})

	t.Run("Invalidate non-existent block", func(t *testing.T) {
		cache := NewBlockCache(10)

		// Should not panic
		cache.Invalidate(999)
		assert.Equal(t, 0, cache.Size())
	})

	t.Run("Clear", func(t *testing.T) {
		cache := NewBlockCache(10)

		for i := uint64(0); i < 5; i++ {
			cache.Put(&Block{ID: i})
		}

		assert.Equal(t, 5, cache.Size())

		cache.Clear()
		assert.Equal(t, 0, cache.Size())

		for i := uint64(0); i < 5; i++ {
			assert.Nil(t, cache.Get(i))
		}
	})

	t.Run("Concurrent access", func(t *testing.T) {
		cache := NewBlockCache(100)

		var wg sync.WaitGroup

		// Concurrent puts
		for i := uint64(0); i < 50; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				cache.Put(&Block{ID: id, Data: []byte{byte(id)}})
			}(i)
		}

		// Concurrent gets
		for i := uint64(0); i < 50; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				cache.Get(id)
			}(i)
		}

		wg.Wait()

		// Should not panic and cache should be in consistent state
		assert.LessOrEqual(t, cache.Size(), 100)
	})
}

// TestFreeListManager tests the free list management.
func TestFreeListManager(t *testing.T) {
	t.Run("NewFreeListManager", func(t *testing.T) {
		fl := NewFreeListManager()
		require.NotNil(t, fl)
		assert.Equal(t, 0, fl.FreeBlockCount())
	})

	t.Run("MarkFree and IsFree", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkFree(5)
		assert.True(t, fl.IsFree(5))
		assert.False(t, fl.IsFree(6))
		assert.Equal(t, 1, fl.FreeBlockCount())
	})

	t.Run("MarkUsed", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkFree(5)
		assert.True(t, fl.IsFree(5))

		fl.MarkUsed(5)
		assert.False(t, fl.IsFree(5))
		assert.Equal(t, 0, fl.FreeBlockCount())
	})

	t.Run("GetFreeBlock", func(t *testing.T) {
		fl := NewFreeListManager()

		// No free blocks
		_, ok := fl.GetFreeBlock()
		assert.False(t, ok)

		// Add some free blocks
		fl.MarkFree(1)
		fl.MarkFree(2)
		fl.MarkFree(3)

		id, ok := fl.GetFreeBlock()
		assert.True(t, ok)
		assert.Contains(t, []uint64{1, 2, 3}, id)
		assert.Equal(t, 2, fl.FreeBlockCount())

		// Block should no longer be free (now in freeBlocksInUse)
		assert.False(t, fl.IsFree(id))
	})

	t.Run("AllocateFromFreeList alias", func(t *testing.T) {
		fl := NewFreeListManager()
		fl.MarkFree(10)

		id, ok := fl.AllocateFromFreeList()
		assert.True(t, ok)
		assert.Equal(t, uint64(10), id)
	})

	t.Run("CommitTransaction", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkFree(1)
		fl.MarkFree(2)

		// Allocate a block (moves to freeBlocksInUse)
		id, _ := fl.GetFreeBlock()

		// Commit clears freeBlocksInUse
		fl.CommitTransaction()

		// Can't get the block back by rolling back after commit
		assert.False(t, fl.IsFree(id))
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkFree(1)

		// Allocate the block
		id, ok := fl.GetFreeBlock()
		require.True(t, ok)
		assert.False(t, fl.IsFree(id))

		// Rollback returns it to free list
		fl.RollbackTransaction()
		assert.True(t, fl.IsFree(id))
	})

	t.Run("Checkpoint", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkUsed(1)
		fl.MarkUsed(2)

		// Checkpoint clears newly used blocks
		fl.Checkpoint()

		// newlyUsedBlocks should be cleared (internal state)
		// MarkFree should work normally
		fl.MarkFree(1)
		assert.True(t, fl.IsFree(1))
	})

	t.Run("Serialize and Deserialize", func(t *testing.T) {
		fl := NewFreeListManager()

		fl.MarkFree(10)
		fl.MarkFree(20)
		fl.MarkFree(30)

		// Serialize
		data, err := fl.SerializeToBytes()
		require.NoError(t, err)

		// Deserialize into new manager
		fl2 := NewFreeListManager()
		err = fl2.DeserializeFromBytes(data)
		require.NoError(t, err)

		assert.True(t, fl2.IsFree(10))
		assert.True(t, fl2.IsFree(20))
		assert.True(t, fl2.IsFree(30))
		assert.Equal(t, 3, fl2.FreeBlockCount())
	})

	t.Run("Serialize empty free list", func(t *testing.T) {
		fl := NewFreeListManager()

		data, err := fl.SerializeToBytes()
		require.NoError(t, err)

		fl2 := NewFreeListManager()
		err = fl2.DeserializeFromBytes(data)
		require.NoError(t, err)

		assert.Equal(t, 0, fl2.FreeBlockCount())
	})

	t.Run("Concurrent access", func(t *testing.T) {
		fl := NewFreeListManager()

		var wg sync.WaitGroup

		// Concurrent marks
		for i := uint64(0); i < 100; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				fl.MarkFree(id)
			}(i)
		}

		wg.Wait()

		// Concurrent reads and allocations
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fl.GetFreeBlock()
			}()
		}

		wg.Wait()

		// Should not panic and be in consistent state
		// Some blocks may remain free, some may be in use
	})
}

// TestBlockManager tests the block manager implementation.
func TestBlockManager(t *testing.T) {
	// Helper to create a test file with proper DuckDB headers
	createTestFile := func(t *testing.T) (*os.File, string) {
		t.Helper()
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "test.duckdb")

		f, err := os.Create(tmpFile)
		require.NoError(t, err)

		// Write file header
		fileHeader := NewFileHeader()
		err = WriteFileHeader(f, fileHeader)
		require.NoError(t, err)

		// Write database headers
		dbHeader := NewDatabaseHeader()
		err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader1Offset)
		require.NoError(t, err)
		err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader2Offset)
		require.NoError(t, err)

		return f, tmpFile
	}

	t.Run("NewBlockManager", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)
		require.NotNil(t, bm)
		assert.Equal(t, DefaultBlockSize, bm.BlockSize())
		assert.Equal(t, uint64(0), bm.BlockCount())
	})

	t.Run("NewBlockManager default cache size", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 0)
		require.NotNil(t, bm)
	})

	t.Run("AllocateBlock", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Block 0 is reserved for metadata, so first allocation returns 1
		id1, err := bm.AllocateBlock()
		require.NoError(t, err)
		assert.Equal(
			t,
			uint64(1),
			id1,
			"first allocation should skip block 0 (reserved for metadata)",
		)

		id2, err := bm.AllocateBlock()
		require.NoError(t, err)
		assert.Equal(t, uint64(2), id2)

		id3, err := bm.AllocateBlock()
		require.NoError(t, err)
		assert.Equal(t, uint64(3), id3)

		// Block count is 4 because we implicitly reserved block 0 + allocated 1,2,3
		assert.Equal(t, uint64(4), bm.BlockCount())
	})

	t.Run("AllocateBlock reuses freed blocks", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Allocate some blocks (block 0 is reserved, so we get 1, 2, 3)
		id1, _ := bm.AllocateBlock() // 1
		id2, _ := bm.AllocateBlock() // 2
		_, _ = bm.AllocateBlock()    // 3

		// Free block 2
		err := bm.FreeBlock(id2)
		require.NoError(t, err)

		// Next allocation should reuse block 2 (from free list)
		id, err := bm.AllocateBlock()
		require.NoError(t, err)
		assert.Equal(t, id1+1, id) // should be block 2 (id1 was 1)
	})

	t.Run("WriteBlock and ReadBlock", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Create test data
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		block := &Block{
			ID:   0,
			Type: BlockRowGroup,
			Data: data,
		}

		// Write block
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Clear cache to force read from disk
		bm.cache.Clear()

		// Read block back
		readBlock, err := bm.ReadBlock(0)
		require.NoError(t, err)
		require.NotNil(t, readBlock)

		assert.Equal(t, block.ID, readBlock.ID)
		assert.Equal(t, block.Data, readBlock.Data)
		assert.NotEqual(t, uint64(0), readBlock.Checksum)
	})

	t.Run("ReadBlock cached", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		data[0] = 0x42

		block := &Block{ID: 0, Data: data}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// First read - from cache (was put there by WriteBlock)
		readBlock1, err := bm.ReadBlock(0)
		require.NoError(t, err)

		// Second read - should also be from cache
		readBlock2, err := bm.ReadBlock(0)
		require.NoError(t, err)

		assert.Equal(t, readBlock1.Data, readBlock2.Data)
	})

	t.Run("ReadBlock checksum validation", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Write a valid block
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		data[0] = 0x42

		block := &Block{ID: 0, Data: data}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Clear cache
		bm.cache.Clear()

		// Corrupt the data in the file (after checksum)
		offset := int64(DataBlocksOffset) + BlockChecksumSize
		_, err = f.WriteAt([]byte{0xFF}, offset)
		require.NoError(t, err)

		// Read should fail with checksum error
		_, err = bm.ReadBlock(0)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrBlockChecksumFailed))
	})

	t.Run("FreeBlock", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Write a block
		block := &Block{ID: 0, Data: make([]byte, DefaultBlockSize-BlockChecksumSize)}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Free it
		err = bm.FreeBlock(0)
		require.NoError(t, err)

		// Should be in free list
		assert.True(t, bm.freeList.IsFree(0))
	})

	t.Run("SetBlockCount", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		bm.SetBlockCount(100)
		assert.Equal(t, uint64(100), bm.BlockCount())
	})

	t.Run("Sync", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Write a block
		block := &Block{ID: 0, Data: make([]byte, DefaultBlockSize-BlockChecksumSize)}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Sync should not fail
		err = bm.Sync()
		require.NoError(t, err)
	})

	t.Run("Close", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Write a block
		block := &Block{ID: 0, Data: make([]byte, DefaultBlockSize-BlockChecksumSize)}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Close
		err = bm.Close()
		require.NoError(t, err)

		// Operations should fail after close
		_, err = bm.ReadBlock(0)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrBlockManagerClosed))

		err = bm.WriteBlock(block)
		assert.True(t, errors.Is(err, ErrBlockManagerClosed))

		_, err = bm.AllocateBlock()
		assert.True(t, errors.Is(err, ErrBlockManagerClosed))

		err = bm.FreeBlock(0)
		assert.True(t, errors.Is(err, ErrBlockManagerClosed))

		err = bm.Sync()
		assert.True(t, errors.Is(err, ErrBlockManagerClosed))

		// Double close should be OK
		err = bm.Close()
		assert.NoError(t, err)
	})

	t.Run("blockOffset calculation", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Block 0 should start at DataBlocksOffset
		offset0 := bm.blockOffset(0)
		assert.Equal(t, int64(DataBlocksOffset), offset0)

		// Block 1 should be one blockSize further
		offset1 := bm.blockOffset(1)
		assert.Equal(t, int64(DataBlocksOffset)+int64(DefaultBlockSize), offset1)

		// Block 10 should be 10*blockSize further
		offset10 := bm.blockOffset(10)
		assert.Equal(t, int64(DataBlocksOffset)+10*int64(DefaultBlockSize), offset10)
	})

	t.Run("Multiple blocks round trip", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 5) // Small cache

		// Write 10 blocks
		for i := uint64(0); i < 10; i++ {
			data := make([]byte, DefaultBlockSize-BlockChecksumSize)
			// Fill with pattern based on block ID
			for j := range data {
				data[j] = byte((i + uint64(j)) % 256)
			}

			block := &Block{ID: i, Data: data}
			err := bm.WriteBlock(block)
			require.NoError(t, err)
		}

		// Clear cache
		bm.cache.Clear()

		// Read all blocks and verify
		for i := uint64(0); i < 10; i++ {
			block, err := bm.ReadBlock(i)
			require.NoError(t, err)

			// Verify pattern
			for j := range block.Data {
				expected := byte((i + uint64(j)) % 256)
				assert.Equal(t, expected, block.Data[j],
					"block %d byte %d mismatch", i, j)
			}
		}
	})

	t.Run("Concurrent read and write", func(t *testing.T) {
		f, _ := createTestFile(t)
		defer func() { _ = f.Close() }()

		bm := NewBlockManager(f, DefaultBlockSize, 50)

		// First, write some blocks
		for i := uint64(0); i < 20; i++ {
			data := make([]byte, DefaultBlockSize-BlockChecksumSize)
			binary.LittleEndian.PutUint64(data, i)
			block := &Block{ID: i, Data: data}
			err := bm.WriteBlock(block)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup

		// Concurrent reads
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := uint64(0); j < 20; j++ {
					_, _ = bm.ReadBlock(j)
				}
			}()
		}

		// Concurrent writes to new blocks
		for i := uint64(20); i < 30; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				data := make([]byte, DefaultBlockSize-BlockChecksumSize)
				binary.LittleEndian.PutUint64(data, id)
				block := &Block{ID: id, Data: data}
				_ = bm.WriteBlock(block)
			}(i)
		}

		wg.Wait()

		// Should not have panicked
		assert.GreaterOrEqual(t, bm.BlockCount(), uint64(20))
	})
}

// TestBlockChecksumIntegration tests that the block checksum is correctly
// computed and verified during read/write operations.
func TestBlockChecksumIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "checksum_test.duckdb")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Write headers
	fileHeader := NewFileHeader()
	err = WriteFileHeader(f, fileHeader)
	require.NoError(t, err)

	dbHeader := NewDatabaseHeader()
	err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader1Offset)
	require.NoError(t, err)
	err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader2Offset)
	require.NoError(t, err)

	bm := NewBlockManager(f, DefaultBlockSize, 10)

	t.Run("Checksum is at beginning of block", func(t *testing.T) {
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		data[0] = 0x42
		data[1] = 0x43
		data[2] = 0x44

		block := &Block{ID: 0, Data: data}
		err := bm.WriteBlock(block)
		require.NoError(t, err)

		// Read raw data from file
		rawData := make([]byte, DefaultBlockSize)
		_, err = f.ReadAt(rawData, int64(DataBlocksOffset))
		require.NoError(t, err)

		// First 8 bytes should be checksum
		storedChecksum := binary.LittleEndian.Uint64(rawData[:BlockChecksumSize])
		assert.NotEqual(t, uint64(0), storedChecksum)

		// Compute checksum of data portion
		dataInFile := rawData[BlockChecksumSize:]
		computedChecksum := checksumBlock(dataInFile)
		assert.Equal(t, storedChecksum, computedChecksum)

		// Verify the data content
		assert.Equal(t, byte(0x42), dataInFile[0])
		assert.Equal(t, byte(0x43), dataInFile[1])
		assert.Equal(t, byte(0x44), dataInFile[2])
	})

	t.Run("Checksum changes with data", func(t *testing.T) {
		data1 := make([]byte, DefaultBlockSize-BlockChecksumSize)
		data1[0] = 0x01

		block1 := &Block{ID: 1, Data: data1}
		err := bm.WriteBlock(block1)
		require.NoError(t, err)

		data2 := make([]byte, DefaultBlockSize-BlockChecksumSize)
		data2[0] = 0x02

		block2 := &Block{ID: 2, Data: data2}
		err = bm.WriteBlock(block2)
		require.NoError(t, err)

		bm.cache.Clear()

		readBlock1, err := bm.ReadBlock(1)
		require.NoError(t, err)

		readBlock2, err := bm.ReadBlock(2)
		require.NoError(t, err)

		// Checksums should be different
		assert.NotEqual(t, readBlock1.Checksum, readBlock2.Checksum)
	})
}

// TestFreeListSerialization tests the serialization round-trip of free lists.
func TestFreeListSerialization(t *testing.T) {
	t.Run("Serialize via BinaryWriter", func(t *testing.T) {
		fl := NewFreeListManager()
		fl.MarkFree(100)
		fl.MarkFree(200)
		fl.MarkFree(300)

		var buf bytes.Buffer
		w := NewBinaryWriter(&buf)

		err := fl.Serialize(w)
		require.NoError(t, err)

		// Check the serialized format
		data := buf.Bytes()
		count := binary.LittleEndian.Uint64(data[0:8])
		assert.Equal(t, uint64(3), count)

		// The block IDs follow (order may vary due to map iteration)
		ids := make(map[uint64]bool)
		for i := 0; i < 3; i++ {
			id := binary.LittleEndian.Uint64(data[8+i*8 : 16+i*8])
			ids[id] = true
		}

		assert.True(t, ids[100])
		assert.True(t, ids[200])
		assert.True(t, ids[300])
	})

	t.Run("Deserialize via BinaryReader", func(t *testing.T) {
		// Manually create serialized data
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint64(2))   // count
		binary.Write(&buf, binary.LittleEndian, uint64(50))  // block 1
		binary.Write(&buf, binary.LittleEndian, uint64(150)) // block 2

		fl := NewFreeListManager()
		r := NewBinaryReader(bytes.NewReader(buf.Bytes()))

		err := fl.Deserialize(r)
		require.NoError(t, err)

		assert.True(t, fl.IsFree(50))
		assert.True(t, fl.IsFree(150))
		assert.False(t, fl.IsFree(100))
		assert.Equal(t, 2, fl.FreeBlockCount())
	})

	t.Run("Deserialize with read error", func(t *testing.T) {
		// Truncated data - only count, no block IDs
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint64(5)) // count says 5 blocks

		fl := NewFreeListManager()
		r := NewBinaryReader(bytes.NewReader(buf.Bytes()))

		err := fl.Deserialize(r)
		assert.Error(t, err)
	})
}

// TestCacheStats tests the cache statistics functionality.
func TestCacheStats(t *testing.T) {
	t.Run("HitRate empty", func(t *testing.T) {
		stats := CacheStats{}
		assert.Equal(t, float64(0), stats.HitRate())
	})

	t.Run("HitRate 100%", func(t *testing.T) {
		stats := CacheStats{Hits: 100, Misses: 0}
		assert.Equal(t, float64(100), stats.HitRate())
	})

	t.Run("HitRate 50%", func(t *testing.T) {
		stats := CacheStats{Hits: 50, Misses: 50}
		assert.Equal(t, float64(50), stats.HitRate())
	})

	t.Run("HitRate with evictions", func(t *testing.T) {
		stats := CacheStats{Hits: 80, Misses: 20, Evictions: 10}
		assert.Equal(t, float64(80), stats.HitRate())
	})

	t.Run("Cache tracks hits", func(t *testing.T) {
		cache := NewBlockCache(10)

		// Put a block
		cache.Put(&Block{ID: 1, Data: []byte("test")})

		// Get it multiple times - should be hits
		for i := 0; i < 5; i++ {
			cache.Get(1)
		}

		stats := cache.Stats()
		assert.Equal(t, uint64(5), stats.Hits)
		assert.Equal(t, uint64(0), stats.Misses)
	})

	t.Run("Cache tracks misses", func(t *testing.T) {
		cache := NewBlockCache(10)

		// Get non-existent blocks - should be misses
		for i := uint64(0); i < 5; i++ {
			cache.Get(i)
		}

		stats := cache.Stats()
		assert.Equal(t, uint64(0), stats.Hits)
		assert.Equal(t, uint64(5), stats.Misses)
	})

	t.Run("Cache tracks evictions", func(t *testing.T) {
		cache := NewBlockCache(3)

		// Fill cache
		for i := uint64(0); i < 3; i++ {
			cache.Put(&Block{ID: i})
		}

		assert.Equal(t, uint64(0), cache.Stats().Evictions)

		// Add more blocks to cause evictions
		for i := uint64(3); i < 6; i++ {
			cache.Put(&Block{ID: i})
		}

		stats := cache.Stats()
		assert.Equal(t, uint64(3), stats.Evictions)
	})

	t.Run("ResetStats", func(t *testing.T) {
		cache := NewBlockCache(10)

		// Generate some stats
		cache.Put(&Block{ID: 1})
		cache.Get(1) // hit
		cache.Get(2) // miss

		stats := cache.Stats()
		assert.Equal(t, uint64(1), stats.Hits)
		assert.Equal(t, uint64(1), stats.Misses)

		// Reset
		cache.ResetStats()

		stats = cache.Stats()
		assert.Equal(t, uint64(0), stats.Hits)
		assert.Equal(t, uint64(0), stats.Misses)
		assert.Equal(t, uint64(0), stats.Evictions)
	})

	t.Run("BlockManager cache stats", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "test.duckdb")

		f, err := os.Create(tmpFile)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		// Write headers
		fileHeader := NewFileHeader()
		err = WriteFileHeader(f, fileHeader)
		require.NoError(t, err)

		dbHeader := NewDatabaseHeader()
		err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader1Offset)
		require.NoError(t, err)
		err = WriteDatabaseHeader(f, dbHeader, DatabaseHeader2Offset)
		require.NoError(t, err)

		bm := NewBlockManager(f, DefaultBlockSize, 10)

		// Write a block
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		block := &Block{ID: 0, Data: data}
		err = bm.WriteBlock(block)
		require.NoError(t, err)

		bm.ResetCacheStats()

		// Read it (should be a hit since WriteBlock caches)
		_, err = bm.ReadBlock(0)
		require.NoError(t, err)

		stats := bm.CacheStats()
		assert.Equal(t, uint64(1), stats.Hits)
	})
}

// BenchmarkBlockCache benchmarks cache operations.
func BenchmarkBlockCache(b *testing.B) {
	b.Run("Get hit", func(b *testing.B) {
		cache := NewBlockCache(1000)

		// Pre-populate cache
		for i := uint64(0); i < 100; i++ {
			cache.Put(&Block{ID: i, Data: make([]byte, 1024)})
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Get(uint64(i % 100))
		}
	})

	b.Run("Get miss", func(b *testing.B) {
		cache := NewBlockCache(1000)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Get(uint64(i))
		}
	})

	b.Run("Put with eviction", func(b *testing.B) {
		cache := NewBlockCache(100)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Put(&Block{ID: uint64(i), Data: make([]byte, 1024)})
		}
	})
}

// BenchmarkBlockManager benchmarks block manager operations.
func BenchmarkBlockManager(b *testing.B) {
	// Create test file once
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.duckdb")

	f, err := os.Create(tmpFile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// Write headers
	fileHeader := NewFileHeader()
	if err := WriteFileHeader(f, fileHeader); err != nil {
		b.Fatal(err)
	}

	dbHeader := NewDatabaseHeader()
	if err := WriteDatabaseHeader(f, dbHeader, DatabaseHeader1Offset); err != nil {
		b.Fatal(err)
	}
	if err := WriteDatabaseHeader(f, dbHeader, DatabaseHeader2Offset); err != nil {
		b.Fatal(err)
	}

	bm := NewBlockManager(f, DefaultBlockSize, 128)

	// Pre-write some blocks for read benchmarks
	for i := uint64(0); i < 100; i++ {
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		block := &Block{ID: i, Data: data}
		if err := bm.WriteBlock(block); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("WriteBlock", func(b *testing.B) {
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			block := &Block{ID: uint64(100 + i), Data: data}
			_ = bm.WriteBlock(block)
		}
	})

	b.Run("ReadBlock cached", func(b *testing.B) {
		// Ensure blocks are cached
		for i := uint64(0); i < 100; i++ {
			_, _ = bm.ReadBlock(i)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = bm.ReadBlock(uint64(i % 100))
		}
	})

	b.Run("AllocateBlock", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = bm.AllocateBlock()
		}
	})
}
