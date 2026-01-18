package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// Metadata block error definitions.
var (
	// ErrMetadataBlockFull indicates all 64 sub-blocks in a storage block are allocated.
	ErrMetadataBlockFull = errors.New("metadata block full: all 64 sub-blocks allocated")

	// ErrInvalidSubBlockIndex indicates an invalid sub-block index (must be 0-63).
	ErrInvalidSubBlockIndex = errors.New("invalid sub-block index: must be 0-63")

	// ErrMetadataChainBroken indicates a metadata chain has an invalid pointer.
	ErrMetadataChainBroken = errors.New("metadata chain broken: invalid next block pointer")
)

// Metadata sub-block constants.
const (
	// MetadataBlocksPerStorage is the number of metadata sub-blocks per storage block.
	// DuckDB divides each 256KB storage block into 64 × 4KB metadata sub-blocks.
	MetadataBlocksPerStorage = 64

	// MetadataSubBlockSize is the size of each metadata sub-block.
	// 256KB / 64 = 4KB per sub-block.
	MetadataSubBlockSize = 4096

	// MetadataNextBlockSize is the size of the next block pointer at the start.
	// Each sub-block begins with an 8-byte pointer to the next sub-block in the chain.
	MetadataNextBlockSize = 8

	// MetadataSubBlockDataSize is the usable data space per sub-block.
	// 4KB - 8 bytes (next pointer) = 4088 bytes of usable data.
	MetadataSubBlockDataSize = MetadataSubBlockSize - MetadataNextBlockSize
)

// MetadataBlockManager manages metadata sub-blocks within storage blocks.
// DuckDB stores catalog metadata in 4KB sub-blocks (64 per 256KB storage block).
// Each sub-block has:
//   - First 8 bytes: Next block pointer (InvalidBlockID if last)
//   - Remaining 4088 bytes: Actual metadata
//
// Sub-blocks can be chained for data larger than 4088 bytes.
type MetadataBlockManager struct {
	// blockManager handles storage block I/O.
	blockManager *BlockManager

	// currentBlockID is the current storage block being used for metadata.
	currentBlockID uint64

	// currentBlockData is the data for the current storage block.
	currentBlockData []byte

	// nextSubBlockIndex is the next sub-block index to allocate (0-63).
	nextSubBlockIndex uint8

	// allocatedSubBlocks tracks which sub-blocks have been allocated in current block.
	// Index i = true if sub-block i is allocated.
	allocatedSubBlocks [MetadataBlocksPerStorage]bool

	// mu protects concurrent access.
	mu sync.Mutex
}

// NewMetadataBlockManager creates a new metadata block manager.
func NewMetadataBlockManager(bm *BlockManager) *MetadataBlockManager {
	return &MetadataBlockManager{
		blockManager:      bm,
		currentBlockID:    InvalidBlockID,
		nextSubBlockIndex: 0,
	}
}

// AllocateSubBlock allocates a new metadata sub-block.
// Returns the MetaBlockPointer for the allocated sub-block.
//
// If all 64 sub-blocks in the current storage block are allocated,
// allocates a new storage block and continues allocation there.
func (m *MetadataBlockManager) AllocateSubBlock() (MetaBlockPointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Allocate a new storage block if needed
	if m.currentBlockID == InvalidBlockID || m.nextSubBlockIndex >= MetadataBlocksPerStorage {
		if err := m.allocateStorageBlock(); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to allocate storage block: %w", err)
		}
	}

	// Find the next available sub-block index
	subBlockIndex := m.nextSubBlockIndex

	// Mark this sub-block as allocated
	m.allocatedSubBlocks[subBlockIndex] = true

	// Advance to next sub-block
	m.nextSubBlockIndex++

	// Create MetaBlockPointer
	ptr := MetaBlockPointer{
		BlockID:    m.currentBlockID,
		BlockIndex: subBlockIndex,
		Offset:     0,
	}

	return ptr, nil
}

// WriteSubBlock writes data to a metadata sub-block.
// If data exceeds MetadataSubBlockDataSize, it chains to additional sub-blocks.
// Returns the MetaBlockPointer for the start of the data.
//
// Algorithm:
//  1. Calculate how many sub-blocks are needed
//  2. Allocate all required sub-blocks
//  3. Write data to sub-blocks with chaining pointers
//  4. Return pointer to first sub-block
func (m *MetadataBlockManager) WriteSubBlock(data []byte) (MetaBlockPointer, error) {
	if len(data) == 0 {
		return MetaBlockPointer{BlockID: InvalidBlockID}, nil
	}

	// Calculate number of sub-blocks needed
	numBlocks := (len(data) + MetadataSubBlockDataSize - 1) / MetadataSubBlockDataSize

	// Allocate all needed sub-blocks
	ptrs := make([]MetaBlockPointer, numBlocks)
	for i := 0; i < numBlocks; i++ {
		ptr, err := m.AllocateSubBlock()
		if err != nil {
			return MetaBlockPointer{}, fmt.Errorf(
				"failed to allocate sub-block %d/%d: %w",
				i+1,
				numBlocks,
				err,
			)
		}
		ptrs[i] = ptr
	}

	// Write data to sub-blocks with chaining
	for i := 0; i < numBlocks; i++ {
		start := i * MetadataSubBlockDataSize
		end := start + MetadataSubBlockDataSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		// Determine next block pointer
		nextBlockPtr := InvalidBlockID
		if i < numBlocks-1 {
			nextBlockPtr = ptrs[i+1].Encode()
		}

		// Write to sub-block
		if err := m.writeToSubBlock(ptrs[i], nextBlockPtr, chunk); err != nil {
			return MetaBlockPointer{}, fmt.Errorf(
				"failed to write sub-block %d/%d: %w",
				i+1,
				numBlocks,
				err,
			)
		}
	}

	return ptrs[0], nil
}

// writeToSubBlock writes data to a specific sub-block within a storage block.
// Format: [nextBlockPtr: 8 bytes][data: up to 4088 bytes]
func (m *MetadataBlockManager) writeToSubBlock(
	ptr MetaBlockPointer,
	nextBlockPtr uint64,
	data []byte,
) error {
	if len(data) > MetadataSubBlockDataSize {
		return fmt.Errorf(
			"data too large for sub-block: %d bytes (max %d)",
			len(data),
			MetadataSubBlockDataSize,
		)
	}

	if ptr.BlockIndex >= MetadataBlocksPerStorage {
		return fmt.Errorf("%w: %d", ErrInvalidSubBlockIndex, ptr.BlockIndex)
	}

	// Read the storage block if not already loaded
	var blockData []byte
	if ptr.BlockID == m.currentBlockID && m.currentBlockData != nil {
		blockData = m.currentBlockData
	} else {
		// Read the block from disk
		block, err := m.blockManager.ReadBlock(ptr.BlockID)
		if err != nil {
			// Block doesn't exist yet - create a new one
			blockData = make([]byte, DefaultBlockSize-BlockChecksumSize)
		} else {
			blockData = block.Data
		}
	}

	// Calculate offset within storage block for this sub-block
	subBlockOffset := uint64(ptr.BlockIndex) * MetadataSubBlockSize

	// Ensure we have enough space in block data
	if uint64(len(blockData)) < subBlockOffset+MetadataSubBlockSize {
		// Expand block data if needed
		newData := make([]byte, DefaultBlockSize-BlockChecksumSize)
		copy(newData, blockData)
		blockData = newData
	}

	// Write next block pointer (first 8 bytes of sub-block)
	binary.LittleEndian.PutUint64(blockData[subBlockOffset:subBlockOffset+8], nextBlockPtr)

	// Write data (after the next block pointer)
	copy(blockData[subBlockOffset+MetadataNextBlockSize:], data)

	// Zero-fill remaining space in sub-block
	dataEnd := subBlockOffset + MetadataNextBlockSize + uint64(len(data))
	subBlockEnd := subBlockOffset + MetadataSubBlockSize
	for i := dataEnd; i < subBlockEnd && i < uint64(len(blockData)); i++ {
		blockData[i] = 0
	}

	// Write the block back to disk
	block := &Block{
		ID:   ptr.BlockID,
		Type: BlockMetaData,
		Data: blockData,
	}

	if err := m.blockManager.WriteBlock(block); err != nil {
		return fmt.Errorf("failed to write storage block %d: %w", ptr.BlockID, err)
	}

	// Update current block cache if this is the current block
	if ptr.BlockID == m.currentBlockID {
		m.currentBlockData = blockData
	}

	return nil
}

// allocateStorageBlock allocates a new storage block for metadata sub-blocks.
// Resets the sub-block allocation tracking.
func (m *MetadataBlockManager) allocateStorageBlock() error {
	// Allocate new storage block
	blockID, err := m.blockManager.AllocateBlock()
	if err != nil {
		return fmt.Errorf("failed to allocate storage block: %w", err)
	}

	// Initialize block data
	blockData := make([]byte, DefaultBlockSize-BlockChecksumSize)

	// Update current block state
	m.currentBlockID = blockID
	m.currentBlockData = blockData
	m.nextSubBlockIndex = 0

	// Clear allocation tracking
	for i := range m.allocatedSubBlocks {
		m.allocatedSubBlocks[i] = false
	}

	return nil
}

// ReadSubBlock reads data from a metadata sub-block, following chains if necessary.
// Returns the complete data by following next block pointers.
func (m *MetadataBlockManager) ReadSubBlock(ptr MetaBlockPointer) ([]byte, error) {
	if !ptr.IsValid() {
		return nil, fmt.Errorf("invalid metadata block pointer")
	}

	var result []byte
	currentPtr := ptr

	// Follow the chain of sub-blocks
	for currentPtr.IsValid() {
		// Read the storage block
		block, err := m.blockManager.ReadBlock(currentPtr.BlockID)
		if err != nil {
			return nil, fmt.Errorf("failed to read storage block %d: %w", currentPtr.BlockID, err)
		}

		// Validate sub-block index
		if currentPtr.BlockIndex >= MetadataBlocksPerStorage {
			return nil, fmt.Errorf("%w: %d", ErrInvalidSubBlockIndex, currentPtr.BlockIndex)
		}

		// Calculate sub-block offset
		subBlockOffset := uint64(currentPtr.BlockIndex) * MetadataSubBlockSize

		// Check bounds
		if subBlockOffset+MetadataSubBlockSize > uint64(len(block.Data)) {
			return nil, fmt.Errorf(
				"sub-block offset %d exceeds block size %d",
				subBlockOffset,
				len(block.Data),
			)
		}

		// Read next block pointer (first 8 bytes)
		nextBlockPtr := binary.LittleEndian.Uint64(block.Data[subBlockOffset : subBlockOffset+8])

		// Read data (remaining bytes in sub-block)
		dataStart := subBlockOffset + MetadataNextBlockSize
		dataEnd := subBlockOffset + MetadataSubBlockSize

		// Find actual data length (trim trailing zeros for last block in chain)
		data := block.Data[dataStart:dataEnd]
		if nextBlockPtr == InvalidBlockID {
			// Last block - trim trailing zeros
			for len(data) > 0 && data[len(data)-1] == 0 {
				data = data[:len(data)-1]
			}
		}

		// Append to result
		result = append(result, data...)

		// Move to next block in chain
		if nextBlockPtr == InvalidBlockID {
			break
		}
		currentPtr = DecodeMetaBlockPointer(nextBlockPtr)

		// Safety check: prevent infinite loops
		if len(result) > 1024*1024 {
			return nil, fmt.Errorf("metadata chain too long: exceeded 1MB")
		}
	}

	return result, nil
}

// Flush writes any pending data to disk.
// Currently a no-op since writes are immediate, but kept for future optimizations.
func (m *MetadataBlockManager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// All writes are currently immediate via writeToSubBlock
	// This method exists for future write buffering optimizations
	return nil
}

// Reset clears the current block state.
// Used when starting fresh metadata allocation.
func (m *MetadataBlockManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentBlockID = InvalidBlockID
	m.currentBlockData = nil
	m.nextSubBlockIndex = 0
	for i := range m.allocatedSubBlocks {
		m.allocatedSubBlocks[i] = false
	}
}

// CurrentBlockID returns the current storage block ID being used for metadata.
// Returns InvalidBlockID if no block is currently allocated.
func (m *MetadataBlockManager) CurrentBlockID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.currentBlockID
}

// AllocatedSubBlockCount returns the number of sub-blocks allocated in the current storage block.
func (m *MetadataBlockManager) AllocatedSubBlockCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for i := 0; i < int(m.nextSubBlockIndex) && i < len(m.allocatedSubBlocks); i++ {
		if m.allocatedSubBlocks[i] {
			count++
		}
	}
	return count
}

// MetadataWriter implements io.Writer for writing to metadata sub-blocks.
// It buffers data and automatically allocates and chains sub-blocks as needed.
type MetadataWriter struct {
	manager *MetadataBlockManager

	// buffer accumulates data before writing to sub-blocks.
	buffer bytes.Buffer

	// startPointer is the first sub-block pointer (set after first flush).
	startPointer MetaBlockPointer

	// hasStartPointer indicates if startPointer is valid.
	hasStartPointer bool
}

// NewMetadataWriter creates a new MetadataWriter using the given manager.
func NewMetadataWriter(manager *MetadataBlockManager) *MetadataWriter {
	return &MetadataWriter{
		manager: manager,
	}
}

// Write implements io.Writer, buffering data for later flush.
func (w *MetadataWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

// Flush writes all buffered data to metadata sub-blocks.
// Returns the MetaBlockPointer to the start of the written data.
func (w *MetadataWriter) Flush() (MetaBlockPointer, error) {
	if w.buffer.Len() == 0 {
		return MetaBlockPointer{BlockID: InvalidBlockID}, nil
	}

	ptr, err := w.manager.WriteSubBlock(w.buffer.Bytes())
	if err != nil {
		return MetaBlockPointer{}, err
	}

	if !w.hasStartPointer {
		w.startPointer = ptr
		w.hasStartPointer = true
	}

	w.buffer.Reset()
	return w.startPointer, nil
}

// GetPointer returns the starting MetaBlockPointer.
// Must call Flush first.
func (w *MetadataWriter) GetPointer() MetaBlockPointer {
	return w.startPointer
}

// Reset clears the buffer and start pointer.
func (w *MetadataWriter) Reset() {
	w.buffer.Reset()
	w.startPointer = MetaBlockPointer{}
	w.hasStartPointer = false
}

// Len returns the current buffer size.
func (w *MetadataWriter) Len() int {
	return w.buffer.Len()
}
