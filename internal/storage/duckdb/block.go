package duckdb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Block manager error definitions.
var (
	// ErrBlockNotFound indicates the requested block ID does not exist.
	ErrBlockNotFound = errors.New("block not found")

	// ErrBlockChecksumFailed indicates a block checksum verification failed.
	ErrBlockChecksumFailed = errors.New("block checksum failed")

	// ErrNoFreeBlocks indicates no free blocks are available for allocation.
	ErrNoFreeBlocks = errors.New("no free blocks available")

	// ErrBlockManagerClosed indicates the block manager has been closed.
	ErrBlockManagerClosed = errors.New("block manager is closed")
)

// DefaultCacheCapacity is the default number of blocks to cache.
const DefaultCacheCapacity = 128

// Block represents a single block in the DuckDB file.
// Each block starts with an 8-byte checksum followed by data.
type Block struct {
	// ID is the block number (0-indexed from data blocks start).
	ID uint64

	// Type indicates the type of data stored in this block.
	Type BlockType

	// Checksum is stored at the BEGINNING of the block (first 8 bytes).
	// It covers the Data bytes only (not itself).
	Checksum uint64

	// Data contains the block contents after the checksum.
	// Size is BlockAllocSize - BlockChecksumSize.
	Data []byte
}

// BlockManager handles reading and writing blocks to the DuckDB file.
// It provides caching and manages the free list for block allocation.
type BlockManager struct {
	// file is the underlying file handle.
	file *os.File

	// blockSize is the size of each block in bytes (default 256KB).
	blockSize uint64

	// blockCount is the total number of allocated blocks.
	blockCount uint64

	// freeList tracks which blocks are available for allocation.
	freeList *FreeListManager

	// cache provides LRU caching of recently accessed blocks.
	cache *BlockCache

	// version is the storage format version.
	version uint64

	// closed indicates whether the block manager has been closed.
	closed bool

	// mu protects concurrent access to the block manager.
	mu sync.RWMutex
}

// FreeListManager tracks free blocks across transactions.
// DuckDB uses multiple sets to handle transactional block management.
type FreeListManager struct {
	// freeBlocks contains currently free blocks available for allocation.
	freeBlocks map[uint64]struct{}

	// freeBlocksInUse contains free blocks being used in the current transaction.
	freeBlocksInUse map[uint64]struct{}

	// newlyUsedBlocks contains blocks used since the last checkpoint.
	newlyUsedBlocks map[uint64]struct{}

	// mu protects concurrent access to the free list.
	mu sync.RWMutex
}

// CacheStats tracks cache performance metrics.
type CacheStats struct {
	// Hits is the number of cache hits.
	Hits uint64
	// Misses is the number of cache misses.
	Misses uint64
	// Evictions is the number of blocks evicted from cache.
	Evictions uint64
}

// HitRate returns the cache hit rate as a percentage (0-100).
// Returns 0 if there have been no accesses.
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total) * 100
}

// BlockCache provides LRU caching for blocks to reduce disk I/O.
type BlockCache struct {
	// capacity is the maximum number of blocks to cache.
	capacity int

	// blocks maps block IDs to cached blocks.
	blocks map[uint64]*list.Element

	// order is a doubly-linked list for LRU ordering.
	// Front is most recently used, back is least recently used.
	order *list.List

	// stats tracks cache performance metrics.
	stats CacheStats

	// mu protects concurrent access to the cache.
	mu sync.RWMutex
}

// cacheEntry wraps a block with its ID for the LRU list.
type cacheEntry struct {
	id    uint64
	block *Block
}

// NewBlockManager creates a new BlockManager for the given file.
func NewBlockManager(file *os.File, blockSize uint64, cacheSize int) *BlockManager {
	if cacheSize <= 0 {
		cacheSize = DefaultCacheCapacity
	}

	return &BlockManager{
		file:      file,
		blockSize: blockSize,
		freeList:  NewFreeListManager(),
		cache:     NewBlockCache(cacheSize),
		version:   CurrentVersion,
	}
}

// NewFreeListManager creates a new FreeListManager.
func NewFreeListManager() *FreeListManager {
	return &FreeListManager{
		freeBlocks:      make(map[uint64]struct{}),
		freeBlocksInUse: make(map[uint64]struct{}),
		newlyUsedBlocks: make(map[uint64]struct{}),
	}
}

// NewBlockCache creates a new BlockCache with the given capacity.
func NewBlockCache(capacity int) *BlockCache {
	if capacity <= 0 {
		capacity = DefaultCacheCapacity
	}

	return &BlockCache{
		capacity: capacity,
		blocks:   make(map[uint64]*list.Element),
		order:    list.New(),
	}
}

// blockOffset calculates the file offset for a given block ID.
// Block offset = FileHeaderSize + 2*DatabaseHeaderSize + blockID * blockSize.
func (bm *BlockManager) blockOffset(id uint64) int64 {
	return int64(DataBlocksOffset) + int64(id)*int64(bm.blockSize)
}

// ReadBlock reads a block from the file by ID.
// Returns a cached block if available, otherwise reads from disk.
func (bm *BlockManager) ReadBlock(id uint64) (*Block, error) {
	bm.mu.RLock()
	if bm.closed {
		bm.mu.RUnlock()
		return nil, ErrBlockManagerClosed
	}
	bm.mu.RUnlock()

	// Check cache first
	if cached := bm.cache.Get(id); cached != nil {
		return cached, nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check after acquiring lock
	if bm.closed {
		return nil, ErrBlockManagerClosed
	}

	// Check cache again (may have been populated while waiting for lock)
	if cached := bm.cache.Get(id); cached != nil {
		return cached, nil
	}

	// Calculate file offset
	offset := bm.blockOffset(id)

	// Read raw block data
	rawData := make([]byte, bm.blockSize)
	n, err := bm.file.ReadAt(rawData, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read block %d: %w", id, err)
	}
	if uint64(n) < bm.blockSize {
		return nil, fmt.Errorf("incomplete block read: got %d bytes, expected %d", n, bm.blockSize)
	}

	// Checksum is at the BEGINNING of the block (first 8 bytes)
	storedChecksum := binary.LittleEndian.Uint64(rawData[:BlockChecksumSize])

	// Data is everything after the checksum
	data := make([]byte, bm.blockSize-BlockChecksumSize)
	copy(data, rawData[BlockChecksumSize:])

	// Verify checksum
	computedChecksum := checksumBlock(data)
	if computedChecksum != storedChecksum {
		return nil, fmt.Errorf("%w: block %d expected checksum %016x, got %016x",
			ErrBlockChecksumFailed, id, storedChecksum, computedChecksum)
	}

	block := &Block{
		ID:       id,
		Checksum: storedChecksum,
		Data:     data,
	}

	// Add to cache
	bm.cache.Put(block)

	return block, nil
}

// WriteBlock writes a block to the file.
// Computes checksum and writes to the appropriate offset.
func (bm *BlockManager) WriteBlock(block *Block) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return ErrBlockManagerClosed
	}

	// Compute checksum for data
	block.Checksum = checksumBlock(block.Data)

	// Build raw block with checksum at beginning
	rawData := make([]byte, bm.blockSize)
	binary.LittleEndian.PutUint64(rawData[:BlockChecksumSize], block.Checksum)

	// Copy data after checksum
	copy(rawData[BlockChecksumSize:], block.Data)

	// Calculate file offset
	offset := bm.blockOffset(block.ID)

	// Write to file
	n, err := bm.file.WriteAt(rawData, offset)
	if err != nil {
		return fmt.Errorf("failed to write block %d: %w", block.ID, err)
	}
	if uint64(n) < bm.blockSize {
		return fmt.Errorf("incomplete block write: wrote %d bytes, expected %d", n, bm.blockSize)
	}

	// Update cache
	bm.cache.Put(block)

	// Update block count if necessary
	if block.ID >= bm.blockCount {
		bm.blockCount = block.ID + 1
	}

	return nil
}

// AllocateBlock allocates a new block for writing.
// Returns a block ID from the free list if available, otherwise allocates new.
func (bm *BlockManager) AllocateBlock() (uint64, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return 0, ErrBlockManagerClosed
	}

	// Try to get a block from the free list
	if id, ok := bm.freeList.GetFreeBlock(); ok {
		return id, nil
	}

	// Allocate a new block ID
	id := bm.blockCount
	bm.blockCount++

	// Mark it as used
	bm.freeList.MarkUsed(id)

	return id, nil
}

// FreeBlock marks a block as free for future reuse.
func (bm *BlockManager) FreeBlock(id uint64) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return ErrBlockManagerClosed
	}

	// Remove from cache
	bm.cache.Invalidate(id)

	// Mark as free
	bm.freeList.MarkFree(id)

	return nil
}

// BlockCount returns the total number of allocated blocks.
func (bm *BlockManager) BlockCount() uint64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	return bm.blockCount
}

// BlockSize returns the block size in bytes.
func (bm *BlockManager) BlockSize() uint64 {
	return bm.blockSize
}

// CacheStats returns the current cache statistics.
func (bm *BlockManager) CacheStats() CacheStats {
	return bm.cache.Stats()
}

// ResetCacheStats resets the cache statistics.
func (bm *BlockManager) ResetCacheStats() {
	bm.cache.ResetStats()
}

// SetBlockCount sets the block count (used when loading from file).
func (bm *BlockManager) SetBlockCount(count uint64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.blockCount = count
}

// Sync flushes all pending writes to disk.
func (bm *BlockManager) Sync() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return ErrBlockManagerClosed
	}

	return bm.file.Sync()
}

// Close closes the block manager and releases resources.
func (bm *BlockManager) Close() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return nil
	}

	bm.closed = true

	// Clear the cache
	bm.cache.Clear()

	// Note: We don't close the file here as it's owned by the caller
	return nil
}

// Get retrieves a block from the cache.
// Returns nil if the block is not cached.
func (bc *BlockCache) Get(id uint64) *Block {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if elem, ok := bc.blocks[id]; ok {
		// Move to front (most recently used)
		bc.order.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		bc.stats.Hits++
		return entry.block
	}

	bc.stats.Misses++
	return nil
}

// Put adds a block to the cache.
// Evicts the least recently used block if at capacity.
func (bc *BlockCache) Put(block *Block) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Check if already in cache
	if elem, ok := bc.blocks[block.ID]; ok {
		// Update the block and move to front
		entry := elem.Value.(*cacheEntry)
		entry.block = block
		bc.order.MoveToFront(elem)
		return
	}

	// Evict if at capacity
	for len(bc.blocks) >= bc.capacity {
		bc.evictLRU()
	}

	// Add new entry at front
	entry := &cacheEntry{
		id:    block.ID,
		block: block,
	}
	elem := bc.order.PushFront(entry)
	bc.blocks[block.ID] = elem
}

// evictLRU removes the least recently used block from the cache.
// Must be called with lock held.
func (bc *BlockCache) evictLRU() {
	if bc.order.Len() == 0 {
		return
	}

	// Get the back element (least recently used)
	back := bc.order.Back()
	if back == nil {
		return
	}

	entry := back.Value.(*cacheEntry)
	delete(bc.blocks, entry.id)
	bc.order.Remove(back)
	bc.stats.Evictions++
}

// Invalidate removes a specific block from the cache.
func (bc *BlockCache) Invalidate(id uint64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if elem, ok := bc.blocks[id]; ok {
		delete(bc.blocks, id)
		bc.order.Remove(elem)
	}
}

// Clear removes all blocks from the cache.
func (bc *BlockCache) Clear() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.blocks = make(map[uint64]*list.Element)
	bc.order.Init()
}

// Size returns the current number of blocks in the cache.
func (bc *BlockCache) Size() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	return len(bc.blocks)
}

// Stats returns a copy of the current cache statistics.
func (bc *BlockCache) Stats() CacheStats {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	return bc.stats
}

// ResetStats resets the cache statistics to zero.
func (bc *BlockCache) ResetStats() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.stats = CacheStats{}
}

// IsFree checks if a block is in the free list.
func (fl *FreeListManager) IsFree(id uint64) bool {
	fl.mu.RLock()
	defer fl.mu.RUnlock()

	_, ok := fl.freeBlocks[id]

	return ok
}

// MarkFree adds a block to the free list.
func (fl *FreeListManager) MarkFree(id uint64) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	fl.freeBlocks[id] = struct{}{}

	// Remove from newly used if present
	delete(fl.newlyUsedBlocks, id)
}

// MarkUsed removes a block from the free list and tracks it as newly used.
func (fl *FreeListManager) MarkUsed(id uint64) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	delete(fl.freeBlocks, id)
	fl.newlyUsedBlocks[id] = struct{}{}
}

// GetFreeBlock returns a free block ID and marks it as in use.
// Returns (id, true) if a free block is available, (0, false) otherwise.
func (fl *FreeListManager) GetFreeBlock() (uint64, bool) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	for id := range fl.freeBlocks {
		delete(fl.freeBlocks, id)
		fl.freeBlocksInUse[id] = struct{}{}

		return id, true
	}

	return 0, false
}

// AllocateFromFreeList returns a free block ID, or 0 if none available.
// This is an alias for GetFreeBlock for backward compatibility.
func (fl *FreeListManager) AllocateFromFreeList() (uint64, bool) {
	return fl.GetFreeBlock()
}

// FreeBlockCount returns the number of free blocks available.
func (fl *FreeListManager) FreeBlockCount() int {
	fl.mu.RLock()
	defer fl.mu.RUnlock()

	return len(fl.freeBlocks)
}

// CommitTransaction moves blocks from freeBlocksInUse to appropriate state.
func (fl *FreeListManager) CommitTransaction() {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Clear the in-use tracking (blocks are now committed)
	fl.freeBlocksInUse = make(map[uint64]struct{})
}

// RollbackTransaction returns blocks from freeBlocksInUse to the free list.
func (fl *FreeListManager) RollbackTransaction() {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Return in-use blocks to free list
	for id := range fl.freeBlocksInUse {
		fl.freeBlocks[id] = struct{}{}
	}

	fl.freeBlocksInUse = make(map[uint64]struct{})
}

// Checkpoint clears newly used blocks (they're now persisted).
func (fl *FreeListManager) Checkpoint() {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	fl.newlyUsedBlocks = make(map[uint64]struct{})
}

// Serialize writes the free list to a BinaryWriter.
func (fl *FreeListManager) Serialize(w *BinaryWriter) error {
	fl.mu.RLock()
	defer fl.mu.RUnlock()

	// Write count of free blocks
	w.WriteUint64(uint64(len(fl.freeBlocks)))

	// Write each free block ID
	for id := range fl.freeBlocks {
		w.WriteUint64(id)
	}

	return w.Err()
}

// Deserialize reads the free list from a BinaryReader.
func (fl *FreeListManager) Deserialize(r *BinaryReader) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Read count of free blocks
	count := r.ReadUint64()
	if r.Err() != nil {
		return r.Err()
	}

	// Clear and read free blocks
	fl.freeBlocks = make(map[uint64]struct{}, count)

	for i := uint64(0); i < count; i++ {
		id := r.ReadUint64()
		if r.Err() != nil {
			return r.Err()
		}
		fl.freeBlocks[id] = struct{}{}
	}

	return nil
}

// SerializeToBytes serializes the free list to bytes.
func (fl *FreeListManager) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	if err := fl.Serialize(w); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeFromBytes deserializes the free list from bytes.
func (fl *FreeListManager) DeserializeFromBytes(data []byte) error {
	r := NewBinaryReader(bytes.NewReader(data))
	return fl.Deserialize(r)
}
