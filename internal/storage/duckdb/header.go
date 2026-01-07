package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Header-related error definitions.
var (
	// ErrNotDuckDBFile indicates the file does not have valid DuckDB magic bytes.
	ErrNotDuckDBFile = errors.New("not a valid DuckDB file: magic bytes mismatch")

	// ErrUnsupportedVersion indicates the file version is newer than supported.
	ErrUnsupportedVersion = errors.New("unsupported DuckDB file version")

	// ErrBothHeadersCorrupted indicates both database headers failed checksum validation.
	ErrBothHeadersCorrupted = errors.New("both database headers are corrupted")

	// ErrChecksumMismatch indicates a block/header checksum verification failed.
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

// FileHeader represents the DuckDB file header stored at offset 0-4095.
// The file header contains magic bytes and version information.
type FileHeader struct {
	// BlockHeaderStorage is reserved space for block manager (first 8 bytes).
	BlockHeaderStorage [8]byte

	// Magic contains the magic bytes "DUCK" identifying a valid DuckDB file.
	Magic [4]byte

	// Version is the storage format version number.
	// Version 67 corresponds to DuckDB v1.4.3.
	Version uint64

	// Flags contains feature flags for the database file.
	Flags uint64
}

// DatabaseHeader represents a database header block (at offset 4096 or 8192).
// DuckDB uses dual-header design for crash recovery - the header with the
// higher iteration count is the active one.
//
// Note: The checksum is stored SEPARATELY at the beginning of each database
// header block (first 8 bytes), NOT as a field in this struct. The checksum
// covers the DatabaseHeader DATA bytes only.
//
// The header is 56 bytes (7 × uint64):
// - iteration: 8 bytes
// - meta_block: 8 bytes (block ID, not a BlockPointer)
// - free_list: 8 bytes (block ID, not a BlockPointer)
// - block_count: 8 bytes
// - block_alloc_size: 8 bytes
// - vector_size: 8 bytes
// - serialization_compatibility: 8 bytes
type DatabaseHeader struct {
	// Iteration is incremented on each checkpoint.
	// Used to determine which of the two headers is current.
	Iteration uint64

	// MetaBlock is the block ID of the catalog metadata block.
	// This is a simple block ID (uint64), not a BlockPointer.
	MetaBlock uint64

	// FreeList is the block ID of the free block list.
	// This is a simple block ID (uint64), not a BlockPointer.
	FreeList uint64

	// BlockCount is the total number of allocated blocks.
	BlockCount uint64

	// BlockAllocSize is the block allocation size (usually 262144 = 256KB).
	BlockAllocSize uint64

	// VectorSize is the number of rows per vector (usually 2048).
	VectorSize uint64

	// SerializationCompatibility is the serialization compatibility version.
	SerializationCompatibility uint64
}

// BlockPointer represents a pointer to a location within a block.
// Used for catalog metadata and data references.
type BlockPointer struct {
	// BlockID is the block number (0-indexed from data blocks start).
	BlockID uint64

	// Offset is the byte offset within the block.
	Offset uint32
}

// MetaBlockPointer points to a metadata block containing serialized data.
// Used in RowGroupPointer to reference column DataPointers.
type MetaBlockPointer struct {
	// BlockID is the metadata block number.
	BlockID uint64

	// Offset is the byte offset within the metadata block.
	Offset uint64
}

// InvalidBlockPointer returns a block pointer that represents an invalid/null pointer.
func InvalidBlockPointer() BlockPointer {
	return BlockPointer{
		BlockID: ^uint64(0),
		Offset:  ^uint32(0),
	}
}

// IsValid returns true if this is a valid (non-null) block pointer.
func (bp BlockPointer) IsValid() bool {
	// A pointer is considered invalid if both BlockID and Offset are at their
	// maximum values (0xFFFFFFFFFFFFFFFF and 0xFFFFFFFF respectively).
	return bp.BlockID != ^uint64(0) || bp.Offset != ^uint32(0)
}

// IsValid returns true if this is a valid meta block pointer.
func (mp MetaBlockPointer) IsValid() bool {
	return mp.BlockID != ^uint64(0)
}

// Checksum constants for DuckDB's checksum algorithm.
// Based on MurmurHash2 variant from robin-hood-hashing.
const (
	// checksumMultiplier is used for 8-byte aligned chunks.
	checksumMultiplier uint64 = 0xbf58476d1ce4e5b9

	// checksumInitial is the initial hash value (same as djb2 hash seed).
	checksumInitial uint64 = 5381

	// murmurM is the mixing constant for MurmurHash2.
	murmurM uint64 = 0xc6a4a7935bd1e995

	// murmurSeed is the seed used for the MurmurHash2 remainder.
	murmurSeed uint64 = 0xe17a1465

	// murmurR is the shift constant for MurmurHash2.
	murmurR uint = 47
)

// checksumBlock computes DuckDB's checksum for a block of data.
// The algorithm:
// 1. Start with initial value 5381
// 2. XOR each 8-byte chunk multiplied by a constant
// 3. Process remaining bytes with MurmurHash2 variant
func checksumBlock(data []byte) uint64 {
	result := checksumInitial

	// Process 8-byte chunks
	fullChunks := len(data) / 8
	for i := 0; i < fullChunks; i++ {
		value := binary.LittleEndian.Uint64(data[i*8:])
		result ^= value * checksumMultiplier
	}

	// Process remaining bytes with MurmurHash2 variant
	remaining := len(data) % 8
	if remaining > 0 {
		offset := fullChunks * 8
		result ^= checksumRemainder(data[offset:])
	}

	return result
}

// checksumRemainder computes MurmurHash2 variant for remaining bytes.
// This is based on robin-hood-hashing's hash implementation.
func checksumRemainder(data []byte) uint64 {
	length := len(data)
	h := murmurSeed ^ (uint64(length) * murmurM)

	// Process 8-byte chunks within the remainder (only if len >= 8)
	fullChunks := length / 8
	for i := 0; i < fullChunks; i++ {
		k := binary.LittleEndian.Uint64(data[i*8:])
		k *= murmurM
		k ^= k >> murmurR
		k *= murmurM
		h ^= k
		h *= murmurM
	}

	// Process remaining tail bytes (0-7 bytes)
	offset := fullChunks * 8
	remaining := data[offset:]

	switch len(remaining) {
	case 7:
		h ^= uint64(remaining[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(remaining[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(remaining[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(remaining[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(remaining[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(remaining[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(remaining[0])
		h *= murmurM
	}

	// Final mixing
	h ^= h >> murmurR
	h *= murmurM
	h ^= h >> murmurR

	return h
}

// DatabaseHeaderDataSize is the size of the DatabaseHeader data in bytes.
// This is the data that gets checksummed.
// Total: 7 × 8 bytes = 56 bytes
const DatabaseHeaderDataSize = 56

// FileHeaderDataSize is the size of the FileHeader data that follows the block header storage.
const FileHeaderDataSize = 20 // Magic(4) + Version(8) + Flags(8) = 20 bytes

// ReadFileHeader reads the file header from offset 0.
func ReadFileHeader(r io.ReaderAt) (*FileHeader, error) {
	data := make([]byte, FileHeaderSize)
	n, err := r.ReadAt(data, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}
	if n < MagicByteOffset+4 {
		return nil, fmt.Errorf("file too small: read %d bytes, need at least %d", n, MagicByteOffset+4)
	}

	header := &FileHeader{}

	// Copy block header storage (first 8 bytes)
	copy(header.BlockHeaderStorage[:], data[0:8])

	// Copy magic bytes (offset 8-11)
	copy(header.Magic[:], data[MagicByteOffset:MagicByteOffset+4])

	// Read version (offset 12-19)
	header.Version = binary.LittleEndian.Uint64(data[12:20])

	// Read flags (offset 20-27)
	header.Flags = binary.LittleEndian.Uint64(data[20:28])

	return header, nil
}

// WriteFileHeader writes the file header to offset 0.
func WriteFileHeader(w io.WriterAt, header *FileHeader) error {
	data := make([]byte, FileHeaderSize)

	// Copy block header storage (first 8 bytes)
	copy(data[0:8], header.BlockHeaderStorage[:])

	// Write magic bytes (offset 8-11)
	copy(data[MagicByteOffset:MagicByteOffset+4], header.Magic[:])

	// Write version (offset 12-19)
	binary.LittleEndian.PutUint64(data[12:20], header.Version)

	// Write flags (offset 20-27)
	binary.LittleEndian.PutUint64(data[20:28], header.Flags)

	// Rest is zero-padded (already zero from make)

	_, err := w.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to write file header: %w", err)
	}

	return nil
}

// NewFileHeader creates a new file header with default values.
func NewFileHeader() *FileHeader {
	header := &FileHeader{
		Version: CurrentVersion,
		Flags:   0,
	}
	copy(header.Magic[:], MagicBytes)
	return header
}

// serializeDatabaseHeader serializes a DatabaseHeader to bytes.
// This is the data that gets checksummed (56 bytes).
func serializeDatabaseHeader(header *DatabaseHeader) []byte {
	buf := new(bytes.Buffer)
	bw := NewBinaryWriter(buf)

	bw.WriteUint64(header.Iteration)
	bw.WriteUint64(header.MetaBlock)
	bw.WriteUint64(header.FreeList)
	bw.WriteUint64(header.BlockCount)
	bw.WriteUint64(header.BlockAllocSize)
	bw.WriteUint64(header.VectorSize)
	bw.WriteUint64(header.SerializationCompatibility)

	return buf.Bytes()
}

// deserializeDatabaseHeader deserializes a DatabaseHeader from bytes.
func deserializeDatabaseHeader(data []byte) (*DatabaseHeader, error) {
	if len(data) < DatabaseHeaderDataSize {
		return nil, fmt.Errorf("database header data too short: got %d bytes, need %d", len(data), DatabaseHeaderDataSize)
	}

	br := NewBinaryReader(bytes.NewReader(data))

	header := &DatabaseHeader{
		Iteration:                  br.ReadUint64(),
		MetaBlock:                  br.ReadUint64(),
		FreeList:                   br.ReadUint64(),
		BlockCount:                 br.ReadUint64(),
		BlockAllocSize:             br.ReadUint64(),
		VectorSize:                 br.ReadUint64(),
		SerializationCompatibility: br.ReadUint64(),
	}

	if err := br.Err(); err != nil {
		return nil, fmt.Errorf("failed to deserialize database header: %w", err)
	}

	return header, nil
}

// ReadDatabaseHeader reads a database header from the specified offset.
// The offset should be either DatabaseHeader1Offset or DatabaseHeader2Offset.
// Returns the header and any checksum validation error.
func ReadDatabaseHeader(r io.ReaderAt, offset int64) (*DatabaseHeader, error) {
	// Read the entire database header block
	blockData := make([]byte, DatabaseHeaderSize)
	n, err := r.ReadAt(blockData, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read database header at offset %d: %w", offset, err)
	}
	if n < BlockChecksumSize+DatabaseHeaderDataSize {
		return nil, fmt.Errorf("database header block too short: got %d bytes", n)
	}

	// Read stored checksum (first 8 bytes of block)
	storedChecksum := binary.LittleEndian.Uint64(blockData[0:BlockChecksumSize])

	// Header data starts after checksum (56 bytes)
	headerData := blockData[BlockChecksumSize : BlockChecksumSize+DatabaseHeaderDataSize]

	// Compute checksum of header data
	computedChecksum := checksumBlock(headerData)

	// Verify checksum
	if storedChecksum != computedChecksum {
		return nil, fmt.Errorf("%w: stored %016x, computed %016x",
			ErrChecksumMismatch, storedChecksum, computedChecksum)
	}

	// Deserialize the header
	return deserializeDatabaseHeader(headerData)
}

// WriteDatabaseHeader writes a database header to the specified offset.
// The offset should be either DatabaseHeader1Offset or DatabaseHeader2Offset.
// Computes and writes the checksum at the beginning of the block.
func WriteDatabaseHeader(w io.WriterAt, header *DatabaseHeader, offset int64) error {
	// Serialize the header data
	headerData := serializeDatabaseHeader(header)

	// Compute checksum
	checksum := checksumBlock(headerData)

	// Build the full block
	blockData := make([]byte, DatabaseHeaderSize)

	// Write checksum at the beginning
	binary.LittleEndian.PutUint64(blockData[0:BlockChecksumSize], checksum)

	// Write header data after checksum
	copy(blockData[BlockChecksumSize:], headerData)

	// Rest is zero-padded

	_, err := w.WriteAt(blockData, offset)
	if err != nil {
		return fmt.Errorf("failed to write database header at offset %d: %w", offset, err)
	}

	return nil
}

// InvalidBlockID is the value used to indicate an invalid/null block reference.
const InvalidBlockID = ^uint64(0) // 0xFFFFFFFFFFFFFFFF

// IsValidBlockID returns true if the block ID is valid (not InvalidBlockID).
func IsValidBlockID(blockID uint64) bool {
	return blockID != InvalidBlockID
}

// NewDatabaseHeader creates a new database header with default values.
func NewDatabaseHeader() *DatabaseHeader {
	return &DatabaseHeader{
		Iteration:                  0,
		MetaBlock:                  InvalidBlockID,
		FreeList:                   InvalidBlockID,
		BlockCount:                 0,
		BlockAllocSize:             DefaultBlockSize,
		VectorSize:                 DefaultVectorSize,
		SerializationCompatibility: CurrentVersion,
	}
}

// ValidateFileHeader checks if a FileHeader is valid.
func ValidateFileHeader(header *FileHeader) error {
	if header == nil {
		return errors.New("file header is nil")
	}

	// Check magic bytes equal "DUCK"
	if !bytes.Equal(header.Magic[:], []byte(MagicBytes)) {
		return fmt.Errorf("%w: got %q, expected %q",
			ErrNotDuckDBFile, string(header.Magic[:]), MagicBytes)
	}

	// Check version is supported
	if header.Version > CurrentVersion {
		return fmt.Errorf("%w: file version %d, max supported %d",
			ErrUnsupportedVersion, header.Version, CurrentVersion)
	}

	return nil
}

// ValidateDatabaseHeader checks if a DatabaseHeader is valid.
func ValidateDatabaseHeader(header *DatabaseHeader) error {
	if header == nil {
		return errors.New("database header is nil")
	}

	// Validate block allocation size is reasonable
	if header.BlockAllocSize == 0 {
		return errors.New("invalid database header: block allocation size is 0")
	}

	// Validate vector size is reasonable
	if header.VectorSize == 0 {
		return errors.New("invalid database header: vector size is 0")
	}

	return nil
}

// SelectActiveHeader returns the active database header from the two candidates.
// The header with the higher iteration count is considered current.
// Returns the active header or an error if both headers are invalid.
func SelectActiveHeader(h1, h2 *DatabaseHeader, err1, err2 error) (*DatabaseHeader, error) {
	// If both have errors, return error
	if err1 != nil && err2 != nil {
		return nil, fmt.Errorf("%w: header1: %v, header2: %v",
			ErrBothHeadersCorrupted, err1, err2)
	}

	// If header 1 has error, return header 2
	if err1 != nil {
		return h2, nil
	}

	// If header 2 has error, return header 1
	if err2 != nil {
		return h1, nil
	}

	// Both are valid - return the one with higher iteration
	if h2.Iteration > h1.Iteration {
		return h2, nil
	}

	return h1, nil
}

// GetActiveHeader reads both database headers and returns the active one.
// This is a convenience function that handles the dual-header logic.
func GetActiveHeader(r io.ReaderAt) (*DatabaseHeader, int, error) {
	h1, err1 := ReadDatabaseHeader(r, DatabaseHeader1Offset)
	h2, err2 := ReadDatabaseHeader(r, DatabaseHeader2Offset)

	header, err := SelectActiveHeader(h1, h2, err1, err2)
	if err != nil {
		return nil, 0, err
	}

	// Determine which header slot is active (1 or 2)
	slot := 1
	if err1 != nil || (err2 == nil && h2.Iteration > h1.Iteration) {
		slot = 2
	}

	return header, slot, nil
}

// GetNextHeaderSlot returns the slot number (1 or 2) for the next checkpoint.
// DuckDB alternates between the two header slots.
func GetNextHeaderSlot(currentSlot int) int {
	if currentSlot == 1 {
		return 2
	}
	return 1
}

// GetHeaderOffset returns the file offset for a header slot (1 or 2).
func GetHeaderOffset(slot int) int64 {
	if slot == 2 {
		return DatabaseHeader2Offset
	}
	return DatabaseHeader1Offset
}

// FileHeaderChecksum computes the checksum for the file header.
// Note: The file header itself typically does not have a checksum in DuckDB's format,
// but we provide this for consistency with the block checksum API.
func FileHeaderChecksum(data []byte) uint64 {
	return checksumBlock(data)
}

// DatabaseHeaderChecksum computes the checksum for a database header.
// The checksum is stored at the beginning of each database header block.
func DatabaseHeaderChecksum(data []byte) uint64 {
	return checksumBlock(data)
}
