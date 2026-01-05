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
type DatabaseHeader struct {
	// Iteration is incremented on each checkpoint.
	// Used to determine which of the two headers is current.
	Iteration uint64

	// MetaBlock points to the catalog metadata block.
	MetaBlock BlockPointer

	// FreeList points to the free block list.
	FreeList BlockPointer

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

// Checksum constants for DuckDB's custom hash algorithm.
const (
	// checksumMultiplier is used for 8-byte aligned chunks.
	checksumMultiplier uint64 = 0xbf58476d1ce4e5b9

	// murmurSeed is the seed used for the MurmurHash variant on remaining bytes.
	murmurSeed uint64 = 0xe17a1465

	// murmurC1 and murmurC2 are MurmurHash mixing constants.
	murmurC1 uint64 = 0x87c37b91114253d5
	murmurC2 uint64 = 0x4cf5ad432745937f
)

// checksumBlock computes DuckDB's custom checksum for a block of data.
// The algorithm processes 8-byte aligned chunks using a multiplication hash,
// then handles remaining bytes with a MurmurHash variant.
func checksumBlock(data []byte) uint64 {
	var hash uint64

	// Process 8-byte chunks
	fullChunks := len(data) / 8
	for i := 0; i < fullChunks; i++ {
		value := binary.LittleEndian.Uint64(data[i*8:])
		hash ^= value * checksumMultiplier
	}

	// Process remaining bytes with MurmurHash variant
	remaining := len(data) % 8
	if remaining > 0 {
		offset := len(data) - remaining
		hash ^= murmurHashBytes(data[offset:], murmurSeed)
	}

	return hash
}

// murmurHashBytes computes a MurmurHash variant for remaining bytes.
// This handles the tail bytes that don't fit in a full 8-byte chunk.
func murmurHashBytes(data []byte, seed uint64) uint64 {
	var k1 uint64

	// Pack remaining bytes into k1 (up to 7 bytes)
	for i := len(data) - 1; i >= 0; i-- {
		k1 = (k1 << 8) | uint64(data[i])
	}

	// MurmurHash3 mixing for the tail
	k1 *= murmurC1
	k1 = rotateLeft64(k1, 31)
	k1 *= murmurC2

	h := seed ^ k1

	// Finalization mix
	h ^= uint64(len(data))
	h = fmix64(h)

	return h
}

// rotateLeft64 performs a left rotation on a 64-bit value.
func rotateLeft64(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}

// fmix64 is the finalization mix function from MurmurHash3.
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// DatabaseHeaderDataSize is the size of the DatabaseHeader data in bytes.
// This is the data that gets checksummed.
const DatabaseHeaderDataSize = 56 // 8 + 12 + 12 + 8 + 8 + 8 + 8 = 64, but BlockPointer is 12 bytes (8+4)
// Actually: Iteration(8) + MetaBlock(8+4=12) + FreeList(8+4=12) + BlockCount(8) + BlockAllocSize(8) + VectorSize(8) + SerializationCompatibility(8) = 64

// Correct calculation:
// Iteration: 8 bytes
// MetaBlock.BlockID: 8 bytes
// MetaBlock.Offset: 4 bytes
// FreeList.BlockID: 8 bytes
// FreeList.Offset: 4 bytes
// BlockCount: 8 bytes
// BlockAllocSize: 8 bytes
// VectorSize: 8 bytes
// SerializationCompatibility: 8 bytes
// Total: 8 + 8 + 4 + 8 + 4 + 8 + 8 + 8 + 8 = 64 bytes

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
// This is the data that gets checksummed.
func serializeDatabaseHeader(header *DatabaseHeader) []byte {
	buf := new(bytes.Buffer)
	bw := NewBinaryWriter(buf)

	bw.WriteUint64(header.Iteration)
	bw.WriteUint64(header.MetaBlock.BlockID)
	bw.WriteUint32(header.MetaBlock.Offset)
	bw.WriteUint64(header.FreeList.BlockID)
	bw.WriteUint32(header.FreeList.Offset)
	bw.WriteUint64(header.BlockCount)
	bw.WriteUint64(header.BlockAllocSize)
	bw.WriteUint64(header.VectorSize)
	bw.WriteUint64(header.SerializationCompatibility)

	return buf.Bytes()
}

// deserializeDatabaseHeader deserializes a DatabaseHeader from bytes.
func deserializeDatabaseHeader(data []byte) (*DatabaseHeader, error) {
	if len(data) < 64 {
		return nil, fmt.Errorf("database header data too short: got %d bytes, need 64", len(data))
	}

	br := NewBinaryReader(bytes.NewReader(data))

	header := &DatabaseHeader{
		Iteration: br.ReadUint64(),
		MetaBlock: BlockPointer{
			BlockID: br.ReadUint64(),
			Offset:  br.ReadUint32(),
		},
		FreeList: BlockPointer{
			BlockID: br.ReadUint64(),
			Offset:  br.ReadUint32(),
		},
		BlockCount:                br.ReadUint64(),
		BlockAllocSize:            br.ReadUint64(),
		VectorSize:                br.ReadUint64(),
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
	if n < BlockChecksumSize+64 {
		return nil, fmt.Errorf("database header block too short: got %d bytes", n)
	}

	// Read stored checksum (first 8 bytes of block)
	storedChecksum := binary.LittleEndian.Uint64(blockData[0:BlockChecksumSize])

	// Header data starts after checksum
	headerData := blockData[BlockChecksumSize : BlockChecksumSize+64]

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

// NewDatabaseHeader creates a new database header with default values.
func NewDatabaseHeader() *DatabaseHeader {
	return &DatabaseHeader{
		Iteration:                  0,
		MetaBlock:                  InvalidBlockPointer(),
		FreeList:                   InvalidBlockPointer(),
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
