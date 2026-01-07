package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChecksumBlock tests the custom checksum algorithm.
// The algorithm starts with checksumInitial (5381) and XORs in each chunk's hash.
func TestChecksumBlock(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		checksum := checksumBlock([]byte{})
		// With no data, result is just the initial value 5381 (0x1505)
		assert.Equal(t, checksumInitial, checksum, "empty data should produce initial value (5381)")
	})

	t.Run("single byte", func(t *testing.T) {
		checksum := checksumBlock([]byte{0x42})
		assert.NotEqual(t, checksumInitial, checksum, "single byte should produce different checksum than initial")
	})

	t.Run("exactly 8 bytes", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		checksum := checksumBlock(data)

		// Compute expected: initial XOR (value * multiplier)
		value := binary.LittleEndian.Uint64(data)
		expected := checksumInitial ^ (value * checksumMultiplier)

		assert.Equal(t, expected, checksum, "8-byte data should XOR multiplication hash with initial")
	})

	t.Run("16 bytes", func(t *testing.T) {
		data := make([]byte, 16)
		for i := range data {
			data[i] = byte(i + 1)
		}
		checksum := checksumBlock(data)

		// Should XOR initial with two multiplication hashes
		v1 := binary.LittleEndian.Uint64(data[0:8])
		v2 := binary.LittleEndian.Uint64(data[8:16])
		expected := checksumInitial ^ (v1 * checksumMultiplier) ^ (v2 * checksumMultiplier)

		assert.Equal(t, expected, checksum, "16-byte data should XOR two hashes with initial")
	})

	t.Run("9 bytes uses MurmurHash for tail", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
		checksum := checksumBlock(data)
		assert.NotEqual(t, uint64(0), checksum, "9-byte data should produce non-zero checksum")

		// Verify it's different from just the 8-byte portion
		checksum8 := checksumBlock(data[:8])
		assert.NotEqual(t, checksum8, checksum, "tail byte should affect checksum")
	})

	t.Run("deterministic", func(t *testing.T) {
		data := []byte("Hello, DuckDB!")
		checksum1 := checksumBlock(data)
		checksum2 := checksumBlock(data)
		assert.Equal(t, checksum1, checksum2, "checksum should be deterministic")
	})

	t.Run("different data produces different checksum", func(t *testing.T) {
		data1 := []byte("Hello, DuckDB!")
		data2 := []byte("Hello, DukDB!!")
		checksum1 := checksumBlock(data1)
		checksum2 := checksumBlock(data2)
		assert.NotEqual(t, checksum1, checksum2, "different data should produce different checksums")
	})
}

// TestFileHeader tests file header creation, reading, and writing.
func TestFileHeader(t *testing.T) {
	t.Run("NewFileHeader", func(t *testing.T) {
		header := NewFileHeader()
		require.NotNil(t, header)
		assert.Equal(t, [4]byte{'D', 'U', 'C', 'K'}, header.Magic)
		assert.Equal(t, CurrentVersion, header.Version)
		assert.Equal(t, uint64(0), header.Flags)
	})

	t.Run("ValidateFileHeader valid", func(t *testing.T) {
		header := NewFileHeader()
		err := ValidateFileHeader(header)
		assert.NoError(t, err)
	})

	t.Run("ValidateFileHeader nil", func(t *testing.T) {
		err := ValidateFileHeader(nil)
		assert.Error(t, err)
	})

	t.Run("ValidateFileHeader wrong magic", func(t *testing.T) {
		header := &FileHeader{
			Magic:   [4]byte{'N', 'O', 'P', 'E'},
			Version: CurrentVersion,
		}
		err := ValidateFileHeader(header)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotDuckDBFile))
	})

	t.Run("ValidateFileHeader unsupported version", func(t *testing.T) {
		header := &FileHeader{
			Magic:   [4]byte{'D', 'U', 'C', 'K'},
			Version: CurrentVersion + 1,
		}
		err := ValidateFileHeader(header)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrUnsupportedVersion))
	})

	t.Run("ValidateFileHeader older version OK", func(t *testing.T) {
		header := &FileHeader{
			Magic:   [4]byte{'D', 'U', 'C', 'K'},
			Version: CurrentVersion - 1,
		}
		err := ValidateFileHeader(header)
		assert.NoError(t, err, "older versions should be accepted")
	})
}

// TestFileHeaderReadWrite tests file header round-trip serialization.
func TestFileHeaderReadWrite(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		// Create a temporary file
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "test.duckdb")

		f, err := os.Create(tmpFile)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		// Write header
		original := NewFileHeader()
		original.Flags = 0x1234567890ABCDEF
		original.BlockHeaderStorage = [8]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}

		err = WriteFileHeader(f, original)
		require.NoError(t, err)

		// Read header back
		read, err := ReadFileHeader(f)
		require.NoError(t, err)

		assert.Equal(t, original.Magic, read.Magic)
		assert.Equal(t, original.Version, read.Version)
		assert.Equal(t, original.Flags, read.Flags)
		assert.Equal(t, original.BlockHeaderStorage, read.BlockHeaderStorage)
	})

	t.Run("file too small", func(t *testing.T) {
		// Create a buffer that's too small
		data := make([]byte, 10) // Less than MagicByteOffset + 4
		reader := bytes.NewReader(data)

		_, err := ReadFileHeader(readerAt{reader})
		assert.Error(t, err)
	})
}

// readerAt wraps a bytes.Reader to implement io.ReaderAt.
type readerAt struct {
	*bytes.Reader
}

func (r readerAt) ReadAt(p []byte, off int64) (n int, err error) {
	return r.Reader.ReadAt(p, off)
}

// writerAt wraps a bytes.Buffer to implement io.WriterAt.
type writerAt struct {
	buf []byte
}

func (w *writerAt) WriteAt(p []byte, off int64) (n int, err error) {
	end := int(off) + len(p)
	if end > len(w.buf) {
		// Extend buffer
		newBuf := make([]byte, end)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	copy(w.buf[off:], p)
	return len(p), nil
}

// TestDatabaseHeader tests database header creation and validation.
func TestDatabaseHeader(t *testing.T) {
	t.Run("NewDatabaseHeader", func(t *testing.T) {
		header := NewDatabaseHeader()
		require.NotNil(t, header)

		assert.Equal(t, uint64(0), header.Iteration)
		assert.False(t, IsValidBlockID(header.MetaBlock))
		assert.False(t, IsValidBlockID(header.FreeList))
		assert.Equal(t, uint64(0), header.BlockCount)
		assert.Equal(t, DefaultBlockSize, header.BlockAllocSize)
		assert.Equal(t, DefaultVectorSize, header.VectorSize)
		assert.Equal(t, CurrentVersion, header.SerializationCompatibility)
	})

	t.Run("ValidateDatabaseHeader valid", func(t *testing.T) {
		header := NewDatabaseHeader()
		err := ValidateDatabaseHeader(header)
		assert.NoError(t, err)
	})

	t.Run("ValidateDatabaseHeader nil", func(t *testing.T) {
		err := ValidateDatabaseHeader(nil)
		assert.Error(t, err)
	})

	t.Run("ValidateDatabaseHeader zero block size", func(t *testing.T) {
		header := NewDatabaseHeader()
		header.BlockAllocSize = 0
		err := ValidateDatabaseHeader(header)
		assert.Error(t, err)
	})

	t.Run("ValidateDatabaseHeader zero vector size", func(t *testing.T) {
		header := NewDatabaseHeader()
		header.VectorSize = 0
		err := ValidateDatabaseHeader(header)
		assert.Error(t, err)
	})
}

// TestDatabaseHeaderReadWrite tests database header round-trip serialization.
func TestDatabaseHeaderReadWrite(t *testing.T) {
	t.Run("round trip header 1", func(t *testing.T) {
		// Create a buffer large enough for the database header
		buf := &writerAt{buf: make([]byte, DatabaseHeader1Offset+DatabaseHeaderSize)}

		// Write header
		original := &DatabaseHeader{
			Iteration:                  42,
			MetaBlock:                  100,
			FreeList:                   300,
			BlockCount:                 500,
			BlockAllocSize:             DefaultBlockSize,
			VectorSize:                 DefaultVectorSize,
			SerializationCompatibility: CurrentVersion,
		}

		err := WriteDatabaseHeader(buf, original, DatabaseHeader1Offset)
		require.NoError(t, err)

		// Read header back
		reader := bytes.NewReader(buf.buf)
		read, err := ReadDatabaseHeader(readerAt{reader}, DatabaseHeader1Offset)
		require.NoError(t, err)

		assert.Equal(t, original.Iteration, read.Iteration)
		assert.Equal(t, original.MetaBlock, read.MetaBlock)
		assert.Equal(t, original.FreeList, read.FreeList)
		assert.Equal(t, original.BlockCount, read.BlockCount)
		assert.Equal(t, original.BlockAllocSize, read.BlockAllocSize)
		assert.Equal(t, original.VectorSize, read.VectorSize)
		assert.Equal(t, original.SerializationCompatibility, read.SerializationCompatibility)
	})

	t.Run("round trip header 2", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DatabaseHeader2Offset+DatabaseHeaderSize)}

		original := &DatabaseHeader{
			Iteration:                  99,
			MetaBlock:                  1,
			FreeList:                   3,
			BlockCount:                 10,
			BlockAllocSize:             DefaultBlockSize,
			VectorSize:                 DefaultVectorSize,
			SerializationCompatibility: CurrentVersion,
		}

		err := WriteDatabaseHeader(buf, original, DatabaseHeader2Offset)
		require.NoError(t, err)

		reader := bytes.NewReader(buf.buf)
		read, err := ReadDatabaseHeader(readerAt{reader}, DatabaseHeader2Offset)
		require.NoError(t, err)

		assert.Equal(t, original.Iteration, read.Iteration)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DatabaseHeader1Offset+DatabaseHeaderSize)}

		original := NewDatabaseHeader()
		original.Iteration = 1

		err := WriteDatabaseHeader(buf, original, DatabaseHeader1Offset)
		require.NoError(t, err)

		// Corrupt the data (not the checksum)
		buf.buf[DatabaseHeader1Offset+BlockChecksumSize] = 0xFF

		reader := bytes.NewReader(buf.buf)
		_, err = ReadDatabaseHeader(readerAt{reader}, DatabaseHeader1Offset)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrChecksumMismatch))
	})
}

// TestSelectActiveHeader tests the dual-header selection logic.
func TestSelectActiveHeader(t *testing.T) {
	h1 := &DatabaseHeader{Iteration: 10}
	h2 := &DatabaseHeader{Iteration: 20}
	someErr := errors.New("some error")

	t.Run("both valid, h2 higher iteration", func(t *testing.T) {
		result, err := SelectActiveHeader(h1, h2, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, h2, result)
	})

	t.Run("both valid, h1 higher iteration", func(t *testing.T) {
		h1Higher := &DatabaseHeader{Iteration: 30}
		result, err := SelectActiveHeader(h1Higher, h2, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, h1Higher, result)
	})

	t.Run("both valid, equal iteration returns h1", func(t *testing.T) {
		h1Equal := &DatabaseHeader{Iteration: 20}
		result, err := SelectActiveHeader(h1Equal, h2, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, h1Equal, result)
	})

	t.Run("h1 error, h2 valid", func(t *testing.T) {
		result, err := SelectActiveHeader(nil, h2, someErr, nil)
		require.NoError(t, err)
		assert.Equal(t, h2, result)
	})

	t.Run("h1 valid, h2 error", func(t *testing.T) {
		result, err := SelectActiveHeader(h1, nil, nil, someErr)
		require.NoError(t, err)
		assert.Equal(t, h1, result)
	})

	t.Run("both errors", func(t *testing.T) {
		err1 := errors.New("error 1")
		err2 := errors.New("error 2")
		_, err := SelectActiveHeader(nil, nil, err1, err2)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrBothHeadersCorrupted))
	})
}

// TestGetActiveHeader tests the convenience function for reading the active header.
func TestGetActiveHeader(t *testing.T) {
	t.Run("header 1 active (higher iteration)", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DataBlocksOffset)}

		h1 := NewDatabaseHeader()
		h1.Iteration = 10
		h2 := NewDatabaseHeader()
		h2.Iteration = 5

		require.NoError(t, WriteDatabaseHeader(buf, h1, DatabaseHeader1Offset))
		require.NoError(t, WriteDatabaseHeader(buf, h2, DatabaseHeader2Offset))

		reader := bytes.NewReader(buf.buf)
		header, slot, err := GetActiveHeader(readerAt{reader})
		require.NoError(t, err)
		assert.Equal(t, uint64(10), header.Iteration)
		assert.Equal(t, 1, slot)
	})

	t.Run("header 2 active (higher iteration)", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DataBlocksOffset)}

		h1 := NewDatabaseHeader()
		h1.Iteration = 5
		h2 := NewDatabaseHeader()
		h2.Iteration = 10

		require.NoError(t, WriteDatabaseHeader(buf, h1, DatabaseHeader1Offset))
		require.NoError(t, WriteDatabaseHeader(buf, h2, DatabaseHeader2Offset))

		reader := bytes.NewReader(buf.buf)
		header, slot, err := GetActiveHeader(readerAt{reader})
		require.NoError(t, err)
		assert.Equal(t, uint64(10), header.Iteration)
		assert.Equal(t, 2, slot)
	})

	t.Run("header 1 corrupted, header 2 valid", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DataBlocksOffset)}

		// Don't write header 1 (leave it as zeros/invalid)
		h2 := NewDatabaseHeader()
		h2.Iteration = 10
		require.NoError(t, WriteDatabaseHeader(buf, h2, DatabaseHeader2Offset))

		reader := bytes.NewReader(buf.buf)
		header, slot, err := GetActiveHeader(readerAt{reader})
		require.NoError(t, err)
		assert.Equal(t, uint64(10), header.Iteration)
		assert.Equal(t, 2, slot)
	})

	t.Run("both headers corrupted", func(t *testing.T) {
		buf := &writerAt{buf: make([]byte, DataBlocksOffset)}

		// Write valid header 1
		h1 := NewDatabaseHeader()
		h1.Iteration = 1
		require.NoError(t, WriteDatabaseHeader(buf, h1, DatabaseHeader1Offset))

		// Write valid header 2
		h2 := NewDatabaseHeader()
		h2.Iteration = 2
		require.NoError(t, WriteDatabaseHeader(buf, h2, DatabaseHeader2Offset))

		// Now corrupt BOTH headers by modifying their data (not checksums)
		buf.buf[DatabaseHeader1Offset+BlockChecksumSize] = 0xFF
		buf.buf[DatabaseHeader2Offset+BlockChecksumSize] = 0xFF

		reader := bytes.NewReader(buf.buf)
		_, _, err := GetActiveHeader(readerAt{reader})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrBothHeadersCorrupted))
	})
}

// TestGetNextHeaderSlot tests the slot alternation logic.
func TestGetNextHeaderSlot(t *testing.T) {
	assert.Equal(t, 2, GetNextHeaderSlot(1))
	assert.Equal(t, 1, GetNextHeaderSlot(2))
	assert.Equal(t, 1, GetNextHeaderSlot(99)) // Invalid slot defaults to 1
}

// TestGetHeaderOffset tests the offset lookup for header slots.
func TestGetHeaderOffset(t *testing.T) {
	assert.Equal(t, int64(DatabaseHeader1Offset), GetHeaderOffset(1))
	assert.Equal(t, int64(DatabaseHeader2Offset), GetHeaderOffset(2))
	assert.Equal(t, int64(DatabaseHeader1Offset), GetHeaderOffset(99)) // Invalid slot defaults to 1
}

// TestBlockPointer tests BlockPointer functionality.
func TestBlockPointer(t *testing.T) {
	t.Run("IsValid", func(t *testing.T) {
		// Valid pointers
		assert.True(t, BlockPointer{BlockID: 0, Offset: 0}.IsValid())
		assert.True(t, BlockPointer{BlockID: 100, Offset: 200}.IsValid())
		assert.True(t, BlockPointer{BlockID: ^uint64(0), Offset: 0}.IsValid())
		assert.True(t, BlockPointer{BlockID: 0, Offset: ^uint32(0)}.IsValid())

		// Invalid pointer (both max values)
		assert.False(t, InvalidBlockPointer().IsValid())
		assert.False(t, BlockPointer{BlockID: ^uint64(0), Offset: ^uint32(0)}.IsValid())
	})

	t.Run("InvalidBlockPointer", func(t *testing.T) {
		invalid := InvalidBlockPointer()
		assert.Equal(t, ^uint64(0), invalid.BlockID)
		assert.Equal(t, ^uint32(0), invalid.Offset)
	})
}

// TestMetaBlockPointer tests MetaBlockPointer functionality.
func TestMetaBlockPointer(t *testing.T) {
	t.Run("IsValid", func(t *testing.T) {
		assert.True(t, MetaBlockPointer{BlockID: 0, Offset: 0}.IsValid())
		assert.True(t, MetaBlockPointer{BlockID: 100, Offset: 200}.IsValid())
		assert.False(t, MetaBlockPointer{BlockID: ^uint64(0), Offset: 0}.IsValid())
	})
}

// TestSerializeDatabaseHeader tests serialization produces correct byte count.
func TestSerializeDatabaseHeader(t *testing.T) {
	header := NewDatabaseHeader()
	data := serializeDatabaseHeader(header)

	// Should be exactly 56 bytes (7 * uint64)
	assert.Len(t, data, 56, "serialized database header should be 56 bytes")
}

// TestDeserializeDatabaseHeader tests deserialization error cases.
func TestDeserializeDatabaseHeader(t *testing.T) {
	t.Run("too short", func(t *testing.T) {
		data := make([]byte, 32) // Less than 56
		_, err := deserializeDatabaseHeader(data)
		assert.Error(t, err)
	})

	t.Run("exact size", func(t *testing.T) {
		header := &DatabaseHeader{
			Iteration:                  123,
			MetaBlock:                  456,
			FreeList:                   111,
			BlockCount:                 333,
			BlockAllocSize:             DefaultBlockSize,
			VectorSize:                 DefaultVectorSize,
			SerializationCompatibility: CurrentVersion,
		}
		data := serializeDatabaseHeader(header)

		result, err := deserializeDatabaseHeader(data)
		require.NoError(t, err)
		assert.Equal(t, header.Iteration, result.Iteration)
		assert.Equal(t, header.MetaBlock, result.MetaBlock)
		assert.Equal(t, header.FreeList, result.FreeList)
	})
}

// TestChecksumConsistency verifies the checksum is consistent between write and read.
func TestChecksumConsistency(t *testing.T) {
	// Create a full file with both headers
	buf := &writerAt{buf: make([]byte, DataBlocksOffset)}

	// Write file header
	fileHeader := NewFileHeader()
	require.NoError(t, WriteFileHeader(buf, fileHeader))

	// Write both database headers
	dbHeader1 := NewDatabaseHeader()
	dbHeader1.Iteration = 1
	dbHeader1.MetaBlock = 10
	require.NoError(t, WriteDatabaseHeader(buf, dbHeader1, DatabaseHeader1Offset))

	dbHeader2 := NewDatabaseHeader()
	dbHeader2.Iteration = 2
	dbHeader2.MetaBlock = 20
	require.NoError(t, WriteDatabaseHeader(buf, dbHeader2, DatabaseHeader2Offset))

	// Read and verify
	reader := bytes.NewReader(buf.buf)

	readFileHeader, err := ReadFileHeader(readerAt{reader})
	require.NoError(t, err)
	require.NoError(t, ValidateFileHeader(readFileHeader))

	readDbHeader, slot, err := GetActiveHeader(readerAt{reader})
	require.NoError(t, err)
	assert.Equal(t, 2, slot, "header 2 should be active (higher iteration)")
	assert.Equal(t, uint64(2), readDbHeader.Iteration)
	assert.Equal(t, uint64(20), readDbHeader.MetaBlock)
}

// TestChecksumRemainder tests the MurmurHash2 variant for remaining bytes.
func TestChecksumRemainder(t *testing.T) {
	t.Run("single byte", func(t *testing.T) {
		result := checksumRemainder([]byte{0x42})
		assert.NotEqual(t, uint64(0), result)
	})

	t.Run("different bytes produce different hashes", func(t *testing.T) {
		h1 := checksumRemainder([]byte{0x01, 0x02, 0x03})
		h2 := checksumRemainder([]byte{0x01, 0x02, 0x04})
		assert.NotEqual(t, h1, h2)
	})

	t.Run("all tail lengths produce valid hashes", func(t *testing.T) {
		// Test all tail lengths from 1 to 7 bytes
		for length := 1; length <= 7; length++ {
			data := make([]byte, length)
			for i := 0; i < length; i++ {
				data[i] = byte(i + 1)
			}
			result := checksumRemainder(data)
			assert.NotEqual(t, uint64(0), result, "length %d should produce non-zero hash", length)
		}
	})
}

// BenchmarkChecksumBlock benchmarks the checksum algorithm.
func BenchmarkChecksumBlock(b *testing.B) {
	// Test with various sizes
	sizes := []int{64, 256, 1024, 4096, 262144} // 64B, 256B, 1KB, 4KB, 256KB

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i)
		}

		b.Run(byteSizeString(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				checksumBlock(data)
			}
		})
	}
}

func byteSizeString(size int) string {
	switch {
	case size >= 1024*1024:
		return string(rune('0'+size/(1024*1024))) + "MB"
	case size >= 1024:
		return string(rune('0'+size/1024)) + "KB"
	default:
		return string(rune('0'+size)) + "B"
	}
}

// BenchmarkDatabaseHeaderRoundTrip benchmarks header serialization.
func BenchmarkDatabaseHeaderRoundTrip(b *testing.B) {
	header := &DatabaseHeader{
		Iteration:                  42,
		MetaBlock:                  100,
		FreeList:                   300,
		BlockCount:                 500,
		BlockAllocSize:             DefaultBlockSize,
		VectorSize:                 DefaultVectorSize,
		SerializationCompatibility: CurrentVersion,
	}

	buf := &writerAt{buf: make([]byte, DatabaseHeaderSize+DatabaseHeader1Offset)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = WriteDatabaseHeader(buf, header, DatabaseHeader1Offset)
		reader := bytes.NewReader(buf.buf)
		_, _ = ReadDatabaseHeader(readerAt{reader}, DatabaseHeader1Offset)
	}
}
