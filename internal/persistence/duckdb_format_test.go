package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMainHeaderReadWrite(t *testing.T) {
	buf := new(bytes.Buffer)

	// Checksum (8 bytes)
	_ = binary.Write(buf, binary.LittleEndian, uint64(0))

	// Magic
	buf.Write([]byte(DuckDBMagicNumber))

	// Version
	_ = binary.Write(buf, binary.LittleEndian, uint64(64))

	// Flags
	for i := 0; i < 4; i++ {
		_ = binary.Write(buf, binary.LittleEndian, uint64(0))
	}

	padding := DuckDBHeaderSize - buf.Len()
	buf.Write(make([]byte, padding))

	reader := bytes.NewReader(buf.Bytes())
	header, err := ReadMainHeader(reader)
	require.NoError(t, err)
	assert.Equal(t, string(header.Magic[:]), DuckDBMagicNumber)
	assert.Equal(t, uint64(64), header.Version)
}

func TestDatabaseHeaderReadWrite(t *testing.T) {
	h := &DatabaseHeader{
		Iteration:      1,
		MetaBlock:      10,
		FreeList:       20,
		BlockCount:     30,
		BlockAllocSize: 262144,
		VectorSize:     2048,
	}

	buf := new(bytes.Buffer)
	err := WriteDatabaseHeader(buf, h)
	require.NoError(t, err)

	assert.Equal(t, DuckDBHeaderSize, buf.Len())

	reader := bytes.NewReader(buf.Bytes())
	readH, err := ReadDatabaseHeader(reader, 64)
	require.NoError(t, err)

	assert.Equal(t, h.Iteration, readH.Iteration)
	assert.Equal(t, h.MetaBlock, readH.MetaBlock)
	assert.Equal(t, h.BlockCount, readH.BlockCount)
}

func TestDualHeaderLogic(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_header_test")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	mainBuf := new(bytes.Buffer)
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(0))
	_, _ = mainBuf.Write([]byte(DuckDBMagicNumber))
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(64))
	_, _ = mainBuf.Write(make([]byte, DuckDBHeaderSize-mainBuf.Len()))

	_, err = tmpFile.Write(mainBuf.Bytes())
	require.NoError(t, err)

	h1 := &DatabaseHeader{Iteration: 1, BlockCount: 10}
	err = WriteDatabaseHeader(tmpFile, h1)
	require.NoError(t, err)

	h2 := &DatabaseHeader{Iteration: 2, BlockCount: 20}
	err = WriteDatabaseHeader(tmpFile, h2)
	require.NoError(t, err)

	err = tmpFile.Close()
	require.NoError(t, err)

	activeH, mainH, err := ReadActiveDatabaseHeader(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, uint64(64), mainH.Version)
	assert.Equal(t, uint64(2), activeH.Iteration)
	assert.Equal(t, uint64(20), activeH.BlockCount)

	newH := &DatabaseHeader{Iteration: 3, BlockCount: 30}
	err = WriteActiveDatabaseHeader(tmpFile.Name(), newH)
	require.NoError(t, err)

	activeH, _, err = ReadActiveDatabaseHeader(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), activeH.Iteration)

	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	_, err = f.Seek(int64(DuckDBHeaderSize), io.SeekStart)
	require.NoError(t, err)
	readH1, err := ReadDatabaseHeader(f, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), readH1.Iteration)

	_, err = f.Seek(int64(DuckDBHeaderSize)*2, io.SeekStart)
	require.NoError(t, err)
	readH2, err := ReadDatabaseHeader(f, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), readH2.Iteration)
}

func TestCorruptedHeader(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.Write(make([]byte, 8))
	buf.Write([]byte("BADM"))
	buf.Write(make([]byte, DuckDBHeaderSize-12))

	reader := bytes.NewReader(buf.Bytes())
	_, err := ReadMainHeader(reader)
	assert.ErrorIs(t, err, ErrInvalidDuckDBMagic)
}

func TestVersionNegotiation(t *testing.T) {
	tests := []struct {
		name        string
		version     uint64
		expectError error
	}{
		{
			name:        "valid version 64",
			version:     64,
			expectError: nil,
		},
		{
			name:        "valid version 65",
			version:     65,
			expectError: nil,
		},
		{
			name:        "valid version 66",
			version:     66,
			expectError: nil,
		},
		{
			name:        "valid version 67",
			version:     67,
			expectError: nil,
		},
		{
			name:        "version too old (63)",
			version:     63,
			expectError: ErrVersionTooOld,
		},
		{
			name:        "version too old (0)",
			version:     0,
			expectError: ErrVersionTooOld,
		},
		{
			name:        "version too new (68)",
			version:     68,
			expectError: ErrVersionTooNew,
		},
		{
			name:        "version too new (100)",
			version:     100,
			expectError: ErrVersionTooNew,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NegotiateVersion(tt.version)
			if tt.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.expectError)
				// Check that error message contains version information
				assert.Contains(t, err.Error(), "version")
			}
		})
	}
}

func TestVersionNegotiationInReadMainHeader(t *testing.T) {
	tests := []struct {
		name        string
		version     uint64
		expectError error
	}{
		{
			name:        "valid version 64",
			version:     64,
			expectError: nil,
		},
		{
			name:        "valid version 65",
			version:     65,
			expectError: nil,
		},
		{
			name:        "valid version 66",
			version:     66,
			expectError: nil,
		},
		{
			name:        "valid version 67",
			version:     67,
			expectError: nil,
		},
		{
			name:        "version too old (50)",
			version:     50,
			expectError: ErrVersionTooOld,
		},
		{
			name:        "version too new (70)",
			version:     70,
			expectError: ErrVersionTooNew,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Checksum (8 bytes)
			_ = binary.Write(buf, binary.LittleEndian, uint64(0))

			// Magic
			buf.Write([]byte(DuckDBMagicNumber))

			// Version
			_ = binary.Write(buf, binary.LittleEndian, tt.version)

			// Flags
			for i := 0; i < 4; i++ {
				_ = binary.Write(buf, binary.LittleEndian, uint64(0))
			}

			// Pad to header size
			padding := DuckDBHeaderSize - buf.Len()
			buf.Write(make([]byte, padding))

			reader := bytes.NewReader(buf.Bytes())
			header, err := ReadMainHeader(reader)

			if tt.expectError == nil {
				require.NoError(t, err)
				assert.Equal(t, tt.version, header.Version)
			} else {
				assert.ErrorIs(t, err, tt.expectError)
				assert.Nil(t, header)
			}
		})
	}
}

func TestVersionCapabilities(t *testing.T) {
	tests := []struct {
		name                              string
		version                           uint64
		hasExplicitSerializationCompat    bool
		expectedDefaultSerializationValue uint64
		isSupported                       bool
	}{
		{
			name:                              "version 64",
			version:                           64,
			hasExplicitSerializationCompat:    false,
			expectedDefaultSerializationValue: 1,
			isSupported:                       true,
		},
		{
			name:                              "version 65",
			version:                           65,
			hasExplicitSerializationCompat:    true,
			expectedDefaultSerializationValue: 1,
			isSupported:                       true,
		},
		{
			name:                              "version 66",
			version:                           66,
			hasExplicitSerializationCompat:    true,
			expectedDefaultSerializationValue: 1,
			isSupported:                       true,
		},
		{
			name:                              "version 67",
			version:                           67,
			hasExplicitSerializationCompat:    true,
			expectedDefaultSerializationValue: 1,
			isSupported:                       true,
		},
		{
			name:                              "version 63 (unsupported)",
			version:                           63,
			hasExplicitSerializationCompat:    false,
			expectedDefaultSerializationValue: 1,
			isSupported:                       false,
		},
		{
			name:                              "version 68 (unsupported)",
			version:                           68,
			hasExplicitSerializationCompat:    true,
			expectedDefaultSerializationValue: 1,
			isSupported:                       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caps := NewVersionCapabilities(tt.version)

			assert.Equal(t, tt.hasExplicitSerializationCompat, caps.HasExplicitSerializationCompat(),
				"HasExplicitSerializationCompat() mismatch")
			assert.Equal(t, tt.expectedDefaultSerializationValue, caps.GetDefaultSerializationCompat(),
				"GetDefaultSerializationCompat() mismatch")
			assert.Equal(t, tt.isSupported, SupportsVersion(tt.version),
				"SupportsVersion() mismatch")
		})
	}
}

func TestGetVersionName(t *testing.T) {
	tests := []struct {
		version      uint64
		expectedName string
	}{
		{64, "v64 (DuckDB 0.9.0 - 1.1.3)"},
		{65, "v65 (DuckDB 1.2.0 - 1.2.2)"},
		{66, "v66 (DuckDB 1.3.0 - 1.3.2)"},
		{67, "v67 (DuckDB 1.4.0 - 1.5.0)"},
		{99, "v99 (unknown)"},
	}

	for _, tt := range tests {
		t.Run(tt.expectedName, func(t *testing.T) {
			name := GetVersionName(tt.version)
			assert.Equal(t, tt.expectedName, name)
		})
	}
}

func TestDatabaseHeaderReadVersionCompatibility(t *testing.T) {
	tests := []struct {
		name                           string
		version                        uint64
		writeSerializationCompat       bool
		serializationCompatValue       uint64
		expectedSerializationCompatOut uint64
	}{
		{
			name:                           "v64 without explicit field",
			version:                        64,
			writeSerializationCompat:       false,
			serializationCompatValue:       0,
			expectedSerializationCompatOut: 1, // implicit default for v64
		},
		{
			name:                           "v65 with explicit field",
			version:                        65,
			writeSerializationCompat:       true,
			serializationCompatValue:       2,
			expectedSerializationCompatOut: 2,
		},
		{
			name:                           "v66 with explicit field",
			version:                        66,
			writeSerializationCompat:       true,
			serializationCompatValue:       3,
			expectedSerializationCompatOut: 3,
		},
		{
			name:                           "v67 with explicit field",
			version:                        67,
			writeSerializationCompat:       true,
			serializationCompatValue:       4,
			expectedSerializationCompatOut: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Write checksum placeholder
			_ = binary.Write(buf, binary.LittleEndian, uint64(0))

			// Write DatabaseHeader fields
			_ = binary.Write(buf, binary.LittleEndian, uint64(1))      // Iteration
			_ = binary.Write(buf, binary.LittleEndian, uint64(10))     // MetaBlock
			_ = binary.Write(buf, binary.LittleEndian, uint64(20))     // FreeList
			_ = binary.Write(buf, binary.LittleEndian, uint64(30))     // BlockCount
			_ = binary.Write(buf, binary.LittleEndian, uint64(262144)) // BlockAllocSize
			_ = binary.Write(buf, binary.LittleEndian, uint64(2048))   // VectorSize

			// Conditionally write serialization compatibility
			if tt.writeSerializationCompat {
				_ = binary.Write(buf, binary.LittleEndian, tt.serializationCompatValue)
			}

			// Pad to header size
			padding := DuckDBHeaderSize - buf.Len()
			if padding > 0 {
				buf.Write(make([]byte, padding))
			}

			// Read the header back
			reader := bytes.NewReader(buf.Bytes())
			header, err := ReadDatabaseHeader(reader, tt.version)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedSerializationCompatOut, header.SerializationCompatibility,
				"SerializationCompatibility mismatch for version %d", tt.version)
			assert.Equal(t, uint64(1), header.Iteration)
			assert.Equal(t, uint64(10), header.MetaBlock)
			assert.Equal(t, uint64(30), header.BlockCount)
		})
	}
}

func TestVersionRangeSupport(t *testing.T) {
	// Test that we support the exact range we claim to support
	for version := uint64(64); version <= 67; version++ {
		t.Run(fmt.Sprintf("v%d_should_be_supported", version), func(t *testing.T) {
			err := NegotiateVersion(version)
			assert.NoError(t, err, "Version %d should be supported", version)
			assert.True(t, SupportsVersion(version), "SupportsVersion should return true for v%d", version)
		})
	}

	// Test versions outside our range
	unsupportedVersions := []uint64{0, 1, 50, 63, 68, 69, 70, 100}
	for _, version := range unsupportedVersions {
		t.Run(fmt.Sprintf("v%d_should_be_unsupported", version), func(t *testing.T) {
			err := NegotiateVersion(version)
			assert.Error(t, err, "Version %d should be unsupported", version)
			assert.False(t, SupportsVersion(version), "SupportsVersion should return false for v%d", version)
		})
	}
}

// Benchmarks for header read/write operations

func BenchmarkReadMainHeader(b *testing.B) {
	buf := new(bytes.Buffer)

	// Checksum (8 bytes)
	binary.Write(buf, binary.LittleEndian, uint64(0))

	// Magic
	buf.Write([]byte(DuckDBMagicNumber))

	// Version
	binary.Write(buf, binary.LittleEndian, uint64(64))

	// Flags
	for i := 0; i < 4; i++ {
		binary.Write(buf, binary.LittleEndian, uint64(0))
	}

	padding := DuckDBHeaderSize - buf.Len()
	buf.Write(make([]byte, padding))

	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, err := ReadMainHeader(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteDatabaseHeader(b *testing.B) {
	h := &DatabaseHeader{
		Iteration:      1,
		MetaBlock:      10,
		FreeList:       20,
		BlockCount:     30,
		BlockAllocSize: 262144,
		VectorSize:     2048,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		err := WriteDatabaseHeader(buf, h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDatabaseHeader(b *testing.B) {
	h := &DatabaseHeader{
		Iteration:      1,
		MetaBlock:      10,
		FreeList:       20,
		BlockCount:     30,
		BlockAllocSize: 262144,
		VectorSize:     2048,
	}

	buf := new(bytes.Buffer)
	err := WriteDatabaseHeader(buf, h)
	if err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, err := ReadDatabaseHeader(reader, 64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadWriteDatabaseHeader_RoundTrip(b *testing.B) {
	h := &DatabaseHeader{
		Iteration:      1,
		MetaBlock:      10,
		FreeList:       20,
		BlockCount:     30,
		BlockAllocSize: 262144,
		VectorSize:     2048,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		err := WriteDatabaseHeader(buf, h)
		if err != nil {
			b.Fatal(err)
		}

		reader := bytes.NewReader(buf.Bytes())
		_, err = ReadDatabaseHeader(reader, 64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadActiveDatabaseHeader(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_header_*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	// Write main header
	mainBuf := new(bytes.Buffer)
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(0))
	_, _ = mainBuf.Write([]byte(DuckDBMagicNumber))
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(64))
	_, _ = mainBuf.Write(make([]byte, DuckDBHeaderSize-mainBuf.Len()))

	_, _ = tmpFile.Write(mainBuf.Bytes())

	// Write two database headers
	h1 := &DatabaseHeader{Iteration: 1, BlockCount: 10}
	_ = WriteDatabaseHeader(tmpFile, h1)

	h2 := &DatabaseHeader{Iteration: 2, BlockCount: 20}
	_ = WriteDatabaseHeader(tmpFile, h2)

	_ = tmpFile.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := ReadActiveDatabaseHeader(tmpFile.Name())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteActiveDatabaseHeader(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_header_write_*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	// Write main header
	mainBuf := new(bytes.Buffer)
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(0))
	_, _ = mainBuf.Write([]byte(DuckDBMagicNumber))
	_ = binary.Write(mainBuf, binary.LittleEndian, uint64(64))
	_, _ = mainBuf.Write(make([]byte, DuckDBHeaderSize-mainBuf.Len()))

	_, _ = tmpFile.Write(mainBuf.Bytes())

	// Write initial headers
	h1 := &DatabaseHeader{Iteration: 1, BlockCount: 10}
	_ = WriteDatabaseHeader(tmpFile, h1)

	h2 := &DatabaseHeader{Iteration: 2, BlockCount: 20}
	_ = WriteDatabaseHeader(tmpFile, h2)

	_ = tmpFile.Close()

	newH := &DatabaseHeader{Iteration: 3, BlockCount: 30}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := WriteActiveDatabaseHeader(tmpFile.Name(), newH)
		if err != nil {
			b.Fatal(err)
		}
		newH.Iteration++
	}
}
