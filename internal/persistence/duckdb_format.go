package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// DuckDB file format constants
const (
	DuckDBMagicNumber = "DUCK"
	DuckDBVersion     = uint64(64) // Current version we write
	DuckDBMinVersion  = uint64(64) // Minimum version we can read
	DuckDBMaxVersion  = uint64(67) // Maximum version we can read (matches DuckDB v1.5.0)
	DuckDBHeaderSize  = 4096
)

// Version compatibility matrix for DuckDB file format
// Based on DuckDB storage_info.cpp VERSION_NUMBER_LOWER and VERSION_NUMBER_UPPER
//
// Storage Version History:
//   v64: Introduced in DuckDB v0.9.0 (September 2023)
//        - First stable format with modern columnar storage
//        - Used through v1.1.3 (January 2025)
//        - Key features:
//          * Dual 4KB rotating headers (blocks 0, 1, 2)
//          * Binary property-based catalog serialization
//          * Row group format with column segments
//          * FSST, RLE, BitPacking, Chimp compression
//          * Serialization compatibility field NOT stored (implicitly 1)
//
//   v65: Introduced in DuckDB v1.2.0 (February 2025)
//        - Enhanced catalog serialization
//        - Key changes from v64:
//          * Serialization compatibility field now EXPLICITLY stored in header
//          * Enhanced type system metadata serialization
//
//   v66: Introduced in DuckDB v1.3.0 (May 2025)
//        - Additional type system improvements
//        - Key changes from v65:
//          * Extended support for nested types (STRUCT, LIST, MAP, UNION)
//          * Improved metadata for complex type hierarchies
//
//   v67: Introduced in DuckDB v1.4.0 (August 2025)
//        - Current version as of v1.5.0 (November 2025)
//        - Key changes from v66:
//          * Added support for new types (BIT, TIME_TZ, TIMESTAMP_TZ)
//          * Enhanced compression metadata
//
// Compatibility:
//   - We write: v64 (for maximum compatibility with DuckDB 0.9.0+)
//   - We read: v64-v67 (all stable releases from 0.9.0 onwards)
//   - Forward compatibility: Limited to v67 (may require updates for newer formats)
//
// Key Format Differences Between Versions:
//   v64 vs v65+: SerializationCompatibility field storage (implicit vs explicit)
//   v65 vs v66: Type system metadata changes (nested type handling)
//   v66 vs v67: New type additions (BIT, TIME_TZ, TIMESTAMP_TZ)
//
// Note: Versions below v64 used different storage formats and are not supported.
// For files from DuckDB < v0.9.0, use the official duckdb tool to upgrade them first.
const (
	// VersionSerializationCompatV64 indicates the serialization compatibility value for version 64
	// In v64, this field was not stored in the header (implicitly 1)
	VersionSerializationCompatV64 = uint64(1)
)

var (
	ErrInvalidDuckDBMagic       = errors.New("invalid DuckDB magic number")
	ErrUnsupportedDuckDBVersion = errors.New("unsupported DuckDB version")
	ErrDuckDBHeaderChecksum     = errors.New("DuckDB header checksum mismatch")
	ErrVersionTooOld            = errors.New("DuckDB version too old")
	ErrVersionTooNew            = errors.New("DuckDB version too new")
)

// MainHeader represents the first 4096 bytes of the file (Block 0)
type MainHeader struct {
	Magic              [4]byte
	Version            uint64
	Flags              [4]uint64
	LibraryGitDesc     string
	LibraryGitHash     string
	EncryptionMetadata [8]byte
	DbIdentifier       [16]byte
	EncryptedCanary    [8]byte
}

// DatabaseHeader represents the rotating headers (Block 1 and 2)
type DatabaseHeader struct {
	Iteration                  uint64
	MetaBlock                  uint64
	FreeList                   uint64
	BlockCount                 uint64
	BlockAllocSize             uint64
	VectorSize                 uint64
	SerializationCompatibility uint64
}

// NegotiateVersion checks if the given version is supported by this implementation.
// Returns nil if the version is supported, or an error with details if unsupported.
func NegotiateVersion(version uint64) error {
	if version < DuckDBMinVersion {
		return fmt.Errorf("%w: version %d is too old (minimum supported: %d)",
			ErrVersionTooOld, version, DuckDBMinVersion)
	}
	if version > DuckDBMaxVersion {
		return fmt.Errorf("%w: version %d is too new (maximum supported: %d)",
			ErrVersionTooNew, version, DuckDBMaxVersion)
	}
	return nil
}

// VersionCapabilities provides version-specific feature detection
type VersionCapabilities struct {
	Version uint64
}

// NewVersionCapabilities creates a version capabilities checker for the given version
func NewVersionCapabilities(version uint64) *VersionCapabilities {
	return &VersionCapabilities{Version: version}
}

// HasExplicitSerializationCompat returns true if this version stores the
// serialization_compatibility field explicitly in the DatabaseHeader.
// Version 64 does not store this field (it's implicitly 1).
// Versions 65+ store it explicitly.
func (v *VersionCapabilities) HasExplicitSerializationCompat() bool {
	return v.Version >= 65
}

// GetDefaultSerializationCompat returns the default serialization compatibility
// value for this version when not explicitly stored in the header.
func (v *VersionCapabilities) GetDefaultSerializationCompat() uint64 {
	// v64 uses implicit value of 1
	if v.Version == 64 {
		return VersionSerializationCompatV64
	}
	// v65+ should have it explicitly stored, but default to a safe value
	return 1
}

// SupportsVersion checks if we support reading/writing this specific version
func SupportsVersion(version uint64) bool {
	return version >= DuckDBMinVersion && version <= DuckDBMaxVersion
}

// GetVersionName returns a human-readable name for the version
func GetVersionName(version uint64) string {
	switch version {
	case 64:
		return "v64 (DuckDB 0.9.0 - 1.1.3)"
	case 65:
		return "v65 (DuckDB 1.2.0 - 1.2.2)"
	case 66:
		return "v66 (DuckDB 1.3.0 - 1.3.2)"
	case 67:
		return "v67 (DuckDB 1.4.0 - 1.5.0)"
	default:
		return fmt.Sprintf("v%d (unknown)", version)
	}
}

// ReadMainHeader reads the MainHeader from the reader (first 4KB)
func ReadMainHeader(r io.Reader) (*MainHeader, error) {
	buf := make([]byte, DuckDBHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read main header: %w", err)
	}

	reader := bytes.NewReader(buf[8:])
	h := &MainHeader{}

	if _, err := io.ReadFull(reader, h.Magic[:]); err != nil {
		return nil, err
	}
	if string(h.Magic[:]) != DuckDBMagicNumber {
		return nil, ErrInvalidDuckDBMagic
	}

	if err := binary.Read(reader, binary.LittleEndian, &h.Version); err != nil {
		return nil, err
	}
	if err := NegotiateVersion(h.Version); err != nil {
		return nil, err
	}

	for i := 0; i < 4; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &h.Flags[i]); err != nil {
			return nil, err
		}
	}

	gitDesc := make([]byte, 32)
	if _, err := io.ReadFull(reader, gitDesc); err != nil {
		return nil, err
	}
	h.LibraryGitDesc = string(bytes.TrimRight(gitDesc, "\x00"))

	gitHash := make([]byte, 32)
	if _, err := io.ReadFull(reader, gitHash); err != nil {
		return nil, err
	}
	h.LibraryGitHash = string(bytes.TrimRight(gitHash, "\x00"))

	if _, err := io.ReadFull(reader, h.EncryptionMetadata[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(reader, h.DbIdentifier[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(reader, h.EncryptedCanary[:]); err != nil {
		return nil, err
	}

	return h, nil
}

// ReadDatabaseHeader reads a DatabaseHeader from the reader
func ReadDatabaseHeader(r io.Reader, version uint64) (*DatabaseHeader, error) {
	buf := make([]byte, DuckDBHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read database header: %w", err)
	}

	reader := bytes.NewReader(buf[8:])

	h := &DatabaseHeader{}
	if err := binary.Read(reader, binary.LittleEndian, &h.Iteration); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.MetaBlock); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.FreeList); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.BlockCount); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.BlockAllocSize); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.VectorSize); err != nil {
		return nil, err
	}

	// Handle version-specific serialization compatibility field
	caps := NewVersionCapabilities(version)
	if caps.HasExplicitSerializationCompat() {
		// v65+ stores this field explicitly
		if err := binary.Read(reader, binary.LittleEndian, &h.SerializationCompatibility); err != nil {
			return nil, err
		}
	} else {
		// v64 does not store this field, use default
		h.SerializationCompatibility = caps.GetDefaultSerializationCompat()
	}

	return h, nil
}

// WriteDatabaseHeader writes a DatabaseHeader to the writer
func WriteDatabaseHeader(w io.Writer, h *DatabaseHeader) error {
	buf := new(bytes.Buffer)

	// Placeholder checksum (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.LittleEndian, h.Iteration); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.MetaBlock); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.FreeList); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.BlockCount); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.BlockAllocSize); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.VectorSize); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.SerializationCompatibility); err != nil {
		return err
	}

	// Pad to 4096
	padding := DuckDBHeaderSize - buf.Len()
	if padding > 0 {
		if _, err := buf.Write(make([]byte, padding)); err != nil {
			return err
		}
	}

	// TODO: Calculate and write actual checksum at offset 0

	_, err := w.Write(buf.Bytes())
	return err
}

// writeMainHeader writes a MainHeader to the writer
func writeMainHeader(w io.Writer, h *MainHeader) error {
	buf := new(bytes.Buffer)

	// Reserved for checksum (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.LittleEndian, h.Magic[:]); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Version); err != nil {
		return err
	}
	for i := 0; i < 4; i++ {
		if err := binary.Write(buf, binary.LittleEndian, h.Flags[i]); err != nil {
			return err
		}
	}

	// Write fixed-size strings with null padding
	gitDescBuf := make([]byte, 32)
	copy(gitDescBuf, []byte(h.LibraryGitDesc))
	if _, err := buf.Write(gitDescBuf); err != nil {
		return err
	}

	gitHashBuf := make([]byte, 32)
	copy(gitHashBuf, []byte(h.LibraryGitHash))
	if _, err := buf.Write(gitHashBuf); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.LittleEndian, h.EncryptionMetadata[:]); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.DbIdentifier[:]); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.EncryptedCanary[:]); err != nil {
		return err
	}

	// Pad to 4096
	padding := DuckDBHeaderSize - buf.Len()
	if padding > 0 {
		if _, err := buf.Write(make([]byte, padding)); err != nil {
			return err
		}
	}

	// TODO: Calculate and write actual checksum at offset 0

	_, err := w.Write(buf.Bytes())
	return err
}
