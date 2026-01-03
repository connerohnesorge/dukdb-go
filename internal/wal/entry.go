// Package wal provides Write-Ahead Logging for durability and crash recovery.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
)

// EntryType represents the type of WAL entry.
type EntryType uint32

// WAL entry type constants matching DuckDB semantics.
const (
	// Catalog operations (1-24)
	EntryCreateTable      EntryType = 1
	EntryDropTable        EntryType = 2
	EntryCreateSchema     EntryType = 3
	EntryDropSchema       EntryType = 4
	EntryCreateView       EntryType = 5
	EntryDropView         EntryType = 6
	EntryCreateSequence   EntryType = 7
	EntryDropSequence     EntryType = 8
	EntrySequenceValue    EntryType = 9
	EntryCreateMacro      EntryType = 10
	EntryDropMacro        EntryType = 11
	EntryCreateTableMacro EntryType = 12
	EntryDropTableMacro   EntryType = 13
	EntryCreateType       EntryType = 14
	EntryDropType         EntryType = 15
	EntryAlterInfo        EntryType = 20
	EntryCreateIndex      EntryType = 23
	EntryDropIndex        EntryType = 24

	// Data operations (25-89)
	EntryUseTable EntryType = 25
	EntryInsert   EntryType = 26
	EntryDelete   EntryType = 27
	EntryUpdate   EntryType = 28
	EntryRowGroup EntryType = 29

	// Transaction boundaries (90-97)
	EntryTxnBegin  EntryType = 90
	EntryTxnCommit EntryType = 91

	// Control entries (98-100)
	EntryVersion    EntryType = 98
	EntryCheckpoint EntryType = 99
	EntryFlush      EntryType = 100
)

// String returns the string representation of the entry type.
func (t EntryType) String() string {
	switch t {
	case EntryCreateTable:
		return "CREATE_TABLE"
	case EntryDropTable:
		return "DROP_TABLE"
	case EntryCreateSchema:
		return "CREATE_SCHEMA"
	case EntryDropSchema:
		return "DROP_SCHEMA"
	case EntryCreateView:
		return "CREATE_VIEW"
	case EntryDropView:
		return "DROP_VIEW"
	case EntryCreateSequence:
		return "CREATE_SEQUENCE"
	case EntryDropSequence:
		return "DROP_SEQUENCE"
	case EntrySequenceValue:
		return "SEQUENCE_VALUE"
	case EntryCreateMacro:
		return "CREATE_MACRO"
	case EntryDropMacro:
		return "DROP_MACRO"
	case EntryCreateTableMacro:
		return "CREATE_TABLE_MACRO"
	case EntryDropTableMacro:
		return "DROP_TABLE_MACRO"
	case EntryCreateType:
		return "CREATE_TYPE"
	case EntryDropType:
		return "DROP_TYPE"
	case EntryAlterInfo:
		return "ALTER_INFO"
	case EntryCreateIndex:
		return "CREATE_INDEX"
	case EntryDropIndex:
		return "DROP_INDEX"
	case EntryUseTable:
		return "USE_TABLE"
	case EntryInsert:
		return "INSERT"
	case EntryDelete:
		return "DELETE"
	case EntryUpdate:
		return "UPDATE"
	case EntryRowGroup:
		return "ROW_GROUP"
	case EntryTxnBegin:
		return "TXN_BEGIN"
	case EntryTxnCommit:
		return "TXN_COMMIT"
	case EntryVersion:
		return "VERSION"
	case EntryCheckpoint:
		return "CHECKPOINT"
	case EntryFlush:
		return "FLUSH"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// Entry is the interface for all WAL entries.
type Entry interface {
	// Type returns the entry type.
	Type() EntryType
	// Serialize writes the entry to the writer.
	Serialize(w io.Writer) error
}

// TxnEntry is an entry associated with a transaction.
type TxnEntry interface {
	Entry
	// TxnID returns the transaction ID for this entry.
	TxnID() uint64
}

// FileHeader represents the WAL file header matching DuckDB's WAL format.
type FileHeader struct {
	Magic          [4]byte // "WAL " (space-padded)
	Version        uint32  // WAL format version (3 for DuckDB compatibility)
	HeaderSize     uint32  // Size of header in bytes (always 24)
	SequenceNumber uint64  // Monotonically increasing sequence number
	Checksum       uint32  // CRC32 checksum of header (excluding checksum field)
}

// MagicBytes is the magic number for DuckDB-compatible WAL files.
var MagicBytes = [4]byte{'W', 'A', 'L', ' '}

// CurrentVersion is the current WAL format version (DuckDB v3).
const CurrentVersion uint32 = 3

// HeaderSize is the size of the file header in bytes.
const HeaderSize = 24 // Magic(4) + Version(4) + HeaderSize(4) + SequenceNumber(8) + Checksum(4)

// EntryHeader is the header for each WAL entry matching DuckDB format (16 bytes).
type EntryHeader struct {
	Type           EntryType // Entry type (4 bytes)
	Flags          uint32    // Entry flags (4 bytes)
	Length         uint32    // Entry payload length in bytes (4 bytes)
	SequenceNumber uint32    // Entry sequence number (4 bytes)
}

// EntryHeaderSize is the size of an entry header in bytes.
const EntryHeaderSize = 4 + 4 + 4 + 4 // Type + Flags + Length + SequenceNumber = 16 bytes

// Entry flags for DuckDB WAL compatibility.
const (
	EntryFlagNone       uint32 = 0
	EntryFlagCompressed uint32 = 1 << 0
	EntryFlagChecksum   uint32 = 1 << 1
)

// CRC64Table is the CRC64 table used for checksums.
var CRC64Table = crc64.MakeTable(crc64.ISO)

// Serialize writes the file header to the writer.
func (h *FileHeader) Serialize(
	w io.Writer,
) error {
	// Calculate checksum over header fields (excluding checksum itself)
	checksum := h.calculateChecksum()
	h.Checksum = checksum

	// Write magic number
	if _, err := w.Write(h.Magic[:]); err != nil {
		return err
	}

	// Write version (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.Version); err != nil {
		return err
	}

	// Write header size (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.HeaderSize); err != nil {
		return err
	}

	// Write sequence number (8 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.SequenceNumber); err != nil {
		return err
	}

	// Write checksum (4 bytes, little-endian)
	return binary.Write(w, binary.LittleEndian, h.Checksum)
}

// Deserialize reads the file header from the reader.
func (h *FileHeader) Deserialize(
	r io.Reader,
) error {
	// Read magic number
	if _, err := io.ReadFull(r, h.Magic[:]); err != nil {
		return err
	}
	if h.Magic != MagicBytes {
		return fmt.Errorf(
			"invalid WAL magic: got %q, want %q",
			string(h.Magic[:]),
			string(MagicBytes[:]),
		)
	}

	// Read version
	if err := binary.Read(r, binary.LittleEndian, &h.Version); err != nil {
		return err
	}
	if h.Version > CurrentVersion {
		return fmt.Errorf(
			"unsupported WAL version: %d (current: %d)",
			h.Version,
			CurrentVersion,
		)
	}

	// Read header size
	if err := binary.Read(r, binary.LittleEndian, &h.HeaderSize); err != nil {
		return err
	}
	if h.HeaderSize != HeaderSize {
		return fmt.Errorf(
			"invalid WAL header size: got %d, expected %d",
			h.HeaderSize,
			HeaderSize,
		)
	}

	// Read sequence number
	if err := binary.Read(r, binary.LittleEndian, &h.SequenceNumber); err != nil {
		return err
	}

	// Read checksum
	if err := binary.Read(r, binary.LittleEndian, &h.Checksum); err != nil {
		return err
	}

	// Verify checksum
	expectedChecksum := h.calculateChecksum()
	if h.Checksum != expectedChecksum {
		return fmt.Errorf(
			"WAL header checksum mismatch: got 0x%08x, expected 0x%08x",
			h.Checksum,
			expectedChecksum,
		)
	}

	return nil
}

// calculateChecksum computes the CRC32 checksum of the header fields (excluding checksum).
func (h *FileHeader) calculateChecksum() uint32 {
	crc := crc32.NewIEEE()

	// Hash magic number
	_, _ = crc.Write(h.Magic[:])

	// Hash version
	_ = binary.Write(crc, binary.LittleEndian, h.Version)

	// Hash header size
	_ = binary.Write(crc, binary.LittleEndian, h.HeaderSize)

	// Hash sequence number
	_ = binary.Write(crc, binary.LittleEndian, h.SequenceNumber)

	return crc.Sum32()
}

// Serialize writes the entry header to the writer.
func (h *EntryHeader) Serialize(w io.Writer) error {
	// Write type (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.Type); err != nil {
		return fmt.Errorf("failed to write entry type: %w", err)
	}

	// Write flags (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.Flags); err != nil {
		return fmt.Errorf("failed to write entry flags: %w", err)
	}

	// Write length (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.Length); err != nil {
		return fmt.Errorf("failed to write entry length: %w", err)
	}

	// Write sequence number (4 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, h.SequenceNumber); err != nil {
		return fmt.Errorf("failed to write entry sequence number: %w", err)
	}

	return nil
}

// Deserialize reads the entry header from the reader.
func (h *EntryHeader) Deserialize(r io.Reader) error {
	// Read type (4 bytes, little-endian)
	if err := binary.Read(r, binary.LittleEndian, &h.Type); err != nil {
		return fmt.Errorf("failed to read entry type: %w", err)
	}

	// Read flags (4 bytes, little-endian)
	if err := binary.Read(r, binary.LittleEndian, &h.Flags); err != nil {
		return fmt.Errorf("failed to read entry flags: %w", err)
	}

	// Read length (4 bytes, little-endian)
	if err := binary.Read(r, binary.LittleEndian, &h.Length); err != nil {
		return fmt.Errorf("failed to read entry length: %w", err)
	}

	// Read sequence number (4 bytes, little-endian)
	if err := binary.Read(r, binary.LittleEndian, &h.SequenceNumber); err != nil {
		return fmt.Errorf("failed to read entry sequence number: %w", err)
	}

	return nil
}

// CalculateChecksum computes the CRC32 checksum of the entry header and payload.
func (h *EntryHeader) CalculateChecksum(payload []byte) uint32 {
	crc := crc32.NewIEEE()

	// Hash header fields
	_ = binary.Write(crc, binary.LittleEndian, h.Type)
	_ = binary.Write(crc, binary.LittleEndian, h.Flags)
	_ = binary.Write(crc, binary.LittleEndian, h.Length)
	_ = binary.Write(crc, binary.LittleEndian, h.SequenceNumber)

	// Hash payload
	_, _ = crc.Write(payload)

	return crc.Sum32()
}

// writeString writes a length-prefixed string to the writer.
func writeString(w io.Writer, s string) error {
	data := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)

	return err
}

// readString reads a length-prefixed string from the reader.
func readString(r io.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}

	return string(data), nil
}

// writeBytes writes length-prefixed bytes to the writer.
func writeBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)

	return err
}

// readBytes reads length-prefixed bytes from the reader.
func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}
