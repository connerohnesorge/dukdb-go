// Package wal provides Write-Ahead Logging for durability and crash recovery.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
)

// EntryType represents the type of WAL entry.
type EntryType uint8

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

// FileHeader represents the WAL file header.
type FileHeader struct {
	Magic     [4]byte // "DWAL"
	Version   uint8   // WAL format version (2 = checksummed)
	Iteration uint64  // Checkpoint iteration counter
}

// MagicBytes is the magic number for WAL files.
var MagicBytes = [4]byte{'D', 'W', 'A', 'L'}

// CurrentVersion is the current WAL format version.
const CurrentVersion uint8 = 2

// HeaderSize is the size of the file header in bytes.
const HeaderSize = 4 + 1 + 8 // Magic + Version + Iteration = 13 bytes

// EntryHeader is the header for each WAL entry.
type EntryHeader struct {
	Size     uint64    // Entry payload size in bytes
	Checksum uint64    // CRC64 checksum covering Size + Type + Data
	Type     EntryType // Entry type
}

// EntryHeaderSize is the size of an entry header in bytes.
const EntryHeaderSize = 8 + 8 + 1 // Size + Checksum + Type = 17 bytes

// CRC64Table is the CRC64 table used for checksums.
var CRC64Table = crc64.MakeTable(crc64.ISO)

// Serialize writes the file header to the writer.
func (h *FileHeader) Serialize(w io.Writer) error {
	if _, err := w.Write(h.Magic[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Version); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, h.Iteration)
}

// Deserialize reads the file header from the reader.
func (h *FileHeader) Deserialize(r io.Reader) error {
	if _, err := io.ReadFull(r, h.Magic[:]); err != nil {
		return err
	}
	if h.Magic != MagicBytes {
		return fmt.Errorf("invalid WAL magic: got %v, want %v", h.Magic, MagicBytes)
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Version); err != nil {
		return err
	}
	if h.Version > CurrentVersion {
		return fmt.Errorf("unsupported WAL version: %d", h.Version)
	}

	return binary.Read(r, binary.LittleEndian, &h.Iteration)
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
