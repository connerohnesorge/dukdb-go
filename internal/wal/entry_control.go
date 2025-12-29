package wal

import (
	"encoding/binary"
	"io"
	"time"
)

// TxnBeginEntry represents a transaction begin WAL entry.
type TxnBeginEntry struct {
	txnID     uint64
	Timestamp int64
}

// NewTxnBeginEntry creates a new TxnBeginEntry.
func NewTxnBeginEntry(txnID uint64, timestamp time.Time) *TxnBeginEntry {
	return &TxnBeginEntry{
		txnID:     txnID,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *TxnBeginEntry) Type() EntryType {
	return EntryTxnBegin
}

// TxnID returns the transaction ID.
func (e *TxnBeginEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *TxnBeginEntry) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, e.Timestamp)
}

// Deserialize reads the entry from the reader.
func (e *TxnBeginEntry) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	return binary.Read(r, binary.LittleEndian, &e.Timestamp)
}

// TxnCommitEntry represents a transaction commit WAL entry.
type TxnCommitEntry struct {
	txnID     uint64
	Timestamp int64
}

// NewTxnCommitEntry creates a new TxnCommitEntry.
func NewTxnCommitEntry(txnID uint64, timestamp time.Time) *TxnCommitEntry {
	return &TxnCommitEntry{
		txnID:     txnID,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *TxnCommitEntry) Type() EntryType {
	return EntryTxnCommit
}

// TxnID returns the transaction ID.
func (e *TxnCommitEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *TxnCommitEntry) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, e.Timestamp)
}

// Deserialize reads the entry from the reader.
func (e *TxnCommitEntry) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	return binary.Read(r, binary.LittleEndian, &e.Timestamp)
}

// CheckpointEntry represents a checkpoint marker WAL entry.
type CheckpointEntry struct {
	Iteration uint64
	Timestamp int64
}

// NewCheckpointEntry creates a new CheckpointEntry.
func NewCheckpointEntry(iteration uint64, timestamp time.Time) *CheckpointEntry {
	return &CheckpointEntry{
		Iteration: iteration,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *CheckpointEntry) Type() EntryType {
	return EntryCheckpoint
}

// Serialize writes the entry to the writer.
func (e *CheckpointEntry) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, e.Iteration); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, e.Timestamp)
}

// Deserialize reads the entry from the reader.
func (e *CheckpointEntry) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Iteration); err != nil {
		return err
	}
	return binary.Read(r, binary.LittleEndian, &e.Timestamp)
}

// FlushEntry represents a WAL flush marker entry.
type FlushEntry struct {
	Timestamp int64
}

// NewFlushEntry creates a new FlushEntry.
func NewFlushEntry(timestamp time.Time) *FlushEntry {
	return &FlushEntry{
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *FlushEntry) Type() EntryType {
	return EntryFlush
}

// Serialize writes the entry to the writer.
func (e *FlushEntry) Serialize(w io.Writer) error {
	return binary.Write(w, binary.LittleEndian, e.Timestamp)
}

// Deserialize reads the entry from the reader.
func (e *FlushEntry) Deserialize(r io.Reader) error {
	return binary.Read(r, binary.LittleEndian, &e.Timestamp)
}

// VersionEntry represents a WAL version/header entry.
type VersionEntry struct {
	Version   uint8
	Iteration uint64
	Timestamp int64
}

// NewVersionEntry creates a new VersionEntry.
func NewVersionEntry(version uint8, iteration uint64, timestamp time.Time) *VersionEntry {
	return &VersionEntry{
		Version:   version,
		Iteration: iteration,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *VersionEntry) Type() EntryType {
	return EntryVersion
}

// Serialize writes the entry to the writer.
func (e *VersionEntry) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, e.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.Iteration); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, e.Timestamp)
}

// Deserialize reads the entry from the reader.
func (e *VersionEntry) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Version); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Iteration); err != nil {
		return err
	}
	return binary.Read(r, binary.LittleEndian, &e.Timestamp)
}
