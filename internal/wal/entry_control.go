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
func NewTxnBeginEntry(
	txnID uint64,
	timestamp time.Time,
) *TxnBeginEntry {
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
func (e *TxnBeginEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *TxnBeginEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// TxnCommitEntry represents a transaction commit WAL entry.
type TxnCommitEntry struct {
	txnID     uint64
	Timestamp int64
}

// NewTxnCommitEntry creates a new TxnCommitEntry.
func NewTxnCommitEntry(
	txnID uint64,
	timestamp time.Time,
) *TxnCommitEntry {
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
func (e *TxnCommitEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *TxnCommitEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// CheckpointEntry represents a checkpoint marker WAL entry.
type CheckpointEntry struct {
	Iteration uint64
	Timestamp int64
}

// NewCheckpointEntry creates a new CheckpointEntry.
func NewCheckpointEntry(
	iteration uint64,
	timestamp time.Time,
) *CheckpointEntry {
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
func (e *CheckpointEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.Iteration); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *CheckpointEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Iteration); err != nil {
		return err
	}

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// FlushEntry represents a WAL flush marker entry.
type FlushEntry struct {
	Timestamp int64
}

// NewFlushEntry creates a new FlushEntry.
func NewFlushEntry(
	timestamp time.Time,
) *FlushEntry {
	return &FlushEntry{
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *FlushEntry) Type() EntryType {
	return EntryFlush
}

// Serialize writes the entry to the writer.
func (e *FlushEntry) Serialize(
	w io.Writer,
) error {
	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *FlushEntry) Deserialize(
	r io.Reader,
) error {
	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// SavepointEntry represents a savepoint creation in the WAL.
type SavepointEntry struct {
	txnID     uint64
	Name      string
	UndoIndex int // Position in undo log when savepoint was created
	Timestamp int64
}

// NewSavepointEntry creates a new SavepointEntry.
func NewSavepointEntry(
	txnID uint64,
	name string,
	undoIndex int,
	timestamp time.Time,
) *SavepointEntry {
	return &SavepointEntry{
		txnID:     txnID,
		Name:      name,
		UndoIndex: undoIndex,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *SavepointEntry) Type() EntryType {
	return EntrySavepoint
}

// TxnID returns the transaction ID.
func (e *SavepointEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *SavepointEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int64(e.UndoIndex)); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *SavepointEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	name, err := readString(r)
	if err != nil {
		return err
	}
	e.Name = name
	var undoIndex int64
	if err := binary.Read(r, binary.LittleEndian, &undoIndex); err != nil {
		return err
	}
	e.UndoIndex = int(undoIndex)

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// ReleaseSavepointEntry represents a savepoint release in the WAL.
type ReleaseSavepointEntry struct {
	txnID     uint64
	Name      string
	Timestamp int64
}

// NewReleaseSavepointEntry creates a new ReleaseSavepointEntry.
func NewReleaseSavepointEntry(
	txnID uint64,
	name string,
	timestamp time.Time,
) *ReleaseSavepointEntry {
	return &ReleaseSavepointEntry{
		txnID:     txnID,
		Name:      name,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *ReleaseSavepointEntry) Type() EntryType {
	return EntryReleaseSavepoint
}

// TxnID returns the transaction ID.
func (e *ReleaseSavepointEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *ReleaseSavepointEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *ReleaseSavepointEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	name, err := readString(r)
	if err != nil {
		return err
	}
	e.Name = name

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// RollbackSavepointEntry represents a rollback to savepoint in the WAL.
type RollbackSavepointEntry struct {
	txnID     uint64
	Name      string
	UndoIndex int // Position rolled back to
	Timestamp int64
}

// NewRollbackSavepointEntry creates a new RollbackSavepointEntry.
func NewRollbackSavepointEntry(
	txnID uint64,
	name string,
	undoIndex int,
	timestamp time.Time,
) *RollbackSavepointEntry {
	return &RollbackSavepointEntry{
		txnID:     txnID,
		Name:      name,
		UndoIndex: undoIndex,
		Timestamp: timestamp.UnixNano(),
	}
}

// Type returns the entry type.
func (e *RollbackSavepointEntry) Type() EntryType {
	return EntryRollbackSavepoint
}

// TxnID returns the transaction ID.
func (e *RollbackSavepointEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *RollbackSavepointEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int64(e.UndoIndex)); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *RollbackSavepointEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	name, err := readString(r)
	if err != nil {
		return err
	}
	e.Name = name
	var undoIndex int64
	if err := binary.Read(r, binary.LittleEndian, &undoIndex); err != nil {
		return err
	}
	e.UndoIndex = int(undoIndex)

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}

// VersionEntry represents a WAL version/header entry.
type VersionEntry struct {
	Version   uint8
	Iteration uint64
	Timestamp int64
}

// NewVersionEntry creates a new VersionEntry.
func NewVersionEntry(
	version uint8,
	iteration uint64,
	timestamp time.Time,
) *VersionEntry {
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
func (e *VersionEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.Iteration); err != nil {
		return err
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		e.Timestamp,
	)
}

// Deserialize reads the entry from the reader.
func (e *VersionEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Version); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Iteration); err != nil {
		return err
	}

	return binary.Read(
		r,
		binary.LittleEndian,
		&e.Timestamp,
	)
}
