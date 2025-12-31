package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

// InsertEntry represents an INSERT WAL entry.
type InsertEntry struct {
	txnID  uint64
	Schema string
	Table  string
	Values [][]any
}

// NewInsertEntry creates a new InsertEntry.
func NewInsertEntry(
	txnID uint64,
	schema, table string,
	values [][]any,
) *InsertEntry {
	return &InsertEntry{
		txnID:  txnID,
		Schema: schema,
		Table:  table,
		Values: values,
	}
}

// Type returns the entry type.
func (e *InsertEntry) Type() EntryType {
	return EntryInsert
}

// TxnID returns the transaction ID.
func (e *InsertEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *InsertEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}

	// Serialize values using gob for flexibility with any types
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e.Values); err != nil {
		return err
	}

	return writeBytes(w, buf.Bytes())
}

// Deserialize reads the entry from the reader.
func (e *InsertEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}

	data, err := readBytes(r)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(bytes.NewReader(data))

	return dec.Decode(&e.Values)
}

// DeleteEntry represents a DELETE WAL entry.
// DeletedData stores the row values before deletion for rollback support (ACID compliance).
type DeleteEntry struct {
	txnID       uint64
	Schema      string
	Table       string
	RowIDs      []uint64
	DeletedData [][]any // Row values before deletion for rollback
}

// NewDeleteEntry creates a new DeleteEntry.
func NewDeleteEntry(
	txnID uint64,
	schema, table string,
	rowIDs []uint64,
) *DeleteEntry {
	return &DeleteEntry{
		txnID:       txnID,
		Schema:      schema,
		Table:       table,
		RowIDs:      rowIDs,
		DeletedData: nil,
	}
}

// NewDeleteEntryWithData creates a new DeleteEntry with deleted row data for rollback support.
func NewDeleteEntryWithData(
	txnID uint64,
	schema, table string,
	rowIDs []uint64,
	deletedData [][]any,
) *DeleteEntry {
	return &DeleteEntry{
		txnID:       txnID,
		Schema:      schema,
		Table:       table,
		RowIDs:      rowIDs,
		DeletedData: deletedData,
	}
}

// GetDeletedData returns the deleted row data.
func (e *DeleteEntry) GetDeletedData() [][]any {
	return e.DeletedData
}

// Type returns the entry type.
func (e *DeleteEntry) Type() EntryType {
	return EntryDelete
}

// TxnID returns the transaction ID.
func (e *DeleteEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *DeleteEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.RowIDs))); err != nil {
		return err
	}
	for _, rowID := range e.RowIDs {
		if err := binary.Write(w, binary.LittleEndian, rowID); err != nil {
			return err
		}
	}

	// Serialize deleted data for rollback support (ACID compliance)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e.DeletedData); err != nil {
		return err
	}

	return writeBytes(w, buf.Bytes())
}

// Deserialize reads the entry from the reader.
func (e *DeleteEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}
	var numRowIDs uint32
	if err := binary.Read(r, binary.LittleEndian, &numRowIDs); err != nil {
		return err
	}
	e.RowIDs = make([]uint64, numRowIDs)
	for i := range e.RowIDs {
		if err := binary.Read(r, binary.LittleEndian, &e.RowIDs[i]); err != nil {
			return err
		}
	}

	// Deserialize deleted data for rollback support
	data, err := readBytes(r)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(bytes.NewReader(data))

	return dec.Decode(&e.DeletedData)
}

// UpdateEntry represents an UPDATE WAL entry.
// BeforeValues stores the row values before update for rollback support (ACID compliance).
// AfterValues (NewValues) stores the row values after update for redo support.
type UpdateEntry struct {
	txnID        uint64
	Schema       string
	Table        string
	RowIDs       []uint64
	ColumnIdxs   []int
	BeforeValues [][]any // Row values before update for rollback (CRITICAL for MVCC)
	NewValues    [][]any // Row values after update for redo (AfterValues)
}

// NewUpdateEntry creates a new UpdateEntry (backward compatible).
func NewUpdateEntry(
	txnID uint64,
	schema, table string,
	rowIDs []uint64,
	columnIdxs []int,
	newValues [][]any,
) *UpdateEntry {
	return &UpdateEntry{
		txnID:        txnID,
		Schema:       schema,
		Table:        table,
		RowIDs:       rowIDs,
		ColumnIdxs:   columnIdxs,
		BeforeValues: nil,
		NewValues:    newValues,
	}
}

// NewUpdateEntryWithBeforeValues creates a new UpdateEntry with before values for rollback support.
func NewUpdateEntryWithBeforeValues(
	txnID uint64,
	schema, table string,
	rowIDs []uint64,
	columnIdxs []int,
	beforeValues [][]any,
	newValues [][]any,
) *UpdateEntry {
	return &UpdateEntry{
		txnID:        txnID,
		Schema:       schema,
		Table:        table,
		RowIDs:       rowIDs,
		ColumnIdxs:   columnIdxs,
		BeforeValues: beforeValues,
		NewValues:    newValues,
	}
}

// GetBeforeValues returns the before values.
func (e *UpdateEntry) GetBeforeValues() [][]any {
	return e.BeforeValues
}

// GetAfterValues returns the after values (alias for NewValues).
func (e *UpdateEntry) GetAfterValues() [][]any {
	return e.NewValues
}

// Type returns the entry type.
func (e *UpdateEntry) Type() EntryType {
	return EntryUpdate
}

// TxnID returns the transaction ID.
func (e *UpdateEntry) TxnID() uint64 {
	return e.txnID
}

// Serialize writes the entry to the writer.
func (e *UpdateEntry) Serialize(
	w io.Writer,
) error {
	if err := binary.Write(w, binary.LittleEndian, e.txnID); err != nil {
		return err
	}
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}

	// Write row IDs
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.RowIDs))); err != nil {
		return err
	}
	for _, rowID := range e.RowIDs {
		if err := binary.Write(w, binary.LittleEndian, rowID); err != nil {
			return err
		}
	}

	// Write column indices
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.ColumnIdxs))); err != nil {
		return err
	}
	for _, idx := range e.ColumnIdxs {
		if err := binary.Write(w, binary.LittleEndian, int32(idx)); err != nil {
			return err
		}
	}

	// Write before values using gob for rollback support (ACID compliance)
	var beforeBuf bytes.Buffer
	beforeEnc := gob.NewEncoder(&beforeBuf)
	if err := beforeEnc.Encode(e.BeforeValues); err != nil {
		return err
	}
	if err := writeBytes(w, beforeBuf.Bytes()); err != nil {
		return err
	}

	// Write new/after values using gob for redo support
	var afterBuf bytes.Buffer
	afterEnc := gob.NewEncoder(&afterBuf)
	if err := afterEnc.Encode(e.NewValues); err != nil {
		return err
	}

	return writeBytes(w, afterBuf.Bytes())
}

// Deserialize reads the entry from the reader.
func (e *UpdateEntry) Deserialize(
	r io.Reader,
) error {
	if err := binary.Read(r, binary.LittleEndian, &e.txnID); err != nil {
		return err
	}
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}

	// Read row IDs
	var numRowIDs uint32
	if err := binary.Read(r, binary.LittleEndian, &numRowIDs); err != nil {
		return err
	}
	e.RowIDs = make([]uint64, numRowIDs)
	for i := range e.RowIDs {
		if err := binary.Read(r, binary.LittleEndian, &e.RowIDs[i]); err != nil {
			return err
		}
	}

	// Read column indices
	var numColumns uint32
	if err := binary.Read(r, binary.LittleEndian, &numColumns); err != nil {
		return err
	}
	e.ColumnIdxs = make([]int, numColumns)
	for i := range e.ColumnIdxs {
		var idx int32
		if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
			return err
		}
		e.ColumnIdxs[i] = int(idx)
	}

	// Read before values using gob for rollback support
	beforeData, err := readBytes(r)
	if err != nil {
		return err
	}
	beforeDec := gob.NewDecoder(bytes.NewReader(beforeData))
	if err := beforeDec.Decode(&e.BeforeValues); err != nil {
		return err
	}

	// Read new/after values using gob for redo support
	afterData, err := readBytes(r)
	if err != nil {
		return err
	}
	afterDec := gob.NewDecoder(bytes.NewReader(afterData))
	return afterDec.Decode(&e.NewValues)
}

// UseTableEntry represents a USE TABLE WAL entry.
// This entry is used to switch the current table context for subsequent operations.
type UseTableEntry struct {
	Schema string
	Table  string
}

// Type returns the entry type.
func (e *UseTableEntry) Type() EntryType {
	return EntryUseTable
}

// Serialize writes the entry to the writer.
func (e *UseTableEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}

	return writeString(w, e.Table)
}

// Deserialize reads the entry from the reader.
func (e *UseTableEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	e.Table, err = readString(r)

	return err
}
