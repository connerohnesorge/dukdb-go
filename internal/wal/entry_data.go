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
func NewInsertEntry(txnID uint64, schema, table string, values [][]any) *InsertEntry {
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
func (e *InsertEntry) Serialize(w io.Writer) error {
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
func (e *InsertEntry) Deserialize(r io.Reader) error {
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
type DeleteEntry struct {
	txnID  uint64
	Schema string
	Table  string
	RowIDs []uint64
}

// NewDeleteEntry creates a new DeleteEntry.
func NewDeleteEntry(txnID uint64, schema, table string, rowIDs []uint64) *DeleteEntry {
	return &DeleteEntry{
		txnID:  txnID,
		Schema: schema,
		Table:  table,
		RowIDs: rowIDs,
	}
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
func (e *DeleteEntry) Serialize(w io.Writer) error {
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
	return nil
}

// Deserialize reads the entry from the reader.
func (e *DeleteEntry) Deserialize(r io.Reader) error {
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
	return nil
}

// UpdateEntry represents an UPDATE WAL entry.
type UpdateEntry struct {
	txnID      uint64
	Schema     string
	Table      string
	RowIDs     []uint64
	ColumnIdxs []int
	NewValues  [][]any
}

// NewUpdateEntry creates a new UpdateEntry.
func NewUpdateEntry(txnID uint64, schema, table string, rowIDs []uint64, columnIdxs []int, newValues [][]any) *UpdateEntry {
	return &UpdateEntry{
		txnID:      txnID,
		Schema:     schema,
		Table:      table,
		RowIDs:     rowIDs,
		ColumnIdxs: columnIdxs,
		NewValues:  newValues,
	}
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
func (e *UpdateEntry) Serialize(w io.Writer) error {
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

	// Write new values using gob
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e.NewValues); err != nil {
		return err
	}
	return writeBytes(w, buf.Bytes())
}

// Deserialize reads the entry from the reader.
func (e *UpdateEntry) Deserialize(r io.Reader) error {
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

	// Read new values using gob
	data, err := readBytes(r)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(&e.NewValues)
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
func (e *UseTableEntry) Serialize(w io.Writer) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	return writeString(w, e.Table)
}

// Deserialize reads the entry from the reader.
func (e *UseTableEntry) Deserialize(r io.Reader) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	e.Table, err = readString(r)
	return err
}
