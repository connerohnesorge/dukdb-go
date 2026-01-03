package wal

import (
	"encoding/binary"
	"io"

	dukdb "github.com/dukdb/dukdb-go"
)

// ColumnDef represents a column definition for WAL serialization.
type ColumnDef struct {
	Name       string
	Type       dukdb.Type
	Nullable   bool
	HasDefault bool
}

// CreateTableEntry represents a CREATE TABLE WAL entry.
type CreateTableEntry struct {
	Schema  string
	Name    string
	Columns []ColumnDef
}

// Type returns the entry type.
func (e *CreateTableEntry) Type() EntryType {
	return EntryCreateTable
}

// Serialize writes the entry to the writer.
func (e *CreateTableEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.Columns))); err != nil {
		return err
	}
	for _, col := range e.Columns {
		if err := writeString(w, col.Name); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint8(col.Type)); err != nil {
			return err
		}
		var flags uint8
		if col.Nullable {
			flags |= 0x01
		}
		if col.HasDefault {
			flags |= 0x02
		}
		if err := binary.Write(w, binary.LittleEndian, flags); err != nil {
			return err
		}
	}

	return nil
}

// Deserialize reads the entry from the reader.
func (e *CreateTableEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Name, err = readString(r); err != nil {
		return err
	}
	var numColumns uint32
	if err := binary.Read(r, binary.LittleEndian, &numColumns); err != nil {
		return err
	}
	e.Columns = make([]ColumnDef, numColumns)
	for i := range e.Columns {
		if e.Columns[i].Name, err = readString(r); err != nil {
			return err
		}
		var typ uint8
		if err := binary.Read(r, binary.LittleEndian, &typ); err != nil {
			return err
		}
		e.Columns[i].Type = dukdb.Type(typ)
		var flags uint8
		if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
			return err
		}
		e.Columns[i].Nullable = flags&0x01 != 0
		e.Columns[i].HasDefault = flags&0x02 != 0
	}

	return nil
}

// DropTableEntry represents a DROP TABLE WAL entry.
type DropTableEntry struct {
	Schema string
	Name   string
}

// Type returns the entry type.
func (e *DropTableEntry) Type() EntryType {
	return EntryDropTable
}

// Serialize writes the entry to the writer.
func (e *DropTableEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}

	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *DropTableEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	e.Name, err = readString(r)

	return err
}

// CreateSchemaEntry represents a CREATE SCHEMA WAL entry.
type CreateSchemaEntry struct {
	Name string
}

// Type returns the entry type.
func (e *CreateSchemaEntry) Type() EntryType {
	return EntryCreateSchema
}

// Serialize writes the entry to the writer.
func (e *CreateSchemaEntry) Serialize(
	w io.Writer,
) error {
	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *CreateSchemaEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	e.Name, err = readString(r)

	return err
}

// DropSchemaEntry represents a DROP SCHEMA WAL entry.
type DropSchemaEntry struct {
	Name string
}

// Type returns the entry type.
func (e *DropSchemaEntry) Type() EntryType {
	return EntryDropSchema
}

// Serialize writes the entry to the writer.
func (e *DropSchemaEntry) Serialize(
	w io.Writer,
) error {
	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *DropSchemaEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	e.Name, err = readString(r)

	return err
}

// CreateViewEntry represents a CREATE VIEW WAL entry.
type CreateViewEntry struct {
	Schema string
	Name   string
	Query  string
}

// Type returns the entry type.
func (e *CreateViewEntry) Type() EntryType {
	return EntryCreateView
}

// Serialize writes the entry to the writer.
func (e *CreateViewEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}

	return writeString(w, e.Query)
}

// Deserialize reads the entry from the reader.
func (e *CreateViewEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Name, err = readString(r); err != nil {
		return err
	}
	e.Query, err = readString(r)

	return err
}

// DropViewEntry represents a DROP VIEW WAL entry.
type DropViewEntry struct {
	Schema string
	Name   string
}

// Type returns the entry type.
func (e *DropViewEntry) Type() EntryType {
	return EntryDropView
}

// Serialize writes the entry to the writer.
func (e *DropViewEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}

	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *DropViewEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	e.Name, err = readString(r)

	return err
}

// CreateIndexEntry represents a CREATE INDEX WAL entry.
type CreateIndexEntry struct {
	Schema    string
	Table     string
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
}

// Type returns the entry type.
func (e *CreateIndexEntry) Type() EntryType {
	return EntryCreateIndex
}

// Serialize writes the entry to the writer.
func (e *CreateIndexEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.Columns))); err != nil {
		return err
	}
	for _, col := range e.Columns {
		if err := writeString(w, col); err != nil {
			return err
		}
	}
	var flags uint8
	if e.IsUnique {
		flags |= 0x01
	}
	if e.IsPrimary {
		flags |= 0x02
	}

	return binary.Write(
		w,
		binary.LittleEndian,
		flags,
	)
}

// Deserialize reads the entry from the reader.
func (e *CreateIndexEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}
	if e.Name, err = readString(r); err != nil {
		return err
	}
	var numColumns uint32
	if err := binary.Read(r, binary.LittleEndian, &numColumns); err != nil {
		return err
	}
	e.Columns = make([]string, numColumns)
	for i := range e.Columns {
		if e.Columns[i], err = readString(r); err != nil {
			return err
		}
	}
	var flags uint8
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return err
	}
	e.IsUnique = flags&0x01 != 0
	e.IsPrimary = flags&0x02 != 0

	return nil
}

// DropIndexEntry represents a DROP INDEX WAL entry.
type DropIndexEntry struct {
	Schema string
	Table  string
	Name   string
}

// Type returns the entry type.
func (e *DropIndexEntry) Type() EntryType {
	return EntryDropIndex
}

// Serialize writes the entry to the writer.
func (e *DropIndexEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}

	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *DropIndexEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}
	e.Name, err = readString(r)

	return err
}

// CreateSequenceEntry represents a CREATE SEQUENCE WAL entry.
type CreateSequenceEntry struct {
	Schema      string
	Name        string
	StartWith   int64
	IncrementBy int64
	MinValue    int64
	MaxValue    int64
	IsCycle     bool
}

// Type returns the entry type.
func (e *CreateSequenceEntry) Type() EntryType {
	return EntryCreateSequence
}

// Serialize writes the entry to the writer.
func (e *CreateSequenceEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.StartWith); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.IncrementBy); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.MinValue); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.MaxValue); err != nil {
		return err
	}
	var flags uint8
	if e.IsCycle {
		flags |= 0x01
	}

	return binary.Write(w, binary.LittleEndian, flags)
}

// Deserialize reads the entry from the reader.
func (e *CreateSequenceEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Name, err = readString(r); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.StartWith); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.IncrementBy); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.MinValue); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.MaxValue); err != nil {
		return err
	}
	var flags uint8
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return err
	}
	e.IsCycle = flags&0x01 != 0

	return nil
}

// DropSequenceEntry represents a DROP SEQUENCE WAL entry.
type DropSequenceEntry struct {
	Schema string
	Name   string
}

// Type returns the entry type.
func (e *DropSequenceEntry) Type() EntryType {
	return EntryDropSequence
}

// Serialize writes the entry to the writer.
func (e *DropSequenceEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}

	return writeString(w, e.Name)
}

// Deserialize reads the entry from the reader.
func (e *DropSequenceEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	e.Name, err = readString(r)

	return err
}

// SequenceValueEntry represents a sequence value advance WAL entry.
// This is logged when nextval() is called to persist the sequence state.
type SequenceValueEntry struct {
	Schema     string
	Name       string
	CurrentVal int64 // The new current value after the advance
}

// Type returns the entry type.
func (e *SequenceValueEntry) Type() EntryType {
	return EntrySequenceValue
}

// Serialize writes the entry to the writer.
func (e *SequenceValueEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Name); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, e.CurrentVal)
}

// Deserialize reads the entry from the reader.
func (e *SequenceValueEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Name, err = readString(r); err != nil {
		return err
	}

	return binary.Read(r, binary.LittleEndian, &e.CurrentVal)
}

// AlterTableEntry represents an ALTER TABLE WAL entry.
type AlterTableEntry struct {
	Schema       string
	Table        string
	Operation    uint8 // AlterTableOp encoded as uint8
	NewTableName string
	OldColumn    string
	NewColumn    string
	Column       string
}

// Type returns the entry type.
func (e *AlterTableEntry) Type() EntryType {
	return EntryAlterInfo
}

// Serialize writes the entry to the writer.
func (e *AlterTableEntry) Serialize(
	w io.Writer,
) error {
	if err := writeString(w, e.Schema); err != nil {
		return err
	}
	if err := writeString(w, e.Table); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.Operation); err != nil {
		return err
	}
	if err := writeString(w, e.NewTableName); err != nil {
		return err
	}
	if err := writeString(w, e.OldColumn); err != nil {
		return err
	}
	if err := writeString(w, e.NewColumn); err != nil {
		return err
	}

	return writeString(w, e.Column)
}

// Deserialize reads the entry from the reader.
func (e *AlterTableEntry) Deserialize(
	r io.Reader,
) error {
	var err error
	if e.Schema, err = readString(r); err != nil {
		return err
	}
	if e.Table, err = readString(r); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Operation); err != nil {
		return err
	}
	if e.NewTableName, err = readString(r); err != nil {
		return err
	}
	if e.OldColumn, err = readString(r); err != nil {
		return err
	}
	if e.NewColumn, err = readString(r); err != nil {
		return err
	}
	e.Column, err = readString(r)

	return err
}
