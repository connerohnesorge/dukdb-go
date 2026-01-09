// Package duckdb implements DuckDB-compatible storage format utilities.
package duckdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// Deserialization error definitions.
var (
	ErrUnexpectedEOF     = errors.New("unexpected end of data")
	ErrInvalidFieldID    = errors.New("invalid field ID")
	ErrUnexpectedFieldID = errors.New("unexpected field ID")
	ErrInvalidObjectEnd  = errors.New("expected object terminator not found")
)

// BinaryDeserializer reads data written by BinarySerializer.
// This format uses:
// - Field IDs (uint16 little-endian) before each property
// - Variable-length encoding for integers
// - Message terminator (0xFFFF) at object end
type BinaryDeserializer struct {
	r   *bufio.Reader
	err error
}

// NewBinaryDeserializer creates a new BinaryDeserializer.
func NewBinaryDeserializer(r io.Reader) *BinaryDeserializer {
	// Use bufio.Reader for efficient buffering and peeking
	var br *bufio.Reader
	if buffered, ok := r.(*bufio.Reader); ok {
		br = buffered
	} else {
		br = bufio.NewReader(r)
	}
	return &BinaryDeserializer{r: br}
}

// Err returns the first error encountered.
func (d *BinaryDeserializer) Err() error {
	return d.err
}

// readBytes reads exactly n bytes.
func (d *BinaryDeserializer) readBytes(n int) []byte {
	if d.err != nil {
		return nil
	}

	buf := make([]byte, n)
	_, err := io.ReadFull(d.r, buf)
	if err != nil {
		if err == io.EOF {
			d.err = ErrUnexpectedEOF
		} else {
			d.err = err
		}
		return nil
	}
	return buf
}

// ReadFieldID reads a uint16 field ID (little-endian).
func (d *BinaryDeserializer) ReadFieldID() uint16 {
	buf := d.readBytes(2)
	if buf == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(buf)
}

// PeekFieldID reads a field ID without consuming it.
// Returns the field ID and whether it was successfully read.
func (d *BinaryDeserializer) PeekFieldID() (uint16, bool) {
	if d.err != nil {
		return 0, false
	}

	// Peek at the next 2 bytes
	buf, err := d.r.Peek(2)
	if err != nil {
		if err == io.EOF {
			d.err = ErrUnexpectedEOF
		} else {
			d.err = err
		}
		return 0, false
	}

	fieldID := binary.LittleEndian.Uint16(buf)
	return fieldID, true
}

// IsObjectEnd checks if the next field ID is the message terminator.
// Consumes the terminator if found.
func (d *BinaryDeserializer) IsObjectEnd() bool {
	if d.err != nil {
		return false
	}

	// Peek first to check if it's the terminator
	fieldID, ok := d.PeekFieldID()
	if !ok {
		return false
	}

	if fieldID == MESSAGE_TERMINATOR_FIELD_ID {
		// Consume the terminator
		d.ReadFieldID()
		return true
	}

	return false
}

// ReadUint8 reads a varint-encoded uint8.
func (d *BinaryDeserializer) ReadUint8() uint8 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return uint8(val)
}

// ReadUint16 reads a varint-encoded uint16.
func (d *BinaryDeserializer) ReadUint16() uint16 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return uint16(val)
}

// ReadUint32 reads a varint-encoded uint32.
func (d *BinaryDeserializer) ReadUint32() uint32 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return uint32(val)
}

// ReadUint64 reads a varint-encoded uint64.
func (d *BinaryDeserializer) ReadUint64() uint64 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return val
}

// ReadInt8 reads a zigzag+varint encoded int8.
func (d *BinaryDeserializer) ReadInt8() int8 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return int8(ZigZagDecode(val))
}

// ReadInt16 reads a zigzag+varint encoded int16.
func (d *BinaryDeserializer) ReadInt16() int16 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return int16(ZigZagDecode(val))
}

// ReadInt32 reads a zigzag+varint encoded int32.
func (d *BinaryDeserializer) ReadInt32() int32 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return int32(ZigZagDecode(val))
}

// ReadInt64 reads a zigzag+varint encoded int64.
func (d *BinaryDeserializer) ReadInt64() int64 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return ZigZagDecode(val)
}

// ReadBool reads a single byte as boolean.
func (d *BinaryDeserializer) ReadBool() bool {
	if d.err != nil {
		return false
	}
	buf := d.readBytes(1)
	if buf == nil {
		return false
	}
	return buf[0] != 0
}

// ReadString reads a varint length-prefixed string.
func (d *BinaryDeserializer) ReadString() string {
	if d.err != nil {
		return ""
	}

	length, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return ""
	}

	if length == 0 {
		return ""
	}

	buf := d.readBytes(int(length))
	if buf == nil {
		return ""
	}

	return string(buf)
}

// ReadBytes reads a varint length-prefixed byte slice.
func (d *BinaryDeserializer) ReadBytes() []byte {
	if d.err != nil {
		return nil
	}

	length, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return nil
	}

	if length == 0 {
		return []byte{}
	}

	buf := d.readBytes(int(length))
	if buf == nil {
		return nil
	}

	return buf
}

// ReadListCount reads the list count (varint).
func (d *BinaryDeserializer) ReadListCount() uint64 {
	if d.err != nil {
		return 0
	}
	val, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
		return 0
	}
	return val
}

// SkipProperty skips over a property value for an unknown property.
// This attempts to skip common types but may fail for complex nested structures.
// If the property type is unknown, this will likely corrupt the stream.
func (d *BinaryDeserializer) SkipProperty() {
	// This is a best-effort approach - without knowing the type,
	// we can't reliably skip. For now, try to read and discard a varint.
	// This works for simple integer properties but not for strings, lists, or objects.
	if d.err != nil {
		return
	}
	_, err := VarIntDecode(d.r)
	if err != nil {
		d.err = err
	}
}

// OnObjectBegin marks the beginning of reading an object.
// Objects don't have headers in BinarySerializer, so this is a no-op.
func (d *BinaryDeserializer) OnObjectBegin() {
	// No-op: objects don't have headers in BinaryDeserializer
}

// OnObjectEnd reads and validates the object terminator (MESSAGE_TERMINATOR_FIELD_ID).
// Returns an error if the terminator is not found.
func (d *BinaryDeserializer) OnObjectEnd() {
	if d.err != nil {
		return
	}

	if !d.IsObjectEnd() {
		d.err = ErrInvalidObjectEnd
	}
}

// ExpectFieldID reads a field ID and verifies it matches the expected value.
// Returns the field ID if successful, or sets an error if it doesn't match.
func (d *BinaryDeserializer) ExpectFieldID(expectedID uint16) uint16 {
	if d.err != nil {
		return 0
	}

	actualID := d.ReadFieldID()
	if d.err != nil {
		return 0
	}

	if actualID != expectedID {
		d.err = ErrUnexpectedFieldID
		return 0
	}

	return actualID
}

// ReadProperty reads a property with the given field ID.
// Returns whether the property was successfully read.
// The value is read using the provided reader function.
func (d *BinaryDeserializer) ReadProperty(expectedID uint16, readFn func()) bool {
	if d.err != nil {
		return false
	}

	// Peek at the next field ID
	fieldID, ok := d.PeekFieldID()
	if !ok {
		return false
	}

	// Check if it matches the expected ID
	if fieldID != expectedID {
		// This property is not present (might be optional or default value)
		return false
	}

	// Consume the field ID
	d.ReadFieldID()

	// Read the property value
	readFn()

	return d.err == nil
}

// ReadOptionalProperty reads an optional property.
// If the property is present (field ID matches), calls readFn.
// Returns true if the property was present and successfully read.
func (d *BinaryDeserializer) ReadOptionalProperty(expectedID uint16, readFn func()) bool {
	return d.ReadProperty(expectedID, readFn)
}
