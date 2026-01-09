// Package duckdb implements DuckDB-compatible storage format utilities.
package duckdb

import (
	"encoding/binary"
	"io"
)

// MESSAGE_TERMINATOR_FIELD_ID marks the end of an object in DuckDB's serialization format
const MESSAGE_TERMINATOR_FIELD_ID uint16 = 0xFFFF

// BinarySerializer writes data in DuckDB's BinarySerializer format.
// This format uses:
// - Field IDs (uint16 little-endian) before each property
// - Variable-length encoding for integers
// - Message terminator (0xFFFF) at object end
type BinarySerializer struct {
	w         io.Writer
	err       error
	debugMode bool // optional, for debugging
}

// NewBinarySerializer creates a new BinarySerializer that writes to w.
func NewBinarySerializer(w io.Writer) *BinarySerializer {
	return &BinarySerializer{
		w: w,
	}
}

// Err returns the first error encountered during serialization.
// Once an error occurs, all subsequent write operations are no-ops.
func (s *BinarySerializer) Err() error {
	return s.err
}

// writeBytes writes raw bytes to the underlying writer.
// If an error has already occurred, this is a no-op.
func (s *BinarySerializer) writeBytes(data []byte) {
	if s.err != nil {
		return
	}
	_, s.err = s.w.Write(data)
}

// OnPropertyBegin writes a field ID as little-endian uint16.
// The tag parameter is for debugging only and is not written to output.
func (s *BinarySerializer) OnPropertyBegin(fieldID uint16, tag string) {
	if s.err != nil {
		return
	}
	// Write field ID as little-endian uint16 (NOT varint)
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], fieldID)
	s.writeBytes(buf[:])
}

// OnObjectBegin is called when starting to serialize an object.
// Objects don't have headers in BinarySerializer, so this is a no-op.
func (s *BinarySerializer) OnObjectBegin() {
	// No-op: objects don't have headers in BinarySerializer
}

// OnObjectEnd is called when finishing object serialization.
// Writes MESSAGE_TERMINATOR_FIELD_ID (0xFFFF) as little-endian uint16.
func (s *BinarySerializer) OnObjectEnd() {
	if s.err != nil {
		return
	}
	// Write MESSAGE_TERMINATOR as little-endian uint16
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], MESSAGE_TERMINATOR_FIELD_ID)
	s.writeBytes(buf[:])
}

// OnListBegin writes the list count as a varint.
func (s *BinarySerializer) OnListBegin(count uint64) {
	if s.err != nil {
		return
	}
	s.err = VarIntEncode(s.w, count)
}

// WriteBool writes a boolean value as a single byte (0 or 1).
func (s *BinarySerializer) WriteBool(value bool) {
	if s.err != nil {
		return
	}
	var b byte
	if value {
		b = 1
	}
	s.writeBytes([]byte{b})
}

// WriteUint8 writes an unsigned 8-bit integer as a varint.
func (s *BinarySerializer) WriteUint8(value uint8) {
	if s.err != nil {
		return
	}
	s.err = VarIntEncode(s.w, uint64(value))
}

// WriteUint16 writes an unsigned 16-bit integer as a varint.
func (s *BinarySerializer) WriteUint16(value uint16) {
	if s.err != nil {
		return
	}
	s.err = VarIntEncode(s.w, uint64(value))
}

// WriteUint32 writes an unsigned 32-bit integer as a varint.
func (s *BinarySerializer) WriteUint32(value uint32) {
	if s.err != nil {
		return
	}
	s.err = VarIntEncode(s.w, uint64(value))
}

// WriteUint64 writes an unsigned 64-bit integer as a varint.
func (s *BinarySerializer) WriteUint64(value uint64) {
	if s.err != nil {
		return
	}
	s.err = VarIntEncode(s.w, value)
}

// WriteInt8 writes a signed 8-bit integer using ZigZag encoding then varint.
func (s *BinarySerializer) WriteInt8(value int8) {
	if s.err != nil {
		return
	}
	encoded := ZigZagEncode(int64(value))
	s.err = VarIntEncode(s.w, encoded)
}

// WriteInt16 writes a signed 16-bit integer using ZigZag encoding then varint.
func (s *BinarySerializer) WriteInt16(value int16) {
	if s.err != nil {
		return
	}
	encoded := ZigZagEncode(int64(value))
	s.err = VarIntEncode(s.w, encoded)
}

// WriteInt32 writes a signed 32-bit integer using ZigZag encoding then varint.
func (s *BinarySerializer) WriteInt32(value int32) {
	if s.err != nil {
		return
	}
	encoded := ZigZagEncode(int64(value))
	s.err = VarIntEncode(s.w, encoded)
}

// WriteInt64 writes a signed 64-bit integer using ZigZag encoding then varint.
func (s *BinarySerializer) WriteInt64(value int64) {
	if s.err != nil {
		return
	}
	encoded := ZigZagEncode(value)
	s.err = VarIntEncode(s.w, encoded)
}

// WriteString writes a string with its length as a varint followed by the bytes.
func (s *BinarySerializer) WriteString(value string) {
	if s.err != nil {
		return
	}
	// Write length as varint
	s.err = VarIntEncode(s.w, uint64(len(value)))
	if s.err != nil {
		return
	}
	// Write string bytes
	s.writeBytes([]byte(value))
}

// WriteBytes writes a byte slice with its length as a varint followed by the bytes.
func (s *BinarySerializer) WriteBytes(value []byte) {
	if s.err != nil {
		return
	}
	// Write length as varint
	s.err = VarIntEncode(s.w, uint64(len(value)))
	if s.err != nil {
		return
	}
	// Write bytes
	s.writeBytes(value)
}

// WriteRawBytes writes bytes directly without a length prefix.
func (s *BinarySerializer) WriteRawBytes(data []byte) {
	if s.err != nil {
		return
	}
	s.writeBytes(data)
}

// WriteProperty writes a property with field ID and value.
// Supports: bool, uint8, uint16, uint32, uint64, int8, int16, int32, int64, string
func (s *BinarySerializer) WriteProperty(fieldID uint16, tag string, value interface{}) {
	if s.err != nil {
		return
	}
	s.OnPropertyBegin(fieldID, tag)
	if s.err != nil {
		return
	}

	switch v := value.(type) {
	case bool:
		s.WriteBool(v)
	case uint8:
		s.WriteUint8(v)
	case uint16:
		s.WriteUint16(v)
	case uint32:
		s.WriteUint32(v)
	case uint64:
		s.WriteUint64(v)
	case int8:
		s.WriteInt8(v)
	case int16:
		s.WriteInt16(v)
	case int32:
		s.WriteInt32(v)
	case int64:
		s.WriteInt64(v)
	case string:
		s.WriteString(v)
	default:
		// Use io.ErrUnexpectedEOF as a placeholder error for unsupported types
		s.err = io.ErrUnexpectedEOF
	}
}

// WritePropertyWithDefault writes a property only if value differs from defaultValue.
// Uses reflect.DeepEqual to compare values.
func (s *BinarySerializer) WritePropertyWithDefault(fieldID uint16, tag string, value, defaultValue interface{}) {
	if s.err != nil {
		return
	}
	// Skip property if value equals default
	if deepEqual(value, defaultValue) {
		return
	}
	s.WriteProperty(fieldID, tag, value)
}

// deepEqual checks if two values are equal. This is a simplified version
// that handles the types we support in WriteProperty.
func deepEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Use simple equality for basic types
	return a == b
}

// WriteObject writes a nested object. The callback fn writes the object's properties.
// Automatically handles OnPropertyBegin, OnObjectBegin, fn(), and OnObjectEnd.
func (s *BinarySerializer) WriteObject(fieldID uint16, tag string, fn func()) {
	if s.err != nil {
		return
	}
	s.OnPropertyBegin(fieldID, tag)
	if s.err != nil {
		return
	}
	s.OnObjectBegin()
	if s.err != nil {
		return
	}
	fn()
	if s.err != nil {
		return
	}
	s.OnObjectEnd()
}

// WriteList writes a list of items. The callback fn is called for each item index.
// Automatically handles OnPropertyBegin, OnListBegin with count, and fn(i) for each item.
func (s *BinarySerializer) WriteList(fieldID uint16, tag string, count uint64, fn func(idx uint64)) {
	if s.err != nil {
		return
	}
	s.OnPropertyBegin(fieldID, tag)
	if s.err != nil {
		return
	}
	s.OnListBegin(count)
	if s.err != nil {
		return
	}
	for i := uint64(0); i < count; i++ {
		fn(i)
		if s.err != nil {
			return
		}
	}
}

// WriteObjectList writes a list of objects. Each item is wrapped in object begin/end.
func (s *BinarySerializer) WriteObjectList(fieldID uint16, tag string, count uint64, fn func(idx uint64)) {
	s.WriteList(fieldID, tag, count, func(idx uint64) {
		if s.err != nil {
			return
		}
		s.OnObjectBegin()
		if s.err != nil {
			return
		}
		fn(idx)
		if s.err != nil {
			return
		}
		s.OnObjectEnd()
	})
}
