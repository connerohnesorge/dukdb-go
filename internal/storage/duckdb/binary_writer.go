package duckdb

import (
	"encoding/binary"
	"io"
	"math"
)

// BinaryWriter provides little-endian binary writing utilities.
// It wraps an io.Writer and tracks any errors encountered during writes.
type BinaryWriter struct {
	// w is the underlying writer.
	w io.Writer

	// err stores the first error encountered during writing.
	// Subsequent writes are no-ops if an error has occurred.
	err error

	// bytesWritten tracks the total bytes written.
	bytesWritten int64
}

// NewBinaryWriter creates a new BinaryWriter wrapping the given writer.
func NewBinaryWriter(w io.Writer) *BinaryWriter {
	return &BinaryWriter{w: w}
}

// Err returns the first error that occurred during writing.
func (bw *BinaryWriter) Err() error {
	return bw.err
}

// BytesWritten returns the total number of bytes written.
func (bw *BinaryWriter) BytesWritten() int64 {
	return bw.bytesWritten
}

// WriteUint8 writes a single unsigned byte.
func (bw *BinaryWriter) WriteUint8(v uint8) {
	if bw.err != nil {
		return
	}

	b := [1]byte{v}

	n, err := bw.w.Write(b[:])
	bw.bytesWritten += int64(n)

	if err != nil {
		bw.err = err
	}
}

// WriteInt8 writes a single signed byte.
func (bw *BinaryWriter) WriteInt8(v int8) {
	bw.WriteUint8(uint8(v))
}

// WriteUint16 writes an unsigned 16-bit integer (little-endian).
func (bw *BinaryWriter) WriteUint16(v uint16) {
	if bw.err != nil {
		return
	}

	var b [2]byte

	binary.LittleEndian.PutUint16(b[:], v)

	n, err := bw.w.Write(b[:])
	bw.bytesWritten += int64(n)

	if err != nil {
		bw.err = err
	}
}

// WriteInt16 writes a signed 16-bit integer (little-endian).
func (bw *BinaryWriter) WriteInt16(v int16) {
	bw.WriteUint16(uint16(v))
}

// WriteUint32 writes an unsigned 32-bit integer (little-endian).
func (bw *BinaryWriter) WriteUint32(v uint32) {
	if bw.err != nil {
		return
	}

	var b [4]byte

	binary.LittleEndian.PutUint32(b[:], v)

	n, err := bw.w.Write(b[:])
	bw.bytesWritten += int64(n)

	if err != nil {
		bw.err = err
	}
}

// WriteInt32 writes a signed 32-bit integer (little-endian).
func (bw *BinaryWriter) WriteInt32(v int32) {
	bw.WriteUint32(uint32(v))
}

// WriteUint64 writes an unsigned 64-bit integer (little-endian).
func (bw *BinaryWriter) WriteUint64(v uint64) {
	if bw.err != nil {
		return
	}

	var b [8]byte

	binary.LittleEndian.PutUint64(b[:], v)

	n, err := bw.w.Write(b[:])
	bw.bytesWritten += int64(n)

	if err != nil {
		bw.err = err
	}
}

// WriteInt64 writes a signed 64-bit integer (little-endian).
func (bw *BinaryWriter) WriteInt64(v int64) {
	bw.WriteUint64(uint64(v))
}

// WriteFloat32 writes a 32-bit float (little-endian IEEE 754).
func (bw *BinaryWriter) WriteFloat32(v float32) {
	bw.WriteUint32(math.Float32bits(v))
}

// WriteFloat64 writes a 64-bit float (little-endian IEEE 754).
func (bw *BinaryWriter) WriteFloat64(v float64) {
	bw.WriteUint64(math.Float64bits(v))
}

// WriteBool writes a boolean value (1 byte, 0 = false, 1 = true).
func (bw *BinaryWriter) WriteBool(v bool) {
	if v {
		bw.WriteUint8(1)
	} else {
		bw.WriteUint8(0)
	}
}

// WriteTrue writes a true boolean value (1 byte with value 1).
func (bw *BinaryWriter) WriteTrue() {
	bw.WriteUint8(1)
}

// WriteFalse writes a false boolean value (1 byte with value 0).
func (bw *BinaryWriter) WriteFalse() {
	bw.WriteUint8(0)
}

// WriteBytes writes a byte slice.
func (bw *BinaryWriter) WriteBytes(b []byte) {
	if bw.err != nil {
		return
	}

	n, err := bw.w.Write(b)
	bw.bytesWritten += int64(n)

	if err != nil {
		bw.err = err
	}
}

// WriteString writes a length-prefixed string (uint32 length followed by bytes).
func (bw *BinaryWriter) WriteString(s string) {
	bw.WriteUint32(uint32(len(s)))

	if s != "" {
		bw.WriteBytes([]byte(s))
	}
}

// WriteUvarint writes a variable-length unsigned integer.
func (bw *BinaryWriter) WriteUvarint(v uint64) {
	if bw.err != nil {
		return
	}

	var buf [10]byte

	n := binary.PutUvarint(buf[:], v)
	bw.WriteBytes(buf[:n])
}

// WriteVarint writes a variable-length signed integer (zigzag encoded).
func (bw *BinaryWriter) WriteVarint(v int64) {
	// Zigzag encode: (v << 1) ^ (v >> 63)
	u := uint64((v << 1) ^ (v >> 63))
	bw.WriteUvarint(u)
}

// WritePadding writes n zero bytes for alignment/padding.
func (bw *BinaryWriter) WritePadding(n int) {
	if bw.err != nil || n <= 0 {
		return
	}

	zeros := make([]byte, n)
	bw.WriteBytes(zeros)
}

// WritePropertyID writes a property ID for serialization.
// Property IDs are used for forward/backward compatibility.
func (bw *BinaryWriter) WritePropertyID(id uint32) {
	bw.WriteUint32(id)
}

// Reset clears the error state, allowing writes to continue.
// Use with caution - this should only be used after handling an error.
func (bw *BinaryWriter) Reset() {
	bw.err = nil
}
