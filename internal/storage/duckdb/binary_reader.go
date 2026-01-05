package duckdb

import (
	"encoding/binary"
	"io"
	"math"
)

// Varint encoding constants.
const (
	varintContinueMask = 0x80 // High bit indicates more bytes follow
	varintValueMask    = 0x7F // Lower 7 bits contain value
	varintShift        = 7    // Bits per byte for varint
)

// BinaryReader provides little-endian binary reading utilities.
// It wraps an io.Reader and tracks any errors encountered during reads.
type BinaryReader struct {
	// r is the underlying reader.
	r io.Reader

	// err stores the first error encountered during reading.
	// Subsequent reads are no-ops if an error has occurred.
	err error

	// bytesRead tracks the total bytes read.
	bytesRead int64
}

// NewBinaryReader creates a new BinaryReader wrapping the given reader.
func NewBinaryReader(r io.Reader) *BinaryReader {
	return &BinaryReader{r: r}
}

// Err returns the first error that occurred during reading.
func (br *BinaryReader) Err() error {
	return br.err
}

// BytesRead returns the total number of bytes read.
func (br *BinaryReader) BytesRead() int64 {
	return br.bytesRead
}

// ReadUint8 reads a single unsigned byte.
func (br *BinaryReader) ReadUint8() uint8 {
	if br.err != nil {
		return 0
	}

	var b [1]byte

	n, err := br.r.Read(b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	return b[0]
}

// ReadInt8 reads a single signed byte.
func (br *BinaryReader) ReadInt8() int8 {
	return int8(br.ReadUint8())
}

// ReadUint16 reads an unsigned 16-bit integer (little-endian).
func (br *BinaryReader) ReadUint16() uint16 {
	if br.err != nil {
		return 0
	}

	var b [2]byte

	n, err := io.ReadFull(br.r, b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	return binary.LittleEndian.Uint16(b[:])
}

// ReadInt16 reads a signed 16-bit integer (little-endian).
func (br *BinaryReader) ReadInt16() int16 {
	return int16(br.ReadUint16())
}

// ReadUint32 reads an unsigned 32-bit integer (little-endian).
func (br *BinaryReader) ReadUint32() uint32 {
	if br.err != nil {
		return 0
	}

	var b [4]byte

	n, err := io.ReadFull(br.r, b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	return binary.LittleEndian.Uint32(b[:])
}

// ReadInt32 reads a signed 32-bit integer (little-endian).
func (br *BinaryReader) ReadInt32() int32 {
	return int32(br.ReadUint32())
}

// ReadUint64 reads an unsigned 64-bit integer (little-endian).
func (br *BinaryReader) ReadUint64() uint64 {
	if br.err != nil {
		return 0
	}

	var b [8]byte

	n, err := io.ReadFull(br.r, b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	return binary.LittleEndian.Uint64(b[:])
}

// ReadInt64 reads a signed 64-bit integer (little-endian).
func (br *BinaryReader) ReadInt64() int64 {
	return int64(br.ReadUint64())
}

// ReadFloat32 reads a 32-bit float (little-endian IEEE 754).
func (br *BinaryReader) ReadFloat32() float32 {
	if br.err != nil {
		return 0
	}

	var b [4]byte

	n, err := io.ReadFull(br.r, b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	bits := binary.LittleEndian.Uint32(b[:])

	return math.Float32frombits(bits)
}

// ReadFloat64 reads a 64-bit float (little-endian IEEE 754).
func (br *BinaryReader) ReadFloat64() float64 {
	if br.err != nil {
		return 0
	}

	var b [8]byte

	n, err := io.ReadFull(br.r, b[:])
	br.bytesRead += int64(n)

	if err != nil {
		br.err = err

		return 0
	}

	bits := binary.LittleEndian.Uint64(b[:])

	return math.Float64frombits(bits)
}

// ReadBool reads a boolean value (1 byte, 0 = false, non-zero = true).
func (br *BinaryReader) ReadBool() bool {
	return br.ReadUint8() != 0
}

// ReadBytes reads exactly n bytes into a new slice.
func (br *BinaryReader) ReadBytes(n int) []byte {
	if br.err != nil {
		return nil
	}

	b := make([]byte, n)

	read, err := io.ReadFull(br.r, b)
	br.bytesRead += int64(read)

	if err != nil {
		br.err = err

		return nil
	}

	return b
}

// ReadString reads a length-prefixed string (uint32 length followed by bytes).
func (br *BinaryReader) ReadString() string {
	if br.err != nil {
		return ""
	}

	length := br.ReadUint32()
	if br.err != nil {
		return ""
	}

	if length == 0 {
		return ""
	}

	b := br.ReadBytes(int(length))

	return string(b)
}

// ReadUvarint reads a variable-length unsigned integer.
func (br *BinaryReader) ReadUvarint() uint64 {
	if br.err != nil {
		return 0
	}

	var result uint64

	var shift uint

	for {
		b := br.ReadUint8()
		if br.err != nil {
			return 0
		}

		result |= uint64(b&varintValueMask) << shift

		if b&varintContinueMask == 0 {
			break
		}

		shift += varintShift
	}

	return result
}

// ReadVarint reads a variable-length signed integer (zigzag encoded).
func (br *BinaryReader) ReadVarint() int64 {
	u := br.ReadUvarint()
	// Zigzag decode: (u >> 1) ^ -(u & 1)
	return int64((u >> 1) ^ -(u & 1))
}

// Skip skips n bytes in the reader.
func (br *BinaryReader) Skip(n int) {
	if br.err != nil {
		return
	}

	// Try to use Seeker if available
	if seeker, ok := br.r.(io.Seeker); ok {
		_, br.err = seeker.Seek(int64(n), io.SeekCurrent)
		if br.err == nil {
			br.bytesRead += int64(n)
		}

		return
	}

	// Otherwise, read and discard
	discarded, err := io.CopyN(io.Discard, br.r, int64(n))
	br.bytesRead += discarded

	if err != nil {
		br.err = err
	}
}

// Remaining returns the number of bytes remaining to be read.
// This only works if the underlying reader implements io.Seeker.
// Returns -1 if remaining cannot be determined.
func (br *BinaryReader) Remaining() int64 {
	if br.err != nil {
		return 0
	}

	seeker, ok := br.r.(io.Seeker)
	if !ok {
		return -1
	}

	// Get current position
	currentPos, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}

	// Get end position
	endPos, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return -1
	}

	// Seek back to current position
	_, err = seeker.Seek(currentPos, io.SeekStart)
	if err != nil {
		br.err = err

		return 0
	}

	return endPos - currentPos
}

// Reset clears the error state, allowing reads to continue.
// Use with caution - this should only be used after handling an error.
func (br *BinaryReader) Reset() {
	br.err = nil
}
