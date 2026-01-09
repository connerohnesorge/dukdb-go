// Package duckdb implements DuckDB-compatible storage format utilities.
package duckdb

import (
	"io"
)

const (
	// varIntContinuationMask is the MSB used to indicate more bytes follow in varint encoding
	varIntContinuationMask = 0x80
	// varIntDataMask extracts the data bits (lower 7 bits) from a varint byte
	varIntDataMask = 0x7F
	// varIntBitsPerByte is the number of data bits per byte in varint encoding
	varIntBitsPerByte = 7
	// varIntMaxShift is the maximum shift value for uint64 (10 bytes * 7 bits = 70)
	varIntMaxShift = 70
)

// VarIntEncode encodes an unsigned 64-bit integer using variable-length encoding.
// This matches DuckDB's BinarySerializer::VarIntEncode format (LEB128).
// For values < 128: single byte
// For larger values: each byte stores 7 bits of data, with MSB = 1 if more bytes follow
func VarIntEncode(w io.Writer, value uint64) error {
	buf := make([]byte, 1)
	v := value

	for v >= varIntContinuationMask {
		buf[0] = byte(v) | varIntContinuationMask
		if _, err := w.Write(buf); err != nil {
			return err
		}
		v >>= varIntBitsPerByte
	}
	buf[0] = byte(v)
	_, err := w.Write(buf)

	return err
}

// VarIntDecode decodes a variable-length encoded unsigned integer.
// This matches DuckDB's BinarySerializer::VarIntDecode format (LEB128).
func VarIntDecode(r io.Reader) (uint64, error) {
	var result uint64
	var shift uint
	buf := make([]byte, 1)

	for {
		if _, err := r.Read(buf); err != nil {
			return 0, err
		}
		b := buf[0]

		// Add the lower 7 bits to the result
		result |= uint64(b&varIntDataMask) << shift

		// If MSB is 0, this is the last byte
		if b&varIntContinuationMask == 0 {
			break
		}

		shift += varIntBitsPerByte

		// Protect against overflow (max 10 bytes for uint64)
		if shift >= varIntMaxShift {
			return 0, io.ErrUnexpectedEOF
		}
	}

	return result, nil
}

// ZigZagEncode encodes a signed 64-bit integer using zigzag encoding.
// This maps signed integers to unsigned integers:
// 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
func ZigZagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// ZigZagDecode decodes a zigzag-encoded integer back to signed.
func ZigZagDecode(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}
