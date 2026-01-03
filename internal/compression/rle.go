package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// RLECodec implements run-length encoding compression.
// RLE is efficient for data with many repeated values (dates, timestamps, etc.).
//
// Format: For each run of identical values:
//   [run_length:varint][value:bytes]
//
// The run_length is encoded as a varint (variable-length integer).
// The value is stored as raw bytes of the specified valueSize.
type RLECodec struct {
	valueSize int // size of each value in bytes
}

// NewRLECodec creates a new RLE codec for values of the specified size.
// valueSize is the number of bytes per value (e.g., 4 for int32, 8 for int64).
func NewRLECodec(valueSize int) *RLECodec {
	if valueSize <= 0 {
		panic("valueSize must be positive")
	}
	return &RLECodec{
		valueSize: valueSize,
	}
}

// Compress compresses the input data using run-length encoding.
// The input data must be a multiple of valueSize.
func (c *RLECodec) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	if len(data)%c.valueSize != 0 {
		return nil, fmt.Errorf("data length %d is not a multiple of value size %d", len(data), c.valueSize)
	}

	buf := new(bytes.Buffer)
	numValues := len(data) / c.valueSize

	i := 0
	for i < numValues {
		// Find the run of identical values starting at position i
		runStart := i
		currentValue := data[i*c.valueSize : (i+1)*c.valueSize]

		// Count how many consecutive values are identical
		runLength := 1
		i++
		for i < numValues {
			nextValue := data[i*c.valueSize : (i+1)*c.valueSize]
			if !bytesEqual(currentValue, nextValue) {
				break
			}
			runLength++
			i++
		}

		// Write the run: [run_length:varint][value:bytes]
		if err := writeVarint(buf, uint64(runLength)); err != nil {
			return nil, fmt.Errorf("failed to write run length: %w", err)
		}
		if _, err := buf.Write(currentValue); err != nil {
			return nil, fmt.Errorf("failed to write value: %w", err)
		}

		_ = runStart // unused, kept for clarity
	}

	return buf.Bytes(), nil
}

// Decompress decompresses RLE-encoded data to the specified destination size.
func (c *RLECodec) Decompress(data []byte, destSize int) ([]byte, error) {
	if destSize == 0 {
		return []byte{}, nil
	}

	if destSize%c.valueSize != 0 {
		return nil, fmt.Errorf("destination size %d is not a multiple of value size %d", destSize, c.valueSize)
	}

	result := make([]byte, destSize)
	r := bytes.NewReader(data)
	offset := 0
	expectedValues := destSize / c.valueSize
	decodedValues := 0

	for offset < destSize {
		// Read run length
		runLength, err := readVarint(r)
		if err != nil {
			if err == io.EOF && decodedValues == expectedValues {
				// We've read all expected values
				break
			}
			return nil, fmt.Errorf("failed to read run length at offset %d: %w", offset, err)
		}

		if runLength == 0 {
			return nil, fmt.Errorf("invalid run length 0 at offset %d", offset)
		}

		// Read the value
		value := make([]byte, c.valueSize)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("failed to read value at offset %d: %w", offset, err)
		}

		// Expand the run: write the value runLength times
		for j := uint64(0); j < runLength; j++ {
			if offset+c.valueSize > destSize {
				return nil, fmt.Errorf("decompressed data exceeds destination size %d", destSize)
			}
			copy(result[offset:offset+c.valueSize], value)
			offset += c.valueSize
			decodedValues++
		}
	}

	if offset != destSize {
		return nil, fmt.Errorf("decompressed size %d does not match expected size %d", offset, destSize)
	}

	return result, nil
}

// Type returns CompressionRLE.
func (c *RLECodec) Type() CompressionType {
	return CompressionRLE
}

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// writeVarint writes a variable-length integer using standard Go encoding.
func writeVarint(w io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := w.Write(buf[:n])
	return err
}

// readVarint reads a variable-length integer using standard Go encoding.
func readVarint(r io.Reader) (uint64, error) {
	var result uint64
	var shift uint
	for {
		b := make([]byte, 1)
		if _, err := r.Read(b); err != nil {
			return 0, err
		}
		result |= uint64(b[0]&0x7F) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
	return result, nil
}
