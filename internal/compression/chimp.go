package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// ChimpCodec implements Chimp compression for floating-point values.
// Chimp is a variant of Gorilla compression optimized for time-series data.
// It uses XOR encoding with leading/trailing zero compression.
//
// Format:
//   - First value: stored directly (8 bytes for float64, 4 bytes for float32)
//   - Subsequent values: XOR with previous value
//   - If XOR == 0: store single '0' bit
//   - If XOR != 0: store '1' bit + leading zeros count + significant bits
//
// This is particularly effective for sensor data, stock prices, and other
// time-series where consecutive values have small differences.
type ChimpCodec struct {
	isDouble bool // true for float64, false for float32
}

// NewChimpCodec creates a new Chimp codec.
// isDouble should be true for float64 values, false for float32 values.
func NewChimpCodec(isDouble bool) *ChimpCodec {
	return &ChimpCodec{
		isDouble: isDouble,
	}
}

// Compress compresses floating-point data using Chimp algorithm.
// Input data must be a multiple of 4 bytes (float32) or 8 bytes (float64).
func (c *ChimpCodec) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	valueSize := 4
	if c.isDouble {
		valueSize = 8
	}

	if len(data)%valueSize != 0 {
		return nil, fmt.Errorf(
			"data length %d is not a multiple of value size %d",
			len(data),
			valueSize,
		)
	}

	numValues := len(data) / valueSize
	if numValues == 0 {
		return []byte{}, nil
	}

	buf := new(bytes.Buffer)
	bw := newBitWriter(buf)

	if c.isDouble {
		return c.compressFloat64(data, numValues, bw, buf)
	}

	return c.compressFloat32(data, numValues, bw, buf)
}

func (c *ChimpCodec) compressFloat64(
	data []byte,
	numValues int,
	bw *bitWriter,
	buf *bytes.Buffer,
) ([]byte, error) {
	// Store first value directly
	firstBits := binary.LittleEndian.Uint64(data[0:8])
	if err := binary.Write(buf, binary.LittleEndian, firstBits); err != nil {
		return nil, fmt.Errorf("failed to write first value: %w", err)
	}

	prevBits := firstBits
	prevLeading := uint8(0)
	prevTrailing := uint8(0)

	// Compress subsequent values
	for i := 1; i < numValues; i++ {
		currentBits := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
		xorBits := currentBits ^ prevBits

		if xorBits == 0 {
			// Value is identical to previous - store 0 bit
			if err := bw.writeBit(0); err != nil {
				return nil, err
			}
		} else {
			// Value differs - store 1 bit + compressed XOR
			if err := bw.writeBit(1); err != nil {
				return nil, err
			}

			leading := uint8(countLeadingZeros64(xorBits))
			trailing := uint8(countTrailingZeros64(xorBits))

			// Check if we can reuse previous leading/trailing
			if leading >= prevLeading && trailing >= prevTrailing {
				// Use previous block size - store 0 bit
				if err := bw.writeBit(0); err != nil {
					return nil, err
				}
				significantBits := 64 - prevLeading - prevTrailing
				xorShifted := xorBits >> prevTrailing
				if err := bw.writeBits(xorShifted, int(significantBits)); err != nil {
					return nil, err
				}
			} else {
				// New block size - store 1 bit + leading (6 bits) + significant bits
				if err := bw.writeBit(1); err != nil {
					return nil, err
				}
				if err := bw.writeBits(uint64(leading), 6); err != nil {
					return nil, err
				}

				significantBits := 64 - leading - trailing
				if significantBits > 64 {
					significantBits = 64
				}
				// Store length of significant bits (6 bits)
				if err := bw.writeBits(uint64(significantBits), 6); err != nil {
					return nil, err
				}

				// Store significant bits
				xorShifted := xorBits >> trailing
				if err := bw.writeBits(xorShifted, int(significantBits)); err != nil {
					return nil, err
				}

				prevLeading = leading
				prevTrailing = trailing
			}
		}

		prevBits = currentBits
	}

	// Flush remaining bits
	if err := bw.flush(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *ChimpCodec) compressFloat32(
	data []byte,
	numValues int,
	bw *bitWriter,
	buf *bytes.Buffer,
) ([]byte, error) {
	// Store first value directly
	firstBits := binary.LittleEndian.Uint32(data[0:4])
	if err := binary.Write(buf, binary.LittleEndian, firstBits); err != nil {
		return nil, fmt.Errorf("failed to write first value: %w", err)
	}

	prevBits := firstBits
	prevLeading := uint8(0)
	prevTrailing := uint8(0)

	// Compress subsequent values
	for i := 1; i < numValues; i++ {
		currentBits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		xorBits := currentBits ^ prevBits

		if xorBits == 0 {
			// Value is identical to previous - store 0 bit
			if err := bw.writeBit(0); err != nil {
				return nil, err
			}
		} else {
			// Value differs - store 1 bit + compressed XOR
			if err := bw.writeBit(1); err != nil {
				return nil, err
			}

			leading := uint8(countLeadingZeros32(xorBits))
			trailing := uint8(countTrailingZeros32(xorBits))

			// Check if we can reuse previous leading/trailing
			if leading >= prevLeading && trailing >= prevTrailing {
				// Use previous block size - store 0 bit
				if err := bw.writeBit(0); err != nil {
					return nil, err
				}
				significantBits := 32 - prevLeading - prevTrailing
				xorShifted := xorBits >> prevTrailing
				if err := bw.writeBits(uint64(xorShifted), int(significantBits)); err != nil {
					return nil, err
				}
			} else {
				// New block size - store 1 bit + leading (5 bits) + significant bits
				if err := bw.writeBit(1); err != nil {
					return nil, err
				}
				if err := bw.writeBits(uint64(leading), 5); err != nil {
					return nil, err
				}

				significantBits := 32 - leading - trailing
				if significantBits > 32 {
					significantBits = 32
				}
				// Store length of significant bits (5 bits)
				if err := bw.writeBits(uint64(significantBits), 5); err != nil {
					return nil, err
				}

				// Store significant bits
				xorShifted := xorBits >> trailing
				if err := bw.writeBits(uint64(xorShifted), int(significantBits)); err != nil {
					return nil, err
				}

				prevLeading = leading
				prevTrailing = trailing
			}
		}

		prevBits = currentBits
	}

	// Flush remaining bits
	if err := bw.flush(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress decompresses Chimp-encoded data.
func (c *ChimpCodec) Decompress(data []byte, destSize int) ([]byte, error) {
	if destSize == 0 {
		return []byte{}, nil
	}

	valueSize := 4
	if c.isDouble {
		valueSize = 8
	}

	if destSize%valueSize != 0 {
		return nil, fmt.Errorf(
			"destination size %d is not a multiple of value size %d",
			destSize,
			valueSize,
		)
	}

	numValues := destSize / valueSize
	if numValues == 0 {
		return []byte{}, nil
	}

	r := bytes.NewReader(data)
	br := newBitReader(r)

	if c.isDouble {
		return c.decompressFloat64(r, br, numValues, destSize)
	}

	return c.decompressFloat32(r, br, numValues, destSize)
}

func (c *ChimpCodec) decompressFloat64(
	r *bytes.Reader,
	br *bitReader,
	numValues int,
	destSize int,
) ([]byte, error) {
	result := make([]byte, destSize)

	// Read first value
	var firstBits uint64
	if err := binary.Read(r, binary.LittleEndian, &firstBits); err != nil {
		return nil, fmt.Errorf("failed to read first value: %w", err)
	}
	binary.LittleEndian.PutUint64(result[0:8], firstBits)

	prevBits := firstBits
	prevLeading := uint8(0)
	prevTrailing := uint8(0)

	// Decompress subsequent values
	for i := 1; i < numValues; i++ {
		controlBit, err := br.readBit()
		if err != nil {
			return nil, fmt.Errorf("failed to read control bit at value %d: %w", i, err)
		}

		var currentBits uint64
		if controlBit == 0 {
			// Value is identical to previous
			currentBits = prevBits
		} else {
			// Read XOR value
			blockControlBit, err := br.readBit()
			if err != nil {
				return nil, fmt.Errorf("failed to read block control bit at value %d: %w", i, err)
			}

			var leading, trailing uint8
			var significantBits int

			if blockControlBit == 0 {
				// Reuse previous block size
				leading = prevLeading
				trailing = prevTrailing
				significantBits = int(64 - leading - trailing)
			} else {
				// New block size
				leadingBits, err := br.readBits(6)
				if err != nil {
					return nil, fmt.Errorf("failed to read leading zeros at value %d: %w", i, err)
				}
				leading = uint8(leadingBits)

				sigBits, err := br.readBits(6)
				if err != nil {
					return nil, fmt.Errorf("failed to read significant bits length at value %d: %w", i, err)
				}
				significantBits = int(sigBits)
				trailing = 64 - leading - uint8(significantBits)

				prevLeading = leading
				prevTrailing = trailing
			}

			// Read significant bits
			xorShifted, err := br.readBits(significantBits)
			if err != nil {
				return nil, fmt.Errorf("failed to read XOR bits at value %d: %w", i, err)
			}

			xorBits := xorShifted << trailing
			currentBits = prevBits ^ xorBits
		}

		binary.LittleEndian.PutUint64(result[i*8:(i+1)*8], currentBits)
		prevBits = currentBits
	}

	return result, nil
}

func (c *ChimpCodec) decompressFloat32(
	r *bytes.Reader,
	br *bitReader,
	numValues int,
	destSize int,
) ([]byte, error) {
	result := make([]byte, destSize)

	// Read first value
	var firstBits uint32
	if err := binary.Read(r, binary.LittleEndian, &firstBits); err != nil {
		return nil, fmt.Errorf("failed to read first value: %w", err)
	}
	binary.LittleEndian.PutUint32(result[0:4], firstBits)

	prevBits := firstBits
	prevLeading := uint8(0)
	prevTrailing := uint8(0)

	// Decompress subsequent values
	for i := 1; i < numValues; i++ {
		controlBit, err := br.readBit()
		if err != nil {
			return nil, fmt.Errorf("failed to read control bit at value %d: %w", i, err)
		}

		var currentBits uint32
		if controlBit == 0 {
			// Value is identical to previous
			currentBits = prevBits
		} else {
			// Read XOR value
			blockControlBit, err := br.readBit()
			if err != nil {
				return nil, fmt.Errorf("failed to read block control bit at value %d: %w", i, err)
			}

			var leading, trailing uint8
			var significantBits int

			if blockControlBit == 0 {
				// Reuse previous block size
				leading = prevLeading
				trailing = prevTrailing
				significantBits = int(32 - leading - trailing)
			} else {
				// New block size
				leadingBits, err := br.readBits(5)
				if err != nil {
					return nil, fmt.Errorf("failed to read leading zeros at value %d: %w", i, err)
				}
				leading = uint8(leadingBits)

				sigBits, err := br.readBits(5)
				if err != nil {
					return nil, fmt.Errorf("failed to read significant bits length at value %d: %w", i, err)
				}
				significantBits = int(sigBits)
				trailing = 32 - leading - uint8(significantBits)

				prevLeading = leading
				prevTrailing = trailing
			}

			// Read significant bits
			xorShifted, err := br.readBits(significantBits)
			if err != nil {
				return nil, fmt.Errorf("failed to read XOR bits at value %d: %w", i, err)
			}

			xorBits := uint32(xorShifted << trailing)
			currentBits = prevBits ^ xorBits
		}

		binary.LittleEndian.PutUint32(result[i*4:(i+1)*4], currentBits)
		prevBits = currentBits
	}

	return result, nil
}

// Type returns CompressionChimp.
func (c *ChimpCodec) Type() CompressionType {
	return CompressionChimp
}

// Bit-level I/O helpers

type bitWriter struct {
	w      io.Writer
	buffer byte
	count  uint
}

func newBitWriter(w io.Writer) *bitWriter {
	return &bitWriter{w: w}
}

func (bw *bitWriter) writeBit(bit uint8) error {
	if bit != 0 {
		bw.buffer |= 1 << (7 - bw.count)
	}
	bw.count++

	if bw.count == 8 {
		if _, err := bw.w.Write([]byte{bw.buffer}); err != nil {
			return err
		}
		bw.buffer = 0
		bw.count = 0
	}

	return nil
}

func (bw *bitWriter) writeBits(value uint64, numBits int) error {
	for i := numBits - 1; i >= 0; i-- {
		bit := uint8((value >> i) & 1)
		if err := bw.writeBit(bit); err != nil {
			return err
		}
	}
	return nil
}

func (bw *bitWriter) flush() error {
	if bw.count > 0 {
		if _, err := bw.w.Write([]byte{bw.buffer}); err != nil {
			return err
		}
		bw.buffer = 0
		bw.count = 0
	}
	return nil
}

type bitReader struct {
	r      io.Reader
	buffer byte
	count  uint
}

func newBitReader(r io.Reader) *bitReader {
	return &bitReader{r: r, count: 8} // Start with count=8 to trigger read on first call
}

func (br *bitReader) readBit() (uint8, error) {
	if br.count == 8 {
		buf := make([]byte, 1)
		if _, err := br.r.Read(buf); err != nil {
			return 0, err
		}
		br.buffer = buf[0]
		br.count = 0
	}

	bit := (br.buffer >> (7 - br.count)) & 1
	br.count++

	return bit, nil
}

func (br *bitReader) readBits(numBits int) (uint64, error) {
	var result uint64
	for i := 0; i < numBits; i++ {
		bit, err := br.readBit()
		if err != nil {
			return 0, err
		}
		result = (result << 1) | uint64(bit)
	}
	return result, nil
}

// Helper functions for counting leading/trailing zeros

func countLeadingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x <= 0x00000000FFFFFFFF {
		n += 32
		x <<= 32
	}
	if x <= 0x0000FFFFFFFFFFFF {
		n += 16
		x <<= 16
	}
	if x <= 0x00FFFFFFFFFFFFFF {
		n += 8
		x <<= 8
	}
	if x <= 0x0FFFFFFFFFFFFFFF {
		n += 4
		x <<= 4
	}
	if x <= 0x3FFFFFFFFFFFFFFF {
		n += 2
		x <<= 2
	}
	if x <= 0x7FFFFFFFFFFFFFFF {
		n++
	}
	return n
}

func countTrailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	return int(math.Log2(float64(x & -x)))
}

func countLeadingZeros32(x uint32) int {
	if x == 0 {
		return 32
	}
	n := 0
	if x <= 0x0000FFFF {
		n += 16
		x <<= 16
	}
	if x <= 0x00FFFFFF {
		n += 8
		x <<= 8
	}
	if x <= 0x0FFFFFFF {
		n += 4
		x <<= 4
	}
	if x <= 0x3FFFFFFF {
		n += 2
		x <<= 2
	}
	if x <= 0x7FFFFFFF {
		n++
	}
	return n
}

func countTrailingZeros32(x uint32) int {
	if x == 0 {
		return 32
	}
	return int(math.Log2(float64(x & -x)))
}
