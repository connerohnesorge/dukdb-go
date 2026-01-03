package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
)

// BitPackCodec implements bit packing compression for integer columns.
// BitPacking stores integers using only the minimum number of bits needed for their range.
// For example, values in the range 0-15 only need 4 bits per value, saving 50% vs 8-bit storage.
//
// Format:
//   [bit_width:uint8][packed_data:bytes]
//
// The bit_width specifies how many bits each value uses (1-64).
// Values are packed sequentially into bytes, potentially spanning byte boundaries.
type BitPackCodec struct {
	bitWidth int // bits per value (1-64)
}

// NewBitPackCodec creates a new bit packing codec with the specified bit width.
// bitWidth must be between 1 and 64 inclusive.
func NewBitPackCodec(bitWidth int) *BitPackCodec {
	if bitWidth < 1 || bitWidth > 64 {
		panic(fmt.Sprintf("bitWidth must be between 1 and 64, got %d", bitWidth))
	}
	return &BitPackCodec{
		bitWidth: bitWidth,
	}
}

// NewBitPackCodecAuto creates a bit packing codec by analyzing the data
// and determining the minimum bit width needed to store all values.
func NewBitPackCodecAuto(data []byte, valueSize int) *BitPackCodec {
	if len(data) == 0 {
		return NewBitPackCodec(1) // default to 1 bit for empty data
	}

	if len(data)%valueSize != 0 {
		panic(fmt.Sprintf("data length %d is not a multiple of value size %d", len(data), valueSize))
	}

	// Find the maximum value to determine required bit width
	var maxVal uint64
	numValues := len(data) / valueSize

	for i := 0; i < numValues; i++ {
		offset := i * valueSize
		var val uint64

		// Read value based on size (little-endian)
		switch valueSize {
		case 1:
			val = uint64(data[offset])
		case 2:
			val = uint64(binary.LittleEndian.Uint16(data[offset : offset+2]))
		case 4:
			val = uint64(binary.LittleEndian.Uint32(data[offset : offset+4]))
		case 8:
			val = binary.LittleEndian.Uint64(data[offset : offset+8])
		default:
			panic(fmt.Sprintf("unsupported value size: %d", valueSize))
		}

		if val > maxVal {
			maxVal = val
		}
	}

	// Calculate minimum bit width needed
	// bits.Len64 returns the minimum number of bits to represent the value
	bitWidth := bits.Len64(maxVal)
	if bitWidth == 0 {
		bitWidth = 1 // At least 1 bit even for all zeros
	}

	return NewBitPackCodec(bitWidth)
}

// Compress compresses the input data using bit packing.
// The input data should be raw integer values in little-endian format.
func (c *BitPackCodec) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		// Just store the bit width for empty data
		return []byte{byte(c.bitWidth)}, nil
	}

	buf := new(bytes.Buffer)

	// Write bit width as header
	if err := buf.WriteByte(byte(c.bitWidth)); err != nil {
		return nil, fmt.Errorf("failed to write bit width: %w", err)
	}

	// Determine value size from bit width
	valueSize := (c.bitWidth + 7) / 8 // round up to nearest byte
	if valueSize > 8 {
		valueSize = 8
	}

	// Validate input size
	if len(data)%valueSize != 0 {
		return nil, fmt.Errorf("data length %d is not a multiple of value size %d", len(data), valueSize)
	}

	numValues := len(data) / valueSize

	// Pack values bit by bit
	var bitBuffer uint64 // buffer for accumulating bits
	var bitsInBuffer int // number of valid bits in buffer

	for i := 0; i < numValues; i++ {
		offset := i * valueSize

		// Read value based on size
		var val uint64
		switch valueSize {
		case 1:
			val = uint64(data[offset])
		case 2:
			val = uint64(binary.LittleEndian.Uint16(data[offset : offset+2]))
		case 3:
			// Handle 3-byte values (e.g., for 17-24 bit widths)
			val = uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16
		case 4:
			val = uint64(binary.LittleEndian.Uint32(data[offset : offset+4]))
		case 5:
			// Handle 5-byte values
			val = uint64(binary.LittleEndian.Uint32(data[offset:offset+4])) | uint64(data[offset+4])<<32
		case 6:
			// Handle 6-byte values
			val = uint64(binary.LittleEndian.Uint32(data[offset:offset+4])) | uint64(binary.LittleEndian.Uint16(data[offset+4:offset+6]))<<32
		case 7:
			// Handle 7-byte values
			val = uint64(binary.LittleEndian.Uint32(data[offset:offset+4])) | uint64(binary.LittleEndian.Uint16(data[offset+4:offset+6]))<<32 | uint64(data[offset+6])<<48
		case 8:
			val = binary.LittleEndian.Uint64(data[offset : offset+8])
		}

		// Mask to only keep the relevant bits
		mask := uint64((1 << c.bitWidth) - 1)
		val &= mask

		// Add value to bit buffer
		bitBuffer |= val << bitsInBuffer
		bitsInBuffer += c.bitWidth

		// Flush complete bytes to output
		for bitsInBuffer >= 8 {
			if err := buf.WriteByte(byte(bitBuffer & 0xFF)); err != nil {
				return nil, fmt.Errorf("failed to write packed byte: %w", err)
			}
			bitBuffer >>= 8
			bitsInBuffer -= 8
		}
	}

	// Flush any remaining bits
	if bitsInBuffer > 0 {
		if err := buf.WriteByte(byte(bitBuffer & 0xFF)); err != nil {
			return nil, fmt.Errorf("failed to write final byte: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// Decompress decompresses bit-packed data to the specified destination size.
func (c *BitPackCodec) Decompress(data []byte, destSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("compressed data is empty")
	}

	// Read bit width from header
	bitWidth := int(data[0])
	if bitWidth < 1 || bitWidth > 64 {
		return nil, fmt.Errorf("invalid bit width in header: %d", bitWidth)
	}

	// Override codec bit width if different (for flexibility)
	if bitWidth != c.bitWidth {
		// Use the width from the data
		bitWidth = int(data[0])
	}

	packedData := data[1:]

	if destSize == 0 {
		return []byte{}, nil
	}

	// Determine value size from bit width
	valueSize := (bitWidth + 7) / 8
	if valueSize > 8 {
		valueSize = 8
	}

	if destSize%valueSize != 0 {
		return nil, fmt.Errorf("destination size %d is not a multiple of value size %d", destSize, valueSize)
	}

	result := make([]byte, destSize)
	numValues := destSize / valueSize

	// Unpack values bit by bit
	var bitBuffer uint64
	var bitsInBuffer int
	packedIdx := 0

	for i := 0; i < numValues; i++ {
		// Ensure we have enough bits in the buffer
		for bitsInBuffer < bitWidth && packedIdx < len(packedData) {
			bitBuffer |= uint64(packedData[packedIdx]) << bitsInBuffer
			bitsInBuffer += 8
			packedIdx++
		}

		if bitsInBuffer < bitWidth {
			return nil, fmt.Errorf("insufficient data: need %d bits but only %d available", bitWidth, bitsInBuffer)
		}

		// Extract value
		mask := uint64((1 << bitWidth) - 1)
		val := bitBuffer & mask
		bitBuffer >>= bitWidth
		bitsInBuffer -= bitWidth

		// Write value to result based on size
		offset := i * valueSize
		switch valueSize {
		case 1:
			result[offset] = byte(val)
		case 2:
			binary.LittleEndian.PutUint16(result[offset:offset+2], uint16(val))
		case 3:
			result[offset] = byte(val)
			result[offset+1] = byte(val >> 8)
			result[offset+2] = byte(val >> 16)
		case 4:
			binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(val))
		case 5:
			binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(val))
			result[offset+4] = byte(val >> 32)
		case 6:
			binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(val))
			binary.LittleEndian.PutUint16(result[offset+4:offset+6], uint16(val>>32))
		case 7:
			binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(val))
			binary.LittleEndian.PutUint16(result[offset+4:offset+6], uint16(val>>32))
			result[offset+6] = byte(val >> 48)
		case 8:
			binary.LittleEndian.PutUint64(result[offset:offset+8], val)
		}
	}

	return result, nil
}

// Type returns CompressionBitPack.
func (c *BitPackCodec) Type() CompressionType {
	return CompressionBitPack
}
