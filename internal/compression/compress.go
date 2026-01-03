// Package compression provides interfaces and implementations for DuckDB compression algorithms.
// DuckDB uses various compression algorithms to reduce storage size:
// - RLE (Run-Length Encoding): for repeated values
// - BitPacking: for integers with limited value range
// - FSST (Fast Static Symbol Table): for string compression
// - Chimp: for floating-point time series
// - Zstd: general-purpose compression
//
// Unsupported Compression Algorithms:
//
// ALP (Adaptive Lossless floating-Point) and Patas compression algorithms are NOT supported
// in this pure Go implementation because they require native C++ code and low-level bit
// manipulation that cannot be efficiently replicated in pure Go without cgo.
//
// Alternatives for unsupported algorithms:
// - For floating-point data: Use Chimp compression (optimized for time-series floats)
// - For general-purpose fallback: Use Zstd compression (excellent compression ratio)
//
// DuckDB's native implementation uses ALP for certain float compression scenarios, but
// Chimp provides comparable compression for time-series data and is fully implemented
// in pure Go. For non-time-series floats or when maximum compatibility is needed,
// Zstd is recommended as it provides good compression across all data types.
package compression

import (
	"fmt"
)

// CompressionType represents different compression algorithms used by DuckDB.
type CompressionType uint8

const (
	// CompressionNone indicates no compression (passthrough).
	CompressionNone CompressionType = 0
	// CompressionRLE uses run-length encoding for repeated values.
	CompressionRLE CompressionType = 1
	// CompressionBitPack uses bit-packing for integers with limited range.
	CompressionBitPack CompressionType = 2
	// CompressionFSST uses Fast Static Symbol Table for string compression.
	CompressionFSST CompressionType = 3
	// CompressionChimp uses Chimp algorithm for floating-point time series.
	CompressionChimp CompressionType = 4
	// CompressionZstd uses Zstandard general-purpose compression.
	CompressionZstd CompressionType = 5
	// Note: DuckDB also defines ALP (Adaptive Lossless floating-Point) and Patas compression
	// types, but they are NOT supported in this pure Go implementation as they require
	// native C++ code. Use CompressionChimp for floating-point data or CompressionZstd
	// as a general-purpose fallback.
)

// String returns the name of the compression type.
func (ct CompressionType) String() string {
	switch ct {
	case CompressionNone:
		return "None"
	case CompressionRLE:
		return "RLE"
	case CompressionBitPack:
		return "BitPack"
	case CompressionFSST:
		return "FSST"
	case CompressionChimp:
		return "Chimp"
	case CompressionZstd:
		return "Zstd"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}

// Compressor defines the interface for data compression.
type Compressor interface {
	// Compress compresses the input data and returns the compressed result.
	Compress(data []byte) ([]byte, error)
	// Type returns the compression type identifier.
	Type() CompressionType
}

// Decompressor defines the interface for data decompression.
type Decompressor interface {
	// Decompress decompresses the input data to the specified destination size.
	// destSize is the expected size of the decompressed data.
	Decompress(data []byte, destSize int) ([]byte, error)
	// Type returns the compression type identifier.
	Type() CompressionType
}

// Codec combines compression and decompression capabilities.
type Codec interface {
	Compressor
	Decompressor
}

// NoneCodec is a passthrough codec that performs no compression.
// It's used when compression is disabled or not beneficial.
type NoneCodec struct{}

// NewNoneCodec creates a new passthrough (no compression) codec.
func NewNoneCodec() *NoneCodec {
	return &NoneCodec{}
}

// Compress returns the data unchanged (passthrough).
func (*NoneCodec) Compress(data []byte) ([]byte, error) {
	// Handle nil input
	if data == nil {
		return nil, nil
	}

	// Make a copy to maintain independence from the original slice
	result := make([]byte, len(data))
	copy(result, data)

	return result, nil
}

// Decompress returns the data unchanged (passthrough).
// The destSize parameter is ignored as no decompression is needed.
func (*NoneCodec) Decompress(data []byte, destSize int) ([]byte, error) {
	// Validate that the data size matches the expected destination size
	if len(data) != destSize {
		return nil, fmt.Errorf("invalid data size: got %d, expected %d", len(data), destSize)
	}

	// Handle nil input
	if data == nil {
		return nil, nil
	}

	// Make a copy to maintain independence from the original slice
	result := make([]byte, len(data))
	copy(result, data)

	return result, nil
}

// Type returns CompressionNone.
func (*NoneCodec) Type() CompressionType {
	return CompressionNone
}

// GetCodec returns a codec for the specified compression type.
// Returns an error if the compression type is not supported.
// For RLE codec, a default value size of 8 bytes is used (suitable for int64, float64, timestamps).
// For BitPack codec, a default bit width of 32 is used (suitable for most integer types).
// For custom sizes, create the codec directly using NewRLECodec(valueSize) or NewBitPackCodec(bitWidth).
func GetCodec(compressionType CompressionType) (Codec, error) {
	switch compressionType {
	case CompressionNone:
		return NewNoneCodec(), nil
	case CompressionRLE:
		// Default to 8-byte values (int64, float64, timestamps)
		return NewRLECodec(8), nil
	case CompressionBitPack:
		// Default to 32-bit width (suitable for most integer types)
		return NewBitPackCodec(32), nil
	case CompressionFSST:
		return NewFSSTCodec(), nil
	case CompressionChimp:
		// Default to float64 (8-byte values)
		return NewChimpCodec(true), nil
	case CompressionZstd:
		// Default to level 3 (balanced compression/speed)
		return NewZstdCodec(3), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// LogicalTypeID represents DuckDB's logical type identifiers.
// These match the LogicalTypeId enum in DuckDB's types.hpp.
type LogicalTypeID uint8

// DuckDB LogicalTypeID constants matching types.hpp enum.
// Only the most commonly used types are defined here.
const (
	// Special types
	LogicalTypeInvalid LogicalTypeID = 0
	LogicalTypeSQLNull LogicalTypeID = 1
	LogicalTypeUnknown LogicalTypeID = 2
	LogicalTypeAny     LogicalTypeID = 3

	// Boolean
	LogicalTypeBoolean LogicalTypeID = 10

	// Integer types
	LogicalTypeTinyInt  LogicalTypeID = 11
	LogicalTypeSmallInt LogicalTypeID = 12
	LogicalTypeInteger  LogicalTypeID = 13
	LogicalTypeBigInt   LogicalTypeID = 14

	// Unsigned integer types
	LogicalTypeUTinyInt  LogicalTypeID = 28
	LogicalTypeUSmallInt LogicalTypeID = 29
	LogicalTypeUInteger  LogicalTypeID = 30
	LogicalTypeUBigInt   LogicalTypeID = 31

	// Date/Time types
	LogicalTypeDate         LogicalTypeID = 15
	LogicalTypeTime         LogicalTypeID = 16
	LogicalTypeTimestampSec LogicalTypeID = 17
	LogicalTypeTimestampMS  LogicalTypeID = 18
	LogicalTypeTimestamp    LogicalTypeID = 19 // microseconds
	LogicalTypeTimestampNS  LogicalTypeID = 20
	LogicalTypeTimestampTZ  LogicalTypeID = 32
	LogicalTypeTimeTZ       LogicalTypeID = 34
	LogicalTypeTimeNS       LogicalTypeID = 35

	// Numeric types
	LogicalTypeDecimal LogicalTypeID = 21
	LogicalTypeFloat   LogicalTypeID = 22
	LogicalTypeDouble  LogicalTypeID = 23

	// String types
	LogicalTypeChar    LogicalTypeID = 24
	LogicalTypeVarchar LogicalTypeID = 25

	// Binary types
	LogicalTypeBlob LogicalTypeID = 26

	// Other scalar types
	LogicalTypeInterval LogicalTypeID = 27
	LogicalTypeBit      LogicalTypeID = 36
	LogicalTypeUUID     LogicalTypeID = 54
	LogicalTypeHugeInt  LogicalTypeID = 50
	LogicalTypeUHugeInt LogicalTypeID = 49

	// Complex types
	LogicalTypeStruct LogicalTypeID = 100
	LogicalTypeList   LogicalTypeID = 101
	LogicalTypeMap    LogicalTypeID = 102
	LogicalTypeEnum   LogicalTypeID = 104
	LogicalTypeUnion  LogicalTypeID = 107
	LogicalTypeArray  LogicalTypeID = 108
)

// SelectCompression selects the best compression algorithm for a given logical type.
// This implements DuckDB's compression selection strategy:
// - Integer types: BitPacking (exploits limited value ranges)
// - Floating-point types: Chimp (optimized for time-series data)
// - String types: FSST (Fast Static Symbol Table)
// - Date/Time types: RLE (Run-Length Encoding for sequential/repeated values)
// - Complex types: No compression (delegate to child types)
//
// For data-aware selection (considering actual data patterns), use SelectCompressionWithData.
func SelectCompression(typeID LogicalTypeID) CompressionType {
	switch typeID {
	// Boolean and integer types: BitPacking
	case LogicalTypeBoolean,
		LogicalTypeTinyInt, LogicalTypeSmallInt, LogicalTypeInteger, LogicalTypeBigInt,
		LogicalTypeUTinyInt, LogicalTypeUSmallInt, LogicalTypeUInteger, LogicalTypeUBigInt,
		LogicalTypeHugeInt, LogicalTypeUHugeInt:
		return CompressionBitPack

	// Floating-point types: Chimp (optimized for time-series)
	case LogicalTypeFloat, LogicalTypeDouble, LogicalTypeDecimal:
		return CompressionChimp

	// String and binary types: FSST
	case LogicalTypeVarchar, LogicalTypeChar, LogicalTypeBlob:
		return CompressionFSST

	// Date/Time types: RLE (often sequential or repeated)
	case LogicalTypeDate, LogicalTypeTime, LogicalTypeTimeNS, LogicalTypeTimeTZ,
		LogicalTypeTimestamp, LogicalTypeTimestampSec, LogicalTypeTimestampMS,
		LogicalTypeTimestampNS, LogicalTypeTimestampTZ,
		LogicalTypeInterval:
		return CompressionRLE

	// UUID and BIT: RLE (often repeated patterns)
	case LogicalTypeUUID, LogicalTypeBit:
		return CompressionRLE

	// Complex types: No compression (child types handle their own compression)
	case LogicalTypeStruct, LogicalTypeList, LogicalTypeMap, LogicalTypeUnion,
		LogicalTypeArray, LogicalTypeEnum:
		return CompressionNone

	// Special types and unknown: No compression
	default:
		return CompressionNone
	}
}

// SelectCompressionWithData selects compression based on both type and actual data.
// This is a data-aware selection strategy that can override type-based selection
// when data patterns suggest a better compression algorithm.
//
// Current implementation uses type-based selection. Future enhancements could:
// - Analyze data for run-length patterns to prefer RLE
// - Check value range to decide between BitPacking and RLE
// - Sample data entropy to decide between specialized and general-purpose compression
func SelectCompressionWithData(typeID LogicalTypeID, data []byte) CompressionType {
	// Start with type-based selection
	compression := SelectCompression(typeID)

	// Data-aware optimizations could be added here
	// For example:
	// - If data is highly repetitive, prefer RLE over BitPacking
	// - If data is random/high-entropy, prefer Zstd over specialized codecs
	// - If data is too small, prefer no compression to avoid overhead

	// For now, use the data length as a simple heuristic
	// Very small data blocks may not benefit from compression
	if len(data) < 64 && compression != CompressionNone {
		// Small data blocks: skip specialized compression overhead
		return CompressionNone
	}

	return compression
}
