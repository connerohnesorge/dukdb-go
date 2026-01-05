// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements comprehensive type conversion
// between DuckDB binary format and Go values for all 46+ supported types.
package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// Type conversion error definitions.
var (
	// ErrInvalidTypeID indicates an unknown or invalid type ID.
	ErrInvalidTypeID = errors.New("invalid type ID")

	// ErrInsufficientData indicates not enough bytes for the type.
	ErrInsufficientData = errors.New("insufficient data for type")

	// ErrInvalidValue indicates the value cannot be encoded to the target type.
	ErrInvalidValue = errors.New("invalid value for type")

	// ErrUnsupportedType indicates the type is not supported for conversion.
	ErrUnsupportedType = errors.New("unsupported type for conversion")

	// ErrInvalidDecimalPrecision indicates invalid decimal width/scale.
	ErrInvalidDecimalPrecision = errors.New("invalid decimal precision")
)

// Time-related constants for DuckDB format.
const (
	// MicrosecondsPerSecond is the number of microseconds in a second.
	MicrosecondsPerSecond = 1_000_000

	// MicrosecondsPerMillisecond is the number of microseconds in a millisecond.
	MicrosecondsPerMillisecond = 1_000

	// NanosecondsPerMicrosecond is the number of nanoseconds in a microsecond.
	NanosecondsPerMicrosecond = 1_000

	// NanosecondsPerSecond is the number of nanoseconds in a second.
	NanosecondsPerSecond = 1_000_000_000

	// SecondsPerDay is the number of seconds in a day.
	SecondsPerDay = 86400

	// DaysPerMonth is the average number of days per month (for intervals).
	DaysPerMonth = 30

	// DuckDBEpoch is the Unix timestamp of 1970-01-01 (Unix epoch).
	DuckDBEpoch = 0
)

// Interval represents a DuckDB INTERVAL value with months, days, and microseconds.
type Interval struct {
	Months int32
	Days   int32
	Micros int64
}

// HugeInt represents a 128-bit signed integer (two's complement).
type HugeInt struct {
	Lower uint64 // Lower 64 bits
	Upper int64  // Upper 64 bits (signed for proper sign handling)
}

// UHugeInt represents a 128-bit unsigned integer.
type UHugeInt struct {
	Lower uint64 // Lower 64 bits
	Upper uint64 // Upper 64 bits
}

// UUID represents a 128-bit UUID value.
type UUID [16]byte

// TimeTZ represents a time with timezone offset.
type TimeTZ struct {
	Micros int64  // Microseconds since midnight
	Offset int32  // Timezone offset in seconds (positive = east of UTC)
}

// DecodeValue converts raw bytes to a Go value based on logical type.
// The mods parameter provides type-specific information like DECIMAL precision.
// Returns the decoded value and any error encountered.
func DecodeValue(data []byte, typeID LogicalTypeID, mods *TypeModifiers) (any, error) {
	if len(data) == 0 {
		return nil, ErrInsufficientData
	}

	switch typeID {
	// Special types
	case TypeInvalid:
		return nil, ErrInvalidTypeID
	case TypeSQLNull:
		return nil, nil // NULL value
	case TypeUnknown, TypeAny, TypeUser, TypeTemplate:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, typeID.String())

	// Boolean
	case TypeBoolean:
		return decodeBool(data)

	// Signed integers
	case TypeTinyInt:
		return decodeTinyInt(data)
	case TypeSmallInt:
		return decodeSmallInt(data)
	case TypeInteger:
		return decodeInteger(data)
	case TypeBigInt:
		return decodeBigInt(data)
	case TypeHugeInt:
		return decodeHugeInt(data)

	// Unsigned integers
	case TypeUTinyInt:
		return decodeUTinyInt(data)
	case TypeUSmallInt:
		return decodeUSmallInt(data)
	case TypeUInteger:
		return decodeUInteger(data)
	case TypeUBigInt:
		return decodeUBigInt(data)
	case TypeUHugeInt:
		return decodeUHugeInt(data)

	// Floating point
	case TypeFloat:
		return decodeFloat(data)
	case TypeDouble:
		return decodeDouble(data)

	// Decimal
	case TypeDecimal:
		return decodeDecimal(data, mods)

	// String types
	case TypeVarchar, TypeStringLiteral:
		return decodeVarchar(data)
	case TypeChar:
		return decodeChar(data, mods)
	case TypeBlob:
		return decodeBlob(data)
	case TypeBit:
		return decodeBit(data)

	// Date/Time types
	case TypeDate:
		return decodeDate(data)
	case TypeTime:
		return decodeTime(data)
	case TypeTimeNS:
		return decodeTimeNS(data)
	case TypeTimeTZ:
		return decodeTimeTZ(data)
	case TypeTimestamp:
		return decodeTimestamp(data)
	case TypeTimestampS:
		return decodeTimestampS(data)
	case TypeTimestampMS:
		return decodeTimestampMS(data)
	case TypeTimestampNS:
		return decodeTimestampNS(data)
	case TypeTimestampTZ:
		return decodeTimestampTZ(data)
	case TypeInterval:
		return decodeInterval(data)

	// UUID
	case TypeUUID:
		return decodeUUID(data)

	// Complex types
	case TypeList:
		return decodeList(data, mods)
	case TypeStruct:
		return decodeStruct(data, mods)
	case TypeMap:
		return decodeMap(data, mods)
	case TypeUnion:
		return decodeUnion(data, mods)
	case TypeArray:
		return decodeArray(data, mods)
	case TypeEnum:
		return decodeEnum(data, mods)

	// Internal types
	case TypePointer, TypeValidity:
		return nil, fmt.Errorf("%w: internal type %s", ErrUnsupportedType, typeID.String())

	// Other types
	case TypeIntegerLiteral, TypeBigNum:
		return decodeBigNum(data)
	case TypeTable, TypeAggregateState, TypeLambda, TypeGeometry, TypeVariant:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, typeID.String())

	default:
		return nil, fmt.Errorf("%w: unknown type %d", ErrInvalidTypeID, typeID)
	}
}

// EncodeValue converts a Go value to raw bytes for storage.
// The mods parameter provides type-specific information like DECIMAL precision.
// Returns the encoded bytes and any error encountered.
func EncodeValue(value any, typeID LogicalTypeID, mods *TypeModifiers) ([]byte, error) {
	if value == nil {
		return nil, nil // NULL values have no data
	}

	switch typeID {
	// Special types
	case TypeInvalid:
		return nil, ErrInvalidTypeID
	case TypeSQLNull:
		return nil, nil

	// Boolean
	case TypeBoolean:
		return encodeBool(value)

	// Signed integers
	case TypeTinyInt:
		return encodeTinyInt(value)
	case TypeSmallInt:
		return encodeSmallInt(value)
	case TypeInteger:
		return encodeInteger(value)
	case TypeBigInt:
		return encodeBigInt(value)
	case TypeHugeInt:
		return encodeHugeInt(value)

	// Unsigned integers
	case TypeUTinyInt:
		return encodeUTinyInt(value)
	case TypeUSmallInt:
		return encodeUSmallInt(value)
	case TypeUInteger:
		return encodeUInteger(value)
	case TypeUBigInt:
		return encodeUBigInt(value)
	case TypeUHugeInt:
		return encodeUHugeInt(value)

	// Floating point
	case TypeFloat:
		return encodeFloat(value)
	case TypeDouble:
		return encodeDouble(value)

	// Decimal
	case TypeDecimal:
		return encodeDecimal(value, mods)

	// String types
	case TypeVarchar, TypeStringLiteral:
		return encodeVarchar(value)
	case TypeChar:
		return encodeChar(value, mods)
	case TypeBlob:
		return encodeBlob(value)
	case TypeBit:
		return encodeBit(value)

	// Date/Time types
	case TypeDate:
		return encodeDate(value)
	case TypeTime:
		return encodeTime(value)
	case TypeTimeNS:
		return encodeTimeNS(value)
	case TypeTimeTZ:
		return encodeTimeTZ(value)
	case TypeTimestamp:
		return encodeTimestamp(value)
	case TypeTimestampS:
		return encodeTimestampS(value)
	case TypeTimestampMS:
		return encodeTimestampMS(value)
	case TypeTimestampNS:
		return encodeTimestampNS(value)
	case TypeTimestampTZ:
		return encodeTimestampTZ(value)
	case TypeInterval:
		return encodeInterval(value)

	// UUID
	case TypeUUID:
		return encodeUUID(value)

	// Complex types
	case TypeList:
		return encodeList(value, mods)
	case TypeStruct:
		return encodeStruct(value, mods)
	case TypeMap:
		return encodeMap(value, mods)
	case TypeUnion:
		return encodeUnion(value, mods)
	case TypeArray:
		return encodeArray(value, mods)
	case TypeEnum:
		return encodeEnum(value, mods)

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, typeID.String())
	}
}

// GetValueSize returns the byte size for a value of the given type.
// Returns 0 for variable-size types (VARCHAR, BLOB, LIST, etc.).
// Returns -1 for unsupported types.
func GetValueSize(typeID LogicalTypeID, mods *TypeModifiers) int {
	switch typeID {
	case TypeBoolean, TypeTinyInt, TypeUTinyInt:
		return 1
	case TypeSmallInt, TypeUSmallInt:
		return 2
	case TypeInteger, TypeUInteger, TypeDate, TypeFloat:
		return 4
	case TypeBigInt, TypeUBigInt, TypeDouble, TypeTime, TypeTimeNS,
		TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		return 8
	case TypeHugeInt, TypeUHugeInt, TypeUUID, TypeInterval:
		return 16
	case TypeTimeTZ:
		return 12 // 8 bytes micros + 4 bytes offset
	case TypeDecimal:
		if mods != nil {
			return DecimalStorageSize(mods.Width)
		}
		return 16 // Default to hugeint backing
	case TypeChar:
		if mods != nil && mods.Length > 0 {
			return int(mods.Length)
		}
		return 0 // Variable if no length specified
	case TypeVarchar, TypeBlob, TypeBit, TypeStringLiteral:
		return 0 // Variable size
	case TypeList, TypeStruct, TypeMap, TypeUnion, TypeArray:
		return 0 // Variable size (complex types)
	case TypeEnum:
		if mods != nil && len(mods.EnumValues) > 0 {
			return EnumStorageSize(len(mods.EnumValues))
		}
		return 4 // Default to uint32
	default:
		return -1 // Unsupported
	}
}

// IsFixedSize returns true if the type has a fixed byte size.
func IsFixedSize(typeID LogicalTypeID) bool {
	switch typeID {
	case TypeBoolean, TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt,
		TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt,
		TypeHugeInt, TypeUHugeInt,
		TypeFloat, TypeDouble,
		TypeDate, TypeTime, TypeTimeNS, TypeTimeTZ,
		TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ,
		TypeInterval, TypeUUID:
		return true
	default:
		return false
	}
}

// IsVariableSize returns true for variable-size types (strings, blobs, lists, etc.).
func IsVariableSize(typeID LogicalTypeID) bool {
	switch typeID {
	case TypeVarchar, TypeBlob, TypeBit, TypeStringLiteral,
		TypeList, TypeStruct, TypeMap, TypeUnion, TypeArray:
		return true
	default:
		return false
	}
}

// DecimalStorageSize returns the byte size needed to store a decimal with the given width (precision).
func DecimalStorageSize(width uint8) int {
	switch {
	case width <= 4:
		return 1 // int8
	case width <= 9:
		return 2 // int16
	case width <= 18:
		return 4 // int32
	case width <= 38:
		return 8 // int64
	default:
		return 16 // int128 (hugeint)
	}
}

// EnumStorageSize returns the byte size needed to store an enum with the given number of values.
func EnumStorageSize(valueCount int) int {
	switch {
	case valueCount <= 256:
		return 1 // uint8
	case valueCount <= 65536:
		return 2 // uint16
	default:
		return 4 // uint32
	}
}

// ============================================================================
// Boolean decoding/encoding
// ============================================================================

func decodeBool(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, ErrInsufficientData
	}
	return data[0] != 0, nil
}

func encodeBool(v any) ([]byte, error) {
	switch b := v.(type) {
	case bool:
		if b {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	default:
		return nil, fmt.Errorf("%w: expected bool, got %T", ErrInvalidValue, v)
	}
}

// ============================================================================
// Signed integer decoding/encoding
// ============================================================================

func decodeTinyInt(data []byte) (int8, error) {
	if len(data) < 1 {
		return 0, ErrInsufficientData
	}
	return int8(data[0]), nil
}

func encodeTinyInt(v any) ([]byte, error) {
	var val int8
	switch n := v.(type) {
	case int8:
		val = n
	case int:
		val = int8(n)
	case int16:
		val = int8(n)
	case int32:
		val = int8(n)
	case int64:
		val = int8(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to int8", ErrInvalidValue, v)
	}
	return []byte{byte(val)}, nil
}

func decodeSmallInt(data []byte) (int16, error) {
	if len(data) < 2 {
		return 0, ErrInsufficientData
	}
	return int16(binary.LittleEndian.Uint16(data)), nil
}

func encodeSmallInt(v any) ([]byte, error) {
	var val int16
	switch n := v.(type) {
	case int16:
		val = n
	case int:
		val = int16(n)
	case int8:
		val = int16(n)
	case int32:
		val = int16(n)
	case int64:
		val = int16(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to int16", ErrInvalidValue, v)
	}
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(val))
	return buf, nil
}

func decodeInteger(data []byte) (int32, error) {
	if len(data) < 4 {
		return 0, ErrInsufficientData
	}
	return int32(binary.LittleEndian.Uint32(data)), nil
}

func encodeInteger(v any) ([]byte, error) {
	var val int32
	switch n := v.(type) {
	case int32:
		val = n
	case int:
		val = int32(n)
	case int8:
		val = int32(n)
	case int16:
		val = int32(n)
	case int64:
		val = int32(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to int32", ErrInvalidValue, v)
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(val))
	return buf, nil
}

func decodeBigInt(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

func encodeBigInt(v any) ([]byte, error) {
	var val int64
	switch n := v.(type) {
	case int64:
		val = n
	case int:
		val = int64(n)
	case int8:
		val = int64(n)
	case int16:
		val = int64(n)
	case int32:
		val = int64(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to int64", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(val))
	return buf, nil
}

func decodeHugeInt(data []byte) (HugeInt, error) {
	if len(data) < 16 {
		return HugeInt{}, ErrInsufficientData
	}
	return HugeInt{
		Lower: binary.LittleEndian.Uint64(data[0:8]),
		Upper: int64(binary.LittleEndian.Uint64(data[8:16])),
	}, nil
}

func encodeHugeInt(v any) ([]byte, error) {
	var h HugeInt
	switch val := v.(type) {
	case HugeInt:
		h = val
	case *big.Int:
		h = bigIntToHugeInt(val)
	case int64:
		if val < 0 {
			h = HugeInt{Lower: uint64(val), Upper: -1}
		} else {
			h = HugeInt{Lower: uint64(val), Upper: 0}
		}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to HugeInt", ErrInvalidValue, v)
	}
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], h.Lower)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(h.Upper))
	return buf, nil
}

// ============================================================================
// Unsigned integer decoding/encoding
// ============================================================================

func decodeUTinyInt(data []byte) (uint8, error) {
	if len(data) < 1 {
		return 0, ErrInsufficientData
	}
	return data[0], nil
}

func encodeUTinyInt(v any) ([]byte, error) {
	var val uint8
	switch n := v.(type) {
	case uint8:
		val = n
	case uint:
		val = uint8(n)
	case uint16:
		val = uint8(n)
	case uint32:
		val = uint8(n)
	case uint64:
		val = uint8(n)
	case int:
		val = uint8(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to uint8", ErrInvalidValue, v)
	}
	return []byte{val}, nil
}

func decodeUSmallInt(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, ErrInsufficientData
	}
	return binary.LittleEndian.Uint16(data), nil
}

func encodeUSmallInt(v any) ([]byte, error) {
	var val uint16
	switch n := v.(type) {
	case uint16:
		val = n
	case uint:
		val = uint16(n)
	case uint8:
		val = uint16(n)
	case uint32:
		val = uint16(n)
	case uint64:
		val = uint16(n)
	case int:
		val = uint16(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to uint16", ErrInvalidValue, v)
	}
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, val)
	return buf, nil
}

func decodeUInteger(data []byte) (uint32, error) {
	if len(data) < 4 {
		return 0, ErrInsufficientData
	}
	return binary.LittleEndian.Uint32(data), nil
}

func encodeUInteger(v any) ([]byte, error) {
	var val uint32
	switch n := v.(type) {
	case uint32:
		val = n
	case uint:
		val = uint32(n)
	case uint8:
		val = uint32(n)
	case uint16:
		val = uint32(n)
	case uint64:
		val = uint32(n)
	case int:
		val = uint32(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to uint32", ErrInvalidValue, v)
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, val)
	return buf, nil
}

func decodeUBigInt(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	return binary.LittleEndian.Uint64(data), nil
}

func encodeUBigInt(v any) ([]byte, error) {
	var val uint64
	switch n := v.(type) {
	case uint64:
		val = n
	case uint:
		val = uint64(n)
	case uint8:
		val = uint64(n)
	case uint16:
		val = uint64(n)
	case uint32:
		val = uint64(n)
	case int:
		val = uint64(n)
	case int64:
		val = uint64(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to uint64", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)
	return buf, nil
}

func decodeUHugeInt(data []byte) (UHugeInt, error) {
	if len(data) < 16 {
		return UHugeInt{}, ErrInsufficientData
	}
	return UHugeInt{
		Lower: binary.LittleEndian.Uint64(data[0:8]),
		Upper: binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}

func encodeUHugeInt(v any) ([]byte, error) {
	var h UHugeInt
	switch val := v.(type) {
	case UHugeInt:
		h = val
	case *big.Int:
		h = bigIntToUHugeInt(val)
	case uint64:
		h = UHugeInt{Lower: val, Upper: 0}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to UHugeInt", ErrInvalidValue, v)
	}
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], h.Lower)
	binary.LittleEndian.PutUint64(buf[8:16], h.Upper)
	return buf, nil
}

// ============================================================================
// Floating point decoding/encoding
// ============================================================================

func decodeFloat(data []byte) (float32, error) {
	if len(data) < 4 {
		return 0, ErrInsufficientData
	}
	bits := binary.LittleEndian.Uint32(data)
	return math.Float32frombits(bits), nil
}

func encodeFloat(v any) ([]byte, error) {
	var val float32
	switch n := v.(type) {
	case float32:
		val = n
	case float64:
		val = float32(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to float32", ErrInvalidValue, v)
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, math.Float32bits(val))
	return buf, nil
}

func decodeDouble(data []byte) (float64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	bits := binary.LittleEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

func encodeDouble(v any) ([]byte, error) {
	var val float64
	switch n := v.(type) {
	case float64:
		val = n
	case float32:
		val = float64(n)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to float64", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(val))
	return buf, nil
}

// ============================================================================
// Decimal decoding/encoding
// ============================================================================

// Decimal represents a DuckDB DECIMAL value with precision and scale.
type Decimal struct {
	Value *big.Int // The unscaled integer value
	Width uint8    // Precision (total digits)
	Scale uint8    // Scale (digits after decimal point)
}

func decodeDecimal(data []byte, mods *TypeModifiers) (Decimal, error) {
	var width, scale uint8
	if mods != nil {
		width = mods.Width
		scale = mods.Scale
	} else {
		// Default to max precision if not specified
		width = 38
		scale = 0
	}

	size := DecimalStorageSize(width)
	if len(data) < size {
		return Decimal{}, ErrInsufficientData
	}

	var val *big.Int
	switch size {
	case 1:
		val = big.NewInt(int64(int8(data[0])))
	case 2:
		val = big.NewInt(int64(int16(binary.LittleEndian.Uint16(data))))
	case 4:
		val = big.NewInt(int64(int32(binary.LittleEndian.Uint32(data))))
	case 8:
		val = big.NewInt(int64(binary.LittleEndian.Uint64(data)))
	case 16:
		// 128-bit signed integer (two's complement)
		h, err := decodeHugeInt(data)
		if err != nil {
			return Decimal{}, err
		}
		val = hugeIntToBigInt(h)
	default:
		return Decimal{}, ErrInvalidDecimalPrecision
	}

	return Decimal{Value: val, Width: width, Scale: scale}, nil
}

func encodeDecimal(v any, mods *TypeModifiers) ([]byte, error) {
	var dec Decimal
	switch val := v.(type) {
	case Decimal:
		dec = val
	case *big.Int:
		width := uint8(38)
		if mods != nil {
			width = mods.Width
		}
		dec = Decimal{Value: val, Width: width}
	case int64:
		width := uint8(18)
		if mods != nil {
			width = mods.Width
		}
		dec = Decimal{Value: big.NewInt(val), Width: width}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to Decimal", ErrInvalidValue, v)
	}

	if mods != nil {
		dec.Width = mods.Width
		dec.Scale = mods.Scale
	}

	size := DecimalStorageSize(dec.Width)
	buf := make([]byte, size)

	switch size {
	case 1:
		buf[0] = byte(dec.Value.Int64())
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(dec.Value.Int64()))
	case 4:
		binary.LittleEndian.PutUint32(buf, uint32(dec.Value.Int64()))
	case 8:
		binary.LittleEndian.PutUint64(buf, uint64(dec.Value.Int64()))
	case 16:
		h := bigIntToHugeInt(dec.Value)
		binary.LittleEndian.PutUint64(buf[0:8], h.Lower)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(h.Upper))
	}

	return buf, nil
}

// ============================================================================
// String type decoding/encoding
// ============================================================================

func decodeVarchar(data []byte) (string, error) {
	if len(data) < 4 {
		return "", ErrInsufficientData
	}
	length := binary.LittleEndian.Uint32(data)
	if len(data) < int(4+length) {
		return "", ErrInsufficientData
	}
	return string(data[4 : 4+length]), nil
}

func encodeVarchar(v any) ([]byte, error) {
	var s string
	switch val := v.(type) {
	case string:
		s = val
	case []byte:
		s = string(val)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to string", ErrInvalidValue, v)
	}
	buf := make([]byte, 4+len(s))
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return buf, nil
}

// decodeChar decodes a fixed-length CHAR value, padding with spaces as needed.
func decodeChar(data []byte, mods *TypeModifiers) (string, error) {
	length := uint32(1)
	if mods != nil && mods.Length > 0 {
		length = mods.Length
	}

	if uint32(len(data)) < length {
		return "", ErrInsufficientData
	}

	// Read the fixed-length string (may contain trailing spaces)
	s := string(data[:length])
	return s, nil
}

// encodeChar encodes a string as a fixed-length CHAR, padding with spaces.
func encodeChar(v any, mods *TypeModifiers) ([]byte, error) {
	var s string
	switch val := v.(type) {
	case string:
		s = val
	case []byte:
		s = string(val)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to CHAR", ErrInvalidValue, v)
	}

	length := uint32(1)
	if mods != nil && mods.Length > 0 {
		length = mods.Length
	}

	buf := make([]byte, length)
	// Fill with spaces first
	for i := range buf {
		buf[i] = ' '
	}
	// Copy string (truncate if too long)
	if uint32(len(s)) > length {
		copy(buf, s[:length])
	} else {
		copy(buf, s)
	}
	return buf, nil
}

func decodeBlob(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, ErrInsufficientData
	}
	length := binary.LittleEndian.Uint32(data)
	if len(data) < int(4+length) {
		return nil, ErrInsufficientData
	}
	result := make([]byte, length)
	copy(result, data[4:4+length])
	return result, nil
}

func encodeBlob(v any) ([]byte, error) {
	var b []byte
	switch val := v.(type) {
	case []byte:
		b = val
	case string:
		b = []byte(val)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to BLOB", ErrInvalidValue, v)
	}
	buf := make([]byte, 4+len(b))
	binary.LittleEndian.PutUint32(buf, uint32(len(b)))
	copy(buf[4:], b)
	return buf, nil
}

// BitString represents a variable-length bit string.
type BitString struct {
	Data   []byte // The bit data
	Length uint64 // Number of bits
}

func decodeBit(data []byte) (BitString, error) {
	if len(data) < 8 {
		return BitString{}, ErrInsufficientData
	}
	length := binary.LittleEndian.Uint64(data)
	byteLen := (length + 7) / 8
	if len(data) < int(8+byteLen) {
		return BitString{}, ErrInsufficientData
	}
	bits := make([]byte, byteLen)
	copy(bits, data[8:8+byteLen])
	return BitString{Data: bits, Length: length}, nil
}

func encodeBit(v any) ([]byte, error) {
	var bs BitString
	switch val := v.(type) {
	case BitString:
		bs = val
	case []byte:
		bs = BitString{Data: val, Length: uint64(len(val) * 8)}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to BIT", ErrInvalidValue, v)
	}
	byteLen := (bs.Length + 7) / 8
	buf := make([]byte, 8+byteLen)
	binary.LittleEndian.PutUint64(buf, bs.Length)
	copy(buf[8:], bs.Data)
	return buf, nil
}

// ============================================================================
// Date/Time decoding/encoding
// ============================================================================

// Reference date for DuckDB: 1970-01-01
var duckdbEpoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func decodeDate(data []byte) (time.Time, error) {
	if len(data) < 4 {
		return time.Time{}, ErrInsufficientData
	}
	days := int32(binary.LittleEndian.Uint32(data))
	return duckdbEpoch.AddDate(0, 0, int(days)), nil
}

func encodeDate(v any) ([]byte, error) {
	var t time.Time
	switch val := v.(type) {
	case time.Time:
		t = val
	case int32:
		// Raw days since epoch
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(val))
		return buf, nil
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to DATE", ErrInvalidValue, v)
	}
	days := int32(t.Sub(duckdbEpoch).Hours() / 24)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(days))
	return buf, nil
}

func decodeTime(data []byte) (time.Duration, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	micros := int64(binary.LittleEndian.Uint64(data))
	return time.Duration(micros) * time.Microsecond, nil
}

func encodeTime(v any) ([]byte, error) {
	var micros int64
	switch val := v.(type) {
	case time.Duration:
		micros = val.Microseconds()
	case int64:
		micros = val
	case time.Time:
		// Extract time of day in microseconds
		micros = int64(val.Hour())*3600*MicrosecondsPerSecond +
			int64(val.Minute())*60*MicrosecondsPerSecond +
			int64(val.Second())*MicrosecondsPerSecond +
			int64(val.Nanosecond())/NanosecondsPerMicrosecond
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIME", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(micros))
	return buf, nil
}

// TimeNS represents a time with nanosecond precision.
type TimeNS struct {
	Nanos int64 // Nanoseconds since midnight
}

func decodeTimeNS(data []byte) (TimeNS, error) {
	if len(data) < 8 {
		return TimeNS{}, ErrInsufficientData
	}
	nanos := int64(binary.LittleEndian.Uint64(data))
	return TimeNS{Nanos: nanos}, nil
}

func encodeTimeNS(v any) ([]byte, error) {
	var nanos int64
	switch val := v.(type) {
	case TimeNS:
		nanos = val.Nanos
	case time.Duration:
		nanos = val.Nanoseconds()
	case int64:
		nanos = val
	case time.Time:
		// Extract time of day in nanoseconds
		nanos = int64(val.Hour())*3600*NanosecondsPerSecond +
			int64(val.Minute())*60*NanosecondsPerSecond +
			int64(val.Second())*NanosecondsPerSecond +
			int64(val.Nanosecond())
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIME_NS", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(nanos))
	return buf, nil
}

func decodeTimeTZ(data []byte) (TimeTZ, error) {
	if len(data) < 12 {
		return TimeTZ{}, ErrInsufficientData
	}
	micros := int64(binary.LittleEndian.Uint64(data[0:8]))
	offset := int32(binary.LittleEndian.Uint32(data[8:12]))
	return TimeTZ{Micros: micros, Offset: offset}, nil
}

func encodeTimeTZ(v any) ([]byte, error) {
	var tz TimeTZ
	switch val := v.(type) {
	case TimeTZ:
		tz = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIME_TZ", ErrInvalidValue, v)
	}
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(tz.Micros))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(tz.Offset))
	return buf, nil
}

func decodeTimestamp(data []byte) (time.Time, error) {
	if len(data) < 8 {
		return time.Time{}, ErrInsufficientData
	}
	micros := int64(binary.LittleEndian.Uint64(data))
	return time.UnixMicro(micros).UTC(), nil
}

func encodeTimestamp(v any) ([]byte, error) {
	var micros int64
	switch val := v.(type) {
	case time.Time:
		micros = val.UnixMicro()
	case int64:
		micros = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIMESTAMP", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(micros))
	return buf, nil
}

func decodeTimestampS(data []byte) (time.Time, error) {
	if len(data) < 8 {
		return time.Time{}, ErrInsufficientData
	}
	secs := int64(binary.LittleEndian.Uint64(data))
	return time.Unix(secs, 0).UTC(), nil
}

func encodeTimestampS(v any) ([]byte, error) {
	var secs int64
	switch val := v.(type) {
	case time.Time:
		secs = val.Unix()
	case int64:
		secs = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIMESTAMP_S", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(secs))
	return buf, nil
}

func decodeTimestampMS(data []byte) (time.Time, error) {
	if len(data) < 8 {
		return time.Time{}, ErrInsufficientData
	}
	millis := int64(binary.LittleEndian.Uint64(data))
	return time.UnixMilli(millis).UTC(), nil
}

func encodeTimestampMS(v any) ([]byte, error) {
	var millis int64
	switch val := v.(type) {
	case time.Time:
		millis = val.UnixMilli()
	case int64:
		millis = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIMESTAMP_MS", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(millis))
	return buf, nil
}

func decodeTimestampNS(data []byte) (time.Time, error) {
	if len(data) < 8 {
		return time.Time{}, ErrInsufficientData
	}
	nanos := int64(binary.LittleEndian.Uint64(data))
	secs := nanos / NanosecondsPerSecond
	nsec := nanos % NanosecondsPerSecond
	return time.Unix(secs, nsec).UTC(), nil
}

func encodeTimestampNS(v any) ([]byte, error) {
	var nanos int64
	switch val := v.(type) {
	case time.Time:
		nanos = val.UnixNano()
	case int64:
		nanos = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to TIMESTAMP_NS", ErrInvalidValue, v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(nanos))
	return buf, nil
}

func decodeTimestampTZ(data []byte) (time.Time, error) {
	// TIMESTAMP_TZ is stored as microseconds since epoch (same as TIMESTAMP)
	// The timezone is implied by the session/context, not stored in the value
	return decodeTimestamp(data)
}

func encodeTimestampTZ(v any) ([]byte, error) {
	return encodeTimestamp(v)
}

func decodeInterval(data []byte) (Interval, error) {
	if len(data) < 16 {
		return Interval{}, ErrInsufficientData
	}
	return Interval{
		Months: int32(binary.LittleEndian.Uint32(data[0:4])),
		Days:   int32(binary.LittleEndian.Uint32(data[4:8])),
		Micros: int64(binary.LittleEndian.Uint64(data[8:16])),
	}, nil
}

func encodeInterval(v any) ([]byte, error) {
	var iv Interval
	switch val := v.(type) {
	case Interval:
		iv = val
	case time.Duration:
		// Convert duration to interval (all in micros)
		iv = Interval{Micros: val.Microseconds()}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to INTERVAL", ErrInvalidValue, v)
	}
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(iv.Months))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(iv.Days))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(iv.Micros))
	return buf, nil
}

// ============================================================================
// UUID decoding/encoding
// ============================================================================

func decodeUUID(data []byte) (UUID, error) {
	if len(data) < 16 {
		return UUID{}, ErrInsufficientData
	}
	var uuid UUID
	copy(uuid[:], data[:16])
	return uuid, nil
}

func encodeUUID(v any) ([]byte, error) {
	switch val := v.(type) {
	case UUID:
		return val[:], nil
	case [16]byte:
		return val[:], nil
	case []byte:
		if len(val) != 16 {
			return nil, fmt.Errorf("%w: UUID must be 16 bytes, got %d", ErrInvalidValue, len(val))
		}
		result := make([]byte, 16)
		copy(result, val)
		return result, nil
	case string:
		return parseUUIDString(val)
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to UUID", ErrInvalidValue, v)
	}
}

// parseUUIDString parses a UUID string in standard format (8-4-4-4-12).
func parseUUIDString(s string) ([]byte, error) {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return nil, fmt.Errorf("%w: invalid UUID string length", ErrInvalidValue)
	}
	result := make([]byte, 16)
	for i := 0; i < 16; i++ {
		var b byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid UUID hex digit", ErrInvalidValue)
		}
		result[i] = b
	}
	return result, nil
}

// String returns the UUID in standard format (8-4-4-4-12).
func (u UUID) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// ============================================================================
// Complex type decoding/encoding (LIST, STRUCT, MAP, UNION, ARRAY, ENUM)
// ============================================================================

// ListValue represents a decoded LIST value.
type ListValue struct {
	Elements []any
	TypeID   LogicalTypeID // Child type
}

func decodeList(data []byte, mods *TypeModifiers) (ListValue, error) {
	if len(data) < 4 {
		return ListValue{}, ErrInsufficientData
	}

	length := binary.LittleEndian.Uint32(data)
	offset := 4

	var childType LogicalTypeID
	var childMods *TypeModifiers
	if mods != nil {
		childType = mods.ChildTypeID
		childMods = mods.ChildType
	}

	elements := make([]any, length)
	for i := uint32(0); i < length; i++ {
		// Check for NULL
		if offset >= len(data) {
			return ListValue{}, ErrInsufficientData
		}
		isNull := data[offset] != 0
		offset++

		if isNull {
			elements[i] = nil
			continue
		}

		// Decode child value
		remaining := data[offset:]
		val, err := DecodeValue(remaining, childType, childMods)
		if err != nil {
			return ListValue{}, fmt.Errorf("list element %d: %w", i, err)
		}
		elements[i] = val

		// Advance offset based on value size
		size := GetValueSize(childType, childMods)
		if size == 0 {
			// Variable size - need to calculate
			size = getEncodedSize(remaining, childType, childMods)
		}
		offset += size
	}

	return ListValue{Elements: elements, TypeID: childType}, nil
}

func encodeList(v any, mods *TypeModifiers) ([]byte, error) {
	var list ListValue
	switch val := v.(type) {
	case ListValue:
		list = val
	case []any:
		childType := TypeVarchar // Default
		if mods != nil {
			childType = mods.ChildTypeID
		}
		list = ListValue{Elements: val, TypeID: childType}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to LIST", ErrInvalidValue, v)
	}

	var childMods *TypeModifiers
	if mods != nil {
		childMods = mods.ChildType
	}

	var buf bytes.Buffer
	// Write length
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(list.Elements)))
	buf.Write(lenBuf)

	// Write elements
	for _, elem := range list.Elements {
		if elem == nil {
			buf.WriteByte(1) // isNull = true
			continue
		}
		buf.WriteByte(0) // isNull = false
		encoded, err := EncodeValue(elem, list.TypeID, childMods)
		if err != nil {
			return nil, err
		}
		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

// StructValue represents a decoded STRUCT value.
type StructValue struct {
	Fields map[string]any
}

func decodeStruct(data []byte, mods *TypeModifiers) (StructValue, error) {
	if mods == nil || len(mods.StructFields) == 0 {
		return StructValue{Fields: make(map[string]any)}, nil
	}

	fields := make(map[string]any, len(mods.StructFields))
	offset := 0

	for _, field := range mods.StructFields {
		if offset >= len(data) {
			return StructValue{}, ErrInsufficientData
		}

		// Check for NULL
		isNull := data[offset] != 0
		offset++

		if isNull {
			fields[field.Name] = nil
			continue
		}

		// Decode field value
		remaining := data[offset:]
		val, err := DecodeValue(remaining, field.Type, field.TypeModifiers)
		if err != nil {
			return StructValue{}, fmt.Errorf("struct field %s: %w", field.Name, err)
		}
		fields[field.Name] = val

		// Advance offset
		size := GetValueSize(field.Type, field.TypeModifiers)
		if size == 0 {
			size = getEncodedSize(remaining, field.Type, field.TypeModifiers)
		}
		offset += size
	}

	return StructValue{Fields: fields}, nil
}

func encodeStruct(v any, mods *TypeModifiers) ([]byte, error) {
	var sv StructValue
	switch val := v.(type) {
	case StructValue:
		sv = val
	case map[string]any:
		sv = StructValue{Fields: val}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to STRUCT", ErrInvalidValue, v)
	}

	if mods == nil || len(mods.StructFields) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	for _, field := range mods.StructFields {
		val, ok := sv.Fields[field.Name]
		if !ok || val == nil {
			buf.WriteByte(1) // isNull = true
			continue
		}
		buf.WriteByte(0) // isNull = false
		encoded, err := EncodeValue(val, field.Type, field.TypeModifiers)
		if err != nil {
			return nil, err
		}
		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

// MapValue represents a decoded MAP value.
// Internally, a MAP is a LIST of STRUCT{key, value}.
type MapValue struct {
	Entries []MapEntry
}

// MapEntry represents a single key-value pair in a MAP.
type MapEntry struct {
	Key   any
	Value any
}

func decodeMap(data []byte, mods *TypeModifiers) (MapValue, error) {
	if len(data) < 4 {
		return MapValue{}, ErrInsufficientData
	}

	length := binary.LittleEndian.Uint32(data)
	offset := 4

	var keyType, valueType LogicalTypeID
	var keyMods, valueMods *TypeModifiers
	if mods != nil {
		keyType = mods.KeyTypeID
		keyMods = mods.KeyType
		valueType = mods.ValueTypeID
		valueMods = mods.ValueType
	}

	entries := make([]MapEntry, length)
	for i := uint32(0); i < length; i++ {
		if offset >= len(data) {
			return MapValue{}, ErrInsufficientData
		}

		// Decode key
		remaining := data[offset:]
		key, err := DecodeValue(remaining, keyType, keyMods)
		if err != nil {
			return MapValue{}, fmt.Errorf("map key %d: %w", i, err)
		}
		size := GetValueSize(keyType, keyMods)
		if size == 0 {
			size = getEncodedSize(remaining, keyType, keyMods)
		}
		offset += size

		// Check value NULL
		if offset >= len(data) {
			return MapValue{}, ErrInsufficientData
		}
		isValueNull := data[offset] != 0
		offset++

		var value any
		if !isValueNull {
			remaining = data[offset:]
			value, err = DecodeValue(remaining, valueType, valueMods)
			if err != nil {
				return MapValue{}, fmt.Errorf("map value %d: %w", i, err)
			}
			size = GetValueSize(valueType, valueMods)
			if size == 0 {
				size = getEncodedSize(remaining, valueType, valueMods)
			}
			offset += size
		}

		entries[i] = MapEntry{Key: key, Value: value}
	}

	return MapValue{Entries: entries}, nil
}

func encodeMap(v any, mods *TypeModifiers) ([]byte, error) {
	var mv MapValue
	switch val := v.(type) {
	case MapValue:
		mv = val
	case map[any]any:
		entries := make([]MapEntry, 0, len(val))
		for k, v := range val {
			entries = append(entries, MapEntry{Key: k, Value: v})
		}
		mv = MapValue{Entries: entries}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to MAP", ErrInvalidValue, v)
	}

	var keyType, valueType LogicalTypeID
	var keyMods, valueMods *TypeModifiers
	if mods != nil {
		keyType = mods.KeyTypeID
		keyMods = mods.KeyType
		valueType = mods.ValueTypeID
		valueMods = mods.ValueType
	}

	var buf bytes.Buffer
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(mv.Entries)))
	buf.Write(lenBuf)

	for _, entry := range mv.Entries {
		// Encode key
		keyBytes, err := EncodeValue(entry.Key, keyType, keyMods)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)

		// Encode value
		if entry.Value == nil {
			buf.WriteByte(1) // isNull = true
		} else {
			buf.WriteByte(0) // isNull = false
			valBytes, err := EncodeValue(entry.Value, valueType, valueMods)
			if err != nil {
				return nil, err
			}
			buf.Write(valBytes)
		}
	}

	return buf.Bytes(), nil
}

// UnionValue represents a decoded UNION value.
type UnionValue struct {
	Tag   uint8 // Index of the active member
	Value any   // The actual value
}

func decodeUnion(data []byte, mods *TypeModifiers) (UnionValue, error) {
	if len(data) < 1 {
		return UnionValue{}, ErrInsufficientData
	}

	tag := data[0]
	if mods == nil || int(tag) >= len(mods.UnionMembers) {
		return UnionValue{Tag: tag}, nil
	}

	member := mods.UnionMembers[tag]
	remaining := data[1:]
	val, err := DecodeValue(remaining, member.Type, member.TypeModifiers)
	if err != nil {
		return UnionValue{}, fmt.Errorf("union member %d: %w", tag, err)
	}

	return UnionValue{Tag: tag, Value: val}, nil
}

func encodeUnion(v any, mods *TypeModifiers) ([]byte, error) {
	var uv UnionValue
	switch val := v.(type) {
	case UnionValue:
		uv = val
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to UNION", ErrInvalidValue, v)
	}

	if mods == nil || int(uv.Tag) >= len(mods.UnionMembers) {
		return []byte{uv.Tag}, nil
	}

	member := mods.UnionMembers[uv.Tag]
	valBytes, err := EncodeValue(uv.Value, member.Type, member.TypeModifiers)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 1+len(valBytes))
	result[0] = uv.Tag
	copy(result[1:], valBytes)
	return result, nil
}

// ArrayValue represents a decoded fixed-size ARRAY value.
type ArrayValue struct {
	Elements []any
	Size     uint32        // Fixed size of the array
	TypeID   LogicalTypeID // Child type
}

func decodeArray(data []byte, mods *TypeModifiers) (ArrayValue, error) {
	size := uint32(0)
	if mods != nil {
		size = mods.Length
	}
	if size == 0 {
		return ArrayValue{}, nil
	}

	var childType LogicalTypeID
	var childMods *TypeModifiers
	if mods != nil {
		childType = mods.ChildTypeID
		childMods = mods.ChildType
	}

	elements := make([]any, size)
	offset := 0
	for i := uint32(0); i < size; i++ {
		if offset >= len(data) {
			return ArrayValue{}, ErrInsufficientData
		}

		// Check for NULL
		isNull := data[offset] != 0
		offset++

		if isNull {
			elements[i] = nil
			continue
		}

		remaining := data[offset:]
		val, err := DecodeValue(remaining, childType, childMods)
		if err != nil {
			return ArrayValue{}, fmt.Errorf("array element %d: %w", i, err)
		}
		elements[i] = val

		valSize := GetValueSize(childType, childMods)
		if valSize == 0 {
			valSize = getEncodedSize(remaining, childType, childMods)
		}
		offset += valSize
	}

	return ArrayValue{Elements: elements, Size: size, TypeID: childType}, nil
}

func encodeArray(v any, mods *TypeModifiers) ([]byte, error) {
	var av ArrayValue
	switch val := v.(type) {
	case ArrayValue:
		av = val
	case []any:
		size := uint32(len(val))
		childType := TypeVarchar
		if mods != nil {
			if mods.Length > 0 {
				size = mods.Length
			}
			childType = mods.ChildTypeID
		}
		av = ArrayValue{Elements: val, Size: size, TypeID: childType}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to ARRAY", ErrInvalidValue, v)
	}

	var childMods *TypeModifiers
	if mods != nil {
		childMods = mods.ChildType
	}

	var buf bytes.Buffer
	for i := uint32(0); i < av.Size; i++ {
		var elem any
		if int(i) < len(av.Elements) {
			elem = av.Elements[i]
		}

		if elem == nil {
			buf.WriteByte(1) // isNull = true
			continue
		}
		buf.WriteByte(0) // isNull = false
		encoded, err := EncodeValue(elem, av.TypeID, childMods)
		if err != nil {
			return nil, err
		}
		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

// EnumValue represents a decoded ENUM value.
type EnumValue struct {
	Index uint32   // Index into the enum values
	Value string   // The string value (if known)
}

func decodeEnum(data []byte, mods *TypeModifiers) (EnumValue, error) {
	valueCount := 0
	if mods != nil {
		valueCount = len(mods.EnumValues)
	}

	size := EnumStorageSize(valueCount)
	if len(data) < size {
		return EnumValue{}, ErrInsufficientData
	}

	var index uint32
	switch size {
	case 1:
		index = uint32(data[0])
	case 2:
		index = uint32(binary.LittleEndian.Uint16(data))
	case 4:
		index = binary.LittleEndian.Uint32(data)
	}

	var value string
	if mods != nil && int(index) < len(mods.EnumValues) {
		value = mods.EnumValues[index]
	}

	return EnumValue{Index: index, Value: value}, nil
}

func encodeEnum(v any, mods *TypeModifiers) ([]byte, error) {
	var index uint32
	switch val := v.(type) {
	case EnumValue:
		index = val.Index
	case uint32:
		index = val
	case int:
		index = uint32(val)
	case string:
		// Find index from value
		if mods != nil {
			for i, ev := range mods.EnumValues {
				if ev == val {
					index = uint32(i)
					break
				}
			}
		}
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to ENUM", ErrInvalidValue, v)
	}

	valueCount := 0
	if mods != nil {
		valueCount = len(mods.EnumValues)
	}

	size := EnumStorageSize(valueCount)
	buf := make([]byte, size)
	switch size {
	case 1:
		buf[0] = byte(index)
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(index))
	case 4:
		binary.LittleEndian.PutUint32(buf, index)
	}

	return buf, nil
}

// ============================================================================
// Big number support
// ============================================================================

func decodeBigNum(data []byte) (*big.Int, error) {
	if len(data) < 4 {
		return nil, ErrInsufficientData
	}
	length := binary.LittleEndian.Uint32(data)
	if len(data) < int(4+length) {
		return nil, ErrInsufficientData
	}
	// BigNum is stored as big-endian bytes
	result := new(big.Int)
	result.SetBytes(data[4 : 4+length])
	return result, nil
}

// ============================================================================
// Helper functions
// ============================================================================

// getEncodedSize returns the encoded size of a variable-size value.
func getEncodedSize(data []byte, typeID LogicalTypeID, mods *TypeModifiers) int {
	switch typeID {
	case TypeVarchar, TypeBlob, TypeStringLiteral:
		if len(data) < 4 {
			return 0
		}
		length := binary.LittleEndian.Uint32(data)
		return 4 + int(length)
	case TypeBit:
		if len(data) < 8 {
			return 0
		}
		length := binary.LittleEndian.Uint64(data)
		byteLen := (length + 7) / 8
		return 8 + int(byteLen)
	case TypeChar:
		if mods != nil && mods.Length > 0 {
			return int(mods.Length)
		}
		return 1
	default:
		// For complex types, we'd need to recursively calculate
		return 0
	}
}

// hugeIntToBigInt converts a HugeInt to a *big.Int.
func hugeIntToBigInt(h HugeInt) *big.Int {
	result := new(big.Int)
	upper := new(big.Int).SetInt64(h.Upper)
	upper.Lsh(upper, 64)
	lower := new(big.Int).SetUint64(h.Lower)
	result.Or(upper, lower)
	return result
}

// bigIntToHugeInt converts a *big.Int to a HugeInt.
func bigIntToHugeInt(b *big.Int) HugeInt {
	// Handle negative numbers
	if b.Sign() < 0 {
		// Two's complement for 128-bit
		complement := new(big.Int).Add(b, new(big.Int).Lsh(big.NewInt(1), 128))
		lower := new(big.Int).And(complement, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1)))
		upper := new(big.Int).Rsh(complement, 64)
		return HugeInt{
			Lower: lower.Uint64(),
			Upper: int64(upper.Uint64()),
		}
	}

	lower := new(big.Int).And(b, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1)))
	upper := new(big.Int).Rsh(b, 64)
	return HugeInt{
		Lower: lower.Uint64(),
		Upper: int64(upper.Uint64()),
	}
}

// bigIntToUHugeInt converts a *big.Int to a UHugeInt.
func bigIntToUHugeInt(b *big.Int) UHugeInt {
	lower := new(big.Int).And(b, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1)))
	upper := new(big.Int).Rsh(b, 64)
	return UHugeInt{
		Lower: lower.Uint64(),
		Upper: upper.Uint64(),
	}
}

// ToBigInt converts a HugeInt to a *big.Int.
func (h HugeInt) ToBigInt() *big.Int {
	return hugeIntToBigInt(h)
}

// ToBigInt converts a UHugeInt to a *big.Int.
func (h UHugeInt) ToBigInt() *big.Int {
	result := new(big.Int)
	upper := new(big.Int).SetUint64(h.Upper)
	upper.Lsh(upper, 64)
	lower := new(big.Int).SetUint64(h.Lower)
	result.Or(upper, lower)
	return result
}

// ToFloat64 converts a Decimal to a float64.
// Note: This may lose precision for large decimals.
func (d Decimal) ToFloat64() float64 {
	if d.Value == nil {
		return 0
	}
	f := new(big.Float).SetInt(d.Value)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil))
	f.Quo(f, divisor)
	result, _ := f.Float64()
	return result
}

// ToDuration converts an Interval to a time.Duration.
// Note: Months are approximated as 30 days.
func (i Interval) ToDuration() time.Duration {
	totalDays := int64(i.Months)*DaysPerMonth + int64(i.Days)
	totalMicros := totalDays*SecondsPerDay*MicrosecondsPerSecond + i.Micros
	return time.Duration(totalMicros) * time.Microsecond
}
