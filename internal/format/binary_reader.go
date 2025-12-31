package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
)

// BinaryReader implements property-based binary deserialization for DuckDB format.
//
// BinaryReader loads all properties from a binary stream into memory and provides
// methods to read individual properties by ID. This matches DuckDB's deserialization
// protocol where properties are written with their IDs in ascending order.
//
// Usage:
//
//	r := NewBinaryReader(reader)
//	err := r.Load()
//	var value uint32
//	err = r.ReadProperty(100, &value)
//	err = r.ReadPropertyWithDefault(101, &name, "")
//	values, err := r.ReadList(200)
type BinaryReader struct {
	r          io.Reader
	properties map[uint32][]byte
}

// NewBinaryReader creates a new BinaryReader that reads from the given io.Reader.
//
// The reader must call Load() to read the properties from the stream before calling
// any of the ReadProperty methods.
func NewBinaryReader(r io.Reader) *BinaryReader {
	return &BinaryReader{
		r:          r,
		properties: make(map[uint32][]byte),
	}
}

// Load reads all properties from the binary stream into memory.
//
// The binary format is:
//   - Property count: uint32 (number of properties)
//   - For each property:
//   - Property ID: uint32
//   - Data length: uint64 (byte length of property data)
//   - Data: []byte (serialized property value)
//
// All multi-byte values are read in little-endian byte order.
//
// This method must be called before any ReadProperty calls.
func (r *BinaryReader) Load() error {
	// Read property count
	var count uint32
	if err := binary.Read(r.r, ByteOrder, &count); err != nil {
		return fmt.Errorf(
			"failed to read property count: %w",
			err,
		)
	}

	r.properties = make(map[uint32][]byte, count)

	for i := range count {
		// Read property ID
		var id uint32
		if err := binary.Read(r.r, ByteOrder, &id); err != nil {
			return fmt.Errorf(
				"failed to read property %d ID: %w",
				i,
				err,
			)
		}

		// Read property data length
		var length uint64
		if err := binary.Read(r.r, ByteOrder, &length); err != nil {
			return fmt.Errorf(
				"failed to read property %d length: %w",
				id,
				err,
			)
		}

		// Read property data
		data := make([]byte, length)
		if _, err := io.ReadFull(r.r, data); err != nil {
			return fmt.Errorf(
				"failed to read property %d data: %w",
				id,
				err,
			)
		}

		r.properties[id] = data
	}

	return nil
}

// ReadProperty reads a required property from the loaded properties.
//
// If the property is not found, ErrRequiredProperty is returned.
//
// The dest parameter must be a pointer to one of the supported types:
//   - *uint8: 1 byte
//   - *uint32: 4 bytes (little-endian)
//   - *uint64: 8 bytes (little-endian)
//   - *string: length (uint64) + data bytes
//   - *[]byte: raw bytes (no length prefix)
//
// Returns an error if the property is missing or if the dest type is not supported.
func (r *BinaryReader) ReadProperty(
	id uint32,
	dest interface{},
) error {
	data, ok := r.properties[id]
	if !ok {
		return fmt.Errorf(
			"%w: property %d",
			ErrRequiredProperty,
			id,
		)
	}

	return r.deserializeValue(data, dest)
}

// ReadPropertyWithDefault reads an optional property from the loaded properties.
//
// If the property is not found, the defaultValue is assigned to dest instead.
// This is used for properties that have default values and may be omitted from
// the binary stream to save space.
//
// The dest parameter must be a pointer to one of the supported types (see ReadProperty).
// The defaultValue must be of the same type as the value pointed to by dest.
//
// Returns an error if the dest type is not supported or if deserialization fails.
func (r *BinaryReader) ReadPropertyWithDefault(
	id uint32,
	dest, defaultValue interface{},
) error {
	data, ok := r.properties[id]
	if !ok {
		// Use default value
		destValue := reflect.ValueOf(dest)
		if destValue.Kind() != reflect.Ptr {
			return fmt.Errorf(
				"dest must be a pointer, got %T",
				dest,
			)
		}
		destValue.Elem().
			Set(reflect.ValueOf(defaultValue))

		return nil
	}

	return r.deserializeValue(data, dest)
}

// ReadList reads a property as a list of strings.
//
// The list format is:
//   - Count: uint64 (number of items, but this is passed as a parameter)
//   - For each item:
//   - Length: uint64 (byte length of string)
//   - Data: []byte (string bytes)
//
// This method is used for ENUM values, STRUCT field names, and other list types
// in the DuckDB binary format.
//
// Returns a defensive copy of the string slice.
// Returns an error if the property is missing or if deserialization fails.
func (r *BinaryReader) ReadList(
	id uint32,
) ([]string, error) {
	data, ok := r.properties[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: property %d",
			ErrRequiredProperty,
			id,
		)
	}

	buf := bytes.NewReader(data)

	// Read item count
	var count uint64
	if err := binary.Read(buf, ByteOrder, &count); err != nil {
		return nil, fmt.Errorf(
			"failed to read list count: %w",
			err,
		)
	}

	items := make([]string, count)

	for i := range count {
		// Read string length
		var strLen uint64
		if err := binary.Read(buf, ByteOrder, &strLen); err != nil {
			return nil, fmt.Errorf(
				"failed to read list item %d length: %w",
				i,
				err,
			)
		}

		// Read string data
		strData := make([]byte, strLen)
		if _, err := io.ReadFull(buf, strData); err != nil {
			return nil, fmt.Errorf(
				"failed to read list item %d data: %w",
				i,
				err,
			)
		}

		items[i] = string(strData)
	}

	// Return defensive copy (already a new slice)
	return items, nil
}

// deserializeValue converts binary data to a Go value.
//
// Supported destination types:
//   - *uint8: 1 byte
//   - *uint32: 4 bytes (little-endian)
//   - *uint64: 8 bytes (little-endian)
//   - *string: length (uint64) + data bytes
//   - *[]byte: raw bytes (no length prefix)
//
// Returns an error if the destination type is not supported or if the data
// cannot be deserialized.
func (r *BinaryReader) deserializeValue(
	data []byte,
	dest interface{},
) error {
	buf := bytes.NewReader(data)

	switch d := dest.(type) {
	case *uint8:
		if err := binary.Read(buf, ByteOrder, d); err != nil {
			return fmt.Errorf("failed to deserialize uint8: %w", err)
		}

	case *uint32:
		if err := binary.Read(buf, ByteOrder, d); err != nil {
			return fmt.Errorf("failed to deserialize uint32: %w", err)
		}

	case *uint64:
		if err := binary.Read(buf, ByteOrder, d); err != nil {
			return fmt.Errorf("failed to deserialize uint64: %w", err)
		}

	case *string:
		// Read string length
		var strLen uint64
		if err := binary.Read(buf, ByteOrder, &strLen); err != nil {
			return fmt.Errorf("failed to read string length: %w", err)
		}

		// Read string data
		strData := make([]byte, strLen)
		if _, err := io.ReadFull(buf, strData); err != nil {
			return fmt.Errorf("failed to read string data: %w", err)
		}

		*d = string(strData)

	case *[]byte:
		// Read raw bytes (no length prefix)
		*d = make([]byte, len(data))
		copy(*d, data)

	default:
		return fmt.Errorf("%w: unsupported destination type: %T", ErrInvalidPropertyType, dest)
	}

	return nil
}
