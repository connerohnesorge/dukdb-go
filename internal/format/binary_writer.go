package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sort"
)

// BinaryWriter implements property-based binary serialization for DuckDB format.
//
// BinaryWriter accumulates properties in memory and writes them in sorted order
// when Flush() is called. This matches DuckDB's serialization protocol where
// properties are written with their IDs in ascending order.
//
// Usage:
//
//	w := NewBinaryWriter(writer)
//	w.WriteProperty(100, uint32(42))
//	w.WritePropertyWithDefault(101, "name", "")
//	w.WriteList(200, []string{"a", "b", "c"})
//	err := w.Flush()
type BinaryWriter struct {
	w          io.Writer
	properties map[uint32][]byte
}

// NewBinaryWriter creates a new BinaryWriter that writes to the given io.Writer.
//
// The writer accumulates properties in memory and writes them in sorted ID order
// when Flush() is called.
func NewBinaryWriter(w io.Writer) *BinaryWriter {
	return &BinaryWriter{
		w:          w,
		properties: make(map[uint32][]byte),
	}
}

// WriteProperty writes a required property to the writer.
//
// The property is not immediately written to the underlying writer; instead, it is
// accumulated in memory and will be written in sorted ID order when Flush() is called.
//
// Supported value types:
//   - uint8, uint32, uint64: Written in little-endian format
//   - string: Written as length (uint64) + data bytes
//   - []byte: Written as-is (caller must handle length prefix if needed)
//   - []string: Written using WriteList format (count + items)
//
// Returns an error if the value type is not supported.
func (w *BinaryWriter) WriteProperty(id uint32, value interface{}) error {
	serialized, err := w.serializeValue(value)
	if err != nil {
		return fmt.Errorf("failed to serialize property %d: %w", id, err)
	}
	w.properties[id] = serialized

	return nil
}

// WritePropertyWithDefault writes an optional property to the writer.
//
// If the value equals the defaultValue (using reflect.DeepEqual), the property
// is not written. This saves space in the binary format by omitting properties
// that have their default values.
//
// The property is not immediately written to the underlying writer; instead, it is
// accumulated in memory and will be written in sorted ID order when Flush() is called.
//
// Returns an error if the value type is not supported.
func (w *BinaryWriter) WritePropertyWithDefault(id uint32, value, defaultValue interface{}) error {
	if reflect.DeepEqual(value, defaultValue) {
		return nil // Skip writing property
	}

	return w.WriteProperty(id, value)
}

// WriteList writes a list of strings as a property.
//
// The list is serialized as:
//   - Count: uint64 (number of items)
//   - For each item:
//   - Length: uint64 (byte length of string)
//   - Data: []byte (string bytes)
//
// This format is used for ENUM values, STRUCT field names, and other list types
// in the DuckDB binary format.
func (w *BinaryWriter) WriteList(id uint32, items []string) error {
	buf := new(bytes.Buffer)

	// Write item count
	if err := binary.Write(buf, ByteOrder, uint64(len(items))); err != nil {
		return fmt.Errorf("failed to write list count: %w", err)
	}

	// Write each item
	for i, item := range items {
		// Write string length
		if err := binary.Write(buf, ByteOrder, uint64(len(item))); err != nil {
			return fmt.Errorf("failed to write list item %d length: %w", i, err)
		}
		// Write string data
		if _, err := buf.WriteString(item); err != nil {
			return fmt.Errorf("failed to write list item %d data: %w", i, err)
		}
	}

	w.properties[id] = buf.Bytes()

	return nil
}

// Flush writes all accumulated properties to the underlying writer in sorted ID order.
//
// The binary format is:
//   - Property count: uint32 (number of properties)
//   - For each property in sorted ID order:
//   - Property ID: uint32
//   - Data length: uint64 (byte length of property data)
//   - Data: []byte (serialized property value)
//
// All multi-byte values are written in little-endian byte order.
//
// After Flush() is called, the property map is cleared and the writer can be reused
// for a new set of properties.
func (w *BinaryWriter) Flush() error {
	// Write property count
	if err := binary.Write(w.w, ByteOrder, uint32(len(w.properties))); err != nil {
		return fmt.Errorf("failed to write property count: %w", err)
	}

	// Get sorted property IDs
	ids := make([]uint32, 0, len(w.properties))
	for id := range w.properties {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Write properties in sorted ID order
	for _, id := range ids {
		// Write property ID
		if err := binary.Write(w.w, ByteOrder, id); err != nil {
			return fmt.Errorf("failed to write property %d ID: %w", id, err)
		}

		// Write property data length
		data := w.properties[id]
		if err := binary.Write(w.w, ByteOrder, uint64(len(data))); err != nil {
			return fmt.Errorf("failed to write property %d length: %w", id, err)
		}

		// Write property data
		if _, err := w.w.Write(data); err != nil {
			return fmt.Errorf("failed to write property %d data: %w", id, err)
		}
	}

	return nil
}

// serializeValue converts a Go value to its binary representation.
//
// Supported types:
//   - uint8: 1 byte
//   - uint32: 4 bytes (little-endian)
//   - uint64: 8 bytes (little-endian)
//   - string: length (uint64) + data bytes
//   - []byte: raw bytes (no length prefix)
//   - []string: count (uint64) + serialized strings
//
// Returns an error if the value type is not supported.
func (w *BinaryWriter) serializeValue(value interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch v := value.(type) {
	case uint8:
		if err := binary.Write(buf, ByteOrder, v); err != nil {
			return nil, err
		}

	case uint32:
		if err := binary.Write(buf, ByteOrder, v); err != nil {
			return nil, err
		}

	case uint64:
		if err := binary.Write(buf, ByteOrder, v); err != nil {
			return nil, err
		}

	case string:
		// Write string length
		if err := binary.Write(buf, ByteOrder, uint64(len(v))); err != nil {
			return nil, err
		}
		// Write string data
		if _, err := buf.WriteString(v); err != nil {
			return nil, err
		}

	case []byte:
		// Write raw bytes (caller handles length if needed)
		if _, err := buf.Write(v); err != nil {
			return nil, err
		}

	case []string:
		// Write count
		if err := binary.Write(buf, ByteOrder, uint64(len(v))); err != nil {
			return nil, err
		}
		// Write each string
		for _, item := range v {
			if err := binary.Write(buf, ByteOrder, uint64(len(item))); err != nil {
				return nil, err
			}
			if _, err := buf.WriteString(item); err != nil {
				return nil, err
			}
		}

	default:
		return nil, fmt.Errorf("unsupported value type: %T", value)
	}

	return buf.Bytes(), nil
}
