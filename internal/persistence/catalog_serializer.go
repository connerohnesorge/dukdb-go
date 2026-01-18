package persistence

import (
	"bytes"
	"fmt"
	"io"
)

// Property IDs for DuckDB's property-based serialization format
const (
	PropertyIDEnd           = 0
	PropertyIDType          = 1
	PropertyIDName          = 2
	PropertyIDNullable      = 3
	PropertyIDDefault       = 4
	PropertyIDPrimaryKey    = 5
	PropertyIDForeignKey    = 6
	PropertyIDChildCount    = 100
	PropertyIDChildType     = 101
	PropertyIDChildName     = 102
	PropertyIDMemberCount   = 103
	PropertyIDMemberType    = 104
	PropertyIDMemberName    = 105
	PropertyIDKeyType       = 106 // MAP key type
	PropertyIDValueType     = 107 // MAP value type
	PropertyIDSchemaCount   = 200
	PropertyIDTableCount    = 201
	PropertyIDColumnCount   = 202
	PropertyIDViewCount     = 203
	PropertyIDViewQuery     = 204
	PropertyIDViewDeps      = 205
	PropertyIDIndexCount    = 206
	PropertyIDIndexTable    = 207
	PropertyIDIndexColumns  = 208
	PropertyIDIndexUnique   = 209
	PropertyIDIndexPrimary  = 210
	PropertyIDSequenceCount = 211
	PropertyIDSequenceStart = 212
	PropertyIDSequenceIncr  = 213
	PropertyIDSequenceMin   = 214
	PropertyIDSequenceMax   = 215
	PropertyIDSequenceCycle = 216
	PropertyIDSequenceCurr  = 217
)

// DuckDB LogicalTypeId constants for serialization
// These match the LogicalTypeId enum in DuckDB's types.hpp
const (
	TypeIDTimestampTZ = 32 // TIMESTAMP WITH TIME ZONE
	TypeIDTimeTZ      = 34 // TIME WITH TIME ZONE
	TypeIDBit         = 36 // BIT (boolean stored as single bit)
)

// BinaryWriter provides utilities for writing binary data in DuckDB format.
// It uses varints for compact encoding of integers and length-prefixed strings.
type BinaryWriter struct {
	buf *bytes.Buffer
}

// NewBinaryWriter creates a new BinaryWriter with an empty buffer.
func NewBinaryWriter() *BinaryWriter {
	return &BinaryWriter{
		buf: new(bytes.Buffer),
	}
}

// WriteVarint writes a signed integer using varint encoding.
// Varints use variable-length encoding where smaller values use fewer bytes.
func (w *BinaryWriter) WriteVarint(v int64) error {
	// Convert to unsigned zigzag encoding for better compression of negative numbers
	uv := uint64(v<<1) ^ uint64(v>>63)
	return w.WriteUvarint(uv)
}

// WriteUvarint writes an unsigned integer using varint encoding.
// Uses the protobuf varint encoding scheme.
func (w *BinaryWriter) WriteUvarint(v uint64) error {
	for v >= 0x80 {
		if err := w.buf.WriteByte(byte(v) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return w.buf.WriteByte(byte(v))
}

// WriteString writes a length-prefixed string.
// Format: <length:varint><data:bytes>
func (w *BinaryWriter) WriteString(s string) error {
	if err := w.WriteUvarint(uint64(len(s))); err != nil {
		return err
	}
	_, err := w.buf.WriteString(s)
	return err
}

// WriteBool writes a boolean as a single byte (0 or 1).
func (w *BinaryWriter) WriteBool(b bool) error {
	if b {
		return w.buf.WriteByte(1)
	}
	return w.buf.WriteByte(0)
}

// WriteBytes writes raw bytes without any length prefix.
func (w *BinaryWriter) WriteBytes(data []byte) error {
	_, err := w.buf.Write(data)
	return err
}

// WriteProperty writes a property with the given ID and value.
// Automatically handles different value types.
func (w *BinaryWriter) WriteProperty(id uint64, value any) error {
	// Write property ID
	if err := w.WriteUvarint(id); err != nil {
		return err
	}

	// Write value based on type
	switch v := value.(type) {
	case int:
		return w.WriteVarint(int64(v))
	case int64:
		return w.WriteVarint(v)
	case uint64:
		return w.WriteUvarint(v)
	case string:
		return w.WriteString(v)
	case bool:
		return w.WriteBool(v)
	case []byte:
		return w.WriteBytes(v)
	default:
		return fmt.Errorf("unsupported property value type: %T", value)
	}
}

// WritePropertyEnd writes the END property marker (property ID 0).
func (w *BinaryWriter) WritePropertyEnd() error {
	return w.WriteUvarint(PropertyIDEnd)
}

// Bytes returns the current buffer contents.
func (w *BinaryWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// Len returns the current buffer length.
func (w *BinaryWriter) Len() int {
	return w.buf.Len()
}

// Reset resets the writer buffer.
func (w *BinaryWriter) Reset() {
	w.buf.Reset()
}

// BinaryReader provides utilities for reading binary data in DuckDB format.
// It reads varints and length-prefixed strings.
type BinaryReader struct {
	r io.Reader
}

// NewBinaryReader creates a new BinaryReader from an io.Reader.
func NewBinaryReader(r io.Reader) *BinaryReader {
	return &BinaryReader{r: r}
}

// NewBinaryReaderFromBytes creates a new BinaryReader from a byte slice.
func NewBinaryReaderFromBytes(data []byte) *BinaryReader {
	return &BinaryReader{r: bytes.NewReader(data)}
}

// ReadVarint reads a signed integer using varint encoding.
func (r *BinaryReader) ReadVarint() (int64, error) {
	uv, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	// Decode zigzag encoding
	v := int64(uv>>1) ^ int64(uv<<63)>>63
	return v, nil
}

// ReadUvarint reads an unsigned integer using varint encoding.
func (r *BinaryReader) ReadUvarint() (uint64, error) {
	var v uint64
	var shift uint
	for {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			if shift >= 64 || (shift == 63 && b > 1) {
				return 0, fmt.Errorf("varint overflow")
			}
			return v | uint64(b)<<shift, nil
		}
		v |= uint64(b&0x7f) << shift
		shift += 7
	}
}

// ReadString reads a length-prefixed string.
// Format: <length:varint><data:bytes>
func (r *BinaryReader) ReadString() (string, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	if length > 1<<30 { // Safety limit: 1GB
		return "", fmt.Errorf("string length too large: %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadBool reads a boolean value (single byte: 0 or 1).
func (r *BinaryReader) ReadBool() (bool, error) {
	b, err := r.readByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

// ReadBytes reads exactly n bytes.
func (r *BinaryReader) ReadBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative byte count: %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// readByte reads a single byte from the reader.
func (r *BinaryReader) readByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r.r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadProperty reads the next property ID.
// Returns the property ID, or an error if reading fails.
// The caller is responsible for reading the property value based on the ID.
func (r *BinaryReader) ReadProperty() (uint64, error) {
	return r.ReadUvarint()
}

// SkipProperty skips a property value by reading and discarding it.
// This is used for unknown properties that we don't handle.
func (r *BinaryReader) SkipProperty(id uint64) error {
	// For now, we don't know the size of the property value,
	// so we can't skip it safely. Caller should handle known properties.
	// This is a placeholder for future implementation.
	return fmt.Errorf("cannot skip unknown property ID %d", id)
}

// PropertyReader provides a high-level interface for reading properties.
type PropertyReader struct {
	reader *BinaryReader
}

// NewPropertyReader creates a new PropertyReader.
func NewPropertyReader(r io.Reader) *PropertyReader {
	return &PropertyReader{
		reader: NewBinaryReader(r),
	}
}

// NewPropertyReaderFromBytes creates a new PropertyReader from bytes.
func NewPropertyReaderFromBytes(data []byte) *PropertyReader {
	return &PropertyReader{
		reader: NewBinaryReaderFromBytes(data),
	}
}

// ReadNextProperty reads the next property ID and returns it.
// Returns PropertyIDEnd (0) when the property list ends.
func (pr *PropertyReader) ReadNextProperty() (uint64, error) {
	return pr.reader.ReadProperty()
}

// ReadVarint reads a varint value for the current property.
func (pr *PropertyReader) ReadVarint() (int64, error) {
	return pr.reader.ReadVarint()
}

// ReadUvarint reads an uvarint value for the current property.
func (pr *PropertyReader) ReadUvarint() (uint64, error) {
	return pr.reader.ReadUvarint()
}

// ReadString reads a string value for the current property.
func (pr *PropertyReader) ReadString() (string, error) {
	return pr.reader.ReadString()
}

// ReadBool reads a bool value for the current property.
func (pr *PropertyReader) ReadBool() (bool, error) {
	return pr.reader.ReadBool()
}

// PropertyWriter provides a high-level interface for writing properties.
type PropertyWriter struct {
	writer *BinaryWriter
}

// NewPropertyWriter creates a new PropertyWriter.
func NewPropertyWriter() *PropertyWriter {
	return &PropertyWriter{
		writer: NewBinaryWriter(),
	}
}

// WriteProperty writes a property with ID and value.
func (pw *PropertyWriter) WriteProperty(id uint64, value any) error {
	return pw.writer.WriteProperty(id, value)
}

// WriteEnd writes the END property marker.
func (pw *PropertyWriter) WriteEnd() error {
	return pw.writer.WritePropertyEnd()
}

// Bytes returns the serialized property data.
func (pw *PropertyWriter) Bytes() []byte {
	return pw.writer.Bytes()
}

// Len returns the current serialized data length.
func (pw *PropertyWriter) Len() int {
	return pw.writer.Len()
}

// SerializeTypeInfo serializes TypeInfo using DuckDB's property-based binary format.
// It writes the type ID and optionally the type name, followed by PropertyIDEnd.
// For basic types (INTEGER, VARCHAR, etc.), only the type ID is written.
// For types that need additional metadata, the type name is also written.
//
// Format:
//
//	PropertyIDType (1) → type ID (varint)
//	[PropertyIDName (2) → type name (string)]  // Optional
//	PropertyIDEnd (0)
func SerializeTypeInfo(w *BinaryWriter, typeID int, typeName string) error {
	// Write the type ID property
	if err := w.WriteProperty(PropertyIDType, uint64(typeID)); err != nil {
		return fmt.Errorf("failed to write type ID: %w", err)
	}

	// Write type name if provided (for types that need it)
	if typeName != "" {
		if err := w.WriteProperty(PropertyIDName, typeName); err != nil {
			return fmt.Errorf("failed to write type name: %w", err)
		}
	}

	// Write END marker
	if err := w.WritePropertyEnd(); err != nil {
		return fmt.Errorf("failed to write END marker: %w", err)
	}

	return nil
}

// DeserializeTypeInfo deserializes TypeInfo from DuckDB's property-based binary format.
// It reads properties until PropertyIDEnd is encountered, extracting the type ID and
// optional type name. Unknown properties are skipped gracefully.
//
// Returns:
//   - typeID: The DuckDB type ID (e.g., 4 for INTEGER, 18 for VARCHAR)
//   - typeName: The type name if present, otherwise an empty string
//   - error: Any error encountered during deserialization
//
// Format:
//
//	PropertyIDType (1) → type ID (varint)
//	[PropertyIDName (2) → type name (string)]  // Optional
//	[Other properties...]                       // Skipped
//	PropertyIDEnd (0)
func DeserializeTypeInfo(r *BinaryReader) (typeID int, typeName string, err error) {
	var hasTypeID bool

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return 0, "", fmt.Errorf("failed to read property ID: %w", err)
		}

		// End of properties
		if propID == PropertyIDEnd {
			break
		}

		switch propID {
		case PropertyIDType:
			// Read type ID as unsigned varint
			typeIDVal, err := r.ReadUvarint()
			if err != nil {
				return 0, "", fmt.Errorf("failed to read type ID: %w", err)
			}
			typeID = int(typeIDVal)
			hasTypeID = true

		case PropertyIDName:
			// Read type name as string
			typeName, err = r.ReadString()
			if err != nil {
				return 0, "", fmt.Errorf("failed to read type name: %w", err)
			}

		default:
			// Unknown property - we need to skip it
			// For now, we'll return an error since we don't know the property's value type
			// In the future, we could implement property metadata to know how to skip
			return 0, "", fmt.Errorf("unknown property ID %d (cannot skip)", propID)
		}
	}

	// Validate that we read at least the type ID
	if !hasTypeID {
		return 0, "", fmt.Errorf("missing required property: PropertyIDType")
	}

	return typeID, typeName, nil
}

// StructField represents a single field in a STRUCT type.
// Each field has a name and a type ID that identifies its data type.
type StructField struct {
	Name   string
	TypeID int
}

// SerializeStructType serializes a STRUCT type with its fields using DuckDB's property-based format.
// STRUCT types contain multiple named fields, each with their own type. The serialization is recursive,
// meaning that child types can themselves be complex types (e.g., a STRUCT containing a STRUCT).
//
// Format:
//
//	PropertyIDType (1) → STRUCT type ID (26)
//	PropertyIDChildCount (100) → number of fields (varint)
//	For each field:
//	  PropertyIDChildName (102) → field name (string)
//	  PropertyIDChildType (101) → field type ID (varint)
//	PropertyIDEnd (0)
//
// The order of child name and child type properties can vary - the implementation writes name first
// for better readability, but the reader must handle both orderings.
func SerializeStructType(w *BinaryWriter, fields []StructField) error {
	// Write the STRUCT type ID (26 is DuckDB's type ID for STRUCT)
	const TypeIDStruct = 26
	if err := w.WriteProperty(PropertyIDType, uint64(TypeIDStruct)); err != nil {
		return fmt.Errorf("failed to write STRUCT type ID: %w", err)
	}

	// Write the number of fields in the struct
	if err := w.WriteProperty(PropertyIDChildCount, uint64(len(fields))); err != nil {
		return fmt.Errorf("failed to write child count: %w", err)
	}

	// Write each field's name and type
	for i, field := range fields {
		// Write field name
		if err := w.WriteProperty(PropertyIDChildName, field.Name); err != nil {
			return fmt.Errorf("failed to write field name at index %d: %w", i, err)
		}

		// Write field type ID
		if err := w.WriteProperty(PropertyIDChildType, uint64(field.TypeID)); err != nil {
			return fmt.Errorf("failed to write field type at index %d: %w", i, err)
		}
	}

	// Write END marker
	if err := w.WritePropertyEnd(); err != nil {
		return fmt.Errorf("failed to write END marker: %w", err)
	}

	return nil
}

// DeserializeStructType deserializes a STRUCT type from DuckDB's property-based binary format.
// It reads properties until PropertyIDEnd is encountered, extracting the field count and
// then reading each field's name and type ID.
//
// The function is flexible in handling different property orderings - it doesn't require
// PropertyIDChildName to come before PropertyIDChildType, and handles them in any order.
//
// Returns:
//   - fields: Slice of StructField containing field names and type IDs
//   - error: Any error encountered during deserialization
//
// Format:
//
//	PropertyIDType (1) → STRUCT type ID (26)
//	PropertyIDChildCount (100) → number of fields (varint)
//	For each field:
//	  PropertyIDChildName (102) → field name (string)
//	  PropertyIDChildType (101) → field type ID (varint)
//	PropertyIDEnd (0)
func DeserializeStructType(r *BinaryReader) ([]StructField, error) {
	var hasTypeID bool
	var hasChildCount bool
	var childCount int
	var fields []StructField
	var pendingName string
	var pendingTypeID int
	var hasPendingName bool
	var hasPendingTypeID bool

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return nil, fmt.Errorf("failed to read property ID: %w", err)
		}

		// End of properties
		if propID == PropertyIDEnd {
			break
		}

		switch propID {
		case PropertyIDType:
			// Read and verify this is a STRUCT type
			typeIDVal, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read type ID: %w", err)
			}
			const TypeIDStruct = 26
			if typeIDVal != TypeIDStruct {
				return nil, fmt.Errorf(
					"expected STRUCT type ID (%d), got %d",
					TypeIDStruct,
					typeIDVal,
				)
			}
			hasTypeID = true

		case PropertyIDChildCount:
			// Read number of fields
			count, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read child count: %w", err)
			}
			childCount = int(count)
			hasChildCount = true
			fields = make([]StructField, 0, childCount)

		case PropertyIDChildName:
			// Read field name
			name, err := r.ReadString()
			if err != nil {
				return nil, fmt.Errorf("failed to read child name: %w", err)
			}
			pendingName = name
			hasPendingName = true

			// If we have both name and type, create the field
			if hasPendingTypeID {
				fields = append(fields, StructField{
					Name:   pendingName,
					TypeID: pendingTypeID,
				})
				hasPendingName = false
				hasPendingTypeID = false
			}

		case PropertyIDChildType:
			// Read field type ID
			typeID, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read child type: %w", err)
			}
			pendingTypeID = int(typeID)
			hasPendingTypeID = true

			// If we have both name and type, create the field
			if hasPendingName {
				fields = append(fields, StructField{
					Name:   pendingName,
					TypeID: pendingTypeID,
				})
				hasPendingName = false
				hasPendingTypeID = false
			}

		default:
			// Unknown property - we can't skip it safely
			return nil, fmt.Errorf("unknown property ID %d (cannot skip)", propID)
		}
	}

	// Validate that we read all required properties
	if !hasTypeID {
		return nil, fmt.Errorf("missing required property: PropertyIDType")
	}
	if !hasChildCount {
		return nil, fmt.Errorf("missing required property: PropertyIDChildCount")
	}

	// Validate that we read the correct number of fields
	if len(fields) != childCount {
		return nil, fmt.Errorf("expected %d fields, got %d", childCount, len(fields))
	}

	return fields, nil
}

// SerializeListType serializes a LIST type with its element type using DuckDB's property-based format.
// LIST types contain a single child type that represents the type of elements in the list.
// For example, LIST(INTEGER) has child type INTEGER, LIST(VARCHAR) has child type VARCHAR.
//
// Format:
//
//	PropertyIDType (1) → LIST type ID (25)
//	PropertyIDChildType (101) → element type ID (varint)
//	PropertyIDEnd (0)
//
// Parameters:
//   - w: BinaryWriter to write the serialized data
//   - elementTypeID: The type ID of the list's element type
//
// Example:
//
//	SerializeListType(w, 4) // LIST(INTEGER)
//	SerializeListType(w, 18) // LIST(VARCHAR)
//	SerializeListType(w, 25) // LIST(LIST(...)) - nested list
func SerializeListType(w *BinaryWriter, elementTypeID int) error {
	// Write the LIST type ID (25 is DuckDB's type ID for LIST)
	const TypeIDList = 25
	if err := w.WriteProperty(PropertyIDType, uint64(TypeIDList)); err != nil {
		return fmt.Errorf("failed to write LIST type ID: %w", err)
	}

	// Write the element type ID
	if err := w.WriteProperty(PropertyIDChildType, uint64(elementTypeID)); err != nil {
		return fmt.Errorf("failed to write element type ID: %w", err)
	}

	// Write END marker
	if err := w.WritePropertyEnd(); err != nil {
		return fmt.Errorf("failed to write END marker: %w", err)
	}

	return nil
}

// DeserializeListType deserializes a LIST type from DuckDB's property-based binary format.
// It reads properties until PropertyIDEnd is encountered, extracting the element type ID.
//
// The function validates that the type ID is correct (25 for LIST) and that the required
// PropertyIDChildType is present.
//
// Returns:
//   - elementTypeID: The type ID of the list's element type
//   - error: Any error encountered during deserialization
//
// Format:
//
//	PropertyIDType (1) → LIST type ID (25)
//	PropertyIDChildType (101) → element type ID (varint)
//	PropertyIDEnd (0)
//
// Example:
//
//	elementTypeID, err := DeserializeListType(r)
//	// elementTypeID might be 4 for LIST(INTEGER), 18 for LIST(VARCHAR), etc.
func DeserializeListType(r *BinaryReader) (elementTypeID int, err error) {
	var hasTypeID bool
	var hasChildType bool

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return 0, fmt.Errorf("failed to read property ID: %w", err)
		}

		// End of properties
		if propID == PropertyIDEnd {
			break
		}

		switch propID {
		case PropertyIDType:
			// Read and verify this is a LIST type
			typeIDVal, err := r.ReadUvarint()
			if err != nil {
				return 0, fmt.Errorf("failed to read type ID: %w", err)
			}
			const TypeIDList = 25
			if typeIDVal != TypeIDList {
				return 0, fmt.Errorf("expected LIST type ID (%d), got %d", TypeIDList, typeIDVal)
			}
			hasTypeID = true

		case PropertyIDChildType:
			// Read element type ID
			typeID, err := r.ReadUvarint()
			if err != nil {
				return 0, fmt.Errorf("failed to read child type: %w", err)
			}
			elementTypeID = int(typeID)
			hasChildType = true

		default:
			// Unknown property - we can't skip it safely
			return 0, fmt.Errorf("unknown property ID %d (cannot skip)", propID)
		}
	}

	// Validate that we read all required properties
	if !hasTypeID {
		return 0, fmt.Errorf("missing required property: PropertyIDType")
	}
	if !hasChildType {
		return 0, fmt.Errorf("missing required property: PropertyIDChildType")
	}

	return elementTypeID, nil
}

// SerializeMapType serializes a MAP type with its key and value types using DuckDB's property-based format.
// MAP types contain two child types: a key type and a value type that define the map's structure.
// For example, MAP(VARCHAR, INTEGER) has key type VARCHAR and value type INTEGER.
//
// Format:
//
//	PropertyIDType (1) → MAP type ID (27)
//	PropertyIDKeyType (106) → key type ID (varint)
//	PropertyIDValueType (107) → value type ID (varint)
//	PropertyIDEnd (0)
//
// Parameters:
//   - w: BinaryWriter to write the serialized data
//   - keyTypeID: The type ID of the map's key type
//   - valueTypeID: The type ID of the map's value type
//
// Example:
//
//	SerializeMapType(w, 18, 4) // MAP(VARCHAR, INTEGER)
//	SerializeMapType(w, 4, 18) // MAP(INTEGER, VARCHAR)
//	SerializeMapType(w, 18, 27) // MAP(VARCHAR, MAP(...)) - nested map
func SerializeMapType(w *BinaryWriter, keyTypeID, valueTypeID int) error {
	// Write the MAP type ID (27 is DuckDB's type ID for MAP)
	const TypeIDMap = 27
	if err := w.WriteProperty(PropertyIDType, uint64(TypeIDMap)); err != nil {
		return fmt.Errorf("failed to write MAP type ID: %w", err)
	}

	// Write the key type ID
	if err := w.WriteProperty(PropertyIDKeyType, uint64(keyTypeID)); err != nil {
		return fmt.Errorf("failed to write key type ID: %w", err)
	}

	// Write the value type ID
	if err := w.WriteProperty(PropertyIDValueType, uint64(valueTypeID)); err != nil {
		return fmt.Errorf("failed to write value type ID: %w", err)
	}

	// Write END marker
	if err := w.WritePropertyEnd(); err != nil {
		return fmt.Errorf("failed to write END marker: %w", err)
	}

	return nil
}

// DeserializeMapType deserializes a MAP type from DuckDB's property-based binary format.
// It reads properties until PropertyIDEnd is encountered, extracting the key and value type IDs.
//
// The function validates that the type ID is correct (27 for MAP) and that both required
// properties (PropertyIDKeyType and PropertyIDValueType) are present.
//
// Returns:
//   - keyTypeID: The type ID of the map's key type
//   - valueTypeID: The type ID of the map's value type
//   - error: Any error encountered during deserialization
//
// Format:
//
//	PropertyIDType (1) → MAP type ID (27)
//	PropertyIDKeyType (106) → key type ID (varint)
//	PropertyIDValueType (107) → value type ID (varint)
//	PropertyIDEnd (0)
//
// Example:
//
//	keyTypeID, valueTypeID, err := DeserializeMapType(r)
//	// keyTypeID might be 18 (VARCHAR), valueTypeID might be 4 (INTEGER) for MAP(VARCHAR, INTEGER)
func DeserializeMapType(r *BinaryReader) (keyTypeID, valueTypeID int, err error) {
	var hasTypeID bool
	var hasKeyType bool
	var hasValueType bool

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read property ID: %w", err)
		}

		// End of properties
		if propID == PropertyIDEnd {
			break
		}

		switch propID {
		case PropertyIDType:
			// Read and verify this is a MAP type
			typeIDVal, err := r.ReadUvarint()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to read type ID: %w", err)
			}
			const TypeIDMap = 27
			if typeIDVal != TypeIDMap {
				return 0, 0, fmt.Errorf("expected MAP type ID (%d), got %d", TypeIDMap, typeIDVal)
			}
			hasTypeID = true

		case PropertyIDKeyType:
			// Read key type ID
			typeID, err := r.ReadUvarint()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to read key type: %w", err)
			}
			keyTypeID = int(typeID)
			hasKeyType = true

		case PropertyIDValueType:
			// Read value type ID
			typeID, err := r.ReadUvarint()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to read value type: %w", err)
			}
			valueTypeID = int(typeID)
			hasValueType = true

		default:
			// Unknown property - we can't skip it safely
			return 0, 0, fmt.Errorf("unknown property ID %d (cannot skip)", propID)
		}
	}

	// Validate that we read all required properties
	if !hasTypeID {
		return 0, 0, fmt.Errorf("missing required property: PropertyIDType")
	}
	if !hasKeyType {
		return 0, 0, fmt.Errorf("missing required property: PropertyIDKeyType")
	}
	if !hasValueType {
		return 0, 0, fmt.Errorf("missing required property: PropertyIDValueType")
	}

	return keyTypeID, valueTypeID, nil
}

// UnionMember represents a single member in a UNION type.
// Each member has a name and a type ID that identifies its data type.
// UNION types represent tagged unions where only one member is active at a time.
type UnionMember struct {
	Name   string
	TypeID int
}

// SerializeUnionType serializes a UNION type with its members using DuckDB's property-based format.
// UNION types contain multiple named members, each with their own type. Unlike STRUCTs where all
// fields exist simultaneously, UNIONs represent a tagged union where only one member is active.
// For example, UNION(num INTEGER, str VARCHAR) can hold either an integer OR a string.
//
// Format:
//
//	PropertyIDType (1) → UNION type ID (28)
//	PropertyIDMemberCount (103) → number of members (varint)
//	For each member:
//	  PropertyIDMemberName (105) → member name (string)
//	  PropertyIDMemberType (104) → member type ID (varint)
//	PropertyIDEnd (0)
//
// Parameters:
//   - w: BinaryWriter to write the serialized data
//   - members: Slice of UnionMember containing member names and type IDs
//
// Example:
//
//	SerializeUnionType(w, []UnionMember{
//	  {Name: "num", TypeID: 4},     // INTEGER
//	  {Name: "str", TypeID: 18},    // VARCHAR
//	}) // UNION(num INTEGER, str VARCHAR)
func SerializeUnionType(w *BinaryWriter, members []UnionMember) error {
	// Write the UNION type ID (28 is DuckDB's type ID for UNION)
	const TypeIDUnion = 28
	if err := w.WriteProperty(PropertyIDType, uint64(TypeIDUnion)); err != nil {
		return fmt.Errorf("failed to write UNION type ID: %w", err)
	}

	// Write the number of members in the union
	if err := w.WriteProperty(PropertyIDMemberCount, uint64(len(members))); err != nil {
		return fmt.Errorf("failed to write member count: %w", err)
	}

	// Write each member's name and type
	for i, member := range members {
		// Write member name
		if err := w.WriteProperty(PropertyIDMemberName, member.Name); err != nil {
			return fmt.Errorf("failed to write member name at index %d: %w", i, err)
		}

		// Write member type ID
		if err := w.WriteProperty(PropertyIDMemberType, uint64(member.TypeID)); err != nil {
			return fmt.Errorf("failed to write member type at index %d: %w", i, err)
		}
	}

	// Write END marker
	if err := w.WritePropertyEnd(); err != nil {
		return fmt.Errorf("failed to write END marker: %w", err)
	}

	return nil
}

// DeserializeUnionType deserializes a UNION type from DuckDB's property-based binary format.
// It reads properties until PropertyIDEnd is encountered, extracting the member count and
// then reading each member's name and type ID.
//
// The function is flexible in handling different property orderings - it doesn't require
// PropertyIDMemberName to come before PropertyIDMemberType, and handles them in any order.
//
// Returns:
//   - members: Slice of UnionMember containing member names and type IDs
//   - error: Any error encountered during deserialization
//
// Format:
//
//	PropertyIDType (1) → UNION type ID (28)
//	PropertyIDMemberCount (103) → number of members (varint)
//	For each member:
//	  PropertyIDMemberName (105) → member name (string)
//	  PropertyIDMemberType (104) → member type ID (varint)
//	PropertyIDEnd (0)
//
// Example:
//
//	members, err := DeserializeUnionType(r)
//	// members might be [{Name: "num", TypeID: 4}, {Name: "str", TypeID: 18}]
//	// for UNION(num INTEGER, str VARCHAR)
func DeserializeUnionType(r *BinaryReader) ([]UnionMember, error) {
	var hasTypeID bool
	var hasMemberCount bool
	var memberCount int
	var members []UnionMember
	var pendingName string
	var pendingTypeID int
	var hasPendingName bool
	var hasPendingTypeID bool

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return nil, fmt.Errorf("failed to read property ID: %w", err)
		}

		// End of properties
		if propID == PropertyIDEnd {
			break
		}

		switch propID {
		case PropertyIDType:
			// Read and verify this is a UNION type
			typeIDVal, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read type ID: %w", err)
			}
			const TypeIDUnion = 28
			if typeIDVal != TypeIDUnion {
				return nil, fmt.Errorf(
					"expected UNION type ID (%d), got %d",
					TypeIDUnion,
					typeIDVal,
				)
			}
			hasTypeID = true

		case PropertyIDMemberCount:
			// Read number of members
			count, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read member count: %w", err)
			}
			memberCount = int(count)
			hasMemberCount = true
			members = make([]UnionMember, 0, memberCount)

		case PropertyIDMemberName:
			// Read member name
			name, err := r.ReadString()
			if err != nil {
				return nil, fmt.Errorf("failed to read member name: %w", err)
			}
			pendingName = name
			hasPendingName = true

			// If we have both name and type, create the member
			if hasPendingTypeID {
				members = append(members, UnionMember{
					Name:   pendingName,
					TypeID: pendingTypeID,
				})
				hasPendingName = false
				hasPendingTypeID = false
			}

		case PropertyIDMemberType:
			// Read member type ID
			typeID, err := r.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read member type: %w", err)
			}
			pendingTypeID = int(typeID)
			hasPendingTypeID = true

			// If we have both name and type, create the member
			if hasPendingName {
				members = append(members, UnionMember{
					Name:   pendingName,
					TypeID: pendingTypeID,
				})
				hasPendingName = false
				hasPendingTypeID = false
			}

		default:
			// Unknown property - we can't skip it safely
			return nil, fmt.Errorf("unknown property ID %d (cannot skip)", propID)
		}
	}

	// Validate that we read all required properties
	if !hasTypeID {
		return nil, fmt.Errorf("missing required property: PropertyIDType")
	}
	if !hasMemberCount {
		return nil, fmt.Errorf("missing required property: PropertyIDMemberCount")
	}

	// Validate that we read the correct number of members
	if len(members) != memberCount {
		return nil, fmt.Errorf("expected %d members, got %d", memberCount, len(members))
	}

	return members, nil
}
