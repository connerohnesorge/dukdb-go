package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dukdb/dukdb-go"
)

// DeserializeTypeInfo deserializes a TypeInfo from binary format using property-based deserialization.
//
// This is the main entry point for TypeInfo deserialization. It reads:
//  1. Property 99: LogicalTypeId (the base type like INTEGER, DECIMAL, LIST, etc.)
//  2. Property 100: ExtraTypeInfoType discriminator
//  3. Type-specific properties (200+) based on the discriminator
//
// Binary Format:
//   - Property count: uint32
//   - For each property in sorted ID order:
//   - Property ID: uint32
//   - Data length: uint64
//   - Data: []byte
//
// For primitive types, discriminator is GENERIC and no additional properties are present.
// For complex types, this delegates to type-specific deserializers based on the discriminator.
//
// Returns ErrUnsupportedTypeForSerialization for UNION types (not supported in v64 format).
func DeserializeTypeInfo(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Read property 99: LogicalTypeId (base type)
	var typeId uint8
	if err := r.ReadProperty(99, &typeId); err != nil {
		return nil, fmt.Errorf(
			"failed to read LogicalTypeId: %w",
			err,
		)
	}

	// Read property 100: type discriminator (ExtraTypeInfoType)
	var discriminator uint32
	if err := r.ReadProperty(PropertyTypeDiscriminator, &discriminator); err != nil {
		return nil, fmt.Errorf(
			"failed to read type discriminator: %w",
			err,
		)
	}

	// If discriminator is GENERIC, this is a primitive type
	if discriminator == ExtraTypeInfoType_GENERIC {
		// Create primitive TypeInfo using the LogicalTypeId
		return dukdb.NewTypeInfo(
			dukdb.Type(typeId),
		)
	}

	// Dispatch to type-specific deserializers based on discriminator
	switch discriminator {
	case ExtraTypeInfoType_DECIMAL:
		return DeserializeDecimal(r)
	case ExtraTypeInfoType_ENUM:
		return DeserializeEnum(r)
	case ExtraTypeInfoType_LIST:
		// Read the LogicalTypeId to determine if this is LIST or MAP
		// MAP has LogicalTypeId TYPE_MAP, but uses LIST_TYPE_INFO for ExtraTypeInfo
		if dukdb.Type(typeId) == dukdb.TYPE_MAP {
			return DeserializeMap(r)
		}

		return DeserializeList(r)
	case ExtraTypeInfoType_STRUCT:
		return DeserializeStruct(r)
	case ExtraTypeInfoType_ARRAY:
		return DeserializeArray(r)
	default:
		return nil, fmt.Errorf(
			"unsupported type discriminator: %d",
			discriminator,
		)
	}
}

// DeserializeDecimal deserializes DECIMAL type details from binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_DECIMAL = 2)
//   - Property 200: uint8 (width, total number of digits 1-38)
//   - Property 201: uint8 (scale, digits after decimal point 0-width)
//
// Example: DECIMAL(18,4) -> width=18, scale=4
func DeserializeDecimal(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	var width, scale uint8

	// Property 200: Width (precision)
	if err := r.ReadPropertyWithDefault(PropertyDecimalWidth, &width, uint8(0)); err != nil {
		return nil, fmt.Errorf(
			"failed to read DECIMAL width: %w",
			err,
		)
	}

	// Property 201: Scale
	if err := r.ReadPropertyWithDefault(PropertyDecimalScale, &scale, uint8(0)); err != nil {
		return nil, fmt.Errorf(
			"failed to read DECIMAL scale: %w",
			err,
		)
	}

	return dukdb.NewDecimalInfo(width, scale)
}

// DeserializeEnum deserializes ENUM type details from binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_ENUM = 6)
//   - Property 200: uint64 (count of enum values)
//   - Property 201: list<string> (enum value strings)
//
// The values list is read using ReadList format:
//   - For each value:
//   - Length: uint64
//   - Data: []byte
//
// Example: ENUM('RED','GREEN','BLUE') -> count=3, values=["RED","GREEN","BLUE"]
func DeserializeEnum(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Property 200: Enum values count
	var count uint64
	if err := r.ReadProperty(PropertyEnumCount, &count); err != nil {
		return nil, fmt.Errorf(
			"failed to read ENUM count: %w",
			err,
		)
	}

	// Property 201: Enum values list
	values, err := r.ReadList(PropertyEnumValues)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read ENUM values: %w",
			err,
		)
	}

	// Validate count matches
	if uint64(len(values)) != count {
		return nil, fmt.Errorf(
			"ENUM count mismatch: expected %d, got %d",
			count,
			len(values),
		)
	}

	// Need at least one value for NewEnumInfo
	if len(values) == 0 {
		return nil, fmt.Errorf(
			"ENUM must have at least one value",
		)
	}

	return dukdb.NewEnumInfo(
		values[0],
		values[1:]...)
}

// DeserializeList deserializes LIST type details from binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_LIST = 4)
//   - Property 200: LogicalType (recursive TypeInfo deserialization of child type)
//
// This handles recursive deserialization for nested types like LIST<LIST<INTEGER>>.
//
// Example: LIST<INTEGER> -> child=TypeInfo(TYPE_INTEGER)
func DeserializeList(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Property 200: Child TypeInfo (recursive deserialization)
	var childBytes []byte
	if err := r.ReadProperty(PropertyChildType, &childBytes); err != nil {
		return nil, fmt.Errorf(
			"failed to read LIST child type: %w",
			err,
		)
	}

	// Deserialize the child TypeInfo
	childBuf := bytes.NewReader(childBytes)
	childReader := NewBinaryReader(childBuf)
	if err := childReader.Load(); err != nil {
		return nil, fmt.Errorf(
			"failed to load LIST child type properties: %w",
			err,
		)
	}

	childInfo, err := DeserializeTypeInfo(
		childReader,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to deserialize LIST child type: %w",
			err,
		)
	}

	return dukdb.NewListInfo(childInfo)
}

// DeserializeArray deserializes ARRAY type details from binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_ARRAY = 9)
//   - Property 200: LogicalType (recursive TypeInfo deserialization of child type)
//   - Property 201: uint32 (fixed array size)
//
// Example: INTEGER[10] -> child=TypeInfo(TYPE_INTEGER), size=10
func DeserializeArray(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Property 200: Child TypeInfo (recursive deserialization)
	var childBytes []byte
	if err := r.ReadProperty(PropertyChildType, &childBytes); err != nil {
		return nil, fmt.Errorf(
			"failed to read ARRAY child type: %w",
			err,
		)
	}

	// Deserialize the child TypeInfo
	childBuf := bytes.NewReader(childBytes)
	childReader := NewBinaryReader(childBuf)
	if err := childReader.Load(); err != nil {
		return nil, fmt.Errorf(
			"failed to load ARRAY child type properties: %w",
			err,
		)
	}

	childInfo, err := DeserializeTypeInfo(
		childReader,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to deserialize ARRAY child type: %w",
			err,
		)
	}

	// Property 201: Array size (fixed size)
	var size uint32
	if err := r.ReadPropertyWithDefault(PropertyArraySize, &size, uint32(0)); err != nil {
		return nil, fmt.Errorf(
			"failed to read ARRAY size: %w",
			err,
		)
	}

	return dukdb.NewArrayInfo(
		childInfo,
		uint64(size),
	)
}

// DeserializeStruct deserializes STRUCT type details from binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_STRUCT = 5)
//   - Property 200: child_list_t<LogicalType> (field definitions)
//   - Count: uint64 (number of fields)
//   - For each field:
//   - Name length: uint64
//   - Name: []byte
//   - LogicalType: (recursive TypeInfo deserialization)
//
// Example: STRUCT(x INTEGER, y VARCHAR) -> 2 fields with names and types
func DeserializeStruct(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Property 200: Field list (child_list_t<LogicalType>)
	var fieldBytes []byte
	if err := r.ReadProperty(PropertyStructFields, &fieldBytes); err != nil {
		return nil, fmt.Errorf(
			"failed to read STRUCT fields: %w",
			err,
		)
	}

	fieldBuf := bytes.NewReader(fieldBytes)

	// Read field count
	var count uint64
	if err := binary.Read(fieldBuf, ByteOrder, &count); err != nil {
		return nil, fmt.Errorf(
			"failed to read STRUCT field count: %w",
			err,
		)
	}

	if count == 0 {
		return nil, fmt.Errorf(
			"STRUCT must have at least one field",
		)
	}

	entries := make([]dukdb.StructEntry, count)

	// Read each field
	for i := range count {
		// Read field name length
		var nameLen uint64
		if err := binary.Read(fieldBuf, ByteOrder, &nameLen); err != nil {
			return nil, fmt.Errorf(
				"failed to read STRUCT field %d name length: %w",
				i,
				err,
			)
		}

		// Read field name data
		nameData := make([]byte, nameLen)
		if _, err := io.ReadFull(fieldBuf, nameData); err != nil {
			return nil, fmt.Errorf(
				"failed to read STRUCT field %d name: %w",
				i,
				err,
			)
		}
		name := string(nameData)

		// Read field TypeInfo (recursive deserialization)
		// We need to read the serialized TypeInfo from the remaining buffer
		// Create a new BinaryReader for this field's TypeInfo
		fieldReader := NewBinaryReader(fieldBuf)
		if err := fieldReader.Load(); err != nil {
			return nil, fmt.Errorf(
				"failed to load STRUCT field %d type properties: %w",
				i,
				err,
			)
		}

		fieldInfo, err := DeserializeTypeInfo(
			fieldReader,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to deserialize STRUCT field %d type: %w",
				i,
				err,
			)
		}

		entry, err := dukdb.NewStructEntry(
			fieldInfo,
			name,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create STRUCT field %d entry: %w",
				i,
				err,
			)
		}

		entries[i] = entry
	}

	return dukdb.NewStructInfo(
		entries[0],
		entries[1:]...)
}

// DeserializeMap deserializes MAP type details from binary format.
//
// NOTE: MAP does NOT have its own ExtraTypeInfoType. In DuckDB, MAP is internally
// represented as LIST<STRUCT<key, value>>. This deserializer extracts the key and value
// types from the STRUCT and constructs a MapInfo.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_LIST = 4) <- Uses LIST discriminator!
//   - Property 200: LogicalType (STRUCT with "key" and "value" fields)
//   - The STRUCT has ExtraTypeInfoType_STRUCT = 5
//   - Field 0: name="key", type=Key TypeInfo
//   - Field 1: name="value", type=Value TypeInfo
//
// Example: MAP<VARCHAR, INTEGER>
//
//	-> LIST<STRUCT<key VARCHAR, value INTEGER>>
//
// This function is called by DeserializeTypeInfo when it detects a LIST with a
// STRUCT<key, value> child pattern.
func DeserializeMap(
	r *BinaryReader,
) (dukdb.TypeInfo, error) {
	// Read property 200: Child TypeInfo (will be a STRUCT)
	var childBytes []byte
	if err := r.ReadProperty(PropertyChildType, &childBytes); err != nil {
		return nil, fmt.Errorf(
			"failed to read MAP child type: %w",
			err,
		)
	}

	// Deserialize the STRUCT
	childBuf := bytes.NewReader(childBytes)
	childReader := NewBinaryReader(childBuf)
	if err := childReader.Load(); err != nil {
		return nil, fmt.Errorf(
			"failed to load MAP child type properties: %w",
			err,
		)
	}

	structInfo, err := DeserializeStruct(
		childReader,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to deserialize MAP struct: %w",
			err,
		)
	}

	// Extract key and value from STRUCT fields
	structDetails, ok := structInfo.Details().(*dukdb.StructDetails)
	if !ok {
		return nil, fmt.Errorf(
			"MAP child must be a STRUCT",
		)
	}

	if len(structDetails.Entries) != 2 {
		return nil, fmt.Errorf(
			"MAP STRUCT must have exactly 2 fields, got %d",
			len(structDetails.Entries),
		)
	}

	if structDetails.Entries[0].Name() != "key" {
		return nil, fmt.Errorf(
			"MAP STRUCT first field must be named 'key', got '%s'",
			structDetails.Entries[0].Name(),
		)
	}

	if structDetails.Entries[1].Name() != "value" {
		return nil, fmt.Errorf(
			"MAP STRUCT second field must be named 'value', got '%s'",
			structDetails.Entries[1].Name(),
		)
	}

	return dukdb.NewMapInfo(
		structDetails.Entries[0].Info(),
		structDetails.Entries[1].Info(),
	)
}
