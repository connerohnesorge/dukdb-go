package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dukdb/dukdb-go"
)

// SerializeTypeInfo serializes a TypeInfo to binary format using property-based serialization.
//
// This is the main entry point for TypeInfo serialization. It handles both primitive types
// (which have no Details) and complex types (which require type-specific serialization).
//
// Binary Format (Complete LogicalType):
//   - Property 99: uint8 (LogicalTypeId - the base type like INTEGER, VARCHAR, etc.)
//   - Property 100: uint32 (ExtraTypeInfoType discriminator - GENERIC for primitives)
//   - For complex types, additional type-specific properties (200+)
//
// For primitive types (nil Details()), we write properties 99 and 100 only.
// For complex types, this delegates to type-specific serializers based on the Details type.
//
// Returns ErrUnsupportedTypeForSerialization for UNION types (not supported in v64 format).
func SerializeTypeInfo(w *BinaryWriter, ti dukdb.TypeInfo) error {
	if ti == nil {
		return errors.New("cannot serialize nil TypeInfo")
	}

	// Property 99: LogicalTypeId (the base type enum)
	// This is the actual type (INTEGER, VARCHAR, DECIMAL, etc.)
	if err := w.WriteProperty(99, uint8(ti.InternalType())); err != nil {
		return fmt.Errorf("failed to write LogicalTypeId: %w", err)
	}

	// Get type-specific details
	details := ti.Details()

	// Handle primitive types (no extra info needed)
	if details == nil {
		// Primitive types write GENERIC discriminator to indicate no ExtraTypeInfo
		if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_GENERIC)); err != nil {
			return fmt.Errorf("failed to write primitive type discriminator: %w", err)
		}

		return nil
	}

	// Handle complex types based on Details type
	switch d := details.(type) {
	case *dukdb.DecimalDetails:
		return SerializeDecimal(w, d)
	case *dukdb.EnumDetails:
		return SerializeEnum(w, d)
	case *dukdb.ListDetails:
		return SerializeList(w, d)
	case *dukdb.ArrayDetails:
		return SerializeArray(w, d)
	case *dukdb.StructDetails:
		return SerializeStruct(w, d)
	case *dukdb.MapDetails:
		return SerializeMap(w, d)
	case *dukdb.UnionDetails:
		// UNION is not supported in v64 format
		return ErrUnsupportedTypeForSerialization
	default:
		return fmt.Errorf("unknown TypeDetails type: %T", details)
	}
}

// SerializeDecimal serializes DECIMAL type details to binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_DECIMAL = 2)
//   - Property 200: uint8 (width, total number of digits 1-38)
//   - Property 201: uint8 (scale, digits after decimal point 0-width)
//
// Example: DECIMAL(18,4) -> width=18, scale=4
func SerializeDecimal(w *BinaryWriter, d *dukdb.DecimalDetails) error {
	// Property 100: Type discriminator (DECIMAL_TYPE_INFO)
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_DECIMAL)); err != nil {
		return fmt.Errorf("failed to write DECIMAL type discriminator: %w", err)
	}

	// Property 200: Width (precision)
	if err := w.WritePropertyWithDefault(PropertyDecimalWidth, d.Width, uint8(0)); err != nil {
		return fmt.Errorf("failed to write DECIMAL width: %w", err)
	}

	// Property 201: Scale
	if err := w.WritePropertyWithDefault(PropertyDecimalScale, d.Scale, uint8(0)); err != nil {
		return fmt.Errorf("failed to write DECIMAL scale: %w", err)
	}

	return nil
}

// SerializeEnum serializes ENUM type details to binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_ENUM = 6)
//   - Property 200: uint64 (count of enum values)
//   - Property 201: list<string> (enum value strings)
//
// The values list is written using WriteList format:
//   - For each value:
//   - Length: uint64
//   - Data: []byte
//
// Example: ENUM('RED','GREEN','BLUE') -> count=3, values=["RED","GREEN","BLUE"]
func SerializeEnum(w *BinaryWriter, e *dukdb.EnumDetails) error {
	// Property 100: Type discriminator (ENUM_TYPE_INFO)
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_ENUM)); err != nil {
		return fmt.Errorf("failed to write ENUM type discriminator: %w", err)
	}

	// Property 200: Enum values count
	if err := w.WriteProperty(PropertyEnumCount, uint64(len(e.Values))); err != nil {
		return fmt.Errorf("failed to write ENUM count: %w", err)
	}

	// Property 201: Enum values list
	if err := w.WriteList(PropertyEnumValues, e.Values); err != nil {
		return fmt.Errorf("failed to write ENUM values: %w", err)
	}

	return nil
}

// SerializeList serializes LIST type details to binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_LIST = 4)
//   - Property 200: LogicalType (recursive TypeInfo serialization of child type)
//
// This handles recursive serialization for nested types like LIST<LIST<INTEGER>>.
//
// Example: LIST<INTEGER> -> child=TypeInfo(TYPE_INTEGER)
func SerializeList(w *BinaryWriter, l *dukdb.ListDetails) error {
	// Property 100: Type discriminator (LIST_TYPE_INFO)
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_LIST)); err != nil {
		return fmt.Errorf("failed to write LIST type discriminator: %w", err)
	}

	// Property 200: Child TypeInfo (recursive serialization)
	childBuf := new(bytes.Buffer)
	childWriter := NewBinaryWriter(childBuf)
	if err := SerializeTypeInfo(childWriter, l.Child); err != nil {
		return fmt.Errorf("failed to serialize LIST child type: %w", err)
	}
	if err := childWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush LIST child type: %w", err)
	}

	// Write the serialized child TypeInfo as a property
	if err := w.WriteProperty(PropertyChildType, childBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write LIST child type property: %w", err)
	}

	return nil
}

// SerializeArray serializes ARRAY type details to binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_ARRAY = 9)
//   - Property 200: LogicalType (recursive TypeInfo serialization of child type)
//   - Property 201: uint32 (fixed array size)
//
// Example: INTEGER[10] -> child=TypeInfo(TYPE_INTEGER), size=10
func SerializeArray(w *BinaryWriter, a *dukdb.ArrayDetails) error {
	// Property 100: Type discriminator (ARRAY_TYPE_INFO)
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_ARRAY)); err != nil {
		return fmt.Errorf("failed to write ARRAY type discriminator: %w", err)
	}

	// Property 200: Child TypeInfo (recursive serialization)
	childBuf := new(bytes.Buffer)
	childWriter := NewBinaryWriter(childBuf)
	if err := SerializeTypeInfo(childWriter, a.Child); err != nil {
		return fmt.Errorf("failed to serialize ARRAY child type: %w", err)
	}
	if err := childWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush ARRAY child type: %w", err)
	}

	// Write the serialized child TypeInfo as a property
	if err := w.WriteProperty(PropertyChildType, childBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write ARRAY child type property: %w", err)
	}

	// Property 201: Array size (fixed size)
	if err := w.WritePropertyWithDefault(PropertyArraySize, uint32(a.Size), uint32(0)); err != nil {
		return fmt.Errorf("failed to write ARRAY size: %w", err)
	}

	return nil
}

// SerializeStruct serializes STRUCT type details to binary format.
//
// Binary Format:
//   - Property 100: uint32 (ExtraTypeInfoType_STRUCT = 5)
//   - Property 200: child_list_t<LogicalType> (field definitions)
//   - Count: uint64 (number of fields)
//   - For each field:
//   - Name length: uint64
//   - Name: []byte
//   - LogicalType: (recursive TypeInfo serialization)
//
// Example: STRUCT(x INTEGER, y VARCHAR) -> 2 fields with names and types
func SerializeStruct(w *BinaryWriter, s *dukdb.StructDetails) error {
	// Property 100: Type discriminator (STRUCT_TYPE_INFO)
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_STRUCT)); err != nil {
		return fmt.Errorf("failed to write STRUCT type discriminator: %w", err)
	}

	// Property 200: Field list (child_list_t<LogicalType>)
	// Serialize as: count + [(name, type), ...]
	fieldBuf := new(bytes.Buffer)

	// Write field count
	if err := binary.Write(fieldBuf, ByteOrder, uint64(len(s.Entries))); err != nil {
		return fmt.Errorf("failed to write STRUCT field count: %w", err)
	}

	// Write each field
	for i, entry := range s.Entries {
		// Write field name length
		name := entry.Name()
		if err := binary.Write(fieldBuf, ByteOrder, uint64(len(name))); err != nil {
			return fmt.Errorf("failed to write STRUCT field %d name length: %w", i, err)
		}

		// Write field name data
		if _, err := fieldBuf.WriteString(name); err != nil {
			return fmt.Errorf("failed to write STRUCT field %d name: %w", i, err)
		}

		// Write field TypeInfo (recursive serialization)
		fieldWriter := NewBinaryWriter(fieldBuf)
		if err := SerializeTypeInfo(fieldWriter, entry.Info()); err != nil {
			return fmt.Errorf("failed to serialize STRUCT field %d type: %w", i, err)
		}
		if err := fieldWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush STRUCT field %d type: %w", i, err)
		}
	}

	// Write the serialized field list as a property
	if err := w.WriteProperty(PropertyStructFields, fieldBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write STRUCT fields property: %w", err)
	}

	return nil
}

// SerializeMap serializes MAP type details to binary format.
//
// NOTE: MAP does NOT have its own ExtraTypeInfoType. In DuckDB, MAP is internally
// represented as LIST<STRUCT<key, value>>. This serializer follows that convention.
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
func SerializeMap(w *BinaryWriter, m *dukdb.MapDetails) error {
	// MAP uses LIST_TYPE_INFO (4) as discriminator, not a separate MAP type
	if err := w.WriteProperty(PropertyTypeDiscriminator, uint32(ExtraTypeInfoType_LIST)); err != nil {
		return fmt.Errorf("failed to write MAP type discriminator: %w", err)
	}

	// Create STRUCT<key, value> as the child type
	keyEntry, err := dukdb.NewStructEntry(m.Key, "key")
	if err != nil {
		return fmt.Errorf("failed to create MAP key struct entry: %w", err)
	}

	valueEntry, err := dukdb.NewStructEntry(m.Value, "value")
	if err != nil {
		return fmt.Errorf("failed to create MAP value struct entry: %w", err)
	}

	structInfo, err := dukdb.NewStructInfo(keyEntry, valueEntry)
	if err != nil {
		return fmt.Errorf("failed to create MAP struct type: %w", err)
	}

	// Property 200: Child TypeInfo (STRUCT with key and value fields)
	childBuf := new(bytes.Buffer)
	childWriter := NewBinaryWriter(childBuf)

	// Serialize the STRUCT TypeInfo
	structDetails, ok := structInfo.Details().(*dukdb.StructDetails)
	if !ok {
		return errors.New("failed to get StructDetails from MAP struct TypeInfo")
	}
	if err := SerializeStruct(childWriter, structDetails); err != nil {
		return fmt.Errorf("failed to serialize MAP struct: %w", err)
	}
	if err := childWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush MAP struct: %w", err)
	}

	// Write the serialized STRUCT as property 200
	if err := w.WriteProperty(PropertyChildType, childBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write MAP child type property: %w", err)
	}

	return nil
}
