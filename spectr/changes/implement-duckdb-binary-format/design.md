# DuckDB Binary Format Implementation Design

## Overview

This document specifies how to implement DuckDB's binary file format (v64) for catalog persistence, enabling cross-implementation compatibility between dukdb-go, DuckDB C++, and duckdb-go v1.4.3.

## DuckDB Binary Format Structure

### File Layout

```
[Magic Number: 4 bytes]     # "DUCK" (0x4455434B)
[Version: 8 bytes]           # Format version (v64 = 64)
[Header Block]               # Metadata about the database
[Catalog Block]              # Schema, tables, types
[Data Blocks]                # Table row data
[Checksum: 8 bytes]          # CRC64 checksum
```

### Property-Based Serialization

DuckDB uses **property-based serialization** with numeric property IDs:

```go
// Base properties (100-199)
const (
    PropertyTypeDiscriminator = 100  // ExtraTypeInfoType enum
    PropertyAlias            = 101  // Optional alias string
    PropertyModifiers        = 102  // Deleted property (for compatibility)
    PropertyExtensionInfo    = 103  // Extension type info
)

// Type-specific properties (200-299)
const (
    // DECIMAL
    PropertyDecimalWidth = 200
    PropertyDecimalScale = 201

    // ENUM
    PropertyEnumValues   = 200
    PropertyEnumDictSize = 201

    // LIST/ARRAY
    PropertyChildType    = 200
    PropertyArraySize    = 201  // ARRAY only

    // STRUCT
    PropertyStructFields = 200  // child_list_t<LogicalType>

    // MAP
    PropertyMapKeyType   = 200
    PropertyMapValueType = 201

    // UNION
    PropertyUnionMembers = 200  // Tagged union types
)
```

### ExtraTypeInfoType Enum

Property 100 (type discriminator) uses the `ExtraTypeInfoType` enum from DuckDB. These values MUST match exactly:

```go
// ExtraTypeInfoType enum values from DuckDB v1.1.3
// Source: duckdb/src/include/duckdb/common/extra_type_info.hpp
const (
    ExtraTypeInfoType_INVALID          = 0
    ExtraTypeInfoType_GENERIC          = 1
    ExtraTypeInfoType_DECIMAL          = 2
    ExtraTypeInfoType_STRING           = 3
    ExtraTypeInfoType_LIST             = 4
    ExtraTypeInfoType_STRUCT           = 5
    ExtraTypeInfoType_ENUM             = 6
    ExtraTypeInfoType_USER             = 7
    ExtraTypeInfoType_AGGREGATE_STATE  = 8
    ExtraTypeInfoType_ARRAY            = 9
    ExtraTypeInfoType_ANY              = 10
    ExtraTypeInfoType_INTEGER_LITERAL  = 11
    ExtraTypeInfoType_TEMPLATE         = 12
    ExtraTypeInfoType_GEO              = 13
)
```

**Types in Scope for P0-1b** (5 ExtraTypeInfoType values + MAP):
- DECIMAL (2) - ExtraTypeInfoType_DECIMAL
- LIST (4) - ExtraTypeInfoType_LIST
- STRUCT (5) - ExtraTypeInfoType_STRUCT
- ENUM (6) - ExtraTypeInfoType_ENUM
- ARRAY (9) - ExtraTypeInfoType_ARRAY
- MAP - Uses LIST_TYPE_INFO (4) with child STRUCT<key, value> (no separate enum value)

**Types Deferred to Future Work** (7 types):
- STRING (3) - Collation info
- USER (7) - User-defined types
- AGGREGATE_STATE (8) - Aggregate function state
- ANY (10) - Any type
- INTEGER_LITERAL (11) - Constant values
- TEMPLATE (12) - Template types
- GEO (13) - Geographic types

## TypeInfo Binary Serialization

### Serialization Protocol

Each TypeInfo is serialized as:

1. **Type Discriminator** (Property 100): Which ExtraTypeInfo type
2. **Base Properties** (101-103): Alias, extension info
3. **Type-Specific Properties** (200+): Width/scale for DECIMAL, values for ENUM, etc.

### TypeDetails → Binary Mapping

#### DECIMAL

```go
func (d *DecimalDetails) Serialize(w *BinaryWriter) error {
    // Property 100: Type discriminator
    w.WriteProperty(100, ExtraTypeInfoType_DECIMAL)

    // Property 200: Width (uint8)
    w.WritePropertyWithDefault(200, d.Width, uint8(0))

    // Property 201: Scale (uint8)
    w.WritePropertyWithDefault(201, d.Scale, uint8(0))

    return nil
}

func DeserializeDecimal(r *BinaryReader) (*DecimalDetails, error) {
    details := &DecimalDetails{}
    r.ReadPropertyWithDefault(200, &details.Width, uint8(0))
    r.ReadPropertyWithDefault(201, &details.Scale, uint8(0))
    return details, nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = DECIMAL_TYPE_INFO enum value
Property 200: uint8 (1 byte)   = width (1-38)
Property 201: uint8 (1 byte)   = scale (0-width)
```

#### ENUM

**NOTE**: Based on DuckDB source code (serialize_types.cpp), ENUM uses property 200 for count and property 201 for the values list.

```go
func (e *EnumDetails) Serialize(w *BinaryWriter) error {
    w.WriteProperty(100, ExtraTypeInfoType_ENUM)

    // Property 200: Values count (idx_t/uint64)
    w.WriteProperty(200, uint64(len(e.Values)))

    // Property 201: Enum values list
    w.WriteList(201, e.Values)  // Uses WriteList API, not WriteProperty

    return nil
}

func DeserializeEnum(r *BinaryReader) (*EnumDetails, error) {
    var count uint64
    r.ReadProperty(200, &count)

    values := r.ReadList(201, count)  // Uses ReadList API

    return &EnumDetails{
        Values: values,  // Defensive copy handled by ReadList
    }, nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = ENUM_TYPE_INFO (6)
Property 200: uint64 (8 bytes) = values_count
Property 201: list<string> = values
    - Written using WriteList API
    - For each string:
        - Length: uint64 (8 bytes)
        - Data: []byte (variable)
```

#### LIST

```go
func (l *ListDetails) Serialize(w *BinaryWriter) error {
    w.WriteProperty(100, ExtraTypeInfoType_LIST)

    // Property 200: Child TypeInfo (recursive)
    w.WriteProperty(200, l.Child)  // Recursive serialization

    return nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = LIST_TYPE_INFO
Property 200: LogicalType (recursive TypeInfo serialization)
```

#### ARRAY

```go
func (a *ArrayDetails) Serialize(w *BinaryWriter) error {
    w.WriteProperty(100, ExtraTypeInfoType_ARRAY)

    // Property 200: Child TypeInfo
    w.WriteProperty(200, a.Child)

    // Property 201: Fixed size
    w.WritePropertyWithDefault(201, uint32(a.Size), uint32(0))

    return nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = ARRAY_TYPE_INFO
Property 200: LogicalType (recursive)
Property 201: uint32 (4 bytes) = fixed size
```

#### STRUCT

```go
func (s *StructDetails) Serialize(w *BinaryWriter) error {
    w.WriteProperty(100, ExtraTypeInfoType_STRUCT)

    // Property 200: Field list (child_list_t<LogicalType>)
    // Format: [(name1, type1), (name2, type2), ...]
    w.WriteProperty(200, s.Entries)

    return nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = STRUCT_TYPE_INFO
Property 200: child_list_t<LogicalType>
    - Count: uint64 (8 bytes)
    - For each field:
        - Name length: uint64 (8 bytes)
        - Name: []byte (variable)
        - LogicalType: (recursive TypeInfo serialization)
```

#### MAP

**NOTE**: MAP does NOT have its own ExtraTypeInfoType. In DuckDB, MAP is a LogicalTypeId that uses `LIST_TYPE_INFO` for serialization. MAP is internally represented as `LIST<STRUCT<key, value>>`.

```go
func (m *MapDetails) Serialize(w *BinaryWriter) error {
    // MAP uses LIST_TYPE_INFO (4) as discriminator
    w.WriteProperty(100, ExtraTypeInfoType_LIST)

    // Property 200: Child TypeInfo (STRUCT with "key" and "value" fields)
    // Create STRUCT(key: m.Key, value: m.Value)
    keyField := StructEntry{Name: "key", Info: m.Key}
    valueField := StructEntry{Name: "value", Info: m.Value}
    structInfo, _ := NewStructInfo(keyField, valueField)

    w.WriteProperty(200, structInfo)  // Recursive: serializes as STRUCT_TYPE_INFO

    return nil
}

func DeserializeMap(r *BinaryReader) (*MapDetails, error) {
    // Read property 200: Child TypeInfo (will be a STRUCT)
    var structInfo TypeInfo
    r.ReadProperty(200, &structInfo)

    // Extract key and value from STRUCT fields
    structDetails := structInfo.Details().(*StructDetails)
    keyInfo := structDetails.Entries[0].Info()  // "key" field
    valueInfo := structDetails.Entries[1].Info() // "value" field

    return &MapDetails{
        Key:   keyInfo,
        Value: valueInfo,
    }, nil
}
```

**Binary Format**:
```
Property 100: uint32 (4 bytes) = LIST_TYPE_INFO (4)
Property 200: LogicalType (STRUCT with "key" and "value" fields)
    - STRUCT uses STRUCT_TYPE_INFO (5) as discriminator
    - STRUCT property 200: field list with 2 entries
        - Entry 0: name="key", type=<Key TypeInfo>
        - Entry 1: name="value", type=<Value TypeInfo>
```

#### UNION - DEFERRED ❌

**STATUS**: **NOT SERIALIZABLE in DuckDB v64 format**

UNION serialization is NOT present in DuckDB v1.1.3 binary format:
- No `UNION_TYPE_INFO` in ExtraTypeInfoType enum
- No `UnionTypeInfo::Serialize` in serialize_types.cpp
- UNION type may be added in future DuckDB versions (v65+)

**Current Behavior**:
- UNION TypeInfo CAN be constructed in-memory (per P0-1a Core TypeInfo)
- UNION TypeInfo CANNOT be serialized to .duckdb files
- Attempting to serialize UNION SHALL return `ErrUnsupportedTypeForSerialization`

**Future Work**: UNION serialization will be added when DuckDB format v65+ includes it.

## Binary Writer/Reader Implementation

### BinaryWriter

```go
type BinaryWriter struct {
    w          io.Writer
    properties map[uint32][]byte  // Property ID → serialized value
}

// WriteProperty writes a required property
func (w *BinaryWriter) WriteProperty(id uint32, value interface{}) error {
    serialized, err := w.serializeValue(value)
    if err != nil {
        return err
    }
    w.properties[id] = serialized
    return nil
}

// WritePropertyWithDefault writes optional property (skip if equals default)
func (w *BinaryWriter) WritePropertyWithDefault(id uint32, value, defaultValue interface{}) error {
    if reflect.DeepEqual(value, defaultValue) {
        return nil  // Skip writing property
    }
    return w.WriteProperty(id, value)
}

// WriteList writes a property as a list/vector (used for ENUM values, STRUCT fields, etc.)
func (w *BinaryWriter) WriteList(id uint32, items []string) error {
    // Serialize list: count + items
    buf := new(bytes.Buffer)

    // Write item count
    binary.Write(buf, binary.LittleEndian, uint64(len(items)))

    // Write each item
    for _, item := range items {
        // Write string length
        binary.Write(buf, binary.LittleEndian, uint64(len(item)))
        // Write string data
        buf.Write([]byte(item))
    }

    w.properties[id] = buf.Bytes()
    return nil
}

// Flush writes all properties to binary stream
func (w *BinaryWriter) Flush() error {
    // Write property count
    binary.Write(w.w, binary.LittleEndian, uint32(len(w.properties)))

    // Write properties in sorted ID order
    ids := make([]uint32, 0, len(w.properties))
    for id := range w.properties {
        ids = append(ids, id)
    }
    sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

    for _, id := range ids {
        // Write property ID
        binary.Write(w.w, binary.LittleEndian, id)

        // Write property data length
        data := w.properties[id]
        binary.Write(w.w, binary.LittleEndian, uint64(len(data)))

        // Write property data
        w.w.Write(data)
    }

    return nil
}
```

### BinaryReader

```go
type BinaryReader struct {
    r          io.Reader
    properties map[uint32][]byte  // Property ID → raw bytes
}

// ReadProperty reads a required property
func (r *BinaryReader) ReadProperty(id uint32, dest interface{}) error {
    data, ok := r.properties[id]
    if !ok {
        return fmt.Errorf("required property %d not found", id)
    }
    return r.deserializeValue(data, dest)
}

// ReadPropertyWithDefault reads optional property
func (r *BinaryReader) ReadPropertyWithDefault(id uint32, dest, defaultValue interface{}) error {
    data, ok := r.properties[id]
    if !ok {
        // Use default value
        reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(defaultValue))
        return nil
    }
    return r.deserializeValue(data, dest)
}

// ReadList reads a property as a list/vector (returns defensive copy)
func (r *BinaryReader) ReadList(id uint32, count uint64) ([]string, error) {
    data, ok := r.properties[id]
    if !ok {
        return nil, fmt.Errorf("required property %d not found", id)
    }

    buf := bytes.NewReader(data)
    items := make([]string, count)

    for i := uint64(0); i < count; i++ {
        // Read string length
        var strLen uint64
        binary.Read(buf, binary.LittleEndian, &strLen)

        // Read string data
        strData := make([]byte, strLen)
        buf.Read(strData)

        items[i] = string(strData)
    }

    // Return defensive copy (already a new slice)
    return items, nil
}

// Load reads all properties from binary stream
func (r *BinaryReader) Load() error {
    // Read property count
    var count uint32
    binary.Read(r.r, binary.LittleEndian, &count)

    r.properties = make(map[uint32][]byte, count)

    for i := uint32(0); i < count; i++ {
        // Read property ID
        var id uint32
        binary.Read(r.r, binary.LittleEndian, &id)

        // Read property data length
        var length uint64
        binary.Read(r.r, binary.LittleEndian, &length)

        // Read property data
        data := make([]byte, length)
        io.ReadFull(r.r, data)

        r.properties[id] = data
    }

    return nil
}
```

## Endianness Handling

DuckDB uses **little-endian** byte order for all multi-byte values:

```go
import "encoding/binary"

var ByteOrder = binary.LittleEndian

// All reads/writes must use ByteOrder
binary.Write(w, ByteOrder, value)
binary.Read(r, ByteOrder, &value)
```

## Recursive Type Handling

Nested types (LIST of STRUCT of MAP) require recursive serialization:

```go
func SerializeTypeInfo(w *BinaryWriter, ti TypeInfo) error {
    // Write base type enum
    w.WriteProperty(100, ti.InternalType())

    // Write type-specific details
    details := ti.Details()
    if details == nil {
        return nil  // Primitive type, no details
    }

    switch d := details.(type) {
    case *DecimalDetails:
        return d.Serialize(w)
    case *EnumDetails:
        return d.Serialize(w)
    case *ListDetails:
        // Recursive: serialize child TypeInfo
        return SerializeTypeInfo(w, d.Child)
    case *StructDetails:
        // Recursive: serialize each field's TypeInfo
        for _, entry := range d.Entries {
            if err := SerializeTypeInfo(w, entry.Info()); err != nil {
                return err
            }
        }
    // ... other types
    }

    return nil
}
```

## Magic Number and Version

```go
const (
    DuckDBMagicNumber = 0x4455434B  // "DUCK"
    DuckDBFormatVersion = 64         // v64 (DuckDB v1.1.3)
)

func WriteHeader(w io.Writer) error {
    // Write magic number (4 bytes)
    binary.Write(w, ByteOrder, uint32(DuckDBMagicNumber))

    // Write format version (8 bytes)
    binary.Write(w, ByteOrder, uint64(DuckDBFormatVersion))

    return nil
}

func ValidateHeader(r io.Reader) error {
    var magic uint32
    binary.Read(r, ByteOrder, &magic)
    if magic != DuckDBMagicNumber {
        return fmt.Errorf("invalid magic number: %x", magic)
    }

    var version uint64
    binary.Read(r, ByteOrder, &version)
    if version != DuckDBFormatVersion {
        return fmt.Errorf("unsupported format version: %d", version)
    }

    return nil
}
```

## Checksum Calculation

DuckDB uses **CRC64** for data integrity:

```go
import "hash/crc64"

var crc64Table = crc64.MakeTable(crc64.ECMA)

func CalculateChecksum(data []byte) uint64 {
    return crc64.Checksum(data, crc64Table)
}

func WriteWithChecksum(w io.Writer, data []byte) error {
    // Write data
    w.Write(data)

    // Calculate and write checksum
    checksum := CalculateChecksum(data)
    binary.Write(w, ByteOrder, checksum)

    return nil
}

func ReadAndVerifyChecksum(r io.Reader, expectedLen int) ([]byte, error) {
    // Read data
    data := make([]byte, expectedLen)
    io.ReadFull(r, data)

    // Read checksum
    var checksum uint64
    binary.Read(r, ByteOrder, &checksum)

    // Verify
    calculated := CalculateChecksum(data)
    if calculated != checksum {
        return nil, fmt.Errorf("checksum mismatch: expected %x, got %x", checksum, calculated)
    }

    return data, nil
}
```

## Catalog Serialization

### Catalog Structure

```go
type CatalogEntry struct {
    Type       CatalogEntryType  // SCHEMA, TABLE, VIEW, etc.
    Name       string
    SchemaName string            // Parent schema
    Data       []byte            // Type-specific data
}

func SerializeCatalog(w io.Writer, catalog *Catalog) error {
    // Write catalog entry count
    entries := catalog.AllEntries()
    binary.Write(w, ByteOrder, uint64(len(entries)))

    // Serialize each entry
    for _, entry := range entries {
        if err := SerializeCatalogEntry(w, entry); err != nil {
            return err
        }
    }

    return nil
}
```

### Table Entry Serialization

```go
func SerializeTableEntry(w io.Writer, table *Table) error {
    bw := NewBinaryWriter(w)

    // Property 100: Entry type (TABLE)
    bw.WriteProperty(100, CatalogEntryType_TABLE)

    // Property 101: Table name
    bw.WriteProperty(101, table.Name)

    // Property 102: Schema name
    bw.WriteProperty(102, table.Schema)

    // Property 200: Column definitions
    bw.WriteProperty(200, table.Columns)

    // Property 201: Constraints
    bw.WritePropertyWithDefault(201, table.Constraints, []Constraint{})

    return bw.Flush()
}
```

### Column Serialization

```go
func SerializeColumn(w *BinaryWriter, col *Column) error {
    // Property 100: Column name
    w.WriteProperty(100, col.Name)

    // Property 101: TypeInfo (recursive)
    if err := SerializeTypeInfo(w, col.TypeInfo); err != nil {
        return err
    }

    // Property 102: Nullable flag
    w.WritePropertyWithDefault(102, col.Nullable, true)

    // Property 103: Default value
    w.WritePropertyWithDefault(103, col.DefaultValue, nil)

    return nil
}
```

## Design Decisions

1. **Property-Based Format**: Matches DuckDB exactly, enables forward/backward compatibility
2. **Little-Endian**: Match DuckDB C++ implementation (most common architecture)
3. **CRC64 Checksums**: Fast, proven integrity checking
4. **Recursive Serialization**: Natural fit for nested types
5. **Optional Properties**: WritePropertyWithDefault skips defaults to save space

## Testing Strategy

### Unit Tests

- Test each TypeDetails serialization independently
- Test recursive types (3+ levels deep)
- Test endianness conversion
- Test checksum calculation

### Integration Tests

- Round-trip: Serialize → Deserialize → Compare
- Cross-implementation: Write with dukdb-go → Read with DuckDB C++
- Cross-implementation: Write with duckdb-go → Read with dukdb-go
- Real-world schemas: TPC-H, TPC-DS

### Compatibility Tests

```go
func TestRoundTripWithDuckDBGo(t *testing.T) {
    // Create TypeInfo with all type variations
    typeInfos := []TypeInfo{
        NewTypeInfo(TYPE_INTEGER),
        NewDecimalInfo(18, 4),
        NewEnumInfo("RED", "GREEN", "BLUE"),
        NewListInfo(NewStructInfo(
            StructEntry{"x", NewTypeInfo(TYPE_INTEGER)},
            StructEntry{"y", NewTypeInfo(TYPE_VARCHAR)},
        )),
        // ... more complex cases
    }

    for _, ti := range typeInfos {
        // Serialize
        buf := &bytes.Buffer{}
        SerializeTypeInfo(buf, ti)

        // Deserialize
        reconstructed, err := DeserializeTypeInfo(buf)
        require.NoError(t, err)

        // Verify exact match
        require.Equal(t, ti, reconstructed)
    }
}
```

## File Structure

```
internal/format/
├── duckdb_format.go        # Constants, magic numbers, version
├── binary_writer.go         # Property-based binary writer
├── binary_reader.go         # Property-based binary reader
├── typeinfo_serializer.go   # TypeInfo → binary
├── typeinfo_deserializer.go # Binary → TypeInfo
├── catalog_serializer.go    # Catalog → binary
├── catalog_deserializer.go  # Binary → Catalog
├── checksum.go              # CRC64 checksum utilities
└── format_test.go           # Comprehensive compatibility tests
```

## Performance Considerations

- **Lazy Property Reading**: Only deserialize properties when accessed
- **Property Map**: O(1) property lookup by ID
- **Streaming**: Support large catalogs without full memory load
- **Checksum Granularity**: Per-block checksums for incremental validation

## Error Handling

```go
var (
    ErrInvalidMagicNumber              = errors.New("invalid DuckDB magic number")
    ErrUnsupportedVersion              = errors.New("unsupported format version")
    ErrChecksumMismatch                = errors.New("checksum verification failed")
    ErrRequiredProperty                = errors.New("required property missing")
    ErrInvalidPropertyType             = errors.New("property type mismatch")
    ErrUnsupportedTypeForSerialization = errors.New("type not supported for serialization in this format version")
)
```
