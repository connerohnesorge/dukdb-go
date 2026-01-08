# DuckDB BinarySerializer Format Design

## Overview

This document specifies DuckDB's BinarySerializer format used for catalog metadata serialization during checkpoints. This format enables cross-implementation compatibility between dukdb-go and DuckDB CLI.

## BinarySerializer Protocol

### Field ID Protocol

DuckDB's BinarySerializer writes each property as:
1. Field ID (uint16) - identifies the property
2. Property value - type-specific encoding

```
[field_id: uint16][value: varies]
```

### Message Terminator

Objects end with a special terminator field ID:

```go
const MESSAGE_TERMINATOR_FIELD_ID uint16 = 0xFFFF
```

### Varint Encoding

All integers use variable-length encoding for space efficiency:

```go
// VarInt encoding (matches DuckDB's VarIntEncode)
func VarIntEncode(w io.Writer, value uint64) error {
    for value >= 0x80 {
        w.Write([]byte{byte(value) | 0x80})
        value >>= 7
    }
    w.Write([]byte{byte(value)})
    return nil
}

func VarIntDecode(r io.Reader) (uint64, error) {
    var result uint64
    var shift uint
    for {
        b := make([]byte, 1)
        r.Read(b)
        result |= uint64(b[0]&0x7F) << shift
        if b[0]&0x80 == 0 {
            break
        }
        shift += 7
    }
    return result, nil
}
```

### Signed Integer Encoding

Signed integers use zigzag encoding before varint:

```go
func ZigZagEncode(n int64) uint64 {
    return uint64((n << 1) ^ (n >> 63))
}

func ZigZagDecode(n uint64) int64 {
    return int64((n >> 1) ^ -(n & 1))
}
```

## Catalog Serialization Format

### Checkpoint Structure

DuckDB's checkpoint writes catalog entries as:

```
Begin()
WriteList(100, "catalog_entries", count, func() {
    for each entry {
        WriteObject(func() {
            WriteEntry(entry)
        })
    }
})
End()
```

### WriteEntry Format

Each catalog entry is written as:

```go
// Property 99: CatalogType enum
serializer.WriteProperty(99, "catalog_type", entry.Type)

// Property 100: Entry-specific data (varies by type)
switch entry.Type {
case CATALOG_ENTRY_SCHEMA:
    serializer.WriteObject(100, "schema", func() {
        WriteCreateSchemaInfo(entry.schema_info)
    })
case CATALOG_ENTRY_TABLE:
    serializer.WriteObject(100, "table", func() {
        WriteCreateTableInfo(entry.table_info)
    })
    serializer.WriteProperty(101, "table_pointer", entry.pointer)
    serializer.WriteProperty(102, "total_rows", entry.rows)
// ... other types
}
```

### CatalogType Enum

```go
const (
    CATALOG_ENTRY_INVALID     uint8 = 0
    CATALOG_ENTRY_TABLE       uint8 = 1
    CATALOG_ENTRY_SCHEMA      uint8 = 2
    CATALOG_ENTRY_VIEW        uint8 = 3
    CATALOG_ENTRY_INDEX       uint8 = 4
    CATALOG_ENTRY_PREPARED    uint8 = 5
    CATALOG_ENTRY_SEQUENCE    uint8 = 6
    CATALOG_ENTRY_MACRO       uint8 = 7
    CATALOG_ENTRY_TABLE_MACRO uint8 = 8
    CATALOG_ENTRY_TYPE        uint8 = 9
    CATALOG_ENTRY_DATABASE    uint8 = 10
    CATALOG_ENTRY_DEPENDENCY  uint8 = 11
    CATALOG_ENTRY_DUCK_INDEX  uint8 = 12
    CATALOG_ENTRY_SECRET      uint8 = 13
)
```

## CreateInfo Serialization

### CreateSchemaInfo

```
Property 100: type (always "schema")
Property 101: catalog name (string)
Property 102: schema name (string)
Property 103: on_conflict (OnCreateConflict enum)
Property 104: comment (string, optional)
Property 105: tags (map<string,string>, optional)
MESSAGE_TERMINATOR (0xFFFF)
```

### CreateTableInfo

```
Property 100: type (always "table")
Property 101: catalog name (string)
Property 102: schema name (string)
Property 103: table name (string)
Property 104: on_conflict (OnCreateConflict enum)
Property 105: temporary (bool)
Property 200: columns (list<ColumnDefinition>)
Property 201: constraints (list<Constraint>)
Property 202: query (optional, for CREATE TABLE AS)
MESSAGE_TERMINATOR (0xFFFF)
```

### ColumnDefinition

```
Property 100: name (string)
Property 101: type (LogicalType)
Property 102: expression (optional, for generated columns)
Property 103: category (ColumnCategory enum)
Property 104: compression_type (optional)
Property 105: comment (string, optional)
Property 106: tags (map<string,string>, optional)
MESSAGE_TERMINATOR (0xFFFF)
```

### LogicalType Serialization

```
Property 100: type_id (LogicalTypeId enum as uint8)
Property 101: physical_type (PhysicalType enum as uint8)
Property 200: type_info (ExtraTypeInfo, type-specific)
MESSAGE_TERMINATOR (0xFFFF)
```

## Metadata Block System

### Block Layout

DuckDB divides each storage block (256KB) into 64 metadata blocks:

```
Storage Block (262144 bytes)
├── Metadata Block 0 (4096 bytes)
│   └── [next_block_id: 8 bytes][data: 4088 bytes]
├── Metadata Block 1 (4096 bytes)
│   └── [next_block_id: 8 bytes][data: 4088 bytes]
├── ...
└── Metadata Block 63 (4096 bytes)
```

### MetaBlockPointer Encoding

```go
// MetaBlockPointer encodes both block_id and block_index
// Lower 56 bits: block_id (physical storage block)
// Upper 8 bits: block_index (which of 64 metadata sub-blocks)

func EncodeMetaBlockPointer(blockID uint64, blockIndex uint8) uint64 {
    return blockID | (uint64(blockIndex) << 56)
}

func DecodeMetaBlockPointer(ptr uint64) (blockID uint64, blockIndex uint8) {
    blockID = ptr & 0x00FFFFFFFFFFFFFF
    blockIndex = uint8(ptr >> 56)
    return
}
```

### Next Block Chain

Each metadata block starts with an 8-byte pointer to the next block:
- `0xFFFFFFFFFFFFFFFF` = no next block (last in chain)
- Otherwise = pointer to continuation block

## BinarySerializer Implementation

### Writer Structure

```go
type BinarySerializer struct {
    w         io.Writer
    debugMode bool
}

func (s *BinarySerializer) OnPropertyBegin(fieldID uint16, tag string) {
    // Write field ID as little-endian uint16
    binary.Write(s.w, binary.LittleEndian, fieldID)
}

func (s *BinarySerializer) OnObjectBegin() {
    // No header for objects
}

func (s *BinarySerializer) OnObjectEnd() {
    // Write message terminator
    binary.Write(s.w, binary.LittleEndian, MESSAGE_TERMINATOR_FIELD_ID)
}

func (s *BinarySerializer) OnListBegin(count uint64) {
    // Write count as varint
    VarIntEncode(s.w, count)
}

func (s *BinarySerializer) WriteValue(value bool) {
    var b byte
    if value {
        b = 1
    }
    s.w.Write([]byte{b})
}

func (s *BinarySerializer) WriteValue(value uint8) {
    VarIntEncode(s.w, uint64(value))
}

func (s *BinarySerializer) WriteValue(value uint64) {
    VarIntEncode(s.w, value)
}

func (s *BinarySerializer) WriteValue(value string) {
    // Length-prefixed string
    VarIntEncode(s.w, uint64(len(value)))
    s.w.Write([]byte(value))
}
```

### Property Methods

```go
func (s *BinarySerializer) WriteProperty(fieldID uint16, tag string, value interface{}) {
    s.OnPropertyBegin(fieldID, tag)
    s.writeValue(value)
}

func (s *BinarySerializer) WritePropertyWithDefault(fieldID uint16, tag string, value, defaultValue interface{}) {
    if reflect.DeepEqual(value, defaultValue) {
        return // Skip property if equals default
    }
    s.WriteProperty(fieldID, tag, value)
}

func (s *BinarySerializer) WriteObject(fieldID uint16, tag string, fn func()) {
    s.OnPropertyBegin(fieldID, tag)
    s.OnObjectBegin()
    fn()
    s.OnObjectEnd()
}

func (s *BinarySerializer) WriteList(fieldID uint16, tag string, count uint64, fn func(idx uint64)) {
    s.OnPropertyBegin(fieldID, tag)
    s.OnListBegin(count)
    for i := uint64(0); i < count; i++ {
        fn(i)
    }
}
```

## Example: Simple Table Serialization

For a table `CREATE TABLE test (id INTEGER, name VARCHAR)`:

```
// Catalog entries list
Field 100: "catalog_entries"
  VarInt: 2 (count: 1 schema + 1 table)

  // Entry 0: Schema
  Object:
    Field 99: VarInt(2) // CATALOG_ENTRY_SCHEMA
    Field 100: Object   // CreateSchemaInfo
      Field 100: "schema" (type)
      Field 101: "" (catalog)
      Field 102: "main" (schema)
      Field 103: VarInt(0) // IGNORE_ON_CONFLICT
      0xFFFF // terminator
    0xFFFF // terminator

  // Entry 1: Table
  Object:
    Field 99: VarInt(1) // CATALOG_ENTRY_TABLE
    Field 100: Object   // CreateTableInfo
      Field 100: "table" (type)
      Field 101: "" (catalog)
      Field 102: "main" (schema)
      Field 103: "test" (table)
      Field 104: VarInt(0) // IGNORE_ON_CONFLICT
      Field 105: 0 (not temporary)
      Field 200: List(2) // columns
        Object: // column 0
          Field 100: "id"
          Field 101: Object // LogicalType INTEGER
            Field 100: VarInt(4) // TYPE_INTEGER
            0xFFFF
          0xFFFF
        Object: // column 1
          Field 100: "name"
          Field 101: Object // LogicalType VARCHAR
            Field 100: VarInt(17) // TYPE_VARCHAR
            0xFFFF
          0xFFFF
      Field 201: List(0) // no constraints
      0xFFFF // terminator
    Field 101: MetaBlockPointer // table_pointer
    Field 102: VarInt(0) // total_rows
    0xFFFF // terminator
```

## Testing Strategy

### Hex Dump Verification

```go
func TestBinarySerializerOutput(t *testing.T) {
    // Create identical structures
    // Serialize with our implementation
    // Compare byte-for-byte with DuckDB reference output
}
```

### Round-Trip Testing

```go
func TestDuckDBCLICompatibility(t *testing.T) {
    // Create database with dukdb-go
    // Open with DuckDB CLI
    // Verify SHOW TABLES output
    // Verify SELECT queries work
}
```

### Edge Cases

- Empty catalog (no entries)
- Unicode table/column names
- Maximum column counts
- Deeply nested types (LIST of STRUCT of MAP)
- All supported data types

## Implementation Order

1. `varint.go` - Varint encoding/decoding
2. `binary_serializer.go` - Core serializer with field IDs
3. `catalog_serializer.go` - Catalog entry serialization
4. `metadata_block.go` - Metadata sub-block management
5. Integration with `catalog_writer.go`
6. Tests and verification
