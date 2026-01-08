# Duckdb Storage Format Specification - Delta

## ADDED Requirements

### Requirement: BinarySerializer Protocol

The system MUST use DuckDB's BinarySerializer protocol for catalog metadata serialization.

#### Scenario: Write field ID before property value
- **Given** a property with field ID 100 and string value "test"
- **When** serialized using BinarySerializer
- **Then** output is: [0x64, 0x00] (field ID as uint16 LE) + [varint length] + "test"

#### Scenario: Write message terminator at object end
- **Given** an object being serialized
- **When** the object serialization completes
- **Then** output includes [0xFF, 0xFF] (MESSAGE_TERMINATOR_FIELD_ID)

#### Scenario: Write list with varint count
- **Given** a list with 3 elements
- **When** serialized using BinarySerializer
- **Then** output starts with varint(3) followed by element data

#### Scenario: Skip property with default value
- **Given** a property with value equal to its default
- **When** WritePropertyWithDefault is called
- **Then** no bytes are written for that property

---

### Requirement: Varint Encoding

The system MUST encode integers using variable-length encoding matching DuckDB.

#### Scenario: Encode small integer (0-127)
- **Given** value 42
- **When** encoded as varint
- **Then** output is single byte [0x2A]

#### Scenario: Encode medium integer (128-16383)
- **Given** value 300
- **When** encoded as varint
- **Then** output is two bytes [0xAC, 0x02]

#### Scenario: Encode large integer
- **Given** value 2^63 - 1
- **When** encoded as varint
- **Then** output uses 9 bytes with continuation bits

#### Scenario: Round-trip varint encoding
- **Given** any uint64 value
- **When** encoded then decoded
- **Then** original value is recovered exactly

---

### Requirement: Catalog Entry Serialization

The system MUST serialize catalog entries using DuckDB's property-based format.

#### Scenario: Serialize schema entry
- **Given** a schema entry for schema "main"
- **When** serialized
- **Then** property 99 contains CATALOG_ENTRY_SCHEMA (2)
- **And** property 100 contains CreateSchemaInfo object
- **And** object ends with MESSAGE_TERMINATOR

#### Scenario: Serialize table entry
- **Given** a table entry for table "users" with 2 columns
- **When** serialized
- **Then** property 99 contains CATALOG_ENTRY_TABLE (1)
- **And** property 100 contains CreateTableInfo object
- **And** property 101 contains MetaBlockPointer
- **And** property 102 contains total_rows count

#### Scenario: Serialize catalog entries list
- **Given** a catalog with 1 schema and 2 tables
- **When** checkpoint is written
- **Then** field 100 contains list with 3 entries
- **And** entries are ordered: schema first, then tables

---

### Requirement: CreateTableInfo Serialization

The system MUST serialize CreateTableInfo with correct property IDs.

#### Scenario: Serialize basic table info
- **Given** CreateTableInfo for table "test" in schema "main"
- **When** serialized
- **Then** property 100 = "table" (type)
- **And** property 101 = "" (catalog)
- **And** property 102 = "main" (schema)
- **And** property 103 = "test" (table name)
- **And** property 200 = columns list

#### Scenario: Serialize table with columns
- **Given** table with columns (id INTEGER, name VARCHAR)
- **When** serialized
- **Then** property 200 contains list of 2 ColumnDefinitions
- **And** each ColumnDefinition has name(100), type(101)

#### Scenario: Serialize column types correctly
- **Given** column with type INTEGER
- **When** LogicalType is serialized
- **Then** property 100 = TYPE_INTEGER enum value (4)

---

### Requirement: Metadata Block System

The system MUST use DuckDB's metadata sub-block allocation system.

#### Scenario: Allocate metadata block
- **Given** metadata needs to be written
- **When** metadata block is allocated
- **Then** storage block is divided into 64 sub-blocks
- **And** sub-block size is 4096 bytes

#### Scenario: Encode MetaBlockPointer
- **Given** storage block ID 5 and sub-block index 10
- **When** encoded as MetaBlockPointer
- **Then** lower 56 bits = 5 (block ID)
- **And** upper 8 bits = 10 (sub-block index)

#### Scenario: Chain metadata blocks
- **Given** metadata larger than 4088 bytes (4096 - 8 byte header)
- **When** metadata is written
- **Then** data spans multiple sub-blocks
- **And** each block header contains pointer to next block
- **And** last block header contains INVALID_BLOCK (-1)

---

### Requirement: DuckDB CLI Compatibility with Catalog

The system MUST produce catalog metadata readable by DuckDB CLI.

#### Scenario: DuckDB CLI reads table list
- **Given** a database file with table "users" created by dukdb-go
- **When** DuckDB CLI executes "SHOW TABLES;"
- **Then** output includes "users"

#### Scenario: DuckDB CLI describes table
- **Given** a database file with table "users (id INTEGER, name VARCHAR)"
- **When** DuckDB CLI executes "DESCRIBE users;"
- **Then** output shows column "id" with type "INTEGER"
- **And** output shows column "name" with type "VARCHAR"

#### Scenario: DuckDB CLI queries empty table
- **Given** a database file with empty table "users"
- **When** DuckDB CLI executes "SELECT * FROM users;"
- **Then** query returns empty result set without error

---

## MODIFIED Requirements

### Requirement: Catalog Loading

The system MUST load catalog metadata from DuckDB files using BinaryDeserializer.

#### Scenario: Load table definitions using BinaryDeserializer
- **Given** a DuckDB file with tables serialized using BinarySerializer
- **When** the catalog is loaded
- **Then** BinaryDeserializer reads field IDs and values
- **And** all table schemas are available
- **And** column names and types are correct

#### Scenario: Handle message terminator during read
- **Given** serialized catalog data with MESSAGE_TERMINATOR
- **When** deserializing an object
- **Then** reading stops at field ID 0xFFFF
- **And** object is fully reconstructed

---

### Requirement: File Writing

The system MUST write catalog using BinarySerializer format.

#### Scenario: Write catalog with BinarySerializer
- **Given** tables have been created
- **When** the database is checkpointed
- **Then** catalog is serialized using BinarySerializer
- **And** field IDs precede each property value
- **And** objects end with MESSAGE_TERMINATOR
- **And** header MetaBlock points to metadata
