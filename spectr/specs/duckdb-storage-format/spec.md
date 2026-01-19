# Duckdb Storage Format Specification

## Requirements

### Requirement: DuckDB File Detection

The system MUST detect DuckDB files by magic bytes and handle them appropriately.

#### Scenario: Detect DuckDB file by magic bytes
- **Given** a file exists at the specified path
- **When** the engine opens the file
- **Then** it reads bytes at offset 8-11
- **And** if bytes equal "DUCK", treats as DuckDB format
- **And** otherwise treats as native dukdb-go format

#### Scenario: Handle non-existent file
- **Given** no file exists at the specified path
- **When** the engine opens with create mode
- **Then** creates new file in configured format (default: duckdb)

---

### Requirement: DuckDB File Header Reading

The system MUST read and validate DuckDB v1.4.3 file headers.

#### Scenario: Read valid header
- **Given** a DuckDB file with valid header
- **When** the file is opened
- **Then** magic bytes "DUCK" are validated
- **And** version number is extracted
- **And** metadata block pointer is extracted
- **And** header checksum is validated

#### Scenario: Reject invalid magic bytes
- **Given** a file without "DUCK" magic bytes at offset 8
- **When** attempting to open as DuckDB format
- **Then** return error "not a valid DuckDB file"

#### Scenario: Reject unsupported version
- **Given** a DuckDB file with version > 64
- **When** the file is opened
- **Then** return error indicating unsupported version

---

### Requirement: Block Management

The system MUST read and write 256KB blocks with checksum validation.

#### Scenario: Read block with valid checksum
- **Given** a block exists in the file
- **When** the block is read
- **Then** data is loaded
- **And** checksum is computed
- **And** checksum matches stored value

#### Scenario: Detect corrupted block
- **Given** a block with corrupted data
- **When** the block is read
- **Then** checksum validation fails
- **And** return error indicating corruption

#### Scenario: Write block with checksum
- **Given** data to write
- **When** a block is written
- **Then** checksum is computed
- **And** checksum is stored with block
- **And** data is written to file

#### Scenario: Cache recently read blocks
- **Given** a block was recently read
- **When** the same block is requested again
- **Then** return cached copy without disk I/O

---

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

### Requirement: Type Mapping

The system MUST correctly map all DuckDB types to dukdb-go types.

#### Scenario: Map primitive types
- **Given** a column with INTEGER type
- **When** the column is read
- **Then** values are TYPE_INTEGER in dukdb-go

#### Scenario: Map complex types
- **Given** a column with LIST(INTEGER) type
- **When** the column is read
- **Then** values are TYPE_LIST with TYPE_INTEGER child

#### Scenario: Map decimal with precision
- **Given** a column with DECIMAL(18,4) type
- **When** the column is read
- **Then** values are TYPE_DECIMAL with width=18, scale=4

#### Scenario: Handle all 43 types
- **Given** columns of each supported type
- **When** the table is read
- **Then** all types are correctly mapped

---

### Requirement: Decompression Support

The system MUST decompress columns using DuckDB compression algorithms.

#### Scenario: Decompress CONSTANT
- **Given** a column compressed with CONSTANT
- **When** the column is read
- **Then** all rows have the same stored value

#### Scenario: Decompress RLE
- **Given** a column compressed with RLE
- **When** the column is read
- **Then** runs are expanded to individual values

#### Scenario: Decompress DICTIONARY
- **Given** a column compressed with DICTIONARY
- **When** the column is read
- **Then** indices are looked up in dictionary
- **And** original values are returned

#### Scenario: Decompress BITPACKING
- **Given** a column compressed with BITPACKING
- **When** the column is read
- **Then** bits are unpacked to original integers

#### Scenario: Decompress FOR
- **Given** a column compressed with FOR
- **When** the column is read
- **Then** reference is added to each delta
- **And** original values are returned

#### Scenario: Handle unsupported compression
- **Given** a column with ALP compression (unsupported)
- **When** the column is read
- **Then** return error with compression type name

---

### Requirement: Row Group Reading

The system MUST read row groups efficiently with lazy loading.

#### Scenario: Lazy load row groups
- **Given** a table with multiple row groups
- **When** the table is opened
- **Then** row group metadata is loaded
- **And** row group data is NOT loaded

#### Scenario: Load row group on demand
- **Given** a scan requests specific rows
- **When** those rows are in an unloaded row group
- **Then** that row group is loaded
- **And** data is returned

#### Scenario: Apply projection pushdown
- **Given** a scan requests only 2 columns
- **When** the scan executes
- **Then** only those 2 columns are decompressed
- **And** other columns remain compressed

---

### Requirement: NULL Handling

The system MUST correctly handle NULL values using validity masks.

#### Scenario: Read NULLs from validity mask
- **Given** a column with NULL values
- **When** the column is read
- **Then** NULL positions match validity mask
- **And** NULL values return nil in Go

#### Scenario: Write NULLs to validity mask
- **Given** data with nil values
- **When** the column is written
- **Then** validity mask marks NULL positions
- **And** data bytes may be arbitrary for NULL positions

---

### Requirement: File Writing

The system MUST write DuckDB-compatible files with correct checksums and metadata format.

#### Scenario: Write file with valid XXH64 checksums
- **Given** a database with tables and data
- **When** the file is checkpointed
- **Then** all block checksums MUST be valid XXH64 with seed 0
- **And** DuckDB CLI MUST validate checksums without error

#### Scenario: Write metadata blocks with correct format
- **Given** catalog metadata to persist
- **When** metadata is written
- **Then** MetaBlockPointer encoding MUST use 56-bit block ID + 8-bit index
- **And** sub-blocks MUST be 4096 bytes each
- **And** block chaining MUST use correct next_block pointers

#### Scenario: Write storage metadata after catalog entries
- **Given** a table with data
- **When** catalog is serialized
- **Then** field 101 (table_pointer) MUST be written after table entry
- **And** field 102 (total_rows) MUST contain correct row count

---

### Requirement: DuckDB CLI Compatibility

The system MUST produce files readable by DuckDB CLI v1.1+.

#### Scenario: DuckDB CLI opens dukdb-go file without errors
- **Given** a database file created by dukdb-go with tables
- **When** opened in DuckDB CLI
- **Then** no checksum errors MUST occur
- **And** no metadata errors MUST occur

#### Scenario: DuckDB CLI queries dukdb-go tables
- **Given** a database file created by dukdb-go
- **When** DuckDB CLI runs `SELECT * FROM table`
- **Then** all rows MUST be returned correctly
- **And** all values MUST match what was inserted

#### Scenario: Round-trip modification preserves data
- **Given** a database file created by dukdb-go
- **When** DuckDB CLI inserts additional rows
- **And** file is reopened in dukdb-go
- **Then** both original and new rows MUST be visible

---

### Requirement: Storage Backend Selection

The system MUST support configurable storage backend with auto-detection.

#### Scenario: Auto-detect storage format
- **Given** a file path is provided
- **When** the engine opens the database
- **Then** check for DuckDB magic bytes
- **And** use appropriate backend

#### Scenario: Configure storage format explicitly
- **Given** config specifies storage_format="duckdb"
- **When** a new database is created
- **Then** use DuckDB format

#### Scenario: Memory database uses native format
- **Given** path is ":memory:"
- **When** database is opened
- **Then** use in-memory storage (no file format)

---

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

### Requirement: Checksum Algorithm Compatibility

The system MUST compute XXH64 checksums identically to DuckDB.

#### Scenario: XXH64 output matches DuckDB
- **Given** a block of known data
- **When** checksum is computed
- **Then** result MUST match DuckDB's XXH64 output for same data
- **And** seed value MUST be 0
- **And** byte order MUST be little-endian

---

### Requirement: Database Header Format Compatibility

The system MUST write database headers in DuckDB-compatible format.

#### Scenario: Dual header slots at correct positions
- **Given** a database file
- **When** checkpoint occurs
- **Then** headers MUST exist at offsets 4096 (H1) and 8192 (H2)
- **And** iteration counter MUST alternate between headers

#### Scenario: Header field layout matches DuckDB
- **Given** a database header
- **When** written to file
- **Then** meta_block pointer MUST be at correct offset
- **And** free_list pointer MUST be at correct offset
- **And** block_count MUST be at correct offset

---

### Requirement: Data Verification Testing

The system MUST include comprehensive tests verifying row data can be read correctly from DuckDB CLI created files.

#### Scenario: Verify integer data from DuckDB CLI file
- **Given** a DuckDB file created by DuckDB CLI with INTEGER column
- **When** rows are scanned using dukdb-go
- **Then** values MUST match those inserted by DuckDB CLI exactly

#### Scenario: Verify string data preserves Unicode
- **Given** a DuckDB file with VARCHAR containing multi-byte UTF-8 (emoji, CJK)
- **When** the column is read
- **Then** string values MUST be byte-identical to original

#### Scenario: Verify all compression algorithms produce correct values
- **Given** columns compressed with CONSTANT, RLE, DICTIONARY, BITPACKING
- **When** data is decompressed and read
- **Then** values MUST match original inserted data

#### Scenario: Verify NULL positions from validity mask
- **Given** a column with NULL values at known positions
- **When** the column is read
- **Then** NULL positions MUST match the original data exactly

#### Scenario: Verify all 43 data types round-trip
- **Given** DuckDB files with columns of each supported type
- **When** data is read
- **Then** all types MUST be correctly mapped with value preservation
