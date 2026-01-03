## ADDED Requirements

### Requirement: DuckDB File Format Support

The system SHALL use the official DuckDB file format for persistent storage, enabling compatibility with the DuckDB ecosystem.

**Changes from base spec**:
- ADDED: DuckDB magic number (`DUCK`) support
- ADDED: Dual 4KB rotating header blocks for crash safety
- ADDED: Format version 64+ support
- ADDED: Binary property-based catalog serialization
- ADDED: Support for advanced compression algorithms

#### Scenario: Open DuckDB file
- GIVEN a file created by official DuckDB with `DUCK` magic number
- WHEN opening the file with dukdb-go
- THEN the header is parsed successfully
- AND catalog is deserialized using DuckDB binary format
- AND data is readable using DuckDB row group format
- AND the database is fully functional

#### Scenario: Create new DuckDB file
- GIVEN no existing database file
- WHEN opening a new database file
- THEN a file with `DUCK` magic number is created
- AND dual 4KB header blocks are initialized
- AND format version 64 is written
- AND all subsequent writes use DuckDB format

#### Scenario: Read dual header blocks
- GIVEN a DuckDB file with two header blocks
- WHEN reading the file header
- THEN both Block A and Block B are read
- AND the newer header (based on salt) is selected
- AND file corruption is detected if headers are inconsistent

#### Scenario: Write header atomically
- GIVEN changes to header metadata
- WHEN writing the updated header
- THEN Block A is written to a temp file
- AND Block B is written to a temp file with flipped salt
- AND Block A is atomically renamed to the target location
- AND Block B is atomically renamed as backup
- AND crash during write does not corrupt existing header

### Requirement: DuckDB Magic Number

The system SHALL use the `DUCK` magic number (0x4455434B) for file identification.

#### Scenario: Magic number detection
- GIVEN a file with bytes `DUCK` at offset 0
- WHEN opening the file
- THEN the file is recognized as a DuckDB-compatible file
- AND format version is read from header

#### Scenario: Invalid magic number
- GIVEN a file without `DUCK` magic number
- WHEN opening the file
- THEN an error indicating unsupported format is returned

### Requirement: Dual Header Block System

The system SHALL implement dual 4KB rotating header blocks for crash-safe updates.

#### Scenario: Header structure validation
- GIVEN a DuckDB file header
- WHEN validating the header structure
- THEN header size is exactly 4096 bytes
- AND checksum covers the header data
- AND salt values are consistent between blocks

#### Scenario: Crash recovery with header blocks
- GIVEN a database file with Block A at offset 0 and Block B at offset 4096
- AND Block A has salt 100, Block B has salt 99
- WHEN reading headers
- THEN Block A is selected as the newer header
- AND database opens successfully

#### Scenario: Header write failure recovery
- GIVEN a database file
- AND a header write is in progress
- WHEN the write fails before completion
- THEN the original header remains intact
- AND temporary header files are cleaned up

### Requirement: DuckDB Format Version Support

The system SHALL support reading and writing DuckDB format version 64 and above.

#### Scenario: Version negotiation
- GIVEN a DuckDB file with version 64
- WHEN reading the file
- THEN version 64 is accepted and processed correctly

#### Scenario: Future version handling
- GIVEN a DuckDB file with a version greater than current support
- WHEN reading the file
- THEN a warning is logged about potential compatibility issues
- AND reading proceeds if the version is backward compatible

### Requirement: Binary Property-Based Catalog Serialization

The system SHALL use DuckDB's binary property-based format for catalog serialization.

#### Scenario: Catalog serialization
- GIVEN a catalog with schemas, tables, and columns
- WHEN serializing to disk
- THEN properties are written using property IDs (not JSON)
- AND each property has a varint ID followed by type-specific encoding

#### Scenario: Catalog deserialization
- GIVEN serialized catalog data in DuckDB binary format
- WHEN deserializing the catalog
- THEN properties are read by their IDs
- AND all type information (including nested types) is reconstructed

#### Scenario: Union type support
- GIVEN a table with a UNION type column
- WHEN serializing the catalog
- THEN UnionType is serialized with member count and member types
- AND deserialization reconstructs the UnionType correctly

#### Scenario: New type support
- GIVEN a table with TIME_TZ or TIMESTAMP_TZ columns
- WHEN serializing the catalog
- THEN these types are correctly serialized
- AND deserialization reconstructs the types correctly

## MODIFIED Requirements

### Requirement: Write-Ahead Log Entry Format

The Write-Ahead Log SHALL use DuckDB's WAL format for compatibility.

**Previous**: Custom binary format with CRC64 checksums
**Updated**: DuckDB WAL format with version 3 entries

#### Scenario: DuckDB WAL entry structure
- GIVEN a WAL entry for INSERT operation
- WHEN serializing the entry
- THEN entry header includes: type, flags, length, sequence number
- AND entry payload follows DuckDB serialization format
- AND checksum is calculated over the entry

#### Scenario: WAL recovery with DuckDB format
- GIVEN a WAL file created by DuckDB
- WHEN performing recovery
- THEN entries are parsed using DuckDB WAL format
- AND committed transactions are replayed correctly

### Requirement: Persistent Storage

Persistent storage SHALL use DuckDB file format.

**Previous**: Simple block-based storage
**Updated**: DuckDB header with `DUCK` magic and dual 4KB blocks

#### Scenario: Persistent file structure
- GIVEN a persistent database file
- WHEN writing data to disk
- THEN file structure follows DuckDB format:
  - Block A (4096 bytes)
  - Block B (4096 bytes)
  - Metadata (property-based)
  - Data storage (row groups)

#### Scenario: In-memory mode unchanged
- GIVEN path `:memory:`
- WHEN opening database
- THEN no file is created
- AND in-memory behavior is unchanged
