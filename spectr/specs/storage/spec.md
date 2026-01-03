# Storage Specification

## Requirements

### Requirement: DuckDB Row Group Format

The storage layer SHALL use DuckDB's row group format for persistent data storage.

**Changes from base spec**:
- ADDED: Row group structure with variable row counts
- ADDED: Column segment storage with compression metadata
- ADDED: Per-column compression selection
- ADDED: Support for ART index persistence

#### Scenario: Row group structure
- GIVEN table data to persist
- WHEN writing to persistent storage
- THEN data is organized into row groups
- AND each row group contains column segments
- AND row group metadata includes row count, column count, and start ID

#### Scenario: Column segment format
- GIVEN a column with data
- WHEN creating a column segment
- THEN segment includes:
  - Segment metadata (type, size, block ID, offset, length)
  - Compression type identifier
  - Compressed data bytes
  - Optional validity bitmap

#### Scenario: Variable row group size
- GIVEN a table with 1,000,000 rows
- WHEN writing to persistent storage
- THEN row groups are created with optimal sizes
- AND each row group contains up to 122,880 rows (typical DuckDB setting)
- AND final row group contains remaining rows

### Requirement: Column Segment Compression

The system SHALL support per-column compression algorithms for efficient storage.

#### Scenario: Compression selection
- GIVEN a column of integers
- WHEN selecting compression
- THEN BitPacking is selected for integer types
- AND RLE is selected for date/time types
- AND FSST is selected for varchar types

#### Scenario: RLE compression
- GIVEN column data with repeated values
- WHEN compressing with RLE
- THEN output contains run-length encoded sequences
- AND each run includes: value, count
- AND decompression reconstructs original data

#### Scenario: FSST compression
- GIVEN varchar column data
- WHEN compressing with FSST
- THEN symbol table is trained on sample data
- AND data is compressed using static symbol encoding
- AND decompression uses the same symbol table

#### Scenario: Chimp compression for floats
- GIVEN double column data
- WHEN compressing with Chimp
- THEN floating-point data is compressed efficiently
- AND NaN and infinity values are handled specially

#### Scenario: Zstd compression
- GIVEN large data blocks
- WHEN compressing with Zstd
- THEN high compression ratio is achieved
- AND streaming compression is used for large blocks

### Requirement: ART Index Persistence

The system SHALL persist Adaptive Radix Tree (ART) indexes to support primary key and unique constraints.

#### Scenario: ART index structure
- GIVEN a primary key index on a table
- WHEN persisting the index
- THEN index nodes are serialized in ART format
- AND node types (inner, leaf, nullptr) are preserved
- AND key encodings for all types are supported

#### Scenario: Index serialization
- GIVEN an ART index with various key types
- WHEN serializing to disk
- THEN each node type is serialized with its specific format
- AND children are referenced by block ID and offset
- AND serialized index is compact

#### Scenario: Index deserialization
- GIVEN serialized ART index data
- WHEN loading the index
- THEN nodes are reconstructed in memory
- AND index is fully functional for lookups

#### Scenario: Index storage in row groups
- GIVEN a table with indexes
- WHEN checkpointing
- THEN index data is stored alongside table data
- AND index blocks are referenced from row group metadata

### Requirement: Block Storage

The storage layer SHALL use DuckDB's block storage system for data blocks.

#### Scenario: Block allocation
- GIVEN data to write
- WHEN allocating blocks
- THEN free block list is consulted
- AND existing free blocks are reused when available
- AND new blocks are allocated from file end

#### Scenario: Block reference
- GIVEN column segment data
- WHEN storing the segment
- THEN segment references block ID and offset
- AND multiple segments can share blocks (with offset tracking)

#### Scenario: Free list management
- GIVEN deleted or updated data
- WHEN blocks become free
- THEN block IDs are added to free list
- AND free list is persisted in file header

### Requirement: Data Chunk Persistence (DuckDB)

Data chunks SHALL be persisted using DuckDB row group format.

**Note**: Replaces legacy custom chunk format with DuckDB row group with column segments and compression.

#### Scenario: Chunk to row group conversion
- GIVEN a DataChunk with multiple columns
- WHEN converting to persistent format
- THEN each column becomes a column segment
- AND compression is applied per column
- AND row group metadata is written

#### Scenario: Row group to chunk conversion
- GIVEN persisted row group data
- WHEN loading into memory
- THEN column segments are read
- AND compression is decompressed
- AND DataChunk is reconstructed with original types

### Requirement: Vector Persistence (DuckDB)

Vectors SHALL be persisted using column segment format.

**Note**: Replaces direct vector serialization with storage in column segments with compression.

#### Scenario: Vector serialization
- GIVEN a Vector with validity mask and data
- WHEN serializing to column segment
- THEN validity mask is stored as packed bits
- AND data is compressed using appropriate algorithm
- AND segment metadata records compression type

#### Scenario: Vector deserialization
- GIVEN a compressed column segment
- WHEN loading into Vector
- THEN compression is decompressed
- AND validity mask is unpacked
- AND Vector is reconstructed with correct type

### Requirement: TypeInfo Persistence (DuckDB)

Type information SHALL be persisted using binary property-based serialization.

**Note**: Replaces JSON serialization with DuckDB binary property-based format.

#### Scenario: Struct type serialization
- GIVEN a StructType with named children
- WHEN serializing
- THEN children are serialized recursively
- AND child names are preserved with property IDs
- AND deserialization reconstructs complete type structure

#### Scenario: List type serialization
- GIVEN a ListType with child type
- WHEN serializing
- THEN child type is serialized
- AND list type ID is written
- AND deserialization creates correct ListType

#### Scenario: Map type serialization
- GIVEN a MapType with key and value types
- WHEN serializing
- THEN both key and value types are serialized
- AND map type ID is written
- AND deserialization creates correct MapType

