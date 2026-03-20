# Format Reading Specification

## Requirements

### Requirement: File Reading Architecture

The system MUST implement a file reading architecture to parse DuckDB v1.4.3 database files.

#### Scenario: Verify Architecture Components
- **Given** the file reader package
- **When** initialized
- **Then** it MUST contain components for file access, header parsing, block management, catalog reading, data reading, index reading, and statistics reading

### Requirement: File Header Reading

The system MUST correctly read and validate the DuckDB file header.

#### Scenario: Read and Validate Header
- **Given** a DuckDB database file
- **When** the header is read
- **Then** it MUST validate the magic number "DUCKDB_1.4.3"
- **And** it MUST validate the version (1.4.3)
- **And** it MUST verify the header checksum

#### Scenario: Detect Features
- **Given** a valid file header
- **When** features are detected
- **Then** it MUST identify block size, compression, encryption, checksum, and mmap flags

### Requirement: Block Reading

The system MUST implement block-level reading with checksum verification and decompression.

#### Scenario: Read Valid Block
- **Given** a valid block ID
- **When** ReadBlock is called
- **Then** it MUST read the block header and data
- **And** it MUST verify the block ID and checksum
- **And** it MUST decompress the data if compressed

#### Scenario: Handle Invalid Block
- **Given** a corrupted block or invalid ID
- **When** ReadBlock is called
- **Then** it MUST return an appropriate error

### Requirement: Catalog Reading

The system MUST parse the catalog structure including schemas, tables, and columns.

#### Scenario: Read Catalog Block
- **Given** a catalog block ID
- **When** ReadCatalog is called
- **Then** it MUST parse the catalog block and build the node tree
- **And** it MUST parse schemas, tables, views, and sequences

#### Scenario: Parse Table Metadata
- **Given** a table catalog node
- **When** parsed
- **Then** it MUST extract table name, ID, columns, constraints, and defaults

### Requirement: Data Reading

The system MUST read data chunks and deserialize vectors for supported types.

#### Scenario: Read Data Chunk
- **Given** a table ID and block IDs
- **When** ReadDataChunk is called
- **Then** it MUST read all blocks for the chunk
- **And** it MUST deserialize the chunk header and vectors

#### Scenario: Deserialize Vectors
- **Given** vector data
- **When** deserialized
- **Then** it MUST correctly handle types like INTEGER, VARCHAR (plain and dictionary encoded)
- **And** it MUST read validity masks

### Requirement: Index Reading

The system MUST support reading Hash and ART indexes.

#### Scenario: Read Hash Index
- **Given** hash index block IDs
- **When** ReadHashIndex is called
- **Then** it MUST reconstruct the hash index structure and buckets

### Requirement: Statistics Reading

The system MUST read table statistics to support query optimization.

#### Scenario: Read Table Statistics
- **Given** a table ID
- **When** ReadTableStatistics is called
- **Then** it MUST locate and deserialize the statistics block
- **And** it MUST cache the statistics

### Requirement: Corruption Detection

The system MUST implement integrity checks for the file and its components.

#### Scenario: Verify File Integrity
- **Given** a database file
- **When** VerifyFileIntegrity is called
- **Then** it MUST verify the header and all blocks
- **And** it MUST verify catalog consistency

### Requirement: Error Handling

The system MUST provide robust error handling and recovery strategies.

#### Scenario: Handle Corrupted Block
- **Given** a corrupted block
- **When** reading fails
- **Then** it MUST attempt to find a backup block
- **Or** it MUST skip the block if possible
- **Or** attempt partial recovery

### Requirement: Performance Optimization

The system MUST implement optimizations such as parallel reading and caching.

#### Scenario: Parallel Block Reading
- **Given** a list of block IDs
- **When** ReadBlocksParallel is called
- **Then** it MUST use multiple workers to read blocks concurrently

#### Scenario: Cache Blocks
- **Given** a read operation
- **When** a block is requested
- **Then** it MUST check the cache before reading from disk

