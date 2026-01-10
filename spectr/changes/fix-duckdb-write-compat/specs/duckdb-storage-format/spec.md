# Spec Delta: duckdb-storage-format

## MODIFIED Requirements

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

## ADDED Requirements

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
