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

The system MUST load catalog metadata from DuckDB files.

#### Scenario: Load table definitions
- **Given** a DuckDB file with tables
- **When** the catalog is loaded
- **Then** all table schemas are available
- **And** column names and types are correct
- **And** constraints are preserved

#### Scenario: Load view definitions
- **Given** a DuckDB file with views
- **When** the catalog is loaded
- **Then** view definitions are available
- **And** underlying queries are preserved

#### Scenario: Load index definitions
- **Given** a DuckDB file with indexes
- **When** the catalog is loaded
- **Then** index metadata is available
- **And** indexed columns are identified

#### Scenario: Load sequence state
- **Given** a DuckDB file with sequences
- **When** the catalog is loaded
- **Then** sequence current values are correct
- **And** increment settings are preserved

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

The system MUST write DuckDB-compatible files.

#### Scenario: Write file header
- **Given** a new database file
- **When** the file is created
- **Then** header includes "DUCK" magic bytes
- **And** version matches v1.4.3
- **And** checksum is valid

#### Scenario: Write catalog
- **Given** tables have been created
- **When** the database is checkpointed
- **Then** catalog is serialized to metadata blocks
- **And** header points to metadata

#### Scenario: Write row groups
- **Given** data has been inserted
- **When** the database is checkpointed
- **Then** row groups are written with compression
- **And** catalog references row groups

#### Scenario: Compression selection
- **Given** data is being written
- **When** compression is selected
- **Then** appropriate algorithm is chosen based on data characteristics
- **And** file size is reasonable

---

### Requirement: DuckDB CLI Compatibility

The system MUST produce files readable by DuckDB v1.4.3.

#### Scenario: DuckDB reads dukdb-go file
- **Given** a database file created by dukdb-go
- **When** opened in DuckDB CLI v1.4.3
- **Then** database opens without error
- **And** all tables are visible
- **And** all data is queryable

#### Scenario: dukdb-go reads DuckDB file
- **Given** a database file created by DuckDB CLI v1.4.3
- **When** opened in dukdb-go
- **Then** database opens without error
- **And** all tables are visible
- **And** all data is queryable

#### Scenario: Round-trip data integrity
- **Given** a database created in DuckDB
- **When** modified in dukdb-go
- **And** reopened in DuckDB
- **Then** all data is preserved
- **And** no corruption occurs

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

