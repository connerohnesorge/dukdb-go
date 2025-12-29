# Tasks: Add Catalog Persistence

## Phase 1: Persistence Package Infrastructure

- [ ] **1.1** Create persistence package structure
  - Create `internal/persistence/` directory
  - Create `file_manager.go` with FileManager struct
  - Define DatabaseMetadata and BlockInfo types
  - Add magic number constant "DUKDBGO\x00"

- [ ] **1.2** Implement file header read/write
  - Write magic number (8 bytes)
  - Write version (4 bytes)
  - Write flags (4 bytes)
  - Write catalog offset placeholder (8 bytes)
  - Write block count (4 bytes)
  - Write reserved space (36 bytes)

- [ ] **1.3** Implement file footer read/write
  - Write SHA-256 checksum of entire file
  - Verify checksum on read
  - Return error on checksum mismatch

- [ ] **1.4** Implement FileManager open/create
  - OpenFile() for existing databases
  - CreateFile() for new databases
  - Validate magic number on open
  - Validate version compatibility

## Phase 2: Catalog Serialization

- [ ] **2.1** Define catalog serialization types
  - CatalogData with Version and Schemas
  - SchemaData with Name and Tables
  - TableData with Name, Schema, Columns, PrimaryKey
  - ColumnData with Name, Type, Nullable, Default, TypeInfo
  - TypeInfo with Precision, Scale, ElementType, Fields, etc.

- [ ] **2.2** Implement Catalog.Export()
  - Lock catalog for reading
  - Iterate all schemas
  - Convert each table to TableData
  - Convert each column to ColumnData
  - Return CatalogData structure

- [ ] **2.3** Implement Catalog.Import()
  - Lock catalog for writing
  - Create schemas from CatalogData
  - Create tables from TableData
  - Create columns from ColumnData
  - Handle type conversion

- [ ] **2.4** Implement catalog file read/write
  - Serialize CatalogData to JSON
  - Compress with gzip
  - Write to file at specified offset
  - Read and decompress on load

- [ ] **2.5** Test catalog serialization round-trip
  - Test empty catalog
  - Test single table
  - Test multiple schemas
  - Test all column types
  - Test nullable columns
  - Test default values

## Phase 3: Storage Serialization

- [ ] **3.1** Implement primitive type serialization
  - writeInt8/readInt8
  - writeInt16/readInt16
  - writeInt32/readInt32
  - writeInt64/readInt64
  - writeFloat32/readFloat32
  - writeFloat64/readFloat64

- [ ] **3.2** Implement string serialization
  - writeVarint for length
  - writeString with length prefix
  - readString with length prefix
  - Handle empty strings

- [ ] **3.3** Implement validity mask serialization
  - Convert uint64[] to byte[]
  - Pack bits efficiently
  - Handle partial last byte
  - Read and convert back

- [ ] **3.4** Implement vector serialization per type
  - TYPE_BOOLEAN
  - TYPE_TINYINT/SMALLINT/INTEGER/BIGINT
  - TYPE_UTINYINT/USMALLINT/UINTEGER/UBIGINT
  - TYPE_FLOAT/DOUBLE (including +Inf, -Inf, NaN, -0.0)
  - TYPE_VARCHAR (UTF-8 encoded)
  - TYPE_BLOB
  - TYPE_DATE/TIME/TIMESTAMP (all variants)
  - TYPE_INTERVAL
  - TYPE_DECIMAL (precision/scale from TypeInfo)
  - TYPE_UUID
  - TYPE_HUGEINT/UHUGEINT (128-bit as [2]int64/[2]uint64)
  - TYPE_ENUM (dictionary in TypeInfo, values as uint32)

- [ ] **3.5** Implement nested type serialization
  - TYPE_LIST: length + child vector
  - TYPE_STRUCT: field count + field names + field vectors
  - TYPE_MAP: key vector + value vector
  - TYPE_ARRAY: fixed size from TypeInfo + child vector

- [ ] **3.6** Implement RowGroup export
  - Write block header (magic, rowCount, colCount)
  - Write each column vector
  - Calculate and store checksum

- [ ] **3.7** Implement RowGroup import
  - Read and verify block header
  - Read each column vector
  - Verify checksum
  - Return populated RowGroup

- [ ] **3.8** Test storage serialization round-trip
  - Test each primitive type
  - Test null handling
  - Test empty vectors
  - Test nested types
  - Test large row groups

## Phase 4: Block Index

- [ ] **4.1** Implement block index structure
  - BlockInfo with table, rowgroup, offset, size, checksum
  - Index as sorted list of BlockInfo
  - Binary search for table lookup

- [ ] **4.2** Implement block index read/write
  - Write entry count
  - Write each BlockInfo
  - Read entry count
  - Read BlockInfo entries

- [ ] **4.3** Implement block management
  - WriteBlock() adds data and updates index
  - ReadBlock() retrieves by BlockInfo
  - DataBlocks() returns all BlockInfo

## Phase 5: Engine Integration

- [ ] **5.1** Add persistence fields to Engine
  - Add `persistent bool` flag
  - Add `path string` field (already exists)
  - Initialize based on path in Open()

- [ ] **5.2** Implement loadFromFile()
  - Open file with FileManager
  - Read and verify header
  - Read catalog and import
  - Read block index
  - Load each table's data blocks

- [ ] **5.3** Implement saveToFile()
  - Create temp file
  - Write header
  - Export and write each table's row groups
  - Export and write catalog
  - Write block index
  - Write footer with checksum
  - Atomic rename

- [ ] **5.4** Wire persistence to Engine.Open()
  - Check if path is ":memory:"
  - Check if file exists
  - Call loadFromFile if exists
  - Set persistent flag

- [ ] **5.5** Wire persistence to Engine.Close()
  - Check persistent flag
  - Call saveToFile
  - Handle errors appropriately

- [ ] **5.6** Implement Storage.Tables() accessor
  - Return map of table name to Table
  - Used by saveToFile for iteration

- [ ] **5.7** Implement Storage.ImportTable()
  - Create table from serialized data
  - Populate row groups
  - Add to storage

## Phase 6: Testing and Validation

- [ ] **6.1** Unit tests for persistence package
  - Test FileManager open/create
  - Test header/footer read/write
  - Test checksum validation

- [ ] **6.2** Unit tests for catalog serialization
  - Test Export/Import round-trip
  - Test all column types
  - Test nested schemas

- [ ] **6.3** Unit tests for storage serialization
  - Test all vector types
  - Test nested types
  - Test large data sets

- [ ] **6.4** Integration tests for persistence
  - Create database, add data, close
  - Reopen and verify data
  - Test multiple tables
  - Test multiple schemas

- [ ] **6.5** Error handling tests
  - Test corrupt header (modified magic number)
  - Test corrupt footer checksum (modified bytes)
  - Test corrupt block checksum (modified data block)
  - Test corrupt catalog JSON (invalid JSON)
  - Test corrupt block index (invalid offsets)
  - Test truncated file mid-block
  - Test missing file
  - Test version mismatch (future version)
  - Test read-only filesystem save failure
  - Test permission denied on temp file
  - Test missing parent directory

- [ ] **6.6** Temp directory isolation (REQUIRED)
  - ALL tests MUST use t.TempDir() for file paths
  - No hardcoded paths (/tmp, /var, etc.)
  - All tests must be parallelizable with -parallel
  - Clean up on test completion

- [ ] **6.7** Atomic save tests
  - Verify temp file created during save
  - Verify checksum computed before rename
  - Verify failed save leaves original unchanged
  - Verify no .tmp file remains on success
  - Verify no .tmp file remains on failure

- [ ] **6.8** Extended type tests
  - Test HUGEINT max/min values
  - Test UHUGEINT max value
  - Test ARRAY fixed-size preservation
  - Test ENUM dictionary and values
  - Test +Infinity, -Infinity, NaN, -0.0
  - Test Unicode strings (emoji, CJK)
  - Test DECIMAL precision/scale preservation
  - Test STRUCT field name preservation

- [ ] **6.9** NULL handling tests
  - Test NULL in every primitive type
  - Test NULL in nested types
  - Test all-NULL columns
  - Test mixed NULL/non-NULL rows

## Validation Criteria

- [ ] File created on close for non-memory databases
- [ ] Data persists across open/close cycles
- [ ] Catalog schemas preserved exactly
- [ ] All data types round-trip correctly
- [ ] Checksum validation catches corruption
- [ ] :memory: databases unchanged
- [ ] Atomic save prevents partial writes
- [ ] All tests pass with deterministic results
