# Implementation Tasks: DuckDB Binary Format

## Phase 1: Core Binary Format Infrastructure (Week 1)

### Task 1: Define Format Constants and Structures
- [ ] Create `internal/format/duckdb_format.go`
- [ ] Define DuckDB magic number constant (0x4455434B)
- [ ] Define format version constant (v64 = 64)
- [ ] Define property ID constants (100-103 for base, 200+ for type-specific)
- [ ] Define ExtraTypeInfoType enum matching DuckDB (see Task 1.5)
- [ ] Define CatalogEntryType enum
- [ ] **Validation**: Constants match DuckDB C++ source exactly

### Task 1.5: Verify ExtraTypeInfoType Enum Values
- [ ] Extract numeric values from `duckdb/src/include/duckdb/common/extra_type_info.hpp`
- [ ] Define all 14 enum constants (INVALID=0 through GEO=13)
- [ ] Document which types are in-scope (5 ExtraTypeInfoType values: DECIMAL, LIST, STRUCT, ENUM, ARRAY; plus MAP which uses LIST_TYPE_INFO)
- [ ] Document which types are deferred (7 types: STRING, USER, AGGREGATE_STATE, ANY, INTEGER_LITERAL, TEMPLATE, GEO)
- [ ] Verify MAP uses LIST_TYPE_INFO (4) with child STRUCT<key, value>
- [ ] **Validation**: Numeric constants match DuckDB v1.1.3 exactly (verified with test)

### Task 2: Implement Binary Writer
- [ ] Create `internal/format/binary_writer.go`
- [ ] Implement BinaryWriter struct with property map
- [ ] Implement WriteProperty(id, value) for required properties
- [ ] Implement WritePropertyWithDefault(id, value, default) for optional properties
- [ ] Implement value serialization for primitive types (uint8, uint32, uint64, string)
- [ ] Implement value serialization for vector types ([]string, []TypeInfo)
- [ ] Implement Flush() to write properties in sorted ID order
- [ ] Use little-endian byte order throughout
- [ ] **Validation**: Unit tests for all primitive type serialization

###Task 3: Implement Binary Reader
- [ ] Create `internal/format/binary_reader.go`
- [ ] Implement BinaryReader struct with property map
- [ ] Implement Load() to read all properties from stream
- [ ] Implement ReadProperty(id, dest) for required properties
- [ ] Implement ReadPropertyWithDefault(id, dest, default) for optional properties
- [ ] Implement value deserialization for primitive types
- [ ] Implement value deserialization for vector types
- [ ] Handle missing required properties with descriptive errors
- [ ] **Validation**: Round-trip tests (write → read → compare)

### Task 4: Implement Checksum Utilities
- [ ] Create `internal/format/checksum.go`
- [ ] Import hash/crc64 with ECMA table
- [ ] Implement CalculateChecksum(data []byte) uint64
- [ ] Implement WriteWithChecksum(w, data) error
- [ ] Implement ReadAndVerifyChecksum(r, expectedLen) ([]byte, error)
- [ ] **Validation**: Checksum detection test with corrupted data

## Phase 2: TypeInfo Binary Serialization (Week 1-2)

### Task 5: Implement DECIMAL Serialization
- [ ] Create `internal/format/typeinfo_serializer.go`
- [ ] Implement SerializeDecimal(w *BinaryWriter, d *DecimalDetails) error
  - Write property 100: DECIMAL_TYPE_INFO discriminator
  - Write property 200: Width (uint8)
  - Write property 201: Scale (uint8)
- [ ] **Validation**: DECIMAL(18,4) serializes to exact bytes matching DuckDB

### Task 6: Implement DECIMAL Deserialization
- [ ] Create `internal/format/typeinfo_deserializer.go`
- [ ] Implement DeserializeDecimal(r *BinaryReader) (*DecimalDetails, error)
  - Read property 200: Width
  - Read property 201: Scale
- [ ] **Validation**: Round-trip DECIMAL(1,0), DECIMAL(18,4), DECIMAL(38,38)

### Task 7: Implement ENUM Serialization
- [ ] Implement SerializeEnum(w *BinaryWriter, e *EnumDetails) error
  - Write property 100: ENUM_TYPE_INFO discriminator (6)
  - Write property 200: Values count (uint64)
  - Write property 201: Values list using WriteList()
- [ ] **Validation**: ENUM with 100 values serializes correctly

### Task 8: Implement ENUM Deserialization
- [ ] Implement DeserializeEnum(r *BinaryReader) (*EnumDetails, error)
  - Read property 200: Values count (uint64)
  - Read property 201: Values list using ReadList()
  - Defensive copy handled by ReadList
- [ ] **Validation**: Round-trip ENUM preserves value order exactly

### Task 9: Implement LIST Serialization
- [ ] Implement SerializeList(w *BinaryWriter, l *ListDetails) error
  - Write property 100: LIST_TYPE_INFO discriminator
  - Write property 200: Child TypeInfo (recursive call)
- [ ] **Validation**: LIST(INTEGER) and LIST(LIST(VARCHAR)) serialize

### Task 10: Implement LIST Deserialization
- [ ] Implement DeserializeList(r *BinaryReader) (*ListDetails, error)
  - Read property 200: Child TypeInfo (recursive call)
- [ ] **Validation**: Nested lists round-trip (3+ levels deep)

### Task 11: Implement ARRAY Serialization
- [ ] Implement SerializeArray(w *BinaryWriter, a *ArrayDetails) error
  - Write property 100: ARRAY_TYPE_INFO discriminator
  - Write property 200: Child TypeInfo
  - Write property 201: Fixed size (uint32)
- [ ] **Validation**: ARRAY(INTEGER, 10) serializes correctly

### Task 12: Implement ARRAY Deserialization
- [ ] Implement DeserializeArray(r *BinaryReader) (*ArrayDetails, error)
  - Read property 200: Child TypeInfo
  - Read property 201: Size
- [ ] **Validation**: Round-trip preserves array size exactly

### Task 13: Implement STRUCT Serialization
- [ ] Implement SerializeStruct(w *BinaryWriter, s *StructDetails) error
  - Write property 100: STRUCT_TYPE_INFO discriminator
  - Write property 200: Field list (count + [(name, type), ...])
  - Handle recursive field TypeInfos
- [ ] **Validation**: STRUCT with 20 fields, nested STRUCTs

### Task 14: Implement STRUCT Deserialization
- [ ] Implement DeserializeStruct(r *BinaryReader) (*StructDetails, error)
  - Read property 200: Field list
  - Reconstruct StructEntry instances
- [ ] **Validation**: Field names and types preserved exactly

### Task 15: Implement MAP Serialization
- [ ] Implement SerializeMap(w *BinaryWriter, m *MapDetails) error
  - Write property 100: MAP_TYPE_INFO discriminator
  - Write property 200: Key TypeInfo (recursive)
  - Write property 201: Value TypeInfo (recursive)
- [ ] **Validation**: MAP(VARCHAR, STRUCT(...)) serializes

### Task 16: Implement MAP Deserialization
- [ ] Implement DeserializeMap(r *BinaryReader) (*MapDetails, error)
  - Read property 200: Key TypeInfo
  - Read property 201: Value TypeInfo
- [ ] **Validation**: Complex MAP types round-trip

### ~~Task 17-18: UNION Serialization~~ - DEFERRED ❌

**STATUS**: UNION serialization is NOT in DuckDB v64 format. Deferred to future work when DuckDB v65+ includes it.

- UNION TypeInfo construction still works (P0-1a Core TypeInfo)
- Attempting to serialize UNION SHALL return `ErrUnsupportedTypeForSerialization`
- No tasks required for P0-1b

## Phase 3: Catalog Binary Serialization (Week 2)

### Task 19: Implement Catalog Entry Serialization
- [ ] Create `internal/format/catalog_serializer.go`
- [ ] Implement SerializeCatalog(w io.Writer, catalog *Catalog) error
  - Write entry count
  - Serialize each catalog entry (schemas, tables, views)
- [ ] Implement SerializeTableEntry(w io.Writer, table *Table) error
  - Write property 100: TABLE entry type
  - Write property 101: Table name
  - Write property 102: Schema name
  - Write property 200: Column definitions (with TypeInfo)
  - Write property 201: Constraints
- [ ] **Validation**: Catalog with 10 tables serializes

### Task 20: Implement Column Serialization
- [ ] Implement SerializeColumn(w *BinaryWriter, col *Column) error
  - Write property 100: Column name
  - Write property 101: TypeInfo (recursive call to TypeInfo serializer)
  - Write property 102: Nullable flag
  - Write property 103: Default value
- [ ] **Validation**: Columns with complex types serialize correctly

### Task 21: Implement File Header/Footer
- [ ] Implement WriteHeader(w io.Writer) error
  - Write magic number (4 bytes): 0x4455434B
  - Write format version (8 bytes): 64
- [ ] Implement ValidateHeader(r io.Reader) error
  - Read and verify magic number
  - Read and verify format version
  - Return descriptive errors for mismatches
- [ ] **Validation**: Invalid headers rejected with clear errors

## Phase 4: Binary Format Reader (Week 2-3)

### Task 22: Implement Catalog Deserialization
- [ ] Create `internal/format/catalog_deserializer.go`
- [ ] Implement DeserializeCatalog(r io.Reader) (*Catalog, error)
  - Read entry count
  - Deserialize each entry
  - Reconstruct catalog structure
- [ ] Implement DeserializeTableEntry(r io.Reader) (*Table, error)
  - Read all properties
  - Reconstruct column list with TypeInfo
  - Reconstruct constraints
- [ ] **Validation**: Catalog round-trip preserves all metadata

### Task 23: Implement TypeInfo Dispatcher
- [ ] Implement DeserializeTypeInfo(r *BinaryReader) (TypeInfo, error)
  - Read property 100: Type discriminator
  - Switch on type to call specific deserializer
  - Handle INVALID_TYPE_INFO (return nil)
- [ ] **Validation**: All 37 types deserialize correctly

### Task 24: Integrate with Catalog Load/Save
- [ ] Update `internal/catalog/catalog.go`
- [ ] Add SaveToDuckDBFormat(path string) error method
  - Open file with atomic write (temp file + rename)
  - Write header
  - Serialize catalog
  - Write checksum
- [ ] Add LoadFromDuckDBFormat(path string) error method
  - Open file
  - Validate header
  - Deserialize catalog
  - Verify checksum
- [ ] **Validation**: Database save/load works end-to-end

## Phase 5: Compatibility Testing (Week 3)

### Task 25: Round-Trip Unit Tests
- [ ] Create `internal/format/format_test.go`
- [ ] Test DECIMAL round-trip (10 cases: 1,0 through 38,38)
- [ ] Test ENUM round-trip (single value, 100 values, Unicode)
- [ ] Test LIST round-trip (primitives, nested 3 levels deep)
- [ ] Test ARRAY round-trip (size 1, size 1000, complex child)
- [ ] Test STRUCT round-trip (1 field, 50 fields, nested)
- [ ] Test MAP round-trip (primitive key/value, complex types)
- [ ] Test UNION round-trip (2 members, 10 members)
- [ ] **Validation**: 100+ round-trip test cases pass

### Task 26: Cross-Implementation Compatibility Tests
- [ ] Create test database files with duckdb-go v1.4.3
  - Table with all 37 types
  - Table with nested types (LIST of STRUCT of MAP)
  - Table with DECIMAL, ENUM, complex types
- [ ] Test reading files created by duckdb-go v1.4.3
  - Verify all TypeInfo metadata matches exactly
  - Verify DECIMAL width/scale preserved
  - Verify ENUM values in order
- [ ] Test writing files readable by DuckDB C++ v1.1.3
  - Use DuckDB CLI to open file
  - Run DESCRIBE TABLE to verify schema
  - Query data to verify integrity
- [ ] **Validation**: Cross-implementation tests pass 100%

### Task 27: Hex Dump Verification
- [ ] Serialize DECIMAL(18,4) and compare hex dump to DuckDB C++ output
- [ ] Serialize ENUM("A","B","C") and compare hex dump
- [ ] Document binary layout differences if any
- [ ] **Validation**: Byte-for-byte match with reference implementation

### Task 28: Error Handling Tests
- [ ] Test invalid magic number detection
- [ ] Test unsupported version detection
- [ ] Test checksum mismatch detection
- [ ] Test truncated file handling
- [ ] Test corrupted property data
- [ ] Test missing required property error messages
- [ ] **Validation**: All error paths tested with descriptive messages

## Phase 6: Performance and Robustness (Week 3)

### Task 29: Performance Benchmarks
- [ ] Benchmark TypeInfo serialization (1000 iterations)
- [ ] Benchmark catalog serialization (100 tables)
- [ ] Benchmark deserialization performance
- [ ] Profile memory allocations
- [ ] **Validation**: Performance comparable to reference implementation

### Task 30: Fuzzing
- [ ] Create fuzz tests for binary reader (random bytes)
- [ ] Fuzz TypeInfo deserializer with invalid property combinations
- [ ] Fuzz catalog deserializer with malformed data
- [ ] **Validation**: No panics, only errors returned

### Task 31: Documentation
- [ ] Document DuckDB v64 format in design.md (complete)
- [ ] Add godoc comments to all public functions
- [ ] Create examples for serialization/deserialization
- [ ] Document property ID mappings
- [ ] **Validation**: go doc shows all examples

## Dependencies

- **Sequential**: Tasks 5-18 (TypeInfo serialization) can be parallelized by type
- **Sequential**: Task 19-21 require Tasks 5-18 complete
- **Sequential**: Task 22-24 require Task 19-21 complete
- **Sequential**: Phase 5 requires Phase 4 complete

## Success Criteria

All tasks completed when:
- [ ] All 6 serializable TypeDetails serialize to DuckDB binary format (DECIMAL, ENUM, LIST, ARRAY, STRUCT, MAP)
- [ ] All 6 serializable TypeDetails deserialize from DuckDB binary format
- [ ] UNION serialization returns ErrUnsupportedTypeForSerialization (deferred to v65+ format)
- [ ] Round-trip tests pass for all serializable types (100+ scenarios)
- [ ] Files created by duckdb-go v1.4.3 load correctly
- [ ] Files written by dukdb-go load in DuckDB C++ v1.1.3
- [ ] Hex dumps match reference implementation byte-for-byte
- [ ] Cross-implementation compatibility test suite passes
- [ ] No binary format drift detected
- [ ] All error paths tested and covered
- [ ] Performance benchmarks show acceptable speed
- [ ] Fuzzing finds no crashes or panics
- [ ] Documentation complete with examples
