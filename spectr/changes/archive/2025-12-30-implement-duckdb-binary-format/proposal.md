# Change: Implement DuckDB Binary Format for Catalog Persistence

## Why

The existing catalog-persistence spec defines WHAT should persist (schemas, tables, TypeInfo metadata) but not HOW. Without DuckDB binary format compatibility, dukdb-go cannot:
- Read .duckdb files created by DuckDB C++ or duckdb-go v1.4.3
- Write files that other DuckDB implementations can open
- Achieve database portability across implementations
- Support migration/backup tools that work with DuckDB ecosystem

**Problem**: The current catalog-persistence spec is format-agnostic. We need to specify DuckDB's exact binary format to enable cross-implementation compatibility.

## What

Implement catalog persistence using **DuckDB's binary file format** (v64 format as of DuckDB v1.1.3):

1. **Binary Format Specification** - Document DuckDB's .duckdb file format structure
2. **TypeInfo Binary Serialization** - Serialize TypeDetails to DuckDB's ExtraTypeInfo binary format (property-based)
3. **Catalog Binary Serialization** - Write catalog metadata in DuckDB-compatible binary format
4. **Binary Format Reader** - Parse .duckdb files created by other DuckDB implementations
5. **Compatibility Testing** - Verify round-trip with DuckDB C++ v1.1.3 and duckdb-go v1.4.3

**Scope Limitation**: This proposal specifies the BINARY FORMAT only. The persistence layer (WAL, checkpoints, atomic saves) is already specified in the `persistence` spec.

**Format Target**: DuckDB format v64 (DuckDB v1.1.3 / duckdb-go v1.4.3)

## Impact

### Users
- ✅ **Enables**: Database portability between dukdb-go and other DuckDB implementations
- ✅ **Unlocks**: Migration tools, backup/restore with DuckDB ecosystem
- ⚠️ **Breaking**: None (pure addition, files written in DuckDB format)

### Codebase
- **New Files**:
  - `internal/format/duckdb_format.go` - Binary format constants and structures
  - `internal/format/typeinfo_serializer.go` - TypeInfo → binary serialization
  - `internal/format/typeinfo_deserializer.go` - Binary → TypeInfo deserialization
  - `internal/format/catalog_serializer.go` - Catalog → binary serialization
  - `internal/format/catalog_deserializer.go` - Binary → Catalog deserialization
  - `internal/format/format_test.go` - Round-trip compatibility tests
- **Modified Files**:
  - `internal/catalog/catalog.go` - Use DuckDB format for save/load
  - `internal/storage/storage.go` - Write DuckDB magic numbers, version headers
- **Dependencies**: Requires P0-1a Core TypeInfo (COMPLETED)
- **Blocks**: Full database persistence with ecosystem compatibility

### Risks
- **Binary Format Complexity**: DuckDB's format is auto-generated and version-specific
- **Endianness**: Must handle big-endian/little-endian correctly
- **Format Evolution**: DuckDB format may change in future versions
- **Mitigation**: Reference DuckDB source code, compatibility test suite, version validation

### Alternatives Considered
1. **Custom binary format** - Rejected: breaks ecosystem compatibility
2. **JSON format** - Rejected: inefficient, not DuckDB-compatible
3. **Protobuf** - Rejected: adds dependency, not DuckDB-compatible

## Success Criteria

- [ ] Read .duckdb files created by duckdb-go v1.4.3 without errors
- [ ] Write .duckdb files readable by DuckDB C++ v1.1.3
- [ ] TypeInfo serialization matches DuckDB's ExtraTypeInfo binary format
- [ ] All serializable types round-trip correctly (primitives + 6 complex types: DECIMAL, ENUM, LIST, ARRAY, STRUCT, MAP)
- [ ] UNION serialization returns ErrUnsupportedTypeForSerialization (not in v64 format)
- [ ] DECIMAL(18,4) width/scale preserved exactly
- [ ] ENUM values preserved in order
- [ ] STRUCT field names and types preserved
- [ ] Nested types (LIST of STRUCT of MAP) serialize correctly
- [ ] File magic number matches DuckDB (first 4 bytes: 0x4455434B)
- [ ] Format version header matches v64 (little-endian uint64 = 64)
- [ ] Compatibility test suite passes (100+ scenarios)
- [ ] No binary format drift (verified with hex dumps)

**Deferred to Future Work**:
- UNION TypeInfo serialization (not in DuckDB v64 format)
- Multi-version format support (only v64 for now)
- Format migration tools
- 7 additional ExtraTypeInfo types (STRING, USER, AGGREGATE_STATE, ANY, INTEGER_LITERAL, TEMPLATE, GEO)

## Dependencies

### Required Before
- ✅ P0-1a Core TypeInfo (TypeInfo interface, 8 constructors, 7 TypeDetails)
- ✅ Catalog structure (exists: `internal/catalog/`)
- ✅ catalog-persistence spec (behavioral requirements exist)

### Enables After
- Full .duckdb file read/write capability
- Database migration from/to other DuckDB implementations
- Backup/restore tools compatible with DuckDB ecosystem
- Schema introspection tools

## Related Specs

- `catalog-persistence` - MODIFIED (adds binary format specification)
- `type-system` - USES TypeInfo serialization
- `persistence` - BUILDS ON (uses WAL/checkpoint layer)

## Rollout Plan

### Phase 1: Binary Format Specification (Week 1)
- Document DuckDB v64 format structure
- Map ExtraTypeInfo property IDs (100, 101, 102, etc.)
- Define binary serialization protocol

### Phase 2: TypeInfo Serialization (Week 1-2)
- Implement binary serializers for 6 serializable TypeDetails (DECIMAL, ENUM, LIST, ARRAY, STRUCT, MAP)
- UNION deferred (not in DuckDB v64 format)
- Handle recursive types (LIST of STRUCT, etc.)
- Endianness handling

### Phase 3: Catalog Serialization (Week 2)
- Serialize table schemas to binary format
- Write format headers (magic, version)
- Implement checksums

### Phase 4: Binary Format Reader (Week 2-3)
- Parse .duckdb file headers
- Deserialize TypeInfo from ExtraTypeInfo binary
- Reconstruct catalog from binary format

### Phase 5: Compatibility Testing (Week 3)
- Round-trip tests with duckdb-go v1.4.3
- Cross-implementation tests with DuckDB C++
- Hex dump verification
- Fuzzing for robustness

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec deltas validated (spectr validate implement-duckdb-binary-format)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed (P0-1a complete)
- [ ] Testing strategy approved (compatibility test suite)
