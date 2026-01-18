# Change: Implement DuckDB's BinarySerializer Format for Catalog Metadata

## Why

DuckDB CLI cannot read database files created by dukdb-go when they contain catalog entries (tables, schemas, views). The current error:

```
INTERNAL Error: Failed to load metadata pointer (id 1, idx 0, ptr 1)
```

**Root Cause**: Our catalog serialization uses a custom format that doesn't match DuckDB's `BinarySerializer` format. DuckDB's checkpoint manager expects a specific property-based serialization format with:
- Field IDs (uint16) preceding each property value
- Message terminator field ID (0xFFFF) at object boundaries
- Varint encoding for integers
- Specific property IDs (99 for catalog_type, 100 for entry data)

**Current State**:
- File header checksum is correct (empty databases work)
- Format version is 64 (compatible with DuckDB v1.0-v1.3)
- Block-level checksums work correctly
- Catalog metadata format is incompatible (custom format)

## What

Implement DuckDB's exact `BinarySerializer` format for catalog metadata serialization:

1. **BinarySerializer Implementation** - Port DuckDB's property-based serialization protocol
2. **Varint Encoding** - Implement variable-length integer encoding matching DuckDB
3. **Catalog Entry Serialization** - Serialize schemas, tables, views using correct property IDs
4. **CreateInfo Serialization** - Serialize CreateSchemaInfo, CreateTableInfo, etc.
5. **Metadata Block Format** - Use DuckDB's metadata sub-block system (64 metadata blocks per storage block)

**Reference**: DuckDB source files:
- `src/common/serializer/binary_serializer.cpp`
- `src/storage/checkpoint_manager.cpp`
- `src/storage/metadata/metadata_reader.cpp`
- `src/include/duckdb/common/serializer/serialization_traits.hpp`

## Impact

### Users
- Files created by dukdb-go will be readable by DuckDB CLI
- Full cross-implementation database portability
- Breaking: None (improves compatibility)

### Codebase
- **New Files**:
  - `internal/storage/duckdb/binary_serializer.go` - BinarySerializer writer
  - `internal/storage/duckdb/binary_deserializer.go` - BinarySerializer reader
  - `internal/storage/duckdb/varint.go` - Varint encoding utilities
  - `internal/storage/duckdb/catalog_serializer.go` - Catalog entry serialization
  - `internal/storage/duckdb/metadata_block.go` - Metadata sub-block management
- **Modified Files**:
  - `internal/storage/duckdb/catalog_writer.go` - Use new serializer
  - `internal/storage/duckdb/duckdb_writer.go` - Update metadata block handling

### Risks
- **Complexity**: DuckDB's format is complex with many edge cases
- **Version Coupling**: Format may change between DuckDB versions
- **Mitigation**: Reference DuckDB source, extensive compatibility tests, version validation

## Success Criteria

- [ ] `TestDuckDBCLIReadsDukdbFile/simple_table` passes (no longer skips)
- [ ] DuckDB CLI can run `SHOW TABLES` on files created by dukdb-go
- [ ] DuckDB CLI can query data from tables created by dukdb-go
- [ ] Round-trip: create in dukdb-go → read in DuckDB CLI → read in dukdb-go
- [ ] BinarySerializer output matches DuckDB's byte-for-byte (hex dump verification)
- [ ] All existing tests continue to pass

## Dependencies

### Required Before
- File header checksum implementation (COMPLETED)
- Block-level checksum implementation (COMPLETED)
- Format version 64 support (COMPLETED)

### Enables After
- Full DuckDB CLI compatibility for read/write
- Database migration tools
- Cross-implementation data sharing

## Related Specs

- `duckdb-storage-format` - MODIFIED (adds BinarySerializer format details)
- `catalog-persistence` - USES (no changes, format is internal)

## Rollout Plan

### Phase 1: BinarySerializer Foundation
- Implement varint encoding
- Implement BinarySerializer writer with field IDs
- Implement message terminator handling

### Phase 2: Catalog Entry Serialization
- Serialize CatalogType enum
- Serialize CreateSchemaInfo
- Serialize CreateTableInfo with column definitions

### Phase 3: Metadata Block System
- Implement metadata sub-block allocation (64 per storage block)
- Update MetaBlockPointer encoding
- Connect to checkpoint flow

### Phase 4: Verification & Testing
- Hex dump comparison with DuckDB output
- Round-trip testing with DuckDB CLI
- Edge case coverage

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec deltas validated (`spectr validate implement-binaryserializer-format`)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed
