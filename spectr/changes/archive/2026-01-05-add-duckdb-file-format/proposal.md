# Proposal: Native DuckDB File Format Support

## Summary

Implement reading and writing of native DuckDB database files (.duckdb) to enable full interoperability with the DuckDB CLI and other DuckDB-compatible tools.

## Motivation

Currently, dukdb-go **cannot** read or write `.duckdb` files:
- Databases created by DuckDB CLI cannot be opened by dukdb-go
- Databases created by dukdb-go cannot be opened by DuckDB CLI
- No file-based persistence in DuckDB-compatible format
- Users must export/import via CSV, Parquet, or JSON for interoperability

This is the **most critical gap** for true DuckDB compatibility. Without native file format support, dukdb-go remains a functionally isolated system that cannot participate in the DuckDB ecosystem.

## Problem Statement

### Current State
- dukdb-go has its own WAL-based persistence format (not compatible with DuckDB)
- Opening a `.duckdb` file created by DuckDB CLI fails or produces garbage
- There is no way to export data from dukdb-go in a format DuckDB can directly read as a database

### Target State
- dukdb-go can open and read any `.duckdb` file created by DuckDB v1.4.3
- dukdb-go can write `.duckdb` files that DuckDB v1.4.3 can open
- Full round-trip compatibility: DuckDB → dukdb-go → DuckDB
- Compression codec parity for reasonable file sizes

## Scope

### In Scope
- DuckDB v1.4.3 storage format parsing (read)
- DuckDB v1.4.3 storage format writing (write)
- Block-based storage with checksums
- Row group reading/writing
- Core compression algorithms (Constant, RLE, Dictionary, BitPacking, FOR)
- Type mapping for all supported DuckDB types
- Catalog serialization (tables, views, indexes, sequences, schemas)
- Integration with existing dukdb-go engine

### Out of Scope (Future Work)
- Advanced compression: ALP (floating point), FSST (strings), CHIMP, PATAS
- Concurrent checkpoint writes
- Encryption support
- Storage extensions
- Delta/Iceberg format variants

## Approach

### Phase 1: File Format Parsing (Read Support)
1. Implement file header parser (magic bytes, version, metadata)
2. Implement block reader with checksum validation
3. Implement catalog block deserializer
4. Implement row group reader
5. Implement priority decompression algorithms

### Phase 2: Catalog Integration
1. Map DuckDB catalog structures to dukdb-go catalog
2. Load table, view, index, sequence definitions
3. Handle cross-schema references

### Phase 3: Data Loading
1. Lazy row group loading
2. Column segment decompression
3. NULL value handling via validity masks
4. Type coercion where needed

### Phase 4: Write Support
1. File header writer
2. Block writer with checksums
3. Catalog serialization
4. Row group writer with compression selection
5. Compression algorithm implementations (write path)

### Phase 5: Integration
1. Modify engine to detect and use DuckDB format
2. Add configuration for format selection
3. Connect to existing WAL for hybrid persistence
4. Checkpoint integration

## Success Criteria

1. **Read Compatibility**: Can open any `.duckdb` file created by DuckDB v1.4.3
2. **Write Compatibility**: Files written by dukdb-go can be opened by DuckDB v1.4.3
3. **Data Integrity**: No data loss or corruption in round-trip scenarios
4. **Compression Parity**: File sizes within 50% of DuckDB for same data
5. **Performance**: Read/write performance within 3x of native DuckDB
6. **Type Coverage**: All 43 dukdb-go types properly mapped

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Undocumented format changes | Medium | High | Pin to v1.4.3, extensive testing |
| Compression complexity | High | Medium | Start with simpler algorithms |
| Type mapping edge cases | Medium | Medium | Comprehensive type tests |
| Large file performance | Medium | Medium | Streaming/lazy loading |
| Format version changes | Low | High | Version detection, fail gracefully |

## Dependencies

- None (pure Go implementation required)
- Uses existing internal packages: storage, catalog, parser
- May leverage pure Go compression libraries for ZSTD/LZ4 block compression

## Affected Specs

- **NEW**: `duckdb-storage-format` - Native file format reading/writing
- **MODIFIED**: `persistence` - Add DuckDB format as persistence option
- **MODIFIED**: `storage` - Abstract storage backend selection
