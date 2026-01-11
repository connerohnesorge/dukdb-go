# Tasks: Fix DuckDB Write Compatibility

## Phase 1: Diagnostic Analysis

- [x] 1.1 Create hex dump comparison tool in `internal/storage/duckdb/debug_compare.go` that shows byte-by-byte differences between dukdb-go and DuckDB files
- [x] 1.2 Create identical test databases (same schema, same data) in both dukdb-go and DuckDB CLI
- [x] 1.3 Generate diff report identifying exact byte offsets where files diverge
- [x] 1.4 Categorize differences: file header, database headers, metadata blocks, row groups, checksums
- [x] 1.5 Document findings in `spectr/changes/fix-duckdb-write-compat/diagnostics.md`

## Phase 2: File Header and Database Headers

- [x] 2.1 Verify file header layout: magic bytes position, version bytes, flags
- [x] 2.2 Verify XXH64 checksum algorithm matches DuckDB (seed value, byte order)
- [x] 2.3 Fix database header structure: meta_block pointer, free_list, block_count, block_alloc_size
- [x] 2.4 Verify dual header slots (H1 at 4096, H2 at 8192) with iteration counter
- [x] 2.5 Add unit tests comparing header bytes with DuckDB reference

## Phase 3: Checksum Implementation

- [x] 3.1 Create test that computes XXH64 on known data and compares with DuckDB output
- [x] 3.2 Verify block checksum position (first 8 bytes of each block)
- [x] 3.3 Verify checksum covers correct byte range (block data excluding checksum itself)
- [x] 3.4 Fix any endianness issues in checksum storage
- [x] 3.5 Add regression tests for checksum calculation

## Phase 4: Metadata Block Format

- [x] 4.1 Verify MetaBlockPointer encoding: 56-bit block ID + 8-bit sub-block index
- [x] 4.2 Verify metadata sub-block size (4096 bytes, 64 per 256KB storage block)
- [x] 4.3 Fix metadata block allocation sequence
- [x] 4.4 Verify next_block pointer in chained metadata
- [x] 4.5 Fix any alignment/padding in metadata blocks
- [x] 4.6 Add tests comparing metadata block layout with DuckDB reference

## Phase 5: Catalog Serialization

- [x] 5.1 Verify BinarySerializer field ID encoding (uint16 little-endian)
- [x] 5.2 Verify varint encoding matches DuckDB (LEB128)
- [x] 5.3 Verify message terminator (0xFFFF) placement
- [x] 5.4 Add storage metadata after each catalog entry: table_pointer (field 101), total_rows (field 102), index info (fields 103-104)
- [x] 5.5 Verify catalog entry count and ordering
- [x] 5.6 Add hex dump comparison tests for catalog bytes

## Phase 6: Row Group Format

- [x] 6.1 Verify row group metadata structure: row count, column count, column metadata offsets
- [x] 6.2 Verify column segment format: compression type, data offset, data size
- [x] 6.3 Verify validity mask encoding for NULL values
- [x] 6.4 Verify string heap format for VARCHAR columns
- [x] 6.5 Verify dictionary compression format: dictionary + indices
- [x] 6.6 Add tests verifying row group bytes match DuckDB reference

## Phase 7: Compression Encoding

- [x] 7.1 Verify CONSTANT compression output format
- [x] 7.2 Verify RLE compression output format
- [x] 7.3 Verify DICTIONARY compression: dictionary encoding, index bit width
- [x] 7.4 Verify BITPACKING compression: bit width selection, packing order
- [x] 7.5 Verify PFOR-DELTA compression if used
- [x] 7.6 Add compression round-trip tests comparing with DuckDB

## Phase 8: Integration Testing

- [x] 8.1 Create test: dukdb-go creates file → DuckDB CLI runs `SHOW TABLES` → verify output
- [x] 8.2 Create test: dukdb-go creates file → DuckDB CLI runs `SELECT *` → verify data
- [x] 8.3 Create test: dukdb-go creates file → DuckDB CLI runs `DESCRIBE` → verify schema
- [x] 8.4 Create round-trip test: dukdb-go → DuckDB CLI modify → dukdb-go read → verify
- [x] 8.5 Test with various table configurations: single column, many columns, many rows

## Phase 9: Cleanup and Documentation

- [x] 9.1 Remove `skipOnFormatError` from all passing write tests
- [x] 9.2 Update gap-analysis.md: GAP-001 status to IMPLEMENTED
- [ ] 9.3 Document any remaining limitations or version requirements
- [ ] 9.4 Add CI job for write compatibility testing
- [ ] 9.5 Update duckdb-storage-format spec with implementation notes
