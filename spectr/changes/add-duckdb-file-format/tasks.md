# Tasks: Native DuckDB File Format Support

## Phase 1: Core Infrastructure

- [ ] 1.1 Create package structure for `internal/storage/duckdb/` with format.go, header.go, block.go, binary.go
- [ ] 1.2 Implement FileHeader and DatabaseHeader structs matching DuckDB v1.4.3 format with dual-header support, magic bytes validation, and custom checksum algorithm
- [ ] 1.3 Implement BlockManager for reading/writing 256KB blocks with checksum at BEGINNING of block and LRU cache
- [ ] 1.4 Implement BinaryReader/BinaryWriter utilities for little-endian serialization

## Phase 2: Catalog Support

- [ ] 2.1 Define catalog entry types (Schema, Table, View, Index, Sequence, Type) matching DuckDB format
- [ ] 2.2 Implement catalog serialization for all entry types including nested types
- [ ] 2.3 Implement catalog deserialization from DuckDB metadata blocks
- [ ] 2.4 Integrate DuckDB catalog structures with existing dukdb-go catalog

## Phase 3: Decompression Algorithms

- [ ] 3.1 Implement CONSTANT decompression for single-value expansion
- [ ] 3.2 Implement RLE decompression for run-length encoded data
- [ ] 3.3 Implement DICTIONARY decompression with dictionary lookup
- [ ] 3.4 Implement BITPACKING decompression for bit-packed integers
- [ ] 3.5 Implement PFOR_DELTA decompression with reference and cumulative delta handling
- [ ] 3.6 Implement BITPACKING modes (AUTO, CONSTANT, CONSTANT_DELTA, DELTA_FOR, FOR)

## Phase 4: Row Group Reading

- [ ] 4.1 Define RowGroup and DataPointer structures matching DuckDB format (not ColumnSegment)
- [ ] 4.2 Implement RowGroupReader for lazy loading with decompression
- [ ] 4.3 Implement TableScanner with row group iteration and projection pushdown
- [ ] 4.4 Implement DuckDB to dukdb-go type conversion for all 46+ types (including CHAR, TIME_NS)

## Phase 5: Write Support

- [ ] 5.1 Implement compression selection heuristics based on data characteristics
- [ ] 5.2 Implement CONSTANT compression write path
- [ ] 5.3 Implement RLE compression write path
- [ ] 5.4 Implement DICTIONARY compression write path
- [ ] 5.5 Implement BITPACKING compression write path
- [ ] 5.6 Implement PFOR_DELTA compression write path
- [ ] 5.7 Implement RowGroupWriter with compression and block allocation
- [ ] 5.8 Implement CatalogWriter for metadata serialization
- [ ] 5.9 Implement complete DuckDBWriter with header, catalog, and row groups

## Phase 6: Integration

- [ ] 6.1 Implement DuckDBStorage satisfying StorageBackend interface
- [ ] 6.2 Integrate with Engine for automatic format detection
- [ ] 6.3 Add configuration options for storage format selection
- [ ] 6.4 Create end-to-end tests for DuckDB CLI interoperability

## Phase 7: Polish

- [ ] 7.1 Optimize performance for block caching and decompression hot paths
- [ ] 7.2 Improve error handling with clear messages for format issues
- [ ] 7.3 Add package documentation and format compatibility notes
