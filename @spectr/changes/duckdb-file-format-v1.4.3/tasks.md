# Tasks: DuckDB Native File Format Support (v1.4.3)

## Phase 1: Core Infrastructure & Block Management
- [ ] **Basic Types & Constants**: Define magic numbers, block types, version constants, and feature flags.
- [ ] **File Header**: Implement `FileHeader` struct with serialization/deserialization and CRC64 checksum validation.
- [ ] **Block Header**: Implement `BlockHeader` struct (32-byte) with CRC32 checksum support.
- [ ] **Block Manager**: 
    - [ ] Implement `BlockManager` for low-level block I/O.
    - [ ] Implement `FreeSpaceManager` with free list and segmentation strategies (First Fit/Best Fit).
    - [ ] Implement block allocation and file extension logic.
- [ ] **Checksum Utilities**: Implement CRC32 and CRC64 validation for blocks and headers.

## Phase 2: Metadata & Catalog Storage
- [ ] **Catalog Serialization**: Implement `CatalogNode` and `CatalogSerializer` for tree-based metadata storage.
- [ ] **Schema & Table Metadata**:
    - [ ] Implement `SchemaMetadata` serialization.
    - [ ] Implement `TableMetadata` including columns, constraints, and block pointers.
    - [ ] Implement `ColumnMetadata` and `TypeMetadata` for DuckDB types.
- [ ] **Statistics Persistence**:
    - [ ] Implement `TableStatistics` and `ColumnStatistics` storage.
    - [ ] Implement Histogram serialization (Buckets, NullBuckets).
- [ ] **Metadata Cache**: Implement LRU/TTL caching for catalog objects and statistics.

## Phase 3: Reading Implementation
- [ ] **FileReader Core**: Implement main entry point for opening and validating DuckDB files.
- [ ] **Block Reader**: Implement block-level reading with decompression support (LZ4, ZSTD).
- [ ] **Catalog Reader**: Implement reconstruction of the database schema from metadata blocks.
- [ ] **Data Reading**:
    - [ ] Implement `DataReader` for `DataChunk` reconstruction.
    - [ ] Implement `Vector` deserialization for all DuckDB v1.4.3 types.
    - [ ] Implement type-specific readers (Dictionary encoding for strings, delta for integers).
- [ ] **Index Reader**: Implement Hash Index and ART Index reconstruction from blocks.
- [ ] **Corruption Detection**: Implement `CorruptionChecker` for file-wide integrity verification.

## Phase 4: Writing Implementation
- [ ] **FileWriter Core**: Implement atomic file creation and header management.
- [ ] **Block Writer**: 
    - [ ] Implement block allocation and writing.
    - [ ] Implement multi-block chain support for large metadata/data objects.
- [ ] **Catalog Writer**: Implement persistence of schema updates and table metadata.
- [ ] **Data Serialization**:
    - [ ] Implement `DataChunkSerializer` and `Vector` serialization.
    - [ ] Implement adaptive compression selection based on data characteristics.
- [ ] **Index Writer**: Implement persistence for Hash and ART indexes.
- [ ] **Transactional Writing**: Implement "Sync to Disk" strategies and atomic commit logic.

## Phase 5: Versioning & Compatibility
- [ ] **Version Manager**: Implement detection and validation of DuckDB file versions.
- [ ] **Compatibility Matrix**: Define support levels for DuckDB v1.2.x through v1.4.x.
- [ ] **Feature Flags**: Implement detection and enforcement of format-specific features (Encryption, MMAP, Checksums).
- [ ] **Migration Framework**:
    - [ ] Implement `MigrationRegistry` and step-based migration execution.
    - [ ] Implement v1.3.x to v1.4.x migration path.

## Phase 6: Advanced Features & Optimization
- [ ] **Memory Management**:
    - [ ] Implement `BufferPool` for block caching.
    - [ ] Implement `MMAPManager` for memory-mapped file access.
- [ ] **Parallel I/O**: Implement worker-based parallel block reading and writing.
- [ ] **Compression Suite**: Implement full support for LZ4, ZSTD, GZIP, Snappy, RLE, and Delta encoding.
- [ ] **Prefetching**: Implement access pattern prediction for optimized range scans.

## Phase 7: Integration & Durability
- [ ] **WAL Integration**: Implement `WALManager` for transaction logging and recovery.
- [ ] **Checkpointing**: Implement `CheckpointManager` for flushing dirty blocks and truncating WAL.
- [ ] **Recovery Logic**: Implement replay mechanism for WAL records after crash/restart.
- [ ] **Transaction Store**: Implement tracking of active and committed transactions in file metadata.

## Phase 8: Verification & Testing
- [ ] **Unit Testing**: Serialization round-trips for all core structures.
- [ ] **Integration Testing**: End-to-end Write-Read-Verify tests.
- [ ] **DuckDB Compatibility**: Test against official DuckDB v1.4.3 generated files.
- [ ] **Performance Benchmarking**: I/O performance comparison with native DuckDB.
- [ ] **Stress Testing**: Concurrent access and large file (GB+) handling.
- [ ] **Recovery Testing**: Validate data integrity after simulated crashes.
