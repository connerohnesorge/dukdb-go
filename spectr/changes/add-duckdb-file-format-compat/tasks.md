# Tasks: DuckDB File Format Compatibility

## 1. Magic Number & Header Implementation

- [ ] 1.1 Create `internal/persistence/duckdb_format.go` with DuckDB format constants
- [ ] 1.2 Define DuckDB header structure (4096 bytes, dual blocks)
- [ ] 1.3 Implement `ReadDuckDBHeader(r io.Reader) (*Header, error)`
- [ ] 1.4 Implement `WriteDuckDBHeader(w io.Writer, h *Header) error`
- [ ] 1.5 Implement header validation (magic number, checksum, version)
- [ ] 1.6 Implement dual-header read logic (choose newer based on salt)
- [ ] 1.7 Implement atomic header write with temp files and rename
- [ ] 1.8 Add tests for header read/write with both header blocks
- [ ] 1.9 Add tests for header corruption detection

## 2. Format Version Upgrade

- [ ] 2.1 Define format version constants (DUCKDB_VERSION = 64)
- [ ] 2.2 Update `internal/persistence/file.go` to use DuckDB version
- [ ] 2.3 Implement version negotiation in header reader
- [ ] 2.4 Add support for reading older DuckDB versions (v64+)
- [ ] 2.5 Document version compatibility matrix
- [ ] 2.6 Add version validation tests

## 3. Binary Catalog Serialization

- [ ] 3.1 Create `internal/persistence/catalog_serializer.go`
- [ ] 3.2 Define property ID constants (PropertyIDType, PropertyIDName, etc.)
- [ ] 3.3 Implement `SerializeTypeInfo(buf *Writer, ti TypeInfo) error`
- [ ] 3.4 Implement `DeserializeTypeInfo(r *Reader) (TypeInfo, error)`
- [ ] 3.5 Implement struct type serialization (recursive child handling)
- [ ] 3.6 Implement list type serialization (child type)
- [ ] 3.7 Implement map type serialization (key/value types)
- [ ] 3.8 Implement union type serialization (members with types)
- [ ] 3.9 Add support for new types: BIT, TIME_TZ, TIMESTAMP_TZ
- [ ] 3.10 Add tests for all TypeInfo serialization round-trips
- [ ] 3.11 Add tests for nested type serialization (STRUCT, LIST, MAP, UNION)

## 4. Compression Implementation

- [ ] 4.1 Create `internal/compression/compress.go` with compression interface
- [ ] 4.2 Implement `RLEEncoder` and `RLEDecoder`
- [ ] 4.3 Implement `BitPackingEncoder` and `BitPackingDecoder`
- [ ] 4.4 Implement `FSSTEncoder` and `FSTSDecoder`
- [ ] 4.5 Implement `ChimpEncoder` and `ChimpDecoder` for floats
- [ ] 4.6 Integrate zstd compression (use github.com/klauspost/compress/zstd)
- [ ] 4.7 Implement compression selection strategy per type
- [ ] 4.8 Add compression benchmarks
- [ ] 4.9 Add compression tests with various data patterns
- [ ] 4.10 Document ALP/Patas as unsupported (requires native C++)

## 5. WAL Format Implementation

- [ ] 5.1 Update `internal/wal/header.go` for DuckDB WAL format
- [ ] 5.2 Define WAL entry types (INSERT, UPDATE, DELETE, etc.)
- [ ] 5.3 Implement DuckDB WAL entry serialization
- [ ] 5.4 Implement DuckDB WAL entry deserialization
- [ ] 5.5 Update `internal/wal/writer.go` for DuckDB format
- [ ] 5.6 Update `internal/wal/reader.go` for DuckDB format
- [ ] 5.7 Add WAL entry checksum verification
- [ ] 5.8 Add tests for DuckDB WAL round-trip
- [ ] 5.9 Add tests for WAL recovery with DuckDB format

## 6. Row Group Format Implementation

- [ ] 6.1 Update `internal/storage/chunk.go` to use DuckDB row group format
- [ ] 6.2 Define `RowGroup` struct with column segments
- [ ] 6.3 Define `ColumnSegment` struct with compression metadata
- [ ] 6.4 Implement `SerializeRowGroup(rg *RowGroup) ([]byte, error)`
- [ ] 6.5 Implement `DeserializeRowGroup(data []byte) (*RowGroup, error)`
- [ ] 6.6 Implement per-column compression selection
- [ ] 6.7 Add row group metadata serialization (row count, column count, etc.)
- [ ] 6.8 Add row group tests with various data types
- [ ] 6.9 Add tests for row group compression/decompression

## 7. Index Persistence

- [ ] 7.1 Create `internal/storage/index/art.go` for ART index
- [ ] 7.2 Implement ART node serialization
- [ ] 7.3 Implement ART key encoding (various types)
- [ ] 7.4 Implement ART deserialization
- [ ] 7.5 Add index storage to row groups
- [ ] 7.6 Update checkpoint to include index data
- [ ] 7.7 Add index persistence tests
- [ ] 7.8 Document index format compatibility

## 8. Migration Utility

- [ ] 8.1 Create `cmd/dukdb-go-migrate/main.go`
- [ ] 8.2 Implement detection of old dukdb-go format (DUKDBGO magic)
- [ ] 8.3 Implement old format catalog deserialization (JSON)
- [ ] 8.4 Implement old format data deserialization
- [ ] 8.5 Implement new format catalog serialization
- [ ] 8.6 Implement new format data serialization
- [ ] 8.7 Add progress reporting
- [ ] 8.8 Add dry-run mode
- [ ] 8.9 Add migration tests with sample databases

## 9. Compatibility Mode

- [ ] 9.1 Add `?format=legacy` query parameter to connection string
- [ ] 9.2 Implement format detection in `Engine.Open()`
- [ ] 9.3 Implement `FormatDetector` interface
- [ ] 9.4 Add compatibility tests for both formats
- [ ] 9.5 Update documentation for format migration

## 10. Integration Tests

- [ ] 10.1 Create integration tests for reading DuckDB files
- [ ] 10.2 Create integration tests for writing DuckDB files
- [ ] 10.3 Create integration tests for WAL recovery
- [ ] 10.4 Create integration tests for checkpoint/restore
- [ ] 10.5 Create integration tests with various data types
- [ ] 10.6 Add compatibility tests against official DuckDB
- [ ] 10.7 Create large dataset tests (>1GB)

## 11. Performance Benchmarks

- [ ] 11.1 Create benchmark for header read/write
- [ ] 11.2 Create benchmark for catalog serialization
- [ ] 11.3 Create benchmark for data compression
- [ ] 11.4 Create benchmark for WAL throughput
- [ ] 11.5 Create benchmark for row group serialization
- [ ] 11.6 Compare performance with old format (if beneficial)

## 12. Documentation

- [ ] 12.1 Update `docs/file-format.md` with new format specification
- [ ] 12.2 Add migration guide documentation
- [ ] 12.3 Add compression options documentation
- [ ] 12.4 Update `README.md` with format compatibility info
- [ ] 12.5 Add examples for using migration utility
