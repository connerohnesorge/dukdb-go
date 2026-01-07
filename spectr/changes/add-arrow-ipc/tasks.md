# Tasks: Arrow/IPC Format Support

## Phase 1: IPC File Reading

- [x] 1.1 Create package structure `internal/io/arrow/` with ipc_reader.go, ipc_writer.go, stream.go, types.go
- [x] 1.2 Implement ArrowFileReader for reading Arrow IPC file format with random access
- [x] 1.3 Implement ArrowStreamReader for reading Arrow IPC stream format with sequential access
- [x] 1.4 Implement Arrow to DuckDB type conversion extending existing arrow_convert.go
- [x] 1.5 Add file format detection (magic bytes "ARROW1") for auto-detection

## Phase 2: IPC File Writing

- [x] 2.1 Implement ArrowFileWriter with configurable compression (None, LZ4, ZSTD)
- [x] 2.2 Implement ArrowStreamWriter for sequential stream output
- [x] 2.3 Implement DuckDB to Arrow type conversion for all 46+ types
- [x] 2.4 Implement RecordBatchBuilder for efficient DataChunk to Arrow Record conversion
- [x] 2.5 Implement proper memory management with Retain/Release pattern

## Phase 3: RecordBatch Streaming

- [x] 3.1 Implement streaming iterator interface for batch-at-a-time processing
- [x] 3.2 Add support for reading specific record batches by index (file format only)
- [x] 3.3 Implement schema extraction and validation
- [x] 3.4 Add cloud storage support via FileSystem interface (S3, GCS, Azure)

## Phase 4: Table Function Integration

- [x] 4.1 Register `read_arrow(path)` table function with schema inference
- [x] 4.2 Register `read_arrow_auto(path)` for format auto-detection
- [x] 4.3 Implement column projection pushdown for read_arrow
- [x] 4.4 Add optional parameters: columns, filter (predicate pushdown)

## Phase 5: COPY Statement Integration

- [x] 5.1 Implement ArrowCopyHandler for COPY TO/FROM
- [x] 5.2 Add `FORMAT 'arrow'` support to COPY TO with compression options
- [x] 5.3 Add `FORMAT 'arrow'` support to COPY FROM
- [x] 5.4 Add `COMPRESSION` option support (none, lz4, zstd)

## Phase 6: Testing & Documentation

- [x] 6.1 Create unit tests for Arrow type conversion (all 46+ types)
- [x] 6.2 Create integration tests for file reading with real Arrow files
- [x] 6.3 Create integration tests for file writing and round-trip verification
- [x] 6.4 Add cloud storage integration tests (S3, GCS, Azure URLs)
- [x] 6.5 Add performance benchmarks for read/write operations
- [x] 6.6 Add package documentation and usage examples
