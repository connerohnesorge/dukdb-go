# Tasks: Arrow/IPC Format Support

## Phase 1: IPC File Reading

- [ ] 1.1 Create package structure `internal/io/arrow/` with ipc_reader.go, ipc_writer.go, stream.go, types.go
- [ ] 1.2 Implement ArrowFileReader for reading Arrow IPC file format with random access
- [ ] 1.3 Implement ArrowStreamReader for reading Arrow IPC stream format with sequential access
- [ ] 1.4 Implement Arrow to DuckDB type conversion extending existing arrow_convert.go
- [ ] 1.5 Add file format detection (magic bytes "ARROW1") for auto-detection

## Phase 2: IPC File Writing

- [ ] 2.1 Implement ArrowFileWriter with configurable compression (None, LZ4, ZSTD)
- [ ] 2.2 Implement ArrowStreamWriter for sequential stream output
- [ ] 2.3 Implement DuckDB to Arrow type conversion for all 46+ types
- [ ] 2.4 Implement RecordBatchBuilder for efficient DataChunk to Arrow Record conversion
- [ ] 2.5 Implement proper memory management with Retain/Release pattern

## Phase 3: RecordBatch Streaming

- [ ] 3.1 Implement streaming iterator interface for batch-at-a-time processing
- [ ] 3.2 Add support for reading specific record batches by index (file format only)
- [ ] 3.3 Implement schema extraction and validation
- [ ] 3.4 Add cloud storage support via FileSystem interface (S3, GCS, Azure)

## Phase 4: Table Function Integration

- [ ] 4.1 Register `read_arrow(path)` table function with schema inference
- [ ] 4.2 Register `read_arrow_auto(path)` for format auto-detection
- [ ] 4.3 Implement column projection pushdown for read_arrow
- [ ] 4.4 Add optional parameters: columns, filter (predicate pushdown)

## Phase 5: COPY Statement Integration

- [ ] 5.1 Implement ArrowCopyHandler for COPY TO/FROM
- [ ] 5.2 Add `FORMAT 'arrow'` support to COPY TO with compression options
- [ ] 5.3 Add `FORMAT 'arrow'` support to COPY FROM
- [ ] 5.4 Add `COMPRESSION` option support (none, lz4, zstd)

## Phase 6: Testing & Documentation

- [ ] 6.1 Create unit tests for Arrow type conversion (all 46+ types)
- [ ] 6.2 Create integration tests for file reading with real Arrow files
- [ ] 6.3 Create integration tests for file writing and round-trip verification
- [ ] 6.4 Add cloud storage integration tests (S3, GCS, Azure URLs)
- [ ] 6.5 Add performance benchmarks for read/write operations
- [ ] 6.6 Add package documentation and usage examples
