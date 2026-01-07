# Change: Add Arrow/IPC Format Support

## Why

Arrow IPC (Inter-Process Communication) format enables efficient data exchange with Arrow-enabled systems. While dukdb-go already has Arrow RecordBatch conversion support, it lacks the ability to read/write Arrow IPC files directly. This gap prevents:

- Loading pre-computed Arrow data from files
- Exporting query results in Arrow IPC format for downstream tools
- Streaming large datasets via Arrow IPC protocol
- Integration with data pipelines that use Arrow IPC as interchange format

## What Changes

### File Format Support

- `read_arrow('path')` - Table function to read Arrow IPC files
- `read_arrow_auto('path')` - Auto-detect Arrow IPC file format
- `COPY table TO 'path' (FORMAT 'arrow')` - Export to Arrow IPC
- `COPY (SELECT ...) TO 'path' (FORMAT 'arrow')` - Export query results to Arrow IPC

### RecordBatch Streaming

- `ArrowStream` type for streaming RecordBatches to/from files
- Support for both file format (random access) and streaming format (sequential)
- Memory-mapped file support for efficient large file handling

### Integration Points

- Cloud storage support (S3, GCS, Azure) via existing FileSystem interface
- Compression support (LZ4, ZSTD) for Arrow IPC blocks
- Type mapping between DuckDB and Arrow (extends existing conversion)

## Impact

- **Affected specs**: file-io (adds Arrow format), arrow-integration (extends with IPC), storage (adds Arrow backend)
- **Affected code**: `internal/io/arrow/` (new), `internal/executor/table_funcs.go`
- **Dependencies**: `github.com/apache/arrow/go/v18` (already a dependency)
- **Consumers**: Data pipelines, ML frameworks, analytics applications

## Breaking Changes

None. This is an additive feature.

## Implementation Notes

Arrow IPC has two formats:
1. **File format** (`.arrow`): Random-access with footer containing schema and block locations
2. **Stream format** (`.arrows`): Sequential access, schema sent first then batches

The pure Go implementation uses `github.com/apache/arrow/go/v18` for:
- IPC file reading via `ipc.NewFileReader`
- IPC file writing via `ipc.NewFileWriter`
- Stream reading via `ipc.NewReader`
- Stream writing via `ipc.NewWriter`
- Memory allocation via Arrow's native allocator
