# Change: Add Columnar Compression for Storage Engine

## Why

The current storage engine stores column data uncompressed in row groups. Each Vector holds raw typed slices (e.g., `[]int64`, `[]string`) at full width regardless of data patterns. For analytical workloads with millions of rows, this wastes memory, increases disk I/O during persistence, and slows full-table scans because the CPU must touch more cache lines. DuckDB v1.4.3 addresses this with lightweight, columnar-aware compression schemes -- Constant, Dictionary, and RLE -- that exploit common data patterns (uniform columns, low-cardinality dimensions, sorted/repeated runs) to shrink column segments by 2-10x without measurable decompression overhead on the scan path.

## What Changes

- Add a `ColumnSegmentCompression` interface in `internal/storage/` that operates on typed column data (not raw bytes) and produces a compact `CompressedSegment` that can be decompressed back to a Vector
- Implement three compression schemes:
  - **Constant**: when all non-NULL values in a segment are identical, store the single value plus a NULL bitmap
  - **Dictionary**: when the number of distinct non-NULL values is below a threshold, store a dictionary of unique values plus per-row index codes
  - **RLE (Run-Length Encoding)**: when consecutive values repeat, store (value, run-length) pairs
- Add an `AnalyzeAndCompress` function that inspects a column segment and selects the best scheme (Constant > Dictionary > RLE > Uncompressed) based on data statistics
- Integrate compression into `RowGroup` (the in-memory columnar storage type in `internal/storage/table.go`, not the `DuckDBRowGroup` disk persistence format in `internal/storage/rowgroup.go`) so that full or flushed row groups compress their columns automatically. Note: no finalization step currently exists; it must be added to `Table.AppendChunk` and `Table.InsertVersioned` when a row group reaches capacity.
- Ensure `TableScanner` (in `internal/storage/table.go`) transparently decompresses on read so all existing query execution is unaffected
- Bitpacking and FSST are explicitly deferred to future work

## Impact

- Affected specs: `storage` (column segment format, row group lifecycle)
- New spec: `columnar-compression` (compression interface, scheme selection, integration)
- Affected code:
  - `internal/storage/column.go` -- Vector may gain a compressed backing store
  - `internal/storage/table.go` -- `RowGroup` (line 765) gains a `compressed` field; `Table.AppendChunk` (line 270) and `Table.InsertVersioned` (line 1079) trigger compression when a row group fills up; `TableScanner.Next` (line 939) decompresses on read
  - `internal/storage/chunk.go` -- DataChunk is a read-only container and does NOT need changes; the scanner (not the chunk) handles decompression
  - `internal/storage/rowgroup.go` -- `DuckDBRowGroup` (the disk persistence format) is NOT modified by this change; columnar compression is in-memory only
  - `internal/compression/` -- existing byte-level codecs (operating on `[]byte` via the `Compressor`/`Decompressor` interfaces) are orthogonal; this change adds semantic columnar compression above them operating on typed `*Vector` data
