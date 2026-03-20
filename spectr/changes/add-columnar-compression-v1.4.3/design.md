## Implementation Details

### Compression Interface

```go
// internal/storage/compression.go

// SegmentCompressionType identifies the columnar compression scheme.
type SegmentCompressionType uint8

const (
    SegmentCompressionNone       SegmentCompressionType = 0
    SegmentCompressionConstant   SegmentCompressionType = 1
    SegmentCompressionDictionary SegmentCompressionType = 2
    SegmentCompressionRLE        SegmentCompressionType = 3
)

// CompressedSegment holds the compressed representation of a column segment.
type CompressedSegment struct {
    Type       SegmentCompressionType
    DataType   dukdb.Type
    Count      int              // number of logical rows
    NullCount  int              // number of NULL values
    Validity   *ValidityMask    // NULL bitmap (shared across all schemes)
    Data       any              // scheme-specific payload (see below)
}

// ConstantPayload stores a single value for all non-NULL rows.
type ConstantPayload struct {
    Value any
}

// DictionaryPayload stores unique values and per-row indices.
type DictionaryPayload struct {
    Dictionary []any    // unique non-NULL values
    Indices    []uint16 // per-row index into Dictionary (0xFFFF = NULL)
}

// RLEPayload stores (value, run-length) pairs.
type RLEPayload struct {
    Values     []any    // one per run
    RunLengths []uint32 // one per run
}
```

### Compression Selection Algorithm

The selection follows a waterfall: try the most compact scheme first, fall back to less compact ones.

```
AnalyzeAndCompress(vec *Vector) *CompressedSegment:
    1. If all non-NULL values are identical -> Constant
    2. If distinct count <= DictionaryThreshold (default 256) -> Dictionary
    3. If average run length >= RLEMinRunLength (default 2.0) -> RLE
    4. Otherwise -> None (store uncompressed)
```

Thresholds are configurable but start with conservative defaults. The analysis pass is O(n) and touches each value exactly once.

### Dictionary Index Width

`DictionaryPayload.Indices` uses `uint16` which supports up to 65,535 distinct values. The `DictionaryThreshold` default of 256 keeps indices well within `uint8` range, but `uint16` gives headroom for future tuning without a format change. If the dictionary exceeds 65,535 entries, the scheme falls back to RLE or Uncompressed.

### Integration with Row Groups

There are two distinct RowGroup types in the codebase that must not be conflated:

1. **`RowGroup`** (`internal/storage/table.go`, line 765): The in-memory columnar storage type with fields `mu sync.RWMutex`, `columns []*Vector`, `count int`, `capacity int`. This is the target for columnar compression.
2. **`DuckDBRowGroup`** (`internal/storage/rowgroup.go`, line 41): The disk persistence format with `MetaData`, `ColumnData []*DuckDBColumnSegment`, and `IndexData`. This is NOT modified by this change.

Compression happens when a row group becomes full (reaches `RowGroupSize`, which is 122880 rows). Currently, the code in `Table.AppendChunk` (line 270) and `Table.InsertVersioned` (line 1079) simply creates a new RowGroup when the current one is full -- there is no finalization step. This change must add a compression step at that transition point: after the old row group fills up and before it becomes read-only, call `Compress()` on it.

```go
// RowGroup (internal/storage/table.go) gains:
type RowGroup struct {
    mu         sync.RWMutex
    columns    []*Vector
    count      int
    capacity   int
    compressed []*CompressedSegment // one per column, nil = uncompressed
}
```

On read, `TableScanner.Next()` (line 939) checks whether a column has a compressed segment. If so, it decompresses the needed row range into a temporary Vector for the chunk being returned. The scanner currently reads row-by-row via `RowGroup.GetValue()` and constructs DataChunks of up to `StandardVectorSize` (2048) rows, so decompression should produce a Vector for the requested row range rather than materializing the entire row group.

### Decompression on the Read Path

Each scheme has a `Decompress(seg *CompressedSegment, startRow, count int) *Vector` function that materializes the requested row range into an uncompressed Vector. The scanner (`TableScanner.Next()` in `internal/storage/table.go`, line 939) currently reads values row-by-row from `rg.columns[col]` via `GetValue()`. When `rg.compressed[col]` is non-nil, the scanner should instead decompress the needed row range into a temporary Vector and read from that, avoiding per-row decompression overhead.

### Thread Safety

Compression must be performed under the Table's write lock (`t.mu`), which is already held in `Table.AppendChunk` and `Table.InsertVersioned` when the row group transition occurs. The RowGroup's own write lock (`rg.mu`) should also be held during compression to prevent concurrent reads from seeing a partially compressed state. Note: the RowGroup write lock is NOT "already held during AppendRow when the group becomes full" -- `RowGroup.AppendRow` only holds its lock for the duration of the append. The compression trigger must explicitly acquire the lock on the now-full row group. Decompression is read-only and safe under the existing RLock.

## Context

The existing `internal/compression/` package provides byte-level codecs (RLE, BitPacking, FSST, Chimp, Zstd) that implement the `Compressor` (operating on `[]byte` input) and `Decompressor` interfaces. These are used by the `DuckDBRowGroup` disk persistence format (via `DuckDBColumnSegment.Compression`), not by the in-memory `RowGroup` type. This proposal adds a higher-level layer that operates on typed column data (`*Vector`) and produces semantically meaningful compressed representations for in-memory use. The two layers are complementary: the byte-level codecs serve the persistence layer (`internal/storage/rowgroup.go`), while the columnar compression serves the in-memory storage layer (`RowGroup` in `internal/storage/table.go`).

## Goals / Non-Goals

- Goals:
  - Reduce in-memory footprint for columns with compressible patterns
  - Transparent decompression so all existing queries work unchanged
  - Simple, well-tested implementations of Constant, Dictionary, RLE
  - Configurable thresholds for scheme selection

- Non-Goals:
  - Bitpacking (deferred -- requires bit-width analysis per integer type)
  - FSST string compression at the columnar level (deferred -- the byte-level FSST codec exists)
  - Disk format changes (compression is in-memory only for this phase)
  - Vectorized decompression / SIMD (future optimization)

## Decisions

- Decision: Use `uint16` for dictionary indices
  - Alternatives: `uint8` (too small for some workloads), `uint32` (wastes space for typical cardinalities)
  - Rationale: 65K distinct values covers the vast majority of dictionary-compressible columns while keeping indices compact

- Decision: Compress at row group finalization, not incrementally
  - Alternatives: Compress each appended chunk immediately
  - Rationale: Compressing a full row group gives better statistics and avoids repeated re-compression during ingestion

- Decision: Columnar compression is separate from byte-level compression
  - Alternatives: Extend existing `compression.Codec` interface
  - Rationale: The abstractions are fundamentally different -- byte codecs work on `[]byte`, columnar compression works on typed `*Vector` data

## Risks / Trade-offs

- Decompression overhead on scan: mitigated by keeping schemes simple (Constant is O(1), Dictionary is O(n) with array lookup, RLE is O(runs))
- Memory spike during compression: mitigated by compressing one column at a time and releasing the original Vector afterward
- Complexity in TableScanner: mitigated by a clean abstraction boundary (compressed vs uncompressed check is a single nil test)

## Open Questions

- Should we support partial decompression for predicate pushdown (e.g., check Dictionary entries before materializing)?
- Should compressed segments be written to WAL, or should WAL always use uncompressed format?
