## 1. Core Compression Types and Interface

- [x] 1.1 Define SegmentCompressionType enum (None, Constant, Dictionary, RLE) in `internal/storage/compression.go`
- [x] 1.2 Define CompressedSegment struct with Type, DataType, Count, NullCount, Validity, and Data fields
- [x] 1.3 Define ConstantPayload, DictionaryPayload, and RLEPayload structs
- [x] 1.4 Write unit tests for type definitions and String() methods

## 2. Constant Compression Implementation

- [x] 2.1 Implement `CompressConstant(vec *Vector) *CompressedSegment` that detects all-same-value columns
- [x] 2.2 Implement `DecompressConstant(seg *CompressedSegment, startRow, count int) *Vector`
- [x] 2.3 Handle edge cases: all-NULL segment, single-row segment, empty segment
- [x] 2.4 Write unit tests for Constant compress/decompress round-trip across all supported types (int, float, string, bool)

## 3. Dictionary Compression Implementation

- [x] 3.1 Implement `CompressDictionary(vec *Vector, threshold int) *CompressedSegment` with configurable distinct-value threshold
- [x] 3.2 Implement `DecompressDictionary(seg *CompressedSegment, startRow, count int) *Vector`
- [x] 3.3 Handle NULL values (tracked via validity mask, not in dictionary)
- [x] 3.4 Write unit tests for Dictionary compress/decompress with low-cardinality string, integer, and mixed-NULL columns

## 4. RLE Compression Implementation

- [x] 4.1 Implement `CompressRLE(vec *Vector, minAvgRunLength float64) *CompressedSegment` with configurable run-length threshold
- [x] 4.2 Implement `DecompressRLE(seg *CompressedSegment, startRow, count int) *Vector`
- [x] 4.3 Handle NULL values as run breaks in the RLE encoding
- [x] 4.4 Write unit tests for RLE compress/decompress with repeated-value, sorted, and random data

## 5. Compression Selection (AnalyzeAndCompress)

- [x] 5.1 Implement `AnalyzeAndCompress(vec *Vector) *CompressedSegment` with the waterfall: Constant > Dictionary > RLE > None
- [x] 5.2 Implement `DecompressSegment(seg *CompressedSegment, startRow, count int) *Vector` dispatcher
- [x] 5.3 Add configurable thresholds (DictionaryThreshold, RLEMinRunLength) with sensible defaults
- [x] 5.4 Write unit tests verifying correct scheme selection for various data patterns

## 6. Row Group Integration

- [x] 6.1 Add `compressed []*CompressedSegment` field to `RowGroup` struct (`internal/storage/table.go`, line 765). Note: this is the in-memory `RowGroup`, NOT the `DuckDBRowGroup` disk format in `internal/storage/rowgroup.go`.
- [x] 6.2 Add `Compress()` method to RowGroup that analyzes and compresses each column
- [x] 6.3 Trigger compression when a row group fills up in `Table.AppendChunk` (line 270) and `Table.InsertVersioned` (line 1079). Currently these methods simply create a new RowGroup when the old one is full -- add a `Compress()` call on the old row group at that transition point. Note: `Table.AppendRow` (line 352) delegates to the current RowGroup's `AppendRow`; `Table.AppendChunk` (line 270) is the batch path.
- [x] 6.4 Write unit tests verifying row group compression triggers at capacity

## 7. Scanner Integration

- [x] 7.1 Update `TableScanner.Next()` (`internal/storage/table.go`, line 939) to check for `rg.compressed[col]` and decompress the needed row range into a temporary Vector, rather than reading row-by-row from `rg.columns[col]` via `GetValue()`. Note: `DataChunk` (`internal/storage/chunk.go`) is a read-only container and does NOT need changes.
- [x] 7.2 Ensure all existing table scan tests continue to pass with compression enabled
- [x] 7.3 Write integration test: insert data, verify compression occurs, scan and verify correctness

## 8. Verification and Cleanup

- [x] 8.1 Run full test suite (`nix develop -c tests`) and confirm no regressions
- [x] 8.2 Run linter (`nix develop -c lint`) and fix any issues
- [x] 8.3 Verify compression reduces memory for representative workloads (low-cardinality, sorted, constant columns)
