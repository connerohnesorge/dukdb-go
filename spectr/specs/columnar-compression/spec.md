# Columnar Compression Specification

## Requirements

### Requirement: Columnar Compression Interface

The system SHALL provide a columnar compression interface that operates on typed Vector data to produce compact in-memory representations of column segments.

#### Scenario: Compression type enumeration

- WHEN the system defines columnar compression types
- THEN the following types SHALL be available: None, Constant, Dictionary, RLE

#### Scenario: CompressedSegment structure

- WHEN a column segment is compressed
- THEN the result SHALL contain the compression type, original data type, row count, null count, a validity mask, and a scheme-specific payload

#### Scenario: Round-trip correctness

- WHEN a Vector is compressed and then decompressed
- THEN the decompressed Vector SHALL contain identical values and NULL positions as the original

### Requirement: Constant Compression

The system SHALL implement Constant compression that stores a single value when all non-NULL values in a column segment are identical.

#### Scenario: All values identical

- WHEN a column segment contains 1000 rows where all non-NULL values equal 42
- THEN Constant compression SHALL be selected
- AND the compressed segment SHALL store only the single value 42 plus the validity mask

#### Scenario: Mixed values reject constant

- WHEN a column segment contains values [1, 2, 1, 2]
- THEN Constant compression SHALL NOT be selected for that segment

#### Scenario: All NULL segment

- WHEN a column segment contains only NULL values
- THEN Constant compression SHALL be selected with a nil constant value

#### Scenario: Constant decompression

- WHEN a Constant-compressed segment with value 42 and 500 rows is decompressed
- THEN the result SHALL be a Vector with 500 entries all equal to 42 (respecting the validity mask for NULLs)

### Requirement: Dictionary Compression

The system SHALL implement Dictionary compression that stores a dictionary of unique values and per-row index codes when the number of distinct non-NULL values is below a configurable threshold.

#### Scenario: Low cardinality column

- WHEN a column segment contains 10000 rows with only 5 distinct string values
- THEN Dictionary compression SHALL be selected
- AND the compressed segment SHALL store the 5 unique values and 10000 uint16 index codes

#### Scenario: High cardinality reject dictionary

- WHEN a column segment contains 10000 rows with 5000 distinct values
- THEN Dictionary compression SHALL NOT be selected for that segment

#### Scenario: Dictionary threshold configuration

- WHEN the dictionary threshold is set to 256
- THEN columns with more than 256 distinct non-NULL values SHALL NOT use Dictionary compression

#### Scenario: Dictionary with NULLs

- WHEN a column segment contains values [A, NULL, B, NULL, A]
- THEN the dictionary SHALL contain [A, B]
- AND NULL positions SHALL be tracked via the validity mask

#### Scenario: Dictionary decompression

- WHEN a Dictionary-compressed segment is decompressed
- THEN each row's value SHALL be looked up from the dictionary using its index code
- AND NULL rows SHALL produce nil values

### Requirement: RLE Compression

The system SHALL implement Run-Length Encoding compression that stores consecutive repeated values as (value, run-length) pairs.

#### Scenario: Repeated runs

- WHEN a column segment contains values [1, 1, 1, 2, 2, 3, 3, 3, 3]
- THEN RLE compression SHALL produce runs: (1, 3), (2, 2), (3, 4)

#### Scenario: No runs reject RLE

- WHEN a column segment contains values [1, 2, 3, 4, 5] with no consecutive repeats
- THEN RLE compression SHALL NOT be selected (average run length below threshold)

#### Scenario: RLE minimum run length threshold

- WHEN the average run length across a segment is below the configured minimum (default 2.0)
- THEN RLE compression SHALL NOT be selected for that segment

#### Scenario: RLE with NULLs

- WHEN a column segment contains values [1, 1, NULL, 1, 1]
- THEN NULL values SHALL be treated as run breaks
- AND the validity mask SHALL track NULL positions independently

#### Scenario: RLE decompression

- WHEN an RLE-compressed segment with runs (A, 3), (B, 2) is decompressed
- THEN the result SHALL be a Vector with values [A, A, A, B, B]

### Requirement: Compression Selection

The system SHALL automatically select the best compression scheme for each column segment based on data analysis.

#### Scenario: Selection waterfall

- WHEN analyzing a column segment for compression
- THEN the system SHALL try schemes in order: Constant, Dictionary, RLE, None
- AND the first applicable scheme SHALL be selected

#### Scenario: Analysis is O(n)

- WHEN analyzing a column segment with n rows
- THEN the analysis SHALL complete in a single pass over the data

#### Scenario: Uncompressed fallback

- WHEN no compression scheme provides a benefit for a column segment
- THEN the segment SHALL remain uncompressed (None)

#### Scenario: Per-column independence

- WHEN a row group has columns [A, B, C]
- THEN each column SHALL be analyzed and compressed independently
- AND column A may use Constant while column B uses Dictionary and column C uses None

### Requirement: Row Group Compression Integration

The system SHALL compress column segments when an in-memory `RowGroup` (`internal/storage/table.go`) becomes full or is flushed. Note: this applies to the in-memory `RowGroup` type, NOT the `DuckDBRowGroup` disk persistence format in `internal/storage/rowgroup.go`.

#### Scenario: Automatic compression on full row group

- WHEN a RowGroup reaches its capacity (`RowGroupSize` = 122880 rows) and a new RowGroup is created in `Table.AppendChunk` or `Table.InsertVersioned`
- THEN each column in the now-full row group SHALL be analyzed and compressed if beneficial

#### Scenario: Transparent decompression on scan

- WHEN a TableScanner reads from a row group with compressed columns
- THEN the scanner SHALL transparently decompress the data
- AND the returned DataChunk SHALL contain uncompressed values identical to what would be returned without compression

#### Scenario: Write path unaffected

- WHEN rows are appended to a non-full row group
- THEN no compression SHALL occur (compression is deferred to finalization)

#### Scenario: Thread safety during compression

- WHEN a row group is compressed after becoming full
- THEN compression SHALL occur under both the Table's write lock (`t.mu`) and the RowGroup's write lock (`rg.mu`)
- AND concurrent readers SHALL not see partially compressed state

