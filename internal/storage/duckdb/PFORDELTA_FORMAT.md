# PFOR-DELTA Compression Format

## Overview

PFOR-DELTA (Patched Frame of Reference with Delta encoding) is a compression algorithm used by DuckDB for integer sequences with small deltas. It's particularly effective for:

- Sequential integers (IDs, counters)
- Timestamps with regular intervals
- Date sequences
- Time series data with consistent gaps

## Format Specification

### Binary Layout

```
[int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
```

**Header (17 bytes):**
- Bytes 0-7: Reference value (int64, little-endian) - the first value in the sequence
- Byte 8: Bit width (uint8) - number of bits needed per delta
- Bytes 9-16: Value count (uint64, little-endian) - total number of values

**Deltas (variable length):**
- Bit-packed delta values using the specified bit width
- Each delta represents the difference from the previous value
- Deltas are unsigned and packed sequentially
- Total delta bytes: `((count - 1) * bitWidth + 7) / 8`

### Example

Compressing `[100, 101, 102, 103, 104]`:

```
Reference: 100 (8 bytes)
Bit width: 1 (1 byte) - each delta is 1, needs 1 bit
Count: 5 (8 bytes)
Deltas: [1, 1, 1, 1] packed into 1 byte
Total: 17 + 1 = 18 bytes (vs 40 bytes uncompressed)
```

## Implementation

### Compression

Located in `compress_write.go`:

- `CompressPFORDelta(data []byte, valueSize int) ([]byte, error)` - Main compression function
- `CompressPFORDeltaFromInt64(values []int64) ([]byte, bool)` - Compress from int64 slice
- `TryCompressPFORDeltaFromBytes(data []byte, valueSize int) ([]byte, bool)` - Try compression from bytes

**Algorithm:**

1. Convert input data to int64 values
2. Calculate deltas from consecutive values
3. Find maximum delta to determine bit width
4. Check if compression is beneficial (compressed size < original size)
5. Pack deltas using bit-packing
6. Write header + packed deltas

### Decompression

Located in `compress.go`:

- `DecompressPFORDelta(data []byte, valueSize int, count uint64) ([]byte, error)` - Main decompression
- `DecompressPFORDeltaToInt64(data []byte) ([]int64, error)` - Decompress to int64 slice

**Algorithm:**

1. Read reference value, bit width, and count from header
2. Unpack deltas using bit width
3. Reconstruct values by adding deltas to previous values
4. Convert back to requested value size

## Compression Effectiveness

### Best Case Scenarios

**Sequential integers (delta = 1):**
- 100 values: 800 bytes → 30 bytes (96.2% savings)
- Bit width: 1 bit per delta

**Timestamps with second intervals:**
- 100 timestamps: 800 bytes → 30 bytes (96.2% savings)
- Deltas are constant (1 second)

**Date sequences (consecutive days):**
- 365 days: 2920 bytes → 63 bytes (97.8% savings)
- Deltas are constant (1 day)

### Variable Performance

**Timestamps with hour intervals:**
- 100 timestamps: 800 bytes → 166 bytes (79.2% savings)
- Larger deltas require more bits

**Timestamps with day intervals:**
- 100 timestamps: 800 bytes → 228 bytes (71.5% savings)
- Even larger deltas

### Not Beneficial

**Highly variable data:**
- Random integers with large gaps
- Deltas require nearly full bit width
- Overhead of header makes it worse than uncompressed

## DuckDB Interoperability

### Compression Type Detection

When DuckDB creates databases with sequential data:

- Column compression is marked as `AUTO` in catalog
- Actual compression is chosen at write time
- May use PFOR_DELTA, BITPACKING, or other algorithms
- Choice depends on data patterns and heuristics

### Test Results

Test databases created with DuckDB CLI show:

1. **Sequential integers (0-999):** `AUTO` compression chosen
2. **Timestamp sequences:** `AUTO` compression chosen
3. **Date sequences:** `AUTO` compression chosen

All files created by DuckDB are readable by dukdb-go, confirming format compatibility.

## Usage Guidelines

### When to Use PFOR-DELTA

✅ **Good candidates:**
- Sequential IDs or counters
- Timestamps with regular intervals (seconds, minutes, hours)
- Date sequences
- Time series with consistent deltas
- Any integer sequence where deltas are small

❌ **Poor candidates:**
- Random integers
- Data with large gaps
- Fewer than ~10 values (overhead not worth it)
- Non-sequential data

### Automatic Selection

The compression selection algorithm in `compress_select.go` automatically chooses PFOR-DELTA when:

1. Data is integer type (TINYINT, SMALLINT, INTEGER, BIGINT)
2. Deltas between consecutive values are small
3. Compression ratio is beneficial (compressed < original)

## Performance Characteristics

### Compression Speed

- Fast: O(n) single pass over data
- Minimal computation (delta calculation + bit packing)
- No dictionary building or complex analysis

### Decompression Speed

- Very fast: O(n) single pass
- Simple bit unpacking + addition
- Sequential memory access pattern

### Memory Usage

- Compression: O(n) for delta array
- Decompression: O(1) streaming decompression possible

## Testing

Comprehensive test suite in `compress_pfordelta_test.go`:

- **Unit tests:** Format verification, edge cases, round-trip
- **Data type tests:** Sequential, timestamps, dates
- **Interop tests:** Compatibility with DuckDB CLI
- **Value size tests:** Support for 1, 2, 4, 8 byte integers

Run tests:
```bash
go test -v -run TestPFORDelta ./internal/storage/duckdb/...
```

## References

- DuckDB source: `src/include/duckdb/common/enums/compression_type.hpp`
- PFOR algorithm: Patched Frame of Reference encoding
- Implementation: `internal/storage/duckdb/compress_write.go` and `compress.go`
