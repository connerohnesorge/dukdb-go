# TableStatistics Format Documentation

## Summary

This document describes the TableStatistics format found in DuckDB database files, extracted from CLI-generated files using the test suite in `table_statistics_format_test.go`.

## The Problem

Task 8.2 is blocked because of a mismatch in table_pointer.offset:
- **CLI database**: table_pointer.offset = 214 bytes
- **dukdb-go writes**: table_pointer.offset = 8 bytes
- **Difference**: 206 bytes

This 206-byte difference is the TableStatistics data that DuckDB CLI writes but dukdb-go currently does not.

## TableStatistics Structure

TableStatistics is written using DuckDB's BinarySerializer format. It appears **before** row_group_count in the table data block.

### Layout in Table Data Block

```
[TableStatistics]              // Offset 0 to table_pointer.offset
[row_group_count (uint64)]     // 8 bytes, raw (not varint)
[RowGroupPointer objects...]   // Starts at table_pointer.offset
```

### TableStatistics BinarySerializer Format

```
Field 100: column_stats (vector<shared_ptr<ColumnStatistics>>)
  - Count (varint): number of columns
  - For each column:
    - Nullable byte: 0x00 = null, non-zero = present
    - If present: ColumnStatistics object (nested BinarySerializer)
      - Contains HyperLogLog data (~100+ bytes per column)
      - Terminated with 0xFF 0xFF

Field 101: table_sample (optional unique_ptr<ReservoirSample>)
  - Nullable byte: indicates if sample is present
  - If present: ReservoirSample object (nested BinarySerializer)

Terminator: 0xFF 0xFF (2 bytes)
```

## Size Analysis

From test runs with DuckDB CLI-generated files:

| Columns | Total Size | Avg per Column |
|---------|------------|----------------|
| 1       | 104 bytes  | 104 bytes      |
| 2       | 211 bytes  | 105 bytes      |
| 3       | 321 bytes  | 107 bytes      |

**Pattern**: Approximately 104-107 bytes per column, plus some overhead.

## Example: 2-Column Table

For a table with 2 columns (id INTEGER, name VARCHAR) and 3 rows:

```
TablePointer.Offset: 214 bytes
Total TableStatistics: 214 bytes
Breakdown:
  - Overhead (field IDs, count, terminators): ~10 bytes
  - Column 0 statistics: ~102 bytes
  - Column 1 statistics: ~102 bytes
```

### Hex Dump (First 100 bytes)

```
0000 | 64 00 01 65 00 03 66 00 64 00 01 ff ff 67 00 01  | d..e..f.d....g..
0010 | 68 00 64 00 00 65 00 01 66 00 00 67 00 c8 00 64  | h.d..e..f..g...d
0020 | 00 01 65 00 01 ff ff c9 00 64 00 01 65 00 03 ff  | ..e......d..e...
0030 | ff ff ff ff ff ff ff 65 00 64 00 01 65 00 03 66  | .......e.d..e..f
0040 | 00 64 00 7f ff ff 67 00 02 68 00 64 00 00 65 00  | .d....g..h.d..e.
0050 | 01 66 00 00 67 00 ff ff ff ff ff ff ff ff ff ff  | .f..g...........
0060 | 64 00 01 65                                      | d..e
```

Interpretation:
- `64 00` (0x0064) = Field 100 (column_stats)
- `01` = Count: 1 element in first structure
- `65 00` (0x0065) = Field 101 (?)
- ... nested ColumnStatistics data ...
- `ff ff` = Terminator

## ColumnStatistics Content

Each ColumnStatistics object contains:
- Field 100: has_stats (bool)
- Field 101: has_null (bool)
- Field 102: type_stats (nested, type-specific statistics)
  - For INTEGER: NumericStats with min/max values
  - For VARCHAR: StringStats with min/max strings
  - For DOUBLE: NumericStats with min/max doubles
- HyperLogLog data for cardinality estimation (~3KB per column)
- Terminator: 0xFF 0xFF

## Impact on dukdb-go

### Current Behavior

dukdb-go writes table_pointer.offset = 8 because it:
1. Does NOT write TableStatistics
2. Immediately writes row_group_count (8 bytes from start)
3. Then writes RowGroupPointer objects

This causes DuckDB CLI to fail when reading dukdb-go files:
```
Error: Serialization Error: Failed to deserialize: field id mismatch, expected: 100, got: 0
```

The CLI expects to find Field 100 (column_stats) at offset 0, but instead finds the raw row_group_count value.

### Solution Required

To achieve CLI compatibility, dukdb-go must:

1. **Write TableStatistics** before row_group_count
2. Calculate proper statistics for each column:
   - has_stats: true
   - has_null: check if any values are NULL
   - min/max values from the data
   - HyperLogLog sketch for cardinality (or minimal placeholder)
3. Update table_pointer.offset to point AFTER TableStatistics

## Minimal TableStatistics Implementation

For initial compatibility, we can write minimal TableStatistics:

```
Field 100: column_stats
  Count: <column_count>
  For each column:
    Nullable byte: 0x01 (present)
    ColumnStatistics:
      Field 100: has_stats = 0 (false)
      Field 101: has_null = 0 (false)
      Terminator: 0xFF 0xFF

Field 101: table_sample (optional)
  Nullable byte: 0x00 (not present)

Terminator: 0xFF 0xFF
```

This minimal version would be approximately:
- Overhead: ~6 bytes (field IDs, counts, terminators)
- Per column: ~8 bytes (nullable + minimal ColumnStatistics)
- Total for N columns: ~(6 + 8*N) bytes

**Note**: This is much smaller than what DuckDB CLI generates (104+ bytes per column), because we're omitting:
- Actual min/max statistics
- HyperLogLog data
- Other metadata

However, it should be sufficient for basic CLI compatibility.

## Test Coverage

Tests in `table_statistics_format_test.go`:

1. **TestExtractTableStatisticsFormat**: Extracts and hex-dumps TableStatistics from a 2-column table
2. **TestTableStatisticsRoundTrip**: Verifies structure for 3-column table
3. **TestMinimalTableStatistics**: Documents minimum size for 1-column table
4. **TestCompareTableStatisticsSizes**: Compares sizes across 1-5 columns

Run tests with:
```bash
go test -v -run TestExtractTableStatisticsFormat ./internal/storage/duckdb/
go test -v -run TestTableStatistics ./internal/storage/duckdb/
```

## Next Steps

1. Implement TableStatistics serialization in `catalog_serializer.go`
2. Update `serializeTableDataPointer()` to write TableStatistics before row_group_count
3. Calculate basic statistics from row data (min/max, has_null)
4. Add HyperLogLog sketches (optional for initial compatibility)
5. Verify CLI can read the generated files

## References

- DuckDB source: `src/storage/table/table_statistics.cpp`
- BinarySerializer format: `src/common/serializer/binary_serializer.cpp`
- Test extraction: `internal/storage/duckdb/table_statistics_format_test.go`
