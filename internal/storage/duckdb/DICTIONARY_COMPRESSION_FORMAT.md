# DuckDB Dictionary Compression Format

This document describes the dictionary compression format used by DuckDB for VARCHAR columns with repeated values.

## Overview

Dictionary compression is used when a VARCHAR column has many repeated values. Instead of storing the full string for each row, DuckDB stores:
1. A dictionary of unique strings
2. Bit-packed indices that map each row to a string in the dictionary

## Format Layout

The dictionary compression format consists of 4 main components:

### 1. Header (20 bytes)

The `dictionary_compression_header_t` structure contains:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 bytes | dict_size | SIZE in bytes of dictionary data (NOT count!) |
| 4 | 4 bytes | dict_end | Absolute end offset of entire compressed segment |
| 8 | 4 bytes | index_buffer_offset | Offset to index buffer |
| 12 | 4 bytes | index_buffer_count | Number of unique strings (index entries) |
| 16 | 4 bytes | bitpacking_width | Bit width for selection buffer |

**Important**: `dict_size` is the SIZE in bytes, not the count of unique strings!

### 2. Selection Buffer (bit-packed)

Starting at offset 20 (immediately after header), continuing until `index_buffer_offset`.

- Contains one index per tuple (row)
- Bit-packed with width specified in `bitpacking_width`
- Each value is an index into the index buffer
- Maps: row_number → index_buffer_entry

### 3. Index Buffer (array of uint32)

Starting at `index_buffer_offset`, containing `index_buffer_count` entries.

- Array of uint32 offsets into dictionary
- Each offset points to where a string starts in the dictionary
- String length is calculated as: `next_offset - current_offset`
- Last entry may be an end marker equal to dictionary size

### 4. Dictionary (concatenated strings)

Starting at `index_buffer_offset + (index_buffer_count * 4)`, ending at `dict_end`.

- Concatenated string bytes
- No length prefixes
- No null terminators
- Strings are extracted using offsets from index buffer

## Example

For a table with:
```sql
VALUES ('apple'), ('banana'), ('apple'), ('cherry'), ('apple'), ('banana'), ('apple'), ('banana')
```

The dictionary compression might look like:

```
Header (20 bytes):
  dict_size: 17                 (6 + 6 + 5 = 17 bytes total)
  dict_end: 61                  (absolute offset where segment ends)
  index_buffer_offset: 28       (header=20 + selection_buffer=8)
  index_buffer_count: 4         (3 unique strings + end marker?)
  bitpacking_width: 2           (2 bits per index: 0, 1, 2, 3)

Selection Buffer (8 bytes at offset 20-27):
  Bit-packed indices: [2, 1, 2, 0, 2, 1, 2, 1]
  (each value is 2 bits, mapping to index buffer)

Index Buffer (16 bytes at offset 28-43):
  [0, 5, 11, 17]
  (offsets into dictionary)

Dictionary (17 bytes at offset 44-60):
  "cherrybananaapple"

Unique strings extracted:
  0: dictionary[0:5] = "cherry"   (but index says 0:5 = "cherr"?)
  1: dictionary[5:11] = "banana"  (but index says 5:11 = "ybanan"?)
  2: dictionary[11:17] = "apple"  (but index says 11:17 = "aapple"?)
```

**Note**: There's a discrepancy in the exact offset interpretation that needs further investigation.

## Comparison with UNCOMPRESSED Format

| Aspect | DICTIONARY | UNCOMPRESSED |
|--------|-----------|--------------|
| Header | 20-byte dictionary_compression_header_t | 8-byte heap header |
| Indices | Bit-packed selection buffer | Cumulative offset index |
| Strings | Concatenated (no lengths) | Concatenated (no lengths) |
| Order | Dictionary order | Reverse order in heap |
| Best for | Few unique values | Many unique values |

## Implementation Status

- **Reading**: Partially implemented in `rowgroup_reader.go`
  - Header parsing: ✓
  - Dictionary extraction: Needs work (offset interpretation issue)
  - Decompression: Not yet implemented (returns error)

- **Writing**: Not yet implemented
  - Requires bitpacking support
  - Requires string deduplication logic
  - Requires selection buffer generation

## Test Coverage

- `TestDictionaryCompressionFormat`: Verifies format structure and header parsing
- `TestDictionaryCompressionDocumentation`: Documents format for reference

## References

- DuckDB source: `src/storage/compression/dictionary/`
- Header definition: `src/include/duckdb/storage/compression/dictionary/common.hpp`
- Decompression: `src/storage/compression/dictionary/decompression.cpp`

## Next Steps

1. Resolve offset interpretation discrepancy in index buffer
2. Implement full dictionary decompression for VARCHAR
3. Add selection buffer bit-unpacking
4. Implement dictionary compression for writing
