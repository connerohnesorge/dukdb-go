# DuckDB Format Compatibility

This document describes the compatibility of this pure Go implementation with DuckDB's native file format.

## Supported Version

- **DuckDB Version**: v1.4.3
- **Storage Format Version**: 67
- **Serialization Compatibility**: 67

Files created by DuckDB versions newer than v1.4.3 may not be readable if they use a newer storage format version. The implementation validates the version and will return an error for unsupported versions.

## File Structure

DuckDB files use a block-based structure with the following layout:

```
Offset 0-4095:     File Header (4KB)
                   ├── Bytes 0-7: Block header storage (reserved)
                   ├── Bytes 8-11: Magic bytes "DUCK"
                   ├── Bytes 12-19: Storage format version (uint64)
                   └── Bytes 20-27: Feature flags (uint64)

Offset 4096-8191:  Database Header 1 (4KB)
                   ├── Bytes 0-7: Checksum (uint64)
                   └── Bytes 8+: Header data (see below)

Offset 8192-12287: Database Header 2 (4KB)
                   ├── Bytes 0-7: Checksum (uint64)
                   └── Bytes 8+: Header data (see below)

Offset 12288+:     Data Blocks (256KB each, configurable)
                   ├── Bytes 0-7: Block checksum (uint64)
                   └── Bytes 8+: Block data
```

### Database Header Fields

Each database header contains:

| Field | Size | Description |
|-------|------|-------------|
| Iteration | 8 bytes | Checkpoint counter (used for crash recovery) |
| MetaBlock | 12 bytes | Pointer to catalog metadata (block ID + offset) |
| FreeList | 12 bytes | Pointer to free block list |
| BlockCount | 8 bytes | Total allocated blocks |
| BlockAllocSize | 8 bytes | Block size (default: 262144 = 256KB) |
| VectorSize | 8 bytes | Rows per vector (default: 2048) |
| SerializationCompat | 8 bytes | Serialization version |

### Dual Header Design

DuckDB uses two database headers for crash recovery:
1. On checkpoint, write to the inactive header slot
2. Increment the iteration counter
3. The header with higher iteration is considered active
4. If crash occurs during checkpoint, previous header remains valid

## Compression Algorithms

### Supported Algorithms

| Algorithm | Status | Description |
|-----------|--------|-------------|
| UNCOMPRESSED | Full | Raw data without compression |
| CONSTANT | Full | Single value repeated for entire segment |
| RLE | Full | Run-length encoding with varint counts |
| DICTIONARY | Full | Dictionary compression with uint32 indices |
| BITPACKING | Full | Bit-packed integers using minimum bits |
| PFOR_DELTA | Full | Packed Frame of Reference with delta encoding |

### Not Yet Supported

| Algorithm | Status | Notes |
|-----------|--------|-------|
| FSST | Not supported | Fast Static Symbol Table for strings |
| ALP | Not supported | Adaptive Lossless floating Point |
| ALPRD | Not supported | ALP for Real Doubles |
| CHIMP | Not supported | Time series float compression |
| PATAS | Not supported | Time series compression |
| ZSTD | Not supported | Zstandard block compression |
| ROARING | Not supported | Roaring bitmap compression |
| DICT_FSST | Not supported | Dictionary + FSST combo |

When encountering unsupported compression, an `ErrUnsupportedCompression` error is returned.

### Compression Details

#### CONSTANT Compression
- Stores a single value that repeats for the entire segment
- Format: `[value bytes]`
- Decompression expands to `count` copies of the value

#### RLE Compression
- Run-length encoding with varint-encoded run counts
- Format: `[(varint count, value bytes)]...`
- Each run stores count as varint followed by value bytes

#### DICTIONARY Compression
- Builds dictionary of unique values, stores indices
- Format: `[uint32 dictSize][values...][uint64 indexCount][uint32 indices...]`
- Supports both fixed-size and variable-size (string) values

#### BITPACKING Compression
- Stores integers using minimum required bits
- Format: `[uint8 bitWidth][uint64 count][packed bits...]`
- Bits are packed in little-endian order

#### PFOR_DELTA Compression
- Frame of Reference with cumulative delta encoding
- Format: `[int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]`
- First value is reference, subsequent values are cumulative deltas

## Type Mapping

### Primitive Types

| DuckDB Type | Go Type | Size | Notes |
|-------------|---------|------|-------|
| BOOLEAN | bool | 1 byte | 0 = false, non-zero = true |
| TINYINT | int8 | 1 byte | Signed 8-bit integer |
| SMALLINT | int16 | 2 bytes | Little-endian |
| INTEGER | int32 | 4 bytes | Little-endian |
| BIGINT | int64 | 8 bytes | Little-endian |
| UTINYINT | uint8 | 1 byte | Unsigned 8-bit |
| USMALLINT | uint16 | 2 bytes | Little-endian |
| UINTEGER | uint32 | 4 bytes | Little-endian |
| UBIGINT | uint64 | 8 bytes | Little-endian |
| FLOAT | float32 | 4 bytes | IEEE 754 |
| DOUBLE | float64 | 8 bytes | IEEE 754 |

### Large Integer Types

| DuckDB Type | Go Type | Size | Notes |
|-------------|---------|------|-------|
| HUGEINT | HugeInt | 16 bytes | Two's complement, lower then upper |
| UHUGEINT | UHugeInt | 16 bytes | Lower then upper |

### String Types

| DuckDB Type | Go Type | Format |
|-------------|---------|--------|
| VARCHAR | string | `[uint32 length][bytes...]` |
| CHAR(n) | string | Fixed n bytes, space-padded |
| BLOB | []byte | `[uint32 length][bytes...]` |
| BIT | BitString | `[uint64 bitLength][bytes...]` |

### Temporal Types

| DuckDB Type | Go Type | Storage |
|-------------|---------|---------|
| DATE | time.Time | int32 days since 1970-01-01 |
| TIME | time.Duration | int64 microseconds since midnight |
| TIME_NS | TimeNS | int64 nanoseconds since midnight |
| TIME_TZ | TimeTZ | int64 micros + int32 offset |
| TIMESTAMP | time.Time | int64 microseconds since epoch |
| TIMESTAMP_S | time.Time | int64 seconds since epoch |
| TIMESTAMP_MS | time.Time | int64 milliseconds since epoch |
| TIMESTAMP_NS | time.Time | int64 nanoseconds since epoch |
| TIMESTAMP_TZ | time.Time | Same as TIMESTAMP |
| INTERVAL | Interval | int32 months + int32 days + int64 micros |

### DECIMAL Type

| Precision | Storage Type | Size |
|-----------|--------------|------|
| 1-4 | int8 | 1 byte |
| 5-9 | int16 | 2 bytes |
| 10-18 | int32 | 4 bytes |
| 19-38 | int64 | 8 bytes |
| 39+ | int128 (HugeInt) | 16 bytes |

### ENUM Type

| Value Count | Storage Type | Size |
|-------------|--------------|------|
| 1-256 | uint8 | 1 byte |
| 257-65536 | uint16 | 2 bytes |
| 65537+ | uint32 | 4 bytes |

### Complex Types

| DuckDB Type | Go Type | Description |
|-------------|---------|-------------|
| LIST | ListValue | Variable-length array |
| STRUCT | StructValue | Named fields |
| MAP | MapValue | Key-value pairs |
| UNION | UnionValue | Tagged union |
| ARRAY(n) | ArrayValue | Fixed-size array |

## Known Limitations

### Not Supported

1. **Advanced Compression**: FSST, ALP, CHIMP, PATAS, ZSTD compression
2. **Encryption**: Encrypted database files are not supported
3. **Concurrent Writes**: Only single-writer is supported
4. **Storage Extensions**: External storage plugins not supported
5. **Delta/Iceberg**: Format variants not supported

### Partial Support

1. **DELETE/UPDATE**: Currently marks rows as modified but full MVCC not implemented
2. **Transactions**: Basic transaction support without full snapshot isolation
3. **Complex Type Nesting**: Deep nesting of complex types may have edge cases

## Testing Against DuckDB CLI

To verify interoperability with DuckDB CLI:

### Creating a test database with DuckDB CLI

```bash
# Install DuckDB CLI (v1.4.3)
# Create test database
duckdb test.duckdb <<EOF
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);
INSERT INTO users VALUES
    (1, 'Alice', 'alice@example.com', '2024-01-15 10:30:00'),
    (2, 'Bob', 'bob@example.com', '2024-01-16 11:45:00'),
    (3, 'Charlie', 'charlie@example.com', '2024-01-17 09:15:00');
CHECKPOINT;
EOF
```

### Reading with dukdb-go

```go
package main

import (
    "fmt"
    "log"

    "github.com/dukdb/dukdb-go/internal/storage/duckdb"
)

func main() {
    storage, err := duckdb.NewDuckDBStorage("test.duckdb", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close()

    iter, err := storage.ScanTable("main", "users", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer iter.Close()

    for iter.Next() {
        row := iter.Row()
        fmt.Printf("User: id=%v, name=%v, email=%v\n", row[0], row[1], row[2])
    }
}
```

### Creating a database with dukdb-go

```go
package main

import (
    "log"

    "github.com/dukdb/dukdb-go/internal/storage/duckdb"
)

func main() {
    storage, err := duckdb.CreateDuckDBStorage("output.duckdb", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close()

    // ... create tables and insert data ...

    if err := storage.Checkpoint(); err != nil {
        log.Fatal(err)
    }
}
```

### Verifying with DuckDB CLI

```bash
duckdb output.duckdb <<EOF
SELECT * FROM users;
EOF
```

## Checksum Algorithm

DuckDB uses a custom checksum algorithm that combines:
1. Multiplication hash for 8-byte aligned chunks
2. MurmurHash3 variant for remaining bytes

The checksum is stored at the beginning of each block (first 8 bytes).

```
Checksum = XOR(chunk[i] * 0xbf58476d1ce4e5b9) ^ MurmurHash(tail)
```

## Row Group Structure

- Default row group size: 122,880 rows
- Default vector size: 2,048 rows
- Each column stored as separate segments within the row group
- Segments may use different compression algorithms

### DataPointer Structure

Each column segment is referenced by a DataPointer:

| Field | Size | Description |
|-------|------|-------------|
| RowStart | 8 bytes | Starting row index |
| TupleCount | 8 bytes | Number of tuples |
| Block | 12 bytes | Block pointer (ID + offset) |
| Compression | 1 byte | Compression type |
| Statistics | variable | Min/max/null count |
| SegmentState | variable | Validity mask location |

## Version History

| DuckDB Version | Storage Version | Notes |
|----------------|-----------------|-------|
| v1.4.3 | 67 | Current target |
| v1.3.x | 64-66 | May be compatible |
| v1.2.x | 60-63 | May have issues |
| v1.1.x | 50-59 | Not tested |
| v1.0.x | 40-49 | Not supported |
| < v1.0 | < 40 | Not supported |

## References

- DuckDB Source: https://github.com/duckdb/duckdb
- DuckDB Documentation: https://duckdb.org/docs/
- Storage Format: `src/storage/` in DuckDB source
- Type Definitions: `src/include/duckdb/common/types.hpp`
