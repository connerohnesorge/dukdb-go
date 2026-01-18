# DuckDB Format Compatibility

This document describes the compatibility of this pure Go implementation with DuckDB's native file format.

## DuckDB CLI Compatibility

### Version Requirements

**Minimum Required DuckDB CLI Version: v1.1.0+**

dukdb-go can read and write files that are compatible with DuckDB CLI v1.1.0 and newer versions. The implementation has been tested against DuckDB CLI v1.4.3.

| Feature | Read Compatibility | Write Compatibility |
|---------|-------------------|---------------------|
| DuckDB CLI v1.1.0+ | ✓ Supported | ✓ Supported |
| DuckDB CLI v0.9.0 - v1.0.x | ⚠ May work | ⚠ May work |
| DuckDB CLI < v0.9.0 | ✗ Not supported | ✗ Not supported |

**Tested Against**: DuckDB CLI v1.4.3 (Andium) d1dc88f950

### Interoperability Status

**Files Created by dukdb-go → DuckDB CLI**:
- ✓ SHOW TABLES works
- ✓ SELECT queries work
- ✓ DESCRIBE works
- ✓ Schema information readable
- ✓ Row data readable
- ✓ Multiple data types supported

**Files Created by DuckDB CLI → dukdb-go**:
- ✓ Read support for all common data types
- ✓ Catalog metadata readable
- ✓ Table schemas readable
- ✓ Row data readable
- ⚠ Some advanced compression formats may not be supported (see limitations below)

### Round-Trip Compatibility

Round-trip workflows are supported:
1. Create database with dukdb-go
2. Modify with DuckDB CLI
3. Read back with dukdb-go

This enables hybrid workflows where DuckDB CLI tools can be used alongside dukdb-go applications.

## Supported Version

- **DuckDB Version**: v1.4.3
- **Storage Format Version**: 64-67 (reads), 64 (writes)
- **Serialization Compatibility**: 64-67 (reads), 64 (writes)

**Version Strategy**:
- dukdb-go **writes** format version 64 for maximum compatibility with DuckDB v0.9.0+
- dukdb-go **reads** format versions 64-67 (DuckDB v0.9.0 through v1.5.0+)

Files created by DuckDB versions newer than v1.5.0 may not be readable if they use a storage format version newer than 67. The implementation validates the version and will return an error for unsupported versions.

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
   - Files using these compression algorithms will return `ErrUnsupportedCompression`
   - DuckDB CLI may choose these automatically for optimal storage
   - **Workaround**: Use DuckDB CLI with `SET preserve_insertion_order = true` to avoid some advanced compression

2. **Encryption**: Encrypted database files are not supported
   - Files with encryption will fail to open
   - No API for encryption keys

3. **Concurrent Writes**: Only single-writer is supported
   - Multiple writers will cause data corruption
   - Use application-level locking if multiple processes need write access

4. **Storage Extensions**: External storage plugins not supported
   - Pure Go implementation cannot load C/C++ shared libraries
   - Extensions like `spatial`, `httpfs`, `icu` cannot be loaded

5. **Delta/Iceberg**: Format variants not supported
   - Standard DuckDB format only
   - Iceberg tables via `iceberg_scan()` are supported separately

### Partial Support

1. **DELETE/UPDATE**: Currently marks rows as modified but full MVCC not implemented
   - Basic DELETE/UPDATE operations work
   - Concurrent transaction isolation is limited

2. **Transactions**: Basic transaction support without full snapshot isolation
   - ACID guarantees provided for single transaction
   - Concurrent transaction isolation may have limitations

3. **Complex Type Nesting**: Deep nesting of complex types may have edge cases
   - Simple LIST, STRUCT, MAP types work well
   - Deep nesting (e.g., LIST of STRUCT of MAP) may have issues

### Edge Cases and Known Issues

1. **Large String Values**: VARCHAR values larger than 4GB are not supported
   - Limited by uint32 length encoding in format

2. **Very Large Tables**: Tables with billions of rows may have performance issues
   - Row group management optimized for typical workloads

3. **Schema Changes**: ALTER TABLE support is limited
   - Adding/dropping columns works
   - Type changes may require table recreation

4. **Checksum Validation**: Checksum errors may occur with format mismatches
   - If you encounter checksum errors, verify DuckDB CLI version compatibility

5. **Metadata Block Chains**: Very long metadata chains (thousands of entries) are untested
   - Typical catalogs with hundreds of tables work fine

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

| DuckDB Version | Storage Version | Read Support | Write Support | Notes |
|----------------|-----------------|--------------|---------------|-------|
| v1.5.0 | 67 | ✓ Yes | ✓ Yes (v64) | Current |
| v1.4.3 | 67 | ✓ Yes | ✓ Yes (v64) | Tested target |
| v1.3.x | 64-66 | ✓ Yes | ✓ Yes (v64) | Compatible |
| v1.2.x | 64-65 | ✓ Yes | ✓ Yes (v64) | Compatible |
| v1.1.x | 64 | ✓ Yes | ✓ Yes (v64) | Minimum recommended |
| v0.9.0 - v1.0.x | 64 | ⚠ May work | ⚠ May work | Not fully tested |
| < v0.9.0 | < 64 | ✗ No | ✗ No | Not supported |

### Version-Specific Features

| Feature | Minimum DuckDB Version | dukdb-go Support |
|---------|------------------------|------------------|
| Basic file format (v64) | v0.9.0 | ✓ Full |
| Serialization compatibility field | v1.2.0 (v65) | ✓ Read only |
| Enhanced nested types | v1.3.0 (v66) | ✓ Read only |
| TIME_TZ, TIMESTAMP_TZ | v1.4.0 (v67) | ✓ Read only |
| BIT type | v1.4.0 (v67) | ✓ Read only |

## Data Type Compatibility Matrix

### Fully Supported for Read/Write

These types work in both directions (dukdb-go ↔ DuckDB CLI):

- INTEGER, BIGINT, SMALLINT, TINYINT (signed and unsigned variants)
- FLOAT, DOUBLE
- VARCHAR, BLOB
- BOOLEAN
- DATE, TIME, TIMESTAMP (all variants)
- DECIMAL(p, s) up to precision 38
- UUID
- LIST, STRUCT, MAP (with supported element types)
- ENUM
- INTERVAL

### Read-Only Support

These types can be read from DuckDB CLI files but may have limitations when writing:

- HUGEINT, UHUGEINT (128-bit integers) - read support full, write untested
- BIT - depends on version
- TIME_TZ, TIMESTAMP_TZ - requires DuckDB v1.4.0+
- Deeply nested complex types (LIST of STRUCT of MAP, etc.)

### Type-Specific Limitations

1. **DECIMAL**: Precision limited to 38 digits
2. **VARCHAR**: Length limited to ~4GB (uint32 max)
3. **ENUM**: Dictionary size limited to uint32 max entries
4. **LIST**: Nesting depth not enforced but very deep nesting untested
5. **UNION**: Tagged union support is basic

## Recommendations for Production Use

### For Maximum Compatibility

1. **Use DuckDB CLI v1.1.0 or newer** for best interoperability
2. **Test round-trip workflows** (create with dukdb-go, read with DuckDB CLI, modify, read back)
3. **Avoid advanced compression** if you need to share files between implementations
4. **Use common data types** (INTEGER, VARCHAR, TIMESTAMP, etc.) for highest compatibility
5. **Checkpoint frequently** to ensure data is written to disk in compatible format

### For Performance

1. **Use appropriate checkpoint thresholds** (see PRAGMA checkpoint_threshold)
2. **Batch inserts** when possible to reduce overhead
3. **Use appropriate data types** (don't use DECIMAL if INTEGER suffices)
4. **Consider compression trade-offs** (supported compression is sufficient for most cases)

### For Debugging Compatibility Issues

If you encounter issues reading files between dukdb-go and DuckDB CLI:

1. **Check DuckDB CLI version**: `duckdb --version`
2. **Verify file format version**: Query the file header
3. **Look for compression errors**: Check if advanced compression is in use
4. **Test with empty table**: Rule out data-specific issues
5. **Enable verbose logging**: Check for checksum or metadata errors
6. **Compare hex dumps**: Use diagnostic tools to identify byte-level differences

## References

- DuckDB Source: https://github.com/duckdb/duckdb
- DuckDB Documentation: https://duckdb.org/docs/
- Storage Format: `src/storage/` in DuckDB source
- Type Definitions: `src/include/duckdb/common/types.hpp`
- Format Version History: `src/storage/storage_info.cpp`

## Related Documentation

- See `docs/pragmas.md` for PRAGMA settings including checkpoint_threshold
- See `spectr/changes/fix-duckdb-write-compat/diagnostics.md` for detailed compatibility analysis
- See `spectr/changes/gap-analysis.md` for overall feature compatibility status
