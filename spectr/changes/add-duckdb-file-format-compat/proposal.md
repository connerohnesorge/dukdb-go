# Change: Add DuckDB File Format Compatibility

## Why

Currently, dukdb-go uses a proprietary file format (`DUKDBGO\x00` magic number, version 1) instead of the official DuckDB format (`DUCK` magic number, version 64+). This prevents:

1. **Interoperability**: Users cannot read or write `.duckdb` files created by the official DuckDB
2. **Ecosystem Integration**: Cannot use DuckDB CLI tools, connectors, or UDFs with dukdb-go databases
3. **Data Portability**: Data created in dukdb-go is locked to the dukdb-go ecosystem
4. **Cloud/S3 Integration**: Official DuckDB cloud features require the standard file format

The current proprietary format has several limitations:
- Magic Number: `DUKDBGO\x00` (vs DuckDB's `DUCK`)
- Format Version: 1 (vs DuckDB's v64+)
- Catalog Storage: GZIP-compressed JSON (vs DuckDB's binary property-based serialization)
- Header: 64-byte single header (vs DuckDB's dual 4KB rotating headers)
- Compression: Basic GZIP (vs DuckDB's ALP, Patas, Chimp, FSST, Zstd)
- WAL: Custom `DWAL` format with CRC64 (vs DuckDB's WAL format)

## What Changes

### Breaking Changes

- **File Format**: All existing dukdb-go database files (`.duckdb`, `.dukdb`) created with the old `DUKDBGO\x00` format will become **incompatible**. A migration utility will be provided.
- **Magic Number**: Changed from `DUKDBGO\x00` to `DUCK` (0x4455434B)
- **Format Version**: Upgraded from version 1 to version 64+
- **Catalog Serialization**: Replaced GZIP-compressed JSON with DuckDB's binary property-based serialization
- **WAL Format**: Custom `DWAL` replaced with DuckDB's WAL format

### New Features

- Full read/write support for official DuckDB `.duckdb` files
- Dual 4KB rotating header blocks for crash safety
- Complete property-based serialization for catalog storage
- Support for advanced compression algorithms (FSST, RLE, BitPacking, Chimp, Zstd)
- ART (Adaptive Radix Tree) index persistence
- Support for newer types: UNION, BIT, TIME_TZ, TIMESTAMP_TZ
- Row group format with variable sizes and per-segment compression

### Internal Changes

- New `internal/persistence/duckdb_format.go` for DuckDB-specific serialization
- New `internal/compression/` package with FSST, RLE, BitPacking implementations
- Updated `internal/storage/` for DuckDB row group format
- Updated `internal/wal/` for DuckDB WAL format

## Impact

### Affected Specs

- `persistence`: Complete replacement of WAL and storage formats
- `storage`: New row group format with column segment storage
- `catalog-persistence`: Binary serialization replacing JSON

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/persistence/file.go` | MODIFIED | Header reading/writing for DuckDB format |
| `internal/persistence/checkpoint.go` | ADDED | Checkpoint to DuckDB file format |
| `internal/persistence/duckdb_format.go` | ADDED | DuckDB-specific serialization |
| `internal/wal/writer.go` | MODIFIED | WAL entry format to match DuckDB |
| `internal/wal/reader.go` | MODIFIED | WAL entry parsing for DuckDB format |
| `internal/storage/chunk.go` | MODIFIED | Row group format adaptation |
| `internal/compression/*.go` | ADDED | Compression algorithm implementations |

### Dependencies

- This proposal depends on: (none - foundational change)
- This proposal blocks: S3/Cloud integration (requires compatible file format)

### Compatibility Mode

A compatibility mode will be provided during a transition period:

```go
// Old format (deprecated, will be removed in v1.0)
db, _ := sql.Open("duckdb-go", "?format=legacy")

// New format (default)
db, _ := sql.Open("duckdb-go", "my.db")  // Reads/writes DuckDB format
```

Migration utility:
```bash
# Migrate old dukdb-go database to new format
dukdb-go migrate --from old.duckdb --to new.duckdb
```
