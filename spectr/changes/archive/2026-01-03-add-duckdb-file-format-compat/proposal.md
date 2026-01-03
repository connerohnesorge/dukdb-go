# Change: Add DuckDB File Format Compatibility

## Why

The system SHALL use the official DuckDB file format for persistent storage, enabling compatibility with the DuckDB ecosystem. This ensures:

1. **Interoperability**: Users can read or write `.duckdb` files created by the official DuckDB
2. **Ecosystem Integration**: Can use DuckDB CLI tools, connectors, or UDFs with dukdb-go databases
3. **Data Portability**: Data created in dukdb-go is portable to the DuckDB ecosystem
4. **Cloud/S3 Integration**: Official DuckDB cloud features can be supported using the standard file format

## What Changes

### Core Features

- **Magic Number**: Uses `DUCK` (0x4455434B)
- **Format Version**: Supports version 64+
- **Catalog Serialization**: Uses DuckDB's binary property-based serialization
- **WAL Format**: Uses DuckDB's WAL format
- Full read/write support for official DuckDB `.duckdb` files
- Dual 4KB rotating header blocks for crash safety
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

- `persistence`: Implementation of WAL and storage formats
- `storage`: New row group format with column segment storage
- `catalog-persistence`: Binary serialization for catalog

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