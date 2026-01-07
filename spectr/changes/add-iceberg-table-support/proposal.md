# Change: Add Iceberg Table Format Support (Pure Go)

## Why

Apache Iceberg is an open table format for huge analytic datasets that provides:
- Schema evolution (add, drop, rename, reorder columns)
- Partition evolution (change partition schemes without rewrites)
- Time travel (query historical versions of tables)
- ACID transactions with optimistic concurrency
- Schema-on-read (flexible column projection)

DuckDB v1.4.3 enables Iceberg by default, but this requires C++ Iceberg bindings. For dukdb-go to maintain pure Go while supporting Iceberg, we need a native Go implementation or integration path.

## What Changes

- **ADDED**: Iceberg table format reader for `iceberg_scan()` function
- **ADDED**: `iceberg_metadata()` function for detailed manifest/file metadata
- **ADDED**: `iceberg_snapshots()` function for snapshot history discovery
- **ADDED**: Iceberg metadata parsing (metadata.json, manifest lists, manifests)
- **ADDED**: Iceberg snapshot discovery and time travel queries (AS OF TIMESTAMP, AS OF SNAPSHOT)
- **ADDED**: Version selection parameters (version, allow_moved_paths, metadata_compression_codec)
- **ADDED**: Version guessing support for tables without version-hint.text
- **ADDED**: Partition pruning using Iceberg partition specs
- **ADDED**: Column projection using Iceberg column stats
- **ADDED**: Delete file handling (positional and equality deletes)
- **ADDED**: DuckDB catalog integration for Iceberg tables
- **ADDED**: REST catalog support with OAuth2 authentication

## Impact

- Affected specs: `specs/table-formats/spec.md`
- Affected code:
  - `internal/io/iceberg/` - Iceberg metadata and data reading
  - `internal/catalog/` - Catalog integration for Iceberg tables
  - `internal/parser/` - Iceberg SQL syntax
  - `internal/executor/` - Iceberg scan execution
- Breaking changes: None
- Dependencies: Pure Go Iceberg library (to be determined)

## Priority

**MEDIUM** - Iceberg is increasingly important for data lake interoperability, but there are pure Go alternatives. Lower priority than checkpoint_threshold (production critical) and ORC (fundamental format).
