# Change: Add ORC File Format Support

## Why

ORC (Optimized Row Columnar) is a widely-used columnar storage format in the Hadoop ecosystem, commonly used with Hive, Spark, and other big data tools. DuckDB v1.4.3 supports ORC for both reading and writing, but dukdb-go currently lacks this capability. This gap prevents users from:
1. Reading ORC files from data lakes and data pipelines
2. Writing data in ORC format for Hadoop ecosystem compatibility
3. Interoperating with Spark, Hive, and other ORC-producing systems

## What Changes

- **ADDED**: ORC file format reader for `read_orc()` and `read_orc_auto()` functions
- **ADDED**: ORC file format writer for `COPY TO` statements with ORC format
- **ADDED**: ORC file type detection and automatic format selection
- **ADDED**: Type mapping between ORC types and DuckDB types
- **ADDED**: Compression support (zlib, snappy, lz4, zstd as supported by ORC format)
- **ADDED**: Stripe-level and column-level reading for performance

## Impact

- Affected specs: `specs/file-formats/spec.md`
- Affected code:
  - `internal/io/` - New ORC reader/writer implementation
  - `internal/parser/` - ORC function parsing
  - `internal/planner/` - ORC scan planning
  - `internal/executor/` - ORC execution operators
- Breaking changes: None
- Dependencies: May need ORC C library wrapper or pure Go implementation

## Priority

**HIGH** - ORC is a fundamental file format for big data interoperability. Without it, dukdb-go cannot read data from many production data pipelines.
