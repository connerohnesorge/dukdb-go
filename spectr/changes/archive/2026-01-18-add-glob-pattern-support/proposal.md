# Change: Add Glob Pattern Support for File I/O Operations

## Why

Glob pattern matching is a **CRITICAL** missing feature that prevents bulk data loading scenarios. DuckDB v1.4.3 has full glob support allowing queries like `SELECT * FROM read_csv('data/*.csv')` and `read_parquet('s3://bucket/**/*.parquet')`. Without this, users must manually iterate over files in application code, which is:
- Inefficient (multiple roundtrips vs single query)
- Error-prone (manual schema alignment)
- Non-standard (breaks DuckDB compatibility)
- Blocks common data lake patterns

This is the **#1 highest priority** missing feature based on user impact analysis.

## What Changes

- **ADDED**: Glob pattern matching in filesystem layer supporting:
  - Single wildcards (`*.csv`)
  - Recursive wildcards (`**/*.parquet` - limited to one per pattern)
  - Character classes (`[a-z]`, `[0-9]`)
  - Bracket negation (`[!abc]` to exclude characters from set)
- **ADDED**: Array of files syntax (`['file1.csv', 'file2.csv']`)
- **ADDED**: Virtual metadata columns (`filename`, `file_row_number`, `file_index`)
- **ADDED**: File glob behavior options (DISALLOW_EMPTY, ALLOW_EMPTY, FALLBACK_GLOB)
- **ADDED**: Union-by-name support for files with different schemas
- **ADDED**: Hive partitioning with auto-detection and type inference
- **ADDED**: CSV `files_to_sniff` option for schema auto-detection with globs
- **ADDED**: Cloud storage glob support (S3 ListObjectsV2, GCS list, Azure list)
- **ADDED**: File ordering (alphabetical by default)
- **MODIFIED**: All table functions (read_csv, read_json, read_parquet, read_xlsx, read_arrow) to accept glob patterns

## Impact

- Affected specs: `specs/file-io/spec.md`
- Affected code:
  - `internal/io/filesystem/` - Glob pattern matching engine
  - `internal/io/csv/`, `internal/io/json/`, `internal/io/parquet/`, `internal/io/xlsx/`, `internal/io/arrow/` - Integration with file readers
  - `internal/executor/table_function_*.go` - Table function updates
  - `internal/parser/` - Array syntax for file lists
- Breaking changes: **None** (additive only)
- Dependencies:
  - Existing filesystem abstraction
  - Cloud storage clients (S3, GCS, Azure)

## Priority

**CRITICAL** - Unblocks bulk data loading (most common use case) and provides parity with DuckDB CLI behavior.
