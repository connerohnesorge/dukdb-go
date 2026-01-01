# Change: Add File Format Support (CSV, JSON, Parquet)

## Why

dukdb-go currently has ZERO file format support - it cannot read or write external data files. This is a critical gap for DuckDB v1.4.3 compatibility, as file-based data loading is fundamental to DuckDB's value proposition. Users cannot:
- Import data from CSV files (`read_csv()`, `COPY FROM`)
- Read Parquet files (`read_parquet()`)
- Load JSON/NDJSON data (`read_json()`)
- Export query results to files (`COPY TO`)

Without file format support, dukdb-go is limited to programmatic data insertion only, eliminating the primary use case for analytical workloads.

## What Changes

### New Capabilities

1. **file-io** - Core file I/O infrastructure:
   - CSV reader with auto-detection (delimiter, quote, escape, header)
   - CSV writer with configurable formatting
   - JSON/NDJSON reader with schema inference
   - JSON writer
   - Parquet reader with pure-Go library (no CGO)
   - Parquet writer with compression support

2. **copy-statement** - COPY statement for bulk data operations:
   - `COPY table FROM 'file.csv' (OPTIONS...)`
   - `COPY table TO 'file.parquet' (FORMAT PARQUET)`
   - `COPY (SELECT ...) TO 'output.json'`

### Modified Capabilities

3. **execution-engine** - Add built-in table functions:
   - `read_csv(path, options...)`
   - `read_csv_auto(path)`
   - `read_json(path, options...)`
   - `read_json_auto(path)`
   - `read_parquet(path)`

4. **parser** - Add COPY statement parsing

### Dependencies

Pure-Go libraries to add:
- `github.com/parquet-go/parquet-go` - Parquet reading/writing
- `github.com/klauspost/compress/zstd` - ZSTD compression
- `github.com/klauspost/compress/snappy` - Snappy compression
- `github.com/pierrec/lz4/v4` - LZ4 compression
- `github.com/andybalholm/brotli` - Brotli compression

Standard library usage:
- `encoding/csv` - CSV parsing (extended)
- `encoding/json` - JSON parsing
- `compress/gzip` - GZIP compression

## Impact

- **Affected specs**: `file-io` (NEW), `copy-statement` (NEW), `execution-engine`, `parser`
- **Affected code**:
  - `internal/io/csv/` - CSV reader/writer
  - `internal/io/json/` - JSON reader/writer
  - `internal/io/parquet/` - Parquet reader/writer
  - `internal/executor/table_function_*.go` - Built-in table functions
  - `internal/parser/ast.go` - COPY statement AST
  - `internal/parser/parser.go` - COPY parsing
  - `internal/executor/physical_copy.go` - COPY execution
- **Breaking changes**: None (additive only)
- **Performance targets**:
  - CSV: 1M rows/second read throughput
  - Parquet: 5M rows/second for columnar reads
  - JSON: 500K objects/second
