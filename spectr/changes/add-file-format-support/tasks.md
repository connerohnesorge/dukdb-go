## 1. Core Infrastructure

- [ ] 1.1 Create `internal/io/` package structure with reader/writer interfaces
- [ ] 1.2 Define `FileReader` interface with `ReadChunk() (*storage.DataChunk, error)`
- [ ] 1.3 Define `FileWriter` interface with `WriteChunk(*storage.DataChunk) error`
- [ ] 1.4 Implement compression wrapper (gzip, zstd, snappy detection and decompression)
- [ ] 1.5 Add file format detection utility (extension + magic bytes)

## 2. CSV Support

- [ ] 2.1 Implement `internal/io/csv/reader.go` with auto-detection
  - Delimiter detection (comma, tab, semicolon, pipe)
  - Quote character detection
  - Header detection
  - NULL value handling
- [ ] 2.2 Implement type inference for CSV columns
  - Sample-based inference (configurable sample size)
  - Support INTEGER, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARCHAR
- [ ] 2.3 Implement `internal/io/csv/writer.go`
  - Configurable delimiter, quote, escape
  - Header writing option
  - NULL representation
- [ ] 2.4 Add unit tests for CSV reader edge cases
  - Quoted fields with embedded delimiters
  - Multi-line values
  - Empty fields, NULL handling
- [ ] 2.5 Add integration tests comparing with DuckDB CLI output

## 3. JSON Support

- [ ] 3.1 Implement `internal/io/json/reader.go` for JSON arrays
  - Parse `[{...}, {...}]` format
  - Stream-parse for memory efficiency
- [ ] 3.2 Implement NDJSON (newline-delimited JSON) support
  - Parse `{...}\n{...}\n` format
  - Handle mixed array/NDJSON detection
- [ ] 3.3 Implement JSON type inference
  - Map JSON types to DuckDB types
  - Handle nested objects as STRUCT
  - Handle arrays as LIST
- [ ] 3.4 Implement `internal/io/json/writer.go`
  - JSON array output
  - NDJSON output option
  - Pretty-print option
- [ ] 3.5 Add unit tests for JSON reader
- [ ] 3.6 Add integration tests comparing with DuckDB CLI

## 4. Parquet Support

- [ ] 4.1 Add `github.com/parquet-go/parquet-go` dependency
- [ ] 4.2 Implement `internal/io/parquet/reader.go`
  - Row group iteration
  - Column projection pushdown
  - Type mapping (Parquet types to DuckDB types)
- [ ] 4.3 Handle nested types in Parquet
  - LIST columns
  - MAP columns
  - STRUCT columns
- [ ] 4.4 Implement `internal/io/parquet/writer.go`
  - Row group sizing
  - Compression options (snappy, gzip, zstd)
  - Type mapping (DuckDB types to Parquet)
- [ ] 4.5 Add unit tests for Parquet reading
- [ ] 4.6 Add integration tests with DuckDB-generated Parquet files
- [ ] 4.7 Performance benchmarks vs. reference implementation

## 5. Table Functions

- [ ] 5.1 Implement `read_csv(path, ...)` table function
  - Register as built-in table function
  - Support options: delimiter, header, nullstr, columns, types
- [ ] 5.2 Implement `read_csv_auto(path)` table function
  - Automatic format detection
  - Type inference
- [ ] 5.3 Implement `read_json(path, ...)` table function
  - Support options: format (array/ndjson), columns, maximum_depth
- [ ] 5.4 Implement `read_json_auto(path)` table function
- [ ] 5.5 Implement `read_parquet(path)` table function
  - Support column projection via SELECT
- [ ] 5.6 Add table function tests

## 6. COPY Statement

- [ ] 6.1 Add CopyStmt to parser AST (`internal/parser/ast.go`)
  - COPY table FROM 'path' (OPTIONS)
  - COPY table TO 'path' (OPTIONS)
  - COPY (query) TO 'path' (OPTIONS)
- [ ] 6.2 Implement COPY parsing (`internal/parser/parser.go`)
  - Parse OPTIONS clause (DELIMITER, HEADER, FORMAT, COMPRESSION, etc.)
- [ ] 6.3 Implement COPY binding (`internal/binder/bind_stmt.go`)
  - Resolve table reference
  - Validate options
  - Bind subquery for COPY (SELECT...)
- [ ] 6.4 Add LogicalCopy operator (`internal/planner/logical.go`)
- [ ] 6.5 Add PhysicalCopy operator (`internal/planner/physical.go`)
- [ ] 6.6 Implement PhysicalCopy execution (`internal/executor/physical_copy.go`)
  - COPY FROM: read file, insert into table
  - COPY TO: scan table/query, write to file
- [ ] 6.7 Add COPY statement tests
  - COPY FROM CSV
  - COPY TO Parquet
  - COPY (SELECT) TO JSON
  - Error handling for missing files, format errors

## 7. Documentation and Finalization

- [ ] 7.1 Update spectr specs with new capabilities
- [ ] 7.2 Run full test suite including new tests
- [ ] 7.3 Run linting and fix any issues
- [ ] 7.4 Performance benchmarks for all formats
- [ ] 7.5 Update CLAUDE.md with new capabilities
