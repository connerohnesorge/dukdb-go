## Context

dukdb-go requires file format support to achieve DuckDB v1.4.3 feature parity. The implementation must:
- Use pure Go (no CGO) per project constraints
- Integrate with existing DataChunk-based execution pipeline
- Support streaming to handle large files without memory exhaustion
- Enable projection/predicate pushdown where applicable

### Stakeholders
- Users importing data from external sources (CSV, Parquet, JSON)
- Users exporting query results for downstream processing
- Developers building ETL pipelines with dukdb-go

### Constraints
- Zero CGO requirement eliminates C-based Parquet libraries
- Must work in TinyGo/WASM environments
- Memory usage must be bounded (DataChunk batching)

## Goals / Non-Goals

### Goals
- Implement read_csv, read_csv_auto table functions
- Implement read_json, read_json_auto table functions
- Implement read_parquet table function
- Implement COPY FROM/TO statement
- Support common compression formats (gzip, zstd, snappy)
- Enable projection pushdown for Parquet
- Handle schema inference for CSV and JSON

### Non-Goals
- Remote file access (S3, HTTP) - separate proposal (add-s3-cloud-integration)
- Arrow IPC file format - already has in-memory conversion
- Excel (.xlsx) support - lower priority
- Delta Lake / Iceberg table formats - future work
- Glob patterns (read_csv('*.csv')) - separate proposal for file system abstraction
- Hive partitioning (PARTITION_BY, HIVE_PARTITIONING) - future enhancement
- UNION_BY_NAME for multi-file schema evolution - future enhancement

## Decisions

### Decision 1: Pure-Go Parquet Library Selection

**Decision**: Use `github.com/parquet-go/parquet-go`

**Alternatives Considered**:
1. `github.com/apache/arrow-go/parquet` - Comprehensive but brings full Arrow dependency
2. `github.com/xitongsys/parquet-go` - Older, less maintained
3. `github.com/parquet-go/parquet-go` - Active development, pure Go, good performance

**Rationale**: parquet-go has active maintenance, good performance benchmarks, supports nested types, and has no CGO requirement. It integrates well with standard Go patterns.

### Decision 2: CSV Parser Approach

**Decision**: Extend standard library `encoding/csv` with wrapper

**Alternatives Considered**:
1. Pure custom parser - Maximum control, high development cost
2. `encoding/csv` wrapper - Leverage stdlib, add features
3. Third-party library (gocsv) - Additional dependency

**Rationale**: Standard library handles core parsing well. We add a wrapper for:
- Auto-detection of delimiter, quote char, header
- NULL value handling
- Type inference
- Streaming with DataChunk integration

### Decision 3: File Format Detection

**Decision**: Use file extension and magic bytes for format detection

**Format Detection Priority**:
1. Explicit FORMAT option in COPY statement
2. File extension (.csv, .parquet, .json, .ndjson)
3. Magic bytes (PAR1 for Parquet, { or [ for JSON)
4. Fall back to CSV

### Decision 4: Memory Management

**Decision**: Streaming with DataChunk batches (2048 rows)

**Approach**:
- CSV: Read and parse line-by-line, batch into DataChunks
- JSON: Stream-parse objects, batch into DataChunks
- Parquet: Read row groups, yield DataChunks

**Memory Bounds**:
- Maximum 10 DataChunks in flight per reader
- Configurable via `max_memory` option
- Back-pressure when executor consumption slows

### Decision 5: Type Inference Strategy

**Decision**: Sample-based inference with configurable sample size

**CSV Type Inference**:
1. Sample first N rows (default: 1000)
2. Attempt parsing as: INTEGER, DOUBLE, DATE, TIMESTAMP, BOOLEAN
3. Fall back to VARCHAR if parsing fails
4. Allow explicit type specification via options

**JSON Type Inference**:
1. Parse first N objects
2. Infer types from JSON value types (number, string, boolean, array, object)
3. Handle mixed types by promoting to VARCHAR
4. Nested objects become STRUCT types

## Risks / Trade-offs

### Risk: Parquet Library Compatibility
- **Risk**: parquet-go may not support all Parquet features
- **Mitigation**: Test against DuckDB-generated Parquet files; document limitations

### Risk: Performance vs. Reference Implementation
- **Risk**: Pure Go may be slower than CGO DuckDB
- **Mitigation**:
  - Target 50% of CGO performance as acceptable
  - Use goroutines for parallel row group reading
  - Profile and optimize hot paths

### Risk: Schema Evolution in Parquet
- **Risk**: Complex schema evolution cases may fail
- **Mitigation**: Start with simple column addition/removal; document unsupported cases

### Trade-off: Memory vs. Speed
- **Choice**: Streaming over memory-resident
- **Consequence**: Lower memory usage, potentially slower for small files
- **Acceptable**: Matches DuckDB's behavior for large file handling

## Migration Plan

No migration needed - additive feature only.

### Rollout Phases
1. CSV support (read_csv, COPY FROM csv)
2. JSON support (read_json, COPY FROM json)
3. Parquet support (read_parquet, COPY FROM/TO parquet)
4. Table function registration in execution engine

### Rollback
- Remove table function registrations
- No data format changes required

## Open Questions

1. **Glob pattern support**: Should `read_csv('data/*.csv')` be supported in initial release?
   - **Recommendation**: Defer to separate proposal (requires file system abstraction)

2. **Parallel CSV reading**: Should CSV files be read in parallel?
   - **Recommendation**: Single-threaded initially; parallel reading is complex for CSV due to line boundaries

3. **Compression auto-detection**: Should `.csv.gz` automatically decompress?
   - **Recommendation**: Yes, detect from extension and magic bytes
