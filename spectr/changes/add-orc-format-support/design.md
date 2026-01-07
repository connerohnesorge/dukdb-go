## Context

ORC (Optimized Row Columnar) is a self-describing, type-aware columnar file format designed for Hadoop workloads. Key characteristics:
- Columnar storage with type information embedded in file footer
- Built-in light-weight compression (zlib, snappy, lz4, zstd)
- Predicate push-down support via file footer statistics
- ACID support (in Hive transactional tables)
- Stripe-based layout for parallel reading

## Goals / Non-Goals

### Goals
- Read ORC files via `read_orc()` and `read_orc_auto()` functions
- Write ORC files via `COPY table TO 'file.orc' (FORMAT ORC)`
- Support all standard DuckDB types mapping to ORC types
- Support ORC compression codecs
- Enable predicate push-down using ORC column statistics
- Follow existing file format patterns (Parquet, CSV, JSON)

### Non-Goals
- ORC write with ACID features (Hive transactional tables)
- ORC file schema evolution
- Custom ORC bloom filters
- ORC file modification/deletion

## Technical Approach

### Option 1: Pure Go ORC Implementation

**Pros**:
- Maintains zero CGO requirement
- Full control over implementation
- Consistent with project's pure Go philosophy

**Cons**:
- Significant implementation effort (ORC spec is complex)
- Risk of correctness issues
- Performance may not match C-based implementations

**Libraries to consider**:
- `github.com/xitongsys/parquet-go` (has ORC support)
- `github.com/apache/orc-go` (if available)

**Effort**: 5-7 months for full implementation (read-only: 3-4 months)

### Option 2: CGO with liborc

**Pros**:
- Battle-tested C implementation from Apache
- Performance matching native ORC tools
- Complete spec compliance

**Cons**:
- Breaks pure Go requirement
- Cross-compilation complexity
- Platform-specific builds

**Effort**: 2-3 weeks for integration

### Decision: Option 1 (Pure Go)

**Rationale**:
- Project's core value prop is pure Go implementation
- ORC reading is the primary use case (most users don't write ORC)
- Can start with read-only and add write later
- Can use existing Parquet implementation patterns

## Implementation Plan

### Phase 1: ORC Reader

```
internal/io/orc/
├── reader.go          # Main ORC reader
├── types.go           # ORC type mapping
├── stripe.go          # Stripe-level reading
├── column.go          # Column vector reading
├── predicate.go       # Statistics-based filtering
└── codec.go           # Compression decompression
```

### Phase 2: ORC Writer (if pure Go library available)

```
internal/io/orc/
├── writer.go          # Main ORC writer
└── stripe_writer.go   # Stripe generation
```

### Phase 3: Integration

- Parser: Add ORC function parsing
- Planner: Add ORC scan plan generation
- Executor: Add ORC execution operator

## Type Mapping

| DuckDB Type | ORC Type | Notes |
|-------------|----------|-------|
| BOOLEAN | BOOLEAN | |
| TINYINT | TINYINT | |
| SMALLINT | SMALLINT | |
| INTEGER | INT | |
| BIGINT | LONG | |
| FLOAT | FLOAT | |
| DOUBLE | DOUBLE | |
| VARCHAR | STRING | |
| BLOB | BINARY | |
| TIMESTAMP | TIMESTAMP | |
| DATE | DATE | |
| INTERVAL | INTERVAL | Map to struct |
| STRUCT | STRUCT | |
| LIST | ARRAY | |
| MAP | MAP | Map to struct of (key, value) |
| DECIMAL(p,s) | DECIMAL | |
| UUID | STRING | Encode as string |
| JSON | STRING | Store as string |
| CHAR(n) | CHAR | Fixed-width string with padding |
| UNION | UNION | ORC-specific type, map to DuckDB UNION |

## Compression Support

| Compression | Reader Support | Writer Support |
|-------------|----------------|----------------|
| NONE | Required | Optional |
| ZLIB | Required | Optional |
| SNAPPY | Required | Optional |
| LZ4 | Required | Optional |
| ZSTD | Required (v1.4.3+) | Optional (v1.4.3+) |

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Pure Go ORC library availability | Could block implementation | Start with read-only, evaluate libraries |
| Type mapping edge cases | Data corruption | Comprehensive test suite |
| Large file memory usage | OOM on huge files | Stripe-level streaming |
| Performance vs C library | User complaints | Document limitations, optimize iteratively |

## Open Questions

1. Should we use an existing pure Go ORC library or implement from scratch?
2. Should write support be in initial scope or deferred?
3. What minimum Go version is required for ORC implementation?

## References

- ORC Spec: https://orc.apache.org/docs/spec.html
- DuckDB ORC: https://duckdb.org/docs/data/orc
- Parquet implementation reference: `internal/io/parquet/`
