# Design: DuckDB File Format Compatibility

## Context

The system SHALL use the official DuckDB file format for persistent storage, enabling compatibility with the DuckDB ecosystem.

**Stakeholders**:
- Application developers needing compatibility with DuckDB ecosystem
- Data engineers requiring data portability between systems
- Cloud storage users needing S3/Cloud integration
- Users migrating from official DuckDB

**Constraints**:
- Pure Go implementation (no CGO)
- Must support reading existing DuckDB files (v64+)
- Must write files compatible with DuckDB v1.3+
- Performance must be competitive with official DuckDB

## Goals / Non-Goals

**Goals**:
1. Read and write standard DuckDB `.duckdb` files
2. Implement dual 4KB rotating header blocks for crash safety
3. Support all DuckDB type serialization (including new types)
4. Implement DuckDB WAL format
5. Provide advanced compression (FSST, RLE, BitPacking, Chimp, Zstd)
6. Support ART index persistence
7. Implement row group format with column segments

**Non-Goals**:
1. ALP/Patas compression (requires native C++ implementations)
2. Full replication protocol
3. Parallel checkpointing (initial implementation)
4. Encryption support

## Decisions

### Decision 1: File Format Structure

Implement DuckDB's dual 4KB rotating headers for crash safety and compatibility.

```
DuckDB File Structure:
┌─────────────────────────────────────────────────────────────┐
│ Block A Header (4096 bytes) - Primary                       │
├─────────────────────────────────────────────────────────────┤
│ Block B Header (4096 bytes) - Mirror (for crash recovery)   │
├─────────────────────────────────────────────────────────────┤
│ Metadata (variable)                                         │
├─────────────────────────────────────────────────────────────┤
│ Checkpoint (optional)                                       │
├─────────────────────────────────────────────────────────────┤
│ Free List (variable)                                        │
├─────────────────────────────────────────────────────────────┤
│ Data Storage (Row Groups, Column Segments)                  │
└─────────────────────────────────────────────────────────────┘

Header Block A/B Format:
┌─────────────────────────────────────────────────────────────┐
│ Magic Number: "DUCK" (4 bytes)                              │
│ Version: 64 (4 bytes, little-endian)                        │
│ Header Size: 4096 (4 bytes)                                 │
│ Flags (4 bytes)                                             │
│ Salt A/B (8 bytes each) - for detecting write order         │
│ Checksum (4 bytes) - CRC32 of header                        │
│ Reserved (4060 bytes)                                       │
└─────────────────────────────────────────────────────────────┘
```

### Decision 2: Magic Number and Version

Use `DUCK` (0x4455434B) with version 64+ for DuckDB compatibility.

```go
const (
    DuckDBMagicNumber = "DUCK"  // 0x4455434B
    DuckDBVersion     = 64      // Current DuckDB format version
)
```

### Decision 3: Catalog Serialization Format

Implement DuckDB's binary property-based serialization for compatibility and performance.

**Property-Based Serialization Format**:
```
Property ID (varint) → Property Value (type-specific encoding)

Property IDs (partial list):
0:   END
1:   TYPE
2:   NAME
3:   NULLABLE
4:   DEFAULT
5:   PRIMARY_KEY
6:   FOREIGN_KEY
...
100: CHILD_TYPE
101: CHILD_NAME
...
```

### Decision 4: Header Writing Strategy

Write to temporary files and swap on success to ensure atomicity and prevent corruption.

### Decision 5: Compression Algorithm Selection

Implement pure Go alternatives for DuckDB algorithms: FSST, RLE, BitPacking, Chimp, Zstd.

### Decision 6: WAL Format

Implement DuckDB WAL format for compatibility with DuckDB tools and recovery.

### Decision 7: Row Group Format

Implement DuckDB row group format with column segments for per-column compression.

### Decision 8: Type Support for New Types

Support UNION, BIT, TIME_TZ, TIMESTAMP_TZ, and INTERVAL.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| ALP/Patas compression | High | Document as unsupported; use Zstd as alternative |
| Performance regression | Medium | Profile and optimize hot paths; add benchmarks |
| Format version handling | Medium | Support multiple versions in reader |
| Memory usage for compression | Medium | Stream compression; limit buffer sizes |

## Implementation Plan

### Phase 1: Reader Implementation
1. Implement DuckDB header parsing
2. Implement catalog deserialization
3. Implement data chunk reading
4. Add WAL reading capability

### Phase 2: Writer Implementation
1. Implement DuckDB header writing
2. Implement catalog serialization
3. Implement data chunk writing
4. Add WAL writing capability

### Phase 3: Compression
1. Implement RLE compression
2. Implement BitPacking
3. Implement FSST
4. Implement Chimp/Zstd

### Phase 4: Index Persistence
1. Implement ART index serialization
2. Add index storage to row groups
3. Update checkpoint to include indexes