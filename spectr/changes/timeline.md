# DuckDB v1.4.3 Compatibility Implementation Timeline

This document provides a chronological timeline for implementing all change proposals in dependency order to achieve full DuckDB v1.4.3 compatibility.

## New Proposals (v1.4.3 Compatibility)

The following proposals were created to address gaps identified during v1.4.3 compatibility analysis:

| Change ID | Feature | Priority | Effort | Dependencies |
|-----------|---------|----------|--------|--------------|
| `add-pragma-checkpoint-threshold` | PRAGMA checkpoint_threshold integration | HIGH | 2-3 weeks | None |
| `verify-azure-write-support` | Azure Blob Storage write verification | MEDIUM | 1-2 weeks | Cloud storage base |
| `add-orc-format-support` | ORC file format support (read/write) | HIGH | 5-7 months | None |
| `add-iceberg-table-support` | Apache Iceberg table format | MEDIUM | 9-12 months | ORC support |
| `add-postgresql-compat-mode` | PostgreSQL wire protocol compatibility | LOW | 3-6 months | None |
| `enhance-cost-based-optimizer` | Advanced statistics and subquery optimization | HIGH | 11-15 weeks | None |
| `fix-index-usage` | Connect optimizer hints to planner (CRITICAL) | CRITICAL | 4 weeks | None |

## Timeline Overview

```
Phase 1: Critical Reliability (Weeks 1-4)
├─→ add-pragma-checkpoint-threshold [P0]
├─→ verify-azure-write-support [P1]
└─→ fix-index-usage [P0 - CRITICAL]

Phase 2: Core Optimizer (Weeks 5-12)
└─→ enhance-cost-based-optimizer [P0]

Phase 3: Core File Formats (Months 4-11)
└─→ add-orc-format-support [P0]

Phase 4: Advanced Table Formats (Months 12-24)
└─→ add-iceberg-table-support [P1]
    (depends on: ORC support)

Phase 5: Compatibility Features (Months 18-24)
└─→ add-postgresql-compat-mode [P2]
    (can run in parallel)
```

---

## Dependency Graph

```
                            ┌─────────────────────────────────┐
                            │       dukdb-go base             │
                            │   (existing implementation)     │
                            └────────────────┬────────────────┘
                                           │
           ┌───────────────┬───────────────┼───────────────┬───────────────┐
           │               │               │               │               │
           ▼               ▼               ▼               ▼               ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ fix-index-usage  │ │ add-pragma-      │ │ verify-azure-    │ │ enhance-cost-    │ │ add-orc-format-  │
│ [P0 - CRITICAL]  │ │ checkpoint-      │ │ write-support    │ │ based-optimizer  │ │ support          │
│ (4 weeks)        │ │ threshold        │ │ [P1 - MEDIUM]    │ │ [P0 - HIGH]      │ │ [P0 - HIGH]      │
│                  │ │ [P0 - CRITICAL]  │ │                  │ │ (11-15 weeks)    │ │ (5-7 months)     │
└────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
         │                    │                    │                    │                    │
         │                    │                    │                    │                    │
         ▼                    ▼                    ▼                    ▼                    ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ Future: Index    │ │ add-postgresql-  │ │ Future: Cloud    │ │ Future: Parallel │ │ add-iceberg-     │
│ Range Scan       │ │ compat-mode      │ │ Optimization     │ │ Execution        │ │ table-support    │
│ Index-Only Scan  │ │ [P2 - LOW]       │ │                  │ │                  │ │ [P1 - MEDIUM]    │
└──────────────────┘ └──────────────────┘ └──────────────────┘ └──────────────────┘ │ (9-12 months)    │
                                                                                  │                  │
                                                                                  └────────┬─────────┘
                                                                                           │
                                                                                           ▼
                                                                                  ┌──────────────────┐
                                                                                  │ add-postgresql-  │
                                                                                  │ compat-mode      │
                                                                                  │ [P2 - LOW]       │
                                                                                  └──────────────────┘
```

---

## Phase 1: Critical Reliability (Weeks 1-4)

### 1.1 PRAGMA checkpoint_threshold Integration

**Change**: `spectr/changes/add-pragma-checkpoint-threshold/`

| Attribute | Value |
|-----------|-------|
| Priority | P0 - CRITICAL |
| Effort | 2-3 weeks |
| Dependencies | None |

**Key Deliverables:**
- Integration between `PRAGMA checkpoint_threshold` and CheckpointManager
- Threshold parsing with suffix support (b, kb, mb, gb)
- Persistent storage in `duckdb.settings` table
- Unit tests for threshold configuration

**Success Criteria:**
- `PRAGMA checkpoint_threshold = '256MB'` works and persists
- CheckpointManager uses configurable threshold
- All existing tests pass

---

### 1.2 Critical Index Usage Fix

**Change**: `spectr/changes/fix-index-usage/`

| Attribute | Value |
|-----------|-------|
| Priority | P0 - CRITICAL |
| Effort | 4 weeks |
| Dependencies | None |

**Key Deliverables:**
- Connect optimizer hints to planner physical plan generation
- `createPhysicalIndexScan()` implementation
- ART range scan support (<, >, BETWEEN)
- EXPLAIN output showing index usage
- Unit and integration tests

**Critical Issue Fixed:**
- Previously: Planner ignored optimizer hints, PhysicalIndexScan never created
- Now: Hints passed from optimizer to planner, index scans actually used

**Success Criteria:**
- `CREATE INDEX` improves query performance
- `EXPLAIN` shows IndexScan when appropriate
- Range queries use index scans
- All existing tests pass

---

### 1.3 Azure Write Support Verification

**Change**: `spectr/changes/verify-azure-write-support/`

| Attribute | Value |
|-----------|-------|
| Priority | P1 - MEDIUM |
| Effort | 1-2 weeks |
| Dependencies | Existing cloud storage implementation |

**Key Deliverables:**
- Verification of existing Azure write implementation
- COPY TO with Azure Blob Storage works correctly
- Documentation for Azure authentication
- Integration tests

**Success Criteria:**
- `COPY table TO 'azure://container/file.parquet'` works
- Both connection string and account/key authentication work
- Documentation complete

---

## Phase 2: Core Optimizer (Weeks 5-12)

### 2.1 Cost-Based Optimizer Enhancement

**Change**: `spectr/changes/enhance-cost-based-optimizer/`

| Attribute | Value |
|-----------|-------|
| Priority | P0 - HIGH |
| Effort | 11-15 weeks |
| Dependencies | None |

**Key Deliverables:**
- Statistics persistence and auto-update
- Subquery decorrelation (correlated EXISTS, SCALAR, ANY)
- Predicate pushdown optimization
- Multi-column statistics and cross-predicate selectivity
- Cardinality learning for runtime adaptation

**Success Criteria:**
- Statistics persist across database restarts
- Correlated subqueries execute efficiently
- Predicate pushdown reduces data processing
- EXPLAIN ANALYZE shows improved cardinality estimates
- TPC-H benchmark shows performance improvement

---

## Phase 3: Core File Formats (Months 4-11)

### 3.1 ORC File Format Support

**Change**: `spectr/changes/add-orc-format-support/`

| Attribute | Value |
|-----------|-------|
| Priority | P0 - HIGH |
| Effort | 5-7 months |
| Dependencies | None |

**Key Deliverables:**
- `read_orc()` and `read_orc_auto()` functions
- ORC file header, footer, and stripe parsing
- Compression support (zlib, snappy, lz4, zstd)
- Type mapping (ORC → DuckDB types) including UNION, CHAR
- Predicate push-down using column statistics
- Bloom filter support
- ORC writer (Phase 2)

**Success Criteria:**
- Read ORC files from Spark, Hive, and other big data tools
- All compression types supported
- Type mapping covers all ORC types
- Performance within 3x of DuckDB

**Note:** ORC support is a prerequisite for Iceberg table format.

---

## Phase 4: Advanced Table Formats (Months 12-24)

### 4.1 Apache Iceberg Table Format Support

**Change**: `spectr/changes/add-iceberg-table-support/`

| Attribute | Value |
|-----------|-------|
| Priority | P1 - MEDIUM |
| Effort | 9-12 months |
| Dependencies | ORC support (3.1), Cloud storage (S3, GCS, Azure) |

**Key Deliverables:**
- Iceberg metadata parser (metadata.json, manifests, snapshots)
- `iceberg_metadata()` and `iceberg_snapshots()` functions for discovery
- Time travel queries (`AS OF TIMESTAMP`, `AS OF SNAPSHOT`)
- Version selection parameters (version, allow_moved_paths, metadata_compression_codec)
- Version guessing for tables without version-hint.text
- Delete file handling (positional, equality)
- REST catalog support with OAuth2
- Partition pruning using Iceberg partition specs
- Schema evolution handling
- Read-only initially (write support future)

**Success Criteria:**
- Read Iceberg tables from Spark/Flink
- Time travel queries work correctly
- Version selection and guessing work
- Delete files handled correctly
- Compatible with Iceberg spec v1 and v2

---

## Phase 5: Compatibility Features (Months 18-24)

### 5.1 PostgreSQL Compatibility Mode

**Change**: `spectr/changes/add-postgresql-compat-mode/`

| Attribute | Value |
|-----------|-------|
| Priority | P2 - LOW |
| Effort | 3-6 months |
| Dependencies | None (can run in parallel) |

**Key Deliverables:**
- PostgreSQL wire protocol server
- PostgreSQL type compatibility layer
- Function name aliases (`now()` → `current_timestamp`)
- PostgreSQL syntax variations (`DISTINCT ON`, `ILIKE`, etc.)
- information_schema compatibility

**Success Criteria:**
- PostgreSQL clients can connect (psql, pgx, libpq)
- Basic queries work correctly
- Can be used for ORM testing

---

## Original Proposals (GAP-001, GAP-002, etc.)

The following proposals from the original timeline remain valid and should be considered for future implementation:

| Gap ID | Feature | Priority | Dependencies |
|--------|---------|----------|--------------|
| GAP-001 | DuckDB File Format | CRITICAL | None |
| GAP-004 | Parallel Execution | CRITICAL | enhance-cost-based-optimizer |
| GAP-005 | Arrow/IPC Format | HIGH | GAP-001 |
| GAP-007 | Savepoints | HIGH | None |
| GAP-008 | Isolation Levels | HIGH | GAP-007 |
| GAP-009 | Snapshot Isolation | HIGH | GAP-008 |
| GAP-011 | Excel/XLSX | MEDIUM | None |
| GAP-012 | Full-Text Search | MEDIUM | None |
| GAP-013 | Vector Similarity | MEDIUM | None |

---

## Implementation Order Recommendations

### Recommended for Production Focus

1. **fix-index-usage** - CRITICAL: Indexes currently not used in queries
2. **add-pragma-checkpoint-threshold** - Critical for production reliability
3. **enhance-cost-based-optimizer** - Improves query performance
4. **add-orc-format-support** - Fundamental for big data interoperability
5. **add-iceberg-table-support** - Critical for data lake workflows
6. **verify-azure-write-support** - Verify v1.4.3 feature support
7. **add-postgresql-compat-mode** - Convenience feature

### Recommended for Full Feature Parity

1. **fix-index-usage** - Critical bug fix
2. **enhance-cost-based-optimizer** - Improves query performance
3. **add-pragma-checkpoint-threshold** - Production reliability
4. **add-orc-format-support** - Core format support
5. **add-iceberg-table-support** - Data lake support
6. **GAP-004** (Parallel Execution) - Multi-core utilization
7. **GAP-005** (Arrow/IPC) - Data science integrations
8. **GAP-007/008/009** - Transaction isolation
9. **add-postgresql-compat-mode** - Compatibility
10. **GAP-012** (Full-Text Search) - Search capabilities
11. **GAP-013** (Vector Similarity) - ML capabilities

---

## Resource Allocation

### Option A: Focused Team (1-2 engineers)

- **Weeks 1-4**: Phase 1 (checkpoint, index fix, Azure)
- **Weeks 5-12**: Phase 2 (optimizer)
- **Months 4-11**: Phase 3 (ORC)
- **Months 12-24**: Phase 4 (Iceberg)
- **Months 18-24**: Phase 5 (PostgreSQL) or original GAPs

**Total timeline**: ~24 months

### Option B: Parallel Teams (2-3 engineers)

**Team A: Critical Fixes (Weeks 1-12)**
- Weeks 1-4: Phase 1 (checkpoint, index fix, Azure)
- Weeks 5-12: Phase 2 (optimizer)

**Team B: Formats (Starts Month 3)**
- Months 3-9: Phase 3 (ORC)
- Months 10-18: Phase 4 (Iceberg)

**Team C: Compatibility (Starts Month 12)**
- Months 12-18: Phase 5 (PostgreSQL)
- Or work on original GAP proposals

**Total timeline**: ~12-18 months

---

## Success Criteria Summary

| Phase | Milestone | Criteria |
|-------|-----------|----------|
| Phase 1 | Checkpoint configuration | `PRAGMA checkpoint_threshold` configurable, Index usage works, Azure writes verified |
| Phase 2 | Optimizer enhancement | Statistics persist, Subqueries decorrelated, Cardinality accurate |
| Phase 3 | ORC support | Read ORC from Spark/Hive, all compressions work, Type mapping complete |
| Phase 4 | Iceberg support | Read Iceberg tables, Time travel works, Delete files handled |
| Phase 5 | PostgreSQL mode | PostgreSQL clients can connect |

---

## Notes

### Already Implemented (No Proposal Needed)
- CSV, JSON, Parquet file formats
- S3, GCS, Azure cloud storage (read)
- Basic ACID transactions with WAL
- 200+ functions
- User-Defined Functions (Scalar, Aggregate, Table)
- Secrets management
- Type system (JSON, GEOMETRY, BIGNUM, VARIANT, LAMBDA)
- DuckDB File Format (native .duckdb files)
- MVCC with full isolation levels

### Not Feasible (Pure Go Limitation)
- C/C++ extensions (GAP-006)
- Full Iceberg write with ACID (requires C++ library)
- Some PostgreSQL wire protocol features

### Out of Scope
- Distributed query execution
- GPU acceleration
- SIMD vectorization
