# dukdb-go Change Proposal Implementation Timeline

This document outlines the chronological implementation order for all active change proposals in `spectr/changes/`. The order is determined by dependencies, logical progression, and the goal of building a complete, drop-in replacement for duckdb-go.

---

## Archived/Completed Proposals

The following proposals have been completed and are archived in `spectr/changes/archive/`:

| Phase | Proposal | Description |
|-------|----------|-------------|
| 1 | add-project-foundation | Go module, core interfaces, build constraints |
| 2 | add-database-driver | database/sql driver registration |
| 3 | add-type-system | 37 DuckDB type constants and TypeInfo |
| 4 | add-process-backend | Backend interface and engine stub |
| 5 | add-query-execution | Query parsing, planning, execution |
| 6 | add-result-handling | Result set scanning and iteration |
| 7 | add-prepared-statements | Prepared statement support |
| 8 | add-data-chunk-api | DataChunk and Vector for columnar data |
| 9 | add-appender-api | Bulk data loading via Appender |
| 10 | add-scalar-udf | Scalar user-defined functions |
| 11 | add-table-udf | Table-valued user-defined functions |
| 12 | add-profiling-api | Query profiling and timing |
| 13 | add-extended-types | Uhugeint, Bit, TimeNS wrappers |
| 14 | add-arrow-integration | Apache Arrow query results |
| 15 | add-replacement-scan | Replacement scan callbacks |
| 16 | add-query-appender | SQL-based bulk insertion |
| 17 | add-statement-introspection | StatementType() via BackendStmtIntrospector |

---

## Active Proposals - Implementation Order

### Phase 1: Core Infrastructure Enhancements

These proposals enhance the core type system and statement handling without introducing new major subsystems.

#### 1.1 add-type-system-enhancements
**Priority:** HIGH
**Dependencies:** None (builds on archived type-system)
**Estimated LOC:** ~750

Completes the type system with typed scanners for complex types:
- `ListScanner[T]`, `MapScanner[K,V]`, `StructScanner[T]`
- `UnionScanner`, `EnumScanner[T]`, `JSONScanner[T]`
- Parameter binding for complex types (`ListValue`, `StructValue`, `MapValue`)
- Type conversion utilities with pointer and nested type support

**Files Created/Modified:**
- NEW: `scan_types.go` (~400 lines)
- NEW: `bind_types.go` (~200 lines)
- NEW: `convert.go` (~150 lines)
- MODIFIED: `type_extended.go` (~50 lines)

---

#### 1.2 add-statement-type-detection
**Priority:** HIGH
**Dependencies:** None (builds on archived statement-introspection)
**Estimated LOC:** ~300

Adds comprehensive statement type detection and classification:
- 3 missing statement types: `MERGE_INTO`, `UPDATE_EXTENSIONS`, `COPY_DATABASE`
- `StmtReturnType` enum: `QUERY_RESULT`, `CHANGED_ROWS`, `NOTHING`
- `StmtProperties` struct with `IsReadOnly`, `IsStreaming`, `ColumnCount`, `ParamCount`
- Helper methods: `IsDML()`, `IsDDL()`, `IsQuery()`, `ModifiesData()`

**Files Created/Modified:**
- MODIFIED: `backend.go` (~100 lines)
- NEW: `stmt_helpers.go` (~50 lines)
- MODIFIED: `stmt.go` (~80 lines)
- MODIFIED: `internal/engine/conn.go` (~40 lines)
- MODIFIED: `internal/parser/*.go` (~30 lines)

---

#### 1.3 add-parameter-type-inference
**Priority:** MEDIUM
**Dependencies:** 1.2 (uses statement type for context)
**Estimated LOC:** ~400

Implements context-based parameter type inference:
- Expected type propagation through expression binding
- Column comparison context: `WHERE col = ?` → column type
- INSERT/UPDATE context: values get column types
- Function argument context: parameters get signature types
- Arithmetic context: defaults to `TYPE_DOUBLE`

**Files Created/Modified:**
- MODIFIED: `internal/binder/binder.go` (~200 lines)
- MODIFIED: `internal/engine/conn.go` (~50 lines)
- NEW: `internal/binder/type_inference.go` (~150 lines)

---

### Phase 2: Performance and API Enhancements

These proposals optimize existing functionality and add new API surface.

#### 2.1 improve-appender-performance
**Priority:** HIGH
**Dependencies:** Phase 1 complete (needs type system enhancements)
**Estimated LOC:** ~350

Replaces row-based INSERT generation with DataChunk-based buffering:
- Remove `buffer [][]any` in favor of `currentChunk DataChunk`
- Direct storage layer writes via `appendChunkToTable()`
- Auto-flush at VectorSize (2048 rows)
- Maintains API compatibility and deterministic testing support

**Files Created/Modified:**
- MODIFIED: `appender.go` (~300 lines refactored)
- MODIFIED: `conn.go` (~20 lines - add appendChunkToTable)
- MODIFIED: Storage layer (~30 lines)

---

#### 2.2 add-aggregate-udf-api
**Priority:** MEDIUM
**Dependencies:** Phase 1 complete (type system for signatures)
**Estimated LOC:** ~1700

Adds aggregate user-defined function support:
- `AggregateFunc`, `AggregateFuncConfig`, `AggregateFuncExecutor`
- State lifecycle: `Init`, `Destroy`
- Core operations: `Update`/`UpdateCtx`, `Combine`, `Finalize`/`FinalizeCtx`
- `AggregateFuncContext` with quartz clock integration
- Registry with overload support and binder integration

**Files Created/Modified:**
- NEW: `aggregate_udf.go` (~700 lines)
- NEW: `aggregate_udf_test.go` (~1000 lines)
- MODIFIED: `conn.go` (~25 lines)
- MODIFIED: `internal/binder/binder.go` (~60 lines)

---

### Phase 3: Arrow Integration Completion

These proposals complete Apache Arrow support for data interchange.

#### 3.1 add-arrow-registerview
**Priority:** MEDIUM
**Dependencies:** Archived arrow-integration
**Estimated LOC:** ~220

Adds `RegisterView()` method for Arrow RecordReaders:
- Register external Arrow data as queryable virtual tables
- Uses replacement scan callback for `FROM view_name` syntax
- Returns release function for cleanup
- Pure Go approach (no CGO Arrow C Data Interface)

**Files Created/Modified:**
- MODIFIED: `arrow.go` (~80 lines)
- MODIFIED: `internal/catalog/catalog.go` (~40 lines)
- NEW: `internal/engine/virtual_table.go` (~100 lines)

---

#### 3.2 complete-arrow-integration
**Priority:** MEDIUM
**Dependencies:** 3.1 (add-arrow-registerview)
**Estimated LOC:** ~750

Completes Arrow integration with bidirectional type mapping:
- `arrowToDuckDBType()` - Arrow → DuckDB conversion
- `arrowSchemaToDuckDB()` - Arrow schema to column info
- `DataChunkToRecordBatch()` - DuckDB → Arrow (copy semantics)
- `recordBatchToDataChunk()` - Arrow → DuckDB (copy semantics)
- `arrowTableSource` implementing `ChunkTableSource`

**Files Created/Modified:**
- MODIFIED: `arrow.go` (~200 lines)
- NEW: `arrow_convert.go` (~400 lines)
- NEW: `arrow_view.go` (~150 lines)

---

### Phase 4: Persistence Layer

These proposals add durable storage. Must be implemented in order.

#### 4.1 add-catalog-persistence
**Priority:** HIGH
**Dependencies:** Phases 1-3 complete
**Estimated LOC:** ~1300

Adds file-based catalog persistence:
- `internal/persistence/` package for file I/O
- Catalog serialization to JSON (gzip compressed)
- Storage serialization for row groups (binary format)
- Atomic save with temp file, verification, and rename
- Support for all 37 DuckDB types
- SHA-256 checksums for data integrity

**Files Created/Modified:**
- NEW: `internal/persistence/` (~600 lines)
- NEW: `internal/catalog/serialize.go` (~200 lines)
- NEW: `internal/storage/serialize.go` (~400 lines)
- MODIFIED: `internal/engine/engine.go` (~100 lines)

---

#### 4.2 add-wal-crash-recovery
**Priority:** HIGH
**Dependencies:** 4.1 (catalog-persistence required)
**Estimated LOC:** ~2800

Adds write-ahead logging and crash recovery:
- WAL entry types matching DuckDB (catalog + data operations)
- CRC64 checksummed entries
- Three-phase recovery: deserialize → checkpoint reconciliation → replay
- `CheckpointManager` with auto-checkpoint at configurable threshold
- PRAGMA support: `checkpoint_threshold`, `wal_autocheckpoint`
- `Close()` performs implicit checkpoint

**Files Created/Modified:**
- NEW: `internal/wal/entry.go` (~200 lines)
- NEW: `internal/wal/writer.go` (~300 lines)
- NEW: `internal/wal/reader.go` (~400 lines)
- NEW: `internal/wal/checkpoint.go` (~400 lines)
- NEW: `internal/storage/persistent.go` (~400 lines)
- MODIFIED: `internal/engine/engine.go` (~50 lines)
- NEW: `internal/engine/pragma.go` (~100 lines)
- NEW: Test files (~800 lines)

---

### Phase 5: Validation and Testing

This proposal creates the comprehensive compatibility test suite.

#### 5.1 add-compatibility-test-suite
**Priority:** HIGH
**Dependencies:** All above proposals (validates everything)
**Estimated LOC:** ~2000

Creates comprehensive compatibility testing framework:
- `DriverAdapter` abstraction for dukdb-go vs duckdb-go
- SQL compatibility tests: DDL, DML, queries, aggregates, window functions
- Type compatibility tests: all 29 supported types
- API compatibility tests: connections, transactions, prepared statements, errors
- Feature compatibility tests: appender, UDFs, profiling, DataChunk
- TPC-H benchmark tests (22 queries) with result comparison
- Deterministic testing with mock clock injection

**Files Created/Modified:**
- NEW: `compatibility/framework.go` (~200 lines)
- NEW: `compatibility/sql_test.go` (~500 lines)
- NEW: `compatibility/types_test.go` (~400 lines)
- NEW: `compatibility/api_test.go` (~300 lines)
- NEW: `compatibility/features_test.go` (~300 lines)
- NEW: `compatibility/tpch_test.go` (~200 lines)
- NEW: `compatibility/deterministic_test.go` (~100 lines)

---

## Implementation Phases Summary

| Phase | Proposals | Focus | Total LOC |
|-------|-----------|-------|-----------|
| 1 | 1.1, 1.2, 1.3 | Core infrastructure | ~1,450 |
| 2 | 2.1, 2.2 | Performance & API | ~2,050 |
| 3 | 3.1, 3.2 | Arrow integration | ~970 |
| 4 | 4.1, 4.2 | Persistence layer | ~4,100 |
| 5 | 5.1 | Validation | ~2,000 |
| **Total** | **10 proposals** | | **~10,570** |

---

## Dependency Graph

```
Phase 1 (Sequential):
  1.1 add-type-system-enhancements
    └─→ 1.2 add-statement-type-detection
          └─→ 1.3 add-parameter-type-inference

Phase 2 (After Phase 1):
  ├─→ 2.1 improve-appender-performance
  └─→ 2.2 add-aggregate-udf-api

Phase 3 (After archived arrow-integration):
  3.1 add-arrow-registerview
    └─→ 3.2 complete-arrow-integration

Phase 4 (After Phases 1-3):
  4.1 add-catalog-persistence
    └─→ 4.2 add-wal-crash-recovery

Phase 5 (After all above):
  5.1 add-compatibility-test-suite
```

---

## Critical Path

The critical path for reaching full DuckDB compatibility:

1. **Type System Enhancements** (1.1) - Required for all complex type handling
2. **Statement Type Detection** (1.2) - Required for proper query classification
3. **Appender Performance** (2.1) - Critical for bulk data loading performance
4. **Catalog Persistence** (4.1) - Enables file-based databases
5. **WAL/Crash Recovery** (4.2) - Enables ACID transaction durability
6. **Compatibility Test Suite** (5.1) - Validates drop-in replacement goal

---

## Parallel Execution Strategy

Three independent tracks can proceed after Phase 1:

**Track A (Performance & Persistence):**
```
improve-appender-performance → catalog-persistence → wal-crash-recovery
```

**Track B (UDF Completion):**
```
add-aggregate-udf-api
```

**Track C (Arrow Completion):**
```
add-arrow-registerview → complete-arrow-integration
```

All tracks converge at:
```
→ add-compatibility-test-suite
```

---

## Validation Checkpoints

After each phase, run:
```bash
go test -race ./...
golangci-lint run
nix develop -c tests
```

---

## Notes

- All proposals maintain quartz clock injection for deterministic testing
- All proposals are additive with no breaking changes to existing APIs
- CGO remains prohibited - all implementations are pure Go
- Test files use `t.TempDir()` for parallel test isolation
