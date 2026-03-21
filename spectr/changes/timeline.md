# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document describes the chronological order in which all active change proposals should be implemented. Proposals are ordered by dependency and complexity.

## Active Proposals

| # | Proposal | Scope | Effort | Dependencies | Status |
|---|----------|-------|--------|-------------|--------|
| 1 | `add-ddl-dml-extensions-v1.4.3` | COMMENT ON, ALTER COLUMN TYPE, DELETE USING | Medium (5-7 days) | None | IMPLEMENTED |
| 2 | `add-ordered-set-aggregates-v1.4.3` | WITHIN GROUP syntax + LISTAGG aggregate | Small (2-3 days) | None | PROPOSED |
| 3 | `add-enum-utility-functions-v1.4.3` | ENUM_RANGE, ENUM_FIRST, ENUM_LAST | Small (1-2 days) | None | PROPOSED |
| 4 | `add-missing-functions-round2-v1.4.3` | SHA1, SETSEED, LIST_VALUE, ANY_VALUE, HISTOGRAM, ARG_MIN/ARG_MAX aliases | Small (2-3 days) | None | PROPOSED |
| 5 | `add-s3-query-integration-v1.4.3` | Harden S3/cloud filesystem integration + tests | Medium (5-7 days) | None | PROPOSED |

## Dependency Graph

```
Phase 1 (no dependencies — fully parallelizable)
├── add-enum-utility-functions-v1.4.3          [1-2 days]  ← smallest, quick win
├── add-missing-functions-round2-v1.4.3        [2-3 days]  ← small functions
├── add-ordered-set-aggregates-v1.4.3          [2-3 days]  ← parser + executor
└── add-s3-query-integration-v1.4.3            [5-7 days]  ← integration testing

Already Completed:
└── add-ddl-dml-extensions-v1.4.3              [DONE]      ← implemented
```

## Phase 1 — Independent Features (All Parallelizable)

All four remaining proposals touch different parts of the codebase and can be implemented simultaneously.

### add-enum-utility-functions-v1.4.3

**Scope**: ENUM_RANGE, ENUM_FIRST, ENUM_LAST introspection functions

**Files touched**: `internal/executor/expr.go`, `internal/binder/utils.go`

**Why first**: Three simple functions that read from existing `TypeEntry.EnumValues` in the catalog. Smallest change — 30-50 lines total.

---

### add-missing-functions-round2-v1.4.3

**Scope**: SHA1, SETSEED, LIST_VALUE/LIST_PACK, ANY_VALUE, HISTOGRAM, ARG_MIN/ARG_MAX aliases

**Files touched**: `internal/executor/expr.go`, `internal/executor/hash.go`, `internal/executor/math.go`, `internal/executor/physical_aggregate.go`, `internal/executor/operator.go`, `internal/binder/utils.go`

**Why Phase 1**: Each function follows established patterns (SHA1 copies SHA256, ANY_VALUE copies FIRST, ARG_MIN/ARG_MAX are case label additions). No cross-dependencies.

---

### add-ordered-set-aggregates-v1.4.3

**Scope**: WITHIN GROUP (ORDER BY ...) syntax + LISTAGG aggregate function

**Files touched**: `internal/parser/parser.go`, `internal/executor/physical_aggregate.go`, `internal/executor/operator.go`, `internal/binder/utils.go`

**Why Phase 1**: Parser change (WITHIN GROUP maps to existing OrderBy field) + one new aggregate. No executor changes for existing ordered-set aggregates (PERCENTILE_CONT/DISC already handle OrderBy).

---

### add-s3-query-integration-v1.4.3

**Scope**: Harden cloud filesystem integration, add LocalStack integration tests for all table functions and COPY operations with S3/GCS/Azure URLs

**Files touched**: `internal/executor/table_function_csv.go`, `internal/executor/table_function_json.go`, `internal/executor/table_function_parquet.go`, `internal/executor/copy_cloud.go`

**Why Phase 1**: Primarily testing and error handling hardening. The core functionality already exists. Independent of all other proposals.

---

## Completed Proposals (Already Implemented)

| Proposal | Status | Date |
|----------|--------|------|
| `add-ddl-dml-extensions-v1.4.3` | Implemented | 2026-03-21 |
| `add-missing-scalar-functions-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-missing-sql-syntax-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-utility-functions-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-list-array-functions-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-comprehensive-json-functions-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-select-star-modifiers-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-foreign-key-enforcement-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-streaming-results-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-replacement-scans-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-columnar-compression-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-export-import-database-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-named-windows-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-is-distinct-from-v1.4.3` | Already implemented | 2026-03-20 |
| `add-function-aliases-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-complex-data-types` | Implemented & Archived | 2026-03-20 |
| `add-adaptive-optimization` | Implemented & Archived | 2026-03-20 |
| `add-attach-detach-database` | Implemented & Archived | 2026-03-20 |

## Implementation Schedule

```
Day 1:      Quick wins (parallelizable)
            ├── Enum utility functions (1-2 days)           ← smallest, done first
            ├── Missing functions round 2 (2-3 days)        ← simple patterns
            ├── Ordered-set aggregates (2-3 days)            ← parser + executor
            └── S3 integration hardening (5-7 days)          ← longest, start day 1

Day 3-4:    Quick wins complete, S3 testing continues

Day 5-7:    S3 integration complete, all features done
```

**Total estimated effort**: 10-15 person-days
**With full parallelization**: 5-7 days elapsed time
**Critical path**: S3 integration (5-7 days)

## Coverage Summary

### Fully Implemented DuckDB v1.4.3 Features

- All SQL syntax: SELECT, INSERT, UPDATE, DELETE, MERGE
- All DDL: CREATE/ALTER/DROP TABLE/VIEW/INDEX/SEQUENCE/SCHEMA/TYPE
- All joins: INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL, ASOF, POSITIONAL
- All set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL], BY NAME
- Window functions: All aggregate/ranking/value functions + WINDOW clause
- CTEs: WITH, WITH RECURSIVE
- Grouping: GROUP BY, GROUPING SETS, ROLLUP, CUBE
- Advanced: PIVOT/UNPIVOT, QUALIFY, SAMPLE, IS DISTINCT FROM
- File formats: Parquet, CSV, JSON, NDJSON, XLSX, Arrow
- Storage: Columnar compression (Dictionary, RLE, Constant), WAL, MVCC
- Transactions: READ UNCOMMITTED through SERIALIZABLE, savepoints
- Extensions: INSTALL/LOAD, ATTACH/DETACH, EXPORT/IMPORT DATABASE
- System: PRAGMA, SET/RESET, information_schema, pg_catalog
- 200+ scalar functions, 40+ aggregate functions

### Remaining Gaps (This Timeline)

1. WITHIN GROUP syntax (parser)
2. LISTAGG aggregate
3. ENUM_RANGE/ENUM_FIRST/ENUM_LAST
4. SHA1, SETSEED, LIST_VALUE/LIST_PACK
5. ANY_VALUE, HISTOGRAM aggregates
6. ARG_MIN/ARG_MAX underscore aliases
7. S3 integration hardening + tests
