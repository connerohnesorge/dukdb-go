# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document describes the chronological order in which all active change proposals should be implemented. Proposals are ordered by dependency and complexity.

## Active Proposals

| # | Proposal | Scope | Effort | Dependencies |
|---|----------|-------|--------|-------------|
| 1 | `add-utility-functions-v1.4.3` | 9 utility functions (system, date, string) | Small (2-3 days) | None |
| 2 | `add-list-array-functions-v1.4.3` | 6 list/array functions | Small (2-3 days) | None |
| 3 | `add-select-star-modifiers-v1.4.3` | EXCLUDE, REPLACE, COLUMNS() | Medium (4-5 days) | None |
| 4 | `add-comprehensive-json-functions-v1.4.3` | 5 JSON functions (scalar, aggregate, table) | Small (3-4 days) | None |
| 5 | `add-foreign-key-enforcement-v1.4.3` | FK constraint parsing + enforcement | Medium (5-7 days) | None |
| 6 | `add-s3-query-integration-v1.4.3` | Wire cloud filesystem to SQL queries | Medium (5-7 days) | None |
| 7 | `add-streaming-results-v1.4.3` | Chunked/streaming result delivery | Medium (5-7 days) | None |
| 8 | `add-columnar-compression-v1.4.3` | Constant, Dictionary, RLE compression | Large (8-10 days) | None |
| 9 | `add-replacement-scans-v1.4.3` | FROM 'file.csv' syntax | Small (2-3 days) | None |
| 10 | `add-export-import-database-v1.4.3` | Complete EXPORT/IMPORT DATABASE | Medium (4-5 days) | Soft dep on #5 (FK DDL) |

## Dependency Graph

```
Phase 1 (no dependencies — fully parallelizable)
├── add-utility-functions-v1.4.3                 [2-3 days]  ← smallest, quick win
├── add-list-array-functions-v1.4.3              [2-3 days]  ← small, quick win
├── add-select-star-modifiers-v1.4.3             [4-5 days]  ← parser+binder
├── add-replacement-scans-v1.4.3                 [2-3 days]  ← parser+binder
├── add-comprehensive-json-functions-v1.4.3      [3-4 days]
├── add-foreign-key-enforcement-v1.4.3           [5-7 days]
├── add-s3-query-integration-v1.4.3              [5-7 days]
├── add-streaming-results-v1.4.3                 [5-7 days]
└── add-columnar-compression-v1.4.3              [8-10 days]

Phase 2 (soft dependency on Phase 1: FK for complete DDL output)
└── add-export-import-database-v1.4.3            [4-5 days]
    └── depends on: add-foreign-key-enforcement (for FK DDL in schema.sql)
```

## Phase 1 — Independent Features (All Parallelizable)

All eight proposals touch different parts of the codebase and can be implemented simultaneously.

### add-utility-functions-v1.4.3

**Scope**: CURRENT_DATABASE, CURRENT_SCHEMA, VERSION (system); DAYNAME, MONTHNAME, YEARWEEK, EPOCH_US (date/time); TRANSLATE, STRIP_ACCENTS (string)

**Files touched**: `internal/executor/expr.go`, `internal/binder/utils.go`, `internal/engine/engine.go` (expose db name setting)

**Why Phase 1**: Self-contained — adds to existing function dispatch pattern. Each function is 5-20 lines.

---

### add-list-array-functions-v1.4.3

**Scope**: LIST_ELEMENT/ARRAY_EXTRACT, LIST_AGGREGATE/ARRAY_AGGREGATE, LIST_REVERSE_SORT/ARRAY_REVERSE_SORT, ARRAY_TO_STRING/LIST_TO_STRING, LIST_ZIP, LIST_RESIZE/ARRAY_RESIZE

**Files touched**: `internal/executor/list_functions.go`, `internal/executor/expr.go`, `internal/binder/utils.go`

**Why Phase 1**: Builds on existing list infrastructure (toSlice, compareValues, evaluateListSort). No cross-dependencies.

---

### add-select-star-modifiers-v1.4.3

**Scope**: SELECT * EXCLUDE(cols), SELECT * REPLACE(expr AS col), COLUMNS('regex') expression

**Files touched**: `internal/parser/ast.go`, `internal/parser/parser.go`, `internal/binder/bind_expr.go`, `internal/binder/bind_stmt.go`, `internal/binder/expressions.go`

**Why Phase 1**: Parser and binder changes only — no executor changes needed. Modifiers resolved at bind time.

---

### add-replacement-scans-v1.4.3

**Scope**: Allow string literals in FROM position to be automatically resolved to table function calls based on file extension (e.g., `FROM 'data.csv'` → `read_csv_auto('data.csv')`)

**Files touched**: `internal/parser/ast.go`, `internal/parser/parser.go`, `internal/binder/bind_stmt.go`

**Why Phase 1**: Parser and binder only — rewrites to existing table functions. No new executor code needed.

---

### add-comprehensive-json-functions-v1.4.3

**Scope**: JSON_CONTAINS, JSON_QUOTE (scalars), JSON_GROUP_ARRAY, JSON_GROUP_OBJECT (aggregates), JSON_EACH (table function)

**Files touched**: `internal/executor/expr.go`, `internal/executor/physical_aggregate.go`, `internal/executor/table_function_json_each.go` (new), `internal/binder/utils.go`

**Why Phase 1**: Self-contained — adds to existing JSON function infrastructure. No cross-dependencies.

---

### add-foreign-key-enforcement-v1.4.3

**Scope**: Parse FOREIGN KEY / REFERENCES constraints, store in catalog, enforce on INSERT/UPDATE/DELETE, CASCADE/RESTRICT actions

**Files touched**: `internal/parser/parser.go`, `internal/catalog/constraint.go` (new), `internal/executor/operator.go`, `internal/executor/physical_update.go`, `internal/executor/physical_delete.go`

**Why Phase 1**: Foundational integrity feature. Export/Import benefits from FK metadata for DDL generation, but FK enforcement itself has no prerequisites.

---

### add-s3-query-integration-v1.4.3

**Scope**: Wire existing S3/GCS/Azure filesystem implementations to `read_csv()`, `read_json()`, `read_parquet()`, and `COPY FROM/TO` for cloud URLs

**Files touched**: `internal/executor/table_function_csv.go`, `internal/executor/table_function_json.go`, `internal/executor/table_function_parquet.go`, `internal/executor/copy_cloud.go`

**Why Phase 1**: The filesystem backends already exist in `internal/io/filesystem/`. This proposal wires them to the query layer. No dependencies on other proposals.

---

### add-streaming-results-v1.4.3

**Scope**: Chunked result delivery via pull-based `StreamingResult` type. Reduces peak memory from O(rows) to O(chunk_size). New `BackendConnStreaming` interface.

**Files touched**: `internal/engine/conn.go`, `internal/executor/operator.go`, `conn.go`, `rows.go`

**Why Phase 1**: Engine-level change affecting how results are delivered. Independent of all other proposals.

---

### add-columnar-compression-v1.4.3

**Scope**: Constant, Dictionary, and RLE compression for in-memory RowGroup columns. Analyze data characteristics and select optimal codec per column.

**Files touched**: `internal/storage/table.go` (RowGroup), `internal/storage/compression/` (new package), `internal/storage/column.go`

**Why Phase 1**: Pure storage layer change. Does not affect query execution, parsing, or binding.

---

## Phase 2 — Export/Import Database

### add-export-import-database-v1.4.3

**Scope**: Complete EXPORT DATABASE (add sequence DDL, index DDL, DEFAULT clauses, FORMAT options, multi-schema file naming) and complete IMPORT DATABASE

**Files touched**: `internal/engine/export_import.go`

**Why Phase 2**: Benefits from FK enforcement being in place so `schema.sql` can include FOREIGN KEY constraints in CREATE TABLE output. Can proceed without FKs but the output will be incomplete.

**Soft dependency**: If FK enforcement (#5) is implemented first, EXPORT DATABASE can emit complete DDL including FK constraints. Without it, exported DDL omits FK definitions — functionally correct but less complete.

---

## Completed Proposals (Archived)

| Proposal | Status | Date |
|----------|--------|------|
| `add-missing-scalar-functions-v1.4.3` | Implemented & Archived | 2026-03-20 |
| `add-missing-sql-syntax-v1.4.3` | Implemented & Archived | 2026-03-20 |

---

## Implementation Schedule

```
Week 1:     Quick wins + start long-running work
             ├── Utility functions (2-3 days)          ← quick win
             ├── List/array functions (2-3 days)        ← quick win
             ├── JSON functions (3-4 days)              ← start early
             ├── SELECT * modifiers (4-5 days)          ← start early
             └── Columnar compression (8-10 days)       ← longest, start day 1

Week 1-2:   Medium features
             ├── FK enforcement (5-7 days)
             ├── S3 query integration (5-7 days)
             └── Streaming results (5-7 days)

Week 2-3:   Phase 2
             └── Export/Import Database (4-5 days)
```

**Total estimated effort**: 41-54 person-days
**With full parallelization**: 2-3 weeks elapsed time
**Critical path**: Columnar compression (8-10 days) → Export/Import (4-5 days) = 12-15 days

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| FK enforcement performance on bulk INSERT | Medium | Medium | Start with NO ACTION/RESTRICT only; parent lookup via index |
| S3 credential resolution edge cases | Medium | Low | Test with localstack; leverage existing secret manager |
| Compression analysis overhead for small tables | Low | Low | Skip compression for RowGroups below threshold |
| Streaming backpressure with slow consumers | Medium | Medium | Context cancellation + configurable channel buffer |
| JSON_EACH on deeply nested structures | Low | Low | Document depth limits; match DuckDB behavior |
| Export/Import round-trip fidelity | Medium | Medium | Integration tests comparing pre/post-export schema |
| COLUMNS regex performance on wide tables | Low | Low | Compile regex once, linear scan over column names |
| toTime() error handling in date functions | Low | Low | Follow existing EPOCH/EPOCH_MS pattern for error returns |
