# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document describes the chronological order in which all active change proposals should be implemented. Proposals are ordered by dependency and complexity.

## Active Proposals

| # | Proposal | Scope | Effort | Dependencies | Status |
|---|----------|-------|--------|-------------|--------|
| 1 | `add-ddl-dml-extensions-v1.4.3` | COMMENT ON, ALTER COLUMN TYPE, DELETE USING | Medium | None | IMPLEMENTED |
| 2 | `add-enum-utility-functions-v1.4.3` | ENUM_RANGE, ENUM_FIRST, ENUM_LAST | Small | None | IMPLEMENTED & ARCHIVED |
| 3 | `add-missing-functions-round2-v1.4.3` | SHA1, SETSEED, LIST_VALUE, ANY_VALUE, HISTOGRAM, ARG_MIN/ARG_MAX | Small | None | IMPLEMENTED & ARCHIVED |
| 4 | `add-function-aliases-v1.4.3` | DATETRUNC, DATEADD, ORD, IFNULL/NVL, BIT_LENGTH, etc. | Small | None | IMPLEMENTED & ARCHIVED |
| 5 | `add-ordered-set-aggregates-v1.4.3` | WITHIN GROUP syntax + LISTAGG aggregate | Small (2-3 days) | None | PROPOSED |
| 6 | `add-s3-query-integration-v1.4.3` | Harden S3/cloud filesystem + tests | Medium (5-7 days) | None | PROPOSED |
| 7 | `add-metadata-commands-v1.4.3` | DESCRIBE, SHOW TABLES/COLUMNS, SUMMARIZE, CALL | Medium (3-5 days) | None | IMPLEMENTED |
| 8 | `add-table-ddl-extensions-v1.4.3` | CREATE OR REPLACE TABLE, TEMP TABLE, ADD/DROP CONSTRAINT | Medium (3-5 days) | None | PROPOSED |
| 9 | `add-standalone-aggregate-filter-v1.4.3` | FILTER (WHERE) on non-window aggregates | Small (1-2 days) | None | PROPOSED |
| 10 | `add-missing-conversion-functions-v1.4.3` | TO_DATE, TO_CHAR, GENERATE_SUBSCRIPTS | Small (1-2 days) | None | PROPOSED |
| 11 | `add-missing-list-string-functions-v1.4.3` | LIST_APPEND, LIST_PREPEND, LIST_HAS, STRING_TO_ARRAY, REGEXP_FULL_MATCH | Small (1-2 days) | None | PROPOSED |
| 12 | `add-missing-aggregates-round3-v1.4.3` | PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG | Small (1-2 days) | None | PROPOSED |
| 13 | `add-reset-statement-v1.4.3` | RESET variable, RESET ALL | Small (1 day) | None | PROPOSED |
| 14 | `add-struct-field-access-v1.4.3` | struct_col.field dot notation | Small (1-2 days) | None | PROPOSED |

## Dependency Graph

```
Phase 1 (no dependencies — fully parallelizable)
├── add-reset-statement-v1.4.3                 [1 day]     ← smallest, parser+conn only
├── add-missing-conversion-functions-v1.4.3    [1-2 days]  ← TO_DATE, TO_CHAR, GENERATE_SUBSCRIPTS
├── add-missing-list-string-functions-v1.4.3   [1-2 days]  ← LIST_APPEND/PREPEND, aliases, REGEXP_FULL_MATCH
├── add-missing-aggregates-round3-v1.4.3       [1-2 days]  ← PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG
├── add-standalone-aggregate-filter-v1.4.3     [1-2 days]  ← FILTER (WHERE) on non-window aggs
├── add-struct-field-access-v1.4.3             [1-2 days]  ← binder disambiguates struct.field
├── add-ordered-set-aggregates-v1.4.3          [2-3 days]  ← WITHIN GROUP + LISTAGG
├── add-table-ddl-extensions-v1.4.3            [3-5 days]  ← OR REPLACE, TEMP, constraints
└── add-s3-query-integration-v1.4.3            [5-7 days]  ← integration testing

Already Completed:
├── add-ddl-dml-extensions-v1.4.3              [DONE]
├── add-enum-utility-functions-v1.4.3          [DONE]
├── add-missing-functions-round2-v1.4.3        [DONE]
├── add-function-aliases-v1.4.3                [DONE]
└── add-metadata-commands-v1.4.3               [DONE]
```

## Phase 1 — Independent Features (All Parallelizable)

All nine remaining proposals touch different parts of the codebase and can be implemented simultaneously.

### add-reset-statement-v1.4.3

**Scope**: RESET variable, RESET ALL statement

**Files touched**: `internal/parser/ast.go`, `internal/parser/parser.go`, `internal/parser/parser_pragma.go`, `internal/engine/conn.go`

**Why first**: Smallest proposal — one new AST node, parser function, and handler. Follows SET pattern exactly. ~50 lines total.

---

### add-missing-conversion-functions-v1.4.3

**Scope**: TO_DATE, TO_CHAR (STRFTIME alias), GENERATE_SUBSCRIPTS

**Files touched**: `internal/executor/expr.go`, `internal/binder/utils.go`

**Why first**: Three functions following established patterns. TO_CHAR is just an alias addition. Smallest change — ~30 lines total.

---

### add-standalone-aggregate-filter-v1.4.3

**Scope**: FILTER (WHERE ...) clause on non-window aggregate functions

**Files touched**: `internal/parser/ast.go`, `internal/parser/parser.go`, `internal/binder/expressions.go`, `internal/binder/bind_expr.go`, `internal/executor/physical_aggregate.go`

**Why Phase 1**: Parser already parses FILTER; just need to allow it without OVER and thread through binder/executor. Window FILTER pattern already exists as reference.

---

### add-ordered-set-aggregates-v1.4.3

**Scope**: WITHIN GROUP (ORDER BY ...) syntax + LISTAGG aggregate function

**Files touched**: `internal/parser/parser.go`, `internal/executor/physical_aggregate.go`, `internal/executor/operator.go`, `internal/binder/utils.go`

**Why Phase 1**: Parser change (WITHIN GROUP maps to existing OrderBy field) + one new aggregate. No executor changes for existing ordered-set aggregates.

---

### add-missing-list-string-functions-v1.4.3

**Scope**: LIST_APPEND/PREPEND, LIST_HAS alias, STRING_TO_ARRAY alias, REGEXP_FULL_MATCH

**Files touched**: `internal/executor/expr.go`, `internal/binder/utils.go`

**Why Phase 1**: Two new list functions (follow LIST_CONCAT pattern), two alias additions, one new regex function. All in executor function dispatch. ~80 lines total.

---

### add-missing-aggregates-round3-v1.4.3

**Scope**: PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG aggregate functions

**Files touched**: `internal/executor/physical_aggregate.go`, `internal/executor/operator.go`, `internal/binder/utils.go`

**Why Phase 1**: Five aggregates following established SUM/AVG/MEDIAN patterns. All in aggregate dispatch. ~120 lines total.

---

### add-struct-field-access-v1.4.3

**Scope**: `struct_col.field` dot notation syntax

**Files touched**: `internal/binder/expressions.go`, `internal/binder/bind_expr.go`, `internal/executor/expr.go`

**Why Phase 1**: New BoundFieldAccess expression type. Binder disambiguates table.column vs struct.field. Follows STRUCT_EXTRACT pattern for evaluation.

---

### add-table-ddl-extensions-v1.4.3

**Scope**: CREATE OR REPLACE TABLE, CREATE TEMP TABLE, ALTER TABLE ADD/DROP CONSTRAINT

**Files touched**: `internal/parser/ast.go`, `internal/parser/parser.go`, `internal/parser/parser_ddl.go`, `internal/binder/bind_stmt.go`, `internal/binder/statements.go`, `internal/planner/physical.go`, `internal/executor/operator.go`, `internal/executor/ddl.go`

**Why Phase 1**: Threading flags through existing pipeline. Parser already parses OR REPLACE and TEMPORARY but doesn't pass them to CREATE TABLE. Constraint infrastructure already exists.

---

### add-s3-query-integration-v1.4.3

**Scope**: Harden cloud filesystem integration, add LocalStack integration tests

**Files touched**: `internal/executor/table_function_csv.go`, `internal/executor/table_function_json.go`, `internal/executor/table_function_parquet.go`, `internal/executor/copy_cloud.go`

**Why Phase 1**: Primarily testing and error handling hardening. Core functionality already exists. Independent of all other proposals.

---

## Completed Proposals (Already Implemented)

| Proposal | Status | Date |
|----------|--------|------|
| `add-metadata-commands-v1.4.3` | Implemented | 2026-03-21 |
| `add-ddl-dml-extensions-v1.4.3` | Implemented | 2026-03-21 |
| `add-enum-utility-functions-v1.4.3` | Implemented & Archived | 2026-03-21 |
| `add-missing-functions-round2-v1.4.3` | Implemented & Archived | 2026-03-21 |
| `add-function-aliases-v1.4.3` | Implemented & Archived | 2026-03-21 |
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
| `add-complex-data-types` | Implemented & Archived | 2026-03-20 |
| `add-adaptive-optimization` | Implemented & Archived | 2026-03-20 |
| `add-attach-detach-database` | Implemented & Archived | 2026-03-20 |

## Implementation Schedule

```
Day 1:      Quick wins (all parallelizable — start everything)
            ├── RESET statement (1 day)                    ← parser + conn handler
            ├── Conversion functions (1-2 days)            ← TO_DATE, TO_CHAR, GENERATE_SUBSCRIPTS
            ├── List/string functions (1-2 days)           ← LIST_APPEND/PREPEND, aliases, REGEXP_FULL_MATCH
            ├── Aggregates round 3 (1-2 days)              ← PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG
            ├── Aggregate FILTER clause (1-2 days)         ← parser lift + executor
            ├── Struct field access (1-2 days)             ← binder + evaluator
            ├── Ordered-set aggregates (2-3 days)          ← WITHIN GROUP + LISTAGG
            ├── Table DDL extensions (3-5 days)            ← OR REPLACE, TEMP, constraints
            └── S3 integration hardening (5-7 days)        ← longest, start day 1

Day 1:      RESET statement complete
Day 2:      Conversion functions, list/string functions, aggregates round 3,
            aggregate FILTER, struct field access complete
Day 3-4:    Ordered-set aggregates complete, table DDL continues
Day 5-7:    Table DDL + S3 integration complete
```

**Total estimated effort**: 19-30 person-days
**With full parallelization**: 5-7 days elapsed time
**Critical path**: S3 integration (5-7 days) or Table DDL extensions (3-5 days)

## Coverage Summary

### Fully Implemented DuckDB v1.4.3 Features

- All SQL syntax: SELECT, INSERT, UPDATE, DELETE, MERGE
- All DDL: CREATE/ALTER/DROP TABLE/VIEW/INDEX/SEQUENCE/SCHEMA/TYPE
- All joins: INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL, ASOF, POSITIONAL
- All set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL], BY NAME
- Window functions: All aggregate/ranking/value functions + WINDOW clause + FILTER
- CTEs: WITH, WITH RECURSIVE
- Grouping: GROUP BY, GROUPING SETS, ROLLUP, CUBE
- Advanced: PIVOT/UNPIVOT, QUALIFY, SAMPLE, IS DISTINCT FROM
- File formats: Parquet, CSV, JSON, NDJSON, XLSX, Arrow
- Storage: Columnar compression (Dictionary, RLE, Constant), WAL, MVCC
- Transactions: READ UNCOMMITTED through SERIALIZABLE, savepoints
- Extensions: INSTALL/LOAD, ATTACH/DETACH, EXPORT/IMPORT DATABASE
- System: PRAGMA, SET/RESET, information_schema, pg_catalog
- 200+ scalar functions, 40+ aggregate functions
- IF/IFF, FORMAT/PRINTF, TYPEOF/PG_TYPEOF, BASE64 encode/decode, URL encode/decode
- ENUM_RANGE/ENUM_FIRST/ENUM_LAST, SHA1, SETSEED, LIST_VALUE
- ANY_VALUE, HISTOGRAM, ARG_MIN/ARG_MAX, COMMENT ON, ALTER COLUMN TYPE

### Remaining Gaps (This Timeline)

1. RESET variable / RESET ALL statement
2. TO_DATE, TO_CHAR, GENERATE_SUBSCRIPTS functions
3. LIST_APPEND, LIST_PREPEND, LIST_HAS, STRING_TO_ARRAY, REGEXP_FULL_MATCH
4. PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG aggregates
5. FILTER clause on non-window aggregates
6. Struct dot notation field access (struct_col.field)
7. WITHIN GROUP syntax + LISTAGG aggregate
8. CREATE OR REPLACE TABLE
9. CREATE TEMP/TEMPORARY TABLE
10. ALTER TABLE ADD/DROP CONSTRAINT
11. S3 integration hardening + tests
