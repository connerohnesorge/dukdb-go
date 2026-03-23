# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document describes the chronological order in which all active change proposals should be implemented. Proposals are ordered by dependency and complexity.

## Active Proposals (8 total)

| # | Proposal | Scope | Effort | Dependencies | Status |
|---|----------|-------|--------|-------------|--------|
| 1 | `add-current-datetime-functions-v1.4.3` | NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, TODAY() | Small (1-2 days) | None | GRADED & READY |
| 2 | `add-missing-string-functions-round4-v1.4.3` | OCTET_LENGTH, INITCAP, SOUNDEX, LCASE/UCASE, LIKE_ESCAPE | Small (1-2 days) | None | GRADED & READY |
| 3 | `add-missing-list-string-functions-v1.4.3` | LIST_APPEND, LIST_PREPEND, LIST_HAS, STRING_TO_ARRAY, REGEXP_FULL_MATCH | Small (1-2 days) | None | GRADED & READY |
| 4 | `add-missing-aggregates-round3-v1.4.3` | PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG | Small (1-2 days) | None | GRADED & READY |
| 5 | `add-missing-numeric-functions-v1.4.3` | SIGNBIT, WIDTH_BUCKET, BETA, SUM_IF/AVG_IF/MIN_IF/MAX_IF | Small-Medium (2-3 days) | None | GRADED & READY |
| 6 | `add-temporal-functions-round2-v1.4.3` | TIME_BUCKET, MAKE_TIMESTAMPTZ, ISODOW/ISOYEAR, DATEPART, EPOCH_NS, TIMEZONE | Medium (2-3 days) | None | GRADED & READY |
| 7 | `add-missing-system-views-v1.4.3` | duckdb_schemas(), duckdb_types() table functions | Medium (2-3 days) | Requires catalog.ListTypes() | GRADED & READY |
| 8 | `add-any-all-some-operators-v1.4.3` | ANY/ALL/SOME quantified comparison operators | Medium-Large (3-5 days) | None | GRADED & READY |

## Dependency Graph

```
Phase 1 — Pure executor additions (no dependencies, fully parallelizable)
├── add-current-datetime-functions-v1.4.3    [1-2 days]  ← parser + executor + binder
├── add-missing-string-functions-round4-v1.4.3 [1-2 days] ← executor only (+ matchLikeWithEscape helper)
├── add-missing-list-string-functions-v1.4.3 [1-2 days]  ← executor only
├── add-missing-aggregates-round3-v1.4.3     [1-2 days]  ← aggregate dispatch only
├── add-missing-numeric-functions-v1.4.3     [2-3 days]  ← executor + aggregate dispatch
└── add-temporal-functions-round2-v1.4.3     [2-3 days]  ← executor + temporal_functions.go

Phase 2 — Requires new infrastructure
├── add-missing-system-views-v1.4.3          [2-3 days]  ← needs catalog.ListTypes() added first
└── add-any-all-some-operators-v1.4.3        [3-5 days]  ← full stack: parser + AST + binder + executor
```

## Phase 1 — Executor-Level Additions (All Parallelizable)

All six Phase 1 proposals add functions to existing dispatch switches and require no new AST nodes or parser changes (except datetime functions which need bare keyword handling).

### add-current-datetime-functions-v1.4.3

**Scope**: NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, TODAY()

**Files touched**: `internal/parser/parser.go` (parseIdentExpr bare keyword cases), `internal/executor/expr.go` (evaluateFunctionCall dispatch), `internal/binder/utils.go` (add TODAY to type inference), `internal/engine/query_cache.go` (add TODAY to volatileFuncs)

**Why first**: Extremely common SQL functions — most applications expect `SELECT NOW()` and `SELECT CURRENT_DATE` to work. Parser change is minimal (3-line case addition to existing keyword switch at parser.go:5039-5083).

---

### add-missing-string-functions-round4-v1.4.3

**Scope**: OCTET_LENGTH, INITCAP, SOUNDEX, LCASE/UCASE aliases, LIKE_ESCAPE

**Files touched**: `internal/executor/expr.go` (5 new cases + matchLikeWithEscape helper), `internal/binder/utils.go` (type inference)

**Why first**: Standard string functions expected by SQL applications. LCASE/UCASE are just alias additions to existing UPPER (line 1233) and LOWER (line 1245) cases. LIKE_ESCAPE requires a new `matchLikeWithEscape()` helper extending the existing `matchLike()` at expr.go:4777.

---

### add-missing-list-string-functions-v1.4.3

**Scope**: LIST_APPEND, LIST_PREPEND, LIST_HAS, STRING_TO_ARRAY, REGEXP_FULL_MATCH

**Files touched**: `internal/executor/expr.go`, `internal/binder/utils.go`

**Why first**: Two new list functions (follow LIST_CONCAT pattern), two alias additions, one regex function. All in executor function dispatch. ~80 lines total.

---

### add-missing-aggregates-round3-v1.4.3

**Scope**: PRODUCT, MAD (Median Absolute Deviation), FAVG, FSUM, BITSTRING_AGG

**Files touched**: `internal/executor/physical_aggregate.go` (computeAggregate dispatch), `internal/executor/operator.go` (isAggregateFunc), `internal/binder/utils.go`

**Why first**: Five aggregates following established SUM/AVG/MEDIAN patterns at physical_aggregate.go:295. Uses existing helpers: `op.collectValues(expr, rows)` at line 1094, `computeMedian()` at aggregate_stats.go:135.

---

### add-missing-numeric-functions-v1.4.3

**Scope**: SIGNBIT, WIDTH_BUCKET, BETA scalar functions + SUM_IF, AVG_IF, MIN_IF, MAX_IF conditional aggregates

**Files touched**: `internal/executor/expr.go` (3 scalar cases), `internal/executor/physical_aggregate.go` (4 aggregate cases), `internal/executor/operator.go` (register in isAggregateFunc), `internal/binder/utils.go`

**Why Phase 1**: Scalar functions use Go stdlib (`math.Signbit`, `math.Lgamma`). Conditional aggregates follow COUNT_IF pattern at physical_aggregate.go:765. Uses `toBool()` at expr.go:4461 and `compareValues()` at expr.go:4575.

---

### add-temporal-functions-round2-v1.4.3

**Scope**: TIME_BUCKET, MAKE_TIMESTAMPTZ, ISODOW/ISOYEAR date parts, DATEPART alias, EPOCH_NS, TIMEZONE

**Files touched**: `internal/executor/temporal_functions.go` (new DatePart constants + extractPart cases + parseDatePart cases), `internal/executor/expr.go` (DATEPART alias at line 1959, new function cases), `internal/binder/utils.go`

**Why Phase 1**: Extends existing temporal infrastructure. New DatePart constants (DatePartISODow, DatePartISOYear, DatePartNanosecond) at temporal_functions.go:19-31. DATEPART is an alias addition to existing DATE_PART case. TIME_BUCKET needs `intervalToMicros()` helper. Uses `toInt64Value()` at expr.go:4487 (NOT toInt64).

---

## Phase 2 — Infrastructure-Dependent Features

### add-missing-system-views-v1.4.3

**Scope**: duckdb_schemas() and duckdb_types() table functions

**Files touched**: `internal/catalog/catalog.go` (add ListTypes() method), `internal/metadata/` (new GetSchemas/GetTypes functions), `internal/executor/system_functions.go` (executeDuckDBSchemas/executeDuckDBTypes), `internal/executor/table_function_csv.go` (register in dispatch at line 112), `internal/metadata/functions.go` (add to systemFunctionNames at line 54)

**Why Phase 2**: Requires adding `ListTypes()` to catalog.Catalog (currently has `ListSchemas()` at line 51 and `ListTables()` at line 244 but no ListTypes). Follows executeDuckDBTables() pattern at system_functions.go:30-53. GetSchemas/GetTypes take 3 params: `(cat *catalog.Catalog, stor *storage.Storage, dbName string)`. ListSchemas() returns `[]*Schema` — must call `.Name()` on each.

---

### add-any-all-some-operators-v1.4.3

**Scope**: `x = ANY (subquery)`, `x > ALL (subquery)`, `x = SOME (subquery)`

**Files touched**: `internal/parser/ast.go` (new QuantifiedComparisonExpr after InSubqueryExpr at line 924), `internal/parser/parser.go` (intercept ANY/ALL/SOME after comparison operators in parseIdentExpr at line 5035), `internal/binder/expressions.go` (new BoundQuantifiedComparison type), `internal/binder/bind_expr.go` (bindQuantifiedComparisonExpr following bindInSubqueryExpr at line 645), `internal/executor/expr.go` (evaluateQuantifiedComparison following evaluateInSubqueryExpr at line 3506)

**Why Phase 2**: Full-stack feature requiring new AST node, parser changes, binder type, and executor evaluation. Most complex remaining proposal. Follows InSubqueryExpr pattern closely. SOME normalizes to ANY. NULL semantics and vacuous truth (empty ALL = true) must be handled.

---

## Implementation Schedule

```
Day 1:      Start all Phase 1 proposals (fully parallelizable)
            ├── Datetime functions (1-2 days)       ← parser bare keywords + executor
            ├── String functions round 4 (1-2 days)  ← executor + matchLikeWithEscape
            ├── List/string functions (1-2 days)     ← executor only
            ├── Aggregates round 3 (1-2 days)        ← aggregate dispatch
            ├── Numeric functions (2-3 days)          ← executor + aggregates
            └── Temporal functions round 2 (2-3 days) ← temporal_functions + executor

Day 2:      Datetime, string, list/string, aggregates round 3 complete
            Start Phase 2 proposals
            ├── System views (2-3 days)              ← catalog.ListTypes() first
            └── ANY/ALL/SOME operators (3-5 days)    ← full stack

Day 3:      Numeric functions, temporal functions round 2 complete
Day 4-5:    System views complete
Day 5-7:    ANY/ALL/SOME operators complete
```

**Total estimated effort**: 15-23 person-days
**With full parallelization**: 5-7 days elapsed time
**Critical path**: ANY/ALL/SOME operators (3-5 days, starts Day 2)

## Completed Proposals (Already Implemented)

| Proposal | Status | Date |
|----------|--------|------|
| `add-struct-field-access-v1.4.3` | Implemented & Archived | 2026-03-21 |
| `add-reset-statement-v1.4.3` | Implemented | 2026-03-21 |
| `add-missing-conversion-functions-v1.4.3` | Implemented | 2026-03-21 |
| `add-standalone-aggregate-filter-v1.4.3` | Implemented | 2026-03-21 |
| `add-ordered-set-aggregates-v1.4.3` | Implemented | 2026-03-21 |
| `add-table-ddl-extensions-v1.4.3` | Implemented | 2026-03-21 |
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

## Remaining Gaps (This Timeline)

1. NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, TODAY() — datetime functions
2. OCTET_LENGTH, INITCAP, SOUNDEX, LCASE/UCASE, LIKE_ESCAPE — string functions
3. LIST_APPEND, LIST_PREPEND, LIST_HAS, STRING_TO_ARRAY, REGEXP_FULL_MATCH — list/string functions
4. PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG — aggregate functions
5. SIGNBIT, WIDTH_BUCKET, BETA, SUM_IF/AVG_IF/MIN_IF/MAX_IF — numeric functions + conditional aggregates
6. TIME_BUCKET, MAKE_TIMESTAMPTZ, ISODOW/ISOYEAR, DATEPART, EPOCH_NS, TIMEZONE — temporal functions
7. duckdb_schemas(), duckdb_types() — system views
8. ANY/ALL/SOME quantified comparison operators
