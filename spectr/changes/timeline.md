# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document orders ALL active change proposals (existing + newly created) by dependency, showing the recommended chronological implementation sequence.

---

## Active Proposals (9 total)

### Pre-Existing (5 proposals)

| # | Change ID | Description | Est. Effort |
|---|-----------|-------------|-------------|
| P1 | `add-missing-scalar-functions-round5-v1.4.3` | UUID(), SPLIT_PART(), SHA512(), E(), INF(), NAN(), LOG(x,base), MILLISECOND(), MICROSECOND() | 2-3 days |
| P2 | `add-aggregate-fixes-round4-v1.4.3` | Aggregate function corrections and completions | 2-3 days |
| P3 | `add-alter-column-defaults-v1.4.3` | ALTER COLUMN SET/DROP DEFAULT, SET/DROP NOT NULL | 2-3 days |
| P4 | `add-group-by-all-v1.4.3` | GROUP BY ALL syntax | 1-2 days |
| P5 | `add-insert-or-replace-ignore-v1.4.3` | INSERT OR REPLACE/IGNORE syntax | 2-3 days |

### Newly Created (4 proposals)

| # | Change ID | Description | Est. Effort |
|---|-----------|-------------|-------------|
| N1 | `add-generated-columns-v1.4.3` | GENERATED ALWAYS AS (expr) STORED columns | 5-7 days |
| N2 | `add-summarize-execution-v1.4.3` | SUMMARIZE table/query execution (refactor existing) | 3-5 days |
| N3 | `add-attach-detach-execution-v1.4.3` | ATTACH/DETACH/USE/CREATE/DROP DATABASE execution | 7-10 days |
| N4 | `add-import-export-database-execution-v1.4.3` | EXPORT/IMPORT DATABASE execution | 5-7 days |

---

## Dependency Graph

```
Phase 0 (all parallel — no dependencies)
  ├── P1: scalar functions round 5
  ├── P2: aggregate fixes round 4 ─────────────────┐
  ├── P3: alter column defaults                     │
  ├── P4: group by all                              │
  └── P5: insert or replace/ignore                  │
                                                    │
Phase 1 (parallel with Phase 0)                     │
  └── N1: generated columns                         │
                                                    │
Phase 2 (after P2)                                  │
  └── N2: summarize execution ◄─────────────────────┘
              (needs correct aggregate functions)

Phase 3 (after Phase 1 recommended)
  └── N3: attach/detach execution
              │
Phase 4 (after Phase 3)
  └── N4: import/export database ◄──────────────────
              (needs catalog enumeration, benefits from multi-db)
```

---

## Phase 0: Quick Wins (Weeks 1-2)

All 5 pre-existing proposals are executor-level additions with no inter-dependencies. They can all be implemented **concurrently**.

| Week | Proposal | What | Key Files |
|------|----------|------|-----------|
| 1 | P1 | UUID(), SPLIT_PART(), SHA512(), etc. | `executor/expr.go` |
| 1 | P2 | Aggregate fixes | `executor/physical_aggregate.go` |
| 1 | P3 | ALTER COLUMN defaults | `executor/ddl.go` |
| 1 | P4 | GROUP BY ALL | `parser/parser.go`, `executor/` |
| 1-2 | P5 | INSERT OR REPLACE/IGNORE | `executor/operator.go` |

**Milestone**: All v1.4.3 function/syntax gaps closed.

---

## Phase 1: Generated Columns (Weeks 2-3, parallel with Phase 0)

| Week | Task | What |
|------|------|------|
| 2 | N1 tasks 1.1-1.4 | Parser: add GeneratedExpr/GeneratedKind to ColumnDefClause |
| 2 | N1 tasks 2.1-2.4 | Binder: validation (no DEFAULT+GENERATED, determinism check) |
| 3 | N1 tasks 3.1-3.3 | Catalog: add fields to ColumnDef |
| 3 | N1 tasks 4.1-4.5 | Executor: INSERT with generated column computation |
| 3 | N1 tasks 5.1-5.3 | Executor: UPDATE with generated column recomputation |
| 3 | N1 tasks 6-9 | SELECT, storage serialization, ALTER TABLE, E2E tests |

**Milestone**: `CREATE TABLE t (x INT, y INT GENERATED ALWAYS AS (x*2) STORED)` works end-to-end.

---

## Phase 2: SUMMARIZE Execution (Weeks 3-4)

Depends on P2 (aggregate fixes) for correct statistics functions.

| Week | Task | What |
|------|------|------|
| 3 | N2 task 0.1 | Remove `handleSummarize()` from conn.go |
| 3 | N2 tasks 1.1-1.2 | Binder: add SummarizeStmt binding |
| 3 | N2 tasks 2.1-2.3 | Planner: PhysicalSummarize plan node |
| 4 | N2 tasks 3.1-3.8 | Executor: statistics accumulation, type-aware comparison, Bessel's correction, percentiles |
| 4 | N2 tasks 4.1-4.4 | SUMMARIZE SELECT support |
| 4 | N2 tasks 5-7 | Parser fix, operator registration, integration tests |

**Milestone**: `SUMMARIZE table_name` returns correct 12-column statistics matching DuckDB format.

---

## Phase 3: Multi-Database Support (Weeks 4-6)

Most complex proposal. Benefits from stable DDL pipeline after Phase 1.

| Week | Task | What |
|------|------|------|
| 4 | N3 tasks 1.1-1.2 | Binder: Bound statement types |
| 4 | N3 tasks 2.1-2.2 | Planner: Logical plan nodes |
| 5 | N3 tasks 3.1-3.3 | Planner: Physical plan nodes |
| 5 | N3 tasks 4.1-4.5 | Executor: ATTACH (OpenDatabase, file handling) |
| 5 | N3 tasks 5.1-5.5 | Executor: DETACH |
| 5 | N3 tasks 6.1-6.4 | Executor: USE |
| 6 | N3 tasks 7.1-7.4 | Executor: CREATE/DROP DATABASE |
| 6 | N3 tasks 8.1-8.4 | Cross-database name resolution |
| 6 | N3 tasks 9-12 | Parser fix, operator reg, system functions, E2E tests |

**Milestone**: `ATTACH 'other.db' AS other; SELECT * FROM other.main.t1;` works.

---

## Phase 4: Database Backup & Migration (Weeks 6-8)

Depends on Phase 3 for catalog enumeration and multi-database context.

| Week | Task | What |
|------|------|------|
| 6 | N4 tasks 2.1-2.9 | Catalog SQL generation (ToCreateSQL, ListMacros, constraints) |
| 7 | N4 tasks 3.1-3.3 | Binder: EXPORT/IMPORT binding |
| 7 | N4 tasks 4.1-4.2 | Parser fixes (ImportDatabaseStmt.Type(), IMPORT keyword) |
| 7 | N4 tasks 5.1-5.8 | Executor: EXPORT DATABASE |
| 7 | N4 tasks 6.1-6.6 | Executor: IMPORT DATABASE |
| 8 | N4 tasks 7-9 | Operator registration, ParseMultiple, E2E tests |

**Milestone**: `EXPORT DATABASE '/backup'; IMPORT DATABASE '/backup';` roundtrips all tables, views, sequences, macros.

---

## Summary

| Metric | Value |
|--------|-------|
| Total proposals | 9 (5 existing + 4 new) |
| Total estimated tasks | ~170 |
| Phases | 5 (0-4) |
| Estimated total elapsed time | 8 weeks |
| Maximum parallelism | 6 proposals in Phases 0+1 |
| Critical path | Phase 0 → Phase 3 → Phase 4 (ATTACH → EXPORT) |

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| ATTACH cross-database transactions | HIGH | Single-database-per-statement; defer cross-db transactions |
| Generated column expression caching | LOW | Cache parsed expressions per table |
| SUMMARIZE percentile accuracy | LOW | Reservoir sampling 10K gives <1% error |
| EXPORT large tables | MEDIUM | Reuse COPY TO; parallel export is future work |
| Binder/planner pipeline complexity | MEDIUM | Follow existing DDL patterns exactly |
