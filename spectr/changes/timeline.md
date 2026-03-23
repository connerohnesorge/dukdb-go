# DuckDB v1.4.3 Feature Parity — Implementation Timeline

**Last updated**: 2026-03-22

This document orders all change proposals by dependency, showing the recommended chronological implementation sequence.

---

## Completed Proposals (3 — implemented and archived)

| # | Change ID | Description | Archived |
|---|-----------|-------------|----------|
| ~~P3~~ | ~~`add-alter-column-defaults-v1.4.3`~~ | ALTER COLUMN SET/DROP DEFAULT, SET/DROP NOT NULL | 2026-03-22 |
| ~~P4~~ | ~~`add-group-by-all-v1.4.3`~~ | GROUP BY ALL syntax | 2026-03-22 |
| ~~P5~~ | ~~`add-insert-or-replace-ignore-v1.4.3`~~ | INSERT OR REPLACE/IGNORE syntax | 2026-03-22 |

---

## Active Proposals (6 remaining)

### Phase 0 — Quick Wins (no dependencies)

| # | Change ID | Description | Est. Effort |
|---|-----------|-------------|-------------|
| P1 | `add-missing-scalar-functions-round5-v1.4.3` | UUID(), SPLIT_PART(), SHA512(), E(), INF(), NAN(), LOG(x,base), MILLISECOND(), MICROSECOND() | 2-3 days |
| P2 | `add-aggregate-fixes-round4-v1.4.3` | Aggregate function registration fixes | 2-3 days |

### Phase 1+ — New Feature Proposals

| # | Change ID | Description | Est. Effort |
|---|-----------|-------------|-------------|
| N1 | `add-generated-columns-v1.4.3` | GENERATED ALWAYS AS (expr) STORED columns | 5-7 days |
| N2 | `add-summarize-execution-v1.4.3` | SUMMARIZE table/query execution (refactor existing) | 3-5 days |
| N3 | `add-attach-detach-execution-v1.4.3` | ATTACH/DETACH/USE/CREATE/DROP DATABASE execution | 7-10 days |
| N4 | `add-import-export-database-execution-v1.4.3` | EXPORT/IMPORT DATABASE execution | 5-7 days |

---

## Dependency Graph

```
Phase 0 (parallel — no dependencies)
  ├── P1: scalar functions round 5
  └── P2: aggregate fixes round 4 ─────────────────┐
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
  └── N4: import/export database
```

---

## Phase 0: Quick Wins (Week 1)

P1 and P2 are executor-level additions with no dependencies. They can be implemented **concurrently**.

| Proposal | What | Key Files |
|----------|------|-----------|
| P1 | UUID(), SPLIT_PART(), SHA512(), etc. | `executor/expr.go` |
| P2 | Aggregate registration fixes | `executor/physical_aggregate.go`, `operator.go` |

**Milestone**: All v1.4.3 function gaps closed.

---

## Phase 1: Generated Columns (Weeks 1-2, parallel with Phase 0)

| Task Group | What |
|------------|------|
| N1 tasks 1.1-1.4 | Parser: add GeneratedExpr/GeneratedKind to ColumnDefClause |
| N1 tasks 2.1-2.4 | Binder: validation (no DEFAULT+GENERATED, determinism check) |
| N1 tasks 3.1-3.3 | Catalog: add fields to ColumnDef in column.go |
| N1 tasks 4.1-4.5 | Executor: INSERT with generated column computation |
| N1 tasks 5.1-5.3 | Executor: UPDATE with generated column recomputation |
| N1 tasks 6-9 | SELECT, storage serialization, ALTER TABLE, E2E tests |

**Milestone**: `CREATE TABLE t (x INT, y INT GENERATED ALWAYS AS (x*2) STORED)` works end-to-end.

---

## Phase 2: SUMMARIZE Execution (Weeks 2-3)

Depends on P2 (aggregate fixes) for correct statistics functions.

| Task Group | What |
|------------|------|
| N2 task 0.1 | Remove `handleSummarize()` from conn.go |
| N2 tasks 1.1-1.2 | Binder: add SummarizeStmt binding |
| N2 tasks 2.1-2.3 | Planner: PhysicalSummarize plan node |
| N2 tasks 3.1-3.8 | Executor: statistics, type-aware comparison, Bessel's correction, percentiles |
| N2 tasks 4.1-4.4 | SUMMARIZE SELECT support |
| N2 tasks 5-7 | Parser fix, operator registration, integration tests |

**Milestone**: `SUMMARIZE table_name` returns correct 12-column statistics matching DuckDB format.

---

## Phase 3: Multi-Database Support (Weeks 3-5)

| Task Group | What |
|------------|------|
| N3 tasks 1.1-1.2 | Binder: Bound statement types |
| N3 tasks 2.1-3.3 | Planner: Logical + Physical plan nodes |
| N3 tasks 4.1-4.5 | Executor: ATTACH (OpenDatabase, file handling) |
| N3 tasks 5.1-6.4 | Executor: DETACH + USE |
| N3 tasks 7.1-8.4 | CREATE/DROP DATABASE + cross-database name resolution |
| N3 tasks 9-12 | Parser fix, operator reg, system functions, E2E tests |

**Milestone**: `ATTACH 'other.db' AS other; SELECT * FROM other.main.t1;` works.

---

## Phase 4: Database Backup & Migration (Weeks 5-6)

| Task Group | What |
|------------|------|
| N4 tasks 2.1-2.9 | Catalog SQL generation (ToCreateSQL, ListMacros, constraints) |
| N4 tasks 3.1-4.2 | Binder + Parser fixes |
| N4 tasks 5.1-5.8 | Executor: EXPORT DATABASE |
| N4 tasks 6.1-6.6 | Executor: IMPORT DATABASE |
| N4 tasks 7-9 | Operator registration, ParseMultiple, E2E tests |

**Milestone**: `EXPORT DATABASE '/backup'; IMPORT DATABASE '/backup';` roundtrips all data.

---

## Summary

| Metric | Value |
|--------|-------|
| Total proposals created | 9 |
| Completed & archived | 3 (ALTER COLUMN, GROUP BY ALL, INSERT OR) |
| Remaining active | 6 |
| Estimated remaining effort | ~6 weeks |
| Critical path | P2 → N2 (SUMMARIZE) and N1 → N3 → N4 (ATTACH → EXPORT) |
