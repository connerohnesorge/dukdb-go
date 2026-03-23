# DuckDB v1.4.3 Feature Parity — Implementation Timeline

**Last updated**: 2026-03-23

---

## Completed Proposals (6 — implemented and archived)

| # | Change ID | Archived |
|---|-----------|----------|
| ~~P3~~ | ~~`add-alter-column-defaults-v1.4.3`~~ | 2026-03-22 |
| ~~P4~~ | ~~`add-group-by-all-v1.4.3`~~ | 2026-03-22 |
| ~~P5~~ | ~~`add-insert-or-replace-ignore-v1.4.3`~~ | 2026-03-22 |
| ~~P2~~ | ~~`add-aggregate-fixes-round4-v1.4.3`~~ | 2026-03-23 |
| ~~P1~~ | ~~`add-missing-scalar-functions-round5-v1.4.3`~~ | 2026-03-23 |
| ~~N2~~ | ~~`add-summarize-execution-v1.4.3`~~ | 2026-03-23 |

---

## Active Proposals (3 remaining)

| # | Change ID | Description | Est. Effort | Phase |
|---|-----------|-------------|-------------|-------|
| N1 | `add-generated-columns-v1.4.3` | GENERATED ALWAYS AS (expr) STORED columns | 5-7 days | 1 |
| N3 | `add-attach-detach-execution-v1.4.3` | ATTACH/DETACH/USE/CREATE/DROP DATABASE execution | 7-10 days | 2 |
| N4 | `add-import-export-database-execution-v1.4.3` | EXPORT/IMPORT DATABASE execution | 5-7 days | 3 |

---

## Dependency Graph

```
Phase 1 (no dependencies)
  └── N1: generated columns

Phase 2 (after Phase 1 recommended)
  └── N3: attach/detach execution

Phase 3 (after Phase 2)
  └── N4: import/export database
```

---

## Phase 1: Generated Columns (Week 1)

| Task Group | What |
|------------|------|
| N1 tasks 1.1-1.4 | Parser: add GeneratedExpr/GeneratedKind to ColumnDefClause |
| N1 tasks 2.1-2.4 | Binder: validation (no DEFAULT+GENERATED, determinism check) |
| N1 tasks 3.1-3.3 | Catalog: add fields to ColumnDef in column.go |
| N1 tasks 4.1-4.5 | Executor: INSERT with generated column computation |
| N1 tasks 5.1-5.3 | Executor: UPDATE with generated column recomputation |
| N1 tasks 6-9 | SELECT, storage serialization, ALTER TABLE, E2E tests |

**Milestone**: `CREATE TABLE t (x INT, y INT GENERATED ALWAYS AS (x*2) STORED)` works.

---

## Phase 2: Multi-Database Support (Weeks 2-3)

| Task Group | What |
|------------|------|
| N3 tasks 1.1-1.2 | Binder: Bound statement types |
| N3 tasks 2.1-3.3 | Planner: Logical + Physical plan nodes |
| N3 tasks 4.1-6.4 | Executor: ATTACH, DETACH, USE |
| N3 tasks 7.1-8.4 | CREATE/DROP DATABASE + cross-database name resolution |
| N3 tasks 9-12 | Parser fix, operator reg, system functions, E2E tests |

**Milestone**: `ATTACH 'other.db' AS other; SELECT * FROM other.main.t1;` works.

---

## Phase 3: Database Backup & Migration (Weeks 3-4)

| Task Group | What |
|------------|------|
| N4 tasks 2.1-2.9 | Catalog SQL generation (ToCreateSQL, ListMacros, constraints) |
| N4 tasks 3.1-4.2 | Binder + Parser fixes |
| N4 tasks 5.1-6.6 | Executor: EXPORT + IMPORT DATABASE |
| N4 tasks 7-9 | Operator registration, ParseMultiple, E2E tests |

**Milestone**: `EXPORT DATABASE '/backup'; IMPORT DATABASE '/backup';` roundtrips all data.

---

## Summary

| Metric | Value |
|--------|-------|
| Total proposals created | 9 |
| Completed & archived | 6 |
| Remaining active | 3 |
| Estimated remaining effort | ~4 weeks |
| Critical path | N1 → N3 → N4 |
