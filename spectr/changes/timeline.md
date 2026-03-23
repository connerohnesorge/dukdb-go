# DuckDB v1.4.3 Feature Parity — Implementation Timeline

**Last updated**: 2026-03-23

---

## Completed Proposals (7 of 9 — implemented and archived)

| Change ID | Archived |
|-----------|----------|
| `add-alter-column-defaults-v1.4.3` | 2026-03-22 |
| `add-group-by-all-v1.4.3` | 2026-03-22 |
| `add-insert-or-replace-ignore-v1.4.3` | 2026-03-22 |
| `add-aggregate-fixes-round4-v1.4.3` | 2026-03-23 |
| `add-missing-scalar-functions-round5-v1.4.3` | 2026-03-23 |
| `add-summarize-execution-v1.4.3` | 2026-03-23 |
| `add-import-export-database-execution-v1.4.3` | 2026-03-23 |

---

## Active Proposals (2 remaining)

| # | Change ID | Description | Est. Effort | Phase |
|---|-----------|-------------|-------------|-------|
| N1 | `add-generated-columns-v1.4.3` | GENERATED ALWAYS AS (expr) STORED columns | 5-7 days | 1 |
| N3 | `add-attach-detach-execution-v1.4.3` | ATTACH/DETACH/USE/CREATE/DROP DATABASE execution | 7-10 days | 2 |

---

## Dependency Graph

```
Phase 1 (no dependencies)
  └── N1: generated columns

Phase 2 (after Phase 1 recommended)
  └── N3: attach/detach execution
```

---

## Phase 1: Generated Columns

| Task Group | What |
|------------|------|
| tasks 1.1-1.4 | Parser: GeneratedExpr/GeneratedKind in ColumnDefClause |
| tasks 2.1-2.4 | Binder: validation (no DEFAULT+GENERATED, determinism) |
| tasks 3.1-3.3 | Catalog: fields on ColumnDef |
| tasks 4.1-5.3 | Executor: INSERT/UPDATE with generated column computation |
| tasks 6-9 | Storage serialization, ALTER TABLE, E2E tests |

## Phase 2: Multi-Database Support

| Task Group | What |
|------------|------|
| tasks 1.1-3.3 | Binder + Planner (Logical + Physical plan nodes) |
| tasks 4.1-6.4 | Executor: ATTACH, DETACH, USE |
| tasks 7.1-8.4 | CREATE/DROP DATABASE + cross-database name resolution |
| tasks 9-12 | Parser fix, operator reg, system functions, E2E tests |

---

## Summary

| Metric | Value |
|--------|-------|
| Total proposals created | 9 |
| Completed & archived | 7 |
| Remaining | 2 |
| Estimated remaining effort | ~2-3 weeks |
