# DuckDB v1.4.3 Feature Parity — Implementation Timeline

This document describes the chronological order in which all remaining change proposals should be implemented to achieve full DuckDB v1.4.3 compatibility. Proposals are ordered by dependency: each phase's prerequisites are satisfied by earlier phases.

## Dependency Graph

```
Phase 1 (no dependencies — can be parallelized)
├── add-try-cast-safe-casting
├── add-similar-to-enum-ddl
├── add-grouping-sets-execution
├── add-union-by-name               ★ NEW
├── add-generate-series-range        ★ NEW
└── add-sql-prepare-execute          ★ NEW

Phase 2 (no dependencies — can be parallelized with Phase 1)
├── add-natural-asof-positional-joins
├── add-create-macro
├── add-upsert-on-conflict           ★ NEW
└── add-table-constraints            ★ NEW

Phase 3 (depends on Phase 1: TRY_CAST, ENUM types used by later features)
├── add-lambda-functions  (depends on: list type maturity from Phase 1)
└── add-icu-collation     (depends on: type system stability from Phase 1)

Phase 4 (depends on Phase 2: extension framework needed for cloud/FTS)
└── add-extension-loading

Phase 5 (depends on Phase 4: extensions register via framework)
├── add-s3-cloud-storage  (depends on: extension-loading, secret system)
├── add-full-text-search  (depends on: extension-loading for FTS extension)
└── add-export-import-database  ★ NEW (depends on: catalog DDL generation)

Phase 6 (depends on Phase 5: cloud storage needed for remote ATTACH)
└── add-attach-detach-database  (depends on: cloud storage for remote DBs)
```

## Phase 1 — Core SQL Gaps (No Dependencies)

These proposals fix fundamental SQL gaps with zero cross-dependencies. They can all be implemented in parallel.

| Proposal | Estimated Effort | Key Deliverables |
|----------|-----------------|------------------|
| `add-try-cast-safe-casting` | Small (2-3 days) | TRY_CAST returns NULL on failure; `::` operator |
| `add-similar-to-enum-ddl` | Medium (4-5 days) | SIMILAR TO operator; CREATE TYPE AS ENUM DDL |
| `add-grouping-sets-execution` | Small (2-3 days) | Fix GROUPING() aliases; fix mixed GROUP BY; complete execution |
| `add-union-by-name` ★ | Small (3-4 days) | UNION [ALL] BY NAME with column matching and NULL padding |
| `add-generate-series-range` ★ | Small (3-4 days) | generate_series() and range() table functions for integers and dates |
| `add-sql-prepare-execute` ★ | Medium (4-5 days) | PREPARE/EXECUTE/DEALLOCATE SQL statements with plan caching |

**Why first**: These are self-contained SQL features. TRY_CAST and ENUM types are referenced by later proposals. GROUPING SETS is nearly complete (just executor bugs to fix). UNION BY NAME, generate_series, and PREPARE/EXECUTE are independent features with no cross-dependencies.

---

## Phase 2 — Advanced SQL Features (No Dependencies)

These also have no strict dependencies but are more complex. Can run in parallel with Phase 1.

| Proposal | Estimated Effort | Key Deliverables |
|----------|-----------------|------------------|
| `add-natural-asof-positional-joins` | Large (7-10 days) | NATURAL JOIN, ASOF JOIN, POSITIONAL JOIN, USING clause |
| `add-create-macro` | Medium (5-7 days) | CREATE MACRO, TABLE MACRO, macro expansion in binder |
| `add-upsert-on-conflict` ★ | Medium (5-7 days) | INSERT ... ON CONFLICT DO NOTHING/UPDATE, EXCLUDED pseudo-table |
| `add-table-constraints` ★ | Large (7-10 days) | UNIQUE, CHECK, FOREIGN KEY constraints with enforcement |

**Why here**: JOIN types and MACROs are frequently used DuckDB features. UPSERT depends on UNIQUE constraint infrastructure (from add-table-constraints or existing PK logic). Table constraints are foundational for data integrity. These can all be parallelized.

**Note**: `add-upsert-on-conflict` and `add-table-constraints` are complementary — UPSERT's conflict detection benefits from UNIQUE constraints. Implement constraints first or in parallel.

---

## Phase 3 — Lambda Functions & Collation (Depends on Phase 1)

| Proposal | Estimated Effort | Dependencies | Key Deliverables |
|----------|-----------------|--------------|------------------|
| `add-lambda-functions` | Large (8-10 days) | Phase 1 (list type, TRY_CAST) | list_transform, list_filter, list_reduce, list_sort |
| `add-icu-collation` | Medium (5-7 days) | Phase 1 (type system) | COLLATE clause, locale-aware sorting, golang.org/x/text |

**Why here**: Lambda functions need the list type system to be stable (ENUM types, TRY_CAST for safe element casting). ICU collation touches the type modifier system which ENUM DDL also modifies.

---

## Phase 4 — Extension Loading Framework (Depends on Phase 2)

| Proposal | Estimated Effort | Dependencies | Key Deliverables |
|----------|-----------------|--------------|------------------|
| `add-extension-loading` | Medium (5-7 days) | Phase 2 (macros use similar catalog patterns) | INSTALL/LOAD, ExtensionRegistry, autoload, duckdb_extensions() |

**Why here**: The extension framework must be in place before cloud storage and FTS can be registered as extensions. It also supersedes the `extension-system-v1.4.3` proposal (which should be archived).

---

## Phase 5 — Cloud Storage, FTS & Database Export (Depends on Phase 4)

| Proposal | Estimated Effort | Dependencies | Key Deliverables |
|----------|-----------------|--------------|------------------|
| `add-s3-cloud-storage` | Large (10-14 days) | Phase 4 (extension framework) | S3/GCS/Azure read/write, httpfs equivalent |
| `add-full-text-search` | Large (10-14 days) | Phase 4 (extension framework) | Inverted index, BM25, PRAGMA create_fts_index |
| `add-export-import-database` ★ | Medium (5-7 days) | Phase 2 (catalog DDL gen) | EXPORT DATABASE, IMPORT DATABASE, schema.sql + data files |

**Why here**: S3 and FTS register as extensions. EXPORT/IMPORT DATABASE depends on catalog DDL generation and COPY TO/FROM infrastructure (already available), plus constraint metadata from Phase 2 for complete DDL output.

---

## Phase 6 — Multi-Database Support (Depends on Phase 5)

| Proposal | Estimated Effort | Dependencies | Key Deliverables |
|----------|-----------------|--------------|------------------|
| `add-attach-detach-database` | Large (10-14 days) | Phase 5 (S3 for remote attach) | ATTACH/DETACH, USE, 3-part names, DatabaseManager |

**Why last**: ATTACH/DETACH is the capstone feature. It depends on:
- Extension framework (to load extensions for attached DB types)
- Cloud storage (to attach remote databases via S3/HTTP)
- All prior SQL features (attached databases must support full SQL)

---

## Summary Timeline

```
Week 1-2:   Phase 1 + Phase 2 (parallel — 10 proposals)
             ├── TRY_CAST (2-3 days)
             ├── SIMILAR TO + ENUM DDL (4-5 days)
             ├── GROUPING SETS (2-3 days)
             ├── UNION BY NAME (3-4 days)              ★ NEW
             ├── generate_series/range (3-4 days)       ★ NEW
             ├── PREPARE/EXECUTE (4-5 days)             ★ NEW
             ├── JOIN types (7-10 days)
             ├── CREATE MACRO (5-7 days)
             ├── UPSERT ON CONFLICT (5-7 days)          ★ NEW
             └── Table Constraints (7-10 days)           ★ NEW

Week 3-4:   Phase 3 (parallel)
             ├── Lambda Functions (8-10 days)
             └── ICU Collation (5-7 days)

Week 4-5:   Phase 4
             └── Extension Loading (5-7 days)

Week 5-8:   Phase 5 (parallel)
             ├── S3/Cloud Storage (10-14 days)
             ├── Full-Text Search (10-14 days)
             └── EXPORT/IMPORT Database (5-7 days)      ★ NEW

Week 8-10:  Phase 6
             └── ATTACH/DETACH Database (10-14 days)
```

**Total estimated effort**: 10-14 weeks (with parallelization within phases)
**New proposals add**: ~28-37 days of additional work, distributed across existing phases

## All Proposals — Complete Inventory

### New Proposals (★ created in this session)

| Proposal | Phase | Effort | Status |
|----------|-------|--------|--------|
| `add-upsert-on-conflict` | 2 | Medium (5-7 days) | Validated, graded, fixed |
| `add-export-import-database` | 5 | Medium (5-7 days) | Validated, graded, fixed |
| `add-sql-prepare-execute` | 1 | Medium (4-5 days) | Validated, graded, fixed |
| `add-generate-series-range` | 1 | Small (3-4 days) | Validated, graded, fixed |
| `add-table-constraints` | 2 | Large (7-10 days) | Validated, graded, fixed |
| `add-union-by-name` | 1 | Small (3-4 days) | Validated, graded, fixed |

### Existing Proposals (from prior sessions)

| Proposal | Phase | Effort | Relationship |
|----------|-------|--------|-------------|
| `add-try-cast-safe-casting` | 1 | Small | Active |
| `add-similar-to-enum-ddl` | 1 | Medium | Active |
| `add-grouping-sets-execution` | 1 | Small | Active |
| `add-natural-asof-positional-joins` | 2 | Large | Active |
| `add-create-macro` | 2 | Medium | Active |
| `add-lambda-functions` | 3 | Large | Active |
| `add-icu-collation` | 3 | Medium | Active |
| `add-extension-loading` | 4 | Medium | Active |
| `add-s3-cloud-storage` | 5 | Large | Active |
| `add-full-text-search` | 5 | Large | Active |
| `add-attach-detach-database` | 6 | Large | Active |

### Existing v1.4.3 Proposals (complementary, can run in parallel)

| Proposal | Status | Relationship |
|----------|--------|-------------|
| `duckdb-file-format-v1.4.3` | Active | Independent — DuckDB file format read/write |
| `complex-data-types-v1.4.3` | Active | Complementary — nested type improvements |
| `query-optimizations-v1.4.3` | Active | Independent — optimizer enhancements |
| `extension-system-v1.4.3` | Active | **Superseded by** `add-extension-loading` — should be archived |
| `window-functions-v1.4.3` | Active | Independent — window function enhancements |
| `recursive-cte-lateral-v1.4.3` | Active | Independent — CTE/lateral improvements |
| `system-functions-metadata-v1.4.3` | Active | Complementary — metadata function additions |

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Lambda function parser disambiguation (-> vs JSON) | High | High | Thorough parser tests; lookahead-based approach documented |
| ASOF JOIN performance on large datasets | Medium | Medium | Start with sort-merge; optimize based on benchmarks |
| Cloud storage credential management complexity | Medium | High | Leverage existing secret system; test with localstack |
| Extension framework function registration complexity | Medium | Medium | Bridge existing hard-coded functions incrementally |
| ATTACH/DETACH cross-database transaction complexity | High | High | Limit to read-only cross-DB transactions initially |
| FK constraint enforcement performance on bulk INSERT | Medium | Medium | Only NO ACTION/RESTRICT supported; simple parent lookup |
| UPSERT hash set memory for large tables | Medium | Low | Reuse existing pkKeys infrastructure; streaming fallback |
| EXPORT DATABASE view dependency ordering | Low | Medium | Topological sort with cycle detection |
| PREPARE plan staleness after DDL | Low | Low | Document limitation; DEALLOCATE + re-PREPARE as workaround |
