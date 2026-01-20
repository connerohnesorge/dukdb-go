# System Functions & Metadata - Quick Start Guide

## TL;DR - Three Proposals, One Mission

**Goal**: Implement DuckDB v1.4.3-compatible system functions and metadata tables in dukdb-go

**Result**: Complete database introspection via SQL queries (three phases)

**Status**: ✅ All proposals created and validated

---

## The Three Proposals

### 1. Phase 1: System Functions Infrastructure 🏗️
**Folder**: `add-system-functions`
**Status**: ✅ Ready for implementation
**Scope**: Foundational infrastructure + 15 system functions + 9 PRAGMA statements
**Tasks**: 54
**Dependencies**: None (start here)

**What it does**:
- Creates `internal/metadata/` package with catalog helpers
- Implements `duckdb_settings()`, `duckdb_functions()`, `duckdb_tables()`, etc.
- Implements PRAGMA statements for backward compatibility
- Metadata accessible via SQL: `SELECT * FROM duckdb_settings()`

**Key files to implement**:
- `internal/metadata/` (10 new Go files)
- `internal/executor/table_function_*.go` (extend existing)
- `internal/parser/` (PRAGMA support)

---

### 2. Phase 2: SQL Standard information_schema 📋
**Folder**: `add-information-schema`
**Status**: ✅ Ready (after Phase 1)
**Scope**: 12 SQL standard metadata views
**Tasks**: 26
**Dependencies**: Must complete Phase 1 first

**What it does**:
- Creates `information_schema` schema (SQL standard)
- Implements `information_schema.tables`, `columns`, `views`, etc.
- Tool compatibility (DBeaver, Power BI, Looker all use information_schema)
- Metadata accessible via SQL: `SELECT * FROM information_schema.tables`

**Key files to implement**:
- `internal/postgres/catalog/information_schema.go` (new)
- Virtual table registry extensions

---

### 3. Phase 3: PostgreSQL pg_catalog + OID System 🐘
**Folder**: `add-postgresql-catalog`
**Status**: ✅ Ready (after Phases 1 & 2)
**Scope**: 13+ PostgreSQL catalog views + OID system
**Tasks**: 67
**Dependencies**: Must complete Phases 1 & 2 first

**What it does**:
- Creates `pg_catalog` schema (PostgreSQL standard)
- Implements `pg_catalog.pg_class`, `pg_namespace`, `pg_attribute`, etc.
- Adds OID (Object Identifier) system for DBeaver/pgAdmin compatibility
- Tool compatibility (DBeaver, pgAdmin, psql all use pg_catalog)
- Metadata accessible via SQL: `SELECT * FROM pg_catalog.pg_class`

**Key files to implement**:
- `internal/postgres/catalog/pg_catalog.go` (extend existing)
- OID generation in `internal/metadata/platform.go`
- Virtual table registry extensions

---

## How They Connect

```
Phase 1: System Functions
    ↓
    Creates: internal/metadata/ package
    Provides: 15 system functions + 9 PRAGMA
    Blocks: Phases 2 & 3 (they need this infrastructure)

Phase 2: Information Schema
    ↓
    Depends On: Phase 1 infrastructure
    Creates: information_schema views (SQL standard)
    Reuses: internal/metadata/ helpers

Phase 3: PostgreSQL Catalog
    ↓
    Depends On: Phases 1 & 2
    Creates: pg_catalog views + OID system (PostgreSQL standard)
    Reuses: internal/metadata/ helpers
```

---

## Quick Feature Table

| Capability | Phase 1 | Phase 2 | Phase 3 |
|-----------|---------|---------|---------|
| System Functions | ✅ (15) | - | - |
| PRAGMA Statements | ✅ (9) | - | - |
| information_schema Views | - | ✅ (12) | - |
| pg_catalog Views | - | - | ✅ (13+) |
| OID System | - | - | ✅ |
| Metadata Infrastructure | ✅ | (reused) | (reused) |
| DuckDB Compatibility | ✅ | ✅ | ✅ |
| SQL Standard Compliance | ✅ | ✅ | - |
| PostgreSQL Compatibility | - | - | ✅ |
| Tool Support (DBeaver, etc.) | ✅ | ✅ | ✅ |

---

## Implementation Checklist

### Pre-Implementation
- [ ] Read `add-system-functions/proposal.md`
- [ ] Read `add-information-schema/proposal.md`
- [ ] Read `add-postgresql-catalog/proposal.md`
- [ ] Review this quick start guide

### Phase 1 (Start Here)
- [ ] Accept proposal: `spectr accept add-system-functions`
- [ ] Create `internal/metadata/` package
- [ ] Implement 10 metadata helper files
- [ ] Implement 15 system functions in executor
- [ ] Implement 9 PRAGMA statements
- [ ] Run all tests (Phase 1)
- [ ] Move to Phase 2

### Phase 2 (After Phase 1 Complete)
- [ ] Accept proposal: `spectr accept add-information-schema`
- [ ] Reuse `internal/metadata/` from Phase 1
- [ ] Create `internal/postgres/catalog/information_schema.go`
- [ ] Implement 12 information_schema views
- [ ] Run all tests (Phase 2)
- [ ] Move to Phase 3

### Phase 3 (After Phases 1 & 2 Complete)
- [ ] Accept proposal: `spectr accept add-postgresql-catalog`
- [ ] Implement OID system
- [ ] Extend `internal/postgres/catalog/pg_catalog.go`
- [ ] Implement 13+ pg_catalog views
- [ ] Run all tests (Phase 3)
- [ ] Done! All metadata infrastructure complete

---

## Key Files Reference

### Phase 1 Files
```
New:
  internal/metadata/
    ├── tables.go           (table metadata extraction)
    ├── columns.go          (column metadata extraction)
    ├── constraints.go      (constraint metadata extraction)
    ├── views.go            (view metadata extraction)
    ├── indexes.go          (index metadata extraction)
    ├── sequences.go        (sequence metadata extraction)
    ├── functions.go        (function metadata extraction)
    ├── settings.go         (settings extraction)
    ├── types.go            (type system helpers)
    └── platform.go         (version/platform info + OID generation)

Modified:
  internal/executor/table_function_csv.go  (add system function dispatch)
  internal/executor/operator.go            (extend table function resolution)
  internal/parser/                         (add PRAGMA parsing if needed)
```

### Phase 2 Files
```
New:
  internal/postgres/catalog/information_schema.go
    (implements 12 information_schema views)

Modified:
  (Virtual table registry - already extensible)
```

### Phase 3 Files
```
Modified:
  internal/postgres/catalog/pg_catalog.go  (extend existing)
  internal/metadata/platform.go            (extend with OID system)

(All other infrastructure reused from Phases 1 & 2)
```

---

## Testing Strategy

### Unit Tests (per phase)
- Metadata helper functions
- Individual system functions/views
- OID generation and collision detection

### Integration Tests
- End-to-end system function queries
- Complex JOINs across views
- Tool introspection (DBeaver, pgAdmin)

### Compatibility Tests
- DuckDB v1.4.3 reference validation
- PostgreSQL v14 behavior matching
- SQL Standard compliance

---

## Success Criteria

### Phase 1 ✅
- All 15 system functions execute correctly
- All PRAGMA statements work
- Output matches DuckDB v1.4.3
- Edge cases handled (empty schemas, etc.)

### Phase 2 ✅
- All 12 information_schema views queryable
- Correct column schemas per SQL standard
- Tools recognize information_schema
- Complex JOINs work

### Phase 3 ✅
- All 13+ pg_catalog views queryable
- OID system enables correct JOINs
- DBeaver/pgAdmin recognize database structure
- PostgreSQL compatibility maintained

---

## Common Questions

**Q: Can I implement phases out of order?**
A: No. Phase 1 is foundational - Phases 2 & 3 depend on its infrastructure.

**Q: How many tasks total?**
A: 147 tasks (54 + 26 + 67). Best to implement sequentially.

**Q: Do I need to modify existing code?**
A: Minimally. Mostly new files in `internal/metadata/` and extensions to existing virtual table machinery.

**Q: Will this break existing functionality?**
A: No. All changes are additive. Existing API unchanged.

**Q: How much code needs to be written?**
A: Rough estimate: 5,000-7,000 lines of new Go code total (mostly repetitive query builders and virtual table implementations).

**Q: When should I test?**
A: After each phase completes. Each phase has integration tests with reference DuckDB.

---

## Documentation References

**For Implementation Details**:
- Phase 1: `spectr/changes/add-system-functions/specs/system-functions/spec.md`
- Phase 1: `spectr/changes/add-system-functions/specs/metadata-infrastructure/spec.md`
- Phase 2: `spectr/changes/add-information-schema/specs/information-schema/spec.md`
- Phase 3: `spectr/changes/add-postgresql-catalog/specs/postgresql-catalog/spec.md`

**For Task Breakdown**:
- Phase 1: `spectr/changes/add-system-functions/tasks.md`
- Phase 2: `spectr/changes/add-information-schema/tasks.md`
- Phase 3: `spectr/changes/add-postgresql-catalog/tasks.md`

**For Overview**:
- `spectr/changes/SYSTEM_FUNCTIONS_SUMMARY.md` (comprehensive guide)
- This file: `spectr/changes/QUICK_START.md` (quick reference)

---

## Next Steps

1. **Read proposals** (30 minutes)
   - Open `add-system-functions/proposal.md`
   - Open `add-information-schema/proposal.md`
   - Open `add-postgresql-catalog/proposal.md`

2. **Review specifications** (45 minutes)
   - Read the spec.md files for each phase
   - Understand requirements and scenarios

3. **Plan implementation** (optional)
   - Read tasks.md for each phase
   - Estimate effort per phase
   - Schedule work

4. **Start Phase 1**
   - Run: `spectr accept add-system-functions`
   - Begin implementation of tasks in order

---

## Command Reference

```bash
# View all proposals
spectr list

# Validate a proposal
spectr validate add-system-functions
spectr validate add-information-schema
spectr validate add-postgresql-catalog

# Accept and start implementation (Phase 1)
spectr accept add-system-functions

# Then later for Phase 2
spectr accept add-information-schema

# Finally for Phase 3
spectr accept add-postgresql-catalog

# View detailed proposal
spectr view add-system-functions
```

---

## Summary

✅ **Status**: All three proposals created and validated
✅ **Ready**: For implementation starting with Phase 1
✅ **Scope**: Complete DuckDB v1.4.3 metadata system + SQL standard compliance + PostgreSQL compatibility
✅ **Effort**: 147 tasks across three coordinated phases
✅ **Impact**: Enable BI tools, ORMs, and database clients to fully introspect dukdb-go

**Next**: Review proposals and start Phase 1 implementation!
