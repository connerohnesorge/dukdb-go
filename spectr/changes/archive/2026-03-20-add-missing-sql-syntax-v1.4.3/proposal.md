# Complete Missing SQL Syntax for DuckDB v1.4.3 Compatibility

**Change ID:** `add-missing-sql-syntax-v1.4.3`
**Created:** 2026-03-20
**Scope:** Small — Targeted fixes to existing partial implementations
**Estimated Complexity:** Small — Narrow gaps in already-implemented features
**User-Visible:** Yes — Fixes edge cases in existing SQL syntax

## Summary

This proposal completes three narrow gaps in existing SQL syntax implementations:

1. **TRUNCATE TABLE IF EXISTS** — The TRUNCATE statement exists but lacks IF EXISTS support and WAL/undo integration
2. **TRUNCATE schema-qualified storage lookup** — Executor doesn't pass schema to storage layer
3. **VALUES type inference** — VALUES binder uses first-non-NULL type per column instead of true type promotion across all rows

## Current State (Verified Against Codebase)

### TRUNCATE — Partially Implemented
- **Parser**: `parseTruncate()` exists at `internal/parser/parser.go:2416-2446`. Parses `TRUNCATE [TABLE] [schema.]table_name`.
- **AST**: `TruncateStmt` at `internal/parser/ast.go:426-429` has `Schema` and `Table` fields but **NO `IfExists` field**.
- **Storage**: `Table.Truncate()` at `internal/storage/table.go:166-183` clears rowGroups, tombstones, rowIDMap, rowVersions. Returns `int64` (no error). **Does NOT clear index entries.**
- **Executor**: `executeTruncate()` at `internal/executor/ddl.go:834-856` resolves table but **uses `e.storage.GetTable(plan.Table)` ignoring `plan.Schema`**. No WAL logging. No undo recording for rollback.
- **Binder**: `internal/binder/bind_stmt.go:3093-3097` always errors if table not found — incompatible with IF EXISTS.

### FETCH FIRST — Fully Implemented (No Changes Needed)
- Parser: `internal/parser/parser.go:442-486` — parses FETCH FIRST/NEXT, WITH TIES
- Binder: `internal/binder/bind_stmt.go:226-231` — validates WITH TIES requires ORDER BY
- Executor: `internal/executor/operator.go:1810-1847` — WITH TIES logic implemented
- **This feature is COMPLETE. Removed from proposal scope.**

### VALUES — Mostly Implemented
- Parser: `parseValuesClauseBody()` at `internal/parser/parser.go:1556-1571`, `parseStandaloneValues()` at lines 1573-1600
- Binder: `internal/binder/bind_stmt.go:501-525` — type inference uses first-non-NULL type per column, not true type promotion
- Planner: `PhysicalValues` at `internal/planner/physical.go:649-660`
- Executor: `executeValues()` wired at `internal/executor/operator.go:407-408`
- **Gap**: Type inference should use supertype promotion (like UNION) not first-non-NULL

## Goals

1. Add `IfExists bool` field to `TruncateStmt` AST and parse `TRUNCATE [TABLE] IF EXISTS`
2. Fix `executeTruncate()` to pass schema to storage layer: `e.storage.GetTableInSchema(plan.Schema, plan.Table)`
3. Add WAL logging and undo recording for TRUNCATE (transactional rollback support)
4. Clear index entries in `Table.Truncate()` if indexes exist
5. Improve VALUES type inference to use true type promotion across all rows

## Non-Goals

- TRUNCATE CASCADE (requires FK graph — separate proposal)
- TRUNCATE RESTART IDENTITY (sequence reset)
- Multi-table TRUNCATE

## Capabilities

### Capability: TRUNCATE IF EXISTS and Transactional Support

**Parser changes:**
- Add `IfExists bool` to `TruncateStmt` struct
- Parse IF EXISTS between optional TABLE keyword and table name

**Binder changes:**
- When `IfExists` is true and table not found, return a no-op bound statement instead of error

**Executor changes:**
- Pass `plan.Schema` to `e.storage.GetTableInSchema()` for schema-qualified truncates
- When `IfExists` and table not found, return `{RowsAffected: 0}` without error
- Add WAL logging: `e.wal.WriteEntry(WALEntryTruncate, ...)`
- Add undo recording: `e.undoRecorder.RecordTruncate(...)` for ROLLBACK support

**Storage changes:**
- In `Table.Truncate()`, also clear associated index entries (check if table has index references)

### Capability: VALUES Type Inference Improvement

**Binder changes:**
- Replace first-non-NULL type selection with true supertype promotion across all rows per column
- Reuse existing type promotion logic from UNION binding
- Insert implicit CAST nodes where row types differ from the inferred column type

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| TRUNCATE undo snapshot too large | Low | Medium | Snapshot chunk refs only (shallow copy) |
| Type promotion changes break existing VALUES queries | Low | Low | Only widens types, never narrows |
