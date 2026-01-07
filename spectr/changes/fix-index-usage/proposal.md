# Change: Fix Index Usage in Query Plans (Critical)

## Why

**CRITICAL FINDING**: There is a complete disconnect between the optimizer's index selection logic and the planner's physical plan generation. The infrastructure for index usage is largely in place, but the planner **never creates PhysicalIndexScan nodes**, rendering the optimizer's access hints useless.

Current state:
- Optimizer generates `AccessHint` with `Method: PlanTypeIndexScan`
- Planner's `createPhysicalPlan()` ignores hints and always creates `PhysicalScan`
- PhysicalIndexScan operator exists but is never invoked
- CREATE INDEX works but has no effect on query performance

This means:
- All the optimizer work is wasted
- CREATE INDEX has no effect on query performance
- Users cannot benefit from indexed lookups
- Unit tests pass but real queries don't use indexes

## What Changes

### Critical Fixes (Priority 1)
- **FIXED**: Connect optimizer hints to planner physical plan generation
- **ADDED**: `createPhysicalIndexScan()` method in planner
- **ADDED**: Hints passed from engine to planner
- **ADDED**: Index scan creation when hints indicate index usage

### Missing Features (Priority 2)
- **ADDED**: Range scan support for `<`, `>`, `BETWEEN` predicates
- **ADDED**: ART index range iterator (LowerBound/UpperBound)
- **ADDED**: IN list predicate index usage

### Optimization (Priority 3)
- **ADDED**: Index-only scans (store values in index)
- **ADDED**: Covering index detection
- **ADDED**: EXPLAIN output showing index usage

## Impact

- Affected specs: `specs/index-usage/spec.md`
- Affected code:
  - `internal/planner/physical.go` - Connect hints to physical plan
  - `internal/engine/conn.go` - Pass hints to planner
  - `internal/storage/index/art.go` - Range scan support (3-4 weeks estimate)
  - `internal/executor/index_scan.go` - Index-only scan support
- Breaking changes: None
- Dependencies: None

## Priority

**CRITICAL** - This is a blocking issue. Indexes are completely unused in query execution despite full infrastructure.

## Effort

**5-6 weeks** (Phase 2 range scan requires 3-4 weeks, not 2 weeks as initially estimated)
