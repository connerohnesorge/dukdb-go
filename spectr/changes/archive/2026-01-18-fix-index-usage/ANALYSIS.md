# Optimizer to Planner Disconnect Analysis

This document details the disconnect between the optimizer's index selection logic
and the planner's physical plan generation, explaining why indexes are never used
in query execution despite full infrastructure being in place.

## Executive Summary

**Problem**: The optimizer generates `AccessHint` with `Method: PlanTypeIndexScan`,
but the planner's `createPhysicalPlan()` ignores these hints and always creates
`PhysicalScan`. This means:

- CREATE INDEX has no effect on query performance
- All optimizer work for index selection is wasted
- PhysicalIndexScan operator exists but is never invoked

## Current Flow Diagram

```
SQL Query: SELECT * FROM users WHERE id = 1
    |
    v
+-------------------+
|     Parser        |  --> AST
+-------------------+
    |
    v
+-------------------+
|     Binder        |  --> BoundSelectStmt
+-------------------+
    |
    v
+-------------------+
|    Optimizer      |  --> OptimizedPlan with AccessHints
|                   |      AccessHints["users"] = {
|                   |        Method: "IndexScan",     <-- OPTIMIZER SAYS: USE INDEX!
|                   |        IndexName: "idx_users_id"
|                   |      }
+-------------------+
    |
    v
+-------------------+
|   engine/conn.go  |  --> Calls convertOptimizerHints()
|                   |      Calls planner.SetHints(hints)  <-- HINTS ARE SET
+-------------------+
    |
    v
+-------------------+
|     Planner       |  --> p.hints contains AccessHints  <-- HINTS ARE STORED
|                   |
| createPhysicalPlan|  --> case *LogicalScan:
|                   |        return &PhysicalScan{...}   <-- BUT NEVER CHECKED!
+-------------------+
    |
    v
+-------------------+
|    Executor       |  --> Full table scan (PhysicalScan)
|                   |      PhysicalIndexScanOperator NEVER USED
+-------------------+
```

## Detailed Code Analysis

### 1. Optimizer Generates Hints Correctly

**File**: `internal/optimizer/optimizer.go`

The optimizer's `generateAccessHints()` method correctly identifies when an index
should be used:

```go
// Line 617-622: generateAccessHints creates hints for all scans
func (o *CostBasedOptimizer) generateAccessHints(plan LogicalPlanNode) map[string]AccessHint {
    hints := make(map[string]AccessHint)
    o.collectAccessHints(plan, hints)
    return hints
}

// Line 674-710: selectAccessHintForFilteredScan determines best access method
func (o *CostBasedOptimizer) selectAccessHintForFilteredScan(
    filter FilterNode,
    scan ScanNode,
) AccessHint {
    // ... extracts predicates and enumerates access methods ...

    methods := o.enumerateAccessMethods(schema, tableName, predicates, requiredColumns, tableStats)
    best := o.selectBestAccessMethod(methods)

    if best == nil || best.Type == PlanTypeSeqScan {
        return AccessHint{Method: PlanTypeSeqScan, IndexName: ""}
    }

    // Best is an index scan - CORRECTLY RETURNS IndexScan hint
    return AccessHint{
        Method:    PlanTypeIndexScan,     // <-- OPTIMIZER SAYS USE INDEX
        IndexName: best.IndexMatch.Index.GetName(),
    }
}
```

### 2. Engine Passes Hints to Planner

**File**: `internal/engine/conn.go`

The hints ARE correctly passed from the optimizer to the planner:

```go
// Line 620-627: Hints are retrieved and set on planner
if _, isSelect := boundStmt.(*binder.BoundSelectStmt); isSelect {
    hints := c.getOptimizationHints(boundStmt)  // <-- Gets hints from optimizer
    if hints != nil {
        p.SetHints(hints)  // <-- Passes hints to planner
    }
}
```

```go
// Line 690-715: convertOptimizerHints converts optimizer hints to planner hints
func convertOptimizerHints(optimizedPlan *optimizer.OptimizedPlan) *planner.OptimizationHints {
    hints := planner.NewOptimizationHints()

    // Convert access hints - THIS WORKS CORRECTLY
    for key, hint := range optimizedPlan.AccessHints {
        hints.AccessHints[key] = planner.AccessHint{
            Method:    string(hint.Method),
            IndexName: hint.IndexName,
        }
    }
    return hints
}
```

### 3. Planner Stores Hints But Never Uses Them for Scans

**File**: `internal/planner/physical.go`

The planner stores hints via `SetHints()`:

```go
// Line 1180-1184: SetHints stores hints
func (p *Planner) SetHints(hints *OptimizationHints) {
    p.hints = hints  // <-- Hints ARE stored
}
```

**THE CRITICAL GAP** - Line 2032-2066: `createPhysicalPlan()` for LogicalScan
NEVER checks access hints:

```go
func (p *Planner) createPhysicalPlan(
    logical LogicalPlan,
) (PhysicalPlan, error) {
    switch l := logical.(type) {
    case *LogicalScan:
        // Check if this is a table function scan
        if l.TableFunction != nil {
            return &PhysicalTableFunctionScan{...}, nil
        }

        // Check if this is a virtual table scan
        if l.VirtualTable != nil {
            return &PhysicalVirtualTableScan{...}, nil
        }

        // ===== MISSING CODE HERE =====
        // Should check: if hint, ok := p.hints.GetAccessHint(l.TableName); ok && hint.Method == "IndexScan" {
        //     return p.createPhysicalIndexScan(l, hint)
        // }
        // =============================

        return &PhysicalScan{           // <-- ALWAYS RETURNS PhysicalScan
            Schema:      l.Schema,
            TableName:   l.TableName,
            Alias:       l.Alias,
            TableDef:    l.TableDef,
            Projections: l.Projections,
        }, nil
```

**Contrast with Join Hints** - Line 2101-2108: Join hints ARE used:

```go
case *LogicalJoin:
    // ...

    // Check for optimization hints for this join
    joinKey := fmt.Sprintf("join_%d", p.joinIndex)
    p.joinIndex++

    // If we have a hint for this join, use it  <-- JOIN HINTS ARE CHECKED!
    if hint, ok := p.hints.GetJoinHint(joinKey); ok {
        return p.createPhysicalJoinFromHint(left, right, l.JoinType, l.Condition, hint)
    }
```

### 4. PhysicalIndexScan Exists But Is Never Created

**File**: `internal/planner/physical.go`

PhysicalIndexScan is defined at lines 70-105:

```go
// PhysicalIndexScan represents a physical index-based table scan.
type PhysicalIndexScan struct {
    Schema    string
    TableName string
    Alias     string
    TableDef  *catalog.TableDef

    IndexName string
    IndexDef  *catalog.IndexDef

    LookupKeys     []binder.BoundExpr  // Key expressions for index lookup
    ResidualFilter binder.BoundExpr    // Filter applied after index lookup
    Projections    []int
    IsIndexOnly    bool

    columns []ColumnBinding
}
```

**But there is no `&PhysicalIndexScan{}` instantiation anywhere in production code.**

A grep for `&PhysicalIndexScan{` shows it only appears in design documents:
- `spectr/changes/fix-index-usage/design.md`
- `spectr/changes/archive/2026-01-07-add-index-usage/design.md`

### 5. Executor Has Full Implementation Ready

**File**: `internal/executor/index_scan.go`

The executor has a complete `PhysicalIndexScanOperator` implementation:

```go
// Line 14-49: Full operator struct
type PhysicalIndexScanOperator struct {
    tableName   string
    schema      string
    tableDef    *catalog.TableDef
    indexName   string
    indexDef    *catalog.IndexDef
    index       *storage.HashIndex
    lookupKeys  []binder.BoundExpr
    columns     []planner.ColumnBinding
    projections []int
    isIndexOnly bool
    // ... execution state ...
}

// Line 164-255: Next() method - fully implemented
func (op *PhysicalIndexScanOperator) Next() (*storage.DataChunk, error) {
    // Performs index lookup and returns matching rows
}

// Line 259-284: performIndexLookup() - evaluates keys and queries index
func (op *PhysicalIndexScanOperator) performIndexLookup() ([]storage.RowID, error) {
    // Evaluates lookup keys and performs index lookup
    rowIDs := op.index.Lookup(keyValues)
    return rowIDs, nil
}
```

This operator is fully functional but **never invoked** because PhysicalIndexScan
plans are never created.

## Summary of the Disconnect

| Component | Status | Code Location |
|-----------|--------|---------------|
| Index creation (CREATE INDEX) | Working | `internal/catalog/index.go` |
| Index storage | Working | `internal/storage/index.go` |
| Optimizer hint generation | Working | `internal/optimizer/optimizer.go:617-710` |
| Optimizer to Engine | Working | `internal/engine/conn.go:620-627` |
| Engine to Planner | Working | `internal/engine/conn.go:690-715` |
| Planner hint storage | Working | `internal/planner/physical.go:1180-1184` |
| **Planner hint usage for scans** | **BROKEN** | `internal/planner/physical.go:2036-2066` |
| PhysicalIndexScan definition | Exists | `internal/planner/physical.go:70-105` |
| PhysicalIndexScan creation | **MISSING** | N/A - no `&PhysicalIndexScan{}` in code |
| PhysicalIndexScanOperator | Working | `internal/executor/index_scan.go` |

## Required Fix

The fix requires modifying `createPhysicalPlan()` in `internal/planner/physical.go`
to check access hints before defaulting to sequential scan:

```go
case *LogicalScan:
    // Check if this is a table function scan
    if l.TableFunction != nil {
        return &PhysicalTableFunctionScan{...}, nil
    }

    // Check if this is a virtual table scan
    if l.VirtualTable != nil {
        return &PhysicalVirtualTableScan{...}, nil
    }

    // NEW: Check for index scan hint
    tableName := l.TableName
    if tableName == "" {
        tableName = l.Alias
    }
    if hint, ok := p.hints.GetAccessHint(tableName); ok && hint.Method == "IndexScan" {
        return p.createPhysicalIndexScan(l, hint)  // NEW METHOD NEEDED
    }

    // Fall back to sequential scan
    return &PhysicalScan{...}, nil
```

And implementing the new `createPhysicalIndexScan()` method that:
1. Retrieves the index from the catalog
2. Extracts lookup keys from filter predicates
3. Creates and returns a `PhysicalIndexScan` plan node

## References

- Optimizer: `internal/optimizer/optimizer.go`
- Engine connector: `internal/engine/conn.go`
- Planner: `internal/planner/physical.go`
- Index scan executor: `internal/executor/index_scan.go`
- Design doc: `spectr/changes/fix-index-usage/design.md`
