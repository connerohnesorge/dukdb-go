# Hints Passing Mechanism Design

This document specifies how optimization hints flow from the optimizer through the
engine to the planner, ultimately resulting in PhysicalIndexScan plan nodes.

## 1. Overview

The hints passing mechanism connects the optimizer's index selection decisions to
the planner's physical plan generation. Currently, hints are generated and passed
correctly, but the planner's `createPhysicalPlan()` for `LogicalScan` ignores them.

```
Optimizer                    Engine                      Planner
    |                           |                           |
    | generateAccessHints()     |                           |
    |-------------------------->|                           |
    |   AccessHint{             |                           |
    |     Method: IndexScan,    |                           |
    |     IndexName: "idx_..."  | convertOptimizerHints()   |
    |   }                       |-------------------------->|
    |                           |   planner.AccessHint{     |
    |                           |     Method: "IndexScan",  | SetHints()
    |                           |     IndexName: "idx_..."  |------------>
    |                           |   }                       | p.hints
    |                           |                           |
    |                           |                           | createPhysicalPlan()
    |                           |                           |      |
    |                           |                           |      v
    |                           |                           | GetAccessHint()
    |                           |                           |      |
    |                           |                           |      v
    |                           |                           | PhysicalIndexScan
```

## 2. Data Structures

### 2.1 Optimizer AccessHint (existing)

Location: `internal/optimizer/optimizer.go`

```go
// AccessHint provides hints for physical access method selection.
type AccessHint struct {
    Method    PhysicalPlanType // SeqScan or IndexScan
    IndexName string           // Index to use (if IndexScan)
}
```

### 2.2 Planner AccessHint (existing)

Location: `internal/planner/hints.go`

```go
// AccessHint provides hints for physical access method.
type AccessHint struct {
    // Method specifies the access method to use.
    // Valid values: "SeqScan", "IndexScan"
    Method string

    // IndexName specifies the index to use (if Method is "IndexScan").
    IndexName string
}
```

### 2.3 Extended AccessHint (PROPOSED)

To support PhysicalIndexScan creation, the planner's AccessHint needs additional
fields to carry predicate information from the optimizer:

```go
// AccessHint provides hints for physical access method.
// Location: internal/planner/hints.go
type AccessHint struct {
    // Method specifies the access method to use.
    // Valid values: "SeqScan", "IndexScan"
    Method string

    // IndexName specifies the index to use (if Method is "IndexScan").
    IndexName string

    // LookupKeys contains expressions that evaluate to the index lookup key values.
    // For equality predicates like "col = 5", this contains the literal 5.
    // For composite indexes, contains values for matched prefix columns.
    // Type: []binder.BoundExpr
    LookupKeys []binder.BoundExpr

    // ResidualFilter contains any filter conditions that couldn't be pushed
    // into the index lookup and must be evaluated after fetching rows.
    // For example, if the index is on (a, b) but the query has WHERE a = 1 AND c > 5,
    // the "c > 5" predicate becomes a residual filter.
    // Type: binder.BoundExpr (nil if no residual filter)
    ResidualFilter binder.BoundExpr

    // IsFullMatch is true if all index columns are matched by predicates.
    // A full match on a unique index guarantees at most one row.
    IsFullMatch bool

    // MatchedPredicates contains the predicates satisfied by the index.
    // These predicates are removed from the filter after index lookup.
    // Used for tracking which predicates the index handles.
    MatchedPredicates []binder.BoundExpr

    // Selectivity is the estimated fraction of rows returned (0.0-1.0).
    // Passed from the optimizer's cost estimation.
    Selectivity float64
}
```

### 2.4 Optimizer IndexMatch to Planner AccessHint Conversion

The optimizer's `IndexMatch` contains `LookupKeys` as `[]PredicateExpr` (interface).
The engine's `convertOptimizerHints()` needs to convert these to `[]binder.BoundExpr`.

```go
// convertOptimizerHints converts optimizer.OptimizedPlan hints to planner.OptimizationHints.
// Location: internal/engine/conn.go
func convertOptimizerHints(optimizedPlan *optimizer.OptimizedPlan) *planner.OptimizationHints {
    if optimizedPlan == nil {
        return nil
    }

    hints := planner.NewOptimizationHints()

    // Convert join hints (existing)
    for key, hint := range optimizedPlan.JoinHints {
        hints.JoinHints[key] = planner.JoinHint{
            Method:    string(hint.Method),
            BuildSide: hint.BuildSide,
        }
    }

    // Convert access hints (EXTENDED)
    for key, hint := range optimizedPlan.AccessHints {
        accessHint := planner.AccessHint{
            Method:      string(hint.Method),
            IndexName:   hint.IndexName,
            IsFullMatch: hint.IsFullMatch,
            Selectivity: hint.Selectivity,
        }

        // Convert lookup keys from optimizer PredicateExpr to binder.BoundExpr
        if hint.LookupKeys != nil {
            accessHint.LookupKeys = convertPredicateExprsToBinderExprs(hint.LookupKeys)
        }

        // Convert residual filter
        if hint.ResidualFilter != nil {
            accessHint.ResidualFilter = convertPredicateExprToBinderExpr(hint.ResidualFilter)
        }

        // Convert matched predicates
        if hint.MatchedPredicates != nil {
            accessHint.MatchedPredicates = convertPredicateExprsToBinderExprs(hint.MatchedPredicates)
        }

        hints.AccessHints[key] = accessHint
    }

    return hints
}
```

## 3. Interface Methods

### 3.1 OptimizationHints.GetAccessHint() (existing)

Location: `internal/planner/hints.go`

```go
// GetAccessHint returns the access hint for the given table, if any.
func (h *OptimizationHints) GetAccessHint(table string) (AccessHint, bool) {
    if h == nil || h.AccessHints == nil {
        return AccessHint{}, false
    }
    hint, ok := h.AccessHints[table]
    return hint, ok
}
```

### 3.2 Planner.SetHints() (existing)

Location: `internal/planner/physical.go`

```go
// SetHints stores hints for use during physical plan creation.
func (p *Planner) SetHints(hints *OptimizationHints) {
    p.hints = hints
}
```

### 3.3 Planner.GetCatalog() (PROPOSED if not existing)

The planner needs catalog access to retrieve index definitions.

```go
// GetCatalog returns the catalog for index/table lookups.
func (p *Planner) GetCatalog() *catalog.Catalog {
    return p.catalog
}
```

## 4. createPhysicalPlan() Modification for LogicalScan

### 4.1 Current Implementation (lines 2036-2066)

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

    // ===== MISSING: Check for index scan hint =====

    return &PhysicalScan{           // <-- ALWAYS returns PhysicalScan
        Schema:      l.Schema,
        TableName:   l.TableName,
        Alias:       l.Alias,
        TableDef:    l.TableDef,
        Projections: l.Projections,
    }, nil
```

### 4.2 Proposed Implementation

```go
case *LogicalScan:
    // Check if this is a table function scan
    if l.TableFunction != nil {
        return &PhysicalTableFunctionScan{
            TableName:  l.TableName,
            Alias:      l.Alias,
            Function:   l.TableFunction,
            columns:    l.columns,
        }, nil
    }

    // Check if this is a virtual table scan
    if l.VirtualTable != nil {
        return &PhysicalVirtualTableScan{
            Schema:       l.Schema,
            TableName:    l.TableName,
            Alias:        l.Alias,
            VirtualTable: l.VirtualTable,
        }, nil
    }

    // NEW: Check for index scan hint
    tableName := l.TableName
    if l.Alias != "" {
        // Try alias first (for queries with table aliases)
        if hint, ok := p.hints.GetAccessHint(l.Alias); ok && hint.Method == "IndexScan" {
            return p.createPhysicalIndexScan(l, hint)
        }
    }
    // Try table name
    if hint, ok := p.hints.GetAccessHint(tableName); ok && hint.Method == "IndexScan" {
        return p.createPhysicalIndexScan(l, hint)
    }

    // Fall back to sequential scan
    return &PhysicalScan{
        Schema:      l.Schema,
        TableName:   l.TableName,
        Alias:       l.Alias,
        TableDef:    l.TableDef,
        Projections: l.Projections,
    }, nil
```

### 4.3 createPhysicalIndexScan() Method (NEW)

Location: `internal/planner/physical.go` (or new file `physical_index_scan.go`)

```go
// createPhysicalIndexScan creates a PhysicalIndexScan node from a LogicalScan and access hint.
// It retrieves the index definition from the catalog and validates it exists.
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint AccessHint) (PhysicalPlan, error) {
    // Get index definition from catalog
    indexDef, ok := p.catalog.GetIndexInSchema(l.Schema, hint.IndexName)
    if !ok {
        // Index not found - this is an error condition
        // The optimizer recommended an index that doesn't exist
        return nil, fmt.Errorf("index %q not found in schema %q (referenced in optimizer hint)",
            hint.IndexName, l.Schema)
    }

    // Validate index is for the correct table
    if !strings.EqualFold(indexDef.Table, l.TableName) {
        return nil, fmt.Errorf("index %q is on table %q but scan is on table %q",
            hint.IndexName, indexDef.Table, l.TableName)
    }

    // Determine if this could be an index-only scan
    // (All required columns are in the index)
    isIndexOnly := isIndexOnlyScan(indexDef, l.Projections, l.TableDef)

    return &PhysicalIndexScan{
        Schema:         l.Schema,
        TableName:      l.TableName,
        Alias:          l.Alias,
        TableDef:       l.TableDef,
        IndexName:      hint.IndexName,
        IndexDef:       indexDef,
        LookupKeys:     hint.LookupKeys,
        ResidualFilter: hint.ResidualFilter,
        Projections:    l.Projections,
        IsIndexOnly:    isIndexOnly,
    }, nil
}

// isIndexOnlyScan determines if all projected columns are in the index.
// This enables potential optimization where we don't need to access the table heap.
// Note: Current HashIndex only stores RowIDs, so true index-only scan is future work.
func isIndexOnlyScan(indexDef *catalog.IndexDef, projections []int, tableDef *catalog.TableDef) bool {
    if projections == nil {
        // Selecting all columns - can't be index-only unless index has all columns
        return false
    }

    // Build set of index columns (lowercase for case-insensitive matching)
    indexCols := make(map[string]bool)
    for _, col := range indexDef.Columns {
        indexCols[strings.ToLower(col)] = true
    }

    // Check if all projected columns are in the index
    for _, projIdx := range projections {
        if projIdx < 0 || projIdx >= len(tableDef.Columns) {
            return false
        }
        colName := strings.ToLower(tableDef.Columns[projIdx].Name)
        if !indexCols[colName] {
            return false
        }
    }

    return true
}
```

## 5. AccessHint Format for Index Scans

### 5.1 Required Fields

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| Method | string | "IndexScan" | Yes |
| IndexName | string | Name of index to use (e.g., "idx_users_id") | Yes |
| LookupKeys | []binder.BoundExpr | Key expressions for index lookup | Yes |
| ResidualFilter | binder.BoundExpr | Filter applied after index lookup | No (nil if none) |
| IsFullMatch | bool | True if all index columns matched | No (default false) |
| Selectivity | float64 | Estimated selectivity (0.0-1.0) | No (default 1.0) |

### 5.2 Example AccessHint Values

**Single-column equality (WHERE id = 5)**:
```go
AccessHint{
    Method:      "IndexScan",
    IndexName:   "idx_users_id",
    LookupKeys:  []binder.BoundExpr{&binder.BoundLiteralExpr{Value: 5}},
    IsFullMatch: true,
    Selectivity: 0.01,
}
```

**Composite index partial match (WHERE a = 1 AND c > 10, index on (a, b))**:
```go
AccessHint{
    Method:         "IndexScan",
    IndexName:      "idx_composite_a_b",
    LookupKeys:     []binder.BoundExpr{&binder.BoundLiteralExpr{Value: 1}},
    ResidualFilter: &binder.BoundBinaryExpr{...}, // c > 10
    IsFullMatch:    false,
    Selectivity:    0.1,
}
```

**IN list predicate (WHERE status IN ('active', 'pending'))**:
```go
AccessHint{
    Method:      "IndexScan",
    IndexName:   "idx_status",
    LookupKeys:  []binder.BoundExpr{
        &binder.BoundLiteralExpr{Value: "active"},
        &binder.BoundLiteralExpr{Value: "pending"},
    },
    IsFullMatch: true,
    Selectivity: 0.2,
}
```

## 6. Error Handling

### 6.1 Index Not Found

When `createPhysicalIndexScan()` is called but the index doesn't exist:

```go
// Error: index "idx_users_id" not found in schema "main"
// This indicates a bug: optimizer generated a hint for a non-existent index
// Action: Return error, do not fall back to sequential scan silently
```

**Why not fall back?** Silent fallback would hide bugs where:
- Index was dropped after optimization but before planning
- Index name was misspelled in optimizer
- Schema resolution is inconsistent

### 6.2 Index Table Mismatch

When the index exists but is for a different table:

```go
// Error: index "idx_orders_id" is on table "orders" but scan is on table "users"
// This indicates incorrect hint generation
// Action: Return error
```

### 6.3 Corrupted Index

Corruption is detected at execution time, not planning time. The executor handles this:

```go
// In executor/operator.go, when creating PhysicalIndexScanOperator:
index := e.storage.GetIndex(plan.Schema, plan.IndexName)
if index == nil {
    return nil, &dukdb.Error{
        Type: dukdb.ErrorTypeExecutor,
        Msg:  fmt.Sprintf("index %q not found in storage (may be corrupted or not yet built)",
            plan.IndexName),
    }
}
```

### 6.4 Error Response Strategy

| Condition | Phase | Response |
|-----------|-------|----------|
| Index not in catalog | Planning | Return error, abort query |
| Index on wrong table | Planning | Return error, abort query |
| Index not in storage | Execution | Return error, abort query |
| Index lookup returns no rows | Execution | Return empty result (normal) |
| Index key evaluation fails | Execution | Return error, abort query |

## 7. Pattern Reference: Join Hints

The implementation follows the existing pattern for join hints (lines 2101-2108):

```go
case *LogicalJoin:
    left, err := p.createPhysicalPlan(l.Left)
    if err != nil {
        return nil, err
    }
    right, err := p.createPhysicalPlan(l.Right)
    if err != nil {
        return nil, err
    }

    // Check for optimization hints for this join
    joinKey := fmt.Sprintf("join_%d", p.joinIndex)
    p.joinIndex++

    // If we have a hint for this join, use it
    if hint, ok := p.hints.GetJoinHint(joinKey); ok {
        return p.createPhysicalJoinFromHint(left, right, l.JoinType, l.Condition, hint)
    }

    // Default: Use hash join for equi-joins, nested loop for others
    if isEquiJoin(l.Condition) {
        return &PhysicalHashJoin{...}, nil
    }
    return &PhysicalNestedLoopJoin{...}, nil
```

**Parallel Structure for Index Scans**:

```go
case *LogicalScan:
    // ... table function and virtual table checks ...

    // Check for optimization hints for this scan
    tableName := l.TableName
    if l.Alias != "" {
        tableName = l.Alias // Use alias for hint lookup if present
    }

    // If we have a hint for this scan, use it
    if hint, ok := p.hints.GetAccessHint(tableName); ok && hint.Method == "IndexScan" {
        return p.createPhysicalIndexScan(l, hint)
    }

    // Default: Sequential scan
    return &PhysicalScan{...}, nil
```

## 8. Implementation Steps

### Step 1: Extend planner.AccessHint

Modify `internal/planner/hints.go`:
- Add LookupKeys, ResidualFilter, IsFullMatch, MatchedPredicates, Selectivity fields
- Import binder package

### Step 2: Extend optimizer.AccessHint

Modify `internal/optimizer/optimizer.go`:
- Add LookupKeys, ResidualFilter, IsFullMatch, MatchedPredicates, Selectivity fields
- Update generateAccessHints() to populate new fields from IndexMatch

### Step 3: Update convertOptimizerHints()

Modify `internal/engine/conn.go`:
- Convert new fields from optimizer hints to planner hints
- Add conversion functions for PredicateExpr to BoundExpr

### Step 4: Add createPhysicalIndexScan()

Add to `internal/planner/physical.go`:
- Implement createPhysicalIndexScan() method
- Add isIndexOnlyScan() helper

### Step 5: Modify createPhysicalPlan() for LogicalScan

Update the LogicalScan case in `createPhysicalPlan()`:
- Add hint lookup before creating PhysicalScan
- Call createPhysicalIndexScan() when hint indicates IndexScan

### Step 6: Add Tests

Create tests in `internal/planner/physical_test.go`:
- Test hint lookup with table name
- Test hint lookup with alias
- Test createPhysicalIndexScan() success path
- Test error handling for missing index
- Test error handling for wrong table

## 9. Files to Modify

| File | Changes |
|------|---------|
| `internal/planner/hints.go` | Extend AccessHint struct |
| `internal/optimizer/optimizer.go` | Extend AccessHint, update generateAccessHints() |
| `internal/engine/conn.go` | Update convertOptimizerHints() |
| `internal/planner/physical.go` | Add createPhysicalIndexScan(), modify LogicalScan case |

## 10. Testing Strategy

### Unit Tests

1. **Hint Lookup Tests**
   - Table name lookup succeeds
   - Alias lookup succeeds (takes precedence)
   - Missing hint returns false
   - SeqScan hint returns PhysicalScan

2. **createPhysicalIndexScan Tests**
   - Valid index returns PhysicalIndexScan
   - Missing index returns error
   - Wrong table returns error
   - IsIndexOnly correctly computed

### Integration Tests

1. **End-to-End Index Usage**
   - CREATE INDEX on column
   - SELECT with equality predicate uses index
   - EXPLAIN shows IndexScan

2. **Composite Index Tests**
   - Full prefix match
   - Partial prefix match with residual filter
   - Non-prefix predicate falls back to seq scan

## 11. References

- Optimizer: `internal/optimizer/optimizer.go` (lines 617-710)
- Engine connector: `internal/engine/conn.go` (lines 690-715)
- Planner hints: `internal/planner/hints.go`
- Planner physical: `internal/planner/physical.go` (lines 2036-2066, 2101-2108)
- Index scan executor: `internal/executor/index_scan.go`
- ANALYSIS.md: `spectr/changes/fix-index-usage/ANALYSIS.md`
