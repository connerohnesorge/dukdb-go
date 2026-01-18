# PhysicalIndexScan Creation Design

This document specifies the design of the `createPhysicalIndexScan()` method that will
be called from `createPhysicalPlan()` when the optimizer recommends an index scan.

## 1. Overview

The `createPhysicalIndexScan()` method is the bridge between optimizer hints and physical
plan creation. It takes a `LogicalScan` node and an `AccessHint` containing index
selection information, and produces a `PhysicalIndexScan` node ready for execution.

```
createPhysicalPlan() for LogicalScan
        |
        v
  Check hints.GetAccessHint()
        |
        v (if Method == "IndexScan")
  createPhysicalIndexScan(LogicalScan, AccessHint)
        |
        v
  PhysicalIndexScan (ready for executor)
```

## 2. Method Signature

```go
// createPhysicalIndexScan creates a PhysicalIndexScan node from a LogicalScan and access hint.
// It retrieves the index definition from the catalog and validates it exists.
//
// Parameters:
//   - l: The LogicalScan node being converted to physical
//   - hint: The AccessHint from the optimizer containing index selection info
//
// Returns:
//   - PhysicalPlan: The constructed PhysicalIndexScan node
//   - error: Non-nil if index not found, table mismatch, or other validation failure
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint AccessHint) (PhysicalPlan, error)
```

## 3. Required Parameters

### 3.1 From LogicalScan (l)

| Field | Type | Description | Usage |
|-------|------|-------------|-------|
| `Schema` | `string` | Schema name (e.g., "main") | Index catalog lookup, PhysicalIndexScan.Schema |
| `TableName` | `string` | Table name | Validate index-table match, PhysicalIndexScan.TableName |
| `Alias` | `string` | Table alias (if any) | PhysicalIndexScan.Alias |
| `TableDef` | `*catalog.TableDef` | Table definition | Column type info, PhysicalIndexScan.TableDef |
| `Projections` | `[]int` | Column indices to project | PhysicalIndexScan.Projections, index-only check |

### 3.2 From AccessHint (hint)

| Field | Type | Description | Usage |
|-------|------|-------------|-------|
| `Method` | `string` | "IndexScan" | Already verified by caller |
| `IndexName` | `string` | Name of index to use | Catalog lookup, PhysicalIndexScan.IndexName |
| `LookupKeys` | `[]binder.BoundExpr` | Key expressions for lookup | PhysicalIndexScan.LookupKeys |
| `ResidualFilter` | `binder.BoundExpr` | Filter to apply after lookup | PhysicalIndexScan.ResidualFilter |
| `IsFullMatch` | `bool` | All index columns matched | Cost hints, potential optimizations |
| `Selectivity` | `float64` | Estimated selectivity | Cost hints (future use) |

## 4. Lookup Key Extraction

### 4.1 From Optimizer to Planner

The optimizer's `IndexMatch.LookupKeys` contains expressions as `[]PredicateExpr`.
These need to be converted to `[]binder.BoundExpr` in `convertOptimizerHints()`.

```go
// Conversion in internal/engine/conn.go
func convertPredicateExprToBinderExpr(pred optimizer.PredicateExpr) binder.BoundExpr {
    // PredicateExpr is typically a wrapper around binder.BoundExpr
    // The optimizer uses interfaces to avoid circular imports
    if boundExpr, ok := pred.(binder.BoundExpr); ok {
        return boundExpr
    }
    // Handle other cases as needed
    return nil
}
```

### 4.2 Key Expression Types

| Predicate Pattern | LookupKeys Content | Example |
|-------------------|-------------------|---------|
| `col = 5` | `[BoundLiteral{5}]` | Single equality |
| `col = 5 AND col2 = 'x'` | `[BoundLiteral{5}, BoundLiteral{'x'}]` | Composite key |
| `col IN (1, 2, 3)` | `[BoundLiteral{1}, BoundLiteral{2}, BoundLiteral{3}]` | Multiple lookups |
| `col = $1` | `[BoundParameterRef{1}]` | Parameterized query |

### 4.3 Handling Empty Lookup Keys

If `hint.LookupKeys` is empty but `hint.Method == "IndexScan"`, this indicates
a bug in hint generation. The method should return an error:

```go
if len(hint.LookupKeys) == 0 {
    return nil, fmt.Errorf("index scan hint for %q has no lookup keys", hint.IndexName)
}
```

## 5. Residual Filter Handling

### 5.1 What is a Residual Filter?

A residual filter contains predicates that cannot be satisfied by the index lookup
alone and must be evaluated after fetching rows from the table.

**Example:**
- Index: `idx_composite_a_b` on columns `(a, b)`
- Query: `WHERE a = 1 AND c > 10`
- Index handles: `a = 1`
- Residual filter: `c > 10`

### 5.2 Residual Filter in PhysicalIndexScan

```go
PhysicalIndexScan{
    // ... other fields ...
    ResidualFilter: hint.ResidualFilter, // May be nil if no residual filter needed
}
```

### 5.3 Executor Responsibility

The executor (`PhysicalIndexScanOperator`) must apply the residual filter after
fetching rows. The current executor implementation does not yet handle residual
filters and will need to be extended:

```go
// In executor/index_scan.go Next() method
// After fetching row by RowID:
if op.residualFilter != nil {
    passes, err := op.evaluateFilter(row)
    if err != nil {
        return nil, err
    }
    if !passes {
        continue // Skip this row
    }
}
```

## 6. PhysicalIndexScan Population

### 6.1 Field Mapping

```go
return &PhysicalIndexScan{
    // Table metadata (from LogicalScan)
    Schema:    l.Schema,
    TableName: l.TableName,
    Alias:     l.Alias,
    TableDef:  l.TableDef,

    // Index metadata (from catalog lookup)
    IndexName: hint.IndexName,
    IndexDef:  indexDef,        // Retrieved from catalog

    // Lookup configuration (from hint)
    LookupKeys:     hint.LookupKeys,
    ResidualFilter: hint.ResidualFilter,

    // Projections (from LogicalScan)
    Projections: l.Projections,

    // Computed fields
    IsIndexOnly: isIndexOnlyScan(indexDef, l.Projections, l.TableDef),
}, nil
```

### 6.2 Catalog Lookup

```go
// Get index definition from catalog
indexDef, ok := p.catalog.GetIndexInSchema(l.Schema, hint.IndexName)
if !ok {
    return nil, fmt.Errorf("index %q not found in schema %q", hint.IndexName, l.Schema)
}
```

Note: The catalog method name may vary. Check existing catalog interface:
- `GetIndex(schema, name)` or
- `GetIndexInSchema(schema, name)` or
- `GetIndexByName(name)`

## 7. Integration with PhysicalIndexScanOperator

### 7.1 Existing Operator Interface

The `PhysicalIndexScanOperator` in `internal/executor/index_scan.go` expects:

```go
NewPhysicalIndexScanOperator(
    tableName string,
    schema string,
    tableDef *catalog.TableDef,
    indexName string,
    indexDef *catalog.IndexDef,
    index *storage.HashIndex,  // Actual index from storage
    lookupKeys []binder.BoundExpr,
    projections []int,
    isIndexOnly bool,
    stor *storage.Storage,
    executor *Executor,
    ctx *ExecutionContext,
)
```

### 7.2 What Planner Provides vs Executor Retrieves

| Data | Planner Provides | Executor Retrieves |
|------|-----------------|-------------------|
| Table/Index names | Yes | - |
| Table definition | Yes | - |
| Index definition | Yes | - |
| Lookup keys | Yes | - |
| Projections | Yes | - |
| IsIndexOnly flag | Yes | - |
| HashIndex instance | No | From storage at execution time |
| Storage reference | No | From execution context |

### 7.3 Residual Filter Gap

**Current gap:** `NewPhysicalIndexScanOperator` does not accept a residual filter
parameter. This needs to be added:

```go
// Updated signature (future task)
NewPhysicalIndexScanOperator(
    // ... existing params ...
    residualFilter binder.BoundExpr,  // NEW
    // ... remaining params ...
)
```

## 8. Edge Cases

### 8.1 Empty Lookup Keys

**Scenario:** `hint.LookupKeys` is empty or nil.

**Action:** Return error. An index scan without lookup keys is invalid.

```go
if len(hint.LookupKeys) == 0 {
    return nil, &dukdb.Error{
        Type: dukdb.ErrorTypePlanner,
        Msg:  fmt.Sprintf("index scan on %q requires lookup keys but none provided", hint.IndexName),
    }
}
```

### 8.2 Missing Index

**Scenario:** Index referenced in hint doesn't exist in catalog.

**Action:** Return error. Do not silently fall back to sequential scan.

```go
indexDef, ok := p.catalog.GetIndexInSchema(l.Schema, hint.IndexName)
if !ok {
    return nil, &dukdb.Error{
        Type: dukdb.ErrorTypePlanner,
        Msg:  fmt.Sprintf("index %q not found in schema %q (referenced in optimizer hint)", hint.IndexName, l.Schema),
    }
}
```

### 8.3 Index-Table Mismatch

**Scenario:** Index exists but is on a different table than the scan.

**Action:** Return error.

```go
if !strings.EqualFold(indexDef.Table, l.TableName) {
    return nil, &dukdb.Error{
        Type: dukdb.ErrorTypePlanner,
        Msg:  fmt.Sprintf("index %q is on table %q but scan is on table %q", hint.IndexName, indexDef.Table, l.TableName),
    }
}
```

### 8.4 Type Mismatches in Lookup Keys

**Scenario:** Lookup key expression type doesn't match index column type.

**Action:** This is better handled at execution time when the expression is evaluated.
The planner should not perform type checking here; it's already done during binding.

### 8.5 Nil Hint or Empty IndexName

**Scenario:** Caller passes a hint with empty IndexName.

**Action:** Return error early.

```go
if hint.IndexName == "" {
    return nil, &dukdb.Error{
        Type: dukdb.ErrorTypePlanner,
        Msg:  "index scan hint has empty index name",
    }
}
```

### 8.6 Index with Dropped Column

**Scenario:** Table had a column dropped that's part of the index.

**Action:** The catalog should handle this (index should be dropped when column is dropped).
If encountered, return error.

## 9. Implementation Steps

### Step 1: Add createPhysicalIndexScan method

Location: `internal/planner/physical.go` (or new file `physical_index_scan.go`)

```go
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint AccessHint) (PhysicalPlan, error) {
    // 1. Validate hint
    if hint.IndexName == "" {
        return nil, &dukdb.Error{...}
    }
    if len(hint.LookupKeys) == 0 {
        return nil, &dukdb.Error{...}
    }

    // 2. Lookup index in catalog
    indexDef, ok := p.catalog.GetIndexInSchema(l.Schema, hint.IndexName)
    if !ok {
        return nil, &dukdb.Error{...}
    }

    // 3. Validate index-table match
    if !strings.EqualFold(indexDef.Table, l.TableName) {
        return nil, &dukdb.Error{...}
    }

    // 4. Compute isIndexOnly
    isIndexOnly := isIndexOnlyScan(indexDef, l.Projections, l.TableDef)

    // 5. Create PhysicalIndexScan
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
```

### Step 2: Add isIndexOnlyScan helper

```go
func isIndexOnlyScan(indexDef *catalog.IndexDef, projections []int, tableDef *catalog.TableDef) bool {
    if projections == nil {
        return false // SELECT * cannot be index-only unless index has all columns
    }

    indexCols := make(map[string]bool)
    for _, col := range indexDef.Columns {
        indexCols[strings.ToLower(col)] = true
    }

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

### Step 3: Modify createPhysicalPlan for LogicalScan

```go
case *LogicalScan:
    // Check for table function scan
    if l.TableFunction != nil {
        return &PhysicalTableFunctionScan{...}, nil
    }

    // Check for virtual table scan
    if l.VirtualTable != nil {
        return &PhysicalVirtualTableScan{...}, nil
    }

    // NEW: Check for index scan hint
    tableName := l.TableName
    if l.Alias != "" {
        if hint, ok := p.hints.GetAccessHint(l.Alias); ok && hint.Method == "IndexScan" {
            return p.createPhysicalIndexScan(l, hint)
        }
    }
    if hint, ok := p.hints.GetAccessHint(tableName); ok && hint.Method == "IndexScan" {
        return p.createPhysicalIndexScan(l, hint)
    }

    // Default: Sequential scan
    return &PhysicalScan{...}, nil
```

## 10. Testing Strategy

### Unit Tests

1. **Valid index scan creation**
   - Create LogicalScan and valid AccessHint
   - Verify PhysicalIndexScan is returned with correct fields

2. **Missing index error**
   - Pass hint with non-existent index name
   - Verify error is returned

3. **Table mismatch error**
   - Pass hint with index on different table
   - Verify error is returned

4. **Empty lookup keys error**
   - Pass hint with empty LookupKeys
   - Verify error is returned

5. **Index-only scan detection**
   - Test with projections that match index columns
   - Test with projections that don't match

6. **Alias vs table name hint lookup**
   - Test that alias takes precedence when both exist

### Integration Tests

1. **End-to-end index usage**
   ```sql
   CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR);
   CREATE INDEX idx_name ON t(name);
   SELECT * FROM t WHERE name = 'test';
   -- Verify EXPLAIN shows IndexScan
   ```

2. **Composite index with residual filter**
   ```sql
   CREATE INDEX idx_ab ON t(a, b);
   SELECT * FROM t WHERE a = 1 AND c > 10;
   -- Index scan on a=1, residual filter on c>10
   ```

## 11. References

- `internal/planner/physical.go`: PhysicalIndexScan struct (lines 70-105)
- `internal/planner/physical.go`: createPhysicalPlan for LogicalScan (lines 2036-2066)
- `internal/executor/index_scan.go`: PhysicalIndexScanOperator
- `internal/planner/hints.go`: AccessHint struct
- `internal/catalog/index.go`: IndexDef struct
- `HINTS_PASSING_DESIGN.md`: Hints flow from optimizer to planner
