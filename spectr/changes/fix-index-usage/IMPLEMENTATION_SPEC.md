# Implementation Specification: Fix Index Usage in Query Plans

This document consolidates all design decisions into a single implementation reference for
fixing the critical disconnect between optimizer index selection and planner physical plan
generation.

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Implementation Phases](#2-implementation-phases)
3. [Phase 1: Connect Optimizer to Planner](#3-phase-1-connect-optimizer-to-planner)
4. [Phase 2: ART Range Scan Support](#4-phase-2-art-range-scan-support)
5. [Phase 3: Index Matcher Range Support](#5-phase-3-index-matcher-range-support)
6. [Phase 4: EXPLAIN Integration](#6-phase-4-explain-integration)
7. [Phase 5: Integration and Testing](#7-phase-5-integration-and-testing)
8. [File Modification Summary](#8-file-modification-summary)
9. [Testing Strategy](#9-testing-strategy)
10. [Acceptance Criteria](#10-acceptance-criteria)
11. [References](#11-references)

---

## 1. Executive Summary

### The Problem

The optimizer generates `AccessHint{Method: PlanTypeIndexScan}` but the planner's
`createPhysicalPlan()` for `LogicalScan` ignores these hints and always creates
`PhysicalScan`, meaning:

- CREATE INDEX has no effect on query performance
- PhysicalIndexScanOperator exists but is never invoked
- All optimizer work for index selection is wasted

### The Solution

1. **Phase 1 (2.x tasks)**: Connect optimizer hints to planner physical plan generation
2. **Phase 2 (3.x tasks)**: Implement ART range scan iterator for range predicates
3. **Phase 3 (4.x tasks)**: Extend index matcher to support range predicates
4. **Phase 4 (5.x tasks)**: Add EXPLAIN output for index scan operators
5. **Phase 5 (6.x-8.x tasks)**: Integration, testing, and verification

### Estimated Timeline

| Phase | Duration | Tasks |
|-------|----------|-------|
| Phase 1 | 1 week | 2.1-2.9 |
| Phase 2 | 3-4 weeks | 3.1-3.7 |
| Phase 3 | 1 week | 4.1-4.6 |
| Phase 4 | 1 week | 5.1-5.5 |
| Phase 5 | 1-2 weeks | 6.1-8.5 |
| **Total** | **5-6 weeks** | |

---

## 2. Implementation Phases

### Phase Dependencies

```
Phase 1: Connect Optimizer to Planner (CRITICAL - must be first)
    |
    +---> Phase 2: ART Range Scan (can start in parallel after 2.4)
    |         |
    |         +---> Phase 3: Index Matcher Range Support (depends on 3.2)
    |
    +---> Phase 4: EXPLAIN Integration (can start in parallel after 2.4)
              |
              +---> Phase 5: Integration Testing (depends on all above)
```

### Task to Phase Mapping

| Phase | Tasks | Description |
|-------|-------|-------------|
| Phase 1 | 2.1-2.9 | Connect Optimizer to Planner |
| Phase 2 | 3.1-3.7 | ART Range Scan Implementation |
| Phase 3 | 4.1-4.6 | Index Matcher Range Support |
| Phase 4 | 5.1-5.5 | EXPLAIN Integration |
| Phase 5 | 6.1-6.5, 7.1-7.7, 8.1-8.5 | Integration and Verification |

---

## 3. Phase 1: Connect Optimizer to Planner

**Tasks**: 2.1-2.9
**Duration**: 1 week
**Priority**: CRITICAL - Must be completed first

### Overview

This phase connects the existing optimizer hint generation to the planner's physical
plan creation. The infrastructure exists but the final connection is missing.

### Task 2.1: Extend Planner.AccessHint

**File**: `internal/planner/hints.go`

**Current State**:
```go
type AccessHint struct {
    Method    string
    IndexName string
}
```

**Required Changes**:
```go
import "github.com/dukdb/dukdb-go/internal/binder"

// AccessHint provides hints for physical access method.
type AccessHint struct {
    // Method specifies the access method: "SeqScan" or "IndexScan"
    Method string

    // IndexName specifies the index to use (if Method is "IndexScan")
    IndexName string

    // LookupKeys contains expressions that evaluate to index lookup key values
    // For equality predicates like "col = 5", contains the literal 5
    LookupKeys []binder.BoundExpr

    // ResidualFilter contains filter conditions not satisfied by index lookup
    // Evaluated after fetching rows from the index
    ResidualFilter binder.BoundExpr

    // IsFullMatch is true if all index columns are matched by predicates
    IsFullMatch bool

    // MatchedPredicates contains predicates satisfied by the index
    MatchedPredicates []binder.BoundExpr

    // Selectivity is the estimated fraction of rows returned (0.0-1.0)
    Selectivity float64
}
```

### Task 2.2: Extend Optimizer.AccessHint

**File**: `internal/optimizer/optimizer.go`

Add corresponding fields to the optimizer's AccessHint:

```go
type AccessHint struct {
    Method            PhysicalPlanType
    IndexName         string
    LookupKeys        []PredicateExpr  // New
    ResidualFilter    PredicateExpr    // New
    IsFullMatch       bool             // New
    MatchedPredicates []PredicateExpr  // New
    Selectivity       float64          // New
}
```

Update `generateAccessHints()` and `selectAccessHintForFilteredScan()` to populate
these new fields from the `IndexMatch` struct.

### Task 2.3: Update convertOptimizerHints()

**File**: `internal/engine/conn.go`

Update the hint conversion to handle the new fields:

```go
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

        // Convert lookup keys
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

// New helper functions
func convertPredicateExprsToBinderExprs(preds []optimizer.PredicateExpr) []binder.BoundExpr {
    result := make([]binder.BoundExpr, 0, len(preds))
    for _, pred := range preds {
        if expr := convertPredicateExprToBinderExpr(pred); expr != nil {
            result = append(result, expr)
        }
    }
    return result
}

func convertPredicateExprToBinderExpr(pred optimizer.PredicateExpr) binder.BoundExpr {
    if pred == nil {
        return nil
    }
    // PredicateExpr is typically a wrapper around binder.BoundExpr
    if boundExpr, ok := pred.(binder.BoundExpr); ok {
        return boundExpr
    }
    return nil
}
```

### Task 2.4: Implement createPhysicalIndexScan()

**File**: `internal/planner/physical.go`

Add the new method to create PhysicalIndexScan nodes:

```go
// createPhysicalIndexScan creates a PhysicalIndexScan node from a LogicalScan and access hint.
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint AccessHint) (PhysicalPlan, error) {
    // Validate hint
    if hint.IndexName == "" {
        return nil, fmt.Errorf("index scan hint has empty index name")
    }
    if len(hint.LookupKeys) == 0 {
        return nil, fmt.Errorf("index scan on %q requires lookup keys but none provided",
            hint.IndexName)
    }

    // Get index definition from catalog
    schemaName := l.Schema
    if schemaName == "" {
        schemaName = "main"
    }

    indexDef, ok := p.catalog.GetIndexInSchema(schemaName, hint.IndexName)
    if !ok {
        return nil, fmt.Errorf("index %q not found in schema %q (referenced in optimizer hint)",
            hint.IndexName, schemaName)
    }

    // Validate index is for the correct table
    if !strings.EqualFold(indexDef.Table, l.TableName) {
        return nil, fmt.Errorf("index %q is on table %q but scan is on table %q",
            hint.IndexName, indexDef.Table, l.TableName)
    }

    // Determine if this could be an index-only scan
    isIndexOnly := isIndexOnlyScan(indexDef, l.Projections, l.TableDef)

    return &PhysicalIndexScan{
        Schema:         schemaName,
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
func isIndexOnlyScan(indexDef *catalog.IndexDef, projections []int, tableDef *catalog.TableDef) bool {
    if projections == nil {
        return false // SELECT * cannot be index-only
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

### Task 2.5: Modify createPhysicalPlan() for LogicalScan

**File**: `internal/planner/physical.go`

Modify the `LogicalScan` case in `createPhysicalPlan()`:

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
    if p.hints != nil {
        // Try alias first (for queries with table aliases)
        if l.Alias != "" {
            if hint, ok := p.hints.GetAccessHint(l.Alias); ok && hint.Method == "IndexScan" {
                return p.createPhysicalIndexScan(l, hint)
            }
        }
        // Try table name
        if hint, ok := p.hints.GetAccessHint(l.TableName); ok && hint.Method == "IndexScan" {
            return p.createPhysicalIndexScan(l, hint)
        }
    }

    // Default: Sequential scan
    return &PhysicalScan{
        Schema:      l.Schema,
        TableName:   l.TableName,
        Alias:       l.Alias,
        TableDef:    l.TableDef,
        Projections: l.Projections,
    }, nil
```

### Task 2.6-2.9: Tests and Error Handling

**Unit Tests** (`internal/planner/physical_test.go`):
- Test hint lookup with table name
- Test hint lookup with alias
- Test createPhysicalIndexScan() success path
- Test error handling for missing index
- Test error handling for wrong table
- Test error handling for corrupted index scenario

**Integration Tests** (`internal/executor/index_scan_test.go`):
- Test CREATE INDEX followed by query that uses index
- Test EXPLAIN shows IndexScan

---

## 4. Phase 2: ART Range Scan Support

**Tasks**: 3.1-3.7
**Duration**: 3-4 weeks
**Depends on**: Can start after Task 2.4 is complete

### Overview

Implement iterator-based range scan support for the ART index to enable `<`, `>`,
`<=`, `>=`, and `BETWEEN` predicates.

### Task 3.1: ARTIterator Struct

**File**: `internal/storage/index/art_iterator.go` (new file)

```go
package index

// ARTIterator provides ordered traversal of keys within a range.
type ARTIterator struct {
    art            *ART
    stack          []iteratorStackEntry
    currentKey     []byte
    lowerBound     []byte
    upperBound     []byte
    lowerInclusive bool
    upperInclusive bool
    exhausted      bool
    initialized    bool
    lastKey        []byte
    lastValue      any
}

type iteratorStackEntry struct {
    node           *ARTNode
    childIndex     int
    prefixConsumed int
}

// RangeScanOptions configures range scan behavior.
type RangeScanOptions struct {
    LowerInclusive bool
    UpperInclusive bool
    Reverse        bool
    MaxResults     int
}

// DefaultRangeScanOptions returns sensible defaults.
func DefaultRangeScanOptions() RangeScanOptions {
    return RangeScanOptions{
        LowerInclusive: true,
        UpperInclusive: false,
        Reverse:        false,
        MaxResults:     0,
    }
}
```

### Task 3.2: RangeScan() Method

**File**: `internal/storage/index/art.go`

```go
// RangeScan creates an iterator for keys in the range [lower, upper).
func (a *ART) RangeScan(lower, upper []byte, opts RangeScanOptions) *ARTIterator {
    // Check for empty range
    if lower != nil && upper != nil {
        cmp := bytes.Compare(lower, upper)
        if cmp > 0 {
            return &ARTIterator{exhausted: true}
        }
        if cmp == 0 && (!opts.LowerInclusive || !opts.UpperInclusive) {
            return &ARTIterator{exhausted: true}
        }
    }

    return &ARTIterator{
        art:            a,
        stack:          make([]iteratorStackEntry, 0, 16),
        currentKey:     make([]byte, 0, 64),
        lowerBound:     lower,
        upperBound:     upper,
        lowerInclusive: opts.LowerInclusive,
        upperInclusive: opts.UpperInclusive,
        exhausted:      false,
        initialized:    false,
    }
}

// Convenience methods
func (a *ART) ScanFrom(lower []byte, inclusive bool) *ARTIterator {
    return a.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: inclusive})
}

func (a *ART) ScanTo(upper []byte, inclusive bool) *ARTIterator {
    return a.RangeScan(nil, upper, RangeScanOptions{UpperInclusive: inclusive})
}

func (a *ART) ScanAll() *ARTIterator {
    return a.RangeScan(nil, nil, DefaultRangeScanOptions())
}
```

### Task 3.3: Iterator Next() Method

**File**: `internal/storage/index/art_iterator.go`

```go
// Next advances to the next key-value pair in range.
func (it *ARTIterator) Next() (key []byte, value any, ok bool) {
    if it.exhausted {
        return nil, nil, false
    }

    if !it.initialized {
        if !it.seekToLowerBound() {
            it.exhausted = true
            return nil, nil, false
        }
        it.initialized = true
    } else {
        if !it.advanceToNextLeaf() {
            it.exhausted = true
            return nil, nil, false
        }
    }

    if it.upperBound != nil && it.exceedsUpperBound() {
        it.exhausted = true
        return nil, nil, false
    }

    it.lastKey = make([]byte, len(it.currentKey))
    copy(it.lastKey, it.currentKey)
    it.lastValue = it.getCurrentValue()

    return it.lastKey, it.lastValue, true
}

// HasNext returns true if there are more entries.
func (it *ARTIterator) HasNext() bool {
    if it.exhausted {
        return false
    }
    // Implementation with peek ahead
    return true
}

// Close releases iterator resources.
func (it *ARTIterator) Close() {
    it.stack = nil
    it.currentKey = nil
    it.lowerBound = nil
    it.upperBound = nil
    it.lastKey = nil
    it.lastValue = nil
    it.exhausted = true
}
```

### Tasks 3.4-3.5: Bound Handling and Composite Keys

See `ART_RANGE_SCAN_DESIGN.md` sections 5 and 6 for detailed algorithms.

### Tasks 3.6-3.7: Unit Tests

**File**: `internal/storage/index/art_iterator_test.go`

Test cases:
- Empty ART returns no results
- Single element range
- Full scan (nil bounds)
- Inclusive vs exclusive bounds
- Range predicates (`<`, `>`, `BETWEEN`)
- Composite key ranges
- Edge cases (type boundaries, prefix mismatch)

---

## 5. Phase 3: Index Matcher Range Support

**Tasks**: 4.1-4.6
**Duration**: 1 week
**Depends on**: Task 3.2 (ART.RangeScan)

### Overview

Extend the index matcher to recognize and handle range predicates, generating
appropriate access hints with range bounds.

### Task 4.1: findRangePredicates() Function

**File**: `internal/optimizer/index_matcher.go`

```go
// findRangePredicates extracts range predicates from a filter.
func (m *IndexMatcher) findRangePredicates(
    filter FilterNode,
    indexCols []string,
) []RangePredicate {
    predicates := extractPredicatesFromFilter(filter)
    var rangePredicates []RangePredicate

    for _, pred := range predicates {
        if rp := m.extractRangePredicate(pred, indexCols); rp != nil {
            rangePredicates = append(rangePredicates, *rp)
        }
    }

    return rangePredicates
}

type RangePredicate struct {
    Column    string
    Op        ComparisonOp
    Value     any
    Inclusive bool
}

type ComparisonOp int

const (
    OpEqual ComparisonOp = iota
    OpLessThan
    OpLessThanOrEqual
    OpGreaterThan
    OpGreaterThanOrEqual
    OpBetween
)
```

### Task 4.2-4.4: Handle Range Predicates

```go
// extractRangePredicate converts a predicate to a RangePredicate if applicable.
func (m *IndexMatcher) extractRangePredicate(
    pred PredicateExpr,
    indexCols []string,
) *RangePredicate {
    binary, ok := pred.(*BinaryPredicate)
    if !ok {
        return nil
    }

    colRef, ok := binary.Left.(*ColumnRef)
    if !ok {
        return nil
    }

    // Check if column is in index
    if !contains(indexCols, colRef.Column) {
        return nil
    }

    literal, ok := binary.Right.(*Literal)
    if !ok {
        return nil
    }

    switch binary.Op {
    case "<":
        return &RangePredicate{Column: colRef.Column, Op: OpLessThan, Value: literal.Value}
    case "<=":
        return &RangePredicate{Column: colRef.Column, Op: OpLessThanOrEqual, Value: literal.Value}
    case ">":
        return &RangePredicate{Column: colRef.Column, Op: OpGreaterThan, Value: literal.Value}
    case ">=":
        return &RangePredicate{Column: colRef.Column, Op: OpGreaterThanOrEqual, Value: literal.Value}
    }

    return nil
}

// MergeRangeBounds combines multiple range predicates into optimal bounds.
func MergeRangeBounds(predicates []RangePredicate) *RangeBounds {
    // Implementation as per ART_RANGE_SCAN_DESIGN.md section 5.3
}
```

### Task 4.5: Integrate with Access Hint Generation

Modify `selectAccessHintForFilteredScan()` to handle range predicates:

```go
func (o *CostBasedOptimizer) selectAccessHintForFilteredScan(
    filter FilterNode,
    scan ScanNode,
) AccessHint {
    // ... existing equality predicate handling ...

    // NEW: Check for range predicates
    rangePredicates := findRangePredicates(filter, indexMatch.Index.Columns)
    if len(rangePredicates) > 0 {
        bounds := MergeRangeBounds(rangePredicates)
        if bounds != nil {
            return AccessHint{
                Method:         PlanTypeIndexScan,
                IndexName:      indexMatch.Index.GetName(),
                IsRangeScan:    true,
                LowerBound:     bounds.Lower,
                UpperBound:     bounds.Upper,
                LowerInclusive: bounds.LowerInclusive,
                UpperInclusive: bounds.UpperInclusive,
            }
        }
    }

    // ... fallback to sequential scan ...
}
```

---

## 6. Phase 4: EXPLAIN Integration

**Tasks**: 5.1-5.5
**Duration**: 1 week
**Depends on**: Task 2.4 (PhysicalIndexScan creation)

### Overview

Add PhysicalIndexScan to EXPLAIN output so users can verify index usage.

### Task 5.1-5.4: EXPLAIN Output Format

**File**: `internal/executor/physical_maintenance.go`

Add case for `PhysicalIndexScan` in `formatPhysicalPlanWithCost()`:

```go
case *planner.PhysicalIndexScan:
    // Determine scan type label
    scanType := "IndexScan"
    if p.IsRangeScan {
        scanType = "IndexRangeScan"
    }
    if p.IsIndexOnly {
        scanType = "IndexOnlyScan"
    }

    // Format main line
    sb.WriteString(fmt.Sprintf("%s%s: %s USING %s %s",
        prefix, scanType, p.TableName, p.IndexName, formatCostAnnotation(cost)))
    if p.Alias != "" && p.Alias != p.TableName {
        sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
    }
    sb.WriteString("\n")

    // Format Index Cond
    if len(p.LookupKeys) > 0 {
        sb.WriteString(fmt.Sprintf("%s  Index Cond: %s\n",
            prefix, formatIndexCondition(p)))
    }

    // Format residual filter
    if p.ResidualFilter != nil {
        sb.WriteString(fmt.Sprintf("%s  Filter: %s\n",
            prefix, formatExpression(p.ResidualFilter)))
    }
```

**Expected Output Format**:

```
IndexScan: users USING idx_email (cost=0.00..0.15 rows=10 width=64)
  Index Cond: (email = 'test@example.com')
```

```
IndexRangeScan: data USING idx_x (cost=0.00..0.50 rows=90 width=32)
  Index Cond: (x >= 10 AND x <= 100)
```

```
IndexScan: orders USING idx_ab (cost=0.00..0.20 rows=15 width=40)
  Index Cond: ((a = 1) AND (b > 5))
  Filter: (c = 'foo')
```

### Task 5.5: EXPLAIN Tests

**File**: `internal/executor/maintenance_test.go`

```go
func TestExplainIndexScan(t *testing.T) {
    db := setupTestDB(t)

    _, err := db.Exec(`
        CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR);
        CREATE INDEX idx_email ON users(email);
    `)
    require.NoError(t, err)

    rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE email = 'test@example.com'")
    require.NoError(t, err)
    defer rows.Close()

    var plan string
    for rows.Next() {
        rows.Scan(&plan)
    }

    assert.Contains(t, plan, "IndexScan")
    assert.Contains(t, plan, "idx_email")
    assert.Contains(t, plan, "email = 'test@example.com'")
}
```

---

## 7. Phase 5: Integration and Testing

**Tasks**: 6.1-8.5
**Duration**: 1-2 weeks
**Depends on**: All previous phases

### Task 6.1-6.5: Integration Tasks

- Connect range scan to PhysicalIndexScan executor
- Update cost model for range scan costs
- Handle residual filters for range scans
- End-to-end testing with range queries
- Test with composite indexes

### Task 7.1-7.7: Comprehensive Testing

| Task | Test Type | Description |
|------|-----------|-------------|
| 7.1 | Unit | Hint passing mechanism |
| 7.2 | Integration | CREATE INDEX -> query uses index |
| 7.3 | Unit | ART range scan |
| 7.4 | Unit | Range predicate matching |
| 7.5 | Integration | Range queries use index |
| 7.6 | Integration | EXPLAIN output for indexes |
| 7.7 | Performance | TPC-H benchmark comparison |

### Task 8.1-8.5: Verification

- Run `spectr validate fix-index-usage`
- Verify index is actually used (EXPLAIN)
- Verify range queries use index
- TPC-H benchmark with/without indexes
- Ensure all existing tests pass

---

## 8. File Modification Summary

### Files to Modify

| File | Phase | Changes |
|------|-------|---------|
| `internal/planner/hints.go` | 1 | Extend AccessHint struct |
| `internal/optimizer/optimizer.go` | 1 | Extend AccessHint, update generateAccessHints() |
| `internal/engine/conn.go` | 1 | Update convertOptimizerHints() |
| `internal/planner/physical.go` | 1 | Add createPhysicalIndexScan(), modify LogicalScan case |
| `internal/storage/index/art.go` | 2 | Add RangeScan() method |
| `internal/optimizer/index_matcher.go` | 3 | Add findRangePredicates(), MergeRangeBounds() |
| `internal/executor/physical_maintenance.go` | 4 | Add PhysicalIndexScan to EXPLAIN |
| `internal/executor/index_scan.go` | 5 | Add range scan support to operator |
| `internal/optimizer/cost_model.go` | 5 | Add EstimateIndexScanCost() |

### New Files to Create

| File | Phase | Description |
|------|-------|-------------|
| `internal/storage/index/art_iterator.go` | 2 | ARTIterator struct and methods |
| `internal/storage/index/art_iterator_test.go` | 2 | Iterator unit tests |
| `internal/planner/physical_index_scan_test.go` | 1 | Physical index scan unit tests |

---

## 9. Testing Strategy

### Unit Testing

| Component | Location | Coverage |
|-----------|----------|----------|
| Hint extension | `internal/planner/hints_test.go` | Field population, GetAccessHint() |
| createPhysicalIndexScan | `internal/planner/physical_index_scan_test.go` | Success, missing index, wrong table |
| ARTIterator | `internal/storage/index/art_iterator_test.go` | Range bounds, traversal |
| Index matcher | `internal/optimizer/index_matcher_test.go` | Range predicate extraction |
| EXPLAIN format | `internal/executor/maintenance_test.go` | IndexScan output format |

### Integration Testing

| Test Case | Query | Expected |
|-----------|-------|----------|
| Simple equality | `SELECT * FROM t WHERE id = 1` | IndexScan: t USING pk_t_id |
| Composite key | `SELECT * FROM t WHERE a = 1 AND b = 2` | IndexScan: t USING idx_ab |
| Range scan | `SELECT * FROM t WHERE x BETWEEN 10 AND 100` | IndexRangeScan: t USING idx_x |
| Residual filter | `SELECT * FROM t WHERE a = 1 AND c > 5` | IndexScan + Filter: (c > 5) |
| No index | `SELECT * FROM t WHERE c = 1` | Scan: t (sequential) |

### Performance Testing

- TPC-H queries with and without indexes
- Measure query execution time improvement
- Verify optimizer chooses index when beneficial

---

## 10. Acceptance Criteria

### Phase 1 Acceptance (Tasks 2.1-2.9)

- [ ] AccessHint struct extended with LookupKeys, ResidualFilter, etc.
- [ ] convertOptimizerHints() converts all new fields
- [ ] createPhysicalIndexScan() creates valid PhysicalIndexScan nodes
- [ ] createPhysicalPlan() checks hints and creates IndexScan when appropriate
- [ ] Error handling for missing/mismatched indexes
- [ ] Unit tests pass for hint passing
- [ ] Integration test: CREATE INDEX -> query uses index

### Phase 2 Acceptance (Tasks 3.1-3.7)

- [ ] ARTIterator struct implemented
- [ ] RangeScan() method works for bounded/unbounded ranges
- [ ] Next() correctly traverses ART in sorted order
- [ ] Inclusive/exclusive bounds handled correctly
- [ ] Composite keys work for range scans
- [ ] All unit tests pass

### Phase 3 Acceptance (Tasks 4.1-4.6)

- [ ] findRangePredicates() extracts range predicates
- [ ] BETWEEN predicates handled
- [ ] <, >, <=, >= predicates handled
- [ ] Composite key ranges work
- [ ] Integration with access hint generation complete
- [ ] Unit tests pass

### Phase 4 Acceptance (Tasks 5.1-5.5)

- [ ] EXPLAIN shows IndexScan for indexed queries
- [ ] Index name displayed in EXPLAIN
- [ ] Lookup keys shown in EXPLAIN
- [ ] Residual filters shown in EXPLAIN
- [ ] All EXPLAIN tests pass

### Phase 5 Acceptance (Tasks 6.1-8.5)

- [ ] Range scan connected to PhysicalIndexScan executor
- [ ] Cost model updated for range scans
- [ ] Residual filters work with range scans
- [ ] End-to-end tests pass
- [ ] TPC-H benchmark shows improvement with indexes
- [ ] All existing tests continue to pass

### Final Acceptance

- [ ] `nix develop -c lint` passes
- [ ] `nix develop -c tests` passes
- [ ] EXPLAIN confirms index usage for appropriate queries
- [ ] No regression in existing functionality

---

## 11. References

### Design Documents

| Document | Description |
|----------|-------------|
| [ANALYSIS.md](ANALYSIS.md) | Problem analysis and current state |
| [HINTS_PASSING_DESIGN.md](HINTS_PASSING_DESIGN.md) | Hints flow from optimizer to planner |
| [PHYSICAL_INDEX_SCAN_DESIGN.md](PHYSICAL_INDEX_SCAN_DESIGN.md) | Creating PhysicalIndexScan nodes |
| [ART_RANGE_SCAN_DESIGN.md](ART_RANGE_SCAN_DESIGN.md) | ART iterator for range scans |
| [EXPLAIN_INTEGRATION_DESIGN.md](EXPLAIN_INTEGRATION_DESIGN.md) | EXPLAIN output for indexes |

### Code References

| File | Description |
|------|-------------|
| `internal/optimizer/optimizer.go` | Lines 617-710: generateAccessHints() |
| `internal/engine/conn.go` | Lines 620-627, 690-715: Hint conversion |
| `internal/planner/physical.go` | Lines 70-105: PhysicalIndexScan, Lines 2036-2066: createPhysicalPlan |
| `internal/executor/index_scan.go` | PhysicalIndexScanOperator implementation |
| `internal/storage/index/art.go` | ART index implementation |
| `internal/optimizer/index_matcher.go` | Index matching and selection |

### Task Reference

See [tasks.jsonc](tasks.jsonc) for the complete task list with status tracking.
