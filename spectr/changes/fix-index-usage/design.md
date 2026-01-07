## Context

The index usage system has a critical disconnect:

### Existing Infrastructure (Working)
1. **Optimizer** (`internal/optimizer/optimizer.go`):
   - `enumerateAccessMethods()` - finds applicable indexes
   - `generateAccessHints()` - creates hints for index scans
   - Cost model with `EstimateIndexScanCost()`

2. **Index Matcher** (`internal/optimizer/index_matcher.go`):
   - `IndexMatcher.FindApplicableIndexes()` - matches predicates to indexes
   - Handles composite index prefix matching
   - IN list predicate support

3. **Executor** (`internal/executor/index_scan.go`):
   - `PhysicalIndexScanOperator` - complete implementation
   - `executeIndexScan()` with lookup, projections, residual filter

4. **Index Implementation** (`internal/storage/index/`):
   - HashIndex - in-memory, stores RowIDs
   - ART index - serialization-compatible, stores RowIDs

### Critical Gap (BROKEN)
The planner **never creates PhysicalIndexScan nodes**:

```go
// internal/planner/physical.go:2036-2066
case *LogicalScan:
    // ... checks for table function, virtual table ...
    return &PhysicalScan{  // ← ALWAYS creates PhysicalScan
        Schema:      l.Schema,
        TableName:   l.TableName,
        // ...
    }, nil
```

The optimizer generates hints, but they're never used.

## Goals / Non-Goals

### Goals
- Connect optimizer hints to planner physical plan generation
- Make indexes actually used in query execution
- Add range scan support for `<`, `>`, `BETWEEN` predicates
- Add EXPLAIN output showing index usage
- Verify CREATE INDEX improves query performance

### Non-Goals
- Full index-only scan optimization (future work)
- JOIN index usage (future work)
- Index hint syntax (`/*+ INDEX(idx) */`)
- Index maintenance on UPDATE/DELETE

## Technical Approach

### Phase 1: Connect Planner to Optimizer

**Pass hints to planner**:
```go
// internal/engine/conn.go

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
    // ... existing code ...

    // Get optimization hints from optimizer
    hints := c.getOptimizationHints(boundStmt)

    // Pass hints to planner
    plan, err := c.planner.Plan(ctx, boundStmt, hints)
    if err != nil {
        return nil, err
    }

    // ... existing code ...
}
```

**Use hints in LogicalScan planning**:
```go
// internal/planner/physical.go

func (p *Planner) createPhysicalPlan(l *LogicalScan) (PhysicalPlan, error) {
    // Check for index scan hint
    if hint, ok := p.hints.GetAccessHint(l.TableName); ok && hint.Method == PlanTypeIndexScan {
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
}
```

**Create PhysicalIndexScan**:
```go
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint AccessHint) (*PhysicalIndexScan, error) {
    // Get index from catalog
    index, err := p.catalog.GetIndex(l.TableName, hint.IndexName)
    if err != nil {
        return nil, err
    }

    // Extract lookup keys from predicates
    keys := extractIndexKeys(hint.Predicates, index.Columns)

    // Handle residual filters
    residualFilters := hint.ResidualFilters

    return &PhysicalIndexScan{
        Schema:          l.Schema,
        TableName:       l.TableName,
        Index:           index,
        LookupKeys:      keys,
        ResidualFilters: residualFilters,
        Projections:     l.Projections,
    }, nil
}
```

### Phase 2: Range Scan Support

**Extend ART with range iterator**:
```go
type ARTIterator struct {
    node     *ART
    stack    []stackEntry
    lower    []byte
    upper    []byte
    inclusiveLower bool
    inclusiveUpper bool
}

func (a *ART) RangeScan(lower, upper []byte, inclusiveLower, inclusiveUpper bool) *ARTIterator {
    // Start from root
    // Traverse to lower bound
    // Yield entries until upper bound
}

func (it *ARTIterator) Next() ([]byte, RowID, bool) {
    // Return next key/rowid in range
    // false when past upper bound
}
```

**Update IndexMatcher for range predicates**:
```go
func (m *IndexMatcher) findRangePredicates(predicates []PredicateExpr) []RangePredicate {
    var ranges []RangePredicate
    for _, pred := range predicates {
        if isRangePredicate(pred) {
            ranges = append(ranges, RangePredicate{
                Column: getColumn(pred),
                Lower:  extractLowerBound(pred),
                Upper:  extractUpperBound(pred),
            })
        }
    }
    return ranges
}
```

### Phase 3: EXPLAIN Integration

**Show index usage in EXPLAIN**:
```go
func (p *PhysicalIndexScan) Explain(w *ExplainWriter) {
    w.Write("IndexScan")
    w.Write("(")
    w.Write("table: %s", p.TableName)
    w.Write(" index: %s", p.Index.Name)
    if len(p.LookupKeys) > 0 {
        w.Write(" keys: %v", p.LookupKeys)
    }
    if len(p.ResidualFilters) > 0 {
        w.Write(" residual: %v", p.ResidualFilters)
    }
    w.Write(")")
}
```

## Implementation Plan

### Phase 1: Connect Planner (1 week)

```
internal/engine/
└── conn.go                  # Pass hints to planner

internal/planner/
├── physical.go              # Use hints in createPhysicalPlan
└── physical_index_scan.go   # Create PhysicalIndexScan (new file)
```

### Phase 2: Range Scan (2 weeks)

```
internal/storage/index/
├── art.go                   # Add RangeScan method
├── art_iterator.go          # Range iterator (new file)
└── index_matcher.go         # Add range predicate matching
```

### Phase 3: EXPLAIN (1 week)

```
internal/planner/
└── explain.go               # Add IndexScan to EXPLAIN output
```

## Effort Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: Connect Planner | 1 week | None |
| Phase 2: Range Scan | 2 weeks | None |
| Phase 3: EXPLAIN | 1 week | None |
| **Total** | **4 weeks** | |

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance regression | Low | Test with TPC-H, compare before/after |
| Index correctness | High | Unit tests for index lookup |
| Range scan correctness | High | Comprehensive range tests |

## Open Questions

1. Should index usage be automatic or require hints?
2. What's the right cost threshold for choosing index vs seq scan?
3. Should we add `SET force_index_scan = true`?

## References

- DuckDB Index Scan: references/duckdb/src/execution/operator/scan/
- ART Index: references/duckdb/src/storage/index/art/
