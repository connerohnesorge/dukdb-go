# Design: Index Usage in Query Plans

This document captures architectural decisions for implementing index usage in query plans for dukdb-go.

## Architecture Overview

```
SQL Query with WHERE clause
    |
    v
Parser -> AST with Filter predicates
    |
    v
Binder -> Resolved column references
    |
    v
Logical Planner -> LogicalFilter over LogicalScan
    |
    v
Cost-Based Optimizer
    |-- Enumerate access methods (SeqScan, IndexScan)
    |-- Estimate costs for each
    |-- Select cheapest access method
    |
    v
Physical Planner -> PhysicalIndexScan or PhysicalSeqScan
    |
    v
Executor -> Results via index lookup or table scan
```

---

## Decision 1: Index Scan Operator

### Context
A new physical operator is needed to perform table access via index lookup rather than sequential scan.

### Decision
Implement PhysicalIndexScan as a new physical operator:

```go
// PhysicalIndexScan performs table access via index lookup
type PhysicalIndexScan struct {
    TableName   string
    Schema      string
    IndexName   string
    IndexDef    *catalog.IndexDef
    LookupKeys  []Expression        // Key values to look up
    Projections []int               // Column indices to project
    IsIndexOnly bool                // True if no heap access needed
}

// Execute performs the index scan
func (s *PhysicalIndexScan) Execute(ctx *ExecutionContext) (*DataChunk, error) {
    // 1. Get index from storage
    index := ctx.Storage.GetIndex(s.Schema, s.IndexName)

    // 2. Evaluate lookup key expressions
    keyValues := s.evaluateKeys(ctx)

    // 3. Look up RowIDs via index
    rowIDs := index.Lookup(keyValues)

    // 4. If index-only scan, return projected columns from index
    if s.IsIndexOnly {
        return s.projectFromIndex(index, rowIDs)
    }

    // 5. Otherwise, fetch rows from heap table
    return s.fetchFromTable(ctx, rowIDs)
}
```

### Rationale
- Clean separation from SeqScan operator
- Supports both index-only and regular index scans
- Leverages existing HashIndex.Lookup() infrastructure
- Fits existing physical operator model

---

## Decision 2: Cost Model for Index Scan

### Context
The cost model must estimate index scan costs accurately to make correct access method decisions.

### Decision
Add index scan cost formulas to CostModel:

```go
// Cost constants for index operations
type CostModel struct {
    // Existing constants...
    SeqPageCost     float64 // Default: 1.0
    RandomPageCost  float64 // Default: 4.0
    CPUTupleCost    float64 // Default: 0.01

    // New index-specific constants
    IndexLookupCost float64 // Cost per index lookup (default: 0.005)
    IndexTupleCost  float64 // Cost per index entry scanned (default: 0.005)
}

// EstimateIndexScanCost calculates cost for index scan
func (m *CostModel) EstimateIndexScanCost(
    indexDef *catalog.IndexDef,
    selectivity float64,
    tableRows int64,
    tablePages int64,
    isIndexOnly bool,
) PlanCost {
    estimatedRows := float64(tableRows) * selectivity

    // Cost = lookup overhead + per-row costs
    startupCost := m.IndexLookupCost

    if isIndexOnly {
        // Index-only: just scan index entries
        totalCost := startupCost + (estimatedRows * m.IndexTupleCost)
        return PlanCost{
            StartupCost: startupCost,
            TotalCost:   totalCost,
            OutputRows:  estimatedRows,
        }
    }

    // Regular index scan: index lookup + random heap access
    // Each row requires random page access to fetch tuple
    randomPages := math.Min(estimatedRows, float64(tablePages))
    heapCost := randomPages * m.RandomPageCost
    tupleCost := estimatedRows * m.CPUTupleCost

    totalCost := startupCost + (estimatedRows * m.IndexTupleCost) + heapCost + tupleCost

    return PlanCost{
        StartupCost: startupCost,
        TotalCost:   totalCost,
        OutputRows:  estimatedRows,
    }
}

// EstimateSeqScanCost for comparison
func (m *CostModel) EstimateSeqScanCost(
    tableRows int64,
    tablePages int64,
    selectivity float64,
) PlanCost {
    seqCost := float64(tablePages) * m.SeqPageCost
    tupleCost := float64(tableRows) * m.CPUTupleCost

    return PlanCost{
        StartupCost: 0,
        TotalCost:   seqCost + tupleCost,
        OutputRows:  float64(tableRows) * selectivity,
    }
}
```

### Index Scan vs Sequential Scan Decision

Index scan is preferred when:
```
IndexScanCost < SeqScanCost
```

This typically happens when:
- Selectivity is low (small fraction of rows match)
- Table is large (many pages)
- Random I/O cost multiplier (4x) does not dominate

**Crossover Point Analysis:**
```
For RandomPageCost = 4.0, SeqPageCost = 1.0:
- Index scan typically wins when selectivity < ~5-10% for medium tables
- Larger tables have lower crossover point
- Smaller tables may prefer seq scan even at low selectivity
```

### Rationale
- Separates index lookup overhead from per-row costs
- Accounts for random I/O penalty for heap access
- Index-only scan eliminates heap I/O entirely
- Compatible with existing cost model structure

---

## Decision 3: Index-Only Scan (Covering Index)

### Context
When an index contains all columns needed by a query, we can avoid accessing the heap table entirely.

### Decision
Detect and implement index-only scan:

```go
// IsCoveringIndex checks if index covers all required columns
func IsCoveringIndex(indexDef *catalog.IndexDef, requiredColumns []string) bool {
    indexCols := make(map[string]bool)
    for _, col := range indexDef.Columns {
        indexCols[col] = true
    }

    for _, required := range requiredColumns {
        if !indexCols[required] {
            return false
        }
    }
    return true
}

// GetRequiredColumns extracts all columns needed by query
func GetRequiredColumns(plan LogicalPlan) []string {
    var columns []string

    // Collect from projections
    for _, proj := range plan.Projections() {
        columns = append(columns, extractColumnRefs(proj)...)
    }

    // Collect from filter predicates
    if filter := plan.Filter(); filter != nil {
        columns = append(columns, extractColumnRefs(filter)...)
    }

    // Deduplicate
    return unique(columns)
}
```

**Index-Only Scan Execution:**
```go
// For index-only scan, we need index to store actual values
// Current HashIndex only stores RowIDs, not column values

// Option A: Extend HashIndex to store column values
type HashIndexWithValues struct {
    HashIndex
    values map[hashKey][]ColumnValue // Column values per key
}

// Option B: Use index to filter, then project from compact storage
// (Simpler, works with current HashIndex)
func (s *PhysicalIndexScan) projectFromIndex(rowIDs []RowID) *DataChunk {
    // Current implementation: still need heap access
    // Future: extend index to store values for true index-only scan
}
```

**Note:** True index-only scan requires index structures that store column values. Current HashIndex only stores RowIDs, so initial implementation will still access heap but with RowID filtering benefit.

### Rationale
- Index-only scan is significant optimization when applicable
- Initial implementation provides RowID filtering benefit
- Full index-only requires index structure changes (future work)
- Matches DuckDB behavior where indexes include payload data

---

## Decision 4: Index Selection in Optimizer

### Context
The optimizer must identify when indexes can be used and select the best index for each table access.

### Decision
Implement index matching and selection:

```go
// IndexMatcher finds applicable indexes for predicates
type IndexMatcher struct {
    catalog *Catalog
}

// FindApplicableIndexes returns indexes usable for the given predicates
func (m *IndexMatcher) FindApplicableIndexes(
    schema, table string,
    predicates []Expression,
) []IndexMatch {
    var matches []IndexMatch

    indexes := m.catalog.GetIndexesForTable(schema, table)

    for _, indexDef := range indexes {
        if match := m.matchIndex(indexDef, predicates); match != nil {
            matches = append(matches, *match)
        }
    }

    return matches
}

// matchIndex checks if index can satisfy any predicate
func (m *IndexMatcher) matchIndex(
    indexDef *catalog.IndexDef,
    predicates []Expression,
) *IndexMatch {
    // For single-column index: find equality predicate on that column
    // For composite index: find equality predicates on prefix columns

    matchedPredicates := []Expression{}
    lookupKeys := []Expression{}

    for i, col := range indexDef.Columns {
        pred := m.findEqualityPredicate(predicates, col)
        if pred == nil {
            break // Must match prefix continuously
        }
        matchedPredicates = append(matchedPredicates, pred)
        lookupKeys = append(lookupKeys, pred.Value)

        if i == 0 {
            // Single column match is valid for hash index
            return &IndexMatch{
                IndexDef:    indexDef,
                Predicates:  matchedPredicates,
                LookupKeys:  lookupKeys,
                Selectivity: m.estimateSelectivity(matchedPredicates),
            }
        }
    }

    if len(matchedPredicates) > 0 {
        return &IndexMatch{
            IndexDef:    indexDef,
            Predicates:  matchedPredicates,
            LookupKeys:  lookupKeys,
            Selectivity: m.estimateSelectivity(matchedPredicates),
        }
    }

    return nil
}

// IndexMatch represents a usable index with its predicates
type IndexMatch struct {
    IndexDef    *catalog.IndexDef
    Predicates  []Expression  // Predicates satisfied by index
    LookupKeys  []Expression  // Key expressions for lookup
    Selectivity float64       // Estimated fraction of rows
}
```

### Rationale
- Separates index matching logic from cost calculation
- Supports both single and composite indexes
- Prefix matching for composite indexes follows standard behavior
- Returns selectivity for cost estimation

---

## Decision 5: Composite Index Handling

### Context
Composite indexes (multiple columns) require special handling for predicate matching.

### Decision
Implement prefix-based matching for composite indexes:

```go
// For index on (a, b, c):
// - WHERE a = 1            -> Use index, lookup (1)
// - WHERE a = 1 AND b = 2  -> Use index, lookup (1, 2)
// - WHERE b = 2            -> Cannot use index (not prefix)
// - WHERE a = 1 AND c = 3  -> Use index for (1), filter c separately

func (m *IndexMatcher) matchCompositeIndex(
    indexDef *catalog.IndexDef,
    predicates []Expression,
) *IndexMatch {
    matchedCols := 0
    lookupKeys := make([]Expression, 0, len(indexDef.Columns))
    matchedPreds := make([]Expression, 0)

    for _, indexCol := range indexDef.Columns {
        pred := m.findEqualityPredicate(predicates, indexCol)
        if pred == nil {
            // Break at first non-matching column
            break
        }
        lookupKeys = append(lookupKeys, pred.Value)
        matchedPreds = append(matchedPreds, pred)
        matchedCols++
    }

    if matchedCols == 0 {
        return nil
    }

    return &IndexMatch{
        IndexDef:        indexDef,
        Predicates:      matchedPreds,
        LookupKeys:      lookupKeys,
        MatchedColumns:  matchedCols,
        IsFullMatch:     matchedCols == len(indexDef.Columns),
        Selectivity:     m.estimateCompositeSelectivity(matchedPreds),
    }
}
```

### Rationale
- Prefix matching is standard for hash/tree indexes
- Partial matches still provide benefit
- Remaining predicates applied as filter after index scan
- Selectivity combines matched predicate selectivities

---

## Decision 6: Integration with Cost-Based Optimizer

### Context
Index scan must integrate with the GAP-002 cost-based optimizer framework.

### Decision
Add index scan enumeration to optimizer:

```go
// Modified optimizer to enumerate index scan alternatives
func (o *CostBasedOptimizer) enumerateAccessMethods(
    scan *LogicalScan,
    filter *LogicalFilter,
) []PhysicalPlan {
    var alternatives []PhysicalPlan

    // Always include sequential scan as baseline
    seqScan := &PhysicalSeqScan{
        TableName: scan.TableName,
        Schema:    scan.Schema,
    }
    if filter != nil {
        seqScan = seqScan.WithFilter(filter.Condition)
    }
    alternatives = append(alternatives, seqScan)

    // Find applicable indexes
    if filter != nil {
        matches := o.indexMatcher.FindApplicableIndexes(
            scan.Schema,
            scan.TableName,
            extractPredicates(filter.Condition),
        )

        for _, match := range matches {
            isCovering := IsCoveringIndex(match.IndexDef, o.getRequiredColumns(scan))

            indexScan := &PhysicalIndexScan{
                TableName:   scan.TableName,
                Schema:      scan.Schema,
                IndexName:   match.IndexDef.Name,
                IndexDef:    match.IndexDef,
                LookupKeys:  match.LookupKeys,
                IsIndexOnly: isCovering,
            }

            // Add remaining filter if index doesn't cover all predicates
            if remainingPreds := getRemainingPredicates(filter, match.Predicates); len(remainingPreds) > 0 {
                indexScan = indexScan.WithFilter(combinePredicates(remainingPreds))
            }

            alternatives = append(alternatives, indexScan)
        }
    }

    return alternatives
}

// selectBestAccessMethod picks cheapest access method
func (o *CostBasedOptimizer) selectBestAccessMethod(
    alternatives []PhysicalPlan,
    stats *TableStatistics,
) PhysicalPlan {
    var best PhysicalPlan
    var bestCost float64 = math.MaxFloat64

    for _, alt := range alternatives {
        cost := o.costModel.EstimateCost(alt, stats)
        if cost.TotalCost < bestCost {
            bestCost = cost.TotalCost
            best = alt
        }
    }

    return best
}
```

### Rationale
- Integrates naturally with existing optimizer structure
- Sequential scan always considered as baseline
- Multiple index scans enumerated if multiple indexes match
- Cost model selects cheapest option

---

## File Organization

```
internal/
|-- optimizer/
|   |-- index_matcher.go    // Index matching logic
|   |-- cost_model.go       // Extended with index scan costs
|   |-- optimizer.go        // Modified access method enumeration
|-- executor/
|   |-- index_scan.go       // PhysicalIndexScan operator
|-- planner/
|   |-- physical.go         // Add IndexScan generation
|-- storage/
|   |-- index.go            // Existing HashIndex (unchanged)
```

---

## Test Strategy

1. **Unit Tests**: Index matching, cost estimation, covering index detection
2. **Integration Tests**: End-to-end query execution with index usage
3. **Cost Comparison Tests**: Verify index chosen only when cheaper
4. **Performance Tests**: Measure speedup vs sequential scan
5. **Composite Index Tests**: Verify prefix matching behavior

### Test Scenarios

```sql
-- Simple indexed lookup
CREATE INDEX idx_id ON users(id);
SELECT * FROM users WHERE id = 42;
-- Expected: IndexScan on idx_id

-- Index not used (no matching predicate)
SELECT * FROM users WHERE name = 'Alice';
-- Expected: SeqScan (no index on name)

-- Index-only scan
CREATE INDEX idx_id_name ON users(id, name);
SELECT id, name FROM users WHERE id = 42;
-- Expected: IndexScan with IsIndexOnly=true

-- Composite index prefix
CREATE INDEX idx_abc ON t(a, b, c);
SELECT * FROM t WHERE a = 1 AND b = 2;
-- Expected: IndexScan with LookupKeys=[1, 2]

-- Large table selectivity test
-- Verify index used for small selectivity, seq scan for large
```
