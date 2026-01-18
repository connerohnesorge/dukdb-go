# EXPLAIN Integration Design for Index Scans

This document specifies how EXPLAIN output will display IndexScan operators, enabling users
to understand when and how their queries use indexes.

## 1. Overview

The current EXPLAIN implementation (`internal/executor/physical_maintenance.go`) formats
physical plan nodes as text with cost annotations. It handles `PhysicalScan` (sequential scan)
but does not yet handle `PhysicalIndexScan`. This design extends EXPLAIN to show:

- Index scan operator type (vs sequential scan)
- Index name being used
- Lookup keys and their values
- Residual filters that are applied post-lookup
- Cost estimates specific to index scans

### Current EXPLAIN Output Format

```
Project: 3 columns (cost=0.00..1.50 rows=100 width=32)
  Filter (cost=0.00..1.00 rows=100 width=32)
    Scan: users (cost=0.00..1.00 rows=1000 width=32) AS u
```

### Target EXPLAIN Output Format

```
Project: 3 columns (cost=0.00..0.25 rows=10 width=32)
  IndexScan: users USING idx_email (cost=0.00..0.15 rows=10 width=32) AS u
    Index Cond: (email = 'test@example.com')
```

## 2. EXPLAIN Output Format Specification

### 2.1 Basic IndexScan Format

```
IndexScan: <table_name> USING <index_name> <cost_annotation> [AS <alias>]
  Index Cond: (<lookup_conditions>)
  [Filter: (<residual_conditions>)]
```

Components:
- `IndexScan:` - Operator type identifier (vs `Scan:` for sequential)
- `<table_name>` - Name of the table being scanned
- `USING <index_name>` - Name of the index being used
- `<cost_annotation>` - Standard cost format: `(cost=startup..total rows=N width=N)`
- `AS <alias>` - Table alias (if different from table name)
- `Index Cond:` - Conditions pushed into the index lookup
- `Filter:` - Residual conditions evaluated after fetching rows

### 2.2 Scan Type Comparison

| Scan Type | Format | When Used |
|-----------|--------|-----------|
| Sequential | `Scan: table_name` | No index, full table scan |
| Index | `IndexScan: table_name USING idx_name` | Equality predicates on indexed columns |
| Index Range | `IndexRangeScan: table_name USING idx_name` | Range predicates (`<`, `>`, `BETWEEN`) |
| Index Only | `IndexOnlyScan: table_name USING idx_name` | All columns satisfied from index |

### 2.3 Information Displayed

| Component | Display | Example |
|-----------|---------|---------|
| Index Name | `USING <name>` | `USING idx_user_email` |
| Lookup Keys | `Index Cond: (col = value)` | `Index Cond: (email = 'test@example.com')` |
| Key Types | Inferred from expression | `= 'string'`, `= 42`, `= NULL` |
| Residual Filters | `Filter: (condition)` | `Filter: (age > 21)` |
| Composite Keys | `(col1, col2) = (v1, v2)` | `(a, b) = (1, 'foo')` |
| Range Bounds | `col op value` | `price >= 100 AND price < 500` |
| Cost Estimates | Standard format | `(cost=0.00..0.15 rows=10 width=32)` |

## 3. Format for Different Query Types

### 3.1 Simple Equality: WHERE id = 42

```sql
EXPLAIN SELECT * FROM users WHERE id = 42;
```

Output:
```
IndexScan: users USING pk_users_id (cost=0.00..0.01 rows=1 width=64)
  Index Cond: (id = 42)
```

### 3.2 Composite Index: WHERE a = 1 AND b = 2

```sql
CREATE INDEX idx_ab ON orders(a, b);
EXPLAIN SELECT * FROM orders WHERE a = 1 AND b = 2;
```

Output:
```
IndexScan: orders USING idx_ab (cost=0.00..0.02 rows=5 width=48)
  Index Cond: ((a = 1) AND (b = 2))
```

Alternative format for composite keys (PostgreSQL-style):
```
IndexScan: orders USING idx_ab (cost=0.00..0.02 rows=5 width=48)
  Index Cond: ((a, b) = (1, 2))
```

### 3.3 Range Scan: WHERE x BETWEEN 10 AND 100

```sql
CREATE INDEX idx_x ON data(x);
EXPLAIN SELECT * FROM data WHERE x BETWEEN 10 AND 100;
```

Output:
```
IndexRangeScan: data USING idx_x (cost=0.00..0.50 rows=90 width=32)
  Index Cond: (x >= 10 AND x <= 100)
```

Range scan with explicit bounds:
```
IndexRangeScan: data USING idx_x (cost=0.00..0.50 rows=90 width=32)
  Index Cond: ((x >= 10) AND (x <= 100))
  Bounds: [10, 100] (inclusive)
```

### 3.4 With Residual Filter: WHERE a = 1 AND b > 5 (index on (a,b))

When the index can only handle part of the predicate:

```sql
CREATE INDEX idx_ab ON data(a, b);
EXPLAIN SELECT * FROM data WHERE a = 1 AND b > 5 AND c = 'foo';
```

Output:
```
IndexRangeScan: data USING idx_ab (cost=0.00..0.20 rows=15 width=40)
  Index Cond: ((a = 1) AND (b > 5))
  Filter: (c = 'foo')
```

### 3.5 Partial Composite Index Match: WHERE a = 1 (index on (a,b))

When only a prefix of a composite index is used:

```sql
CREATE INDEX idx_ab ON data(a, b);
EXPLAIN SELECT * FROM data WHERE a = 1;
```

Output:
```
IndexScan: data USING idx_ab (cost=0.00..0.30 rows=100 width=40)
  Index Cond: (a = 1)
  Note: Using prefix of composite index
```

### 3.6 IN List: WHERE id IN (1, 2, 3)

```sql
EXPLAIN SELECT * FROM users WHERE id IN (1, 2, 3);
```

Output:
```
IndexScan: users USING pk_users_id (cost=0.00..0.03 rows=3 width=64)
  Index Cond: (id = ANY(ARRAY[1, 2, 3]))
```

Or alternatively:
```
IndexScan: users USING pk_users_id (cost=0.00..0.03 rows=3 width=64)
  Index Cond: (id IN (1, 2, 3))
  Lookups: 3
```

### 3.7 Index-Only Scan

When all required columns are in the index:

```sql
CREATE INDEX idx_email_name ON users(email, name);
EXPLAIN SELECT email, name FROM users WHERE email = 'test@example.com';
```

Output:
```
IndexOnlyScan: users USING idx_email_name (cost=0.00..0.01 rows=1 width=48)
  Index Cond: (email = 'test@example.com')
  Heap Fetches: 0 (estimated)
```

Note: Current HashIndex stores only RowIDs, so true index-only scans require
future work to store values in the index. This output format is for future use.

### 3.8 Join with Index Lookup

```sql
EXPLAIN SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.email = 'test@example.com';
```

Output:
```
NestedLoopJoin (cost=0.00..1.50 rows=10 width=128)
  -> IndexScan: users USING idx_users_email (cost=0.00..0.01 rows=1 width=64) AS u
       Index Cond: (email = 'test@example.com')
  -> IndexScan: orders USING idx_orders_user_id (cost=0.00..0.05 rows=10 width=64) AS o
       Index Cond: (user_id = u.id)
```

## 4. Cost Display for Index Scans

### 4.1 Cost Components

Index scan costs are composed of:

```
Total Cost = Startup Cost + I/O Cost + CPU Cost

Where:
  Startup Cost = IndexAccessCost (typically 0)
  I/O Cost     = EstimatedRows * RandomPageCost * IndexTupleCost
  CPU Cost     = EstimatedRows * CPUIndexTupleCost
```

### 4.2 Cost Annotation Format

Standard format (consistent with sequential scans):
```
(cost=startup..total rows=estimated_rows width=tuple_width)
```

Example:
```
IndexScan: users USING idx_email (cost=0.00..0.15 rows=10 width=64)
```

### 4.3 EXPLAIN ANALYZE Additional Metrics

For `EXPLAIN ANALYZE`, show actual vs estimated:

```
IndexScan: users USING idx_email (cost=0.00..0.15 rows=10 width=64) (actual rows=8 time=0.12ms)
  Index Cond: (email LIKE 'test%')
  Index Lookups: 8
  Heap Fetches: 8
  Rows Removed by Filter: 0
```

## 5. Integration with Existing Infrastructure

### 5.1 Files to Modify

| File | Changes |
|------|---------|
| `internal/executor/physical_maintenance.go` | Add `PhysicalIndexScan` case in `formatPhysicalPlanWithCost()` |
| `internal/executor/physical_maintenance.go` | Add `PhysicalIndexScan` case in `formatPhysicalPlanWithAnalyze()` |
| `internal/executor/physical_maintenance.go` | Update `physicalPlanAdapter.PhysicalPlanType()` |
| `internal/optimizer/cost_model.go` | Add `EstimateIndexScanCost()` integration |

### 5.2 formatPhysicalPlanWithCost Changes

Add new case for `PhysicalIndexScan`:

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

    // Format Index Cond
    if len(p.LookupKeys) > 0 {
        sb.WriteString("\n")
        sb.WriteString(fmt.Sprintf("%s  Index Cond: %s",
            prefix, formatIndexCondition(p)))
    }

    // Format residual filter
    if p.ResidualFilter != nil {
        sb.WriteString("\n")
        sb.WriteString(fmt.Sprintf("%s  Filter: %s",
            prefix, formatExpression(p.ResidualFilter)))
    }
```

### 5.3 Helper Functions

```go
// formatIndexCondition formats the index lookup conditions.
func formatIndexCondition(scan *planner.PhysicalIndexScan) string {
    if scan.IndexDef == nil || len(scan.LookupKeys) == 0 {
        return "(unknown)"
    }

    var parts []string
    indexCols := scan.IndexDef.Columns

    for i, key := range scan.LookupKeys {
        var colName string
        if i < len(indexCols) {
            colName = indexCols[i]
        } else {
            colName = fmt.Sprintf("col%d", i)
        }

        keyStr := formatExpressionValue(key)

        // Determine operator based on scan type
        op := "="
        if scan.IsRangeScan && i == len(scan.LookupKeys)-1 {
            // Last key in range scan may have different operator
            op = formatRangeOperator(scan)
        }

        parts = append(parts, fmt.Sprintf("(%s %s %s)", colName, op, keyStr))
    }

    if len(parts) == 1 {
        return parts[0]
    }
    return "(" + strings.Join(parts, " AND ") + ")"
}

// formatRangeOperator determines the operator for range scans.
func formatRangeOperator(scan *planner.PhysicalIndexScan) string {
    // Based on LowerBound/UpperBound properties
    if scan.LowerBound != nil && scan.UpperBound != nil {
        if scan.LowerInclusive && scan.UpperInclusive {
            return "BETWEEN"
        }
        return ">= AND <=" // Will be expanded in actual condition
    }
    if scan.LowerBound != nil {
        if scan.LowerInclusive {
            return ">="
        }
        return ">"
    }
    if scan.UpperBound != nil {
        if scan.UpperInclusive {
            return "<="
        }
        return "<"
    }
    return "="
}

// formatExpressionValue formats a bound expression for display.
func formatExpressionValue(expr binder.BoundExpr) string {
    switch e := expr.(type) {
    case *binder.BoundLiteral:
        return formatLiteralValue(e.Value)
    case *binder.BoundColumnRef:
        if e.Table != "" {
            return fmt.Sprintf("%s.%s", e.Table, e.Column)
        }
        return e.Column
    case *binder.BoundParameterRef:
        return fmt.Sprintf("$%d", e.Index)
    default:
        return "<expr>"
    }
}

// formatLiteralValue formats a literal value for display.
func formatLiteralValue(v any) string {
    if v == nil {
        return "NULL"
    }
    switch val := v.(type) {
    case string:
        return fmt.Sprintf("'%s'", escapeString(val))
    case int64:
        return fmt.Sprintf("%d", val)
    case float64:
        return fmt.Sprintf("%g", val)
    case bool:
        if val {
            return "true"
        }
        return "false"
    case []byte:
        return fmt.Sprintf("'\\x%x'", val)
    default:
        return fmt.Sprintf("%v", val)
    }
}

// escapeString escapes single quotes in strings for display.
func escapeString(s string) string {
    return strings.ReplaceAll(s, "'", "''")
}
```

### 5.4 physicalPlanAdapter Update

Add case for `PhysicalIndexScan`:

```go
func (a *physicalPlanAdapter) PhysicalPlanType() string {
    switch p := a.plan.(type) {
    case *planner.PhysicalScan:
        return "PhysicalScan"
    case *planner.PhysicalIndexScan:
        if p.IsIndexOnly {
            return "PhysicalIndexOnlyScan"
        }
        if p.IsRangeScan {
            return "PhysicalIndexRangeScan"
        }
        return "PhysicalIndexScan"
    // ... existing cases ...
    }
}
```

### 5.5 Cost Model Integration

The cost model should provide index-specific cost estimates:

```go
// In optimizer/cost_model.go

// EstimateIndexScanCost estimates the cost of an index scan operation.
func (m *CostModel) EstimateIndexScanCost(
    rowCount float64,
    selectivity float64,
    isIndexOnly bool,
) PlanCost {
    estimatedRows := rowCount * selectivity

    // Index lookup cost
    lookupCost := m.constants.IndexLookupCost

    // Per-tuple index access cost
    indexTupleCost := estimatedRows * m.constants.IndexTupleCost

    // Heap fetch cost (if not index-only)
    heapFetchCost := 0.0
    if !isIndexOnly {
        heapFetchCost = estimatedRows * m.constants.RandomPageCost * 0.1
    }

    totalCost := lookupCost + indexTupleCost + heapFetchCost

    return PlanCost{
        StartupCost: 0,
        TotalCost:   totalCost,
        OutputRows:  estimatedRows,
        OutputWidth: 64, // Will be computed from actual columns
    }
}
```

## 6. Extended PhysicalIndexScan Fields

To support comprehensive EXPLAIN output, `PhysicalIndexScan` may need additional fields:

```go
type PhysicalIndexScan struct {
    // Existing fields...
    Schema         string
    TableName      string
    Alias          string
    TableDef       *catalog.TableDef
    IndexName      string
    IndexDef       *catalog.IndexDef
    LookupKeys     []binder.BoundExpr
    Projections    []int
    IsIndexOnly    bool
    ResidualFilter binder.BoundExpr

    // New fields for EXPLAIN support
    IsRangeScan     bool              // True for range predicates
    LowerBound      binder.BoundExpr  // Lower bound for range scan
    UpperBound      binder.BoundExpr  // Upper bound for range scan
    LowerInclusive  bool              // Lower bound inclusive
    UpperInclusive  bool              // Upper bound inclusive
    ScanDirection   string            // "Forward" or "Backward"

    // For EXPLAIN ANALYZE
    actualRows      int64             // Actual rows returned (set at execution)
    indexLookups    int64             // Number of index lookups performed
    heapFetches     int64             // Number of heap fetches
    rowsFiltered    int64             // Rows removed by residual filter
}
```

## 7. Test Cases

### 7.1 Unit Tests

```go
func TestExplainIndexScan(t *testing.T) {
    testCases := []struct {
        name     string
        plan     *planner.PhysicalIndexScan
        expected string
    }{
        {
            name: "simple equality",
            plan: &planner.PhysicalIndexScan{
                TableName: "users",
                IndexName: "idx_email",
                LookupKeys: []binder.BoundExpr{
                    &binder.BoundLiteral{Value: "test@example.com"},
                },
            },
            expected: `IndexScan: users USING idx_email (cost=0.00..0.01 rows=1 width=64)
  Index Cond: (email = 'test@example.com')`,
        },
        {
            name: "composite key",
            plan: &planner.PhysicalIndexScan{
                TableName: "orders",
                IndexName: "idx_ab",
                IndexDef:  &catalog.IndexDef{Columns: []string{"a", "b"}},
                LookupKeys: []binder.BoundExpr{
                    &binder.BoundLiteral{Value: int64(1)},
                    &binder.BoundLiteral{Value: int64(2)},
                },
            },
            expected: `IndexScan: orders USING idx_ab (cost=0.00..0.02 rows=5 width=48)
  Index Cond: ((a = 1) AND (b = 2))`,
        },
        {
            name: "with residual filter",
            plan: &planner.PhysicalIndexScan{
                TableName:      "data",
                IndexName:      "idx_a",
                LookupKeys:     []binder.BoundExpr{&binder.BoundLiteral{Value: int64(1)}},
                ResidualFilter: &binder.BoundBinaryExpr{...},
            },
            expected: `IndexScan: data USING idx_a (cost=0.00..0.10 rows=20 width=32)
  Index Cond: (a = 1)
  Filter: (b > 5)`,
        },
        {
            name: "range scan",
            plan: &planner.PhysicalIndexScan{
                TableName:      "data",
                IndexName:      "idx_x",
                IsRangeScan:    true,
                LowerBound:     &binder.BoundLiteral{Value: int64(10)},
                UpperBound:     &binder.BoundLiteral{Value: int64(100)},
                LowerInclusive: true,
                UpperInclusive: true,
            },
            expected: `IndexRangeScan: data USING idx_x (cost=0.00..0.50 rows=90 width=32)
  Index Cond: (x >= 10 AND x <= 100)`,
        },
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            result := formatPhysicalPlanWithCost(tc.plan, 0, costModel)
            assert.Equal(t, tc.expected, result)
        })
    }
}
```

### 7.2 Integration Tests

```go
func TestExplainIndexUsage(t *testing.T) {
    db := setupTestDB(t)

    // Create table and index
    _, err := db.Exec(`
        CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR, name VARCHAR);
        CREATE INDEX idx_email ON users(email);
        INSERT INTO users VALUES (1, 'test@example.com', 'Test User');
    `)
    require.NoError(t, err)

    // Test EXPLAIN shows index usage
    rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE email = 'test@example.com'")
    require.NoError(t, err)
    defer rows.Close()

    var plan string
    for rows.Next() {
        err := rows.Scan(&plan)
        require.NoError(t, err)
    }

    assert.Contains(t, plan, "IndexScan")
    assert.Contains(t, plan, "idx_email")
    assert.Contains(t, plan, "email = 'test@example.com'")
}

func TestExplainSequentialFallback(t *testing.T) {
    db := setupTestDB(t)

    // Create table without index
    _, err := db.Exec(`
        CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR);
    `)
    require.NoError(t, err)

    // EXPLAIN should show Scan (not IndexScan) for non-indexed column
    rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE email = 'test@example.com'")
    require.NoError(t, err)
    defer rows.Close()

    var plan string
    for rows.Next() {
        rows.Scan(&plan)
    }

    assert.Contains(t, plan, "Scan:")
    assert.NotContains(t, plan, "IndexScan")
}
```

## 8. Implementation Checklist

### Phase 1: Basic IndexScan Display
- [ ] Add `PhysicalIndexScan` case to `formatPhysicalPlanWithCost()`
- [ ] Implement `formatIndexCondition()` helper
- [ ] Implement `formatExpressionValue()` helper
- [ ] Update `physicalPlanAdapter.PhysicalPlanType()`
- [ ] Add unit tests for basic equality scans

### Phase 2: Range Scan Display
- [ ] Add range scan detection logic
- [ ] Implement `formatRangeOperator()` helper
- [ ] Add `IndexRangeScan` display type
- [ ] Add unit tests for range scans

### Phase 3: Residual Filter Display
- [ ] Add residual filter output
- [ ] Implement expression formatting for filters
- [ ] Add unit tests for scans with residual filters

### Phase 4: Composite Index Display
- [ ] Handle multi-column lookup keys
- [ ] Format composite conditions appropriately
- [ ] Add unit tests for composite indexes

### Phase 5: EXPLAIN ANALYZE Support
- [ ] Add `PhysicalIndexScan` case to `formatPhysicalPlanWithAnalyze()`
- [ ] Add metrics collection during index scan execution
- [ ] Display actual vs estimated rows
- [ ] Add integration tests for EXPLAIN ANALYZE

### Phase 6: Index-Only Scan Display
- [ ] Add `IndexOnlyScan` display type
- [ ] Show heap fetch count (0 for true index-only)
- [ ] Future: Update when HashIndex supports value storage

## 9. References

- Current EXPLAIN implementation: `internal/executor/physical_maintenance.go` (lines 419-728)
- PhysicalIndexScan struct: `internal/planner/physical.go` (lines 70-143)
- Cost model: `internal/optimizer/cost_model.go`
- PhysicalIndexScan Design: `spectr/changes/fix-index-usage/PHYSICAL_INDEX_SCAN_DESIGN.md`
- ART Range Scan Design: `spectr/changes/fix-index-usage/ART_RANGE_SCAN_DESIGN.md`
- Hints Passing Design: `spectr/changes/fix-index-usage/HINTS_PASSING_DESIGN.md`
