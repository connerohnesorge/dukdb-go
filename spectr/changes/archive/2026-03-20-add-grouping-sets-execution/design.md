# GROUPING SETS/ROLLUP/CUBE Execution Design

## Overview

This document describes how GROUPING SETS, ROLLUP, and CUBE are parsed, bound, planned, and executed in dukdb-go. The core strategy is: ROLLUP and CUBE are syntactic sugar expanded to GROUPING SETS in the binder; the executor runs one aggregate pass per grouping set and UNION ALLs the results.

## Current Architecture

### Parser

AST nodes in `internal/parser/ast_grouping.go`:

- `GroupingSetExpr` - holds `Type` (Simple/Rollup/Cube) and `Exprs [][]Expr` (list of sets, each a list of column expressions)
- `RollupExpr` - convenience wrapper with `Exprs []Expr`
- `CubeExpr` - convenience wrapper with `Exprs []Expr`

The parser in `internal/parser/parser.go` recognizes `GROUPING SETS(...)`, `ROLLUP(...)`, and `CUBE(...)` in GROUP BY clauses.

### Binder: ROLLUP/CUBE Expansion

The binder expands ROLLUP and CUBE into explicit grouping sets at bind time. This happens in `internal/binder/bind_expr.go`:

```go
// bindRollupExpr expands ROLLUP(a, b, c) to:
//   sets[0] = [a, b, c]  -- full grouping
//   sets[1] = [a, b]     -- drop last
//   sets[2] = [a]        -- drop last two
//   sets[3] = []          -- grand total (empty set)
func (b *Binder) bindRollupExpr(e *parser.RollupExpr) (*BoundGroupingSetExpr, error) {
    boundExprs := bindAll(e.Exprs)
    numExprs := len(boundExprs)
    sets := make([][]BoundExpr, numExprs+1)
    for i := 0; i <= numExprs; i++ {
        setSize := numExprs - i
        set := make([]BoundExpr, setSize)
        copy(set, boundExprs[:setSize])
        sets[i] = set
    }
    return &BoundGroupingSetExpr{Type: BoundGroupingSetRollup, Sets: sets}, nil
}

// bindCubeExpr expands CUBE(a, b) to all 2^n combinations:
//   mask=11 -> [a, b], mask=10 -> [a], mask=01 -> [b], mask=00 -> []
func (b *Binder) bindCubeExpr(e *parser.CubeExpr) (*BoundGroupingSetExpr, error) {
    boundExprs := bindAll(e.Exprs)
    numExprs := len(boundExprs)
    numSets := 1 << numExprs
    sets := make([][]BoundExpr, 0, numSets)
    for mask := numSets - 1; mask >= 0; mask-- {
        set := make([]BoundExpr, 0)
        for i := 0; i < numExprs; i++ {
            bitPos := numExprs - 1 - i
            if (mask & (1 << bitPos)) != 0 {
                set = append(set, boundExprs[i])
            }
        }
        sets = append(sets, set)
    }
    return &BoundGroupingSetExpr{Type: BoundGroupingSetCube, Sets: sets}, nil
}
```

After binding, ROLLUP and CUBE are indistinguishable from explicit GROUPING SETS -- they are all `BoundGroupingSetExpr` with a `Sets [][]BoundExpr` field.

### Planner: GroupingSetsPlan Logical Node Structure

The planner does not introduce a separate `LogicalGroupingSets` node. Instead, the existing `LogicalAggregate` carries grouping sets information:

```go
type LogicalAggregate struct {
    Child        LogicalPlan
    GroupBy      []binder.BoundExpr       // All unique columns across all grouping sets
    Aggregates   []binder.BoundExpr       // Aggregate functions (SUM, COUNT, etc.)
    Aliases      []string
    GroupingSets [][]binder.BoundExpr     // The expanded grouping sets
    GroupingCalls []*binder.BoundGroupingCall  // GROUPING() calls in SELECT
}
```

The `extractGroupingSets` function in `internal/planner/physical.go` takes the GROUP BY list, finds any `BoundGroupingSetExpr`, extracts its sets into `GroupingSets`, and collects all unique columns into `GroupBy`. This same structure flows through to `PhysicalHashAggregate`.

### Execution Strategy

The executor in `executeHashAggregateWithGroupingSets` uses a "per-set aggregate + UNION ALL" approach:

```
For each grouping_set in plan.GroupingSets:
  1. Build inGroupingSet map (which columns are active in this set)
  2. Group all child rows by ONLY the columns in this grouping set
  3. For each group:
     a. Emit GROUP BY columns: active columns get their value, inactive get NULL
     b. Compute aggregates (SUM, COUNT, etc.) over the group's rows
     c. Compute GROUPING() bitmasks
  4. Append all group rows to the unified result
```

The key executor function:

```go
func (e *Executor) executeHashAggregateWithGroupingSets(
    ctx *ExecutionContext,
    plan *planner.PhysicalHashAggregate,
    childResult *ExecutionResult,
) (*ExecutionResult, error) {
    numGroupBy := len(plan.GroupBy)
    numAgg := len(plan.Aggregates)
    numGroupingCalls := len(plan.GroupingCalls)
    columns := make([]string, numGroupBy+numAgg+numGroupingCalls)

    // ... column name setup ...

    result := &ExecutionResult{
        Columns: columns,
        Rows:    make([]map[string]any, 0),
    }

    for _, groupingSet := range plan.GroupingSets {
        // Determine which GROUP BY columns are active
        inGroupingSet := make(map[string]bool)
        for _, expr := range groupingSet {
            key := getGroupByExprKey(expr)
            inGroupingSet[key] = true
        }

        // Hash-group child rows by active columns only
        type groupKey string
        groups := make(map[groupKey][]map[string]any)
        groupOrder := make([]groupKey, 0)

        for _, row := range childResult.Rows {
            keyParts := make([]any, len(groupingSet))
            for i, expr := range groupingSet {
                val, _ := e.evaluateExpr(ctx, expr, row)
                keyParts[i] = val
            }
            key := groupKey(formatGroupKey(keyParts))
            if _, exists := groups[key]; !exists {
                groupOrder = append(groupOrder, key)
            }
            groups[key] = append(groups[key], row)
        }

        // Empty grouping set (grand total): one group with all rows
        if len(groups) == 0 && len(groupingSet) == 0 {
            groupOrder = append(groupOrder, "")
            groups[""] = childResult.Rows
        }

        for _, key := range groupOrder {
            groupRows := groups[key]
            row := make(map[string]any)

            // GROUP BY columns: active = value, inactive = NULL
            for j, expr := range plan.GroupBy {
                exprKey := getGroupByExprKey(expr)
                if inGroupingSet[exprKey] {
                    val, _ := e.evaluateExpr(ctx, expr, groupRows[0])
                    row[columns[j]] = val
                } else {
                    row[columns[j]] = nil
                }
            }

            // Aggregates
            for j, expr := range plan.Aggregates {
                val, _ := e.computeAggregate(ctx, expr, groupRows)
                row[columns[numGroupBy+j]] = val
            }

            // GROUPING() bitmasks
            for j, gc := range plan.GroupingCalls {
                bitmask := e.computeGroupingBitmask(gc, plan.GroupBy, inGroupingSet)
                row[columns[numGroupBy+numAgg+j]] = bitmask
            }

            result.Rows = append(result.Rows, row)
        }
    }

    return result, nil
}
```

### GROUPING() Function Implementation

`GROUPING(col1, col2, ...)` returns an integer bitmask. For each argument (left to right, MSB first):
- Bit = 1 if the column is NOT in the current grouping set (i.e., aggregated/NULL)
- Bit = 0 if the column IS in the current grouping set (i.e., grouped)

```go
func (e *Executor) computeGroupingBitmask(
    gc *binder.BoundGroupingCall,
    allGroupBy []binder.BoundExpr,
    inGroupingSet map[string]bool,
) int64 {
    var bitmask int64 = 0
    for i, arg := range gc.Args {
        var key string
        if arg.Table != "" {
            key = arg.Table + "." + arg.Column
        } else {
            key = arg.Column
        }
        if !inGroupingSet[key] {
            // Column is aggregated (NULL) -- bit = 1
            bitmask |= 1 << (len(gc.Args) - 1 - i)
        }
    }
    return bitmask
}
```

Example trace for `GROUPING(region, product)`:

| Grouping Set | region in set? | product in set? | Bitmask (binary) | Bitmask (int) |
|---|---|---|---|---|
| (region, product) | yes | yes | 00 | 0 |
| (region) | yes | no | 01 | 1 |
| (product) | no | yes | 10 | 2 |
| () | no | no | 11 | 3 |

## Context

This change completes execution support for a SQL standard feature that is already parsed and bound. The planner and executor scaffolding is in place; the work focuses on fixing a column naming bug in GROUPING() output, completing mixed GROUP BY support in the planner, and adding test coverage.

## Goals / Non-Goals

**Goals:**
- Correct execution of GROUPING SETS, ROLLUP, and CUBE for all standard SQL patterns
- Correct GROUPING() bitmask computation
- Integration tests covering the full parser-to-result pipeline
- Handle edge cases: NULL GROUP BY keys, mixed regular + grouping set GROUP BY

**Non-Goals:**
- Optimized execution (e.g., sharing partial aggregates across grouping sets)
- Parallel execution of grouping sets
- GROUPING_ID() function (non-standard, can be added later)
- Window functions combined with grouping sets

## Decisions

### Decision 1: No separate LogicalGroupingSets node

The existing `LogicalAggregate` with `GroupingSets` field is sufficient. Adding a separate node would require planner and optimizer changes with no functional benefit.

**Alternatives considered:** Dedicated `LogicalGroupingSets` node wrapping multiple `LogicalAggregate` children. Rejected because it would complicate the optimizer and the current approach already works.

### Decision 2: Per-set aggregate with UNION ALL

Each grouping set runs its own aggregate pass over the full child result. This is simple and correct.

**Alternatives considered:** Single-pass aggregate that tracks multiple group key spaces simultaneously. Rejected for initial implementation due to complexity; can be added as a performance optimization later.

### Decision 3: GROUPING() column naming fix

The executor loop for GROUPING() columns (operator.go:1356-1359) hardcodes `"GROUPING"` as the column name for every GROUPING() call. When multiple GROUPING() calls appear in the SELECT list, they all map to the same key in the row map, causing data loss. The fix uses `plan.Aliases[numGroupBy+numAgg+i]` to assign the correct alias from the planner.

Note: The empty grouping set (grand total) handling is already correct. The `groups` map is re-initialized per grouping set iteration, so the `len(groups) == 0 && len(groupingSet) == 0` check correctly triggers for the empty set regardless of other sets.

### Decision 4: Mixed GROUP BY with GROUPING SETS

When a query combines regular GROUP BY columns with a GROUPING SETS/ROLLUP/CUBE expression (e.g., `GROUP BY department, ROLLUP(region, product)`), the regular columns must be prepended to each expanded grouping set. The `extractGroupingSets` function currently collects regular columns and grouping set columns separately without merging them. The fix prepends regular columns to every set in the expanded grouping sets list.

## Risks / Trade-offs

- **Performance:** Running N aggregate passes for N grouping sets is O(N * R) where R is the number of input rows. For large datasets with many grouping sets this can be slow. Acceptable for correctness-first approach; optimization is a future concern.
- **Memory:** The full child result is materialized in memory before grouping. This matches the existing aggregate execution pattern.

## Open Questions

None. The design follows established patterns in the codebase.
