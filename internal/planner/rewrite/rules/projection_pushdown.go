package rules

import (
	"sort"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// ProjectionPushdownRule pushes projections to scans.
type ProjectionPushdownRule struct{}

func (ProjectionPushdownRule) Name() string { return "projection_pushdown" }

func (ProjectionPushdownRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ProjectionPushdown {
		return plan, false
	}
	return pushdownProjection(plan, ctx)
}

func pushdownProjection(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	child, exprs, aliases, ok := ctx.Adapter.AsProject(plan)
	if !ok {
		children := ctx.Adapter.Children(plan)
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]rewrite.Plan, len(children))
		for i, ch := range children {
			rewritten, childChanged := pushdownProjection(ch, ctx)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return ctx.Adapter.ReplaceChildren(plan, newChildren), true
	}

	scanPlan, filterExpr, ok := unwrapFilterForScan(child, ctx)
	if !ok {
		rewrittenChild, childChanged := pushdownProjection(child, ctx)
		if !childChanged {
			return plan, false
		}
		return ctx.Adapter.NewProject(rewrittenChild, exprs, aliases), true
	}

	_, _, _, currentProj, columns, ok := ctx.Adapter.AsScan(scanPlan)
	if !ok {
		return plan, false
	}

	required, ok := requiredColumns(exprs, filterExpr, columns)
	if !ok || len(required) == 0 {
		return plan, false
	}

	if sameProjections(required, currentProj) {
		return plan, false
	}

	newScan := ctx.Adapter.WithScanProjections(scanPlan, required)
	if filterExpr != nil {
		newScan = ctx.Adapter.NewFilter(newScan, filterExpr)
	}
	return ctx.Adapter.NewProject(newScan, exprs, aliases), true
}

func unwrapFilterForScan(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, binder.BoundExpr, bool) {
	if _, _, _, _, _, ok := ctx.Adapter.AsScan(plan); ok {
		return plan, nil, true
	}
	if child, cond, ok := ctx.Adapter.AsFilter(plan); ok {
		if _, _, _, _, _, ok := ctx.Adapter.AsScan(child); ok {
			return child, cond, true
		}
	}
	return nil, nil, false
}

func requiredColumns(exprs []binder.BoundExpr, filterExpr binder.BoundExpr, columns []rewrite.Column) ([]int, bool) {
	needed := make(map[int]struct{})
	nameCounts := make(map[string]int)
	for _, col := range columns {
		nameCounts[col.Column]++
	}

	ok := true
	collect := func(expr binder.BoundExpr) {
		rewrite.WalkExpr(expr, func(node binder.BoundExpr) {
			ref, okRef := node.(*binder.BoundColumnRef)
			if !okRef {
				return
			}
			idx, found := findColumnIndex(ref, columns, nameCounts)
			if !found {
				ok = false
				return
			}
			needed[idx] = struct{}{}
		})
	}

	for _, expr := range exprs {
		collect(expr)
		// Also collect outer columns referenced by correlated subqueries embedded in the
		// project expressions (e.g. scalar subqueries with BoundCorrelatedColumnRef).
		collectCorrelatedFromSubquery(expr, columns, nameCounts, needed)
	}
	if filterExpr != nil {
		collect(filterExpr)
	}

	if !ok {
		return nil, false
	}

	result := make([]int, 0, len(needed))
	for idx := range needed {
		result = append(result, idx)
	}
	sort.Ints(result)
	return result, true
}

// collectCorrelatedFromSubquery walks a BoundSelectStmt embedded as an expression
// (scalar subquery) and collects any BoundCorrelatedColumnRef nodes that reference
// outer-scope columns. These columns must be present in the outer scan's projection.
func collectCorrelatedFromSubquery(expr binder.BoundExpr, columns []rewrite.Column, nameCounts map[string]int, needed map[int]struct{}) {
	stmt, ok := expr.(*binder.BoundSelectStmt)
	if !ok {
		return
	}
	visitCorrelated := func(node binder.BoundExpr) {
		ref, okRef := node.(*binder.BoundCorrelatedColumnRef)
		if !okRef {
			return
		}
		// Find the outer column that this correlated ref points to
		for _, col := range columns {
			if ref.Table != "" {
				if col.Table == ref.Table && col.Column == ref.Column {
					needed[col.ColumnIdx] = struct{}{}
					return
				}
			} else if nameCounts[ref.Column] == 1 && col.Column == ref.Column {
				needed[col.ColumnIdx] = struct{}{}
				return
			}
		}
	}
	// Walk the subquery's WHERE clause and SELECT expressions for correlated refs
	walkSubqueryExprs(stmt, visitCorrelated)
}

// walkSubqueryExprs walks expressions inside a BoundSelectStmt (WHERE, SELECT columns,
// JOINs) using the rewrite.WalkExpr visitor.
func walkSubqueryExprs(stmt *binder.BoundSelectStmt, visit func(binder.BoundExpr)) {
	if stmt == nil {
		return
	}
	if stmt.Where != nil {
		rewrite.WalkExpr(stmt.Where, visit)
	}
	for _, col := range stmt.Columns {
		rewrite.WalkExpr(col.Expr, visit)
	}
	if stmt.Having != nil {
		rewrite.WalkExpr(stmt.Having, visit)
	}
}

func findColumnIndex(ref *binder.BoundColumnRef, columns []rewrite.Column, nameCounts map[string]int) (int, bool) {
	if ref.Table != "" {
		for _, col := range columns {
			if col.Table == ref.Table && col.Column == ref.Column {
				return col.ColumnIdx, true
			}
		}
		return 0, false
	}

	if nameCounts[ref.Column] != 1 {
		return 0, false
	}
	for _, col := range columns {
		if col.Column == ref.Column {
			return col.ColumnIdx, true
		}
	}
	return 0, false
}

func sameProjections(required []int, current []int) bool {
	if len(current) == 0 {
		return false
	}
	if len(required) != len(current) {
		return false
	}
	for i := range required {
		if required[i] != current[i] {
			return false
		}
	}
	return true
}
