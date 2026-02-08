package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// FilterPushdownRule pushes filters closer to data sources.
type FilterPushdownRule struct{}

func (FilterPushdownRule) Name() string { return "filter_pushdown" }

func (FilterPushdownRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.PredicatePushdown {
		return plan, false
	}
	return pushdownFilters(plan, ctx)
}

func pushdownFilters(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	if child, cond, ok := ctx.Adapter.AsFilter(plan); ok {
		rewrittenChild, childChanged := pushdownFilters(child, ctx)
		if innerChild, innerCond, ok := ctx.Adapter.AsFilter(rewrittenChild); ok {
			combined := combineAND(cond, innerCond)
			return ctx.Adapter.NewFilter(innerChild, combined), true
		}

		if left, right, joinType, joinCond, ok := ctx.Adapter.AsJoin(rewrittenChild); ok {
			if joinType == rewrite.JoinTypeInner || joinType == rewrite.JoinTypeCross {
				return pushFilterIntoJoin(cond, left, right, joinType, joinCond, ctx)
			}
		}

		if childChanged {
			return ctx.Adapter.NewFilter(rewrittenChild, cond), true
		}
		return plan, false
	}

	children := ctx.Adapter.Children(plan)
	if len(children) == 0 {
		return plan, false
	}
	changed := false
	newChildren := make([]rewrite.Plan, len(children))
	for i, child := range children {
		rewritten, childChanged := pushdownFilters(child, ctx)
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

func pushFilterIntoJoin(cond binder.BoundExpr, left, right rewrite.Plan, joinType rewrite.JoinType, joinCond binder.BoundExpr, ctx *rewrite.Context) (rewrite.Plan, bool) {
	conjuncts := splitAndConjuncts(cond)
	leftTables := planTableSet(left, ctx)
	rightTables := planTableSet(right, ctx)
	var leftFilters []binder.BoundExpr
	var rightFilters []binder.BoundExpr
	var keep []binder.BoundExpr

	for _, part := range conjuncts {
		tables, unknown := exprTables(part)
		if unknown || len(tables) == 0 {
			keep = append(keep, part)
			continue
		}
		if isSubset(tables, leftTables) {
			leftFilters = append(leftFilters, part)
		} else if isSubset(tables, rightTables) {
			rightFilters = append(rightFilters, part)
		} else {
			keep = append(keep, part)
		}
	}

	if len(leftFilters) > 0 {
		left = addFilter(left, combineANDList(leftFilters), ctx)
	}
	if len(rightFilters) > 0 {
		right = addFilter(right, combineANDList(rightFilters), ctx)
	}

	newJoin := ctx.Adapter.NewJoin(left, right, joinType, joinCond)
	if len(keep) == 0 {
		return newJoin, true
	}
	return ctx.Adapter.NewFilter(newJoin, combineANDList(keep)), true
}

func addFilter(plan rewrite.Plan, cond binder.BoundExpr, ctx *rewrite.Context) rewrite.Plan {
	if cond == nil {
		return plan
	}
	if child, existing, ok := ctx.Adapter.AsFilter(plan); ok {
		return ctx.Adapter.NewFilter(child, combineAND(existing, cond))
	}
	return ctx.Adapter.NewFilter(plan, cond)
}

func splitAndConjuncts(expr binder.BoundExpr) []binder.BoundExpr {
	if bin, ok := expr.(*binder.BoundBinaryExpr); ok && bin.Op == parser.OpAnd {
		left := splitAndConjuncts(bin.Left)
		right := splitAndConjuncts(bin.Right)
		return append(left, right...)
	}
	return []binder.BoundExpr{expr}
}

func combineAND(left, right binder.BoundExpr) binder.BoundExpr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &binder.BoundBinaryExpr{Left: left, Op: parser.OpAnd, Right: right, ResType: dukdb.TYPE_BOOLEAN}
}

func combineANDList(exprs []binder.BoundExpr) binder.BoundExpr {
	if len(exprs) == 0 {
		return nil
	}
	result := exprs[0]
	for i := 1; i < len(exprs); i++ {
		result = combineAND(result, exprs[i])
	}
	return result
}

func exprTables(expr binder.BoundExpr) (map[string]struct{}, bool) {
	tables := make(map[string]struct{})
	unknown := false
	rewrite.WalkExpr(expr, func(node binder.BoundExpr) {
		if col, ok := node.(*binder.BoundColumnRef); ok {
			if col.Table == "" {
				unknown = true
				return
			}
			tables[col.Table] = struct{}{}
		}
	})
	return tables, unknown
}

func planTableSet(plan rewrite.Plan, ctx *rewrite.Context) map[string]struct{} {
	tables := make(map[string]struct{})
	if ctx == nil || ctx.Adapter == nil {
		return tables
	}
	for _, col := range ctx.Adapter.OutputColumns(plan) {
		if col.Table != "" {
			tables[col.Table] = struct{}{}
		}
	}
	return tables
}

func isSubset(subset, set map[string]struct{}) bool {
	for key := range subset {
		if _, ok := set[key]; !ok {
			return false
		}
	}
	return true
}
