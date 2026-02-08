package rules

import "github.com/dukdb/dukdb-go/internal/planner/rewrite"

// JoinReorderingRule reorders joins based on estimated cost.
type JoinReorderingRule struct{}

func (JoinReorderingRule) Name() string { return "join_reordering" }

func (JoinReorderingRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.JoinReordering {
		return plan, false
	}
	return reorderJoins(plan, ctx)
}

func reorderJoins(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	left, right, joinType, cond, ok := ctx.Adapter.AsJoin(plan)
	if ok {
		newLeft, leftChanged := reorderJoins(left, ctx)
		newRight, rightChanged := reorderJoins(right, ctx)
		changed := leftChanged || rightChanged
		newJoin := ctx.Adapter.NewJoin(newLeft, newRight, joinType, cond)
		if joinType != rewrite.JoinTypeInner && joinType != rewrite.JoinTypeCross {
			return newJoin, changed
		}
		if ctx.Estimator == nil {
			return newJoin, changed
		}
		leftCost := ctx.Estimator.Estimate(newLeft)
		rightCost := ctx.Estimator.Estimate(newRight)
		if leftCost > rightCost*1.1 {
			return ctx.Adapter.NewJoin(newRight, newLeft, joinType, cond), true
		}
		return newJoin, changed
	}

	children := ctx.Adapter.Children(plan)
	if len(children) == 0 {
		return plan, false
	}
	changed := false
	newChildren := make([]rewrite.Plan, len(children))
	for i, child := range children {
		rewritten, childChanged := reorderJoins(child, ctx)
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
