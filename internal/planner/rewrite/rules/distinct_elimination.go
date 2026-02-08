package rules

import "github.com/dukdb/dukdb-go/internal/planner/rewrite"

// DistinctEliminationRule removes redundant DISTINCT operations.
type DistinctEliminationRule struct{}

func (DistinctEliminationRule) Name() string { return "distinct_elimination" }

func (DistinctEliminationRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.DistinctElimination {
		return plan, false
	}
	return eliminateDistinct(plan, ctx)
}

func eliminateDistinct(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	if child, ok := ctx.Adapter.AsDistinct(plan); ok {
		if _, innerOK := ctx.Adapter.AsDistinct(child); innerOK {
			return child, true
		}
	}

	children := ctx.Adapter.Children(plan)
	if len(children) == 0 {
		return plan, false
	}
	changed := false
	newChildren := make([]rewrite.Plan, len(children))
	for i, child := range children {
		rewritten, childChanged := eliminateDistinct(child, ctx)
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
