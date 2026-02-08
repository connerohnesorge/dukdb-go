package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// SubqueryUnnestRule converts simple IN/EXISTS subqueries to joins.
type SubqueryUnnestRule struct{}

func (SubqueryUnnestRule) Name() string { return "subquery_unnest" }

func (SubqueryUnnestRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || ctx.PlanSubquery == nil || !ctx.Config.SubqueryUnnesting {
		return plan, false
	}
	return unnestSubqueries(plan, ctx)
}

func unnestSubqueries(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	if child, cond, ok := ctx.Adapter.AsFilter(plan); ok {
		rewrittenChild, childChanged := unnestSubqueries(child, ctx)
		conjuncts := splitAndConjuncts(cond)
		var remaining []binder.BoundExpr
		changed := childChanged
		currentPlan := rewrittenChild

		for _, conjunct := range conjuncts {
			rewritten, ok := unnestSubqueryExpr(conjunct, currentPlan, ctx)
			if ok {
				currentPlan = rewritten
				changed = true
				continue
			}
			remaining = append(remaining, conjunct)
		}

		if !changed {
			return plan, false
		}
		if len(remaining) == 0 {
			return currentPlan, true
		}
		return ctx.Adapter.NewFilter(currentPlan, combineANDList(remaining)), true
	}

	children := ctx.Adapter.Children(plan)
	if len(children) == 0 {
		return plan, false
	}
	changed := false
	newChildren := make([]rewrite.Plan, len(children))
	for i, child := range children {
		rewritten, childChanged := unnestSubqueries(child, ctx)
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

func unnestSubqueryExpr(expr binder.BoundExpr, plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	switch sub := expr.(type) {
	case *binder.BoundExistsExpr:
		if isCorrelatedSubquery(sub.Subquery) {
			return nil, false
		}
		right, err := ctx.PlanSubquery(sub.Subquery)
		if err != nil {
			return nil, false
		}
		joinType := rewrite.JoinTypeSemi
		if sub.Not {
			joinType = rewrite.JoinTypeAnti
		}
		return ctx.Adapter.NewJoin(plan, right, joinType, nil), true
	case *binder.BoundInSubqueryExpr:
		if isCorrelatedSubquery(sub.Subquery) {
			return nil, false
		}
		right, err := ctx.PlanSubquery(sub.Subquery)
		if err != nil {
			return nil, false
		}
		rightCols := ctx.Adapter.OutputColumns(right)
		if len(rightCols) == 0 {
			return nil, false
		}
		rightRef := &binder.BoundColumnRef{
			Table:   rightCols[0].Table,
			Column:  rightCols[0].Column,
			ColType: rightCols[0].Type,
		}
		joinCond := &binder.BoundBinaryExpr{Left: sub.Expr, Op: parser.OpEq, Right: rightRef, ResType: dukdb.TYPE_BOOLEAN}
		joinType := rewrite.JoinTypeSemi
		if sub.Not {
			joinType = rewrite.JoinTypeAnti
		}
		return ctx.Adapter.NewJoin(plan, right, joinType, joinCond), true
	}
	return nil, false
}

func isCorrelatedSubquery(stmt *binder.BoundSelectStmt) bool {
	if stmt == nil {
		return false
	}
	allowed := make(map[string]struct{})
	for _, ref := range stmt.From {
		addTableAlias(allowed, ref)
	}
	for _, join := range stmt.Joins {
		addTableAlias(allowed, join.Table)
	}

	correlated := false
	for _, col := range stmt.Columns {
		rewrite.WalkExpr(col.Expr, func(expr binder.BoundExpr) {
			if ref, ok := expr.(*binder.BoundColumnRef); ok {
				if ref.Table != "" {
					if _, ok := allowed[ref.Table]; !ok {
						correlated = true
					}
				}
			}
		})
	}
	rewrite.WalkExpr(stmt.Where, func(expr binder.BoundExpr) {
		if ref, ok := expr.(*binder.BoundColumnRef); ok {
			if ref.Table != "" {
				if _, ok := allowed[ref.Table]; !ok {
					correlated = true
				}
			}
		}
	})
	return correlated
}

func addTableAlias(set map[string]struct{}, ref *binder.BoundTableRef) {
	if ref == nil {
		return
	}
	alias := ref.Alias
	if alias == "" {
		alias = ref.TableName
	}
	if alias != "" {
		set[alias] = struct{}{}
	}
}
