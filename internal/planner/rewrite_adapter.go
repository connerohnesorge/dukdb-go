package planner

import (
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

type plannerAdapter struct{}

func (plannerAdapter) Children(plan rewrite.Plan) []rewrite.Plan {
	if plan == nil {
		return nil
	}
	if lp, ok := plan.(LogicalPlan); ok {
		children := lp.Children()
		out := make([]rewrite.Plan, len(children))
		for i, child := range children {
			out[i] = child
		}
		return out
	}
	return nil
}

func (plannerAdapter) ReplaceChildren(plan rewrite.Plan, children []rewrite.Plan) rewrite.Plan {
	if plan == nil {
		return nil
	}
	lp, ok := plan.(LogicalPlan)
	if !ok {
		return plan
	}
	castChildren := make([]LogicalPlan, len(children))
	for i, child := range children {
		if child == nil {
			castChildren[i] = nil
			continue
		}
		castChildren[i] = child.(LogicalPlan)
	}
	return replaceChildren(lp, castChildren)
}

func (plannerAdapter) OutputColumns(plan rewrite.Plan) []rewrite.Column {
	if plan == nil {
		return nil
	}
	lp, ok := plan.(LogicalPlan)
	if !ok {
		return nil
	}
	cols := lp.OutputColumns()
	out := make([]rewrite.Column, len(cols))
	for i, col := range cols {
		out[i] = rewrite.Column{
			Table:     col.Table,
			Column:    col.Column,
			Type:      col.Type,
			TableIdx:  col.TableIdx,
			ColumnIdx: col.ColumnIdx,
		}
	}
	return out
}

func (plannerAdapter) RewriteExpressions(plan rewrite.Plan, rewriter rewrite.ExprRewriter) (rewrite.Plan, bool) {
	if plan == nil {
		return nil, false
	}
	switch node := plan.(type) {
	case *LogicalFilter:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		expr, exprChanged := rewrite.RewriteExpr(node.Condition, rewriter)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalFilter{Child: child, Condition: expr}, true
	case *LogicalProject:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		exprs, exprChanged := rewriteExprSlice(node.Expressions, rewriter)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalProject{Child: child, Expressions: exprs, Aliases: append([]string(nil), node.Aliases...)}, true
	case *LogicalJoin:
		left, leftChanged := rewritePlanExpressions(node.Left, rewriter)
		right, rightChanged := rewritePlanExpressions(node.Right, rewriter)
		expr, exprChanged := rewrite.RewriteExpr(node.Condition, rewriter)
		if !leftChanged && !rightChanged && !exprChanged {
			return plan, false
		}
		return &LogicalJoin{Left: left, Right: right, JoinType: node.JoinType, Condition: expr}, true
	case *LogicalLateralJoin:
		left, leftChanged := rewritePlanExpressions(node.Left, rewriter)
		right, rightChanged := rewritePlanExpressions(node.Right, rewriter)
		expr, exprChanged := rewrite.RewriteExpr(node.Condition, rewriter)
		if !leftChanged && !rightChanged && !exprChanged {
			return plan, false
		}
		return &LogicalLateralJoin{Left: left, Right: right, JoinType: node.JoinType, Condition: expr}, true
	case *LogicalAggregate:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		groupBy, groupChanged := rewriteExprSlice(node.GroupBy, rewriter)
		aggs, aggChanged := rewriteExprSlice(node.Aggregates, rewriter)
		if !childChanged && !groupChanged && !aggChanged {
			return plan, false
		}
		return &LogicalAggregate{
			Child:         child,
			GroupBy:       groupBy,
			Aggregates:    aggs,
			Aliases:       append([]string(nil), node.Aliases...),
			GroupingSets:  node.GroupingSets,
			GroupingCalls: node.GroupingCalls,
		}, true
	case *LogicalSort:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		orderBy := make([]*binder.BoundOrderBy, len(node.OrderBy))
		exprChanged := false
		for i, ob := range node.OrderBy {
			rewritten, changed := rewrite.RewriteExpr(ob.Expr, rewriter)
			if changed {
				exprChanged = true
			}
			orderBy[i] = &binder.BoundOrderBy{Expr: rewritten, Desc: ob.Desc, Collation: ob.Collation}
		}
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalSort{Child: child, OrderBy: orderBy}, true
	case *LogicalLimit:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		limitExpr, limitChanged := rewrite.RewriteExpr(node.LimitExpr, rewriter)
		offsetExpr, offsetChanged := rewrite.RewriteExpr(node.OffsetExpr, rewriter)
		if !childChanged && !limitChanged && !offsetChanged {
			return plan, false
		}
		return &LogicalLimit{Child: child, Limit: node.Limit, Offset: node.Offset, LimitExpr: limitExpr, OffsetExpr: offsetExpr, WithTies: node.WithTies, OrderBy: node.OrderBy}, true
	case *LogicalDistinctOn:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		exprs, exprChanged := rewriteExprSlice(node.DistinctOn, rewriter)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalDistinctOn{Child: child, DistinctOn: exprs, OrderBy: node.OrderBy}, true
	case *LogicalWindow:
		child, childChanged := rewritePlanExpressions(node.Child, rewriter)
		rewrittenExprs := make([]*binder.BoundWindowExpr, len(node.WindowExprs))
		exprChanged := false
		for i, win := range node.WindowExprs {
			rewritten, changed := rewriteWindowExpr(win, rewriter)
			rewrittenExprs[i] = rewritten
			if changed {
				exprChanged = true
			}
		}
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalWindow{Child: child, WindowExprs: rewrittenExprs}, true
	default:
		lp, ok := plan.(LogicalPlan)
		if !ok {
			return plan, false
		}
		children := lp.Children()
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			rewritten, childChanged := rewritePlanExpressions(child, rewriter)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return replaceChildren(lp, newChildren), true
	}
}

func (plannerAdapter) AsFilter(plan rewrite.Plan) (rewrite.Plan, binder.BoundExpr, bool) {
	filter, ok := plan.(*LogicalFilter)
	if !ok {
		return nil, nil, false
	}
	return filter.Child, filter.Condition, true
}

func (plannerAdapter) NewFilter(child rewrite.Plan, cond binder.BoundExpr) rewrite.Plan {
	if child == nil {
		return &LogicalFilter{Condition: cond}
	}
	return &LogicalFilter{Child: child.(LogicalPlan), Condition: cond}
}

func (plannerAdapter) AsProject(plan rewrite.Plan) (rewrite.Plan, []binder.BoundExpr, []string, bool) {
	proj, ok := plan.(*LogicalProject)
	if !ok {
		return nil, nil, nil, false
	}
	return proj.Child, proj.Expressions, proj.Aliases, true
}

func (plannerAdapter) NewProject(child rewrite.Plan, exprs []binder.BoundExpr, aliases []string) rewrite.Plan {
	if child == nil {
		return &LogicalProject{Expressions: exprs, Aliases: aliases}
	}
	return &LogicalProject{Child: child.(LogicalPlan), Expressions: exprs, Aliases: aliases}
}

func (plannerAdapter) AsJoin(plan rewrite.Plan) (rewrite.Plan, rewrite.Plan, rewrite.JoinType, binder.BoundExpr, bool) {
	join, ok := plan.(*LogicalJoin)
	if !ok {
		return nil, nil, 0, nil, false
	}
	return join.Left, join.Right, toRewriteJoinType(join.JoinType), join.Condition, true
}

func (plannerAdapter) NewJoin(left, right rewrite.Plan, joinType rewrite.JoinType, cond binder.BoundExpr) rewrite.Plan {
	return &LogicalJoin{Left: left.(LogicalPlan), Right: right.(LogicalPlan), JoinType: fromRewriteJoinType(joinType), Condition: cond}
}

func (plannerAdapter) AsDistinct(plan rewrite.Plan) (rewrite.Plan, bool) {
	distinct, ok := plan.(*LogicalDistinct)
	if !ok {
		return nil, false
	}
	return distinct.Child, true
}

func (plannerAdapter) NewDistinct(child rewrite.Plan) rewrite.Plan {
	return &LogicalDistinct{Child: child.(LogicalPlan)}
}

func (plannerAdapter) AsScan(plan rewrite.Plan) (schema, table, alias string, projections []int, columns []rewrite.Column, ok bool) {
	scan, ok := plan.(*LogicalScan)
	if !ok {
		return "", "", "", nil, nil, false
	}
	cols := scan.OutputColumns()
	out := make([]rewrite.Column, len(cols))
	for i, col := range cols {
		out[i] = rewrite.Column{
			Table:     col.Table,
			Column:    col.Column,
			Type:      col.Type,
			TableIdx:  col.TableIdx,
			ColumnIdx: col.ColumnIdx,
		}
	}
	return scan.Schema, scan.TableName, scan.Alias, scan.Projections, out, true
}

func (plannerAdapter) WithScanProjections(plan rewrite.Plan, projections []int) rewrite.Plan {
	scan, ok := plan.(*LogicalScan)
	if !ok {
		return plan
	}
	next := *scan
	next.Projections = append([]int(nil), projections...)
	next.columns = nil
	return &next
}

func rewritePlanExpressions(plan LogicalPlan, rewriter rewrite.ExprRewriter) (LogicalPlan, bool) {
	rewritten, changed := plannerAdapter{}.RewriteExpressions(plan, rewriter)
	if !changed {
		return plan, false
	}
	return rewritten.(LogicalPlan), true
}

func rewriteExprSlice(exprs []binder.BoundExpr, rewriter rewrite.ExprRewriter) ([]binder.BoundExpr, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	changed := false
	rewritten := make([]binder.BoundExpr, len(exprs))
	for i, expr := range exprs {
		next, exprChanged := rewrite.RewriteExpr(expr, rewriter)
		if exprChanged {
			changed = true
		}
		rewritten[i] = next
	}
	return rewritten, changed
}

func rewriteWindowExpr(expr *binder.BoundWindowExpr, rewriter rewrite.ExprRewriter) (*binder.BoundWindowExpr, bool) {
	if expr == nil {
		return nil, false
	}
	changed := false
	args, argsChanged := rewriteExprSlice(expr.Args, rewriter)
	if argsChanged {
		changed = true
	}
	partition, partChanged := rewriteExprSlice(expr.PartitionBy, rewriter)
	if partChanged {
		changed = true
	}
	orderBy := make([]binder.BoundWindowOrder, len(expr.OrderBy))
	orderChanged := false
	for i, ob := range expr.OrderBy {
		rewritten, changedExpr := rewrite.RewriteExpr(ob.Expr, rewriter)
		if changedExpr {
			orderChanged = true
		}
		orderBy[i] = binder.BoundWindowOrder{Expr: rewritten, Desc: ob.Desc, NullsFirst: ob.NullsFirst}
	}
	if orderChanged {
		changed = true
	}
	filterExpr, filterChanged := rewrite.RewriteExpr(expr.Filter, rewriter)
	if filterChanged {
		changed = true
	}
	if !changed {
		return expr, false
	}
	return &binder.BoundWindowExpr{
		FunctionName: expr.FunctionName,
		FunctionType: expr.FunctionType,
		Args:         args,
		PartitionBy:  partition,
		OrderBy:      orderBy,
		Frame:        expr.Frame,
		ResType:      expr.ResType,
		IgnoreNulls:  expr.IgnoreNulls,
		Filter:       filterExpr,
		Distinct:     expr.Distinct,
		ResultIndex:  expr.ResultIndex,
		Alias:        expr.Alias,
	}, true
}

func toRewriteJoinType(j JoinType) rewrite.JoinType {
	switch j {
	case JoinTypeInner:
		return rewrite.JoinTypeInner
	case JoinTypeLeft:
		return rewrite.JoinTypeLeft
	case JoinTypeRight:
		return rewrite.JoinTypeRight
	case JoinTypeFull:
		return rewrite.JoinTypeFull
	case JoinTypeCross:
		return rewrite.JoinTypeCross
	case JoinTypeSemi:
		return rewrite.JoinTypeSemi
	case JoinTypeAnti:
		return rewrite.JoinTypeAnti
	default:
		return rewrite.JoinTypeInner
	}
}

func fromRewriteJoinType(j rewrite.JoinType) JoinType {
	switch j {
	case rewrite.JoinTypeInner:
		return JoinTypeInner
	case rewrite.JoinTypeLeft:
		return JoinTypeLeft
	case rewrite.JoinTypeRight:
		return JoinTypeRight
	case rewrite.JoinTypeFull:
		return JoinTypeFull
	case rewrite.JoinTypeCross:
		return JoinTypeCross
	case rewrite.JoinTypeSemi:
		return JoinTypeSemi
	case rewrite.JoinTypeAnti:
		return JoinTypeAnti
	default:
		return JoinTypeInner
	}
}

func replaceChildren(plan LogicalPlan, children []LogicalPlan) LogicalPlan {
	switch node := plan.(type) {
	case *LogicalFilter:
		return &LogicalFilter{Child: children[0], Condition: node.Condition}
	case *LogicalProject:
		return &LogicalProject{Child: children[0], Expressions: node.Expressions, Aliases: node.Aliases}
	case *LogicalJoin:
		return &LogicalJoin{Left: children[0], Right: children[1], JoinType: node.JoinType, Condition: node.Condition}
	case *LogicalLateralJoin:
		return &LogicalLateralJoin{Left: children[0], Right: children[1], JoinType: node.JoinType, Condition: node.Condition}
	case *LogicalAggregate:
		return &LogicalAggregate{Child: children[0], GroupBy: node.GroupBy, Aggregates: node.Aggregates, Aliases: node.Aliases, GroupingSets: node.GroupingSets, GroupingCalls: node.GroupingCalls}
	case *LogicalSort:
		return &LogicalSort{Child: children[0], OrderBy: node.OrderBy}
	case *LogicalLimit:
		return &LogicalLimit{Child: children[0], Limit: node.Limit, Offset: node.Offset, LimitExpr: node.LimitExpr, OffsetExpr: node.OffsetExpr, WithTies: node.WithTies, OrderBy: node.OrderBy}
	case *LogicalDistinct:
		return &LogicalDistinct{Child: children[0]}
	case *LogicalDistinctOn:
		return &LogicalDistinctOn{Child: children[0], DistinctOn: node.DistinctOn, OrderBy: node.OrderBy}
	case *LogicalWindow:
		return &LogicalWindow{Child: children[0], WindowExprs: node.WindowExprs}
	case *LogicalSample:
		return &LogicalSample{Child: children[0], Sample: node.Sample}
	case *LogicalSetOp:
		return &LogicalSetOp{Left: children[0], Right: children[1], OpType: node.OpType}
	case *LogicalCopyTo:
		return &LogicalCopyTo{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Columns: node.Columns, FilePath: node.FilePath, Options: node.Options, Source: children[0]}
	case *LogicalInsert:
		return &LogicalInsert{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Columns: node.Columns, Values: node.Values, Source: children[0], Returning: node.Returning}
	case *LogicalUpdate:
		return &LogicalUpdate{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Set: node.Set, Source: children[0], Returning: node.Returning}
	case *LogicalDelete:
		return &LogicalDelete{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Source: children[0], Returning: node.Returning}
	case *LogicalMerge:
		return &LogicalMerge{Schema: node.Schema, TargetTable: node.TargetTable, TargetTableDef: node.TargetTableDef, TargetAlias: node.TargetAlias, SourcePlan: children[0], OnCondition: node.OnCondition, WhenMatched: node.WhenMatched, WhenNotMatched: node.WhenNotMatched, WhenNotMatchedBySource: node.WhenNotMatchedBySource, Returning: node.Returning}
	case *LogicalCTEScan:
		if len(children) > 0 {
			return &LogicalCTEScan{CTEName: node.CTEName, Alias: node.Alias, Columns: node.Columns, CTEPlan: children[0], IsRecursive: node.IsRecursive}
		}
	case *LogicalRecursiveCTE:
		if len(children) >= 2 {
			return &LogicalRecursiveCTE{CTEName: node.CTEName, BasePlan: children[0], RecursivePlan: children[1], Columns: node.Columns}
		}
	case *LogicalExplain:
		return &LogicalExplain{Child: children[0], Analyze: node.Analyze, RewriteStats: node.RewriteStats}
	}
	return plan
}

func newPlannerAdapter() plannerAdapter {
	return plannerAdapter{}
}
