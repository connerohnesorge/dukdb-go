package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// ComparisonSimplificationRule simplifies comparisons when safe.
type ComparisonSimplificationRule struct{}

func (ComparisonSimplificationRule) Name() string { return "comparison_simplification" }

func (ComparisonSimplificationRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		bin, ok := expr.(*binder.BoundBinaryExpr)
		if !ok || !isComparisonOp(bin.Op) {
			return expr, false
		}
		leftLit, leftOK := asLiteral(bin.Left)
		rightLit, rightOK := asLiteral(bin.Right)
		if leftOK && rightOK {
			if folded, ok := foldBinaryLiteral(bin.Op, leftLit, rightLit, bin.ResType); ok {
				return folded, true
			}
		}
		if leftOK && rightOK && leftLit.Value == nil && rightLit.Value == nil {
			return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
		}
		return expr, false
	})
}

// InListSimplificationRule converts single-element IN lists to comparisons.
type InListSimplificationRule struct{}

func (InListSimplificationRule) Name() string { return "in_list_simplification" }

func (InListSimplificationRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		inList, ok := expr.(*binder.BoundInListExpr)
		if !ok || len(inList.Values) != 1 {
			return expr, false
		}
		op := parser.OpEq
		if inList.Not {
			op = parser.OpNe
		}
		return &binder.BoundBinaryExpr{Left: inList.Expr, Op: op, Right: inList.Values[0], ResType: dukdb.TYPE_BOOLEAN}, true
	})
}

func isComparisonOp(op parser.BinaryOp) bool {
	switch op {
	case parser.OpEq, parser.OpNe, parser.OpLt, parser.OpLe, parser.OpGt, parser.OpGe:
		return true
	default:
		return false
	}
}
