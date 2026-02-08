package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// NullSimplificationRule simplifies NULL-related expressions.
type NullSimplificationRule struct{}

func (NullSimplificationRule) Name() string { return "null_simplification" }

func (NullSimplificationRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		bin, ok := expr.(*binder.BoundBinaryExpr)
		if !ok {
			return expr, false
		}

		leftLit, leftOK := asLiteral(bin.Left)
		rightLit, rightOK := asLiteral(bin.Right)

		if isComparisonOp(bin.Op) {
			if (leftOK && leftLit.Value == nil) || (rightOK && rightLit.Value == nil) {
				return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
			}
		}

		if bin.Op == parser.OpAnd || bin.Op == parser.OpOr {
			if leftOK && leftLit.Value == nil {
				if rightOK {
					return evaluateNullBoolean(bin.Op, leftLit, rightLit)
				}
			}
			if rightOK && rightLit.Value == nil {
				if leftOK {
					return evaluateNullBoolean(bin.Op, leftLit, rightLit)
				}
			}
		}

		return expr, false
	})
}

func evaluateNullBoolean(op parser.BinaryOp, left, right *binder.BoundLiteral) (binder.BoundExpr, bool) {
	if left == nil || right == nil {
		return nil, false
	}
	if left.Value == nil && right.Value == nil {
		return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
	}
	var other any
	if left.Value == nil {
		other = right.Value
	} else if right.Value == nil {
		other = left.Value
	}
	b, ok := other.(bool)
	if !ok {
		return nil, false
	}
	switch op {
	case parser.OpAnd:
		if !b {
			return &binder.BoundLiteral{Value: false, ValType: dukdb.TYPE_BOOLEAN}, true
		}
		return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
	case parser.OpOr:
		if b {
			return &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN}, true
		}
		return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
	default:
		return nil, false
	}
}
