package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// ArithmeticIdentityRule removes arithmetic identities.
type ArithmeticIdentityRule struct{}

func (ArithmeticIdentityRule) Name() string { return "arithmetic_identity" }

func (ArithmeticIdentityRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		bin, ok := expr.(*binder.BoundBinaryExpr)
		if !ok {
			return expr, false
		}
		if simplified, ok := simplifyArithmeticIdentity(bin.Op, bin.Left, bin.Right, bin.ResType); ok {
			return simplified, true
		}
		return expr, false
	})
}

func simplifyArithmeticIdentity(op parser.BinaryOp, left, right binder.BoundExpr, resType dukdb.Type) (binder.BoundExpr, bool) {
	if lit, ok := asLiteral(left); ok {
		if isZeroLiteral(lit) {
			switch op {
			case parser.OpAdd:
				return right, true
			case parser.OpMul:
				return &binder.BoundLiteral{Value: 0, ValType: resType}, true
			}
		}
		if isOneLiteral(lit) && op == parser.OpMul {
			return right, true
		}
	}

	if lit, ok := asLiteral(right); ok {
		if isZeroLiteral(lit) {
			switch op {
			case parser.OpAdd, parser.OpSub:
				return left, true
			case parser.OpMul:
				return &binder.BoundLiteral{Value: 0, ValType: resType}, true
			}
		}
		if isOneLiteral(lit) {
			switch op {
			case parser.OpMul, parser.OpDiv:
				return left, true
			}
		}
	}

	return nil, false
}
