package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// BooleanSimplificationRule simplifies boolean logic.
type BooleanSimplificationRule struct{}

func (BooleanSimplificationRule) Name() string { return "boolean_simplification" }

func (BooleanSimplificationRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		bin, ok := expr.(*binder.BoundBinaryExpr)
		if !ok {
			return expr, false
		}
		if bin.Op != parser.OpAnd && bin.Op != parser.OpOr {
			return expr, false
		}
		if simplified, ok := simplifyBoolean(bin.Op, bin.Left, bin.Right); ok {
			return simplified, true
		}
		return expr, false
	})
}

// DeMorganRule applies De Morgan's laws to NOT expressions.
type DeMorganRule struct{}

func (DeMorganRule) Name() string { return "de_morgan" }

func (DeMorganRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ExpressionRewrites {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		unary, ok := expr.(*binder.BoundUnaryExpr)
		if !ok || unary.Op != parser.OpNot {
			return expr, false
		}
		bin, ok := unary.Expr.(*binder.BoundBinaryExpr)
		if !ok {
			return expr, false
		}
		switch bin.Op {
		case parser.OpAnd:
			left := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Left, ResType: dukdb.TYPE_BOOLEAN}
			right := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Right, ResType: dukdb.TYPE_BOOLEAN}
			return &binder.BoundBinaryExpr{Left: left, Op: parser.OpOr, Right: right, ResType: dukdb.TYPE_BOOLEAN}, true
		case parser.OpOr:
			left := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Left, ResType: dukdb.TYPE_BOOLEAN}
			right := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Right, ResType: dukdb.TYPE_BOOLEAN}
			return &binder.BoundBinaryExpr{Left: left, Op: parser.OpAnd, Right: right, ResType: dukdb.TYPE_BOOLEAN}, true
		default:
			return expr, false
		}
	})
}

func simplifyBoolean(op parser.BinaryOp, left, right binder.BoundExpr) (binder.BoundExpr, bool) {
	leftTri, leftOK := triValue(left)
	rightTri, rightOK := triValue(right)

	if leftOK && rightOK {
		result := evalTriBool(op, leftTri, rightTri)
		if !result.valid {
			return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
		}
		return &binder.BoundLiteral{Value: result.value, ValType: dukdb.TYPE_BOOLEAN}, true
	}

	if leftOK {
		if op == parser.OpAnd {
			if leftTri.valid && !leftTri.value {
				return &binder.BoundLiteral{Value: false, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if leftTri.valid && leftTri.value {
				return right, true
			}
		}
		if op == parser.OpOr {
			if leftTri.valid && leftTri.value {
				return &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if leftTri.valid && !leftTri.value {
				return right, true
			}
		}
	}

	if rightOK {
		if op == parser.OpAnd {
			if rightTri.valid && !rightTri.value {
				return &binder.BoundLiteral{Value: false, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if rightTri.valid && rightTri.value {
				return left, true
			}
		}
		if op == parser.OpOr {
			if rightTri.valid && rightTri.value {
				return &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if rightTri.valid && !rightTri.value {
				return left, true
			}
		}
	}

	return nil, false
}
