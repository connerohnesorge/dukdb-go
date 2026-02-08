package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// ConstantFoldingRule folds constant expressions.
type ConstantFoldingRule struct{}

func (ConstantFoldingRule) Name() string { return "constant_folding" }

func (ConstantFoldingRule) Apply(plan rewrite.Plan, ctx *rewrite.Context) (rewrite.Plan, bool) {
	if ctx == nil || ctx.Adapter == nil || !ctx.Config.ConstantFolding {
		return plan, false
	}
	return ctx.Adapter.RewriteExpressions(plan, func(expr binder.BoundExpr) (binder.BoundExpr, bool) {
		bin, ok := expr.(*binder.BoundBinaryExpr)
		if !ok {
			return expr, false
		}
		leftLit, leftOK := asLiteral(bin.Left)
		rightLit, rightOK := asLiteral(bin.Right)
		if !leftOK || !rightOK {
			return expr, false
		}
		if folded, ok := foldBinaryLiteral(bin.Op, leftLit, rightLit, bin.ResType); ok {
			return folded, true
		}
		return expr, false
	})
}

func foldBinaryLiteral(op parser.BinaryOp, left, right *binder.BoundLiteral, resType dukdb.Type) (binder.BoundExpr, bool) {
	if left.Value == nil || right.Value == nil {
		return nil, false
	}
	if lnum, lok := toNumber(left.Value); lok {
		if rnum, rok := toNumber(right.Value); rok {
			switch op {
			case parser.OpAdd:
				result := convertToResultType(lnum+rnum, resType)
				return &binder.BoundLiteral{Value: result, ValType: resType}, true
			case parser.OpSub:
				result := convertToResultType(lnum-rnum, resType)
				return &binder.BoundLiteral{Value: result, ValType: resType}, true
			case parser.OpMul:
				result := convertToResultType(lnum*rnum, resType)
				return &binder.BoundLiteral{Value: result, ValType: resType}, true
			case parser.OpDiv:
				if rnum == 0 {
					return nil, false
				}
				result := convertToResultType(lnum/rnum, resType)
				return &binder.BoundLiteral{Value: result, ValType: resType}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lnum == rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lnum != rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpLt:
				return &binder.BoundLiteral{Value: lnum < rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpLe:
				return &binder.BoundLiteral{Value: lnum <= rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpGt:
				return &binder.BoundLiteral{Value: lnum > rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpGe:
				return &binder.BoundLiteral{Value: lnum >= rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	if lbool, ok := left.Value.(bool); ok {
		if rbool, ok := right.Value.(bool); ok {
			switch op {
			case parser.OpAnd:
				return &binder.BoundLiteral{Value: lbool && rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpOr:
				return &binder.BoundLiteral{Value: lbool || rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lbool == rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lbool != rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	if lstr, ok := left.Value.(string); ok {
		if rstr, ok := right.Value.(string); ok {
			switch op {
			case parser.OpConcat:
				return &binder.BoundLiteral{Value: lstr + rstr, ValType: dukdb.TYPE_VARCHAR}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lstr == rstr, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lstr != rstr, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	return nil, false
}

// convertToResultType converts a float64 result to the appropriate type based on resType.
// This ensures integer operations return integers, not floats.
func convertToResultType(val float64, resType dukdb.Type) any {
	switch resType {
	case dukdb.TYPE_TINYINT:
		return int8(val)
	case dukdb.TYPE_SMALLINT:
		return int16(val)
	case dukdb.TYPE_INTEGER:
		return int32(val)
	case dukdb.TYPE_BIGINT:
		return int64(val)
	case dukdb.TYPE_UTINYINT:
		return uint8(val)
	case dukdb.TYPE_USMALLINT:
		return uint16(val)
	case dukdb.TYPE_UINTEGER:
		return uint32(val)
	case dukdb.TYPE_UBIGINT:
		return uint64(val)
	case dukdb.TYPE_FLOAT:
		return float32(val)
	case dukdb.TYPE_DOUBLE:
		return val
	default:
		// For unknown types, return as float64
		return val
	}
}
