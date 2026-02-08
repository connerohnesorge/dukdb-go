package rules

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
)

type triState struct {
	valid bool
	value bool
}

func asLiteral(expr binder.BoundExpr) (*binder.BoundLiteral, bool) {
	lit, ok := expr.(*binder.BoundLiteral)
	return lit, ok
}

func isZeroLiteral(lit *binder.BoundLiteral) bool {
	if lit == nil || lit.Value == nil {
		return false
	}
	switch v := lit.Value.(type) {
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case float32:
		return v == 0
	case float64:
		return v == 0
	default:
		return false
	}
}

func isOneLiteral(lit *binder.BoundLiteral) bool {
	if lit == nil || lit.Value == nil {
		return false
	}
	switch v := lit.Value.(type) {
	case int:
		return v == 1
	case int8:
		return v == 1
	case int16:
		return v == 1
	case int32:
		return v == 1
	case int64:
		return v == 1
	case uint:
		return v == 1
	case uint8:
		return v == 1
	case uint16:
		return v == 1
	case uint32:
		return v == 1
	case uint64:
		return v == 1
	case float32:
		return v == 1
	case float64:
		return v == 1
	default:
		return false
	}
}

func toNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func triValue(expr binder.BoundExpr) (triState, bool) {
	lit, ok := expr.(*binder.BoundLiteral)
	if !ok {
		return triState{}, false
	}
	if lit.Value == nil || lit.ValType == dukdb.TYPE_SQLNULL {
		return triState{valid: false}, true
	}
	if b, ok := lit.Value.(bool); ok {
		return triState{valid: true, value: b}, true
	}
	return triState{}, false
}

func evalTriBool(op parser.BinaryOp, left, right triState) triState {
	switch op {
	case parser.OpAnd:
		if left.valid && !left.value {
			return triState{valid: true, value: false}
		}
		if right.valid && !right.value {
			return triState{valid: true, value: false}
		}
		if left.valid && right.valid {
			return triState{valid: true, value: left.value && right.value}
		}
		return triState{valid: false}
	case parser.OpOr:
		if left.valid && left.value {
			return triState{valid: true, value: true}
		}
		if right.valid && right.value {
			return triState{valid: true, value: true}
		}
		if left.valid && right.valid {
			return triState{valid: true, value: left.value || right.value}
		}
		return triState{valid: false}
	default:
		return triState{valid: false}
	}
}
