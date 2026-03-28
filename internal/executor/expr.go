package executor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/collation"
	geomutil "github.com/dukdb/dukdb-go/internal/io/geometry"
	jsonutil "github.com/dukdb/dukdb-go/internal/io/json"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/wal"
	"github.com/google/uuid"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/unicode/norm"
)

// evaluateExpr evaluates an expression and returns the result.
func (e *Executor) evaluateExpr(
	ctx *ExecutionContext,
	expr interface{},
	row map[string]any,
) (any, error) {
	if expr == nil {
		return nil, nil
	}

	switch ex := expr.(type) {
	case *binder.BoundLiteral:
		return ex.Value, nil

	case *binder.BoundColumnRef:
		// First check the current row
		if row != nil {
			// Try with table prefix first to avoid ambiguity when multiple tables have same column name
			if ex.Table != "" {
				key := ex.Table + "." + ex.Column
				if val, ok := row[key]; ok {
					return val, nil
				}
			}
			// Then try column name directly (for backwards compatibility and simple queries)
			if val, ok := row[ex.Column]; ok {
				return val, nil
			}
		}

		// Then check correlated values from outer scope (for LATERAL subqueries)
		if ctx.CorrelatedValues != nil {
			if ex.Table != "" {
				key := ex.Table + "." + ex.Column
				if val, ok := ctx.CorrelatedValues[key]; ok {
					return val, nil
				}
			}
			if val, ok := ctx.CorrelatedValues[ex.Column]; ok {
				return val, nil
			}
		}

		return nil, nil

	case *binder.BoundFieldAccess:
		// Evaluate the struct expression (the struct-typed column or sub-expression)
		structVal, err := e.evaluateExpr(ctx, ex.Struct, row)
		if err != nil {
			return nil, fmt.Errorf("evaluating struct expression for field access: %w", err)
		}
		if structVal == nil {
			return nil, nil // NULL propagation: NULL.field => NULL
		}
		m, ok := structVal.(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("expected struct value for field access, got %T", structVal),
			}
		}
		// Try exact field name first
		val, exists := m[ex.Field]
		if exists {
			return val, nil
		}
		// Case-insensitive fallback
		for k, v := range m {
			if strings.EqualFold(k, ex.Field) {
				return v, nil
			}
		}
		return nil, nil // Field not found returns NULL

	case *binder.BoundParameter:
		if ctx.Args == nil || ex.Position <= 0 || ex.Position > len(ctx.Args) {
			return nil, nil
		}

		return ctx.Args[ex.Position-1].Value, nil

	case *binder.BoundExcludedColumnRef:
		// EXCLUDED values are stored in the row map under "EXCLUDED.column" keys
		if row != nil {
			key := "EXCLUDED." + ex.ColumnName
			if val, ok := row[key]; ok {
				return val, nil
			}
		}
		return nil, nil

	case *binder.BoundBinaryExpr:
		return e.evaluateBinaryExpr(ctx, ex, row)

	case *binder.BoundUnaryExpr:
		return e.evaluateUnaryExpr(ctx, ex, row)

	case *binder.BoundFunctionCall:
		return e.evaluateFunctionCall(ctx, ex, row)

	case *binder.BoundCastExpr:
		val, err := e.evaluateExpr(ctx, ex.Expr, row)
		if err != nil {
			if ex.TryCast {
				return nil, nil
			}
			return nil, err
		}

		result, err := castValue(val, ex.TargetType)
		if err != nil {
			if ex.TryCast {
				return nil, nil
			}
			return nil, err
		}
		return result, nil

	case *binder.BoundCaseExpr:
		return e.evaluateCaseExpr(ctx, ex, row)

	case *binder.BoundBetweenExpr:
		return e.evaluateBetweenExpr(ctx, ex, row)

	case *binder.BoundInListExpr:
		return e.evaluateInListExpr(ctx, ex, row)

	case *binder.BoundInSubqueryExpr:
		return e.evaluateInSubqueryExpr(ctx, ex, row)

	case *binder.BoundQuantifiedComparison:
		return e.evaluateQuantifiedComparison(ctx, ex, row)

	case *binder.BoundExtractExpr:
		return e.evaluateExtractExpr(ctx, ex, row)

	case *binder.BoundIntervalLiteral:
		// Return as an Interval struct
		return Interval{
			Months: ex.Months,
			Days:   ex.Days,
			Micros: ex.Micros,
		}, nil

	case *binder.BoundWindowExpr:
		// Window expressions should have already been evaluated by the PhysicalWindowExecutor
		// and their results stored in the row map using the function name as the key.
		// Look up the pre-computed result.
		if row == nil {
			return nil, nil
		}
		// Try the function name directly
		if val, ok := row[ex.FunctionName]; ok {
			return val, nil
		}
		// Try uppercase version
		if val, ok := row[strings.ToUpper(ex.FunctionName)]; ok {
			return val, nil
		}
		// Try lowercase version
		if val, ok := row[strings.ToLower(ex.FunctionName)]; ok {
			return val, nil
		}
		// Window result not found - return nil
		return nil, nil

	case *binder.BoundSequenceCall:
		return e.evaluateSequenceCall(ctx, ex)

	case *binder.BoundGroupingCall:
		// GROUPING() function calls are evaluated during aggregate execution.
		// If we get here, the GROUPING value should already be in the row.
		if row == nil {
			return nil, nil
		}
		// Look up the pre-computed GROUPING value in the row
		if val, ok := row["GROUPING"]; ok {
			return val, nil
		}
		// GROUPING value not found - this shouldn't happen during normal execution
		return nil, nil

	case *binder.BoundGroupingSetExpr:
		// Grouping set expressions should not be evaluated directly.
		// They are handled by the planner/executor for grouping sets.
		return nil, nil

	case *binder.BoundArrayExpr:
		return e.evaluateArrayExpr(ctx, ex, row)

	case *binder.BoundSelectStmt:
		return e.evaluateScalarSubquery(ctx, ex, row)

	case *binder.BoundSimilarToExpr:
		leftVal, err := e.evaluateExpr(ctx, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		rightVal, err := e.evaluateExpr(ctx, ex.Pattern, row)
		if err != nil {
			return nil, err
		}
		escape := ex.Escape
		if escape == 0 {
			escape = '\\'
		}
		result, err := matchSimilarTo(toString(leftVal), toString(rightVal), escape)
		if err != nil {
			return nil, err
		}
		if ex.Not {
			return !result.(bool), nil
		}
		return result, nil

	// Parser AST expression types - used for CHECK constraint evaluation
	case *parser.ColumnRef:
		if row != nil {
			// Try with table prefix
			if ex.Table != "" {
				key := ex.Table + "." + ex.Column
				if val, ok := row[key]; ok {
					return val, nil
				}
			}
			// Try column name directly
			if val, ok := row[ex.Column]; ok {
				return val, nil
			}
			// Try case-insensitive
			lower := strings.ToLower(ex.Column)
			if val, ok := row[lower]; ok {
				return val, nil
			}
		}
		return nil, nil

	case *parser.Literal:
		return ex.Value, nil

	case *parser.BinaryExpr:
		leftVal, err := e.evaluateExpr(ctx, ex.Left, row)
		if err != nil {
			return nil, err
		}
		rightVal, err := e.evaluateExpr(ctx, ex.Right, row)
		if err != nil {
			return nil, err
		}
		return e.evaluateParserBinaryOp(ex.Op, leftVal, rightVal)

	case *parser.UnaryExpr:
		val, err := e.evaluateExpr(ctx, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case parser.OpNot:
			if val == nil {
				return nil, nil
			}
			if b, ok := val.(bool); ok {
				return !b, nil
			}
			return nil, nil
		case parser.OpNeg:
			return negateValue(val)
		case parser.OpIsNull:
			return val == nil, nil
		case parser.OpIsNotNull:
			return val != nil, nil
		}
		return nil, nil

	case *parser.FunctionCall:
		// Handle raw parser function calls (used by generated column expressions).
		// Evaluate arguments, wrap as BoundLiterals, and delegate to the function evaluator.
		boundArgs := make([]binder.BoundExpr, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evaluateExpr(ctx, arg, row)
			if err != nil {
				return nil, err
			}
			boundArgs[i] = &binder.BoundLiteral{Value: val, ValType: dukdb.TYPE_ANY}
		}
		boundFn := &binder.BoundFunctionCall{
			Name:     strings.ToUpper(ex.Name),
			Args:     boundArgs,
			Distinct: ex.Distinct,
			Star:     ex.Star,
		}
		return e.evaluateFunctionCall(ctx, boundFn, row)

	case *parser.CastExpr:
		val, err := e.evaluateExpr(ctx, ex.Expr, row)
		if err != nil {
			if ex.TryCast {
				return nil, nil
			}
			return nil, err
		}
		result, err := castValue(val, ex.TargetType)
		if err != nil {
			if ex.TryCast {
				return nil, nil
			}
			return nil, err
		}
		return result, nil

	case *binder.BoundLambdaExpr:
		// Lambda expressions should not be evaluated directly; they are passed to
		// lambda-accepting functions (list_transform, list_filter, etc.)
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "lambda expressions cannot be evaluated directly",
		}

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("unsupported expression type: %T", expr),
		}
	}
}

// evaluateExprAsBool evaluates an expression and returns it as a boolean.
func (e *Executor) evaluateExprAsBool(
	ctx *ExecutionContext,
	expr interface{},
	row map[string]any,
) (bool, error) {
	val, err := e.evaluateExpr(ctx, expr, row)
	if err != nil {
		return false, err
	}

	return toBool(val), nil
}

func (e *Executor) evaluateBinaryExpr(
	ctx *ExecutionContext,
	expr *binder.BoundBinaryExpr,
	row map[string]any,
) (any, error) {
	left, err := e.evaluateExpr(
		ctx,
		expr.Left,
		row,
	)
	if err != nil {
		return nil, err
	}

	right, err := e.evaluateExpr(
		ctx,
		expr.Right,
		row,
	)
	if err != nil {
		return nil, err
	}

	// Handle NULL propagation for most operators
	if left == nil || right == nil {
		switch expr.Op {
		case parser.OpAnd:
			// NULL AND FALSE = FALSE, NULL AND TRUE = NULL
			if left == nil && right != nil {
				if !toBool(right) {
					return false, nil
				}
			}
			if right == nil && left != nil {
				if !toBool(left) {
					return false, nil
				}
			}

			return nil, nil
		case parser.OpOr:
			// NULL OR TRUE = TRUE, NULL OR FALSE = NULL
			if left == nil && right != nil {
				if toBool(right) {
					return true, nil
				}
			}
			if right == nil && left != nil {
				if toBool(left) {
					return true, nil
				}
			}

			return nil, nil
		case parser.OpIs:
			return left == nil &&
				right == nil, nil
		case parser.OpIsNot:
			return left != nil ||
				right != nil, nil
		case parser.OpIsDistinctFrom:
			if left == nil && right == nil {
				return false, nil
			}
			if left == nil || right == nil {
				return true, nil
			}
			return compareValues(left, right) != 0, nil
		case parser.OpIsNotDistinctFrom:
			if left == nil && right == nil {
				return true, nil
			}
			if left == nil || right == nil {
				return false, nil
			}
			return compareValues(left, right) == 0, nil
		default:
			return nil, nil
		}
	}

	switch expr.Op {
	// Arithmetic operators
	case parser.OpAdd:
		return addValues(left, right)
	case parser.OpSub:
		return subValues(left, right)
	case parser.OpMul:
		return mulValues(left, right)
	case parser.OpDiv:
		return divValues(left, right)
	case parser.OpMod:
		return modValues(left, right)

	// Comparison operators
	case parser.OpEq:
		return compareValues(
			left,
			right,
		) == 0, nil
	case parser.OpNe:
		return compareValues(
			left,
			right,
		) != 0, nil
	case parser.OpIsDistinctFrom:
		return compareValues(left, right) != 0, nil
	case parser.OpIsNotDistinctFrom:
		return compareValues(left, right) == 0, nil
	case parser.OpLt:
		return compareValues(left, right) < 0, nil
	case parser.OpLe:
		return compareValues(
			left,
			right,
		) <= 0, nil
	case parser.OpGt:
		return compareValues(left, right) > 0, nil
	case parser.OpGe:
		return compareValues(
			left,
			right,
		) >= 0, nil

	// Logical operators
	case parser.OpAnd:
		return toBool(left) && toBool(right), nil
	case parser.OpOr:
		return toBool(left) || toBool(right), nil

	// String operators
	case parser.OpLike:
		return matchLike(
			toString(left),
			toString(right),
			true,
		), nil
	case parser.OpILike:
		return matchLike(
			strings.ToLower(toString(left)),
			strings.ToLower(toString(right)),
			true,
		), nil
	case parser.OpNotLike:
		return !matchLike(
			toString(left),
			toString(right),
			true,
		), nil
	case parser.OpNotILike:
		return !matchLike(
			strings.ToLower(toString(left)),
			strings.ToLower(toString(right)),
			true,
		), nil
	case parser.OpSimilarTo:
		return matchSimilarTo(toString(left), toString(right), '\\')
	case parser.OpNotSimilarTo:
		result, err := matchSimilarTo(toString(left), toString(right), '\\')
		if err != nil {
			return nil, err
		}
		return !result.(bool), nil
	case parser.OpConcat:
		return toString(
			left,
		) + toString(
			right,
		), nil

	// JSON operators
	case parser.OpJSONExtract:
		return extractJSONValue(left, right, false)
	case parser.OpJSONText:
		return extractJSONValue(left, right, true)

	// Bitwise operators
	case parser.OpBitwiseAnd:
		return bitwiseAndValue(left, right)
	case parser.OpBitwiseOr:
		return bitwiseOrValue(left, right)
	case parser.OpBitwiseXor:
		return bitwiseXorValue(left, right)
	case parser.OpShiftLeft:
		return bitwiseShiftLeftValue(left, right)
	case parser.OpShiftRight:
		return bitwiseShiftRightValue(left, right)

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unsupported binary operator: %d",
				expr.Op,
			),
		}
	}
}

// evaluateParserBinaryOp evaluates a parser binary operation on two already-evaluated values.
// Used for CHECK constraint evaluation where expressions are parser AST nodes.
func (e *Executor) evaluateParserBinaryOp(op parser.BinaryOp, left, right any) (any, error) {
	// Handle NULL propagation
	if left == nil || right == nil {
		switch op {
		case parser.OpAnd:
			if left == nil && right != nil {
				if !toBool(right) {
					return false, nil
				}
			}
			if right == nil && left != nil {
				if !toBool(left) {
					return false, nil
				}
			}
			return nil, nil
		case parser.OpOr:
			if left == nil && right != nil {
				if toBool(right) {
					return true, nil
				}
			}
			if right == nil && left != nil {
				if toBool(left) {
					return true, nil
				}
			}
			return nil, nil
		case parser.OpIs:
			return left == nil && right == nil, nil
		case parser.OpIsNot:
			return left != nil || right != nil, nil
		case parser.OpIsDistinctFrom:
			if left == nil && right == nil {
				return false, nil
			}
			if left == nil || right == nil {
				return true, nil
			}
			return compareValues(left, right) != 0, nil
		case parser.OpIsNotDistinctFrom:
			if left == nil && right == nil {
				return true, nil
			}
			if left == nil || right == nil {
				return false, nil
			}
			return compareValues(left, right) == 0, nil
		default:
			return nil, nil
		}
	}

	switch op {
	case parser.OpAdd:
		return addValues(left, right)
	case parser.OpSub:
		return subValues(left, right)
	case parser.OpMul:
		return mulValues(left, right)
	case parser.OpDiv:
		return divValues(left, right)
	case parser.OpMod:
		return modValues(left, right)
	case parser.OpEq:
		return compareValues(left, right) == 0, nil
	case parser.OpNe:
		return compareValues(left, right) != 0, nil
	case parser.OpIsDistinctFrom:
		return compareValues(left, right) != 0, nil
	case parser.OpIsNotDistinctFrom:
		return compareValues(left, right) == 0, nil
	case parser.OpLt:
		return compareValues(left, right) < 0, nil
	case parser.OpLe:
		return compareValues(left, right) <= 0, nil
	case parser.OpGt:
		return compareValues(left, right) > 0, nil
	case parser.OpGe:
		return compareValues(left, right) >= 0, nil
	case parser.OpAnd:
		return toBool(left) && toBool(right), nil
	case parser.OpOr:
		return toBool(left) || toBool(right), nil
	case parser.OpLike:
		return matchLike(toString(left), toString(right), true), nil
	case parser.OpILike:
		return matchLike(
			strings.ToLower(toString(left)),
			strings.ToLower(toString(right)),
			true,
		), nil
	case parser.OpNotLike:
		return !matchLike(toString(left), toString(right), true), nil
	case parser.OpNotILike:
		return !matchLike(
			strings.ToLower(toString(left)),
			strings.ToLower(toString(right)),
			true,
		), nil
	case parser.OpConcat:
		return toString(left) + toString(right), nil
	default:
		return nil, fmt.Errorf("unsupported parser binary operator: %d", op)
	}
}

func (e *Executor) evaluateUnaryExpr(
	ctx *ExecutionContext,
	expr *binder.BoundUnaryExpr,
	row map[string]any,
) (any, error) {
	val, err := e.evaluateExpr(
		ctx,
		expr.Expr,
		row,
	)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case parser.OpNot:
		if val == nil {
			return nil, nil
		}

		return !toBool(val), nil
	case parser.OpNeg:
		if val == nil {
			return nil, nil
		}

		return negateValue(val)
	case parser.OpPos:
		return val, nil
	case parser.OpIsNull:
		return val == nil, nil
	case parser.OpIsNotNull:
		return val != nil, nil
	case parser.OpBitwiseNot:
		return bitwiseNotValue(val)
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unsupported unary operator: %d",
				expr.Op,
			),
		}
	}
}

func (e *Executor) evaluateFunctionCall(
	ctx *ExecutionContext,
	fn *binder.BoundFunctionCall,
	row map[string]any,
) (any, error) {
	// Lambda-accepting functions must intercept BEFORE eager argument evaluation,
	// because lambda arguments (BoundLambdaExpr) cannot be evaluated as regular expressions.
	switch fn.Name {
	case "LIST_TRANSFORM", "ARRAY_APPLY", "APPLY":
		return e.handleListLambdaFunction(ctx, fn, row, "list_transform")
	case "LIST_FILTER", "ARRAY_FILTER", "FILTER":
		return e.handleListLambdaFunction(ctx, fn, row, "list_filter")
	case "LIST_SORT", "ARRAY_SORT":
		// list_sort may or may not have a lambda; check if any arg is a lambda
		hasLambda := false
		for _, arg := range fn.Args {
			if _, ok := arg.(*binder.BoundLambdaExpr); ok {
				hasLambda = true
				break
			}
		}
		if hasLambda || len(fn.Args) >= 1 {
			return e.handleListLambdaFunction(ctx, fn, row, "list_sort")
		}
	case "STRUCT_PACK":
		// struct_pack uses named arguments which must be evaluated from fn.NamedArgs
		result := make(map[string]any)
		if fn.NamedArgs != nil {
			for name, argExpr := range fn.NamedArgs {
				val, err := e.evaluateExpr(ctx, argExpr, row)
				if err != nil {
					return nil, err
				}
				result[name] = val
			}
		}
		// Also evaluate any positional args
		for i, arg := range fn.Args {
			val, err := e.evaluateExpr(ctx, arg, row)
			if err != nil {
				return nil, err
			}
			result[fmt.Sprintf("v%d", i+1)] = val
		}
		return result, nil
	}

	// Evaluate arguments
	args := make([]any, len(fn.Args))
	for i, arg := range fn.Args {
		val, err := e.evaluateExpr(ctx, arg, row)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	// Execute function
	switch fn.Name {
	case "ABS":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ABS requires 1 argument",
			}
		}

		return absValue(args[0])

	// Rounding functions
	case "ROUND":
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ROUND requires 1 or 2 arguments",
			}
		}
		var decimals any
		if len(args) == 2 {
			decimals = args[1]
		}
		return roundValue(args[0], decimals)

	case "CEIL", "CEILING":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "CEIL requires 1 argument",
			}
		}
		return ceilValue(args[0])

	case "FLOOR":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FLOOR requires 1 argument",
			}
		}
		return floorValue(args[0])

	case "TRUNC", "TRUNCATE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TRUNC requires 1 argument",
			}
		}
		return truncValue(args[0])

	case "ROUND_EVEN":
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ROUND_EVEN requires 1 or 2 arguments",
			}
		}
		var decimals any
		if len(args) == 2 {
			decimals = args[1]
		}
		return roundEvenValue(args[0], decimals)

	case "EVEN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "EVEN requires 1 argument",
			}
		}
		return evenValue(args[0])

	// Scientific functions
	case "SQRT":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SQRT requires 1 argument",
			}
		}
		return sqrtValue(args[0])

	case "CBRT":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "CBRT requires 1 argument",
			}
		}
		return cbrtValue(args[0])

	case "POW", "POWER":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "POW requires 2 arguments",
			}
		}
		return powValue(args[0], args[1])

	case "EXP":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "EXP requires 1 argument",
			}
		}
		return expValue(args[0])

	case "LN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LN requires 1 argument",
			}
		}
		return lnValue(args[0])

	case "LOG", "LOG10":
		if len(args) == 1 {
			return log10Value(args[0])
		}
		if len(args) == 2 {
			// LOG(x, base) = ln(x) / ln(base)
			if args[0] == nil || args[1] == nil {
				return nil, nil
			}
			x, ok1 := toFloat64(args[0])
			base, ok2 := toFloat64(args[1])
			if !ok1 || !ok2 {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  "LOG: arguments must be numeric",
				}
			}
			if base <= 0 || base == 1 || x <= 0 {
				return nil, nil // NULL for invalid inputs
			}
			return math.Log(x) / math.Log(base), nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "LOG requires 1 or 2 arguments",
		}

	case "LOG2":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LOG2 requires 1 argument",
			}
		}
		return log2Value(args[0])

	case "SIGNBIT":
		if len(args) != 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "SIGNBIT requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		return math.Signbit(toFloat64Value(args[0])), nil

	case "WIDTH_BUCKET":
		if len(args) != 4 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "WIDTH_BUCKET requires 4 arguments"}
		}
		for _, a := range args {
			if a == nil {
				return nil, nil
			}
		}
		wbValue := toFloat64Value(args[0])
		wbMin := toFloat64Value(args[1])
		wbMax := toFloat64Value(args[2])
		wbBuckets := toInt64Value(args[3])
		if wbBuckets <= 0 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "WIDTH_BUCKET: number of buckets must be positive"}
		}
		if wbMin >= wbMax {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "WIDTH_BUCKET: min must be less than max"}
		}
		if wbValue < wbMin {
			return int64(0), nil
		}
		if wbValue >= wbMax {
			return wbBuckets + 1, nil
		}
		wbBucket := int64((wbValue-wbMin)/(wbMax-wbMin)*float64(wbBuckets)) + 1
		if wbBucket > wbBuckets {
			wbBucket = wbBuckets
		}
		return wbBucket, nil

	case "BETA":
		if len(args) != 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "BETA requires 2 arguments"}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		betaA := toFloat64Value(args[0])
		betaB := toFloat64Value(args[1])
		lgA, _ := math.Lgamma(betaA)
		lgB, _ := math.Lgamma(betaB)
		lgAB, _ := math.Lgamma(betaA + betaB)
		return math.Exp(lgA + lgB - lgAB), nil

	case "GAMMA":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "GAMMA requires 1 argument",
			}
		}
		return gammaValue(args[0])

	case "LGAMMA":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LGAMMA requires 1 argument",
			}
		}
		return lgammaValue(args[0])

	case "FACTORIAL":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FACTORIAL requires 1 argument",
			}
		}
		return factorialValue(args[0])

	// Trigonometric functions
	case "SIN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SIN requires 1 argument",
			}
		}
		return sinValue(args[0])

	case "COS":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "COS requires 1 argument",
			}
		}
		return cosValue(args[0])

	case "TAN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TAN requires 1 argument",
			}
		}
		return tanValue(args[0])

	case "COT":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "COT requires 1 argument",
			}
		}
		return cotValue(args[0])

	case "ASIN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ASIN requires 1 argument",
			}
		}
		return asinValue(args[0])

	case "ACOS":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ACOS requires 1 argument",
			}
		}
		return acosValue(args[0])

	case "ATAN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ATAN requires 1 argument",
			}
		}
		return atanValue(args[0])

	case "ATAN2":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ATAN2 requires 2 arguments",
			}
		}
		return atan2Value(args[0], args[1])

	case "DEGREES":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "DEGREES requires 1 argument",
			}
		}
		return degreesValue(args[0])

	case "RADIANS":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RADIANS requires 1 argument",
			}
		}
		return radiansValue(args[0])

	case "PI":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "PI requires 0 arguments",
			}
		}
		return piValue()

	case "E":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "E requires 0 arguments",
			}
		}
		return math.E, nil

	case "INF", "INFINITY":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "INF requires 0 arguments",
			}
		}
		return math.Inf(1), nil

	case "NAN":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "NAN requires 0 arguments",
			}
		}
		return math.NaN(), nil

	case "RANDOM", "RAND":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RANDOM requires 0 arguments",
			}
		}
		return randomValue(ctx)

	case "UUID", "GEN_RANDOM_UUID":
		if len(args) != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "UUID requires 0 arguments",
			}
		}
		return uuid.New().String(), nil

	case "SETSEED":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SETSEED requires exactly 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		seedFloat, ok := toFloat64(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SETSEED: argument must be a numeric value",
			}
		}
		if seedFloat < 0 || seedFloat > 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SETSEED: seed must be between 0 and 1",
			}
		}
		seed := int64(seedFloat * float64(1<<63))
		ctx.conn.SetSetting("random_seed", fmt.Sprintf("%d", seed))
		return nil, nil

	case "SIGN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SIGN requires 1 argument",
			}
		}
		return signValue(args[0])

	case "GCD":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "GCD requires 2 arguments",
			}
		}
		return gcdValue(args[0], args[1])

	case "LCM":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LCM requires 2 arguments",
			}
		}
		return lcmValue(args[0], args[1])

	case "ISNAN":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ISNAN requires 1 argument",
			}
		}
		return isnanValue(args[0])

	case "ISINF":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ISINF requires 1 argument",
			}
		}
		return isinfValue(args[0])

	case "ISFINITE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ISFINITE requires 1 argument",
			}
		}
		return isfiniteValue(args[0])

	// Bitwise functions
	case "BIT_COUNT":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "BIT_COUNT requires 1 argument",
			}
		}
		return bitCountValue(args[0])

	case "BIT_LENGTH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("BIT_LENGTH requires exactly 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		switch v := args[0].(type) {
		case string:
			return int64(len(v) * 8), nil
		case []byte:
			return int64(len(v) * 8), nil
		default:
			s := toString(args[0])
			return int64(len(s) * 8), nil
		}

	case "GET_BIT":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("GET_BIT requires exactly 2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		var getBitData []byte
		switch v := args[0].(type) {
		case string:
			getBitData = []byte(v)
		case []byte:
			getBitData = v
		default:
			getBitData = []byte(toString(args[0]))
		}
		getBitIdx := toInt64Value(args[1])
		if getBitIdx < 0 || getBitIdx >= int64(len(getBitData)*8) {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("GET_BIT: bit index %d out of range [0, %d)", getBitIdx, len(getBitData)*8),
			}
		}
		getBitByteIdx := getBitIdx / 8
		getBitBitIdx := uint(getBitIdx % 8)
		return int32((getBitData[getBitByteIdx] >> (7 - getBitBitIdx)) & 1), nil

	case "SET_BIT":
		if len(args) != 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("SET_BIT requires exactly 3 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		var setBitData []byte
		switch v := args[0].(type) {
		case string:
			setBitData = []byte(v)
		case []byte:
			setBitData = make([]byte, len(v))
			copy(setBitData, v)
		default:
			setBitData = []byte(toString(args[0]))
		}
		setBitIdx := toInt64Value(args[1])
		newBit := toInt64Value(args[2])
		if newBit != 0 && newBit != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("SET_BIT: bit value must be 0 or 1, got %d", newBit),
			}
		}
		if setBitIdx < 0 || setBitIdx >= int64(len(setBitData)*8) {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("SET_BIT: bit index %d out of range [0, %d)", setBitIdx, len(setBitData)*8),
			}
		}
		setBitByteIdx := setBitIdx / 8
		setBitBitIdx := uint(setBitIdx % 8)
		if newBit == 1 {
			setBitData[setBitByteIdx] |= 1 << (7 - setBitBitIdx)
		} else {
			setBitData[setBitByteIdx] &^= 1 << (7 - setBitBitIdx)
		}
		if _, ok := args[0].(string); ok {
			return string(setBitData), nil
		}
		return setBitData, nil

	// Hyperbolic functions
	case "SINH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SINH requires 1 argument",
			}
		}
		return sinhValue(args[0])

	case "COSH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "COSH requires 1 argument",
			}
		}
		return coshValue(args[0])

	case "TANH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TANH requires 1 argument",
			}
		}
		return tanhValue(args[0])

	case "ASINH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ASINH requires 1 argument",
			}
		}
		return asinhValue(args[0])

	case "ACOSH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ACOSH requires 1 argument",
			}
		}
		return acoshValue(args[0])

	case "ATANH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ATANH requires 1 argument",
			}
		}
		return atanhValue(args[0])

	case "UPPER", "UCASE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "UPPER requires 1 argument",
			}
		}

		return strings.ToUpper(
			toString(args[0]),
		), nil

	case "LOWER", "LCASE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LOWER requires 1 argument",
			}
		}

		return strings.ToLower(
			toString(args[0]),
		), nil

	case "OCTET_LENGTH":
		if len(args) != 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "OCTET_LENGTH requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		return int64(len(toString(args[0]))), nil

	case "INITCAP":
		if len(args) != 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "INITCAP requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		initcapStr := toString(args[0])
		initcapResult := make([]byte, 0, len(initcapStr))
		capitalizeNext := true
		for i := 0; i < len(initcapStr); i++ {
			ch := initcapStr[i]
			if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
				ch == '_' || ch == '-' || ch == '.' || ch == ',' ||
				ch == ';' || ch == ':' || ch == '!' || ch == '?' {
				capitalizeNext = true
				initcapResult = append(initcapResult, ch)
			} else if capitalizeNext {
				if ch >= 'a' && ch <= 'z' {
					initcapResult = append(initcapResult, ch-32)
				} else {
					initcapResult = append(initcapResult, ch)
				}
				capitalizeNext = false
			} else {
				if ch >= 'A' && ch <= 'Z' {
					initcapResult = append(initcapResult, ch+32)
				} else {
					initcapResult = append(initcapResult, ch)
				}
			}
		}
		return string(initcapResult), nil

	case "SOUNDEX":
		if len(args) != 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "SOUNDEX requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		soundexStr := strings.ToUpper(toString(args[0]))
		if len(soundexStr) == 0 {
			return "", nil
		}
		soundexMap := map[byte]byte{
			'B': '1', 'F': '1', 'P': '1', 'V': '1',
			'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
			'D': '3', 'T': '3',
			'L': '4',
			'M': '5', 'N': '5',
			'R': '6',
		}
		soundexResult := []byte{soundexStr[0]}
		lastCode := soundexMap[soundexStr[0]]
		for i := 1; i < len(soundexStr) && len(soundexResult) < 4; i++ {
			code, ok := soundexMap[soundexStr[i]]
			if ok && code != lastCode {
				soundexResult = append(soundexResult, code)
				lastCode = code
			} else if !ok {
				lastCode = 0
			}
		}
		for len(soundexResult) < 4 {
			soundexResult = append(soundexResult, '0')
		}
		return string(soundexResult), nil

	case "LIKE_ESCAPE":
		if len(args) != 3 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "LIKE_ESCAPE requires 3 arguments (string, pattern, escape_char)"}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		likeEscStr := toString(args[0])
		likeEscPattern := toString(args[1])
		likeEscChar := byte(0)
		if args[2] != nil {
			esc := toString(args[2])
			if len(esc) > 0 {
				likeEscChar = esc[0]
			}
		}
		return matchLikeWithEscape(likeEscStr, likeEscPattern, likeEscChar), nil

	case "LENGTH",
		"CHAR_LENGTH",
		"CHARACTER_LENGTH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LENGTH requires 1 argument",
			}
		}

		return int64(len(toString(args[0]))), nil

	case "SPLIT_PART":
		if len(args) != 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SPLIT_PART requires 3 arguments (string, delimiter, index)",
			}
		}
		if args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		str := toString(args[0])
		delim := toString(args[1])
		spIdx, spOk := toInt64(args[2])
		if !spOk {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SPLIT_PART: index must be integer",
			}
		}
		parts := strings.Split(str, delim)
		if spIdx > 0 {
			// 1-based positive indexing
			if int(spIdx) > len(parts) {
				return "", nil
			}
			return parts[spIdx-1], nil
		} else if spIdx < 0 {
			// Negative indexing from end
			pos := len(parts) + int(spIdx)
			if pos < 0 {
				return "", nil
			}
			return parts[pos], nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "SPLIT_PART: index must not be zero",
		}

	case "TRIM":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TRIM requires 1 argument",
			}
		}

		return strings.TrimSpace(
			toString(args[0]),
		), nil

	case "LTRIM":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LTRIM requires 1 argument",
			}
		}

		return strings.TrimLeft(
			toString(args[0]),
			" ",
		), nil

	case "RTRIM":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RTRIM requires 1 argument",
			}
		}

		return strings.TrimRight(
			toString(args[0]),
			" ",
		), nil

	case "CONCAT":
		var result strings.Builder
		for _, arg := range args {
			result.WriteString(toString(arg))
		}

		return result.String(), nil

	case "COALESCE":
		for _, arg := range args {
			if arg != nil {
				return arg, nil
			}
		}

		return nil, nil

	case "IFNULL", "NVL":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("IFNULL requires exactly 2 arguments, got %d", len(args)),
			}
		}
		if args[0] != nil {
			return args[0], nil
		}
		return args[1], nil

	case "NULLIF":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "NULLIF requires 2 arguments",
			}
		}
		if compareValues(args[0], args[1]) == 0 {
			return nil, nil
		}

		return args[0], nil

	case "GREATEST":
		if len(args) == 0 {
			return nil, nil
		}
		var result any
		for _, arg := range args {
			if arg == nil {
				// GREATEST returns NULL if any argument is NULL (PostgreSQL behavior)
				return nil, nil
			}
			if result == nil || compareValues(arg, result) > 0 {
				result = arg
			}
		}
		return result, nil

	case "LEAST":
		if len(args) == 0 {
			return nil, nil
		}
		var result any
		for _, arg := range args {
			if arg == nil {
				// LEAST returns NULL if any argument is NULL (PostgreSQL behavior)
				return nil, nil
			}
			if result == nil || compareValues(arg, result) < 0 {
				result = arg
			}
		}
		return result, nil

	case "SUBSTR", "SUBSTRING":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SUBSTR requires 2 or 3 arguments",
			}
		}
		s := toString(args[0])
		start := int(
			toInt64Value(args[1]),
		) - 1 // SQL uses 1-based indexing
		if start < 0 {
			start = 0
		}
		if start >= len(s) {
			return "", nil
		}
		if len(args) == 3 {
			length := int(toInt64Value(args[2]))
			if start+length > len(s) {
				length = len(s) - start
			}

			return s[start : start+length], nil
		}

		return s[start:], nil

	case "REPLACE":
		if len(args) != 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REPLACE requires 3 arguments",
			}
		}

		return strings.ReplaceAll(
			toString(args[0]),
			toString(args[1]),
			toString(args[2]),
		), nil

	case "TRANSLATE":
		if len(args) != 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TRANSLATE requires 3 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		str := toString(args[0])
		fromStr := toString(args[1])
		toStr := toString(args[2])

		fromRunes := []rune(fromStr)
		toRunes := []rune(toStr)
		mapping := make(map[rune]rune)
		deleteSet := make(map[rune]bool)
		for i, r := range fromRunes {
			if i < len(toRunes) {
				mapping[r] = toRunes[i]
			} else {
				deleteSet[r] = true
			}
		}

		var sb strings.Builder
		for _, r := range str {
			if deleteSet[r] {
				continue
			}
			if replacement, ok := mapping[r]; ok {
				sb.WriteRune(replacement)
			} else {
				sb.WriteRune(r)
			}
		}
		return sb.String(), nil

	case "STRIP_ACCENTS":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "STRIP_ACCENTS requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		str := toString(args[0])
		// NFD decompose, then remove combining diacritical marks
		t := norm.NFD.String(str)
		var sb strings.Builder
		for _, r := range t {
			if !unicode.Is(unicode.Mn, r) {
				sb.WriteRune(r)
			}
		}
		return sb.String(), nil

	// Regular Expression Functions
	case "REGEXP_MATCHES":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REGEXP_MATCHES requires 2 arguments",
			}
		}
		return regexpMatchesValue(args[0], args[1])

	case "REGEXP_FULL_MATCH":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("REGEXP_FULL_MATCH requires 2 arguments, got %d", len(args)),
			}
		}
		return regexpFullMatchValue(args[0], args[1])

	case "REGEXP_REPLACE":
		if len(args) < 3 || len(args) > 4 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REGEXP_REPLACE requires 3 or 4 arguments",
			}
		}
		flags := interface{}(nil)
		if len(args) == 4 {
			flags = args[3]
		}
		return regexpReplaceValue(args[0], args[1], args[2], flags)

	case "REGEXP_EXTRACT":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REGEXP_EXTRACT requires 2 or 3 arguments",
			}
		}
		group := interface{}(nil)
		if len(args) == 3 {
			group = args[2]
		}
		return regexpExtractValue(args[0], args[1], group)

	case "REGEXP_EXTRACT_ALL":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REGEXP_EXTRACT_ALL requires 2 or 3 arguments",
			}
		}
		group := interface{}(nil)
		if len(args) == 3 {
			group = args[2]
		}
		return regexpExtractAllValue(args[0], args[1], group)

	case "REGEXP_SPLIT_TO_ARRAY":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REGEXP_SPLIT_TO_ARRAY requires 2 arguments",
			}
		}
		return regexpSplitToArrayValue(args[0], args[1])

	// Concatenation and Splitting
	case "CONCAT_WS":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "CONCAT_WS requires at least 2 arguments",
			}
		}
		return concatWSValue(args[0], args[1:]...)

	case "STRING_SPLIT", "STRING_TO_ARRAY":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "STRING_SPLIT requires 2 arguments",
			}
		}
		return stringSplitValue(args[0], args[1])

	case "STRING_SPLIT_REGEX":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "STRING_SPLIT_REGEX requires 2 arguments",
			}
		}
		return regexpSplitToArrayValue(args[0], args[1])

	// Padding Functions
	case "LPAD":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LPAD requires 2 or 3 arguments",
			}
		}
		fill := interface{}(nil)
		if len(args) == 3 {
			fill = args[2]
		}
		return lpadValue(args[0], args[1], fill)

	case "RPAD":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RPAD requires 2 or 3 arguments",
			}
		}
		fill := interface{}(nil)
		if len(args) == 3 {
			fill = args[2]
		}
		return rpadValue(args[0], args[1], fill)

	// String Manipulation
	case "REVERSE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REVERSE requires 1 argument",
			}
		}
		return reverseValue(args[0])

	case "REPEAT":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "REPEAT requires 2 arguments",
			}
		}
		return repeatValue(args[0], args[1])

	case "LEFT":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LEFT requires 2 arguments",
			}
		}
		return leftValue(args[0], args[1])

	case "RIGHT":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RIGHT requires 2 arguments",
			}
		}
		return rightValue(args[0], args[1])

	case "POSITION":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "POSITION requires 2 arguments",
			}
		}
		return positionValue(args[0], args[1])

	case "STRPOS", "INSTR":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "STRPOS requires 2 arguments",
			}
		}
		// Note: reversed argument order
		return positionValue(args[1], args[0])

	case "CONTAINS":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "CONTAINS requires 2 arguments",
			}
		}
		return containsValue(args[0], args[1])

	case "PREFIX", "STARTS_WITH":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "PREFIX requires 2 arguments",
			}
		}
		return prefixValue(args[0], args[1])

	case "SUFFIX", "ENDS_WITH":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SUFFIX requires 2 arguments",
			}
		}
		return suffixValue(args[0], args[1])

	// Encoding Functions
	case "ASCII", "ORD":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "ASCII requires 1 argument",
			}
		}
		return asciiValue(args[0])

	case "CHR":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "CHR requires 1 argument",
			}
		}
		return chrValue(args[0])

	case "UNICODE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "UNICODE requires 1 argument",
			}
		}
		return unicodeValue(args[0])

	case "ENCODE":
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENCODE requires 1-2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		encodeStr := toString(args[0])
		encodeCharset := "UTF-8"
		if len(args) == 2 && args[1] != nil {
			encodeCharset = strings.ToUpper(toString(args[1]))
		}
		switch encodeCharset {
		case "UTF-8", "UTF8":
			return []byte(encodeStr), nil
		case "LATIN1", "ISO-8859-1", "ISO88591":
			encoder := charmap.ISO8859_1.NewEncoder()
			encoded, err := encoder.Bytes([]byte(encodeStr))
			if err != nil {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  fmt.Sprintf("ENCODE: cannot encode to %s: %v", encodeCharset, err),
				}
			}
			return encoded, nil
		case "ASCII":
			result := make([]byte, 0, len(encodeStr))
			for _, b := range []byte(encodeStr) {
				if b <= 127 {
					result = append(result, b)
				}
			}
			return result, nil
		default:
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENCODE: unsupported encoding %q", encodeCharset),
			}
		}

	case "DECODE":
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("DECODE requires 1-2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		var decodeData []byte
		switch v := args[0].(type) {
		case []byte:
			decodeData = v
		case string:
			decodeData = []byte(v)
		default:
			decodeData = []byte(toString(args[0]))
		}
		decodeCharset := "UTF-8"
		if len(args) == 2 && args[1] != nil {
			decodeCharset = strings.ToUpper(toString(args[1]))
		}
		switch decodeCharset {
		case "UTF-8", "UTF8":
			return string(decodeData), nil
		case "LATIN1", "ISO-8859-1", "ISO88591":
			decoder := charmap.ISO8859_1.NewDecoder()
			decoded, err := decoder.Bytes(decodeData)
			if err != nil {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  fmt.Sprintf("DECODE: cannot decode from %s: %v", decodeCharset, err),
				}
			}
			return string(decoded), nil
		case "ASCII":
			return string(decodeData), nil
		default:
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("DECODE: unsupported encoding %q", decodeCharset),
			}
		}

	// Hash Functions
	case "MD5":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "MD5 requires 1 argument",
			}
		}
		return md5Value(args[0])

	case "SHA256":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SHA256 requires 1 argument",
			}
		}
		return sha256Value(args[0])

	case "SHA1":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SHA1 requires 1 argument",
			}
		}
		return sha1Value(args[0])

	case "SHA512":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "SHA512 requires 1 argument",
			}
		}
		return sha512Value(args[0])

	case "HASH":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "HASH requires 1 argument",
			}
		}
		return hashStringValue(args[0])

	// String Distance Functions
	case "LEVENSHTEIN":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LEVENSHTEIN requires 2 arguments",
			}
		}
		return levenshteinValue(args[0], args[1])

	case "DAMERAU_LEVENSHTEIN":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "DAMERAU_LEVENSHTEIN requires 2 arguments",
			}
		}
		return damerauLevenshteinValue(args[0], args[1])

	case "HAMMING":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "HAMMING requires 2 arguments",
			}
		}
		return hammingValue(args[0], args[1])

	case "JACCARD":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JACCARD requires 2 arguments",
			}
		}
		return jaccardValue(args[0], args[1])

	case "JARO_SIMILARITY":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JARO_SIMILARITY requires 2 arguments",
			}
		}
		return jaroSimilarityValue(args[0], args[1])

	case "JARO_WINKLER_SIMILARITY":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JARO_WINKLER_SIMILARITY requires 2 arguments",
			}
		}
		return jaroWinklerSimilarityValue(args[0], args[1])

	// Aliases for existing functions
	case "STRIP":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "STRIP requires 1 argument",
			}
		}
		return strings.TrimSpace(toString(args[0])), nil

	case "LSTRIP":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LSTRIP requires 1 argument",
			}
		}
		return strings.TrimLeft(toString(args[0]), " "), nil

	case "RSTRIP":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "RSTRIP requires 1 argument",
			}
		}
		return strings.TrimRight(toString(args[0]), " "), nil

	// Date/Time extraction functions
	case "YEAR":
		return evalYear(args)

	case "MONTH":
		return evalMonth(args)

	case "DAY":
		return evalDay(args)

	case "HOUR":
		return evalHour(args)

	case "MINUTE":
		return evalMinute(args)

	case "SECOND":
		return evalSecond(args)

	case "MILLISECOND":
		return evalMillisecond(args)

	case "MICROSECOND":
		return evalMicrosecond(args)

	case "DAYOFWEEK":
		return evalDayOfWeek(args)

	case "DAYOFYEAR":
		return evalDayOfYear(args)

	case "WEEK":
		return evalWeek(args)

	case "QUARTER":
		return evalQuarter(args)

	// Date arithmetic functions
	case "DATE_ADD", "DATEADD":
		return evalDateAdd(args)

	case "DATE_SUB":
		return evalDateSub(args)

	case "DATE_DIFF", "DATEDIFF":
		return evalDateDiff(args)

	case "DATE_TRUNC", "DATETRUNC":
		return evalDateTrunc(args)

	case "DATE_PART", "DATEPART":
		return evalDatePart(args)

	case "AGE":
		return evalAge(args)

	case "LAST_DAY":
		return evalLastDay(args)

	// Date construction functions
	case "MAKE_DATE":
		return evalMakeDate(args)

	case "TIME_BUCKET":
		if len(args) < 2 || len(args) > 3 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "TIME_BUCKET requires 2 or 3 arguments"}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		tbInterval, tbErr := toInterval(args[0])
		if tbErr != nil {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("TIME_BUCKET: invalid interval: %v", tbErr)}
		}
		tbTs, tbErr := toTime(args[1])
		if tbErr != nil {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("TIME_BUCKET: invalid timestamp: %v", tbErr)}
		}
		tbOrigin := time.Date(2000, 1, 3, 0, 0, 0, 0, tbTs.Location())
		if len(args) == 3 && args[2] != nil {
			tbOrigin, tbErr = toTime(args[2])
			if tbErr != nil {
				return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("TIME_BUCKET: invalid origin: %v", tbErr)}
			}
		}
		tbBucketMicros := tbInterval.Micros + int64(tbInterval.Days)*24*60*60*1_000_000
		if tbBucketMicros <= 0 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "TIME_BUCKET: bucket width must be positive"}
		}
		tbTsMicros := tbTs.UnixMicro()
		tbOriginMicros := tbOrigin.UnixMicro()
		tbDiff := tbTsMicros - tbOriginMicros
		tbBucketStart := tbOriginMicros + (tbDiff/tbBucketMicros)*tbBucketMicros
		if tbDiff < 0 && tbDiff%tbBucketMicros != 0 {
			tbBucketStart -= tbBucketMicros
		}
		return time.UnixMicro(tbBucketStart), nil

	case "MAKE_TIMESTAMP":
		return evalMakeTimestamp(args)

	case "MAKE_TIMESTAMPTZ":
		if len(args) < 6 || len(args) > 7 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "MAKE_TIMESTAMPTZ requires 6 or 7 arguments"}
		}
		for _, mtzArg := range args[:6] {
			if mtzArg == nil {
				return nil, nil
			}
		}
		mtzYear := int(toInt64Value(args[0]))
		mtzMonth := time.Month(toInt64Value(args[1]))
		mtzDay := int(toInt64Value(args[2]))
		mtzHour := int(toInt64Value(args[3]))
		mtzMin := int(toInt64Value(args[4]))
		mtzSec := toFloat64Value(args[5])
		mtzWholeSec := int(mtzSec)
		mtzNsec := int((mtzSec - float64(mtzWholeSec)) * 1e9)
		mtzLoc := time.UTC
		if len(args) == 7 && args[6] != nil {
			mtzTzName := toString(args[6])
			var mtzErr error
			mtzLoc, mtzErr = time.LoadLocation(mtzTzName)
			if mtzErr != nil {
				return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("MAKE_TIMESTAMPTZ: unknown timezone: %s", mtzTzName)}
			}
		}
		return time.Date(mtzYear, mtzMonth, mtzDay, mtzHour, mtzMin, mtzWholeSec, mtzNsec, mtzLoc), nil

	case "MAKE_TIME":
		return evalMakeTime(args)

	// Formatting/Parsing functions
	case "STRFTIME", "TO_CHAR":
		return evalStrftime(args)

	case "STRPTIME":
		return evalStrptime(args)

	case "TO_DATE":
		// TO_DATE(string, format) or TO_DATE(string)
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("TO_DATE requires 1 or 2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		if len(args) == 2 {
			// Use STRPTIME-style parsing, then convert to DATE
			result, err := evalStrptime(args)
			if err != nil {
				return nil, err
			}
			// evalStrptime returns int64 (microseconds since epoch)
			if ts, ok := result.(int64); ok {
				t := timestampToTime(ts)
				return timeToDate(t), nil
			}
			return result, nil
		}
		// 1-arg: auto-detect ISO format
		toDateStr := toString(args[0])
		toDateT, toDateErr := time.Parse("2006-01-02", toDateStr)
		if toDateErr != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("TO_DATE: cannot parse %q as date", toDateStr),
			}
		}
		return timeToDate(toDateT), nil

	case "TO_TIMESTAMP":
		return evalToTimestamp(args)

	case "NOW", "CURRENT_TIMESTAMP":
		return time.Now(), nil

	case "CURRENT_DATE", "TODAY":
		now := time.Now()
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()), nil

	case "CURRENT_TIME":
		now := time.Now()
		return time.Date(0, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), now.Location()), nil

	case "EPOCH":
		return evalEpoch(args)

	case "EPOCH_MS":
		return evalEpochMs(args)

	case "EPOCH_NS":
		if len(args) != 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "EPOCH_NS requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		epochNs := toInt64Value(args[0])
		return time.Unix(0, epochNs), nil

	case "TIMEZONE":
		if len(args) != 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "TIMEZONE requires 2 arguments"}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		tzName := toString(args[0])
		tzLoc, tzErr := time.LoadLocation(tzName)
		if tzErr != nil {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("TIMEZONE: unknown timezone: %s", tzName)}
		}
		tzTs, tzErr := toTime(args[1])
		if tzErr != nil {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("TIMEZONE: invalid timestamp: %v", tzErr)}
		}
		return tzTs.In(tzLoc), nil

	// Interval extraction functions
	case "TO_YEARS":
		return evalToYears(args)

	case "TO_MONTHS":
		return evalToMonths(args)

	case "TO_DAYS":
		return evalToDays(args)

	case "TO_HOURS":
		return evalToHours(args)

	case "TO_MINUTES":
		return evalToMinutes(args)

	case "TO_SECONDS":
		return evalToSeconds(args)

	// Total extraction functions
	case "TOTAL_YEARS":
		return evalTotalYears(args)

	case "TOTAL_MONTHS":
		return evalTotalMonths(args)

	case "TOTAL_DAYS":
		return evalTotalDays(args)

	case "TOTAL_HOURS":
		return evalTotalHours(args)

	case "TOTAL_MINUTES":
		return evalTotalMinutes(args)

	case "TOTAL_SECONDS":
		return evalTotalSeconds(args)

	// JSON functions
	case "JSON_EXTRACT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_EXTRACT requires 2 arguments",
			}
		}
		return extractJSONValue(args[0], args[1], false)

	case "JSON_EXTRACT_STRING", "JSON_EXTRACT_TEXT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fn.Name + " requires 2 arguments",
			}
		}
		return extractJSONValue(args[0], args[1], true)

	case "JSON_VALID":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_VALID requires 1 argument",
			}
		}
		return isValidJSON(args[0])

	case "JSON_TYPE":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_TYPE requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalJSONType(args[0])

	case "JSON_KEYS":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_KEYS requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalJSONKeys(args[0])

	case "JSON_ARRAY_LENGTH":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_ARRAY_LENGTH requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalJSONArrayLength(args[0])

	case "TO_JSON":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TO_JSON requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalToJSON(args[0])

	case "JSON_MERGE_PATCH":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_MERGE_PATCH requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalJSONMergePatch(args[0], args[1])

	case "JSON_OBJECT", "JSON_BUILD_OBJECT":
		return evalJSONObject(args)

	case "JSON_ARRAY":
		return evalJSONArray(args)

	// Geometry functions
	case "ST_GEOMFROMTEXT", "ST_GEOMETRYFROMTEXT":
		return executeSTGeomFromText(args)
	case "ST_ASTEXT", "ST_ASWKT":
		return executeSTAsText(args)
	case "ST_ASBINARY", "ST_ASWKB":
		return executeSTAsBinary(args)
	case "ST_GEOMETRYTYPE":
		return executeSTGeometryType(args)
	case "ST_X":
		return executeSTX(args)
	case "ST_Y":
		return executeSTY(args)
	case "ST_Z":
		return executeSTZ(args)
	case "ST_SRID":
		return executeSTSRID(args)
	case "ST_SETSRID":
		return executeSTSetSRID(args)
	case "ST_POINT":
		return executeSTPoint(args)
	case "ST_MAKELINE":
		return executeSTMakeLine(args)

	// Spatial distance functions
	case "ST_DISTANCE":
		return executeSTDistance(args)
	case "ST_DISTANCE_SPHERE":
		return executeSTDistanceSphere(args)

	// Spatial predicate functions
	case "ST_CONTAINS":
		return executeSTContains(args)
	case "ST_WITHIN":
		return executeSTWithin(args)
	case "ST_INTERSECTS":
		return executeSTIntersects(args)
	case "ST_DISJOINT":
		return executeSTDisjoint(args)
	case "ST_TOUCHES":
		return executeSTTouches(args)
	case "ST_CROSSES":
		return executeSTCrosses(args)
	case "ST_OVERLAPS":
		return executeSTOverlaps(args)
	case "ST_EQUALS":
		return executeSTEquals(args)

	// Spatial analysis functions
	case "ST_ENVELOPE":
		return executeSTEnvelope(args)

	// Geometric analysis functions (Phase 4)
	case "ST_AREA":
		return executeSTArea(args)
	case "ST_LENGTH", "ST_PERIMETER":
		return executeSTLength(args)
	case "ST_CENTROID":
		return executeSTCentroid(args)

	// Set operations (Phase 5)
	case "ST_UNION":
		return executeSTUnion(args)
	case "ST_INTERSECTION":
		return executeSTIntersection(args)
	case "ST_DIFFERENCE":
		return executeSTDifference(args)
	case "ST_BUFFER":
		return executeSTBuffer(args)
	case "ST_MAKEPOLYGON":
		return executeSTMakePolygon(args)

	// ---- Struct functions ----
	case "STRUCT_INSERT":
		// STRUCT_INSERT(struct, key1, val1, key2, val2, ...)
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "struct_insert requires at least 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		srcStruct, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("struct_insert: first argument must be a struct, got %T", args[0]),
			}
		}
		if (len(args)-1)%2 != 0 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "struct_insert: key/value arguments must come in pairs",
			}
		}
		result := make(map[string]any, len(srcStruct)+(len(args)-1)/2)
		for k, v := range srcStruct {
			result[k] = v
		}
		for i := 1; i < len(args); i += 2 {
			key := toString(args[i])
			result[key] = args[i+1]
		}
		return result, nil

	case "STRUCT_KEYS":
		// STRUCT_KEYS(struct) - returns sorted list of field names
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "struct_keys requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		sk, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("struct_keys: argument must be a struct, got %T", args[0]),
			}
		}
		skKeys := make([]string, 0, len(sk))
		for k := range sk {
			skKeys = append(skKeys, k)
		}
		sort.Strings(skKeys)
		skResult := make([]any, len(skKeys))
		for i, k := range skKeys {
			skResult[i] = k
		}
		return skResult, nil

	case "STRUCT_EXTRACT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "struct_extract requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		structVal, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("struct_extract: first argument must be a struct, got %T", args[0]),
			}
		}
		fieldName := toString(args[1])
		val, exists := structVal[fieldName]
		if !exists {
			return nil, nil
		}
		return val, nil

	// ---- List constructor functions ----
	case "LIST_VALUE", "LIST_PACK":
		result := make([]any, 0, len(args))
		result = append(result, args...)
		return result, nil

	// ---- Map functions ----
	case "MAP":
		// MAP(keys_array, values_array)
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "MAP requires 2 arguments (keys array, values array)",
			}
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		keys, ok1 := toSlice(args[0])
		vals, ok2 := toSlice(args[1])
		if !ok1 || !ok2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "MAP requires two array arguments",
			}
		}
		if len(keys) != len(vals) {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "MAP key and value arrays must have the same length",
			}
		}
		result := make(map[string]any, len(keys))
		for i := 0; i < len(keys); i++ {
			result[toString(keys[i])] = vals[i]
		}
		return result, nil

	case "MAP_KEYS":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_keys requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_keys: argument must be a map, got %T", args[0]),
			}
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		result := make([]any, len(keys))
		for i, k := range keys {
			result[i] = k
		}
		return result, nil

	case "MAP_VALUES":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_values requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_values: argument must be a map, got %T", args[0]),
			}
		}
		// Get values in key-sorted order for determinism
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		vals := make([]any, len(keys))
		for i, k := range keys {
			vals[i] = m[k]
		}
		return vals, nil

	case "MAP_CONTAINS_KEY":
		// MAP_CONTAINS_KEY(map, key) - returns bool
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_contains_key requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_contains_key: first argument must be a map, got %T", args[0]),
			}
		}
		searchKey := toString(args[1])
		_, exists := m[searchKey]
		return exists, nil

	case "MAP_ENTRIES":
		// MAP_ENTRIES(map) - returns []any of {key, value} maps
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_entries requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_entries: argument must be a map, got %T", args[0]),
			}
		}
		meKeys := make([]string, 0, len(m))
		for k := range m {
			meKeys = append(meKeys, k)
		}
		sort.Strings(meKeys)
		entries := make([]any, len(meKeys))
		for i, k := range meKeys {
			entries[i] = map[string]any{"key": k, "value": m[k]}
		}
		return entries, nil

	case "MAP_FROM_ENTRIES":
		// MAP_FROM_ENTRIES(entries) - takes []any of {key, value} maps, returns map
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_from_entries requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		mfeSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_from_entries: argument must be an array, got %T", args[0]),
			}
		}
		result := make(map[string]any, len(mfeSlice))
		for _, entry := range mfeSlice {
			entryMap, ok := entry.(map[string]any)
			if !ok {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  fmt.Sprintf("map_from_entries: each entry must be a struct with key/value fields, got %T", entry),
				}
			}
			k, hasKey := entryMap["key"]
			v, hasVal := entryMap["value"]
			if !hasKey || !hasVal {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  "map_from_entries: each entry must have 'key' and 'value' fields",
				}
			}
			result[toString(k)] = v
		}
		return result, nil

	case "MAP_EXTRACT":
		// MAP_EXTRACT(map, key) - extract value by key from map
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "map_extract requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("map_extract: first argument must be a map, got %T", args[0]),
			}
		}
		extractKey := toString(args[1])
		val, exists := m[extractKey]
		if !exists {
			return nil, nil
		}
		return val, nil

	case "ELEMENT_AT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "element_at requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		m, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("element_at: first argument must be a map, got %T", args[0]),
			}
		}
		key := toString(args[1])
		val, exists := m[key]
		if !exists {
			return nil, nil
		}
		return val, nil

	case "UNION_TAG":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "union_tag requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		utMap, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("union_tag: argument must be a union (map), got %T", args[0]),
			}
		}
		tag, exists := utMap["__tag"]
		if !exists {
			return nil, nil
		}
		return toString(tag), nil

	case "UNION_EXTRACT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "union_extract requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		ueMap, ok := args[0].(map[string]any)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("union_extract: first argument must be a union (map), got %T", args[0]),
			}
		}
		requestedTag := toString(args[1])
		currentTag := ueMap["__tag"]
		if toString(currentTag) == requestedTag {
			return ueMap["__value"], nil
		}
		return nil, nil

	case "LIST_CONTAINS", "ARRAY_CONTAINS", "LIST_HAS":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_contains requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		lcSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("list_contains: first argument must be a list, got %T", args[0]),
			}
		}
		needle := args[1]
		needleStr := fmt.Sprintf("%v", needle)
		for _, elem := range lcSlice {
			if elem == needle || fmt.Sprintf("%v", elem) == needleStr {
				return true, nil
			}
		}
		return false, nil

	case "LIST_POSITION", "ARRAY_POSITION", "LIST_INDEXOF":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_position requires 2 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		lpSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("list_position: first argument must be a list, got %T", args[0]),
			}
		}
		lpNeedle := args[1]
		lpNeedleStr := fmt.Sprintf("%v", lpNeedle)
		for i, elem := range lpSlice {
			if elem == lpNeedle || fmt.Sprintf("%v", elem) == lpNeedleStr {
				return int64(i + 1), nil
			}
		}
		return nil, nil

	case "LIST_APPEND", "ARRAY_APPEND", "ARRAY_PUSH_BACK":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("LIST_APPEND requires 2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		laSlice, laOk := toSlice(args[0])
		if !laOk {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LIST_APPEND first argument must be a list",
			}
		}
		laResult := make([]any, len(laSlice)+1)
		copy(laResult, laSlice)
		laResult[len(laSlice)] = args[1]
		return laResult, nil

	case "LIST_PREPEND", "ARRAY_PREPEND", "ARRAY_PUSH_FRONT":
		// DuckDB signature: LIST_PREPEND(element, list) — element FIRST
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("LIST_PREPEND requires 2 arguments, got %d", len(args)),
			}
		}
		if args[1] == nil {
			return nil, nil
		}
		lpList, lpListOk := toSlice(args[1])
		if !lpListOk {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LIST_PREPEND second argument must be a list",
			}
		}
		lpResult := make([]any, len(lpList)+1)
		lpResult[0] = args[0]
		copy(lpResult[1:], lpList)
		return lpResult, nil

	case "LIST_CONCAT", "ARRAY_CONCAT", "ARRAY_CAT":
		if len(args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_concat requires 2 arguments",
			}
		}
		if args[0] == nil && args[1] == nil {
			return nil, nil
		}
		var concatResult []any
		if args[0] != nil {
			s1, ok := toSlice(args[0])
			if !ok {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  fmt.Sprintf("list_concat: first argument must be a list, got %T", args[0]),
				}
			}
			concatResult = append(concatResult, s1...)
		}
		if args[1] != nil {
			s2, ok := toSlice(args[1])
			if !ok {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  fmt.Sprintf("list_concat: second argument must be a list, got %T", args[1]),
				}
			}
			concatResult = append(concatResult, s2...)
		}
		return concatResult, nil

	case "LIST_DISTINCT", "ARRAY_DISTINCT", "LIST_UNIQUE", "ARRAY_UNIQUE":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_distinct requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		ldSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("list_distinct: argument must be a list, got %T", args[0]),
			}
		}
		seen := make(map[string]bool, len(ldSlice))
		ldResult := make([]any, 0, len(ldSlice))
		for _, elem := range ldSlice {
			key := fmt.Sprintf("%v", elem)
			if !seen[key] {
				seen[key] = true
				ldResult = append(ldResult, elem)
			}
		}
		return ldResult, nil

	case "LIST_REVERSE", "ARRAY_REVERSE":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_reverse requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		lrSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("list_reverse: argument must be a list, got %T", args[0]),
			}
		}
		lrResult := make([]any, len(lrSlice))
		for i, elem := range lrSlice {
			lrResult[len(lrSlice)-1-i] = elem
		}
		return lrResult, nil

	case "LIST_SLICE", "ARRAY_SLICE":
		if len(args) < 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "list_slice requires 3 arguments",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		lsSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("list_slice: first argument must be a list, got %T", args[0]),
			}
		}
		begin := int(toInt64Value(args[1]))
		end := int(toInt64Value(args[2]))
		if begin < 1 {
			begin = 1
		}
		if end > len(lsSlice) {
			end = len(lsSlice)
		}
		if begin > end+1 || begin > len(lsSlice) {
			return []any{}, nil
		}
		// Convert from 1-based to 0-based indexing
		return append([]any{}, lsSlice[begin-1:end]...), nil

	case "FLATTEN":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "flatten requires 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		fSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("flatten: argument must be a list, got %T", args[0]),
			}
		}
		var flatResult []any
		for _, elem := range fSlice {
			if inner, ok := toSlice(elem); ok {
				flatResult = append(flatResult, inner...)
			} else {
				flatResult = append(flatResult, elem)
			}
		}
		return flatResult, nil

	case "IF", "IFF":
		if len(args) != 3 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("IF requires 3 arguments, got %d", len(args)),
			}
		}
		// IF does NOT return NULL when condition is NULL — returns false_value
		cond := args[0]
		if cond == nil || !toBool(cond) {
			return args[2], nil
		}
		return args[1], nil

	case "TYPEOF":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "TYPEOF requires 1 argument",
			}
		}
		return duckdbTypeName(args[0]), nil

	case "PG_TYPEOF":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "PG_TYPEOF requires 1 argument",
			}
		}
		return pgTypeName(args[0]), nil

	case "BASE64_ENCODE", "BASE64", "TO_BASE64":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("BASE64_ENCODE requires 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		var data []byte
		switch v := args[0].(type) {
		case []byte:
			data = v
		case string:
			data = []byte(v)
		default:
			data = []byte(toString(args[0]))
		}
		return base64.StdEncoding.EncodeToString(data), nil

	case "BASE64_DECODE", "FROM_BASE64":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("BASE64_DECODE requires 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		s := toString(args[0])
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("BASE64_DECODE: invalid base64: %v", err),
			}
		}
		return decoded, nil

	case "URL_ENCODE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("URL_ENCODE requires 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return url.QueryEscape(toString(args[0])), nil

	case "URL_DECODE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("URL_DECODE requires 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		decodedURL, err := url.QueryUnescape(toString(args[0]))
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("URL_DECODE: invalid encoding: %v", err),
			}
		}
		return decodedURL, nil

	case "FORMAT", "PRINTF":
		if len(args) < 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FORMAT requires at least 1 argument",
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		fmtStr := toString(args[0])
		return formatString(fmtStr, args[1:])

	case "JSON_CONTAINS":
		if len(args) != 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("JSON_CONTAINS requires 2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		return evalJSONContains(args[0], args[1])

	case "JSON_QUOTE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("JSON_QUOTE requires 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return "null", nil
		}
		quoted, err := json.Marshal(args[0])
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("JSON_QUOTE: cannot quote value: %v", err),
			}
		}
		return string(quoted), nil

	case "LIST_ELEMENT", "ARRAY_EXTRACT":
		if len(args) < 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_element requires 2 arguments"}
		}
		if args[0] == nil {
			return nil, nil
		}
		leSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_element: first argument must be a list, got %T", args[0])}
		}
		idx, ok := toInt64(args[1])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_element: index must be an integer"}
		}
		// 1-based indexing, negative counts from end
		if idx == 0 {
			return nil, nil // out of bounds
		}
		var absIdx int
		if idx > 0 {
			absIdx = int(idx) - 1
		} else {
			absIdx = len(leSlice) + int(idx)
		}
		if absIdx < 0 || absIdx >= len(leSlice) {
			return nil, nil // out of bounds returns NULL
		}
		return leSlice[absIdx], nil

	case "LIST_AGGREGATE", "ARRAY_AGGREGATE":
		if len(args) < 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_aggregate requires at least 2 arguments"}
		}
		if args[0] == nil {
			return nil, nil
		}
		laSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_aggregate: first argument must be a list, got %T", args[0])}
		}
		aggName := strings.ToLower(toString(args[1]))
		return evaluateListAggregate(laSlice, aggName, args[2:])

	case "LIST_REVERSE_SORT", "ARRAY_REVERSE_SORT":
		if len(args) < 1 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_reverse_sort requires 1 argument"}
		}
		if args[0] == nil {
			return nil, nil
		}
		lrsSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_reverse_sort: argument must be a list, got %T", args[0])}
		}
		result := make([]any, len(lrsSlice))
		copy(result, lrsSlice)
		sort.SliceStable(result, func(i, j int) bool {
			// NULLs sort to end
			if result[i] == nil && result[j] == nil {
				return false
			}
			if result[i] == nil {
				return false
			}
			if result[j] == nil {
				return true
			}
			return compareValues(result[i], result[j]) > 0
		})
		return result, nil

	case "ARRAY_TO_STRING", "LIST_TO_STRING":
		if len(args) < 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "array_to_string requires at least 2 arguments"}
		}
		if args[0] == nil {
			return nil, nil
		}
		atsSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("array_to_string: first argument must be a list, got %T", args[0])}
		}
		sep := toString(args[1])
		nullStr := ""
		hasNullStr := len(args) >= 3 && args[2] != nil
		if hasNullStr {
			nullStr = toString(args[2])
		}
		var atsParts []string
		for _, v := range atsSlice {
			if v == nil {
				if hasNullStr {
					atsParts = append(atsParts, nullStr)
				}
				// Skip NULLs when no null_string provided
				continue
			}
			atsParts = append(atsParts, toString(v))
		}
		return strings.Join(atsParts, sep), nil

	case "LIST_ZIP":
		if len(args) < 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_zip requires at least 2 arguments"}
		}
		// Convert all args to slices
		var lzLists [][]any
		lzMaxLen := 0
		for i, arg := range args {
			if arg == nil {
				lzLists = append(lzLists, nil)
				continue
			}
			s, ok := toSlice(arg)
			if !ok {
				return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_zip: argument %d must be a list, got %T", i+1, arg)}
			}
			lzLists = append(lzLists, s)
			if len(s) > lzMaxLen {
				lzMaxLen = len(s)
			}
		}
		// Build result: list of structs (map[string]any)
		lzResult := make([]any, lzMaxLen)
		for i := 0; i < lzMaxLen; i++ {
			row := make(map[string]any)
			for j, list := range lzLists {
				key := fmt.Sprintf("f%d", j+1)
				if list != nil && i < len(list) {
					row[key] = list[i]
				} else {
					row[key] = nil
				}
			}
			lzResult[i] = row
		}
		return lzResult, nil

	case "LIST_RESIZE", "ARRAY_RESIZE":
		if len(args) < 2 {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_resize requires at least 2 arguments"}
		}
		if args[0] == nil {
			return nil, nil
		}
		lrSlice, ok := toSlice(args[0])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_resize: first argument must be a list, got %T", args[0])}
		}
		lrSize, ok := toInt64(args[1])
		if !ok {
			return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "list_resize: size must be an integer"}
		}
		if lrSize < 0 {
			lrSize = 0
		}
		var lrFillValue any
		if len(args) >= 3 {
			lrFillValue = args[2]
		}
		lrResult := make([]any, int(lrSize))
		for i := 0; i < int(lrSize); i++ {
			if i < len(lrSlice) {
				lrResult[i] = lrSlice[i]
			} else {
				lrResult[i] = lrFillValue
			}
		}
		return lrResult, nil

	case "GENERATE_SUBSCRIPTS":
		if len(args) < 1 || len(args) > 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("GENERATE_SUBSCRIPTS requires 1 or 2 arguments, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		gsArr, gsOk := args[0].([]any)
		if !gsOk {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "GENERATE_SUBSCRIPTS requires a list argument",
			}
		}
		gsResult := make([]any, len(gsArr))
		for gsI := range gsArr {
			gsResult[gsI] = int64(gsI + 1)
		}
		return gsResult, nil

	// System functions
	case "CURRENT_DATABASE":
		if ctx.conn != nil {
			dbName := ctx.conn.GetSetting("database_name")
			if dbName != "" {
				return dbName, nil
			}
		}
		return "memory", nil

	case "CURRENT_SCHEMA":
		if ctx.conn != nil {
			schema := ctx.conn.GetSetting("search_path")
			if schema != "" {
				return schema, nil
			}
		}
		return "main", nil

	case "VERSION":
		return "v1.4.3 (dukdb-go)", nil

	// Date/time functions
	case "DAYNAME":
		if len(args) < 1 {
			return nil, fmt.Errorf("DAYNAME requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		dnTime, err := toTime(args[0])
		if err != nil {
			return nil, err
		}
		return dnTime.Weekday().String(), nil

	case "MONTHNAME":
		if len(args) < 1 {
			return nil, fmt.Errorf("MONTHNAME requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		mnTime, err := toTime(args[0])
		if err != nil {
			return nil, err
		}
		return mnTime.Month().String(), nil

	case "YEARWEEK":
		if len(args) < 1 {
			return nil, fmt.Errorf("YEARWEEK requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		ywTime, err := toTime(args[0])
		if err != nil {
			return nil, err
		}
		ywYear, ywWeek := ywTime.ISOWeek()
		return int64(ywYear*100 + ywWeek), nil

	case "EPOCH_US":
		if len(args) < 1 {
			return nil, fmt.Errorf("EPOCH_US requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		euTime, err := toTime(args[0])
		if err != nil {
			return nil, err
		}
		return euTime.UnixMicro(), nil

	case "ENUM_RANGE":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_RANGE requires exactly 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		typeName := toString(args[0])
		typeEntry, ok := e.catalog.GetType(typeName, "")
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_RANGE: type %q not found", typeName),
			}
		}
		if typeEntry.TypeKind != "ENUM" {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_RANGE: type %q is not an ENUM type", typeName),
			}
		}
		result := make([]any, len(typeEntry.EnumValues))
		for i, v := range typeEntry.EnumValues {
			result[i] = stripEnumQuotes(v)
		}
		return result, nil

	case "ENUM_FIRST":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_FIRST requires exactly 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		typeName := toString(args[0])
		typeEntry, ok := e.catalog.GetType(typeName, "")
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_FIRST: type %q not found", typeName),
			}
		}
		if len(typeEntry.EnumValues) == 0 {
			return nil, nil
		}
		return stripEnumQuotes(typeEntry.EnumValues[0]), nil

	case "ENUM_LAST":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_LAST requires exactly 1 argument, got %d", len(args)),
			}
		}
		if args[0] == nil {
			return nil, nil
		}
		typeName := toString(args[0])
		typeEntry, ok := e.catalog.GetType(typeName, "")
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("ENUM_LAST: type %q not found", typeName),
			}
		}
		if len(typeEntry.EnumValues) == 0 {
			return nil, nil
		}
		return stripEnumQuotes(typeEntry.EnumValues[len(typeEntry.EnumValues)-1]), nil

	default:
		// For aggregate functions called in scalar context, return NULL
		// The main fix for aggregate functions in projections is in executeProject
		// which looks up pre-computed aggregate results by alias
		switch fn.Name {
		// Basic aggregates
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return nil, nil
		// Statistical aggregates
		case "MEDIAN", "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC",
			"MODE", "ENTROPY", "SKEWNESS", "KURTOSIS",
			"VAR_POP", "VAR_SAMP", "VARIANCE",
			"STDDEV_POP", "STDDEV_SAMP", "STDDEV":
			return nil, nil
		// Approximate aggregates
		case "APPROX_COUNT_DISTINCT", "APPROX_QUANTILE", "APPROX_MEDIAN":
			return nil, nil
		// String/List aggregates
		case "STRING_AGG", "GROUP_CONCAT", "LISTAGG", "LIST", "ARRAY_AGG", "LIST_DISTINCT":
			return nil, nil
		// Time series aggregates
		case "COUNT_IF", "FIRST", "LAST", "ANY_VALUE", "ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY", "HISTOGRAM":
			return nil, nil
		// Regression aggregates
		case "COVAR_POP", "COVAR_SAMP", "CORR",
			"REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2",
			"REGR_COUNT", "REGR_AVGX", "REGR_AVGY",
			"REGR_SXX", "REGR_SYY", "REGR_SXY":
			return nil, nil
		// Boolean aggregates
		case "BOOL_AND", "BOOL_OR", "EVERY":
			return nil, nil
		// Bitwise aggregates
		case "BIT_AND", "BIT_OR", "BIT_XOR":
			return nil, nil
		// JSON aggregates
		case "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT":
			return nil, nil
		// Round 3 aggregates
		case "PRODUCT", "MAD", "FAVG", "FSUM", "BITSTRING_AGG":
			return nil, nil
		// Round 4 aggregates
		case "ARBITRARY", "MEAN", "GEOMETRIC_MEAN", "GEOMEAN", "WEIGHTED_AVG":
			return nil, nil
		}

		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unknown function: %s",
				fn.Name,
			),
		}
	}
}

func (e *Executor) evaluateCaseExpr(
	ctx *ExecutionContext,
	expr *binder.BoundCaseExpr,
	row map[string]any,
) (any, error) {
	var operand any
	var err error

	if expr.Operand != nil {
		operand, err = e.evaluateExpr(
			ctx,
			expr.Operand,
			row,
		)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		condVal, err := e.evaluateExpr(
			ctx,
			when.Condition,
			row,
		)
		if err != nil {
			return nil, err
		}

		var matches bool
		if expr.Operand != nil {
			// Simple CASE
			matches = compareValues(
				operand,
				condVal,
			) == 0
		} else {
			// Searched CASE
			matches = toBool(condVal)
		}

		if matches {
			return e.evaluateExpr(
				ctx,
				when.Result,
				row,
			)
		}
	}

	if expr.Else != nil {
		return e.evaluateExpr(ctx, expr.Else, row)
	}

	return nil, nil
}

func (e *Executor) evaluateBetweenExpr(
	ctx *ExecutionContext,
	expr *binder.BoundBetweenExpr,
	row map[string]any,
) (any, error) {
	val, err := e.evaluateExpr(
		ctx,
		expr.Expr,
		row,
	)
	if err != nil {
		return nil, err
	}

	low, err := e.evaluateExpr(ctx, expr.Low, row)
	if err != nil {
		return nil, err
	}

	high, err := e.evaluateExpr(
		ctx,
		expr.High,
		row,
	)
	if err != nil {
		return nil, err
	}

	if val == nil || low == nil || high == nil {
		return nil, nil
	}

	inRange := compareValues(val, low) >= 0 &&
		compareValues(val, high) <= 0
	if expr.Not {
		return !inRange, nil
	}

	return inRange, nil
}

func (e *Executor) evaluateInListExpr(
	ctx *ExecutionContext,
	expr *binder.BoundInListExpr,
	row map[string]any,
) (any, error) {
	val, err := e.evaluateExpr(
		ctx,
		expr.Expr,
		row,
	)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	for _, item := range expr.Values {
		itemVal, err := e.evaluateExpr(
			ctx,
			item,
			row,
		)
		if err != nil {
			return nil, err
		}
		if itemVal != nil &&
			compareValues(val, itemVal) == 0 {
			if expr.Not {
				return false, nil
			}

			return true, nil
		}
	}

	if expr.Not {
		return true, nil
	}

	return false, nil
}

// evaluateArrayExpr evaluates an array literal expression.
// Returns a []any slice containing the evaluated elements.
// This is primarily used for passing arrays of file paths to table functions.
func (e *Executor) evaluateArrayExpr(
	ctx *ExecutionContext,
	expr *binder.BoundArrayExpr,
	row map[string]any,
) (any, error) {
	result := make([]any, 0, len(expr.Elements))

	for _, elem := range expr.Elements {
		val, err := e.evaluateExpr(ctx, elem, row)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}

	return result, nil
}

func (e *Executor) evaluateInSubqueryExpr(
	ctx *ExecutionContext,
	expr *binder.BoundInSubqueryExpr,
	row map[string]any,
) (any, error) {
	// Evaluate the left-hand side expression
	val, err := e.evaluateExpr(
		ctx,
		expr.Expr,
		row,
	)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Execute the subquery to get the result set
	// We need to plan and execute the subquery
	plan, err := e.planner.Plan(expr.Subquery)
	if err != nil {
		return nil, err
	}

	subqueryResult, err := e.Execute(
		ctx.Context,
		plan,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Check if val is in the subquery results
	// Subquery should return a single column
	for _, subRow := range subqueryResult.Rows {
		// Get the first (and should be only) column value
		for _, subVal := range subRow {
			if subVal != nil &&
				compareValues(val, subVal) == 0 {
				if expr.Not {
					return false, nil
				}

				return true, nil
			}

			break // Only check first column
		}
	}

	if expr.Not {
		return true, nil
	}

	return false, nil
}

func (e *Executor) evaluateQuantifiedComparison(
	ctx *ExecutionContext,
	expr *binder.BoundQuantifiedComparison,
	row map[string]any,
) (any, error) {
	leftVal, err := e.evaluateExpr(ctx, expr.Left, row)
	if err != nil {
		return nil, err
	}
	if leftVal == nil {
		return nil, nil // NULL op ANY/ALL -> NULL
	}

	// Execute subquery
	plan, err := e.planner.Plan(expr.Subquery)
	if err != nil {
		return nil, err
	}
	subqueryResult, err := e.Execute(ctx.Context, plan, ctx.Args)
	if err != nil {
		return nil, err
	}

	if expr.Quantifier == "ANY" {
		hasNull := false
		for _, subRow := range subqueryResult.Rows {
			for _, v := range subRow {
				if v == nil {
					hasNull = true
				} else {
					cmp := compareValues(leftVal, v)
					match := applyComparisonOp(cmp, expr.Op)
					if match {
						return true, nil
					}
				}
				break // only first column
			}
		}
		if hasNull {
			return nil, nil
		}
		return false, nil
	}
	// ALL
	hasNull := false
	for _, subRow := range subqueryResult.Rows {
		for _, v := range subRow {
			if v == nil {
				hasNull = true
			} else {
				cmp := compareValues(leftVal, v)
				match := applyComparisonOp(cmp, expr.Op)
				if !match {
					return false, nil
				}
			}
			break // only first column
		}
	}
	if len(subqueryResult.Rows) == 0 {
		return true, nil // vacuous truth for ALL
	}
	if hasNull {
		return nil, nil
	}
	return true, nil
}

// applyComparisonOp applies a comparison operator to a compareValues result.
func applyComparisonOp(cmp int, op parser.BinaryOp) bool {
	switch op {
	case parser.OpEq:
		return cmp == 0
	case parser.OpNe:
		return cmp != 0
	case parser.OpLt:
		return cmp < 0
	case parser.OpLe:
		return cmp <= 0
	case parser.OpGt:
		return cmp > 0
	case parser.OpGe:
		return cmp >= 0
	default:
		return false
	}
}

// computeAggregate computes an aggregate function over a set of rows.
func (e *Executor) computeAggregate(
	ctx *ExecutionContext,
	expr interface{},
	rows []map[string]any,
) (any, error) {
	fn, ok := expr.(*binder.BoundFunctionCall)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "expected aggregate function",
		}
	}

	// Apply FILTER clause: pre-filter rows before aggregation
	if fn.Filter != nil {
		filteredRows := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			filterVal, err := e.evaluateExpr(ctx, fn.Filter, row)
			if err != nil || !toBool(filterVal) {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		rows = filteredRows
	}

	switch fn.Name {
	case "COUNT":
		if fn.Star {
			return int64(len(rows)), nil
		}
		count := int64(0)
		seen := make(map[string]bool)
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := e.evaluateExpr(
					ctx,
					fn.Args[0],
					row,
				)
				if err != nil {
					return nil, err
				}
				if val != nil {
					if fn.Distinct {
						key := formatValue(val)
						if !seen[key] {
							seen[key] = true
							count++
						}
					} else {
						count++
					}
				}
			}
		}

		return count, nil

	case "SUM":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		hasValue := false
		for _, row := range rows {
			val, err := e.evaluateExpr(
				ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				hasValue = true
			}
		}
		if !hasValue {
			return nil, nil
		}

		return sum, nil

	case "AVG", "MEAN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		count := 0
		for _, row := range rows {
			val, err := e.evaluateExpr(
				ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}

		return sum / float64(count), nil

	case "MIN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var minVal any
		for _, row := range rows {
			val, err := e.evaluateExpr(
				ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if minVal == nil ||
					compareValues(
						val,
						minVal,
					) < 0 {
					minVal = val
				}
			}
		}

		return minVal, nil

	case "MAX":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var maxVal any
		for _, row := range rows {
			val, err := e.evaluateExpr(
				ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if maxVal == nil ||
					compareValues(
						val,
						maxVal,
					) > 0 {
					maxVal = val
				}
			}
		}

		return maxVal, nil

	// Statistical aggregate functions
	case "MEDIAN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeMedian(values)

	case "QUANTILE":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		qVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		q := toFloat64Value(qVal)
		return computeQuantile(values, q)

	case "PERCENTILE_CONT":
		// WITHIN GROUP syntax: PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 && len(fn.Args) >= 1 {
			values, err := e.collectAggValues(ctx, fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			pVal, err := e.evaluateExpr(ctx, fn.Args[0], nil)
			if err != nil {
				return nil, err
			}
			p := toFloat64Value(pVal)
			return computePercentileCont(values, p)
		}
		// Traditional syntax: PERCENTILE_CONT(col, p)
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		pVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		p := toFloat64Value(pVal)
		return computePercentileCont(values, p)

	case "PERCENTILE_DISC":
		// WITHIN GROUP syntax: PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 && len(fn.Args) >= 1 {
			values, err := e.collectAggValues(ctx, fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			pVal, err := e.evaluateExpr(ctx, fn.Args[0], nil)
			if err != nil {
				return nil, err
			}
			p := toFloat64Value(pVal)
			return computePercentileDisc(values, p)
		}
		// Traditional syntax: PERCENTILE_DISC(col, p)
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		pVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		p := toFloat64Value(pVal)
		return computePercentileDisc(values, p)

	case "MODE":
		// WITHIN GROUP syntax: MODE() WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 {
			values, err := e.collectAggValues(ctx, fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			return computeMode(values)
		}
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeMode(values)

	case "ENTROPY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeEntropy(values)

	case "SKEWNESS":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeSkewness(values)

	case "KURTOSIS":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeKurtosis(values)

	case "VAR_POP":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeVarPop(values)

	case "VAR_SAMP", "VARIANCE":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeVarSamp(values)

	case "STDDEV_POP":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeStddevPop(values)

	case "STDDEV_SAMP", "STDDEV":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeStddevSamp(values)

	// String aggregate functions
	case "STRING_AGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Get delimiter from second argument, default to comma
		delimiter := ","
		if len(fn.Args) >= 2 {
			delimVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeStringAgg(values, delimiter)

	case "LISTAGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		delimiter := "" // LISTAGG defaults to empty string
		if len(fn.Args) >= 2 {
			delimVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeStringAgg(values, delimiter)

	case "GROUP_CONCAT":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Get delimiter from second argument, default to comma
		delimiter := ","
		if len(fn.Args) >= 2 {
			delimVal, err := e.evaluateExpr(ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeGroupConcat(values, delimiter)

	case "LIST", "ARRAY_AGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Handle DISTINCT modifier
		if fn.Distinct {
			return computeListDistinct(values)
		}
		return computeList(values)

	case "LIST_DISTINCT":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeListDistinct(values)

	case "JSON_GROUP_ARRAY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeJSONGroupArray(values)

	case "JSON_GROUP_OBJECT":
		if len(fn.Args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_GROUP_OBJECT requires 2 arguments (key, value)",
			}
		}
		keys, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		vals, err := e.collectAggValuesWithOrderBy(ctx, fn.Args[1], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeJSONGroupObject(keys, vals)

	// Conditional aggregate functions
	case "SUM_IF":
		sum := 0.0
		hasValue := false
		for _, row := range rows {
			if len(fn.Args) >= 2 {
				condVal, err := e.evaluateExpr(ctx, fn.Args[1], row)
				if err != nil {
					return nil, err
				}
				if condVal != nil && toBool(condVal) {
					val, err := e.evaluateExpr(ctx, fn.Args[0], row)
					if err != nil {
						return nil, err
					}
					if val != nil {
						sum += toFloat64Value(val)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil, nil
		}
		return sum, nil

	case "AVG_IF":
		avgIfSum := 0.0
		avgIfCount := int64(0)
		for _, row := range rows {
			if len(fn.Args) >= 2 {
				condVal, err := e.evaluateExpr(ctx, fn.Args[1], row)
				if err != nil {
					return nil, err
				}
				if condVal != nil && toBool(condVal) {
					val, err := e.evaluateExpr(ctx, fn.Args[0], row)
					if err != nil {
						return nil, err
					}
					if val != nil {
						avgIfSum += toFloat64Value(val)
						avgIfCount++
					}
				}
			}
		}
		if avgIfCount == 0 {
			return nil, nil
		}
		return avgIfSum / float64(avgIfCount), nil

	case "MIN_IF":
		var minIfVal any
		for _, row := range rows {
			if len(fn.Args) >= 2 {
				condVal, err := e.evaluateExpr(ctx, fn.Args[1], row)
				if err != nil {
					return nil, err
				}
				if condVal != nil && toBool(condVal) {
					val, err := e.evaluateExpr(ctx, fn.Args[0], row)
					if err != nil {
						return nil, err
					}
					if val != nil {
						if minIfVal == nil || compareValues(val, minIfVal) < 0 {
							minIfVal = val
						}
					}
				}
			}
		}
		return minIfVal, nil

	case "MAX_IF":
		var maxIfVal any
		for _, row := range rows {
			if len(fn.Args) >= 2 {
				condVal, err := e.evaluateExpr(ctx, fn.Args[1], row)
				if err != nil {
					return nil, err
				}
				if condVal != nil && toBool(condVal) {
					val, err := e.evaluateExpr(ctx, fn.Args[0], row)
					if err != nil {
						return nil, err
					}
					if val != nil {
						if maxIfVal == nil || compareValues(val, maxIfVal) > 0 {
							maxIfVal = val
						}
					}
				}
			}
		}
		return maxIfVal, nil

	// Time series aggregate functions
	case "COUNT_IF":
		if len(fn.Args) == 0 {
			return int64(0), nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeCountIf(values)

	case "FIRST", "ANY_VALUE", "ARBITRARY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeFirst(values)

	case "LAST":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeLast(values)

	case "HISTOGRAM":
		if len(fn.Args) != 1 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeHistogram(values)

	case "ARGMIN", "ARG_MIN", "MIN_BY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		argValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		valValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeArgmin(argValues, valValues)

	case "ARGMAX", "ARG_MAX", "MAX_BY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		argValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		valValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeArgmax(argValues, valValues)

	// Boolean aggregate functions
	case "BOOL_AND", "EVERY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBoolAnd(values)

	case "BOOL_OR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBoolOr(values)

	// Bitwise aggregate functions
	case "BIT_AND":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitAnd(values)

	case "BIT_OR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitOr(values)

	case "BIT_XOR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitXor(values)

	case "PRODUCT":
		product := 1.0
		hasValue := false
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := e.evaluateExpr(ctx, fn.Args[0], row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					product *= toFloat64Value(val)
					hasValue = true
				}
			}
		}
		if !hasValue {
			return nil, nil
		}
		return product, nil

	case "MAD":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, nil
		}
		medianVal, err := computeMedian(values)
		if err != nil {
			return nil, err
		}
		if medianVal == nil {
			return nil, nil
		}
		median := toFloat64Value(medianVal)
		deviations := make([]any, 0, len(values))
		for _, v := range values {
			if v != nil {
				deviations = append(deviations, math.Abs(toFloat64Value(v)-median))
			}
		}
		if len(deviations) == 0 {
			return nil, nil
		}
		return computeMedian(deviations)

	case "FAVG":
		sum := 0.0
		compensation := 0.0
		count := int64(0)
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := e.evaluateExpr(ctx, fn.Args[0], row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					y := toFloat64Value(val) - compensation
					t := sum + y
					compensation = (t - sum) - y
					sum = t
					count++
				}
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case "FSUM":
		sum := 0.0
		compensation := 0.0
		hasValue := false
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := e.evaluateExpr(ctx, fn.Args[0], row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					y := toFloat64Value(val) - compensation
					t := sum + y
					compensation = (t - sum) - y
					sum = t
					hasValue = true
				}
			}
		}
		if !hasValue {
			return nil, nil
		}
		return sum, nil

	case "BITSTRING_AGG":
		var bits []byte
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := e.evaluateExpr(ctx, fn.Args[0], row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					if toBool(val) {
						bits = append(bits, '1')
					} else {
						bits = append(bits, '0')
					}
				}
			}
		}
		if len(bits) == 0 {
			return nil, nil
		}
		return string(bits), nil

	case "GEOMETRIC_MEAN", "GEOMEAN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		var logSum float64
		count := 0
		for _, val := range values {
			if val == nil {
				continue
			}
			f := toFloat64Value(val)
			if f <= 0 {
				return nil, nil
			}
			logSum += math.Log(f)
			count++
		}
		if count == 0 {
			return nil, nil
		}
		return math.Exp(logSum / float64(count)), nil

	case "WEIGHTED_AVG":
		if len(fn.Args) < 2 {
			return nil, fmt.Errorf("WEIGHTED_AVG requires 2 arguments (value, weight)")
		}
		valValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		weightValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		var sumVW, sumW float64
		hasValues := false
		for i, val := range valValues {
			if val == nil {
				continue
			}
			if i >= len(weightValues) || weightValues[i] == nil {
				continue
			}
			v := toFloat64Value(val)
			w := toFloat64Value(weightValues[i])
			sumVW += v * w
			sumW += w
			hasValues = true
		}
		if !hasValues || sumW == 0 {
			return nil, nil
		}
		return sumVW / sumW, nil

	case "COVAR_POP":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCovarPop(yValues, xValues)

	case "COVAR_SAMP":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCovarSamp(yValues, xValues)

	case "CORR":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCorr(yValues, xValues)

	case "REGR_SLOPE":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSlope(yValues, xValues)

	case "REGR_INTERCEPT":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrIntercept(yValues, xValues)

	case "REGR_R2":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrR2(yValues, xValues)

	case "REGR_COUNT":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrCount(yValues, xValues)

	case "REGR_AVGX":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrAvgX(yValues, xValues)

	case "REGR_AVGY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrAvgY(yValues, xValues)

	case "REGR_SXX":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSXX(yValues, xValues)

	case "REGR_SYY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSYY(yValues, xValues)

	case "REGR_SXY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := e.collectAggValues(ctx, fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := e.collectAggValues(ctx, fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSXY(yValues, xValues)

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unknown aggregate function: %s",
				fn.Name,
			),
		}
	}
}

// collectAggValues collects all values from an expression across rows for aggregation.
// Returns a slice of values (including NULLs as nil).
func (e *Executor) collectAggValues(
	ctx *ExecutionContext,
	expr interface{},
	rows []map[string]any,
) ([]any, error) {
	values := make([]any, 0, len(rows))
	for _, row := range rows {
		val, err := e.evaluateExpr(ctx, expr, row)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// collectAggValuesWithOrderBy collects values along with their ORDER BY sorting keys.
// Returns the values sorted according to the ORDER BY clause.
func (e *Executor) collectAggValuesWithOrderBy(
	ctx *ExecutionContext,
	valueExpr interface{},
	orderBy []binder.BoundOrderByExpr,
	rows []map[string]any,
) ([]any, error) {
	if len(orderBy) == 0 {
		return e.collectAggValues(ctx, valueExpr, rows)
	}

	// Collect values with their sorting keys
	type valueWithKey struct {
		value    any
		orderKey []any
	}
	items := make([]valueWithKey, 0, len(rows))

	for _, row := range rows {
		val, err := e.evaluateExpr(ctx, valueExpr, row)
		if err != nil {
			return nil, err
		}

		// Collect ORDER BY key values
		keys := make([]any, len(orderBy))
		for i, ob := range orderBy {
			keyVal, err := e.evaluateExpr(ctx, ob.Expr, row)
			if err != nil {
				return nil, err
			}
			keys[i] = keyVal
		}

		items = append(items, valueWithKey{value: val, orderKey: keys})
	}

	// Sort items based on ORDER BY keys
	sort.SliceStable(items, func(i, j int) bool {
		for k, ob := range orderBy {
			cmp := compareValues(items[i].orderKey[k], items[j].orderKey[k])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0 // descending
				}
				return cmp < 0 // ascending
			}
		}
		return false // equal, preserve order
	})

	// Extract sorted values
	result := make([]any, len(items))
	for i, item := range items {
		result[i] = item.value
	}
	return result, nil
}

// compareRows compares two rows using ORDER BY expressions.
func (e *Executor) compareRows(
	ctx *ExecutionContext,
	a, b map[string]any,
	orderBy []*binder.BoundOrderBy,
) (int, error) {
	for _, order := range orderBy {
		valA, err := e.evaluateExpr(
			ctx,
			order.Expr,
			a,
		)
		if err != nil {
			return 0, err
		}
		valB, err := e.evaluateExpr(
			ctx,
			order.Expr,
			b,
		)
		if err != nil {
			return 0, err
		}

		cmp := compareWithCollation(valA, valB, order.Collation)
		if cmp != 0 {
			if order.Desc {
				return -cmp, nil
			}

			return cmp, nil
		}
	}

	return 0, nil
}

// compareWithCollation compares two values, using the specified collation for
// string values. If collationName is empty, falls back to default comparison.
func compareWithCollation(a, b any, collationName string) int {
	if collationName == "" {
		return compareValues(a, b)
	}

	// Only apply collation to string values.
	strA, okA := a.(string)
	strB, okB := b.(string)
	if okA && okB {
		c, ok := collation.DefaultRegistry.Get(collationName)
		if ok {
			return c.Compare(strA, strB)
		}
	}

	return compareValues(a, b)
}

// Helper functions

func toBool(v any) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return true
	}
}

func toString(v any) string {
	if v == nil {
		return ""
	}

	return fmt.Sprintf("%v", v)
}

func toInt64Value(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case string:
		return 0
	default:
		return 0
	}
}

func toFloat64Value(v any) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	default:
		return 0
	}
}

// toFloat64Slice converts a slice of any type to []float64.
// Returns the converted slice and true if the input is a slice with convertible elements.
// Returns nil and false if the input is not a slice or contains non-numeric elements.
func toFloat64Slice(v any) ([]float64, bool) {
	if v == nil {
		return nil, false
	}
	switch val := v.(type) {
	case []float64:
		return val, true
	case []any:
		result := make([]float64, len(val))
		for i, elem := range val {
			f := toFloat64Value(elem)
			result[i] = f
		}
		return result, true
	case []float32:
		result := make([]float64, len(val))
		for i, elem := range val {
			result[i] = float64(elem)
		}
		return result, true
	case []int64:
		result := make([]float64, len(val))
		for i, elem := range val {
			result[i] = float64(elem)
		}
		return result, true
	case []int:
		result := make([]float64, len(val))
		for i, elem := range val {
			result[i] = float64(elem)
		}
		return result, true
	case []int32:
		result := make([]float64, len(val))
		for i, elem := range val {
			result[i] = float64(elem)
		}
		return result, true
	default:
		return nil, false
	}
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			if aNum < bNum {
				return -1
			}
			if aNum > bNum {
				return 1
			}

			return 0
		}
	}

	// Fall back to string comparison
	aStr := toString(a)
	bStr := toString(b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}

	return 0
}

func toNumber(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	case dukdb.Decimal:
		return val.Float64(), true
	default:
		return 0, false
	}
}

func addValues(a, b any) (any, error) {
	// Handle date/timestamp + interval -> date/timestamp
	if interval, ok := b.(Interval); ok {
		return addIntervalToTemporal(a, interval)
	}
	if interval, ok := a.(Interval); ok {
		// interval + date/timestamp is commutative
		return addIntervalToTemporal(b, interval)
	}

	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			return aNum + bNum, nil
		}
	}
	// String concatenation fallback
	return toString(a) + toString(b), nil
}

// addIntervalToTemporal adds an interval to a date or timestamp value.
func addIntervalToTemporal(temporal any, interval Interval) (any, error) {
	switch v := temporal.(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		result := addInterval(t, interval)
		return timeToDate(result), nil
	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		result := addInterval(t, interval)
		return timeToTimestamp(result), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("cannot add interval to type %T", temporal),
		}
	}
}

func subValues(a, b any) (any, error) {
	// Handle date/timestamp - interval -> date/timestamp
	if interval, ok := b.(Interval); ok {
		// Negate the interval
		negInterval := Interval{
			Months: -interval.Months,
			Days:   -interval.Days,
			Micros: -interval.Micros,
		}
		return addIntervalToTemporal(a, negInterval)
	}

	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			return aNum - bNum, nil
		}
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot subtract non-numeric values",
	}
}

func mulValues(a, b any) (any, error) {
	// Handle interval * number -> interval
	if interval, ok := a.(Interval); ok {
		if factor, ok := toNumber(b); ok {
			return Interval{
				Months: int32(float64(interval.Months) * factor),
				Days:   int32(float64(interval.Days) * factor),
				Micros: int64(float64(interval.Micros) * factor),
			}, nil
		}
	}
	// Handle number * interval -> interval
	if interval, ok := b.(Interval); ok {
		if factor, ok := toNumber(a); ok {
			return Interval{
				Months: int32(float64(interval.Months) * factor),
				Days:   int32(float64(interval.Days) * factor),
				Micros: int64(float64(interval.Micros) * factor),
			}, nil
		}
	}

	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			return aNum * bNum, nil
		}
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot multiply non-numeric values",
	}
}

func divValues(a, b any) (any, error) {
	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			if bNum == 0 {
				return nil, dukdb.ErrDivisionByZero
			}

			return aNum / bNum, nil
		}
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot divide non-numeric values",
	}
}

func modValues(a, b any) (any, error) {
	if aNum, ok := toNumber(a); ok {
		if bNum, ok := toNumber(b); ok {
			if bNum == 0 {
				return nil, dukdb.ErrDivisionByZero
			}

			return math.Mod(aNum, bNum), nil
		}
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot modulo non-numeric values",
	}
}

func negateValue(v any) (any, error) {
	if num, ok := toNumber(v); ok {
		return -num, nil
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot negate non-numeric value",
	}
}

func absValue(v any) (any, error) {
	if num, ok := toNumber(v); ok {
		return math.Abs(num), nil
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  "cannot compute ABS of non-numeric value",
	}
}

func castValue(
	v any,
	targetType dukdb.Type,
) (any, error) {
	// Use the new validation-enabled cast function which returns proper errors
	// with PostgreSQL SQLSTATE codes for invalid casts.
	return castValueWithValidation(v, targetType)
}

// evaluateExtractExpr evaluates an EXTRACT(part FROM source) expression.
// This delegates to the DATE_PART logic, returning DOUBLE per SQL standard.
func (e *Executor) evaluateExtractExpr(
	ctx *ExecutionContext,
	expr *binder.BoundExtractExpr,
	row map[string]any,
) (any, error) {
	// Evaluate the source expression
	sourceVal, err := e.evaluateExpr(ctx, expr.Source, row)
	if err != nil {
		return nil, err
	}

	// NULL propagation
	if sourceVal == nil {
		return nil, nil
	}

	// Delegate to evalDatePart - it takes (part, timestamp) args
	// The part needs to be normalized to lowercase for the DATE_PART function
	part := strings.ToLower(expr.Part)
	args := []any{part, sourceVal}

	return evalDatePart(args)
}

func matchLike(
	s, pattern string,
	caseSensitive bool,
) bool {
	// Convert SQL LIKE pattern to regex-like matching
	// % = any sequence of characters
	// _ = single character
	// This is a simple implementation

	pi := 0 // pattern index
	si := 0 // string index

	lastWild := -1
	lastMatch := 0

	for si < len(s) {
		if pi < len(pattern) &&
			(pattern[pi] == '_' || (caseSensitive && pattern[pi] == s[si]) || (!caseSensitive && strings.EqualFold(string(pattern[pi]), string(s[si])))) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			lastWild = pi
			lastMatch = si
			pi++
		} else if lastWild != -1 {
			pi = lastWild + 1
			lastMatch++
			si = lastMatch
		} else {
			return false
		}
	}

	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}

	return pi == len(pattern)
}

// matchLikeWithEscape performs LIKE pattern matching with a custom escape character.
func matchLikeWithEscape(s, pattern string, escapeChar byte) bool {
	si, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) {
			// Handle escape character
			if escapeChar != 0 && pattern[pi] == escapeChar && pi+1 < len(pattern) {
				pi++ // skip escape char
				if si < len(s) && s[si] == pattern[pi] {
					si++
					pi++
					continue
				}
				return false
			}
			// Handle wildcards
			if pattern[pi] == '%' {
				starIdx = pi
				matchIdx = si
				pi++
				continue
			}
			if pattern[pi] == '_' || pattern[pi] == s[si] {
				si++
				pi++
				continue
			}
		}
		if starIdx >= 0 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
			continue
		}
		return false
	}
	// Consume trailing %
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

// evaluateScalarSubquery evaluates a scalar subquery (a SELECT that returns a single value).
// It executes the subquery and returns the first column of the first row.
// If the subquery returns no rows, NULL is returned.
func (e *Executor) evaluateScalarSubquery(
	ctx *ExecutionContext,
	stmt *binder.BoundSelectStmt,
	row map[string]any,
) (any, error) {
	// Create a child execution context with correlated values from the current row
	subCtx := &ExecutionContext{
		Context:          ctx.Context,
		Args:             ctx.Args,
		CorrelatedValues: row,
		conn:             ctx.conn,
	}

	// Plan and execute the subquery
	plan, err := e.planner.Plan(stmt)
	if err != nil {
		return nil, err
	}

	subqueryResult, err := e.executeWithContext(subCtx, plan)
	if err != nil {
		return nil, err
	}

	// Scalar subquery returns NULL if no rows
	if len(subqueryResult.Rows) == 0 {
		return nil, nil
	}

	// Get the first row and return the first column value
	firstRow := subqueryResult.Rows[0]
	if len(subqueryResult.Columns) > 0 {
		return firstRow[subqueryResult.Columns[0]], nil
	}

	// No columns means NULL result
	return nil, nil
}

// evaluateSequenceCall evaluates a sequence function call (NEXTVAL or CURRVAL).
func (e *Executor) evaluateSequenceCall(
	ctx *ExecutionContext,
	call *binder.BoundSequenceCall,
) (any, error) {
	// Get the sequence from the catalog
	seq, ok := e.catalog.GetSequenceInSchema(call.SchemaName, call.SequenceName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg: fmt.Sprintf(
				"sequence not found: %s.%s",
				call.SchemaName,
				call.SequenceName,
			),
		}
	}

	// Call the appropriate method based on the function name
	switch call.FunctionName {
	case "NEXTVAL":
		val, err := seq.NextVal()
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  err.Error(),
			}
		}

		// Log the sequence advance to WAL for persistence
		if e.wal != nil {
			entry := &wal.SequenceValueEntry{
				Schema:     call.SchemaName,
				Name:       call.SequenceName,
				CurrentVal: seq.GetCurrentVal(),
			}
			if err := e.wal.WriteEntry(entry); err != nil {
				// Log error but don't fail the operation
				// The sequence state will be lost on restart but the operation succeeds
				// A more robust implementation might rollback the sequence advance
			}
		}

		return val, nil

	case "CURRVAL":
		val, err := seq.CurrVal()
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  err.Error(),
			}
		}
		return val, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unknown sequence function: %s",
				call.FunctionName,
			),
		}
	}
}

// extractJSONValue extracts a value from JSON data using a key or index.
// If asText is true, returns the value as a string (for ->> operator).
// If asText is false, returns the value as JSON (for -> operator).
func extractJSONValue(left any, right any, asText bool) (any, error) {
	if left == nil || right == nil {
		return nil, nil
	}

	// Convert left operand to JSON string
	jsonStr := toString(left)
	if jsonStr == "" {
		return nil, nil
	}

	var result any
	var err error

	// Right operand can be a string key or an integer index
	switch r := right.(type) {
	case string:
		// Key access
		result, err = jsonutil.ExtractByKey(jsonStr, r)
	case int64:
		// Array index access
		result, err = jsonutil.ExtractByIndex(jsonStr, int(r))
	case int:
		// Array index access
		result, err = jsonutil.ExtractByIndex(jsonStr, r)
	case int32:
		// Array index access
		result, err = jsonutil.ExtractByIndex(jsonStr, int(r))
	case float64:
		// Integer-like float as array index
		result, err = jsonutil.ExtractByIndex(jsonStr, int(r))
	default:
		// Try as string key
		result, err = jsonutil.ExtractByKey(jsonStr, toString(right))
	}

	if err != nil {
		return nil, nil // Return nil on error, not an error
	}

	if result == nil {
		return nil, nil
	}

	if asText {
		// ->> operator: return as text
		return jsonutil.ValueToJSON(result)
	}

	// -> operator: return as JSON string
	jsonResult, err := jsonutil.ValueToJSON(result)
	if err != nil {
		return nil, nil
	}
	return jsonResult, nil
}

// isValidJSON checks if a string is valid JSON.
func isValidJSON(v any) (bool, error) {
	if v == nil {
		return false, nil
	}
	return jsonutil.IsValidJSON(toString(v)), nil
}

// evalJSONType returns the type of a JSON value as a string.
func evalJSONType(v any) (any, error) {
	s := toString(v)
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_TYPE: invalid JSON input",
		}
	}
	switch parsed.(type) {
	case map[string]any:
		return "OBJECT", nil
	case []any:
		return "ARRAY", nil
	case string:
		return "VARCHAR", nil
	case float64:
		// Check if it's an integer value
		f := parsed.(float64)
		if f == float64(int64(f)) {
			return "BIGINT", nil
		}
		return "DOUBLE", nil
	case bool:
		return "BOOLEAN", nil
	case nil:
		return "NULL", nil
	default:
		return "NULL", nil
	}
}

// evalJSONKeys returns the keys of a JSON object as a list of strings.
func evalJSONKeys(v any) (any, error) {
	s := toString(v)
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_KEYS: invalid JSON input",
		}
	}
	obj, ok := parsed.(map[string]any)
	if !ok {
		return nil, nil
	}
	keys := make([]any, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].(string) < keys[j].(string)
	})
	return keys, nil
}

// evalJSONArrayLength returns the length of a JSON array.
func evalJSONArrayLength(v any) (any, error) {
	s := toString(v)
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_ARRAY_LENGTH: invalid JSON input",
		}
	}
	arr, ok := parsed.([]any)
	if !ok {
		return nil, nil
	}
	return int64(len(arr)), nil
}

// evalToJSON converts a Go value to its JSON string representation.
func evalToJSON(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("TO_JSON: failed to marshal value: %v", err),
		}
	}
	return string(b), nil
}

// evalJSONMergePatch implements RFC 7386 JSON Merge Patch.
func evalJSONMergePatch(v1, v2 any) (any, error) {
	s1 := toString(v1)
	s2 := toString(v2)
	var parsed1, parsed2 any
	if err := json.Unmarshal([]byte(s1), &parsed1); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_MERGE_PATCH: invalid JSON in first argument",
		}
	}
	if err := json.Unmarshal([]byte(s2), &parsed2); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_MERGE_PATCH: invalid JSON in second argument",
		}
	}
	merged := jsonMergePatch(parsed1, parsed2)
	b, err := json.Marshal(merged)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_MERGE_PATCH: failed to marshal result: %v", err),
		}
	}
	return string(b), nil
}

// jsonMergePatch recursively merges two parsed JSON values per RFC 7386.
func jsonMergePatch(target, patch any) any {
	patchObj, patchIsObj := patch.(map[string]any)
	if !patchIsObj {
		return patch
	}
	targetObj, targetIsObj := target.(map[string]any)
	if !targetIsObj {
		targetObj = make(map[string]any)
	}
	result := make(map[string]any, len(targetObj))
	for k, v := range targetObj {
		result[k] = v
	}
	for k, v := range patchObj {
		if v == nil {
			delete(result, k)
		} else {
			result[k] = jsonMergePatch(result[k], v)
		}
	}
	return result
}

// evalJSONObject builds a JSON object from alternating key/value pairs.
func evalJSONObject(args []any) (any, error) {
	if len(args)%2 != 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "JSON_OBJECT requires an even number of arguments (key/value pairs)",
		}
	}
	obj := make(map[string]any, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key := toString(args[i])
		obj[key] = args[i+1]
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_OBJECT: failed to marshal result: %v", err),
		}
	}
	return string(b), nil
}

// evalJSONArray builds a JSON array from the given arguments.
func evalJSONArray(args []any) (any, error) {
	arr := make([]any, len(args))
	copy(arr, args)
	b, err := json.Marshal(arr)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_ARRAY: failed to marshal result: %v", err),
		}
	}
	return string(b), nil
}

// Geometry function implementations

// executeSTGeomFromText parses WKT text into a geometry.
func executeSTGeomFromText(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_GeomFromText requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	wkt := toString(args[0])
	geom, err := geomutil.ParseWKT(wkt)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_GeomFromText: %v", err),
		}
	}
	return geom, nil
}

// executeSTAsText converts a geometry to WKT.
func executeSTAsText(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_AsText requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_AsText: expected Geometry, got %T", args[0]),
		}
	}

	wkt, err := geomutil.FormatWKT(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_AsText: %v", err),
		}
	}
	return wkt, nil
}

// executeSTAsBinary returns the WKB representation of a geometry.
func executeSTAsBinary(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_AsBinary requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_AsBinary: expected Geometry, got %T", args[0]),
		}
	}

	return geom.WKB(), nil
}

// executeSTGeometryType returns the type name of a geometry.
func executeSTGeometryType(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_GeometryType requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_GeometryType: expected Geometry, got %T", args[0]),
		}
	}

	return geom.String(), nil
}

// executeSTX returns the X coordinate of a POINT geometry.
func executeSTX(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_X requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_X: expected Geometry, got %T", args[0]),
		}
	}

	x, err := geom.X()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_X: %v", err),
		}
	}
	return x, nil
}

// executeSTY returns the Y coordinate of a POINT geometry.
func executeSTY(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Y requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Y: expected Geometry, got %T", args[0]),
		}
	}

	y, err := geom.Y()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Y: %v", err),
		}
	}
	return y, nil
}

// executeSTZ returns the Z coordinate of a POINT geometry (if 3D).
func executeSTZ(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Z requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Z: expected Geometry, got %T", args[0]),
		}
	}

	z, err := geom.Z()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Z: %v", err),
		}
	}
	return z, nil
}

// executeSTSRID returns the SRID of a geometry.
func executeSTSRID(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_SRID requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_SRID: expected Geometry, got %T", args[0]),
		}
	}

	return int64(geom.GetSRID()), nil
}

// executeSTSetSRID returns a new geometry with the specified SRID.
func executeSTSetSRID(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_SetSRID requires 2 arguments",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_SetSRID: expected Geometry, got %T", args[0]),
		}
	}

	srid := int32(toInt64Value(args[1]))
	return geom.WithSRID(srid), nil
}

// executeSTPoint creates a POINT geometry from X and Y coordinates.
func executeSTPoint(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Point requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	x := toFloat64Value(args[0])
	y := toFloat64Value(args[1])

	wkt := fmt.Sprintf("POINT(%v %v)", x, y)
	return geomutil.ParseWKT(wkt)
}

// executeSTMakeLine creates a LINESTRING from two POINT geometries.
func executeSTMakeLine(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_MakeLine requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: expected Geometry, got %T", args[1]),
		}
	}

	// Get coordinates from both points
	x1, err := geom1.X()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: first argument: %v", err),
		}
	}
	y1, err := geom1.Y()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: first argument: %v", err),
		}
	}
	x2, err := geom2.X()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: second argument: %v", err),
		}
	}
	y2, err := geom2.Y()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakeLine: second argument: %v", err),
		}
	}

	wkt := fmt.Sprintf("LINESTRING(%v %v, %v %v)", x1, y1, x2, y2)
	return geomutil.ParseWKT(wkt)
}

// executeSTDistance calculates the Euclidean distance between two geometries.
func executeSTDistance(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Distance requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance: expected Geometry, got %T", args[1]),
		}
	}

	dist, err := geomutil.Distance(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance: %v", err),
		}
	}
	return dist, nil
}

// executeSTDistanceSphere calculates the great-circle distance using Haversine formula.
func executeSTDistanceSphere(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Distance_Sphere requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance_Sphere: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance_Sphere: expected Geometry, got %T", args[1]),
		}
	}

	dist, err := geomutil.DistanceSphere(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Distance_Sphere: %v", err),
		}
	}
	return dist, nil
}

// executeSTContains checks if the first geometry contains the second.
func executeSTContains(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Contains requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Contains: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Contains: expected Geometry, got %T", args[1]),
		}
	}

	contains, err := geomutil.Contains(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Contains: %v", err),
		}
	}
	return contains, nil
}

// executeSTWithin checks if the first geometry is within the second.
func executeSTWithin(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Within requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Within: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Within: expected Geometry, got %T", args[1]),
		}
	}

	within, err := geomutil.Within(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Within: %v", err),
		}
	}
	return within, nil
}

// executeSTIntersects checks if geometries intersect.
func executeSTIntersects(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Intersects requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersects: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersects: expected Geometry, got %T", args[1]),
		}
	}

	intersects, err := geomutil.Intersects(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersects: %v", err),
		}
	}
	return intersects, nil
}

// executeSTDisjoint checks if geometries are disjoint.
func executeSTDisjoint(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Disjoint requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Disjoint: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Disjoint: expected Geometry, got %T", args[1]),
		}
	}

	disjoint, err := geomutil.Disjoint(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Disjoint: %v", err),
		}
	}
	return disjoint, nil
}

// executeSTTouches checks if geometries touch.
func executeSTTouches(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Touches requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Touches: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Touches: expected Geometry, got %T", args[1]),
		}
	}

	touches, err := geomutil.Touches(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Touches: %v", err),
		}
	}
	return touches, nil
}

// executeSTCrosses checks if geometries cross.
func executeSTCrosses(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Crosses requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Crosses: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Crosses: expected Geometry, got %T", args[1]),
		}
	}

	crosses, err := geomutil.Crosses(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Crosses: %v", err),
		}
	}
	return crosses, nil
}

// executeSTOverlaps checks if geometries overlap.
func executeSTOverlaps(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Overlaps requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Overlaps: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Overlaps: expected Geometry, got %T", args[1]),
		}
	}

	overlaps, err := geomutil.Overlaps(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Overlaps: %v", err),
		}
	}
	return overlaps, nil
}

// executeSTEquals checks if geometries are spatially equal.
func executeSTEquals(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Equals requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Equals: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Equals: expected Geometry, got %T", args[1]),
		}
	}

	equals, err := geomutil.Equals(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Equals: %v", err),
		}
	}
	return equals, nil
}

// executeSTEnvelope returns the bounding box as a polygon.
func executeSTEnvelope(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Envelope requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Envelope: expected Geometry, got %T", args[0]),
		}
	}

	envelope, err := geomutil.Envelope(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Envelope: %v", err),
		}
	}
	return envelope, nil
}

// executeSTArea calculates the area of a geometry.
func executeSTArea(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Area requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Area: expected Geometry, got %T", args[0]),
		}
	}

	area, err := geomutil.Area(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Area: %v", err),
		}
	}
	return area, nil
}

// executeSTLength calculates the length/perimeter of a geometry.
func executeSTLength(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Length requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Length: expected Geometry, got %T", args[0]),
		}
	}

	length, err := geomutil.Length(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Length: %v", err),
		}
	}
	return length, nil
}

// executeSTCentroid calculates the geometric center of a geometry.
func executeSTCentroid(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Centroid requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Centroid: expected Geometry, got %T", args[0]),
		}
	}

	centroid, err := geomutil.Centroid(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Centroid: %v", err),
		}
	}
	return centroid, nil
}

// Phase 5: Set Operations

// executeSTUnion combines two geometries into one.
func executeSTUnion(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Union requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Union: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Union: expected Geometry, got %T", args[1]),
		}
	}

	result, err := geomutil.Union(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Union: %v", err),
		}
	}
	return result, nil
}

// executeSTIntersection returns the shared area between two geometries.
func executeSTIntersection(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Intersection requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersection: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersection: expected Geometry, got %T", args[1]),
		}
	}

	result, err := geomutil.Intersection(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Intersection: %v", err),
		}
	}
	if result == nil {
		return nil, nil // Return untyped nil for SQL NULL
	}
	return result, nil
}

// executeSTDifference returns g1 minus g2.
func executeSTDifference(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Difference requires 2 arguments",
		}
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	geom1, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Difference: expected Geometry, got %T", args[0]),
		}
	}

	geom2, ok := args[1].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Difference: expected Geometry, got %T", args[1]),
		}
	}

	result, err := geomutil.Difference(geom1, geom2)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Difference: %v", err),
		}
	}
	if result == nil {
		return nil, nil // Return untyped nil for SQL NULL
	}
	return result, nil
}

// executeSTBuffer creates a buffer zone around a geometry.
func executeSTBuffer(args []any) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_Buffer requires 2 arguments",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Buffer: expected Geometry, got %T", args[0]),
		}
	}

	distance := toFloat64Value(args[1])

	result, err := geomutil.Buffer(geom, distance)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_Buffer: %v", err),
		}
	}
	return result, nil
}

// executeSTMakePolygon creates a polygon from a closed linestring.
func executeSTMakePolygon(args []any) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "ST_MakePolygon requires 1 argument",
		}
	}
	if args[0] == nil {
		return nil, nil
	}

	geom, ok := args[0].(*geomutil.Geometry)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakePolygon: expected Geometry, got %T", args[0]),
		}
	}

	result, err := geomutil.MakePolygon(geom)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("ST_MakePolygon: %v", err),
		}
	}
	return result, nil
}

func duckdbTypeName(val any) string {
	if val == nil {
		return "NULL"
	}
	switch val.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32:
		return "INTEGER"
	case int8:
		return "TINYINT"
	case int16:
		return "SMALLINT"
	case int64:
		return "BIGINT"
	case float32:
		return "FLOAT"
	case float64:
		return "DOUBLE"
	case string:
		return "VARCHAR"
	case []byte:
		return "BLOB"
	case time.Time:
		return "TIMESTAMP"
	case []any:
		return "LIST"
	case map[string]any:
		return "STRUCT"
	default:
		return "VARCHAR"
	}
}

func pgTypeName(val any) string {
	if val == nil {
		return "unknown"
	}
	switch val.(type) {
	case bool:
		return "boolean"
	case int, int32:
		return "integer"
	case int8:
		return "smallint"
	case int16:
		return "smallint"
	case int64:
		return "bigint"
	case float32:
		return "real"
	case float64:
		return "double precision"
	case string:
		return "character varying"
	case []byte:
		return "bytea"
	case time.Time:
		return "timestamp without time zone"
	case []any:
		return "ARRAY"
	case map[string]any:
		return "record"
	default:
		return "character varying"
	}
}

func evalJSONContains(jsonVal, searchVal any) (bool, error) {
	jsonStr := toString(jsonVal)
	var doc any
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return false, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_CONTAINS: invalid JSON: %v", err),
		}
	}
	// Marshal the search value to compare as JSON
	searchJSON, err := json.Marshal(searchVal)
	if err != nil {
		return false, nil
	}
	return jsonContainsValue(doc, searchJSON), nil
}

func jsonContainsValue(doc any, searchJSON []byte) bool {
	// Marshal the current document node
	docJSON, err := json.Marshal(doc)
	if err != nil {
		return false
	}
	// Direct comparison
	if string(docJSON) == string(searchJSON) {
		return true
	}
	// Recurse into arrays and objects
	switch d := doc.(type) {
	case []any:
		for _, elem := range d {
			if jsonContainsValue(elem, searchJSON) {
				return true
			}
		}
	case map[string]any:
		for _, v := range d {
			if jsonContainsValue(v, searchJSON) {
				return true
			}
		}
	}
	return false
}

// evaluateListAggregate applies an aggregate function over list elements.
func evaluateListAggregate(list []any, aggName string, extraArgs []any) (any, error) {
	// Filter NULLs for most aggregates
	var nonNull []any
	for _, v := range list {
		if v != nil {
			nonNull = append(nonNull, v)
		}
	}

	switch aggName {
	case "sum":
		var sum float64
		for _, v := range nonNull {
			f, _ := toFloat64(v)
			sum += f
		}
		// Return int if all were ints
		allInt := true
		for _, v := range nonNull {
			if _, ok := v.(float64); ok {
				allInt = false
				break
			}
		}
		if allInt {
			return int64(sum), nil
		}
		return sum, nil
	case "avg":
		if len(nonNull) == 0 {
			return nil, nil
		}
		var sum float64
		for _, v := range nonNull {
			f, _ := toFloat64(v)
			sum += f
		}
		return sum / float64(len(nonNull)), nil
	case "min":
		if len(nonNull) == 0 {
			return nil, nil
		}
		minVal := nonNull[0]
		for _, v := range nonNull[1:] {
			if compareValues(v, minVal) < 0 {
				minVal = v
			}
		}
		return minVal, nil
	case "max":
		if len(nonNull) == 0 {
			return nil, nil
		}
		maxVal := nonNull[0]
		for _, v := range nonNull[1:] {
			if compareValues(v, maxVal) > 0 {
				maxVal = v
			}
		}
		return maxVal, nil
	case "count":
		return int64(len(nonNull)), nil
	case "first":
		if len(list) == 0 {
			return nil, nil
		}
		return list[0], nil
	case "last":
		if len(list) == 0 {
			return nil, nil
		}
		return list[len(list)-1], nil
	case "string_agg":
		sep := ","
		if len(extraArgs) > 0 && extraArgs[0] != nil {
			sep = toString(extraArgs[0])
		}
		var parts []string
		for _, v := range nonNull {
			parts = append(parts, toString(v))
		}
		return strings.Join(parts, sep), nil
	case "bool_and":
		for _, v := range nonNull {
			if !toBool(v) {
				return false, nil
			}
		}
		return true, nil
	case "bool_or":
		for _, v := range nonNull {
			if toBool(v) {
				return true, nil
			}
		}
		return false, nil
	default:
		return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("list_aggregate: unknown aggregate %q", aggName)}
	}
}

// stripEnumQuotes removes surrounding single quotes from an enum value string.
// Enum values are stored with their original quote delimiters from the parser (e.g., "'sad'").
// This function returns the unquoted value (e.g., "sad").
func stripEnumQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
