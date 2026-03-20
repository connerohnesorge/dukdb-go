package executor

import (
	"fmt"
	"sort"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// handleListLambdaFunction handles list functions that accept lambda arguments.
// Lambda arguments must not be eagerly evaluated; they are passed through as BoundLambdaExpr.
func (e *Executor) handleListLambdaFunction(
	ctx *ExecutionContext,
	fn *binder.BoundFunctionCall,
	row map[string]any,
	funcName string,
) (any, error) {
	// Evaluate non-lambda args, pass lambda args through
	args := make([]any, len(fn.Args))
	for i, arg := range fn.Args {
		if lambda, ok := arg.(*binder.BoundLambdaExpr); ok {
			args[i] = lambda // Pass lambda through unevaluated
		} else {
			val, err := e.evaluateExpr(ctx, arg, row)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
	}

	switch funcName {
	case "list_transform":
		return e.evaluateListTransform(ctx, args, row)
	case "list_filter":
		return e.evaluateListFilter(ctx, args, row)
	case "list_sort":
		return e.evaluateListSort(ctx, args, row)
	default:
		return nil, fmt.Errorf("unknown list function: %s", funcName)
	}
}

// evaluateListTransform implements list_transform(list, lambda).
// Applies the lambda to each element and returns a new list.
func (e *Executor) evaluateListTransform(
	ctx *ExecutionContext,
	args []any,
	row map[string]any,
) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_transform requires 2 arguments",
		}
	}

	// NULL list returns NULL
	if args[0] == nil {
		return nil, nil
	}

	list, ok := toSlice(args[0])
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_transform: first argument must be a list",
		}
	}

	lambda, ok := args[1].(*binder.BoundLambdaExpr)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_transform: second argument must be a lambda",
		}
	}

	if len(lambda.Params) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_transform: lambda must have exactly 1 parameter",
		}
	}

	result := make([]any, len(list))
	for i, elem := range list {
		val, err := e.evaluateLambdaBody(ctx, lambda, []any{elem}, row)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

// evaluateListFilter implements list_filter(list, lambda).
// Returns elements where lambda returns true.
func (e *Executor) evaluateListFilter(
	ctx *ExecutionContext,
	args []any,
	row map[string]any,
) (any, error) {
	if len(args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_filter requires 2 arguments",
		}
	}

	// NULL list returns NULL
	if args[0] == nil {
		return nil, nil
	}

	list, ok := toSlice(args[0])
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_filter: first argument must be a list",
		}
	}

	lambda, ok := args[1].(*binder.BoundLambdaExpr)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_filter: second argument must be a lambda",
		}
	}

	var result []any
	for _, elem := range list {
		val, err := e.evaluateLambdaBody(ctx, lambda, []any{elem}, row)
		if err != nil {
			return nil, err
		}
		if toBool(val) {
			result = append(result, elem)
		}
	}
	if result == nil {
		result = []any{}
	}
	return result, nil
}

// evaluateListSort implements list_sort(list) or list_sort(list, 'order', 'nulls').
// Performs natural ascending sort by default.
func (e *Executor) evaluateListSort(
	ctx *ExecutionContext,
	args []any,
	row map[string]any,
) (any, error) {
	if len(args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_sort requires at least 1 argument",
		}
	}

	// NULL list returns NULL
	if args[0] == nil {
		return nil, nil
	}

	list, ok := toSlice(args[0])
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "list_sort: first argument must be a list",
		}
	}

	result := make([]any, len(list))
	copy(result, list)

	sort.SliceStable(result, func(i, j int) bool {
		return compareValues(result[i], result[j]) < 0
	})

	return result, nil
}

// evaluateLambdaBody evaluates a lambda's body expression with parameter bindings.
func (e *Executor) evaluateLambdaBody(
	ctx *ExecutionContext,
	lambda *binder.BoundLambdaExpr,
	values []any,
	row map[string]any,
) (any, error) {
	// Create a row with lambda parameters bound
	lambdaRow := make(map[string]any)
	// Copy existing row context
	for k, v := range row {
		lambdaRow[k] = v
	}
	// Bind lambda parameters (these shadow any columns with the same name)
	for i, param := range lambda.Params {
		if i < len(values) {
			lambdaRow[param] = values[i]
		}
	}

	// Evaluate the lambda body expression using the parser expression evaluator
	return e.evaluateParserExpr(ctx, lambda.BodyExpr, lambdaRow)
}

// evaluateParserExpr evaluates a raw parser expression with a row context.
// This is used for lambda bodies where the expression was not fully bound
// because lambda parameters are not real column references.
func (e *Executor) evaluateParserExpr(
	ctx *ExecutionContext,
	expr parser.Expr,
	row map[string]any,
) (any, error) {
	if expr == nil {
		return nil, nil
	}

	switch ex := expr.(type) {
	case *parser.ColumnRef:
		// Look up in row (lambda parameter or column value)
		if val, ok := row[ex.Column]; ok {
			return val, nil
		}
		if ex.Table != "" {
			if val, ok := row[ex.Table+"."+ex.Column]; ok {
				return val, nil
			}
		}
		return nil, nil

	case *parser.Literal:
		return ex.Value, nil

	case *parser.BinaryExpr:
		left, err := e.evaluateParserExpr(ctx, ex.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateParserExpr(ctx, ex.Right, row)
		if err != nil {
			return nil, err
		}
		// Use the existing evaluateParserBinaryOp from expr.go (signature: op, left, right)
		return e.evaluateParserBinaryOp(ex.Op, left, right)

	case *parser.UnaryExpr:
		val, err := e.evaluateParserExpr(ctx, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case parser.OpNeg:
			return negateParserValue(val), nil
		case parser.OpNot:
			if val == nil {
				return nil, nil
			}
			return !toBool(val), nil
		case parser.OpIsNull:
			return val == nil, nil
		case parser.OpIsNotNull:
			return val != nil, nil
		default:
			return val, nil
		}

	case *parser.FunctionCall:
		// Evaluate function args and call as a builtin
		fnArgs := make([]any, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evaluateParserExpr(ctx, arg, row)
			if err != nil {
				return nil, err
			}
			fnArgs[i] = val
		}
		return e.evaluateBuiltinFunctionByName(ex.Name, fnArgs)

	case *parser.CaseExpr:
		return e.evaluateParserCaseExpr(ctx, ex, row)

	case *parser.CastExpr:
		val, err := e.evaluateParserExpr(ctx, ex.Expr, row)
		if err != nil {
			if ex.TryCast {
				return nil, nil
			}
			return nil, err
		}
		return castValue(val, ex.TargetType)

	case *parser.BetweenExpr:
		val, err := e.evaluateParserExpr(ctx, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		low, err := e.evaluateParserExpr(ctx, ex.Low, row)
		if err != nil {
			return nil, err
		}
		high, err := e.evaluateParserExpr(ctx, ex.High, row)
		if err != nil {
			return nil, err
		}
		result := compareValues(val, low) >= 0 && compareValues(val, high) <= 0
		if ex.Not {
			return !result, nil
		}
		return result, nil

	case *parser.ArrayExpr:
		elements := make([]any, len(ex.Elements))
		for i, elem := range ex.Elements {
			val, err := e.evaluateParserExpr(ctx, elem, row)
			if err != nil {
				return nil, err
			}
			elements[i] = val
		}
		return elements, nil

	default:
		return nil, fmt.Errorf("unsupported expression type in lambda: %T", expr)
	}
}

// evaluateParserCaseExpr evaluates a CASE expression from the parser AST.
func (e *Executor) evaluateParserCaseExpr(
	ctx *ExecutionContext,
	expr *parser.CaseExpr,
	row map[string]any,
) (any, error) {
	if expr.Operand != nil {
		operand, err := e.evaluateParserExpr(ctx, expr.Operand, row)
		if err != nil {
			return nil, err
		}
		for _, w := range expr.Whens {
			cond, err := e.evaluateParserExpr(ctx, w.Condition, row)
			if err != nil {
				return nil, err
			}
			if compareValues(operand, cond) == 0 {
				return e.evaluateParserExpr(ctx, w.Result, row)
			}
		}
	} else {
		for _, w := range expr.Whens {
			cond, err := e.evaluateParserExpr(ctx, w.Condition, row)
			if err != nil {
				return nil, err
			}
			if toBool(cond) {
				return e.evaluateParserExpr(ctx, w.Result, row)
			}
		}
	}
	if expr.Else != nil {
		return e.evaluateParserExpr(ctx, expr.Else, row)
	}
	return nil, nil
}

// evaluateBuiltinFunctionByName evaluates a builtin function by name with pre-evaluated args.
// This is used by the lambda expression evaluator for function calls within lambda bodies.
func (e *Executor) evaluateBuiltinFunctionByName(name string, args []any) (any, error) {
	// Create bound literals for the args and delegate to evaluateFunctionCall
	boundArgs := make([]binder.BoundExpr, len(args))
	for i, arg := range args {
		t := dukdb.TYPE_ANY
		if arg != nil {
			switch arg.(type) {
			case int64:
				t = dukdb.TYPE_BIGINT
			case float64:
				t = dukdb.TYPE_DOUBLE
			case string:
				t = dukdb.TYPE_VARCHAR
			case bool:
				t = dukdb.TYPE_BOOLEAN
			}
		}
		boundArgs[i] = &binder.BoundLiteral{Value: arg, ValType: t}
	}

	fn := &binder.BoundFunctionCall{
		Name: name,
		Args: boundArgs,
	}

	ctx := &ExecutionContext{}
	return e.evaluateFunctionCall(ctx, fn, nil)
}

// toSlice converts various typed slices to []any.
func toSlice(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	switch s := v.(type) {
	case []any:
		return s, true
	case []int64:
		result := make([]any, len(s))
		for i, val := range s {
			result[i] = val
		}
		return result, true
	case []float64:
		result := make([]any, len(s))
		for i, val := range s {
			result[i] = val
		}
		return result, true
	case []string:
		result := make([]any, len(s))
		for i, val := range s {
			result[i] = val
		}
		return result, true
	case []int:
		result := make([]any, len(s))
		for i, val := range s {
			result[i] = int64(val)
		}
		return result, true
	case []int32:
		result := make([]any, len(s))
		for i, val := range s {
			result[i] = int64(val)
		}
		return result, true
	default:
		return nil, false
	}
}

// negateParserValue negates a numeric value for lambda body evaluation.
func negateParserValue(v any) any {
	switch val := v.(type) {
	case int64:
		return -val
	case float64:
		return -val
	case int:
		return -val
	default:
		return v
	}
}
