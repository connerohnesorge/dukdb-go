package executor

import (
	"fmt"
	"math"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/wal"
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
		if row == nil {
			return nil, nil
		}
		// Try column name directly first
		if val, ok := row[ex.Column]; ok {
			return val, nil
		}
		// Try with table prefix
		key := ex.Table + "." + ex.Column
		if val, ok := row[key]; ok {
			return val, nil
		}

		return nil, nil

	case *binder.BoundParameter:
		if ctx.Args == nil || ex.Position <= 0 || ex.Position > len(ctx.Args) {
			return nil, nil
		}

		return ctx.Args[ex.Position-1].Value, nil

	case *binder.BoundBinaryExpr:
		return e.evaluateBinaryExpr(ctx, ex, row)

	case *binder.BoundUnaryExpr:
		return e.evaluateUnaryExpr(ctx, ex, row)

	case *binder.BoundFunctionCall:
		return e.evaluateFunctionCall(ctx, ex, row)

	case *binder.BoundCastExpr:
		val, err := e.evaluateExpr(ctx, ex.Expr, row)
		if err != nil {
			return nil, err
		}

		return castValue(val, ex.TargetType)

	case *binder.BoundCaseExpr:
		return e.evaluateCaseExpr(ctx, ex, row)

	case *binder.BoundBetweenExpr:
		return e.evaluateBetweenExpr(ctx, ex, row)

	case *binder.BoundInListExpr:
		return e.evaluateInListExpr(ctx, ex, row)

	case *binder.BoundInSubqueryExpr:
		return e.evaluateInSubqueryExpr(ctx, ex, row)

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
	case parser.OpConcat:
		return toString(
			left,
		) + toString(
			right,
		), nil

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

	case "UPPER":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "UPPER requires 1 argument",
			}
		}

		return strings.ToUpper(
			toString(args[0]),
		), nil

	case "LOWER":
		if len(args) != 1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "LOWER requires 1 argument",
			}
		}

		return strings.ToLower(
			toString(args[0]),
		), nil

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

	case "DAYOFWEEK":
		return evalDayOfWeek(args)

	case "DAYOFYEAR":
		return evalDayOfYear(args)

	case "WEEK":
		return evalWeek(args)

	case "QUARTER":
		return evalQuarter(args)

	// Date arithmetic functions
	case "DATE_ADD":
		return evalDateAdd(args)

	case "DATE_SUB":
		return evalDateSub(args)

	case "DATE_DIFF", "DATEDIFF":
		return evalDateDiff(args)

	case "DATE_TRUNC":
		return evalDateTrunc(args)

	case "DATE_PART":
		return evalDatePart(args)

	case "AGE":
		return evalAge(args)

	case "LAST_DAY":
		return evalLastDay(args)

	// Date construction functions
	case "MAKE_DATE":
		return evalMakeDate(args)

	case "MAKE_TIMESTAMP":
		return evalMakeTimestamp(args)

	case "MAKE_TIME":
		return evalMakeTime(args)

	// Formatting/Parsing functions
	case "STRFTIME":
		return evalStrftime(args)

	case "STRPTIME":
		return evalStrptime(args)

	case "TO_TIMESTAMP":
		return evalToTimestamp(args)

	case "EPOCH":
		return evalEpoch(args)

	case "EPOCH_MS":
		return evalEpochMs(args)

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

	default:
		// For aggregate functions called in scalar context, return NULL
		// The main fix for aggregate functions in projections is in executeProject
		// which looks up pre-computed aggregate results by alias
		switch fn.Name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
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

	case "AVG":
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

		cmp := compareValues(valA, valB)
		if cmp != 0 {
			if order.Desc {
				return -cmp, nil
			}

			return cmp, nil
		}
	}

	return 0, nil
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
	if v == nil {
		return nil, nil
	}

	switch targetType {
	case dukdb.TYPE_BOOLEAN:
		return toBool(v), nil
	case dukdb.TYPE_INTEGER:
		return int32(toInt64Value(v)), nil
	case dukdb.TYPE_BIGINT:
		return toInt64Value(v), nil
	case dukdb.TYPE_DOUBLE:
		return toFloat64Value(v), nil
	case dukdb.TYPE_VARCHAR:
		return toString(v), nil
	default:
		return v, nil
	}
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
