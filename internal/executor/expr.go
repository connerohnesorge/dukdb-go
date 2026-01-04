package executor

import (
	"fmt"
	"math"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	geomutil "github.com/dukdb/dukdb-go/internal/io/geometry"
	jsonutil "github.com/dukdb/dukdb-go/internal/io/json"
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

	// JSON operators
	case parser.OpJSONExtract:
		return extractJSONValue(left, right, false)
	case parser.OpJSONText:
		return extractJSONValue(left, right, true)

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
		case "STRING_AGG", "GROUP_CONCAT", "LIST", "ARRAY_AGG", "LIST_DISTINCT":
			return nil, nil
		// Time series aggregates
		case "COUNT_IF", "FIRST", "LAST", "ARGMIN", "ARGMAX", "MIN_BY", "MAX_BY":
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
	case dukdb.TYPE_INVALID, dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT, dukdb.TYPE_FLOAT,
		dukdb.TYPE_TIMESTAMP, dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_INTERVAL,
		dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT, dukdb.TYPE_BLOB, dukdb.TYPE_DECIMAL,
		dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS,
		dukdb.TYPE_ENUM, dukdb.TYPE_LIST, dukdb.TYPE_STRUCT, dukdb.TYPE_MAP, dukdb.TYPE_ARRAY,
		dukdb.TYPE_UUID, dukdb.TYPE_UNION, dukdb.TYPE_BIT, dukdb.TYPE_TIME_TZ,
		dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY, dukdb.TYPE_BIGNUM, dukdb.TYPE_SQLNULL,
		dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY, dukdb.TYPE_LAMBDA, dukdb.TYPE_VARIANT:
		return v, nil
	}
	return v, nil
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
