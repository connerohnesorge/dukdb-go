package binder

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
)

func (b *Binder) bindExpr(
	expr parser.Expr,
	expectedType dukdb.Type,
) (BoundExpr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.ColumnRef:
		return b.bindColumnRef(e)
	case *parser.Literal:
		return &BoundLiteral{Value: e.Value, ValType: e.Type}, nil
	case *parser.Parameter:
		b.scope.paramCount++
		pos := e.Position
		if pos == 0 {
			pos = b.scope.paramCount
		}
		// Use expected type if available, otherwise TYPE_ANY
		inferredType := expectedType
		if inferredType == dukdb.TYPE_INVALID || inferredType == 0 {
			inferredType = dukdb.TYPE_ANY
		}
		// Store in scope for later retrieval
		b.scope.params[pos] = inferredType

		return &BoundParameter{Position: pos, ParamType: inferredType}, nil
	case *parser.BinaryExpr:
		return b.bindBinaryExpr(e)
	case *parser.UnaryExpr:
		return b.bindUnaryExpr(e, expectedType)
	case *parser.FunctionCall:
		return b.bindFunctionCall(e)
	case *parser.CastExpr:
		// For CAST, the inner expression type is not constrained by outer context
		inner, err := b.bindExpr(e.Expr, e.TargetType)
		if err != nil {
			return nil, err
		}

		return &BoundCastExpr{Expr: inner, TargetType: e.TargetType}, nil
	case *parser.CaseExpr:
		return b.bindCaseExpr(e, expectedType)
	case *parser.BetweenExpr:
		return b.bindBetweenExpr(e)
	case *parser.InListExpr:
		return b.bindInListExpr(e)
	case *parser.InSubqueryExpr:
		return b.bindInSubqueryExpr(e)
	case *parser.ExistsExpr:
		return b.bindExistsExpr(e)
	case *parser.StarExpr:
		return b.bindStarExpr(e)
	case *parser.SelectStmt:
		return b.bindSelect(e)
	case *parser.ExtractExpr:
		return b.bindExtractExpr(e)
	case *parser.IntervalLiteral:
		return b.bindIntervalLiteral(e)
	case *parser.WindowExpr:
		return b.bindWindowExpr(e)
	default:
		return nil, b.errorf("unsupported expression type: %T", expr)
	}
}

func (b *Binder) bindColumnRef(
	ref *parser.ColumnRef,
) (*BoundColumnRef, error) {
	if ref.Table != "" {
		// Qualified column reference
		tableRef, ok := b.scope.tables[ref.Table]
		if !ok {
			return nil, b.errorf(
				"table not found: %s",
				ref.Table,
			)
		}

		for _, col := range tableRef.Columns {
			if strings.EqualFold(
				col.Column,
				ref.Column,
			) {
				return &BoundColumnRef{
					Table:     ref.Table,
					Column:    col.Column,
					ColumnIdx: col.ColumnIdx,
					ColType:   col.Type,
				}, nil
			}
		}

		return nil, b.errorf(
			"column not found: %s.%s",
			ref.Table,
			ref.Column,
		)
	}

	// Unqualified column reference - search all tables
	var found *BoundColumn
	var foundTable string
	for tableName, tableRef := range b.scope.tables {
		for _, col := range tableRef.Columns {
			if strings.EqualFold(
				col.Column,
				ref.Column,
			) {
				if found != nil {
					return nil, b.errorf(
						"ambiguous column reference: %s",
						ref.Column,
					)
				}
				found = col
				foundTable = tableName
			}
		}
	}

	if found == nil {
		return nil, b.errorf(
			"column not found: %s",
			ref.Column,
		)
	}

	return &BoundColumnRef{
		Table:     foundTable,
		Column:    found.Column,
		ColumnIdx: found.ColumnIdx,
		ColType:   found.Type,
	}, nil
}

func (b *Binder) bindBinaryExpr(
	e *parser.BinaryExpr,
) (*BoundBinaryExpr, error) {
	// Bind left first without expectation
	left, err := b.bindExpr(
		e.Left,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// For comparison and LIKE operators, use left's type as expected for right
	var rightExpected dukdb.Type
	switch e.Op {
	case parser.OpEq,
		parser.OpNe,
		parser.OpLt,
		parser.OpLe,
		parser.OpGt,
		parser.OpGe:
		rightExpected = left.ResultType()
	case parser.OpLike,
		parser.OpILike,
		parser.OpNotLike,
		parser.OpNotILike:
		rightExpected = dukdb.TYPE_VARCHAR
	case parser.OpAdd,
		parser.OpSub,
		parser.OpMul,
		parser.OpDiv,
		parser.OpMod:
		rightExpected = dukdb.TYPE_DOUBLE // arithmetic context
	default:
		rightExpected = dukdb.TYPE_ANY
	}

	right, err := b.bindExpr(
		e.Right,
		rightExpected,
	)
	if err != nil {
		return nil, err
	}

	// If left was parameter with TYPE_ANY and right has concrete type, update left
	if leftParam, ok := left.(*BoundParameter); ok {
		if leftParam.ParamType == dukdb.TYPE_ANY &&
			right.ResultType() != dukdb.TYPE_ANY {
			leftParam.ParamType = right.ResultType()
			b.scope.params[leftParam.Position] = right.ResultType()
		}
	}

	// Determine result type
	var resType dukdb.Type
	switch e.Op {
	case parser.OpEq,
		parser.OpNe,
		parser.OpLt,
		parser.OpLe,
		parser.OpGt,
		parser.OpGe,
		parser.OpAnd,
		parser.OpOr,
		parser.OpLike,
		parser.OpILike,
		parser.OpNotLike,
		parser.OpNotILike,
		parser.OpIn,
		parser.OpNotIn,
		parser.OpIs,
		parser.OpIsNot:
		resType = dukdb.TYPE_BOOLEAN
	case parser.OpConcat:
		resType = dukdb.TYPE_VARCHAR
	default:
		// For arithmetic, use the more precise type
		resType = promoteType(
			left.ResultType(),
			right.ResultType(),
		)
	}

	return &BoundBinaryExpr{
		Left:    left,
		Op:      e.Op,
		Right:   right,
		ResType: resType,
	}, nil
}

func (b *Binder) bindUnaryExpr(
	e *parser.UnaryExpr,
	expectedType dukdb.Type,
) (*BoundUnaryExpr, error) {
	inner, err := b.bindExpr(e.Expr, expectedType)
	if err != nil {
		return nil, err
	}

	var resType dukdb.Type
	switch e.Op {
	case parser.OpNot,
		parser.OpIsNull,
		parser.OpIsNotNull:
		resType = dukdb.TYPE_BOOLEAN
	default:
		resType = inner.ResultType()
	}

	return &BoundUnaryExpr{
		Op:      e.Op,
		Expr:    inner,
		ResType: resType,
	}, nil
}

func (b *Binder) bindFunctionCall(
	f *parser.FunctionCall,
) (BoundExpr, error) {
	// Handle sequence functions (NEXTVAL, CURRVAL)
	funcNameUpper := strings.ToUpper(f.Name)
	if funcNameUpper == "NEXTVAL" || funcNameUpper == "CURRVAL" {
		return b.bindSequenceCall(f)
	}

	// Get expected argument types from function signature if available
	argTypes := getFunctionArgTypes(
		f.Name,
		len(f.Args),
	)

	var args []BoundExpr
	for i, arg := range f.Args {
		expectedType := dukdb.TYPE_ANY
		if i < len(argTypes) {
			expectedType = argTypes[i]
		}
		bound, err := b.bindExpr(
			arg,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		args = append(args, bound)
	}

	// Check for scalar UDF first
	if b.udfResolver != nil {
		argTypes := make([]dukdb.Type, len(args))
		for i, arg := range args {
			argTypes[i] = arg.ResultType()
		}

		if udfInfo, resType, found := b.udfResolver.LookupScalarUDF(f.Name, argTypes); found {
			// Build argument info for constant folding.
			// For volatile functions, skip foldability detection to prevent caching.
			argInfo := make(
				[]dukdb.ScalarUDFArg,
				len(args),
			)
			isVolatile := b.udfResolver.IsVolatile(
				udfInfo,
			)
			if !isVolatile {
				for i, arg := range args {
					if lit, ok := arg.(*BoundLiteral); ok {
						argInfo[i] = dukdb.ScalarUDFArg{
							Foldable: true,
							Value:    lit.Value,
						}
					}
				}
			}

			// Call ScalarBinder callback if present (skipped for volatile functions)
			bindCtx, err := b.udfResolver.BindScalarUDF(
				udfInfo,
				argInfo,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"scalar UDF '%s' bind error: %w",
					f.Name,
					err,
				)
			}

			return &BoundScalarUDF{
				Name:    f.Name,
				Args:    args,
				ResType: resType,
				UDFInfo: udfInfo,
				ArgInfo: argInfo,
				BindCtx: bindCtx,
			}, nil
		}
	}

	// Fall back to built-in function
	resType := inferFunctionResultType(
		f.Name,
		args,
	)

	return &BoundFunctionCall{
		Name:     f.Name,
		Args:     args,
		Distinct: f.Distinct,
		Star:     f.Star,
		ResType:  resType,
	}, nil
}

func (b *Binder) bindCaseExpr(
	e *parser.CaseExpr,
	expectedType dukdb.Type,
) (*BoundCaseExpr, error) {
	bound := &BoundCaseExpr{}

	if e.Operand != nil {
		operand, err := b.bindExpr(
			e.Operand,
			dukdb.TYPE_ANY,
		)
		if err != nil {
			return nil, err
		}
		bound.Operand = operand
	}

	for _, w := range e.Whens {
		cond, err := b.bindExpr(
			w.Condition,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		result, err := b.bindExpr(
			w.Result,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		bound.Whens = append(
			bound.Whens,
			&BoundWhenClause{
				Condition: cond,
				Result:    result,
			},
		)
	}

	if e.Else != nil {
		elseExpr, err := b.bindExpr(
			e.Else,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		bound.Else = elseExpr
	}

	// Determine result type from THEN/ELSE expressions
	if len(bound.Whens) > 0 {
		bound.ResType = bound.Whens[0].Result.ResultType()
	} else if bound.Else != nil {
		bound.ResType = bound.Else.ResultType()
	} else {
		bound.ResType = dukdb.TYPE_SQLNULL
	}

	return bound, nil
}

func (b *Binder) bindBetweenExpr(
	e *parser.BetweenExpr,
) (*BoundBetweenExpr, error) {
	// Bind expr first to get its type
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// Use expr's type as expected type for low and high bounds
	exprType := expr.ResultType()

	low, err := b.bindExpr(e.Low, exprType)
	if err != nil {
		return nil, err
	}

	high, err := b.bindExpr(e.High, exprType)
	if err != nil {
		return nil, err
	}

	return &BoundBetweenExpr{
		Expr: expr,
		Low:  low,
		High: high,
		Not:  e.Not,
	}, nil
}

func (b *Binder) bindInListExpr(
	e *parser.InListExpr,
) (*BoundInListExpr, error) {
	// Bind expr first to get its type
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// Use expr's type as expected type for IN list values
	exprType := expr.ResultType()

	var values []BoundExpr
	for _, v := range e.Values {
		bound, err := b.bindExpr(v, exprType)
		if err != nil {
			return nil, err
		}
		values = append(values, bound)
	}

	return &BoundInListExpr{
		Expr:   expr,
		Values: values,
		Not:    e.Not,
	}, nil
}

func (b *Binder) bindInSubqueryExpr(
	e *parser.InSubqueryExpr,
) (*BoundInSubqueryExpr, error) {
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	subquery, err := b.bindSelect(e.Subquery)
	if err != nil {
		return nil, err
	}

	return &BoundInSubqueryExpr{
		Expr:     expr,
		Subquery: subquery,
		Not:      e.Not,
	}, nil
}

func (b *Binder) bindExistsExpr(
	e *parser.ExistsExpr,
) (*BoundExistsExpr, error) {
	subquery, err := b.bindSelect(e.Subquery)
	if err != nil {
		return nil, err
	}

	return &BoundExistsExpr{
		Subquery: subquery,
		Not:      e.Not,
	}, nil
}

func (b *Binder) bindStarExpr(
	e *parser.StarExpr,
) (*BoundStarExpr, error) {
	bound := &BoundStarExpr{Table: e.Table}

	if e.Table != "" {
		// Specific table's columns
		tableRef, ok := b.scope.tables[e.Table]
		if !ok {
			return nil, b.errorf(
				"table not found: %s",
				e.Table,
			)
		}
		bound.Columns = tableRef.Columns
	} else {
		// All tables' columns
		for _, tableRef := range b.scope.tables {
			bound.Columns = append(bound.Columns, tableRef.Columns...)
		}
	}

	return bound, nil
}

// bindExtractExpr binds an EXTRACT(part FROM source) expression.
// The source should be a temporal type (DATE, TIMESTAMP, or TIME).
// Returns DOUBLE per SQL standard.
func (b *Binder) bindExtractExpr(
	e *parser.ExtractExpr,
) (*BoundExtractExpr, error) {
	// Bind the source expression - expect a temporal type
	source, err := b.bindExpr(e.Source, dukdb.TYPE_TIMESTAMP)
	if err != nil {
		return nil, err
	}

	return &BoundExtractExpr{
		Part:   e.Part,
		Source: source,
	}, nil
}

// bindIntervalLiteral binds an INTERVAL literal expression.
// Returns TYPE_INTERVAL with the parsed months, days, and microseconds components.
func (b *Binder) bindIntervalLiteral(
	e *parser.IntervalLiteral,
) (*BoundIntervalLiteral, error) {
	return &BoundIntervalLiteral{
		Months: e.Months,
		Days:   e.Days,
		Micros: e.Micros,
	}, nil
}

// bindWindowExpr binds a window expression.
// This validates the window function, binds all subexpressions, applies default frame,
// and infers the result type.
func (b *Binder) bindWindowExpr(
	e *parser.WindowExpr,
) (*BoundWindowExpr, error) {
	funcName := strings.ToUpper(e.Function.Name)

	// Determine function type and validate it can be used as a window function
	var funcType WindowFunctionType
	var supportsIgnoreNulls bool

	if info := GetWindowFunctionInfo(funcName); info != nil {
		funcType = info.FuncType
		supportsIgnoreNulls = info.SupportsIgnoreNulls

		// Validate argument count
		argCount := len(e.Function.Args)
		if argCount < info.MinArgs || argCount > info.MaxArgs {
			if info.MinArgs == info.MaxArgs {
				return nil, b.errorf(
					"window function %s requires exactly %d argument(s), got %d",
					funcName, info.MinArgs, argCount)
			}
			return nil, b.errorf(
				"window function %s requires between %d and %d arguments, got %d",
				funcName, info.MinArgs, info.MaxArgs, argCount)
		}
	} else if IsAggregateWindowCapable(funcName) {
		funcType = WindowFunctionAggregate
		supportsIgnoreNulls = false
	} else {
		return nil, b.errorf(
			"function %s cannot be used as a window function", e.Function.Name)
	}

	// Validate IGNORE NULLS is only used with value functions
	if e.IgnoreNulls && !supportsIgnoreNulls {
		return nil, b.errorf(
			"IGNORE NULLS can only be used with LAG, LEAD, FIRST_VALUE, LAST_VALUE, or NTH_VALUE")
	}

	// Validate FILTER is only used with aggregate functions
	if e.Filter != nil && funcType != WindowFunctionAggregate {
		return nil, b.errorf(
			"FILTER clause can only be used with aggregate window functions")
	}

	// Validate DISTINCT is only used with aggregate functions
	if e.Distinct && funcType != WindowFunctionAggregate {
		return nil, b.errorf(
			"DISTINCT can only be used with aggregate window functions")
	}

	// Bind function arguments
	boundArgs := make([]BoundExpr, 0, len(e.Function.Args))
	for _, arg := range e.Function.Args {
		bound, err := b.bindExpr(arg, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundArgs = append(boundArgs, bound)
	}

	// Function-specific argument validation
	if err := b.validateWindowFunctionArgs(funcName, boundArgs); err != nil {
		return nil, err
	}

	// Bind PARTITION BY expressions
	partitionBy := make([]BoundExpr, 0, len(e.PartitionBy))
	for _, expr := range e.PartitionBy {
		bound, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		partitionBy = append(partitionBy, bound)
	}

	// Bind ORDER BY expressions
	orderBy := make([]BoundWindowOrder, 0, len(e.OrderBy))
	for _, ob := range e.OrderBy {
		bound, err := b.bindExpr(ob.Expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		orderBy = append(orderBy, BoundWindowOrder{
			Expr:       bound,
			Desc:       ob.Desc,
			NullsFirst: ob.NullsFirst,
		})
	}

	// Apply default frame based on ORDER BY presence
	frame := e.Frame
	if frame == nil {
		if len(orderBy) > 0 {
			// Default: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			frame = &parser.WindowFrame{
				Type: parser.FrameTypeRange,
				Start: parser.WindowBound{
					Type: parser.BoundUnboundedPreceding,
				},
				End: parser.WindowBound{
					Type: parser.BoundCurrentRow,
				},
				Exclude: parser.ExcludeNoOthers,
			}
		} else {
			// Default: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			frame = &parser.WindowFrame{
				Type: parser.FrameTypeRows,
				Start: parser.WindowBound{
					Type: parser.BoundUnboundedPreceding,
				},
				End: parser.WindowBound{
					Type: parser.BoundUnboundedFollowing,
				},
				Exclude: parser.ExcludeNoOthers,
			}
		}
	}

	// Validate frame
	if err := b.validateWindowFrame(frame, orderBy); err != nil {
		return nil, err
	}

	// Bind FILTER expression
	var boundFilter BoundExpr
	if e.Filter != nil {
		var err error
		boundFilter, err = b.bindExpr(e.Filter, dukdb.TYPE_BOOLEAN)
		if err != nil {
			return nil, err
		}
	}

	// Infer result type
	resType := InferWindowFunctionResultType(funcName, boundArgs)

	return &BoundWindowExpr{
		FunctionName: funcName,
		FunctionType: funcType,
		Args:         boundArgs,
		PartitionBy:  partitionBy,
		OrderBy:      orderBy,
		Frame:        frame,
		ResType:      resType,
		IgnoreNulls:  e.IgnoreNulls,
		Filter:       boundFilter,
		Distinct:     e.Distinct,
	}, nil
}

// validateWindowFunctionArgs validates function-specific arguments.
func (b *Binder) validateWindowFunctionArgs(funcName string, args []BoundExpr) error {
	switch funcName {
	case "NTILE":
		// NTILE argument must be a positive integer
		if len(args) > 0 {
			if lit, ok := args[0].(*BoundLiteral); ok {
				if val, ok := getIntValue(lit.Value); ok && val <= 0 {
					return b.errorf("NTILE bucket count must be a positive integer, got %d", val)
				}
			}
		}

	case "LAG", "LEAD":
		// Offset (second arg) must be non-negative
		if len(args) >= 2 {
			if lit, ok := args[1].(*BoundLiteral); ok {
				if val, ok := getIntValue(lit.Value); ok && val < 0 {
					return b.errorf("%s offset must be non-negative, got %d", funcName, val)
				}
			}
		}

	case "NTH_VALUE":
		// Index (second arg) must be positive
		if len(args) >= 2 {
			if lit, ok := args[1].(*BoundLiteral); ok {
				if val, ok := getIntValue(lit.Value); ok && val <= 0 {
					return b.errorf("NTH_VALUE index must be positive, got %d", val)
				}
			}
		}
	}

	return nil
}

// validateWindowFrame validates the window frame specification.
func (b *Binder) validateWindowFrame(frame *parser.WindowFrame, orderBy []BoundWindowOrder) error {
	if frame == nil {
		return nil
	}

	// RANGE frame requires exactly one ORDER BY column
	if frame.Type == parser.FrameTypeRange {
		// Check if any bound uses an offset (N PRECEDING or N FOLLOWING)
		hasOffset := (frame.Start.Type == parser.BoundPreceding || frame.Start.Type == parser.BoundFollowing) ||
			(frame.End.Type == parser.BoundPreceding || frame.End.Type == parser.BoundFollowing)

		if hasOffset && len(orderBy) != 1 {
			return b.errorf("RANGE frame with offset requires exactly one ORDER BY column, got %d", len(orderBy))
		}
	}

	// Validate frame bounds are logically valid (start <= end)
	if !isValidFrameBounds(frame.Start.Type, frame.End.Type) {
		return b.errorf("window frame start must not be after frame end")
	}

	return nil
}

// isValidFrameBounds checks if start bound <= end bound.
func isValidFrameBounds(start, end parser.BoundType) bool {
	// Order: UNBOUNDED_PRECEDING < PRECEDING < CURRENT_ROW < FOLLOWING < UNBOUNDED_FOLLOWING
	startRank := boundTypeRank(start)
	endRank := boundTypeRank(end)
	return startRank <= endRank
}

// boundTypeRank returns the relative order of a bound type.
func boundTypeRank(bt parser.BoundType) int {
	switch bt {
	case parser.BoundUnboundedPreceding:
		return 0
	case parser.BoundPreceding:
		return 1
	case parser.BoundCurrentRow:
		return 2
	case parser.BoundFollowing:
		return 3
	case parser.BoundUnboundedFollowing:
		return 4
	default:
		return 2 // Default to CURRENT_ROW rank
	}
}

// getIntValue extracts an integer value from a literal.
func getIntValue(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

// bindSequenceCall binds a sequence function call (NEXTVAL or CURRVAL).
func (b *Binder) bindSequenceCall(f *parser.FunctionCall) (*BoundSequenceCall, error) {
	funcNameUpper := strings.ToUpper(f.Name)

	// Validate argument count: both NEXTVAL and CURRVAL take exactly 1 argument
	if len(f.Args) != 1 {
		return nil, b.errorf("%s requires exactly 1 argument (sequence name)", funcNameUpper)
	}

	// The argument must be a string literal (sequence name)
	lit, ok := f.Args[0].(*parser.Literal)
	if !ok || lit.Type != dukdb.TYPE_VARCHAR {
		return nil, b.errorf("%s argument must be a string literal (sequence name)", funcNameUpper)
	}

	sequenceName, ok := lit.Value.(string)
	if !ok {
		return nil, b.errorf("%s argument must be a string literal (sequence name)", funcNameUpper)
	}

	// Parse the sequence name (may be qualified as schema.sequence)
	schemaName := "main"
	parts := strings.Split(sequenceName, ".")
	if len(parts) == 2 {
		schemaName = parts[0]
		sequenceName = parts[1]
	} else if len(parts) > 2 {
		return nil, b.errorf("invalid sequence name: %s", sequenceName)
	}

	// Validate that the sequence exists in the catalog
	if b.catalog != nil {
		schema, ok := b.catalog.GetSchema(schemaName)
		if !ok {
			return nil, b.errorf("schema not found: %s", schemaName)
		}

		_, ok = schema.GetSequence(sequenceName)
		if !ok {
			return nil, b.errorf("sequence not found: %s.%s", schemaName, sequenceName)
		}
	}

	return &BoundSequenceCall{
		FunctionName: funcNameUpper,
		SchemaName:   schemaName,
		SequenceName: sequenceName,
	}, nil
}
