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
