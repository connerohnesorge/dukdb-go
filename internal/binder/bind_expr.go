package binder

import (
	"fmt"
	"regexp"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
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

		return &BoundCastExpr{Expr: inner, TargetType: e.TargetType, TryCast: e.TryCast}, nil
	case *parser.CaseExpr:
		return b.bindCaseExpr(e, expectedType)
	case *parser.BetweenExpr:
		return b.bindBetweenExpr(e)
	case *parser.InListExpr:
		return b.bindInListExpr(e)
	case *parser.InSubqueryExpr:
		return b.bindInSubqueryExpr(e)
	case *parser.QuantifiedComparisonExpr:
		return b.bindQuantifiedComparisonExpr(e)
	case *parser.ExistsExpr:
		return b.bindExistsExpr(e)
	case *parser.StarExpr:
		return b.bindStarExpr(e)
	case *parser.ColumnsExpr:
		return b.bindColumnsExpr(e)
	case *parser.SelectStmt:
		// Scalar subquery: push outer scope so correlated column references resolve correctly.
		pop := b.pushOuterScope()
		result, err := b.bindSelect(e)
		pop()
		return result, err
	case *parser.ExtractExpr:
		return b.bindExtractExpr(e)
	case *parser.IntervalLiteral:
		return b.bindIntervalLiteral(e)
	case *parser.WindowExpr:
		return b.bindWindowExpr(e)
	case *parser.GroupingSetExpr:
		return b.bindGroupingSetExpr(e)
	case *parser.RollupExpr:
		return b.bindRollupExpr(e)
	case *parser.CubeExpr:
		return b.bindCubeExpr(e)
	case *parser.ArrayExpr:
		return b.bindArrayExpr(e)
	case *parser.MapLiteralExpr:
		return b.bindMapLiteralExpr(e)
	case *parser.SubscriptExpr:
		return b.bindSubscriptExpr(e)
	case *parser.LambdaExpr:
		// Lambda expressions store the raw parser body for runtime evaluation.
		// Lambda params are not real columns -- they are bound at execution time.
		return &BoundLambdaExpr{Params: e.Params, BodyExpr: e.Body}, nil
	case *parser.SimilarToExpr:
		expr, err := b.bindExpr(e.Expr, dukdb.TYPE_VARCHAR)
		if err != nil {
			return nil, err
		}
		pattern, err := b.bindExpr(e.Pattern, dukdb.TYPE_VARCHAR)
		if err != nil {
			return nil, err
		}
		var escapeRune rune
		if e.Escape != "" {
			escapeRune = []rune(e.Escape)[0]
		}
		return &BoundSimilarToExpr{
			Expr:    expr,
			Pattern: pattern,
			Escape:  escapeRune,
			Not:     e.Not,
		}, nil
	default:
		return nil, b.errorf("unsupported expression type: %T", expr)
	}
}

func (b *Binder) bindColumnRef(
	ref *parser.ColumnRef,
) (BoundExpr, error) {
	if ref.Table != "" {
		// Qualified column reference -- try as table.column first
		tableRef, ok := b.scope.tables[ref.Table]
		if ok {
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

		// Table not found in current scope -- try as struct_column.field_name
		// We check for TYPE_STRUCT or TYPE_ANY (since CTAS may not preserve struct type info)
		for _, tRef := range b.scope.tables {
			for _, col := range tRef.Columns {
				if strings.EqualFold(col.Column, ref.Table) &&
					(col.Type == dukdb.TYPE_STRUCT || col.Type == dukdb.TYPE_ANY) {
					structRef := &BoundColumnRef{
						Table:     tRef.Alias,
						Column:    col.Column,
						ColumnIdx: col.ColumnIdx,
						ColType:   col.Type,
					}
					if structRef.Table == "" {
						structRef.Table = tRef.TableName
					}
					return &BoundFieldAccess{
						Struct:  structRef,
						Field:   ref.Column,
						ResType: dukdb.TYPE_ANY,
					}, nil
				}
			}
		}

		// Correlated column resolution: search outer scopes (innermost first)
		for i := len(b.outerScopes) - 1; i >= 0; i-- {
			outerScope := b.outerScopes[i]
			if outerTableRef, ok := outerScope.tables[ref.Table]; ok {
				for _, col := range outerTableRef.Columns {
					if strings.EqualFold(col.Column, ref.Column) {
						return &BoundCorrelatedColumnRef{
							BoundColumnRef: BoundColumnRef{
								Table:     ref.Table,
								Column:    col.Column,
								ColumnIdx: col.ColumnIdx,
								ColType:   col.Type,
							},
							Depth: len(b.outerScopes) - i,
						}, nil
					}
				}
			}
		}

		return nil, b.errorf(
			"table or struct column not found: %s",
			ref.Table,
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
		parser.OpIsNot,
		parser.OpIsDistinctFrom,
		parser.OpIsNotDistinctFrom:
		resType = dukdb.TYPE_BOOLEAN
	case parser.OpConcat:
		resType = dukdb.TYPE_VARCHAR
	case parser.OpJSONExtract:
		// -> returns JSON
		resType = dukdb.TYPE_JSON
	case parser.OpJSONText:
		// ->> returns VARCHAR
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
	// Handle special functions
	funcNameUpper := strings.ToUpper(f.Name)

	// Handle sequence functions (NEXTVAL, CURRVAL)
	if funcNameUpper == "NEXTVAL" || funcNameUpper == "CURRVAL" {
		return b.bindSequenceCall(f)
	}

	// Handle GROUPING() function for GROUPING SETS/ROLLUP/CUBE
	if funcNameUpper == "GROUPING" {
		return b.bindGroupingCall(f)
	}

	// Check for scalar macro expansion before built-in function lookup
	if b.catalog != nil {
		funcNameLower := strings.ToLower(f.Name)
		if macro, ok := b.catalog.GetMacro("", funcNameLower); ok && macro.Type == catalog.MacroTypeScalar {
			expanded, err := b.expandScalarMacro(macro, f.Args, 0)
			if err != nil {
				return nil, err
			}
			return b.bindExpr(expanded, dukdb.TYPE_ANY)
		}
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

	// Bind named arguments (e.g., struct_pack(name := 'Alice'))
	var namedArgs map[string]BoundExpr
	if len(f.NamedArgs) > 0 {
		namedArgs = make(map[string]BoundExpr, len(f.NamedArgs))
		for name, argExpr := range f.NamedArgs {
			bound, err := b.bindExpr(argExpr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			namedArgs[name] = bound
		}
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

	// Bind ORDER BY expressions within aggregate function
	var boundOrderBy []BoundOrderByExpr
	for _, ob := range f.OrderBy {
		boundExpr, err := b.bindExpr(ob.Expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, fmt.Errorf("binding ORDER BY in %s: %w", f.Name, err)
		}
		boundOrderBy = append(boundOrderBy, BoundOrderByExpr{
			Expr: boundExpr,
			Desc: ob.Desc,
		})
	}

	// Bind FILTER expression
	var boundFilter BoundExpr
	if f.Filter != nil {
		var filterErr error
		boundFilter, filterErr = b.bindExpr(f.Filter, dukdb.TYPE_BOOLEAN)
		if filterErr != nil {
			return nil, fmt.Errorf("binding FILTER in %s: %w", f.Name, filterErr)
		}
	}

	return &BoundFunctionCall{
		Name:      f.Name,
		Args:      args,
		NamedArgs: namedArgs,
		Distinct:  f.Distinct,
		Star:      f.Star,
		OrderBy:   boundOrderBy,
		ResType:   resType,
		Filter:    boundFilter,
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

	// Determine result type as common type of all THEN/ELSE expressions
	bound.ResType = inferCaseResultType(bound.Whens, bound.Else)

	return bound, nil
}

// inferCaseResultType determines the common result type for a CASE expression.
// It finds the common supertype of all THEN branches and the ELSE branch (if any).
func inferCaseResultType(whens []*BoundWhenClause, elseExpr BoundExpr) dukdb.Type {
	if len(whens) == 0 && elseExpr == nil {
		return dukdb.TYPE_SQLNULL
	}

	var resultType dukdb.Type

	// Collect types from all THEN branches
	for i, when := range whens {
		if i == 0 {
			resultType = when.Result.ResultType()
		} else {
			resultType = promoteType(resultType, when.Result.ResultType())
		}
	}

	// Include ELSE branch in type promotion
	if elseExpr != nil {
		if len(whens) == 0 {
			resultType = elseExpr.ResultType()
		} else {
			resultType = promoteType(resultType, elseExpr.ResultType())
		}
	}

	// If we still have an unresolved type, default to VARCHAR for safety
	if resultType == dukdb.TYPE_ANY || resultType == dukdb.TYPE_INVALID {
		return dukdb.TYPE_VARCHAR
	}

	return resultType
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

	// Push outer scope so the subquery can see correlated columns from the outer query.
	pop := b.pushOuterScope()
	subquery, err := b.bindSelect(e.Subquery)
	pop()
	if err != nil {
		return nil, err
	}

	return &BoundInSubqueryExpr{
		Expr:     expr,
		Subquery: subquery,
		Not:      e.Not,
	}, nil
}

func (b *Binder) bindQuantifiedComparisonExpr(
	e *parser.QuantifiedComparisonExpr,
) (*BoundQuantifiedComparison, error) {
	left, err := b.bindExpr(e.Left, dukdb.TYPE_ANY)
	if err != nil {
		return nil, err
	}

	// Push outer scope so the subquery can see correlated columns from the outer query.
	pop := b.pushOuterScope()
	subquery, err := b.bindSelect(e.Subquery)
	pop()
	if err != nil {
		return nil, err
	}

	return &BoundQuantifiedComparison{
		Left:       left,
		Op:         e.Op,
		Quantifier: e.Quantifier,
		Subquery:   subquery,
	}, nil
}

func (b *Binder) bindExistsExpr(
	e *parser.ExistsExpr,
) (*BoundExistsExpr, error) {
	// Push outer scope so the subquery can see correlated columns from the outer query.
	pop := b.pushOuterScope()
	subquery, err := b.bindSelect(e.Subquery)
	pop()
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

	// Apply EXCLUDE filter
	if len(e.Exclude) > 0 {
		// Validate all excluded columns exist
		for _, excl := range e.Exclude {
			found := false
			for _, col := range bound.Columns {
				if strings.EqualFold(col.Column, excl) {
					found = true
					break
				}
			}
			if !found {
				return nil, b.errorf("EXCLUDE column %q not found", excl)
			}
		}
		// Filter out excluded columns
		var filtered []*BoundColumn
		for _, col := range bound.Columns {
			excluded := false
			for _, excl := range e.Exclude {
				if strings.EqualFold(col.Column, excl) {
					excluded = true
					break
				}
			}
			if !excluded {
				filtered = append(filtered, col)
			}
		}
		bound.Columns = filtered
	}

	// Bind REPLACE expressions
	if len(e.Replace) > 0 {
		bound.Replacements = make(map[string]BoundExpr)
		for _, repl := range e.Replace {
			// Validate column exists
			found := false
			for _, col := range bound.Columns {
				if strings.EqualFold(col.Column, repl.Column) {
					found = true
					break
				}
			}
			if !found {
				return nil, b.errorf("REPLACE column %q not found", repl.Column)
			}
			boundExpr, err := b.bindExpr(repl.Expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			bound.Replacements[strings.ToUpper(repl.Column)] = boundExpr
		}
	}

	return bound, nil
}

// bindColumnsExpr binds a COLUMNS(pattern) expression.
// It matches column names in scope against the given regex pattern
// and returns a BoundStarExpr containing the matched columns.
func (b *Binder) bindColumnsExpr(
	e *parser.ColumnsExpr,
) (*BoundStarExpr, error) {
	re, err := regexp.Compile(e.Pattern)
	if err != nil {
		return nil, b.errorf("COLUMNS: invalid regex pattern %q: %v", e.Pattern, err)
	}

	bound := &BoundStarExpr{}

	// Match against all columns in scope
	for _, tableRef := range b.scope.tables {
		for _, col := range tableRef.Columns {
			if re.MatchString(col.Column) {
				bound.Columns = append(bound.Columns, col)
			}
		}
	}

	if len(bound.Columns) == 0 {
		return nil, b.errorf("COLUMNS('%s') matched no columns", e.Pattern)
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
	// Resolve named window reference
	if e.RefName != "" {
		refUpper := strings.ToUpper(e.RefName)
		def, ok := b.windowDefs[refUpper]
		if !ok {
			return nil, b.errorf("undefined window: %s", e.RefName)
		}
		// Merge window def into the expression
		if len(e.PartitionBy) == 0 {
			e.PartitionBy = def.PartitionBy
		} else if len(def.PartitionBy) > 0 {
			return nil, b.errorf("cannot override PARTITION BY of window %s", e.RefName)
		}
		if len(e.OrderBy) == 0 {
			e.OrderBy = def.OrderBy
		} else if len(def.OrderBy) > 0 {
			return nil, b.errorf("cannot override ORDER BY of window %s", e.RefName)
		}
		if e.Frame == nil {
			e.Frame = def.Frame
		} else if def.Frame != nil {
			return nil, b.errorf("cannot override frame of window %s", e.RefName)
		}
	}

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
			return b.errorf(
				"RANGE frame with offset requires exactly one ORDER BY column, got %d",
				len(orderBy),
			)
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

// ---------- Grouping Set Binding Functions ----------

// bindGroupingSetExpr binds a GROUPING SETS expression.
// GROUPING SETS allows explicit specification of multiple grouping levels.
//
// Example: GROUP BY GROUPING SETS ((a, b), (a), ())
func (b *Binder) bindGroupingSetExpr(e *parser.GroupingSetExpr) (*BoundGroupingSetExpr, error) {
	var boundType BoundGroupingSetType
	switch e.Type {
	case parser.GroupingSetSimple:
		boundType = BoundGroupingSetSimple
	case parser.GroupingSetRollup:
		boundType = BoundGroupingSetRollup
	case parser.GroupingSetCube:
		boundType = BoundGroupingSetCube
	}

	// Bind each grouping set
	boundSets := make([][]BoundExpr, 0, len(e.Exprs))
	for _, set := range e.Exprs {
		boundSet := make([]BoundExpr, 0, len(set))
		for _, expr := range set {
			boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			boundSet = append(boundSet, boundExpr)
		}
		boundSets = append(boundSets, boundSet)
	}

	return &BoundGroupingSetExpr{
		Type: boundType,
		Sets: boundSets,
	}, nil
}

// bindRollupExpr binds a ROLLUP expression by expanding it into grouping sets.
// ROLLUP(a, b, c) expands to: (a, b, c), (a, b), (a), ()
//
// This creates a hierarchical set of groupings from left to right,
// providing subtotals for each level plus a grand total.
func (b *Binder) bindRollupExpr(e *parser.RollupExpr) (*BoundGroupingSetExpr, error) {
	// First bind all the expressions
	boundExprs := make([]BoundExpr, 0, len(e.Exprs))
	for _, expr := range e.Exprs {
		boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundExprs = append(boundExprs, boundExpr)
	}

	// Expand ROLLUP into grouping sets
	// ROLLUP(a, b, c) -> (a, b, c), (a, b), (a), ()
	numExprs := len(boundExprs)
	sets := make([][]BoundExpr, numExprs+1)

	for i := 0; i <= numExprs; i++ {
		// Create set with first (numExprs - i) expressions
		setSize := numExprs - i
		set := make([]BoundExpr, setSize)
		for j := 0; j < setSize; j++ {
			set[j] = boundExprs[j]
		}
		sets[i] = set
	}

	return &BoundGroupingSetExpr{
		Type: BoundGroupingSetRollup,
		Sets: sets,
	}, nil
}

// bindCubeExpr binds a CUBE expression by expanding it into all possible grouping sets.
// CUBE(a, b) expands to: (a, b), (a), (b), ()
//
// For n expressions, CUBE generates 2^n grouping sets representing all
// possible combinations of the grouping columns.
func (b *Binder) bindCubeExpr(e *parser.CubeExpr) (*BoundGroupingSetExpr, error) {
	// First bind all the expressions
	boundExprs := make([]BoundExpr, 0, len(e.Exprs))
	for _, expr := range e.Exprs {
		boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundExprs = append(boundExprs, boundExpr)
	}

	// Expand CUBE into all possible grouping sets
	// CUBE(a, b) -> (a, b), (a), (b), ()
	// Use bitmask to generate all combinations: 2^n sets
	numExprs := len(boundExprs)
	numSets := 1 << numExprs // 2^n
	sets := make([][]BoundExpr, 0, numSets)

	// Generate sets in descending order of size (most columns first)
	// We iterate from all bits set (full set) down to no bits set (empty set)
	for mask := numSets - 1; mask >= 0; mask-- {
		set := make([]BoundExpr, 0)
		for i := 0; i < numExprs; i++ {
			// Check if bit i is set (from left to right, MSB first)
			bitPos := numExprs - 1 - i
			if (mask & (1 << bitPos)) != 0 {
				set = append(set, boundExprs[i])
			}
		}
		sets = append(sets, set)
	}

	return &BoundGroupingSetExpr{
		Type: BoundGroupingSetCube,
		Sets: sets,
	}, nil
}

// bindGroupingCall binds a GROUPING(col1, col2, ...) function call.
// The GROUPING function returns a bitmask indicating which columns are
// aggregated (null) in the current grouping set.
func (b *Binder) bindGroupingCall(f *parser.FunctionCall) (*BoundGroupingCall, error) {
	if len(f.Args) == 0 {
		return nil, b.errorf("GROUPING() requires at least one argument")
	}

	// All arguments must be column references
	args := make([]*BoundColumnRef, 0, len(f.Args))
	for _, arg := range f.Args {
		colRef, ok := arg.(*parser.ColumnRef)
		if !ok {
			return nil, b.errorf("GROUPING() arguments must be column references")
		}

		boundExpr, err := b.bindColumnRef(colRef)
		if err != nil {
			return nil, err
		}
		boundRef, ok := boundExpr.(*BoundColumnRef)
		if !ok {
			return nil, b.errorf("GROUPING() arguments must be simple column references, not struct field access")
		}
		args = append(args, boundRef)
	}

	return &BoundGroupingCall{
		Args: args,
	}, nil
}

// bindArrayExpr binds an array literal expression.
// Arrays in table functions are typically lists of file paths.
// Example: ['file1.csv', 'file2.csv']
func (b *Binder) bindArrayExpr(e *parser.ArrayExpr) (*BoundArrayExpr, error) {
	elements := make([]BoundExpr, 0, len(e.Elements))
	var elemType dukdb.Type = dukdb.TYPE_SQLNULL // Start with null for empty arrays

	for _, elem := range e.Elements {
		boundElem, err := b.bindExpr(elem, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		elements = append(elements, boundElem)

		// Infer element type from first non-null element
		if elemType == dukdb.TYPE_SQLNULL {
			elemType = boundElem.ResultType()
		} else if boundElem.ResultType() != dukdb.TYPE_SQLNULL {
			// Promote type if needed
			elemType = promoteType(elemType, boundElem.ResultType())
		}
	}

	// Default to VARCHAR for empty arrays (common for file paths)
	if elemType == dukdb.TYPE_SQLNULL && len(elements) == 0 {
		elemType = dukdb.TYPE_VARCHAR
	}

	return &BoundArrayExpr{
		Elements: elements,
		ElemType: elemType,
	}, nil
}

// bindMapLiteralExpr binds a MAP literal expression.
// MAP {'key1': val1, 'key2': val2}
func (b *Binder) bindMapLiteralExpr(e *parser.MapLiteralExpr) (*BoundMapLiteralExpr, error) {
	entries := make([]BoundMapLiteralEntry, 0, len(e.Entries))
	keyType := dukdb.TYPE_SQLNULL
	valueType := dukdb.TYPE_SQLNULL

	for _, entry := range e.Entries {
		boundKey, err := b.bindExpr(entry.Key, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundValue, err := b.bindExpr(entry.Value, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		entries = append(entries, BoundMapLiteralEntry{Key: boundKey, Value: boundValue})

		if keyType == dukdb.TYPE_SQLNULL {
			keyType = boundKey.ResultType()
		} else {
			keyType = promoteType(keyType, boundKey.ResultType())
		}
		if valueType == dukdb.TYPE_SQLNULL {
			valueType = boundValue.ResultType()
		} else {
			valueType = promoteType(valueType, boundValue.ResultType())
		}
	}

	if keyType == dukdb.TYPE_SQLNULL {
		keyType = dukdb.TYPE_VARCHAR
	}
	if valueType == dukdb.TYPE_SQLNULL {
		valueType = dukdb.TYPE_VARCHAR
	}

	return &BoundMapLiteralExpr{
		Entries:   entries,
		KeyType:   keyType,
		ValueType: valueType,
	}, nil
}

// bindSubscriptExpr binds a subscript expression: expr[index].
// The base expression must evaluate to a list/array type, and the index must be an integer.
// DuckDB uses 1-based indexing.
func (b *Binder) bindSubscriptExpr(e *parser.SubscriptExpr) (BoundExpr, error) {
	base, err := b.bindExpr(e.Base, dukdb.TYPE_ANY)
	if err != nil {
		return nil, err
	}

	index, err := b.bindExpr(e.Index, dukdb.TYPE_INTEGER)
	if err != nil {
		return nil, err
	}

	// Determine the element type from the base expression
	elemType := dukdb.TYPE_ANY
	if arrayExpr, ok := base.(*BoundArrayExpr); ok {
		elemType = arrayExpr.ElemType
	}

	return &BoundSubscriptExpr{
		Base:     base,
		Index:    index,
		ElemType: elemType,
	}, nil
}
