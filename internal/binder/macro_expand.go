package binder

import (
	"fmt"
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// maxMacroExpansionDepth prevents infinite recursion from macros that call themselves.
const maxMacroExpansionDepth = 32

// expandScalarMacro expands a scalar macro invocation by parsing its body,
// substituting parameters with the provided arguments, and returning the
// resulting expression for further binding.
func (b *Binder) expandScalarMacro(
	macro *catalog.MacroDef,
	args []parser.Expr,
	depth int,
) (parser.Expr, error) {
	if depth > maxMacroExpansionDepth {
		return nil, fmt.Errorf(
			"macro expansion depth limit exceeded (max %d) for macro %s",
			maxMacroExpansionDepth, macro.Name,
		)
	}

	// Validate argument count
	minArgs := 0
	for _, p := range macro.Params {
		if !p.HasDefault {
			minArgs++
		}
	}
	if len(args) < minArgs {
		return nil, fmt.Errorf(
			"macro %s requires at least %d argument(s), got %d",
			macro.Name, minArgs, len(args),
		)
	}
	if len(args) > len(macro.Params) {
		return nil, fmt.Errorf(
			"macro %s accepts at most %d argument(s), got %d",
			macro.Name, len(macro.Params), len(args),
		)
	}

	// Parse macro body as "SELECT <body>" to get the expression
	wrapSQL := "SELECT " + macro.Body
	stmt, err := parser.Parse(wrapSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid macro body for %s: %v", macro.Name, err,
		)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || len(selectStmt.Columns) == 0 {
		return nil, fmt.Errorf(
			"invalid macro body for %s", macro.Name,
		)
	}
	bodyExpr := selectStmt.Columns[0].Expr

	// Build substitution map: param name -> argument expression
	subst := make(map[string]parser.Expr)
	for i, param := range macro.Params {
		if i < len(args) {
			subst[strings.ToLower(param.Name)] = args[i]
		} else if param.HasDefault {
			// Parse default expression
			defSQL := "SELECT " + param.DefaultExpr
			defStmt, err := parser.Parse(defSQL)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid default for param %s: %v",
					param.Name, err,
				)
			}
			defSelect, ok := defStmt.(*parser.SelectStmt)
			if !ok || len(defSelect.Columns) == 0 {
				return nil, fmt.Errorf(
					"invalid default for param %s",
					param.Name,
				)
			}
			subst[strings.ToLower(param.Name)] = defSelect.Columns[0].Expr
		}
	}

	// Substitute parameters in the body expression
	return substituteParamsInExpr(bodyExpr, subst), nil
}

// expandTableMacro expands a table macro invocation by parsing its query body,
// substituting parameters, and binding the result as a subquery.
func (b *Binder) expandTableMacro(
	ref parser.TableRef,
	macro *catalog.MacroDef,
) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction

	// Validate argument count
	minArgs := 0
	for _, p := range macro.Params {
		if !p.HasDefault {
			minArgs++
		}
	}
	if len(tableFunc.Args) < minArgs {
		return nil, b.errorf(
			"macro %s requires at least %d argument(s), got %d",
			macro.Name, minArgs, len(tableFunc.Args),
		)
	}
	if len(tableFunc.Args) > len(macro.Params) {
		return nil, b.errorf(
			"macro %s accepts at most %d argument(s), got %d",
			macro.Name, len(macro.Params), len(tableFunc.Args),
		)
	}

	// Parse macro query
	stmt, err := parser.Parse(macro.Query)
	if err != nil {
		return nil, fmt.Errorf("invalid table macro query: %v", err)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("invalid table macro query: expected SELECT")
	}

	// Build substitution map
	subst := make(map[string]parser.Expr)
	for i, param := range macro.Params {
		if i < len(tableFunc.Args) {
			subst[strings.ToLower(param.Name)] = tableFunc.Args[i]
		} else if param.HasDefault {
			defSQL := "SELECT " + param.DefaultExpr
			defStmt, parseErr := parser.Parse(defSQL)
			if parseErr != nil {
				continue
			}
			if defSelect, ok := defStmt.(*parser.SelectStmt); ok && len(defSelect.Columns) > 0 {
				subst[strings.ToLower(param.Name)] = defSelect.Columns[0].Expr
			}
		}
	}

	// Substitute parameters in the query
	substitutedQuery := substituteParamsInSelect(selectStmt, subst)
	substitutedQuery.IsSubquery = true

	alias := ref.Alias
	if alias == "" {
		alias = macro.Name
	}

	// Bind the substituted query as a subquery
	subquery, err := b.bindSelect(substitutedQuery)
	if err != nil {
		return nil, err
	}

	boundRef := &BoundTableRef{
		Alias:    alias,
		Subquery: subquery,
	}

	// Create columns from subquery
	for i, col := range subquery.Columns {
		colName := col.Alias
		if colName == "" {
			colName = fmt.Sprintf("col%d", i)
		}
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     colName,
				ColumnIdx:  i,
				Type:       col.Expr.ResultType(),
				SourceType: "subquery",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = alias

	return boundRef, nil
}

// substituteParamsInSelect walks a SelectStmt and substitutes parameter
// references in all expression positions.
func substituteParamsInSelect(
	s *parser.SelectStmt,
	subst map[string]parser.Expr,
) *parser.SelectStmt {
	result := *s // shallow copy

	// Substitute in columns
	if len(s.Columns) > 0 {
		newCols := make([]parser.SelectColumn, len(s.Columns))
		for i, col := range s.Columns {
			newCols[i] = parser.SelectColumn{
				Expr:  substituteParamsInExpr(col.Expr, subst),
				Alias: col.Alias,
				Star:  col.Star,
			}
		}
		result.Columns = newCols
	}

	// Substitute in WHERE
	if s.Where != nil {
		result.Where = substituteParamsInExpr(s.Where, subst)
	}

	// Substitute in GROUP BY
	if len(s.GroupBy) > 0 {
		newGroupBy := make([]parser.Expr, len(s.GroupBy))
		for i, g := range s.GroupBy {
			newGroupBy[i] = substituteParamsInExpr(g, subst)
		}
		result.GroupBy = newGroupBy
	}

	// Substitute in HAVING
	if s.Having != nil {
		result.Having = substituteParamsInExpr(s.Having, subst)
	}

	// Substitute in ORDER BY
	if len(s.OrderBy) > 0 {
		newOrderBy := make([]parser.OrderByExpr, len(s.OrderBy))
		for i, ob := range s.OrderBy {
			newOrderBy[i] = parser.OrderByExpr{
				Expr:       substituteParamsInExpr(ob.Expr, subst),
				Desc:       ob.Desc,
				NullsFirst: ob.NullsFirst,
			}
		}
		result.OrderBy = newOrderBy
	}

	return &result
}

// substituteParamsInExpr walks an expression tree and replaces unqualified
// ColumnRef nodes whose names match macro parameter names with the
// corresponding argument expressions.
func substituteParamsInExpr(
	expr parser.Expr,
	subst map[string]parser.Expr,
) parser.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *parser.ColumnRef:
		// Only replace unqualified column references matching param names
		if e.Table == "" {
			if replacement, ok := subst[strings.ToLower(e.Column)]; ok {
				return replacement
			}
		}
		return e

	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:  substituteParamsInExpr(e.Left, subst),
			Op:    e.Op,
			Right: substituteParamsInExpr(e.Right, subst),
		}

	case *parser.UnaryExpr:
		return &parser.UnaryExpr{
			Op:   e.Op,
			Expr: substituteParamsInExpr(e.Expr, subst),
		}

	case *parser.FunctionCall:
		newArgs := make([]parser.Expr, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = substituteParamsInExpr(arg, subst)
		}
		var newNamedArgs map[string]parser.Expr
		if e.NamedArgs != nil {
			newNamedArgs = make(map[string]parser.Expr, len(e.NamedArgs))
			for name, arg := range e.NamedArgs {
				newNamedArgs[name] = substituteParamsInExpr(arg, subst)
			}
		}
		return &parser.FunctionCall{
			Name:      e.Name,
			Args:      newArgs,
			NamedArgs: newNamedArgs,
			Distinct:  e.Distinct,
			Star:      e.Star,
			OrderBy:   e.OrderBy,
		}

	case *parser.CastExpr:
		return &parser.CastExpr{
			Expr:       substituteParamsInExpr(e.Expr, subst),
			TargetType: e.TargetType,
			TryCast:    e.TryCast,
		}

	case *parser.CaseExpr:
		newWhens := make([]parser.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			newWhens[i] = parser.WhenClause{
				Condition: substituteParamsInExpr(w.Condition, subst),
				Result:    substituteParamsInExpr(w.Result, subst),
			}
		}
		return &parser.CaseExpr{
			Operand: substituteParamsInExpr(e.Operand, subst),
			Whens:   newWhens,
			Else:    substituteParamsInExpr(e.Else, subst),
		}

	case *parser.BetweenExpr:
		return &parser.BetweenExpr{
			Expr: substituteParamsInExpr(e.Expr, subst),
			Low:  substituteParamsInExpr(e.Low, subst),
			High: substituteParamsInExpr(e.High, subst),
			Not:  e.Not,
		}

	case *parser.InListExpr:
		newValues := make([]parser.Expr, len(e.Values))
		for i, v := range e.Values {
			newValues[i] = substituteParamsInExpr(v, subst)
		}
		return &parser.InListExpr{
			Expr:   substituteParamsInExpr(e.Expr, subst),
			Values: newValues,
			Not:    e.Not,
		}

	case *parser.SimilarToExpr:
		return &parser.SimilarToExpr{
			Expr:    substituteParamsInExpr(e.Expr, subst),
			Pattern: substituteParamsInExpr(e.Pattern, subst),
			Escape:  e.Escape,
			Not:     e.Not,
		}

	default:
		// For literals, stars, parameters, and other leaf/complex nodes,
		// return as-is.
		return expr
	}
}
