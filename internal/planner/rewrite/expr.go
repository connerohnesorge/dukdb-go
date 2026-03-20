package rewrite

import "github.com/dukdb/dukdb-go/internal/binder"

// RewriteExpr applies a rewriter to an expression tree.
func RewriteExpr(expr binder.BoundExpr, rewriter ExprRewriter) (binder.BoundExpr, bool) {
	if expr == nil {
		return nil, false
	}

	changed := false
	switch node := expr.(type) {
	case *binder.BoundBinaryExpr:
		left, leftChanged := RewriteExpr(node.Left, rewriter)
		right, rightChanged := RewriteExpr(node.Right, rewriter)
		if leftChanged || rightChanged {
			changed = true
			expr = &binder.BoundBinaryExpr{Left: left, Op: node.Op, Right: right, ResType: node.ResType}
		}
	case *binder.BoundUnaryExpr:
		inner, innerChanged := RewriteExpr(node.Expr, rewriter)
		if innerChanged {
			changed = true
			expr = &binder.BoundUnaryExpr{Op: node.Op, Expr: inner, ResType: node.ResType}
		}
	case *binder.BoundBetweenExpr:
		exprNode, exprChanged := RewriteExpr(node.Expr, rewriter)
		low, lowChanged := RewriteExpr(node.Low, rewriter)
		high, highChanged := RewriteExpr(node.High, rewriter)
		if exprChanged || lowChanged || highChanged {
			changed = true
			expr = &binder.BoundBetweenExpr{Expr: exprNode, Low: low, High: high, Not: node.Not}
		}
	case *binder.BoundInListExpr:
		exprNode, exprChanged := RewriteExpr(node.Expr, rewriter)
		values, valuesChanged := rewriteExprSlice(node.Values, rewriter)
		if exprChanged || valuesChanged {
			changed = true
			expr = &binder.BoundInListExpr{Expr: exprNode, Values: values, Not: node.Not}
		}
	case *binder.BoundCastExpr:
		inner, innerChanged := RewriteExpr(node.Expr, rewriter)
		if innerChanged {
			changed = true
			expr = &binder.BoundCastExpr{Expr: inner, TargetType: node.TargetType, TryCast: node.TryCast}
		}
	case *binder.BoundCaseExpr:
		operand, opChanged := RewriteExpr(node.Operand, rewriter)
		whens := make([]*binder.BoundWhenClause, len(node.Whens))
		localChanged := opChanged
		for i, when := range node.Whens {
			cond, condChanged := RewriteExpr(when.Condition, rewriter)
			result, resChanged := RewriteExpr(when.Result, rewriter)
			if condChanged || resChanged {
				localChanged = true
			}
			whens[i] = &binder.BoundWhenClause{Condition: cond, Result: result}
		}
		elseExpr, elseChanged := RewriteExpr(node.Else, rewriter)
		if localChanged || elseChanged {
			changed = true
			expr = &binder.BoundCaseExpr{Operand: operand, Whens: whens, Else: elseExpr, ResType: node.ResType}
		}
	case *binder.BoundFunctionCall:
		args, argsChanged := rewriteExprSlice(node.Args, rewriter)
		orderByChanged := false
		orderBy := make([]binder.BoundOrderByExpr, len(node.OrderBy))
		for i, ob := range node.OrderBy {
			rewritten, changedExpr := RewriteExpr(ob.Expr, rewriter)
			if changedExpr {
				orderByChanged = true
			}
			orderBy[i] = binder.BoundOrderByExpr{Expr: rewritten, Desc: ob.Desc}
		}
		if argsChanged || orderByChanged {
			changed = true
			expr = &binder.BoundFunctionCall{
				Name:      node.Name,
				Args:      args,
				NamedArgs: node.NamedArgs,
				Distinct:  node.Distinct,
				Star:      node.Star,
				OrderBy:   orderBy,
				ResType:   node.ResType,
			}
		}
	case *binder.BoundScalarUDF:
		args, argsChanged := rewriteExprSlice(node.Args, rewriter)
		if argsChanged {
			changed = true
			expr = &binder.BoundScalarUDF{
				Name:    node.Name,
				Args:    args,
				ResType: node.ResType,
				UDFInfo: node.UDFInfo,
				ArgInfo: node.ArgInfo,
				BindCtx: node.BindCtx,
			}
		}
	case *binder.BoundArrayExpr:
		elems, elemsChanged := rewriteExprSlice(node.Elements, rewriter)
		if elemsChanged {
			changed = true
			expr = &binder.BoundArrayExpr{Elements: elems, ElemType: node.ElemType}
		}
	case *binder.BoundSimilarToExpr:
		exprNode, exprChanged := RewriteExpr(node.Expr, rewriter)
		patNode, patChanged := RewriteExpr(node.Pattern, rewriter)
		if exprChanged || patChanged {
			changed = true
			expr = &binder.BoundSimilarToExpr{Expr: exprNode, Pattern: patNode, Escape: node.Escape, Not: node.Not}
		}
	}

	if rewriter == nil {
		return expr, changed
	}
	rewritten, applied := rewriter(expr)
	if applied {
		return rewritten, true
	}
	return expr, changed
}

func rewriteExprSlice(exprs []binder.BoundExpr, rewriter ExprRewriter) ([]binder.BoundExpr, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	changed := false
	rewritten := make([]binder.BoundExpr, len(exprs))
	for i, expr := range exprs {
		next, exprChanged := RewriteExpr(expr, rewriter)
		if exprChanged {
			changed = true
		}
		rewritten[i] = next
	}
	return rewritten, changed
}

// WalkExpr visits each node in an expression tree.
func WalkExpr(expr binder.BoundExpr, visit func(binder.BoundExpr)) {
	if expr == nil {
		return
	}
	visit(expr)
	switch node := expr.(type) {
	case *binder.BoundBinaryExpr:
		WalkExpr(node.Left, visit)
		WalkExpr(node.Right, visit)
	case *binder.BoundUnaryExpr:
		WalkExpr(node.Expr, visit)
	case *binder.BoundBetweenExpr:
		WalkExpr(node.Expr, visit)
		WalkExpr(node.Low, visit)
		WalkExpr(node.High, visit)
	case *binder.BoundInListExpr:
		WalkExpr(node.Expr, visit)
		for _, val := range node.Values {
			WalkExpr(val, visit)
		}
	case *binder.BoundCastExpr:
		WalkExpr(node.Expr, visit)
	case *binder.BoundCaseExpr:
		WalkExpr(node.Operand, visit)
		for _, when := range node.Whens {
			WalkExpr(when.Condition, visit)
			WalkExpr(when.Result, visit)
		}
		WalkExpr(node.Else, visit)
	case *binder.BoundFunctionCall:
		for _, arg := range node.Args {
			WalkExpr(arg, visit)
		}
		for _, ob := range node.OrderBy {
			WalkExpr(ob.Expr, visit)
		}
	case *binder.BoundScalarUDF:
		for _, arg := range node.Args {
			WalkExpr(arg, visit)
		}
	case *binder.BoundArrayExpr:
		for _, elem := range node.Elements {
			WalkExpr(elem, visit)
		}
	case *binder.BoundSimilarToExpr:
		WalkExpr(node.Expr, visit)
		WalkExpr(node.Pattern, visit)
	}
}
