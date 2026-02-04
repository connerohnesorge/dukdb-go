package planner

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// RewriteConfig controls rule-based rewrites during logical planning.
type RewriteConfig struct {
	Enabled            bool
	ExpressionRewrites bool
	PredicatePushdown  bool
	JoinReordering     bool
	SubqueryUnnesting  bool
	ConstantFolding    bool
	ViewExpansion      bool
	IterationLimit     int
	CostThreshold      float64
}

// DefaultRewriteConfig returns the default rewrite configuration.
func DefaultRewriteConfig() RewriteConfig {
	return RewriteConfig{
		Enabled:            true,
		ExpressionRewrites: true,
		PredicatePushdown:  true,
		JoinReordering:     true,
		SubqueryUnnesting:  true,
		ConstantFolding:    true,
		ViewExpansion:      true,
		IterationLimit:     100,
		CostThreshold:      1.05,
	}
}

// RewriteStats tracks rule application during a rewrite pass.
type RewriteStats struct {
	Iterations int
	Applied    map[string]int
	Skipped    map[string]int
}

// CostEstimator estimates relative cost for rewrite pruning.
type CostEstimator interface {
	Estimate(plan LogicalPlan) float64
}

// RewriteContext carries config and services for rule application.
type RewriteContext struct {
	Config       RewriteConfig
	Stats        *RewriteStats
	Estimator    CostEstimator
	PlanSubquery func(*binder.BoundSelectStmt) (LogicalPlan, error)
}

// RewriteRule applies a logical transformation.
type RewriteRule interface {
	Name() string
	Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool)
}

// RewriteEngine applies rule-based rewrites with fixed-point iteration.
type RewriteEngine struct {
	config       RewriteConfig
	estimator    CostEstimator
	rules        []RewriteRule
	planSubquery func(*binder.BoundSelectStmt) (LogicalPlan, error)
}

// NewRewriteEngine creates a rewrite engine with default rules.
func NewRewriteEngine(cat *catalog.Catalog, config RewriteConfig) *RewriteEngine {
	engine := &RewriteEngine{
		config:    config,
		estimator: newPlanCostEstimator(cat),
	}
	engine.rules = engine.defaultRules()
	return engine
}

// SetRewriteConfig updates the planner rewrite configuration.
func (p *Planner) SetRewriteConfig(config RewriteConfig) {
	p.rewriteConfig = config
}

func (p *Planner) applyRewrites(plan LogicalPlan) (LogicalPlan, *RewriteStats) {
	engine := NewRewriteEngine(p.catalog, p.rewriteConfig)
	engine.planSubquery = p.planSelect
	return engine.Apply(plan)
}

func (e *RewriteEngine) defaultRules() []RewriteRule {
	var rules []RewriteRule
	if e.config.ExpressionRewrites || e.config.ConstantFolding {
		rules = append(rules, &expressionRewriteRule{})
	}
	if e.config.PredicatePushdown {
		rules = append(rules, &predicatePushdownRule{})
	}
	if e.config.JoinReordering {
		rules = append(rules, &joinReorderRule{})
	}
	if e.config.SubqueryUnnesting {
		rules = append(rules, &subqueryUnnestRule{})
	}
	if e.config.ViewExpansion {
		rules = append(rules, &viewExpansionRule{})
	}
	return rules
}

// Apply runs the rewrite engine to a fixed point.
func (e *RewriteEngine) Apply(plan LogicalPlan) (LogicalPlan, *RewriteStats) {
	stats := &RewriteStats{
		Applied: make(map[string]int),
		Skipped: make(map[string]int),
	}

	if !e.config.Enabled || plan == nil {
		return plan, stats
	}

	ctx := &RewriteContext{
		Config:       e.config,
		Stats:        stats,
		Estimator:    e.estimator,
		PlanSubquery: e.planSubquery,
	}

	limit := e.config.IterationLimit
	if limit <= 0 {
		limit = 1
	}

	for iter := 0; iter < limit; iter++ {
		changed := false
		for _, rule := range e.rules {
			newPlan, applied := rule.Apply(plan, ctx)
			if !applied {
				continue
			}

			if e.config.CostThreshold > 0 && ctx.Estimator != nil {
				before := ctx.Estimator.Estimate(plan)
				after := ctx.Estimator.Estimate(newPlan)
				if after > before*e.config.CostThreshold {
					stats.Skipped[rule.Name()]++
					continue
				}
			}

			plan = newPlan
			stats.Applied[rule.Name()]++
			changed = true
		}

		if !changed {
			stats.Iterations = iter + 1
			return plan, stats
		}
		stats.Iterations = iter + 1
	}

	return plan, stats
}

// planCostEstimator uses simple heuristics and catalog stats.
type planCostEstimator struct {
	stats *optimizer.StatisticsManager
}

func newPlanCostEstimator(cat *catalog.Catalog) *planCostEstimator {
	return &planCostEstimator{stats: optimizer.NewStatisticsManager(cat)}
}

func (e *planCostEstimator) Estimate(plan LogicalPlan) float64 {
	if plan == nil {
		return 0
	}
	switch node := plan.(type) {
	case *LogicalScan:
		if node.TableDef != nil {
			if stats := node.TableDef.Statistics; stats != nil {
				return float64(stats.RowCount)
			}
		}
		if e.stats != nil {
			stats := e.stats.GetTableStats(node.Schema, node.TableName)
			if stats != nil {
				return float64(stats.RowCount)
			}
		}
		return 1000
	case *LogicalFilter:
		return e.Estimate(node.Child) * 0.5
	case *LogicalProject:
		return e.Estimate(node.Child)
	case *LogicalJoin:
		return e.Estimate(node.Left) + e.Estimate(node.Right)
	case *LogicalLateralJoin:
		return e.Estimate(node.Left) + e.Estimate(node.Right)
	case *LogicalAggregate:
		return e.Estimate(node.Child) * 0.5
	case *LogicalLimit:
		return e.Estimate(node.Child)
	default:
		total := 0.0
		for _, child := range plan.Children() {
			total += e.Estimate(child)
		}
		if total == 0 {
			return 1
		}
		return total
	}
}

type expressionRewriteRule struct{}

func (*expressionRewriteRule) Name() string { return "expression_rewrite" }

func (*expressionRewriteRule) Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	rewritten, changed := rewritePlanExpressions(plan, ctx)
	return rewritten, changed
}

type predicatePushdownRule struct{}

func (*predicatePushdownRule) Name() string { return "predicate_pushdown" }

func (*predicatePushdownRule) Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	return pushdownFilters(plan)
}

type joinReorderRule struct{}

func (*joinReorderRule) Name() string { return "join_reorder" }

func (*joinReorderRule) Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	return reorderJoins(plan, ctx)
}

type subqueryUnnestRule struct{}

func (*subqueryUnnestRule) Name() string { return "subquery_unnest" }

func (*subqueryUnnestRule) Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	return unnestSubqueries(plan, ctx)
}

type viewExpansionRule struct{}

func (*viewExpansionRule) Name() string { return "view_expansion" }

func (*viewExpansionRule) Apply(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	return plan, false
}

func rewritePlanExpressions(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	if plan == nil {
		return nil, false
	}

	switch node := plan.(type) {
	case *LogicalFilter:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		expr, exprChanged := rewriteExpr(node.Condition, ctx)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalFilter{Child: child, Condition: expr}, true

	case *LogicalProject:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		exprs, exprChanged := rewriteExprSlice(node.Expressions, ctx)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalProject{
			Child:       child,
			Expressions: exprs,
			Aliases:     append([]string(nil), node.Aliases...),
		}, true

	case *LogicalJoin:
		left, leftChanged := rewritePlanExpressions(node.Left, ctx)
		right, rightChanged := rewritePlanExpressions(node.Right, ctx)
		expr, exprChanged := rewriteExpr(node.Condition, ctx)
		if !leftChanged && !rightChanged && !exprChanged {
			return plan, false
		}
		return &LogicalJoin{
			Left:      left,
			Right:     right,
			JoinType:  node.JoinType,
			Condition: expr,
		}, true

	case *LogicalLateralJoin:
		left, leftChanged := rewritePlanExpressions(node.Left, ctx)
		right, rightChanged := rewritePlanExpressions(node.Right, ctx)
		expr, exprChanged := rewriteExpr(node.Condition, ctx)
		if !leftChanged && !rightChanged && !exprChanged {
			return plan, false
		}
		return &LogicalLateralJoin{
			Left:      left,
			Right:     right,
			JoinType:  node.JoinType,
			Condition: expr,
		}, true

	case *LogicalAggregate:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		groupBy, groupChanged := rewriteExprSlice(node.GroupBy, ctx)
		aggs, aggChanged := rewriteExprSlice(node.Aggregates, ctx)
		if !childChanged && !groupChanged && !aggChanged {
			return plan, false
		}
		return &LogicalAggregate{
			Child:         child,
			GroupBy:       groupBy,
			Aggregates:    aggs,
			Aliases:       append([]string(nil), node.Aliases...),
			GroupingSets:  node.GroupingSets,
			GroupingCalls: node.GroupingCalls,
		}, true

	case *LogicalSort:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		orderBy := make([]*binder.BoundOrderBy, len(node.OrderBy))
		exprChanged := false
		for i, ob := range node.OrderBy {
			rewritten, changed := rewriteExpr(ob.Expr, ctx)
			if changed {
				exprChanged = true
			}
			orderBy[i] = &binder.BoundOrderBy{Expr: rewritten, Desc: ob.Desc}
		}
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalSort{Child: child, OrderBy: orderBy}, true

	case *LogicalLimit:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		limitExpr, limitChanged := rewriteExpr(node.LimitExpr, ctx)
		offsetExpr, offsetChanged := rewriteExpr(node.OffsetExpr, ctx)
		if !childChanged && !limitChanged && !offsetChanged {
			return plan, false
		}
		return &LogicalLimit{
			Child:      child,
			Limit:      node.Limit,
			Offset:     node.Offset,
			LimitExpr:  limitExpr,
			OffsetExpr: offsetExpr,
		}, true

	case *LogicalDistinctOn:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		exprs, exprChanged := rewriteExprSlice(node.DistinctOn, ctx)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalDistinctOn{
			Child:      child,
			DistinctOn: exprs,
			OrderBy:    node.OrderBy,
		}, true

	case *LogicalWindow:
		child, childChanged := rewritePlanExpressions(node.Child, ctx)
		rewrittenExprs := make([]*binder.BoundWindowExpr, len(node.WindowExprs))
		exprChanged := false
		for i, win := range node.WindowExprs {
			rewritten, changed := rewriteWindowExpr(win, ctx)
			rewrittenExprs[i] = rewritten
			if changed {
				exprChanged = true
			}
		}
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &LogicalWindow{Child: child, WindowExprs: rewrittenExprs}, true

	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			rewritten, childChanged := rewritePlanExpressions(child, ctx)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return replaceChildren(plan, newChildren), true
	}
}

func rewriteExprSlice(exprs []binder.BoundExpr, ctx *RewriteContext) ([]binder.BoundExpr, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	changed := false
	rewritten := make([]binder.BoundExpr, len(exprs))
	for i, expr := range exprs {
		next, exprChanged := rewriteExpr(expr, ctx)
		if exprChanged {
			changed = true
		}
		rewritten[i] = next
	}
	return rewritten, changed
}

func rewriteExpr(expr binder.BoundExpr, ctx *RewriteContext) (binder.BoundExpr, bool) {
	if expr == nil {
		return nil, false
	}

	switch node := expr.(type) {
	case *binder.BoundBinaryExpr:
		left, leftChanged := rewriteExpr(node.Left, ctx)
		right, rightChanged := rewriteExpr(node.Right, ctx)
		rewritten := node
		if leftChanged || rightChanged {
			rewritten = &binder.BoundBinaryExpr{Left: left, Op: node.Op, Right: right, ResType: node.ResType}
		}
		simplified, simpChanged := simplifyBinaryExpr(rewritten, ctx)
		if simpChanged {
			return simplified, true
		}
		return rewritten, leftChanged || rightChanged

	case *binder.BoundUnaryExpr:
		inner, innerChanged := rewriteExpr(node.Expr, ctx)
		rewritten := node
		if innerChanged {
			rewritten = &binder.BoundUnaryExpr{Op: node.Op, Expr: inner, ResType: node.ResType}
		}
		simplified, simpChanged := simplifyUnaryExpr(rewritten, ctx)
		if simpChanged {
			return simplified, true
		}
		return rewritten, innerChanged

	case *binder.BoundBetweenExpr:
		exprNode, exprChanged := rewriteExpr(node.Expr, ctx)
		low, lowChanged := rewriteExpr(node.Low, ctx)
		high, highChanged := rewriteExpr(node.High, ctx)
		if !exprChanged && !lowChanged && !highChanged {
			return expr, false
		}
		return &binder.BoundBetweenExpr{
			Expr: exprNode,
			Low:  low,
			High: high,
			Not:  node.Not,
		}, true

	case *binder.BoundInListExpr:
		exprNode, exprChanged := rewriteExpr(node.Expr, ctx)
		values, valuesChanged := rewriteExprSlice(node.Values, ctx)
		rewritten := node
		if exprChanged || valuesChanged {
			rewritten = &binder.BoundInListExpr{Expr: exprNode, Values: values, Not: node.Not}
		}
		simplified, simpChanged := simplifyInListExpr(rewritten)
		if simpChanged {
			return simplified, true
		}
		return rewritten, exprChanged || valuesChanged

	case *binder.BoundCastExpr:
		inner, innerChanged := rewriteExpr(node.Expr, ctx)
		if !innerChanged {
			return expr, false
		}
		return &binder.BoundCastExpr{Expr: inner, TargetType: node.TargetType}, true

	case *binder.BoundCaseExpr:
		changed := false
		operand, opChanged := rewriteExpr(node.Operand, ctx)
		if opChanged {
			changed = true
		}
		whens := make([]*binder.BoundWhenClause, len(node.Whens))
		for i, when := range node.Whens {
			cond, condChanged := rewriteExpr(when.Condition, ctx)
			result, resChanged := rewriteExpr(when.Result, ctx)
			if condChanged || resChanged {
				changed = true
			}
			whens[i] = &binder.BoundWhenClause{Condition: cond, Result: result}
		}
		elseExpr, elseChanged := rewriteExpr(node.Else, ctx)
		if elseChanged {
			changed = true
		}
		if !changed {
			return expr, false
		}
		return &binder.BoundCaseExpr{
			Operand: operand,
			Whens:   whens,
			Else:    elseExpr,
			ResType: node.ResType,
		}, true

	case *binder.BoundFunctionCall:
		args, argsChanged := rewriteExprSlice(node.Args, ctx)
		orderByChanged := false
		orderBy := make([]binder.BoundOrderByExpr, len(node.OrderBy))
		for i, ob := range node.OrderBy {
			rewritten, changed := rewriteExpr(ob.Expr, ctx)
			if changed {
				orderByChanged = true
			}
			orderBy[i] = binder.BoundOrderByExpr{Expr: rewritten, Desc: ob.Desc}
		}
		if !argsChanged && !orderByChanged {
			return expr, false
		}
		return &binder.BoundFunctionCall{
			Name:     node.Name,
			Args:     args,
			Distinct: node.Distinct,
			Star:     node.Star,
			OrderBy:  orderBy,
			ResType:  node.ResType,
		}, true

	case *binder.BoundScalarUDF:
		args, argsChanged := rewriteExprSlice(node.Args, ctx)
		if !argsChanged {
			return expr, false
		}
		return &binder.BoundScalarUDF{
			Name:    node.Name,
			Args:    args,
			ResType: node.ResType,
			UDFInfo: node.UDFInfo,
			ArgInfo: node.ArgInfo,
			BindCtx: node.BindCtx,
		}, true

	case *binder.BoundArrayExpr:
		elems, elemsChanged := rewriteExprSlice(node.Elements, ctx)
		if !elemsChanged {
			return expr, false
		}
		return &binder.BoundArrayExpr{Elements: elems, ElemType: node.ElemType}, true

	default:
		return expr, false
	}
}

func simplifyBinaryExpr(expr *binder.BoundBinaryExpr, ctx *RewriteContext) (binder.BoundExpr, bool) {
	if expr == nil {
		return expr, false
	}

	if ctx == nil || (!ctx.Config.ExpressionRewrites && !ctx.Config.ConstantFolding) {
		return expr, false
	}

	leftLit, leftOK := asLiteral(expr.Left)
	rightLit, rightOK := asLiteral(expr.Right)

	if leftOK && rightOK {
		if folded, ok := foldBinaryLiteral(expr.Op, leftLit, rightLit, expr.ResType); ok {
			return folded, true
		}
	}

	if expr.Op == parser.OpAnd || expr.Op == parser.OpOr {
		if simplified, ok := simplifyBoolean(expr.Op, expr.Left, expr.Right); ok {
			return simplified, true
		}
	}

	if simplified, ok := simplifyArithmeticIdentity(expr.Op, expr.Left, expr.Right, expr.ResType); ok {
		return simplified, true
	}

	return expr, false
}

func simplifyUnaryExpr(expr *binder.BoundUnaryExpr, ctx *RewriteContext) (binder.BoundExpr, bool) {
	if expr == nil {
		return expr, false
	}
	if ctx == nil || (!ctx.Config.ExpressionRewrites && !ctx.Config.ConstantFolding) {
		return expr, false
	}

	if expr.Op == parser.OpNot {
		if lit, ok := asLiteral(expr.Expr); ok {
			if lit.Value == nil {
				return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
			}
			if b, ok := lit.Value.(bool); ok {
				return &binder.BoundLiteral{Value: !b, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
		if bin, ok := expr.Expr.(*binder.BoundBinaryExpr); ok {
			switch bin.Op {
			case parser.OpAnd:
				left := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Left, ResType: dukdb.TYPE_BOOLEAN}
				right := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Right, ResType: dukdb.TYPE_BOOLEAN}
				return &binder.BoundBinaryExpr{Left: left, Op: parser.OpOr, Right: right, ResType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpOr:
				left := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Left, ResType: dukdb.TYPE_BOOLEAN}
				right := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: bin.Right, ResType: dukdb.TYPE_BOOLEAN}
				return &binder.BoundBinaryExpr{Left: left, Op: parser.OpAnd, Right: right, ResType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	return expr, false
}

func simplifyInListExpr(expr *binder.BoundInListExpr) (binder.BoundExpr, bool) {
	if expr == nil || len(expr.Values) != 1 {
		return expr, false
	}
	op := parser.OpEq
	if expr.Not {
		op = parser.OpNe
	}
	return &binder.BoundBinaryExpr{Left: expr.Expr, Op: op, Right: expr.Values[0], ResType: dukdb.TYPE_BOOLEAN}, true
}

func isComparisonOp(op parser.BinaryOp) bool {
	switch op {
	case parser.OpEq, parser.OpNe, parser.OpLt, parser.OpLe, parser.OpGt, parser.OpGe:
		return true
	default:
		return false
	}
}

func simplifyArithmeticIdentity(op parser.BinaryOp, left, right binder.BoundExpr, resType dukdb.Type) (binder.BoundExpr, bool) {
	if lit, ok := asLiteral(left); ok {
		if isZeroLiteral(lit) {
			switch op {
			case parser.OpAdd:
				return right, true
			case parser.OpMul:
				return &binder.BoundLiteral{Value: 0, ValType: resType}, true
			}
		}
		if isOneLiteral(lit) && op == parser.OpMul {
			return right, true
		}
	}

	if lit, ok := asLiteral(right); ok {
		if isZeroLiteral(lit) {
			switch op {
			case parser.OpAdd, parser.OpSub:
				return left, true
			case parser.OpMul:
				return &binder.BoundLiteral{Value: 0, ValType: resType}, true
			}
		}
		if isOneLiteral(lit) {
			switch op {
			case parser.OpMul, parser.OpDiv:
				return left, true
			}
		}
	}

	return nil, false
}

func simplifyBoolean(op parser.BinaryOp, left, right binder.BoundExpr) (binder.BoundExpr, bool) {
	leftTri, leftOK := triValue(left)
	rightTri, rightOK := triValue(right)

	if leftOK && rightOK {
		result := evalTriBool(op, leftTri, rightTri)
		if !result.valid {
			return &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL}, true
		}
		return &binder.BoundLiteral{Value: result.value, ValType: dukdb.TYPE_BOOLEAN}, true
	}

	if leftOK {
		if op == parser.OpAnd {
			if leftTri.valid && !leftTri.value {
				return &binder.BoundLiteral{Value: false, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if leftTri.valid && leftTri.value {
				return right, true
			}
		}
		if op == parser.OpOr {
			if leftTri.valid && leftTri.value {
				return &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if leftTri.valid && !leftTri.value {
				return right, true
			}
		}
	}

	if rightOK {
		if op == parser.OpAnd {
			if rightTri.valid && !rightTri.value {
				return &binder.BoundLiteral{Value: false, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if rightTri.valid && rightTri.value {
				return left, true
			}
		}
		if op == parser.OpOr {
			if rightTri.valid && rightTri.value {
				return &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN}, true
			}
			if rightTri.valid && !rightTri.value {
				return left, true
			}
		}
	}

	return nil, false
}

func foldBinaryLiteral(op parser.BinaryOp, left, right *binder.BoundLiteral, resType dukdb.Type) (binder.BoundExpr, bool) {
	if left.Value == nil || right.Value == nil {
		return nil, false
	}
	if lnum, lok := toNumber(left.Value); lok {
		if rnum, rok := toNumber(right.Value); rok {
			switch op {
			case parser.OpAdd:
				return &binder.BoundLiteral{Value: lnum + rnum, ValType: resType}, true
			case parser.OpSub:
				return &binder.BoundLiteral{Value: lnum - rnum, ValType: resType}, true
			case parser.OpMul:
				return &binder.BoundLiteral{Value: lnum * rnum, ValType: resType}, true
			case parser.OpDiv:
				if rnum == 0 {
					return nil, false
				}
				return &binder.BoundLiteral{Value: lnum / rnum, ValType: resType}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lnum == rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lnum != rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpLt:
				return &binder.BoundLiteral{Value: lnum < rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpLe:
				return &binder.BoundLiteral{Value: lnum <= rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpGt:
				return &binder.BoundLiteral{Value: lnum > rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpGe:
				return &binder.BoundLiteral{Value: lnum >= rnum, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	if lbool, ok := left.Value.(bool); ok {
		if rbool, ok := right.Value.(bool); ok {
			switch op {
			case parser.OpAnd:
				return &binder.BoundLiteral{Value: lbool && rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpOr:
				return &binder.BoundLiteral{Value: lbool || rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lbool == rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lbool != rbool, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	if lstr, ok := left.Value.(string); ok {
		if rstr, ok := right.Value.(string); ok {
			switch op {
			case parser.OpConcat:
				return &binder.BoundLiteral{Value: lstr + rstr, ValType: dukdb.TYPE_VARCHAR}, true
			case parser.OpEq:
				return &binder.BoundLiteral{Value: lstr == rstr, ValType: dukdb.TYPE_BOOLEAN}, true
			case parser.OpNe:
				return &binder.BoundLiteral{Value: lstr != rstr, ValType: dukdb.TYPE_BOOLEAN}, true
			}
		}
	}

	return nil, false
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

type triState struct {
	valid bool
	value bool
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

func exprEquivalent(left, right binder.BoundExpr) (bool, bool) {
	switch l := left.(type) {
	case *binder.BoundColumnRef:
		if r, ok := right.(*binder.BoundColumnRef); ok {
			return l.Table == r.Table && l.Column == r.Column, true
		}
	case *binder.BoundLiteral:
		if r, ok := right.(*binder.BoundLiteral); ok {
			return fmt.Sprintf("%v", l.Value) == fmt.Sprintf("%v", r.Value), true
		}
	}
	return false, false
}

func rewriteWindowExpr(expr *binder.BoundWindowExpr, ctx *RewriteContext) (*binder.BoundWindowExpr, bool) {
	if expr == nil {
		return nil, false
	}
	changed := false
	args, argsChanged := rewriteExprSlice(expr.Args, ctx)
	if argsChanged {
		changed = true
	}
	partition, partChanged := rewriteExprSlice(expr.PartitionBy, ctx)
	if partChanged {
		changed = true
	}
	orderBy := make([]binder.BoundWindowOrder, len(expr.OrderBy))
	orderChanged := false
	for i, ob := range expr.OrderBy {
		rewritten, changedExpr := rewriteExpr(ob.Expr, ctx)
		if changedExpr {
			orderChanged = true
		}
		orderBy[i] = binder.BoundWindowOrder{Expr: rewritten, Desc: ob.Desc, NullsFirst: ob.NullsFirst}
	}
	if orderChanged {
		changed = true
	}
	filterExpr, filterChanged := rewriteExpr(expr.Filter, ctx)
	if filterChanged {
		changed = true
	}
	if !changed {
		return expr, false
	}
	return &binder.BoundWindowExpr{
		FunctionName: expr.FunctionName,
		FunctionType: expr.FunctionType,
		Args:         args,
		PartitionBy:  partition,
		OrderBy:      orderBy,
		Frame:        expr.Frame,
		ResType:      expr.ResType,
		IgnoreNulls:  expr.IgnoreNulls,
		Filter:       filterExpr,
		Distinct:     expr.Distinct,
		ResultIndex:  expr.ResultIndex,
		Alias:        expr.Alias,
	}, true
}

func pushdownFilters(plan LogicalPlan) (LogicalPlan, bool) {
	if plan == nil {
		return nil, false
	}
	switch node := plan.(type) {
	case *LogicalFilter:
		child, childChanged := pushdownFilters(node.Child)
		if filterChild, ok := child.(*LogicalFilter); ok {
			combined := combineAND(node.Condition, filterChild.Condition)
			return &LogicalFilter{Child: filterChild.Child, Condition: combined}, true
		}
		if join, ok := child.(*LogicalJoin); ok {
			if join.JoinType == JoinTypeInner || join.JoinType == JoinTypeCross {
				return pushFilterIntoJoin(node.Condition, join)
			}
		}
		if childChanged {
			return &LogicalFilter{Child: child, Condition: node.Condition}, true
		}
		return plan, false

	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			rewritten, childChanged := pushdownFilters(child)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return replaceChildren(plan, newChildren), true
	}
}

func pushFilterIntoJoin(cond binder.BoundExpr, join *LogicalJoin) (LogicalPlan, bool) {
	conjuncts := splitAndConjuncts(cond)
	leftTables := planTableSet(join.Left)
	rightTables := planTableSet(join.Right)
	var leftFilters []binder.BoundExpr
	var rightFilters []binder.BoundExpr
	var keep []binder.BoundExpr

	for _, part := range conjuncts {
		tables, unknown := exprTables(part)
		if unknown || len(tables) == 0 {
			keep = append(keep, part)
			continue
		}
		if isSubset(tables, leftTables) {
			leftFilters = append(leftFilters, part)
		} else if isSubset(tables, rightTables) {
			rightFilters = append(rightFilters, part)
		} else {
			keep = append(keep, part)
		}
	}

	left := join.Left
	right := join.Right
	if len(leftFilters) > 0 {
		left = addFilter(left, combineANDList(leftFilters))
	}
	if len(rightFilters) > 0 {
		right = addFilter(right, combineANDList(rightFilters))
	}

	newJoin := &LogicalJoin{Left: left, Right: right, JoinType: join.JoinType, Condition: join.Condition}
	if len(keep) == 0 {
		return newJoin, true
	}
	return &LogicalFilter{Child: newJoin, Condition: combineANDList(keep)}, true
}

func addFilter(plan LogicalPlan, cond binder.BoundExpr) LogicalPlan {
	if cond == nil {
		return plan
	}
	if filter, ok := plan.(*LogicalFilter); ok {
		return &LogicalFilter{Child: filter.Child, Condition: combineAND(filter.Condition, cond)}
	}
	return &LogicalFilter{Child: plan, Condition: cond}
}

func splitAndConjuncts(expr binder.BoundExpr) []binder.BoundExpr {
	if bin, ok := expr.(*binder.BoundBinaryExpr); ok && bin.Op == parser.OpAnd {
		left := splitAndConjuncts(bin.Left)
		right := splitAndConjuncts(bin.Right)
		return append(left, right...)
	}
	return []binder.BoundExpr{expr}
}

func combineAND(left, right binder.BoundExpr) binder.BoundExpr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &binder.BoundBinaryExpr{Left: left, Op: parser.OpAnd, Right: right, ResType: dukdb.TYPE_BOOLEAN}
}

func combineANDList(exprs []binder.BoundExpr) binder.BoundExpr {
	if len(exprs) == 0 {
		return nil
	}
	result := exprs[0]
	for i := 1; i < len(exprs); i++ {
		result = combineAND(result, exprs[i])
	}
	return result
}

func exprTables(expr binder.BoundExpr) (map[string]struct{}, bool) {
	tables := make(map[string]struct{})
	unknown := false
	walkExpr(expr, func(node binder.BoundExpr) {
		if col, ok := node.(*binder.BoundColumnRef); ok {
			if col.Table == "" {
				unknown = true
				return
			}
			tables[col.Table] = struct{}{}
		}
	})
	return tables, unknown
}

func walkExpr(expr binder.BoundExpr, visit func(binder.BoundExpr)) {
	if expr == nil {
		return
	}
	visit(expr)
	switch node := expr.(type) {
	case *binder.BoundBinaryExpr:
		walkExpr(node.Left, visit)
		walkExpr(node.Right, visit)
	case *binder.BoundUnaryExpr:
		walkExpr(node.Expr, visit)
	case *binder.BoundBetweenExpr:
		walkExpr(node.Expr, visit)
		walkExpr(node.Low, visit)
		walkExpr(node.High, visit)
	case *binder.BoundInListExpr:
		walkExpr(node.Expr, visit)
		for _, val := range node.Values {
			walkExpr(val, visit)
		}
	case *binder.BoundCastExpr:
		walkExpr(node.Expr, visit)
	case *binder.BoundCaseExpr:
		walkExpr(node.Operand, visit)
		for _, when := range node.Whens {
			walkExpr(when.Condition, visit)
			walkExpr(when.Result, visit)
		}
		walkExpr(node.Else, visit)
	case *binder.BoundFunctionCall:
		for _, arg := range node.Args {
			walkExpr(arg, visit)
		}
		for _, ob := range node.OrderBy {
			walkExpr(ob.Expr, visit)
		}
	case *binder.BoundScalarUDF:
		for _, arg := range node.Args {
			walkExpr(arg, visit)
		}
	case *binder.BoundArrayExpr:
		for _, elem := range node.Elements {
			walkExpr(elem, visit)
		}
	}
}

func planTableSet(plan LogicalPlan) map[string]struct{} {
	tables := make(map[string]struct{})
	for _, col := range plan.OutputColumns() {
		if col.Table != "" {
			tables[col.Table] = struct{}{}
		}
	}
	return tables
}

func isSubset(subset, set map[string]struct{}) bool {
	for key := range subset {
		if _, ok := set[key]; !ok {
			return false
		}
	}
	return true
}

func reorderJoins(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	if plan == nil {
		return nil, false
	}
	switch node := plan.(type) {
	case *LogicalJoin:
		left, leftChanged := reorderJoins(node.Left, ctx)
		right, rightChanged := reorderJoins(node.Right, ctx)
		changed := leftChanged || rightChanged

		newJoin := &LogicalJoin{Left: left, Right: right, JoinType: node.JoinType, Condition: node.Condition}
		if node.JoinType != JoinTypeInner && node.JoinType != JoinTypeCross {
			return newJoin, changed
		}
		if ctx == nil || ctx.Estimator == nil {
			return newJoin, changed
		}
		leftCost := ctx.Estimator.Estimate(left)
		rightCost := ctx.Estimator.Estimate(right)
		if leftCost > rightCost*1.1 {
			return &LogicalJoin{Left: right, Right: left, JoinType: node.JoinType, Condition: node.Condition}, true
		}
		return newJoin, changed

	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			rewritten, childChanged := reorderJoins(child, ctx)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return replaceChildren(plan, newChildren), true
	}
}

func unnestSubqueries(plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	if plan == nil {
		return nil, false
	}
	switch node := plan.(type) {
	case *LogicalFilter:
		child, childChanged := unnestSubqueries(node.Child, ctx)
		conjuncts := splitAndConjuncts(node.Condition)
		var remaining []binder.BoundExpr
		changed := childChanged
		currentPlan := child

		for _, conjunct := range conjuncts {
			rewritten, ok := unnestSubqueryExpr(conjunct, currentPlan, ctx)
			if ok {
				currentPlan = rewritten
				changed = true
				continue
			}
			remaining = append(remaining, conjunct)
		}

		if !changed {
			return plan, false
		}
		if len(remaining) == 0 {
			return currentPlan, true
		}
		return &LogicalFilter{Child: currentPlan, Condition: combineANDList(remaining)}, true

	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, false
		}
		changed := false
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			rewritten, childChanged := unnestSubqueries(child, ctx)
			if childChanged {
				changed = true
			}
			newChildren[i] = rewritten
		}
		if !changed {
			return plan, false
		}
		return replaceChildren(plan, newChildren), true
	}
}

func unnestSubqueryExpr(expr binder.BoundExpr, plan LogicalPlan, ctx *RewriteContext) (LogicalPlan, bool) {
	if ctx == nil || ctx.PlanSubquery == nil {
		return nil, false
	}

	switch sub := expr.(type) {
	case *binder.BoundExistsExpr:
		if isCorrelatedSubquery(sub.Subquery) {
			return nil, false
		}
		right, err := ctx.PlanSubquery(sub.Subquery)
		if err != nil {
			return nil, false
		}
		joinType := JoinTypeSemi
		if sub.Not {
			joinType = JoinTypeAnti
		}
		return &LogicalJoin{Left: plan, Right: right, JoinType: joinType}, true

	case *binder.BoundInSubqueryExpr:
		if isCorrelatedSubquery(sub.Subquery) {
			return nil, false
		}
		right, err := ctx.PlanSubquery(sub.Subquery)
		if err != nil {
			return nil, false
		}
		rightCols := right.OutputColumns()
		if len(rightCols) == 0 {
			return nil, false
		}
		rightRef := &binder.BoundColumnRef{
			Table:   rightCols[0].Table,
			Column:  rightCols[0].Column,
			ColType: rightCols[0].Type,
		}
		joinCond := &binder.BoundBinaryExpr{Left: sub.Expr, Op: parser.OpEq, Right: rightRef, ResType: dukdb.TYPE_BOOLEAN}
		joinType := JoinTypeSemi
		if sub.Not {
			joinType = JoinTypeAnti
		}
		return &LogicalJoin{Left: plan, Right: right, JoinType: joinType, Condition: joinCond}, true
	}
	return nil, false
}

func isCorrelatedSubquery(stmt *binder.BoundSelectStmt) bool {
	if stmt == nil {
		return false
	}
	allowed := make(map[string]struct{})
	for _, ref := range stmt.From {
		addTableAlias(allowed, ref)
	}
	for _, join := range stmt.Joins {
		addTableAlias(allowed, join.Table)
	}

	correlated := false
	for _, col := range stmt.Columns {
		walkExpr(col.Expr, func(expr binder.BoundExpr) {
			if ref, ok := expr.(*binder.BoundColumnRef); ok {
				if ref.Table != "" {
					if _, ok := allowed[ref.Table]; !ok {
						correlated = true
					}
				}
			}
		})
	}
	walkExpr(stmt.Where, func(expr binder.BoundExpr) {
		if ref, ok := expr.(*binder.BoundColumnRef); ok {
			if ref.Table != "" {
				if _, ok := allowed[ref.Table]; !ok {
					correlated = true
				}
			}
		}
	})
	return correlated
}

func addTableAlias(set map[string]struct{}, ref *binder.BoundTableRef) {
	if ref == nil {
		return
	}
	alias := ref.Alias
	if alias == "" {
		alias = ref.TableName
	}
	if alias != "" {
		set[alias] = struct{}{}
	}
}

func replaceChildren(plan LogicalPlan, children []LogicalPlan) LogicalPlan {
	switch node := plan.(type) {
	case *LogicalFilter:
		return &LogicalFilter{Child: children[0], Condition: node.Condition}
	case *LogicalProject:
		return &LogicalProject{Child: children[0], Expressions: node.Expressions, Aliases: node.Aliases}
	case *LogicalJoin:
		return &LogicalJoin{Left: children[0], Right: children[1], JoinType: node.JoinType, Condition: node.Condition}
	case *LogicalLateralJoin:
		return &LogicalLateralJoin{Left: children[0], Right: children[1], JoinType: node.JoinType, Condition: node.Condition}
	case *LogicalAggregate:
		return &LogicalAggregate{Child: children[0], GroupBy: node.GroupBy, Aggregates: node.Aggregates, Aliases: node.Aliases, GroupingSets: node.GroupingSets, GroupingCalls: node.GroupingCalls}
	case *LogicalSort:
		return &LogicalSort{Child: children[0], OrderBy: node.OrderBy}
	case *LogicalLimit:
		return &LogicalLimit{Child: children[0], Limit: node.Limit, Offset: node.Offset, LimitExpr: node.LimitExpr, OffsetExpr: node.OffsetExpr}
	case *LogicalDistinct:
		return &LogicalDistinct{Child: children[0]}
	case *LogicalDistinctOn:
		return &LogicalDistinctOn{Child: children[0], DistinctOn: node.DistinctOn, OrderBy: node.OrderBy}
	case *LogicalWindow:
		return &LogicalWindow{Child: children[0], WindowExprs: node.WindowExprs}
	case *LogicalSample:
		return &LogicalSample{Child: children[0], Sample: node.Sample}
	case *LogicalSetOp:
		return &LogicalSetOp{Left: children[0], Right: children[1], OpType: node.OpType}
	case *LogicalCopyTo:
		return &LogicalCopyTo{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Columns: node.Columns, FilePath: node.FilePath, Options: node.Options, Source: children[0]}
	case *LogicalInsert:
		return &LogicalInsert{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Columns: node.Columns, Values: node.Values, Source: children[0], Returning: node.Returning}
	case *LogicalUpdate:
		return &LogicalUpdate{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Set: node.Set, Source: children[0], Returning: node.Returning}
	case *LogicalDelete:
		return &LogicalDelete{Schema: node.Schema, Table: node.Table, TableDef: node.TableDef, Source: children[0], Returning: node.Returning}
	case *LogicalMerge:
		return &LogicalMerge{Schema: node.Schema, TargetTable: node.TargetTable, TargetTableDef: node.TargetTableDef, TargetAlias: node.TargetAlias, SourcePlan: children[0], OnCondition: node.OnCondition, WhenMatched: node.WhenMatched, WhenNotMatched: node.WhenNotMatched, WhenNotMatchedBySource: node.WhenNotMatchedBySource, Returning: node.Returning}
	case *LogicalCTEScan:
		if len(children) > 0 {
			return &LogicalCTEScan{CTEName: node.CTEName, Alias: node.Alias, Columns: node.Columns, CTEPlan: children[0], IsRecursive: node.IsRecursive}
		}
	case *LogicalRecursiveCTE:
		if len(children) >= 2 {
			return &LogicalRecursiveCTE{CTEName: node.CTEName, BasePlan: children[0], RecursivePlan: children[1], Columns: node.Columns}
		}
	}
	return plan
}
