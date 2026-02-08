package rewrite

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
)

// Plan is an opaque logical plan representation.
type Plan = any

// Column represents a column in a plan output.
type Column struct {
	Table     string
	Column    string
	Type      dukdb.Type
	TableIdx  int
	ColumnIdx int
}

// JoinType represents join type semantics for rewrites.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
	JoinTypeSemi
	JoinTypeAnti
)

// ExprRewriter rewrites a bound expression.
type ExprRewriter func(binder.BoundExpr) (binder.BoundExpr, bool)

// Adapter bridges rewrite rules to concrete plan implementations.
type Adapter interface {
	Children(plan Plan) []Plan
	ReplaceChildren(plan Plan, children []Plan) Plan
	OutputColumns(plan Plan) []Column
	RewriteExpressions(plan Plan, rewriter ExprRewriter) (Plan, bool)

	AsFilter(plan Plan) (Plan, binder.BoundExpr, bool)
	NewFilter(child Plan, cond binder.BoundExpr) Plan

	AsProject(plan Plan) (Plan, []binder.BoundExpr, []string, bool)
	NewProject(child Plan, exprs []binder.BoundExpr, aliases []string) Plan

	AsJoin(plan Plan) (Plan, Plan, JoinType, binder.BoundExpr, bool)
	NewJoin(left, right Plan, joinType JoinType, cond binder.BoundExpr) Plan

	AsDistinct(plan Plan) (Plan, bool)
	NewDistinct(child Plan) Plan

	AsScan(plan Plan) (schema, table, alias string, projections []int, columns []Column, ok bool)
	WithScanProjections(plan Plan, projections []int) Plan
}

// CostEstimator estimates relative cost for rewrite pruning.
type CostEstimator interface {
	Estimate(plan Plan) float64
}

// Rule applies a logical transformation.
type Rule interface {
	Name() string
	Apply(plan Plan, ctx *Context) (Plan, bool)
}
