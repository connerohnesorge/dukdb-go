package rules

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
	"github.com/stretchr/testify/require"
)

type mockScan struct {
	alias       string
	columns     []rewrite.Column
	projections []int
}

type mockFilter struct {
	child rewrite.Plan
	cond  binder.BoundExpr
}

type mockProject struct {
	child   rewrite.Plan
	exprs   []binder.BoundExpr
	aliases []string
}

type mockJoin struct {
	left     rewrite.Plan
	right    rewrite.Plan
	joinType rewrite.JoinType
	cond     binder.BoundExpr
}

type mockDistinct struct {
	child rewrite.Plan
}

type mockAdapter struct{}

func (mockAdapter) Children(plan rewrite.Plan) []rewrite.Plan {
	switch node := plan.(type) {
	case *mockFilter:
		return []rewrite.Plan{node.child}
	case *mockProject:
		return []rewrite.Plan{node.child}
	case *mockJoin:
		return []rewrite.Plan{node.left, node.right}
	case *mockDistinct:
		return []rewrite.Plan{node.child}
	default:
		return nil
	}
}

func (mockAdapter) ReplaceChildren(plan rewrite.Plan, children []rewrite.Plan) rewrite.Plan {
	switch node := plan.(type) {
	case *mockFilter:
		return &mockFilter{child: children[0], cond: node.cond}
	case *mockProject:
		return &mockProject{child: children[0], exprs: node.exprs, aliases: node.aliases}
	case *mockJoin:
		return &mockJoin{left: children[0], right: children[1], joinType: node.joinType, cond: node.cond}
	case *mockDistinct:
		return &mockDistinct{child: children[0]}
	default:
		return plan
	}
}

func (mockAdapter) OutputColumns(plan rewrite.Plan) []rewrite.Column {
	switch node := plan.(type) {
	case *mockScan:
		return scanOutputColumns(node)
	case *mockFilter:
		return mockAdapter{}.OutputColumns(node.child)
	case *mockProject:
		return mockAdapter{}.OutputColumns(node.child)
	case *mockJoin:
		left := mockAdapter{}.OutputColumns(node.left)
		right := mockAdapter{}.OutputColumns(node.right)
		out := make([]rewrite.Column, 0, len(left)+len(right))
		out = append(out, left...)
		out = append(out, right...)
		return out
	case *mockDistinct:
		return mockAdapter{}.OutputColumns(node.child)
	default:
		return nil
	}
}

func (mockAdapter) RewriteExpressions(plan rewrite.Plan, rewriter rewrite.ExprRewriter) (rewrite.Plan, bool) {
	switch node := plan.(type) {
	case *mockFilter:
		child, childChanged := mockAdapter{}.RewriteExpressions(node.child, rewriter)
		cond, condChanged := rewrite.RewriteExpr(node.cond, rewriter)
		if !childChanged && !condChanged {
			return plan, false
		}
		return &mockFilter{child: child, cond: cond}, true
	case *mockProject:
		child, childChanged := mockAdapter{}.RewriteExpressions(node.child, rewriter)
		exprs, exprChanged := rewriteExprSlice(node.exprs, rewriter)
		if !childChanged && !exprChanged {
			return plan, false
		}
		return &mockProject{child: child, exprs: exprs, aliases: node.aliases}, true
	case *mockJoin:
		left, leftChanged := mockAdapter{}.RewriteExpressions(node.left, rewriter)
		right, rightChanged := mockAdapter{}.RewriteExpressions(node.right, rewriter)
		cond, condChanged := rewrite.RewriteExpr(node.cond, rewriter)
		if !leftChanged && !rightChanged && !condChanged {
			return plan, false
		}
		return &mockJoin{left: left, right: right, joinType: node.joinType, cond: cond}, true
	default:
		return plan, false
	}
}

func (mockAdapter) AsFilter(plan rewrite.Plan) (rewrite.Plan, binder.BoundExpr, bool) {
	node, ok := plan.(*mockFilter)
	if !ok {
		return nil, nil, false
	}
	return node.child, node.cond, true
}

func (mockAdapter) NewFilter(child rewrite.Plan, cond binder.BoundExpr) rewrite.Plan {
	return &mockFilter{child: child, cond: cond}
}

func (mockAdapter) AsProject(plan rewrite.Plan) (rewrite.Plan, []binder.BoundExpr, []string, bool) {
	node, ok := plan.(*mockProject)
	if !ok {
		return nil, nil, nil, false
	}
	return node.child, node.exprs, node.aliases, true
}

func (mockAdapter) NewProject(child rewrite.Plan, exprs []binder.BoundExpr, aliases []string) rewrite.Plan {
	return &mockProject{child: child, exprs: exprs, aliases: aliases}
}

func (mockAdapter) AsJoin(plan rewrite.Plan) (rewrite.Plan, rewrite.Plan, rewrite.JoinType, binder.BoundExpr, bool) {
	node, ok := plan.(*mockJoin)
	if !ok {
		return nil, nil, 0, nil, false
	}
	return node.left, node.right, node.joinType, node.cond, true
}

func (mockAdapter) NewJoin(left, right rewrite.Plan, joinType rewrite.JoinType, cond binder.BoundExpr) rewrite.Plan {
	return &mockJoin{left: left, right: right, joinType: joinType, cond: cond}
}

func (mockAdapter) AsDistinct(plan rewrite.Plan) (rewrite.Plan, bool) {
	node, ok := plan.(*mockDistinct)
	if !ok {
		return nil, false
	}
	return node.child, true
}

func (mockAdapter) NewDistinct(child rewrite.Plan) rewrite.Plan {
	return &mockDistinct{child: child}
}

func (mockAdapter) AsScan(plan rewrite.Plan) (string, string, string, []int, []rewrite.Column, bool) {
	node, ok := plan.(*mockScan)
	if !ok {
		return "", "", "", nil, nil, false
	}
	return "", "", node.alias, node.projections, scanOutputColumns(node), true
}

func (mockAdapter) WithScanProjections(plan rewrite.Plan, projections []int) rewrite.Plan {
	node, ok := plan.(*mockScan)
	if !ok {
		return plan
	}
	return &mockScan{alias: node.alias, columns: node.columns, projections: append([]int(nil), projections...)}
}

func scanOutputColumns(scan *mockScan) []rewrite.Column {
	if scan == nil {
		return nil
	}
	if len(scan.projections) == 0 {
		return scan.columns
	}
	out := make([]rewrite.Column, 0, len(scan.projections))
	for _, idx := range scan.projections {
		for _, col := range scan.columns {
			if col.ColumnIdx == idx {
				out = append(out, col)
				break
			}
		}
	}
	return out
}

func TestConstantFoldingRule(t *testing.T) {
	expr := &binder.BoundBinaryExpr{
		Left: &binder.BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_BIGINT},
		Op:   parser.OpAdd,
		Right: &binder.BoundBinaryExpr{
			Left:    &binder.BoundLiteral{Value: int64(2), ValType: dukdb.TYPE_BIGINT},
			Op:      parser.OpMul,
			Right:   &binder.BoundLiteral{Value: int64(3), ValType: dukdb.TYPE_BIGINT},
			ResType: dukdb.TYPE_BIGINT,
		},
		ResType: dukdb.TYPE_BIGINT,
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := ConstantFoldingRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	lit, ok := filter.cond.(*binder.BoundLiteral)
	require.True(t, ok)
	require.Equal(t, int64(7), lit.Value)
}

func TestBooleanSimplificationRule(t *testing.T) {
	expr := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER},
		Op:      parser.OpAnd,
		Right:   &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN},
		ResType: dukdb.TYPE_BOOLEAN,
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := BooleanSimplificationRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	_, ok = filter.cond.(*binder.BoundColumnRef)
	require.True(t, ok)
}

func TestNullSimplificationRule(t *testing.T) {
	expr := &binder.BoundBinaryExpr{
		Left:    &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN},
		Op:      parser.OpAnd,
		Right:   &binder.BoundLiteral{Value: nil, ValType: dukdb.TYPE_SQLNULL},
		ResType: dukdb.TYPE_BOOLEAN,
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := NullSimplificationRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	lit, ok := filter.cond.(*binder.BoundLiteral)
	require.True(t, ok)
	require.Nil(t, lit.Value)
}

func TestComparisonSimplificationRule(t *testing.T) {
	expr := &binder.BoundBinaryExpr{
		Left:    &binder.BoundLiteral{Value: int64(3), ValType: dukdb.TYPE_BIGINT},
		Op:      parser.OpEq,
		Right:   &binder.BoundLiteral{Value: int64(3), ValType: dukdb.TYPE_BIGINT},
		ResType: dukdb.TYPE_BOOLEAN,
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := ComparisonSimplificationRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	lit, ok := filter.cond.(*binder.BoundLiteral)
	require.True(t, ok)
	require.Equal(t, true, lit.Value)
}

func TestArithmeticIdentityRule(t *testing.T) {
	expr := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER},
		Op:      parser.OpAdd,
		Right:   &binder.BoundLiteral{Value: int64(0), ValType: dukdb.TYPE_BIGINT},
		ResType: dukdb.TYPE_INTEGER,
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := ArithmeticIdentityRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	_, ok = filter.cond.(*binder.BoundColumnRef)
	require.True(t, ok)
}

func TestDeMorganRule(t *testing.T) {
	inner := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER},
		Op:      parser.OpAnd,
		Right:   &binder.BoundColumnRef{Table: "t1", Column: "b", ColType: dukdb.TYPE_INTEGER},
		ResType: dukdb.TYPE_BOOLEAN,
	}
	expr := &binder.BoundUnaryExpr{Op: parser.OpNot, Expr: inner, ResType: dukdb.TYPE_BOOLEAN}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := DeMorganRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	bin, ok := filter.cond.(*binder.BoundBinaryExpr)
	require.True(t, ok)
	require.Equal(t, parser.OpOr, bin.Op)
}

func TestInListSimplificationRule(t *testing.T) {
	expr := &binder.BoundInListExpr{
		Expr:   &binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER},
		Values: []binder.BoundExpr{&binder.BoundLiteral{Value: int64(5), ValType: dukdb.TYPE_BIGINT}},
	}
	plan := &mockFilter{child: &mockScan{alias: "t1"}, cond: expr}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := InListSimplificationRule{}.Apply(plan, ctx)
	require.True(t, ok)
	filter := rewritten.(*mockFilter)
	bin, ok := filter.cond.(*binder.BoundBinaryExpr)
	require.True(t, ok)
	require.Equal(t, parser.OpEq, bin.Op)
}

func TestFilterPushdownRule(t *testing.T) {
	left := &mockScan{alias: "t1", columns: []rewrite.Column{{Table: "t1", Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0}}}
	right := &mockScan{alias: "t2", columns: []rewrite.Column{{Table: "t2", Column: "b", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0}}}
	join := &mockJoin{left: left, right: right, joinType: rewrite.JoinTypeInner}
	cond := &binder.BoundBinaryExpr{
		Left: &binder.BoundBinaryExpr{
			Left:    &binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpGt,
			Right:   &binder.BoundLiteral{Value: int64(5), ValType: dukdb.TYPE_BIGINT},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		Op: parser.OpAnd,
		Right: &binder.BoundBinaryExpr{
			Left:    &binder.BoundColumnRef{Table: "t2", Column: "b", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpLt,
			Right:   &binder.BoundLiteral{Value: int64(10), ValType: dukdb.TYPE_BIGINT},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}
	plan := &mockFilter{child: join, cond: cond}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := FilterPushdownRule{}.Apply(plan, ctx)
	require.True(t, ok)
	newJoin := rewritten.(*mockJoin)
	_, leftIsFilter := newJoin.left.(*mockFilter)
	_, rightIsFilter := newJoin.right.(*mockFilter)
	require.True(t, leftIsFilter)
	require.True(t, rightIsFilter)
}

func TestProjectionPushdownRule(t *testing.T) {
	scan := &mockScan{alias: "t1", columns: []rewrite.Column{
		{Table: "t1", Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "t1", Column: "b", Type: dukdb.TYPE_INTEGER, ColumnIdx: 1},
	}}
	project := &mockProject{
		child:   scan,
		exprs:   []binder.BoundExpr{&binder.BoundColumnRef{Table: "t1", Column: "a", ColType: dukdb.TYPE_INTEGER}},
		aliases: []string{"a"},
	}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := ProjectionPushdownRule{}.Apply(project, ctx)
	require.True(t, ok)
	newProject := rewritten.(*mockProject)
	newScan := newProject.child.(*mockScan)
	require.Equal(t, []int{0}, newScan.projections)
}

func TestJoinReorderingRule(t *testing.T) {
	left := &mockScan{alias: "left"}
	right := &mockScan{alias: "right"}
	join := &mockJoin{left: left, right: right, joinType: rewrite.JoinTypeInner}
	ctx := &rewrite.Context{
		Config:    rewrite.DefaultConfig(),
		Adapter:   mockAdapter{},
		Estimator: mockEstimator{costs: map[string]float64{"left": 1000, "right": 10}},
	}

	rewritten, ok := JoinReorderingRule{}.Apply(join, ctx)
	require.True(t, ok)
	newJoin := rewritten.(*mockJoin)
	require.Equal(t, "right", newJoin.left.(*mockScan).alias)
	require.Equal(t, "left", newJoin.right.(*mockScan).alias)
}

func TestDistinctEliminationRule(t *testing.T) {
	scan := &mockScan{alias: "t1"}
	inner := &mockDistinct{child: scan}
	outer := &mockDistinct{child: inner}
	ctx := &rewrite.Context{Config: rewrite.DefaultConfig(), Adapter: mockAdapter{}}

	rewritten, ok := DistinctEliminationRule{}.Apply(outer, ctx)
	require.True(t, ok)
	_, ok = rewritten.(*mockDistinct)
	require.True(t, ok)
	require.Equal(t, inner, rewritten)
}

type mockEstimator struct {
	costs map[string]float64
}

func (e mockEstimator) Estimate(plan rewrite.Plan) float64 {
	if scan, ok := plan.(*mockScan); ok {
		return e.costs[scan.alias]
	}
	return 1
}

func rewriteExprSlice(exprs []binder.BoundExpr, rewriter rewrite.ExprRewriter) ([]binder.BoundExpr, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	changed := false
	rewritten := make([]binder.BoundExpr, len(exprs))
	for i, expr := range exprs {
		next, exprChanged := rewrite.RewriteExpr(expr, rewriter)
		if exprChanged {
			changed = true
		}
		rewritten[i] = next
	}
	return rewritten, changed
}
