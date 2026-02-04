package planner

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/require"
)

func TestRewriteExpressionConstantFolding(t *testing.T) {
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

	plan := &LogicalFilter{Child: &LogicalDummyScan{}, Condition: expr}
	engine := NewRewriteEngine(nil, DefaultRewriteConfig())
	rewritten, _ := engine.Apply(plan)

	filter := rewritten.(*LogicalFilter)
	lit, ok := filter.Condition.(*binder.BoundLiteral)
	require.True(t, ok, "expected constant fold to literal")
	require.Equal(t, int64(7), lit.Value)
}

func TestPredicatePushdownInnerJoin(t *testing.T) {
	left := newScan("t1", []string{"a"})
	right := newScan("t2", []string{"b"})
	join := &LogicalJoin{Left: left, Right: right, JoinType: JoinTypeInner}

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

	plan := &LogicalFilter{Child: join, Condition: cond}
	engine := NewRewriteEngine(nil, DefaultRewriteConfig())
	rewritten, _ := engine.Apply(plan)

	newJoin, ok := rewritten.(*LogicalJoin)
	require.True(t, ok, "expected filter to be pushed into join")

	_, leftIsFilter := newJoin.Left.(*LogicalFilter)
	_, rightIsFilter := newJoin.Right.(*LogicalFilter)
	require.True(t, leftIsFilter, "expected left filter")
	require.True(t, rightIsFilter, "expected right filter")
}

func TestJoinReorderUsesEstimates(t *testing.T) {
	left := newScanWithRows("left", []string{"id"}, 1000)
	right := newScanWithRows("right", []string{"id"}, 10)
	join := &LogicalJoin{Left: left, Right: right, JoinType: JoinTypeInner}

	engine := NewRewriteEngine(nil, DefaultRewriteConfig())
	rewritten, _ := engine.Apply(join)

	reordered := rewritten.(*LogicalJoin)
	require.Equal(t, "right", reordered.Left.(*LogicalScan).Alias)
	require.Equal(t, "left", reordered.Right.(*LogicalScan).Alias)
}

func TestUnnestInSubqueryToJoin(t *testing.T) {
	cat := catalog.NewCatalog()
	planner := NewPlanner(cat)

	left := newScan("t1", []string{"id"})

	subquery := &binder.BoundSelectStmt{
		Columns: []*binder.BoundSelectColumn{
			{Expr: &binder.BoundColumnRef{Table: "t2", Column: "id", ColType: dukdb.TYPE_INTEGER}},
		},
		From: []*binder.BoundTableRef{
			{Schema: "main", TableName: "t2", Alias: "t2", TableDef: newTableDef("t2", []string{"id"})},
		},
	}

	inExpr := &binder.BoundInSubqueryExpr{
		Expr:     &binder.BoundColumnRef{Table: "t1", Column: "id", ColType: dukdb.TYPE_INTEGER},
		Subquery: subquery,
	}
	plan := &LogicalFilter{Child: left, Condition: inExpr}

	rewritten, _ := planner.applyRewrites(plan)
	join, ok := rewritten.(*LogicalJoin)
	require.True(t, ok, "expected IN subquery to be unnested")
	require.Equal(t, JoinTypeSemi, join.JoinType)
}

func newScan(alias string, columns []string) *LogicalScan {
	return newScanWithRows(alias, columns, 100)
}

func newScanWithRows(alias string, columns []string, rows int64) *LogicalScan {
	tableDef := newTableDef(alias, columns)
	tableDef.Statistics = optimizer.NewTableStatistics()
	tableDef.Statistics.RowCount = rows
	return &LogicalScan{Schema: "main", TableName: alias, Alias: alias, TableDef: tableDef}
}

func newTableDef(name string, columns []string) *catalog.TableDef {
	defs := make([]*catalog.ColumnDef, len(columns))
	for i, col := range columns {
		defs[i] = &catalog.ColumnDef{Name: col, Type: dukdb.TYPE_INTEGER}
	}
	return &catalog.TableDef{Name: name, Columns: defs}
}
