package binder

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a catalog with test tables for advanced SQL tests
func setupAdvancedTestCatalog() *catalog.Catalog {
	cat := catalog.NewCatalog()

	// Create employees table for recursive CTE tests
	empColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("manager_id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("department", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("salary", dukdb.TYPE_INTEGER),
	}
	empTable := catalog.NewTableDef("employees", empColumns)
	_ = cat.CreateTableInSchema("main", empTable)

	// Create sales table for grouping sets tests
	salesColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("region", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("product", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("quarter", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("amount", dukdb.TYPE_INTEGER),
	}
	salesTable := catalog.NewTableDef("sales", salesColumns)
	_ = cat.CreateTableInSchema("main", salesTable)

	// Create target table for MERGE tests
	targetColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("value", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("status", dukdb.TYPE_VARCHAR),
	}
	targetTable := catalog.NewTableDef("target", targetColumns)
	_ = cat.CreateTableInSchema("main", targetTable)

	// Create source table for MERGE tests
	sourceColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("value", dukdb.TYPE_INTEGER),
	}
	sourceTable := catalog.NewTableDef("source", sourceColumns)
	_ = cat.CreateTableInSchema("main", sourceTable)

	// Create orders table for LATERAL join tests
	ordersColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("user_id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("total", dukdb.TYPE_INTEGER),
	}
	ordersTable := catalog.NewTableDef("orders", ordersColumns)
	_ = cat.CreateTableInSchema("main", ordersTable)

	// Create users table
	usersColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	}
	usersTable := catalog.NewTableDef("users", usersColumns)
	_ = cat.CreateTableInSchema("main", usersTable)

	return cat
}

// ---------- 5.2.1 Binding Tests for Recursive CTEs ----------

func TestBindCTEBasic(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "WITH tmp AS (SELECT 1 AS n) SELECT * FROM tmp"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)
	require.NotNil(t, boundStmt)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.NotEmpty(t, selectStmt.CTEs)
	assert.Equal(t, "tmp", selectStmt.CTEs[0].Name)
}

func TestBoundCTEStructure(t *testing.T) {
	// Test BoundCTE structure directly
	cte := &BoundCTE{
		Name:        "org_tree",
		Columns:     []string{"id", "name", "level"},
		Query:       &BoundSelectStmt{},
		Recursive:   true,
		ResultTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
		ResultNames: []string{"id", "name", "level"},
	}

	assert.True(t, cte.Recursive)
	assert.Equal(t, "org_tree", cte.Name)
	assert.Len(t, cte.Columns, 3)
	assert.Len(t, cte.ResultTypes, 3)
}

func TestBindCTEWithMultipleCTEs(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.Len(t, selectStmt.CTEs, 2)
}

// ---------- 5.2.2 Binding Tests for Grouping Sets ----------

func TestBindGroupByBasic(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT region, SUM(amount) FROM sales GROUP BY region"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)
	require.NotNil(t, boundStmt)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.NotEmpty(t, selectStmt.GroupBy)
}

func TestBindGroupByMultipleColumns(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT region, product, SUM(amount) FROM sales GROUP BY region, product"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.Len(t, selectStmt.GroupBy, 2)
}

func TestBindGroupByWithHaving(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 1000"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.NotNil(t, selectStmt.Having)
}

// ---------- 5.2.3 Binding Tests for LATERAL Correlations ----------

func TestBindJoinBasic(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)
	require.NotNil(t, boundStmt)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.NotEmpty(t, selectStmt.Joins)
}

func TestBindLeftJoin(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	boundStmt, err := binder.Bind(stmt)
	require.NoError(t, err)

	selectStmt, ok := boundStmt.(*BoundSelectStmt)
	require.True(t, ok)
	assert.NotEmpty(t, selectStmt.Joins)
	assert.Equal(t, parser.JoinTypeLeft, selectStmt.Joins[0].Type)
}

func TestBoundTableRefLateral(t *testing.T) {
	// Test BoundTableRef structure with Lateral flag
	tableRef := &BoundTableRef{
		Schema:    "main",
		TableName: "subquery_result",
		Alias:     "sq",
		Lateral:   true,
	}

	assert.True(t, tableRef.Lateral)
	assert.Equal(t, "sq", tableRef.Alias)
}

// ---------- 5.2.4 Binding Tests for MERGE INTO ----------

func TestBoundMergeStmtStructure(t *testing.T) {
	// Test BoundMergeStmt structure directly
	cat := setupAdvancedTestCatalog()
	targetDef, _ := cat.GetTable("target")

	stmt := &BoundMergeStmt{
		Schema:         "main",
		TargetTable:    "target",
		TargetTableDef: targetDef,
		TargetAlias:    "t",
		SourceRef: &BoundTableRef{
			TableName: "source",
			Alias:     "s",
		},
		OnCondition: &BoundBinaryExpr{
			Left:    &BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpEq,
			Right:   &BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		WhenMatched: []*BoundMergeAction{
			{
				Type: BoundMergeActionUpdate,
				Update: []*BoundSetClause{
					{
						ColumnIdx: 1,
						Value:     &BoundColumnRef{Column: "value", ColType: dukdb.TYPE_INTEGER},
					},
				},
			},
		},
		WhenNotMatched: []*BoundMergeAction{
			{
				Type:          BoundMergeActionInsert,
				InsertColumns: []int{0, 1},
				InsertValues: []BoundExpr{
					&BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
					&BoundColumnRef{Column: "value", ColType: dukdb.TYPE_INTEGER},
				},
			},
		},
	}

	assert.Equal(t, dukdb.STATEMENT_TYPE_MERGE_INTO, stmt.Type())
	assert.Equal(t, "target", stmt.TargetTable)
	assert.Equal(t, "t", stmt.TargetAlias)
	assert.NotNil(t, stmt.OnCondition)
	assert.Len(t, stmt.WhenMatched, 1)
	assert.Len(t, stmt.WhenNotMatched, 1)
}

func TestBoundMergeActionTypes(t *testing.T) {
	tests := []struct {
		name       string
		actionType BoundMergeActionType
	}{
		{"Update", BoundMergeActionUpdate},
		{"Delete", BoundMergeActionDelete},
		{"Insert", BoundMergeActionInsert},
		{"DoNothing", BoundMergeActionDoNothing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := &BoundMergeAction{Type: tt.actionType}
			assert.Equal(t, tt.actionType, action.Type)
		})
	}
}

func TestBoundMergeActionWithCondition(t *testing.T) {
	action := &BoundMergeAction{
		Type: BoundMergeActionUpdate,
		Cond: &BoundBinaryExpr{
			Left:    &BoundColumnRef{Column: "status", ColType: dukdb.TYPE_VARCHAR},
			Op:      parser.OpEq,
			Right:   &BoundLiteral{Value: "active", ValType: dukdb.TYPE_VARCHAR},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		Update: []*BoundSetClause{
			{ColumnIdx: 1, Value: &BoundLiteral{Value: int64(100), ValType: dukdb.TYPE_INTEGER}},
		},
	}

	require.NotNil(t, action.Cond)
	assert.Equal(t, BoundMergeActionUpdate, action.Type)
	assert.Len(t, action.Update, 1)
}

// ---------- PIVOT/UNPIVOT Binding Tests ----------

func TestBoundPivotStmtStructure(t *testing.T) {
	stmt := &BoundPivotStmt{
		Source: &BoundTableRef{
			TableName: "sales",
		},
		ForColumn: &BoundColumnRef{
			Column:  "quarter",
			ColType: dukdb.TYPE_VARCHAR,
		},
		InValues: []any{"Q1", "Q2", "Q3", "Q4"},
		Aggregates: []*BoundPivotAggregate{
			{
				Function: "SUM",
				Expr:     &BoundColumnRef{Column: "amount", ColType: dukdb.TYPE_INTEGER},
				Alias:    "total",
			},
		},
		GroupBy: []BoundExpr{
			&BoundColumnRef{Column: "product", ColType: dukdb.TYPE_VARCHAR},
		},
		Alias: "pivoted_sales",
	}

	assert.Equal(t, dukdb.STATEMENT_TYPE_PIVOT, stmt.Type())
	assert.Equal(t, "sales", stmt.Source.TableName)
	assert.Equal(t, "quarter", stmt.ForColumn.Column)
	assert.Len(t, stmt.InValues, 4)
	assert.Len(t, stmt.Aggregates, 1)
	assert.Equal(t, "SUM", stmt.Aggregates[0].Function)
}

func TestBoundUnpivotStmtStructure(t *testing.T) {
	stmt := &BoundUnpivotStmt{
		Source: &BoundTableRef{
			TableName: "quarterly_sales",
		},
		ValueColumn:    "amount",
		NameColumn:     "quarter",
		UnpivotColumns: []string{"Q1", "Q2", "Q3", "Q4"},
		Alias:          "unpivoted_sales",
	}

	assert.Equal(t, dukdb.STATEMENT_TYPE_UNPIVOT, stmt.Type())
	assert.Equal(t, "quarterly_sales", stmt.Source.TableName)
	assert.Equal(t, "amount", stmt.ValueColumn)
	assert.Equal(t, "quarter", stmt.NameColumn)
	assert.Len(t, stmt.UnpivotColumns, 4)
}

// ---------- RETURNING Clause Binding Tests ----------

func TestBoundInsertWithReturning(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	targetDef, _ := cat.GetTable("target")

	stmt := &BoundInsertStmt{
		Schema:   "main",
		Table:    "target",
		TableDef: targetDef,
		Columns:  []int{0, 1},
		Values: [][]BoundExpr{
			{
				&BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
				&BoundLiteral{Value: int64(100), ValType: dukdb.TYPE_INTEGER},
			},
		},
		Returning: []*BoundSelectColumn{
			{Star: true},
		},
	}

	assert.Len(t, stmt.Returning, 1)
	assert.True(t, stmt.Returning[0].Star)
}

func TestBoundUpdateWithReturning(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	targetDef, _ := cat.GetTable("target")

	stmt := &BoundUpdateStmt{
		Schema:   "main",
		Table:    "target",
		TableDef: targetDef,
		Set: []*BoundSetClause{
			{ColumnIdx: 1, Value: &BoundLiteral{Value: int64(200), ValType: dukdb.TYPE_INTEGER}},
		},
		Where: &BoundBinaryExpr{
			Left:    &BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpEq,
			Right:   &BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		Returning: []*BoundSelectColumn{
			{Expr: &BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER}},
			{Expr: &BoundColumnRef{Column: "value", ColType: dukdb.TYPE_INTEGER}},
		},
	}

	assert.Len(t, stmt.Returning, 2)
}

func TestBoundDeleteWithReturning(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	targetDef, _ := cat.GetTable("target")

	stmt := &BoundDeleteStmt{
		Schema:   "main",
		Table:    "target",
		TableDef: targetDef,
		Where: &BoundBinaryExpr{
			Left:    &BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpEq,
			Right:   &BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
			ResType: dukdb.TYPE_BOOLEAN,
		},
		Returning: []*BoundSelectColumn{
			{Star: true},
		},
	}

	assert.Len(t, stmt.Returning, 1)
	assert.True(t, stmt.Returning[0].Star)
}

// ---------- DISTINCT ON and QUALIFY Binding Tests ----------

func TestBoundSelectWithDistinctOn(t *testing.T) {
	stmt := &BoundSelectStmt{
		Distinct: false,
		DistinctOn: []BoundExpr{
			&BoundColumnRef{Column: "category", ColType: dukdb.TYPE_VARCHAR},
		},
		Columns: []*BoundSelectColumn{
			{Star: true},
		},
	}

	assert.Len(t, stmt.DistinctOn, 1)
	col, ok := stmt.DistinctOn[0].(*BoundColumnRef)
	require.True(t, ok)
	assert.Equal(t, "category", col.Column)
}

func TestBoundSelectWithQualify(t *testing.T) {
	stmt := &BoundSelectStmt{
		Columns: []*BoundSelectColumn{
			{Star: true},
		},
		Qualify: &BoundBinaryExpr{
			Left:    &BoundColumnRef{Column: "row_num", ColType: dukdb.TYPE_INTEGER},
			Op:      parser.OpEq,
			Right:   &BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
			ResType: dukdb.TYPE_BOOLEAN,
		},
	}

	require.NotNil(t, stmt.Qualify)
	binExpr, ok := stmt.Qualify.(*BoundBinaryExpr)
	require.True(t, ok)
	assert.Equal(t, parser.OpEq, binExpr.Op)
}

// ---------- SAMPLE Clause Binding Tests ----------

func TestBoundSelectWithSample(t *testing.T) {
	seed := int64(42)
	tests := []struct {
		name   string
		sample *BoundSampleOptions
	}{
		{
			name: "Bernoulli sampling",
			sample: &BoundSampleOptions{
				Method:     parser.SampleBernoulli,
				Percentage: 10.0,
			},
		},
		{
			name: "System sampling",
			sample: &BoundSampleOptions{
				Method:     parser.SampleSystem,
				Percentage: 5.0,
			},
		},
		{
			name: "Reservoir sampling",
			sample: &BoundSampleOptions{
				Method: parser.SampleReservoir,
				Rows:   100,
			},
		},
		{
			name: "Sampling with seed",
			sample: &BoundSampleOptions{
				Method:     parser.SampleBernoulli,
				Percentage: 10.0,
				Seed:       &seed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &BoundSelectStmt{
				Columns: []*BoundSelectColumn{{Star: true}},
				Sample:  tt.sample,
			}
			require.NotNil(t, stmt.Sample)
		})
	}
}

// ---------- Expression Binding Error Tests ----------

func TestBindNonexistentColumnInWhere(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT * FROM sales WHERE nonexistent_column > 100"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	_, err = binder.Bind(stmt)
	require.Error(t, err)

	dukdbErr, ok := err.(*dukdb.Error)
	require.True(t, ok)
	assert.Equal(t, dukdb.ErrorTypeBinder, dukdbErr.Type)
}

func TestBindNonexistentTable(t *testing.T) {
	cat := setupAdvancedTestCatalog()
	binder := NewBinder(cat)

	sql := "SELECT * FROM nonexistent_table"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	_, err = binder.Bind(stmt)
	require.Error(t, err)

	dukdbErr, ok := err.(*dukdb.Error)
	require.True(t, ok)
	assert.Equal(t, dukdb.ErrorTypeBinder, dukdbErr.Type)
}

// ---------- Statement Type Tests ----------

func TestBoundMergeStmtType(t *testing.T) {
	stmt := &BoundMergeStmt{}
	assert.Equal(t, dukdb.STATEMENT_TYPE_MERGE_INTO, stmt.Type())
}

func TestBoundPivotStmtType(t *testing.T) {
	stmt := &BoundPivotStmt{}
	assert.Equal(t, dukdb.STATEMENT_TYPE_PIVOT, stmt.Type())
}

func TestBoundUnpivotStmtType(t *testing.T) {
	stmt := &BoundUnpivotStmt{}
	assert.Equal(t, dukdb.STATEMENT_TYPE_UNPIVOT, stmt.Type())
}

func TestBoundMergeActionConstants(t *testing.T) {
	assert.Equal(t, BoundMergeActionType(0), BoundMergeActionUpdate)
	assert.Equal(t, BoundMergeActionType(1), BoundMergeActionDelete)
	assert.Equal(t, BoundMergeActionType(2), BoundMergeActionInsert)
	assert.Equal(t, BoundMergeActionType(3), BoundMergeActionDoNothing)
}
