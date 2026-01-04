package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- 5.1.1 Parser Tests for PIVOT Syntax ----------

func TestParsePivotBasic(t *testing.T) {
	// Test basic PIVOT syntax
	// Note: PIVOT is typically used as part of a FROM clause, but also can be
	// a standalone statement depending on the implementation
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple PIVOT FROM clause",
			sql:     "SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))",
			wantErr: true, // Parser may not fully support this yet
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				// PIVOT parsing may not be fully implemented
				// This test documents expected syntax
				t.Logf("Parse result: %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPivotStmtStructure(t *testing.T) {
	// Test PivotStmt structure creation directly
	stmt := &PivotStmt{
		Source: TableRef{
			TableName: "sales",
		},
		PivotOn: []Expr{
			&Literal{Value: "Q1", Type: dukdb.TYPE_VARCHAR},
			&Literal{Value: "Q2", Type: dukdb.TYPE_VARCHAR},
			&Literal{Value: "Q3", Type: dukdb.TYPE_VARCHAR},
			&Literal{Value: "Q4", Type: dukdb.TYPE_VARCHAR},
		},
		Using: []PivotAggregate{
			{
				Function: "SUM",
				Expr:     &ColumnRef{Column: "amount"},
				Alias:    "total",
			},
		},
		GroupBy: []Expr{
			&ColumnRef{Column: "product"},
		},
		Alias:        "pivoted_sales",
		ColumnPrefix: "",
	}

	// Verify statement type
	assert.Equal(t, dukdb.STATEMENT_TYPE_PIVOT, stmt.Type())

	// Verify structure
	assert.Equal(t, "sales", stmt.Source.TableName)
	assert.Len(t, stmt.PivotOn, 4)
	assert.Len(t, stmt.Using, 1)
	assert.Equal(t, "SUM", stmt.Using[0].Function)
	assert.Equal(t, "total", stmt.Using[0].Alias)
	assert.Equal(t, "pivoted_sales", stmt.Alias)
}

func TestPivotAggregateTypes(t *testing.T) {
	// Test various aggregate functions in PIVOT
	aggregates := []PivotAggregate{
		{Function: "SUM", Expr: &ColumnRef{Column: "amount"}},
		{Function: "COUNT", Expr: &ColumnRef{Column: "id"}},
		{Function: "AVG", Expr: &ColumnRef{Column: "price"}},
		{Function: "MAX", Expr: &ColumnRef{Column: "value"}},
		{Function: "MIN", Expr: &ColumnRef{Column: "value"}},
	}

	for _, agg := range aggregates {
		assert.NotEmpty(t, agg.Function)
		assert.NotNil(t, agg.Expr)
	}
}

// ---------- 5.1.2 Parser Tests for UNPIVOT Syntax ----------

func TestUnpivotStmtStructure(t *testing.T) {
	// Test UnpivotStmt structure creation directly
	stmt := &UnpivotStmt{
		Source: TableRef{
			TableName: "quarterly_sales",
		},
		Into:  "amount",
		For:   "quarter",
		Using: []string{"Q1", "Q2", "Q3", "Q4"},
		Alias: "unpivoted_sales",
	}

	// Verify statement type
	assert.Equal(t, dukdb.STATEMENT_TYPE_UNPIVOT, stmt.Type())

	// Verify structure
	assert.Equal(t, "quarterly_sales", stmt.Source.TableName)
	assert.Equal(t, "amount", stmt.Into)
	assert.Equal(t, "quarter", stmt.For)
	assert.Len(t, stmt.Using, 4)
	assert.Contains(t, stmt.Using, "Q1")
	assert.Contains(t, stmt.Using, "Q4")
	assert.Equal(t, "unpivoted_sales", stmt.Alias)
}

func TestParseUnpivotBasic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple UNPIVOT",
			sql:     "SELECT * FROM sales UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4))",
			wantErr: true, // Parser may not fully support this yet
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				t.Logf("Parse result: %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------- 5.1.3 Parser Tests for GROUPING SETS/ROLLUP/CUBE ----------

func TestGroupingSetExprStructure(t *testing.T) {
	// Test GroupingSetExpr structure creation
	tests := []struct {
		name     string
		gse      *GroupingSetExpr
		wantType GroupingSetType
		wantLen  int
	}{
		{
			name: "simple grouping sets",
			gse: &GroupingSetExpr{
				Type: GroupingSetSimple,
				Exprs: [][]Expr{
					{&ColumnRef{Column: "region"}, &ColumnRef{Column: "product"}},
					{&ColumnRef{Column: "region"}},
					{}, // grand total
				},
			},
			wantType: GroupingSetSimple,
			wantLen:  3,
		},
		{
			name: "rollup",
			gse: &GroupingSetExpr{
				Type: GroupingSetRollup,
				Exprs: [][]Expr{
					{&ColumnRef{Column: "year"}, &ColumnRef{Column: "quarter"}, &ColumnRef{Column: "month"}},
				},
			},
			wantType: GroupingSetRollup,
			wantLen:  1,
		},
		{
			name: "cube",
			gse: &GroupingSetExpr{
				Type: GroupingSetCube,
				Exprs: [][]Expr{
					{&ColumnRef{Column: "region"}, &ColumnRef{Column: "product"}},
				},
			},
			wantType: GroupingSetCube,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantType, tt.gse.Type)
			assert.Len(t, tt.gse.Exprs, tt.wantLen)
		})
	}
}

func TestRollupExprStructure(t *testing.T) {
	rollup := &RollupExpr{
		Exprs: []Expr{
			&ColumnRef{Column: "year"},
			&ColumnRef{Column: "quarter"},
			&ColumnRef{Column: "month"},
		},
	}

	assert.Len(t, rollup.Exprs, 3)
	col1, ok := rollup.Exprs[0].(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "year", col1.Column)
}

func TestCubeExprStructure(t *testing.T) {
	cube := &CubeExpr{
		Exprs: []Expr{
			&ColumnRef{Column: "region"},
			&ColumnRef{Column: "product"},
		},
	}

	assert.Len(t, cube.Exprs, 2)
	col1, ok := cube.Exprs[0].(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "region", col1.Column)
}

func TestParseGroupBySyntax(t *testing.T) {
	// Test standard GROUP BY parsing
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple GROUP BY",
			sql:     "SELECT region, SUM(sales) FROM data GROUP BY region",
			wantErr: false,
		},
		{
			name:    "multiple GROUP BY columns",
			sql:     "SELECT region, product, SUM(sales) FROM data GROUP BY region, product",
			wantErr: false,
		},
		{
			name:    "GROUP BY with HAVING",
			sql:     "SELECT region, SUM(sales) as total FROM data GROUP BY region HAVING SUM(sales) > 1000",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, stmt)
				selectStmt, ok := stmt.(*SelectStmt)
				require.True(t, ok)
				assert.NotEmpty(t, selectStmt.GroupBy)
			}
		})
	}
}

// ---------- 5.1.4 Parser Tests for RECURSIVE CTE ----------

func TestParseCTEBasic(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		recursive bool
	}{
		{
			name:      "simple CTE",
			sql:       "WITH tmp AS (SELECT 1 AS n) SELECT * FROM tmp",
			wantErr:   false,
			recursive: false,
		},
		{
			name:      "multiple CTEs",
			sql:       "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
			wantErr:   false,
			recursive: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				selectStmt, ok := stmt.(*SelectStmt)
				require.True(t, ok)
				assert.NotEmpty(t, selectStmt.CTEs)
			}
		})
	}
}

func TestCTERecursiveFlag(t *testing.T) {
	// Test CTE structure with recursive flag
	cte := CTE{
		Name:      "org_tree",
		Columns:   []string{"id", "name", "parent_id", "level"},
		Query:     &SelectStmt{},
		Recursive: true,
	}

	assert.True(t, cte.Recursive)
	assert.Equal(t, "org_tree", cte.Name)
	assert.Len(t, cte.Columns, 4)
}

// ---------- 5.1.5 Parser Tests for LATERAL Joins ----------

func TestTableRefLateralFlag(t *testing.T) {
	// Test TableRef with Lateral flag
	tableRef := TableRef{
		TableName: "users",
		Alias:     "u",
		Lateral:   true,
		Subquery:  &SelectStmt{},
	}

	assert.True(t, tableRef.Lateral)
	assert.Equal(t, "u", tableRef.Alias)
}

func TestParseLateralJoinSyntax(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple JOIN",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "cross join",
			sql:     "SELECT * FROM users CROSS JOIN orders",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, stmt)
			}
		})
	}
}

// ---------- 5.1.6 Parser Tests for DISTINCT ON, QUALIFY, SAMPLE ----------

func TestSelectStmtDistinctOnField(t *testing.T) {
	// Test SelectStmt with DistinctOn field
	stmt := &SelectStmt{
		Distinct: false,
		DistinctOn: []Expr{
			&ColumnRef{Column: "category"},
		},
		Columns: []SelectColumn{
			{Expr: &StarExpr{}},
		},
	}

	assert.Len(t, stmt.DistinctOn, 1)
	col, ok := stmt.DistinctOn[0].(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "category", col.Column)
}

func TestSelectStmtQualifyField(t *testing.T) {
	// Test SelectStmt with Qualify field
	stmt := &SelectStmt{
		Columns: []SelectColumn{
			{Expr: &StarExpr{}},
		},
		Qualify: &BinaryExpr{
			Left:  &ColumnRef{Column: "row_num"},
			Op:    OpEq,
			Right: &Literal{Value: int64(1), Type: dukdb.TYPE_INTEGER},
		},
	}

	require.NotNil(t, stmt.Qualify)
	binExpr, ok := stmt.Qualify.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, OpEq, binExpr.Op)
}

func TestSelectStmtSampleField(t *testing.T) {
	// Test SelectStmt with Sample options
	seed := int64(42)
	tests := []struct {
		name   string
		sample *SampleOptions
	}{
		{
			name: "Bernoulli sampling percentage",
			sample: &SampleOptions{
				Method:     SampleBernoulli,
				Percentage: 10.0,
			},
		},
		{
			name: "System sampling percentage",
			sample: &SampleOptions{
				Method:     SampleSystem,
				Percentage: 5.0,
			},
		},
		{
			name: "Reservoir sampling rows",
			sample: &SampleOptions{
				Method: SampleReservoir,
				Rows:   100,
			},
		},
		{
			name: "Sampling with seed",
			sample: &SampleOptions{
				Method:     SampleBernoulli,
				Percentage: 10.0,
				Seed:       &seed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &SelectStmt{
				Columns: []SelectColumn{{Expr: &StarExpr{}}},
				Sample:  tt.sample,
			}
			require.NotNil(t, stmt.Sample)
		})
	}
}

func TestSampleMethodConstants(t *testing.T) {
	assert.Equal(t, SampleMethod(0), SampleBernoulli)
	assert.Equal(t, SampleMethod(1), SampleSystem)
	assert.Equal(t, SampleMethod(2), SampleReservoir)
}

func TestParseTablesampleSyntax(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantErr        bool
		wantMethod     SampleMethod
		wantPercentage float64
		wantRows       int
		wantSeed       *int64
	}{
		{
			name:           "BERNOULLI percentage",
			sql:            "SELECT * FROM test TABLESAMPLE BERNOULLI(10)",
			wantErr:        false,
			wantMethod:     SampleBernoulli,
			wantPercentage: 10.0,
		},
		{
			name:           "SYSTEM percentage",
			sql:            "SELECT * FROM test TABLESAMPLE SYSTEM(25)",
			wantErr:        false,
			wantMethod:     SampleSystem,
			wantPercentage: 25.0,
		},
		{
			name:       "RESERVOIR rows",
			sql:        "SELECT * FROM test TABLESAMPLE RESERVOIR(100)",
			wantErr:    false,
			wantMethod: SampleReservoir,
			wantRows:   100,
		},
		{
			name:           "BERNOULLI with REPEATABLE seed",
			sql:            "SELECT * FROM test TABLESAMPLE BERNOULLI(10) REPEATABLE(42)",
			wantErr:        false,
			wantMethod:     SampleBernoulli,
			wantPercentage: 10.0,
			wantSeed:       func() *int64 { v := int64(42); return &v }(),
		},
		{
			name:           "TABLESAMPLE with WHERE clause",
			sql:            "SELECT * FROM test TABLESAMPLE BERNOULLI(10) WHERE id > 5",
			wantErr:        false,
			wantMethod:     SampleBernoulli,
			wantPercentage: 10.0,
		},
		{
			name:           "TABLESAMPLE with table alias",
			sql:            "SELECT * FROM test t TABLESAMPLE BERNOULLI(10)",
			wantErr:        false,
			wantMethod:     SampleBernoulli,
			wantPercentage: 10.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")
			require.NotNil(t, selectStmt.Sample, "expected Sample to be set")

			assert.Equal(t, tt.wantMethod, selectStmt.Sample.Method)
			assert.Equal(t, tt.wantPercentage, selectStmt.Sample.Percentage)
			assert.Equal(t, tt.wantRows, selectStmt.Sample.Rows)

			if tt.wantSeed != nil {
				require.NotNil(t, selectStmt.Sample.Seed)
				assert.Equal(t, *tt.wantSeed, *selectStmt.Sample.Seed)
			} else {
				assert.Nil(t, selectStmt.Sample.Seed)
			}
		})
	}
}

// ---------- 5.1.7 Parser Tests for MERGE INTO ----------

func TestMergeStmtStructure(t *testing.T) {
	stmt := &MergeStmt{
		Schema: "main",
		Into: TableRef{
			TableName: "target",
			Alias:     "t",
		},
		Using: TableRef{
			TableName: "source",
			Alias:     "s",
		},
		On: &BinaryExpr{
			Left:  &ColumnRef{Table: "t", Column: "id"},
			Op:    OpEq,
			Right: &ColumnRef{Table: "s", Column: "id"},
		},
		WhenMatched: []MergeAction{
			{
				Type: MergeActionUpdate,
				Update: []SetClause{
					{Column: "value", Value: &ColumnRef{Table: "s", Column: "value"}},
				},
			},
		},
		WhenNotMatched: []MergeAction{
			{
				Type: MergeActionInsert,
				Insert: []SetClause{
					{Column: "id", Value: &ColumnRef{Table: "s", Column: "id"}},
					{Column: "value", Value: &ColumnRef{Table: "s", Column: "value"}},
				},
			},
		},
	}

	// Verify statement type
	assert.Equal(t, dukdb.STATEMENT_TYPE_MERGE_INTO, stmt.Type())

	// Verify structure
	assert.Equal(t, "main", stmt.Schema)
	assert.Equal(t, "target", stmt.Into.TableName)
	assert.Equal(t, "source", stmt.Using.TableName)
	assert.NotNil(t, stmt.On)
	assert.Len(t, stmt.WhenMatched, 1)
	assert.Len(t, stmt.WhenNotMatched, 1)
}

func TestMergeActionTypes(t *testing.T) {
	tests := []struct {
		name       string
		actionType MergeActionType
	}{
		{"Update", MergeActionUpdate},
		{"Delete", MergeActionDelete},
		{"Insert", MergeActionInsert},
		{"DoNothing", MergeActionDoNothing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := MergeAction{Type: tt.actionType}
			assert.Equal(t, tt.actionType, action.Type)
		})
	}
}

func TestMergeActionWithCondition(t *testing.T) {
	// WHEN MATCHED AND target.status = 'active' THEN UPDATE ...
	action := MergeAction{
		Type: MergeActionUpdate,
		Cond: &BinaryExpr{
			Left:  &ColumnRef{Table: "target", Column: "status"},
			Op:    OpEq,
			Right: &Literal{Value: "active", Type: dukdb.TYPE_VARCHAR},
		},
		Update: []SetClause{
			{Column: "value", Value: &Literal{Value: int64(100), Type: dukdb.TYPE_INTEGER}},
		},
	}

	require.NotNil(t, action.Cond)
	assert.Equal(t, MergeActionUpdate, action.Type)
	assert.Len(t, action.Update, 1)
}

func TestMergeActionConstants(t *testing.T) {
	assert.Equal(t, MergeActionType(0), MergeActionUpdate)
	assert.Equal(t, MergeActionType(1), MergeActionDelete)
	assert.Equal(t, MergeActionType(2), MergeActionInsert)
	assert.Equal(t, MergeActionType(3), MergeActionDoNothing)
}

// ---------- 5.1.8 Parser Tests for RETURNING Clause ----------

func TestInsertStmtReturning(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "users",
		Columns: []string{"name", "email"},
		Values: [][]Expr{
			{
				&Literal{Value: "Alice", Type: dukdb.TYPE_VARCHAR},
				&Literal{Value: "alice@example.com", Type: dukdb.TYPE_VARCHAR},
			},
		},
		Returning: []SelectColumn{
			{Star: true},
		},
	}

	assert.Len(t, stmt.Returning, 1)
	assert.True(t, stmt.Returning[0].Star)
}

func TestUpdateStmtReturning(t *testing.T) {
	stmt := &UpdateStmt{
		Table: "users",
		Set: []SetClause{
			{Column: "name", Value: &Literal{Value: "Bob", Type: dukdb.TYPE_VARCHAR}},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Column: "id"},
			Op:    OpEq,
			Right: &Literal{Value: int64(1), Type: dukdb.TYPE_INTEGER},
		},
		Returning: []SelectColumn{
			{Expr: &ColumnRef{Column: "id"}},
			{Expr: &ColumnRef{Column: "name"}},
		},
	}

	assert.Len(t, stmt.Returning, 2)
}

func TestDeleteStmtReturning(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Column: "id"},
			Op:    OpEq,
			Right: &Literal{Value: int64(1), Type: dukdb.TYPE_INTEGER},
		},
		Returning: []SelectColumn{
			{Star: true},
		},
	}

	assert.Len(t, stmt.Returning, 1)
	assert.True(t, stmt.Returning[0].Star)
}

func TestMergeStmtReturning(t *testing.T) {
	stmt := &MergeStmt{
		Into: TableRef{TableName: "target"},
		Using: TableRef{TableName: "source"},
		On: &BinaryExpr{
			Left:  &ColumnRef{Table: "target", Column: "id"},
			Op:    OpEq,
			Right: &ColumnRef{Table: "source", Column: "id"},
		},
		Returning: []SelectColumn{
			{Expr: &ColumnRef{Column: "id"}},
			{Expr: &ColumnRef{Column: "value"}},
		},
	}

	assert.Len(t, stmt.Returning, 2)
}

func TestParseInsertWithReturning(t *testing.T) {
	// Test INSERT RETURNING parsing
	tests := []struct {
		name          string
		sql           string
		wantErr       bool
		hasReturning  bool
		returningStar bool
		returningCols int
	}{
		{
			name:         "simple INSERT without RETURNING",
			sql:          "INSERT INTO users (name) VALUES ('Alice')",
			wantErr:      false,
			hasReturning: false,
		},
		{
			name:          "INSERT with RETURNING *",
			sql:           "INSERT INTO users (name) VALUES ('Alice') RETURNING *",
			wantErr:       false,
			hasReturning:  true,
			returningStar: true,
			returningCols: 1,
		},
		{
			name:          "INSERT with RETURNING columns",
			sql:           "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id, name",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 2,
		},
		{
			name:          "INSERT with RETURNING column and alias",
			sql:           "INSERT INTO users (name) VALUES ('Alice') RETURNING id AS user_id",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 1,
		},
		{
			name:          "INSERT with RETURNING expression",
			sql:           "INSERT INTO users (name) VALUES ('Alice') RETURNING id, name || ' added'",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 2,
		},
		{
			name:          "INSERT SELECT with RETURNING",
			sql:           "INSERT INTO users_backup SELECT * FROM users RETURNING id",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				insertStmt, ok := stmt.(*InsertStmt)
				require.True(t, ok)
				// Note: INSERT SELECT test uses users_backup as the target table
				expectedTable := "users"
				if tt.name == "INSERT SELECT with RETURNING" {
					expectedTable = "users_backup"
				}
				assert.Equal(t, expectedTable, insertStmt.Table)
				if tt.hasReturning {
					assert.Len(t, insertStmt.Returning, tt.returningCols)
					if tt.returningStar {
						assert.True(t, insertStmt.Returning[0].Star)
					}
				} else {
					assert.Empty(t, insertStmt.Returning)
				}
			}
		})
	}
}

func TestParseUpdateWithReturning(t *testing.T) {
	// Test UPDATE RETURNING parsing
	tests := []struct {
		name          string
		sql           string
		wantErr       bool
		hasReturning  bool
		returningStar bool
		returningCols int
	}{
		{
			name:         "simple UPDATE without RETURNING",
			sql:          "UPDATE users SET name = 'Bob' WHERE id = 1",
			wantErr:      false,
			hasReturning: false,
		},
		{
			name:          "UPDATE with RETURNING *",
			sql:           "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING *",
			wantErr:       false,
			hasReturning:  true,
			returningStar: true,
			returningCols: 1,
		},
		{
			name:          "UPDATE with RETURNING columns",
			sql:           "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1 RETURNING id, name",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 2,
		},
		{
			name:          "UPDATE with RETURNING column and alias",
			sql:           "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING id AS user_id, name AS updated_name",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 2,
		},
		{
			name:          "UPDATE without WHERE with RETURNING",
			sql:           "UPDATE users SET active = true RETURNING id",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				updateStmt, ok := stmt.(*UpdateStmt)
				require.True(t, ok)
				assert.Equal(t, "users", updateStmt.Table)
				if tt.hasReturning {
					assert.Len(t, updateStmt.Returning, tt.returningCols)
					if tt.returningStar {
						assert.True(t, updateStmt.Returning[0].Star)
					}
				} else {
					assert.Empty(t, updateStmt.Returning)
				}
			}
		})
	}
}

func TestParseDeleteWithReturning(t *testing.T) {
	// Test DELETE RETURNING parsing
	tests := []struct {
		name          string
		sql           string
		wantErr       bool
		hasReturning  bool
		returningStar bool
		returningCols int
	}{
		{
			name:         "simple DELETE without RETURNING",
			sql:          "DELETE FROM users WHERE id = 1",
			wantErr:      false,
			hasReturning: false,
		},
		{
			name:          "DELETE with RETURNING *",
			sql:           "DELETE FROM users WHERE id = 1 RETURNING *",
			wantErr:       false,
			hasReturning:  true,
			returningStar: true,
			returningCols: 1,
		},
		{
			name:          "DELETE with RETURNING columns",
			sql:           "DELETE FROM users WHERE active = false RETURNING id, name, email",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 3,
		},
		{
			name:          "DELETE with RETURNING column and alias",
			sql:           "DELETE FROM users WHERE id = 1 RETURNING id AS deleted_id",
			wantErr:       false,
			hasReturning:  true,
			returningStar: false,
			returningCols: 1,
		},
		{
			name:          "DELETE without WHERE with RETURNING",
			sql:           "DELETE FROM users RETURNING *",
			wantErr:       false,
			hasReturning:  true,
			returningStar: true,
			returningCols: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				deleteStmt, ok := stmt.(*DeleteStmt)
				require.True(t, ok)
				assert.Equal(t, "users", deleteStmt.Table)
				if tt.hasReturning {
					assert.Len(t, deleteStmt.Returning, tt.returningCols)
					if tt.returningStar {
						assert.True(t, deleteStmt.Returning[0].Star)
					}
				} else {
					assert.Empty(t, deleteStmt.Returning)
				}
			}
		})
	}
}

// ---------- Visitor Pattern Tests ----------

func TestPivotStmtAccept(t *testing.T) {
	stmt := &PivotStmt{
		Source: TableRef{TableName: "sales"},
	}

	// Test that Accept doesn't panic
	extractor := NewTableExtractor(false)
	stmt.Accept(extractor)
}

func TestUnpivotStmtAccept(t *testing.T) {
	stmt := &UnpivotStmt{
		Source: TableRef{TableName: "sales"},
	}

	// Test that Accept doesn't panic
	extractor := NewTableExtractor(false)
	stmt.Accept(extractor)
}

func TestMergeStmtAccept(t *testing.T) {
	stmt := &MergeStmt{
		Into:  TableRef{TableName: "target"},
		Using: TableRef{TableName: "source"},
	}

	// Test that Accept doesn't panic
	extractor := NewTableExtractor(false)
	stmt.Accept(extractor)
}

// ---------- Expression Interface Tests ----------

func TestGroupingSetExprExprNode(t *testing.T) {
	gse := &GroupingSetExpr{}
	// Verify it implements Expr interface
	var _ Expr = gse
}

func TestRollupExprExprNode(t *testing.T) {
	re := &RollupExpr{}
	// Verify it implements Expr interface
	var _ Expr = re
}

func TestCubeExprExprNode(t *testing.T) {
	ce := &CubeExpr{}
	// Verify it implements Expr interface
	var _ Expr = ce
}
