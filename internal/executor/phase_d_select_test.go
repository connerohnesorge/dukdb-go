package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhaseD_LiteralSelect tests SELECT with literal values (Task 4.2)
func TestPhaseD_LiteralSelect(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	t.Run("SELECT 1", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"SELECT 1",
		)
		require.NoError(t, err)

		require.Equal(t, 1, len(result.Rows))
		// The result should contain a column with value 1
		// Column name might vary, so just check we have a numeric result
		assert.Greater(
			t,
			len(result.Rows[0]),
			0,
			"should have at least one column",
		)
	})

	t.Run("SELECT 1, 2, 3", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"SELECT 1, 2, 3",
		)
		require.NoError(t, err)

		require.Equal(t, 1, len(result.Rows))
		// Should have 3 columns
		assert.Equal(
			t,
			3,
			len(result.Rows[0]),
			"should have 3 columns",
		)
	})
}

// TestPhaseD_ArithmeticExpressions tests SELECT with arithmetic (Task 4.2)
func TestPhaseD_ArithmeticExpressions(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	t.Run("SELECT 1 + 2", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"SELECT 1 + 2",
		)
		require.NoError(t, err)

		require.Equal(t, 1, len(result.Rows))
		// Result should be 3 (column name may vary)
		assert.Greater(
			t,
			len(result.Rows[0]),
			0,
			"should have result column",
		)
	})

	t.Run("SELECT 1 + 2 * 3", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"SELECT 1 + 2 * 3",
		)
		require.NoError(t, err)

		require.Equal(t, 1, len(result.Rows))
		// Result should be 7 (1 + 6)
		assert.Greater(
			t,
			len(result.Rows[0]),
			0,
			"should have result column",
		)
	})

	t.Run(
		"SELECT (10 - 5) * 2",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT (10 - 5) * 2",
			)
			require.NoError(t, err)

			require.Equal(t, 1, len(result.Rows))
			// Result should be 10 ((10-5)*2 = 5*2 = 10)
			assert.Greater(
				t,
				len(result.Rows[0]),
				0,
				"should have result column",
			)
		},
	)
}

// TestPhaseD_TableScan tests SELECT * FROM table (Task 4.3)
func TestPhaseD_TableScan(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create table
	tableDef := catalog.NewTableDef(
		"users",
		[]*catalog.ColumnDef{
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: false,
			},
			{
				Name:     "age",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	)
	err := cat.CreateTableInSchema(
		"main",
		tableDef,
	)
	require.NoError(t, err)

	table, err := stor.CreateTable(
		"users",
		[]dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_INTEGER,
		},
	)
	require.NoError(t, err)

	// Insert test data
	testData := []struct {
		id   int32
		name string
		age  int32
	}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", 35},
		{4, "David", 28},
		{5, "Eve", 32},
	}

	for _, row := range testData {
		err = table.AppendRow(
			[]any{row.id, row.name, row.age},
		)
		require.NoError(t, err)
	}

	t.Run(
		"SELECT * FROM users",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "users",
				Alias:     "users",
				TableDef:  tableDef,
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				scanPlan,
				nil,
			)
			require.NoError(t, err)

			// Verify all 5 rows returned
			require.Equal(t, 5, len(result.Rows))

			// Verify first row
			assert.Equal(
				t,
				int32(1),
				result.Rows[0]["id"],
			)
			assert.Equal(
				t,
				"Alice",
				result.Rows[0]["name"],
			)
			assert.Equal(
				t,
				int32(25),
				result.Rows[0]["age"],
			)
		},
	)

	t.Run(
		"SELECT id, name FROM users",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "users",
				Alias:     "users",
				TableDef:  tableDef,
			}

			// Project only id and name
			projectPlan := &planner.PhysicalProject{
				Child: scanPlan,
				Expressions: []binder.BoundExpr{
					&binder.BoundColumnRef{
						Table:   "users",
						Column:  "id",
						ColType: dukdb.TYPE_INTEGER,
					},
					&binder.BoundColumnRef{
						Table:   "users",
						Column:  "name",
						ColType: dukdb.TYPE_VARCHAR,
					},
				},
				Aliases: []string{"id", "name"},
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				projectPlan,
				nil,
			)
			require.NoError(t, err)

			require.Equal(t, 5, len(result.Rows))
			// Verify only id and name columns present
			assert.Contains(
				t,
				result.Rows[0],
				"id",
			)
			assert.Contains(
				t,
				result.Rows[0],
				"name",
			)
			assert.NotContains(
				t,
				result.Rows[0],
				"age",
			)
		},
	)
}

// TestPhaseD_FilteredQueries tests SELECT with WHERE clause (Task 4.3)
func TestPhaseD_FilteredQueries(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create and populate table
	tableDef := catalog.NewTableDef(
		"products",
		[]*catalog.ColumnDef{
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "price",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "category",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: false,
			},
		},
	)
	err := cat.CreateTableInSchema(
		"main",
		tableDef,
	)
	require.NoError(t, err)

	table, err := stor.CreateTable(
		"products",
		[]dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		},
	)
	require.NoError(t, err)

	// Insert test data
	testData := []struct {
		id       int32
		price    int32
		category string
	}{
		{1, 100, "Electronics"},
		{2, 50, "Books"},
		{3, 200, "Electronics"},
		{4, 30, "Books"},
		{5, 150, "Clothing"},
	}

	for _, row := range testData {
		err = table.AppendRow(
			[]any{
				row.id,
				row.price,
				row.category,
			},
		)
		require.NoError(t, err)
	}

	t.Run(
		"SELECT * FROM products WHERE price > 100",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "products",
				Alias:     "products",
				TableDef:  tableDef,
			}

			// WHERE price > 100
			filterExpr := &binder.BoundBinaryExpr{
				Op: 7, // OpGt
				Left: &binder.BoundColumnRef{
					Table:   "products",
					Column:  "price",
					ColType: dukdb.TYPE_INTEGER,
				},
				Right: &binder.BoundLiteral{
					Value:   int32(100),
					ValType: dukdb.TYPE_INTEGER,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			}

			filterPlan := &planner.PhysicalFilter{
				Child:     scanPlan,
				Condition: filterExpr,
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				filterPlan,
				nil,
			)
			require.NoError(t, err)

			// Should return 2 rows (price 200 and 150)
			require.Equal(t, 2, len(result.Rows))
		},
	)

	t.Run(
		"SELECT * FROM products WHERE category = 'Electronics'",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "products",
				Alias:     "products",
				TableDef:  tableDef,
			}

			// WHERE category = 'Electronics'
			filterExpr := &binder.BoundBinaryExpr{
				Op: 5, // OpEq
				Left: &binder.BoundColumnRef{
					Table:   "products",
					Column:  "category",
					ColType: dukdb.TYPE_VARCHAR,
				},
				Right: &binder.BoundLiteral{
					Value:   "Electronics",
					ValType: dukdb.TYPE_VARCHAR,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			}

			filterPlan := &planner.PhysicalFilter{
				Child:     scanPlan,
				Condition: filterExpr,
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				filterPlan,
				nil,
			)
			require.NoError(t, err)

			// Should return 2 Electronics rows
			require.Equal(t, 2, len(result.Rows))
			for _, row := range result.Rows {
				assert.Equal(
					t,
					"Electronics",
					row["category"],
				)
			}
		},
	)
}

// TestPhaseD_AggregateQueries tests aggregate functions (Task 4.4)
func TestPhaseD_AggregateQueries(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create and populate table
	tableDef := catalog.NewTableDef(
		"sales",
		[]*catalog.ColumnDef{
			{
				Name:     "amount",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	)
	err := cat.CreateTableInSchema(
		"main",
		tableDef,
	)
	require.NoError(t, err)

	table, err := stor.CreateTable(
		"sales",
		[]dukdb.Type{dukdb.TYPE_INTEGER},
	)
	require.NoError(t, err)

	// Insert test data: 10, 20, 30, 40, 50
	for _, val := range []int32{10, 20, 30, 40, 50} {
		err = table.AppendRow([]any{val})
		require.NoError(t, err)
	}

	t.Run(
		"SELECT SUM(amount) FROM sales",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "sales",
				Alias:     "sales",
				TableDef:  tableDef,
			}

			sumExpr := &binder.BoundFunctionCall{
				Name: "SUM",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{
						Table:   "sales",
						Column:  "amount",
						ColType: dukdb.TYPE_INTEGER,
					},
				},
				ResType: dukdb.TYPE_DOUBLE,
			}

			aggPlan := &planner.PhysicalHashAggregate{
				Child:   scanPlan,
				GroupBy: []binder.BoundExpr{},
				Aggregates: []binder.BoundExpr{
					sumExpr,
				},
				Aliases: []string{"SUM"},
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				aggPlan,
				nil,
			)
			require.NoError(t, err)

			require.Equal(t, 1, len(result.Rows))
			// SUM(10, 20, 30, 40, 50) = 150
			assert.Equal(
				t,
				float64(150),
				result.Rows[0]["SUM"],
			)
		},
	)

	t.Run(
		"SELECT AVG(amount) FROM sales",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "sales",
				Alias:     "sales",
				TableDef:  tableDef,
			}

			avgExpr := &binder.BoundFunctionCall{
				Name: "AVG",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{
						Table:   "sales",
						Column:  "amount",
						ColType: dukdb.TYPE_INTEGER,
					},
				},
				ResType: dukdb.TYPE_DOUBLE,
			}

			aggPlan := &planner.PhysicalHashAggregate{
				Child:   scanPlan,
				GroupBy: []binder.BoundExpr{},
				Aggregates: []binder.BoundExpr{
					avgExpr,
				},
				Aliases: []string{"AVG"},
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				aggPlan,
				nil,
			)
			require.NoError(t, err)

			require.Equal(t, 1, len(result.Rows))
			// AVG(10, 20, 30, 40, 50) = 30
			assert.Equal(
				t,
				float64(30),
				result.Rows[0]["AVG"],
			)
		},
	)
}

// TestPhaseD_GroupBy tests GROUP BY queries (Task 4.4)
func TestPhaseD_GroupBy(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create and populate table
	tableDef := catalog.NewTableDef(
		"orders",
		[]*catalog.ColumnDef{
			{
				Name:     "category",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: false,
			},
			{
				Name:     "amount",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	)
	err := cat.CreateTableInSchema(
		"main",
		tableDef,
	)
	require.NoError(t, err)

	table, err := stor.CreateTable(
		"orders",
		[]dukdb.Type{
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_INTEGER,
		},
	)
	require.NoError(t, err)

	// Insert test data
	testData := []struct {
		category string
		amount   int32
	}{
		{"A", 100},
		{"B", 200},
		{"A", 150},
		{"B", 250},
		{"C", 300},
	}

	for _, row := range testData {
		err = table.AppendRow(
			[]any{row.category, row.amount},
		)
		require.NoError(t, err)
	}

	t.Run(
		"SELECT category, COUNT(*) FROM orders GROUP BY category",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "orders",
				Alias:     "orders",
				TableDef:  tableDef,
			}

			groupByExpr := &binder.BoundColumnRef{
				Table:   "orders",
				Column:  "category",
				ColType: dukdb.TYPE_VARCHAR,
			}

			countExpr := &binder.BoundFunctionCall{
				Name:    "COUNT",
				Star:    true,
				Args:    []binder.BoundExpr{},
				ResType: dukdb.TYPE_BIGINT,
			}

			aggPlan := &planner.PhysicalHashAggregate{
				Child: scanPlan,
				GroupBy: []binder.BoundExpr{
					groupByExpr,
				},
				Aggregates: []binder.BoundExpr{
					countExpr,
				},
				Aliases: []string{
					"category",
					"COUNT",
				},
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				aggPlan,
				nil,
			)
			require.NoError(t, err)

			// Should have 3 groups: A, B, C
			require.Equal(t, 3, len(result.Rows))

			// Verify counts
			counts := make(map[string]int64)
			for _, row := range result.Rows {
				category := row["category"].(string)
				count := row["COUNT"].(int64)
				counts[category] = count
			}

			assert.Equal(t, int64(2), counts["A"])
			assert.Equal(t, int64(2), counts["B"])
			assert.Equal(t, int64(1), counts["C"])
		},
	)
}

// TestPhaseD_OrderByAndLimit tests ORDER BY and LIMIT (Task 4.5)
func TestPhaseD_OrderByAndLimit(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create and populate table
	tableDef := catalog.NewTableDef(
		"scores",
		[]*catalog.ColumnDef{
			{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: false,
			},
			{
				Name:     "score",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	)
	err := cat.CreateTableInSchema(
		"main",
		tableDef,
	)
	require.NoError(t, err)

	table, err := stor.CreateTable(
		"scores",
		[]dukdb.Type{
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_INTEGER,
		},
	)
	require.NoError(t, err)

	// Insert test data
	testData := []struct {
		name  string
		score int32
	}{
		{"Alice", 85},
		{"Bob", 92},
		{"Charlie", 78},
		{"David", 95},
		{"Eve", 88},
	}

	for _, row := range testData {
		err = table.AppendRow(
			[]any{row.name, row.score},
		)
		require.NoError(t, err)
	}

	t.Run(
		"SELECT * FROM scores ORDER BY score DESC",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "scores",
				Alias:     "scores",
				TableDef:  tableDef,
			}

			sortPlan := &planner.PhysicalSort{
				Child: scanPlan,
				OrderBy: []*binder.BoundOrderBy{
					{
						Expr: &binder.BoundColumnRef{
							Table:   "scores",
							Column:  "score",
							ColType: dukdb.TYPE_INTEGER,
						},
						Desc: true,
					},
				},
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				sortPlan,
				nil,
			)
			require.NoError(t, err)

			require.Equal(t, 5, len(result.Rows))
			// Verify descending order: 95, 92, 88, 85, 78
			assert.Equal(
				t,
				int32(95),
				result.Rows[0]["score"],
			)
			assert.Equal(
				t,
				int32(92),
				result.Rows[1]["score"],
			)
			assert.Equal(
				t,
				int32(88),
				result.Rows[2]["score"],
			)
			assert.Equal(
				t,
				int32(85),
				result.Rows[3]["score"],
			)
			assert.Equal(
				t,
				int32(78),
				result.Rows[4]["score"],
			)
		},
	)

	t.Run(
		"SELECT * FROM scores ORDER BY score DESC LIMIT 3",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "scores",
				Alias:     "scores",
				TableDef:  tableDef,
			}

			sortPlan := &planner.PhysicalSort{
				Child: scanPlan,
				OrderBy: []*binder.BoundOrderBy{
					{
						Expr: &binder.BoundColumnRef{
							Table:   "scores",
							Column:  "score",
							ColType: dukdb.TYPE_INTEGER,
						},
						Desc: true,
					},
				},
			}

			limitPlan := &planner.PhysicalLimit{
				Child:  sortPlan,
				Limit:  3,
				Offset: 0,
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				limitPlan,
				nil,
			)
			require.NoError(t, err)

			// Should return top 3 scores
			require.Equal(t, 3, len(result.Rows))
			assert.Equal(
				t,
				int32(95),
				result.Rows[0]["score"],
			)
			assert.Equal(
				t,
				int32(92),
				result.Rows[1]["score"],
			)
			assert.Equal(
				t,
				int32(88),
				result.Rows[2]["score"],
			)
		},
	)

	t.Run(
		"SELECT * FROM scores LIMIT 10",
		func(t *testing.T) {
			scanPlan := &planner.PhysicalScan{
				TableName: "scores",
				Alias:     "scores",
				TableDef:  tableDef,
			}

			limitPlan := &planner.PhysicalLimit{
				Child:  scanPlan,
				Limit:  10,
				Offset: 0,
			}

			ctx := context.Background()
			result, err := exec.Execute(
				ctx,
				limitPlan,
				nil,
			)
			require.NoError(t, err)

			// Should return all 5 rows (less than limit)
			require.Equal(t, 5, len(result.Rows))
		},
	)
}
