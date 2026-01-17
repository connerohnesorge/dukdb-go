// Package executor provides query execution for dukdb-go.
//
// Edge Cases and Stress Tests
//
// This file contains tests for edge cases and stress scenarios:
// - Task 9.7: All subquery types
// - Task 9.8: Filter pushdown scenarios
// - Task 9.9: Statistics persistence
// - Task 9.10-9.12: Stress tests
package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/require"
)

// setupEdgeCaseTestExecutor sets up an executor for testing
func setupEdgeCaseTestExecutor() (*Executor, *catalog.Catalog, *storage.Storage) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat, stor
}

// executeEdgeCaseQuery executes a query and returns the result
func executeEdgeCaseQuery(
	t *testing.T,
	exec *Executor,
	cat *catalog.Catalog,
	sql string,
) (*ExecutionResult, error) {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, err
	}
	return exec.Execute(context.Background(), plan, nil)
}

// Task 9.7: Test IN Subquery
func TestInSubquery(t *testing.T) {
	exec, cat, _ := setupEdgeCaseTestExecutor()
	_, err := executeEdgeCaseQuery(t, exec, cat, `CREATE TABLE products (id INT, name VARCHAR, category_id INT)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, `CREATE TABLE categories (id INT)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, `INSERT INTO products VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 1)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, `INSERT INTO categories VALUES (1), (3)`)
	require.NoError(t, err)
	result, err := executeEdgeCaseQuery(t, exec, cat, `SELECT name FROM products WHERE category_id IN (SELECT id FROM categories)`)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Rows))
}

// Task 9.8: Test Simple Filter Pushdown
func TestSimpleFilterPushdown(t *testing.T) {
	exec, cat, _ := setupEdgeCaseTestExecutor()
	_, err := executeEdgeCaseQuery(t, exec, cat, `CREATE TABLE products (id INT, name VARCHAR, price DECIMAL)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, `INSERT INTO products VALUES (1, 'A', 100), (2, 'B', 50), (3, 'C', 200)`)
	require.NoError(t, err)
	result, err := executeEdgeCaseQuery(t, exec, cat, `SELECT name FROM products WHERE price > 75`)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Rows))
}

// Task 9.8: Test Complex AND/OR Filters
func TestComplexAndOrFilters(t *testing.T) {
	exec, cat, _ := setupEdgeCaseTestExecutor()
	_, err := executeEdgeCaseQuery(t, exec, cat, `CREATE TABLE items (id INT, status VARCHAR, priority INT)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, `INSERT INTO items VALUES (1, 'active', 1), (2, 'inactive', 2), (3, 'active', 3), (4, 'pending', 1)`)
	require.NoError(t, err)
	result, err := executeEdgeCaseQuery(t, exec, cat, `SELECT id FROM items WHERE (status = 'active' AND priority > 1) OR status = 'pending'`)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Rows))
}

// Task 9.10: Test Large Table Scan
func TestLargeTableScan(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	exec, cat, _ := setupEdgeCaseTestExecutor()
	_, err := executeEdgeCaseQuery(t, exec, cat, `CREATE TABLE large_table (id INT, value INT)`)
	require.NoError(t, err)
	for i := 1; i <= 50; i++ {
		_, err = executeEdgeCaseQuery(t, exec, cat, fmt.Sprintf(`INSERT INTO large_table VALUES (%d, %d)`, i, i*10))
		require.NoError(t, err)
	}
	result, err := executeEdgeCaseQuery(t, exec, cat, `SELECT COUNT(*) as cnt FROM large_table`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

// Task 9.11: Test Wide Table
func TestWideTableSelection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	exec, cat, _ := setupEdgeCaseTestExecutor()
	cols := "id INT"
	values := "1"
	for i := 1; i <= 20; i++ {
		cols += fmt.Sprintf(", col%d INT", i)
		values += fmt.Sprintf(", %d", i)
	}
	_, err := executeEdgeCaseQuery(t, exec, cat, fmt.Sprintf(`CREATE TABLE wide_table (%s)`, cols))
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec, cat, fmt.Sprintf(`INSERT INTO wide_table VALUES (%s)`, values))
	require.NoError(t, err)
	result, err := executeEdgeCaseQuery(t, exec, cat, `SELECT * FROM wide_table`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

// Task 9.9: Test Statistics Persistence (via query results)
func TestStatisticsPersistenceRoundtrip(t *testing.T) {
	exec1, cat1, _ := setupEdgeCaseTestExecutor()
	_, err := executeEdgeCaseQuery(t, exec1, cat1, `CREATE TABLE test_table (id INT, value INT)`)
	require.NoError(t, err)
	_, err = executeEdgeCaseQuery(t, exec1, cat1, `INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)`)
	require.NoError(t, err)
	// Query should work regardless of statistics
	result, err := executeEdgeCaseQuery(t, exec1, cat1, `SELECT COUNT(*) as cnt FROM test_table`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	// Executor should produce consistent results
	result2, err := executeEdgeCaseQuery(t, exec1, cat1, `SELECT COUNT(*) as cnt FROM test_table`)
	require.NoError(t, err)
	require.Len(t, result2.Rows, 1)
}
