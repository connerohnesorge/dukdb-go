package executor

import (
	"context"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestTable creates a test table for testing.
func setupTestTable(t *testing.T, cat *catalog.Catalog, stor *storage.Storage) {
	// Create table in catalog
	cols := []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
		{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		{Name: "value", Type: dukdb.TYPE_DOUBLE, Nullable: true},
	}
	tableDef := catalog.NewTableDef("test_table", cols)
	tableDef.PrimaryKey = []int{0}
	err := cat.CreateTableInSchema("main", tableDef)
	require.NoError(t, err)

	// Create table in storage
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	_, err = stor.CreateTable("test_table", types)
	require.NoError(t, err)
}

// executeSQL parses, binds, plans, and executes a SQL statement.
func executeSQL(t *testing.T, exec *Executor, cat *catalog.Catalog, sql string) (*ExecutionResult, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	b := binder.NewBinder(cat)
	bound, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}

	plan, err := exec.planner.Plan(bound)
	if err != nil {
		return nil, err
	}

	return exec.Execute(context.Background(), plan, nil)
}

func TestPragmaDatabaseSize(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "PRAGMA database_size")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"database_size", "block_count", "table_count"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Contains(t, result.Rows[0], "table_count")
}

func TestPragmaTableInfo(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "PRAGMA table_info('test_table')")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}, result.Columns)
	require.Len(t, result.Rows, 3) // 3 columns

	// Check column info
	assert.Equal(t, "id", result.Rows[0]["name"])
	assert.Equal(t, "BIGINT", result.Rows[0]["type"])
	assert.Equal(t, int64(1), result.Rows[0]["notnull"]) // NOT NULL
	assert.Equal(t, int64(1), result.Rows[0]["pk"])      // Primary key

	assert.Equal(t, "name", result.Rows[1]["name"])
	assert.Equal(t, "VARCHAR", result.Rows[1]["type"])
	assert.Equal(t, int64(0), result.Rows[1]["notnull"]) // NULLABLE
}

func TestPragmaShowTables(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "PRAGMA tables")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"name"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "test_table", result.Rows[0]["name"])
}

func TestPragmaVersion(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	result, err := executeSQL(t, exec, cat, "PRAGMA version")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"library_version"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Contains(t, result.Rows[0]["library_version"].(string), "dukdb-go")
}

func TestPragmaFunctions(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	result, err := executeSQL(t, exec, cat, "PRAGMA functions")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"name", "type"}, result.Columns)
	require.True(t, len(result.Rows) > 0, "should have at least one function")
}

func TestPragmaDatabaseList(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	result, err := executeSQL(t, exec, cat, "PRAGMA database_list")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"database_name", "path"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "main", result.Rows[0]["database_name"])
}

func TestPragmaCollations(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	result, err := executeSQL(t, exec, cat, "PRAGMA collations")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"collation_name"}, result.Columns)
	require.True(t, len(result.Rows) > 0, "should have at least one collation")
}

func TestPragmaMemoryLimit(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Get memory limit
	result, err := executeSQL(t, exec, cat, "PRAGMA memory_limit")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []string{"memory_limit"}, result.Columns)

	// Set memory limit
	result, err = executeSQL(t, exec, cat, "PRAGMA memory_limit = '8GB'")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestPragmaThreads(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Get threads
	result, err := executeSQL(t, exec, cat, "PRAGMA threads")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []string{"threads"}, result.Columns)

	// Set threads
	result, err = executeSQL(t, exec, cat, "PRAGMA threads = 8")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestPragmaEnableDisableProfiling(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Enable profiling
	result, err := executeSQL(t, exec, cat, "PRAGMA enable_profiling")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Disable profiling
	result, err = executeSQL(t, exec, cat, "PRAGMA disable_profiling")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestExplainSelect(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM test_table")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"explain_plan"}, result.Columns)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)
	assert.Contains(t, planText, "Scan")
	assert.Contains(t, planText, "test_table")
	// Verify cost annotations are present (task 8.1)
	assert.Contains(t, planText, "(cost=")
	assert.Contains(t, planText, "rows=")
	assert.Contains(t, planText, "width=")
}

func TestExplainAnalyze(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "EXPLAIN ANALYZE SELECT * FROM test_table")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"explain_plan"}, result.Columns)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)
	// Verify cost annotations and actual metrics are present (task 8.3)
	assert.Contains(t, planText, "(cost=")
	assert.Contains(t, planText, "rows=")
	assert.Contains(t, planText, "(actual rows=")
	assert.Contains(t, planText, "time=")
}

// TestExplainWithCostAnnotations tests that EXPLAIN output includes cost estimates (task 8.4)
func TestExplainWithCostAnnotations(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	testCases := []struct {
		name     string
		sql      string
		contains []string
	}{
		{
			name: "simple scan",
			sql:  "EXPLAIN SELECT * FROM test_table",
			contains: []string{
				"Scan: test_table",
				"(cost=",
				"rows=",
				"width=",
			},
		},
		{
			name: "scan with filter",
			sql:  "EXPLAIN SELECT * FROM test_table WHERE id > 1",
			contains: []string{
				"Filter",
				"Scan",
				"(cost=",
				"rows=",
			},
		},
		{
			name: "scan with projection",
			sql:  "EXPLAIN SELECT id, name FROM test_table",
			contains: []string{
				"Project",
				"columns",
				"(cost=",
				"rows=",
			},
		},
		{
			name: "scan with sort",
			sql:  "EXPLAIN SELECT * FROM test_table ORDER BY id",
			contains: []string{
				"Sort",
				"Scan",
				"(cost=",
				"rows=",
			},
		},
		{
			name: "scan with limit",
			sql:  "EXPLAIN SELECT * FROM test_table LIMIT 10",
			contains: []string{
				"Limit",
				"Scan",
				"(cost=",
				"rows=",
			},
		},
		{
			name: "scan with distinct",
			sql:  "EXPLAIN SELECT DISTINCT id FROM test_table",
			contains: []string{
				"Distinct",
				"(cost=",
				"rows=",
			},
		},
		{
			name: "dummy scan",
			sql:  "EXPLAIN SELECT 1",
			contains: []string{
				"DummyScan",
				"(cost=",
				"rows=",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executeSQL(t, exec, cat, tc.sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Len(t, result.Rows, 1)

			planText := result.Rows[0]["explain_plan"].(string)
			for _, expected := range tc.contains {
				assert.Contains(t, planText, expected, "Expected %q in plan: %s", expected, planText)
			}
		})
	}
}

// TestExplainAnalyzeActualVsEstimated tests EXPLAIN ANALYZE shows both actual and estimated values (task 8.3)
func TestExplainAnalyzeActualVsEstimated(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTableWithData(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "EXPLAIN ANALYZE SELECT * FROM stats_table")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)

	// Should contain estimated metrics
	assert.Contains(t, planText, "(cost=", "Should contain estimated startup..total cost")
	assert.Contains(t, planText, "rows=", "Should contain estimated rows")
	assert.Contains(t, planText, "width=", "Should contain estimated width")

	// Should contain actual metrics
	assert.Contains(t, planText, "(actual rows=", "Should contain actual rows")
	assert.Contains(t, planText, "time=", "Should contain actual time")
	assert.Contains(t, planText, "ms)", "Time should be in milliseconds")
}

// TestExplainCostFormat tests the cost annotation format (task 8.2)
func TestExplainCostFormat(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM test_table")
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)

	// Verify format: (cost=startup..total rows=N width=N)
	// Cost should be in format X.XX..Y.YY
	assert.Regexp(t, `cost=\d+\.\d+\.\.\d+\.\d+`, planText, "Cost should be in format startup..total")
	assert.Regexp(t, `rows=\d+`, planText, "Rows should be a number")
	assert.Regexp(t, `width=\d+`, planText, "Width should be a number")
}

// TestExplainWithJoin tests EXPLAIN for join operations
func TestExplainWithJoin(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create two tables
	_, err := executeSQL(t, exec, cat, "CREATE TABLE customers (id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = executeSQL(t, exec, cat, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE)")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"EXPLAIN SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id")
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)

	// Should contain join operator with cost
	assert.True(t, strings.Contains(planText, "HashJoin") || strings.Contains(planText, "NestedLoopJoin"),
		"Plan should contain a join operator: %s", planText)
	assert.Contains(t, planText, "(cost=", "Join should have cost annotation")
	assert.Contains(t, planText, "rows=", "Join should have estimated rows")

	// Should contain scans for both tables
	assert.Contains(t, planText, "customers", "Should reference customers table")
	assert.Contains(t, planText, "orders", "Should reference orders table")
}

// TestExplainWithAggregate tests EXPLAIN for aggregate operations
func TestExplainWithAggregate(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTableWithData(t, cat, stor)

	result, err := executeSQL(t, exec, cat,
		"EXPLAIN SELECT name, COUNT(*) FROM stats_table GROUP BY name")
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)

	// Should contain aggregate operator with cost
	assert.Contains(t, planText, "HashAggregate", "Plan should contain HashAggregate: %s", planText)
	assert.Contains(t, planText, "(cost=", "Aggregate should have cost annotation")
	assert.Contains(t, planText, "rows=", "Aggregate should have estimated rows")
}

func TestVacuum(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	// VACUUM entire database
	result, err := executeSQL(t, exec, cat, "VACUUM")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Rows, 0) // No rows returned

	// VACUUM specific table
	result, err = executeSQL(t, exec, cat, "VACUUM test_table")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestVacuumTableNotFound(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	_, err := executeSQL(t, exec, cat, "VACUUM nonexistent_table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestAnalyze(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor)

	// ANALYZE entire database
	result, err := executeSQL(t, exec, cat, "ANALYZE")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Rows, 0) // No rows returned

	// ANALYZE specific table
	result, err = executeSQL(t, exec, cat, "ANALYZE test_table")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestAnalyzeTableNotFound(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	_, err := executeSQL(t, exec, cat, "ANALYZE nonexistent_table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCheckpoint(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Simple CHECKPOINT
	result, err := executeSQL(t, exec, cat, "CHECKPOINT")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Rows, 0) // No rows returned

	// CHECKPOINT FORCE
	result, err = executeSQL(t, exec, cat, "CHECKPOINT FORCE")
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestUnknownPragma(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Unknown pragma should return empty result, not error
	result, err := executeSQL(t, exec, cat, "PRAGMA unknown_pragma")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Rows, 0)
}

func TestPragmaTableInfoNotFound(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	_, err := executeSQL(t, exec, cat, "PRAGMA table_info('nonexistent_table')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// setupTestTableWithData creates a test table with data for statistics testing.
func setupTestTableWithData(t *testing.T, cat *catalog.Catalog, stor *storage.Storage) {
	// Create table in catalog
	cols := []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
		{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		{Name: "value", Type: dukdb.TYPE_DOUBLE, Nullable: true},
	}
	tableDef := catalog.NewTableDef("stats_table", cols)
	tableDef.PrimaryKey = []int{0}
	err := cat.CreateTableInSchema("main", tableDef)
	require.NoError(t, err)

	// Create table in storage
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	table, err := stor.CreateTable("stats_table", types)
	require.NoError(t, err)

	// Insert test data
	chunk := storage.NewDataChunkWithCapacity(types, 100)
	chunk.AppendRow([]any{int64(1), "Alice", float64(10.5)})
	chunk.AppendRow([]any{int64(2), "Bob", float64(20.3)})
	chunk.AppendRow([]any{int64(3), "Charlie", float64(15.7)})
	chunk.AppendRow([]any{int64(4), "Alice", nil})
	chunk.AppendRow([]any{int64(5), nil, float64(25.0)})
	err = table.AppendChunk(chunk)
	require.NoError(t, err)
}

func TestAnalyzeCollectsStatistics(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTableWithData(t, cat, stor)

	// Before ANALYZE, there should be no statistics
	tableDef, ok := cat.GetTableInSchema("main", "stats_table")
	require.True(t, ok)
	assert.Nil(t, tableDef.Statistics)

	// Run ANALYZE
	result, err := executeSQL(t, exec, cat, "ANALYZE stats_table")
	require.NoError(t, err)
	require.NotNil(t, result)

	// After ANALYZE, statistics should be populated
	tableDef, ok = cat.GetTableInSchema("main", "stats_table")
	require.True(t, ok)
	require.NotNil(t, tableDef.Statistics, "statistics should be populated after ANALYZE")

	stats := tableDef.Statistics
	assert.Equal(t, int64(5), stats.RowCount)
	assert.True(t, stats.IsAnalyzed())
	assert.Equal(t, 3, len(stats.Columns))

	// Check ID column stats
	idStats := stats.GetColumnStats("id")
	require.NotNil(t, idStats)
	assert.Equal(t, float64(0), idStats.NullFraction) // No nulls
	assert.Equal(t, int64(5), idStats.DistinctCount)  // All unique

	// Check name column stats
	nameStats := stats.GetColumnStats("name")
	require.NotNil(t, nameStats)
	assert.Equal(t, 0.2, nameStats.NullFraction) // 1 out of 5 is null
	assert.Equal(t, int64(3), nameStats.DistinctCount)

	// Check value column stats
	valueStats := stats.GetColumnStats("value")
	require.NotNil(t, valueStats)
	assert.Equal(t, 0.2, valueStats.NullFraction) // 1 out of 5 is null
	assert.Equal(t, int64(4), valueStats.DistinctCount)
}

func TestAnalyzeAllTables(t *testing.T) {
	exec, cat, stor := setupTestExecutor()

	// Create multiple tables
	setupTestTable(t, cat, stor)
	setupTestTableWithData(t, cat, stor)

	// Run ANALYZE on all tables
	result, err := executeSQL(t, exec, cat, "ANALYZE")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Both tables should have statistics
	testTable, ok := cat.GetTableInSchema("main", "test_table")
	require.True(t, ok)
	require.NotNil(t, testTable.Statistics, "test_table should have statistics after ANALYZE")

	statsTable, ok := cat.GetTableInSchema("main", "stats_table")
	require.True(t, ok)
	require.NotNil(t, statsTable.Statistics, "stats_table should have statistics after ANALYZE")
}

func TestAnalyzeEmptyTable(t *testing.T) {
	exec, cat, stor := setupTestExecutor()
	setupTestTable(t, cat, stor) // Creates empty table

	// Run ANALYZE on empty table
	result, err := executeSQL(t, exec, cat, "ANALYZE test_table")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Statistics should be collected for empty table
	tableDef, ok := cat.GetTableInSchema("main", "test_table")
	require.True(t, ok)
	require.NotNil(t, tableDef.Statistics)

	stats := tableDef.Statistics
	assert.Equal(t, int64(0), stats.RowCount)
	assert.True(t, stats.IsAnalyzed())

	// Column stats should exist but have appropriate values for empty table
	idStats := stats.GetColumnStats("id")
	require.NotNil(t, idStats)
	assert.Equal(t, int64(0), idStats.DistinctCount)
	assert.Nil(t, idStats.MinValue)
	assert.Nil(t, idStats.MaxValue)
}
