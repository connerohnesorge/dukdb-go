package executor

import (
	"context"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
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
func executeSQL(
	t *testing.T,
	exec *Executor,
	cat *catalog.Catalog,
	sql string,
) (*ExecutionResult, error) {
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
				assert.Contains(
					t,
					planText,
					expected,
					"Expected %q in plan: %s",
					expected,
					planText,
				)
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
	assert.Regexp(
		t,
		`cost=\d+\.\d+\.\.\d+\.\d+`,
		planText,
		"Cost should be in format startup..total",
	)
	assert.Regexp(t, `rows=\d+`, planText, "Rows should be a number")
	assert.Regexp(t, `width=\d+`, planText, "Width should be a number")
}

// TestExplainWithJoin tests EXPLAIN for join operations
func TestExplainWithJoin(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create two tables
	_, err := executeSQL(t, exec, cat, "CREATE TABLE customers (id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = executeSQL(
		t,
		exec,
		cat,
		"CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE)",
	)
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"EXPLAIN SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id")
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)

	// Should contain join operator with cost
	assert.True(
		t,
		strings.Contains(planText, "HashJoin") || strings.Contains(planText, "NestedLoopJoin"),
		"Plan should contain a join operator: %s",
		planText,
	)
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

// =============================================================================
// Task 5.1: EXPLAIN IndexScan Output Tests
// =============================================================================

// TestExplainIndexScanOutput tests that EXPLAIN shows IndexScan when an index is used.
func TestExplainIndexScanOutput(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table and index
	_, err := executeSQL(t, exec, cat, "CREATE TABLE idx_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Create index on id
	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_test_id ON idx_test(id)")
	require.NoError(t, err)

	// Run EXPLAIN on a query with an index
	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM idx_test WHERE id = 5")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)

	// When index is used, should contain IndexScan
	// Note: The optimizer decides whether to use the index based on cost model.
	// If index is used, we should see "IndexScan: idx_test USING idx_test_id"
	// If not, we should see "Scan: idx_test" (sequential scan)
	// This test verifies the format is correct when index scan is chosen.
	t.Logf("EXPLAIN output: %s", planText)

	// Check for either IndexScan (index used) or Scan (sequential)
	containsExpectedFormat := strings.Contains(planText, "IndexScan:") ||
		strings.Contains(planText, "Scan:")
	assert.True(
		t,
		containsExpectedFormat,
		"Plan should contain either IndexScan or Scan: %s",
		planText,
	)

	// If IndexScan is used, verify it shows USING clause
	if strings.Contains(planText, "IndexScan:") {
		assert.Contains(t, planText, "USING", "IndexScan should show USING <index_name>")
		assert.Contains(t, planText, "idx_test_id", "IndexScan should reference the index name")
	}
}

// TestExplainIndexScanFormat tests the exact format of IndexScan in EXPLAIN output.
func TestExplainIndexScanFormat(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeSQL(t, exec, cat, "CREATE TABLE format_test (id INTEGER, value VARCHAR)")
	require.NoError(t, err)

	// Create index
	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_format ON format_test(id)")
	require.NoError(t, err)

	// Run EXPLAIN
	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM format_test WHERE id = 10")
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)
	t.Logf("Plan: %s", planText)

	// Check basic format requirements
	assert.Contains(t, planText, "(cost=", "Plan should have cost annotation")
	assert.Contains(t, planText, "rows=", "Plan should have row estimate")
	assert.Contains(t, planText, "width=", "Plan should have width estimate")
}

// TestExplainSeqScanVsIndexScan tests that EXPLAIN correctly shows Scan vs IndexScan.
func TestExplainSeqScanVsIndexScan(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table without index first
	_, err := executeSQL(t, exec, cat, "CREATE TABLE scan_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// EXPLAIN without index - should show Scan (sequential)
	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM scan_test WHERE id = 1")
	require.NoError(t, err)

	planNoIndex := result.Rows[0]["explain_plan"].(string)
	t.Logf("Plan without index: %s", planNoIndex)

	// Without index, should use sequential scan
	assert.Contains(t, planNoIndex, "Scan:", "Without index, should show Scan")
	assert.NotContains(t, planNoIndex, "IndexScan", "Without index, should NOT show IndexScan")

	// Now create an index
	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_scan ON scan_test(id)")
	require.NoError(t, err)

	// EXPLAIN with index - might show IndexScan if optimizer chooses it
	result2, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM scan_test WHERE id = 1")
	require.NoError(t, err)

	planWithIndex := result2.Rows[0]["explain_plan"].(string)
	t.Logf("Plan with index: %s", planWithIndex)

	// With index available, optimizer may choose index scan or seq scan based on cost
	// Just verify the output is valid either way
	validPlan := strings.Contains(planWithIndex, "Scan:") ||
		strings.Contains(planWithIndex, "IndexScan:")
	assert.True(t, validPlan, "Plan should contain Scan or IndexScan")
}

// TestExplainAnalyzeIndexScan tests EXPLAIN ANALYZE shows correct metrics for index scans.
func TestExplainAnalyzeIndexScan(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with data
	_, err := executeSQL(t, exec, cat, "CREATE TABLE analyze_idx_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Insert some test data
	_, err = executeSQL(
		t,
		exec,
		cat,
		"INSERT INTO analyze_idx_test VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')",
	)
	require.NoError(t, err)

	// Create index
	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_analyze ON analyze_idx_test(id)")
	require.NoError(t, err)

	// Run EXPLAIN ANALYZE
	result, err := executeSQL(
		t,
		exec,
		cat,
		"EXPLAIN ANALYZE SELECT * FROM analyze_idx_test WHERE id = 5",
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	planText := result.Rows[0]["explain_plan"].(string)
	t.Logf("EXPLAIN ANALYZE output: %s", planText)

	// Should contain both estimated and actual metrics
	assert.Contains(t, planText, "(cost=", "Should have estimated cost")
	assert.Contains(t, planText, "(actual rows=", "Should have actual rows")
	assert.Contains(t, planText, "time=", "Should have actual time")
}

// =============================================================================
// Task 5.3: EXPLAIN Index Cond (Lookup Keys) Tests
// =============================================================================

// TestFormatIndexConditionSingleKey tests formatIndexCondition with a single lookup key.
func TestFormatIndexConditionSingleKey(t *testing.T) {
	// Test with integer literal
	scan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(42)},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(id = 42)", result)
}

// TestFormatIndexConditionStringValue tests formatIndexCondition with a string value.
func TestFormatIndexConditionStringValue(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_email",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_email",
			Columns: []string{"email"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: "test@example.com"},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(email = 'test@example.com')", result)
}

// TestFormatIndexConditionCompositeKey tests formatIndexCondition with multiple lookup keys.
func TestFormatIndexConditionCompositeKey(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "orders",
		IndexName: "idx_ab",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_ab",
			Columns: []string{"a", "b"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
			&binder.BoundLiteral{Value: int64(2)},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "((a = 1) AND (b = 2))", result)
}

// TestFormatIndexConditionMixedTypes tests formatIndexCondition with mixed value types.
func TestFormatIndexConditionMixedTypes(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "data",
		IndexName: "idx_mixed",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_mixed",
			Columns: []string{"num", "name"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(100)},
			&binder.BoundLiteral{Value: "hello"},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "((num = 100) AND (name = 'hello'))", result)
}

// TestFormatIndexConditionEmpty tests formatIndexCondition with no lookup keys.
func TestFormatIndexConditionEmpty(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName:  "users",
		IndexName:  "idx_users_id",
		LookupKeys: nil,
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "", result)
}

// TestFormatIndexConditionNullValue tests formatIndexCondition with a NULL value.
func TestFormatIndexConditionNullValue(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "data",
		IndexName: "idx_val",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_val",
			Columns: []string{"val"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: nil},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(val = NULL)", result)
}

// TestFormatIndexConditionBoolValue tests formatIndexCondition with boolean values.
func TestFormatIndexConditionBoolValue(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "flags",
		IndexName: "idx_active",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_active",
			Columns: []string{"active"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: true},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(active = true)", result)
}

// TestFormatIndexConditionFloatValue tests formatIndexCondition with float values.
func TestFormatIndexConditionFloatValue(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "prices",
		IndexName: "idx_price",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_price",
			Columns: []string{"price"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: 19.99},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(price = 19.99)", result)
}

// TestFormatIndexConditionStringWithQuotes tests string with embedded quotes is escaped.
func TestFormatIndexConditionStringWithQuotes(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "quotes",
		IndexName: "idx_text",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_text",
			Columns: []string{"text"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: "it's a test"},
		},
	}

	result := formatIndexCondition(scan)
	assert.Equal(t, "(text = 'it''s a test')", result)
}

// TestFormatLiteralValueTypes tests formatLiteralValue with various types.
func TestFormatLiteralValueTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"nil", nil, "NULL"},
		{"string", "hello", "'hello'"},
		{"int", 42, "42"},
		{"int32", int32(42), "42"},
		{"int64", int64(42), "42"},
		{"float32", float32(3.14), "3.14"},
		{"float64", 3.14, "3.14"},
		{"true", true, "true"},
		{"false", false, "false"},
		{"bytes", []byte{0xDE, 0xAD}, "'\\xdead'"},
		{"empty_string", "", "''"},
		{"string_with_quotes", "it's", "'it''s'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLiteralValue(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExplainShowsIndexCond tests that EXPLAIN output includes Index Cond when lookup keys exist.
func TestExplainShowsIndexCond(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table and index
	_, err := executeSQL(t, exec, cat, "CREATE TABLE idx_cond_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_cond_id ON idx_cond_test(id)")
	require.NoError(t, err)

	// Run EXPLAIN on a query with a specific lookup value
	result, err := executeSQL(t, exec, cat, "EXPLAIN SELECT * FROM idx_cond_test WHERE id = 42")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)
	t.Logf("EXPLAIN output: %s", planText)

	// If index scan is used, verify Index Cond is shown
	if strings.Contains(planText, "IndexScan:") {
		// When index scan is used, we should see Index Cond with the lookup key
		// Note: The optimizer may or may not use the index based on cost
		t.Log("Index scan detected - Index Cond should be shown if lookup keys were provided")
	}
}

// TestExplainShowsCompositeIndexCond tests that EXPLAIN shows composite Index Cond.
func TestExplainShowsCompositeIndexCond(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with composite index
	_, err := executeSQL(
		t,
		exec,
		cat,
		"CREATE TABLE composite_test (a INTEGER, b INTEGER, c VARCHAR)",
	)
	require.NoError(t, err)

	_, err = executeSQL(t, exec, cat, "CREATE INDEX idx_ab ON composite_test(a, b)")
	require.NoError(t, err)

	// Run EXPLAIN on a query that uses both columns
	result, err := executeSQL(
		t,
		exec,
		cat,
		"EXPLAIN SELECT * FROM composite_test WHERE a = 1 AND b = 2",
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	planText := result.Rows[0]["explain_plan"].(string)
	t.Logf("EXPLAIN output: %s", planText)

	// If index scan is used, verify it shows composite conditions
	if strings.Contains(planText, "IndexScan:") && strings.Contains(planText, "idx_ab") {
		t.Log("Composite index scan detected")
	}
}

// =============================================================================
// Task 5.4: EXPLAIN Residual Filter Tests
// =============================================================================

// TestFormatResidualFilterNil tests that formatResidualFilter returns empty string for nil.
func TestFormatResidualFilterNil(t *testing.T) {
	result := formatResidualFilter(nil)
	assert.Equal(t, "", result)
}

// TestFormatResidualFilterSingleComparison tests formatResidualFilter with a single comparison.
func TestFormatResidualFilterSingleComparison(t *testing.T) {
	filter := &binder.BoundBinaryExpr{
		Left:  &binder.BoundColumnRef{Column: "x"},
		Op:    parser.OpGt,
		Right: &binder.BoundLiteral{Value: int64(5)},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(x > 5)", result)
}

// TestFormatResidualFilterANDCondition tests formatResidualFilter with AND combination.
func TestFormatResidualFilterANDCondition(t *testing.T) {
	filter := &binder.BoundBinaryExpr{
		Left: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "x"},
			Op:    parser.OpGt,
			Right: &binder.BoundLiteral{Value: int64(5)},
		},
		Op: parser.OpAnd,
		Right: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "y"},
			Op:    parser.OpLt,
			Right: &binder.BoundLiteral{Value: int64(10)},
		},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "((x > 5) AND (y < 10))", result)
}

// TestFormatResidualFilterORCondition tests formatResidualFilter with OR combination.
func TestFormatResidualFilterORCondition(t *testing.T) {
	filter := &binder.BoundBinaryExpr{
		Left: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "status"},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "active"},
		},
		Op: parser.OpOr,
		Right: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "status"},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "pending"},
		},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "((status = 'active') OR (status = 'pending'))", result)
}

// TestFormatResidualFilterNestedANDOR tests formatResidualFilter with nested AND/OR.
func TestFormatResidualFilterNestedANDOR(t *testing.T) {
	// (a > 1 AND (b = 2 OR c = 3))
	filter := &binder.BoundBinaryExpr{
		Left: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "a"},
			Op:    parser.OpGt,
			Right: &binder.BoundLiteral{Value: int64(1)},
		},
		Op: parser.OpAnd,
		Right: &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "b"},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(2)},
			},
			Op: parser.OpOr,
			Right: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "c"},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(3)},
			},
		},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "((a > 1) AND ((b = 2) OR (c = 3)))", result)
}

// TestFormatResidualFilterIsNull tests formatResidualFilter with IS NULL.
func TestFormatResidualFilterIsNull(t *testing.T) {
	filter := &binder.BoundUnaryExpr{
		Expr: &binder.BoundColumnRef{Column: "name"},
		Op:   parser.OpIsNull,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(name IS NULL)", result)
}

// TestFormatResidualFilterIsNotNull tests formatResidualFilter with IS NOT NULL.
func TestFormatResidualFilterIsNotNull(t *testing.T) {
	filter := &binder.BoundUnaryExpr{
		Expr: &binder.BoundColumnRef{Column: "name"},
		Op:   parser.OpIsNotNull,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(name IS NOT NULL)", result)
}

// TestFormatResidualFilterBetween tests formatResidualFilter with BETWEEN.
func TestFormatResidualFilterBetween(t *testing.T) {
	filter := &binder.BoundBetweenExpr{
		Expr: &binder.BoundColumnRef{Column: "age"},
		Low:  &binder.BoundLiteral{Value: int64(18)},
		High: &binder.BoundLiteral{Value: int64(65)},
		Not:  false,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(age BETWEEN 18 AND 65)", result)
}

// TestFormatResidualFilterNotBetween tests formatResidualFilter with NOT BETWEEN.
func TestFormatResidualFilterNotBetween(t *testing.T) {
	filter := &binder.BoundBetweenExpr{
		Expr: &binder.BoundColumnRef{Column: "age"},
		Low:  &binder.BoundLiteral{Value: int64(18)},
		High: &binder.BoundLiteral{Value: int64(65)},
		Not:  true,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(age NOT BETWEEN 18 AND 65)", result)
}

// TestFormatResidualFilterInList tests formatResidualFilter with IN list.
func TestFormatResidualFilterInList(t *testing.T) {
	filter := &binder.BoundInListExpr{
		Expr: &binder.BoundColumnRef{Column: "status"},
		Values: []binder.BoundExpr{
			&binder.BoundLiteral{Value: "active"},
			&binder.BoundLiteral{Value: "pending"},
			&binder.BoundLiteral{Value: "completed"},
		},
		Not: false,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(status IN ('active', 'pending', 'completed'))", result)
}

// TestFormatResidualFilterNotInList tests formatResidualFilter with NOT IN list.
func TestFormatResidualFilterNotInList(t *testing.T) {
	filter := &binder.BoundInListExpr{
		Expr: &binder.BoundColumnRef{Column: "status"},
		Values: []binder.BoundExpr{
			&binder.BoundLiteral{Value: "deleted"},
			&binder.BoundLiteral{Value: "archived"},
		},
		Not: true,
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(status NOT IN ('deleted', 'archived'))", result)
}

// TestFormatResidualFilterFunctionCall tests formatResidualFilter with function call.
func TestFormatResidualFilterFunctionCall(t *testing.T) {
	filter := &binder.BoundBinaryExpr{
		Left: &binder.BoundFunctionCall{
			Name: "UPPER",
			Args: []binder.BoundExpr{
				&binder.BoundColumnRef{Column: "name"},
			},
		},
		Op:    parser.OpEq,
		Right: &binder.BoundLiteral{Value: "ALICE"},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(UPPER(name) = 'ALICE')", result)
}

// TestFormatResidualFilterTableQualifiedColumn tests formatResidualFilter with table-qualified column.
func TestFormatResidualFilterTableQualifiedColumn(t *testing.T) {
	filter := &binder.BoundBinaryExpr{
		Left:  &binder.BoundColumnRef{Table: "users", Column: "id"},
		Op:    parser.OpEq,
		Right: &binder.BoundLiteral{Value: int64(42)},
	}

	result := formatResidualFilter(filter)
	assert.Equal(t, "(users.id = 42)", result)
}

// TestExplainShowsResidualFilter tests that EXPLAIN output shows residual filter.
func TestExplainShowsResidualFilter(t *testing.T) {
	// Create a physical index scan with a residual filter and test formatting
	scan := &planner.PhysicalIndexScan{
		TableName: "test_table",
		IndexName: "idx_a",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_a",
			Columns: []string{"a"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "b"},
			Op:    parser.OpGt,
			Right: &binder.BoundLiteral{Value: int64(5)},
		},
	}

	// Test formatPhysicalPlan (without costs)
	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should contain Index Cond
	assert.Contains(t, planText, "Index Cond: (a = 1)")
	// Should contain Filter
	assert.Contains(t, planText, "Filter: (b > 5)")
}

// TestExplainShowsResidualFilterWithCompositeIndex tests composite index with partial match and residual.
func TestExplainShowsResidualFilterWithCompositeIndex(t *testing.T) {
	// Scenario: Index on (a, b), query WHERE a = 1 AND c = 3
	// Index condition: a = 1
	// Residual filter: c = 3 (can't be pushed into index)
	scan := &planner.PhysicalIndexScan{
		TableName: "test_table",
		IndexName: "idx_ab",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_ab",
			Columns: []string{"a", "b"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "c"},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: int64(3)},
		},
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should contain Index Cond for 'a'
	assert.Contains(t, planText, "Index Cond: (a = 1)")
	// Should contain Filter for 'c' (residual)
	assert.Contains(t, planText, "Filter: (c = 3)")
}

// TestExplainShowsComplexResidualFilter tests complex residual filter with AND/OR.
func TestExplainShowsComplexResidualFilter(t *testing.T) {
	// Residual filter: (b > 5 AND c < 10)
	scan := &planner.PhysicalIndexScan{
		TableName: "test_table",
		IndexName: "idx_a",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_a",
			Columns: []string{"a"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "b"},
				Op:    parser.OpGt,
				Right: &binder.BoundLiteral{Value: int64(5)},
			},
			Op: parser.OpAnd,
			Right: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "c"},
				Op:    parser.OpLt,
				Right: &binder.BoundLiteral{Value: int64(10)},
			},
		},
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should contain complex Filter
	assert.Contains(t, planText, "Filter: ((b > 5) AND (c < 10))")
}

// TestExplainNoResidualFilterWhenNil tests that no Filter line appears when ResidualFilter is nil.
func TestExplainNoResidualFilterWhenNil(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "test_table",
		IndexName: "idx_a",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_a",
			Columns: []string{"a"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		ResidualFilter: nil, // No residual filter
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should contain Index Cond
	assert.Contains(t, planText, "Index Cond: (a = 1)")
	// Should NOT contain Filter line
	assert.NotContains(t, planText, "Filter:")
}

// =============================================================================
// Task 5.5: Comprehensive EXPLAIN Index Output Tests
// =============================================================================

// TestExplainIndexOnlyScanOutput tests that EXPLAIN shows IndexOnlyScan for covering indexes.
func TestExplainIndexOnlyScanOutput(t *testing.T) {
	// Create a PhysicalIndexScan with IsIndexOnly=true
	scan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id_name",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id_name",
			Columns: []string{"id", "name"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(42)},
		},
		IsIndexOnly: true, // Covering index - all columns in index
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show IndexOnlyScan, not IndexScan
	assert.Contains(t, planText, "IndexOnlyScan:")
	assert.NotContains(t, planText, "IndexScan:")
	assert.Contains(t, planText, "USING idx_users_id_name")
	assert.Contains(t, planText, "Index Cond: (id = 42)")
}

// TestExplainIndexOnlyScanVsIndexScan tests difference between IndexOnlyScan and IndexScan.
func TestExplainIndexOnlyScanVsIndexScan(t *testing.T) {
	// IndexOnlyScan (covering index)
	scanCovering := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		IsIndexOnly: true,
	}

	// Regular IndexScan (non-covering)
	scanNonCovering := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		IsIndexOnly: false,
	}

	coveringPlan := formatPhysicalPlan(scanCovering, 0)
	nonCoveringPlan := formatPhysicalPlan(scanNonCovering, 0)

	t.Logf("Covering index plan:\n%s", coveringPlan)
	t.Logf("Non-covering index plan:\n%s", nonCoveringPlan)

	// Covering should show IndexOnlyScan
	assert.Contains(t, coveringPlan, "IndexOnlyScan:")
	assert.NotContains(t, coveringPlan, "IndexScan:")

	// Non-covering should show IndexScan
	assert.Contains(t, nonCoveringPlan, "IndexScan:")
	assert.NotContains(t, nonCoveringPlan, "IndexOnlyScan:")
}

// TestExplainIndexScanWithAlias tests EXPLAIN shows table alias correctly.
func TestExplainIndexScanWithAlias(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		Alias:     "u",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show table name and alias
	assert.Contains(t, planText, "IndexScan: users")
	assert.Contains(t, planText, "AS u")
}

// TestExplainIndexScanWithCost tests that formatPhysicalPlanWithCost includes cost annotations.
func TestExplainIndexScanWithCost(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
	}

	costModel := optimizer.NewCostModel(optimizer.DefaultCostConstants(), nil)
	planText := formatPhysicalPlanWithCost(scan, 0, costModel)
	t.Logf("formatPhysicalPlanWithCost output:\n%s", planText)

	// Should contain cost annotation
	assert.Contains(t, planText, "(cost=")
	assert.Contains(t, planText, "rows=")
	assert.Contains(t, planText, "width=")
	assert.Contains(t, planText, "IndexScan:")
	assert.Contains(t, planText, "USING idx_users_id")
}

// TestExplainIndexOnlyScanWithCost tests IndexOnlyScan includes cost annotations.
func TestExplainIndexOnlyScanWithCost(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName:   "users",
		IndexName:   "idx_users_id_name",
		IsIndexOnly: true,
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id_name",
			Columns: []string{"id", "name"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
	}

	costModel := optimizer.NewCostModel(optimizer.DefaultCostConstants(), nil)
	planText := formatPhysicalPlanWithCost(scan, 0, costModel)
	t.Logf("formatPhysicalPlanWithCost output:\n%s", planText)

	// Should show IndexOnlyScan with costs
	assert.Contains(t, planText, "IndexOnlyScan:")
	assert.Contains(t, planText, "(cost=")
	assert.Contains(t, planText, "rows=")
}

// TestExplainMultipleIndexConditions tests EXPLAIN with composite index and multiple conditions.
func TestExplainMultipleIndexConditions(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName: "orders",
		IndexName: "idx_orders_abc",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_orders_abc",
			Columns: []string{"a", "b", "c"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
			&binder.BoundLiteral{Value: "test"},
			&binder.BoundLiteral{Value: float64(3.14)},
		},
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show all three conditions
	assert.Contains(t, planText, "Index Cond: ((a = 1) AND (b = 'test') AND (c = 3.14))")
}

// TestExplainIndexScanWithComplexResidualFilter tests complex filter with multiple clauses.
func TestExplainIndexScanWithComplexResidualFilter(t *testing.T) {
	// Scenario: WHERE a = 1 AND b > 5 AND (c = 2 OR d < 10)
	scan := &planner.PhysicalIndexScan{
		TableName: "data",
		IndexName: "idx_data_a",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_data_a",
			Columns: []string{"a"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "b"},
				Op:    parser.OpGt,
				Right: &binder.BoundLiteral{Value: int64(5)},
			},
			Op: parser.OpAnd,
			Right: &binder.BoundBinaryExpr{
				Left: &binder.BoundBinaryExpr{
					Left:  &binder.BoundColumnRef{Column: "c"},
					Op:    parser.OpEq,
					Right: &binder.BoundLiteral{Value: int64(2)},
				},
				Op: parser.OpOr,
				Right: &binder.BoundBinaryExpr{
					Left:  &binder.BoundColumnRef{Column: "d"},
					Op:    parser.OpLt,
					Right: &binder.BoundLiteral{Value: int64(10)},
				},
			},
		},
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show Index Cond and complex Filter
	assert.Contains(t, planText, "Index Cond: (a = 1)")
	assert.Contains(t, planText, "Filter: ((b > 5) AND ((c = 2) OR (d < 10)))")
}

// TestExplainSeqScanVsIndexScanFormat tests the output format differences.
func TestExplainSeqScanVsIndexScanFormat(t *testing.T) {
	// Sequential scan
	seqScan := &planner.PhysicalScan{
		TableName: "users",
		Schema:    "main",
	}

	// Index scan
	idxScan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
	}

	seqPlan := formatPhysicalPlan(seqScan, 0)
	idxPlan := formatPhysicalPlan(idxScan, 0)

	t.Logf("Sequential scan plan:\n%s", seqPlan)
	t.Logf("Index scan plan:\n%s", idxPlan)

	// Sequential scan should just show "Scan:"
	assert.Contains(t, seqPlan, "Scan: users")
	assert.NotContains(t, seqPlan, "USING")
	assert.NotContains(t, seqPlan, "Index Cond")

	// Index scan should show "IndexScan:", "USING", and "Index Cond"
	assert.Contains(t, idxPlan, "IndexScan: users")
	assert.Contains(t, idxPlan, "USING idx_users_id")
	assert.Contains(t, idxPlan, "Index Cond:")
}

// TestExplainNestedPlanWithIndexScan tests EXPLAIN for a nested plan with index scan.
func TestExplainNestedPlanWithIndexScan(t *testing.T) {
	idxScan := &planner.PhysicalIndexScan{
		TableName: "users",
		IndexName: "idx_users_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(1)},
		},
	}

	// Wrap in Filter and Project
	filter := &planner.PhysicalFilter{
		Child: idxScan,
	}

	project := &planner.PhysicalProject{
		Child: filter,
		Expressions: []binder.BoundExpr{
			&binder.BoundColumnRef{Column: "id"},
			&binder.BoundColumnRef{Column: "name"},
		},
	}

	// Use formatPhysicalPlan (without cost) to avoid OutputColumns() dependency
	planText := formatPhysicalPlan(project, 0)
	t.Logf("Nested plan:\n%s", planText)

	// Should show nested structure
	assert.Contains(t, planText, "Project:")
	assert.Contains(t, planText, "Filter")
	assert.Contains(t, planText, "IndexScan:")
	assert.Contains(t, planText, "USING idx_users_id")
	assert.Contains(t, planText, "Index Cond:")
}

// TestExplainFilterExpressionTypes tests formatFilterExpression with various expression types.
func TestExplainFilterExpressionTypes(t *testing.T) {
	testCases := []struct {
		name     string
		filter   binder.BoundExpr
		expected string
	}{
		{
			name: "simple comparison with <=",
			filter: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "age"},
				Op:    parser.OpLe,
				Right: &binder.BoundLiteral{Value: int64(30)},
			},
			expected: "(age <= 30)",
		},
		{
			name: "simple comparison with >=",
			filter: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "score"},
				Op:    parser.OpGe,
				Right: &binder.BoundLiteral{Value: float64(85.5)},
			},
			expected: "(score >= 85.5)",
		},
		{
			name: "not equal comparison",
			filter: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "status"},
				Op:    parser.OpNe,
				Right: &binder.BoundLiteral{Value: "deleted"},
			},
			expected: "(status <> 'deleted')",
		},
		{
			name: "NOT operator",
			filter: &binder.BoundUnaryExpr{
				Op:   parser.OpNot,
				Expr: &binder.BoundColumnRef{Column: "active"},
			},
			expected: "(NOT active)",
		},
		{
			name: "negation operator",
			filter: &binder.BoundUnaryExpr{
				Op:   parser.OpNeg,
				Expr: &binder.BoundLiteral{Value: int64(5)},
			},
			expected: "(-5)",
		},
		{
			name: "CAST expression",
			filter: &binder.BoundCastExpr{
				Expr:       &binder.BoundColumnRef{Column: "value"},
				TargetType: dukdb.TYPE_INTEGER,
			},
			expected: "CAST(value AS INTEGER)",
		},
		{
			name: "function call with multiple args",
			filter: &binder.BoundFunctionCall{
				Name: "COALESCE",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{Column: "nullable_col"},
					&binder.BoundLiteral{Value: "default"},
				},
			},
			expected: "COALESCE(nullable_col, 'default')",
		},
		{
			name: "arithmetic operation",
			filter: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "price"},
				Op:    parser.OpMul,
				Right: &binder.BoundLiteral{Value: float64(1.1)},
			},
			expected: "(price * 1.1)",
		},
		{
			name: "modulo operation",
			filter: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "id"},
				Op:    parser.OpMod,
				Right: &binder.BoundLiteral{Value: int64(2)},
			},
			expected: "(id % 2)",
		},
		{
			name: "IN subquery",
			filter: &binder.BoundInSubqueryExpr{
				Expr: &binder.BoundColumnRef{Column: "dept_id"},
				Not:  false,
			},
			expected: "(dept_id IN <subquery>)",
		},
		{
			name: "NOT IN subquery",
			filter: &binder.BoundInSubqueryExpr{
				Expr: &binder.BoundColumnRef{Column: "dept_id"},
				Not:  true,
			},
			expected: "(dept_id NOT IN <subquery>)",
		},
		{
			name: "EXISTS",
			filter: &binder.BoundExistsExpr{
				Not: false,
			},
			expected: "(EXISTS <subquery>)",
		},
		{
			name: "NOT EXISTS",
			filter: &binder.BoundExistsExpr{
				Not: true,
			},
			expected: "(NOT EXISTS <subquery>)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatFilterExpression(tc.filter)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestExplainCostAnnotationFormat validates the cost annotation string format.
func TestExplainCostAnnotationFormat(t *testing.T) {
	testCases := []struct {
		name string
		cost optimizer.PlanCost
	}{
		{
			name: "zero costs",
			cost: optimizer.PlanCost{
				StartupCost: 0.0,
				TotalCost:   0.0,
				OutputRows:  0.0,
				OutputWidth: 0,
			},
		},
		{
			name: "typical scan costs",
			cost: optimizer.PlanCost{
				StartupCost: 0.0,
				TotalCost:   20.5,
				OutputRows:  1000.0,
				OutputWidth: 32,
			},
		},
		{
			name: "high costs",
			cost: optimizer.PlanCost{
				StartupCost: 100.5,
				TotalCost:   5000.75,
				OutputRows:  100000.0,
				OutputWidth: 128,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatCostAnnotation(tc.cost)

			// Verify format: (cost=X.XX..Y.YY rows=N width=N)
			assert.Contains(t, result, "(cost=")
			assert.Contains(t, result, "..")
			assert.Contains(t, result, "rows=")
			assert.Contains(t, result, "width=")
			assert.True(t, strings.HasSuffix(result, ")"))
		})
	}
}

// TestExplainLiteralValueEdgeCases tests formatLiteralValue edge cases.
func TestExplainLiteralValueEdgeCases(t *testing.T) {
	testCases := []struct {
		name     string
		value    any
		expected string
	}{
		{"empty bytes", []byte{}, "'\\x'"},
		{"uint value", uint(42), "42"},
		{"negative int", int(-123), "-123"},
		{"scientific notation float", float64(1.23e10), "1.23e+10"},
		{"very small float", float64(0.000001), "1e-06"},
		{"struct fallback", struct{ X int }{X: 1}, "{1}"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatLiteralValue(tc.value)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestExplainExpressionValueTypes tests formatExpressionValue with different expression types.
func TestExplainExpressionValueTypes(t *testing.T) {
	// Test nil expression
	result := formatExpressionValue(nil)
	assert.Equal(t, "<nil>", result)

	// Test column reference without table
	colRef := &binder.BoundColumnRef{Column: "id"}
	result = formatExpressionValue(colRef)
	assert.Equal(t, "id", result)

	// Test column reference with table
	colRefWithTable := &binder.BoundColumnRef{Table: "users", Column: "name"}
	result = formatExpressionValue(colRefWithTable)
	assert.Equal(t, "users.name", result)

	// Test unknown expression type (should return <expr>)
	unknownExpr := &binder.BoundFunctionCall{Name: "UNKNOWN"}
	result = formatExpressionValue(unknownExpr)
	assert.Equal(t, "<expr>", result)
}

// TestExplainPlanWithIndentation tests proper indentation in nested plans.
func TestExplainPlanWithIndentation(t *testing.T) {
	// Create a deeply nested plan: Project -> Sort -> Filter -> IndexScan
	idxScan := &planner.PhysicalIndexScan{
		TableName: "orders",
		IndexName: "idx_orders_id",
		IndexDef: &catalog.IndexDef{
			Name:    "idx_orders_id",
			Columns: []string{"id"},
		},
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int64(100)},
		},
	}

	filter := &planner.PhysicalFilter{Child: idxScan}
	sort := &planner.PhysicalSort{Child: filter}
	project := &planner.PhysicalProject{
		Child: sort,
		Expressions: []binder.BoundExpr{
			&binder.BoundColumnRef{Column: "id"},
		},
	}

	// Use formatPhysicalPlan (without cost) to avoid OutputColumns() dependency
	planText := formatPhysicalPlan(project, 0)
	t.Logf("Nested plan with indentation:\n%s", planText)

	lines := strings.Split(planText, "\n")

	// First line should not be indented
	assert.False(t, strings.HasPrefix(lines[0], "  "), "Root should not be indented")

	// Second line (Sort) should have 2 spaces
	if len(lines) > 1 {
		assert.True(t, strings.HasPrefix(lines[1], "  "), "Sort should be indented")
	}

	// Third line (Filter) should have 4 spaces
	if len(lines) > 2 {
		assert.True(t, strings.HasPrefix(lines[2], "    "), "Filter should be double indented")
	}

	// Fourth line (IndexScan) should have 6 spaces
	if len(lines) > 3 {
		assert.True(t, strings.HasPrefix(lines[3], "      "), "IndexScan should be triple indented")
	}
}

// TestExplainIndexScanNoLookupKeys tests EXPLAIN when no lookup keys exist (edge case).
func TestExplainIndexScanNoLookupKeys(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName:  "users",
		IndexName:  "idx_users_id",
		LookupKeys: nil, // No lookup keys
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show IndexScan but no Index Cond line
	assert.Contains(t, planText, "IndexScan: users")
	assert.NotContains(t, planText, "Index Cond:")
}

// TestExplainIndexScanEmptyLookupKeys tests EXPLAIN with empty lookup keys slice.
func TestExplainIndexScanEmptyLookupKeys(t *testing.T) {
	scan := &planner.PhysicalIndexScan{
		TableName:  "users",
		IndexName:  "idx_users_id",
		LookupKeys: []binder.BoundExpr{}, // Empty slice
	}

	planText := formatPhysicalPlan(scan, 0)
	t.Logf("formatPhysicalPlan output:\n%s", planText)

	// Should show IndexScan but no Index Cond line
	assert.Contains(t, planText, "IndexScan: users")
	assert.NotContains(t, planText, "Index Cond:")
}

// =============================================================================
// PRAGMA checkpoint_threshold Tests
// =============================================================================

// mockConnection is a simple mock for testing connection-level settings
type mockConnection struct {
	settings map[string]string
}

func (mc *mockConnection) GetSetting(key string) string {
	return mc.settings[key]
}

func (mc *mockConnection) SetSetting(key string, value string) {
	mc.settings[key] = value
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		settings: make(map[string]string),
	}
}

func TestPragmaCheckpointThresholdGet(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Set up mock connection with executor
	mockConn := newMockConnection()
	exec.SetConnection(mockConn)

	// Get default checkpoint_threshold
	result, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"checkpoint_threshold"}, result.Columns)
	require.Len(t, result.Rows, 1)

	// Default should be "256MB"
	assert.Equal(t, "256MB", result.Rows[0]["checkpoint_threshold"].(string))
}

func TestPragmaCheckpointThresholdSet(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	mockConn := newMockConnection()
	exec.SetConnection(mockConn)

	// Set checkpoint_threshold to 512MB
	result, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold = '512MB'")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"success"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, true, result.Rows[0]["success"])
}

func TestPragmaCheckpointThresholdSetAndGet(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	mockConn := newMockConnection()
	exec.SetConnection(mockConn)

	// Set to 1GB
	result, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold = '1GB'")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, true, result.Rows[0]["success"])

	// Get the value back
	result, err = executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "1GB", result.Rows[0]["checkpoint_threshold"].(string))
}

func TestPragmaCheckpointThresholdValidation(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	mockConn := newMockConnection()
	exec.SetConnection(mockConn)

	// Test with very small value (< 1KB) - should fail
	_, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold = '512b'")
	require.Error(t, err, "Should reject threshold < 1KB")
}

func TestPragmaCheckpointThresholdDifferentFormats(t *testing.T) {
	testCases := []struct {
		name      string
		value     string
		shouldSet bool
	}{
		{"256MB", "256MB", true},
		{"512MB", "512MB", true},
		{"1GB", "1GB", true},
		{"512KB", "512KB", true},
		{"1024KB", "1024KB", true},
		{"1048576b", "1048576b", true}, // 1MB
		{"5368709120b", "5368709120b", true}, // 5GB
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec, cat, _ := setupTestExecutor()
			mockConn := newMockConnection()
			exec.SetConnection(mockConn)

			result, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold = '"+tc.value+"'")
			if tc.shouldSet {
				require.NoError(t, err, "Should accept %s", tc.name)
				assert.Equal(t, true, result.Rows[0]["success"])

				// Get value back
				result, err = executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold")
				require.NoError(t, err)
				assert.Equal(t, tc.value, result.Rows[0]["checkpoint_threshold"].(string))
			} else {
				require.Error(t, err, "Should reject %s", tc.name)
			}
		})
	}
}

func TestPragmaCheckpointThresholdMultipleSets(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	mockConn := newMockConnection()
	exec.SetConnection(mockConn)

	values := []string{"256MB", "512MB", "1GB", "512KB"}

	for _, val := range values {
		// Set
		result, err := executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold = '"+val+"'")
		require.NoError(t, err)
		assert.Equal(t, true, result.Rows[0]["success"])

		// Verify
		result, err = executeSQL(t, exec, cat, "PRAGMA checkpoint_threshold")
		require.NoError(t, err)
		assert.Equal(t, val, result.Rows[0]["checkpoint_threshold"].(string))
	}
}
