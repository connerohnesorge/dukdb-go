package executor

import (
	"context"
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
	assert.Contains(t, planText, "ANALYZE mode")
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
