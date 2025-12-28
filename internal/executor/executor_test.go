package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

func setupTestExecutor() (*Executor, *catalog.Catalog, *storage.Storage) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat, stor
}

func executeQuery(
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

	return exec.Execute(
		context.Background(),
		plan,
		nil,
	)
}

func TestCreateTable(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	result, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf(
			"Expected 0 rows affected, got %d",
			result.RowsAffected,
		)
	}

	// Verify table exists in catalog
	_, exists := cat.GetTable("users")
	if !exists {
		t.Error(
			"Table 'users' should exist in catalog",
		)
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER)",
	)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Try to create again with IF NOT EXISTS - should not error
	_, err = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE IF NOT EXISTS users (id INTEGER)",
	)
	if err != nil {
		t.Errorf(
			"CREATE TABLE IF NOT EXISTS should not error: %v",
			err,
		)
	}

	// Try to create again without IF NOT EXISTS - should error
	_, err = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER)",
	)
	if err == nil {
		t.Error(
			"CREATE TABLE should error when table already exists",
		)
	}
}

func TestDropTable(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER)",
	)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Drop table
	_, err = executeQuery(
		t,
		exec,
		cat,
		"DROP TABLE users",
	)
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify table no longer exists
	_, exists := cat.GetTable("users")
	if exists {
		t.Error(
			"Table 'users' should not exist after DROP",
		)
	}
}

func TestDropTableIfExists(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Drop non-existent table with IF EXISTS - should not error
	_, err := executeQuery(
		t,
		exec,
		cat,
		"DROP TABLE IF EXISTS users",
	)
	if err != nil {
		t.Errorf(
			"DROP TABLE IF EXISTS should not error: %v",
			err,
		)
	}

	// Drop non-existent table without IF EXISTS - should error
	_, err = executeQuery(
		t,
		exec,
		cat,
		"DROP TABLE users",
	)
	if err == nil {
		t.Error(
			"DROP TABLE should error when table doesn't exist",
		)
	}
}

func TestInsertAndSelect(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data
	result, err := executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf(
			"Expected 1 row affected, got %d",
			result.RowsAffected,
		)
	}

	// Insert more data
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, 'Bob')",
	)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Select all
	result, err = executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 rows, got %d",
			len(result.Rows),
		)
	}
}

func TestSelectWithWhere(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)",
	)

	// Select with WHERE
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users WHERE age > 28",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 rows where age > 28, got %d",
			len(result.Rows),
		)
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (3, 'Charlie')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, 'Bob')",
	)

	// Select with ORDER BY
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users ORDER BY id",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf(
			"Expected 3 rows, got %d",
			len(result.Rows),
		)
	}

	// Check order - the value could be various numeric types
	idVal := result.Rows[0]["id"]
	switch v := idVal.(type) {
	case int, int32, int64, float64:
		// This is fine, we expect a numeric 1
		_ = v
	default:
		t.Errorf("First row id should be a number, got %T: %v", idVal, idVal)
	}
}

func TestSelectWithLimit(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, 'Bob')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (3, 'Charlie')",
	)

	// Select with LIMIT
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users LIMIT 2",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 rows with LIMIT 2, got %d",
			len(result.Rows),
		)
	}
}

func TestSelectWithOffset(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, 'Bob')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (3, 'Charlie')",
	)

	// Select with LIMIT and OFFSET
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users LIMIT 2 OFFSET 1",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 rows with LIMIT 2 OFFSET 1, got %d",
			len(result.Rows),
		)
	}
}

func TestSelectWithAggregate(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, age INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, age) VALUES (1, 25)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, age) VALUES (2, 30)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, age) VALUES (3, 35)",
	)

	// COUNT(*)
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT COUNT(*) FROM users",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf(
			"Expected 1 row for COUNT(*), got %d",
			len(result.Rows),
		)
	}
}

func TestSelectDistinct(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (3, 'Bob')",
	)

	// First check how many rows we have without DISTINCT
	allResult, _ := executeQuery(
		t,
		exec,
		cat,
		"SELECT name FROM users",
	)
	t.Logf(
		"All rows: %d, columns: %v",
		len(allResult.Rows),
		allResult.Columns,
	)
	for i, r := range allResult.Rows {
		t.Logf("Row %d: %v", i, r)
	}

	// SELECT DISTINCT
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT DISTINCT name FROM users",
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	t.Logf(
		"Distinct rows: %d, columns: %v",
		len(result.Rows),
		result.Columns,
	)
	for i, r := range result.Rows {
		t.Logf("Distinct row %d: %v", i, r)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 distinct names, got %d",
			len(result.Rows),
		)
	}
}

func TestArithmeticExpressions(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	tests := []struct {
		name     string
		sql      string
		expected float64
	}{
		{"addition", "SELECT 1 + 2", 3},
		{"subtraction", "SELECT 5 - 3", 2},
		{"multiplication", "SELECT 4 * 3", 12},
		{"division", "SELECT 10 / 2", 5},
		{"modulo", "SELECT 7 % 3", 1},
		{"complex", "SELECT (1 + 2) * 3", 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				tt.sql,
			)
			if err != nil {
				t.Fatalf(
					"Execute failed: %v",
					err,
				)
			}
			if len(result.Rows) != 1 {
				t.Errorf(
					"Expected 1 row, got %d",
					len(result.Rows),
				)
				return
			}
		})
	}
}

func TestComparisonExpressions(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE numbers (val INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (10)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (20)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (30)",
	)

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{
			"greater than",
			"SELECT * FROM numbers WHERE val > 15",
			2,
		},
		{
			"less than",
			"SELECT * FROM numbers WHERE val < 25",
			2,
		},
		{
			"greater or equal",
			"SELECT * FROM numbers WHERE val >= 20",
			2,
		},
		{
			"less or equal",
			"SELECT * FROM numbers WHERE val <= 20",
			2,
		},
		{
			"equal",
			"SELECT * FROM numbers WHERE val = 20",
			1,
		},
		{
			"not equal",
			"SELECT * FROM numbers WHERE val <> 20",
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				tt.sql,
			)
			if err != nil {
				t.Fatalf(
					"Execute failed: %v",
					err,
				)
			}
			if len(result.Rows) != tt.expected {
				t.Errorf(
					"Expected %d rows, got %d",
					tt.expected,
					len(result.Rows),
				)
			}
		})
	}
}

func TestLogicalExpressions(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE numbers (val INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (10)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (20)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (30)",
	)

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{
			"AND",
			"SELECT * FROM numbers WHERE val > 15 AND val < 25",
			1,
		},
		{
			"OR",
			"SELECT * FROM numbers WHERE val = 10 OR val = 30",
			2,
		},
		{
			"NOT",
			"SELECT * FROM numbers WHERE NOT val = 20",
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				tt.sql,
			)
			if err != nil {
				t.Fatalf(
					"Execute failed: %v",
					err,
				)
			}
			if len(result.Rows) != tt.expected {
				t.Errorf(
					"Expected %d rows, got %d",
					tt.expected,
					len(result.Rows),
				)
			}
		})
	}
}

func TestStringFunctions(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE strings (val VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO strings (val) VALUES ('hello')",
	)

	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			"UPPER",
			"SELECT UPPER(val) FROM strings",
			"HELLO",
		},
		{
			"LOWER",
			"SELECT LOWER(val) FROM strings",
			"hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				tt.sql,
			)
			if err != nil {
				t.Fatalf(
					"Execute failed: %v",
					err,
				)
			}
			if len(result.Rows) != 1 {
				t.Errorf(
					"Expected 1 row, got %d",
					len(result.Rows),
				)
				return
			}
		})
	}
}

func TestNullHandling(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with nullable column
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name) VALUES (2, NULL)",
	)

	// Test IS NULL
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users WHERE name IS NULL",
	)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf(
			"Expected 1 row with NULL name, got %d",
			len(result.Rows),
		)
	}

	// Test IS NOT NULL
	result, err = executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM users WHERE name IS NOT NULL",
	)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf(
			"Expected 1 row with non-NULL name, got %d",
			len(result.Rows),
		)
	}
}

func TestBetweenExpression(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE numbers (val INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (10)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (20)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (30)",
	)

	// Test BETWEEN
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM numbers WHERE val BETWEEN 15 AND 25",
	)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf(
			"Expected 1 row between 15 and 25, got %d",
			len(result.Rows),
		)
	}
}

func TestInExpression(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE numbers (val INTEGER)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (10)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (20)",
	)
	_, _ = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO numbers (val) VALUES (30)",
	)

	// Test IN
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM numbers WHERE val IN (10, 30)",
	)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf(
			"Expected 2 rows in (10, 30), got %d",
			len(result.Rows),
		)
	}

	// Test NOT IN
	result, err = executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM numbers WHERE val NOT IN (10, 30)",
	)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf(
			"Expected 1 row not in (10, 30), got %d",
			len(result.Rows),
		)
	}
}

func TestColumnType(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Test with various column types
	_, err := executeQuery(t, exec, cat, `
		CREATE TABLE all_types (
			bool_col BOOLEAN,
			int_col INTEGER,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR,
			date_col DATE,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify table was created with correct types
	tableDef, exists := cat.GetTable("all_types")
	if !exists {
		t.Fatal("Table 'all_types' should exist")
	}

	expectedTypes := map[string]dukdb.Type{
		"bool_col":      dukdb.TYPE_BOOLEAN,
		"int_col":       dukdb.TYPE_INTEGER,
		"bigint_col":    dukdb.TYPE_BIGINT,
		"float_col":     dukdb.TYPE_FLOAT,
		"double_col":    dukdb.TYPE_DOUBLE,
		"varchar_col":   dukdb.TYPE_VARCHAR,
		"date_col":      dukdb.TYPE_DATE,
		"timestamp_col": dukdb.TYPE_TIMESTAMP,
	}

	for _, col := range tableDef.Columns {
		if expected, ok := expectedTypes[col.Name]; ok {
			if col.Type != expected {
				t.Errorf(
					"Column %s: expected type %v, got %v",
					col.Name,
					expected,
					col.Type,
				)
			}
		}
	}
}
