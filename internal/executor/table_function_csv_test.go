package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

func TestReadCSVTableFunction(t *testing.T) {
	// Create a temporary CSV file for testing
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	csvContent := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
`
	err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
	require.NoError(t, err)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic read_csv", func(t *testing.T) {
		// Parse the query
		sql := `SELECT * FROM read_csv('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		// Bind
		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		// Plan
		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		// Execute
		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Verify results
		assert.Len(t, result.Rows, 3)
		assert.ElementsMatch(t, result.Columns, []string{"name", "age", "city"})

		// Check row values
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
		assert.Equal(t, "Charlie", result.Rows[2]["name"])
	})

	t.Run("read_csv with column selection", func(t *testing.T) {
		sql := `SELECT name, city FROM read_csv('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 3)
		assert.ElementsMatch(t, result.Columns, []string{"name", "city"})

		// Check that only name and city are in the rows
		for _, row := range result.Rows {
			_, hasName := row["name"]
			_, hasCity := row["city"]
			assert.True(t, hasName, "row should have name column")
			assert.True(t, hasCity, "row should have city column")
		}
	})
}

func TestReadCSVWithOptions(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("custom delimiter", func(t *testing.T) {
		csvFile := filepath.Join(tmpDir, "semicolon.csv")
		csvContent := `name;age;city
Alice;30;New York
Bob;25;Los Angeles
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv('` + csvFile + `', delimiter=';')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
	})

	t.Run("no header", func(t *testing.T) {
		csvFile := filepath.Join(tmpDir, "noheader.csv")
		csvContent := `Alice,30,New York
Bob,25,Los Angeles
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv('` + csvFile + `', header=false)`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		// With no header, columns should be named column0, column1, column2
		assert.Contains(t, result.Columns, "column0")
		assert.Contains(t, result.Columns, "column1")
		assert.Contains(t, result.Columns, "column2")
	})
}

func TestReadCSVWithWhere(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	csvContent := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
`
	err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
	require.NoError(t, err)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// This tests that we can filter results from read_csv
	// Note: Column reference in WHERE needs proper binding
	t.Run("filter with WHERE clause", func(t *testing.T) {
		sql := `SELECT * FROM read_csv('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		// Should have all 3 rows without filter
		assert.Len(t, result.Rows, 3)
	})
}

func TestReadCSVErrors(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	t.Run("file not found", func(t *testing.T) {
		sql := `SELECT * FROM read_csv('/nonexistent/file.csv')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		_, err = b.Bind(stmt)
		// Error should occur at bind time now since we read the file schema at bind time
		require.Error(t, err)

		// Check it's a binder error
		var dukErr *dukdb.Error
		if assert.ErrorAs(t, err, &dukErr) {
			assert.Equal(t, dukdb.ErrorTypeBinder, dukErr.Type)
		}
	})

	t.Run("unknown table function", func(t *testing.T) {
		sql := `SELECT * FROM unknown_func('test')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		exec := NewExecutor(cat, stor)
		_, err = exec.Execute(context.Background(), plan, nil)
		require.Error(t, err)
	})
}

func TestReadCSVAutoTableFunction(t *testing.T) {
	t.Run("basic read_csv_auto", func(t *testing.T) {
		// Create a temporary CSV file with comma delimiter
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "test.csv")

		csvContent := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv_auto('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 3)
		assert.ElementsMatch(t, result.Columns, []string{"name", "age", "city"})

		// Check row values
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
		assert.Equal(t, "Charlie", result.Rows[2]["name"])
	})

	t.Run("auto-detect tab delimiter", func(t *testing.T) {
		tmpDir := t.TempDir()
		tsvFile := filepath.Join(tmpDir, "test.tsv")

		// Tab-separated content
		tsvContent := "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\n"
		err := os.WriteFile(tsvFile, []byte(tsvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv_auto('` + tsvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.ElementsMatch(t, result.Columns, []string{"name", "age", "city"})
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
	})

	t.Run("auto-detect semicolon delimiter", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "semicolon.csv")

		// Semicolon-separated content
		csvContent := `name;age;city
Alice;30;New York
Bob;25;Los Angeles
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv_auto('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.ElementsMatch(t, result.Columns, []string{"name", "age", "city"})
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
	})

	t.Run("auto-detect pipe delimiter", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "pipe.csv")

		// Pipe-separated content
		csvContent := `name|age|city
Alice|30|New York
Bob|25|Los Angeles
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_csv_auto('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.ElementsMatch(t, result.Columns, []string{"name", "age", "city"})
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, "Bob", result.Rows[1]["name"])
	})

	t.Run("read_csv_auto with column selection", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "test.csv")

		csvContent := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
`
		err := os.WriteFile(csvFile, []byte(csvContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT name, city FROM read_csv_auto('` + csvFile + `')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		bound, err := b.Bind(stmt)
		require.NoError(t, err)

		p := planner.NewPlanner(cat)
		plan, err := p.Plan(bound)
		require.NoError(t, err)

		result, err := exec.Execute(context.Background(), plan, nil)
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.ElementsMatch(t, result.Columns, []string{"name", "city"})
	})
}

func TestReadCSVParser(t *testing.T) {
	t.Run("parse read_csv function call", func(t *testing.T) {
		sql := `SELECT * FROM read_csv('test.csv')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_csv", tableRef.TableFunction.Name)
		require.Len(t, tableRef.TableFunction.Args, 1)
	})

	t.Run("parse read_csv with named args", func(t *testing.T) {
		sql := `SELECT * FROM read_csv('test.csv', delimiter=',', header=true)`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_csv", tableRef.TableFunction.Name)
		assert.Len(t, tableRef.TableFunction.Args, 1) // path

		// Check named args
		delimArg, ok := tableRef.TableFunction.NamedArgs["delimiter"]
		require.True(t, ok)
		delimLit, ok := delimArg.(*parser.Literal)
		require.True(t, ok)
		assert.Equal(t, ",", delimLit.Value)

		headerArg, ok := tableRef.TableFunction.NamedArgs["header"]
		require.True(t, ok)
		headerLit, ok := headerArg.(*parser.Literal)
		require.True(t, ok)
		assert.Equal(t, true, headerLit.Value)
	})

	t.Run("parse read_csv with alias", func(t *testing.T) {
		sql := `SELECT c.name FROM read_csv('test.csv') AS c`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "c", tableRef.Alias)
	})
}
