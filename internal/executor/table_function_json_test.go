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

func TestReadJSONTableFunction(t *testing.T) {
	// Create a temporary JSON file for testing
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "test.json")

	jsonContent := `[
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "Los Angeles"},
		{"name": "Charlie", "age": 35, "city": "Chicago"}
	]`
	err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
	require.NoError(t, err)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic read_json", func(t *testing.T) {
		sql := `SELECT * FROM read_json('` + jsonFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"age", "city", "name"})

		// Find Alice row
		var aliceRow map[string]any
		for _, row := range result.Rows {
			if row["name"] == "Alice" {
				aliceRow = row
				break
			}
		}
		require.NotNil(t, aliceRow, "should find Alice row")
		assert.Equal(t, "Alice", aliceRow["name"])
	})

	t.Run("read_json with column selection", func(t *testing.T) {
		sql := `SELECT name, city FROM read_json('` + jsonFile + `')`
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

func TestReadJSONAutoTableFunction(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("auto-detect JSON array", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "array.json")
		jsonContent := `[
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25}
		]`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_json_auto('` + jsonFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"age", "name"})
	})

	t.Run("auto-detect NDJSON", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "ndjson.json")
		jsonContent := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}
`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_json_auto('` + jsonFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"age", "name"})
	})
}

func TestReadNDJSONTableFunction(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("basic read_ndjson", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "test.ndjson")
		jsonContent := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}
`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_ndjson('` + jsonFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"age", "name"})

		// Find Alice row
		var aliceRow map[string]any
		for _, row := range result.Rows {
			if row["name"] == "Alice" {
				aliceRow = row
				break
			}
		}
		require.NotNil(t, aliceRow, "should find Alice row")
		assert.Equal(t, "Alice", aliceRow["name"])
	})

	t.Run("read_ndjson with column selection", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "test2.ndjson")
		jsonContent := `{"name": "Alice", "age": 30, "city": "New York"}
{"name": "Bob", "age": 25, "city": "Los Angeles"}
`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT name, city FROM read_ndjson('` + jsonFile + `')`
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

func TestReadJSONWithOptions(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("format=newline_delimited", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "ndjson.json")
		jsonContent := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_json('` + jsonFile + `', format='newline_delimited')`
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
	})

	t.Run("format=array", func(t *testing.T) {
		jsonFile := filepath.Join(tmpDir, "array.json")
		jsonContent := `[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]`
		err := os.WriteFile(jsonFile, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		stor := storage.NewStorage()
		exec := NewExecutor(cat, stor)

		sql := `SELECT * FROM read_json('` + jsonFile + `', format='array')`
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
	})
}

func TestReadJSONErrors(t *testing.T) {
	cat := catalog.NewCatalog()

	t.Run("file not found", func(t *testing.T) {
		sql := `SELECT * FROM read_json('/nonexistent/file.json')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		b := binder.NewBinder(cat)
		_, err = b.Bind(stmt)
		require.Error(t, err)

		// Check it's a binder error
		var dukErr *dukdb.Error
		if assert.ErrorAs(t, err, &dukErr) {
			assert.Equal(t, dukdb.ErrorTypeBinder, dukErr.Type)
		}
	})
}

func TestReadJSONParser(t *testing.T) {
	t.Run("parse read_json function call", func(t *testing.T) {
		sql := `SELECT * FROM read_json('test.json')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_json", tableRef.TableFunction.Name)
		require.Len(t, tableRef.TableFunction.Args, 1)
	})

	t.Run("parse read_json_auto function call", func(t *testing.T) {
		sql := `SELECT * FROM read_json_auto('test.json')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_json_auto", tableRef.TableFunction.Name)
	})

	t.Run("parse read_ndjson function call", func(t *testing.T) {
		sql := `SELECT * FROM read_ndjson('test.ndjson')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_ndjson", tableRef.TableFunction.Name)
	})

	t.Run("parse read_json with named args", func(t *testing.T) {
		sql := `SELECT * FROM read_json('test.json', format='newline_delimited')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_json", tableRef.TableFunction.Name)
		assert.Len(t, tableRef.TableFunction.Args, 1) // path

		// Check named args
		formatArg, ok := tableRef.TableFunction.NamedArgs["format"]
		require.True(t, ok)
		formatLit, ok := formatArg.(*parser.Literal)
		require.True(t, ok)
		assert.Equal(t, "newline_delimited", formatLit.Value)
	})

	t.Run("parse read_json with alias", func(t *testing.T) {
		sql := `SELECT j.name FROM read_json('test.json') AS j`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "j", tableRef.Alias)
	})
}
