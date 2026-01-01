package executor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// createTestParquetFile creates a test Parquet file and returns its path.
func createTestParquetFile(t *testing.T, tmpDir string) string {
	t.Helper()

	type TestRecord struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Value  float64 `parquet:"value"`
		Active bool    `parquet:"active"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[TestRecord](buf)

	records := []TestRecord{
		{ID: 1, Name: "Alice", Value: 100.5, Active: true},
		{ID: 2, Name: "Bob", Value: 200.75, Active: false},
		{ID: 3, Name: "Charlie", Value: 300.0, Active: true},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, "test.parquet")
	err = os.WriteFile(filePath, buf.Bytes(), 0o644)
	require.NoError(t, err)

	return filePath
}

// createLargeTestParquetFile creates a test Parquet file with many rows.
func createLargeTestParquetFile(t *testing.T, tmpDir string, numRows int) string {
	t.Helper()

	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	records := make([]Record, numRows)
	for i := range numRows {
		records[i] = Record{
			ID:    int64(i),
			Value: "test",
		}
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, numRows, n)

	err = writer.Close()
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, "large.parquet")
	err = os.WriteFile(filePath, buf.Bytes(), 0o644)
	require.NoError(t, err)

	return filePath
}

func TestReadParquetTableFunction(t *testing.T) {
	tmpDir := t.TempDir()
	parquetFile := createTestParquetFile(t, tmpDir)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic read_parquet", func(t *testing.T) {
		sql := `SELECT * FROM read_parquet('` + parquetFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"id", "name", "value", "active"})

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
		assert.Equal(t, int64(1), aliceRow["id"])
		assert.Equal(t, true, aliceRow["active"])
	})

	t.Run("read_parquet with column selection", func(t *testing.T) {
		sql := `SELECT id, name FROM read_parquet('` + parquetFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"id", "name"})

		// Check that only id and name are in the rows
		for _, row := range result.Rows {
			_, hasID := row["id"]
			_, hasName := row["name"]
			assert.True(t, hasID, "row should have id column")
			assert.True(t, hasName, "row should have name column")
		}
	})
}

func TestReadParquetLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	numRows := 100
	parquetFile := createLargeTestParquetFile(t, tmpDir, numRows)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("read large parquet file", func(t *testing.T) {
		sql := `SELECT * FROM read_parquet('` + parquetFile + `')`
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

		assert.Len(t, result.Rows, numRows)
	})
}

func TestReadParquetErrors(t *testing.T) {
	cat := catalog.NewCatalog()

	t.Run("file not found", func(t *testing.T) {
		sql := `SELECT * FROM read_parquet('/nonexistent/file.parquet')`
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

func TestReadParquetParser(t *testing.T) {
	t.Run("parse read_parquet function call", func(t *testing.T) {
		sql := `SELECT * FROM read_parquet('test.parquet')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_parquet", tableRef.TableFunction.Name)
		require.Len(t, tableRef.TableFunction.Args, 1)
	})

	t.Run("parse read_parquet with alias", func(t *testing.T) {
		sql := `SELECT p.id FROM read_parquet('test.parquet') AS p`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "p", tableRef.Alias)
	})
}
