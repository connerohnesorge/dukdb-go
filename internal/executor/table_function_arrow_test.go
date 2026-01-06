// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	arrowio "github.com/dukdb/dukdb-go/internal/io/arrow"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestArrowFile creates a test Arrow IPC file with sample data.
func createTestArrowFile(t *testing.T, dir string) string {
	t.Helper()

	arrowFile := filepath.Join(dir, "test.arrow")

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create record batch
	alloc := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(alloc)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idBuilder.NewArray()
	defer idArr.Release()

	nameBuilder := array.NewStringBuilder(alloc)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	nameArr := nameBuilder.NewArray()
	defer nameArr.Release()

	valueBuilder := array.NewFloat64Builder(alloc)
	defer valueBuilder.Release()
	valueBuilder.AppendValues([]float64{1.5, 2.5, 3.5}, nil)
	valueArr := valueBuilder.NewArray()
	defer valueArr.Release()

	record := array.NewRecord(schema, []arrow.Array{idArr, nameArr, valueArr}, 3)
	defer record.Release()

	// Write to file
	file, err := os.Create(arrowFile)
	require.NoError(t, err)

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema))
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	return arrowFile
}

// createTestArrowStreamFile creates a test Arrow IPC stream file.
func createTestArrowStreamFile(t *testing.T, dir string) string {
	t.Helper()

	streamFile := filepath.Join(dir, "test.arrows")

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Create record batch
	alloc := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(alloc)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int64{10, 20}, nil)
	idArr := idBuilder.NewArray()
	defer idArr.Release()

	nameBuilder := array.NewStringBuilder(alloc)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"Test1", "Test2"}, nil)
	nameArr := nameBuilder.NewArray()
	defer nameArr.Release()

	record := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 2)
	defer record.Release()

	// Write to file
	file, err := os.Create(streamFile)
	require.NoError(t, err)

	writer := ipc.NewWriter(file, ipc.WithSchema(schema))

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	return streamFile
}

func TestReadArrowTableFunction(t *testing.T) {
	// Create temp directory for test files
	tmpDir := t.TempDir()

	// Create test Arrow file
	arrowFile := createTestArrowFile(t, tmpDir)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("basic read_arrow", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow('` + arrowFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"id", "name", "value"})

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
		assert.Equal(t, 1.5, aliceRow["value"])
	})

	t.Run("read_arrow with column selection", func(t *testing.T) {
		sql := `SELECT id, name FROM read_arrow('` + arrowFile + `')`
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

		// Check that only id and name are in the result columns
		for _, row := range result.Rows {
			_, hasID := row["id"]
			_, hasName := row["name"]
			assert.True(t, hasID, "row should have id column")
			assert.True(t, hasName, "row should have name column")
		}
	})

	t.Run("read_arrow with WHERE clause", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow('` + arrowFile + `') WHERE id > 1`
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

		// Should only have rows where id > 1 (Bob and Charlie)
		assert.Len(t, result.Rows, 2)
		for _, row := range result.Rows {
			id, ok := row["id"].(int64)
			require.True(t, ok)
			assert.True(t, id > 1)
		}
	})

	t.Run("read_arrow with alias", func(t *testing.T) {
		sql := `SELECT a.id, a.name FROM read_arrow('` + arrowFile + `') AS a WHERE a.id = 2`
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

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, int64(2), result.Rows[0]["id"])
		assert.Equal(t, "Bob", result.Rows[0]["name"])
	})
}

func TestReadArrowErrors(t *testing.T) {
	cat := catalog.NewCatalog()

	t.Run("file not found", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow('/nonexistent/file.arrow')`
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

func TestReadArrowAutoTableFunction(t *testing.T) {
	// Create temp directory for test files
	tmpDir := t.TempDir()

	// Create test files in both formats
	arrowFile := createTestArrowFile(t, tmpDir)
	streamFile := createTestArrowStreamFile(t, tmpDir)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("read_arrow_auto with file format", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow_auto('` + arrowFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"id", "name", "value"})
	})

	t.Run("read_arrow_auto with stream format", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow_auto('` + streamFile + `')`
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
		assert.ElementsMatch(t, result.Columns, []string{"id", "name"})

		// Verify data
		var foundTest1, foundTest2 bool
		for _, row := range result.Rows {
			if row["name"] == "Test1" {
				foundTest1 = true
				assert.Equal(t, int64(10), row["id"])
			}
			if row["name"] == "Test2" {
				foundTest2 = true
				assert.Equal(t, int64(20), row["id"])
			}
		}
		assert.True(t, foundTest1, "should find Test1")
		assert.True(t, foundTest2, "should find Test2")
	})
}

func TestReadArrowWithColumnsOption(t *testing.T) {
	// Create temp directory for test files
	tmpDir := t.TempDir()

	// Create test Arrow file
	arrowFile := createTestArrowFile(t, tmpDir)

	// Test column projection in reader (schema inference)
	t.Run("inferArrowSchema with columns option", func(t *testing.T) {
		opts := arrowio.DefaultReaderOptions()
		opts.Columns = []string{"id", "name"}

		reader, err := arrowio.NewReaderFromPath(arrowFile, opts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		columns, err := reader.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, columns)

		types, err := reader.Types()
		require.NoError(t, err)
		assert.Len(t, types, 2)
	})
}

func TestArrowFormatDetection(t *testing.T) {
	t.Run("detect file format from extension", func(t *testing.T) {
		tests := []struct {
			path     string
			expected arrowio.ArrowFormat
		}{
			{"data.arrow", arrowio.FormatFile},
			{"data.feather", arrowio.FormatFile},
			{"data.ipc", arrowio.FormatFile},
			{"data.arrows", arrowio.FormatStream},
			{"data.ARROW", arrowio.FormatFile},
			{"data.ARROWS", arrowio.FormatStream},
			{"data.txt", arrowio.FormatUnknown},
			{"data", arrowio.FormatUnknown},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				format := arrowio.DetectFormatFromPath(tt.path)
				assert.Equal(t, tt.expected, format)
			})
		}
	})
}

func TestReadArrowParser(t *testing.T) {
	t.Run("parse read_arrow function call", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow('test.arrow')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)
		require.NotNil(t, selectStmt.From)
		require.Len(t, selectStmt.From.Tables, 1)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_arrow", tableRef.TableFunction.Name)
		require.Len(t, tableRef.TableFunction.Args, 1)
	})

	t.Run("parse read_arrow_auto function call", func(t *testing.T) {
		sql := `SELECT * FROM read_arrow_auto('test.arrow')`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "read_arrow_auto", tableRef.TableFunction.Name)
	})

	t.Run("parse read_arrow with alias", func(t *testing.T) {
		sql := `SELECT a.id FROM read_arrow('test.arrow') AS a`
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		tableRef := selectStmt.From.Tables[0]
		require.NotNil(t, tableRef.TableFunction)
		assert.Equal(t, "a", tableRef.Alias)
	})
}
