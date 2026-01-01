// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestCopyFromCSV tests importing data from a CSV file into a table.
func TestCopyFromCSV(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()
	csvPath := filepath.Join(tempDir, "test_data.csv")

	// Write test CSV file
	csvContent := `id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
`
	err := os.WriteFile(csvPath, []byte(csvContent), 0644)
	require.NoError(t, err)

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	// Create table in catalog with column definitions
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
	}
	tableDef := catalog.NewTableDef("users", columns)
	err = cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create table in storage
	_, err = stor.CreateTable("users", tableDef.ColumnTypes())
	require.NoError(t, err)

	// Parse COPY FROM statement
	sql := `COPY users FROM '` + csvPath + `' (FORMAT CSV, HEADER true)`
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	copyStmt, ok := stmt.(*parser.CopyStmt)
	require.True(t, ok, "Expected CopyStmt, got %T", stmt)
	assert.Equal(t, "users", copyStmt.TableName)
	assert.True(t, copyStmt.IsFrom)
	assert.Equal(t, csvPath, copyStmt.FilePath)

	// Bind the statement
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Plan the statement
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Execute
	exec := NewExecutor(cat, stor)
	result, err := exec.Execute(nil, plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsAffected)

	// Verify data was imported
	table, ok := stor.GetTable("users")
	require.True(t, ok)

	scanner := table.Scan()
	rows := 0
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}
		rows += chunk.Count()
	}
	assert.Equal(t, 3, rows)
}

// TestCopyToCSV tests exporting data from a table to a CSV file.
func TestCopyToCSV(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()
	csvPath := filepath.Join(tempDir, "output.csv")

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	// Create and populate table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("price", dukdb.TYPE_DOUBLE),
	}
	tableDef := catalog.NewTableDef("products", columns)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	table, err := stor.CreateTable("products", tableDef.ColumnTypes())
	require.NoError(t, err)

	// Insert test data
	chunk := storage.NewDataChunkWithCapacity(tableDef.ColumnTypes(), 3)
	chunk.AppendRow([]any{1, "Widget", 9.99})
	chunk.AppendRow([]any{2, "Gadget", 19.99})
	chunk.AppendRow([]any{3, "Doodad", 4.99})
	_, err = table.InsertChunk(chunk)
	require.NoError(t, err)

	// Parse COPY TO statement
	sql := `COPY products TO '` + csvPath + `' (FORMAT CSV, HEADER true)`
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	copyStmt, ok := stmt.(*parser.CopyStmt)
	require.True(t, ok)
	assert.Equal(t, "products", copyStmt.TableName)
	assert.False(t, copyStmt.IsFrom)

	// Bind the statement
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Plan the statement
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Execute
	exec := NewExecutor(cat, stor)
	result, err := exec.Execute(nil, plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsAffected)

	// Verify file was created
	_, err = os.Stat(csvPath)
	require.NoError(t, err)

	// Read and verify content
	content, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	assert.Contains(t, string(content), "Widget")
	assert.Contains(t, string(content), "Gadget")
	assert.Contains(t, string(content), "Doodad")
}

// TestCopyToParquet tests exporting data to a Parquet file.
func TestCopyToParquet(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()
	parquetPath := filepath.Join(tempDir, "output.parquet")

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	// Create and populate table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
	}
	tableDef := catalog.NewTableDef("data", columns)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	table, err := stor.CreateTable("data", tableDef.ColumnTypes())
	require.NoError(t, err)

	// Insert test data
	chunk := storage.NewDataChunkWithCapacity(tableDef.ColumnTypes(), 2)
	chunk.AppendRow([]any{1, "first"})
	chunk.AppendRow([]any{2, "second"})
	_, err = table.InsertChunk(chunk)
	require.NoError(t, err)

	// Parse COPY TO PARQUET statement
	sql := `COPY data TO '` + parquetPath + `' (FORMAT PARQUET)`
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	// Bind the statement
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Plan the statement
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Execute
	exec := NewExecutor(cat, stor)
	result, err := exec.Execute(nil, plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected)

	// Verify file was created
	_, err = os.Stat(parquetPath)
	require.NoError(t, err)
}

// TestCopyToJSON tests exporting data to a JSON file.
func TestCopyToJSON(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()
	jsonPath := filepath.Join(tempDir, "output.json")

	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	// Create and populate table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	}
	tableDef := catalog.NewTableDef("items", columns)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	table, err := stor.CreateTable("items", tableDef.ColumnTypes())
	require.NoError(t, err)

	// Insert test data
	chunk := storage.NewDataChunkWithCapacity(tableDef.ColumnTypes(), 2)
	chunk.AppendRow([]any{1, "item1"})
	chunk.AppendRow([]any{2, "item2"})
	_, err = table.InsertChunk(chunk)
	require.NoError(t, err)

	// Parse COPY TO JSON statement
	sql := `COPY items TO '` + jsonPath + `' (FORMAT JSON)`
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	// Bind the statement
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Plan the statement
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Execute
	exec := NewExecutor(cat, stor)
	result, err := exec.Execute(nil, plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected)

	// Verify file was created
	_, err = os.Stat(jsonPath)
	require.NoError(t, err)

	// Read and verify content contains JSON
	content, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	assert.Contains(t, string(content), "item1")
	assert.Contains(t, string(content), "item2")
}

// TestParseCopyStatement tests parsing of various COPY statement formats.
func TestParseCopyStatement(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		isFrom   bool
		path     string
		options  map[string]any
		hasQuery bool
	}{
		{
			name:   "COPY FROM CSV",
			sql:    "COPY users FROM '/path/to/file.csv' (FORMAT CSV)",
			table:  "users",
			isFrom: true,
			path:   "/path/to/file.csv",
			options: map[string]any{
				"FORMAT": "CSV",
			},
		},
		{
			name:   "COPY TO CSV with options",
			sql:    "COPY users TO '/path/to/file.csv' (FORMAT CSV, DELIMITER '|', HEADER true)",
			table:  "users",
			isFrom: false,
			path:   "/path/to/file.csv",
			options: map[string]any{
				"FORMAT":    "CSV",
				"DELIMITER": "|",
				"HEADER":    true,
			},
		},
		{
			name:   "COPY TO PARQUET",
			sql:    "COPY data TO '/path/to/file.parquet' (FORMAT PARQUET)",
			table:  "data",
			isFrom: false,
			path:   "/path/to/file.parquet",
			options: map[string]any{
				"FORMAT": "PARQUET",
			},
		},
		{
			name:   "COPY TO JSON",
			sql:    "COPY items TO '/output.json' (FORMAT JSON)",
			table:  "items",
			isFrom: false,
			path:   "/output.json",
			options: map[string]any{
				"FORMAT": "JSON",
			},
		},
		{
			name:     "COPY query TO CSV",
			sql:      "COPY (SELECT id, name FROM users) TO '/output.csv' (FORMAT CSV)",
			isFrom:   false,
			path:     "/output.csv",
			hasQuery: true,
			options: map[string]any{
				"FORMAT": "CSV",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			require.NoError(t, err)

			copyStmt, ok := stmt.(*parser.CopyStmt)
			require.True(t, ok, "Expected CopyStmt, got %T", stmt)

			assert.Equal(t, tt.table, copyStmt.TableName)
			assert.Equal(t, tt.isFrom, copyStmt.IsFrom)
			assert.Equal(t, tt.path, copyStmt.FilePath)
			assert.Equal(t, tt.hasQuery, copyStmt.Query != nil)

			for key, expected := range tt.options {
				actual, ok := copyStmt.Options[key]
				assert.True(t, ok, "Option %s not found", key)
				assert.Equal(t, expected, actual, "Option %s mismatch", key)
			}
		})
	}
}

// TestCopyFromNonExistentFile tests error handling for missing files.
func TestCopyFromNonExistentFile(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	// Create table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	}
	tableDef := catalog.NewTableDef("test", columns)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	_, err = stor.CreateTable("test", tableDef.ColumnTypes())
	require.NoError(t, err)

	// Parse COPY FROM with non-existent file
	sql := "COPY test FROM '/nonexistent/path/file.csv' (FORMAT CSV)"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	// Bind
	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Plan
	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Execute should fail
	exec := NewExecutor(cat, stor)
	_, err = exec.Execute(nil, plan, nil)
	assert.Error(t, err)
}

// TestCopyColumnList tests COPY with specific column list.
func TestCopyColumnList(t *testing.T) {
	sql := "COPY users (id, name) TO '/output.csv' (FORMAT CSV)"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	copyStmt, ok := stmt.(*parser.CopyStmt)
	require.True(t, ok)

	assert.Equal(t, "users", copyStmt.TableName)
	assert.Equal(t, []string{"id", "name"}, copyStmt.Columns)
	assert.False(t, copyStmt.IsFrom)
}

// TestCopySchemaQualifiedTable tests COPY with schema-qualified table name.
func TestCopySchemaQualifiedTable(t *testing.T) {
	sql := "COPY main.users FROM '/input.csv' (FORMAT CSV)"
	stmt, err := parser.Parse(sql)
	require.NoError(t, err)

	copyStmt, ok := stmt.(*parser.CopyStmt)
	require.True(t, ok)

	assert.Equal(t, "main", copyStmt.Schema)
	assert.Equal(t, "users", copyStmt.TableName)
	assert.True(t, copyStmt.IsFrom)
}
