package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/metadata"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSystemFunctions(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	table := catalog.NewTableDef(
		"users",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		},
	)
	require.NoError(t, table.SetPrimaryKey([]string{"id"}))
	require.NoError(t, cat.CreateTable(table))
	_, err := stor.CreateTable("users", table.ColumnTypes())
	require.NoError(t, err)

	schema, ok := cat.GetSchema(metadata.DefaultSchemaName)
	require.True(t, ok)
	require.NoError(t, schema.CreateIndex(catalog.NewIndexDef("users_name_idx", metadata.DefaultSchemaName, "users", []string{"name"}, true)))
	require.NoError(t, schema.CreateView(catalog.NewViewDefWithDependencies("v_users", metadata.DefaultSchemaName, "SELECT * FROM users", []string{"users"})))
	require.NoError(t, schema.CreateSequence(catalog.NewSequenceDef("user_seq", metadata.DefaultSchemaName)))

	tests := []struct {
		name         string
		expectedCols []string
		expectRows   bool
	}{
		{"duckdb_settings", metadata.ColumnNames(metadata.DuckDBSettingsColumns()), true},
		{"duckdb_tables", metadata.ColumnNames(metadata.DuckDBTablesColumns()), true},
		{"duckdb_columns", metadata.ColumnNames(metadata.DuckDBColumnsColumns()), true},
		{"duckdb_views", metadata.ColumnNames(metadata.DuckDBViewsColumns()), true},
		{"duckdb_functions", metadata.ColumnNames(metadata.DuckDBFunctionsColumns()), true},
		{"duckdb_constraints", metadata.ColumnNames(metadata.DuckDBConstraintsColumns()), true},
		{"duckdb_indexes", metadata.ColumnNames(metadata.DuckDBIndexesColumns()), true},
		{"duckdb_databases", metadata.ColumnNames(metadata.DuckDBDatabasesColumns()), true},
		{"duckdb_sequences", metadata.ColumnNames(metadata.DuckDBSequencesColumns()), true},
		{"duckdb_dependencies", metadata.ColumnNames(metadata.DuckDBDependenciesColumns()), true},
		{"duckdb_optimizers", metadata.ColumnNames(metadata.DuckDBOptimizersColumns()), true},
		{"duckdb_keywords", metadata.ColumnNames(metadata.DuckDBKeywordsColumns()), true},
		{"duckdb_extensions", metadata.ColumnNames(metadata.DuckDBExtensionsColumns()), false},
		{"duckdb_memory_usage", metadata.ColumnNames(metadata.DuckDBMemoryUsageColumns()), true},
		{"duckdb_temp_directory", metadata.ColumnNames(metadata.DuckDBTempDirectoryColumns()), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executeSystemQuery(t, cat, exec, "SELECT * FROM "+tt.name+"()")
			require.Equal(t, tt.expectedCols, result.Columns)
			if tt.expectRows {
				require.NotEmpty(t, result.Rows)
			}
		})
	}
}

func executeSystemQuery(t *testing.T, cat *catalog.Catalog, exec *Executor, sql string) *ExecutionResult {
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
	return result
}
