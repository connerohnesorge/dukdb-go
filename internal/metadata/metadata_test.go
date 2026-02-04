package metadata

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestMetadataHelpers(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

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

	schema, ok := cat.GetSchema(DefaultSchemaName)
	require.True(t, ok)

	index := catalog.NewIndexDef("users_name_idx", DefaultSchemaName, "users", []string{"name"}, true)
	require.NoError(t, schema.CreateIndex(index))

	view := catalog.NewViewDefWithDependencies("v_users", DefaultSchemaName, "SELECT * FROM users", []string{"users"})
	require.NoError(t, schema.CreateView(view))

	seq := catalog.NewSequenceDef("user_seq", DefaultSchemaName)
	require.NoError(t, schema.CreateSequence(seq))

	tables := GetTables(cat, stor, DefaultDatabaseName)
	require.NotEmpty(t, tables)

	columns := GetColumns(cat, DefaultDatabaseName)
	require.Len(t, columns, 2)

	constraints := GetConstraints(cat, DefaultDatabaseName)
	require.NotEmpty(t, constraints)

	indexes := GetIndexes(cat, DefaultDatabaseName)
	require.Len(t, indexes, 1)

	views := GetViews(cat, DefaultDatabaseName)
	require.Len(t, views, 1)

	sequences := GetSequences(cat, DefaultDatabaseName)
	require.Len(t, sequences, 1)

	dependencies := GetDependencies(cat, DefaultDatabaseName)
	require.Len(t, dependencies, 1)

	functions := GetFunctions(nil, DefaultDatabaseName)
	require.NotEmpty(t, functions)

	keywords := GetKeywords()
	require.NotEmpty(t, keywords)
}
