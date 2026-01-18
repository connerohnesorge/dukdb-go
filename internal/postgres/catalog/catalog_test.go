package catalog

import (
	"testing"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
)

// TestIsInformationSchemaQuery tests detection of information_schema queries.
func TestIsInformationSchemaQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "select from tables",
			query:    "SELECT * FROM information_schema.tables",
			expected: true,
		},
		{
			name:     "select from columns lowercase",
			query:    "select * from information_schema.columns",
			expected: true,
		},
		{
			name:     "select from columns mixed case",
			query:    "SELECT * FROM Information_Schema.Columns",
			expected: true,
		},
		{
			name:     "select from regular table",
			query:    "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "select from schemata",
			query:    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'main'",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInformationSchemaQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetViewName tests extracting the view name from queries.
func TestGetViewName(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "tables view",
			query:    "SELECT * FROM information_schema.tables",
			expected: "tables",
		},
		{
			name:     "columns view with where",
			query:    "SELECT * FROM information_schema.columns WHERE table_name = 'foo'",
			expected: "columns",
		},
		{
			name:     "schemata view",
			query:    "SELECT schema_name FROM information_schema.schemata",
			expected: "schemata",
		},
		{
			name:     "views view",
			query:    "SELECT * FROM information_schema.views",
			expected: "views",
		},
		{
			name:     "not information_schema",
			query:    "SELECT * FROM users",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetViewName(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseWhereClause tests parsing WHERE clause filters.
func TestParseWhereClause(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []Filter
	}{
		{
			name:     "single filter",
			query:    "SELECT * FROM information_schema.tables WHERE table_name = 'users'",
			expected: []Filter{{Column: "table_name", Value: "users"}},
		},
		{
			name:  "multiple filters",
			query: "SELECT * FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'users'",
			expected: []Filter{
				{Column: "table_schema", Value: "main"},
				{Column: "table_name", Value: "users"},
			},
		},
		{
			name:     "no where clause",
			query:    "SELECT * FROM information_schema.tables",
			expected: nil,
		},
		{
			name:     "filter with double quotes",
			query:    `SELECT * FROM information_schema.tables WHERE table_name = "users"`,
			expected: []Filter{{Column: "table_name", Value: "users"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseWhereClause(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// mockCatalog implements CatalogProvider for testing.
type mockCatalog struct {
	catalog *catalog.Catalog
}

func newMockCatalog() *mockCatalog {
	cat := catalog.NewCatalog()
	return &mockCatalog{catalog: cat}
}

func (m *mockCatalog) ListSchemas() []*catalog.Schema {
	return m.catalog.ListSchemas()
}

func (m *mockCatalog) GetSchema(name string) (*catalog.Schema, bool) {
	return m.catalog.GetSchema(name)
}

func (m *mockCatalog) ListTablesInSchema(schemaName string) []*catalog.TableDef {
	return m.catalog.ListTablesInSchema(schemaName)
}

func (m *mockCatalog) GetTableInSchema(schemaName, tableName string) (*catalog.TableDef, bool) {
	return m.catalog.GetTableInSchema(schemaName, tableName)
}

func (m *mockCatalog) GetViewInSchema(schemaName, viewName string) (*catalog.ViewDef, bool) {
	return m.catalog.GetViewInSchema(schemaName, viewName)
}

func (m *mockCatalog) GetIndexesForTable(schemaName, tableName string) []*catalog.IndexDef {
	return m.catalog.GetIndexesForTable(schemaName, tableName)
}

func (m *mockCatalog) GetSequenceInSchema(
	schemaName, sequenceName string,
) (*catalog.SequenceDef, bool) {
	return m.catalog.GetSequenceInSchema(schemaName, sequenceName)
}

// createTestCatalog creates a mock catalog with test data.
func createTestCatalog() *mockCatalog {
	mock := newMockCatalog()

	// Create a test table
	usersCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("email", dukdb.TYPE_VARCHAR).WithNullable(true),
		catalog.NewColumnDef("created_at", dukdb.TYPE_TIMESTAMP).
			WithNullable(true).
			WithDefault("CURRENT_TIMESTAMP"),
	}
	usersTable := catalog.NewTableDef("users", usersCols)
	_ = usersTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(usersTable)

	// Create another test table
	postsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("user_id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("title", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("content", dukdb.TYPE_VARCHAR).WithNullable(true),
	}
	postsTable := catalog.NewTableDef("posts", postsCols)
	_ = postsTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(postsTable)

	// Create a view
	view := catalog.NewViewDef("active_users", "main", "SELECT * FROM users WHERE active = true")
	_ = mock.catalog.CreateView(view)

	// Create a unique index
	idx := catalog.NewIndexDef("users_email_idx", "main", "users", []string{"email"}, true)
	_ = mock.catalog.CreateIndex(idx)

	// Create a sequence
	seq := catalog.NewSequenceDef("user_id_seq", "main")
	seq.StartWith = 1
	seq.IncrementBy = 1
	_ = mock.catalog.CreateSequence(seq)

	return mock
}

// TestQueryTables tests information_schema.tables queries.
func TestQueryTables(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all tables", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.tables")
		require.NotNil(t, result)
		assert.Equal(t, tablesColumns, result.Columns)

		// Should have 2 tables + 1 view
		assert.Len(t, result.Rows, 3)

		// Check column values
		for _, row := range result.Rows {
			assert.Equal(t, "dukdb", row["table_catalog"])
			assert.Equal(t, "main", row["table_schema"])
			assert.NotEmpty(t, row["table_name"])
		}
	})

	t.Run("filter by table name", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.tables WHERE table_name = 'users'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "users", result.Rows[0]["table_name"])
		assert.Equal(t, "BASE TABLE", result.Rows[0]["table_type"])
	})

	t.Run("filter by table type view", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "active_users", result.Rows[0]["table_name"])
	})
}

// TestQueryColumns tests information_schema.columns queries.
func TestQueryColumns(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all columns", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.columns")
		require.NotNil(t, result)
		assert.Equal(t, columnsColumns, result.Columns)

		// Should have 4 columns from users + 4 from posts = 8
		assert.Len(t, result.Rows, 8)
	})

	t.Run("filter by table name", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.columns WHERE table_name = 'users'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 4)

		// Check ordinal positions
		for i, row := range result.Rows {
			assert.Equal(t, i+1, row["ordinal_position"])
		}
	})

	t.Run("filter by column name", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.columns WHERE column_name = 'id'")
		require.NotNil(t, result)
		// Both users and posts have id column
		assert.Len(t, result.Rows, 2)
	})

	t.Run("check data types", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'id'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "integer", row["data_type"])
		assert.Equal(t, "NO", row["is_nullable"])
	})

	t.Run("check nullable column", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'email'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "YES", row["is_nullable"])
	})
}

// TestQuerySchemata tests information_schema.schemata queries.
func TestQuerySchemata(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all schemata", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.schemata")
		require.NotNil(t, result)
		assert.Equal(t, schemataColumns, result.Columns)

		// Should have main + information_schema + pg_catalog
		assert.Len(t, result.Rows, 3)

		// Check that main schema is present
		found := false
		for _, row := range result.Rows {
			if row["schema_name"] == "main" {
				found = true
				break
			}
		}
		assert.True(t, found, "main schema should be present")
	})

	t.Run("filter by schema name", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.schemata WHERE schema_name = 'main'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "main", result.Rows[0]["schema_name"])
	})
}

// TestQueryViews tests information_schema.views queries.
func TestQueryViews(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all views", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.views")
		require.NotNil(t, result)
		assert.Equal(t, viewsColumns, result.Columns)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "active_users", row["table_name"])
		assert.Equal(t, "SELECT * FROM users WHERE active = true", row["view_definition"])
	})
}

// TestQueryTableConstraints tests information_schema.table_constraints queries.
func TestQueryTableConstraints(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all constraints", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.table_constraints")
		require.NotNil(t, result)
		assert.Equal(t, tableConstraintsColumns, result.Columns)

		// Should have 2 primary keys + 1 unique index
		assert.Len(t, result.Rows, 3)
	})

	t.Run("filter by table name", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.table_constraints WHERE table_name = 'users'",
		)
		require.NotNil(t, result)
		// users has primary key + unique index on email
		assert.Len(t, result.Rows, 2)
	})

	t.Run("filter by constraint type", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY'",
		)
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 2)
	})
}

// TestQueryKeyColumnUsage tests information_schema.key_column_usage queries.
func TestQueryKeyColumnUsage(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all key columns", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.key_column_usage")
		require.NotNil(t, result)
		assert.Equal(t, keyColumnUsageColumns, result.Columns)

		// Should have 2 primary key columns + 1 unique index column
		assert.Len(t, result.Rows, 3)
	})

	t.Run("filter by constraint name", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.key_column_usage WHERE constraint_name = 'users_pkey'",
		)
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "id", result.Rows[0]["column_name"])
	})
}

// TestQuerySequences tests information_schema.sequences queries.
func TestQuerySequences(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("select all sequences", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.sequences")
		require.NotNil(t, result)
		assert.Equal(t, sequencesColumns, result.Columns)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "user_id_seq", row["sequence_name"])
		assert.Equal(t, "bigint", row["data_type"])
		assert.Equal(t, "1", row["start_value"])
		assert.Equal(t, "1", row["increment"])
	})
}

// TestUnsupportedView tests that unsupported views return nil.
func TestUnsupportedView(t *testing.T) {
	mock := createTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	result := is.Query("SELECT * FROM information_schema.unsupported_view")
	assert.Nil(t, result)
}
