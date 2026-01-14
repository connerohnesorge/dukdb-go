package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalcatalog "github.com/dukdb/dukdb-go/internal/catalog"
	pgcatalog "github.com/dukdb/dukdb-go/internal/postgres/catalog"

	dukdb "github.com/dukdb/dukdb-go"
)

// TestIsCatalogQuery tests detection of catalog queries.
func TestIsCatalogQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "information_schema.tables",
			query:    "SELECT * FROM information_schema.tables",
			expected: true,
		},
		{
			name:     "information_schema.columns",
			query:    "SELECT * FROM information_schema.columns WHERE table_name = 'users'",
			expected: true,
		},
		{
			name:     "pg_catalog.pg_namespace",
			query:    "SELECT * FROM pg_catalog.pg_namespace",
			expected: true,
		},
		{
			name:     "pg_catalog.pg_class",
			query:    "SELECT relname FROM pg_catalog.pg_class",
			expected: true,
		},
		{
			name:     "regular table",
			query:    "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "mixed case information_schema",
			query:    "SELECT * FROM Information_Schema.Tables",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCatalogQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsPgFunctionQuery tests detection of PostgreSQL system function calls.
func TestIsPgFunctionQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "current_database",
			query:    "SELECT current_database()",
			expected: true,
		},
		{
			name:     "current_schema",
			query:    "SELECT current_schema()",
			expected: true,
		},
		{
			name:     "version",
			query:    "SELECT version()",
			expected: true,
		},
		{
			name:     "current_user",
			query:    "SELECT current_user",
			expected: true,
		},
		{
			name:     "session_user",
			query:    "SELECT session_user",
			expected: true,
		},
		{
			name:     "pg_backend_pid",
			query:    "SELECT pg_backend_pid()",
			expected: true,
		},
		{
			name:     "pg_get_userbyid",
			query:    "SELECT pg_get_userbyid(10)",
			expected: true,
		},
		{
			name:     "pg_catalog qualified",
			query:    "SELECT pg_catalog.pg_get_userbyid(10)",
			expected: true,
		},
		{
			name:     "pg_encoding_to_char",
			query:    "SELECT pg_encoding_to_char(6)",
			expected: true,
		},
		{
			name:     "pg_is_in_recovery",
			query:    "SELECT pg_is_in_recovery()",
			expected: true,
		},
		{
			name:     "regular select",
			query:    "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "multiple functions",
			query:    "SELECT current_database(), current_user, version()",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPgFunctionQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRewritePgFunctions tests rewriting PostgreSQL system functions.
func TestRewritePgFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		dbName   string
		username string
		version  string
		expected string
	}{
		{
			name:     "current_database",
			query:    "SELECT current_database()",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'testdb'",
		},
		{
			name:     "current_schema",
			query:    "SELECT current_schema()",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'public'",
		},
		{
			name:     "current_user",
			query:    "SELECT current_user",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'testuser'",
		},
		{
			name:     "session_user",
			query:    "SELECT session_user",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'testuser'",
		},
		{
			name:     "version",
			query:    "SELECT version()",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'PostgreSQL 14.0 (dukdb-go compatible)'",
		},
		{
			name:     "pg_get_userbyid",
			query:    "SELECT pg_get_userbyid(10)",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'testuser'",
		},
		{
			name:     "pg_encoding_to_char",
			query:    "SELECT pg_encoding_to_char(6)",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT 'UTF8'",
		},
		{
			name:     "pg_is_in_recovery",
			query:    "SELECT pg_is_in_recovery()",
			dbName:   "testdb",
			username: "testuser",
			version:  "14.0",
			expected: "SELECT false",
		},
		{
			name:     "multiple functions",
			query:    "SELECT current_database(), current_user",
			dbName:   "mydb",
			username: "myuser",
			version:  "14.0",
			expected: "SELECT 'mydb', 'myuser'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RewritePgFunctions(tt.query, tt.dbName, tt.username, tt.version, 12345)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsSelectFromDual tests detection of Oracle/MySQL dual table queries.
func TestIsSelectFromDual(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "from dual uppercase",
			query:    "SELECT 1 FROM DUAL",
			expected: true,
		},
		{
			name:     "from dual lowercase",
			query:    "select 1 from dual",
			expected: true,
		},
		{
			name:     "from dual mixed case",
			query:    "SELECT 1 FROM Dual",
			expected: true,
		},
		{
			name:     "regular select",
			query:    "SELECT * FROM users",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSelectFromDual(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestHandleSelectFromDual tests rewriting Oracle/MySQL dual table queries.
func TestHandleSelectFromDual(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "simple select from dual",
			query:    "SELECT 1 FROM DUAL",
			expected: "SELECT 1",
		},
		{
			name:     "select with expression from dual",
			query:    "SELECT 1 + 2 FROM dual",
			expected: "SELECT 1 + 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HandleSelectFromDual(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// mockCatalogAdapter implements CatalogProvider for testing
type mockCatalogAdapter struct {
	catalog *internalcatalog.Catalog
}

func newMockCatalogAdapter() *mockCatalogAdapter {
	cat := internalcatalog.NewCatalog()
	return &mockCatalogAdapter{catalog: cat}
}

func (m *mockCatalogAdapter) ListSchemas() []*internalcatalog.Schema {
	return m.catalog.ListSchemas()
}

func (m *mockCatalogAdapter) GetSchema(name string) (*internalcatalog.Schema, bool) {
	return m.catalog.GetSchema(name)
}

func (m *mockCatalogAdapter) ListTablesInSchema(schemaName string) []*internalcatalog.TableDef {
	return m.catalog.ListTablesInSchema(schemaName)
}

func (m *mockCatalogAdapter) GetTableInSchema(schemaName, tableName string) (*internalcatalog.TableDef, bool) {
	return m.catalog.GetTableInSchema(schemaName, tableName)
}

func (m *mockCatalogAdapter) GetViewInSchema(schemaName, viewName string) (*internalcatalog.ViewDef, bool) {
	return m.catalog.GetViewInSchema(schemaName, viewName)
}

func (m *mockCatalogAdapter) GetIndexesForTable(schemaName, tableName string) []*internalcatalog.IndexDef {
	return m.catalog.GetIndexesForTable(schemaName, tableName)
}

func (m *mockCatalogAdapter) GetSequenceInSchema(schemaName, sequenceName string) (*internalcatalog.SequenceDef, bool) {
	return m.catalog.GetSequenceInSchema(schemaName, sequenceName)
}

// TestCatalogHandler tests the catalog handler.
func TestCatalogHandler(t *testing.T) {
	mock := newMockCatalogAdapter()

	// Create a test table
	usersCols := []*internalcatalog.ColumnDef{
		internalcatalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		internalcatalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).WithNullable(false),
		internalcatalog.NewColumnDef("email", dukdb.TYPE_VARCHAR).WithNullable(true),
	}
	usersTable := internalcatalog.NewTableDef("users", usersCols)
	_ = usersTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(usersTable)

	// Create catalog handler
	handler := NewCatalogHandler(mock, "testdb")
	require.NotNil(t, handler)

	t.Run("query information_schema.tables", func(t *testing.T) {
		result := handler.infoSchema.Query("SELECT * FROM information_schema.tables")
		require.NotNil(t, result)

		// Should have the users table
		found := false
		for _, row := range result.Rows {
			if row["table_name"] != "users" {
				continue
			}
			found = true
			assert.Equal(t, "testdb", row["table_catalog"])
			assert.Equal(t, "main", row["table_schema"])
			assert.Equal(t, "BASE TABLE", row["table_type"])
		}
		assert.True(t, found, "users table should be present")
	})

	t.Run("query information_schema.columns", func(t *testing.T) {
		result := handler.infoSchema.Query("SELECT * FROM information_schema.columns WHERE table_name = 'users'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 3) // id, name, email
	})

	t.Run("query pg_catalog.pg_namespace", func(t *testing.T) {
		result := handler.pgCatalog.Query("SELECT * FROM pg_catalog.pg_namespace")
		require.NotNil(t, result)

		// Should have system schemas
		schemaNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["nspname"].(string); ok {
				schemaNames = append(schemaNames, name)
			}
		}
		assert.Contains(t, schemaNames, "pg_catalog")
		assert.Contains(t, schemaNames, "information_schema")
	})
}

// TestCatalogProviderAdapter tests the catalog provider adapter.
func TestCatalogProviderAdapter(t *testing.T) {
	cat := internalcatalog.NewCatalog()

	// Create a test table
	cols := []*internalcatalog.ColumnDef{
		internalcatalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	}
	table := internalcatalog.NewTableDef("test_table", cols)
	err := cat.CreateTable(table)
	require.NoError(t, err)

	// Create adapter
	adapter := newCatalogProviderAdapter(cat)

	t.Run("ListSchemas", func(t *testing.T) {
		schemas := adapter.ListSchemas()
		assert.NotEmpty(t, schemas)

		schemaNames := make([]string, 0)
		for _, s := range schemas {
			schemaNames = append(schemaNames, s.Name())
		}
		assert.Contains(t, schemaNames, "main")
	})

	t.Run("GetSchema", func(t *testing.T) {
		schema, ok := adapter.GetSchema("main")
		assert.True(t, ok)
		assert.NotNil(t, schema)
		assert.Equal(t, "main", schema.Name())

		_, ok = adapter.GetSchema("nonexistent")
		assert.False(t, ok)
	})

	t.Run("ListTablesInSchema", func(t *testing.T) {
		tables := adapter.ListTablesInSchema("main")
		assert.NotEmpty(t, tables)

		tableNames := make([]string, 0)
		for _, t := range tables {
			tableNames = append(tableNames, t.Name)
		}
		assert.Contains(t, tableNames, "test_table")
	})

	t.Run("GetTableInSchema", func(t *testing.T) {
		table, ok := adapter.GetTableInSchema("main", "test_table")
		assert.True(t, ok)
		assert.NotNil(t, table)
		assert.Equal(t, "test_table", table.Name)

		_, ok = adapter.GetTableInSchema("main", "nonexistent")
		assert.False(t, ok)
	})
}

// Compile-time assertion that catalogProviderAdapter implements pgcatalog.CatalogProvider
var _ pgcatalog.CatalogProvider = (*catalogProviderAdapter)(nil)
