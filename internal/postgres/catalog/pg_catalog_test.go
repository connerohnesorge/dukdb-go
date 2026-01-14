package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsPgCatalogQuery tests detection of pg_catalog queries.
func TestIsPgCatalogQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "select from pg_namespace",
			query:    "SELECT * FROM pg_catalog.pg_namespace",
			expected: true,
		},
		{
			name:     "select from pg_class lowercase",
			query:    "select * from pg_catalog.pg_class",
			expected: true,
		},
		{
			name:     "select from pg_catalog mixed case",
			query:    "SELECT * FROM Pg_Catalog.Pg_Type",
			expected: true,
		},
		{
			name:     "select from regular table",
			query:    "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "select from pg_settings with where",
			query:    "SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPgCatalogQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetPgCatalogViewName tests extracting the view name from pg_catalog queries.
func TestGetPgCatalogViewName(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "pg_namespace view",
			query:    "SELECT * FROM pg_catalog.pg_namespace",
			expected: "pg_namespace",
		},
		{
			name:     "pg_class view with where",
			query:    "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'",
			expected: "pg_class",
		},
		{
			name:     "pg_attribute view",
			query:    "SELECT attname FROM pg_catalog.pg_attribute",
			expected: "pg_attribute",
		},
		{
			name:     "pg_type view",
			query:    "SELECT * FROM pg_catalog.pg_type",
			expected: "pg_type",
		},
		{
			name:     "not pg_catalog",
			query:    "SELECT * FROM users",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetPgCatalogViewName(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestQueryPgNamespace tests pg_catalog.pg_namespace queries.
func TestQueryPgNamespace(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all namespaces", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_namespace")
		require.NotNil(t, result)
		assert.Equal(t, pgNamespaceColumns, result.Columns)

		// Should have pg_catalog + information_schema + public (from main)
		assert.GreaterOrEqual(t, len(result.Rows), 3)

		// Check that system namespaces are present
		foundPgCatalog := false
		foundInfoSchema := false
		foundPublic := false
		for _, row := range result.Rows {
			switch row["nspname"] {
			case "pg_catalog":
				foundPgCatalog = true
				assert.Equal(t, pgCatalogNamespaceOID, row["oid"])
			case "information_schema":
				foundInfoSchema = true
			case "public":
				foundPublic = true
			}
		}
		assert.True(t, foundPgCatalog, "pg_catalog namespace should be present")
		assert.True(t, foundInfoSchema, "information_schema namespace should be present")
		assert.True(t, foundPublic, "public namespace should be present")
	})

	t.Run("filter by nspname", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "pg_catalog", result.Rows[0]["nspname"])
	})
}

// TestQueryPgClass tests pg_catalog.pg_class queries.
func TestQueryPgClass(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all relations", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_class")
		require.NotNil(t, result)
		assert.Equal(t, pgClassColumns, result.Columns)

		// Should have tables, views, indexes, and sequences
		assert.GreaterOrEqual(t, len(result.Rows), 4) // At least 2 tables + 1 view + 1 index + 1 sequence
	})

	t.Run("filter by relkind table", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'")
		require.NotNil(t, result)
		// Should have users and posts tables
		assert.GreaterOrEqual(t, len(result.Rows), 2)

		for _, row := range result.Rows {
			assert.Equal(t, relKindTable, row["relkind"])
		}
	})

	t.Run("filter by relkind view", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'v'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "active_users", result.Rows[0]["relname"])
	})

	t.Run("filter by relkind index", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'i'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "users_email_idx", result.Rows[0]["relname"])
	})

	t.Run("filter by relkind sequence", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'S'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "user_id_seq", result.Rows[0]["relname"])
	})
}

// TestQueryPgAttribute tests pg_catalog.pg_attribute queries.
func TestQueryPgAttribute(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all attributes", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_attribute")
		require.NotNil(t, result)
		assert.Equal(t, pgAttributeColumns, result.Columns)

		// Should have 4 columns from users + 4 from posts = 8
		assert.Len(t, result.Rows, 8)
	})

	t.Run("check column attributes", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_attribute WHERE attname = 'id'")
		require.NotNil(t, result)
		// Both users and posts have id column
		assert.Len(t, result.Rows, 2)

		for _, row := range result.Rows {
			assert.Equal(t, "id", row["attname"])
			assert.Equal(t, int64(1), row["attnum"]) // First column
			assert.Equal(t, true, row["attnotnull"]) // NOT NULL
		}
	})
}

// TestQueryPgType tests pg_catalog.pg_type queries.
func TestQueryPgType(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all types", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_type")
		require.NotNil(t, result)
		assert.Equal(t, pgTypeColumns, result.Columns)

		// Should have all built-in types
		assert.GreaterOrEqual(t, len(result.Rows), len(builtinTypes))
	})

	t.Run("filter by typname", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_type WHERE typname = 'int4'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "int4", row["typname"])
		assert.Equal(t, int64(23), row["oid"]) // OID_INT4
		assert.Equal(t, int64(4), row["typlen"])
		assert.Equal(t, true, row["typbyval"])
	})

	t.Run("filter by typcategory numeric", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'N'")
		require.NotNil(t, result)
		// Should have int2, int4, int8, float4, float8, numeric, oid
		assert.GreaterOrEqual(t, len(result.Rows), 5)
	})

	t.Run("filter array types", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'A'")
		require.NotNil(t, result)
		assert.GreaterOrEqual(t, len(result.Rows), 5)

		for _, row := range result.Rows {
			// Array type names start with underscore
			typname := row["typname"].(string)
			assert.Equal(t, "_", typname[:1])
		}
	})
}

// TestQueryPgIndex tests pg_catalog.pg_index queries.
func TestQueryPgIndex(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all indexes", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_index")
		require.NotNil(t, result)
		assert.Equal(t, pgIndexColumns, result.Columns)

		// Should have at least the users_email_idx
		assert.GreaterOrEqual(t, len(result.Rows), 1)
	})

	t.Run("check unique index exists", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_index")
		require.NotNil(t, result)

		// Find the unique index
		foundUnique := false
		for _, row := range result.Rows {
			if row["indisunique"] == true {
				foundUnique = true
				break
			}
		}
		assert.True(t, foundUnique, "should have at least one unique index (users_email_idx)")
	})
}

// TestQueryPgDatabase tests pg_catalog.pg_database queries.
func TestQueryPgDatabase(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all databases", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_database")
		require.NotNil(t, result)
		assert.Equal(t, pgDatabaseColumns, result.Columns)

		// Should have dukdb + template0 + template1
		assert.Len(t, result.Rows, 3)
	})

	t.Run("filter by datname", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_database WHERE datname = 'dukdb'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "dukdb", result.Rows[0]["datname"])
		assert.Equal(t, true, result.Rows[0]["datallowconn"])
	})
}

// TestQueryPgSettings tests pg_catalog.pg_settings queries.
func TestQueryPgSettings(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all settings", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_settings")
		require.NotNil(t, result)
		assert.Equal(t, pgSettingsColumns, result.Columns)

		// Should have all server settings
		assert.Equal(t, len(serverSettings), len(result.Rows))
	})

	t.Run("filter by name", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_settings WHERE name = 'server_version'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "server_version", row["name"])
		assert.Contains(t, row["setting"], "DukDB")
	})

	t.Run("filter by category", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_settings WHERE category = 'Preset Options'")
		require.NotNil(t, result)
		assert.GreaterOrEqual(t, len(result.Rows), 2) // server_version, server_version_num
	})
}

// TestQueryPgTables tests pg_catalog.pg_tables queries.
func TestQueryPgTables(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all tables", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_tables")
		require.NotNil(t, result)
		assert.Equal(t, pgTablesColumns, result.Columns)

		// Should have users and posts
		assert.Len(t, result.Rows, 2)
	})

	t.Run("filter by schemaname", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 2)

		for _, row := range result.Rows {
			assert.Equal(t, "public", row["schemaname"])
		}
	})

	t.Run("filter by tablename", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'users'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "users", result.Rows[0]["tablename"])
	})
}

// TestQueryPgViews tests pg_catalog.pg_views queries.
func TestQueryPgViews(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all views", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_views")
		require.NotNil(t, result)
		assert.Equal(t, pgViewsColumns, result.Columns)

		// Should have active_users view
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "active_users", result.Rows[0]["viewname"])
	})
}

// TestQueryPgProc tests pg_catalog.pg_proc queries.
func TestQueryPgProc(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all functions", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_proc")
		require.NotNil(t, result)
		assert.Equal(t, pgProcColumns, result.Columns)

		// Should have all built-in functions
		assert.Equal(t, len(builtinFunctions), len(result.Rows))
	})

	t.Run("filter by proname", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_proc WHERE proname = 'now'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "now", result.Rows[0]["proname"])
	})

	t.Run("filter aggregate functions", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_proc WHERE prokind = 'a'")
		require.NotNil(t, result)
		assert.GreaterOrEqual(t, len(result.Rows), 5) // count, sum, avg, min, max, etc.
	})
}

// TestQueryPgConstraint tests pg_catalog.pg_constraint queries.
func TestQueryPgConstraint(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	t.Run("select all constraints", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_constraint")
		require.NotNil(t, result)
		assert.Equal(t, pgConstraintColumns, result.Columns)

		// Should have 2 primary keys + 1 unique constraint
		assert.Len(t, result.Rows, 3)
	})

	t.Run("filter by contype primary key", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_constraint WHERE contype = 'p'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // users_pkey, posts_pkey
	})

	t.Run("filter by contype unique", func(t *testing.T) {
		result := pgCat.Query("SELECT * FROM pg_catalog.pg_constraint WHERE contype = 'u'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1) // users_email_idx
	})
}

// TestUnsupportedPgCatalogView tests that unsupported views return nil.
func TestUnsupportedPgCatalogView(t *testing.T) {
	mock := createTestCatalog()
	pgCat := NewPgCatalog(mock, "dukdb")

	result := pgCat.Query("SELECT * FROM pg_catalog.pg_unsupported_view")
	assert.Nil(t, result)
}

// TestGenerateOID tests OID generation consistency.
func TestGenerateOID(t *testing.T) {
	// Same input should always produce same OID
	oid1 := generateOID("table:main.users")
	oid2 := generateOID("table:main.users")
	assert.Equal(t, oid1, oid2)

	// Different inputs should (very likely) produce different OIDs
	oid3 := generateOID("table:main.posts")
	assert.NotEqual(t, oid1, oid3)

	// OIDs should be >= 16384 (to avoid conflicts with PostgreSQL built-in OIDs)
	assert.GreaterOrEqual(t, oid1, int64(16384))
}
