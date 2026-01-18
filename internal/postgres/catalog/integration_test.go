package catalog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// This file contains integration tests for system view queries that are commonly
// executed by ORMs, database tools, and PostgreSQL clients like psql, pgx,
// Prisma, TypeORM, and SQLAlchemy.

// Constants for common PostgreSQL values used in tests
const (
	pgNullableYes = "YES"
	pgNullableNo  = "NO"
)

// =============================================================================
// Test Data Setup
// =============================================================================

// createORMTestCatalog creates a catalog with comprehensive test data that
// simulates a realistic database schema with multiple tables, views, indexes,
// constraints, and sequences - similar to what ORMs would encounter.
func createORMTestCatalog() *mockCatalog {
	mock := newMockCatalog()

	// Create accounts table (e.g., for multi-tenant apps)
	accountsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("created_at", dukdb.TYPE_TIMESTAMP).
			WithNullable(true).
			WithDefault("CURRENT_TIMESTAMP"),
		catalog.NewColumnDef("is_active", dukdb.TYPE_BOOLEAN).
			WithNullable(false).
			WithDefault("true"),
	}
	accountsTable := catalog.NewTableDef("accounts", accountsCols)
	_ = accountsTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(accountsTable)

	// Create users table with foreign key relationship
	usersCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("account_id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("email", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("password_hash", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("first_name", dukdb.TYPE_VARCHAR).WithNullable(true),
		catalog.NewColumnDef("last_name", dukdb.TYPE_VARCHAR).WithNullable(true),
		catalog.NewColumnDef("role", dukdb.TYPE_VARCHAR).WithNullable(false).WithDefault("'user'"),
		catalog.NewColumnDef("created_at", dukdb.TYPE_TIMESTAMP).
			WithNullable(true).
			WithDefault("CURRENT_TIMESTAMP"),
		catalog.NewColumnDef("updated_at", dukdb.TYPE_TIMESTAMP).WithNullable(true),
		catalog.NewColumnDef("deleted_at", dukdb.TYPE_TIMESTAMP).WithNullable(true),
	}
	usersTable := catalog.NewTableDef("users", usersCols)
	_ = usersTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(usersTable)

	// Create posts table
	postsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_BIGINT).WithNullable(false),
		catalog.NewColumnDef("author_id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("title", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("slug", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("content", dukdb.TYPE_VARCHAR).WithNullable(true),
		catalog.NewColumnDef("published", dukdb.TYPE_BOOLEAN).
			WithNullable(false).
			WithDefault("false"),
		catalog.NewColumnDef("view_count", dukdb.TYPE_INTEGER).WithNullable(false).WithDefault("0"),
		catalog.NewColumnDef("created_at", dukdb.TYPE_TIMESTAMP).WithNullable(true),
		catalog.NewColumnDef("updated_at", dukdb.TYPE_TIMESTAMP).WithNullable(true),
	}
	postsTable := catalog.NewTableDef("posts", postsCols)
	_ = postsTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(postsTable)

	// Create comments table
	commentsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_BIGINT).WithNullable(false),
		catalog.NewColumnDef("post_id", dukdb.TYPE_BIGINT).WithNullable(false),
		catalog.NewColumnDef("user_id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("parent_id", dukdb.TYPE_BIGINT).WithNullable(true),
		catalog.NewColumnDef("body", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("created_at", dukdb.TYPE_TIMESTAMP).WithNullable(true),
	}
	commentsTable := catalog.NewTableDef("comments", commentsCols)
	_ = commentsTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(commentsTable)

	// Create tags table
	tagsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).WithNullable(false),
		catalog.NewColumnDef("slug", dukdb.TYPE_VARCHAR).WithNullable(false),
	}
	tagsTable := catalog.NewTableDef("tags", tagsCols)
	_ = tagsTable.SetPrimaryKey([]string{"id"})
	_ = mock.catalog.CreateTable(tagsTable)

	// Create post_tags junction table (many-to-many)
	postTagsCols := []*catalog.ColumnDef{
		catalog.NewColumnDef("post_id", dukdb.TYPE_BIGINT).WithNullable(false),
		catalog.NewColumnDef("tag_id", dukdb.TYPE_INTEGER).WithNullable(false),
	}
	postTagsTable := catalog.NewTableDef("post_tags", postTagsCols)
	_ = postTagsTable.SetPrimaryKey([]string{"post_id", "tag_id"})
	_ = mock.catalog.CreateTable(postTagsTable)

	// Create views
	activeUsersView := catalog.NewViewDef("active_users", "main",
		"SELECT id, email, first_name, last_name FROM users WHERE deleted_at IS NULL")
	_ = mock.catalog.CreateView(activeUsersView)

	publishedPostsView := catalog.NewViewDef(
		"published_posts",
		"main",
		"SELECT p.id, p.title, p.slug, u.email AS author_email FROM posts p JOIN users u ON p.author_id = u.id WHERE p.published = true",
	)
	_ = mock.catalog.CreateView(publishedPostsView)

	// Create indexes
	usersEmailIdx := catalog.NewIndexDef(
		"users_email_idx",
		"main",
		"users",
		[]string{"email"},
		true,
	)
	_ = mock.catalog.CreateIndex(usersEmailIdx)

	usersAccountIdx := catalog.NewIndexDef(
		"users_account_id_idx",
		"main",
		"users",
		[]string{"account_id"},
		false,
	)
	_ = mock.catalog.CreateIndex(usersAccountIdx)

	postsSlugIdx := catalog.NewIndexDef("posts_slug_idx", "main", "posts", []string{"slug"}, true)
	_ = mock.catalog.CreateIndex(postsSlugIdx)

	postsAuthorIdx := catalog.NewIndexDef(
		"posts_author_id_idx",
		"main",
		"posts",
		[]string{"author_id"},
		false,
	)
	_ = mock.catalog.CreateIndex(postsAuthorIdx)

	commentsPostIdx := catalog.NewIndexDef(
		"comments_post_id_idx",
		"main",
		"comments",
		[]string{"post_id"},
		false,
	)
	_ = mock.catalog.CreateIndex(commentsPostIdx)

	tagsSlugIdx := catalog.NewIndexDef("tags_slug_idx", "main", "tags", []string{"slug"}, true)
	_ = mock.catalog.CreateIndex(tagsSlugIdx)

	// Create sequences
	usersSeq := catalog.NewSequenceDef("users_id_seq", "main")
	usersSeq.StartWith = 1
	usersSeq.IncrementBy = 1
	_ = mock.catalog.CreateSequence(usersSeq)

	postsSeq := catalog.NewSequenceDef("posts_id_seq", "main")
	postsSeq.StartWith = 1
	postsSeq.IncrementBy = 1
	_ = mock.catalog.CreateSequence(postsSeq)

	commentsSeq := catalog.NewSequenceDef("comments_id_seq", "main")
	commentsSeq.StartWith = 1
	commentsSeq.IncrementBy = 1
	_ = mock.catalog.CreateSequence(commentsSeq)

	return mock
}

// =============================================================================
// Prisma Introspection Query Tests
// =============================================================================

// TestPrismaIntrospectionQueries tests queries that Prisma uses to introspect
// the database schema. Prisma is a popular TypeScript/JavaScript ORM.
func TestPrismaIntrospectionQueries(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("prisma columns introspection", func(t *testing.T) {
		// Prisma query to get column information
		query := `SELECT table_name, column_name, data_type, is_nullable, column_default
                  FROM information_schema.columns
                  WHERE table_schema = 'main'`
		result := is.Query(query)
		require.NotNil(t, result)

		// Should have all columns from all tables
		assert.GreaterOrEqual(t, len(result.Rows), 20) // We have many columns across tables

		// Verify expected columns exist
		foundColumns := make(map[string]bool)
		for _, row := range result.Rows {
			tableName := row["table_name"].(string)
			columnName := row["column_name"].(string)
			foundColumns[tableName+"."+columnName] = true

			// Verify data_type is present
			assert.NotEmpty(
				t,
				row["data_type"],
				"data_type should be present for %s.%s",
				tableName,
				columnName,
			)
			// Verify is_nullable is YES or NO
			isNullable := row["is_nullable"].(string)
			assert.True(
				t,
				isNullable == pgNullableYes || isNullable == pgNullableNo,
				"is_nullable should be YES or NO for %s.%s, got %s",
				tableName,
				columnName,
				isNullable,
			)
		}

		// Verify key columns exist
		assert.True(t, foundColumns["users.id"], "users.id should exist")
		assert.True(t, foundColumns["users.email"], "users.email should exist")
		assert.True(t, foundColumns["posts.title"], "posts.title should exist")
		assert.True(t, foundColumns["posts.published"], "posts.published should exist")
	})

	t.Run("prisma constraints introspection", func(t *testing.T) {
		// Prisma query to get constraints and key columns (simplified)
		result := is.Query(`SELECT constraint_name, constraint_type, table_name
                           FROM information_schema.table_constraints
                           WHERE table_schema = 'main'`)
		require.NotNil(t, result)

		// Should have primary keys and unique constraints
		primaryKeys := 0
		uniqueConstraints := 0
		for _, row := range result.Rows {
			constraintType := row["constraint_type"].(string)
			switch constraintType {
			case "PRIMARY KEY":
				primaryKeys++
			case "UNIQUE":
				uniqueConstraints++
			}
		}

		assert.Equal(
			t,
			6,
			primaryKeys,
			"should have 6 primary keys (accounts, users, posts, comments, tags, post_tags)",
		)
		assert.GreaterOrEqual(t, uniqueConstraints, 3, "should have at least 3 unique constraints")
	})

	t.Run("prisma key column usage introspection", func(t *testing.T) {
		// Query to get key column usage
		result := is.Query(`SELECT constraint_name, column_name, table_name
                           FROM information_schema.key_column_usage
                           WHERE table_schema = 'main'`)
		require.NotNil(t, result)

		// Check post_tags has composite primary key
		postTagsColumns := make([]string, 0)
		for _, row := range result.Rows {
			if row["table_name"] == "post_tags" && row["constraint_name"] == "post_tags_pkey" {
				postTagsColumns = append(postTagsColumns, row["column_name"].(string))
			}
		}
		assert.Len(t, postTagsColumns, 2, "post_tags should have 2 columns in primary key")
		assert.Contains(t, postTagsColumns, "post_id")
		assert.Contains(t, postTagsColumns, "tag_id")
	})
}

// =============================================================================
// TypeORM Query Tests
// =============================================================================

// TestTypeORMQueries tests queries that TypeORM uses for schema introspection.
func TestTypeORMQueries(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("typeorm tables query", func(t *testing.T) {
		// TypeORM query for tables
		result := is.Query("SELECT * FROM information_schema.tables WHERE table_schema = 'main'")
		require.NotNil(t, result)

		tableNames := make([]string, 0)
		for _, row := range result.Rows {
			if row["table_type"] == "BASE TABLE" {
				tableNames = append(tableNames, row["table_name"].(string))
			}
		}

		assert.Contains(t, tableNames, "users")
		assert.Contains(t, tableNames, "posts")
		assert.Contains(t, tableNames, "comments")
		assert.Contains(t, tableNames, "accounts")
		assert.Contains(t, tableNames, "tags")
		assert.Contains(t, tableNames, "post_tags")
	})

	t.Run("typeorm pg_type query", func(t *testing.T) {
		// TypeORM queries pg_type for type information
		result := pg.Query("SELECT * FROM pg_catalog.pg_type")
		require.NotNil(t, result)

		// Should have built-in types
		typeNames := make(map[string]bool)
		for _, row := range result.Rows {
			if name, ok := row["typname"].(string); ok {
				typeNames[name] = true
			}
		}

		// Check common types exist
		assert.True(t, typeNames["int4"], "int4 type should exist")
		assert.True(t, typeNames["int8"], "int8 type should exist")
		assert.True(t, typeNames["varchar"], "varchar type should exist")
		assert.True(t, typeNames["bool"], "bool type should exist")
		assert.True(t, typeNames["timestamp"], "timestamp type should exist")
	})

	t.Run("typeorm pg_class for tables", func(t *testing.T) {
		// TypeORM queries pg_class for table information
		result := pg.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'")
		require.NotNil(t, result)

		tableNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["relname"].(string); ok {
				tableNames = append(tableNames, name)
			}
		}

		assert.Contains(t, tableNames, "users")
		assert.Contains(t, tableNames, "posts")
	})

	t.Run("typeorm pg_class for views", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'v'")
		require.NotNil(t, result)

		viewNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["relname"].(string); ok {
				viewNames = append(viewNames, name)
			}
		}

		assert.Contains(t, viewNames, "active_users")
		assert.Contains(t, viewNames, "published_posts")
	})

	t.Run("typeorm pg_class for indexes", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'i'")
		require.NotNil(t, result)

		indexNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["relname"].(string); ok {
				indexNames = append(indexNames, name)
			}
		}

		assert.Contains(t, indexNames, "users_email_idx")
		assert.Contains(t, indexNames, "posts_slug_idx")
	})

	t.Run("typeorm pg_class for sequences", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_class WHERE relkind = 'S'")
		require.NotNil(t, result)

		seqNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["relname"].(string); ok {
				seqNames = append(seqNames, name)
			}
		}

		assert.Contains(t, seqNames, "users_id_seq")
		assert.Contains(t, seqNames, "posts_id_seq")
	})
}

// =============================================================================
// psql Startup Query Tests
// =============================================================================

// TestPsqlStartupQueries tests queries that psql executes during startup.
func TestPsqlStartupQueries(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("psql pg_settings for server_version", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_settings WHERE name = 'server_version'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "server_version", row["name"])
		// Should contain version information
		setting := row["setting"].(string)
		assert.Contains(t, setting, "DukDB")
	})

	t.Run("psql pg_settings for search_path", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_settings WHERE name = 'search_path'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "search_path", row["name"])
	})

	t.Run("psql pg_database query", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_database")
		require.NotNil(t, result)

		dbNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["datname"].(string); ok {
				dbNames = append(dbNames, name)
			}
		}

		// Should have the current database
		assert.Contains(t, dbNames, "dukdb")
	})

	t.Run("psql pg_namespace query", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_namespace")
		require.NotNil(t, result)

		namespaceNames := make([]string, 0)
		for _, row := range result.Rows {
			if name, ok := row["nspname"].(string); ok {
				namespaceNames = append(namespaceNames, name)
			}
		}

		assert.Contains(t, namespaceNames, "pg_catalog")
		assert.Contains(t, namespaceNames, "information_schema")
		assert.Contains(t, namespaceNames, "public")
	})
}

// =============================================================================
// SQLAlchemy Reflection Query Tests
// =============================================================================

// TestSQLAlchemyReflectionQueries tests queries that SQLAlchemy uses for reflection.
func TestSQLAlchemyReflectionQueries(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("sqlalchemy pg_class for relations", func(t *testing.T) {
		// SQLAlchemy query for tables and views
		// Note: This is a simplified version - SQLAlchemy uses JOINs
		result := pg.Query("SELECT * FROM pg_catalog.pg_class")
		require.NotNil(t, result)

		// Filter by relkind to simulate the WHERE clause
		tables := make([]string, 0)
		views := make([]string, 0)
		for _, row := range result.Rows {
			relname := row["relname"].(string)
			relkind := row["relkind"].(string)
			switch relkind {
			case "r": // regular table
				tables = append(tables, relname)
			case "v": // view
				views = append(views, relname)
			}
		}

		assert.Contains(t, tables, "users")
		assert.Contains(t, tables, "posts")
		assert.Contains(t, views, "active_users")
		assert.Contains(t, views, "published_posts")
	})

	t.Run("sqlalchemy pg_tables query", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
		require.NotNil(t, result)

		tableNames := make([]string, 0)
		for _, row := range result.Rows {
			tableNames = append(tableNames, row["tablename"].(string))
		}

		assert.Contains(t, tableNames, "users")
		assert.Contains(t, tableNames, "posts")
		assert.Contains(t, tableNames, "comments")
		assert.Contains(t, tableNames, "accounts")
	})

	t.Run("sqlalchemy pg_views query", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_views WHERE schemaname = 'public'")
		require.NotNil(t, result)

		viewNames := make([]string, 0)
		for _, row := range result.Rows {
			viewNames = append(viewNames, row["viewname"].(string))
		}

		assert.Contains(t, viewNames, "active_users")
		assert.Contains(t, viewNames, "published_posts")
	})
}

// =============================================================================
// Column and Data Type Tests
// =============================================================================

// TestColumnMetadataAccuracy tests that column metadata is accurate and complete.
func TestColumnMetadataAccuracy(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("integer column metadata", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'id'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "integer", row["data_type"])
		assert.Equal(t, pgNullableNo, row["is_nullable"])
		assert.Equal(t, 1, row["ordinal_position"])
	})

	t.Run("bigint column metadata", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'posts' AND column_name = 'id'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "bigint", row["data_type"])
		assert.Equal(t, pgNullableNo, row["is_nullable"])
	})

	t.Run("boolean column metadata", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'posts' AND column_name = 'published'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "boolean", row["data_type"])
		assert.Equal(t, pgNullableNo, row["is_nullable"])
	})

	t.Run("timestamp column metadata", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'created_at'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "timestamp without time zone", row["data_type"])
		assert.Equal(t, pgNullableYes, row["is_nullable"])
		// Check datetime precision
		assert.Equal(t, 6, row["datetime_precision"])
	})

	t.Run("varchar column metadata", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'email'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "character varying", row["data_type"])
		assert.Equal(t, pgNullableNo, row["is_nullable"])
		assert.Equal(t, "varchar", row["udt_name"])
	})

	t.Run("column with default value", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'posts' AND column_name = 'view_count'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.NotNil(t, row["column_default"])
		assert.Equal(t, "0", row["column_default"])
	})
}

// =============================================================================
// pg_catalog Attribute Tests
// =============================================================================

// TestPgAttributeMetadata tests pg_catalog.pg_attribute for column metadata.
func TestPgAttributeMetadata(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("attribute for users table", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_attribute")
		require.NotNil(t, result)

		// Find users table columns
		usersColumns := make(map[string]map[string]any)
		for _, row := range result.Rows {
			attname := row["attname"].(string)
			// In a real scenario, we'd filter by attrelid, but for this test
			// we just collect all attributes
			usersColumns[attname] = row
		}

		// Check common columns exist
		assert.Contains(t, usersColumns, "id")
		assert.Contains(t, usersColumns, "email")
		assert.Contains(t, usersColumns, "first_name")
	})

	t.Run("attnum ordering", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_attribute WHERE attname = 'id'")
		require.NotNil(t, result)
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// ID should be first column (attnum = 1) in tables that have it
		for _, row := range result.Rows {
			assert.Equal(t, int64(1), row["attnum"])
		}
	})

	t.Run("attnotnull for required columns", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_attribute WHERE attname = 'email'")
		require.NotNil(t, result)

		for _, row := range result.Rows {
			assert.Equal(t, true, row["attnotnull"], "email column should be NOT NULL")
		}
	})
}

// =============================================================================
// pg_proc (Functions) Tests
// =============================================================================

// TestPgProcFunctions tests pg_catalog.pg_proc for function introspection.
func TestPgProcFunctions(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("query all functions", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_proc")
		require.NotNil(t, result)
		assert.GreaterOrEqual(t, len(result.Rows), 5) // Should have many built-in functions
	})

	t.Run("find aggregate functions", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_proc WHERE prokind = 'a'")
		require.NotNil(t, result)

		aggFunctions := make([]string, 0)
		for _, row := range result.Rows {
			aggFunctions = append(aggFunctions, row["proname"].(string))
		}

		// Should have common aggregates
		assert.Contains(t, aggFunctions, "count")
		assert.Contains(t, aggFunctions, "sum")
		assert.Contains(t, aggFunctions, "avg")
		assert.Contains(t, aggFunctions, "min")
		assert.Contains(t, aggFunctions, "max")
	})

	t.Run("find now function", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_proc WHERE proname = 'now'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
	})
}

// =============================================================================
// pg_constraint Tests
// =============================================================================

// TestPgConstraintMetadata tests pg_catalog.pg_constraint for constraint info.
func TestPgConstraintMetadata(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("query primary key constraints", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_constraint WHERE contype = 'p'")
		require.NotNil(t, result)

		pkNames := make([]string, 0)
		for _, row := range result.Rows {
			pkNames = append(pkNames, row["conname"].(string))
		}

		assert.Contains(t, pkNames, "users_pkey")
		assert.Contains(t, pkNames, "posts_pkey")
		assert.Contains(t, pkNames, "accounts_pkey")
	})

	t.Run("query unique constraints", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_constraint WHERE contype = 'u'")
		require.NotNil(t, result)

		uniqueNames := make([]string, 0)
		for _, row := range result.Rows {
			uniqueNames = append(uniqueNames, row["conname"].(string))
		}

		assert.Contains(t, uniqueNames, "users_email_idx")
		assert.Contains(t, uniqueNames, "posts_slug_idx")
		assert.Contains(t, uniqueNames, "tags_slug_idx")
	})
}

// =============================================================================
// pg_index Tests
// =============================================================================

// TestPgIndexMetadata tests pg_catalog.pg_index for index information.
func TestPgIndexMetadata(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("query all indexes", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_index")
		require.NotNil(t, result)

		assert.GreaterOrEqual(t, len(result.Rows), 6) // We created several indexes
	})

	t.Run("query unique indexes", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_index")
		require.NotNil(t, result)

		uniqueCount := 0
		nonUniqueCount := 0
		for _, row := range result.Rows {
			if row["indisunique"] == true {
				uniqueCount++
			} else {
				nonUniqueCount++
			}
		}

		assert.GreaterOrEqual(t, uniqueCount, 3, "should have at least 3 unique indexes")
		assert.GreaterOrEqual(t, nonUniqueCount, 3, "should have at least 3 non-unique indexes")
	})
}

// =============================================================================
// Sequence Tests
// =============================================================================

// TestSequenceMetadata tests information_schema.sequences for sequence info.
func TestSequenceMetadata(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("query all sequences", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.sequences")
		require.NotNil(t, result)

		seqNames := make([]string, 0)
		for _, row := range result.Rows {
			seqNames = append(seqNames, row["sequence_name"].(string))
		}

		assert.Contains(t, seqNames, "users_id_seq")
		assert.Contains(t, seqNames, "posts_id_seq")
		assert.Contains(t, seqNames, "comments_id_seq")
	})

	t.Run("sequence data type is bigint", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.sequences WHERE sequence_name = 'users_id_seq'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "bigint", row["data_type"])
		assert.Equal(t, "1", row["start_value"])
		assert.Equal(t, "1", row["increment"])
	})
}

// =============================================================================
// View Metadata Tests
// =============================================================================

// TestViewMetadata tests information_schema.views for view information.
func TestViewMetadata(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("query all views", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.views")
		require.NotNil(t, result)

		viewNames := make([]string, 0)
		for _, row := range result.Rows {
			viewNames = append(viewNames, row["table_name"].(string))
		}

		assert.Contains(t, viewNames, "active_users")
		assert.Contains(t, viewNames, "published_posts")
	})

	t.Run("view definition is present", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.views WHERE table_name = 'active_users'",
		)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		definition := row["view_definition"].(string)
		assert.NotEmpty(t, definition)
		assert.Contains(t, definition, "SELECT")
		assert.Contains(t, definition, "FROM users")
	})
}

// =============================================================================
// Error Case Tests
// =============================================================================

// TestErrorCases tests error handling for invalid queries.
func TestErrorCases(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("unsupported information_schema view returns nil", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.routines")
		assert.Nil(t, result)
	})

	t.Run("unsupported pg_catalog view returns nil", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_am")
		assert.Nil(t, result)
	})

	t.Run("filter returns empty for non-existent table", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.tables WHERE table_name = 'nonexistent_table'",
		)
		require.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("filter returns empty for non-existent schema", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_schema = 'nonexistent_schema'",
		)
		require.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("empty catalog returns empty results", func(t *testing.T) {
		emptyMock := newMockCatalog()
		emptyIS := NewInformationSchema(emptyMock, "dukdb")

		result := emptyIS.Query(
			"SELECT * FROM information_schema.tables WHERE table_schema = 'main'",
		)
		require.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})
}

// =============================================================================
// Mixed Query Tests
// =============================================================================

// TestMixedQueries tests queries that combine catalog data with filters.
func TestMixedQueries(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("filter tables by schema and type", func(t *testing.T) {
		// Query all base tables in main schema
		result := is.Query(
			"SELECT * FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'",
		)
		require.NotNil(t, result)

		// Should have all 6 base tables
		assert.Len(t, result.Rows, 6)
	})

	t.Run("filter views by schema and type", func(t *testing.T) {
		result := is.Query(
			"SELECT * FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'VIEW'",
		)
		require.NotNil(t, result)

		// Should have 2 views
		assert.Len(t, result.Rows, 2)
	})

	t.Run("filter columns by nullable", func(t *testing.T) {
		// Get all nullable columns from users table
		result := is.Query(
			"SELECT * FROM information_schema.columns WHERE table_name = 'users' AND is_nullable = 'YES'",
		)
		require.NotNil(t, result)

		// users has first_name, last_name, created_at, updated_at, deleted_at as nullable
		assert.Len(t, result.Rows, 5)
	})

	t.Run("filter pg_settings by category", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_settings WHERE category = 'Preset Options'")
		require.NotNil(t, result)

		// Should have version-related settings
		settingNames := make([]string, 0)
		for _, row := range result.Rows {
			settingNames = append(settingNames, row["name"].(string))
		}
		assert.Contains(t, settingNames, "server_version")
	})
}

// =============================================================================
// Query Detection Tests
// =============================================================================

// TestQueryDetection tests the query detection functions.
func TestQueryDetection(t *testing.T) {
	t.Run("detect information_schema with JOIN", func(t *testing.T) {
		query := `SELECT tc.constraint_name, kcu.column_name, tc.constraint_type
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name`
		assert.True(t, IsInformationSchemaQuery(query))
	})

	t.Run("detect pg_catalog with subquery", func(t *testing.T) {
		query := `SELECT c.relname
                  FROM pg_catalog.pg_class c
                  WHERE c.oid IN (SELECT indexrelid FROM pg_catalog.pg_index)`
		assert.True(t, IsPgCatalogQuery(query))
	})

	t.Run("detect information_schema in lowercase", func(t *testing.T) {
		query := "select * from information_schema.tables"
		assert.True(t, IsInformationSchemaQuery(query))
		assert.Equal(t, "tables", GetViewName(query))
	})

	t.Run("detect pg_catalog in lowercase", func(t *testing.T) {
		query := "select * from pg_catalog.pg_class"
		assert.True(t, IsPgCatalogQuery(query))
		assert.Equal(t, "pg_class", GetPgCatalogViewName(query))
	})
}

// =============================================================================
// WHERE Clause Parsing Tests
// =============================================================================

// TestWhereClauseParsing tests the WHERE clause parsing functionality.
func TestWhereClauseParsing(t *testing.T) {
	t.Run("parse multiple AND conditions", func(t *testing.T) {
		query := "SELECT * FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'users' AND is_nullable = 'YES'"
		filters := parseWhereClause(query)

		assert.Len(t, filters, 3)

		filterMap := make(map[string]string)
		for _, f := range filters {
			filterMap[f.Column] = f.Value
		}

		assert.Equal(t, "main", filterMap["table_schema"])
		assert.Equal(t, "users", filterMap["table_name"])
		assert.Equal(t, pgNullableYes, filterMap["is_nullable"])
	})

	t.Run("parse with ORDER BY", func(t *testing.T) {
		query := "SELECT * FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position"
		filters := parseWhereClause(query)

		assert.Len(t, filters, 1)
		assert.Equal(t, "table_name", filters[0].Column)
		assert.Equal(t, "users", filters[0].Value)
	})

	t.Run("parse with LIMIT", func(t *testing.T) {
		query := "SELECT * FROM information_schema.tables WHERE table_schema = 'main' LIMIT 10"
		filters := parseWhereClause(query)

		assert.Len(t, filters, 1)
		assert.Equal(t, "table_schema", filters[0].Column)
		assert.Equal(t, "main", filters[0].Value)
	})

	t.Run("parse case insensitive AND", func(t *testing.T) {
		query := "SELECT * FROM information_schema.columns WHERE table_schema = 'main' and table_name = 'users'"
		filters := parseWhereClause(query)

		assert.Len(t, filters, 2)
	})
}

// =============================================================================
// Schemata Tests
// =============================================================================

// TestSchemataView tests information_schema.schemata queries.
func TestSchemataView(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")

	t.Run("list all schemas", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.schemata")
		require.NotNil(t, result)

		schemaNames := make([]string, 0)
		for _, row := range result.Rows {
			schemaNames = append(schemaNames, row["schema_name"].(string))
		}

		// Should have main, information_schema, and pg_catalog
		assert.Contains(t, schemaNames, "main")
		assert.Contains(t, schemaNames, "information_schema")
		assert.Contains(t, schemaNames, "pg_catalog")
	})

	t.Run("filter by schema_name", func(t *testing.T) {
		result := is.Query("SELECT * FROM information_schema.schemata WHERE schema_name = 'main'")
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "dukdb", row["catalog_name"])
		assert.Equal(t, "main", row["schema_name"])
		assert.Equal(t, "dukdb", row["schema_owner"])
	})
}

// =============================================================================
// Combined information_schema and pg_catalog Tests
// =============================================================================

// TestCombinedCatalogViews tests that information_schema and pg_catalog
// views return consistent data.
func TestCombinedCatalogViews(t *testing.T) {
	mock := createORMTestCatalog()
	is := NewInformationSchema(mock, "dukdb")
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("table count matches between views", func(t *testing.T) {
		// Get tables from information_schema
		isResult := is.Query(
			"SELECT * FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'",
		)
		require.NotNil(t, isResult)

		// Get tables from pg_catalog
		pgResult := pg.Query("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
		require.NotNil(t, pgResult)

		// Should have same number of tables
		assert.Equal(t, len(isResult.Rows), len(pgResult.Rows))
	})

	t.Run("view count matches between views", func(t *testing.T) {
		// Get views from information_schema
		isResult := is.Query("SELECT * FROM information_schema.views WHERE table_schema = 'main'")
		require.NotNil(t, isResult)

		// Get views from pg_catalog
		pgResult := pg.Query("SELECT * FROM pg_catalog.pg_views WHERE schemaname = 'public'")
		require.NotNil(t, pgResult)

		// Should have same number of views
		assert.Equal(t, len(isResult.Rows), len(pgResult.Rows))
	})

	t.Run("constraint count matches between views", func(t *testing.T) {
		// Get constraints from information_schema
		isResult := is.Query(
			"SELECT * FROM information_schema.table_constraints WHERE table_schema = 'main'",
		)
		require.NotNil(t, isResult)

		// Get constraints from pg_catalog
		pgResult := pg.Query("SELECT * FROM pg_catalog.pg_constraint")
		require.NotNil(t, pgResult)

		// Should have same number of constraints
		assert.Equal(t, len(isResult.Rows), len(pgResult.Rows))
	})
}

// =============================================================================
// pg_type Category Tests
// =============================================================================

// TestPgTypeCategories tests pg_type categories for different data types.
func TestPgTypeCategories(t *testing.T) {
	mock := createORMTestCatalog()
	pg := NewPgCatalog(mock, "dukdb")

	t.Run("numeric types have category N", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'N'")
		require.NotNil(t, result)

		typeNames := make([]string, 0)
		for _, row := range result.Rows {
			typeNames = append(typeNames, row["typname"].(string))
		}

		assert.Contains(t, typeNames, "int4")
		assert.Contains(t, typeNames, "int8")
		assert.Contains(t, typeNames, "float4")
		assert.Contains(t, typeNames, "float8")
	})

	t.Run("string types have category S", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'S'")
		require.NotNil(t, result)

		typeNames := make([]string, 0)
		for _, row := range result.Rows {
			typeNames = append(typeNames, row["typname"].(string))
		}

		assert.Contains(t, typeNames, "varchar")
		assert.Contains(t, typeNames, "text")
	})

	t.Run("boolean types have category B", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'B'")
		require.NotNil(t, result)

		assert.GreaterOrEqual(t, len(result.Rows), 1)

		typeNames := make([]string, 0)
		for _, row := range result.Rows {
			typeNames = append(typeNames, row["typname"].(string))
		}

		assert.Contains(t, typeNames, "bool")
	})

	t.Run("array types have category A", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'A'")
		require.NotNil(t, result)

		// Array type names should start with underscore
		for _, row := range result.Rows {
			typname := row["typname"].(string)
			assert.True(t, strings.HasPrefix(typname, "_"),
				"array type %s should start with underscore", typname)
		}
	})

	t.Run("datetime types have category D", func(t *testing.T) {
		result := pg.Query("SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'D'")
		require.NotNil(t, result)

		typeNames := make([]string, 0)
		for _, row := range result.Rows {
			typeNames = append(typeNames, row["typname"].(string))
		}

		assert.Contains(t, typeNames, "date")
		assert.Contains(t, typeNames, "timestamp")
		assert.Contains(t, typeNames, "timestamptz")
	})
}
