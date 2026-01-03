package binder

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBindCreateView tests binding of CREATE VIEW statements.
func TestBindCreateView(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test table for the view to reference
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("valid view", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE VIEW my_view AS SELECT id, name FROM test_table")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		viewStmt, ok := bound.(*BoundCreateViewStmt)
		require.True(t, ok)
		assert.Equal(t, "main", viewStmt.Schema)
		assert.Equal(t, "my_view", viewStmt.View)
		assert.False(t, viewStmt.IfNotExists)
		assert.NotNil(t, viewStmt.Query)
	})

	t.Run("view with IF NOT EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE VIEW IF NOT EXISTS my_view2 AS SELECT * FROM test_table")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		viewStmt, ok := bound.(*BoundCreateViewStmt)
		require.True(t, ok)
		assert.True(t, viewStmt.IfNotExists)
	})

	t.Run("duplicate view without IF NOT EXISTS", func(t *testing.T) {
		// First create the view
		view := catalog.NewViewDef("existing_view", "main", "SELECT * FROM test_table")
		err := cat.CreateView(view)
		require.NoError(t, err)

		// Try to create it again
		stmt, err := parser.Parse("CREATE VIEW existing_view AS SELECT * FROM test_table")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "view already exists")
	})

	t.Run("duplicate view with IF NOT EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE VIEW IF NOT EXISTS existing_view AS SELECT * FROM test_table")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		viewStmt, ok := bound.(*BoundCreateViewStmt)
		require.True(t, ok)
		assert.True(t, viewStmt.IfNotExists)
	})
}

// TestBindDropView tests binding of DROP VIEW statements.
func TestBindDropView(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test view
	view := catalog.NewViewDef("test_view", "main", "SELECT 1")
	err := cat.CreateView(view)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("drop existing view", func(t *testing.T) {
		stmt, err := parser.Parse("DROP VIEW test_view")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropViewStmt)
		require.True(t, ok)
		assert.Equal(t, "main", dropStmt.Schema)
		assert.Equal(t, "test_view", dropStmt.View)
		assert.False(t, dropStmt.IfExists)
	})

	t.Run("drop non-existent view without IF EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("DROP VIEW nonexistent_view")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "view not found")
	})

	t.Run("drop non-existent view with IF EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("DROP VIEW IF EXISTS nonexistent_view")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropViewStmt)
		require.True(t, ok)
		assert.True(t, dropStmt.IfExists)
	})
}

// TestBindCreateIndex tests binding of CREATE INDEX statements.
func TestBindCreateIndex(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test table
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("email", dukdb.TYPE_VARCHAR),
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("valid index", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE INDEX idx_name ON test_table(name)")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		indexStmt, ok := bound.(*BoundCreateIndexStmt)
		require.True(t, ok)
		assert.Equal(t, "main", indexStmt.Schema)
		assert.Equal(t, "test_table", indexStmt.Table)
		assert.Equal(t, "idx_name", indexStmt.Index)
		assert.Equal(t, []string{"name"}, indexStmt.Columns)
		assert.False(t, indexStmt.IsUnique)
	})

	t.Run("unique index", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE UNIQUE INDEX idx_email ON test_table(email)")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		indexStmt, ok := bound.(*BoundCreateIndexStmt)
		require.True(t, ok)
		assert.True(t, indexStmt.IsUnique)
	})

	t.Run("multi-column index", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE INDEX idx_name_email ON test_table(name, email)")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		indexStmt, ok := bound.(*BoundCreateIndexStmt)
		require.True(t, ok)
		assert.Equal(t, []string{"name", "email"}, indexStmt.Columns)
	})

	t.Run("index on non-existent table", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE INDEX idx ON nonexistent(id)")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table not found")
	})

	t.Run("index on non-existent column", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE INDEX idx ON test_table(nonexistent)")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column not found")
	})

	t.Run("duplicate index without IF NOT EXISTS", func(t *testing.T) {
		// First create the index
		idx := catalog.NewIndexDef("existing_idx", "main", "test_table", []string{"id"}, false)
		err := cat.CreateIndex(idx)
		require.NoError(t, err)

		// Try to create it again
		stmt, err := parser.Parse("CREATE INDEX existing_idx ON test_table(id)")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index already exists")
	})
}

// TestBindDropIndex tests binding of DROP INDEX statements.
func TestBindDropIndex(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test table
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create a test index
	idx := catalog.NewIndexDef("test_idx", "main", "test_table", []string{"id"}, false)
	err = cat.CreateIndex(idx)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("drop existing index", func(t *testing.T) {
		stmt, err := parser.Parse("DROP INDEX test_idx")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropIndexStmt)
		require.True(t, ok)
		assert.Equal(t, "main", dropStmt.Schema)
		assert.Equal(t, "test_idx", dropStmt.Index)
	})

	t.Run("drop non-existent index with IF EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("DROP INDEX IF EXISTS nonexistent_idx")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropIndexStmt)
		require.True(t, ok)
		assert.True(t, dropStmt.IfExists)
	})
}

// TestBindCreateSequence tests binding of CREATE SEQUENCE statements.
func TestBindCreateSequence(t *testing.T) {
	cat := catalog.NewCatalog()
	binder := NewBinder(cat)

	t.Run("simple sequence", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE SEQUENCE my_seq")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		seqStmt, ok := bound.(*BoundCreateSequenceStmt)
		require.True(t, ok)
		assert.Equal(t, "main", seqStmt.Schema)
		assert.Equal(t, "my_seq", seqStmt.Sequence)
	})

	t.Run("sequence with options", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 5")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		seqStmt, ok := bound.(*BoundCreateSequenceStmt)
		require.True(t, ok)
		assert.Equal(t, int64(100), seqStmt.StartWith)
		assert.Equal(t, int64(5), seqStmt.IncrementBy)
	})

	t.Run("duplicate sequence without IF NOT EXISTS", func(t *testing.T) {
		// First create the sequence
		seq := catalog.NewSequenceDef("existing_seq", "main")
		err := cat.CreateSequence(seq)
		require.NoError(t, err)

		// Try to create it again
		stmt, err := parser.Parse("CREATE SEQUENCE existing_seq")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sequence already exists")
	})
}

// TestBindDropSequence tests binding of DROP SEQUENCE statements.
func TestBindDropSequence(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test sequence
	seq := catalog.NewSequenceDef("test_seq", "main")
	err := cat.CreateSequence(seq)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("drop existing sequence", func(t *testing.T) {
		stmt, err := parser.Parse("DROP SEQUENCE test_seq")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropSequenceStmt)
		require.True(t, ok)
		assert.Equal(t, "main", dropStmt.Schema)
		assert.Equal(t, "test_seq", dropStmt.Sequence)
	})

	t.Run("drop non-existent sequence with IF EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("DROP SEQUENCE IF EXISTS nonexistent_seq")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropSequenceStmt)
		require.True(t, ok)
		assert.True(t, dropStmt.IfExists)
	})
}

// TestBindCreateSchema tests binding of CREATE SCHEMA statements.
func TestBindCreateSchema(t *testing.T) {
	cat := catalog.NewCatalog()
	binder := NewBinder(cat)

	t.Run("create new schema", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE SCHEMA my_schema")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		schemaStmt, ok := bound.(*BoundCreateSchemaStmt)
		require.True(t, ok)
		assert.Equal(t, "my_schema", schemaStmt.Schema)
		assert.False(t, schemaStmt.IfNotExists)
	})

	t.Run("duplicate schema without IF NOT EXISTS", func(t *testing.T) {
		// main schema already exists
		stmt, err := parser.Parse("CREATE SCHEMA main")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema already exists")
	})

	t.Run("duplicate schema with IF NOT EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("CREATE SCHEMA IF NOT EXISTS main")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		schemaStmt, ok := bound.(*BoundCreateSchemaStmt)
		require.True(t, ok)
		assert.True(t, schemaStmt.IfNotExists)
	})
}

// TestBindDropSchema tests binding of DROP SCHEMA statements.
func TestBindDropSchema(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test schema
	_, err := cat.CreateSchema("test_schema")
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("cannot drop main schema", func(t *testing.T) {
		stmt, err := parser.Parse("DROP SCHEMA main")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot drop main schema")
	})

	t.Run("drop empty schema", func(t *testing.T) {
		stmt, err := parser.Parse("DROP SCHEMA test_schema")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropSchemaStmt)
		require.True(t, ok)
		assert.Equal(t, "test_schema", dropStmt.Schema)
		assert.False(t, dropStmt.Cascade)
	})

	t.Run("drop schema with objects without CASCADE", func(t *testing.T) {
		// Create schema with a table
		_, err := cat.CreateSchema("schema_with_table")
		require.NoError(t, err)

		tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		})
		err = cat.CreateTableInSchema("schema_with_table", tableDef)
		require.NoError(t, err)

		stmt, err := parser.Parse("DROP SCHEMA schema_with_table")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contains objects")
	})

	t.Run("drop schema with CASCADE", func(t *testing.T) {
		stmt, err := parser.Parse("DROP SCHEMA schema_with_table CASCADE")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		dropStmt, ok := bound.(*BoundDropSchemaStmt)
		require.True(t, ok)
		assert.True(t, dropStmt.Cascade)
	})
}

// TestBindAlterTable tests binding of ALTER TABLE statements.
func TestBindAlterTable(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a test table
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("rename table", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE test_table RENAME TO new_table")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		alterStmt, ok := bound.(*BoundAlterTableStmt)
		require.True(t, ok)
		assert.Equal(t, parser.AlterTableRenameTo, alterStmt.Operation)
		assert.Equal(t, "new_table", alterStmt.NewTableName)
	})

	t.Run("rename column", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE test_table RENAME COLUMN name TO full_name")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		alterStmt, ok := bound.(*BoundAlterTableStmt)
		require.True(t, ok)
		assert.Equal(t, parser.AlterTableRenameColumn, alterStmt.Operation)
		assert.Equal(t, "name", alterStmt.OldColumn)
		assert.Equal(t, "full_name", alterStmt.NewColumn)
	})

	t.Run("drop column", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE test_table DROP COLUMN name")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		alterStmt, ok := bound.(*BoundAlterTableStmt)
		require.True(t, ok)
		assert.Equal(t, parser.AlterTableDropColumn, alterStmt.Operation)
		assert.Equal(t, "name", alterStmt.DropColumn)
	})

	t.Run("add column", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE test_table ADD COLUMN email VARCHAR")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		alterStmt, ok := bound.(*BoundAlterTableStmt)
		require.True(t, ok)
		assert.Equal(t, parser.AlterTableAddColumn, alterStmt.Operation)
		assert.NotNil(t, alterStmt.AddColumn)
		assert.Equal(t, "email", alterStmt.AddColumn.Name)
		assert.Equal(t, dukdb.TYPE_VARCHAR, alterStmt.AddColumn.Type)
	})

	t.Run("alter non-existent table without IF EXISTS", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE nonexistent RENAME TO new_name")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table not found")
	})

	t.Run("rename non-existent column", func(t *testing.T) {
		stmt, err := parser.Parse("ALTER TABLE test_table RENAME COLUMN nonexistent TO new_name")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column not found")
	})
}

// TestViewResolution tests that views are properly resolved in SELECT statements.
func TestViewResolution(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a base table
	tableDef := catalog.NewTableDef("base_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create a view
	view := catalog.NewViewDef("test_view", "main", "SELECT id, value FROM base_table")
	err = cat.CreateView(view)
	require.NoError(t, err)

	binder := NewBinder(cat)

	t.Run("select from view", func(t *testing.T) {
		stmt, err := parser.Parse("SELECT * FROM test_view")
		require.NoError(t, err)

		bound, err := binder.Bind(stmt)
		require.NoError(t, err)

		selectStmt, ok := bound.(*BoundSelectStmt)
		require.True(t, ok)
		assert.Len(t, selectStmt.From, 1)

		// Check that the view was resolved
		tableRef := selectStmt.From[0]
		assert.Equal(t, "test_view", tableRef.TableName)
		assert.NotNil(t, tableRef.ViewDef)
	})

	t.Run("select specific columns from view", func(t *testing.T) {
		stmt, err := parser.Parse("SELECT id FROM test_view WHERE value = 'test'")
		require.NoError(t, err)

		_, err = binder.Bind(stmt)
		require.NoError(t, err)
	})
}
