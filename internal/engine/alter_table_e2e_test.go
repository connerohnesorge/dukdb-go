package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAlterTableRenameToE2E tests ALTER TABLE RENAME TO end-to-end.
func TestAlterTableRenameToE2E(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(ctx, "CREATE TABLE old_table (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO old_table VALUES (1, 'Alice')", nil)
	require.NoError(t, err)

	// Rename the table
	_, err = conn.Execute(ctx, "ALTER TABLE old_table RENAME TO new_table", nil)
	require.NoError(t, err)

	// Query with old name should fail
	_, _, err = conn.Query(ctx, "SELECT * FROM old_table", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// Query with new name should succeed
	rows, cols, err := conn.Query(ctx, "SELECT * FROM new_table", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(cols))
	require.Equal(t, 1, len(rows))
	require.Equal(t, int32(1), rows[0]["id"])
	require.Equal(t, "Alice", rows[0]["name"])
}

// TestAlterTableRenameColumnE2E tests ALTER TABLE RENAME COLUMN end-to-end.
func TestAlterTableRenameColumnE2E(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(
		ctx,
		"CREATE TABLE users (id INTEGER, old_name VARCHAR, age INTEGER)",
		nil,
	)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'Alice', 30)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (2, 'Bob', 25)", nil)
	require.NoError(t, err)

	// Rename the column
	_, err = conn.Execute(ctx, "ALTER TABLE users RENAME COLUMN old_name TO new_name", nil)
	require.NoError(t, err)

	// Query with old column name should fail
	_, _, err = conn.Query(ctx, "SELECT old_name FROM users", nil)
	require.Error(t, err)

	// Query with new column name should succeed
	rows, cols, err := conn.Query(ctx, "SELECT id, new_name, age FROM users ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(cols))
	require.Equal(t, 2, len(rows))
	require.Equal(t, int32(1), rows[0]["id"])
	require.Equal(t, "Alice", rows[0]["new_name"])
	require.Equal(t, int32(2), rows[1]["id"])
	require.Equal(t, "Bob", rows[1]["new_name"])

	// SELECT * should work with renamed column
	rows, cols, err = conn.Query(ctx, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(cols))
	require.Contains(t, cols, "new_name")
	require.NotContains(t, cols, "old_name")
}

// TestAlterTableDropColumnE2E tests ALTER TABLE DROP COLUMN end-to-end.
func TestAlterTableDropColumnE2E(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(
		ctx,
		"CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE, category VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO products VALUES (1, 'Apple', 1.5, 'Fruit')", nil)
	require.NoError(t, err)

	// Drop a column
	_, err = conn.Execute(ctx, "ALTER TABLE products DROP COLUMN category", nil)
	require.NoError(t, err)

	// Query with dropped column should fail
	_, _, err = conn.Query(ctx, "SELECT category FROM products", nil)
	require.Error(t, err)

	// Query without dropped column should succeed
	rows, cols, err := conn.Query(ctx, "SELECT * FROM products", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(cols))
	require.Contains(t, cols, "id")
	require.Contains(t, cols, "name")
	require.Contains(t, cols, "price")
	require.NotContains(t, cols, "category")
	require.Equal(t, 1, len(rows))
}

// TestAlterTableAddColumnE2E tests ALTER TABLE ADD COLUMN end-to-end.
func TestAlterTableAddColumnE2E(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(ctx, "CREATE TABLE employees (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO employees VALUES (1, 'Alice')", nil)
	require.NoError(t, err)

	// Add a column
	_, err = conn.Execute(ctx, "ALTER TABLE employees ADD COLUMN salary DOUBLE", nil)
	require.NoError(t, err)

	// Query with new column should succeed
	rows, cols, err := conn.Query(ctx, "SELECT * FROM employees", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(cols))
	require.Contains(t, cols, "id")
	require.Contains(t, cols, "name")
	require.Contains(t, cols, "salary")
	require.Equal(t, 1, len(rows))
	// New column should have NULL for existing rows
	require.Nil(t, rows[0]["salary"])

	// Insert new data with the new column
	_, err = conn.Execute(ctx, "INSERT INTO employees VALUES (2, 'Bob', 50000.0)", nil)
	require.NoError(t, err)

	rows, _, err = conn.Query(ctx, "SELECT * FROM employees WHERE id = 2", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, float64(50000.0), rows[0]["salary"])
}

// TestAlterTableErrorCases tests error cases for ALTER TABLE.
func TestAlterTableErrorCases(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(ctx, "CREATE TABLE test_table (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	t.Run("RenameToExistingTable", func(t *testing.T) {
		// Create another table
		_, err = conn.Execute(ctx, "CREATE TABLE other_table (x INTEGER)", nil)
		require.NoError(t, err)

		// Try to rename to existing table name
		_, err = conn.Execute(ctx, "ALTER TABLE test_table RENAME TO other_table", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("RenameNonexistentTable", func(t *testing.T) {
		// Try to rename non-existent table
		_, err = conn.Execute(ctx, "ALTER TABLE nonexistent RENAME TO something", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("RenameToSameName", func(t *testing.T) {
		// Renaming to the same name should fail (table already exists)
		_, err = conn.Execute(ctx, "ALTER TABLE test_table RENAME TO test_table", nil)
		require.Error(t, err)
	})

	t.Run("RenameColumnToExisting", func(t *testing.T) {
		// Try to rename column to an existing column name
		_, err = conn.Execute(ctx, "ALTER TABLE test_table RENAME COLUMN id TO name", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("RenameNonexistentColumn", func(t *testing.T) {
		// Try to rename non-existent column
		_, err = conn.Execute(
			ctx,
			"ALTER TABLE test_table RENAME COLUMN nonexistent TO something",
			nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("DropNonexistentColumn", func(t *testing.T) {
		// Try to drop non-existent column
		_, err = conn.Execute(ctx, "ALTER TABLE test_table DROP COLUMN nonexistent", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("DropLastColumn", func(t *testing.T) {
		// Create a table with one column
		_, err = conn.Execute(ctx, "CREATE TABLE single_col (id INTEGER)", nil)
		require.NoError(t, err)

		// Try to drop the last column
		_, err = conn.Execute(ctx, "ALTER TABLE single_col DROP COLUMN id", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "last column")
	})

	t.Run("AddExistingColumn", func(t *testing.T) {
		// Try to add a column that already exists
		_, err = conn.Execute(ctx, "ALTER TABLE test_table ADD COLUMN id INTEGER", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})
}

// TestAlterTableWithIndexes tests that indexes are properly updated when columns are renamed.
func TestAlterTableWithIndexes(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, email VARCHAR, age INTEGER)", nil)
	require.NoError(t, err)

	// Create an index on email
	_, err = conn.Execute(ctx, "CREATE INDEX idx_email ON users (email)", nil)
	require.NoError(t, err)

	// Rename the email column
	_, err = conn.Execute(ctx, "ALTER TABLE users RENAME COLUMN email TO email_address", nil)
	require.NoError(t, err)

	// The index should still exist and reference the new column name
	// (We can't easily verify index internals in this test, but we can verify
	// that basic operations still work)

	// Try to drop the renamed column - should fail because it's indexed
	_, err = conn.Execute(ctx, "ALTER TABLE users DROP COLUMN email_address", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by index")
}

// TestAlterTableMultipleOperations tests multiple ALTER TABLE operations in sequence.
func TestAlterTableMultipleOperations(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a table
	_, err = conn.Execute(ctx, "CREATE TABLE data (a INTEGER, b VARCHAR, c DOUBLE)", nil)
	require.NoError(t, err)

	// Insert data
	_, err = conn.Execute(ctx, "INSERT INTO data VALUES (1, 'test', 3.14)", nil)
	require.NoError(t, err)

	// Add a column
	_, err = conn.Execute(ctx, "ALTER TABLE data ADD COLUMN d INTEGER", nil)
	require.NoError(t, err)

	// Rename a column
	_, err = conn.Execute(ctx, "ALTER TABLE data RENAME COLUMN b TO b_renamed", nil)
	require.NoError(t, err)

	// Drop a column
	_, err = conn.Execute(ctx, "ALTER TABLE data DROP COLUMN c", nil)
	require.NoError(t, err)

	// Verify final state
	rows, cols, err := conn.Query(ctx, "SELECT * FROM data", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(cols))
	require.Contains(t, cols, "a")
	require.Contains(t, cols, "b_renamed")
	require.Contains(t, cols, "d")
	require.NotContains(t, cols, "b")
	require.NotContains(t, cols, "c")
	require.Equal(t, 1, len(rows))
	require.Equal(t, int32(1), rows[0]["a"])
	require.Equal(t, "test", rows[0]["b_renamed"])

	// Rename the table
	_, err = conn.Execute(ctx, "ALTER TABLE data RENAME TO final_data", nil)
	require.NoError(t, err)

	// Verify with new table name
	rows, _, err = conn.Query(ctx, "SELECT * FROM final_data", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
}

// TestAlterTableIfExists tests ALTER TABLE with IF EXISTS.
func TestAlterTableIfExists(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Try to alter non-existent table with IF EXISTS - should succeed silently
	_, err = conn.Execute(ctx, "ALTER TABLE IF EXISTS nonexistent RENAME TO something", nil)
	require.NoError(t, err)

	// Create a table and verify IF EXISTS works
	_, err = conn.Execute(ctx, "CREATE TABLE test (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "ALTER TABLE IF EXISTS test RENAME TO test_renamed", nil)
	require.NoError(t, err)

	// Verify the rename worked
	_, _, err = conn.Query(ctx, "SELECT * FROM test_renamed", nil)
	require.NoError(t, err)
}
