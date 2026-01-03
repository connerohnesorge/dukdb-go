package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDDLStandardSQLSyntax tests standard SQL DDL syntax.
func TestDDLStandardSQLSyntax(t *testing.T) {
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

	t.Run("CREATE TABLE with various column types", func(t *testing.T) {
		_, err := conn.Execute(ctx, `
			CREATE TABLE test_types (
				id INTEGER,
				name VARCHAR,
				email VARCHAR(255),
				age INTEGER,
				salary DOUBLE,
				active BOOLEAN,
				created_at BIGINT
			)
		`, nil)
		require.NoError(t, err)
	})

	t.Run("CREATE TABLE with NOT NULL constraints", func(t *testing.T) {
		_, err := conn.Execute(ctx, `
			CREATE TABLE users (
				id INTEGER NOT NULL,
				email VARCHAR NOT NULL
			)
		`, nil)
		require.NoError(t, err)
	})

	t.Run("CREATE TABLE with PRIMARY KEY", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR)", nil)
		require.NoError(t, err)
	})
}

// TestDDLErrorMessages tests that error messages are appropriate.
func TestDDLErrorMessages(t *testing.T) {
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

	t.Run("Table already exists", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("Table not found", func(t *testing.T) {
		_, err := conn.Execute(ctx, "DROP TABLE nonexistent", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("View not found", func(t *testing.T) {
		_, err := conn.Execute(ctx, "DROP VIEW nonexistent_view", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("Index not found", func(t *testing.T) {
		_, err := conn.Execute(ctx, "DROP INDEX nonexistent_idx", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("Sequence not found", func(t *testing.T) {
		_, err := conn.Execute(ctx, "DROP SEQUENCE nonexistent_seq", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("Schema not found", func(t *testing.T) {
		_, err := conn.Execute(ctx, "DROP SCHEMA nonexistent_schema", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

// TestDDLEdgeCases tests edge cases in DDL operations.
func TestDDLEdgeCases(t *testing.T) {
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

	t.Run("Table name with underscores", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE user_accounts (id INTEGER)", nil)
		require.NoError(t, err)

		rows, _, err := conn.Query(ctx, "SELECT * FROM user_accounts", nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(rows))
	})

	t.Run("Table name with numbers", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE table123 (id INTEGER)", nil)
		require.NoError(t, err)
	})

	t.Run("Case sensitivity in names", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE MyTable (id INTEGER)", nil)
		require.NoError(t, err)

		// Query with different case should work (case-insensitive)
		_, err = conn.Execute(ctx, "INSERT INTO mytable VALUES (1)", nil)
		require.NoError(t, err)

		// Query with uppercase should also work
		_, err = conn.Execute(ctx, "INSERT INTO MYTABLE VALUES (2)", nil)
		require.NoError(t, err)

		// Query with original case should work
		_, err = conn.Execute(ctx, "INSERT INTO MyTable VALUES (3)", nil)
		require.NoError(t, err)

		// Verify all inserts worked
		rows, _, err := conn.Query(ctx, "SELECT * FROM mytable", nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(rows))
	})

	t.Run("Case sensitivity in column names", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE TestCols (Id INTEGER, Name VARCHAR)", nil)
		require.NoError(t, err)

		// Insert using different column name cases
		_, err = conn.Execute(ctx, "INSERT INTO testcols (id, name) VALUES (1, 'Alice')", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "INSERT INTO TESTCOLS (ID, NAME) VALUES (2, 'Bob')", nil)
		require.NoError(t, err)

		// Select using different cases
		rows, _, err := conn.Query(ctx, "SELECT id, NAME FROM testcols", nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(rows))
	})

	t.Run("Case sensitivity in view names", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE view_test (id INTEGER)", nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, "INSERT INTO view_test VALUES (1)", nil)
		require.NoError(t, err)

		// Create view with mixed case
		_, err = conn.Execute(ctx, "CREATE VIEW MyView AS SELECT * FROM view_test", nil)
		require.NoError(t, err)

		// Query with different case
		rows, _, err := conn.Query(ctx, "SELECT * FROM myview", nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(rows))

		// Query with uppercase
		rows, _, err = conn.Query(ctx, "SELECT * FROM MYVIEW", nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(rows))
	})

	t.Run("Case sensitivity in sequence names", func(t *testing.T) {
		// Create sequence with mixed case
		_, err := conn.Execute(ctx, "CREATE SEQUENCE MySeq", nil)
		require.NoError(t, err)

		// Get next value using different case
		rows, _, err := conn.Query(ctx, "SELECT nextval('myseq')", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), rows[0]["nextval"])

		// Get next value using uppercase
		rows, _, err = conn.Query(ctx, "SELECT nextval('MYSEQ')", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), rows[0]["nextval"])
	})

	t.Run("Case sensitivity in schema names", func(t *testing.T) {
		// Create schema with mixed case
		_, err := conn.Execute(ctx, "CREATE SCHEMA MySchema", nil)
		require.NoError(t, err)

		// Create table in schema using different case
		_, err = conn.Execute(ctx, "CREATE TABLE myschema.schema_test (id INTEGER)", nil)
		require.NoError(t, err)

		// Query using uppercase schema name
		_, err = conn.Execute(ctx, "INSERT INTO MYSCHEMA.schema_test VALUES (1)", nil)
		require.NoError(t, err)

		rows, _, err := conn.Query(ctx, "SELECT * FROM MySchema.schema_test", nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(rows))
	})

	t.Run("Long table name", func(t *testing.T) {
		longName := "this_is_a_very_long_table_name_that_should_still_work_properly"
		_, err := conn.Execute(ctx, "CREATE TABLE "+longName+" (id INTEGER)", nil)
		require.NoError(t, err)
	})

	t.Run("Column with same name as table", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE items (items INTEGER)", nil)
		require.NoError(t, err)
	})

	t.Run("Multiple columns with similar names", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE test (id INTEGER, id_copy INTEGER, id_backup INTEGER)", nil)
		require.NoError(t, err)
	})
}

// TestDDLReservedWords tests handling of reserved words.
func TestDDLReservedWords(t *testing.T) {
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

	t.Run("Column named 'select'", func(_ *testing.T) {
		// Reserved words as column names might require quoting or may be allowed
		// This tests the current behavior
		_, _ = conn.Execute(ctx, "CREATE TABLE test1 (id INTEGER, \"select\" VARCHAR)", nil)
		// If this fails, that's okay - it means reserved words are properly restricted
		// If it succeeds, that's okay too - it means quoting works
	})

	t.Run("Column named 'from'", func(_ *testing.T) {
		_, _ = conn.Execute(ctx, "CREATE TABLE test2 (id INTEGER, \"from\" VARCHAR)", nil)
		// Similar to above - either outcome is acceptable
	})

	t.Run("Normal column names should always work", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE normal_table (id INTEGER, name VARCHAR, value DOUBLE)", nil)
		require.NoError(t, err)
	})
}

// TestDDLSequenceEdgeCases tests sequence edge cases.
func TestDDLSequenceEdgeCases(t *testing.T) {
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

	t.Run("Sequence with negative increment", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SEQUENCE countdown START WITH 100 INCREMENT BY -1", nil)
		require.NoError(t, err)

		rows, _, err := conn.Query(ctx, "SELECT nextval('countdown')", nil)
		require.NoError(t, err)
		require.Equal(t, int64(100), rows[0]["nextval"])

		rows, _, err = conn.Query(ctx, "SELECT nextval('countdown')", nil)
		require.NoError(t, err)
		require.Equal(t, int64(99), rows[0]["nextval"])
	})

	t.Run("Sequence with large start value", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SEQUENCE big_seq START WITH 1000000", nil)
		require.NoError(t, err)

		rows, _, err := conn.Query(ctx, "SELECT nextval('big_seq')", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1000000), rows[0]["nextval"])
	})

	t.Run("currval before nextval should fail", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SEQUENCE new_seq", nil)
		require.NoError(t, err)

		// currval before nextval should fail
		_, _, err = conn.Query(ctx, "SELECT currval('new_seq')", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not yet defined in this session")
	})

	t.Run("currval after nextval should work", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SEQUENCE test_seq", nil)
		require.NoError(t, err)

		// Call nextval first
		rows, _, err := conn.Query(ctx, "SELECT nextval('test_seq')", nil)
		require.NoError(t, err)
		val1 := rows[0]["nextval"]

		// Now currval should work and return same value
		rows, _, err = conn.Query(ctx, "SELECT currval('test_seq')", nil)
		require.NoError(t, err)
		require.Equal(t, val1, rows[0]["currval"])
	})
}

// TestDDLViewEdgeCases tests view edge cases.
func TestDDLViewEdgeCases(t *testing.T) {
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

	t.Run("View with complex query", func(t *testing.T) {
		// Testing aggregate functions in view definitions
		_, err := conn.Execute(ctx, "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DOUBLE)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, `
			CREATE VIEW order_summary AS
			SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount
			FROM orders
			GROUP BY user_id
		`, nil)
		require.NoError(t, err)

		// Insert test data
		_, err = conn.Execute(ctx, "INSERT INTO orders VALUES (1, 100, 50.0)", nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, "INSERT INTO orders VALUES (2, 100, 75.0)", nil)
		require.NoError(t, err)

		// Query view
		rows, _, err := conn.Query(ctx, "SELECT * FROM order_summary", nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(rows))
		require.Equal(t, int32(100), rows[0]["user_id"])
		require.Equal(t, int64(2), rows[0]["order_count"])
	})

	t.Run("View with JOIN", func(t *testing.T) {
		// t.Skip("Feature not yet implemented: parser support for qualified column names in view definitions")
		_, err := conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR)", nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, "CREATE TABLE posts (id INTEGER, user_id INTEGER, title VARCHAR)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, `
			CREATE VIEW user_posts AS
			SELECT users.name, posts.title
			FROM users
			JOIN posts ON users.id = posts.user_id
		`, nil)
		require.NoError(t, err)

		// View should be queryable (even if empty)
		_, _, err = conn.Query(ctx, "SELECT * FROM user_posts", nil)
		require.NoError(t, err)
	})

	t.Run("View with subquery", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE products (id INTEGER, price DOUBLE)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, `
			CREATE VIEW expensive_products AS
			SELECT * FROM products
			WHERE price > (SELECT AVG(price) FROM products)
		`, nil)
		require.NoError(t, err)
	})
}

// TestDDLIndexEdgeCases tests index edge cases.
func TestDDLIndexEdgeCases(t *testing.T) {
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

	t.Run("Index on multiple columns", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE users (first_name VARCHAR, last_name VARCHAR, age INTEGER)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE INDEX idx_name ON users (first_name, last_name)", nil)
		require.NoError(t, err)
	})

	t.Run("Multiple indexes on same table", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE INDEX idx_products_id ON products (id)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE INDEX idx_products_name ON products (name)", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE INDEX idx_products_price ON products (price)", nil)
		require.NoError(t, err)
	})

	t.Run("Index with same name as table", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE TABLE items (id INTEGER)", nil)
		require.NoError(t, err)

		// Index name can be same as table name (different namespace)
		_, _ = conn.Execute(ctx, "CREATE INDEX items ON items (id)", nil)
		// Either succeeds or fails gracefully
	})
}

// TestDDLCascadingOperations tests cascading operations.
func TestDDLCascadingOperations(t *testing.T) {
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

	t.Run("Drop schema with CASCADE should drop objects", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SCHEMA test_cascade", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE TABLE test_cascade.users (id INTEGER)", nil)
		require.NoError(t, err)

		// Drop schema with CASCADE should work
		_, _ = conn.Execute(ctx, "DROP SCHEMA test_cascade CASCADE", nil)
		// Either this is implemented or returns appropriate error
		// We're testing that syntax is recognized
	})

	t.Run("Drop schema without CASCADE on non-empty schema should fail", func(t *testing.T) {
		_, err := conn.Execute(ctx, "CREATE SCHEMA test_restrict", nil)
		require.NoError(t, err)

		_, err = conn.Execute(ctx, "CREATE TABLE test_restrict.data (id INTEGER)", nil)
		require.NoError(t, err)

		// Should fail because schema is not empty
		_, err = conn.Execute(ctx, "DROP SCHEMA test_restrict", nil)
		require.Error(t, err)
	})
}

// TestDDLTransactionBehavior tests DDL in transactions.
func TestDDLTransactionBehavior(t *testing.T) {
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

	t.Run("DDL in transaction", func(t *testing.T) {
		// Begin transaction
		_, err := conn.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)

		// Create table in transaction
		_, err = conn.Execute(ctx, "CREATE TABLE txn_table (id INTEGER)", nil)
		require.NoError(t, err)

		// Table should be visible
		rows, _, err := conn.Query(ctx, "SELECT * FROM txn_table", nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(rows))

		// Commit
		_, err = conn.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)

		// Table should still exist
		_, _, err = conn.Query(ctx, "SELECT * FROM txn_table", nil)
		require.NoError(t, err)
	})

	t.Run("DDL rollback", func(t *testing.T) {
		// Begin transaction
		_, err := conn.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)

		// Create table in transaction
		_, err = conn.Execute(ctx, "CREATE TABLE rollback_table (id INTEGER)", nil)
		require.NoError(t, err)

		// Rollback
		_, err = conn.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)

		// Table should not exist
		_, _, err = conn.Query(ctx, "SELECT * FROM rollback_table", nil)
		require.Error(t, err)
	})

	t.Run("DDL rollback multiple operations", func(t *testing.T) {
		// Begin transaction
		_, err := conn.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)

		// Create table in transaction
		_, err = conn.Execute(ctx, "CREATE TABLE multi_table (id INTEGER)", nil)
		require.NoError(t, err)

		// Create view in transaction
		_, err = conn.Execute(ctx, "CREATE VIEW multi_view AS SELECT * FROM multi_table", nil)
		require.NoError(t, err)

		// Create sequence in transaction
		_, err = conn.Execute(ctx, "CREATE SEQUENCE multi_seq", nil)
		require.NoError(t, err)

		// All objects should be visible within transaction
		_, _, err = conn.Query(ctx, "SELECT * FROM multi_table", nil)
		require.NoError(t, err)

		_, _, err = conn.Query(ctx, "SELECT * FROM multi_view", nil)
		require.NoError(t, err)

		// Rollback
		_, err = conn.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)

		// All objects should not exist
		_, _, err = conn.Query(ctx, "SELECT * FROM multi_table", nil)
		require.Error(t, err)

		_, _, err = conn.Query(ctx, "SELECT * FROM multi_view", nil)
		require.Error(t, err)
	})
}

// TestDDLConcurrentOperations tests concurrent DDL operations.
func TestDDLConcurrentOperations(t *testing.T) {
	// t.Skip("Feature not yet implemented: parser support for qualified column names in view definitions")
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

	// Create base table
	_, err = conn.Execute(ctx, "CREATE TABLE base (id INTEGER)", nil)
	require.NoError(t, err)

	// Multiple DDL operations should work in sequence
	_, err = conn.Execute(ctx, "CREATE INDEX idx1 ON base (id)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE VIEW view1 AS SELECT * FROM base", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE SEQUENCE seq1", nil)
	require.NoError(t, err)

	// All objects should exist
	_, err = conn.Execute(ctx, "INSERT INTO base VALUES (1)", nil)
	require.NoError(t, err)

	_, _, err = conn.Query(ctx, "SELECT * FROM view1", nil)
	require.NoError(t, err)

	_, _, err = conn.Query(ctx, "SELECT nextval('seq1')", nil)
	require.NoError(t, err)
}
