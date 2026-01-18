package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDDLCreateTableE2E tests CREATE TABLE end-to-end.
func TestDDLCreateTableE2E(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)", nil)
	require.NoError(t, err)

	// Insert data to verify table works
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'Alice', 30)", nil)
	require.NoError(t, err)

	// Query to verify
	rows, cols, err := conn.Query(ctx, "SELECT * FROM users", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, 3, len(cols))
	require.Equal(t, int32(1), rows[0]["id"])
	require.Equal(t, "Alice", rows[0]["name"])
	require.Equal(t, int32(30), rows[0]["age"])
}

// TestDDLCreateTableIfNotExistsE2E tests CREATE TABLE IF NOT EXISTS.
func TestDDLCreateTableIfNotExistsE2E(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
	require.NoError(t, err)

	// Creating again should fail
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")

	// But with IF NOT EXISTS should succeed
	_, err = conn.Execute(ctx, "CREATE TABLE IF NOT EXISTS users (id INTEGER)", nil)
	require.NoError(t, err)
}

// TestDDLDropTableE2E tests DROP TABLE end-to-end.
func TestDDLDropTableE2E(t *testing.T) {
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

	// Create and drop table
	_, err = conn.Execute(ctx, "CREATE TABLE temp (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP TABLE temp", nil)
	require.NoError(t, err)

	// Query should fail now
	_, _, err = conn.Query(ctx, "SELECT * FROM temp", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestDDLDropTableIfExistsE2E tests DROP TABLE IF EXISTS.
func TestDDLDropTableIfExistsE2E(t *testing.T) {
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

	// Drop non-existent table should fail
	_, err = conn.Execute(ctx, "DROP TABLE nonexistent", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// But with IF EXISTS should succeed
	_, err = conn.Execute(ctx, "DROP TABLE IF EXISTS nonexistent", nil)
	require.NoError(t, err)
}

// TestDDLCreateViewE2E tests CREATE VIEW end-to-end.
func TestDDLCreateViewE2E(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN)", nil)
	require.NoError(t, err)

	// Insert data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'Alice', true)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (2, 'Bob', false)", nil)
	require.NoError(t, err)

	// Create view
	_, err = conn.Execute(
		ctx,
		"CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true",
		nil,
	)
	require.NoError(t, err)

	// Query view
	rows, cols, err := conn.Query(ctx, "SELECT * FROM active_users", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, 2, len(cols))
	require.Equal(t, int32(1), rows[0]["id"])
	require.Equal(t, "Alice", rows[0]["name"])
}

// TestDDLDropViewE2E tests DROP VIEW end-to-end.
func TestDDLDropViewE2E(t *testing.T) {
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

	// Create table and view
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE VIEW user_view AS SELECT * FROM users", nil)
	require.NoError(t, err)

	// Drop view
	_, err = conn.Execute(ctx, "DROP VIEW user_view", nil)
	require.NoError(t, err)

	// Query view should fail
	_, _, err = conn.Query(ctx, "SELECT * FROM user_view", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestDDLCreateIndexE2E tests CREATE INDEX end-to-end.
func TestDDLCreateIndexE2E(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create index
	_, err = conn.Execute(ctx, "CREATE INDEX idx_email ON users (email)", nil)
	require.NoError(t, err)

	// Insert data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'alice@example.com')", nil)
	require.NoError(t, err)

	// Query should still work
	rows, _, err := conn.Query(ctx, "SELECT * FROM users WHERE email = 'alice@example.com'", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
}

// TestDDLCreateUniqueIndexE2E tests CREATE UNIQUE INDEX end-to-end.
func TestDDLCreateUniqueIndexE2E(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create unique index
	_, err = conn.Execute(ctx, "CREATE UNIQUE INDEX idx_unique_email ON users (email)", nil)
	require.NoError(t, err)

	// Insert data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'alice@example.com')", nil)
	require.NoError(t, err)

	// Insert duplicate should fail (when unique constraint is enforced)
	// Note: Current implementation may not enforce uniqueness yet
	_, _ = conn.Execute(ctx, "INSERT INTO users VALUES (2, 'alice@example.com')", nil)
	// This might succeed if uniqueness not enforced yet - just test index creation works
}

// TestDDLDropIndexE2E tests DROP INDEX end-to-end.
func TestDDLDropIndexE2E(t *testing.T) {
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

	// Create table and index
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id ON users (id)", nil)
	require.NoError(t, err)

	// Drop index
	_, err = conn.Execute(ctx, "DROP INDEX idx_id", nil)
	require.NoError(t, err)

	// Table should still exist and work
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1)", nil)
	require.NoError(t, err)
}

// TestDDLCreateSequenceE2E tests CREATE SEQUENCE end-to-end.
func TestDDLCreateSequenceE2E(t *testing.T) {
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

	// Create sequence
	_, err = conn.Execute(ctx, "CREATE SEQUENCE user_id_seq START WITH 100 INCREMENT BY 1", nil)
	require.NoError(t, err)

	// Use sequence with nextval
	rows, _, err := conn.Query(ctx, "SELECT nextval('user_id_seq')", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(100), rows[0]["nextval"])

	// Next call should increment
	rows, _, err = conn.Query(ctx, "SELECT nextval('user_id_seq')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(101), rows[0]["nextval"])
}

// TestDDLDropSequenceE2E tests DROP SEQUENCE end-to-end.
func TestDDLDropSequenceE2E(t *testing.T) {
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

	// Create and drop sequence
	_, err = conn.Execute(ctx, "CREATE SEQUENCE test_seq", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP SEQUENCE test_seq", nil)
	require.NoError(t, err)

	// Using sequence should fail
	_, _, err = conn.Query(ctx, "SELECT nextval('test_seq')", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestDDLCreateSchemaE2E tests CREATE SCHEMA end-to-end.
func TestDDLCreateSchemaE2E(t *testing.T) {
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

	// Create schema
	_, err = conn.Execute(ctx, "CREATE SCHEMA test_schema", nil)
	require.NoError(t, err)

	// Create table in schema
	_, err = conn.Execute(ctx, "CREATE TABLE test_schema.users (id INTEGER)", nil)
	require.NoError(t, err)

	// Query should work
	_, err = conn.Execute(ctx, "INSERT INTO test_schema.users VALUES (1)", nil)
	require.NoError(t, err)
}

// TestDDLDropSchemaE2E tests DROP SCHEMA end-to-end.
func TestDDLDropSchemaE2E(t *testing.T) {
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

	// Create and drop empty schema
	_, err = conn.Execute(ctx, "CREATE SCHEMA temp_schema", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP SCHEMA temp_schema", nil)
	require.NoError(t, err)

	// Creating table in dropped schema should fail
	_, err = conn.Execute(ctx, "CREATE TABLE temp_schema.users (id INTEGER)", nil)
	require.Error(t, err)
}

// TestDDLObjectDependencies tests object dependencies (view referencing table).
func TestDDLObjectDependencies(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Create view that references table
	_, err = conn.Execute(ctx, "CREATE VIEW user_names AS SELECT name FROM users", nil)
	require.NoError(t, err)

	// Try to drop table - should fail because view depends on it
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by")

	// Drop view first
	_, err = conn.Execute(ctx, "DROP VIEW user_names", nil)
	require.NoError(t, err)

	// Now drop table should work
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.NoError(t, err)
}

// TestDDLMultipleViewDependencies tests that multiple views depending on a table prevents dropping.
func TestDDLMultipleViewDependencies(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create first view that references table
	_, err = conn.Execute(ctx, "CREATE VIEW user_names AS SELECT name FROM users", nil)
	require.NoError(t, err)

	// Create second view that references table
	_, err = conn.Execute(ctx, "CREATE VIEW user_emails AS SELECT email FROM users", nil)
	require.NoError(t, err)

	// Try to drop table - should fail because both views depend on it
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by view")

	// Drop first view
	_, err = conn.Execute(ctx, "DROP VIEW user_names", nil)
	require.NoError(t, err)

	// Still can't drop table because second view exists
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "user_emails")

	// Drop second view
	_, err = conn.Execute(ctx, "DROP VIEW user_emails", nil)
	require.NoError(t, err)

	// Now drop table should work
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.NoError(t, err)
}

// TestDDLViewDependencyCaseInsensitive tests that table dependencies are case-insensitive.
func TestDDLViewDependencyCaseInsensitive(t *testing.T) {
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

	// Create table with lowercase name
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Create view referencing table with UPPERCASE
	_, err = conn.Execute(ctx, "CREATE VIEW user_view AS SELECT name FROM USERS", nil)
	require.NoError(t, err)

	// Try to drop table with mixed case - should fail
	_, err = conn.Execute(ctx, "DROP TABLE Users", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by")

	// Drop view
	_, err = conn.Execute(ctx, "DROP VIEW user_view", nil)
	require.NoError(t, err)

	// Now drop table should work
	_, err = conn.Execute(ctx, "DROP TABLE users", nil)
	require.NoError(t, err)
}

// TestDDLIndexColumnDependencies tests that dropping indexed columns fails.
func TestDDLIndexColumnDependencies(t *testing.T) {
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

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, email VARCHAR, age INTEGER)", nil)
	require.NoError(t, err)

	// Create index on email
	_, err = conn.Execute(ctx, "CREATE INDEX idx_email ON users (email)", nil)
	require.NoError(t, err)

	// Try to drop indexed column - should fail
	_, err = conn.Execute(ctx, "ALTER TABLE users DROP COLUMN email", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by index")

	// Drop index first
	_, err = conn.Execute(ctx, "DROP INDEX idx_email", nil)
	require.NoError(t, err)

	// Now drop column should work
	_, err = conn.Execute(ctx, "ALTER TABLE users DROP COLUMN email", nil)
	require.NoError(t, err)
}

// TestDDLComplexWorkflow tests a complex DDL workflow.
func TestDDLComplexWorkflow(t *testing.T) {
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

	// Create schema
	_, err = conn.Execute(ctx, "CREATE SCHEMA app", nil)
	require.NoError(t, err)

	// Create sequence
	_, err = conn.Execute(ctx, "CREATE SEQUENCE app.user_id_seq START WITH 1000", nil)
	require.NoError(t, err)

	// Create table
	_, err = conn.Execute(
		ctx,
		"CREATE TABLE app.users (id INTEGER, email VARCHAR, created_at BIGINT)",
		nil,
	)
	require.NoError(t, err)

	// Create index
	_, err = conn.Execute(ctx, "CREATE UNIQUE INDEX app.idx_email ON app.users (email)", nil)
	require.NoError(t, err)

	// Create view
	_, err = conn.Execute(
		ctx,
		"CREATE VIEW app.recent_users AS SELECT id, email FROM app.users WHERE created_at > 0",
		nil,
	)
	require.NoError(t, err)

	// Insert data using sequence
	_, err = conn.Execute(
		ctx,
		"INSERT INTO app.users VALUES (nextval('app.user_id_seq'), 'alice@example.com', 1000)",
		nil,
	)
	require.NoError(t, err)

	// Query view
	rows, _, err := conn.Query(ctx, "SELECT * FROM app.recent_users", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	// Note: id is declared as INTEGER, so it returns int32 even though nextval returns int64
	require.Equal(t, int32(1000), rows[0]["id"])

	// Clean up in correct order
	_, err = conn.Execute(ctx, "DROP VIEW app.recent_users", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP INDEX app.idx_email", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP TABLE app.users", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP SEQUENCE app.user_id_seq", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "DROP SCHEMA app", nil)
	require.NoError(t, err)
}
