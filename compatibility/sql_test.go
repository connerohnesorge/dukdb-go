package compatibility

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SQLCompatibilityTests covers basic SQL operations for DDL, DML, and queries.
var SQLCompatibilityTests = []CompatibilityTest{
	// DDL Tests
	{Name: "CreateTable", Category: "sql", Test: testCreateTable},
	{Name: "CreateTableIfNotExists", Category: "sql", Test: testCreateTableIfNotExists},
	{Name: "DropTable", Category: "sql", Test: testDropTable},
	{Name: "DropTableIfExists", Category: "sql", Test: testDropTableIfExists},

	// DML Tests
	{Name: "InsertValues", Category: "sql", Test: testInsertValues},

	// Query Tests (Basic)
	{Name: "SelectStar", Category: "sql", Test: testSelectStar},
	{Name: "SelectColumns", Category: "sql", Test: testSelectColumns},
	{Name: "SelectWhere", Category: "sql", Test: testSelectWhere},
	{Name: "SelectLimit", Category: "sql", Test: testSelectLimit},
	{Name: "SelectLimitOffset", Category: "sql", Test: testSelectLimitOffset},
	{Name: "SelectDistinct", Category: "sql", Test: testSelectDistinct},

	// Query Tests (Advanced)
	{Name: "SelectJoinInner", Category: "sql", Test: testSelectJoinInner},
	{Name: "SelectJoinLeft", Category: "sql", Test: testSelectJoinLeft},
}

// DDL Tests

func testCreateTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_create (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	// Verify table exists by inserting and querying
	_, err = db.Exec(`INSERT INTO test_create VALUES (1, 'test')`)
	require.NoError(t, err)

	var id int
	var name string
	err = db.QueryRow(`SELECT id, name FROM test_create`).Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "test", name)
}

func testCreateTableIfNotExists(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_if_not_exists (id INTEGER)`)
	require.NoError(t, err)

	// Should not error when table already exists
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_if_not_exists (id INTEGER)`)
	require.NoError(t, err)
}

func testDropTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_drop (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP TABLE test_drop`)
	require.NoError(t, err)

	// Verify table no longer exists
	_, err = db.Exec(`SELECT * FROM test_drop`)
	require.Error(t, err)
}

func testDropTableIfExists(t *testing.T, db *sql.DB) {
	// Should not error when table doesn't exist
	_, err := db.Exec(`DROP TABLE IF EXISTS nonexistent_table`)
	require.NoError(t, err)

	// Create and drop
	_, err = db.Exec(`CREATE TABLE test_drop_if (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP TABLE IF EXISTS test_drop_if`)
	require.NoError(t, err)
}

// DML Tests

func testInsertValues(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_insert (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	result, err := db.Exec(`INSERT INTO test_insert VALUES (1, 'Alice')`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

// Query Tests (Basic)

func testSelectStar(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_select_star (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_select_star VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM test_select_star ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id   int
		name string
	}
	for rows.Next() {
		var r struct {
			id   int
			name string
		}
		err := rows.Scan(&r.id, &r.name)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, 1, results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.Equal(t, 2, results[1].id)
	assert.Equal(t, "Bob", results[1].name)
}

func testSelectColumns(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_select_cols (id INTEGER, name VARCHAR, extra INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_select_cols VALUES (1, 'Alice', 100)`)
	require.NoError(t, err)

	var id int
	var name string
	err = db.QueryRow(`SELECT id, name FROM test_select_cols`).Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "Alice", name)
}

func testSelectWhere(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_select_where (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_select_where VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	var name string
	err = db.QueryRow(`SELECT name FROM test_select_where WHERE id = 2`).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Bob", name)
}

func testSelectLimit(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_limit (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_limit VALUES (1), (2), (3), (4), (5)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id FROM test_limit ORDER BY id LIMIT 3`)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	require.Len(t, ids, 3)
	assert.Equal(t, []int{1, 2, 3}, ids)
}

func testSelectLimitOffset(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_limit_offset (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_limit_offset VALUES (1), (2), (3), (4), (5)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id FROM test_limit_offset ORDER BY id LIMIT 2 OFFSET 2`)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	require.Len(t, ids, 2)
	assert.Equal(t, []int{3, 4}, ids)
}

func testSelectDistinct(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_distinct (category VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_distinct VALUES ('A'), ('B'), ('A'), ('C'), ('B')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT DISTINCT category FROM test_distinct ORDER BY category`)
	require.NoError(t, err)
	defer rows.Close()

	var categories []string
	for rows.Next() {
		var cat string
		require.NoError(t, rows.Scan(&cat))
		categories = append(categories, cat)
	}
	require.NoError(t, rows.Err())

	require.Len(t, categories, 3)
	assert.Equal(t, []string{"A", "B", "C"}, categories)
}

// Query Tests (Advanced)

func testSelectJoinInner(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE users (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE user_orders (user_id INTEGER, product VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO user_orders VALUES (1, 'Apple'), (1, 'Banana'), (2, 'Cherry')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT u.name, o.product FROM users u INNER JOIN user_orders o ON u.id = o.user_id ORDER BY u.name, o.product`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		name    string
		product string
	}
	for rows.Next() {
		var r struct {
			name    string
			product string
		}
		require.NoError(t, rows.Scan(&r.name, &r.product))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, "Alice", results[0].name)
	assert.Equal(t, "Apple", results[0].product)
	assert.Equal(t, "Alice", results[1].name)
	assert.Equal(t, "Banana", results[1].product)
	assert.Equal(t, "Bob", results[2].name)
	assert.Equal(t, "Cherry", results[2].product)
}

func testSelectJoinLeft(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE left_users (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE left_orders (user_id INTEGER, product VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO left_users VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO left_orders VALUES (1, 'Apple')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT u.name, o.product FROM left_users u LEFT JOIN left_orders o ON u.id = o.user_id ORDER BY u.name`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		name    string
		product sql.NullString
	}
	for rows.Next() {
		var r struct {
			name    string
			product sql.NullString
		}
		require.NoError(t, rows.Scan(&r.name, &r.product))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "Alice", results[0].name)
	assert.True(t, results[0].product.Valid)
	assert.Equal(t, "Apple", results[0].product.String)
	assert.Equal(t, "Bob", results[1].name)
	assert.False(t, results[1].product.Valid)
}
