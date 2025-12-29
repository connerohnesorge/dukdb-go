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
	{Name: "AlterTableAddColumn", Category: "sql", Test: testAlterTableAddColumn},
	{Name: "AlterTableDropColumn", Category: "sql", Test: testAlterTableDropColumn},

	// DML Tests
	{Name: "InsertValues", Category: "sql", Test: testInsertValues},
	{Name: "InsertMultipleRows", Category: "sql", Test: testInsertMultipleRows},
	{Name: "Update", Category: "sql", Test: testUpdate},
	{Name: "UpdateWithWhere", Category: "sql", Test: testUpdateWithWhere},
	{Name: "Delete", Category: "sql", Test: testDelete},
	{Name: "DeleteWithWhere", Category: "sql", Test: testDeleteWithWhere},

	// Query Tests (Basic)
	{Name: "SelectStar", Category: "sql", Test: testSelectStar},
	{Name: "SelectColumns", Category: "sql", Test: testSelectColumns},
	{Name: "SelectWhere", Category: "sql", Test: testSelectWhere},
	{Name: "SelectOrderBy", Category: "sql", Test: testSelectOrderBy},
	{Name: "SelectOrderByDesc", Category: "sql", Test: testSelectOrderByDesc},
	{Name: "SelectLimit", Category: "sql", Test: testSelectLimit},
	{Name: "SelectLimitOffset", Category: "sql", Test: testSelectLimitOffset},
	{Name: "SelectDistinct", Category: "sql", Test: testSelectDistinct},

	// Query Tests (Advanced)
	{Name: "SelectGroupBy", Category: "sql", Test: testSelectGroupBy},
	{Name: "SelectHaving", Category: "sql", Test: testSelectHaving},
	{Name: "SelectJoinInner", Category: "sql", Test: testSelectJoinInner},
	{Name: "SelectJoinLeft", Category: "sql", Test: testSelectJoinLeft},
	{Name: "SelectSubquery", Category: "sql", Test: testSelectSubquery},
	{Name: "SelectCTE", Category: "sql", Test: testSelectCTE},
	{Name: "SelectUnion", Category: "sql", Test: testSelectUnion},
	{Name: "SelectUnionAll", Category: "sql", Test: testSelectUnionAll},

	// Aggregate Function Tests
	{Name: "AggCount", Category: "sql", Test: testAggCount},
	{Name: "AggCountStar", Category: "sql", Test: testAggCountStar},
	{Name: "AggSum", Category: "sql", Test: testAggSum},
	{Name: "AggAvg", Category: "sql", Test: testAggAvg},
	{Name: "AggMin", Category: "sql", Test: testAggMin},
	{Name: "AggMax", Category: "sql", Test: testAggMax},
	{Name: "AggCountDistinct", Category: "sql", Test: testAggCountDistinct},

	// Window Function Tests
	{Name: "WindowRowNumber", Category: "sql", Test: testWindowRowNumber},
	{Name: "WindowRank", Category: "sql", Test: testWindowRank},
	{Name: "WindowDenseRank", Category: "sql", Test: testWindowDenseRank},
	{Name: "WindowLag", Category: "sql", Test: testWindowLag},
	{Name: "WindowLead", Category: "sql", Test: testWindowLead},
	{Name: "WindowSum", Category: "sql", Test: testWindowSum},
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

func testAlterTableAddColumn(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_alter_add (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE test_alter_add ADD COLUMN name VARCHAR`)
	require.NoError(t, err)

	// Verify new column exists
	_, err = db.Exec(`INSERT INTO test_alter_add (id, name) VALUES (1, 'test')`)
	require.NoError(t, err)

	var name string
	err = db.QueryRow(`SELECT name FROM test_alter_add WHERE id = 1`).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "test", name)
}

func testAlterTableDropColumn(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_alter_drop (id INTEGER, name VARCHAR, extra INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE test_alter_drop DROP COLUMN extra`)
	require.NoError(t, err)

	// Verify column no longer exists
	_, err = db.Exec(`INSERT INTO test_alter_drop (id, name, extra) VALUES (1, 'test', 100)`)
	require.Error(t, err)

	// But id and name should still work
	_, err = db.Exec(`INSERT INTO test_alter_drop (id, name) VALUES (1, 'test')`)
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

func testInsertMultipleRows(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_insert_multi (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	result, err := db.Exec(`INSERT INTO test_insert_multi VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), rowsAffected)

	// Verify all rows
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM test_insert_multi`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func testUpdate(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_update (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_update VALUES (1, 'Alice')`)
	require.NoError(t, err)

	result, err := db.Exec(`UPDATE test_update SET name = 'Updated'`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	var name string
	err = db.QueryRow(`SELECT name FROM test_update`).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Updated", name)
}

func testUpdateWithWhere(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_update_where (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_update_where VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	result, err := db.Exec(`UPDATE test_update_where SET name = 'Updated' WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only id=1 was updated
	var name string
	err = db.QueryRow(`SELECT name FROM test_update_where WHERE id = 2`).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Bob", name)
}

func testDelete(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_delete (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_delete VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	result, err := db.Exec(`DELETE FROM test_delete`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), rowsAffected)

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM test_delete`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func testDeleteWithWhere(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_delete_where (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_delete_where VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	result, err := db.Exec(`DELETE FROM test_delete_where WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only id=1 was deleted
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM test_delete_where`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
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

func testSelectOrderBy(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_order (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_order VALUES (3, 'Charlie'), (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT name FROM test_order ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	require.Len(t, names, 3)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)
}

func testSelectOrderByDesc(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE test_order_desc (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_order_desc VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT name FROM test_order_desc ORDER BY id DESC`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	require.Len(t, names, 3)
	assert.Equal(t, []string{"Charlie", "Bob", "Alice"}, names)
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

func testSelectGroupBy(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE orders (id INTEGER, category VARCHAR, amount DOUBLE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO orders VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT category, SUM(amount) as total FROM orders GROUP BY category ORDER BY category`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		category string
		total    float64
	}
	for rows.Next() {
		var r struct {
			category string
			total    float64
		}
		require.NoError(t, rows.Scan(&r.category, &r.total))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "A", results[0].category)
	assert.Equal(t, 250.0, results[0].total)
	assert.Equal(t, "B", results[1].category)
	assert.Equal(t, 200.0, results[1].total)
}

func testSelectHaving(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE orders_having (category VARCHAR, amount DOUBLE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO orders_having VALUES ('A', 100), ('B', 200), ('A', 150), ('C', 50)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT category, SUM(amount) as total FROM orders_having GROUP BY category HAVING SUM(amount) > 100 ORDER BY category`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		category string
		total    float64
	}
	for rows.Next() {
		var r struct {
			category string
			total    float64
		}
		require.NoError(t, rows.Scan(&r.category, &r.total))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "A", results[0].category)
	assert.Equal(t, 250.0, results[0].total)
	assert.Equal(t, "B", results[1].category)
	assert.Equal(t, 200.0, results[1].total)
}

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

func testSelectSubquery(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE sub_data (id INTEGER, value INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO sub_data VALUES (1, 10), (2, 20), (3, 30)`)
	require.NoError(t, err)

	var avg float64
	err = db.QueryRow(`SELECT AVG(value) FROM sub_data WHERE value > (SELECT AVG(value) FROM sub_data)`).Scan(&avg)
	require.NoError(t, err)
	assert.Equal(t, 25.0, avg)
}

func testSelectCTE(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE cte_data (id INTEGER, parent_id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO cte_data VALUES (1, NULL, 'Root'), (2, 1, 'Child1'), (3, 1, 'Child2')`)
	require.NoError(t, err)

	rows, err := db.Query(`
		WITH roots AS (
			SELECT id, name FROM cte_data WHERE parent_id IS NULL
		)
		SELECT c.name
		FROM cte_data c
		INNER JOIN roots r ON c.parent_id = r.id
		ORDER BY c.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	require.Len(t, names, 2)
	assert.Equal(t, []string{"Child1", "Child2"}, names)
}

func testSelectUnion(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE union_a (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE union_b (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO union_a VALUES (1), (2), (3)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO union_b VALUES (2), (3), (4)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM union_a UNION SELECT val FROM union_b ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var val int
		require.NoError(t, rows.Scan(&val))
		vals = append(vals, val)
	}
	require.NoError(t, rows.Err())

	// UNION removes duplicates
	require.Len(t, vals, 4)
	assert.Equal(t, []int{1, 2, 3, 4}, vals)
}

func testSelectUnionAll(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE union_all_a (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE union_all_b (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO union_all_a VALUES (1), (2)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO union_all_b VALUES (2), (3)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM union_all_a UNION ALL SELECT val FROM union_all_b ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var val int
		require.NoError(t, rows.Scan(&val))
		vals = append(vals, val)
	}
	require.NoError(t, rows.Err())

	// UNION ALL keeps duplicates
	require.Len(t, vals, 4)
	assert.Equal(t, []int{1, 2, 2, 3}, vals)
}

// Aggregate Function Tests

func testAggCount(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_count (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_count VALUES (1), (2), (NULL), (3)`)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(val) FROM agg_count`).Scan(&count)
	require.NoError(t, err)
	// COUNT(val) excludes NULLs
	assert.Equal(t, 3, count)
}

func testAggCountStar(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_count_star (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_count_star VALUES (1), (2), (NULL), (3)`)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM agg_count_star`).Scan(&count)
	require.NoError(t, err)
	// COUNT(*) includes NULLs
	assert.Equal(t, 4, count)
}

func testAggSum(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_sum (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_sum VALUES (10), (20), (30)`)
	require.NoError(t, err)

	var sum int64
	err = db.QueryRow(`SELECT SUM(val) FROM agg_sum`).Scan(&sum)
	require.NoError(t, err)
	assert.Equal(t, int64(60), sum)
}

func testAggAvg(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_avg (val DOUBLE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_avg VALUES (10.0), (20.0), (30.0)`)
	require.NoError(t, err)

	var avg float64
	err = db.QueryRow(`SELECT AVG(val) FROM agg_avg`).Scan(&avg)
	require.NoError(t, err)
	assert.Equal(t, 20.0, avg)
}

func testAggMin(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_min (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_min VALUES (30), (10), (20)`)
	require.NoError(t, err)

	var min int
	err = db.QueryRow(`SELECT MIN(val) FROM agg_min`).Scan(&min)
	require.NoError(t, err)
	assert.Equal(t, 10, min)
}

func testAggMax(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_max (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_max VALUES (10), (30), (20)`)
	require.NoError(t, err)

	var max int
	err = db.QueryRow(`SELECT MAX(val) FROM agg_max`).Scan(&max)
	require.NoError(t, err)
	assert.Equal(t, 30, max)
}

func testAggCountDistinct(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE agg_count_distinct (val VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_count_distinct VALUES ('A'), ('B'), ('A'), ('C'), ('B')`)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(DISTINCT val) FROM agg_count_distinct`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

// Window Function Tests

func testWindowRowNumber(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_row (category VARCHAR, val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_row VALUES ('A', 10), ('A', 20), ('B', 30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT category, val, ROW_NUMBER() OVER (PARTITION BY category ORDER BY val) as rn FROM win_row ORDER BY category, val`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		category string
		val      int
		rn       int
	}
	for rows.Next() {
		var r struct {
			category string
			val      int
			rn       int
		}
		require.NoError(t, rows.Scan(&r.category, &r.val, &r.rn))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, 1, results[0].rn) // First in A
	assert.Equal(t, 2, results[1].rn) // Second in A
	assert.Equal(t, 1, results[2].rn) // First in B
}

func testWindowRank(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_rank (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_rank VALUES (10), (20), (20), (30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val, RANK() OVER (ORDER BY val) as r FROM win_rank ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var ranks []int
	for rows.Next() {
		var val, r int
		require.NoError(t, rows.Scan(&val, &r))
		ranks = append(ranks, r)
	}
	require.NoError(t, rows.Err())

	// RANK skips numbers for ties
	require.Len(t, ranks, 4)
	assert.Equal(t, []int{1, 2, 2, 4}, ranks)
}

func testWindowDenseRank(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_dense_rank (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_dense_rank VALUES (10), (20), (20), (30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val, DENSE_RANK() OVER (ORDER BY val) as dr FROM win_dense_rank ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var ranks []int
	for rows.Next() {
		var val, dr int
		require.NoError(t, rows.Scan(&val, &dr))
		ranks = append(ranks, dr)
	}
	require.NoError(t, rows.Err())

	// DENSE_RANK doesn't skip numbers for ties
	require.Len(t, ranks, 4)
	assert.Equal(t, []int{1, 2, 2, 3}, ranks)
}

func testWindowLag(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_lag (id INTEGER, val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_lag VALUES (1, 10), (2, 20), (3, 30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id, val, LAG(val, 1) OVER (ORDER BY id) as prev_val FROM win_lag ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id      int
		val     int
		prevVal sql.NullInt64
	}
	for rows.Next() {
		var r struct {
			id      int
			val     int
			prevVal sql.NullInt64
		}
		require.NoError(t, rows.Scan(&r.id, &r.val, &r.prevVal))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.False(t, results[0].prevVal.Valid) // No previous for first row
	assert.Equal(t, int64(10), results[1].prevVal.Int64)
	assert.Equal(t, int64(20), results[2].prevVal.Int64)
}

func testWindowLead(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_lead (id INTEGER, val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_lead VALUES (1, 10), (2, 20), (3, 30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id, val, LEAD(val, 1) OVER (ORDER BY id) as next_val FROM win_lead ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id      int
		val     int
		nextVal sql.NullInt64
	}
	for rows.Next() {
		var r struct {
			id      int
			val     int
			nextVal sql.NullInt64
		}
		require.NoError(t, rows.Scan(&r.id, &r.val, &r.nextVal))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(20), results[0].nextVal.Int64)
	assert.Equal(t, int64(30), results[1].nextVal.Int64)
	assert.False(t, results[2].nextVal.Valid) // No next for last row
}

func testWindowSum(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE win_sum (id INTEGER, val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO win_sum VALUES (1, 10), (2, 20), (3, 30)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id, val, SUM(val) OVER (ORDER BY id) as running_total FROM win_sum ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id           int
		val          int
		runningTotal int64
	}
	for rows.Next() {
		var r struct {
			id           int
			val          int
			runningTotal int64
		}
		require.NoError(t, rows.Scan(&r.id, &r.val, &r.runningTotal))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(10), results[0].runningTotal)
	assert.Equal(t, int64(30), results[1].runningTotal)
	assert.Equal(t, int64(60), results[2].runningTotal)
}
