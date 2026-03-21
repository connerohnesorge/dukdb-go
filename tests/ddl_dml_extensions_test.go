package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// COMMENT ON tests
// ---------------------------------------------------------------------------

// TestCommentOnTable sets a comment on a table and verifies no error.
func TestCommentOnTable(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE comment_tbl (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("COMMENT ON TABLE comment_tbl IS 'This is my table'")
	require.NoError(t, err)
}

// TestCommentOnColumn sets a comment on a column using table.column syntax.
func TestCommentOnColumn(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE comment_col_tbl (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("COMMENT ON COLUMN comment_col_tbl.name IS 'The user name'")
	require.NoError(t, err)
}

// TestCommentOnTableDropWithNull sets a comment then drops it with IS NULL.
func TestCommentOnTableDropWithNull(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE comment_drop_tbl (id INTEGER)")
	require.NoError(t, err)

	// Set a comment
	_, err = db.Exec("COMMENT ON TABLE comment_drop_tbl IS 'temporary comment'")
	require.NoError(t, err)

	// Drop the comment with IS NULL
	_, err = db.Exec("COMMENT ON TABLE comment_drop_tbl IS NULL")
	require.NoError(t, err)
}

// TestCommentOnNonexistentTable verifies that commenting on a nonexistent table
// returns an error.
func TestCommentOnNonexistentTable(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("COMMENT ON TABLE no_such_table IS 'oops'")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestCommentOnView sets a comment on a view.
func TestCommentOnView(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE comment_view_base (id INTEGER, val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE VIEW comment_view AS SELECT id, val FROM comment_view_base")
	require.NoError(t, err)

	_, err = db.Exec("COMMENT ON VIEW comment_view IS 'a useful view'")
	require.NoError(t, err)
}

// TestCommentOnIndex sets a comment on an index.
func TestCommentOnIndex(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE comment_idx_tbl (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE INDEX comment_idx ON comment_idx_tbl (id)")
	require.NoError(t, err)

	_, err = db.Exec("COMMENT ON INDEX comment_idx IS 'index on id column'")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// ALTER TABLE ALTER COLUMN TYPE tests
// ---------------------------------------------------------------------------

// TestAlterColumnTypeVarcharToInteger creates a table with a VARCHAR column
// containing numeric strings, alters the type to INTEGER, and verifies conversion.
func TestAlterColumnTypeVarcharToInteger(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE alter_v2i (id INTEGER, val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO alter_v2i VALUES (1, '100'), (2, '200'), (3, '300')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE alter_v2i ALTER COLUMN val TYPE INTEGER")
	require.NoError(t, err)

	rows, err := db.Query("SELECT id, val FROM alter_v2i ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	expected := []int64{100, 200, 300}
	idx := 0
	for rows.Next() {
		var id, val int64
		err := rows.Scan(&id, &val)
		require.NoError(t, err)
		assert.Equal(t, int64(idx+1), id)
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx, "expected 3 rows")
}

// TestAlterColumnTypeSetDataTypeSyntax tests the SET DATA TYPE alternative syntax.
func TestAlterColumnTypeSetDataTypeSyntax(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE alter_sdt_ext (val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO alter_sdt_ext VALUES ('42')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE alter_sdt_ext ALTER COLUMN val SET DATA TYPE INTEGER")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT val FROM alter_sdt_ext").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

// TestAlterColumnTypeInvalidConversion tests that altering a VARCHAR column with
// non-numeric data to INTEGER produces an error.
func TestAlterColumnTypeInvalidConversion(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE alter_bad (val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO alter_bad VALUES ('not_a_number')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE alter_bad ALTER COLUMN val TYPE INTEGER")
	require.Error(t, err, "converting non-numeric varchar to integer should fail")
}

// TestAlterColumnTypeNonexistentColumnExt tests that altering a column that does
// not exist returns an error.
func TestAlterColumnTypeNonexistentColumnExt(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE alter_nocol_ext (id INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE alter_nocol_ext ALTER COLUMN ghost TYPE BIGINT")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DELETE USING tests
// ---------------------------------------------------------------------------

// TestDeleteUsingSingleTable creates orders and customers tables, and uses
// DELETE ... USING to delete orders where a matching customer exists in the
// USING table. Verifies that all matching rows from the cross-join are deleted.
func TestDeleteUsingSingleTable(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE del_customers (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE del_orders (id INTEGER, customer_id INTEGER, amount INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO del_customers VALUES (2, 'Bob')`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO del_orders VALUES
		(10, 1, 100),
		(20, 2, 200),
		(30, 2, 150),
		(40, 3, 300)`)
	require.NoError(t, err)

	// Delete orders belonging to customer id=2 (the only customer in del_customers)
	result, err := db.Exec(`DELETE FROM del_orders
		USING del_customers
		WHERE del_orders.customer_id = del_customers.id`)
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	// Orders 20 and 30 both have customer_id=2, which matches del_customers
	assert.Equal(t, int64(2), affected, "should delete orders matching the USING table join")

	// Verify remaining rows
	rows, err := db.Query("SELECT id FROM del_orders ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{10, 40}, ids, "orders for non-matching customers should remain")
}

// TestDeleteUsingMultipleTables tests DELETE FROM t1 USING t2, t3 WHERE conditions
// with multiple USING tables joined together.
func TestDeleteUsingMultipleTables(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE del_m1 (id INTEGER, val VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE del_m2 (ref_id INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE del_m3 (ref_id INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO del_m1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')`)
	require.NoError(t, err)
	// del_m2 has refs for ids 1 and 3
	_, err = db.Exec(`INSERT INTO del_m2 VALUES (1), (3)`)
	require.NoError(t, err)
	// del_m3 has refs for ids 1 and 2
	_, err = db.Exec(`INSERT INTO del_m3 VALUES (1), (2)`)
	require.NoError(t, err)

	// Delete rows from del_m1 that exist in BOTH del_m2 AND del_m3 (only id=1)
	_, err = db.Exec(`DELETE FROM del_m1
		USING del_m2, del_m3
		WHERE del_m1.id = del_m2.ref_id
		AND del_m1.id = del_m3.ref_id`)
	require.NoError(t, err)

	rows, err := db.Query("SELECT id FROM del_m1 ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	// Only id=1 is in both del_m2 and del_m3, so it should be deleted
	assert.Equal(t, []int64{2, 3, 4}, ids, "only rows matching both USING tables should be deleted")
}

// TestDeleteUsingWithReturning tests DELETE ... USING ... RETURNING and verifies
// that the deleted rows are returned by the query.
func TestDeleteUsingWithReturning(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE del_ret_main (id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE del_ret_ref (ref_id INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO del_ret_main VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')`)
	require.NoError(t, err)
	// Only reference ids 2 and 3
	_, err = db.Exec(`INSERT INTO del_ret_ref VALUES (2), (3)`)
	require.NoError(t, err)

	// Delete with RETURNING
	rows, err := db.Query(`DELETE FROM del_ret_main
		USING del_ret_ref
		WHERE del_ret_main.id = del_ret_ref.ref_id
		RETURNING del_ret_main.id, del_ret_main.name`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type deletedRow struct {
		id   int64
		name string
	}
	var deleted []deletedRow
	for rows.Next() {
		var r deletedRow
		require.NoError(t, rows.Scan(&r.id, &r.name))
		deleted = append(deleted, r)
	}
	require.NoError(t, rows.Err())

	// Rows 2 and 3 should have been deleted and returned
	require.Len(t, deleted, 2)
	assert.Equal(t, int64(2), deleted[0].id)
	assert.Equal(t, "beta", deleted[0].name)
	assert.Equal(t, int64(3), deleted[1].id)
	assert.Equal(t, "gamma", deleted[1].name)

	// Verify only row 1 remains
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM del_ret_main").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestDeleteUsingNonexistentTable tests that DELETE ... USING a nonexistent
// table returns an error.
func TestDeleteUsingNonexistentTable(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE del_exist (id INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec(`DELETE FROM del_exist
		USING nonexistent_table
		WHERE del_exist.id = nonexistent_table.id`)
	require.Error(t, err)
}
