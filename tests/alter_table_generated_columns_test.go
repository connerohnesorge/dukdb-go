package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

// TestAlterTableDropGeneratedColumn verifies that a generated column can be
// dropped with ALTER TABLE DROP COLUMN just like a regular column (task 8.1).
func TestAlterTableDropGeneratedColumn(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE products (
		price DOUBLE,
		quantity INTEGER,
		total DOUBLE GENERATED ALWAYS AS (price * quantity) STORED
	)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO products (price, quantity) VALUES (5.0, 4)")
	require.NoError(t, err)

	// Drop the generated column — should succeed.
	_, err = db.Exec("ALTER TABLE products DROP COLUMN total")
	require.NoError(t, err)

	// Query should now only have price and quantity columns.
	rows, err := db.Query("SELECT * FROM products")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, []string{"price", "quantity"}, cols)
}

// TestAlterTableDropBaseColumnReferencedByGenerated verifies that dropping a
// base column that is referenced by a generated column expression is rejected
// with a descriptive error (task 8.2).
func TestAlterTableDropBaseColumnReferencedByGenerated(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE orders (
		price DOUBLE,
		quantity INTEGER,
		total DOUBLE GENERATED ALWAYS AS (price * quantity) STORED
	)`)
	require.NoError(t, err)

	// Dropping a base column that the generated column depends on should fail.
	_, err = db.Exec("ALTER TABLE orders DROP COLUMN price")
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by generated column")

	_, err = db.Exec("ALTER TABLE orders DROP COLUMN quantity")
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced by generated column")
}

// TestAlterTableAddGeneratedColumnStored verifies that a STORED generated
// column can be added via ALTER TABLE ADD COLUMN (task 8.3).
func TestAlterTableAddGeneratedColumnStored(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE items (price DOUBLE, quantity INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO items (price, quantity) VALUES (3.0, 5)")
	require.NoError(t, err)

	// Add a STORED generated column.
	_, err = db.Exec(
		"ALTER TABLE items ADD COLUMN total DOUBLE GENERATED ALWAYS AS (price * quantity) STORED",
	)
	require.NoError(t, err)

	// Insert a new row — the generated column should be computed automatically.
	_, err = db.Exec("INSERT INTO items (price, quantity) VALUES (2.5, 4)")
	require.NoError(t, err)

	// Verify the generated value for the newly inserted row.
	var total float64
	err = db.QueryRow(
		"SELECT total FROM items WHERE price = 2.5",
	).Scan(&total)
	require.NoError(t, err)
	require.Equal(t, 10.0, total)
}

// TestAlterTableAddGeneratedColumnVirtual verifies that a VIRTUAL generated
// column can be added via ALTER TABLE ADD COLUMN (task 8.3).
func TestAlterTableAddGeneratedColumnVirtual(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE sales (a INTEGER, b INTEGER)`)
	require.NoError(t, err)

	// Add a VIRTUAL generated column.
	_, err = db.Exec(
		"ALTER TABLE sales ADD COLUMN c INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL",
	)
	require.NoError(t, err)

	// Insert a row and verify the generated column value.
	_, err = db.Exec("INSERT INTO sales (a, b) VALUES (10, 20)")
	require.NoError(t, err)

	var c int
	err = db.QueryRow("SELECT c FROM sales").Scan(&c)
	require.NoError(t, err)
	require.Equal(t, 30, c)
}

// TestAlterTableDropGeneratedColumnThenBaseColumnAllowed verifies that after
// dropping the generated column, the base column it referenced can now be
// dropped freely (task 8.2 / task 8.1 interaction).
func TestAlterTableDropGeneratedColumnThenBaseColumnAllowed(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t (
		x INTEGER,
		y INTEGER,
		z INTEGER GENERATED ALWAYS AS (x + y) STORED
	)`)
	require.NoError(t, err)

	// Drop the generated column first.
	_, err = db.Exec("ALTER TABLE t DROP COLUMN z")
	require.NoError(t, err)

	// Now dropping the formerly-referenced base column should succeed.
	_, err = db.Exec("ALTER TABLE t DROP COLUMN x")
	require.NoError(t, err)
}
