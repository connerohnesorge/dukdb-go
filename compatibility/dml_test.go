package compatibility

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DMLCompatibilityTests covers UPDATE and DELETE operations for compatibility testing.
var DMLCompatibilityTests = []CompatibilityTest{
	// UPDATE Tests
	{
		Name:     "UpdateSingleRow",
		Category: "dml",
		Test:     testUpdateSingleRow,
	},
	{
		Name:     "UpdateMultipleRows",
		Category: "dml",
		Test:     testUpdateMultipleRows,
	},
	{
		Name:     "UpdateNoMatching",
		Category: "dml",
		Test:     testUpdateNoMatching,
	},
	// DELETE Tests
	{
		Name:     "DeleteSingleRow",
		Category: "dml",
		Test:     testDeleteSingleRow,
	},
	{
		Name:     "DeleteMultipleRows",
		Category: "dml",
		Test:     testDeleteMultipleRows,
	},
	{
		Name:     "DeleteNoMatching",
		Category: "dml",
		Test:     testDeleteNoMatching,
	},
	{
		Name:     "DeleteAllRows",
		Category: "dml",
		Test:     testDeleteAllRows,
	},
}

// UPDATE Tests

func testUpdateSingleRow(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE employees (id INTEGER, name VARCHAR, salary INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO employees VALUES (1, 'Alice', 50000), (2, 'Bob', 55000), (3, 'Charlie', 60000)`,
	)
	require.NoError(t, err)

	// Execute UPDATE for single row
	result, err := db.Exec(
		`UPDATE employees SET salary = 60000 WHERE id = 1`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 1
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected, "Expected 1 row affected")

	// Verify the data state with SELECT
	var salary int
	err = db.QueryRow(`SELECT salary FROM employees WHERE id = 1`).Scan(&salary)
	require.NoError(t, err)
	assert.Equal(t, 60000, salary, "Salary should be updated to 60000")

	// Verify other rows are unchanged
	var bobSalary int
	err = db.QueryRow(`SELECT salary FROM employees WHERE id = 2`).Scan(&bobSalary)
	require.NoError(t, err)
	assert.Equal(t, 55000, bobSalary, "Bob's salary should be unchanged")
}

func testUpdateMultipleRows(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE employees_multi (id INTEGER, name VARCHAR, salary INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO employees_multi VALUES
		(1, 'Alice', 40000),
		(2, 'Bob', 45000),
		(3, 'Charlie', 60000),
		(4, 'Diana', 35000)`,
	)
	require.NoError(t, err)

	// Execute UPDATE for multiple rows (salary < 50000)
	result, err := db.Exec(
		`UPDATE employees_multi SET salary = 70000 WHERE salary < 50000`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 3 (Alice, Bob, Diana)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), rowsAffected, "Expected 3 rows affected")

	// Verify the data state with SELECT
	rows, err := db.Query(
		`SELECT id, salary FROM employees_multi ORDER BY id`,
	)
	require.NoError(t, err)
	defer rows.Close()

	expectedSalaries := map[int]int{
		1: 70000, // Alice - updated
		2: 70000, // Bob - updated
		3: 60000, // Charlie - unchanged
		4: 70000, // Diana - updated
	}

	for rows.Next() {
		var id, salary int
		err := rows.Scan(&id, &salary)
		require.NoError(t, err)
		assert.Equal(
			t,
			expectedSalaries[id],
			salary,
			"Salary for id=%d should be %d",
			id,
			expectedSalaries[id],
		)
	}
	require.NoError(t, rows.Err())
}

func testUpdateNoMatching(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE employees_no_match (id INTEGER, name VARCHAR, salary INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO employees_no_match VALUES (1, 'Alice', 50000), (2, 'Bob', 55000)`,
	)
	require.NoError(t, err)

	// Execute UPDATE with no matching rows
	result, err := db.Exec(
		`UPDATE employees_no_match SET salary = 80000 WHERE id = 9999`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 0
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected, "Expected 0 rows affected")

	// Verify original data is unchanged
	var salary int
	err = db.QueryRow(`SELECT salary FROM employees_no_match WHERE id = 1`).Scan(&salary)
	require.NoError(t, err)
	assert.Equal(t, 50000, salary, "Salary should be unchanged")
}

// DELETE Tests

func testDeleteSingleRow(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO products VALUES (1, 'Apple', 1.50), (2, 'Banana', 0.75), (3, 'Cherry', 2.00)`,
	)
	require.NoError(t, err)

	// Execute DELETE for single row
	result, err := db.Exec(
		`DELETE FROM products WHERE id = 1`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 1
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected, "Expected 1 row affected")

	// Verify the data state with SELECT - count remaining rows
	rows, err := db.Query(`SELECT id FROM products ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	assert.Len(t, ids, 2, "Should have 2 rows remaining")
	assert.Equal(t, []int{2, 3}, ids, "Only rows with id 2 and 3 should remain")
}

func testDeleteMultipleRows(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE products_multi (id INTEGER, name VARCHAR, price DOUBLE)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO products_multi VALUES
		(1, 'Apple', 5.00),
		(2, 'Banana', 0.75),
		(3, 'Cherry', 8.00),
		(4, 'Date', 3.00)`,
	)
	require.NoError(t, err)

	// Execute DELETE for multiple rows (price < 10)
	result, err := db.Exec(
		`DELETE FROM products_multi WHERE price < 10`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 4 (all items have price < 10)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(4), rowsAffected, "Expected 4 rows affected")

	// Verify all rows are deleted by querying remaining rows
	rows, err := db.Query(`SELECT id FROM products_multi`)
	require.NoError(t, err)
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, rowCount, "Should have 0 rows remaining")
}

func testDeleteNoMatching(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE products_no_match (id INTEGER, name VARCHAR, price DOUBLE)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO products_no_match VALUES (1, 'Apple', 1.50), (2, 'Banana', 0.75)`,
	)
	require.NoError(t, err)

	// Execute DELETE with no matching rows
	result, err := db.Exec(
		`DELETE FROM products_no_match WHERE id = 9999`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 0
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected, "Expected 0 rows affected")

	// Verify original data is unchanged by querying all rows
	rows, err := db.Query(`SELECT id FROM products_no_match ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	assert.Len(t, ids, 2, "Should still have 2 rows")
	assert.Equal(t, []int{1, 2}, ids, "Original rows should be unchanged")
}

func testDeleteAllRows(t *testing.T, db *sql.DB) {
	// Create table with test data
	_, err := db.Exec(
		`CREATE TABLE products_all (id INTEGER, name VARCHAR, price DOUBLE)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO products_all VALUES
		(1, 'Apple', 1.50),
		(2, 'Banana', 0.75),
		(3, 'Cherry', 2.00),
		(4, 'Date', 3.50),
		(5, 'Elderberry', 5.00)`,
	)
	require.NoError(t, err)

	// Execute DELETE for all rows (no WHERE clause)
	result, err := db.Exec(
		`DELETE FROM products_all`,
	)
	require.NoError(t, err)

	// Verify RowsAffected = 5
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(5), rowsAffected, "Expected 5 rows affected")

	// Verify all rows are deleted by querying remaining rows
	rows, err := db.Query(`SELECT id FROM products_all`)
	require.NoError(t, err)
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, rowCount, "Should have 0 rows remaining")

	// Verify table still exists (can insert into it)
	_, err = db.Exec(`INSERT INTO products_all VALUES (1, 'Fig', 4.00)`)
	require.NoError(t, err)

	// Verify the new row exists
	var id int
	var name string
	err = db.QueryRow(`SELECT id, name FROM products_all WHERE id = 1`).Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 1, id, "ID should be 1")
	assert.Equal(t, "Fig", name, "Name should be Fig")
}
