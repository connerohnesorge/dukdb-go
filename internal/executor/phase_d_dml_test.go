package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Phase D: End-to-End DML Tests (Tasks 4.7-4.10)
// ============================================================================

// TestPhaseD_Insert tests all INSERT scenarios (Task 4.7)
func TestPhaseD_Insert(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)",
	)
	require.NoError(t, err)

	t.Run(
		"INSERT single row",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO users VALUES (1, 'Alice', 25)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify insertion
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM users WHERE id = 1",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(1),
				result.Rows[0]["id"],
			)
			assert.Equal(
				t,
				"Alice",
				result.Rows[0]["name"],
			)
			assert.Equal(
				t,
				int32(25),
				result.Rows[0]["age"],
			)
		},
	)

	t.Run(
		"INSERT multiple rows sequentially",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO users VALUES (2, 'Bob', 30)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			result, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO users VALUES (3, 'Charlie', 35)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify total rows
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM users",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))
		},
	)

	t.Run(
		"INSERT with NULL values",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO users VALUES (4, NULL, NULL)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify NULL values stored correctly
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM users WHERE id = 4",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Nil(t, result.Rows[0]["name"])
			assert.Nil(t, result.Rows[0]["age"])
		},
	)

	t.Run(
		"INSERT with partial columns",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO users (id, name) VALUES (5, 'David')",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify row with NULL age
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM users WHERE id = 5",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				"David",
				result.Rows[0]["name"],
			)
			assert.Nil(t, result.Rows[0]["age"])
		},
	)
}

// TestPhaseD_Update tests all UPDATE scenarios (Task 4.8)
func TestPhaseD_Update(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE employees (id INTEGER, name VARCHAR, salary INTEGER)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO employees VALUES (1, 'Alice', 50000)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO employees VALUES (2, 'Bob', 60000)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO employees VALUES (3, 'Charlie', 55000)",
	)
	require.NoError(t, err)

	t.Run(
		"UPDATE single row with WHERE",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE employees SET salary = 55000 WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify update
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT salary FROM employees WHERE id = 1",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(55000),
				result.Rows[0]["salary"],
			)
		},
	)

	t.Run(
		"UPDATE multiple rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE employees SET salary = 70000 WHERE id > 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(2),
				result.RowsAffected,
			)

			// Verify updates
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT salary FROM employees WHERE id > 1",
			)
			require.NoError(t, err)
			require.Equal(t, 2, len(result.Rows))
			for _, row := range result.Rows {
				assert.Equal(
					t,
					int32(70000),
					row["salary"],
				)
			}
		},
	)

	t.Run(
		"UPDATE with no matching rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE employees SET salary = 80000 WHERE id = 999",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)
		},
	)

	t.Run(
		"UPDATE all rows (no WHERE clause)",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE employees SET salary = 60000",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(3),
				result.RowsAffected,
			)

			// Verify all rows updated
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT salary FROM employees",
			)
			require.NoError(t, err)
			require.Equal(t, 3, len(result.Rows))
			for _, row := range result.Rows {
				assert.Equal(
					t,
					int32(60000),
					row["salary"],
				)
			}
		},
	)

	t.Run(
		"UPDATE multiple columns",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE employees SET name = 'Alicia', salary = 65000 WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify both columns updated
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT name, salary FROM employees WHERE id = 1",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				"Alicia",
				result.Rows[0]["name"],
			)
			assert.Equal(
				t,
				int32(65000),
				result.Rows[0]["salary"],
			)
		},
	)

	t.Run("UPDATE to NULL", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE employees SET name = NULL WHERE id = 2",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			int64(1),
			result.RowsAffected,
		)

		// Verify NULL update
		result, err = executeQuery(
			t,
			exec,
			cat,
			"SELECT name FROM employees WHERE id = 2",
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		assert.Nil(t, result.Rows[0]["name"])
	})
}

// TestPhaseD_Delete tests all DELETE scenarios (Task 4.9)
func TestPhaseD_Delete(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE products (id INTEGER, name VARCHAR, stock INTEGER)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO products VALUES (1, 'Widget', 100)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO products VALUES (2, 'Gadget', 50)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO products VALUES (3, 'Doohickey', 75)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO products VALUES (4, 'Thingamajig', 0)",
	)
	require.NoError(t, err)

	t.Run(
		"DELETE single row with WHERE",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM products WHERE id = 2",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify deletion
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM products WHERE id = 2",
			)
			require.NoError(t, err)
			assert.Equal(t, 0, len(result.Rows))

			// Verify other rows still exist
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM products",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))
		},
	)

	t.Run(
		"DELETE multiple rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM products WHERE stock < 100",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(2),
				result.RowsAffected,
			)

			// Verify only one row remains
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM products",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(1),
				result.Rows[0]["id"],
			)
		},
	)

	t.Run(
		"DELETE with no matching rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM products WHERE id = 999",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)
		},
	)

	t.Run(
		"DELETE all rows (no WHERE clause)",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM products",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify table is empty
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM products",
			)
			require.NoError(t, err)
			assert.Equal(t, 0, len(result.Rows))
		},
	)
}

// TestPhaseD_DMLIntegration tests mixed DML operations (Task 4.10)
func TestPhaseD_DMLIntegration(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE inventory (id INTEGER, item VARCHAR, quantity INTEGER)",
	)
	require.NoError(t, err)

	t.Run(
		"Complete DML workflow",
		func(t *testing.T) {
			// Step 1: INSERT initial data
			_, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (1, 'Apples', 100)",
			)
			require.NoError(t, err)
			_, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (2, 'Bananas', 150)",
			)
			require.NoError(t, err)
			_, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (3, 'Oranges', 200)",
			)
			require.NoError(t, err)

			// Verify initial state
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM inventory",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))

			// Step 2: UPDATE some quantities
			result, err = executeQuery(
				t,
				exec,
				cat,
				"UPDATE inventory SET quantity = 120 WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify update
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT quantity FROM inventory WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int32(120),
				result.Rows[0]["quantity"],
			)

			// Step 3: DELETE some items
			result, err = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM inventory WHERE id = 2",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify deletion
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM inventory",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))

			// Step 4: INSERT new item
			result, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (4, 'Grapes', 80)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Final verification
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM inventory",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))

			// Verify specific items
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT item FROM inventory WHERE id = 4",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				"Grapes",
				result.Rows[0]["item"],
			)
		},
	)

	t.Run(
		"Conditional UPDATE based on current values",
		func(t *testing.T) {
			// Clear table
			_, _ = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM inventory",
			)

			// Insert test data
			_, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (1, 'Item1', 50)",
			)
			require.NoError(t, err)
			_, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (2, 'Item2', 150)",
			)
			require.NoError(t, err)

			// Update only items with quantity > 100
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE inventory SET quantity = 200 WHERE quantity > 100",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify selective update
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT quantity FROM inventory WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int32(50),
				result.Rows[0]["quantity"],
			) // Unchanged

			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT quantity FROM inventory WHERE id = 2",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int32(200),
				result.Rows[0]["quantity"],
			) // Updated
		},
	)

	t.Run(
		"DELETE with complex WHERE conditions",
		func(t *testing.T) {
			// Clear and repopulate
			_, _ = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM inventory",
			)
			_, _ = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (1, 'A', 10)",
			)
			_, _ = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (2, 'B', 20)",
			)
			_, _ = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO inventory VALUES (3, 'C', 30)",
			)

			// Delete items with quantity between 15 and 25
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM inventory WHERE quantity > 15 AND quantity < 25",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify only item with quantity=20 was deleted
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM inventory",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))
		},
	)
}

// TestPhaseD_DMLPersistence verifies data persistence after DML (Task 4.10)
func TestPhaseD_DMLPersistence(t *testing.T) {
	exec, cat, stor := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE persistent (id INTEGER, value VARCHAR)",
	)
	require.NoError(t, err)

	t.Run(
		"Data persists across multiple operations",
		func(t *testing.T) {
			// Insert
			_, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO persistent VALUES (1, 'first')",
			)
			require.NoError(t, err)

			// Verify via storage layer
			table, ok := stor.GetTable(
				"persistent",
			)
			require.True(
				t,
				ok,
				"table should exist in storage",
			)
			require.NotNil(t, table)

			// Read from storage using scanner
			scanner := table.Scan()
			require.NotNil(t, scanner)

			// Verify via SELECT
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM persistent",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				"first",
				result.Rows[0]["value"],
			)

			// Update
			_, err = executeQuery(
				t,
				exec,
				cat,
				"UPDATE persistent SET value = 'updated' WHERE id = 1",
			)
			require.NoError(t, err)

			// Verify update persisted
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM persistent WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				"updated",
				result.Rows[0]["value"],
			)

			// Insert more data
			_, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO persistent VALUES (2, 'second')",
			)
			require.NoError(t, err)

			// Verify both rows exist
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM persistent",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))

			// Delete one row
			_, err = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM persistent WHERE id = 1",
			)
			require.NoError(t, err)

			// Verify only one row remains
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM persistent",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(2),
				result.Rows[0]["id"],
			)
		},
	)
}

// TestPhaseD_DMLEdgeCases tests edge cases for DML operations (Task 4.10)
func TestPhaseD_DMLEdgeCases(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE edge_cases (id INTEGER, value VARCHAR)",
	)
	require.NoError(t, err)

	t.Run(
		"UPDATE non-existent row",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE edge_cases SET value = 'test' WHERE id = 999",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)
		},
	)

	t.Run(
		"DELETE from empty table",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM edge_cases",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)
		},
	)

	t.Run(
		"INSERT then DELETE same row",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO edge_cases VALUES (1, 'temp')",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			result, err = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM edge_cases WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify table is empty
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM edge_cases",
			)
			require.NoError(t, err)
			assert.Equal(t, 0, len(result.Rows))
		},
	)

	t.Run(
		"UPDATE same row multiple times",
		func(t *testing.T) {
			_, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO edge_cases VALUES (2, 'initial')",
			)
			require.NoError(t, err)

			for i := 1; i <= 5; i++ {
				result, err := executeQuery(
					t,
					exec,
					cat,
					"UPDATE edge_cases SET value = 'updated' WHERE id = 2",
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					int64(1),
					result.RowsAffected,
				)
			}

			// Verify final value
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT value FROM edge_cases WHERE id = 2",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				"updated",
				result.Rows[0]["value"],
			)
		},
	)
}
