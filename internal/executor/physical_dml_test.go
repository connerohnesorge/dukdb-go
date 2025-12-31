package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhysicalDelete(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table and insert test data
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE test_delete (id INTEGER, name VARCHAR)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_delete VALUES (1, 'Alice')",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_delete VALUES (2, 'Bob')",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_delete VALUES (3, 'Charlie')",
	)
	require.NoError(t, err)

	// Verify initial data
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM test_delete",
	)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))

	t.Run(
		"DELETE with WHERE clause",
		func(t *testing.T) {
			// Delete one row
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM test_delete WHERE id = 2",
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
				"SELECT * FROM test_delete",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))

			// Verify correct rows remain
			var ids []int32
			for _, row := range result.Rows {
				// Handle int32 (INTEGER type in DuckDB)
				switch v := row["id"].(type) {
				case int32:
					ids = append(ids, v)
				case int64:
					ids = append(ids, int32(v))
				case int:
					ids = append(ids, int32(v))
				}
			}
			assert.Contains(t, ids, int32(1))
			assert.Contains(t, ids, int32(3))
			assert.NotContains(t, ids, int32(2))
		},
	)

	t.Run(
		"DELETE with WHERE matching multiple rows",
		func(t *testing.T) {
			// Add more data
			_, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO test_delete VALUES (4, 'David')",
			)
			require.NoError(t, err)
			_, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO test_delete VALUES (5, 'Eve')",
			)
			require.NoError(t, err)

			// Delete multiple rows (id > 3)
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM test_delete WHERE id > 3",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(2),
				result.RowsAffected,
			)

			// Verify deletion
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM test_delete",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))
		},
	)

	t.Run(
		"DELETE with no matching rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM test_delete WHERE id = 999",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)

			// Verify no rows deleted
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM test_delete",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))
		},
	)

	t.Run("DELETE all rows", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM test_delete",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			int64(2),
			result.RowsAffected,
		)

		// Verify table is empty
		result, err = executeQuery(
			t,
			exec,
			cat,
			"SELECT * FROM test_delete",
		)
		require.NoError(t, err)
		assert.Equal(t, 0, len(result.Rows))
	})
}

func TestPhysicalUpdate(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table and insert test data
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE test_update (id INTEGER, value INTEGER, name VARCHAR)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_update VALUES (1, 100, 'Alice')",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_update VALUES (2, 200, 'Bob')",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_update VALUES (3, 300, 'Charlie')",
	)
	require.NoError(t, err)

	t.Run(
		"UPDATE single column with WHERE",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE test_update SET value = 150 WHERE id = 1",
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
				"SELECT id, value FROM test_update WHERE id = 1",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(150),
				result.Rows[0]["value"],
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
				"UPDATE test_update SET value = 999 WHERE id > 1",
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
				"SELECT id, value FROM test_update WHERE id > 1",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))
			for _, row := range result.Rows {
				assert.Equal(
					t,
					int32(999),
					row["value"],
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
				"UPDATE test_update SET value = 777 WHERE id = 999",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(0),
				result.RowsAffected,
			)
		},
	)

	t.Run("UPDATE all rows", func(t *testing.T) {
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE test_update SET value = 500",
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
			"SELECT value FROM test_update",
		)
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows))
		for _, row := range result.Rows {
			assert.Equal(
				t,
				int32(500),
				row["value"],
			)
		}
	})

	t.Run(
		"UPDATE multiple columns",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"UPDATE test_update SET value = 123, name = 'Updated' WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify updates
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT id, value, name FROM test_update WHERE id = 1",
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(result.Rows))
			assert.Equal(
				t,
				int32(123),
				result.Rows[0]["value"],
			)
			assert.Equal(
				t,
				"Updated",
				result.Rows[0]["name"],
			)
		},
	)
}

func TestPhysicalInsert(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE test_insert (id INTEGER, name VARCHAR)",
	)
	require.NoError(t, err)

	t.Run(
		"INSERT single row",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO test_insert VALUES (1, 'Alice')",
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
				"SELECT * FROM test_insert",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, len(result.Rows))
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
		},
	)

	t.Run(
		"INSERT multiple rows",
		func(t *testing.T) {
			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO test_insert VALUES (2, 'Bob')",
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
				"INSERT INTO test_insert VALUES (3, 'Charlie')",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify all rows
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM test_insert",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))
		},
	)

	t.Run(
		"INSERT with NULL values",
		func(t *testing.T) {
			_, err := executeQuery(
				t,
				exec,
				cat,
				"CREATE TABLE test_null (id INTEGER, name VARCHAR)",
			)
			require.NoError(t, err)

			result, err := executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO test_null VALUES (1, NULL)",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(1),
				result.RowsAffected,
			)

			// Verify NULL insertion
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM test_null",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, len(result.Rows))
			assert.Nil(t, result.Rows[0]["name"])
		},
	)
}

func TestDMLIntegration(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE items (id INTEGER, qty INTEGER, price INTEGER)",
	)
	require.NoError(t, err)

	// Insert data
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO items VALUES (1, 10, 100)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO items VALUES (2, 20, 200)",
	)
	require.NoError(t, err)
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO items VALUES (3, 30, 300)",
	)
	require.NoError(t, err)

	t.Run(
		"INSERT, UPDATE, DELETE cycle",
		func(t *testing.T) {
			// Verify initial state
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM items",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))

			// Update some rows
			result, err = executeQuery(
				t,
				exec,
				cat,
				"UPDATE items SET qty = 50 WHERE id <= 2",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int64(2),
				result.RowsAffected,
			)

			// Verify update
			result, err = executeQuery(
				t,
				exec,
				cat,
				"SELECT qty FROM items WHERE id = 1",
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				int32(50),
				result.Rows[0]["qty"],
			)

			// Delete a row
			result, err = executeQuery(
				t,
				exec,
				cat,
				"DELETE FROM items WHERE id = 2",
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
				"SELECT * FROM items",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, len(result.Rows))

			// Insert new row
			result, err = executeQuery(
				t,
				exec,
				cat,
				"INSERT INTO items VALUES (4, 40, 400)",
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
				"SELECT * FROM items",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, len(result.Rows))
		},
	)
}
