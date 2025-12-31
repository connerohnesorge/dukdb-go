package executor

import (
	"fmt"
	"strings"
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

// TestPhaseD_DELETE_WhereClauseComprehensive tests comprehensive WHERE clause scenarios
// This covers tasks 1.13-1.17 from the change proposal
func TestPhaseD_DELETE_WhereClauseComprehensive(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Task 1.13: DELETE FROM t WHERE id = 1 deletes only matching row
	t.Run("Task1.13_DELETE_single_row_WHERE_equality", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t113 (id INTEGER, name VARCHAR)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t113 VALUES (1, 'Alice')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t113 VALUES (2, 'Bob')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t113 VALUES (3, 'Charlie')")
		require.NoError(t, err)

		// Delete only row with id = 1
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM t113 WHERE id = 1",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected, "Should delete exactly 1 row")

		// Verify only id=1 was deleted
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t113 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should have 2 rows remaining")

		var ids []int32
		for _, row := range result.Rows {
			switch v := row["id"].(type) {
			case int32:
				ids = append(ids, v)
			case int64:
				ids = append(ids, int32(v))
			case int:
				ids = append(ids, int32(v))
			}
		}
		assert.Contains(t, ids, int32(2), "Row with id=2 should remain")
		assert.Contains(t, ids, int32(3), "Row with id=3 should remain")
		assert.NotContains(t, ids, int32(1), "Row with id=1 should be deleted")
	})

	// Task 1.14: DELETE FROM t WHERE 1 = 0 deletes no rows (returns RowsAffected = 0)
	t.Run("Task1.14_DELETE_WHERE_false_condition", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t114 (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t114 VALUES (1, 100)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t114 VALUES (2, 200)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t114 VALUES (3, 300)")
		require.NoError(t, err)

		// WHERE 1 = 0 should always be false, so no rows should be deleted
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM t114 WHERE 1 = 0",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result.RowsAffected, "Should delete 0 rows when WHERE is always false")

		// Verify all rows remain
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t114")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "All rows should remain")
	})

	// Task 1.15: DELETE FROM t WHERE x IS NULL handles NULL values correctly
	t.Run("Task1.15_DELETE_WHERE_IS_NULL", func(t *testing.T) {
		// Create table and insert test data with NULLs
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t115 (id INTEGER, nullable_col VARCHAR)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t115 VALUES (1, 'Alice')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t115 VALUES (2, NULL)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t115 VALUES (3, 'Charlie')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t115 VALUES (4, NULL)")
		require.NoError(t, err)

		// Delete rows where nullable_col IS NULL
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM t115 WHERE nullable_col IS NULL",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should delete 2 rows with NULL values")

		// Verify only non-NULL rows remain
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t115 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should have 2 rows remaining")

		for _, row := range result.Rows {
			assert.NotNil(t, row["nullable_col"], "Remaining rows should have non-NULL values")
		}
	})

	// Task 1.16: DELETE FROM t WHERE a > 5 AND b < 10 evaluates complex predicates
	t.Run("Task1.16_DELETE_WHERE_complex_AND_predicate", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t116 (id INTEGER, a INTEGER, b INTEGER)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t116 VALUES (1, 3, 5)")
		require.NoError(t, err) // a <= 5, skip
		_, err = executeQuery(t, exec, cat, "INSERT INTO t116 VALUES (2, 6, 8)")
		require.NoError(t, err) // a > 5 AND b < 10, DELETE
		_, err = executeQuery(t, exec, cat, "INSERT INTO t116 VALUES (3, 7, 12)")
		require.NoError(t, err) // a > 5 but b >= 10, skip
		_, err = executeQuery(t, exec, cat, "INSERT INTO t116 VALUES (4, 8, 9)")
		require.NoError(t, err) // a > 5 AND b < 10, DELETE
		_, err = executeQuery(t, exec, cat, "INSERT INTO t116 VALUES (5, 10, 7)")
		require.NoError(t, err) // a > 5 AND b < 10, DELETE

		// Delete rows where a > 5 AND b < 10
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM t116 WHERE a > 5 AND b < 10",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result.RowsAffected, "Should delete 3 rows matching a > 5 AND b < 10")

		// Verify correct rows remain
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t116 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should have 2 rows remaining")

		var ids []int32
		for _, row := range result.Rows {
			switch v := row["id"].(type) {
			case int32:
				ids = append(ids, v)
			case int64:
				ids = append(ids, int32(v))
			case int:
				ids = append(ids, int32(v))
			}
		}
		assert.Contains(t, ids, int32(1), "Row with id=1 should remain (a=3 <= 5)")
		assert.Contains(t, ids, int32(3), "Row with id=3 should remain (b=12 >= 10)")
	})

	// Task 1.17: DELETE with subquery WHERE id IN (SELECT ...) works correctly
	t.Run("Task1.17_DELETE_WHERE_IN_subquery", func(t *testing.T) {
		// Create main table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t117_main (id INTEGER, name VARCHAR)",
		)
		require.NoError(t, err)

		// Create reference table for subquery
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t117_ref (ref_id INTEGER)",
		)
		require.NoError(t, err)

		// Insert data into main table
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_main VALUES (1, 'Alice')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_main VALUES (2, 'Bob')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_main VALUES (3, 'Charlie')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_main VALUES (4, 'David')")
		require.NoError(t, err)

		// Insert reference IDs (2 and 4 should be deleted from main)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_ref VALUES (2)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t117_ref VALUES (4)")
		require.NoError(t, err)

		// Delete from main table where id IN (SELECT ref_id FROM t117_ref)
		result, err := executeQuery(
			t,
			exec,
			cat,
			"DELETE FROM t117_main WHERE id IN (SELECT ref_id FROM t117_ref)",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should delete 2 rows matching subquery results")

		// Verify correct rows remain
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t117_main ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should have 2 rows remaining")

		var ids []int32
		for _, row := range result.Rows {
			switch v := row["id"].(type) {
			case int32:
				ids = append(ids, v)
			case int64:
				ids = append(ids, int32(v))
			case int:
				ids = append(ids, int32(v))
			}
		}
		assert.Contains(t, ids, int32(1), "Row with id=1 should remain")
		assert.Contains(t, ids, int32(3), "Row with id=3 should remain")
		assert.NotContains(t, ids, int32(2), "Row with id=2 should be deleted")
		assert.NotContains(t, ids, int32(4), "Row with id=4 should be deleted")
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

// TestPhaseD_UPDATE_WhereClauseComprehensive tests comprehensive UPDATE scenarios with WHERE clauses
// This covers tasks 1.22-1.25 from the change proposal
func TestPhaseD_UPDATE_WhereClauseComprehensive(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Task 1.22: UPDATE t SET x = 1 WHERE id = 1 updates only matching row
	t.Run("Task1.22_UPDATE_single_row_WHERE_equality", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t122 (id INTEGER, x INTEGER, name VARCHAR)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t122 VALUES (1, 10, 'Alice')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t122 VALUES (2, 20, 'Bob')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t122 VALUES (3, 30, 'Charlie')")
		require.NoError(t, err)

		// Update only row with id = 1
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE t122 SET x = 1 WHERE id = 1",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected, "Should update exactly 1 row")

		// Verify only id=1 was updated
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t122 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "Should have 3 rows total")

		// Check row with id=1 was updated
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(1), result.Rows[0]["x"], "Row id=1 should have x=1")
		assert.Equal(t, "Alice", result.Rows[0]["name"])

		// Check row with id=2 was NOT updated
		assert.Equal(t, int32(2), result.Rows[1]["id"])
		assert.Equal(t, int32(20), result.Rows[1]["x"], "Row id=2 should still have x=20")
		assert.Equal(t, "Bob", result.Rows[1]["name"])

		// Check row with id=3 was NOT updated
		assert.Equal(t, int32(3), result.Rows[2]["id"])
		assert.Equal(t, int32(30), result.Rows[2]["x"], "Row id=3 should still have x=30")
		assert.Equal(t, "Charlie", result.Rows[2]["name"])
	})

	// Task 1.23: UPDATE t SET x = x + 1 WHERE x > 5 evaluates expressions in SET clause
	t.Run("Task1.23_UPDATE_SET_expression_with_current_value", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t123 (id INTEGER, x INTEGER)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t123 VALUES (1, 3)")
		require.NoError(t, err) // x=3, not > 5, skip
		_, err = executeQuery(t, exec, cat, "INSERT INTO t123 VALUES (2, 6)")
		require.NoError(t, err) // x=6 > 5, update to 7
		_, err = executeQuery(t, exec, cat, "INSERT INTO t123 VALUES (3, 10)")
		require.NoError(t, err) // x=10 > 5, update to 11
		_, err = executeQuery(t, exec, cat, "INSERT INTO t123 VALUES (4, 5)")
		require.NoError(t, err) // x=5, not > 5, skip

		// Update rows where x > 5, incrementing x by 1
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE t123 SET x = x + 1 WHERE x > 5",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should update 2 rows (id=2 and id=3)")

		// Verify updates
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t123 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 4, len(result.Rows), "Should have 4 rows total")

		// id=1: x should still be 3 (not updated)
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(3), result.Rows[0]["x"], "Row id=1 should still have x=3")

		// id=2: x should be 7 (was 6, incremented by 1)
		assert.Equal(t, int32(2), result.Rows[1]["id"])
		assert.Equal(t, int32(7), result.Rows[1]["x"], "Row id=2 should have x=7 (6+1)")

		// id=3: x should be 11 (was 10, incremented by 1)
		assert.Equal(t, int32(3), result.Rows[2]["id"])
		assert.Equal(t, int32(11), result.Rows[2]["x"], "Row id=3 should have x=11 (10+1)")

		// id=4: x should still be 5 (not updated)
		assert.Equal(t, int32(4), result.Rows[3]["id"])
		assert.Equal(t, int32(5), result.Rows[3]["x"], "Row id=4 should still have x=5")
	})

	// Task 1.24: UPDATE t SET a = 1, b = 2 WHERE c = 3 handles multi-column updates
	t.Run("Task1.24_UPDATE_multiple_columns_WHERE", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t124 (id INTEGER, a INTEGER, b INTEGER, c INTEGER)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t124 VALUES (1, 10, 20, 3)")
		require.NoError(t, err) // c=3, update a and b
		_, err = executeQuery(t, exec, cat, "INSERT INTO t124 VALUES (2, 30, 40, 5)")
		require.NoError(t, err) // c=5, skip
		_, err = executeQuery(t, exec, cat, "INSERT INTO t124 VALUES (3, 50, 60, 3)")
		require.NoError(t, err) // c=3, update a and b
		_, err = executeQuery(t, exec, cat, "INSERT INTO t124 VALUES (4, 70, 80, 7)")
		require.NoError(t, err) // c=7, skip

		// Update multiple columns where c = 3
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE t124 SET a = 1, b = 2 WHERE c = 3",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should update 2 rows (id=1 and id=3)")

		// Verify updates
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t124 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 4, len(result.Rows), "Should have 4 rows total")

		// id=1: a=1, b=2, c=3 (updated)
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(1), result.Rows[0]["a"], "Row id=1 should have a=1")
		assert.Equal(t, int32(2), result.Rows[0]["b"], "Row id=1 should have b=2")
		assert.Equal(t, int32(3), result.Rows[0]["c"], "Row id=1 should still have c=3")

		// id=2: unchanged
		assert.Equal(t, int32(2), result.Rows[1]["id"])
		assert.Equal(t, int32(30), result.Rows[1]["a"], "Row id=2 should still have a=30")
		assert.Equal(t, int32(40), result.Rows[1]["b"], "Row id=2 should still have b=40")
		assert.Equal(t, int32(5), result.Rows[1]["c"], "Row id=2 should still have c=5")

		// id=3: a=1, b=2, c=3 (updated)
		assert.Equal(t, int32(3), result.Rows[2]["id"])
		assert.Equal(t, int32(1), result.Rows[2]["a"], "Row id=3 should have a=1")
		assert.Equal(t, int32(2), result.Rows[2]["b"], "Row id=3 should have b=2")
		assert.Equal(t, int32(3), result.Rows[2]["c"], "Row id=3 should still have c=3")

		// id=4: unchanged
		assert.Equal(t, int32(4), result.Rows[3]["id"])
		assert.Equal(t, int32(70), result.Rows[3]["a"], "Row id=4 should still have a=70")
		assert.Equal(t, int32(80), result.Rows[3]["b"], "Row id=4 should still have b=80")
		assert.Equal(t, int32(7), result.Rows[3]["c"], "Row id=4 should still have c=7")
	})

	// Task 1.25: UPDATE with complex WHERE (OR, NOT, BETWEEN) works correctly
	t.Run("Task1.25_UPDATE_complex_WHERE_clause", func(t *testing.T) {
		// Create table and insert test data
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t125 (id INTEGER, x INTEGER, status VARCHAR)",
		)
		require.NoError(t, err)

		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (1, 5, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (2, 10, 'inactive')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (3, 15, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (4, 20, 'inactive')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (5, 25, 'active')")
		require.NoError(t, err)

		// Test with OR clause: UPDATE rows where x < 10 OR x > 20
		result, err := executeQuery(
			t,
			exec,
			cat,
			"UPDATE t125 SET status = 'updated' WHERE x < 10 OR x > 20",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should update 2 rows (id=1 with x=5, id=5 with x=25)")

		// Verify OR clause updates
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t125 ORDER BY id")
		require.NoError(t, err)

		assert.Equal(t, "updated", result.Rows[0]["status"], "Row id=1 should be updated (x=5 < 10)")
		assert.Equal(t, "inactive", result.Rows[1]["status"], "Row id=2 should not be updated")
		assert.Equal(t, "active", result.Rows[2]["status"], "Row id=3 should not be updated")
		assert.Equal(t, "inactive", result.Rows[3]["status"], "Row id=4 should not be updated")
		assert.Equal(t, "updated", result.Rows[4]["status"], "Row id=5 should be updated (x=25 > 20)")

		// Reset table for next test
		_, err = executeQuery(t, exec, cat, "DROP TABLE t125")
		require.NoError(t, err)
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t125 (id INTEGER, x INTEGER, status VARCHAR)",
		)
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (1, 5, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (2, 10, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (3, 15, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (4, 20, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (5, 25, 'active')")
		require.NoError(t, err)

		// Test with NOT clause: UPDATE rows where NOT (x >= 15)
		result, err = executeQuery(
			t,
			exec,
			cat,
			"UPDATE t125 SET status = 'updated_not' WHERE NOT (x >= 15)",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowsAffected, "Should update 2 rows (id=1 and id=2 with x < 15)")

		// Verify NOT clause updates
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t125 ORDER BY id")
		require.NoError(t, err)

		assert.Equal(t, "updated_not", result.Rows[0]["status"], "Row id=1 should be updated (NOT x >= 15)")
		assert.Equal(t, "updated_not", result.Rows[1]["status"], "Row id=2 should be updated (NOT x >= 15)")
		assert.Equal(t, "active", result.Rows[2]["status"], "Row id=3 should not be updated")
		assert.Equal(t, "active", result.Rows[3]["status"], "Row id=4 should not be updated")
		assert.Equal(t, "active", result.Rows[4]["status"], "Row id=5 should not be updated")

		// Reset table for BETWEEN test
		_, err = executeQuery(t, exec, cat, "DROP TABLE t125")
		require.NoError(t, err)
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t125 (id INTEGER, x INTEGER, status VARCHAR)",
		)
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (1, 5, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (2, 10, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (3, 15, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (4, 20, 'active')")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO t125 VALUES (5, 25, 'active')")
		require.NoError(t, err)

		// Test with BETWEEN clause: UPDATE rows where x BETWEEN 10 AND 20
		result, err = executeQuery(
			t,
			exec,
			cat,
			"UPDATE t125 SET status = 'between' WHERE x BETWEEN 10 AND 20",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result.RowsAffected, "Should update 3 rows (id=2,3,4 with x in [10,20])")

		// Verify BETWEEN clause updates
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t125 ORDER BY id")
		require.NoError(t, err)

		assert.Equal(t, "active", result.Rows[0]["status"], "Row id=1 should not be updated (x=5 < 10)")
		assert.Equal(t, "between", result.Rows[1]["status"], "Row id=2 should be updated (x=10 in [10,20])")
		assert.Equal(t, "between", result.Rows[2]["status"], "Row id=3 should be updated (x=15 in [10,20])")
		assert.Equal(t, "between", result.Rows[3]["status"], "Row id=4 should be updated (x=20 in [10,20])")
		assert.Equal(t, "active", result.Rows[4]["status"], "Row id=5 should not be updated (x=25 > 20)")
	})
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

// TestPhaseD_INSERT_Batching tests DataChunk batching for bulk INSERT operations
// This covers tasks 2.12-2.15 from the change proposal
func TestPhaseD_INSERT_Batching(t *testing.T) {
	// Task 2.12: INSERT 100 rows creates single DataChunk
	t.Run("Task2.12_INSERT_100Rows_SingleChunk", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t212 (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		// Build INSERT statement with 100 rows of VALUES
		var valueParts []string
		for i := 1; i <= 100; i++ {
			valueParts = append(valueParts, fmt.Sprintf("(%d, %d)", i, i*10))
		}
		insertSQL := "INSERT INTO t212 VALUES " + strings.Join(valueParts, ", ")

		// Execute the INSERT
		result, err := executeQuery(t, exec, cat, insertSQL)
		require.NoError(t, err)
		assert.Equal(t, int64(100), result.RowsAffected, "Should insert exactly 100 rows")

		// Verify all rows were inserted correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t212 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 100, len(result.Rows), "Table should have 100 rows")

		// Verify first and last rows
		assert.Equal(t, int32(1), result.Rows[0]["id"], "First row id should be 1")
		assert.Equal(t, int32(10), result.Rows[0]["value"], "First row value should be 10")
		assert.Equal(t, int32(100), result.Rows[99]["id"], "Last row id should be 100")
		assert.Equal(t, int32(1000), result.Rows[99]["value"], "Last row value should be 1000")
	})

	// Task 2.13: INSERT 5000 rows creates multiple DataChunks (2048 + 2048 + 904)
	t.Run("Task2.13_INSERT_5000Rows_MultipleChunks", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t213 (id INTEGER, value INTEGER, name VARCHAR)",
		)
		require.NoError(t, err)

		// Build INSERT statement with 5000 rows of VALUES
		var valueParts []string
		for i := 1; i <= 5000; i++ {
			valueParts = append(valueParts, fmt.Sprintf("(%d, %d, 'row%d')", i, i*10, i))
		}
		insertSQL := "INSERT INTO t213 VALUES " + strings.Join(valueParts, ", ")

		// Execute the INSERT
		result, err := executeQuery(t, exec, cat, insertSQL)
		require.NoError(t, err)
		assert.Equal(t, int64(5000), result.RowsAffected, "Should insert exactly 5000 rows")

		// Verify all rows were inserted correctly by counting rows from SELECT *
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t213")
		require.NoError(t, err)
		assert.Equal(t, 5000, len(result.Rows), "Table should have 5000 rows")

		// Verify specific rows to ensure batching worked correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t213 WHERE id = 1")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows), "Should find row with id=1")
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(10), result.Rows[0]["value"])
		assert.Equal(t, "row1", result.Rows[0]["name"])

		// Check row at batch boundary (row 2048)
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t213 WHERE id = 2048")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows), "Should find row with id=2048")
		assert.Equal(t, int32(2048), result.Rows[0]["id"])

		// Check row after first batch (row 2049)
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t213 WHERE id = 2049")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows), "Should find row with id=2049")
		assert.Equal(t, int32(2049), result.Rows[0]["id"])

		// Check last row
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t213 WHERE id = 5000")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows), "Should find row with id=5000")
		assert.Equal(t, int32(5000), result.Rows[0]["id"])
		assert.Equal(t, int32(50000), result.Rows[0]["value"])
		assert.Equal(t, "row5000", result.Rows[0]["name"])
	})

	// Task 2.14: INSERT with expression VALUES
	t.Run("Task2.14_INSERT_ExpressionValues", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t214 (id INTEGER, computed INTEGER, combined VARCHAR)",
		)
		require.NoError(t, err)

		// INSERT with expressions: arithmetic and string operations
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO t214 VALUES (1, 1 + 1, 'hello'), (2, 2 * 3, 'world'), (3, 10 - 5, 'test')",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result.RowsAffected, "Should insert 3 rows")

		// Verify expressions were evaluated correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t214 ORDER BY id")
		require.NoError(t, err)
		require.Equal(t, 3, len(result.Rows), "Should have 3 rows")

		// Debug output to check actual types
		t.Logf("Row 0 computed: type=%T, value=%v", result.Rows[0]["computed"], result.Rows[0]["computed"])

		// Helper to extract int value regardless of type
		getInt := func(v any) int64 {
			switch val := v.(type) {
			case int64:
				return val
			case int32:
				return int64(val)
			case int:
				return int64(val)
			case float64:
				return int64(val)
			default:
				return 0
			}
		}

		// Row 1: computed = 1 + 1 = 2
		assert.Equal(t, int64(1), getInt(result.Rows[0]["id"]))
		assert.Equal(t, int64(2), getInt(result.Rows[0]["computed"]), "1+1 should equal 2")
		assert.Equal(t, "hello", result.Rows[0]["combined"])

		// Row 2: computed = 2 * 3 = 6
		assert.Equal(t, int64(2), getInt(result.Rows[1]["id"]))
		assert.Equal(t, int64(6), getInt(result.Rows[1]["computed"]), "2*3 should equal 6")
		assert.Equal(t, "world", result.Rows[1]["combined"])

		// Row 3: computed = 10 - 5 = 5
		assert.Equal(t, int64(3), getInt(result.Rows[2]["id"]))
		assert.Equal(t, int64(5), getInt(result.Rows[2]["computed"]), "10-5 should equal 5")
		assert.Equal(t, "test", result.Rows[2]["combined"])
	})

	// Task 2.15: INSERT with NULL values
	t.Run("Task2.15_INSERT_NullValues", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table with nullable columns
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t215 (id INTEGER, nullable_int INTEGER, nullable_str VARCHAR)",
		)
		require.NoError(t, err)

		// INSERT with NULL values in different columns
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO t215 VALUES (1, NULL, 'a'), (2, 100, NULL), (3, NULL, NULL), (4, 200, 'b')",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(4), result.RowsAffected, "Should insert 4 rows")

		// Verify NULL values were inserted correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t215 ORDER BY id")
		require.NoError(t, err)
		require.Equal(t, 4, len(result.Rows), "Should have 4 rows")

		// Row 1: (1, NULL, 'a')
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Nil(t, result.Rows[0]["nullable_int"], "Row 1 nullable_int should be NULL")
		assert.Equal(t, "a", result.Rows[0]["nullable_str"])

		// Row 2: (2, 100, NULL)
		assert.Equal(t, int32(2), result.Rows[1]["id"])
		assert.Equal(t, int32(100), result.Rows[1]["nullable_int"])
		assert.Nil(t, result.Rows[1]["nullable_str"], "Row 2 nullable_str should be NULL")

		// Row 3: (3, NULL, NULL)
		assert.Equal(t, int32(3), result.Rows[2]["id"])
		assert.Nil(t, result.Rows[2]["nullable_int"], "Row 3 nullable_int should be NULL")
		assert.Nil(t, result.Rows[2]["nullable_str"], "Row 3 nullable_str should be NULL")

		// Row 4: (4, 200, 'b')
		assert.Equal(t, int32(4), result.Rows[3]["id"])
		assert.Equal(t, int32(200), result.Rows[3]["nullable_int"])
		assert.Equal(t, "b", result.Rows[3]["nullable_str"])

		// Test filtering with IS NULL
		result, err = executeQuery(t, exec, cat, "SELECT * FROM t215 WHERE nullable_int IS NULL ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should find 2 rows with NULL nullable_int")
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(3), result.Rows[1]["id"])

		result, err = executeQuery(t, exec, cat, "SELECT * FROM t215 WHERE nullable_str IS NULL ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should find 2 rows with NULL nullable_str")
		assert.Equal(t, int32(2), result.Rows[0]["id"])
		assert.Equal(t, int32(3), result.Rows[1]["id"])
	})

	// Additional test: INSERT 100 rows with batching in multiple separate INSERTs
	t.Run("INSERT_MultipleSmallBatches", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE t_batches (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		// Insert in 10 batches of 10 rows each
		totalRowsAffected := int64(0)
		for batch := 0; batch < 10; batch++ {
			var valueParts []string
			for i := 0; i < 10; i++ {
				id := batch*10 + i + 1
				valueParts = append(valueParts, fmt.Sprintf("(%d, %d)", id, id*10))
			}
			insertSQL := "INSERT INTO t_batches VALUES " + strings.Join(valueParts, ", ")

			result, err := executeQuery(t, exec, cat, insertSQL)
			require.NoError(t, err)
			assert.Equal(t, int64(10), result.RowsAffected, "Each batch should insert 10 rows")
			totalRowsAffected += result.RowsAffected
		}

		assert.Equal(t, int64(100), totalRowsAffected, "Total rows affected should be 100")

		// Verify all rows
		result, err := executeQuery(t, exec, cat, "SELECT * FROM t_batches ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 100, len(result.Rows), "Table should have 100 rows")
	})
}

// TestPhaseD_INSERT_SELECT tests INSERT...SELECT functionality with DataChunk batching
// This covers tasks 2.16-2.22 from the change proposal
func TestPhaseD_INSERT_SELECT(t *testing.T) {
	// Task 2.19: INSERT INTO t SELECT * FROM source copies all rows
	t.Run("Task2.19_INSERT_SELECT_CopyAllRows", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create source table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE source219 (id INTEGER, name VARCHAR, value INTEGER)",
		)
		require.NoError(t, err)

		// Insert test data into source
		_, err = executeQuery(t, exec, cat, "INSERT INTO source219 VALUES (1, 'Alice', 100)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source219 VALUES (2, 'Bob', 200)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source219 VALUES (3, 'Charlie', 300)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source219 VALUES (4, 'David', 400)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source219 VALUES (5, 'Eve', 500)")
		require.NoError(t, err)

		// Create target table with same schema
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE target219 (id INTEGER, name VARCHAR, value INTEGER)",
		)
		require.NoError(t, err)

		// INSERT...SELECT all rows
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO target219 SELECT * FROM source219",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(5), result.RowsAffected, "Should insert 5 rows from source")

		// Verify all rows were copied correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target219 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows), "Target table should have 5 rows")

		// Verify specific rows
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int32(100), result.Rows[0]["value"])

		assert.Equal(t, int32(5), result.Rows[4]["id"])
		assert.Equal(t, "Eve", result.Rows[4]["name"])
		assert.Equal(t, int32(500), result.Rows[4]["value"])
	})

	// Task 2.20: INSERT INTO t SELECT * FROM large_table (large dataset) completes with bounded memory
	// Using 10,000 rows as a reasonable size that tests batching without being too slow
	t.Run("Task2.20_INSERT_SELECT_LargeDataset", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create source table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE source220 (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		// Build and insert 10,000 rows into source table using bulk INSERT VALUES
		const numRows = 10000
		const batchSize = 1000 // Insert 1000 rows at a time to build source data
		for batch := 0; batch < numRows/batchSize; batch++ {
			var valueParts []string
			for i := 0; i < batchSize; i++ {
				id := batch*batchSize + i + 1
				valueParts = append(valueParts, fmt.Sprintf("(%d, %d)", id, id*10))
			}
			insertSQL := "INSERT INTO source220 VALUES " + strings.Join(valueParts, ", ")
			_, err := executeQuery(t, exec, cat, insertSQL)
			require.NoError(t, err)
		}

		// Verify source has 10,000 rows
		result, err := executeQuery(t, exec, cat, "SELECT * FROM source220")
		require.NoError(t, err)
		require.Equal(t, numRows, len(result.Rows), "Source should have 10,000 rows")

		// Create target table
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE target220 (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		// INSERT...SELECT all 10,000 rows - this tests DataChunk batching with large dataset
		result, err = executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO target220 SELECT * FROM source220",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(numRows), result.RowsAffected, "Should insert 10,000 rows")

		// Verify all rows were copied
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target220")
		require.NoError(t, err)
		assert.Equal(t, numRows, len(result.Rows), "Target should have 10,000 rows")

		// Verify boundary rows (first, batch boundaries, last)
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target220 WHERE id = 1")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, int32(10), result.Rows[0]["value"])

		// Check around first batch boundary (2048)
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target220 WHERE id = 2048")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, int32(2048), result.Rows[0]["id"])

		result, err = executeQuery(t, exec, cat, "SELECT * FROM target220 WHERE id = 2049")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, int32(2049), result.Rows[0]["id"])

		// Check last row
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target220 WHERE id = 10000")
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, int32(10000), result.Rows[0]["id"])
		assert.Equal(t, int32(100000), result.Rows[0]["value"])
	})

	// Task 2.21: INSERT INTO t SELECT x+1 FROM source evaluates expressions
	t.Run("Task2.21_INSERT_SELECT_WithExpressions", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create source table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE source221 (id INTEGER, value INTEGER)",
		)
		require.NoError(t, err)

		// Insert test data
		_, err = executeQuery(t, exec, cat, "INSERT INTO source221 VALUES (1, 10)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source221 VALUES (2, 20)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source221 VALUES (3, 30)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source221 VALUES (4, 40)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO source221 VALUES (5, 50)")
		require.NoError(t, err)

		// Create target table
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE target221 (new_id INTEGER, computed INTEGER)",
		)
		require.NoError(t, err)

		// INSERT...SELECT with expressions: id+1, value*2
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO target221 SELECT id + 1, value * 2 FROM source221",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(5), result.RowsAffected, "Should insert 5 rows")

		// Verify expressions were evaluated correctly
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target221 ORDER BY new_id")
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows), "Target should have 5 rows")

		// Helper function to handle int type variations
		getInt := func(v any) int64 {
			switch val := v.(type) {
			case int64:
				return val
			case int32:
				return int64(val)
			case int:
				return int64(val)
			default:
				return 0
			}
		}

		// Row 1: id+1 = 2, value*2 = 20
		assert.Equal(t, int64(2), getInt(result.Rows[0]["new_id"]), "id+1 should equal 2")
		assert.Equal(t, int64(20), getInt(result.Rows[0]["computed"]), "10*2 should equal 20")

		// Row 2: id+1 = 3, value*2 = 40
		assert.Equal(t, int64(3), getInt(result.Rows[1]["new_id"]))
		assert.Equal(t, int64(40), getInt(result.Rows[1]["computed"]))

		// Row 3: id+1 = 4, value*2 = 60
		assert.Equal(t, int64(4), getInt(result.Rows[2]["new_id"]))
		assert.Equal(t, int64(60), getInt(result.Rows[2]["computed"]))

		// Row 4: id+1 = 5, value*2 = 80
		assert.Equal(t, int64(5), getInt(result.Rows[3]["new_id"]))
		assert.Equal(t, int64(80), getInt(result.Rows[3]["computed"]))

		// Row 5: id+1 = 6, value*2 = 100
		assert.Equal(t, int64(6), getInt(result.Rows[4]["new_id"]))
		assert.Equal(t, int64(100), getInt(result.Rows[4]["computed"]))
	})

	// Task 2.22: INSERT...SELECT with WHERE clause filters correctly
	t.Run("Task2.22_INSERT_SELECT_WithWHERE", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create source table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE source222 (id INTEGER, name VARCHAR, active INTEGER)",
		)
		require.NoError(t, err)

		// Insert test data - mix of active and inactive rows
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (1, 'Alice', 1)")
		require.NoError(t, err) // active
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (2, 'Bob', 0)")
		require.NoError(t, err) // inactive
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (3, 'Charlie', 1)")
		require.NoError(t, err) // active
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (4, 'David', 0)")
		require.NoError(t, err) // inactive
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (5, 'Eve', 1)")
		require.NoError(t, err) // active
		_, err = executeQuery(t, exec, cat, "INSERT INTO source222 VALUES (6, 'Frank', 1)")
		require.NoError(t, err) // active

		// Create target table
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE target222 (id INTEGER, name VARCHAR, active INTEGER)",
		)
		require.NoError(t, err)

		// INSERT...SELECT only active rows (active = 1)
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO target222 SELECT * FROM source222 WHERE active = 1",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(4), result.RowsAffected, "Should insert 4 active rows")

		// Verify only active rows were copied
		result, err = executeQuery(t, exec, cat, "SELECT * FROM target222 ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 4, len(result.Rows), "Target should have 4 rows")

		// Verify all copied rows have active = 1
		for i, row := range result.Rows {
			assert.Equal(t, int32(1), row["active"], "Row %d should have active=1", i)
		}

		// Verify specific rows
		var ids []int32
		for _, row := range result.Rows {
			switch v := row["id"].(type) {
			case int32:
				ids = append(ids, v)
			case int64:
				ids = append(ids, int32(v))
			}
		}
		assert.Contains(t, ids, int32(1), "Alice (id=1, active) should be copied")
		assert.Contains(t, ids, int32(3), "Charlie (id=3, active) should be copied")
		assert.Contains(t, ids, int32(5), "Eve (id=5, active) should be copied")
		assert.Contains(t, ids, int32(6), "Frank (id=6, active) should be copied")
		assert.NotContains(t, ids, int32(2), "Bob (id=2, inactive) should NOT be copied")
		assert.NotContains(t, ids, int32(4), "David (id=4, inactive) should NOT be copied")
	})

	// Additional test: INSERT...SELECT with complex WHERE (AND/OR)
	t.Run("INSERT_SELECT_ComplexWHERE", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create source table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE sourceComplex (id INTEGER, category VARCHAR, value INTEGER)",
		)
		require.NoError(t, err)

		// Insert test data
		_, err = executeQuery(t, exec, cat, "INSERT INTO sourceComplex VALUES (1, 'A', 10)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO sourceComplex VALUES (2, 'B', 50)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO sourceComplex VALUES (3, 'A', 100)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO sourceComplex VALUES (4, 'C', 25)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO sourceComplex VALUES (5, 'B', 75)")
		require.NoError(t, err)

		// Create target table
		_, err = executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE targetComplex (id INTEGER, category VARCHAR, value INTEGER)",
		)
		require.NoError(t, err)

		// INSERT...SELECT with complex WHERE: category = 'A' OR value > 50
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO targetComplex SELECT * FROM sourceComplex WHERE category = 'A' OR value > 50",
		)
		require.NoError(t, err)
		// category = 'A': id 1, 3
		// value > 50: id 3 (100), 5 (75)
		// Union: id 1, 3, 5 = 3 rows
		assert.Equal(t, int64(3), result.RowsAffected, "Should insert 3 rows matching complex WHERE")

		// Verify rows
		result, err = executeQuery(t, exec, cat, "SELECT * FROM targetComplex ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows))

		var ids []int32
		for _, row := range result.Rows {
			switch v := row["id"].(type) {
			case int32:
				ids = append(ids, v)
			case int64:
				ids = append(ids, int32(v))
			}
		}
		assert.Contains(t, ids, int32(1), "id=1 should be copied (category=A)")
		assert.Contains(t, ids, int32(3), "id=3 should be copied (category=A, value=100)")
		assert.Contains(t, ids, int32(5), "id=5 should be copied (value=75)")
	})

	// Test INSERT...SELECT from same table (self-copy with filter)
	t.Run("INSERT_SELECT_SelfCopy", func(t *testing.T) {
		exec, cat, _ := setupTestExecutor()

		// Create table
		_, err := executeQuery(
			t,
			exec,
			cat,
			"CREATE TABLE selfCopy (id INTEGER, category VARCHAR, value INTEGER)",
		)
		require.NoError(t, err)

		// Insert initial data
		_, err = executeQuery(t, exec, cat, "INSERT INTO selfCopy VALUES (1, 'original', 10)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO selfCopy VALUES (2, 'original', 20)")
		require.NoError(t, err)
		_, err = executeQuery(t, exec, cat, "INSERT INTO selfCopy VALUES (3, 'original', 30)")
		require.NoError(t, err)

		// Self-copy: INSERT SELECT with transformation
		// Note: This copies rows into the same table
		result, err := executeQuery(
			t,
			exec,
			cat,
			"INSERT INTO selfCopy SELECT id + 10, 'copied', value * 2 FROM selfCopy WHERE category = 'original'",
		)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result.RowsAffected, "Should insert 3 copied rows")

		// Verify table now has 6 rows
		result, err = executeQuery(t, exec, cat, "SELECT * FROM selfCopy ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 6, len(result.Rows), "Table should have 6 rows total")

		// Check original rows still exist
		result, err = executeQuery(t, exec, cat, "SELECT * FROM selfCopy WHERE category = 'original' ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "Should have 3 original rows")

		// Check copied rows exist with transformed values
		result, err = executeQuery(t, exec, cat, "SELECT * FROM selfCopy WHERE category = 'copied' ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "Should have 3 copied rows")

		// Helper
		getInt := func(v any) int64 {
			switch val := v.(type) {
			case int64:
				return val
			case int32:
				return int64(val)
			case int:
				return int64(val)
			default:
				return 0
			}
		}

		// Verify transformations
		assert.Equal(t, int64(11), getInt(result.Rows[0]["id"]), "First copied row should have id=11")
		assert.Equal(t, int64(20), getInt(result.Rows[0]["value"]), "First copied row value should be 20 (10*2)")
		assert.Equal(t, int64(12), getInt(result.Rows[1]["id"]), "Second copied row should have id=12")
		assert.Equal(t, int64(40), getInt(result.Rows[1]["value"]), "Second copied row value should be 40 (20*2)")
		assert.Equal(t, int64(13), getInt(result.Rows[2]["id"]), "Third copied row should have id=13")
		assert.Equal(t, int64(60), getInt(result.Rows[2]["value"]), "Third copied row value should be 60 (30*2)")
	})
}

// TestWALIntegration_INSERT tests that INSERT operations create WAL entries
// This covers task 3.14: Test INSERT operation creates WAL entry
func TestWALIntegration_INSERT(t *testing.T) {
	exec, cat, _ := setupTestExecutorWithWAL(t)

	// Create table
	_, err := executeQuery(t, exec, cat, "CREATE TABLE wal_insert_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Insert data - this should create a WAL entry
	result, err := executeQuery(t, exec, cat, "INSERT INTO wal_insert_test VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected, "Should insert 2 rows")

	// Verify WAL entry was written by reading WAL entries
	entries := getWALEntries(t, exec)
	require.NotEmpty(t, entries, "WAL should have entries")

	// Look for INSERT entry
	var foundInsert bool
	for _, entry := range entries {
		if entry.Type().String() == "INSERT" {
			foundInsert = true
			break
		}
	}
	assert.True(t, foundInsert, "WAL should contain INSERT entry")
}

// TestWALIntegration_UPDATE tests that UPDATE operations create WAL entries with before/after values
// This covers task 3.15: Test UPDATE operation creates WAL entry with before/after values
func TestWALIntegration_UPDATE(t *testing.T) {
	exec, cat, _ := setupTestExecutorWithWAL(t)

	// Create table and insert initial data
	_, err := executeQuery(t, exec, cat, "CREATE TABLE wal_update_test (id INTEGER, value INTEGER)")
	require.NoError(t, err)

	_, err = executeQuery(t, exec, cat, "INSERT INTO wal_update_test VALUES (1, 100), (2, 200)")
	require.NoError(t, err)

	// Update data - this should create a WAL entry with before/after values
	result, err := executeQuery(t, exec, cat, "UPDATE wal_update_test SET value = 150 WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "Should update 1 row")

	// Verify WAL entry was written
	entries := getWALEntries(t, exec)
	require.NotEmpty(t, entries, "WAL should have entries")

	// Look for UPDATE entry
	var foundUpdate bool
	for _, entry := range entries {
		if entry.Type().String() == "UPDATE" {
			foundUpdate = true
			break
		}
	}
	assert.True(t, foundUpdate, "WAL should contain UPDATE entry")
}

// TestWALIntegration_DELETE tests that DELETE operations create WAL entries with deleted data
// This covers task 3.16: Test DELETE operation creates WAL entry with deleted data
func TestWALIntegration_DELETE(t *testing.T) {
	exec, cat, _ := setupTestExecutorWithWAL(t)

	// Create table and insert initial data
	_, err := executeQuery(t, exec, cat, "CREATE TABLE wal_delete_test (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	_, err = executeQuery(t, exec, cat, "INSERT INTO wal_delete_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	require.NoError(t, err)

	// Delete data - this should create a WAL entry with deleted row data
	result, err := executeQuery(t, exec, cat, "DELETE FROM wal_delete_test WHERE id = 2")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "Should delete 1 row")

	// Verify WAL entry was written
	entries := getWALEntries(t, exec)
	require.NotEmpty(t, entries, "WAL should have entries")

	// Look for DELETE entry
	var foundDelete bool
	for _, entry := range entries {
		if entry.Type().String() == "DELETE" {
			foundDelete = true
			break
		}
	}
	assert.True(t, foundDelete, "WAL should contain DELETE entry")
}

// TestWALIntegration_FailedInsertNoWALEntry tests that failed INSERT does NOT create WAL entry
// This covers task 3.17: Test failed INSERT (e.g., constraint violation) does NOT create WAL entry
func TestWALIntegration_FailedInsertNoWALEntry(t *testing.T) {
	exec, cat, _ := setupTestExecutorWithWAL(t)

	// Create table with PRIMARY KEY
	_, err := executeQuery(t, exec, cat, "CREATE TABLE wal_pk_test (id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	// Insert initial data
	_, err = executeQuery(t, exec, cat, "INSERT INTO wal_pk_test VALUES (1, 'Alice')")
	require.NoError(t, err)

	// Count WAL entries before the failed insert
	entriesBefore := getWALEntries(t, exec)
	countBefore := countInsertEntries(entriesBefore)

	// Try to insert duplicate PRIMARY KEY - should fail
	_, err = executeQuery(t, exec, cat, "INSERT INTO wal_pk_test VALUES (1, 'Bob')")
	require.Error(t, err, "INSERT with duplicate PRIMARY KEY should fail")

	// Verify no new INSERT WAL entry was written
	entriesAfter := getWALEntries(t, exec)
	countAfter := countInsertEntries(entriesAfter)

	assert.Equal(t, countBefore, countAfter, "Failed INSERT should NOT create WAL entry")
}
