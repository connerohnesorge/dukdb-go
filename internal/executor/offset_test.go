package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOffsetWithoutLimit tests OFFSET clause without LIMIT
func TestOffsetWithoutLimit(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(t, exec, cat, "CREATE TABLE t (id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Insert rows
	for i := 1; i <= 10; i++ {
		_, err := executeQuery(
			t,
			exec,
			cat,
			fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d')", i, i),
		)
		require.NoError(t, err)
	}

	t.Run("OFFSET 5 without LIMIT", func(t *testing.T) {
		result, err := executeQuery(t, exec, cat, "SELECT * FROM t ORDER BY id OFFSET 5")
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows), "Expected 5 rows (10 total - 5 offset)")
		// First row should be id=6
		if len(result.Rows) > 0 {
			assert.Equal(t, int32(6), result.Rows[0]["id"])
		}
	})

	t.Run("OFFSET 0 returns all rows", func(t *testing.T) {
		result, err := executeQuery(t, exec, cat, "SELECT * FROM t ORDER BY id OFFSET 0")
		require.NoError(t, err)
		assert.Equal(t, 10, len(result.Rows), "Expected all 10 rows")
	})

	t.Run("OFFSET larger than row count returns empty", func(t *testing.T) {
		result, err := executeQuery(t, exec, cat, "SELECT * FROM t ORDER BY id OFFSET 100")
		require.NoError(t, err)
		assert.Equal(t, 0, len(result.Rows), "Expected 0 rows when offset > row count")
	})

	t.Run("LIMIT 3 OFFSET 2", func(t *testing.T) {
		result, err := executeQuery(t, exec, cat, "SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 2")
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "Expected 3 rows")
		// First row should be id=3 (rows 3, 4, 5)
		if len(result.Rows) > 0 {
			assert.Equal(t, int32(3), result.Rows[0]["id"])
		}
	})
}
