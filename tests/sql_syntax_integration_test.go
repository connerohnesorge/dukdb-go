package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLSyntaxIntegration exercises TRUNCATE TABLE, FETCH FIRST, and
// standalone VALUES together as an end-to-end integration suite.
func TestSQLSyntaxIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test 1: TRUNCATE basic -- CREATE TABLE, INSERT rows, TRUNCATE, verify empty
	t.Run("TruncateBasic", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE trunc_basic (id INTEGER, name VARCHAR)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO trunc_basic VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)
		require.NoError(t, err)

		// Verify rows exist before truncate
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM trunc_basic`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Truncate
		_, err = db.Exec(`TRUNCATE TABLE trunc_basic`)
		require.NoError(t, err)

		// Verify table is empty
		err = db.QueryRow(`SELECT COUNT(*) FROM trunc_basic`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Clean up
		_, err = db.Exec(`DROP TABLE trunc_basic`)
		require.NoError(t, err)
	})

	// Test 2: TRUNCATE on non-existent table returns an error
	t.Run("TruncateNonExistentTableErrors", func(t *testing.T) {
		_, err := db.Exec(`TRUNCATE TABLE no_such_table`)
		require.Error(t, err, "TRUNCATE on non-existent table should produce an error")
		assert.Contains(t, err.Error(), "not found")
	})

	// Test 3: TRUNCATE schema-qualified table
	t.Run("TruncateSchemaQualified", func(t *testing.T) {
		_, err := db.Exec(`CREATE SCHEMA test_schema`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE TABLE test_schema.trunc_sq (id INTEGER, val VARCHAR)`)
		require.NoError(t, err)

		_, err = db.Exec(
			`INSERT INTO test_schema.trunc_sq VALUES (1, 'x'), (2, 'y')`,
		)
		require.NoError(t, err)

		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM test_schema.trunc_sq`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		// Truncate using schema-qualified name
		_, err = db.Exec(`TRUNCATE TABLE test_schema.trunc_sq`)
		require.NoError(t, err)

		err = db.QueryRow(`SELECT COUNT(*) FROM test_schema.trunc_sq`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Clean up
		_, err = db.Exec(`DROP TABLE test_schema.trunc_sq`)
		require.NoError(t, err)
		_, err = db.Exec(`DROP SCHEMA test_schema`)
		require.NoError(t, err)
	})

	// Test 4: INSERT from VALUES subquery then TRUNCATE
	t.Run("InsertFromValuesThenTruncate", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE val_trunc (id INTEGER, label VARCHAR)`)
		require.NoError(t, err)

		// Insert using INSERT ... SELECT from VALUES subquery
		_, err = db.Exec(
			`INSERT INTO val_trunc SELECT * FROM (VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty')) AS t`,
		)
		require.NoError(t, err)

		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM val_trunc`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Verify data ordering
		rows, err := db.Query(`SELECT id, label FROM val_trunc ORDER BY id`)
		require.NoError(t, err)
		defer rows.Close()

		expected := []struct {
			id    int
			label string
		}{
			{10, "ten"},
			{20, "twenty"},
			{30, "thirty"},
		}

		idx := 0
		for rows.Next() {
			var id int
			var label string
			err = rows.Scan(&id, &label)
			require.NoError(t, err)
			assert.Equal(t, expected[idx].id, id)
			assert.Equal(t, expected[idx].label, label)
			idx++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 3, idx)

		// Truncate
		_, err = db.Exec(`TRUNCATE val_trunc`)
		require.NoError(t, err)

		err = db.QueryRow(`SELECT COUNT(*) FROM val_trunc`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Clean up
		_, err = db.Exec(`DROP TABLE val_trunc`)
		require.NoError(t, err)
	})

	// Test 5: VALUES with FETCH FIRST
	t.Run("ValuesWithFetchFirst", func(t *testing.T) {
		rows, err := db.Query(
			`SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t FETCH FIRST 2 ROWS ONLY`,
		)
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			col1 int64
			col2 string
		}
		var results []row
		for rows.Next() {
			var r row
			err = rows.Scan(&r.col1, &r.col2)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 2, len(results), "FETCH FIRST 2 ROWS ONLY should return exactly 2 rows")
		assert.Equal(t, int64(1), results[0].col1)
		assert.Equal(t, "a", results[0].col2)
		assert.Equal(t, int64(2), results[1].col1)
		assert.Equal(t, "b", results[1].col2)
	})

	// Test 6: TRUNCATE then re-insert from VALUES
	t.Run("TruncateThenReinsertFromValues", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE trunc_reinsert (id INTEGER, tag VARCHAR)`)
		require.NoError(t, err)

		// Initial insert
		_, err = db.Exec(`INSERT INTO trunc_reinsert VALUES (1, 'first'), (2, 'second')`)
		require.NoError(t, err)

		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM trunc_reinsert`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		// Truncate
		_, err = db.Exec(`TRUNCATE TABLE trunc_reinsert`)
		require.NoError(t, err)

		err = db.QueryRow(`SELECT COUNT(*) FROM trunc_reinsert`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Re-insert using VALUES subquery
		_, err = db.Exec(
			`INSERT INTO trunc_reinsert SELECT * FROM (VALUES (100, 'hundred'), (200, 'two-hundred')) AS v`,
		)
		require.NoError(t, err)

		err = db.QueryRow(`SELECT COUNT(*) FROM trunc_reinsert`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		// Verify new data
		rows, err := db.Query(`SELECT id, tag FROM trunc_reinsert ORDER BY id`)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id int
		var tag string
		err = rows.Scan(&id, &tag)
		require.NoError(t, err)
		assert.Equal(t, 100, id)
		assert.Equal(t, "hundred", tag)

		require.True(t, rows.Next())
		err = rows.Scan(&id, &tag)
		require.NoError(t, err)
		assert.Equal(t, 200, id)
		assert.Equal(t, "two-hundred", tag)

		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())

		// Clean up
		_, err = db.Exec(`DROP TABLE trunc_reinsert`)
		require.NoError(t, err)
	})

	// Test 7: FETCH FIRST on VALUES result with ORDER BY and WITH TIES
	t.Run("FetchFirstWithTiesOnValues", func(t *testing.T) {
		// Create a VALUES result set with duplicate ORDER BY values to
		// exercise WITH TIES. The values (score column) are:
		//   10, 20, 20, 30, 40
		// ORDER BY score, FETCH FIRST 2 ROWS WITH TIES should return 3
		// rows because the 2nd and 3rd rows tie on score=20.
		rows, err := db.Query(
			`SELECT * FROM (VALUES (1, 10), (2, 20), (3, 20), (4, 30), (5, 40)) AS t ORDER BY column2 FETCH FIRST 2 ROWS WITH TIES`,
		)
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			col1  int64
			score int64
		}
		var results []row
		for rows.Next() {
			var r row
			err = rows.Scan(&r.col1, &r.score)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// WITH TIES: row at position 2 has score=20, and row at position 3
		// also has score=20, so it must be included. Total = 3.
		require.Equal(t, 3, len(results), "WITH TIES should include tied rows beyond the limit boundary")
		assert.Equal(t, int64(10), results[0].score)
		assert.Equal(t, int64(20), results[1].score)
		assert.Equal(t, int64(20), results[2].score)
	})
}
