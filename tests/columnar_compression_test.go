package tests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnarCompression(t *testing.T) {
	t.Run("constant column values", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE const_test (id INT, status VARCHAR)")
		require.NoError(t, err)

		stmt, err := db.Prepare("INSERT INTO const_test VALUES (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		const rowCount = 5000
		for i := 0; i < rowCount; i++ {
			_, err = stmt.Exec(i, "active")
			require.NoError(t, err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM const_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, rowCount, count)

		// Verify all status values are correct
		var distinctCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT status) FROM const_test").Scan(&distinctCount)
		require.NoError(t, err)
		assert.Equal(t, 1, distinctCount)

		var status string
		err = db.QueryRow("SELECT status FROM const_test WHERE id = 2500").Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "active", status)
	})

	t.Run("dictionary column values", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE dict_test (id INT, category VARCHAR)")
		require.NoError(t, err)

		categories := []string{"electronics", "clothing", "food", "books", "toys"}

		stmt, err := db.Prepare("INSERT INTO dict_test VALUES (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		const rowCount = 5000
		for i := 0; i < rowCount; i++ {
			_, err = stmt.Exec(i, categories[i%len(categories)])
			require.NoError(t, err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM dict_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, rowCount, count)

		var distinctCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT category) FROM dict_test").Scan(&distinctCount)
		require.NoError(t, err)
		assert.Equal(t, len(categories), distinctCount)

		// Verify specific values based on the cycling pattern
		for i := 0; i < 5; i++ {
			var cat string
			err = db.QueryRow("SELECT category FROM dict_test WHERE id = ?", i).Scan(&cat)
			require.NoError(t, err)
			assert.Equal(t, categories[i%len(categories)], cat)
		}

		// Verify a value in the middle
		var midCat string
		err = db.QueryRow("SELECT category FROM dict_test WHERE id = 2503").Scan(&midCat)
		require.NoError(t, err)
		assert.Equal(t, categories[2503%len(categories)], midCat)
	})

	t.Run("mixed compression types", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE mixed_test (id INT, constant_col INT, dict_col VARCHAR, random_col INT)")
		require.NoError(t, err)

		dictValues := []string{"alpha", "beta", "gamma"}

		stmt, err := db.Prepare("INSERT INTO mixed_test VALUES (?, ?, ?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		const rowCount = 5000
		for i := 0; i < rowCount; i++ {
			_, err = stmt.Exec(i, 42, dictValues[i%len(dictValues)], i)
			require.NoError(t, err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM mixed_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, rowCount, count)

		// Verify constant column
		var minConst, maxConst int
		err = db.QueryRow("SELECT MIN(constant_col), MAX(constant_col) FROM mixed_test").Scan(&minConst, &maxConst)
		require.NoError(t, err)
		assert.Equal(t, 42, minConst)
		assert.Equal(t, 42, maxConst)

		// Verify dictionary column distinct count
		var distinctDict int
		err = db.QueryRow("SELECT COUNT(DISTINCT dict_col) FROM mixed_test").Scan(&distinctDict)
		require.NoError(t, err)
		assert.Equal(t, len(dictValues), distinctDict)

		// Verify random column preserves unique values
		var distinctRandom int
		err = db.QueryRow("SELECT COUNT(DISTINCT random_col) FROM mixed_test").Scan(&distinctRandom)
		require.NoError(t, err)
		assert.Equal(t, rowCount, distinctRandom)

		// Spot-check specific rows
		spotChecks := []int{0, 100, 2500, 4999}
		for _, id := range spotChecks {
			var gotID, gotConst, gotRandom int
			var gotDict string
			err = db.QueryRow("SELECT id, constant_col, dict_col, random_col FROM mixed_test WHERE id = ?", id).
				Scan(&gotID, &gotConst, &gotDict, &gotRandom)
			require.NoError(t, err, "failed for id=%d", id)
			assert.Equal(t, id, gotID)
			assert.Equal(t, 42, gotConst)
			assert.Equal(t, dictValues[id%len(dictValues)], gotDict)
			assert.Equal(t, id, gotRandom)
		}
	})

	t.Run("data survives compression round-trip", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE roundtrip_test (id INT, value VARCHAR)")
		require.NoError(t, err)

		// Use a transaction for the batch insert
		tx, err := db.Begin()
		require.NoError(t, err)

		stmt, err := tx.Prepare("INSERT INTO roundtrip_test VALUES (?, ?)")
		require.NoError(t, err)

		const rowCount = 5000
		for i := 0; i < rowCount; i++ {
			_, err = stmt.Exec(i, fmt.Sprintf("row-%d", i))
			require.NoError(t, err)
		}
		stmt.Close()

		err = tx.Commit()
		require.NoError(t, err)

		// Verify total count
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM roundtrip_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, rowCount, count)

		// Verify beginning
		var val string
		err = db.QueryRow("SELECT value FROM roundtrip_test WHERE id = 0").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "row-0", val)

		// Verify middle
		err = db.QueryRow("SELECT value FROM roundtrip_test WHERE id = 2500").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "row-2500", val)

		// Verify end
		err = db.QueryRow("SELECT value FROM roundtrip_test WHERE id = 4999").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "row-4999", val)

		// Verify ordering with LIMIT/OFFSET
		rows, err := db.Query("SELECT id, value FROM roundtrip_test ORDER BY id LIMIT 3")
		require.NoError(t, err)
		defer rows.Close()

		expectedIDs := []int{0, 1, 2}
		idx := 0
		for rows.Next() {
			var id int
			var v string
			err = rows.Scan(&id, &v)
			require.NoError(t, err)
			assert.Equal(t, expectedIDs[idx], id)
			assert.Equal(t, fmt.Sprintf("row-%d", expectedIDs[idx]), v)
			idx++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, len(expectedIDs), idx)
	})
}
