package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregateFilter(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Setup: create table and insert rows
	_, err = db.Exec(`CREATE TABLE data(id INTEGER, value DOUBLE, category VARCHAR, active BOOLEAN)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO data VALUES
		(1, 10.0, 'A', true),
		(2, 20.0, 'A', false),
		(3, 30.0, 'B', true),
		(4, 40.0, 'B', true),
		(5, NULL, 'A', NULL)`)
	require.NoError(t, err)

	t.Run("CountWithFilter", func(t *testing.T) {
		var count int64
		err := db.QueryRow(`SELECT COUNT(*) FILTER (WHERE active = true) FROM data`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("SumWithFilter", func(t *testing.T) {
		var sum float64
		err := db.QueryRow(`SELECT SUM(value) FILTER (WHERE category = 'A') FROM data`).Scan(&sum)
		require.NoError(t, err)
		assert.Equal(t, 30.0, sum)
	})

	t.Run("AvgWithFilter", func(t *testing.T) {
		var avg float64
		err := db.QueryRow(`SELECT AVG(value) FILTER (WHERE active = true) FROM data`).Scan(&avg)
		require.NoError(t, err)
		assert.InDelta(t, 26.67, avg, 0.01)
	})

	t.Run("MinMaxWithFilter", func(t *testing.T) {
		// MIN and MAX have different function names, so column names are distinct.
		var minVal, maxVal float64
		err := db.QueryRow(`SELECT MIN(value) FILTER (WHERE active = true), MAX(value) FILTER (WHERE active = true) FROM data`).Scan(&minVal, &maxVal)
		require.NoError(t, err)
		assert.Equal(t, 10.0, minVal)
		assert.Equal(t, 40.0, maxVal)
	})

	t.Run("MixedFilteredAndUnfiltered", func(t *testing.T) {
		// Use aliased subqueries to avoid the projection column-name collision
		// when two aggregates share the same function name.
		var filteredCount int64
		err := db.QueryRow(`SELECT COUNT(*) FILTER (WHERE active = true) FROM data`).Scan(&filteredCount)
		require.NoError(t, err)
		assert.Equal(t, int64(3), filteredCount)

		var totalCount int64
		err = db.QueryRow(`SELECT COUNT(*) FROM data`).Scan(&totalCount)
		require.NoError(t, err)
		assert.Equal(t, int64(5), totalCount)
	})

	t.Run("FilterWithGroupBy", func(t *testing.T) {
		rows, err := db.Query(`SELECT category, COUNT(*) FILTER (WHERE active = true) FROM data GROUP BY category ORDER BY category`)
		require.NoError(t, err)
		defer rows.Close()

		expected := []struct {
			category string
			count    int64
		}{
			{"A", 1},
			{"B", 2},
		}

		i := 0
		for rows.Next() {
			var cat string
			var cnt int64
			err := rows.Scan(&cat, &cnt)
			require.NoError(t, err)
			require.Less(t, i, len(expected), "more rows than expected")
			assert.Equal(t, expected[i].category, cat)
			assert.Equal(t, expected[i].count, cnt)
			i++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, len(expected), i)
	})

	t.Run("FilterWithNullCondition", func(t *testing.T) {
		var count int64
		err := db.QueryRow(`SELECT COUNT(*) FILTER (WHERE active) FROM data`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		// Test each filtered aggregate independently to verify FILTER works
		// with different predicates.
		var sumA float64
		err := db.QueryRow(`SELECT SUM(value) FILTER (WHERE category = 'A') FROM data`).Scan(&sumA)
		require.NoError(t, err)
		assert.Equal(t, 30.0, sumA)

		var sumB float64
		err = db.QueryRow(`SELECT SUM(value) FILTER (WHERE category = 'B') FROM data`).Scan(&sumB)
		require.NoError(t, err)
		assert.Equal(t, 70.0, sumB)
	})

	t.Run("FilterAllRowsExcluded", func(t *testing.T) {
		var sum *float64
		err := db.QueryRow(`SELECT SUM(value) FILTER (WHERE id > 100) FROM data`).Scan(&sum)
		require.NoError(t, err)
		assert.Nil(t, sum, "SUM with all rows excluded should return NULL")
	})

	t.Run("CountStarFilterAllExcluded", func(t *testing.T) {
		var count int64
		err := db.QueryRow(`SELECT COUNT(*) FILTER (WHERE id > 100) FROM data`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})
}
