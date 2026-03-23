package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregatesRound3(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE agg_data(id INTEGER, val DOUBLE, flag BOOLEAN, grp VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO agg_data VALUES
		(1, 2.0, true, 'A'),
		(2, 3.0, false, 'A'),
		(3, 4.0, true, 'B'),
		(4, 5.0, true, 'B'),
		(5, NULL, NULL, 'A')`)
	require.NoError(t, err)

	// --- PRODUCT ---

	t.Run("ProductBasic", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT PRODUCT(val) FROM agg_data WHERE val IS NOT NULL`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 120.0, result)
	})

	t.Run("ProductWithNulls", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT PRODUCT(val) FROM agg_data`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 120.0, result)
	})

	t.Run("ProductEmpty", func(t *testing.T) {
		var result *float64
		err := db.QueryRow(`SELECT PRODUCT(val) FROM agg_data WHERE id > 100`).Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// --- MAD ---

	t.Run("MadBasic", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT MAD(val) FROM agg_data WHERE val IS NOT NULL`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 0.01)
	})

	t.Run("MadEmpty", func(t *testing.T) {
		var result *float64
		err := db.QueryRow(`SELECT MAD(val) FROM agg_data WHERE id > 100`).Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// --- FAVG ---

	t.Run("FavgBasic", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT FAVG(val) FROM agg_data WHERE val IS NOT NULL`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 3.5, result, 0.01)
	})

	// --- FSUM ---

	t.Run("FsumBasic", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT FSUM(val) FROM agg_data WHERE val IS NOT NULL`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 14.0, result)
	})

	t.Run("FsumEmpty", func(t *testing.T) {
		var result *float64
		err := db.QueryRow(`SELECT FSUM(val) FROM agg_data WHERE id > 100`).Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// --- BITSTRING_AGG ---

	t.Run("BitstringAggBasic", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT BITSTRING_AGG(flag) FROM agg_data WHERE flag IS NOT NULL`).Scan(&result)
		require.NoError(t, err)
		assert.Len(t, result, 4)
		for _, c := range result {
			assert.True(t, c == '0' || c == '1', "expected only '0' and '1' characters, got %c", c)
		}
	})

	// --- GROUP BY ---

	t.Run("ProductWithGroupBy", func(t *testing.T) {
		rows, err := db.Query(`SELECT grp, PRODUCT(val) FROM agg_data WHERE val IS NOT NULL GROUP BY grp ORDER BY grp`)
		require.NoError(t, err)
		defer rows.Close()

		expected := []struct {
			grp     string
			product float64
		}{
			{"A", 6.0},
			{"B", 20.0},
		}

		i := 0
		for rows.Next() {
			var grp string
			var product float64
			err := rows.Scan(&grp, &product)
			require.NoError(t, err)
			require.Less(t, i, len(expected))
			assert.Equal(t, expected[i].grp, grp)
			assert.Equal(t, expected[i].product, product)
			i++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, len(expected), i)
	})
}
