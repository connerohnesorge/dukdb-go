package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimilarTo(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test_similar (name VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_similar VALUES ('hello'), ('world'), ('help'), ('heap'), ('hero')`)
	require.NoError(t, err)

	t.Run("basic SIMILAR TO with percent wildcard", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name SIMILAR TO 'hel%' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"hello", "help"}, results)
	})

	t.Run("SIMILAR TO with underscore wildcard", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name SIMILAR TO 'he__' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"heap", "help", "hero"}, results)
	})

	t.Run("SIMILAR TO with alternation", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name SIMILAR TO '(hello|world)' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"hello", "world"}, results)
	})

	t.Run("SIMILAR TO with character class", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name SIMILAR TO 'he[al]%' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"heap", "hello", "help"}, results)
	})

	t.Run("NOT SIMILAR TO", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name NOT SIMILAR TO 'hel%' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"heap", "hero", "world"}, results)
	})

	t.Run("SIMILAR TO with negated character class", func(t *testing.T) {
		rows, err := db.Query(`SELECT name FROM test_similar WHERE name SIMILAR TO 'he[!l]%' ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			results = append(results, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"heap", "hero"}, results)
	})

	t.Run("SIMILAR TO with ESCAPE clause", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_escape (val VARCHAR)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO test_escape VALUES ('100%'), ('100 percent'), ('100x')`)
		require.NoError(t, err)

		rows, err := db.Query(`SELECT val FROM test_escape WHERE val SIMILAR TO '100#%' ESCAPE '#' ORDER BY val`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var val string
			require.NoError(t, rows.Scan(&val))
			results = append(results, val)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"100%"}, results)
	})

	t.Run("NOT SIMILAR TO with ESCAPE clause", func(t *testing.T) {
		rows, err := db.Query(`SELECT val FROM test_escape WHERE val NOT SIMILAR TO '100#%' ESCAPE '#' ORDER BY val`)
		require.NoError(t, err)
		defer rows.Close()

		var results []string
		for rows.Next() {
			var val string
			require.NoError(t, rows.Scan(&val))
			results = append(results, val)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"100 percent", "100x"}, results)
	})

	t.Run("SIMILAR TO empty pattern matches empty string", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT '' SIMILAR TO ''`).Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("SIMILAR TO exact match", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT 'abc' SIMILAR TO 'abc'`).Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("SIMILAR TO no match", func(t *testing.T) {
		var result bool
		err := db.QueryRow(`SELECT 'abc' SIMILAR TO 'def'`).Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})
}
