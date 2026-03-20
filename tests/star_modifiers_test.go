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

func TestStarModifiers(t *testing.T) {
	t.Run("Exclude", func(t *testing.T) {
		t.Run("single column exclude", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT, c INT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 2, 3)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * EXCLUDE(b) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "c"}, cols)

			require.True(t, rows.Next())
			var a, c int
			err = rows.Scan(&a, &c)
			require.NoError(t, err)
			assert.Equal(t, 1, a)
			assert.Equal(t, 3, c)
			assert.False(t, rows.Next())
		})

		t.Run("multiple columns exclude", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT, c INT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 2, 3)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * EXCLUDE(a, c) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"b"}, cols)

			require.True(t, rows.Next())
			var b int
			err = rows.Scan(&b)
			require.NoError(t, err)
			assert.Equal(t, 2, b)
			assert.False(t, rows.Next())
		})

		t.Run("table-qualified exclude", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT, c INT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 2, 3)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT t.* EXCLUDE(b) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "c"}, cols)

			require.True(t, rows.Next())
			var a, c int
			err = rows.Scan(&a, &c)
			require.NoError(t, err)
			assert.Equal(t, 1, a)
			assert.Equal(t, 3, c)
			assert.False(t, rows.Next())
		})

		t.Run("error exclude non-existent column", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT, c INT)")
			require.NoError(t, err)

			_, err = db.Query("SELECT * EXCLUDE(nonexistent) FROM t")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	})

	t.Run("Replace", func(t *testing.T) {
		t.Run("single replacement with function", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b VARCHAR)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 'hello')")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * REPLACE(UPPER(b) AS b) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, cols)

			require.True(t, rows.Next())
			var a int
			var b string
			err = rows.Scan(&a, &b)
			require.NoError(t, err)
			assert.Equal(t, 1, a)
			assert.Equal(t, "HELLO", b)
			assert.False(t, rows.Next())
		})

		t.Run("expression replacement", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 2)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * REPLACE(a + 10 AS a) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, cols)

			require.True(t, rows.Next())
			var a, b int
			err = rows.Scan(&a, &b)
			require.NoError(t, err)
			assert.Equal(t, 11, a)
			assert.Equal(t, 2, b)
			assert.False(t, rows.Next())
		})

		t.Run("multiple replacements", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (1, 2)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * REPLACE(a * 2 AS a, b * 3 AS b) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, cols)

			require.True(t, rows.Next())
			var a, b int
			err = rows.Scan(&a, &b)
			require.NoError(t, err)
			assert.Equal(t, 2, a)
			assert.Equal(t, 6, b)
			assert.False(t, rows.Next())
		})
	})

	t.Run("Columns", func(t *testing.T) {
		t.Run("pattern match", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(price_usd INT, price_eur INT, name VARCHAR)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (100, 90, 'item')")
			require.NoError(t, err)

			rows, err := db.Query("SELECT COLUMNS('price_.*') FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"price_usd", "price_eur"}, cols)

			require.True(t, rows.Next())
			var usd, eur int
			err = rows.Scan(&usd, &eur)
			require.NoError(t, err)
			assert.Equal(t, 100, usd)
			assert.Equal(t, 90, eur)
			assert.False(t, rows.Next())
		})

		t.Run("match all columns", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(price_usd INT, price_eur INT, name VARCHAR)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO t VALUES (100, 90, 'item')")
			require.NoError(t, err)

			rows, err := db.Query("SELECT COLUMNS('.*') FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"price_usd", "price_eur", "name"}, cols)

			require.True(t, rows.Next())
			var usd, eur int
			var name string
			err = rows.Scan(&usd, &eur, &name)
			require.NoError(t, err)
			assert.Equal(t, 100, usd)
			assert.Equal(t, 90, eur)
			assert.Equal(t, "item", name)
			assert.False(t, rows.Next())
		})

		t.Run("error no match", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT)")
			require.NoError(t, err)

			_, err = db.Query("SELECT COLUMNS('xyz_.*') FROM t")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "matched no columns")
		})

		t.Run("error invalid regex", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT)")
			require.NoError(t, err)

			_, err = db.Query("SELECT COLUMNS('[invalid') FROM t")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid regex")
		})
	})

	t.Run("EdgeCases", func(t *testing.T) {
		t.Run("empty table with exclude", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE t(a INT, b INT)")
			require.NoError(t, err)

			rows, err := db.Query("SELECT * EXCLUDE(a) FROM t")
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"b"}, cols)

			assert.False(t, rows.Next())
		})
	})
}
