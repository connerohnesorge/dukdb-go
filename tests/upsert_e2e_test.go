package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpsertDoNothing verifies that ON CONFLICT DO NOTHING skips conflicting
// rows, leaving the original row intact.
func TestUpsertDoNothing(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_test(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_test VALUES (1, 'alice')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_test VALUES (1, 'bob') ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM upsert_test WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name, "ON CONFLICT DO NOTHING should keep the original row")
}

// TestUpsertDoUpdate verifies that ON CONFLICT DO UPDATE SET updates the
// conflicting row with the new value.
func TestUpsertDoUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_upd(id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_upd VALUES (1, 10)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_upd VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val")
	require.NoError(t, err)

	var val int64
	err = db.QueryRow("SELECT val FROM upsert_upd WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, int64(20), val, "ON CONFLICT DO UPDATE should replace val with EXCLUDED.val")
}

// TestUpsertDoNothingNonConflict verifies that non-conflicting rows are
// inserted normally even when ON CONFLICT DO NOTHING is specified.
func TestUpsertDoNothingNonConflict(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_mix(id INTEGER PRIMARY KEY, name VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_mix VALUES (1, 'alice')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_mix VALUES (1, 'bob'), (2, 'charlie') ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM upsert_mix").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "should have 2 rows total")

	var name string
	err = db.QueryRow("SELECT name FROM upsert_mix WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "charlie", name, "non-conflicting row should be inserted")
}

// TestUpsertDoUpdateExcluded verifies that EXCLUDED references the incoming
// values and can be used in expressions with the existing row.
func TestUpsertDoUpdateExcluded(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_exc(id INTEGER PRIMARY KEY, cnt INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_exc VALUES (1, 5)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_exc VALUES (1, 3) ON CONFLICT (id) DO UPDATE SET cnt = cnt + EXCLUDED.cnt")
	require.NoError(t, err)

	var cnt int64
	err = db.QueryRow("SELECT cnt FROM upsert_exc WHERE id = 1").Scan(&cnt)
	require.NoError(t, err)
	assert.Equal(t, int64(8), cnt, "cnt should be 5 + 3 = 8")
}

// TestUpsertMultipleRows verifies that inserting multiple rows with a mix of
// conflicts and new rows works correctly with DO UPDATE.
func TestUpsertMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_multi(id INTEGER PRIMARY KEY, val VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_multi VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_multi VALUES (1, 'x'), (2, 'y'), (3, 'z') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val")
	require.NoError(t, err)

	rows, err := db.Query("SELECT val FROM upsert_multi ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"x", "y", "z"}, vals, "all rows should reflect upserted values")
}

// TestUpsertDoNothingEmptyTarget verifies that DO NOTHING without an explicit
// conflict target infers the primary key constraint.
func TestUpsertDoNothingEmptyTarget(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upsert_infer(id INTEGER PRIMARY KEY, data VARCHAR)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_infer VALUES (1, 'original')")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO upsert_infer VALUES (1, 'duplicate') ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	var data string
	err = db.QueryRow("SELECT data FROM upsert_infer WHERE id = 1").Scan(&data)
	require.NoError(t, err)
	assert.Equal(t, "original", data, "DO NOTHING should keep the original row")
}
