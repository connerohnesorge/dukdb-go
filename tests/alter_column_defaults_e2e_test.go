package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestAlterColumnSetDefault(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t1 ALTER COLUMN b SET DEFAULT 'hello'")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t1 (a) VALUES (1)")
	require.NoError(t, err)

	var b sql.NullString
	err = db.QueryRow("SELECT b FROM t1 WHERE a = 1").Scan(&b)
	require.NoError(t, err)
	require.True(t, b.Valid)
	require.Equal(t, "hello", b.String)
}

func TestAlterColumnDropDefault(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t2 (a INTEGER, b TEXT DEFAULT 'old')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t2 ALTER COLUMN b DROP DEFAULT")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t2 (a) VALUES (1)")
	require.NoError(t, err)

	var b sql.NullString
	err = db.QueryRow("SELECT b FROM t2 WHERE a = 1").Scan(&b)
	require.NoError(t, err)
	require.False(t, b.Valid) // Should be NULL (no default)
}

func TestAlterColumnSetNotNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t3 (a INTEGER, b TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t3 VALUES (1, 'x')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t3 ALTER COLUMN b SET NOT NULL")
	require.NoError(t, err)

	// Now inserting NULL should fail
	_, err = db.Exec("INSERT INTO t3 VALUES (2, NULL)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT NULL")
}

func TestAlterColumnSetNotNullWithExistingNulls(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t4 (a INTEGER, b TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t4 VALUES (1, NULL)")
	require.NoError(t, err)

	// Should fail because column has NULL values
	_, err = db.Exec("ALTER TABLE t4 ALTER COLUMN b SET NOT NULL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NULL")
}

func TestAlterColumnDropNotNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t5 (a INTEGER, b TEXT NOT NULL)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t5 ALTER COLUMN b DROP NOT NULL")
	require.NoError(t, err)

	// Now inserting NULL should work
	_, err = db.Exec("INSERT INTO t5 VALUES (1, NULL)")
	require.NoError(t, err)

	var b sql.NullString
	err = db.QueryRow("SELECT b FROM t5 WHERE a = 1").Scan(&b)
	require.NoError(t, err)
	require.False(t, b.Valid)
}

func TestAlterColumnTypeStillWorks(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t6 (a INTEGER, b VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t6 VALUES (1, '42')")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t6 ALTER COLUMN b TYPE INTEGER")
	require.NoError(t, err)
}

func TestAlterColumnSetDefaultInteger(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t7 (a INTEGER, b INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TABLE t7 ALTER COLUMN b SET DEFAULT 42")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t7 (a) VALUES (1)")
	require.NoError(t, err)

	var b int
	err = db.QueryRow("SELECT b FROM t7 WHERE a = 1").Scan(&b)
	require.NoError(t, err)
	require.Equal(t, 42, b)
}
