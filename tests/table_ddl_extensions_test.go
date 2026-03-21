package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableDDLExtensions(t *testing.T) {
	// -----------------------------------------------------------------------
	// CREATE OR REPLACE TABLE
	// -----------------------------------------------------------------------

	t.Run("CreateOrReplaceNewTable", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE OR REPLACE TABLE t1(id INTEGER, name VARCHAR)")
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO t1 VALUES (1, 'alice')")
		require.NoError(t, err)

		var id int
		var name string
		err = db.QueryRow("SELECT id, name FROM t1 WHERE id = 1").Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "alice", name)
	})

	t.Run("CreateOrReplaceExistingTable", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		// Create original table and insert data.
		_, err = db.Exec("CREATE TABLE t2(id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO t2 VALUES (42)")
		require.NoError(t, err)

		// Replace the table with a new schema.
		_, err = db.Exec("CREATE OR REPLACE TABLE t2(id INTEGER, name VARCHAR, age INTEGER)")
		require.NoError(t, err)

		// Old data should be gone.
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "old data should be gone after replace")

		// Verify new schema has 3 columns by inserting a full row.
		_, err = db.Exec("INSERT INTO t2 VALUES (1, 'bob', 30)")
		require.NoError(t, err)

		var id int
		var name string
		var age int
		err = db.QueryRow("SELECT id, name, age FROM t2").Scan(&id, &name, &age)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "bob", name)
		assert.Equal(t, 30, age)
	})

	t.Run("CreateOrReplaceIfNotExistsConflict", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE OR REPLACE TABLE IF NOT EXISTS t3(x INT)")
		require.Error(t, err, "should reject combining OR REPLACE with IF NOT EXISTS")
	})

	// -----------------------------------------------------------------------
	// CREATE TEMP TABLE
	// -----------------------------------------------------------------------

	t.Run("CreateTempTable", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TEMP TABLE tmp1(id INTEGER, val VARCHAR)")
		require.NoError(t, err)

		// Temp tables live in the "temp" schema; use qualified name for DML.
		_, err = db.Exec("INSERT INTO temp.tmp1 VALUES (1, 'hello')")
		require.NoError(t, err)

		var id int
		var val string
		err = db.QueryRow("SELECT id, val FROM temp.tmp1").Scan(&id, &val)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "hello", val)
	})

	t.Run("CreateTemporaryTable", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TEMPORARY TABLE tmp2(x INT)")
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO temp.tmp2 VALUES (99)")
		require.NoError(t, err)

		var x int
		err = db.QueryRow("SELECT x FROM temp.tmp2").Scan(&x)
		require.NoError(t, err)
		assert.Equal(t, 99, x)
	})

	t.Run("TempTableInTempSchema", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TEMP TABLE tmp3(id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO temp.tmp3 VALUES (7)")
		require.NoError(t, err)

		var id int
		err = db.QueryRow("SELECT id FROM temp.tmp3").Scan(&id)
		require.NoError(t, err)
		assert.Equal(t, 7, id)
	})

	// -----------------------------------------------------------------------
	// ALTER TABLE ADD CONSTRAINT
	// -----------------------------------------------------------------------

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t4(id INTEGER, name VARCHAR)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t4 ADD CONSTRAINT uq_name UNIQUE(name)")
		require.NoError(t, err)
	})

	t.Run("AddCheckConstraint", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t5(id INTEGER, age INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t5 ADD CHECK(age > 0)")
		require.NoError(t, err)
	})

	t.Run("AddForeignKeyConstraint", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
		require.NoError(t, err)

		_, err = db.Exec("CREATE TABLE child(id INTEGER, parent_id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE child ADD CONSTRAINT fk_parent FOREIGN KEY(parent_id) REFERENCES parent(id)")
		require.NoError(t, err)
	})

	t.Run("AddConstraintNonExistentColumn", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t6(id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t6 ADD CONSTRAINT uq UNIQUE(nonexistent)")
		require.Error(t, err)
	})

	t.Run("AddFKNonExistentRefTable", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t7(id INTEGER, ref_id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t7 ADD CONSTRAINT fk FOREIGN KEY(ref_id) REFERENCES ghost(id)")
		require.Error(t, err)
	})

	// -----------------------------------------------------------------------
	// ALTER TABLE DROP CONSTRAINT
	// -----------------------------------------------------------------------

	t.Run("DropConstraint", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t8(id INTEGER, name VARCHAR)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t8 ADD CONSTRAINT uq_n UNIQUE(name)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t8 DROP CONSTRAINT uq_n")
		require.NoError(t, err)
	})

	t.Run("DropConstraintNonExistent", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t9(id INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("ALTER TABLE t9 DROP CONSTRAINT no_such")
		require.Error(t, err)
	})

	t.Run("DropConstraintIfExists", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE t10(id INTEGER)")
		require.NoError(t, err)

		// The parser accepts IF EXISTS after the constraint name:
		// ALTER TABLE t10 DROP CONSTRAINT no_such IF EXISTS
		_, err = db.Exec("ALTER TABLE t10 DROP CONSTRAINT no_such IF EXISTS")
		require.NoError(t, err, "IF EXISTS should suppress error for missing constraint")
	})
}
