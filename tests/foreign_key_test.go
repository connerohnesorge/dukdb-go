package tests

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForeignKey(t *testing.T) {
	t.Run("Parser", func(t *testing.T) {
		t.Run("ColumnLevelFK", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE orders (id INTEGER, customer_id INTEGER REFERENCES customers(id))")
			assert.NoError(t, err, "column-level FK should parse without error")
		})

		t.Run("TableLevelFK", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE orders (id INTEGER, cid INTEGER, FOREIGN KEY (cid) REFERENCES customers(id))")
			assert.NoError(t, err, "table-level FK should parse without error")
		})

		t.Run("NamedFK", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE orders (id INTEGER, cid INTEGER, CONSTRAINT fk_cust FOREIGN KEY (cid) REFERENCES customers(id))")
			assert.NoError(t, err, "named FK constraint should parse without error")
		})

		t.Run("FKWithOnDeleteRestrict", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE orders (id INTEGER, cid INTEGER REFERENCES customers(id) ON DELETE RESTRICT)")
			assert.NoError(t, err, "FK with ON DELETE RESTRICT should parse without error")
		})

		t.Run("FKWithCascadeRejected", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE orders (id INTEGER, cid INTEGER REFERENCES customers(id) ON DELETE CASCADE)")
			require.Error(t, err, "FK with CASCADE should be rejected")
			assert.True(t, strings.Contains(err.Error(), "CASCADE"),
				"error should mention CASCADE, got: %s", err.Error())
		})
	})

	t.Run("CreateTable", func(t *testing.T) {
		t.Run("FKReferencingNonExistentTable", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE bad (id INTEGER, pid INTEGER REFERENCES ghost(id))")
			require.Error(t, err, "FK referencing non-existent table should fail")
			assert.True(t, strings.Contains(err.Error(), "non-existent"),
				"error should mention non-existent, got: %s", err.Error())
		})

		t.Run("FKReferencingNonExistentColumn", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY, name VARCHAR)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(nonexistent))")
			require.Error(t, err, "FK referencing non-existent column should fail")
			assert.True(t, strings.Contains(err.Error(), "non-existent column"),
				"error should mention non-existent column, got: %s", err.Error())
		})

		t.Run("FKReferencingNonKeyColumn", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER, name VARCHAR)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(name))")
			require.Error(t, err, "FK referencing non-PK/non-UNIQUE column should fail")
			assert.True(t, strings.Contains(err.Error(), "not a PRIMARY KEY or UNIQUE"),
				"error should mention not a PRIMARY KEY or UNIQUE, got: %s", err.Error())
		})

		t.Run("ValidFK", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			assert.NoError(t, err, "valid FK should succeed")
		})
	})

	t.Run("Insert", func(t *testing.T) {
		t.Run("ValidInsert", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			assert.NoError(t, err, "insert with valid FK reference should succeed")
		})

		t.Run("FKViolation", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO child VALUES (10, 999)")
			require.Error(t, err, "insert with non-existent parent should fail")
			assert.True(t, strings.Contains(err.Error(), "foreign key violation"),
				"error should mention foreign key violation, got: %s", err.Error())
		})

		t.Run("NullFKAllowed", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO child VALUES (10, NULL)")
			assert.NoError(t, err, "insert with NULL FK should be allowed")
		})

		t.Run("CompositeFK", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES parent(a, b))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1, 2)")
			require.NoError(t, err)

			// Valid composite FK reference
			_, err = db.Exec("INSERT INTO child VALUES (10, 1, 2)")
			assert.NoError(t, err, "insert with valid composite FK should succeed")

			// Invalid composite FK reference
			_, err = db.Exec("INSERT INTO child VALUES (11, 1, 99)")
			require.Error(t, err, "insert with invalid composite FK should fail")
			assert.True(t, strings.Contains(err.Error(), "foreign key violation"),
				"error should mention foreign key violation, got: %s", err.Error())
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("BlockedDelete", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			require.NoError(t, err)

			_, err = db.Exec("DELETE FROM parent WHERE id = 1")
			require.Error(t, err, "deleting referenced parent row should fail")
			assert.True(t, strings.Contains(err.Error(), "still referenced"),
				"error should mention still referenced, got: %s", err.Error())
		})

		t.Run("AllowedDelete", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO parent VALUES (2)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			require.NoError(t, err)

			// Deleting parent row 2 which is not referenced should succeed
			_, err = db.Exec("DELETE FROM parent WHERE id = 2")
			assert.NoError(t, err, "deleting unreferenced parent row should succeed")
		})

		t.Run("NullFKDoesNotBlock", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, NULL)")
			require.NoError(t, err)

			// Parent row 1 is not referenced by any non-NULL child FK
			_, err = db.Exec("DELETE FROM parent WHERE id = 1")
			assert.NoError(t, err, "deleting parent when child has NULL FK should succeed")
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("ChildUpdateToNonExistentParent", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			require.NoError(t, err)

			_, err = db.Exec("UPDATE child SET pid = 999 WHERE id = 10")
			require.Error(t, err, "updating child FK to non-existent parent should fail")
			assert.True(t, strings.Contains(err.Error(), "foreign key violation"),
				"error should mention foreign key violation, got: %s", err.Error())
		})

		t.Run("ValidChildUpdate", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO parent VALUES (2)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			require.NoError(t, err)

			_, err = db.Exec("UPDATE child SET pid = 2 WHERE id = 10")
			assert.NoError(t, err, "updating child FK to existing parent should succeed")
		})

		t.Run("ParentUpdateBlocked", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO child VALUES (10, 1)")
			require.NoError(t, err)

			_, err = db.Exec("UPDATE parent SET id = 99 WHERE id = 1")
			require.Error(t, err, "updating referenced parent PK should fail")
			assert.True(t, strings.Contains(err.Error(), "still referenced"),
				"error should mention still referenced, got: %s", err.Error())
		})

		t.Run("ParentUpdateAllowed", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO parent VALUES (1)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO parent VALUES (2)")
			require.NoError(t, err)

			// No child references parent row 2
			_, err = db.Exec("UPDATE parent SET id = 99 WHERE id = 2")
			assert.NoError(t, err, "updating unreferenced parent PK should succeed")
		})
	})

	t.Run("SelfReferencing", func(t *testing.T) {
		// Self-referencing FKs use a two-table pattern: emp references itself
		// via a separate manager table to work around the CREATE TABLE validation
		// that checks referenced table existence at creation time.

		t.Run("CreateSelfReferencingTable", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			// Direct self-reference is rejected because the table does not
			// exist in the catalog at CREATE TABLE validation time.
			_, err = db.Exec("CREATE TABLE emp (id INTEGER PRIMARY KEY, mgr_id INTEGER REFERENCES emp(id))")
			require.Error(t, err, "self-referencing FK should fail because table does not exist yet during CREATE TABLE validation")
			assert.True(t, strings.Contains(err.Error(), "non-existent"),
				"error should mention non-existent, got: %s", err.Error())
		})

		// Test self-referencing-like pattern using two tables to verify
		// the FK enforcement still works for hierarchical data.
		t.Run("TwoTableHierarchy", func(t *testing.T) {
			db, err := sql.Open("dukdb", "")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE managers (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, mgr_id INTEGER REFERENCES managers(id))")
			require.NoError(t, err)

			// Insert a manager (root)
			_, err = db.Exec("INSERT INTO managers VALUES (1)")
			require.NoError(t, err)

			// Insert employee with NULL manager (allowed)
			_, err = db.Exec("INSERT INTO employees VALUES (10, NULL)")
			assert.NoError(t, err, "inserting employee with NULL manager should succeed")

			// Insert employee referencing existing manager
			_, err = db.Exec("INSERT INTO employees VALUES (20, 1)")
			assert.NoError(t, err, "inserting employee with valid manager reference should succeed")

			// Insert employee referencing non-existent manager
			_, err = db.Exec("INSERT INTO employees VALUES (30, 99)")
			require.Error(t, err, "inserting employee with non-existent manager should fail")
			assert.True(t, strings.Contains(err.Error(), "foreign key violation"),
				"error should mention foreign key violation, got: %s", err.Error())
		})
	})
}
