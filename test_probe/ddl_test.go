package test_probe

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestDDLAndSchema(t *testing.T) {

	// -----------------------------------------------------------------------
	// 1. CREATE TABLE with various column types
	// -----------------------------------------------------------------------
	t.Run("CreateTableVariousTypes", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE type_zoo (
			col_integer    INTEGER,
			col_bigint     BIGINT,
			col_smallint   SMALLINT,
			col_tinyint    TINYINT,
			col_float      FLOAT,
			col_double     DOUBLE,
			col_boolean    BOOLEAN,
			col_varchar    VARCHAR,
			col_text       TEXT,
			col_blob       BLOB,
			col_date       DATE,
			col_time       TIME,
			col_timestamp  TIMESTAMP,
			col_interval   INTERVAL,
			col_decimal    DECIMAL(18,4),
			col_hugeint    HUGEINT,
			col_uuid       UUID
		)`)
		logResult(t, "CREATE TABLE type_zoo", err)

		// Insert a row to verify the table is usable.
		_, err = db.Exec(`INSERT INTO type_zoo (col_integer, col_bigint, col_smallint, col_tinyint,
			col_float, col_double, col_boolean, col_varchar, col_text, col_blob,
			col_date, col_time, col_timestamp, col_interval, col_decimal, col_hugeint, col_uuid)
			VALUES (1, 9223372036854775807, 32000, 127,
			3.14, 2.718281828, true, 'hello', 'world', '\x0102',
			'2024-01-15', '13:45:00', '2024-01-15 13:45:00', INTERVAL '1 day',
			12345.6789, '9999999999999999999'::HUGEINT,
			'550e8400-e29b-41d4-a716-446655440000')`)
		logResult(t, "INSERT into type_zoo", err)

		var cnt int
		err = db.QueryRow("SELECT COUNT(*) FROM type_zoo").Scan(&cnt)
		logResult(t, "SELECT COUNT from type_zoo", err)
		if err == nil {
			t.Logf("  row count = %d", cnt)
		}
	})

	// -----------------------------------------------------------------------
	// 2. CREATE TABLE with PRIMARY KEY (single and composite)
	// -----------------------------------------------------------------------
	t.Run("PrimaryKeySingle", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE pk_single (id INTEGER PRIMARY KEY, name VARCHAR)`)
		logResult(t, "CREATE TABLE pk_single", err)

		_, err = db.Exec(`INSERT INTO pk_single VALUES (1, 'alice')`)
		logResult(t, "INSERT pk_single", err)

		// Duplicate PK should fail.
		_, err = db.Exec(`INSERT INTO pk_single VALUES (1, 'bob')`)
		if err != nil {
			t.Logf("[ OK ] duplicate PK correctly rejected: %v", err)
		} else {
			t.Log("[FAIL] duplicate PK was NOT rejected")
		}
	})

	t.Run("PrimaryKeyComposite", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE pk_composite (a INTEGER, b INTEGER, name VARCHAR, PRIMARY KEY(a, b))`)
		logResult(t, "CREATE TABLE pk_composite", err)

		_, err = db.Exec(`INSERT INTO pk_composite VALUES (1, 1, 'first')`)
		logResult(t, "INSERT pk_composite (1,1)", err)

		_, err = db.Exec(`INSERT INTO pk_composite VALUES (1, 2, 'second')`)
		logResult(t, "INSERT pk_composite (1,2)", err)

		// Duplicate composite PK should fail.
		_, err = db.Exec(`INSERT INTO pk_composite VALUES (1, 1, 'dup')`)
		if err != nil {
			t.Logf("[ OK ] duplicate composite PK correctly rejected: %v", err)
		} else {
			t.Log("[FAIL] duplicate composite PK was NOT rejected")
		}
	})

	// -----------------------------------------------------------------------
	// 3. CREATE TABLE with NOT NULL constraints
	// -----------------------------------------------------------------------
	t.Run("NotNullConstraint", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE notnull_tbl (id INTEGER NOT NULL, name VARCHAR NOT NULL)`)
		logResult(t, "CREATE TABLE notnull_tbl", err)

		_, err = db.Exec(`INSERT INTO notnull_tbl VALUES (1, 'ok')`)
		logResult(t, "INSERT valid row", err)

		_, err = db.Exec(`INSERT INTO notnull_tbl VALUES (NULL, 'bad')`)
		if err != nil {
			t.Logf("[ OK ] NULL correctly rejected for NOT NULL column: %v", err)
		} else {
			t.Log("[FAIL] NULL was NOT rejected for NOT NULL column")
		}
	})

	// -----------------------------------------------------------------------
	// 4. CREATE TABLE with DEFAULT values
	// -----------------------------------------------------------------------
	t.Run("DefaultValues", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE defaults_tbl (
			id INTEGER,
			status VARCHAR DEFAULT 'active',
			score INTEGER DEFAULT 100,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`)
		logResult(t, "CREATE TABLE defaults_tbl", err)

		_, err = db.Exec(`INSERT INTO defaults_tbl (id) VALUES (1)`)
		logResult(t, "INSERT with defaults", err)

		var status string
		var score int
		err = db.QueryRow("SELECT status, score FROM defaults_tbl WHERE id = 1").Scan(&status, &score)
		logResult(t, "SELECT defaults", err)
		if err == nil {
			t.Logf("  status=%q, score=%d", status, score)
		}
	})

	// -----------------------------------------------------------------------
	// 5. CREATE TABLE with CHECK constraints
	// -----------------------------------------------------------------------
	t.Run("CheckConstraint", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE check_tbl (id INTEGER, age INTEGER CHECK (age >= 0))`)
		logResult(t, "CREATE TABLE check_tbl", err)

		_, err = db.Exec(`INSERT INTO check_tbl VALUES (1, 25)`)
		logResult(t, "INSERT valid check", err)

		_, err = db.Exec(`INSERT INTO check_tbl VALUES (2, -1)`)
		if err != nil {
			t.Logf("[ OK ] CHECK constraint correctly rejected negative age: %v", err)
		} else {
			t.Log("[FAIL] CHECK constraint did NOT reject negative age")
		}
	})

	// -----------------------------------------------------------------------
	// 6. CREATE TABLE with UNIQUE constraints
	// -----------------------------------------------------------------------
	t.Run("UniqueConstraint", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE unique_tbl (id INTEGER, email VARCHAR UNIQUE)`)
		logResult(t, "CREATE TABLE unique_tbl", err)

		_, err = db.Exec(`INSERT INTO unique_tbl VALUES (1, 'a@b.com')`)
		logResult(t, "INSERT unique first", err)

		_, err = db.Exec(`INSERT INTO unique_tbl VALUES (2, 'a@b.com')`)
		if err != nil {
			t.Logf("[ OK ] UNIQUE constraint correctly rejected duplicate: %v", err)
		} else {
			t.Log("[FAIL] UNIQUE constraint did NOT reject duplicate")
		}
	})

	// -----------------------------------------------------------------------
	// 7. CREATE TABLE IF NOT EXISTS
	// -----------------------------------------------------------------------
	t.Run("CreateTableIfNotExists", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE ine_tbl (id INTEGER)`)
		logResult(t, "CREATE TABLE ine_tbl (first)", err)

		// Should not error.
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS ine_tbl (id INTEGER)`)
		logResult(t, "CREATE TABLE IF NOT EXISTS ine_tbl (second)", err)

		// Without IF NOT EXISTS, should error.
		_, err = db.Exec(`CREATE TABLE ine_tbl (id INTEGER)`)
		if err != nil {
			t.Logf("[ OK ] duplicate CREATE TABLE correctly rejected: %v", err)
		} else {
			t.Log("[FAIL] duplicate CREATE TABLE was NOT rejected")
		}
	})

	// -----------------------------------------------------------------------
	// 8. DROP TABLE and DROP TABLE IF EXISTS
	// -----------------------------------------------------------------------
	t.Run("DropTable", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE drop_me (id INTEGER)`)
		logResult(t, "CREATE TABLE drop_me", err)

		_, err = db.Exec(`DROP TABLE drop_me`)
		logResult(t, "DROP TABLE drop_me", err)

		// Verify it is gone.
		_, err = db.Exec(`INSERT INTO drop_me VALUES (1)`)
		if err != nil {
			t.Logf("[ OK ] table correctly dropped, insert failed: %v", err)
		} else {
			t.Log("[FAIL] table was NOT dropped, insert succeeded")
		}
	})

	t.Run("DropTableIfExists", func(t *testing.T) {
		db := openDB(t)

		// Drop non-existent table with IF EXISTS should not error.
		_, err := db.Exec(`DROP TABLE IF EXISTS ghost_table`)
		logResult(t, "DROP TABLE IF EXISTS ghost_table", err)

		// Without IF EXISTS, should error.
		_, err = db.Exec(`DROP TABLE ghost_table`)
		if err != nil {
			t.Logf("[ OK ] DROP non-existent table correctly errored: %v", err)
		} else {
			t.Log("[FAIL] DROP non-existent table did NOT error")
		}
	})

	// -----------------------------------------------------------------------
	// 9. ALTER TABLE RENAME TO
	// -----------------------------------------------------------------------
	t.Run("AlterTableRenameTo", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE rename_src (id INTEGER, val TEXT)`)
		logResult(t, "CREATE TABLE rename_src", err)

		_, err = db.Exec(`INSERT INTO rename_src VALUES (1, 'hello')`)
		logResult(t, "INSERT into rename_src", err)

		_, err = db.Exec(`ALTER TABLE rename_src RENAME TO rename_dst`)
		logResult(t, "ALTER TABLE RENAME TO", err)

		var val string
		err = db.QueryRow("SELECT val FROM rename_dst WHERE id = 1").Scan(&val)
		logResult(t, "SELECT from renamed table", err)
		if err == nil {
			t.Logf("  val=%q", val)
		}
	})

	// -----------------------------------------------------------------------
	// 10. ALTER TABLE ADD COLUMN
	// -----------------------------------------------------------------------
	t.Run("AlterTableAddColumn", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE addcol_tbl (id INTEGER)`)
		logResult(t, "CREATE TABLE addcol_tbl", err)

		_, err = db.Exec(`INSERT INTO addcol_tbl VALUES (1)`)
		logResult(t, "INSERT before add column", err)

		_, err = db.Exec(`ALTER TABLE addcol_tbl ADD COLUMN name VARCHAR`)
		logResult(t, "ALTER TABLE ADD COLUMN", err)

		_, err = db.Exec(`INSERT INTO addcol_tbl VALUES (2, 'bob')`)
		logResult(t, "INSERT after add column", err)

		var name sql.NullString
		err = db.QueryRow("SELECT name FROM addcol_tbl WHERE id = 2").Scan(&name)
		logResult(t, "SELECT added column", err)
		if err == nil {
			t.Logf("  name=%v", name)
		}
	})

	// -----------------------------------------------------------------------
	// 11. ALTER TABLE DROP COLUMN
	// -----------------------------------------------------------------------
	t.Run("AlterTableDropColumn", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE dropcol_tbl (id INTEGER, extra VARCHAR, val INTEGER)`)
		logResult(t, "CREATE TABLE dropcol_tbl", err)

		_, err = db.Exec(`INSERT INTO dropcol_tbl VALUES (1, 'remove-me', 42)`)
		logResult(t, "INSERT before drop column", err)

		_, err = db.Exec(`ALTER TABLE dropcol_tbl DROP COLUMN extra`)
		logResult(t, "ALTER TABLE DROP COLUMN extra", err)

		// Verify column is gone.
		var val int
		err = db.QueryRow("SELECT val FROM dropcol_tbl WHERE id = 1").Scan(&val)
		logResult(t, "SELECT remaining column after drop", err)
		if err == nil {
			t.Logf("  val=%d", val)
		}

		// Selecting dropped column should fail.
		_, err = db.Exec(`SELECT extra FROM dropcol_tbl`)
		if err != nil {
			t.Logf("[ OK ] dropped column correctly unresolvable: %v", err)
		} else {
			t.Log("[FAIL] dropped column was still accessible")
		}
	})

	// -----------------------------------------------------------------------
	// 12. ALTER TABLE RENAME COLUMN
	// -----------------------------------------------------------------------
	t.Run("AlterTableRenameColumn", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE renamecol_tbl (id INTEGER, old_name VARCHAR)`)
		logResult(t, "CREATE TABLE renamecol_tbl", err)

		_, err = db.Exec(`INSERT INTO renamecol_tbl VALUES (1, 'data')`)
		logResult(t, "INSERT into renamecol_tbl", err)

		_, err = db.Exec(`ALTER TABLE renamecol_tbl RENAME COLUMN old_name TO new_name`)
		logResult(t, "ALTER TABLE RENAME COLUMN", err)

		var val string
		err = db.QueryRow("SELECT new_name FROM renamecol_tbl WHERE id = 1").Scan(&val)
		logResult(t, "SELECT renamed column", err)
		if err == nil {
			t.Logf("  new_name=%q", val)
		}
	})

	// -----------------------------------------------------------------------
	// 13. CREATE VIEW and DROP VIEW
	// -----------------------------------------------------------------------
	t.Run("CreateAndDropView", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE view_base (id INTEGER, category VARCHAR, amount DOUBLE)`)
		logResult(t, "CREATE TABLE view_base", err)

		_, err = db.Exec(`INSERT INTO view_base VALUES (1, 'A', 10.5), (2, 'B', 20.0), (3, 'A', 15.0)`)
		logResult(t, "INSERT into view_base", err)

		_, err = db.Exec(`CREATE VIEW v_category_a AS SELECT * FROM view_base WHERE category = 'A'`)
		logResult(t, "CREATE VIEW v_category_a", err)

		var cnt int
		err = db.QueryRow("SELECT COUNT(*) FROM v_category_a").Scan(&cnt)
		logResult(t, "SELECT from view", err)
		if err == nil {
			t.Logf("  count from view = %d", cnt)
		}

		_, err = db.Exec(`DROP VIEW v_category_a`)
		logResult(t, "DROP VIEW v_category_a", err)

		// Selecting from dropped view should fail.
		_, err = db.Exec(`SELECT * FROM v_category_a`)
		if err != nil {
			t.Logf("[ OK ] dropped view correctly inaccessible: %v", err)
		} else {
			t.Log("[FAIL] dropped view was still accessible")
		}
	})

	// -----------------------------------------------------------------------
	// 14. CREATE OR REPLACE VIEW
	// -----------------------------------------------------------------------
	t.Run("CreateOrReplaceView", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE corv_base (id INTEGER, val INTEGER)`)
		logResult(t, "CREATE TABLE corv_base", err)

		_, err = db.Exec(`INSERT INTO corv_base VALUES (1, 10), (2, 20), (3, 30)`)
		logResult(t, "INSERT into corv_base", err)

		_, err = db.Exec(`CREATE VIEW corv_view AS SELECT * FROM corv_base WHERE val > 5`)
		logResult(t, "CREATE VIEW corv_view", err)

		_, err = db.Exec(`CREATE OR REPLACE VIEW corv_view AS SELECT * FROM corv_base WHERE val > 15`)
		logResult(t, "CREATE OR REPLACE VIEW corv_view", err)

		var cnt int
		err = db.QueryRow("SELECT COUNT(*) FROM corv_view").Scan(&cnt)
		logResult(t, "SELECT from replaced view", err)
		if err == nil {
			t.Logf("  count from replaced view = %d (expected 2)", cnt)
		}
	})

	// -----------------------------------------------------------------------
	// 15. CREATE SCHEMA and DROP SCHEMA
	// -----------------------------------------------------------------------
	t.Run("CreateAndDropSchema", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SCHEMA myschema`)
		logResult(t, "CREATE SCHEMA myschema", err)

		// Drop the schema.
		_, err = db.Exec(`DROP SCHEMA myschema`)
		logResult(t, "DROP SCHEMA myschema", err)
	})

	// -----------------------------------------------------------------------
	// 16. Cross-schema table creation (schema.table)
	// -----------------------------------------------------------------------
	t.Run("CrossSchemaTable", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SCHEMA sales`)
		logResult(t, "CREATE SCHEMA sales", err)

		_, err = db.Exec(`CREATE TABLE sales.orders (id INTEGER, product VARCHAR, qty INTEGER)`)
		logResult(t, "CREATE TABLE sales.orders", err)

		_, err = db.Exec(`INSERT INTO sales.orders VALUES (1, 'widget', 5)`)
		logResult(t, "INSERT into sales.orders", err)

		var product string
		err = db.QueryRow("SELECT product FROM sales.orders WHERE id = 1").Scan(&product)
		logResult(t, "SELECT from sales.orders", err)
		if err == nil {
			t.Logf("  product=%q", product)
		}
	})

	// -----------------------------------------------------------------------
	// 17. CREATE SEQUENCE with various options and NEXTVAL/CURRVAL
	// -----------------------------------------------------------------------
	t.Run("SequenceBasic", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SEQUENCE seq_basic START WITH 1 INCREMENT BY 1`)
		logResult(t, "CREATE SEQUENCE seq_basic", err)

		var v1, v2 int64
		err = db.QueryRow("SELECT NEXTVAL('seq_basic')").Scan(&v1)
		logResult(t, "NEXTVAL first call", err)
		if err == nil {
			t.Logf("  nextval #1 = %d", v1)
		}

		err = db.QueryRow("SELECT NEXTVAL('seq_basic')").Scan(&v2)
		logResult(t, "NEXTVAL second call", err)
		if err == nil {
			t.Logf("  nextval #2 = %d", v2)
		}

		var cv int64
		err = db.QueryRow("SELECT CURRVAL('seq_basic')").Scan(&cv)
		logResult(t, "CURRVAL", err)
		if err == nil {
			t.Logf("  currval = %d", cv)
		}
	})

	t.Run("SequenceWithOptions", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SEQUENCE seq_opts START WITH 10 INCREMENT BY 5 MINVALUE 0 MAXVALUE 100 CYCLE`)
		logResult(t, "CREATE SEQUENCE seq_opts", err)

		var val int64
		err = db.QueryRow("SELECT NEXTVAL('seq_opts')").Scan(&val)
		logResult(t, "NEXTVAL seq_opts", err)
		if err == nil {
			t.Logf("  nextval = %d (expected 10)", val)
		}

		err = db.QueryRow("SELECT NEXTVAL('seq_opts')").Scan(&val)
		logResult(t, "NEXTVAL seq_opts second", err)
		if err == nil {
			t.Logf("  nextval = %d (expected 15)", val)
		}
	})

	t.Run("DropSequence", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SEQUENCE seq_drop`)
		logResult(t, "CREATE SEQUENCE seq_drop", err)

		_, err = db.Exec(`DROP SEQUENCE seq_drop`)
		logResult(t, "DROP SEQUENCE seq_drop", err)

		_, err = db.Exec(`SELECT NEXTVAL('seq_drop')`)
		if err != nil {
			t.Logf("[ OK ] dropped sequence correctly unavailable: %v", err)
		} else {
			t.Log("[FAIL] dropped sequence was still available")
		}
	})

	// -----------------------------------------------------------------------
	// 18. CREATE INDEX and CREATE UNIQUE INDEX
	// -----------------------------------------------------------------------
	t.Run("CreateIndex", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE idx_tbl (id INTEGER, name VARCHAR, score INTEGER)`)
		logResult(t, "CREATE TABLE idx_tbl", err)

		_, err = db.Exec(`CREATE INDEX idx_name ON idx_tbl(name)`)
		logResult(t, "CREATE INDEX idx_name", err)

		_, err = db.Exec(`CREATE UNIQUE INDEX idx_id ON idx_tbl(id)`)
		logResult(t, "CREATE UNIQUE INDEX idx_id", err)

		// Insert and query to verify the table still works with indexes.
		_, err = db.Exec(`INSERT INTO idx_tbl VALUES (1, 'alice', 90), (2, 'bob', 85)`)
		logResult(t, "INSERT with indexes", err)

		var name string
		err = db.QueryRow("SELECT name FROM idx_tbl WHERE id = 1").Scan(&name)
		logResult(t, "SELECT via index", err)
		if err == nil {
			t.Logf("  name=%q", name)
		}
	})

	// -----------------------------------------------------------------------
	// 19. DROP INDEX
	// -----------------------------------------------------------------------
	t.Run("DropIndex", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE drop_idx_tbl (id INTEGER, val VARCHAR)`)
		logResult(t, "CREATE TABLE drop_idx_tbl", err)

		_, err = db.Exec(`CREATE INDEX drop_idx ON drop_idx_tbl(val)`)
		logResult(t, "CREATE INDEX drop_idx", err)

		_, err = db.Exec(`DROP INDEX drop_idx`)
		logResult(t, "DROP INDEX drop_idx", err)

		// Verify the table is still usable.
		_, err = db.Exec(`INSERT INTO drop_idx_tbl VALUES (1, 'test')`)
		logResult(t, "INSERT after DROP INDEX", err)
	})

	// -----------------------------------------------------------------------
	// 20. CREATE TABLE AS SELECT (CTAS)
	// -----------------------------------------------------------------------
	t.Run("CreateTableAsSelect", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE ctas_src (id INTEGER, category VARCHAR, amount DOUBLE)`)
		logResult(t, "CREATE TABLE ctas_src", err)

		_, err = db.Exec(`INSERT INTO ctas_src VALUES (1, 'X', 10.0), (2, 'Y', 20.0), (3, 'X', 30.0)`)
		logResult(t, "INSERT into ctas_src", err)

		_, err = db.Exec(`CREATE TABLE ctas_dst AS SELECT category, SUM(amount) AS total FROM ctas_src GROUP BY category`)
		logResult(t, "CREATE TABLE AS SELECT", err)

		rows, err := db.Query("SELECT * FROM ctas_dst ORDER BY 1")
		logResult(t, "SELECT from CTAS table", err)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var cat string
				var total float64
				if scanErr := rows.Scan(&cat, &total); scanErr != nil {
					t.Logf("  scan error: %v", scanErr)
				} else {
					t.Logf("  category=%q total=%.1f", cat, total)
				}
			}
		}
	})

	// -----------------------------------------------------------------------
	// 21. Generated/computed columns
	// -----------------------------------------------------------------------
	t.Run("GeneratedColumns", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE gen_tbl (
			price DOUBLE,
			qty INTEGER,
			total DOUBLE GENERATED ALWAYS AS (price * qty)
		)`)
		logResult(t, "CREATE TABLE gen_tbl with generated column", err)

		_, err = db.Exec(`INSERT INTO gen_tbl (price, qty) VALUES (9.99, 3)`)
		logResult(t, "INSERT into gen_tbl", err)

		var total float64
		err = db.QueryRow("SELECT total FROM gen_tbl").Scan(&total)
		logResult(t, "SELECT generated column", err)
		if err == nil {
			t.Logf("  total=%.2f (expected 29.97)", total)
		}
	})

	// -----------------------------------------------------------------------
	// Extra: combined scenarios
	// -----------------------------------------------------------------------
	t.Run("MultipleConstraintsCombined", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE multi_constraint (
			id INTEGER PRIMARY KEY,
			email VARCHAR NOT NULL UNIQUE,
			age INTEGER DEFAULT 0 CHECK (age >= 0),
			status VARCHAR DEFAULT 'pending'
		)`)
		logResult(t, "CREATE TABLE multi_constraint", err)

		_, err = db.Exec(`INSERT INTO multi_constraint (id, email) VALUES (1, 'test@example.com')`)
		logResult(t, "INSERT with multiple constraints/defaults", err)

		var age int
		var status string
		err = db.QueryRow("SELECT age, status FROM multi_constraint WHERE id = 1").Scan(&age, &status)
		logResult(t, "SELECT defaults from multi_constraint", err)
		if err == nil {
			t.Logf("  age=%d, status=%q", age, status)
		}
	})

	t.Run("DropViewIfExists", func(t *testing.T) {
		db := openDB(t)

		// Should not error on nonexistent view.
		_, err := db.Exec(`DROP VIEW IF EXISTS ghost_view`)
		logResult(t, "DROP VIEW IF EXISTS ghost_view", err)
	})

	t.Run("SchemaWithTableAndView", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SCHEMA analytics`)
		logResult(t, "CREATE SCHEMA analytics", err)

		_, err = db.Exec(`CREATE TABLE analytics.metrics (id INTEGER, value DOUBLE)`)
		logResult(t, "CREATE TABLE analytics.metrics", err)

		_, err = db.Exec(`INSERT INTO analytics.metrics VALUES (1, 99.9), (2, 50.0)`)
		logResult(t, "INSERT into analytics.metrics", err)

		_, err = db.Exec(`CREATE VIEW analytics.high_metrics AS SELECT * FROM analytics.metrics WHERE value > 75`)
		logResult(t, "CREATE VIEW analytics.high_metrics", err)

		var cnt int
		err = db.QueryRow("SELECT COUNT(*) FROM analytics.high_metrics").Scan(&cnt)
		logResult(t, "SELECT from cross-schema view", err)
		if err == nil {
			t.Logf("  count=%d (expected 1)", cnt)
		}
	})

	t.Run("SequenceUsedInInsert", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE SEQUENCE auto_id START WITH 1`)
		logResult(t, "CREATE SEQUENCE auto_id", err)

		_, err = db.Exec(`CREATE TABLE seq_users (id INTEGER, name VARCHAR)`)
		logResult(t, "CREATE TABLE seq_users", err)

		for i := 0; i < 3; i++ {
			_, err = db.Exec(fmt.Sprintf(`INSERT INTO seq_users VALUES (NEXTVAL('auto_id'), 'user_%d')`, i))
			logResult(t, fmt.Sprintf("INSERT with NEXTVAL #%d", i), err)
		}

		rows, err := db.Query("SELECT id, name FROM seq_users ORDER BY id")
		logResult(t, "SELECT from seq_users", err)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id int
				var name string
				if scanErr := rows.Scan(&id, &name); scanErr != nil {
					t.Logf("  scan error: %v", scanErr)
				} else {
					t.Logf("  id=%d name=%q", id, name)
				}
			}
		}
	})

	t.Run("IndexOnMultipleColumns", func(t *testing.T) {
		db := openDB(t)

		_, err := db.Exec(`CREATE TABLE multi_idx (a INTEGER, b VARCHAR, c DOUBLE)`)
		logResult(t, "CREATE TABLE multi_idx", err)

		_, err = db.Exec(`CREATE INDEX idx_ab ON multi_idx(a, b)`)
		logResult(t, "CREATE INDEX on (a, b)", err)

		_, err = db.Exec(`INSERT INTO multi_idx VALUES (1, 'x', 1.0), (2, 'y', 2.0), (1, 'z', 3.0)`)
		logResult(t, "INSERT into multi_idx", err)

		var c float64
		err = db.QueryRow("SELECT c FROM multi_idx WHERE a = 1 AND b = 'x'").Scan(&c)
		logResult(t, "SELECT using composite index", err)
		if err == nil {
			t.Logf("  c=%.1f", c)
		}
	})
}
