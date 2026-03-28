package test_probe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TestTransactionsAndIsolation exercises transaction and concurrency scenarios.
// ---------------------------------------------------------------------------
func TestTransactionsAndIsolation(t *testing.T) {
	// 1. Basic BEGIN/COMMIT
	t.Run("BasicBeginCommit", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_commit(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_commit VALUES (1, 'hello')")
		if err != nil {
			logResult(t, "insert", err)
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var val string
		err = db.QueryRow("SELECT val FROM t_commit WHERE id = 1").Scan(&val)
		if err != nil {
			logResult(t, "query after commit", err)
			return
		}
		if val != "hello" {
			logResult(t, "value check", fmt.Errorf("expected 'hello', got '%s'", val))
			return
		}
		logResult(t, "BasicBeginCommit", nil)
	})

	// 2. Basic BEGIN/ROLLBACK
	t.Run("BasicBeginRollback", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_rollback(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rollback VALUES (1, 'before')")
		if err != nil {
			logResult(t, "insert before", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rollback VALUES (2, 'inside_tx')")
		if err != nil {
			logResult(t, "insert inside tx", err)
			return
		}
		_, err = db.Exec("ROLLBACK")
		if err != nil {
			logResult(t, "rollback", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_rollback").Scan(&count)
		if err != nil {
			logResult(t, "count after rollback", err)
			return
		}
		if count != 1 {
			logResult(t, "count check", fmt.Errorf("expected 1 row after rollback, got %d", count))
			return
		}
		logResult(t, "BasicBeginRollback", nil)
	})

	// 3. Autocommit mode
	t.Run("Autocommit", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_auto(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_auto VALUES (1)")
		if err != nil {
			logResult(t, "insert", err)
			return
		}
		var id int
		err = db.QueryRow("SELECT id FROM t_auto WHERE id = 1").Scan(&id)
		if err != nil {
			logResult(t, "query", err)
			return
		}
		if id != 1 {
			logResult(t, "value check", fmt.Errorf("expected 1, got %d", id))
			return
		}
		logResult(t, "Autocommit", nil)
	})

	// 4. SAVEPOINT creation
	t.Run("SavepointCreation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_sp(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("SAVEPOINT sp1")
		if err != nil {
			logResult(t, "savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_sp VALUES (1)")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		logResult(t, "SavepointCreation", err)
	})

	// 5. ROLLBACK TO SAVEPOINT
	t.Run("RollbackToSavepoint", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_rbsp(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rbsp VALUES (1)")
		if err != nil {
			logResult(t, "insert 1", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("SAVEPOINT sp1")
		if err != nil {
			logResult(t, "savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_rbsp VALUES (2)")
		if err != nil {
			logResult(t, "insert 2", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("ROLLBACK TO SAVEPOINT sp1")
		if err != nil {
			logResult(t, "rollback to savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_rbsp").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != 1 {
			logResult(t, "count check", fmt.Errorf("expected 1 row (row 2 rolled back), got %d", count))
			return
		}
		logResult(t, "RollbackToSavepoint", nil)
	})

	// 6. RELEASE SAVEPOINT
	t.Run("ReleaseSavepoint", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_relsp(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("SAVEPOINT sp1")
		if err != nil {
			logResult(t, "savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_relsp VALUES (1)")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("RELEASE SAVEPOINT sp1")
		if err != nil {
			logResult(t, "release savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_relsp").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != 1 {
			logResult(t, "count check", fmt.Errorf("expected 1, got %d", count))
			return
		}
		logResult(t, "ReleaseSavepoint", nil)
	})

	// 7. Nested savepoints
	t.Run("NestedSavepoints", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_nested(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_nested VALUES (1)")
		if err != nil {
			logResult(t, "insert 1", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("SAVEPOINT outer_sp")
		if err != nil {
			logResult(t, "outer savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_nested VALUES (2)")
		if err != nil {
			logResult(t, "insert 2", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("SAVEPOINT inner_sp")
		if err != nil {
			logResult(t, "inner savepoint", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_nested VALUES (3)")
		if err != nil {
			logResult(t, "insert 3", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		// Rollback inner savepoint: row 3 should be gone
		_, err = db.Exec("ROLLBACK TO SAVEPOINT inner_sp")
		if err != nil {
			logResult(t, "rollback to inner", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		// Release outer savepoint: rows 1, 2 should remain
		_, err = db.Exec("RELEASE SAVEPOINT outer_sp")
		if err != nil {
			logResult(t, "release outer", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_nested").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != 2 {
			logResult(t, "count check", fmt.Errorf("expected 2 rows, got %d", count))
			return
		}
		logResult(t, "NestedSavepoints", nil)
	})

	// 8. READ UNCOMMITTED isolation level
	t.Run("ReadUncommittedIsolation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_ru(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
		if err != nil {
			logResult(t, "begin read uncommitted", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_ru VALUES (1, 'uncommitted')")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		logResult(t, "ReadUncommittedIsolation", err)
	})

	// 9. READ COMMITTED isolation level
	t.Run("ReadCommittedIsolation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_rc(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
		if err != nil {
			logResult(t, "begin read committed", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rc VALUES (1, 'committed')")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		logResult(t, "ReadCommittedIsolation", err)
	})

	// 10. REPEATABLE READ isolation level
	t.Run("RepeatableReadIsolation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_rr(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			logResult(t, "begin repeatable read", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rr VALUES (1, 'repeatable')")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		logResult(t, "RepeatableReadIsolation", err)
	})

	// 11. SERIALIZABLE isolation level (default)
	t.Run("SerializableIsolation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_ser(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			logResult(t, "begin serializable", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_ser VALUES (1, 'serializable')")
		if err != nil {
			logResult(t, "insert", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		logResult(t, "SerializableIsolation", err)
	})

	// 12. Dirty read test
	t.Run("DirtyReadTest", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_dirty(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_dirty VALUES (1, 'original')")
		if err != nil {
			logResult(t, "initial insert", err)
			return
		}

		// Start a transaction that modifies data but does not commit
		conn1, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn1", err)
			return
		}
		defer conn1.Close()

		_, err = conn1.ExecContext(context.Background(), "BEGIN")
		if err != nil {
			logResult(t, "conn1 begin", err)
			return
		}
		_, err = conn1.ExecContext(context.Background(), "UPDATE t_dirty SET val = 'modified' WHERE id = 1")
		if err != nil {
			logResult(t, "conn1 update", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}

		// Read from another connection with READ UNCOMMITTED
		conn2, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn2", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		defer conn2.Close()

		_, err = conn2.ExecContext(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
		if err != nil {
			logResult(t, "conn2 begin read uncommitted", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		var val string
		err = conn2.QueryRowContext(context.Background(), "SELECT val FROM t_dirty WHERE id = 1").Scan(&val)
		if err != nil {
			logResult(t, "conn2 read", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			_, _ = conn2.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		t.Logf("  READ UNCOMMITTED saw value: '%s' (dirty read visible if 'modified')", val)
		_, _ = conn2.ExecContext(context.Background(), "COMMIT")
		_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
		logResult(t, "DirtyReadTest", nil)
	})

	// 13. Non-repeatable read test
	t.Run("NonRepeatableReadTest", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_nrr(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_nrr VALUES (1, 'initial')")
		if err != nil {
			logResult(t, "initial insert", err)
			return
		}

		conn1, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn1", err)
			return
		}
		defer conn1.Close()

		// Start a REPEATABLE READ transaction and read the value
		_, err = conn1.ExecContext(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			logResult(t, "conn1 begin", err)
			return
		}
		var val1 string
		err = conn1.QueryRowContext(context.Background(), "SELECT val FROM t_nrr WHERE id = 1").Scan(&val1)
		if err != nil {
			logResult(t, "conn1 first read", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		t.Logf("  First read in REPEATABLE READ: '%s'", val1)

		// Another connection modifies and commits
		_, err = db.Exec("UPDATE t_nrr SET val = 'changed' WHERE id = 1")
		if err != nil {
			logResult(t, "external update", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}

		// Read again in the same transaction
		var val2 string
		err = conn1.QueryRowContext(context.Background(), "SELECT val FROM t_nrr WHERE id = 1").Scan(&val2)
		if err != nil {
			logResult(t, "conn1 second read", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		t.Logf("  Second read in REPEATABLE READ: '%s' (should be same as first for repeatable read)", val2)
		_, _ = conn1.ExecContext(context.Background(), "COMMIT")
		logResult(t, "NonRepeatableReadTest", nil)
	})

	// 14. Phantom read test
	t.Run("PhantomReadTest", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_phantom(id INTEGER, category TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_phantom VALUES (1, 'A'), (2, 'A')")
		if err != nil {
			logResult(t, "initial insert", err)
			return
		}

		conn1, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn1", err)
			return
		}
		defer conn1.Close()

		_, err = conn1.ExecContext(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			logResult(t, "conn1 begin", err)
			return
		}

		var count1 int
		err = conn1.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t_phantom WHERE category = 'A'").Scan(&count1)
		if err != nil {
			logResult(t, "conn1 first count", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		t.Logf("  First count in SERIALIZABLE: %d", count1)

		// Another connection inserts a new row
		_, err = db.Exec("INSERT INTO t_phantom VALUES (3, 'A')")
		if err != nil {
			logResult(t, "external insert", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}

		var count2 int
		err = conn1.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t_phantom WHERE category = 'A'").Scan(&count2)
		if err != nil {
			logResult(t, "conn1 second count", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		t.Logf("  Second count in SERIALIZABLE: %d (should be same as first to prevent phantom)", count2)
		_, _ = conn1.ExecContext(context.Background(), "COMMIT")
		logResult(t, "PhantomReadTest", nil)
	})

	// 15. Write-write conflict in SERIALIZABLE
	t.Run("WriteWriteConflict", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_wwc(id INTEGER PRIMARY KEY, val INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_wwc VALUES (1, 100)")
		if err != nil {
			logResult(t, "initial insert", err)
			return
		}

		conn1, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn1", err)
			return
		}
		defer conn1.Close()

		conn2, err := db.Conn(context.Background())
		if err != nil {
			logResult(t, "conn2", err)
			return
		}
		defer conn2.Close()

		// Both connections start SERIALIZABLE transactions
		_, err = conn1.ExecContext(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			logResult(t, "conn1 begin", err)
			return
		}
		_, err = conn2.ExecContext(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			logResult(t, "conn2 begin", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			return
		}

		// Both update the same row
		_, err = conn1.ExecContext(context.Background(), "UPDATE t_wwc SET val = 200 WHERE id = 1")
		if err != nil {
			logResult(t, "conn1 update", err)
			_, _ = conn1.ExecContext(context.Background(), "ROLLBACK")
			_, _ = conn2.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		_, err = conn2.ExecContext(context.Background(), "UPDATE t_wwc SET val = 300 WHERE id = 1")
		if err != nil {
			// This might fail immediately with conflict detection
			t.Logf("  conn2 update returned error (expected for write-write conflict): %v", err)
		}

		// Try to commit both - one should fail for serializable
		err1 := func() error {
			_, e := conn1.ExecContext(context.Background(), "COMMIT")
			return e
		}()
		err2 := func() error {
			_, e := conn2.ExecContext(context.Background(), "COMMIT")
			return e
		}()

		if err1 != nil {
			t.Logf("  conn1 commit error: %v", err1)
		}
		if err2 != nil {
			t.Logf("  conn2 commit error: %v", err2)
		}

		// At least one should have succeeded, and for SERIALIZABLE at least one may fail
		if err1 == nil || err2 == nil {
			t.Logf("  At least one transaction committed successfully (expected for conflict detection)")
		}
		logResult(t, "WriteWriteConflict", nil)
	})

	// 16. SET default_transaction_isolation
	t.Run("SetDefaultTransactionIsolation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("SET default_transaction_isolation = 'READ COMMITTED'")
		if err != nil {
			logResult(t, "set default isolation", err)
			return
		}
		logResult(t, "SetDefaultTransactionIsolation", nil)
	})

	// 17. SHOW transaction_isolation
	t.Run("ShowTransactionIsolation", func(t *testing.T) {
		db := openDB(t)
		var level string
		err := db.QueryRow("SHOW transaction_isolation").Scan(&level)
		if err != nil {
			logResult(t, "show transaction_isolation", err)
			return
		}
		t.Logf("  Current transaction_isolation: '%s'", level)
		logResult(t, "ShowTransactionIsolation", nil)
	})

	// 18. Multiple concurrent transactions with goroutines
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_conc(id INTEGER, worker INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}

		const numWorkers = 5
		const insertsPerWorker = 10
		var wg sync.WaitGroup
		errCh := make(chan error, numWorkers)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < insertsPerWorker; i++ {
					_, e := db.Exec("INSERT INTO t_conc VALUES (?, ?)", i, workerID)
					if e != nil {
						errCh <- fmt.Errorf("worker %d insert %d: %v", workerID, i, e)
						return
					}
				}
			}(w)
		}

		wg.Wait()
		close(errCh)

		var errs []string
		for e := range errCh {
			errs = append(errs, e.Error())
		}
		if len(errs) > 0 {
			logResult(t, "concurrent inserts", fmt.Errorf("errors: %s", strings.Join(errs, "; ")))
			return
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_conc").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		expected := numWorkers * insertsPerWorker
		if count != expected {
			logResult(t, "count check", fmt.Errorf("expected %d rows, got %d", expected, count))
			return
		}
		t.Logf("  All %d rows inserted by %d workers", count, numWorkers)
		logResult(t, "ConcurrentTransactions", nil)
	})

	// 19. Transaction with DDL
	t.Run("TransactionWithDDL", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("CREATE TABLE t_ddl_tx(id INTEGER, name TEXT)")
		if err != nil {
			logResult(t, "create table in tx", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("INSERT INTO t_ddl_tx VALUES (1, 'created_in_tx')")
		if err != nil {
			logResult(t, "insert in tx", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var name string
		err = db.QueryRow("SELECT name FROM t_ddl_tx WHERE id = 1").Scan(&name)
		if err != nil {
			logResult(t, "query after ddl tx", err)
			return
		}
		if name != "created_in_tx" {
			logResult(t, "value check", fmt.Errorf("expected 'created_in_tx', got '%s'", name))
			return
		}
		logResult(t, "TransactionWithDDL", nil)
	})

	// 20. Transaction with multiple statements
	t.Run("TransactionMultipleStatements", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_multi(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		for i := 0; i < 5; i++ {
			_, err = db.Exec("INSERT INTO t_multi VALUES (?, ?)", i, fmt.Sprintf("val_%d", i))
			if err != nil {
				logResult(t, fmt.Sprintf("insert %d", i), err)
				_, _ = db.Exec("ROLLBACK")
				return
			}
		}
		_, err = db.Exec("UPDATE t_multi SET val = 'updated' WHERE id = 0")
		if err != nil {
			logResult(t, "update", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("DELETE FROM t_multi WHERE id = 4")
		if err != nil {
			logResult(t, "delete", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_multi").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != 4 {
			logResult(t, "count check", fmt.Errorf("expected 4 rows, got %d", count))
			return
		}
		logResult(t, "TransactionMultipleStatements", nil)
	})

	// 21. Rollback after error
	t.Run("RollbackAfterError", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_rberr(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rberr VALUES (1)")
		if err != nil {
			logResult(t, "insert before tx", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_rberr VALUES (2)")
		if err != nil {
			logResult(t, "insert in tx", err)
			_, _ = db.Exec("ROLLBACK")
			return
		}
		// Deliberately cause an error (reference non-existent table)
		_, err = db.Exec("INSERT INTO nonexistent_table VALUES (99)")
		if err != nil {
			t.Logf("  Expected error from bad insert: %v", err)
		}
		// Rollback the transaction
		_, err = db.Exec("ROLLBACK")
		if err != nil {
			// Some engines auto-rollback after error; that is acceptable
			t.Logf("  Rollback after error: %v (may already be rolled back)", err)
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_rberr").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		t.Logf("  Rows after rollback: %d (should be 1 if tx was rolled back)", count)
		logResult(t, "RollbackAfterError", nil)
	})

	// 22. Large transaction (many inserts)
	t.Run("LargeTransaction", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_large(id INTEGER, data TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("BEGIN")
		if err != nil {
			logResult(t, "begin", err)
			return
		}
		const numRows = 1000
		for i := 0; i < numRows; i++ {
			_, err = db.Exec("INSERT INTO t_large VALUES (?, ?)", i, fmt.Sprintf("data_%d", i))
			if err != nil {
				logResult(t, fmt.Sprintf("insert %d", i), err)
				_, _ = db.Exec("ROLLBACK")
				return
			}
		}
		_, err = db.Exec("COMMIT")
		if err != nil {
			logResult(t, "commit", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_large").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != numRows {
			logResult(t, "count check", fmt.Errorf("expected %d rows, got %d", numRows, count))
			return
		}
		t.Logf("  Successfully inserted and committed %d rows", count)
		logResult(t, "LargeTransaction", nil)
	})
}

// ---------------------------------------------------------------------------
// TestDatabaseSQLInterface exercises the database/sql interface compatibility.
// ---------------------------------------------------------------------------
func TestDatabaseSQLInterface(t *testing.T) {
	// 1. sql.Open and Ping
	t.Run("OpenAndPing", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		if err != nil {
			logResult(t, "sql.Open", err)
			return
		}
		defer db.Close()
		err = db.Ping()
		logResult(t, "OpenAndPing", err)
	})

	// 2. db.Begin() and tx.Commit()
	t.Run("BeginAndCommit", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_bc(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		tx, err := db.Begin()
		if err != nil {
			logResult(t, "db.Begin", err)
			return
		}
		_, err = tx.Exec("INSERT INTO t_bc VALUES (1)")
		if err != nil {
			logResult(t, "tx.Exec", err)
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			logResult(t, "tx.Commit", err)
			return
		}
		var id int
		err = db.QueryRow("SELECT id FROM t_bc").Scan(&id)
		if err != nil {
			logResult(t, "query after commit", err)
			return
		}
		if id != 1 {
			logResult(t, "value check", fmt.Errorf("expected 1, got %d", id))
			return
		}
		logResult(t, "BeginAndCommit", nil)
	})

	// 3. db.Begin() and tx.Rollback()
	t.Run("BeginAndRollback", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_br(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		tx, err := db.Begin()
		if err != nil {
			logResult(t, "db.Begin", err)
			return
		}
		_, err = tx.Exec("INSERT INTO t_br VALUES (1)")
		if err != nil {
			logResult(t, "tx.Exec", err)
			_ = tx.Rollback()
			return
		}
		err = tx.Rollback()
		if err != nil {
			logResult(t, "tx.Rollback", err)
			return
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_br").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		if count != 0 {
			logResult(t, "count check", fmt.Errorf("expected 0 after rollback, got %d", count))
			return
		}
		logResult(t, "BeginAndRollback", nil)
	})

	// 4. db.Exec() for DDL
	t.Run("ExecDDL", func(t *testing.T) {
		db := openDB(t)
		result, err := db.Exec("CREATE TABLE t_ddl(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		t.Logf("  DDL result: %v", result)

		// Verify the table exists by inserting and querying
		_, err = db.Exec("INSERT INTO t_ddl VALUES (1, 'Alice', 30)")
		if err != nil {
			logResult(t, "insert into created table", err)
			return
		}
		logResult(t, "ExecDDL", nil)
	})

	// 5. db.Query() and rows.Scan()
	t.Run("QueryAndScan", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_query(id INTEGER, name TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_query VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
		if err != nil {
			logResult(t, "insert", err)
			return
		}

		rows, err := db.Query("SELECT id, name FROM t_query ORDER BY id")
		if err != nil {
			logResult(t, "query", err)
			return
		}
		defer rows.Close()

		var results []string
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				logResult(t, "scan", err)
				return
			}
			results = append(results, fmt.Sprintf("%d:%s", id, name))
		}
		if err := rows.Err(); err != nil {
			logResult(t, "rows.Err", err)
			return
		}
		if len(results) != 3 {
			logResult(t, "result count", fmt.Errorf("expected 3 rows, got %d", len(results)))
			return
		}
		t.Logf("  Query results: %v", results)
		logResult(t, "QueryAndScan", nil)
	})

	// 6. db.QueryRow()
	t.Run("QueryRow", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_qr(id INTEGER, val TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_qr VALUES (1, 'single')")
		if err != nil {
			logResult(t, "insert", err)
			return
		}
		var val string
		err = db.QueryRow("SELECT val FROM t_qr WHERE id = 1").Scan(&val)
		if err != nil {
			logResult(t, "QueryRow.Scan", err)
			return
		}
		if val != "single" {
			logResult(t, "value check", fmt.Errorf("expected 'single', got '%s'", val))
			return
		}

		// Test no rows
		err = db.QueryRow("SELECT val FROM t_qr WHERE id = 999").Scan(&val)
		if err != sql.ErrNoRows {
			logResult(t, "QueryRow no rows", fmt.Errorf("expected sql.ErrNoRows, got %v", err))
			return
		}
		t.Logf("  QueryRow correctly returns sql.ErrNoRows for missing row")
		logResult(t, "QueryRow", nil)
	})

	// 7. Prepared statements
	t.Run("PreparedStatements", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_prep(id INTEGER, name TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}

		// Prepare insert
		stmtInsert, err := db.Prepare("INSERT INTO t_prep VALUES (?, ?)")
		if err != nil {
			logResult(t, "prepare insert", err)
			return
		}
		defer stmtInsert.Close()

		for i := 1; i <= 5; i++ {
			_, err = stmtInsert.Exec(i, fmt.Sprintf("name_%d", i))
			if err != nil {
				logResult(t, fmt.Sprintf("stmt.Exec %d", i), err)
				return
			}
		}

		// Prepare select
		stmtQuery, err := db.Prepare("SELECT name FROM t_prep WHERE id = ?")
		if err != nil {
			logResult(t, "prepare select", err)
			return
		}
		defer stmtQuery.Close()

		var name string
		err = stmtQuery.QueryRow(3).Scan(&name)
		if err != nil {
			logResult(t, "stmt.QueryRow", err)
			return
		}
		if name != "name_3" {
			logResult(t, "value check", fmt.Errorf("expected 'name_3', got '%s'", name))
			return
		}
		t.Logf("  Prepared statement query returned: '%s'", name)
		logResult(t, "PreparedStatements", nil)
	})

	// 8. Named parameters (if supported)
	t.Run("NamedParameters", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_named(id INTEGER, name TEXT)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		// Try named parameters using $1, $2 syntax
		_, err = db.Exec("INSERT INTO t_named VALUES ($1, $2)", 1, "named_test")
		if err != nil {
			t.Logf("  Named parameters ($1, $2) not supported: %v", err)
			// Try positional as fallback info
			_, err = db.Exec("INSERT INTO t_named VALUES (?, ?)", 1, "named_test")
			if err != nil {
				logResult(t, "positional params fallback", err)
				return
			}
			t.Logf("  Positional parameters (?, ?) work as alternative")
		} else {
			t.Logf("  Named parameters ($1, $2) supported")
		}
		logResult(t, "NamedParameters", nil)
	})

	// 9. Multiple result sets (if supported)
	t.Run("MultipleResultSets", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_mrs(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		_, err = db.Exec("INSERT INTO t_mrs VALUES (1), (2), (3)")
		if err != nil {
			logResult(t, "insert", err)
			return
		}
		// Most Go SQL drivers do not support multiple result sets.
		// We just verify a single query works.
		rows, err := db.Query("SELECT id FROM t_mrs ORDER BY id")
		if err != nil {
			logResult(t, "query", err)
			return
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				logResult(t, "scan", err)
				return
			}
			count++
		}
		t.Logf("  Query returned %d rows (multiple result sets typically not supported in Go sql)", count)
		logResult(t, "MultipleResultSets", nil)
	})

	// 10. Connection pooling (multiple goroutines)
	t.Run("ConnectionPooling", func(t *testing.T) {
		db := openDB(t)
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(2)

		_, err := db.Exec("CREATE TABLE t_pool(id INTEGER, worker INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}

		const numGoroutines = 10
		var wg sync.WaitGroup
		errCh := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				_, e := db.Exec("INSERT INTO t_pool VALUES (?, ?)", gid, gid)
				if e != nil {
					errCh <- fmt.Errorf("goroutine %d: %v", gid, e)
				}
			}(i)
		}
		wg.Wait()
		close(errCh)

		var errs []string
		for e := range errCh {
			errs = append(errs, e.Error())
		}
		if len(errs) > 0 {
			logResult(t, "pool inserts", fmt.Errorf("errors: %s", strings.Join(errs, "; ")))
			return
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM t_pool").Scan(&count)
		if err != nil {
			logResult(t, "count query", err)
			return
		}
		t.Logf("  Pool test: %d rows from %d goroutines with max 5 conns", count, numGoroutines)
		logResult(t, "ConnectionPooling", nil)
	})

	// 11. Context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_ctx(id INTEGER)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}

		// Cancel context before query
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = db.QueryContext(ctx, "SELECT * FROM t_ctx")
		if err != nil {
			t.Logf("  Query with cancelled context returned: %v", err)
			if err == context.Canceled || strings.Contains(err.Error(), "cancel") {
				t.Logf("  Correctly detected context cancellation")
			} else {
				t.Logf("  Error is not context.Canceled but query was rejected (acceptable)")
			}
		} else {
			t.Logf("  Query with cancelled context succeeded (engine may not check context)")
		}

		// Test with timeout
		ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel2()
		time.Sleep(1 * time.Millisecond) // Ensure timeout fires
		_, err = db.QueryContext(ctx2, "SELECT * FROM t_ctx")
		if err != nil {
			t.Logf("  Query with expired context returned: %v", err)
		} else {
			t.Logf("  Query with expired timeout context succeeded (engine may not check context)")
		}
		logResult(t, "ContextCancellation", nil)
	})

	// 12. Null handling
	t.Run("NullHandling", func(t *testing.T) {
		db := openDB(t)
		_, err := db.Exec("CREATE TABLE t_null(id INTEGER, name TEXT, age INTEGER, score DOUBLE, active BOOLEAN)")
		if err != nil {
			logResult(t, "create table", err)
			return
		}
		// Insert a row with all NULLs except id
		_, err = db.Exec("INSERT INTO t_null VALUES (1, NULL, NULL, NULL, NULL)")
		if err != nil {
			logResult(t, "insert nulls", err)
			return
		}
		// Insert a row with all values
		_, err = db.Exec("INSERT INTO t_null VALUES (2, 'Bob', 25, 95.5, true)")
		if err != nil {
			logResult(t, "insert non-nulls", err)
			return
		}

		// Scan NULL row
		var (
			id     int
			name   sql.NullString
			age    sql.NullInt64
			score  sql.NullFloat64
			active sql.NullBool
		)
		err = db.QueryRow("SELECT id, name, age, score, active FROM t_null WHERE id = 1").Scan(&id, &name, &age, &score, &active)
		if err != nil {
			logResult(t, "scan null row", err)
			return
		}
		if name.Valid || age.Valid || score.Valid || active.Valid {
			logResult(t, "null check", fmt.Errorf("expected all NULLs, got name.Valid=%v age.Valid=%v score.Valid=%v active.Valid=%v",
				name.Valid, age.Valid, score.Valid, active.Valid))
			return
		}
		t.Logf("  NULL row: name.Valid=%v, age.Valid=%v, score.Valid=%v, active.Valid=%v", name.Valid, age.Valid, score.Valid, active.Valid)

		// Scan non-NULL row
		err = db.QueryRow("SELECT id, name, age, score, active FROM t_null WHERE id = 2").Scan(&id, &name, &age, &score, &active)
		if err != nil {
			logResult(t, "scan non-null row", err)
			return
		}
		if !name.Valid || !age.Valid || !score.Valid || !active.Valid {
			logResult(t, "non-null check", fmt.Errorf("expected all valid, got name.Valid=%v age.Valid=%v score.Valid=%v active.Valid=%v",
				name.Valid, age.Valid, score.Valid, active.Valid))
			return
		}
		if name.String != "Bob" || age.Int64 != 25 || score.Float64 != 95.5 || !active.Bool {
			logResult(t, "value check", fmt.Errorf("unexpected values: name=%s age=%d score=%f active=%v",
				name.String, age.Int64, score.Float64, active.Bool))
			return
		}
		t.Logf("  Non-NULL row: name=%s, age=%d, score=%.1f, active=%v", name.String, age.Int64, score.Float64, active.Bool)
		logResult(t, "NullHandling", nil)
	})
}
