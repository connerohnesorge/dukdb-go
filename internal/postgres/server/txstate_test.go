package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionState(t *testing.T) {
	tests := []struct {
		state    TransactionState
		expected string
		isValid  bool
	}{
		{TxStateIdle, "idle", true},
		{TxStateInTx, "in transaction", true},
		{TxStateFailed, "failed transaction", true},
		{TransactionState('X'), "unknown", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
			assert.Equal(t, tt.isValid, tt.state.IsValid())
		})
	}
}

func TestTransactionStateBytes(t *testing.T) {
	// Verify the byte values match PostgreSQL protocol
	assert.Equal(t, byte('I'), byte(TxStateIdle))
	assert.Equal(t, byte('T'), byte(TxStateInTx))
	assert.Equal(t, byte('E'), byte(TxStateFailed))
}

func TestGetTransactionState(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	t.Run("nil session returns idle", func(t *testing.T) {
		state := GetTransactionState(nil)
		assert.Equal(t, TxStateIdle, state)
	})

	t.Run("new session returns idle", func(t *testing.T) {
		session := NewSession(server, "user", "db", "127.0.0.1:12345")
		state := GetTransactionState(session)
		assert.Equal(t, TxStateIdle, state)
	})

	t.Run("session in transaction returns in_tx", func(t *testing.T) {
		session := NewSession(server, "user", "db", "127.0.0.1:12345")
		session.SetInTransaction(true)
		state := GetTransactionState(session)
		assert.Equal(t, TxStateInTx, state)
	})

	t.Run("session with aborted transaction returns failed", func(t *testing.T) {
		session := NewSession(server, "user", "db", "127.0.0.1:12345")
		session.SetInTransaction(true)
		session.SetTransactionAborted(true)
		state := GetTransactionState(session)
		assert.Equal(t, TxStateFailed, state)
	})

	t.Run("aborted but not in transaction returns idle", func(t *testing.T) {
		session := NewSession(server, "user", "db", "127.0.0.1:12345")
		session.SetTransactionAborted(true) // This shouldn't happen normally
		state := GetTransactionState(session)
		assert.Equal(t, TxStateIdle, state)
	})
}

func TestSessionGetTransactionState(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Test idle state
	assert.Equal(t, TxStateIdle, session.GetTransactionState())

	// Start transaction
	session.SetInTransaction(true)
	assert.Equal(t, TxStateInTx, session.GetTransactionState())

	// Abort transaction
	session.SetTransactionAborted(true)
	assert.Equal(t, TxStateFailed, session.GetTransactionState())

	// End transaction
	session.SetInTransaction(false)
	session.SetTransactionAborted(false)
	assert.Equal(t, TxStateIdle, session.GetTransactionState())
}

func TestNotice(t *testing.T) {
	t.Run("NewNotice with warning severity", func(t *testing.T) {
		notice := NewNotice(SeverityWarning, "test warning")
		assert.Equal(t, SeverityWarning, notice.Severity)
		assert.Equal(t, CodeWarning, notice.Code)
		assert.Equal(t, "test warning", notice.Message)
	})

	t.Run("NewNotice with other severity", func(t *testing.T) {
		notice := NewNotice(SeverityInfo, "test info")
		assert.Equal(t, SeverityInfo, notice.Severity)
		assert.Equal(t, CodeSuccessfulCompletion, notice.Code)
		assert.Equal(t, "test info", notice.Message)
	})

	t.Run("NewWarning", func(t *testing.T) {
		notice := NewWarning("warning message")
		assert.Equal(t, SeverityWarning, notice.Severity)
		assert.Equal(t, CodeWarning, notice.Code)
	})

	t.Run("NewInfo", func(t *testing.T) {
		notice := NewInfo("info message")
		assert.Equal(t, SeverityInfo, notice.Severity)
	})

	t.Run("NewDebug", func(t *testing.T) {
		notice := NewDebug("debug message")
		assert.Equal(t, SeverityDebug, notice.Severity)
	})

	t.Run("notice builder methods", func(t *testing.T) {
		notice := NewWarning("test").
			WithDetail("detail text").
			WithHint("hint text").
			WithPosition(42).
			WithWhere("function test_func").
			WithCode("01001")

		assert.Equal(t, "detail text", notice.Detail)
		assert.Equal(t, "hint text", notice.Hint)
		assert.Equal(t, 42, notice.Position)
		assert.Equal(t, "function test_func", notice.Where)
		assert.Equal(t, "01001", notice.Code)
	})
}

func TestNewErrTransactionAborted(t *testing.T) {
	err := NewErrTransactionAborted()
	assert.NotNil(t, err)
	assert.Equal(t, CodeInvalidTransactionState, err.Code)
	assert.Equal(t, ErrTransactionAbortedMessage, err.Message)
	assert.Contains(t, err.Hint, "ROLLBACK")
}

func TestGetCommandTagAllCommands(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	handler := NewHandler(server)

	tests := []struct {
		query        string
		rowsAffected int64
		expected     string
	}{
		// DML commands with row counts
		{"INSERT INTO t VALUES (1)", 1, "INSERT 0 1"},
		{"INSERT INTO t VALUES (1), (2), (3)", 3, "INSERT 0 3"},
		{"UPDATE t SET x = 1", 5, "UPDATE 5"},
		{"DELETE FROM t WHERE id = 1", 2, "DELETE 2"},
		{"MERGE INTO t USING s ON t.id = s.id", 10, "MERGE 10"},
		{"COPY t FROM 'file.csv'", 100, "COPY 100"},

		// Cursor operations
		{"FETCH 10 FROM cursor1", 10, "FETCH 10"},
		{"FETCH ALL FROM cursor1", 50, "FETCH 50"},
		{"MOVE 5 FROM cursor1", 5, "MOVE 5"},
		{"MOVE FORWARD ALL FROM cursor1", 100, "MOVE 100"},

		// CREATE commands
		{"CREATE TABLE t (id INT)", 0, "CREATE TABLE"},
		{"CREATE INDEX idx ON t(col)", 0, "CREATE INDEX"},
		{"CREATE VIEW v AS SELECT 1", 0, "CREATE VIEW"},
		{"CREATE MATERIALIZED VIEW mv AS SELECT 1", 0, "CREATE MATERIALIZED VIEW"},
		{"CREATE SCHEMA myschema", 0, "CREATE SCHEMA"},
		{"CREATE SEQUENCE seq", 0, "CREATE SEQUENCE"},
		{"CREATE DATABASE mydb", 0, "CREATE DATABASE"},
		{"CREATE TYPE mytype AS ENUM ('a', 'b')", 0, "CREATE TYPE"},
		{"CREATE FUNCTION f() RETURNS INT", 0, "CREATE FUNCTION"},
		{"CREATE PROCEDURE p()", 0, "CREATE PROCEDURE"},
		{"CREATE TRIGGER tr BEFORE INSERT ON t", 0, "CREATE TRIGGER"},
		{"CREATE EXTENSION pg_stat_statements", 0, "CREATE EXTENSION"},
		{"CREATE ROLE myrole", 0, "CREATE ROLE"},
		{"CREATE USER myuser", 0, "CREATE ROLE"},
		{"CREATE TABLESPACE ts LOCATION '/data'", 0, "CREATE TABLESPACE"},
		{"CREATE FOREIGN TABLE ft (col INT)", 0, "CREATE FOREIGN TABLE"},
		{"CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw", 0, "CREATE SERVER"},
		{"CREATE FOREIGN DATA WRAPPER myfdw", 0, "CREATE FOREIGN DATA WRAPPER"},
		{"CREATE AGGREGATE myagg(INT)", 0, "CREATE AGGREGATE"},
		{"CREATE OPERATOR +(INT, INT)", 0, "CREATE OPERATOR"},
		{"CREATE COLLATION mycoll FROM pg_catalog.default", 0, "CREATE COLLATION"},
		{"CREATE DOMAIN mydomain AS TEXT", 0, "CREATE DOMAIN"},
		{"CREATE RULE myrule AS ON INSERT TO t DO INSTEAD NOTHING", 0, "CREATE RULE"},
		{"CREATE POLICY mypolicy ON t", 0, "CREATE POLICY"},
		{"CREATE SOMETHING_UNKNOWN", 0, "CREATE"},

		// DROP commands
		{"DROP TABLE t", 0, "DROP TABLE"},
		{"DROP INDEX idx", 0, "DROP INDEX"},
		{"DROP VIEW v", 0, "DROP VIEW"},
		{"DROP MATERIALIZED VIEW mv", 0, "DROP MATERIALIZED VIEW"},
		{"DROP SCHEMA myschema", 0, "DROP SCHEMA"},
		{"DROP SEQUENCE seq", 0, "DROP SEQUENCE"},
		{"DROP DATABASE mydb", 0, "DROP DATABASE"},
		{"DROP TYPE mytype", 0, "DROP TYPE"},
		{"DROP FUNCTION f()", 0, "DROP FUNCTION"},
		{"DROP PROCEDURE p()", 0, "DROP PROCEDURE"},
		{"DROP TRIGGER tr ON t", 0, "DROP TRIGGER"},
		{"DROP EXTENSION pg_stat_statements", 0, "DROP EXTENSION"},
		{"DROP ROLE myrole", 0, "DROP ROLE"},
		{"DROP USER myuser", 0, "DROP ROLE"},
		{"DROP TABLESPACE ts", 0, "DROP TABLESPACE"},
		{"DROP FOREIGN TABLE ft", 0, "DROP FOREIGN TABLE"},
		{"DROP SERVER myserver", 0, "DROP SERVER"},
		{"DROP FOREIGN DATA WRAPPER myfdw", 0, "DROP FOREIGN DATA WRAPPER"},
		{"DROP AGGREGATE myagg(INT)", 0, "DROP AGGREGATE"},
		{"DROP OPERATOR +(INT, INT)", 0, "DROP OPERATOR"},
		{"DROP COLLATION mycoll", 0, "DROP COLLATION"},
		{"DROP DOMAIN mydomain", 0, "DROP DOMAIN"},
		{"DROP RULE myrule ON t", 0, "DROP RULE"},
		{"DROP POLICY mypolicy ON t", 0, "DROP POLICY"},
		{"DROP SOMETHING_UNKNOWN", 0, "DROP"},

		// ALTER commands
		{"ALTER TABLE t ADD COLUMN col INT", 0, "ALTER TABLE"},
		{"ALTER INDEX idx RENAME TO new_idx", 0, "ALTER INDEX"},
		{"ALTER VIEW v RENAME TO new_v", 0, "ALTER VIEW"},
		{"ALTER MATERIALIZED VIEW mv RENAME TO new_mv", 0, "ALTER MATERIALIZED VIEW"},
		{"ALTER SCHEMA s RENAME TO new_s", 0, "ALTER SCHEMA"},
		{"ALTER SEQUENCE seq RESTART", 0, "ALTER SEQUENCE"},
		{"ALTER DATABASE db SET param = value", 0, "ALTER DATABASE"},
		{"ALTER TYPE mytype ADD VALUE 'c'", 0, "ALTER TYPE"},
		{"ALTER FUNCTION f() OWNER TO admin", 0, "ALTER FUNCTION"},
		{"ALTER PROCEDURE p() OWNER TO admin", 0, "ALTER PROCEDURE"},
		{"ALTER TRIGGER tr ON t RENAME TO new_tr", 0, "ALTER TRIGGER"},
		{"ALTER EXTENSION ext UPDATE", 0, "ALTER EXTENSION"},
		{"ALTER ROLE myrole SET param = value", 0, "ALTER ROLE"},
		{"ALTER USER myuser SET param = value", 0, "ALTER ROLE"},
		{"ALTER TABLESPACE ts SET (random_page_cost = 1.0)", 0, "ALTER TABLESPACE"},
		{"ALTER FOREIGN TABLE ft ADD COLUMN col INT", 0, "ALTER FOREIGN TABLE"},
		{"ALTER SERVER myserver OPTIONS (SET host 'newhost')", 0, "ALTER SERVER"},
		{
			"ALTER FOREIGN DATA WRAPPER myfdw OPTIONS (SET wrapper 'new')",
			0,
			"ALTER FOREIGN DATA WRAPPER",
		},
		{"ALTER AGGREGATE myagg(INT) RENAME TO new_agg", 0, "ALTER AGGREGATE"},
		{"ALTER OPERATOR +(INT, INT) SET (restrict = myrestrict)", 0, "ALTER OPERATOR"},
		{"ALTER COLLATION mycoll RENAME TO new_coll", 0, "ALTER COLLATION"},
		{"ALTER DOMAIN mydomain SET DEFAULT 'test'", 0, "ALTER DOMAIN"},
		{"ALTER RULE myrule ON t RENAME TO new_rule", 0, "ALTER RULE"},
		{"ALTER POLICY mypolicy ON t RENAME TO new_policy", 0, "ALTER POLICY"},
		{
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO public",
			0,
			"ALTER DEFAULT PRIVILEGES",
		},

		// Transaction commands
		{"BEGIN", 0, "BEGIN"},
		{"BEGIN TRANSACTION", 0, "BEGIN"},
		{"BEGIN ISOLATION LEVEL SERIALIZABLE", 0, "BEGIN"},
		{"START TRANSACTION", 0, "START TRANSACTION"},
		{"START TRANSACTION ISOLATION LEVEL READ COMMITTED", 0, "START TRANSACTION"},
		{"COMMIT", 0, "COMMIT"},
		{"COMMIT TRANSACTION", 0, "COMMIT"},
		{"END", 0, "COMMIT"},
		{"END TRANSACTION", 0, "COMMIT"},
		{"ROLLBACK", 0, "ROLLBACK"},
		{"ROLLBACK TRANSACTION", 0, "ROLLBACK"},
		{"ROLLBACK TO SAVEPOINT sp1", 0, "ROLLBACK"},
		{"SAVEPOINT sp1", 0, "SAVEPOINT"},
		{"RELEASE SAVEPOINT sp1", 0, "RELEASE"},
		{"RELEASE sp1", 0, "RELEASE"},

		// Session commands
		{"SET search_path = public", 0, "SET"},
		{"SET LOCAL timezone = 'UTC'", 0, "SET"},
		{"RESET search_path", 0, "RESET"},
		{"RESET ALL", 0, "RESET"},
		{"DISCARD ALL", 0, "DISCARD ALL"},
		{"DISCARD PLANS", 0, "DISCARD PLANS"},
		{"DISCARD SEQUENCES", 0, "DISCARD SEQUENCES"},
		{"DISCARD TEMP", 0, "DISCARD TEMP"},
		{"DISCARD TEMPORARY", 0, "DISCARD TEMP"},

		// Cursor commands
		{"DECLARE cursor1 CURSOR FOR SELECT 1", 0, "DECLARE CURSOR"},
		{"CLOSE cursor1", 0, "CLOSE CURSOR"},
		{"CLOSE ALL", 0, "CLOSE CURSOR"},

		// Prepared statement commands
		{"PREPARE stmt AS SELECT 1", 0, "PREPARE"},
		{"EXECUTE stmt", 0, "EXECUTE"},
		{"EXECUTE stmt(1, 2)", 0, "EXECUTE"},
		{"DEALLOCATE stmt", 0, "DEALLOCATE"},
		{"DEALLOCATE ALL", 0, "DEALLOCATE"},

		// Utility commands
		{"TRUNCATE TABLE t", 0, "TRUNCATE TABLE"},
		{"TRUNCATE t, u, v", 0, "TRUNCATE TABLE"},
		{"GRANT SELECT ON t TO user1", 0, "GRANT"},
		{"GRANT ALL PRIVILEGES ON DATABASE db TO user1", 0, "GRANT"},
		{"REVOKE SELECT ON t FROM user1", 0, "REVOKE"},
		{"REVOKE ALL PRIVILEGES ON DATABASE db FROM user1", 0, "REVOKE"},
		{"VACUUM", 0, "VACUUM"},
		{"VACUUM FULL", 0, "VACUUM"},
		{"VACUUM t", 0, "VACUUM"},
		{"ANALYZE", 0, "ANALYZE"},
		{"ANALYZE t", 0, "ANALYZE"},
		{"CLUSTER t USING idx", 0, "CLUSTER"},
		{"CLUSTER", 0, "CLUSTER"},
		{"REINDEX TABLE t", 0, "REINDEX"},
		{"REINDEX DATABASE db", 0, "REINDEX"},
		{"CHECKPOINT", 0, "CHECKPOINT"},
		{"LOCK TABLE t IN ACCESS SHARE MODE", 0, "LOCK TABLE"},
		{"LOCK t, u IN EXCLUSIVE MODE", 0, "LOCK TABLE"},

		// EXPLAIN commands
		{"EXPLAIN SELECT 1", 0, "EXPLAIN"},
		{"EXPLAIN ANALYZE SELECT 1", 0, "EXPLAIN"},
		{"EXPLAIN (ANALYZE, BUFFERS) SELECT 1", 0, "EXPLAIN"},

		// Notification commands
		{"LISTEN channel1", 0, "LISTEN"},
		{"UNLISTEN channel1", 0, "UNLISTEN"},
		{"UNLISTEN *", 0, "UNLISTEN"},
		{"NOTIFY channel1", 0, "NOTIFY"},
		{"NOTIFY channel1, 'payload'", 0, "NOTIFY"},

		// Security and metadata commands
		{"COMMENT ON TABLE t IS 'test'", 0, "COMMENT"},
		{"COMMENT ON COLUMN t.c IS 'test'", 0, "COMMENT"},
		{"SECURITY LABEL ON TABLE t IS 'secret'", 0, "SECURITY LABEL"},

		// Materialized view commands
		{"REFRESH MATERIALIZED VIEW mv", 0, "REFRESH MATERIALIZED VIEW"},
		{"REFRESH MATERIALIZED VIEW CONCURRENTLY mv", 0, "REFRESH MATERIALIZED VIEW"},

		// Other commands
		{"LOAD 'plpgsql'", 0, "LOAD"},
		{"DO $$ BEGIN RAISE NOTICE 'test'; END $$", 0, "DO"},
		{"CALL myproc()", 0, "CALL"},
		{
			"IMPORT FOREIGN SCHEMA myschema FROM SERVER myserver INTO public",
			0,
			"IMPORT FOREIGN SCHEMA",
		},

		// Default case
		{"SOME_UNKNOWN_COMMAND", 0, "OK"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := handler.getCommandTag(tt.query, tt.rowsAffected)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAllowedInAbortedTransaction(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	handler := NewHandler(server)

	allowedCommands := []string{
		"ROLLBACK",
		"ROLLBACK TO SAVEPOINT sp1",
		"COMMIT",
		"COMMIT TRANSACTION",
		"END",
		"END TRANSACTION",
		"RELEASE",
		"RELEASE SAVEPOINT sp1",
	}

	for _, cmd := range allowedCommands {
		t.Run("allowed: "+cmd, func(t *testing.T) {
			result := handler.isAllowedInAbortedTransaction(cmd)
			assert.True(t, result, "command should be allowed in aborted transaction: %s", cmd)
		})
	}

	blockedCommands := []string{
		"SELECT 1",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t",
		"CREATE TABLE t (id INT)",
		"DROP TABLE t",
		"BEGIN",
		"SAVEPOINT sp2",
		"SET search_path = public",
	}

	for _, cmd := range blockedCommands {
		t.Run("blocked: "+cmd, func(t *testing.T) {
			result := handler.isAllowedInAbortedTransaction(cmd)
			assert.False(t, result, "command should be blocked in aborted transaction: %s", cmd)
		})
	}
}

func TestTransactionAbortedErrorRecovery(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Start a transaction
	session.SetInTransaction(true)
	assert.Equal(t, TxStateInTx, session.GetTransactionState())

	// Simulate an error occurring
	session.SetTransactionAborted(true)
	assert.Equal(t, TxStateFailed, session.GetTransactionState())

	// Rollback should clear the aborted state
	session.SetInTransaction(false)
	session.SetTransactionAborted(false)
	assert.Equal(t, TxStateIdle, session.GetTransactionState())
}
