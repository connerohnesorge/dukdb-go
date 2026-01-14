package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPreprocessQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "simple query",
			input:    "SELECT * FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "whitespace normalization",
			input:    "SELECT   *   FROM   test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "newline normalization",
			input:    "SELECT *\nFROM test\nWHERE id = 1",
			expected: "SELECT * FROM test WHERE id = 1",
		},
		{
			name:     "tab normalization",
			input:    "SELECT *\tFROM\ttest",
			expected: "SELECT * FROM test",
		},
		{
			name:     "leading and trailing whitespace",
			input:    "  SELECT * FROM test  ",
			expected: "SELECT * FROM test",
		},
		{
			name:     "line comment removal",
			input:    "SELECT * FROM test -- this is a comment",
			expected: "SELECT * FROM test",
		},
		{
			name:     "block comment removal",
			input:    "SELECT /* this is a block comment */ * FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "multiline block comment",
			input:    "SELECT * /* comment\nspanning\nlines */ FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "preserve string content",
			input:    "SELECT '--not a comment' FROM test",
			expected: "SELECT '--not a comment' FROM test",
		},
		{
			name:     "preserve string with block comment chars",
			input:    "SELECT '/* not a comment */' FROM test",
			expected: "SELECT '/* not a comment */' FROM test",
		},
		{
			name:     "preserve quoted identifier",
			input:    "SELECT \"column--name\" FROM test",
			expected: "SELECT \"column--name\" FROM test",
		},
		{
			name:     "escaped single quote",
			input:    "SELECT 'it''s a test' FROM test",
			expected: "SELECT 'it''s a test' FROM test",
		},
		{
			name:     "escaped double quote in identifier",
			input:    "SELECT \"col\"\"name\" FROM test",
			expected: "SELECT \"col\"\"name\" FROM test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := preprocessQuery(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStripComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no comments",
			input:    "SELECT * FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "line comment at end",
			input:    "SELECT * FROM test -- comment",
			expected: "SELECT * FROM test  ", // Extra space from comment replacement + trailing
		},
		{
			name:     "line comment in middle",
			input:    "SELECT * -- select all\nFROM test",
			expected: "SELECT *  \nFROM test", // Extra space from comment replacement
		},
		{
			name:     "block comment",
			input:    "SELECT /* comment */ * FROM test",
			expected: "SELECT   * FROM test", // Space from comment + original space
		},
		{
			name:     "nested looking block comment",
			input:    "SELECT /* outer /* inner */ * FROM test",
			expected: "SELECT   * FROM test", // Space from comment + original space
		},
		{
			name:     "string with line comment chars",
			input:    "INSERT INTO test VALUES ('value--with--dashes')",
			expected: "INSERT INTO test VALUES ('value--with--dashes')",
		},
		{
			name:     "string with block comment chars",
			input:    "INSERT INTO test VALUES ('/*value*/')",
			expected: "INSERT INTO test VALUES ('/*value*/')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripComments(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single statement without semicolon",
			input:    "SELECT * FROM test",
			expected: []string{"SELECT * FROM test"},
		},
		{
			name:     "single statement with semicolon",
			input:    "SELECT * FROM test;",
			expected: []string{"SELECT * FROM test"},
		},
		{
			name:     "two statements",
			input:    "SELECT 1; SELECT 2",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "three statements with trailing semicolon",
			input:    "SELECT 1; SELECT 2; SELECT 3;",
			expected: []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		},
		{
			name:     "semicolon in string literal",
			input:    "SELECT 'a;b' FROM test; SELECT 2",
			expected: []string{"SELECT 'a;b' FROM test", "SELECT 2"},
		},
		{
			name:     "semicolon in double-quoted identifier",
			input:    "SELECT \"col;name\" FROM test; SELECT 2",
			expected: []string{"SELECT \"col;name\" FROM test", "SELECT 2"},
		},
		{
			name:     "empty statements filtered",
			input:    "SELECT 1;; SELECT 2",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "whitespace only between statements",
			input:    "SELECT 1;   \n\t  ; SELECT 2",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "escaped quote in string",
			input:    "SELECT 'it''s a test;'; SELECT 2",
			expected: []string{"SELECT 'it''s a test;'", "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitStatements(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no change needed",
			input:    "SELECT * FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "multiple spaces",
			input:    "SELECT  *    FROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "tabs",
			input:    "SELECT\t*\tFROM\ttest",
			expected: "SELECT * FROM test",
		},
		{
			name:     "newlines",
			input:    "SELECT\n*\nFROM\ntest",
			expected: "SELECT * FROM test",
		},
		{
			name:     "carriage returns",
			input:    "SELECT\r\n*\r\nFROM test",
			expected: "SELECT * FROM test",
		},
		{
			name:     "preserve whitespace in string",
			input:    "SELECT 'hello  world' FROM test",
			expected: "SELECT 'hello  world' FROM test",
		},
		{
			name:     "preserve whitespace in identifier",
			input:    "SELECT \"col  name\" FROM test",
			expected: "SELECT \"col  name\" FROM test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeWhitespace(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCommandTagExtended(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	handler := NewHandler(server)

	tests := []struct {
		query        string
		rowsAffected int64
		expected     string
	}{
		// Transaction commands
		{"START TRANSACTION", 0, "START TRANSACTION"},
		{"END", 0, "COMMIT"},
		{"SAVEPOINT sp1", 0, "SAVEPOINT"},
		{"RELEASE SAVEPOINT sp1", 0, "RELEASE"},

		// DDL commands
		{"CREATE DATABASE mydb", 0, "CREATE DATABASE"},
		{"CREATE TYPE mytype AS ENUM ('a', 'b')", 0, "CREATE TYPE"},
		{"CREATE FUNCTION myfunc() RETURNS INT", 0, "CREATE FUNCTION"},
		{"DROP DATABASE mydb", 0, "DROP DATABASE"},
		{"DROP TYPE mytype", 0, "DROP TYPE"},
		{"DROP FUNCTION myfunc", 0, "DROP FUNCTION"},

		// ALTER commands
		{"ALTER INDEX idx RENAME TO new_idx", 0, "ALTER INDEX"},
		{"ALTER VIEW v RENAME TO new_v", 0, "ALTER VIEW"},
		{"ALTER SCHEMA s RENAME TO new_s", 0, "ALTER SCHEMA"},
		{"ALTER SEQUENCE seq RESTART", 0, "ALTER SEQUENCE"},
		{"ALTER DATABASE db SET param = value", 0, "ALTER DATABASE"},

		// Utility commands
		{"TRUNCATE TABLE test", 0, "TRUNCATE TABLE"},
		{"GRANT SELECT ON test TO user", 0, "GRANT"},
		{"REVOKE SELECT ON test FROM user", 0, "REVOKE"},
		{"VACUUM", 0, "VACUUM"},
		{"ANALYZE", 0, "ANALYZE"},
		{"CLUSTER", 0, "CLUSTER"},
		{"REINDEX TABLE test", 0, "REINDEX"},
		{"LOCK TABLE test IN ACCESS SHARE MODE", 0, "LOCK TABLE"},
		{"COMMENT ON TABLE test IS 'comment'", 0, "COMMENT"},
		{"LISTEN channel", 0, "LISTEN"},
		{"UNLISTEN channel", 0, "UNLISTEN"},
		{"NOTIFY channel", 0, "NOTIFY"},
		{"PREPARE stmt AS SELECT 1", 0, "PREPARE"},
		{"EXECUTE stmt", 0, "EXECUTE"},
		{"DEALLOCATE stmt", 0, "DEALLOCATE"},
		{"RESET search_path", 0, "RESET"},

		// MERGE command
		{"MERGE INTO t USING s ON t.id = s.id", 5, "MERGE 5"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := handler.getCommandTag(tt.query, tt.rowsAffected)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSessionVariables(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Test session variables
	session.SetVariable("search_path", "public")
	val, ok := session.GetVariable("search_path")
	assert.True(t, ok)
	assert.Equal(t, "public", val)

	// Test local variables (transaction-scoped)
	session.SetInTransaction(true)
	session.SetLocalVariable("search_path", "private")
	val, ok = session.GetVariable("search_path")
	assert.True(t, ok)
	assert.Equal(t, "private", val) // Local takes precedence

	// Clear local variables
	session.ClearLocalVariables()
	val, ok = session.GetVariable("search_path")
	assert.True(t, ok)
	assert.Equal(t, "public", val) // Back to session variable

	// Test delete variable
	session.DeleteVariable("search_path")
	_, ok = session.GetVariable("search_path")
	assert.False(t, ok)

	// Test reset variables
	session.SetVariable("var1", "value1")
	session.SetVariable("var2", "value2")
	session.ResetVariables()
	_, ok = session.GetVariable("var1")
	assert.False(t, ok)
	_, ok = session.GetVariable("var2")
	assert.False(t, ok)
}

func TestSessionTransactionState(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	assert.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Test isolation level
	assert.Equal(t, "read committed", session.GetIsolationLevel())
	session.SetIsolationLevel("serializable")
	assert.Equal(t, "serializable", session.GetIsolationLevel())

	// Test transaction aborted state
	assert.False(t, session.IsTransactionAborted())
	session.SetTransactionAborted(true)
	assert.True(t, session.IsTransactionAborted())
	session.SetTransactionAborted(false)
	assert.False(t, session.IsTransactionAborted())

	// Test GetAllVariables
	session.SetVariable("var1", "value1")
	session.SetVariable("var2", "value2")
	session.SetInTransaction(true)
	session.SetLocalVariable("var2", "local_value2")
	session.SetLocalVariable("var3", "local_value3")

	allVars := session.GetAllVariables()
	assert.Equal(t, "value1", allVars["var1"])
	assert.Equal(t, "local_value2", allVars["var2"]) // Local takes precedence
	assert.Equal(t, "local_value3", allVars["var3"])
}
