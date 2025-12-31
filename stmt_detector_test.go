package dukdb

import (
	"testing"
)

func TestDetectStatementType(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected StmtType
	}{
		// Basic statement types
		{
			name:     "SELECT",
			sql:      "SELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "SELECT lowercase",
			sql:      "select * from users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "SELECT mixed case",
			sql:      "SeLeCt * from users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "INSERT",
			sql:      "INSERT INTO users (name) VALUES ('John')",
			expected: STATEMENT_TYPE_INSERT,
		},
		{
			name:     "UPDATE",
			sql:      "UPDATE users SET name = 'Jane' WHERE id = 1",
			expected: STATEMENT_TYPE_UPDATE,
		},
		{
			name:     "DELETE",
			sql:      "DELETE FROM users WHERE id = 1",
			expected: STATEMENT_TYPE_DELETE,
		},
		{
			name:     "CREATE TABLE",
			sql:      "CREATE TABLE users (id INTEGER)",
			expected: STATEMENT_TYPE_CREATE,
		},
		{
			name:     "DROP TABLE",
			sql:      "DROP TABLE users",
			expected: STATEMENT_TYPE_DROP,
		},
		{
			name:     "ALTER TABLE",
			sql:      "ALTER TABLE users ADD COLUMN email VARCHAR",
			expected: STATEMENT_TYPE_ALTER,
		},
		{
			name:     "EXPLAIN",
			sql:      "EXPLAIN SELECT * FROM users",
			expected: STATEMENT_TYPE_EXPLAIN,
		},

		// Transaction statements
		{
			name:     "BEGIN",
			sql:      "BEGIN",
			expected: STATEMENT_TYPE_TRANSACTION,
		},
		{
			name:     "BEGIN TRANSACTION",
			sql:      "BEGIN TRANSACTION",
			expected: STATEMENT_TYPE_TRANSACTION,
		},
		{
			name:     "START TRANSACTION",
			sql:      "START TRANSACTION",
			expected: STATEMENT_TYPE_TRANSACTION,
		},
		{
			name:     "COMMIT",
			sql:      "COMMIT",
			expected: STATEMENT_TYPE_TRANSACTION,
		},
		{
			name:     "ROLLBACK",
			sql:      "ROLLBACK",
			expected: STATEMENT_TYPE_TRANSACTION,
		},

		// Other statements
		{
			name:     "COPY",
			sql:      "COPY users TO 'users.csv'",
			expected: STATEMENT_TYPE_COPY,
		},
		{
			name:     "ANALYZE",
			sql:      "ANALYZE users",
			expected: STATEMENT_TYPE_ANALYZE,
		},
		{
			name:     "SET",
			sql:      "SET threads TO 4",
			expected: STATEMENT_TYPE_SET,
		},
		{
			name:     "PRAGMA",
			sql:      "PRAGMA database_size",
			expected: STATEMENT_TYPE_PRAGMA,
		},
		{
			name:     "VACUUM",
			sql:      "VACUUM",
			expected: STATEMENT_TYPE_VACUUM,
		},
		{
			name:     "ATTACH",
			sql:      "ATTACH DATABASE 'test.db' AS test",
			expected: STATEMENT_TYPE_ATTACH,
		},
		{
			name:     "DETACH",
			sql:      "DETACH DATABASE test",
			expected: STATEMENT_TYPE_DETACH,
		},
		{
			name:     "MERGE",
			sql:      "MERGE INTO target USING source ON target.id = source.id",
			expected: STATEMENT_TYPE_MERGE_INTO,
		},

		// WITH clause (CTE) - should detect as SELECT
		{
			name:     "WITH CTE",
			sql:      "WITH cte AS (SELECT 1) SELECT * FROM cte",
			expected: STATEMENT_TYPE_SELECT,
		},

		// Whitespace handling
		{
			name:     "leading spaces",
			sql:      "   SELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "leading tabs",
			sql:      "\t\tSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "leading newlines",
			sql:      "\n\nSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "mixed whitespace",
			sql:      "  \t\n  SELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},

		// Comment handling
		{
			name:     "single-line comment --",
			sql:      "-- This is a comment\nSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "single-line comment //",
			sql:      "// This is a comment\nSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "multi-line comment",
			sql:      "/* This is\na multi-line\ncomment */\nSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "multiple comments",
			sql:      "-- First comment\n/* Second comment */\nSELECT * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},
		{
			name:     "inline comment after keyword",
			sql:      "SELECT /* comment */ * FROM users",
			expected: STATEMENT_TYPE_SELECT,
		},

		// Edge cases
		{
			name:     "empty string",
			sql:      "",
			expected: STATEMENT_TYPE_INVALID,
		},
		{
			name:     "only whitespace",
			sql:      "   \t\n   ",
			expected: STATEMENT_TYPE_INVALID,
		},
		{
			name:     "only comments",
			sql:      "-- just a comment",
			expected: STATEMENT_TYPE_INVALID,
		},
		{
			name:     "unknown keyword",
			sql:      "UNKNOWN_STATEMENT something",
			expected: STATEMENT_TYPE_INVALID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectStatementType(tt.sql)
			if result != tt.expected {
				t.Errorf("detectStatementType(%q) = %v, want %v",
					tt.sql, result, tt.expected)
			}
		})
	}
}

func TestExtractFirstKeyword(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple keyword",
			sql:      "SELECT * FROM users",
			expected: "SELECT",
		},
		{
			name:     "lowercase",
			sql:      "select * from users",
			expected: "SELECT",
		},
		{
			name:     "with leading whitespace",
			sql:      "   SELECT * FROM users",
			expected: "SELECT",
		},
		{
			name:     "with single-line comment",
			sql:      "-- comment\nSELECT * FROM users",
			expected: "SELECT",
		},
		{
			name:     "with multi-line comment",
			sql:      "/* comment */SELECT * FROM users",
			expected: "SELECT",
		},
		{
			name:     "empty input",
			sql:      "",
			expected: "",
		},
		{
			name:     "only whitespace",
			sql:      "   ",
			expected: "",
		},
		{
			name:     "only comment",
			sql:      "-- only comment",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFirstKeyword(tt.sql)
			if result != tt.expected {
				t.Errorf("extractFirstKeyword(%q) = %q, want %q",
					tt.sql, result, tt.expected)
			}
		})
	}
}
