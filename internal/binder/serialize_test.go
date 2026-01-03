package binder

import (
	"testing"

	"github.com/dukdb/dukdb-go/internal/parser"
)

func TestSerializeSelectStmt(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple select with qualified columns",
			sql:      "SELECT users.id, users.name FROM users",
			expected: "SELECT users.id, users.name FROM users",
		},
		{
			name:     "select with join and qualified columns",
			sql:      "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
			expected: "SELECT users.name, posts.title FROM users JOIN posts ON (users.id = posts.user_id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Serialize it
			serialized := serializeSelectStmt(selectStmt)

			// Verify it can be parsed again
			_, err = parser.Parse(serialized)
			if err != nil {
				t.Errorf("Serialized SQL failed to parse: %v\nSerialized: %s", err, serialized)
			}

			t.Logf("Original:   %s", tt.sql)
			t.Logf("Serialized: %s", serialized)
		})
	}
}

func TestSerializeAndReparse(t *testing.T) {
	// Test that we can serialize and then reparse a view definition
	sql := "SELECT id, name FROM users WHERE active = TRUE"
	
	// Parse
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	
	selectStmt := stmt.(*parser.SelectStmt)
	
	// Serialize
	serialized := serializeSelectStmt(selectStmt)
	t.Logf("Original:   %s", sql)
	t.Logf("Serialized: %s", serialized)
	
	// Reparse
	reparsed, err := parser.Parse(serialized)
	if err != nil {
		t.Fatalf("Failed to reparse: %v", err)
	}
	
	// Verify it's still a SELECT
	_, ok := reparsed.(*parser.SelectStmt)
	if !ok {
		t.Errorf("Reparsed statement is not a SELECT: %T", reparsed)
	}
}
