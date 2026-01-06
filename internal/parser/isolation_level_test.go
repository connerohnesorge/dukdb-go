package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------- Isolation Level AST Tests ----------

// TestIsolationLevelConstants verifies that all isolation level constants are defined
// and have the expected values.
func TestIsolationLevelConstants(t *testing.T) {
	// Verify constants are defined with expected ordering
	// IsolationLevelSerializable is the default (zero value)
	assert.Equal(t, IsolationLevel(0), IsolationLevelSerializable, "IsolationLevelSerializable should be zero value (default)")
	assert.Equal(t, IsolationLevel(1), IsolationLevelRepeatableRead)
	assert.Equal(t, IsolationLevel(2), IsolationLevelReadCommitted)
	assert.Equal(t, IsolationLevel(3), IsolationLevelReadUncommitted)
}

// TestIsolationLevelString verifies that the String() method returns correct
// human-readable names for each isolation level.
func TestIsolationLevelString(t *testing.T) {
	tests := []struct {
		level    IsolationLevel
		expected string
	}{
		{IsolationLevelSerializable, "SERIALIZABLE"},
		{IsolationLevelRepeatableRead, "REPEATABLE READ"},
		{IsolationLevelReadCommitted, "READ COMMITTED"},
		{IsolationLevelReadUncommitted, "READ UNCOMMITTED"},
		{IsolationLevel(99), "UNKNOWN"}, // Unknown level
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.level.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBeginStmtIsolationLevel verifies that BeginStmt correctly stores
// the isolation level field.
func TestBeginStmtIsolationLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    IsolationLevel
		expected string
	}{
		{
			name:     "default isolation level (SERIALIZABLE)",
			level:    IsolationLevelSerializable,
			expected: "SERIALIZABLE",
		},
		{
			name:     "REPEATABLE READ",
			level:    IsolationLevelRepeatableRead,
			expected: "REPEATABLE READ",
		},
		{
			name:     "READ COMMITTED",
			level:    IsolationLevelReadCommitted,
			expected: "READ COMMITTED",
		},
		{
			name:     "READ UNCOMMITTED",
			level:    IsolationLevelReadUncommitted,
			expected: "READ UNCOMMITTED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &BeginStmt{
				IsolationLevel: tt.level,
			}

			assert.Equal(t, tt.level, stmt.IsolationLevel)
			assert.Equal(t, tt.expected, stmt.IsolationLevel.String())
		})
	}
}

// TestBeginStmtDefaultIsolationLevel verifies that a BeginStmt with no
// explicit isolation level defaults to SERIALIZABLE (zero value).
func TestBeginStmtDefaultIsolationLevel(t *testing.T) {
	// Create a BeginStmt without setting IsolationLevel
	stmt := &BeginStmt{}

	// Default should be SERIALIZABLE (zero value)
	assert.Equal(t, IsolationLevelSerializable, stmt.IsolationLevel)
	assert.Equal(t, "SERIALIZABLE", stmt.IsolationLevel.String())
}

// TestBeginStmtStmtNode verifies that BeginStmt implements the Statement interface.
func TestBeginStmtStmtNode(t *testing.T) {
	stmt := &BeginStmt{
		IsolationLevel: IsolationLevelReadCommitted,
	}

	// stmtNode() should not panic
	stmt.stmtNode()

	// Type() should return STATEMENT_TYPE_TRANSACTION
	assert.NotNil(t, stmt.Type())
}
