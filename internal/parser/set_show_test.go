package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- SET Statement Parsing Tests ----------

// TestParseSetIsolationLevel tests parsing of SET statements for isolation levels.
func TestParseSetIsolationLevel(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		variable string
		value    string
		wantErr  bool
	}{
		{
			name:     "SET default_transaction_isolation to READ UNCOMMITTED (quoted)",
			sql:      "SET default_transaction_isolation = 'READ UNCOMMITTED'",
			variable: "default_transaction_isolation",
			value:    "READ UNCOMMITTED",
			wantErr:  false,
		},
		{
			name:     "SET default_transaction_isolation to READ COMMITTED (quoted)",
			sql:      "SET default_transaction_isolation = 'READ COMMITTED'",
			variable: "default_transaction_isolation",
			value:    "READ COMMITTED",
			wantErr:  false,
		},
		{
			name:     "SET default_transaction_isolation to REPEATABLE READ (quoted)",
			sql:      "SET default_transaction_isolation = 'REPEATABLE READ'",
			variable: "default_transaction_isolation",
			value:    "REPEATABLE READ",
			wantErr:  false,
		},
		{
			name:     "SET default_transaction_isolation to SERIALIZABLE (quoted)",
			sql:      "SET default_transaction_isolation = 'SERIALIZABLE'",
			variable: "default_transaction_isolation",
			value:    "SERIALIZABLE",
			wantErr:  false,
		},
		{
			name:     "SET with TO keyword",
			sql:      "SET default_transaction_isolation TO 'READ COMMITTED'",
			variable: "default_transaction_isolation",
			value:    "READ COMMITTED",
			wantErr:  false,
		},
		{
			name:     "SET with unquoted identifier value",
			sql:      "SET default_transaction_isolation = SERIALIZABLE",
			variable: "default_transaction_isolation",
			value:    "SERIALIZABLE",
			wantErr:  false,
		},
		{
			name:     "SET with multi-word unquoted value",
			sql:      "SET default_transaction_isolation = READ UNCOMMITTED",
			variable: "default_transaction_isolation",
			value:    "READ UNCOMMITTED",
			wantErr:  false,
		},
		{
			name:     "SET transaction_isolation (synonym)",
			sql:      "SET transaction_isolation = 'SERIALIZABLE'",
			variable: "transaction_isolation",
			value:    "SERIALIZABLE",
			wantErr:  false,
		},
		{
			name:     "SET with lowercase quoted value",
			sql:      "SET default_transaction_isolation = 'read committed'",
			variable: "default_transaction_isolation",
			value:    "read committed",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, stmt)

			setStmt, ok := stmt.(*SetStmt)
			require.True(t, ok, "expected *SetStmt, got %T", stmt)

			assert.Equal(t, tt.variable, setStmt.Variable)
			assert.Equal(t, tt.value, setStmt.Value)
		})
	}
}

// TestParseSetErrors tests error cases for SET statement parsing.
func TestParseSetErrors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "SET without variable name",
			sql:     "SET = 'value'",
			wantErr: true,
		},
		{
			name:    "SET without equals or TO",
			sql:     "SET default_transaction_isolation 'value'",
			wantErr: true,
		},
		{
			name:    "SET without value",
			sql:     "SET default_transaction_isolation =",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---------- SHOW Statement Parsing Tests ----------

// TestParseShowStatement tests parsing of SHOW statements.
func TestParseShowStatement(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		variable string
		wantErr  bool
	}{
		{
			name:     "SHOW transaction_isolation",
			sql:      "SHOW transaction_isolation",
			variable: "transaction_isolation",
			wantErr:  false,
		},
		{
			name:     "SHOW default_transaction_isolation",
			sql:      "SHOW default_transaction_isolation",
			variable: "default_transaction_isolation",
			wantErr:  false,
		},
		{
			name:     "SHOW with semicolon",
			sql:      "SHOW transaction_isolation;",
			variable: "transaction_isolation",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, stmt)

			showStmt, ok := stmt.(*ShowStmt)
			require.True(t, ok, "expected *ShowStmt, got %T", stmt)

			assert.Equal(t, tt.variable, showStmt.Variable)
		})
	}
}

// TestParseShowErrors tests error cases for SHOW statement parsing.
func TestParseShowErrors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "SHOW without variable name",
			sql:     "SHOW",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---------- Statement Type Tests ----------

// TestSetStmtType verifies that SetStmt returns correct statement type.
func TestSetStmtType(t *testing.T) {
	stmt := &SetStmt{
		Variable: "default_transaction_isolation",
		Value:    "SERIALIZABLE",
	}
	// SetStmt should not panic when calling stmtNode()
	stmt.stmtNode()
	// Type() should return STATEMENT_TYPE_SET
	assert.NotNil(t, stmt.Type())
}

// TestShowStmtType verifies that ShowStmt returns correct statement type.
func TestShowStmtType(t *testing.T) {
	stmt := &ShowStmt{
		Variable: "transaction_isolation",
	}
	// ShowStmt should not panic when calling stmtNode()
	stmt.stmtNode()
	// Type() should return STATEMENT_TYPE_SELECT (since it returns rows)
	assert.NotNil(t, stmt.Type())
}
