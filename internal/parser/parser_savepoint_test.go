package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSavepoint(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantErr  bool
	}{
		{
			name:     "Simple savepoint",
			sql:      "SAVEPOINT sp1",
			wantName: "sp1",
		},
		{
			name:     "Savepoint with underscore",
			sql:      "SAVEPOINT my_savepoint",
			wantName: "my_savepoint",
		},
		{
			name:     "Savepoint with numbers",
			sql:      "SAVEPOINT sp123",
			wantName: "sp123",
		},
		{
			name:     "Savepoint with quoted name",
			sql:      "SAVEPOINT 'my savepoint'",
			wantName: "my savepoint",
		},
		{
			name:     "Savepoint with semicolon",
			sql:      "SAVEPOINT sp1;",
			wantName: "sp1",
		},
		{
			name:    "Missing savepoint name",
			sql:     "SAVEPOINT",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			sp, ok := stmt.(*SavepointStmt)
			require.True(t, ok, "expected SavepointStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, sp.Name)
			assert.Equal(t, dukdb.STATEMENT_TYPE_TRANSACTION, sp.Type())
		})
	}
}

func TestParseRollbackToSavepoint(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantErr  bool
	}{
		{
			name:     "Simple rollback to savepoint",
			sql:      "ROLLBACK TO SAVEPOINT sp1",
			wantName: "sp1",
		},
		{
			name:     "Rollback to savepoint with underscore",
			sql:      "ROLLBACK TO SAVEPOINT my_savepoint",
			wantName: "my_savepoint",
		},
		{
			name:     "Rollback to savepoint with quoted name",
			sql:      "ROLLBACK TO SAVEPOINT 'my savepoint'",
			wantName: "my savepoint",
		},
		{
			name:     "Rollback to savepoint with semicolon",
			sql:      "ROLLBACK TO SAVEPOINT sp1;",
			wantName: "sp1",
		},
		{
			name:    "Missing SAVEPOINT keyword",
			sql:     "ROLLBACK TO sp1",
			wantErr: true,
		},
		{
			name:    "Missing savepoint name",
			sql:     "ROLLBACK TO SAVEPOINT",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			rb, ok := stmt.(*RollbackToSavepointStmt)
			require.True(t, ok, "expected RollbackToSavepointStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, rb.Name)
			assert.Equal(t, dukdb.STATEMENT_TYPE_TRANSACTION, rb.Type())
		})
	}
}

func TestParseReleaseSavepoint(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantErr  bool
	}{
		{
			name:     "Simple release savepoint",
			sql:      "RELEASE SAVEPOINT sp1",
			wantName: "sp1",
		},
		{
			name:     "Release savepoint with underscore",
			sql:      "RELEASE SAVEPOINT my_savepoint",
			wantName: "my_savepoint",
		},
		{
			name:     "Release savepoint with quoted name",
			sql:      "RELEASE SAVEPOINT 'my savepoint'",
			wantName: "my savepoint",
		},
		{
			name:     "Release savepoint with semicolon",
			sql:      "RELEASE SAVEPOINT sp1;",
			wantName: "sp1",
		},
		{
			name:    "Missing SAVEPOINT keyword",
			sql:     "RELEASE sp1",
			wantErr: true,
		},
		{
			name:    "Missing savepoint name",
			sql:     "RELEASE SAVEPOINT",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			rel, ok := stmt.(*ReleaseSavepointStmt)
			require.True(t, ok, "expected ReleaseSavepointStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, rel.Name)
			assert.Equal(t, dukdb.STATEMENT_TYPE_TRANSACTION, rel.Type())
		})
	}
}

func TestParseRollbackPlain(t *testing.T) {
	// Test that plain ROLLBACK still works after our changes
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "Simple ROLLBACK",
			sql:  "ROLLBACK",
		},
		{
			name: "ROLLBACK TRANSACTION",
			sql:  "ROLLBACK TRANSACTION",
		},
		{
			name: "ROLLBACK with semicolon",
			sql:  "ROLLBACK;",
		},
		{
			name: "ROLLBACK TRANSACTION with semicolon",
			sql:  "ROLLBACK TRANSACTION;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			rb, ok := stmt.(*RollbackStmt)
			require.True(t, ok, "expected RollbackStmt, got %T", stmt)

			assert.Equal(t, dukdb.STATEMENT_TYPE_TRANSACTION, rb.Type())
		})
	}
}

func TestSavepointStatementTypes(t *testing.T) {
	tests := []struct {
		sql      string
		stmtType dukdb.StmtType
		typeName string
	}{
		{"SAVEPOINT sp1", dukdb.STATEMENT_TYPE_TRANSACTION, "SavepointStmt"},
		{"ROLLBACK TO SAVEPOINT sp1", dukdb.STATEMENT_TYPE_TRANSACTION, "RollbackToSavepointStmt"},
		{"RELEASE SAVEPOINT sp1", dukdb.STATEMENT_TYPE_TRANSACTION, "ReleaseSavepointStmt"},
		{"ROLLBACK", dukdb.STATEMENT_TYPE_TRANSACTION, "RollbackStmt"},
		{"ROLLBACK TRANSACTION", dukdb.STATEMENT_TYPE_TRANSACTION, "RollbackStmt"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			assert.Equal(t, tt.stmtType, stmt.Type())
		})
	}
}
