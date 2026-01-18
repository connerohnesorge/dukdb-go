package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePragma(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantName  string
		wantArgs  int
		wantValue bool
		wantErr   bool
	}{
		{
			name:     "Simple pragma",
			sql:      "PRAGMA database_size",
			wantName: "database_size",
		},
		{
			name:     "Pragma with single argument",
			sql:      "PRAGMA table_info('users')",
			wantName: "table_info",
			wantArgs: 1,
		},
		{
			name:     "Pragma with multiple arguments",
			sql:      "PRAGMA storage_info('mytable', 'detail')",
			wantName: "storage_info",
			wantArgs: 2,
		},
		{
			name:      "Pragma with assignment",
			sql:       "PRAGMA max_memory = '2GB'",
			wantName:  "max_memory",
			wantValue: true,
		},
		{
			name:      "Pragma with numeric value",
			sql:       "PRAGMA threads = 4",
			wantName:  "threads",
			wantValue: true,
		},
		{
			name:     "Pragma functions",
			sql:      "PRAGMA functions",
			wantName: "functions",
		},
		{
			name:     "Pragma enable_profiling",
			sql:      "PRAGMA enable_profiling",
			wantName: "enable_profiling",
		},
		{
			name:    "Missing pragma name",
			sql:     "PRAGMA",
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

			pragma, ok := stmt.(*PragmaStmt)
			require.True(t, ok, "expected PragmaStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, pragma.Name)
			assert.Equal(t, tt.wantArgs, len(pragma.Args))
			if tt.wantValue {
				assert.NotNil(t, pragma.Value)
			} else {
				assert.Nil(t, pragma.Value)
			}
			assert.Equal(t, dukdb.STATEMENT_TYPE_PRAGMA, pragma.Type())
		})
	}
}

func TestParseExplain(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantAnalyze bool
		queryType   string
		wantErr     bool
	}{
		{
			name:        "Simple EXPLAIN SELECT",
			sql:         "EXPLAIN SELECT * FROM users",
			wantAnalyze: false,
			queryType:   "SelectStmt",
		},
		{
			name:        "EXPLAIN ANALYZE SELECT",
			sql:         "EXPLAIN ANALYZE SELECT * FROM users",
			wantAnalyze: true,
			queryType:   "SelectStmt",
		},
		{
			name:        "EXPLAIN with complex query",
			sql:         "EXPLAIN SELECT a, b FROM users WHERE id > 10 ORDER BY a",
			wantAnalyze: false,
			queryType:   "SelectStmt",
		},
		{
			name:        "EXPLAIN INSERT",
			sql:         "EXPLAIN INSERT INTO users (name) VALUES ('Alice')",
			wantAnalyze: false,
			queryType:   "InsertStmt",
		},
		{
			name:        "EXPLAIN UPDATE",
			sql:         "EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1",
			wantAnalyze: false,
			queryType:   "UpdateStmt",
		},
		{
			name:        "EXPLAIN DELETE",
			sql:         "EXPLAIN DELETE FROM users WHERE id = 1",
			wantAnalyze: false,
			queryType:   "DeleteStmt",
		},
		{
			name:        "EXPLAIN ANALYZE DELETE",
			sql:         "EXPLAIN ANALYZE DELETE FROM users WHERE id = 1",
			wantAnalyze: true,
			queryType:   "DeleteStmt",
		},
		{
			name:    "EXPLAIN without query",
			sql:     "EXPLAIN",
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

			explain, ok := stmt.(*ExplainStmt)
			require.True(t, ok, "expected ExplainStmt, got %T", stmt)

			assert.Equal(t, tt.wantAnalyze, explain.Analyze)
			assert.Equal(t, dukdb.STATEMENT_TYPE_EXPLAIN, explain.Type())

			// Check query type
			switch tt.queryType {
			case "SelectStmt":
				_, ok := explain.Query.(*SelectStmt)
				assert.True(t, ok, "expected SelectStmt, got %T", explain.Query)
			case "InsertStmt":
				_, ok := explain.Query.(*InsertStmt)
				assert.True(t, ok, "expected InsertStmt, got %T", explain.Query)
			case "UpdateStmt":
				_, ok := explain.Query.(*UpdateStmt)
				assert.True(t, ok, "expected UpdateStmt, got %T", explain.Query)
			case "DeleteStmt":
				_, ok := explain.Query.(*DeleteStmt)
				assert.True(t, ok, "expected DeleteStmt, got %T", explain.Query)
			}
		})
	}
}

func TestParseVacuum(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantTable  string
		wantSchema string
		wantErr    bool
	}{
		{
			name:      "VACUUM entire database",
			sql:       "VACUUM",
			wantTable: "",
		},
		{
			name:      "VACUUM specific table",
			sql:       "VACUUM users",
			wantTable: "users",
		},
		{
			name:       "VACUUM with schema",
			sql:        "VACUUM myschema.mytable",
			wantTable:  "mytable",
			wantSchema: "myschema",
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

			vacuum, ok := stmt.(*VacuumStmt)
			require.True(t, ok, "expected VacuumStmt, got %T", stmt)

			assert.Equal(t, tt.wantTable, vacuum.TableName)
			assert.Equal(t, tt.wantSchema, vacuum.Schema)
			assert.Equal(t, dukdb.STATEMENT_TYPE_VACUUM, vacuum.Type())
		})
	}
}

func TestParseAnalyze(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantTable  string
		wantSchema string
		wantErr    bool
	}{
		{
			name:      "ANALYZE entire database",
			sql:       "ANALYZE",
			wantTable: "",
		},
		{
			name:      "ANALYZE specific table",
			sql:       "ANALYZE users",
			wantTable: "users",
		},
		{
			name:       "ANALYZE with schema",
			sql:        "ANALYZE myschema.mytable",
			wantTable:  "mytable",
			wantSchema: "myschema",
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

			analyze, ok := stmt.(*AnalyzeStmt)
			require.True(t, ok, "expected AnalyzeStmt, got %T", stmt)

			assert.Equal(t, tt.wantTable, analyze.TableName)
			assert.Equal(t, tt.wantSchema, analyze.Schema)
			assert.Equal(t, dukdb.STATEMENT_TYPE_ANALYZE, analyze.Type())
		})
	}
}

func TestParseCheckpoint(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantDatabase string
		wantForce    bool
		wantErr      bool
	}{
		{
			name:         "Simple CHECKPOINT",
			sql:          "CHECKPOINT",
			wantDatabase: "",
			wantForce:    false,
		},
		{
			name:         "CHECKPOINT with database",
			sql:          "CHECKPOINT mydb",
			wantDatabase: "mydb",
			wantForce:    false,
		},
		{
			name:         "CHECKPOINT FORCE",
			sql:          "CHECKPOINT FORCE",
			wantDatabase: "",
			wantForce:    true,
		},
		{
			name:         "CHECKPOINT with database and FORCE",
			sql:          "CHECKPOINT mydb FORCE",
			wantDatabase: "mydb",
			wantForce:    true,
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

			checkpoint, ok := stmt.(*CheckpointStmt)
			require.True(t, ok, "expected CheckpointStmt, got %T", stmt)

			assert.Equal(t, tt.wantDatabase, checkpoint.Database)
			assert.Equal(t, tt.wantForce, checkpoint.Force)
			// CHECKPOINT uses TRANSACTION statement type
			assert.Equal(t, dukdb.STATEMENT_TYPE_TRANSACTION, checkpoint.Type())
		})
	}
}

func TestMaintenanceStatementTypes(t *testing.T) {
	tests := []struct {
		sql      string
		stmtType dukdb.StmtType
	}{
		{"PRAGMA database_size", dukdb.STATEMENT_TYPE_PRAGMA},
		{"EXPLAIN SELECT 1", dukdb.STATEMENT_TYPE_EXPLAIN},
		{"VACUUM", dukdb.STATEMENT_TYPE_VACUUM},
		{"ANALYZE", dukdb.STATEMENT_TYPE_ANALYZE},
		{"CHECKPOINT", dukdb.STATEMENT_TYPE_TRANSACTION},
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
