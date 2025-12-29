package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmtTypeName(t *testing.T) {
	tests := []struct {
		stmtType StmtType
		expected string
	}{
		{STATEMENT_TYPE_INVALID, "INVALID"},
		{STATEMENT_TYPE_SELECT, "SELECT"},
		{STATEMENT_TYPE_INSERT, "INSERT"},
		{STATEMENT_TYPE_UPDATE, "UPDATE"},
		{STATEMENT_TYPE_EXPLAIN, "EXPLAIN"},
		{STATEMENT_TYPE_DELETE, "DELETE"},
		{STATEMENT_TYPE_PREPARE, "PREPARE"},
		{STATEMENT_TYPE_CREATE, "CREATE"},
		{STATEMENT_TYPE_EXECUTE, "EXECUTE"},
		{STATEMENT_TYPE_ALTER, "ALTER"},
		{STATEMENT_TYPE_TRANSACTION, "TRANSACTION"},
		{STATEMENT_TYPE_COPY, "COPY"},
		{STATEMENT_TYPE_ANALYZE, "ANALYZE"},
		{STATEMENT_TYPE_VARIABLE_SET, "VARIABLE_SET"},
		{STATEMENT_TYPE_CREATE_FUNC, "CREATE_FUNC"},
		{STATEMENT_TYPE_DROP, "DROP"},
		{STATEMENT_TYPE_EXPORT, "EXPORT"},
		{STATEMENT_TYPE_PRAGMA, "PRAGMA"},
		{STATEMENT_TYPE_VACUUM, "VACUUM"},
		{STATEMENT_TYPE_CALL, "CALL"},
		{STATEMENT_TYPE_SET, "SET"},
		{STATEMENT_TYPE_LOAD, "LOAD"},
		{STATEMENT_TYPE_RELATION, "RELATION"},
		{STATEMENT_TYPE_EXTENSION, "EXTENSION"},
		{STATEMENT_TYPE_LOGICAL_PLAN, "LOGICAL_PLAN"},
		{STATEMENT_TYPE_ATTACH, "ATTACH"},
		{STATEMENT_TYPE_DETACH, "DETACH"},
		{STATEMENT_TYPE_MULTI, "MULTI"},
		{STATEMENT_TYPE_MERGE_INTO, "MERGE_INTO"},
		{STATEMENT_TYPE_UPDATE_EXTENSIONS, "UPDATE_EXTENSIONS"},
		{STATEMENT_TYPE_COPY_DATABASE, "COPY_DATABASE"},
		{StmtType(999), "UNKNOWN"}, // Unknown type
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, StmtTypeName(tc.stmtType))
			assert.Equal(t, tc.expected, tc.stmtType.String())
		})
	}
}

func TestStmtTypeReturnType(t *testing.T) {
	tests := []struct {
		stmtType     StmtType
		expectedType StmtReturnType
	}{
		// Query result types
		{STATEMENT_TYPE_SELECT, RETURN_QUERY_RESULT},
		{STATEMENT_TYPE_EXPLAIN, RETURN_QUERY_RESULT},
		{STATEMENT_TYPE_PRAGMA, RETURN_QUERY_RESULT},
		{STATEMENT_TYPE_CALL, RETURN_QUERY_RESULT},
		{STATEMENT_TYPE_RELATION, RETURN_QUERY_RESULT},
		{STATEMENT_TYPE_LOGICAL_PLAN, RETURN_QUERY_RESULT},

		// Changed rows types
		{STATEMENT_TYPE_INSERT, RETURN_CHANGED_ROWS},
		{STATEMENT_TYPE_UPDATE, RETURN_CHANGED_ROWS},
		{STATEMENT_TYPE_DELETE, RETURN_CHANGED_ROWS},
		{STATEMENT_TYPE_MERGE_INTO, RETURN_CHANGED_ROWS},
		{STATEMENT_TYPE_COPY, RETURN_CHANGED_ROWS},

		// Nothing types
		{STATEMENT_TYPE_CREATE, RETURN_NOTHING},
		{STATEMENT_TYPE_DROP, RETURN_NOTHING},
		{STATEMENT_TYPE_ALTER, RETURN_NOTHING},
		{STATEMENT_TYPE_PREPARE, RETURN_NOTHING},
		{STATEMENT_TYPE_EXECUTE, RETURN_NOTHING},
		{STATEMENT_TYPE_TRANSACTION, RETURN_NOTHING},
		{STATEMENT_TYPE_ANALYZE, RETURN_NOTHING},
		{STATEMENT_TYPE_VARIABLE_SET, RETURN_NOTHING},
		{STATEMENT_TYPE_CREATE_FUNC, RETURN_NOTHING},
		{STATEMENT_TYPE_EXPORT, RETURN_NOTHING},
		{STATEMENT_TYPE_VACUUM, RETURN_NOTHING},
		{STATEMENT_TYPE_SET, RETURN_NOTHING},
		{STATEMENT_TYPE_LOAD, RETURN_NOTHING},
		{STATEMENT_TYPE_EXTENSION, RETURN_NOTHING},
		{STATEMENT_TYPE_ATTACH, RETURN_NOTHING},
		{STATEMENT_TYPE_DETACH, RETURN_NOTHING},
		{STATEMENT_TYPE_MULTI, RETURN_NOTHING},
		{STATEMENT_TYPE_UPDATE_EXTENSIONS, RETURN_NOTHING},
		{STATEMENT_TYPE_COPY_DATABASE, RETURN_NOTHING},
	}

	for _, tc := range tests {
		t.Run(tc.stmtType.String(), func(t *testing.T) {
			assert.Equal(t, tc.expectedType, tc.stmtType.ReturnType())
		})
	}
}

func TestStmtTypeIsDML(t *testing.T) {
	dmlTypes := []StmtType{
		STATEMENT_TYPE_INSERT,
		STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE,
		STATEMENT_TYPE_MERGE_INTO,
	}

	nonDMLTypes := []StmtType{
		STATEMENT_TYPE_SELECT,
		STATEMENT_TYPE_CREATE,
		STATEMENT_TYPE_DROP,
		STATEMENT_TYPE_ALTER,
		STATEMENT_TYPE_EXPLAIN,
		STATEMENT_TYPE_PRAGMA,
	}

	for _, st := range dmlTypes {
		t.Run(st.String()+"_is_DML", func(t *testing.T) {
			assert.True(t, st.IsDML())
		})
	}

	for _, st := range nonDMLTypes {
		t.Run(st.String()+"_is_not_DML", func(t *testing.T) {
			assert.False(t, st.IsDML())
		})
	}
}

func TestStmtTypeIsDDL(t *testing.T) {
	ddlTypes := []StmtType{
		STATEMENT_TYPE_CREATE,
		STATEMENT_TYPE_DROP,
		STATEMENT_TYPE_ALTER,
		STATEMENT_TYPE_CREATE_FUNC,
	}

	nonDDLTypes := []StmtType{
		STATEMENT_TYPE_SELECT,
		STATEMENT_TYPE_INSERT,
		STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE,
		STATEMENT_TYPE_EXPLAIN,
		STATEMENT_TYPE_PRAGMA,
	}

	for _, st := range ddlTypes {
		t.Run(st.String()+"_is_DDL", func(t *testing.T) {
			assert.True(t, st.IsDDL())
		})
	}

	for _, st := range nonDDLTypes {
		t.Run(st.String()+"_is_not_DDL", func(t *testing.T) {
			assert.False(t, st.IsDDL())
		})
	}
}

func TestStmtTypeIsQuery(t *testing.T) {
	queryTypes := []StmtType{
		STATEMENT_TYPE_SELECT,
		STATEMENT_TYPE_EXPLAIN,
		STATEMENT_TYPE_PRAGMA,
		STATEMENT_TYPE_CALL,
		STATEMENT_TYPE_RELATION,
		STATEMENT_TYPE_LOGICAL_PLAN,
	}

	nonQueryTypes := []StmtType{
		STATEMENT_TYPE_INSERT,
		STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE,
		STATEMENT_TYPE_CREATE,
		STATEMENT_TYPE_DROP,
	}

	for _, st := range queryTypes {
		t.Run(st.String()+"_is_query", func(t *testing.T) {
			assert.True(t, st.IsQuery())
		})
	}

	for _, st := range nonQueryTypes {
		t.Run(st.String()+"_is_not_query", func(t *testing.T) {
			assert.False(t, st.IsQuery())
		})
	}
}

func TestStmtTypeModifiesData(t *testing.T) {
	modifyingTypes := []StmtType{
		STATEMENT_TYPE_INSERT,
		STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE,
		STATEMENT_TYPE_MERGE_INTO,
		STATEMENT_TYPE_CREATE,
		STATEMENT_TYPE_DROP,
		STATEMENT_TYPE_ALTER,
		STATEMENT_TYPE_CREATE_FUNC,
	}

	nonModifyingTypes := []StmtType{
		STATEMENT_TYPE_SELECT,
		STATEMENT_TYPE_EXPLAIN,
		STATEMENT_TYPE_PRAGMA,
	}

	for _, st := range modifyingTypes {
		t.Run(st.String()+"_modifies_data", func(t *testing.T) {
			assert.True(t, st.ModifiesData())
		})
	}

	for _, st := range nonModifyingTypes {
		t.Run(st.String()+"_does_not_modify_data", func(t *testing.T) {
			assert.False(t, st.ModifiesData())
		})
	}
}

func TestStmtTypeIsTransaction(t *testing.T) {
	assert.True(t, STATEMENT_TYPE_TRANSACTION.IsTransaction())
	assert.False(t, STATEMENT_TYPE_SELECT.IsTransaction())
	assert.False(t, STATEMENT_TYPE_INSERT.IsTransaction())
}

func TestStmtReturnTypeString(t *testing.T) {
	tests := []struct {
		returnType StmtReturnType
		expected   string
	}{
		{RETURN_QUERY_RESULT, "QUERY_RESULT"},
		{RETURN_CHANGED_ROWS, "CHANGED_ROWS"},
		{RETURN_NOTHING, "NOTHING"},
		{StmtReturnType(99), "UNKNOWN"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.returnType.String())
		})
	}
}

func TestAllStatementTypeConstantsExist(t *testing.T) {
	// Verify all 31 statement types (0-30) have defined constants
	expectedTypes := map[StmtType]string{
		STATEMENT_TYPE_INVALID:           "INVALID",
		STATEMENT_TYPE_SELECT:            "SELECT",
		STATEMENT_TYPE_INSERT:            "INSERT",
		STATEMENT_TYPE_UPDATE:            "UPDATE",
		STATEMENT_TYPE_EXPLAIN:           "EXPLAIN",
		STATEMENT_TYPE_DELETE:            "DELETE",
		STATEMENT_TYPE_PREPARE:           "PREPARE",
		STATEMENT_TYPE_CREATE:            "CREATE",
		STATEMENT_TYPE_EXECUTE:           "EXECUTE",
		STATEMENT_TYPE_ALTER:             "ALTER",
		STATEMENT_TYPE_TRANSACTION:       "TRANSACTION",
		STATEMENT_TYPE_COPY:              "COPY",
		STATEMENT_TYPE_ANALYZE:           "ANALYZE",
		STATEMENT_TYPE_VARIABLE_SET:      "VARIABLE_SET",
		STATEMENT_TYPE_CREATE_FUNC:       "CREATE_FUNC",
		STATEMENT_TYPE_DROP:              "DROP",
		STATEMENT_TYPE_EXPORT:            "EXPORT",
		STATEMENT_TYPE_PRAGMA:            "PRAGMA",
		STATEMENT_TYPE_VACUUM:            "VACUUM",
		STATEMENT_TYPE_CALL:              "CALL",
		STATEMENT_TYPE_SET:               "SET",
		STATEMENT_TYPE_LOAD:              "LOAD",
		STATEMENT_TYPE_RELATION:          "RELATION",
		STATEMENT_TYPE_EXTENSION:         "EXTENSION",
		STATEMENT_TYPE_LOGICAL_PLAN:      "LOGICAL_PLAN",
		STATEMENT_TYPE_ATTACH:            "ATTACH",
		STATEMENT_TYPE_DETACH:            "DETACH",
		STATEMENT_TYPE_MULTI:             "MULTI",
		STATEMENT_TYPE_MERGE_INTO:        "MERGE_INTO",
		STATEMENT_TYPE_UPDATE_EXTENSIONS: "UPDATE_EXTENSIONS",
		STATEMENT_TYPE_COPY_DATABASE:     "COPY_DATABASE",
	}

	assert.Equal(t, 31, len(expectedTypes), "Should have 31 statement types (0-30)")

	for stmtType, expectedName := range expectedTypes {
		assert.Equal(
			t,
			expectedName,
			StmtTypeName(stmtType),
			"StmtType %d should have name %s",
			stmtType,
			expectedName,
		)
	}
}
