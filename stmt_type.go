package dukdb

// StmtTypeName returns the string name of a statement type.
func StmtTypeName(t StmtType) string {
	switch t {
	case STATEMENT_TYPE_INVALID:
		return "INVALID"
	case STATEMENT_TYPE_SELECT:
		return "SELECT"
	case STATEMENT_TYPE_INSERT:
		return "INSERT"
	case STATEMENT_TYPE_UPDATE:
		return "UPDATE"
	case STATEMENT_TYPE_EXPLAIN:
		return "EXPLAIN"
	case STATEMENT_TYPE_DELETE:
		return "DELETE"
	case STATEMENT_TYPE_PREPARE:
		return "PREPARE"
	case STATEMENT_TYPE_CREATE:
		return "CREATE"
	case STATEMENT_TYPE_EXECUTE:
		return "EXECUTE"
	case STATEMENT_TYPE_ALTER:
		return "ALTER"
	case STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION"
	case STATEMENT_TYPE_COPY:
		return "COPY"
	case STATEMENT_TYPE_ANALYZE:
		return "ANALYZE"
	case STATEMENT_TYPE_VARIABLE_SET:
		return "VARIABLE_SET"
	case STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE_FUNC"
	case STATEMENT_TYPE_DROP:
		return "DROP"
	case STATEMENT_TYPE_EXPORT:
		return "EXPORT"
	case STATEMENT_TYPE_PRAGMA:
		return "PRAGMA"
	case STATEMENT_TYPE_VACUUM:
		return "VACUUM"
	case STATEMENT_TYPE_CALL:
		return "CALL"
	case STATEMENT_TYPE_SET:
		return "SET"
	case STATEMENT_TYPE_LOAD:
		return "LOAD"
	case STATEMENT_TYPE_RELATION:
		return "RELATION"
	case STATEMENT_TYPE_EXTENSION:
		return "EXTENSION"
	case STATEMENT_TYPE_LOGICAL_PLAN:
		return "LOGICAL_PLAN"
	case STATEMENT_TYPE_ATTACH:
		return "ATTACH"
	case STATEMENT_TYPE_DETACH:
		return "DETACH"
	case STATEMENT_TYPE_MULTI:
		return "MULTI"
	case STATEMENT_TYPE_MERGE_INTO:
		return "MERGE_INTO"
	case STATEMENT_TYPE_UPDATE_EXTENSIONS:
		return "UPDATE_EXTENSIONS"
	case STATEMENT_TYPE_COPY_DATABASE:
		return "COPY_DATABASE"
	default:
		return "UNKNOWN"
	}
}

// String returns the string representation of a statement type.
func (t StmtType) String() string {
	return StmtTypeName(t)
}

// ReturnType returns what kind of result the statement produces.
func (t StmtType) ReturnType() StmtReturnType {
	switch t {
	case STATEMENT_TYPE_SELECT, STATEMENT_TYPE_EXPLAIN,
		STATEMENT_TYPE_PRAGMA, STATEMENT_TYPE_CALL,
		STATEMENT_TYPE_RELATION, STATEMENT_TYPE_LOGICAL_PLAN:
		return RETURN_QUERY_RESULT

	case STATEMENT_TYPE_INSERT, STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE, STATEMENT_TYPE_MERGE_INTO,
		STATEMENT_TYPE_COPY:
		return RETURN_CHANGED_ROWS

	default:
		return RETURN_NOTHING
	}
}

// IsDML returns true for INSERT, UPDATE, DELETE, MERGE statements.
func (t StmtType) IsDML() bool {
	switch t {
	case STATEMENT_TYPE_INSERT, STATEMENT_TYPE_UPDATE,
		STATEMENT_TYPE_DELETE, STATEMENT_TYPE_MERGE_INTO:
		return true
	default:
		return false
	}
}

// IsDDL returns true for CREATE, DROP, ALTER statements.
func (t StmtType) IsDDL() bool {
	switch t {
	case STATEMENT_TYPE_CREATE, STATEMENT_TYPE_DROP,
		STATEMENT_TYPE_ALTER, STATEMENT_TYPE_CREATE_FUNC:
		return true
	default:
		return false
	}
}

// IsQuery returns true for statements that return result sets.
func (t StmtType) IsQuery() bool {
	return t.ReturnType() == RETURN_QUERY_RESULT
}

// ModifiesData returns true if statement writes to database.
func (t StmtType) ModifiesData() bool {
	return t.IsDML() || t.IsDDL()
}

// IsTransaction returns true for BEGIN, COMMIT, ROLLBACK.
func (t StmtType) IsTransaction() bool {
	return t == STATEMENT_TYPE_TRANSACTION
}

// String returns the string representation of a return type.
func (t StmtReturnType) String() string {
	switch t {
	case RETURN_QUERY_RESULT:
		return "QUERY_RESULT"
	case RETURN_CHANGED_ROWS:
		return "CHANGED_ROWS"
	case RETURN_NOTHING:
		return "NOTHING"
	default:
		return "UNKNOWN"
	}
}
