package server

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPgErrorBasic(t *testing.T) {
	err := NewPgError(CodeUndefinedTable, "relation \"test\" does not exist")

	assert.Equal(t, SeverityError, err.Severity)
	assert.Equal(t, CodeUndefinedTable, err.Code)
	assert.Equal(t, "relation \"test\" does not exist", err.Message)
	assert.Contains(t, err.Error(), CodeUndefinedTable)
	assert.Contains(t, err.Error(), "relation \"test\" does not exist")
}

func TestPgErrorWithDetail(t *testing.T) {
	err := NewPgErrorWithDetail(
		CodeUniqueViolation,
		"duplicate key value violates unique constraint",
		"Key (id)=(1) already exists.",
		"Try a different value",
	)

	assert.Equal(t, CodeUniqueViolation, err.Code)
	assert.Equal(t, "duplicate key value violates unique constraint", err.Message)
	assert.Equal(t, "Key (id)=(1) already exists.", err.Detail)
	assert.Equal(t, "Try a different value", err.Hint)
	assert.Contains(t, err.Error(), "DETAIL")
}

func TestPgErrorChainedMethods(t *testing.T) {
	err := NewPgError(CodeUndefinedColumn, "column \"foo\" does not exist").
		WithSeverity(SeverityWarning).
		WithDetail("The column was removed in a migration").
		WithHint("Check the column name").
		WithPosition(15).
		WithSchema("public").
		WithTable("users").
		WithColumn("foo")

	assert.Equal(t, SeverityWarning, err.Severity)
	assert.Equal(t, "The column was removed in a migration", err.Detail)
	assert.Equal(t, "Check the column name", err.Hint)
	assert.Equal(t, 15, err.Position)
	assert.Equal(t, "public", err.SchemaName)
	assert.Equal(t, "users", err.TableName)
	assert.Equal(t, "foo", err.ColumnName)
}

func TestPgErrorWithConstraint(t *testing.T) {
	err := NewPgError(CodeForeignKeyViolation, "foreign key violation").
		WithConstraint("fk_user_id")

	assert.Equal(t, "fk_user_id", err.ConstraintName)
}

func TestCommonErrorConstructors(t *testing.T) {
	tests := []struct {
		name     string
		err      *PgError
		code     string
		contains string
	}{
		{
			name:     "syntax error",
			err:      ErrSyntaxError("invalid syntax near 'FROM'", 25),
			code:     CodeSyntaxError,
			contains: "invalid syntax",
		},
		{
			name:     "undefined table",
			err:      ErrUndefinedTable("users"),
			code:     CodeUndefinedTable,
			contains: "users",
		},
		{
			name:     "undefined column",
			err:      ErrUndefinedColumn("email", "relation \"users\""),
			code:     CodeUndefinedColumn,
			contains: "email",
		},
		{
			name:     "undefined function",
			err:      ErrUndefinedFunction("my_func", []string{"integer", "text"}),
			code:     CodeUndefinedFunction,
			contains: "my_func",
		},
		{
			name:     "unique violation",
			err:      ErrUniqueViolation("users_pkey", "Key (id)=(1) already exists"),
			code:     CodeUniqueViolation,
			contains: "users_pkey",
		},
		{
			name:     "foreign key violation",
			err:      ErrForeignKeyViolation("fk_posts_user_id", "Key (user_id)=(999) not found"),
			code:     CodeForeignKeyViolation,
			contains: "fk_posts_user_id",
		},
		{
			name:     "not null violation",
			err:      ErrNotNullViolation("name", "users"),
			code:     CodeNotNullViolation,
			contains: "name",
		},
		{
			name:     "invalid text representation",
			err:      ErrInvalidTextRepresentation("integer", "abc"),
			code:     CodeInvalidTextRepresentation,
			contains: "abc",
		},
		{
			name:     "division by zero",
			err:      ErrDivisionByZero(),
			code:     CodeDivisionByZero,
			contains: "division by zero",
		},
		{
			name:     "serialization failure",
			err:      ErrSerializationFailure("concurrent update detected"),
			code:     CodeSerializationFailure,
			contains: "serialize",
		},
		{
			name:     "deadlock detected",
			err:      ErrDeadlockDetected("Process 1 waits for Process 2"),
			code:     CodeDeadlockDetected,
			contains: "deadlock",
		},
		{
			name:     "feature not supported",
			err:      ErrFeatureNotSupported("RETURNING clause"),
			code:     CodeFeatureNotSupported,
			contains: "not supported",
		},
		{
			name:     "no active transaction",
			err:      ErrNoActiveSQLTransaction(),
			code:     CodeNoActiveSQLTransaction,
			contains: "no transaction",
		},
		{
			name:     "active transaction",
			err:      ErrActiveSQLTransaction(),
			code:     CodeActiveSQLTransaction,
			contains: "already",
		},
		{
			name:     "read-only transaction",
			err:      ErrReadOnlySQLTransaction(),
			code:     CodeReadOnlySQLTransaction,
			contains: "read-only",
		},
		{
			name:     "connection failure",
			err:      ErrConnectionFailure("server closed connection"),
			code:     CodeConnectionFailure,
			contains: "connection failure",
		},
		{
			name:     "invalid password",
			err:      ErrInvalidPassword("admin"),
			code:     CodeInvalidPassword,
			contains: "admin",
		},
		{
			name:     "internal error",
			err:      ErrInternalError("unexpected state"),
			code:     CodeInternalError,
			contains: "internal error",
		},
		{
			name:     "query canceled",
			err:      ErrQueryCanceled(),
			code:     CodeQueryCanceled,
			contains: "canceling",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.code, tt.err.Code)
			assert.Contains(t, tt.err.Error(), tt.contains)
		})
	}
}

func TestToPgError(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode string
	}{
		{
			name:         "nil error",
			err:          nil,
			expectedCode: "",
		},
		{
			name:         "already PgError",
			err:          NewPgError(CodeUndefinedTable, "test"),
			expectedCode: CodeUndefinedTable,
		},
		{
			name:         "syntax error inference",
			err:          errors.New("syntax error at or near 'WHERE'"),
			expectedCode: CodeSyntaxError,
		},
		{
			name:         "table does not exist",
			err:          errors.New("table \"users\" does not exist"),
			expectedCode: CodeUndefinedTable,
		},
		{
			name:         "relation does not exist",
			err:          errors.New("relation \"users\" does not exist"),
			expectedCode: CodeUndefinedTable,
		},
		{
			name:         "column does not exist",
			err:          errors.New("column \"email\" does not exist"),
			expectedCode: CodeUndefinedColumn,
		},
		{
			name:         "function does not exist",
			err:          errors.New("function my_func() does not exist"),
			expectedCode: CodeUndefinedFunction,
		},
		{
			name:         "table already exists",
			err:          errors.New("table \"users\" already exists"),
			expectedCode: CodeDuplicateTable,
		},
		{
			name:         "object already exists",
			err:          errors.New("index \"idx\" already exists"),
			expectedCode: CodeDuplicateObject,
		},
		{
			name:         "duplicate key",
			err:          errors.New("duplicate key value violates unique constraint"),
			expectedCode: CodeUniqueViolation,
		},
		{
			name:         "unique constraint",
			err:          errors.New("violates unique constraint \"users_email_key\""),
			expectedCode: CodeUniqueViolation,
		},
		{
			name:         "foreign key violation",
			err:          errors.New("foreign key constraint violation"),
			expectedCode: CodeForeignKeyViolation,
		},
		{
			name:         "not null violation",
			err:          errors.New("null value in column violates not null constraint"),
			expectedCode: CodeNotNullViolation,
		},
		{
			name:         "division by zero",
			err:          errors.New("division by zero"),
			expectedCode: CodeDivisionByZero,
		},
		{
			name:         "permission denied",
			err:          errors.New("permission denied for table users"),
			expectedCode: CodeInsufficientPrivilege,
		},
		{
			name:         "access denied",
			err:          errors.New("access denied to schema public"),
			expectedCode: CodeInsufficientPrivilege,
		},
		{
			name:         "connection error",
			err:          errors.New("connection to server lost"),
			expectedCode: CodeConnectionException,
		},
		{
			name:         "serialization failure",
			err:          errors.New("could not serialize access due to concurrent update"),
			expectedCode: CodeSerializationFailure,
		},
		{
			name:         "deadlock",
			err:          errors.New("deadlock detected"),
			expectedCode: CodeDeadlockDetected,
		},
		{
			name:         "out of memory",
			err:          errors.New("out of memory"),
			expectedCode: CodeOutOfMemory,
		},
		{
			name:         "feature not supported",
			err:          errors.New("this feature is not supported"),
			expectedCode: CodeFeatureNotSupported,
		},
		{
			name:         "not implemented",
			err:          errors.New("operation not implemented"),
			expectedCode: CodeFeatureNotSupported,
		},
		{
			name:         "unknown error",
			err:          errors.New("some random error"),
			expectedCode: CodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgError(tt.err)
			if tt.err == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedCode, result.Code)
			}
		})
	}
}

func TestErrorCodeConstants(t *testing.T) {
	// Test that error codes have the expected format (5 characters)
	codes := []string{
		CodeSuccessfulCompletion,
		CodeWarning,
		CodeNoData,
		CodeConnectionException,
		CodeConnectionFailure,
		CodeFeatureNotSupported,
		CodeDataException,
		CodeSyntaxError,
		CodeUndefinedTable,
		CodeUndefinedColumn,
		CodeUniqueViolation,
		CodeForeignKeyViolation,
		CodeNotNullViolation,
		CodeSerializationFailure,
		CodeDeadlockDetected,
		CodeInternalError,
	}

	for _, code := range codes {
		assert.Len(t, code, 5, "Error code %s should be 5 characters", code)
	}
}

func TestSeverityConstants(t *testing.T) {
	severities := []string{
		SeverityError,
		SeverityFatal,
		SeverityPanic,
		SeverityWarning,
		SeverityNotice,
		SeverityDebug,
		SeverityInfo,
		SeverityLog,
	}

	for _, sev := range severities {
		assert.NotEmpty(t, sev)
	}

	// Verify standard severity values
	assert.Equal(t, "ERROR", SeverityError)
	assert.Equal(t, "FATAL", SeverityFatal)
	assert.Equal(t, "WARNING", SeverityWarning)
}
