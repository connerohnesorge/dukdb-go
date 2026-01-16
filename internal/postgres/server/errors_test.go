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

// =============================================================================
// Task 19.1: Comprehensive Engine Error to SQLSTATE Code Mapping Tests
// =============================================================================

func TestToPgErrorWithContext(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		query        string
		expectedCode string
		checkHint    bool
		checkTable   bool
		checkColumn  bool
	}{
		// Syntax errors
		{
			name:         "syntax error at or near token",
			err:          errors.New("syntax error at or near 'SELEC'"),
			expectedCode: CodeSyntaxError,
		},
		{
			name:         "unexpected token",
			err:          errors.New("unexpected token: WHERE"),
			expectedCode: CodeSyntaxError,
		},
		{
			name:         "parse error",
			err:          errors.New("parse error in SQL statement"),
			expectedCode: CodeSyntaxError,
		},

		// Undefined object errors with name extraction
		{
			name:         "table does not exist with name extraction",
			err:          errors.New("table 'users' does not exist"),
			expectedCode: CodeUndefinedTable,
			checkHint:    true,
			checkTable:   true,
		},
		{
			name:         "relation does not exist",
			err:          errors.New("relation 'orders' does not exist"),
			expectedCode: CodeUndefinedTable,
			checkHint:    true,
		},
		{
			name:         "column does not exist with name extraction",
			err:          errors.New("column 'email' does not exist"),
			expectedCode: CodeUndefinedColumn,
			checkHint:    true,
			checkColumn:  true,
		},
		{
			name:         "schema does not exist",
			err:          errors.New("schema 'myschema' does not exist"),
			expectedCode: CodeInvalidSchemaName,
		},
		{
			name:         "prepared statement does not exist",
			err:          errors.New("prepared statement 'stmt1' does not exist"),
			expectedCode: CodeUndefinedPStmt,
		},

		// Duplicate object errors
		{
			name:         "table already exists",
			err:          errors.New("table 'users' already exists"),
			expectedCode: CodeDuplicateTable,
			checkHint:    true,
		},
		{
			name:         "index already exists",
			err:          errors.New("index 'idx_users_email' already exists"),
			expectedCode: CodeDuplicateObject,
			checkHint:    true,
		},
		{
			name:         "schema already exists",
			err:          errors.New("schema 'myschema' already exists"),
			expectedCode: CodeDuplicateSchema,
			checkHint:    true,
		},

		// Constraint violations
		{
			name:         "unique constraint violation",
			err:          errors.New("duplicate key value violates unique constraint 'users_pkey'"),
			expectedCode: CodeUniqueViolation,
		},
		{
			name:         "foreign key violation",
			err:          errors.New("foreign key constraint 'fk_orders_user' violated"),
			expectedCode: CodeForeignKeyViolation,
		},
		{
			name:         "check constraint violation",
			err:          errors.New("check constraint 'positive_amount' violated"),
			expectedCode: CodeCheckViolation,
		},
		{
			name:         "not null constraint",
			err:          errors.New("null value in column 'name' violates not-null constraint"),
			expectedCode: CodeNotNullViolation,
			checkColumn:  true,
		},

		// Data type errors
		{
			name:         "invalid input syntax for type",
			err:          errors.New("invalid input syntax for type integer: 'abc'"),
			expectedCode: CodeInvalidTextRepresentation,
			checkHint:    true,
		},
		{
			name:         "cannot cast",
			err:          errors.New("cannot cast value from text to integer"),
			expectedCode: CodeCannotCoerce,
			checkHint:    true,
		},
		{
			name:         "numeric value out of range",
			err:          errors.New("numeric value out of range"),
			expectedCode: CodeNumericValueOutOfRange,
			checkHint:    true,
		},
		{
			name:         "integer overflow",
			err:          errors.New("integer overflow occurred"),
			expectedCode: CodeNumericValueOutOfRange,
		},
		{
			name:         "invalid date format",
			err:          errors.New("invalid date format"),
			expectedCode: CodeInvalidDatetimeFormat,
			checkHint:    true,
		},

		// Transaction errors
		{
			name:         "serialization failure",
			err:          errors.New("could not serialize access due to concurrent update"),
			expectedCode: CodeSerializationFailure,
			checkHint:    true,
		},
		{
			name:         "deadlock detected",
			err:          errors.New("deadlock detected"),
			expectedCode: CodeDeadlockDetected,
			checkHint:    true,
		},
		{
			name:         "read-only transaction",
			err:          errors.New("cannot execute in read-only transaction"),
			expectedCode: CodeReadOnlySQLTransaction,
		},

		// Connection errors
		{
			name:         "connection closed",
			err:          errors.New("connection was closed unexpectedly"),
			expectedCode: CodeConnectionFailure,
		},
		{
			name:         "too many connections",
			err:          errors.New("too many connections"),
			expectedCode: CodeTooManyConnections,
			checkHint:    true,
		},

		// Resource errors
		{
			name:         "out of memory",
			err:          errors.New("out of memory"),
			expectedCode: CodeOutOfMemory,
			checkHint:    true,
		},
		{
			name:         "memory limit exceeded",
			err:          errors.New("memory limit exceeded"),
			expectedCode: CodeOutOfMemory,
		},
		{
			name:         "disk full",
			err:          errors.New("disk full"),
			expectedCode: CodeDiskFull,
		},

		// Feature support
		{
			name:         "not supported",
			err:          errors.New("feature not supported"),
			expectedCode: CodeFeatureNotSupported,
		},
		{
			name:         "not implemented",
			err:          errors.New("not implemented yet"),
			expectedCode: CodeFeatureNotSupported,
		},

		// Query cancellation
		{
			name:         "query canceled",
			err:          errors.New("query canceled"),
			expectedCode: CodeQueryCanceled,
		},
		{
			name:         "statement timeout",
			err:          errors.New("statement timeout"),
			expectedCode: CodeQueryCanceled,
		},

		// Ambiguous references
		{
			name:         "ambiguous column",
			err:          errors.New("ambiguous column 'id'"),
			expectedCode: CodeAmbiguousColumn,
			checkHint:    true,
		},
		{
			name:         "ambiguous function",
			err:          errors.New("ambiguous function call"),
			expectedCode: CodeAmbiguousFunction,
		},

		// Grouping errors
		{
			name:         "must appear in GROUP BY",
			err:          errors.New("column must appear in GROUP BY clause"),
			expectedCode: CodeGroupingError,
			checkHint:    true,
		},

		// I/O errors
		{
			name:         "file not found",
			err:          errors.New("file not found: data.csv"),
			expectedCode: CodeIOError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgErrorWithContext(tt.err, tt.query)
			assert.NotNil(t, result)
			assert.Equal(t, tt.expectedCode, result.Code)

			if tt.checkHint {
				assert.NotEmpty(t, result.Hint, "Expected hint to be set")
			}
			if tt.checkTable {
				assert.NotEmpty(t, result.TableName, "Expected table name to be extracted")
			}
			if tt.checkColumn {
				assert.NotEmpty(t, result.ColumnName, "Expected column name to be extracted")
			}

			// All errors should have a routine set
			assert.NotEmpty(t, result.Routine, "Expected routine to be set")
		})
	}
}

func TestToPgErrorWithContextNil(t *testing.T) {
	result := ToPgErrorWithContext(nil, "SELECT 1")
	assert.Nil(t, result)
}

func TestToPgErrorWithContextAlreadyPgError(t *testing.T) {
	original := NewPgError(CodeUndefinedTable, "test error")
	result := ToPgErrorWithContext(original, "SELECT * FROM test")
	assert.Equal(t, original, result)
	assert.NotEmpty(t, result.Routine) // Routine should be added
}

// =============================================================================
// Task 19.2: Error Hints and Details Tests
// =============================================================================

func TestAddHintForError(t *testing.T) {
	tests := []struct {
		name        string
		pgErr       *PgError
		expectHint  bool
		existingHint string
	}{
		{
			name: "undefined table gets hint",
			pgErr: &PgError{
				Code:    CodeUndefinedTable,
				Message: "relation does not exist",
			},
			expectHint: true,
		},
		{
			name: "undefined column gets hint",
			pgErr: &PgError{
				Code:    CodeUndefinedColumn,
				Message: "column does not exist",
			},
			expectHint: true,
		},
		{
			name: "syntax error gets hint",
			pgErr: &PgError{
				Code:    CodeSyntaxError,
				Message: "syntax error near SELECT",
			},
			expectHint: true,
		},
		{
			name: "existing hint is preserved",
			pgErr: &PgError{
				Code:    CodeUndefinedTable,
				Message: "table does not exist",
				Hint:    "Custom hint",
			},
			expectHint: true,
			existingHint: "Custom hint",
		},
		{
			name: "error without matching pattern gets no hint",
			pgErr: &PgError{
				Code:    CodeInternalError,
				Message: "something weird happened",
			},
			expectHint: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AddHintForError(tt.pgErr)
			if tt.existingHint != "" {
				assert.Equal(t, tt.existingHint, result.Hint)
			} else if tt.expectHint {
				assert.NotEmpty(t, result.Hint)
			}
		})
	}
}

func TestAddHintForErrorNil(t *testing.T) {
	result := AddHintForError(nil)
	assert.Nil(t, result)
}

func TestSuggestSimilarNames(t *testing.T) {
	candidates := []string{"users", "orders", "products", "customers", "user_settings"}

	tests := []struct {
		name       string
		input      string
		threshold  float64
		expectAny  bool
		expectLen  int
	}{
		{
			name:      "exact match",
			input:     "users",
			threshold: 0.8,
			expectAny: true,
		},
		{
			name:      "typo - missing letter",
			input:     "user",
			threshold: 0.7,
			expectAny: true,
		},
		{
			name:      "typo - wrong letter",
			input:     "usars",
			threshold: 0.7,
			expectAny: true,
		},
		{
			name:      "no match - completely different",
			input:     "xyz",
			threshold: 0.8,
			expectAny: false,
		},
		{
			name:      "empty input",
			input:     "",
			threshold: 0.8,
			expectAny: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SuggestSimilarNames(tt.input, candidates, tt.threshold)
			if tt.expectAny {
				assert.NotEmpty(t, result)
			} else {
				assert.Empty(t, result)
			}
			// Never more than 3 suggestions
			assert.LessOrEqual(t, len(result), 3)
		})
	}
}

func TestSuggestSimilarNamesEmptyCandidates(t *testing.T) {
	result := SuggestSimilarNames("test", nil, 0.8)
	assert.Nil(t, result)

	result = SuggestSimilarNames("test", []string{}, 0.8)
	assert.Nil(t, result)
}

func TestCalculateSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		minSim   float64
		maxSim   float64
	}{
		{"identical strings", "hello", "hello", 1.0, 1.0},
		{"empty strings identical", "", "", 1.0, 1.0}, // empty strings are identical
		{"one empty", "hello", "", 0.0, 0.0},
		{"one letter difference", "hello", "hallo", 0.7, 0.9},
		{"completely different", "abc", "xyz", 0.0, 0.1},
		{"case difference", "hello", "HELLO", 0.0, 0.5}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := calculateSimilarity(tt.a, tt.b)
			assert.GreaterOrEqual(t, sim, tt.minSim)
			assert.LessOrEqual(t, sim, tt.maxSim)
		})
	}
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},
		{"hello", "", 5},
		{"", "hello", 5},
		{"hello", "hello", 0},
		{"hello", "hallo", 1},
		{"kitten", "sitting", 3},
		{"abc", "xyz", 3},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			result := levenshteinDistance(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Task 19.3: Error Context Tests
// =============================================================================

func TestPgErrorContextMethods(t *testing.T) {
	err := NewPgError(CodeSyntaxError, "test error")

	// Test WithWhere
	_ = err.WithWhere("PL/pgSQL function foo() line 10")
	assert.Equal(t, "PL/pgSQL function foo() line 10", err.Where)

	// Test WithRoutine
	_ = err.WithRoutine("execQuery")
	assert.Equal(t, "execQuery", err.Routine)

	// Test WithFile
	_ = err.WithFile("handler.go")
	assert.Equal(t, "handler.go", err.File)

	// Test WithLine
	_ = err.WithLine(42)
	assert.Equal(t, 42, err.Line)

	// Test WithDataType
	_ = err.WithDataType("integer")
	assert.Equal(t, "integer", err.DataTypeName)

	// Test WithInternalQuery
	_ = err.WithInternalQuery("SELECT 1")
	assert.Equal(t, "SELECT 1", err.InternalQuery)

	// Test WithInternalPosition
	_ = err.WithInternalPosition(5)
	assert.Equal(t, 5, err.InternalPosition)
}

func TestPgErrorAddContext(t *testing.T) {
	err := NewPgError(CodeSyntaxError, "test error")
	_ = err.AddContext()

	// AddContext should populate File, Line, and Routine
	assert.NotEmpty(t, err.File)
	assert.Greater(t, err.Line, 0)
	assert.NotEmpty(t, err.Routine)
}

func TestExtractPositionFromMessage(t *testing.T) {
	tests := []struct {
		name     string
		message  string
		expected int
	}{
		{"at position pattern", "error at position 42", 42},
		{"position colon pattern", "position: 100", 100},
		{"character pattern", "character 25 in query", 25},
		{"offset pattern", "offset 50", 50},
		{"no position", "some error without position", 0},
		{"empty message", "", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPositionFromMessage(tt.message)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Task 19.4: Error Logging Tests
// =============================================================================

func TestErrorLogLevel(t *testing.T) {
	assert.Equal(t, ErrorLogLevel(0), ErrorLogNone)
	assert.Equal(t, ErrorLogLevel(1), ErrorLogMinimal)
	assert.Equal(t, ErrorLogLevel(2), ErrorLogStandard)
	assert.Equal(t, ErrorLogLevel(3), ErrorLogVerbose)
	assert.Equal(t, ErrorLogLevel(4), ErrorLogDebug)

	// Verify ordering
	assert.Less(t, ErrorLogNone, ErrorLogMinimal)
	assert.Less(t, ErrorLogMinimal, ErrorLogStandard)
	assert.Less(t, ErrorLogStandard, ErrorLogVerbose)
	assert.Less(t, ErrorLogVerbose, ErrorLogDebug)
}

func TestNewErrorLogger(t *testing.T) {
	logger := NewErrorLogger(nil, ErrorLogVerbose)
	assert.NotNil(t, logger)
	assert.Equal(t, ErrorLogVerbose, logger.level)
	assert.True(t, logger.logQuery) // Verbose enables query logging
}

func TestErrorLoggerSetLevel(t *testing.T) {
	logger := NewErrorLogger(nil, ErrorLogMinimal)
	logger.SetLevel(ErrorLogDebug)
	assert.Equal(t, ErrorLogDebug, logger.level)
}

func TestErrorLoggerSetLogQuery(t *testing.T) {
	logger := NewErrorLogger(nil, ErrorLogMinimal)
	assert.False(t, logger.logQuery)
	logger.SetLogQuery(true)
	assert.True(t, logger.logQuery)
}

func TestGlobalErrorLogger(t *testing.T) {
	original := GetGlobalErrorLogger()
	defer SetGlobalErrorLogger(original)

	newLogger := NewErrorLogger(nil, ErrorLogDebug)
	SetGlobalErrorLogger(newLogger)
	assert.Equal(t, newLogger, GetGlobalErrorLogger())
}

func TestLogErrorNilInputs(t *testing.T) {
	logger := NewErrorLogger(nil, ErrorLogStandard)
	// Should not panic with nil error
	logger.LogError(nil, "SELECT 1")

	// Should not panic with nil logger
	var nilLogger *ErrorLogger
	nilLogger.LogError(NewPgError(CodeSyntaxError, "test"), "SELECT 1")
}

// =============================================================================
// Task 19.5: RAISE NOTICE/WARNING Support Tests
// =============================================================================

func TestRaiseNoticeCollector(t *testing.T) {
	nc := NewRaiseNoticeCollector()
	assert.NotNil(t, nc)
	assert.Equal(t, 0, nc.Count())

	// Raise a notice
	nc.RaiseNotice("Processing row %d", 1)
	assert.Equal(t, 1, nc.Count())

	// Get notices
	notices := nc.GetNotices()
	assert.Len(t, notices, 1)
	assert.Equal(t, SeverityNotice, notices[0].Severity)
	assert.Equal(t, "Processing row 1", notices[0].Message)

	// Clear notices
	nc.Clear()
	assert.Equal(t, 0, nc.Count())
}

func TestRaiseNoticeCollectorRaiseTypes(t *testing.T) {
	nc := NewRaiseNoticeCollector()

	// Test all raise methods
	nc.RaiseNotice("notice message")
	nc.RaiseWarning("warning message")
	nc.RaiseInfo("info message")
	nc.RaiseDebug("debug message")
	nc.RaiseLog("log message")

	notices := nc.GetNotices()
	assert.Len(t, notices, 5)

	assert.Equal(t, SeverityNotice, notices[0].Severity)
	assert.Equal(t, SeverityWarning, notices[1].Severity)
	assert.Equal(t, SeverityInfo, notices[2].Severity)
	assert.Equal(t, SeverityDebug, notices[3].Severity)
	assert.Equal(t, SeverityLog, notices[4].Severity)
}

func TestRaiseNoticeCollectorHandler(t *testing.T) {
	nc := NewRaiseNoticeCollector()

	var handlerCalled bool
	var receivedNotice *Notice

	nc.AddHandler(func(notice *Notice) {
		handlerCalled = true
		receivedNotice = notice
	})

	nc.RaiseWarning("test warning")

	assert.True(t, handlerCalled)
	assert.NotNil(t, receivedNotice)
	assert.Equal(t, "test warning", receivedNotice.Message)
	assert.Equal(t, SeverityWarning, receivedNotice.Severity)
}

func TestRaiseNoticeCollectorMultipleHandlers(t *testing.T) {
	nc := NewRaiseNoticeCollector()

	callCount := 0
	nc.AddHandler(func(notice *Notice) { callCount++ })
	nc.AddHandler(func(notice *Notice) { callCount++ })

	nc.RaiseNotice("test")

	assert.Equal(t, 2, callCount)
}

func TestNoticeToPgError(t *testing.T) {
	notice := NewNotice(SeverityWarning, "test warning").
		WithDetail("more details").
		WithHint("try this").
		WithWhere("function foo()").
		WithPosition(10)

	pgErr := NoticeToPgError(notice)
	assert.NotNil(t, pgErr)
	assert.Equal(t, SeverityWarning, pgErr.Severity)
	assert.Equal(t, "test warning", pgErr.Message)
	assert.Equal(t, "more details", pgErr.Detail)
	assert.Equal(t, "try this", pgErr.Hint)
	assert.Equal(t, "function foo()", pgErr.Where)
	assert.Equal(t, 10, pgErr.Position)
}

// =============================================================================
// Additional Integration Tests
// =============================================================================

func TestErrorMappingRulesCompile(t *testing.T) {
	// Verify all error mapping rules have valid regex patterns
	for i, rule := range errorMappingRules {
		assert.NotNil(t, rule.Pattern, "Rule %d should have a pattern", i)
		assert.NotEmpty(t, rule.Code, "Rule %d should have a code", i)
	}
}

func TestErrorMappingRulesPriority(t *testing.T) {
	// Test that more specific patterns come before general ones
	// "syntax error at or near" should match before "syntax error"
	err := errors.New("syntax error at or near 'SELECT'")
	pgErr := ToPgErrorWithContext(err, "")
	assert.Equal(t, CodeSyntaxError, pgErr.Code)
}
