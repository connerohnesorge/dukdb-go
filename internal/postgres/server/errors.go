// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"fmt"
	"strings"
)

// PostgreSQL error severity levels.
const (
	SeverityError   = "ERROR"
	SeverityFatal   = "FATAL"
	SeverityPanic   = "PANIC"
	SeverityWarning = "WARNING"
	SeverityNotice  = "NOTICE"
	SeverityDebug   = "DEBUG"
	SeverityInfo    = "INFO"
	SeverityLog     = "LOG"
)

// PostgreSQL SQLSTATE error codes.
// These are standard PostgreSQL error codes following the SQL standard.
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	// Class 00 - Successful Completion
	CodeSuccessfulCompletion = "00000"

	// Class 01 - Warning
	CodeWarning = "01000"

	// Class 02 - No Data
	CodeNoData = "02000"

	// Class 03 - SQL Statement Not Yet Complete
	CodeSQLStatementNotYetComplete = "03000"

	// Class 08 - Connection Exception
	CodeConnectionException          = "08000"
	CodeConnectionDoesNotExist       = "08003"
	CodeConnectionFailure            = "08006"
	CodeSQLClientUnableToEstablishSQLConnection = "08001"
	CodeSQLServerRejectedEstablishmentOfSQLConnection = "08004"

	// Class 0A - Feature Not Supported
	CodeFeatureNotSupported = "0A000"

	// Class 22 - Data Exception
	CodeDataException                  = "22000"
	CodeNumericValueOutOfRange         = "22003"
	CodeInvalidTextRepresentation      = "22P02"
	CodeInvalidDatetimeFormat          = "22007"
	CodeDivisionByZero                 = "22012"
	CodeStringDataRightTruncation      = "22001"
	CodeNullValueNotAllowed            = "22004"

	// Class 23 - Integrity Constraint Violation
	CodeIntegrityConstraintViolation = "23000"
	CodeNotNullViolation             = "23502"
	CodeForeignKeyViolation          = "23503"
	CodeUniqueViolation              = "23505"
	CodeCheckViolation               = "23514"

	// Class 25 - Invalid Transaction State
	CodeInvalidTransactionState          = "25000"
	CodeActiveSQLTransaction             = "25001"
	CodeBranchTransactionAlreadyActive   = "25002"
	CodeInappropriateAccessModeForBranchTransaction = "25003"
	CodeInappropriateIsolationLevelForBranchTransaction = "25004"
	CodeNoActiveSQLTransaction           = "25P01"
	CodeReadOnlySQLTransaction           = "25006"

	// Class 28 - Invalid Authorization Specification
	CodeInvalidAuthorizationSpecification = "28000"
	CodeInvalidPassword                   = "28P01"

	// Class 34 - Invalid Cursor Name
	CodeInvalidCursorName = "34000"

	// Class 3D - Invalid Catalog Name
	CodeInvalidCatalogName = "3D000"

	// Class 3F - Invalid Schema Name
	CodeInvalidSchemaName = "3F000"

	// Class 40 - Transaction Rollback
	CodeTransactionRollback           = "40000"
	CodeSerializationFailure          = "40001"
	CodeTransactionIntegrityConstraintViolation = "40002"
	CodeStatementCompletionUnknown    = "40003"
	CodeDeadlockDetected              = "40P01"

	// Class 42 - Syntax Error or Access Rule Violation
	CodeSyntaxErrorOrAccessRuleViolation = "42000"
	CodeSyntaxError                      = "42601"
	CodeInsufficientPrivilege            = "42501"
	CodeCannotCoerce                     = "42846"
	CodeGroupingError                    = "42803"
	CodeWindowingError                   = "42P20"
	CodeInvalidRecursion                 = "42P19"
	CodeInvalidForeignKey                = "42830"
	CodeInvalidName                      = "42602"
	CodeNameTooLong                      = "42622"
	CodeReservedName                     = "42939"
	CodeDatatypeMismatch                 = "42804"
	CodeIndeterminateDatatype            = "42P18"
	CodeWrongObjectType                  = "42809"
	CodeUndefinedColumn                  = "42703"
	CodeUndefinedFunction                = "42883"
	CodeUndefinedTable                   = "42P01"
	CodeUndefinedParameter               = "42P02"
	CodeUndefinedObject                  = "42704"
	CodeUndefinedPStmt                   = "26000" // Invalid SQL statement name (prepared statement)
	CodeDuplicateColumn                  = "42701"
	CodeDuplicateCursor                  = "42P03"
	CodeDuplicateDatabase                = "42P04"
	CodeDuplicateFunction                = "42723"
	CodeDuplicatePreparedStatement       = "42P05"
	CodeDuplicateSchema                  = "42P06"
	CodeDuplicateTable                   = "42P07"
	CodeDuplicateAlias                   = "42712"
	CodeDuplicateObject                  = "42710"
	CodeAmbiguousColumn                  = "42702"
	CodeAmbiguousFunction                = "42725"
	CodeAmbiguousParameter               = "42P08"
	CodeAmbiguousAlias                   = "42P09"
	CodeInvalidColumnReference           = "42P10"
	CodeInvalidColumnDefinition          = "42611"
	CodeInvalidCursorDefinition          = "42P11"
	CodeInvalidDatabaseDefinition        = "42P12"
	CodeInvalidFunctionDefinition        = "42P13"
	CodeInvalidPreparedStatementDefinition = "42P14"
	CodeInvalidSchemaDefinition          = "42P15"
	CodeInvalidTableDefinition           = "42P16"
	CodeInvalidObjectDefinition          = "42P17"

	// Class 53 - Insufficient Resources
	CodeInsufficientResources = "53000"
	CodeDiskFull              = "53100"
	CodeOutOfMemory           = "53200"
	CodeTooManyConnections    = "53300"

	// Class 54 - Program Limit Exceeded
	CodeProgramLimitExceeded = "54000"
	CodeStatementTooComplex  = "54001"
	CodeTooManyColumns       = "54011"
	CodeTooManyArguments     = "54023"

	// Class 55 - Object Not In Prerequisite State
	CodeObjectNotInPrerequisiteState = "55000"
	CodeObjectInUse                  = "55006"

	// Class 57 - Operator Intervention
	CodeOperatorIntervention = "57000"
	CodeQueryCanceled        = "57014"
	CodeAdminShutdown        = "57P01"
	CodeCrashShutdown        = "57P02"

	// Class 58 - System Error
	CodeSystemError  = "58000"
	CodeIOError      = "58030"

	// Class XX - Internal Error
	CodeInternalError = "XX000"
)

// PgError represents a PostgreSQL protocol error with SQLSTATE codes.
// It implements the error interface and can be used to send properly
// formatted error responses to PostgreSQL clients.
type PgError struct {
	// Severity is the error severity level (ERROR, FATAL, WARNING, etc.)
	Severity string

	// Code is the SQLSTATE error code (e.g., "42P01" for undefined_table)
	Code string

	// Message is the primary error message
	Message string

	// Detail provides additional detail about the error
	Detail string

	// Hint provides a suggestion for how to fix the problem
	Hint string

	// Position is the cursor position in the query where the error occurred (1-indexed)
	// A value of 0 means the position is not available
	Position int

	// InternalPosition is like Position but for internally generated queries
	InternalPosition int

	// InternalQuery is the text of the internally generated query
	InternalQuery string

	// Where indicates the context in which the error occurred
	Where string

	// SchemaName is the name of the schema related to the error
	SchemaName string

	// TableName is the name of the table related to the error
	TableName string

	// ColumnName is the name of the column related to the error
	ColumnName string

	// DataTypeName is the name of the data type related to the error
	DataTypeName string

	// ConstraintName is the name of the constraint related to the error
	ConstraintName string

	// File is the source file where the error was reported
	File string

	// Line is the source line number where the error was reported
	Line int

	// Routine is the name of the source-code routine reporting the error
	Routine string
}

// Error implements the error interface.
func (e *PgError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s\nDETAIL: %s", e.Code, e.Message, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewPgError creates a new PgError with the given code and message.
func NewPgError(code, message string) *PgError {
	return &PgError{
		Severity: SeverityError,
		Code:     code,
		Message:  message,
	}
}

// NewPgErrorWithDetail creates a new PgError with detail and hint.
func NewPgErrorWithDetail(code, message, detail, hint string) *PgError {
	return &PgError{
		Severity: SeverityError,
		Code:     code,
		Message:  message,
		Detail:   detail,
		Hint:     hint,
	}
}

// WithSeverity sets the error severity.
func (e *PgError) WithSeverity(severity string) *PgError {
	e.Severity = severity
	return e
}

// WithDetail sets the error detail.
func (e *PgError) WithDetail(detail string) *PgError {
	e.Detail = detail
	return e
}

// WithHint sets the error hint.
func (e *PgError) WithHint(hint string) *PgError {
	e.Hint = hint
	return e
}

// WithPosition sets the cursor position.
func (e *PgError) WithPosition(position int) *PgError {
	e.Position = position
	return e
}

// WithSchema sets the schema name.
func (e *PgError) WithSchema(schema string) *PgError {
	e.SchemaName = schema
	return e
}

// WithTable sets the table name.
func (e *PgError) WithTable(table string) *PgError {
	e.TableName = table
	return e
}

// WithColumn sets the column name.
func (e *PgError) WithColumn(column string) *PgError {
	e.ColumnName = column
	return e
}

// WithConstraint sets the constraint name.
func (e *PgError) WithConstraint(constraint string) *PgError {
	e.ConstraintName = constraint
	return e
}

// Common error constructors for convenience.

// ErrSyntaxError creates a syntax error at the given position.
func ErrSyntaxError(message string, position int) *PgError {
	return NewPgError(CodeSyntaxError, message).WithPosition(position)
}

// ErrUndefinedTable creates an undefined table error.
func ErrUndefinedTable(tableName string) *PgError {
	return NewPgError(CodeUndefinedTable, fmt.Sprintf("relation \"%s\" does not exist", tableName)).
		WithTable(tableName)
}

// ErrUndefinedColumn creates an undefined column error.
func ErrUndefinedColumn(columnName, context string) *PgError {
	msg := fmt.Sprintf("column \"%s\" does not exist", columnName)
	if context != "" {
		msg = fmt.Sprintf("column \"%s\" of %s does not exist", columnName, context)
	}
	return NewPgError(CodeUndefinedColumn, msg).WithColumn(columnName)
}

// ErrUndefinedFunction creates an undefined function error.
func ErrUndefinedFunction(funcName string, argTypes []string) *PgError {
	args := strings.Join(argTypes, ", ")
	return NewPgError(CodeUndefinedFunction,
		fmt.Sprintf("function %s(%s) does not exist", funcName, args)).
		WithHint("No function matches the given name and argument types. You might need to add explicit type casts.")
}

// ErrUniqueViolation creates a unique constraint violation error.
func ErrUniqueViolation(constraintName, detail string) *PgError {
	return NewPgError(CodeUniqueViolation,
		fmt.Sprintf("duplicate key value violates unique constraint \"%s\"", constraintName)).
		WithDetail(detail).
		WithConstraint(constraintName)
}

// ErrForeignKeyViolation creates a foreign key violation error.
func ErrForeignKeyViolation(constraintName, detail string) *PgError {
	return NewPgError(CodeForeignKeyViolation,
		fmt.Sprintf("insert or update on table violates foreign key constraint \"%s\"", constraintName)).
		WithDetail(detail).
		WithConstraint(constraintName)
}

// ErrNotNullViolation creates a NOT NULL constraint violation error.
func ErrNotNullViolation(columnName, tableName string) *PgError {
	return NewPgError(CodeNotNullViolation,
		fmt.Sprintf("null value in column \"%s\" of relation \"%s\" violates not-null constraint", columnName, tableName)).
		WithColumn(columnName).
		WithTable(tableName)
}

// ErrInvalidTextRepresentation creates an invalid text representation error.
func ErrInvalidTextRepresentation(typeName, value string) *PgError {
	return NewPgError(CodeInvalidTextRepresentation,
		fmt.Sprintf("invalid input syntax for type %s: \"%s\"", typeName, value))
}

// ErrDivisionByZero creates a division by zero error.
func ErrDivisionByZero() *PgError {
	return NewPgError(CodeDivisionByZero, "division by zero")
}

// ErrSerializationFailure creates a serialization failure error.
func ErrSerializationFailure(detail string) *PgError {
	return NewPgError(CodeSerializationFailure,
		"could not serialize access due to concurrent update").
		WithDetail(detail)
}

// ErrDeadlockDetected creates a deadlock detection error.
func ErrDeadlockDetected(detail string) *PgError {
	return NewPgError(CodeDeadlockDetected, "deadlock detected").
		WithDetail(detail).
		WithHint("See server log for query details.")
}

// ErrFeatureNotSupported creates a feature not supported error.
func ErrFeatureNotSupported(feature string) *PgError {
	return NewPgError(CodeFeatureNotSupported, fmt.Sprintf("%s is not supported", feature))
}

// ErrNoActiveSQLTransaction creates a no active transaction error.
func ErrNoActiveSQLTransaction() *PgError {
	return NewPgError(CodeNoActiveSQLTransaction, "there is no transaction in progress")
}

// ErrActiveSQLTransaction creates an active transaction error.
func ErrActiveSQLTransaction() *PgError {
	return NewPgError(CodeActiveSQLTransaction, "there is already a transaction in progress")
}

// ErrReadOnlySQLTransaction creates a read-only transaction error.
func ErrReadOnlySQLTransaction() *PgError {
	return NewPgError(CodeReadOnlySQLTransaction, "cannot execute statement in a read-only transaction")
}

// ErrConnectionFailure creates a connection failure error.
func ErrConnectionFailure(detail string) *PgError {
	return NewPgError(CodeConnectionFailure, "connection failure").
		WithDetail(detail).
		WithSeverity(SeverityFatal)
}

// ErrInvalidPassword creates an invalid password error.
func ErrInvalidPassword(username string) *PgError {
	return NewPgError(CodeInvalidPassword,
		fmt.Sprintf("password authentication failed for user \"%s\"", username)).
		WithSeverity(SeverityFatal)
}

// ErrInternalError creates an internal error.
func ErrInternalError(detail string) *PgError {
	return NewPgError(CodeInternalError, "internal error").WithDetail(detail)
}

// ErrQueryCanceled creates a query canceled error.
func ErrQueryCanceled() *PgError {
	return NewPgError(CodeQueryCanceled, "canceling statement due to user request")
}

// ToPgError attempts to convert a standard error to a PgError.
// If the error is already a PgError, it returns it directly.
// Otherwise, it creates a new PgError based on the error message,
// attempting to infer an appropriate error code.
func ToPgError(err error) *PgError {
	if err == nil {
		return nil
	}

	// Check if it's already a PgError
	if pgErr, ok := err.(*PgError); ok {
		return pgErr
	}

	// Try to infer the error code from the message
	msg := err.Error()
	msgLower := strings.ToLower(msg)

	switch {
	case strings.Contains(msgLower, "syntax error"):
		return NewPgError(CodeSyntaxError, msg)
	case strings.Contains(msgLower, "does not exist"):
		if strings.Contains(msgLower, "table") || strings.Contains(msgLower, "relation") {
			return NewPgError(CodeUndefinedTable, msg)
		}
		if strings.Contains(msgLower, "column") {
			return NewPgError(CodeUndefinedColumn, msg)
		}
		if strings.Contains(msgLower, "function") {
			return NewPgError(CodeUndefinedFunction, msg)
		}
		return NewPgError(CodeUndefinedObject, msg)
	case strings.Contains(msgLower, "already exists"):
		if strings.Contains(msgLower, "table") {
			return NewPgError(CodeDuplicateTable, msg)
		}
		return NewPgError(CodeDuplicateObject, msg)
	case strings.Contains(msgLower, "duplicate key") || strings.Contains(msgLower, "unique constraint"):
		return NewPgError(CodeUniqueViolation, msg)
	case strings.Contains(msgLower, "foreign key"):
		return NewPgError(CodeForeignKeyViolation, msg)
	case strings.Contains(msgLower, "not null"):
		return NewPgError(CodeNotNullViolation, msg)
	case strings.Contains(msgLower, "division by zero"):
		return NewPgError(CodeDivisionByZero, msg)
	case strings.Contains(msgLower, "permission denied") || strings.Contains(msgLower, "access denied"):
		return NewPgError(CodeInsufficientPrivilege, msg)
	case strings.Contains(msgLower, "connection"):
		return NewPgError(CodeConnectionException, msg)
	case strings.Contains(msgLower, "serializ"):
		return NewPgError(CodeSerializationFailure, msg)
	case strings.Contains(msgLower, "deadlock"):
		return NewPgError(CodeDeadlockDetected, msg)
	case strings.Contains(msgLower, "out of memory"):
		return NewPgError(CodeOutOfMemory, msg)
	case strings.Contains(msgLower, "not supported") || strings.Contains(msgLower, "not implemented"):
		return NewPgError(CodeFeatureNotSupported, msg)
	default:
		// Default to internal error for unknown errors
		return NewPgError(CodeInternalError, msg)
	}
}
