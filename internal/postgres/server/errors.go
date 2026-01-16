// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"fmt"
	"log/slog"
	"regexp"
	"runtime"
	"strings"
	"sync"
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

// CastErrorInterface defines the interface for cast errors from the executor package.
// This allows ToPgError to detect cast errors without importing the executor package.
type CastErrorInterface interface {
	error
	GetSQLState() string
	GetMessage() string
	GetDetail() string
	GetHint() string
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

	// Check if it's a CastError (duck-typed to avoid import cycle)
	if castErr, ok := err.(CastErrorInterface); ok {
		pgErr := NewPgError(castErr.GetSQLState(), castErr.GetMessage())
		if detail := castErr.GetDetail(); detail != "" {
			pgErr.Detail = detail
		}
		if hint := castErr.GetHint(); hint != "" {
			pgErr.Hint = hint
		}
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
	// Handle cast/conversion errors based on message patterns
	case strings.Contains(msgLower, "invalid input syntax"):
		return NewPgError(CodeInvalidTextRepresentation, msg)
	case strings.Contains(msgLower, "out of range"):
		return NewPgError(CodeNumericValueOutOfRange, msg)
	case strings.Contains(msgLower, "invalid") && (strings.Contains(msgLower, "date") || strings.Contains(msgLower, "time") || strings.Contains(msgLower, "timestamp")):
		return NewPgError(CodeInvalidDatetimeFormat, msg)
	default:
		// Default to internal error for unknown errors
		return NewPgError(CodeInternalError, msg)
	}
}

// =============================================================================
// Task 19.1: Comprehensive Engine Error to SQLSTATE Code Mapping
// =============================================================================

// ErrorMappingRule defines a rule for mapping error messages to SQLSTATE codes.
type ErrorMappingRule struct {
	// Pattern is a regexp pattern to match against error messages
	Pattern *regexp.Regexp
	// Code is the SQLSTATE code to use when the pattern matches
	Code string
	// ExtractNames is true if the pattern contains capture groups for extracting names
	ExtractNames bool
	// HintTemplate is an optional hint template (can use $1, $2, etc. for captured groups)
	HintTemplate string
	// DetailTemplate is an optional detail template
	DetailTemplate string
}

// errorMappingRules contains comprehensive mapping rules for engine errors.
// Rules are checked in order; first match wins.
var errorMappingRules = []ErrorMappingRule{
	// Syntax errors
	{Pattern: regexp.MustCompile(`(?i)syntax error at or near ['"']?(\w+)['"']?`), Code: CodeSyntaxError, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)syntax error`), Code: CodeSyntaxError},
	{Pattern: regexp.MustCompile(`(?i)unexpected token`), Code: CodeSyntaxError},
	{Pattern: regexp.MustCompile(`(?i)expected .* but found`), Code: CodeSyntaxError},
	{Pattern: regexp.MustCompile(`(?i)invalid syntax`), Code: CodeSyntaxError},
	{Pattern: regexp.MustCompile(`(?i)parse error`), Code: CodeSyntaxError},

	// Undefined objects
	{Pattern: regexp.MustCompile(`(?i)table ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedTable, ExtractNames: true, HintTemplate: "Check the table name for typos or verify the table exists."},
	{Pattern: regexp.MustCompile(`(?i)relation ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedTable, ExtractNames: true, HintTemplate: "Check the relation name for typos or verify it exists."},
	{Pattern: regexp.MustCompile(`(?i)column ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedColumn, ExtractNames: true, HintTemplate: "Check the column name for typos or use SELECT * to see available columns."},
	{Pattern: regexp.MustCompile(`(?i)column ['"']?(\w+)['"']? of relation ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedColumn, ExtractNames: true, DetailTemplate: "Column \"$1\" is not present in table \"$2\"."},
	{Pattern: regexp.MustCompile(`(?i)function ['"']?(\w+)['"']?\s*\(([^)]*)\)\s*does not exist`), Code: CodeUndefinedFunction, ExtractNames: true, HintTemplate: "No function matches the given name and argument types. You might need to add explicit type casts."},
	{Pattern: regexp.MustCompile(`(?i)schema ['"']?(\w+)['"']? does not exist`), Code: CodeInvalidSchemaName, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)database ['"']?(\w+)['"']? does not exist`), Code: CodeInvalidCatalogName, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)index ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedObject, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)sequence ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedObject, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)view ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedTable, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)type ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedObject, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)prepared statement ['"']?(\w+)['"']? does not exist`), Code: CodeUndefinedPStmt, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)cursor ['"']?(\w+)['"']? does not exist`), Code: CodeInvalidCursorName, ExtractNames: true},

	// Duplicate objects
	{Pattern: regexp.MustCompile(`(?i)table ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateTable, ExtractNames: true, HintTemplate: "Use CREATE TABLE IF NOT EXISTS or DROP TABLE first."},
	{Pattern: regexp.MustCompile(`(?i)relation ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateTable, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)index ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateObject, ExtractNames: true, HintTemplate: "Use CREATE INDEX IF NOT EXISTS or DROP INDEX first."},
	{Pattern: regexp.MustCompile(`(?i)schema ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateSchema, ExtractNames: true, HintTemplate: "Use CREATE SCHEMA IF NOT EXISTS or DROP SCHEMA first."},
	{Pattern: regexp.MustCompile(`(?i)database ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateDatabase, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)column ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateColumn, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)function ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateFunction, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)sequence ['"']?(\w+)['"']? already exists`), Code: CodeDuplicateObject, ExtractNames: true},

	// Constraint violations
	{Pattern: regexp.MustCompile(`(?i)duplicate key.*violates unique constraint ['"']?(\w+)['"']?`), Code: CodeUniqueViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)unique constraint ['"']?(\w+)['"']? violated`), Code: CodeUniqueViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)unique constraint violation`), Code: CodeUniqueViolation},
	{Pattern: regexp.MustCompile(`(?i)foreign key constraint ['"']?(\w+)['"']? violated`), Code: CodeForeignKeyViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)violates foreign key constraint`), Code: CodeForeignKeyViolation},
	{Pattern: regexp.MustCompile(`(?i)check constraint ['"']?(\w+)['"']? violated`), Code: CodeCheckViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)violates check constraint`), Code: CodeCheckViolation},
	{Pattern: regexp.MustCompile(`(?i)not[- ]?null constraint.*column ['"']?(\w+)['"']?`), Code: CodeNotNullViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)null value in column ['"']?(\w+)['"']?.*not[- ]?null`), Code: CodeNotNullViolation, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)null value not allowed`), Code: CodeNotNullViolation},

	// Data type errors
	{Pattern: regexp.MustCompile(`(?i)invalid input syntax for (?:type )?(\w+):\s*['"']?([^'"]*)['"']?`), Code: CodeInvalidTextRepresentation, ExtractNames: true, HintTemplate: "Verify the value format matches the expected type \"$1\"."},
	{Pattern: regexp.MustCompile(`(?i)cannot cast.*from (\w+) to (\w+)`), Code: CodeCannotCoerce, ExtractNames: true, HintTemplate: "Use an explicit CAST expression or convert the data."},
	{Pattern: regexp.MustCompile(`(?i)type mismatch`), Code: CodeDatatypeMismatch},
	{Pattern: regexp.MustCompile(`(?i)cannot coerce`), Code: CodeCannotCoerce},
	{Pattern: regexp.MustCompile(`(?i)numeric value out of range`), Code: CodeNumericValueOutOfRange, HintTemplate: "Check that the value fits within the target type's range."},
	{Pattern: regexp.MustCompile(`(?i)integer out of range`), Code: CodeNumericValueOutOfRange},
	{Pattern: regexp.MustCompile(`(?i)value out of range`), Code: CodeNumericValueOutOfRange},
	{Pattern: regexp.MustCompile(`(?i)overflow`), Code: CodeNumericValueOutOfRange},
	{Pattern: regexp.MustCompile(`(?i)invalid date`), Code: CodeInvalidDatetimeFormat, HintTemplate: "Use ISO 8601 format: YYYY-MM-DD."},
	{Pattern: regexp.MustCompile(`(?i)invalid time`), Code: CodeInvalidDatetimeFormat, HintTemplate: "Use ISO 8601 format: HH:MM:SS."},
	{Pattern: regexp.MustCompile(`(?i)invalid timestamp`), Code: CodeInvalidDatetimeFormat, HintTemplate: "Use ISO 8601 format: YYYY-MM-DD HH:MM:SS."},
	{Pattern: regexp.MustCompile(`(?i)invalid interval`), Code: CodeInvalidDatetimeFormat},

	// Arithmetic errors
	{Pattern: regexp.MustCompile(`(?i)division by zero`), Code: CodeDivisionByZero, HintTemplate: "Add a check to avoid dividing by zero."},

	// Transaction errors
	{Pattern: regexp.MustCompile(`(?i)could not serialize access`), Code: CodeSerializationFailure, HintTemplate: "Retry the transaction."},
	{Pattern: regexp.MustCompile(`(?i)serialization failure`), Code: CodeSerializationFailure, HintTemplate: "Retry the transaction."},
	{Pattern: regexp.MustCompile(`(?i)deadlock detected`), Code: CodeDeadlockDetected, HintTemplate: "Retry the transaction. Consider reducing transaction size or reordering operations."},
	{Pattern: regexp.MustCompile(`(?i)no transaction in progress`), Code: CodeNoActiveSQLTransaction},
	{Pattern: regexp.MustCompile(`(?i)already.*transaction in progress`), Code: CodeActiveSQLTransaction},
	{Pattern: regexp.MustCompile(`(?i)read[- ]?only transaction`), Code: CodeReadOnlySQLTransaction},
	{Pattern: regexp.MustCompile(`(?i)transaction aborted`), Code: CodeTransactionRollback},

	// Connection errors
	{Pattern: regexp.MustCompile(`(?i)connection.*closed`), Code: CodeConnectionFailure},
	{Pattern: regexp.MustCompile(`(?i)connection.*lost`), Code: CodeConnectionFailure},
	{Pattern: regexp.MustCompile(`(?i)connection.*refused`), Code: CodeSQLClientUnableToEstablishSQLConnection},
	{Pattern: regexp.MustCompile(`(?i)connection.*timeout`), Code: CodeConnectionFailure},
	{Pattern: regexp.MustCompile(`(?i)too many connections`), Code: CodeTooManyConnections, HintTemplate: "Wait for existing connections to close or increase max_connections."},

	// Permission errors
	{Pattern: regexp.MustCompile(`(?i)permission denied`), Code: CodeInsufficientPrivilege},
	{Pattern: regexp.MustCompile(`(?i)access denied`), Code: CodeInsufficientPrivilege},
	{Pattern: regexp.MustCompile(`(?i)not authorized`), Code: CodeInsufficientPrivilege},

	// Resource errors
	{Pattern: regexp.MustCompile(`(?i)out of memory`), Code: CodeOutOfMemory, HintTemplate: "Reduce query complexity or add more memory."},
	{Pattern: regexp.MustCompile(`(?i)memory limit exceeded`), Code: CodeOutOfMemory},
	{Pattern: regexp.MustCompile(`(?i)disk full`), Code: CodeDiskFull},
	{Pattern: regexp.MustCompile(`(?i)no space left`), Code: CodeDiskFull},

	// Feature support
	{Pattern: regexp.MustCompile(`(?i)not supported`), Code: CodeFeatureNotSupported},
	{Pattern: regexp.MustCompile(`(?i)not implemented`), Code: CodeFeatureNotSupported},
	{Pattern: regexp.MustCompile(`(?i)unsupported`), Code: CodeFeatureNotSupported},

	// Query cancellation
	{Pattern: regexp.MustCompile(`(?i)query canceled`), Code: CodeQueryCanceled},
	{Pattern: regexp.MustCompile(`(?i)statement canceled`), Code: CodeQueryCanceled},
	{Pattern: regexp.MustCompile(`(?i)statement timeout`), Code: CodeQueryCanceled},
	{Pattern: regexp.MustCompile(`(?i)lock timeout`), Code: CodeQueryCanceled},

	// Ambiguous references
	{Pattern: regexp.MustCompile(`(?i)ambiguous column ['"']?(\w+)['"']?`), Code: CodeAmbiguousColumn, ExtractNames: true, HintTemplate: "Qualify the column name with a table alias."},
	{Pattern: regexp.MustCompile(`(?i)column ['"']?(\w+)['"']? is ambiguous`), Code: CodeAmbiguousColumn, ExtractNames: true},
	{Pattern: regexp.MustCompile(`(?i)ambiguous function`), Code: CodeAmbiguousFunction},

	// Grouping errors
	{Pattern: regexp.MustCompile(`(?i)must appear in.*GROUP BY`), Code: CodeGroupingError, HintTemplate: "Add the column to GROUP BY or use an aggregate function."},
	{Pattern: regexp.MustCompile(`(?i)not.*in.*aggregate`), Code: CodeGroupingError},

	// I/O errors
	{Pattern: regexp.MustCompile(`(?i)file not found`), Code: CodeIOError},
	{Pattern: regexp.MustCompile(`(?i)cannot read`), Code: CodeIOError},
	{Pattern: regexp.MustCompile(`(?i)cannot write`), Code: CodeIOError},
	{Pattern: regexp.MustCompile(`(?i)i/o error`), Code: CodeIOError},
}

// ToPgErrorWithContext converts an error to PgError with enhanced context and hints.
// This is the enhanced version that applies comprehensive error mapping rules.
func ToPgErrorWithContext(err error, query string) *PgError {
	if err == nil {
		return nil
	}

	// Check if it's already a PgError
	if pgErr, ok := err.(*PgError); ok {
		// Add context if missing
		if pgErr.Routine == "" {
			pgErr.Routine = getCallerRoutine()
		}
		return pgErr
	}

	// Check if it's a CastError (duck-typed to avoid import cycle)
	if castErr, ok := err.(CastErrorInterface); ok {
		pgErr := NewPgError(castErr.GetSQLState(), castErr.GetMessage())
		if detail := castErr.GetDetail(); detail != "" {
			pgErr.Detail = detail
		}
		if hint := castErr.GetHint(); hint != "" {
			pgErr.Hint = hint
		}
		pgErr.Routine = getCallerRoutine()
		return pgErr
	}

	msg := err.Error()

	// Try pattern-based mapping
	for _, rule := range errorMappingRules {
		matches := rule.Pattern.FindStringSubmatch(msg)
		if matches != nil {
			pgErr := NewPgError(rule.Code, msg)

			// Apply hint template if present
			if rule.HintTemplate != "" {
				hint := rule.HintTemplate
				for i, match := range matches {
					if i > 0 {
						hint = strings.ReplaceAll(hint, fmt.Sprintf("$%d", i), match)
					}
				}
				pgErr.Hint = hint
			}

			// Apply detail template if present
			if rule.DetailTemplate != "" {
				detail := rule.DetailTemplate
				for i, match := range matches {
					if i > 0 {
						detail = strings.ReplaceAll(detail, fmt.Sprintf("$%d", i), match)
					}
				}
				pgErr.Detail = detail
			}

			// Extract names from capture groups
			if rule.ExtractNames && len(matches) > 1 {
				switch rule.Code {
				case CodeUndefinedTable, CodeDuplicateTable:
					pgErr.TableName = matches[1]
				case CodeUndefinedColumn, CodeDuplicateColumn, CodeAmbiguousColumn:
					pgErr.ColumnName = matches[1]
					if len(matches) > 2 {
						pgErr.TableName = matches[2]
					}
				case CodeUniqueViolation, CodeForeignKeyViolation, CodeCheckViolation:
					pgErr.ConstraintName = matches[1]
				case CodeNotNullViolation:
					pgErr.ColumnName = matches[1]
				case CodeInvalidSchemaName, CodeDuplicateSchema:
					pgErr.SchemaName = matches[1]
				}
			}

			// Try to extract position from query if available
			if query != "" {
				position := extractPositionFromMessage(msg)
				if position > 0 && position <= len(query) {
					pgErr.Position = position
				}
			}

			// Add routine context
			pgErr.Routine = getCallerRoutine()

			return pgErr
		}
	}

	// Fallback to basic ToPgError
	pgErr := ToPgError(err)
	pgErr.Routine = getCallerRoutine()
	return pgErr
}

// extractPositionFromMessage attempts to extract a position number from an error message.
func extractPositionFromMessage(msg string) int {
	// Look for patterns like "at position 42", "position: 42", "character 42"
	patterns := []string{
		`(?i)at position\s*(\d+)`,
		`(?i)position:\s*(\d+)`,
		`(?i)character\s*(\d+)`,
		`(?i)offset\s*(\d+)`,
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p)
		matches := re.FindStringSubmatch(msg)
		if len(matches) > 1 {
			var pos int
			_, _ = fmt.Sscanf(matches[1], "%d", &pos)
			return pos
		}
	}
	return 0
}

// getCallerRoutine returns the name of the calling function for error context.
func getCallerRoutine() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return ""
	}
	// Extract just the function name without package path
	name := fn.Name()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	return name
}

// =============================================================================
// Task 19.2: Error Hints and Details for Common Issues
// =============================================================================

// ErrorHint contains common error hints for improving user experience.
type ErrorHint struct {
	Code    string
	Pattern string
	Hint    string
}

// commonErrorHints provides helpful hints for common error patterns.
var commonErrorHints = []ErrorHint{
	// Table/Relation errors
	{Code: CodeUndefinedTable, Pattern: "does not exist", Hint: "Check the table name for typos. Use \\dt to list available tables."},

	// Column errors
	{Code: CodeUndefinedColumn, Pattern: "does not exist", Hint: "Check the column name for typos. Use \\d tablename to see available columns."},
	{Code: CodeAmbiguousColumn, Pattern: "ambiguous", Hint: "Qualify the column with a table alias, e.g., t.column_name."},

	// Function errors
	{Code: CodeUndefinedFunction, Pattern: "does not exist", Hint: "Check the function name and argument types. You might need explicit type casts."},

	// Constraint errors
	{Code: CodeUniqueViolation, Pattern: "unique constraint", Hint: "The value already exists. Use a different value or use ON CONFLICT for upsert."},
	{Code: CodeForeignKeyViolation, Pattern: "foreign key", Hint: "Ensure the referenced row exists in the parent table."},
	{Code: CodeNotNullViolation, Pattern: "not-null", Hint: "Provide a value for the required column or alter the column to allow NULL."},
	{Code: CodeCheckViolation, Pattern: "check constraint", Hint: "The value does not satisfy the check constraint. Review the constraint definition."},

	// Type errors
	{Code: CodeInvalidTextRepresentation, Pattern: "invalid input syntax", Hint: "Check the value format. Use explicit CAST or type conversion."},
	{Code: CodeCannotCoerce, Pattern: "cannot cast", Hint: "Use a two-step conversion or a different approach for this type conversion."},
	{Code: CodeNumericValueOutOfRange, Pattern: "out of range", Hint: "Use a larger numeric type (e.g., BIGINT instead of INTEGER) or check for overflow."},

	// Transaction errors
	{Code: CodeSerializationFailure, Pattern: "serialize", Hint: "Retry the transaction. Consider reducing isolation level if conflicts are frequent."},
	{Code: CodeDeadlockDetected, Pattern: "deadlock", Hint: "Retry the transaction. Consider reordering operations or using shorter transactions."},

	// Resource errors
	{Code: CodeOutOfMemory, Pattern: "memory", Hint: "Reduce batch size or query complexity. Consider pagination for large result sets."},
	{Code: CodeTooManyConnections, Pattern: "connections", Hint: "Close unused connections or use connection pooling. Consider increasing max_connections."},

	// Syntax errors
	{Code: CodeSyntaxError, Pattern: "syntax error", Hint: "Check the SQL syntax near the indicated position. Common issues: missing commas, unbalanced quotes."},
}

// AddHintForError adds a helpful hint to a PgError based on common patterns.
func AddHintForError(pgErr *PgError) *PgError {
	if pgErr == nil || pgErr.Hint != "" {
		return pgErr
	}

	msgLower := strings.ToLower(pgErr.Message)
	for _, eh := range commonErrorHints {
		if pgErr.Code == eh.Code && strings.Contains(msgLower, eh.Pattern) {
			pgErr.Hint = eh.Hint
			return pgErr
		}
	}
	return pgErr
}

// SuggestSimilarNames suggests similar table/column names for "does not exist" errors.
// This helps users identify typos in their queries.
func SuggestSimilarNames(name string, candidates []string, threshold float64) []string {
	if len(candidates) == 0 {
		return nil
	}

	var suggestions []string
	nameLower := strings.ToLower(name)

	for _, candidate := range candidates {
		candidateLower := strings.ToLower(candidate)
		similarity := calculateSimilarity(nameLower, candidateLower)
		if similarity >= threshold {
			suggestions = append(suggestions, candidate)
		}
	}

	// Limit to top 3 suggestions
	if len(suggestions) > 3 {
		suggestions = suggestions[:3]
	}

	return suggestions
}

// calculateSimilarity returns a similarity score between 0 and 1 for two strings.
// Uses a simple character-based comparison.
func calculateSimilarity(a, b string) float64 {
	if a == b {
		return 1.0
	}
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}

	// Simple Levenshtein-based similarity
	distance := levenshteinDistance(a, b)
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	return 1.0 - float64(distance)/float64(maxLen)
}

// levenshteinDistance calculates the edit distance between two strings.
func levenshteinDistance(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	// Create matrix
	matrix := make([][]int, len(a)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(b)+1)
	}

	// Initialize first column and row
	for i := 0; i <= len(a); i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len(b); j++ {
		matrix[0][j] = j
	}

	// Fill in the rest
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			matrix[i][j] = min3(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}

	return matrix[len(a)][len(b)]
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// =============================================================================
// Task 19.3: Error Context (WHERE, ROUTINE)
// =============================================================================

// WithWhere sets the WHERE context field (e.g., "PL/pgSQL function foo() line 10 at RETURN").
func (e *PgError) WithWhere(where string) *PgError {
	e.Where = where
	return e
}

// WithRoutine sets the routine name that generated the error.
func (e *PgError) WithRoutine(routine string) *PgError {
	e.Routine = routine
	return e
}

// WithFile sets the source file name.
func (e *PgError) WithFile(file string) *PgError {
	e.File = file
	return e
}

// WithLine sets the source line number.
func (e *PgError) WithLine(line int) *PgError {
	e.Line = line
	return e
}

// WithDataType sets the data type name related to the error.
func (e *PgError) WithDataType(dataType string) *PgError {
	e.DataTypeName = dataType
	return e
}

// WithInternalQuery sets the internal query that generated the error.
func (e *PgError) WithInternalQuery(query string) *PgError {
	e.InternalQuery = query
	return e
}

// WithInternalPosition sets the position in the internal query.
func (e *PgError) WithInternalPosition(pos int) *PgError {
	e.InternalPosition = pos
	return e
}

// AddContext adds context information to an error using the current call stack.
func (e *PgError) AddContext() *PgError {
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		// Extract just the filename
		if idx := strings.LastIndex(file, "/"); idx >= 0 {
			file = file[idx+1:]
		}
		e.File = file
		e.Line = line

		fn := runtime.FuncForPC(pc)
		if fn != nil {
			name := fn.Name()
			if idx := strings.LastIndex(name, "."); idx >= 0 {
				name = name[idx+1:]
			}
			e.Routine = name
		}
	}
	return e
}

// =============================================================================
// Task 19.4: Error Logging with Configurable Verbosity
// =============================================================================

// ErrorLogLevel defines the verbosity level for error logging.
type ErrorLogLevel int

const (
	// ErrorLogNone disables error logging.
	ErrorLogNone ErrorLogLevel = iota
	// ErrorLogMinimal logs only error code and message.
	ErrorLogMinimal
	// ErrorLogStandard logs error code, message, detail, and hint.
	ErrorLogStandard
	// ErrorLogVerbose logs all error fields including context.
	ErrorLogVerbose
	// ErrorLogDebug logs everything plus stack trace.
	ErrorLogDebug
)

// ErrorLogger handles error logging with configurable verbosity.
type ErrorLogger struct {
	mu       sync.RWMutex
	logger   *slog.Logger
	level    ErrorLogLevel
	logQuery bool
}

// globalErrorLogger is the default error logger instance.
var globalErrorLogger = &ErrorLogger{
	level:    ErrorLogStandard,
	logQuery: false,
}

// NewErrorLogger creates a new ErrorLogger with the given logger and level.
func NewErrorLogger(logger *slog.Logger, level ErrorLogLevel) *ErrorLogger {
	return &ErrorLogger{
		logger:   logger,
		level:    level,
		logQuery: level >= ErrorLogVerbose,
	}
}

// SetGlobalErrorLogger sets the global error logger.
func SetGlobalErrorLogger(logger *ErrorLogger) {
	globalErrorLogger = logger
}

// GetGlobalErrorLogger returns the global error logger.
func GetGlobalErrorLogger() *ErrorLogger {
	return globalErrorLogger
}

// SetLevel sets the logging level.
func (el *ErrorLogger) SetLevel(level ErrorLogLevel) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.level = level
}

// SetLogger sets the slog logger.
func (el *ErrorLogger) SetLogger(logger *slog.Logger) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.logger = logger
}

// SetLogQuery enables or disables query logging with errors.
func (el *ErrorLogger) SetLogQuery(enabled bool) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.logQuery = enabled
}

// LogError logs a PgError with the configured verbosity.
func (el *ErrorLogger) LogError(pgErr *PgError, query string) {
	if el == nil || pgErr == nil {
		return
	}

	el.mu.RLock()
	level := el.level
	logger := el.logger
	logQuery := el.logQuery
	el.mu.RUnlock()

	if level == ErrorLogNone || logger == nil {
		return
	}

	// Build log attributes based on verbosity level
	var attrs []any

	// Minimal: code and message
	if level >= ErrorLogMinimal {
		attrs = append(attrs,
			"code", pgErr.Code,
			"severity", pgErr.Severity,
			"message", pgErr.Message,
		)
	}

	// Standard: add detail and hint
	if level >= ErrorLogStandard {
		if pgErr.Detail != "" {
			attrs = append(attrs, "detail", pgErr.Detail)
		}
		if pgErr.Hint != "" {
			attrs = append(attrs, "hint", pgErr.Hint)
		}
	}

	// Verbose: add context and names
	if level >= ErrorLogVerbose {
		if pgErr.Position > 0 {
			attrs = append(attrs, "position", pgErr.Position)
		}
		if pgErr.Where != "" {
			attrs = append(attrs, "where", pgErr.Where)
		}
		if pgErr.Routine != "" {
			attrs = append(attrs, "routine", pgErr.Routine)
		}
		if pgErr.SchemaName != "" {
			attrs = append(attrs, "schema", pgErr.SchemaName)
		}
		if pgErr.TableName != "" {
			attrs = append(attrs, "table", pgErr.TableName)
		}
		if pgErr.ColumnName != "" {
			attrs = append(attrs, "column", pgErr.ColumnName)
		}
		if pgErr.ConstraintName != "" {
			attrs = append(attrs, "constraint", pgErr.ConstraintName)
		}
		if logQuery && query != "" {
			// Truncate query for logging if too long
			if len(query) > 200 {
				query = query[:200] + "..."
			}
			attrs = append(attrs, "query", query)
		}
	}

	// Debug: add file/line info
	if level >= ErrorLogDebug {
		if pgErr.File != "" {
			attrs = append(attrs, "file", pgErr.File)
		}
		if pgErr.Line > 0 {
			attrs = append(attrs, "line", pgErr.Line)
		}
	}

	// Log at appropriate level based on severity
	switch pgErr.Severity {
	case SeverityFatal, SeverityPanic:
		logger.Error("database error", attrs...)
	case SeverityWarning:
		logger.Warn("database warning", attrs...)
	case SeverityNotice, SeverityInfo, SeverityLog:
		logger.Info("database notice", attrs...)
	case SeverityDebug:
		logger.Debug("database debug", attrs...)
	default:
		logger.Error("database error", attrs...)
	}
}

// LogErrorGlobal logs an error using the global error logger.
func LogErrorGlobal(pgErr *PgError, query string) {
	globalErrorLogger.LogError(pgErr, query)
}

// =============================================================================
// Task 19.5: RAISE NOTICE/WARNING Support
// =============================================================================

// RaiseNoticeCollector collects notices raised during query execution.
// This is used to support RAISE NOTICE/WARNING from PL/pgSQL-like functionality.
// It uses the existing Notice type from txstate.go but adds collection capabilities.
type RaiseNoticeCollector struct {
	mu       sync.Mutex
	notices  []*Notice
	handlers []RaiseNoticeHandler
}

// RaiseNoticeHandler is a function that handles notice/warning messages.
type RaiseNoticeHandler func(notice *Notice)

// NewRaiseNoticeCollector creates a new notice collector.
func NewRaiseNoticeCollector() *RaiseNoticeCollector {
	return &RaiseNoticeCollector{
		notices: make([]*Notice, 0),
	}
}

// AddHandler adds a handler that will be called for each notice.
func (nc *RaiseNoticeCollector) AddHandler(handler RaiseNoticeHandler) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.handlers = append(nc.handlers, handler)
}

// Raise adds a notice and notifies handlers.
func (nc *RaiseNoticeCollector) Raise(notice *Notice) {
	nc.mu.Lock()
	nc.notices = append(nc.notices, notice)
	handlers := make([]RaiseNoticeHandler, len(nc.handlers))
	copy(handlers, nc.handlers)
	nc.mu.Unlock()

	// Call handlers outside the lock
	for _, handler := range handlers {
		handler(notice)
	}
}

// RaiseNotice is a convenience method for RAISE NOTICE.
func (nc *RaiseNoticeCollector) RaiseNotice(format string, args ...any) {
	nc.Raise(NewNotice(SeverityNotice, fmt.Sprintf(format, args...)))
}

// RaiseWarning is a convenience method for RAISE WARNING.
func (nc *RaiseNoticeCollector) RaiseWarning(format string, args ...any) {
	nc.Raise(NewWarning(fmt.Sprintf(format, args...)))
}

// RaiseInfo is a convenience method for RAISE INFO.
func (nc *RaiseNoticeCollector) RaiseInfo(format string, args ...any) {
	nc.Raise(NewInfo(fmt.Sprintf(format, args...)))
}

// RaiseDebug is a convenience method for RAISE DEBUG.
func (nc *RaiseNoticeCollector) RaiseDebug(format string, args ...any) {
	nc.Raise(NewDebug(fmt.Sprintf(format, args...)))
}

// RaiseLog is a convenience method for RAISE LOG.
func (nc *RaiseNoticeCollector) RaiseLog(format string, args ...any) {
	nc.Raise(NewNotice(SeverityLog, fmt.Sprintf(format, args...)))
}

// GetNotices returns all collected notices.
func (nc *RaiseNoticeCollector) GetNotices() []*Notice {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	result := make([]*Notice, len(nc.notices))
	copy(result, nc.notices)
	return result
}

// Clear removes all collected notices.
func (nc *RaiseNoticeCollector) Clear() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.notices = nc.notices[:0]
}

// Count returns the number of collected notices.
func (nc *RaiseNoticeCollector) Count() int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return len(nc.notices)
}

// NoticeToPgError converts a Notice to a PgError for sending via the wire protocol.
// This is useful when the wire protocol expects a PgError structure.
func NoticeToPgError(n *Notice) *PgError {
	return &PgError{
		Severity: n.Severity,
		Code:     n.Code,
		Message:  n.Message,
		Detail:   n.Detail,
		Hint:     n.Hint,
		Where:    n.Where,
		Position: n.Position,
	}
}
