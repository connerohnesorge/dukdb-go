package dukdb

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorType represents the type of a DuckDB error.
type ErrorType int

// Error type constants matching DuckDB's error classification.
const (
	ErrorTypeInvalid              ErrorType = 0  // Invalid type.
	ErrorTypeOutOfRange           ErrorType = 1  // The type's value is out of range.
	ErrorTypeConversion           ErrorType = 2  // Conversion/casting error.
	ErrorTypeUnknownType          ErrorType = 3  // The type is unknown.
	ErrorTypeDecimal              ErrorType = 4  // Decimal-related error.
	ErrorTypeMismatchType         ErrorType = 5  // Types don't match.
	ErrorTypeDivideByZero         ErrorType = 6  // Division by zero.
	ErrorTypeObjectSize           ErrorType = 7  // Exceeds object size.
	ErrorTypeInvalidType          ErrorType = 8  // Incompatible types.
	ErrorTypeSerialization        ErrorType = 9  // Type serialization error.
	ErrorTypeTransaction          ErrorType = 10 // Transaction conflict.
	ErrorTypeNotImplemented       ErrorType = 11 // Missing functionality.
	ErrorTypeExpression           ErrorType = 12 // Expression error.
	ErrorTypeCatalog              ErrorType = 13 // Catalog error.
	ErrorTypeParser               ErrorType = 14 // Error during parsing.
	ErrorTypePlanner              ErrorType = 15 // Error during planning.
	ErrorTypeScheduler            ErrorType = 16 // Scheduling error.
	ErrorTypeExecutor             ErrorType = 17 // Executor error.
	ErrorTypeConstraint           ErrorType = 18 // Constraint violation.
	ErrorTypeIndex                ErrorType = 19 // Index error.
	ErrorTypeStat                 ErrorType = 20 // Statistics error.
	ErrorTypeConnection           ErrorType = 21 // Connection error.
	ErrorTypeSyntax               ErrorType = 22 // Invalid syntax.
	ErrorTypeSettings             ErrorType = 23 // Settings-related error.
	ErrorTypeBinder               ErrorType = 24 // Binding error.
	ErrorTypeNetwork              ErrorType = 25 // Network error.
	ErrorTypeOptimizer            ErrorType = 26 // Optimizer error.
	ErrorTypeNullPointer          ErrorType = 27 // Null-pointer exception.
	ErrorTypeIO                   ErrorType = 28 // IO exception.
	ErrorTypeInterrupt            ErrorType = 29 // Query interruption.
	ErrorTypeFatal                ErrorType = 30 // Fatal exception. Non-recoverable.
	ErrorTypeInternal             ErrorType = 31 // Internal exception. Indicates a bug.
	ErrorTypeInvalidInput         ErrorType = 32 // Invalid input.
	ErrorTypeOutOfMemory          ErrorType = 33 // Out-of-memory error.
	ErrorTypePermission           ErrorType = 34 // Invalid permissions.
	ErrorTypeParameterNotResolved ErrorType = 35 // Error when resolving types.
	ErrorTypeParameterNotAllowed  ErrorType = 36 // Invalid parameter.
	ErrorTypeDependency           ErrorType = 37 // Dependency error.
	ErrorTypeHTTP                 ErrorType = 38 // HTTP error.
	ErrorTypeMissingExtension     ErrorType = 39 // Usage of a non-loaded extension.
	ErrorTypeAutoLoad             ErrorType = 40 // Usage of a non-loaded extension that cannot be loaded automatically.
	ErrorTypeSequence             ErrorType = 41 // Sequence error.
	ErrorTypeInvalidConfiguration ErrorType = 42 // Invalid configuration error.
	ErrorTypeClosed               ErrorType = 43 // Resource is closed.
	ErrorTypeBadState             ErrorType = 44 // Invalid state for operation.
)

// errorPrefixMap maps error message prefixes to their corresponding ErrorType.
var errorPrefixMap = map[string]ErrorType{
	"Invalid Error":                ErrorTypeInvalid,
	"Out of Range Error":           ErrorTypeOutOfRange,
	"Conversion Error":             ErrorTypeConversion,
	"Error":                        ErrorTypeUnknownType,
	"Decimal Error":                ErrorTypeDecimal,
	"Mismatch Type Error":          ErrorTypeMismatchType,
	"Divide by Zero Error":         ErrorTypeDivideByZero,
	"Object Size Error":            ErrorTypeObjectSize,
	"Invalid type Error":           ErrorTypeInvalidType,
	"Serialization Error":          ErrorTypeSerialization,
	"TransactionContext Error":     ErrorTypeTransaction,
	"Not implemented Error":        ErrorTypeNotImplemented,
	"Expression Error":             ErrorTypeExpression,
	"Catalog Error":                ErrorTypeCatalog,
	"Parser Error":                 ErrorTypeParser,
	"Planner Error":                ErrorTypePlanner,
	"Scheduler Error":              ErrorTypeScheduler,
	"Executor Error":               ErrorTypeExecutor,
	"Constraint Error":             ErrorTypeConstraint,
	"Index Error":                  ErrorTypeIndex,
	"Stat Error":                   ErrorTypeStat,
	"Connection Error":             ErrorTypeConnection,
	"Syntax Error":                 ErrorTypeSyntax,
	"Settings Error":               ErrorTypeSettings,
	"Binder Error":                 ErrorTypeBinder,
	"Network Error":                ErrorTypeNetwork,
	"Optimizer Error":              ErrorTypeOptimizer,
	"NullPointer Error":            ErrorTypeNullPointer,
	"IO Error":                     ErrorTypeIO,
	"INTERRUPT Error":              ErrorTypeInterrupt,
	"FATAL Error":                  ErrorTypeFatal,
	"INTERNAL Error":               ErrorTypeInternal,
	"Invalid Input Error":          ErrorTypeInvalidInput,
	"Out of Memory Error":          ErrorTypeOutOfMemory,
	"Permission Error":             ErrorTypePermission,
	"Parameter Not Resolved Error": ErrorTypeParameterNotResolved,
	"Parameter Not Allowed Error":  ErrorTypeParameterNotAllowed,
	"Dependency Error":             ErrorTypeDependency,
	"HTTP Error":                   ErrorTypeHTTP,
	"Missing Extension Error":      ErrorTypeMissingExtension,
	"Extension Autoloading Error":  ErrorTypeAutoLoad,
	"Sequence Error":               ErrorTypeSequence,
	"Invalid Configuration Error":  ErrorTypeInvalidConfiguration,
	"Closed Error":                 ErrorTypeClosed,
	"Bad State Error":              ErrorTypeBadState,
}

// Error represents a DuckDB error with a type and message.
type Error struct {
	Type ErrorType
	Msg  string
}

// Error returns the error message.
func (e *Error) Error() string {
	return e.Msg
}

// Is compares errors by their message content.
func (e *Error) Is(err error) bool {
	var other *Error
	if errors.As(err, &other) {
		return other.Msg == e.Msg
	}

	return false
}

// getDuckDBError parses an error message and returns an Error with the appropriate type.
func getDuckDBError(errMsg string) error {
	errType := ErrorTypeInvalid

	// Find the end of the prefix ("<error-type> Error: ").
	if idx := strings.Index(errMsg, ": "); idx != -1 {
		if typ, ok := errorPrefixMap[errMsg[:idx]]; ok {
			errType = typ
		}
	}

	return &Error{
		Type: errType,
		Msg:  errMsg,
	}
}

// Common error variables for internal use.
var (
	errConnect = errors.New(
		"could not connect to database",
	)
	errParseDSN = errors.New(
		"could not parse DSN for database",
	)
	errClosedCon = errors.New(
		"closed connection",
	)
	errNoBackend = errors.New(
		"no backend registered",
	)
	errClosedStmt = errors.New("closed statement")
	errBeginTx    = errors.New(
		"could not begin transaction",
	)
	errMultipleTx = errors.New(
		"multiple transactions",
	)
)

// maxDecimalWidth is the maximum width for DECIMAL types.
const maxDecimalWidth = 38

// Exported error variables for use by backend implementations.
var (
	// ErrConnectionClosed indicates the connection has been closed.
	ErrConnectionClosed = errors.New(
		"connection closed",
	)

	// ErrTransactionAlreadyEnded indicates the transaction has already been committed or rolled back.
	ErrTransactionAlreadyEnded = errors.New(
		"transaction already ended",
	)

	// ErrTableNotFound indicates the specified table was not found.
	ErrTableNotFound = errors.New(
		"table not found",
	)

	// ErrTableAlreadyExists indicates the table already exists.
	ErrTableAlreadyExists = errors.New(
		"table already exists",
	)

	// ErrColumnNotFound indicates the specified column was not found.
	ErrColumnNotFound = errors.New(
		"column not found",
	)

	// ErrTypeMismatch indicates a type mismatch error.
	ErrTypeMismatch = errors.New("type mismatch")

	// ErrInvalidSyntax indicates a SQL syntax error.
	ErrInvalidSyntax = errors.New(
		"invalid syntax",
	)

	// ErrNotImplemented indicates the feature is not implemented.
	ErrNotImplemented = errors.New(
		"not implemented",
	)

	// ErrDivisionByZero indicates a division by zero error.
	ErrDivisionByZero = errors.New(
		"division by zero",
	)

	// ErrNullConstraint indicates a NULL constraint violation.
	ErrNullConstraint = errors.New(
		"NULL constraint violation",
	)

	// ErrNotSupported indicates the feature is not supported by the backend.
	ErrNotSupported = errors.New(
		"not supported",
	)
)

// TypeInfo-specific error variables (for API compatibility with duckdb-go).
var (
	// errAPI is the base error for API errors.
	errAPI = errors.New("API error")

	// errEmptyName indicates an empty name was provided.
	errEmptyName = errors.New("empty name")

	// errInvalidDecimalWidth indicates an invalid decimal width.
	errInvalidDecimalWidth = fmt.Errorf(
		"the DECIMAL width must be between 1 and %d",
		maxDecimalWidth,
	)

	// errInvalidDecimalScale indicates an invalid decimal scale.
	errInvalidDecimalScale = errors.New(
		"the DECIMAL scale must be less than or equal to the width",
	)

	// errInvalidArraySize indicates an invalid array size.
	errInvalidArraySize = errors.New(
		"invalid ARRAY size",
	)
)

// Error message constants for TypeInfo.
const (
	driverErrMsg          = "database/sql/driver"
	unsupportedTypeErrMsg = "unsupported data type"
	tryOtherFuncErrMsg    = "please try this function instead"
	indexErrMsg           = "index"
	interfaceIsNilErrMsg  = "interface is nil"
	duplicateNameErrMsg   = "duplicate name"
)

// getError wraps an error with the driver prefix and base error.
func getError(errDriver, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", driverErrMsg, errDriver)
	}

	return fmt.Errorf("%s: %w: %s", driverErrMsg, errDriver, err.Error())
}

// unsupportedTypeError creates an error for unsupported types.
func unsupportedTypeError(name string) error {
	return fmt.Errorf("%s: %s", unsupportedTypeErrMsg, name)
}

// tryOtherFuncError creates an error suggesting a different function.
func tryOtherFuncError(hint string) error {
	return fmt.Errorf("%s: %s", tryOtherFuncErrMsg, hint)
}

// addIndexToError adds an index to an error message.
func addIndexToError(err error, idx int) error {
	return fmt.Errorf("%w: %s: %d", err, indexErrMsg, idx)
}

// interfaceIsNilError creates an error for nil interface parameters.
func interfaceIsNilError(interfaceName string) error {
	return fmt.Errorf("%s: %s", interfaceIsNilErrMsg, interfaceName)
}

// duplicateNameError creates an error for duplicate names.
func duplicateNameError(name string) error {
	return fmt.Errorf("%s: %s", duplicateNameErrMsg, name)
}
