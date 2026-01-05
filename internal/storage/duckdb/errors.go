// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file consolidates all DuckDB format-related errors
// with clear, actionable messages for debugging and user feedback.
package duckdb

import (
	"errors"
	"fmt"
)

// FormatError represents a DuckDB file format error with detailed context.
// It provides structured error information for debugging and logging.
type FormatError struct {
	// Op is the operation that failed (e.g., "read block", "validate header").
	Op string

	// Path is the file path involved (if applicable).
	Path string

	// BlockID is the block ID involved (if applicable).
	BlockID uint64

	// HasBlockID indicates whether BlockID is set.
	HasBlockID bool

	// Err is the underlying error.
	Err error
}

// Error returns a formatted error message with context.
func (e *FormatError) Error() string {
	if e.Path != "" && e.HasBlockID {
		return fmt.Sprintf("duckdb format: %s failed for %s block %d: %v",
			e.Op, e.Path, e.BlockID, e.Err)
	}
	if e.Path != "" {
		return fmt.Sprintf("duckdb format: %s failed for %s: %v",
			e.Op, e.Path, e.Err)
	}
	if e.HasBlockID {
		return fmt.Sprintf("duckdb format: %s failed for block %d: %v",
			e.Op, e.BlockID, e.Err)
	}
	return fmt.Sprintf("duckdb format: %s failed: %v", e.Op, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *FormatError) Unwrap() error {
	return e.Err
}

// File validation errors.
// Note: ErrFileNotFound is defined in duckdb_writer.go.
// Note: ErrFileExists is defined in duckdb_writer.go.
var (
	// ErrFileTooSmall indicates the file is too small to contain valid headers.
	ErrFileTooSmall = errors.New("file too small to be a valid DuckDB file")

	// ErrCorruptedFile indicates general file corruption was detected.
	ErrCorruptedFile = errors.New("corrupted DuckDB file: structure validation failed")
)

// Header validation errors.
// These errors are returned when validating file or database headers.
var (
	// ErrInvalidMagicBytes indicates the file doesn't have the "DUCK" magic bytes.
	ErrInvalidMagicBytes = errors.New("not a valid DuckDB file: expected 'DUCK' magic bytes at offset 8")

	// ErrVersionTooOld indicates the file version is older than supported.
	ErrVersionTooOld = errors.New("DuckDB file version too old: consider upgrading the file")

	// ErrVersionTooNew indicates the file version is newer than supported.
	ErrVersionTooNew = errors.New("DuckDB file version too new: this implementation supports up to version 67 (DuckDB v1.4.3)")

	// ErrInvalidBlockSize indicates an invalid block allocation size in the header.
	ErrInvalidBlockSize = errors.New("invalid block allocation size: must be positive")

	// ErrInvalidVectorSize indicates an invalid vector size in the header.
	ErrInvalidVectorSize = errors.New("invalid vector size: must be positive")

	// ErrHeaderChecksumFailed indicates a header checksum verification failed.
	ErrHeaderChecksumFailed = errors.New("header checksum verification failed: data may be corrupted")

	// ErrHeader1Corrupted indicates the first database header is corrupted.
	ErrHeader1Corrupted = errors.New("database header 1 is corrupted")

	// ErrHeader2Corrupted indicates the second database header is corrupted.
	ErrHeader2Corrupted = errors.New("database header 2 is corrupted")
)

// Block-related errors.
// These errors are returned during block I/O operations.
var (
	// ErrBlockTooLarge indicates the block data exceeds maximum allowed size.
	ErrBlockTooLarge = errors.New("block data exceeds maximum size of 256KB")

	// ErrBlockOutOfRange indicates the block ID is beyond the file's block count.
	ErrBlockOutOfRange = errors.New("block ID out of range: exceeds file block count")

	// ErrIncompleteBlockRead indicates fewer bytes were read than expected.
	ErrIncompleteBlockRead = errors.New("incomplete block read: file may be truncated")

	// ErrIncompleteBlockWrite indicates fewer bytes were written than expected.
	ErrIncompleteBlockWrite = errors.New("incomplete block write: disk may be full")
)

// Catalog-related errors.
// These errors are returned during catalog operations.
// Note: ErrTableNotFound is defined in catalog_writer.go.
// Note: ErrTableExists, ErrViewExists, ErrIndexExists, ErrSequenceExists are defined in duckdb_writer.go.
var (
	// ErrCatalogCorrupted indicates the catalog metadata is corrupted.
	ErrCatalogCorrupted = errors.New("catalog metadata corrupted: unable to read database schema")

	// ErrSchemaExists indicates a schema with the name already exists.
	ErrSchemaExists = errors.New("schema already exists")

	// ErrViewNotFound indicates the requested view was not found.
	ErrViewNotFound = errors.New("view not found")

	// ErrIndexNotFound indicates the requested index was not found.
	ErrIndexNotFound = errors.New("index not found")

	// ErrSequenceNotFound indicates the requested sequence was not found.
	ErrSequenceNotFound = errors.New("sequence not found")

	// ErrInvalidCatalogType indicates an unknown or invalid catalog entry type.
	ErrInvalidCatalogType = errors.New("invalid catalog entry type")
)

// Type conversion errors.
// These errors are returned during DuckDB type to Go type conversions.
var (
	// ErrTypeMismatch indicates a type mismatch during conversion.
	ErrTypeMismatch = errors.New("type mismatch during conversion")

	// ErrTypeNotSupported indicates the DuckDB type is not supported by this implementation.
	ErrTypeNotSupported = errors.New("DuckDB type not supported by this implementation")

	// ErrInvalidEnum indicates an invalid enum value was encountered.
	ErrInvalidEnum = errors.New("invalid enum value: index out of range")

	// ErrInvalidDecimal indicates an invalid decimal value or precision.
	ErrInvalidDecimal = errors.New("invalid decimal: precision or scale out of range")

	// ErrInvalidInterval indicates an invalid interval value.
	ErrInvalidInterval = errors.New("invalid interval value")

	// ErrInvalidUUID indicates an invalid UUID value.
	ErrInvalidUUID = errors.New("invalid UUID: must be 16 bytes")

	// ErrOverflow indicates a numeric overflow during conversion.
	ErrOverflow = errors.New("numeric overflow during type conversion")
)

// Row group errors.
// These errors are returned during row group operations.
var (
	// ErrRowGroupCorrupted indicates row group data is corrupted.
	ErrRowGroupCorrupted = errors.New("row group data corrupted")

	// ErrInvalidColumnCount indicates the row group has an unexpected column count.
	ErrInvalidColumnCount = errors.New("invalid column count in row group")

	// ErrInvalidRowCount indicates the row count doesn't match the expected value.
	ErrInvalidRowCount = errors.New("invalid row count: mismatch between header and data")

	// ErrDataPointerInvalid indicates a data pointer references an invalid location.
	ErrDataPointerInvalid = errors.New("data pointer references invalid block location")
)

// Error constructors for creating contextual errors.

// NewFormatError creates a new FormatError with the given operation and error.
func NewFormatError(op string, err error) *FormatError {
	return &FormatError{
		Op:  op,
		Err: err,
	}
}

// NewFormatErrorWithPath creates a FormatError with a file path context.
func NewFormatErrorWithPath(op, path string, err error) *FormatError {
	return &FormatError{
		Op:   op,
		Path: path,
		Err:  err,
	}
}

// NewFormatErrorWithBlock creates a FormatError with a block ID context.
func NewFormatErrorWithBlock(op string, blockID uint64, err error) *FormatError {
	return &FormatError{
		Op:         op,
		BlockID:    blockID,
		HasBlockID: true,
		Err:        err,
	}
}

// NewChecksumError creates an error for checksum verification failures.
func NewChecksumError(blockID uint64, expected, actual uint64) error {
	return &FormatError{
		Op:         "verify checksum",
		BlockID:    blockID,
		HasBlockID: true,
		Err:        fmt.Errorf("expected %016x, got %016x", expected, actual),
	}
}

// NewVersionError creates an error for unsupported file versions.
func NewVersionError(version uint64) error {
	if version < CurrentVersion {
		return &FormatError{
			Op:  "version check",
			Err: fmt.Errorf("version %d is too old (minimum: %d)", version, CurrentVersion),
		}
	}
	return &FormatError{
		Op:  "version check",
		Err: fmt.Errorf("version %d is too new (maximum supported: %d)", version, CurrentVersion),
	}
}

// NewCompressionError creates an error for compression/decompression failures.
func NewCompressionError(compression CompressionType, err error) error {
	return &FormatError{
		Op:  fmt.Sprintf("decompress %s", compression.String()),
		Err: err,
	}
}

// NewBlockReadError creates an error for block read failures.
func NewBlockReadError(blockID uint64, err error) error {
	return &FormatError{
		Op:         "read block",
		BlockID:    blockID,
		HasBlockID: true,
		Err:        err,
	}
}

// NewBlockWriteError creates an error for block write failures.
func NewBlockWriteError(blockID uint64, err error) error {
	return &FormatError{
		Op:         "write block",
		BlockID:    blockID,
		HasBlockID: true,
		Err:        err,
	}
}

// NewCatalogError creates an error for catalog operation failures.
func NewCatalogError(op string, name string, err error) error {
	return &FormatError{
		Op:  fmt.Sprintf("%s '%s'", op, name),
		Err: err,
	}
}

// NewTypeConversionError creates an error for type conversion failures.
func NewTypeConversionError(fromType, toType string, err error) error {
	return &FormatError{
		Op:  fmt.Sprintf("convert %s to %s", fromType, toType),
		Err: err,
	}
}

// Error type checking helpers.

// IsFormatError returns true if the error is a FormatError.
func IsFormatError(err error) bool {
	var fe *FormatError
	return errors.As(err, &fe)
}

// IsChecksumError returns true if the error is related to checksum verification.
func IsChecksumError(err error) bool {
	return errors.Is(err, ErrBlockChecksumFailed) ||
		errors.Is(err, ErrHeaderChecksumFailed) ||
		errors.Is(err, ErrChecksumMismatch)
}

// IsVersionError returns true if the error is related to version compatibility.
func IsVersionError(err error) bool {
	return errors.Is(err, ErrUnsupportedVersion) ||
		errors.Is(err, ErrVersionTooOld) ||
		errors.Is(err, ErrVersionTooNew)
}

// IsCorruptionError returns true if the error indicates data corruption.
func IsCorruptionError(err error) bool {
	return errors.Is(err, ErrCorruptedFile) ||
		errors.Is(err, ErrCatalogCorrupted) ||
		errors.Is(err, ErrRowGroupCorrupted) ||
		errors.Is(err, ErrBothHeadersCorrupted) ||
		IsChecksumError(err)
}

// IsFileError returns true if the error is related to file operations.
func IsFileError(err error) bool {
	return errors.Is(err, ErrFileNotFound) ||
		errors.Is(err, ErrFileTooSmall) ||
		errors.Is(err, ErrNotDuckDBFile) ||
		errors.Is(err, ErrInvalidMagicBytes)
}

// IsBlockError returns true if the error is related to block operations.
func IsBlockError(err error) bool {
	return errors.Is(err, ErrBlockNotFound) ||
		errors.Is(err, ErrBlockChecksumFailed) ||
		errors.Is(err, ErrBlockTooLarge) ||
		errors.Is(err, ErrBlockOutOfRange) ||
		errors.Is(err, ErrIncompleteBlockRead) ||
		errors.Is(err, ErrIncompleteBlockWrite)
}

// IsCatalogError returns true if the error is related to catalog operations.
func IsCatalogError(err error) bool {
	return errors.Is(err, ErrCatalogCorrupted) ||
		errors.Is(err, ErrSchemaNotFound) ||
		errors.Is(err, ErrTableNotFound) ||
		errors.Is(err, ErrViewNotFound) ||
		errors.Is(err, ErrIndexNotFound) ||
		errors.Is(err, ErrSequenceNotFound) ||
		errors.Is(err, ErrInvalidCatalogEntry) ||
		errors.Is(err, ErrInvalidCatalogType)
}

// IsCompressionError returns true if the error is related to compression.
func IsCompressionError(err error) bool {
	return errors.Is(err, ErrUnsupportedCompression) ||
		errors.Is(err, ErrConstantDataTooShort) ||
		errors.Is(err, ErrRLEDataTruncated) ||
		errors.Is(err, ErrDictionaryDataTruncated) ||
		errors.Is(err, ErrBitPackingDataTruncated) ||
		errors.Is(err, ErrPFORDeltaDataTruncated)
}

// IsTypeError returns true if the error is related to type conversion.
func IsTypeError(err error) bool {
	return errors.Is(err, ErrTypeMismatch) ||
		errors.Is(err, ErrTypeNotSupported) ||
		errors.Is(err, ErrUnsupportedType) ||
		errors.Is(err, ErrInvalidTypeID) ||
		errors.Is(err, ErrInvalidValue) ||
		errors.Is(err, ErrOverflow)
}

// IsTransactionError returns true if the error is related to transactions.
func IsTransactionError(err error) bool {
	return errors.Is(err, ErrTransactionNotFound) ||
		errors.Is(err, ErrTransactionAlreadyActive) ||
		errors.Is(err, ErrNoActiveTransaction)
}

// IsStorageError returns true if the error is related to storage operations.
func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorageClosed) ||
		errors.Is(err, ErrReadOnlyMode) ||
		errors.Is(err, ErrBlockManagerClosed)
}

// GetFormatError extracts the FormatError from an error chain, if present.
func GetFormatError(err error) *FormatError {
	var fe *FormatError
	if errors.As(err, &fe) {
		return fe
	}
	return nil
}

// WrapWithContext wraps an error with additional context for DuckDB operations.
func WrapWithContext(err error, op string) error {
	if err == nil {
		return nil
	}
	// Don't double-wrap FormatErrors
	if IsFormatError(err) {
		return err
	}
	return NewFormatError(op, err)
}

// WrapBlockError wraps an error with block context.
func WrapBlockError(err error, blockID uint64, op string) error {
	if err == nil {
		return nil
	}
	return NewFormatErrorWithBlock(op, blockID, err)
}

// WrapPathError wraps an error with file path context.
func WrapPathError(err error, path string, op string) error {
	if err == nil {
		return nil
	}
	return NewFormatErrorWithPath(op, path, err)
}
