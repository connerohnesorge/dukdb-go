package duckdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *FormatError
		expected string
	}{
		{
			name: "basic error",
			err: &FormatError{
				Op:  "read header",
				Err: errors.New("IO error"),
			},
			expected: "duckdb format: read header failed: IO error",
		},
		{
			name: "error with path",
			err: &FormatError{
				Op:   "open file",
				Path: "/tmp/test.duckdb",
				Err:  errors.New("permission denied"),
			},
			expected: "duckdb format: open file failed for /tmp/test.duckdb: permission denied",
		},
		{
			name: "error with block ID",
			err: &FormatError{
				Op:         "read block",
				BlockID:    42,
				HasBlockID: true,
				Err:        errors.New("checksum mismatch"),
			},
			expected: "duckdb format: read block failed for block 42: checksum mismatch",
		},
		{
			name: "error with path and block ID",
			err: &FormatError{
				Op:         "verify",
				Path:       "/tmp/test.duckdb",
				BlockID:    100,
				HasBlockID: true,
				Err:        errors.New("corrupted"),
			},
			expected: "duckdb format: verify failed for /tmp/test.duckdb block 100: corrupted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestFormatError_Unwrap(t *testing.T) {
	underlying := errors.New("underlying error")
	fe := &FormatError{
		Op:  "test",
		Err: underlying,
	}

	// Test Unwrap
	assert.Equal(t, underlying, fe.Unwrap())

	// Test errors.Is
	assert.True(t, errors.Is(fe, underlying))

	// Test with sentinel errors
	fe2 := &FormatError{
		Op:  "checksum",
		Err: ErrBlockChecksumFailed,
	}
	assert.True(t, errors.Is(fe2, ErrBlockChecksumFailed))
}

func TestNewFormatError(t *testing.T) {
	err := NewFormatError("test operation", errors.New("test error"))
	require.NotNil(t, err)
	assert.Equal(t, "test operation", err.Op)
	assert.Equal(t, "", err.Path)
	assert.False(t, err.HasBlockID)
	assert.Contains(t, err.Error(), "test operation")
	assert.Contains(t, err.Error(), "test error")
}

func TestNewFormatErrorWithPath(t *testing.T) {
	err := NewFormatErrorWithPath("open", "/path/to/file.duckdb", errors.New("not found"))
	require.NotNil(t, err)
	assert.Equal(t, "open", err.Op)
	assert.Equal(t, "/path/to/file.duckdb", err.Path)
	assert.False(t, err.HasBlockID)
	assert.Contains(t, err.Error(), "/path/to/file.duckdb")
}

func TestNewFormatErrorWithBlock(t *testing.T) {
	err := NewFormatErrorWithBlock("read", 123, errors.New("IO error"))
	require.NotNil(t, err)
	assert.Equal(t, "read", err.Op)
	assert.Equal(t, uint64(123), err.BlockID)
	assert.True(t, err.HasBlockID)
	assert.Contains(t, err.Error(), "block 123")
}

func TestNewChecksumError(t *testing.T) {
	err := NewChecksumError(42, 0xDEADBEEF, 0xCAFEBABE)
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Equal(t, "verify checksum", fe.Op)
	assert.Equal(t, uint64(42), fe.BlockID)
	assert.True(t, fe.HasBlockID)
	assert.Contains(t, fe.Error(), "deadbeef")
	assert.Contains(t, fe.Error(), "cafebabe")
}

func TestNewVersionError(t *testing.T) {
	// Test version too old
	errOld := NewVersionError(50)
	require.NotNil(t, errOld)
	fe, ok := errOld.(*FormatError)
	require.True(t, ok)
	assert.Equal(t, "version check", fe.Op)
	assert.Contains(t, fe.Error(), "too old")
	assert.Contains(t, fe.Error(), "50")

	// Test version too new
	errNew := NewVersionError(100)
	require.NotNil(t, errNew)
	fe2, ok := errNew.(*FormatError)
	require.True(t, ok)
	assert.Contains(t, fe2.Error(), "too new")
	assert.Contains(t, fe2.Error(), "100")
}

func TestNewCompressionError(t *testing.T) {
	err := NewCompressionError(CompressionRLE, errors.New("data truncated"))
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Contains(t, fe.Op, "decompress")
	assert.Contains(t, fe.Op, "RLE")
	assert.Contains(t, fe.Error(), "data truncated")
}

func TestNewBlockReadError(t *testing.T) {
	err := NewBlockReadError(99, errors.New("disk error"))
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Equal(t, "read block", fe.Op)
	assert.Equal(t, uint64(99), fe.BlockID)
	assert.True(t, fe.HasBlockID)
}

func TestNewBlockWriteError(t *testing.T) {
	err := NewBlockWriteError(77, errors.New("disk full"))
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Equal(t, "write block", fe.Op)
	assert.Equal(t, uint64(77), fe.BlockID)
}

func TestNewCatalogError(t *testing.T) {
	err := NewCatalogError("load table", "users", errors.New("corrupted"))
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Contains(t, fe.Op, "load table")
	assert.Contains(t, fe.Op, "users")
}

func TestNewTypeConversionError(t *testing.T) {
	err := NewTypeConversionError("HUGEINT", "int64", errors.New("overflow"))
	require.NotNil(t, err)

	fe, ok := err.(*FormatError)
	require.True(t, ok)
	assert.Contains(t, fe.Op, "HUGEINT")
	assert.Contains(t, fe.Op, "int64")
}

func TestIsFormatError(t *testing.T) {
	// FormatError should return true
	fe := &FormatError{Op: "test", Err: errors.New("error")}
	assert.True(t, IsFormatError(fe))

	// Wrapped FormatError should return true
	wrapped := fmt.Errorf("wrapper: %w", fe)
	assert.True(t, IsFormatError(wrapped))

	// Regular error should return false
	regular := errors.New("regular error")
	assert.False(t, IsFormatError(regular))

	// nil should return false
	assert.False(t, IsFormatError(nil))
}

func TestIsChecksumError(t *testing.T) {
	assert.True(t, IsChecksumError(ErrBlockChecksumFailed))
	assert.True(t, IsChecksumError(ErrHeaderChecksumFailed))
	assert.True(t, IsChecksumError(ErrChecksumMismatch))

	// Wrapped errors
	assert.True(t, IsChecksumError(fmt.Errorf("wrap: %w", ErrBlockChecksumFailed)))

	// Non-checksum errors
	assert.False(t, IsChecksumError(errors.New("other error")))
	assert.False(t, IsChecksumError(ErrBlockNotFound))
}

func TestIsVersionError(t *testing.T) {
	assert.True(t, IsVersionError(ErrUnsupportedVersion))
	assert.True(t, IsVersionError(ErrVersionTooOld))
	assert.True(t, IsVersionError(ErrVersionTooNew))

	// Wrapped
	assert.True(t, IsVersionError(fmt.Errorf("wrap: %w", ErrVersionTooNew)))

	// Non-version errors
	assert.False(t, IsVersionError(errors.New("other error")))
}

func TestIsCorruptionError(t *testing.T) {
	assert.True(t, IsCorruptionError(ErrCorruptedFile))
	assert.True(t, IsCorruptionError(ErrCatalogCorrupted))
	assert.True(t, IsCorruptionError(ErrRowGroupCorrupted))
	assert.True(t, IsCorruptionError(ErrBothHeadersCorrupted))

	// Checksum errors are corruption errors
	assert.True(t, IsCorruptionError(ErrBlockChecksumFailed))
	assert.True(t, IsCorruptionError(ErrHeaderChecksumFailed))

	// Non-corruption errors
	assert.False(t, IsCorruptionError(ErrFileNotFound))
}

func TestIsFileError(t *testing.T) {
	assert.True(t, IsFileError(ErrFileNotFound))
	assert.True(t, IsFileError(ErrFileTooSmall))
	assert.True(t, IsFileError(ErrNotDuckDBFile))
	assert.True(t, IsFileError(ErrInvalidMagicBytes))

	assert.False(t, IsFileError(ErrBlockNotFound))
}

func TestIsBlockError(t *testing.T) {
	assert.True(t, IsBlockError(ErrBlockNotFound))
	assert.True(t, IsBlockError(ErrBlockChecksumFailed))
	assert.True(t, IsBlockError(ErrBlockTooLarge))
	assert.True(t, IsBlockError(ErrBlockOutOfRange))
	assert.True(t, IsBlockError(ErrIncompleteBlockRead))
	assert.True(t, IsBlockError(ErrIncompleteBlockWrite))

	assert.False(t, IsBlockError(ErrTableNotFound))
}

func TestIsCatalogError(t *testing.T) {
	assert.True(t, IsCatalogError(ErrCatalogCorrupted))
	assert.True(t, IsCatalogError(ErrSchemaNotFound))
	assert.True(t, IsCatalogError(ErrTableNotFound))
	assert.True(t, IsCatalogError(ErrViewNotFound))
	assert.True(t, IsCatalogError(ErrIndexNotFound))
	assert.True(t, IsCatalogError(ErrSequenceNotFound))
	assert.True(t, IsCatalogError(ErrInvalidCatalogEntry))
	assert.True(t, IsCatalogError(ErrInvalidCatalogType))

	assert.False(t, IsCatalogError(ErrBlockNotFound))
}

func TestIsCompressionError(t *testing.T) {
	assert.True(t, IsCompressionError(ErrUnsupportedCompression))
	assert.True(t, IsCompressionError(ErrConstantDataTooShort))
	assert.True(t, IsCompressionError(ErrRLEDataTruncated))
	assert.True(t, IsCompressionError(ErrDictionaryDataTruncated))
	assert.True(t, IsCompressionError(ErrBitPackingDataTruncated))
	assert.True(t, IsCompressionError(ErrPFORDeltaDataTruncated))

	assert.False(t, IsCompressionError(ErrBlockNotFound))
}

func TestIsTypeError(t *testing.T) {
	assert.True(t, IsTypeError(ErrTypeMismatch))
	assert.True(t, IsTypeError(ErrTypeNotSupported))
	assert.True(t, IsTypeError(ErrUnsupportedType))
	assert.True(t, IsTypeError(ErrInvalidTypeID))
	assert.True(t, IsTypeError(ErrInvalidValue))
	assert.True(t, IsTypeError(ErrOverflow))

	assert.False(t, IsTypeError(ErrBlockNotFound))
}

func TestIsTransactionError(t *testing.T) {
	assert.True(t, IsTransactionError(ErrTransactionNotFound))
	assert.True(t, IsTransactionError(ErrTransactionAlreadyActive))
	assert.True(t, IsTransactionError(ErrNoActiveTransaction))

	assert.False(t, IsTransactionError(ErrBlockNotFound))
}

func TestIsStorageError(t *testing.T) {
	assert.True(t, IsStorageError(ErrStorageClosed))
	assert.True(t, IsStorageError(ErrReadOnlyMode))
	assert.True(t, IsStorageError(ErrBlockManagerClosed))

	assert.False(t, IsStorageError(ErrTableNotFound))
}

func TestGetFormatError(t *testing.T) {
	// Direct FormatError
	fe := &FormatError{Op: "test", Err: errors.New("error")}
	result := GetFormatError(fe)
	assert.Equal(t, fe, result)

	// Wrapped FormatError
	wrapped := fmt.Errorf("wrapper: %w", fe)
	result = GetFormatError(wrapped)
	assert.Equal(t, fe, result)

	// Non-FormatError returns nil
	regular := errors.New("regular")
	result = GetFormatError(regular)
	assert.Nil(t, result)

	// nil returns nil
	result = GetFormatError(nil)
	assert.Nil(t, result)
}

func TestWrapWithContext(t *testing.T) {
	// nil error returns nil
	assert.Nil(t, WrapWithContext(nil, "test"))

	// Regular error gets wrapped
	err := WrapWithContext(errors.New("base error"), "operation")
	require.NotNil(t, err)
	assert.True(t, IsFormatError(err))
	assert.Contains(t, err.Error(), "operation")
	assert.Contains(t, err.Error(), "base error")

	// FormatError doesn't get double-wrapped
	fe := &FormatError{Op: "original", Err: errors.New("error")}
	result := WrapWithContext(fe, "new operation")
	assert.Equal(t, fe, result)
}

func TestWrapBlockError(t *testing.T) {
	// nil error returns nil
	assert.Nil(t, WrapBlockError(nil, 42, "test"))

	// Regular error gets wrapped with block context
	err := WrapBlockError(errors.New("disk error"), 100, "read")
	require.NotNil(t, err)
	fe := GetFormatError(err)
	require.NotNil(t, fe)
	assert.Equal(t, uint64(100), fe.BlockID)
	assert.True(t, fe.HasBlockID)
	assert.Contains(t, err.Error(), "block 100")
}

func TestWrapPathError(t *testing.T) {
	// nil error returns nil
	assert.Nil(t, WrapPathError(nil, "/path", "test"))

	// Regular error gets wrapped with path context
	err := WrapPathError(errors.New("not found"), "/tmp/test.duckdb", "open")
	require.NotNil(t, err)
	fe := GetFormatError(err)
	require.NotNil(t, fe)
	assert.Equal(t, "/tmp/test.duckdb", fe.Path)
	assert.Contains(t, err.Error(), "/tmp/test.duckdb")
}

func TestErrorsAs(t *testing.T) {
	fe := &FormatError{
		Op:         "test",
		BlockID:    42,
		HasBlockID: true,
		Err:        ErrBlockChecksumFailed,
	}

	// Test errors.As with FormatError
	wrapped := fmt.Errorf("outer: %w", fe)
	var target *FormatError
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, uint64(42), target.BlockID)
}

func TestSentinelErrorMessages(t *testing.T) {
	// Verify sentinel errors have clear, actionable messages
	tests := []struct {
		err      error
		contains []string
	}{
		{ErrInvalidMagicBytes, []string{"DUCK", "magic bytes"}},
		{ErrVersionTooNew, []string{"version", "67", "v1.4.3"}},
		{ErrBlockTooLarge, []string{"256KB"}},
		{ErrCatalogCorrupted, []string{"catalog", "corrupted"}},
		{ErrInvalidUUID, []string{"UUID", "16 bytes"}},
		{ErrFileTooSmall, []string{"too small"}},
		{ErrCorruptedFile, []string{"corrupted"}},
		{ErrTypeMismatch, []string{"mismatch"}},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			for _, s := range tt.contains {
				assert.Contains(t, tt.err.Error(), s,
					"error message should contain '%s'", s)
			}
		})
	}
}

func TestErrorChaining(t *testing.T) {
	// Test that errors properly chain for debugging
	inner := ErrBlockChecksumFailed
	middle := NewFormatErrorWithBlock("read", 42, inner)
	outer := fmt.Errorf("failed to load table: %w", middle)

	// Should be able to find inner error
	assert.True(t, errors.Is(outer, ErrBlockChecksumFailed))

	// Should be able to extract FormatError
	fe := GetFormatError(outer)
	require.NotNil(t, fe)
	assert.Equal(t, uint64(42), fe.BlockID)
}
