package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverRegistration(t *testing.T) {
	// Test that the driver is registered under the name "dukdb"
	drivers := sql.Drivers()
	found := false
	for _, name := range drivers {
		if name == "dukdb" {
			found = true
			break
		}
	}
	assert.True(
		t,
		found,
		"driver 'dukdb' should be registered",
	)
}

func TestDriverImplementsInterface(t *testing.T) {
	// Verify that Driver implements driver.Driver interface
	var d driver.Driver = &Driver{}
	assert.NotNil(t, d)
}

func TestDriverOpenWithoutBackend(t *testing.T) {
	// Clear any registered backend first
	RegisterBackend(nil)
	defer func() {
		// Restore the engine backend by importing the engine package
		// The engine package auto-registers via init()
	}()

	// Without a backend registered, Open should fail
	d := Driver{}
	_, err := d.Open(":memory:")
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"no backend registered",
	)
}

func TestDriverOpenConnectorWithoutBackend(
	t *testing.T,
) {
	// Clear any registered backend
	RegisterBackend(nil)

	d := Driver{}
	connector, err := d.OpenConnector(":memory:")
	// With lazy initialization, OpenConnector succeeds
	require.NoError(t, err)

	// But Connect fails without a backend
	_, err = connector.Connect(
		context.Background(),
	)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"no backend registered",
	)
}

func TestNewConnectorWithoutBackend(
	t *testing.T,
) {
	// Clear any registered backend
	RegisterBackend(nil)

	// With lazy initialization, NewConnector succeeds
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)

	// But Connect fails without a backend
	_, err = connector.Connect(
		context.Background(),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoBackend)
}

func TestErrorTypes(t *testing.T) {
	// Test that error types are properly defined
	tests := []struct {
		errType ErrorType
		value   int
	}{
		{ErrorTypeInvalid, 0},
		{ErrorTypeOutOfRange, 1},
		{ErrorTypeConversion, 2},
		{ErrorTypeUnknownType, 3},
		{ErrorTypeDecimal, 4},
		{ErrorTypeMismatchType, 5},
		{ErrorTypeDivideByZero, 6},
		{ErrorTypeObjectSize, 7},
		{ErrorTypeInvalidType, 8},
		{ErrorTypeSerialization, 9},
		{ErrorTypeTransaction, 10},
		{ErrorTypeNotImplemented, 11},
		{ErrorTypeExpression, 12},
		{ErrorTypeCatalog, 13},
		{ErrorTypeParser, 14},
		{ErrorTypePlanner, 15},
		{ErrorTypeScheduler, 16},
		{ErrorTypeExecutor, 17},
		{ErrorTypeConstraint, 18},
		{ErrorTypeIndex, 19},
		{ErrorTypeStat, 20},
		{ErrorTypeConnection, 21},
		{ErrorTypeSyntax, 22},
		{ErrorTypeSettings, 23},
		{ErrorTypeBinder, 24},
		{ErrorTypeNetwork, 25},
		{ErrorTypeOptimizer, 26},
		{ErrorTypeNullPointer, 27},
		{ErrorTypeIO, 28},
		{ErrorTypeInterrupt, 29},
		{ErrorTypeFatal, 30},
		{ErrorTypeInternal, 31},
		{ErrorTypeInvalidInput, 32},
		{ErrorTypeOutOfMemory, 33},
		{ErrorTypePermission, 34},
		{ErrorTypeParameterNotResolved, 35},
		{ErrorTypeParameterNotAllowed, 36},
		{ErrorTypeDependency, 37},
		{ErrorTypeHTTP, 38},
		{ErrorTypeMissingExtension, 39},
		{ErrorTypeAutoLoad, 40},
		{ErrorTypeSequence, 41},
		{ErrorTypeInvalidConfiguration, 42},
	}

	for _, tt := range tests {
		assert.Equal(
			t,
			tt.value,
			int(tt.errType),
			"ErrorType value mismatch",
		)
	}
}

func TestGetDuckDBError(t *testing.T) {
	tests := []struct {
		msg      string
		expected ErrorType
	}{
		{
			"Parser Error: syntax error",
			ErrorTypeParser,
		},
		{
			"Binder Error: column not found",
			ErrorTypeBinder,
		},
		{
			"Constraint Error: duplicate key",
			ErrorTypeConstraint,
		},
		{
			"Some random error without prefix",
			ErrorTypeInvalid,
		},
		{
			"Invalid Input Error: bad input",
			ErrorTypeInvalidInput,
		},
	}

	for _, tt := range tests {
		err := getDuckDBError(tt.msg)
		duckErr, ok := err.(*Error)
		require.True(
			t,
			ok,
			"expected *Error type",
		)
		assert.Equal(
			t,
			tt.expected,
			duckErr.Type,
			"error type mismatch for: %s",
			tt.msg,
		)
		assert.Equal(
			t,
			tt.msg,
			duckErr.Msg,
			"error message should be preserved",
		)
	}
}

func TestErrorInterface(t *testing.T) {
	err := &Error{
		Type: ErrorTypeParser,
		Msg:  "Parser Error: syntax error at line 1",
	}

	// Test Error() method
	assert.Equal(
		t,
		"Parser Error: syntax error at line 1",
		err.Error(),
	)

	// Test Is() method with matching error
	other := &Error{
		Msg: "Parser Error: syntax error at line 1",
	}
	assert.True(t, err.Is(other))

	// Test Is() method with non-matching error
	different := &Error{
		Msg: "Different error message",
	}
	assert.False(t, err.Is(different))
}

func TestStatementTypes(t *testing.T) {
	// Test that statement types are properly defined
	tests := []struct {
		stmtType StmtType
		value    int
	}{
		{STATEMENT_TYPE_INVALID, 0},
		{STATEMENT_TYPE_SELECT, 1},
		{STATEMENT_TYPE_INSERT, 2},
		{STATEMENT_TYPE_UPDATE, 3},
		{STATEMENT_TYPE_EXPLAIN, 4},
		{STATEMENT_TYPE_DELETE, 5},
		{STATEMENT_TYPE_PREPARE, 6},
		{STATEMENT_TYPE_CREATE, 7},
		{STATEMENT_TYPE_EXECUTE, 8},
		{STATEMENT_TYPE_ALTER, 9},
		{STATEMENT_TYPE_TRANSACTION, 10},
		{STATEMENT_TYPE_COPY, 11},
		{STATEMENT_TYPE_ANALYZE, 12},
		{STATEMENT_TYPE_VARIABLE_SET, 13},
		{STATEMENT_TYPE_CREATE_FUNC, 14},
		{STATEMENT_TYPE_DROP, 15},
		{STATEMENT_TYPE_EXPORT, 16},
		{STATEMENT_TYPE_PRAGMA, 17},
		{STATEMENT_TYPE_VACUUM, 18},
		{STATEMENT_TYPE_CALL, 19},
		{STATEMENT_TYPE_SET, 20},
		{STATEMENT_TYPE_LOAD, 21},
		{STATEMENT_TYPE_RELATION, 22},
		{STATEMENT_TYPE_EXTENSION, 23},
		{STATEMENT_TYPE_LOGICAL_PLAN, 24},
		{STATEMENT_TYPE_ATTACH, 25},
		{STATEMENT_TYPE_DETACH, 26},
		{STATEMENT_TYPE_MULTI, 27},
	}

	for _, tt := range tests {
		assert.Equal(
			t,
			tt.value,
			int(tt.stmtType),
			"StmtType value mismatch",
		)
	}
}

func TestParseDSN(t *testing.T) {
	tests := []struct {
		dsn          string
		expectedPath string
		accessMode   string
		threads      int
	}{
		{":memory:", ":memory:", "automatic", 0},
		{
			":memory:?access_mode=read_only",
			":memory:",
			"read_only",
			0,
		},
		{
			":memory:?threads=4",
			":memory:",
			"automatic",
			4,
		},
		{"", ":memory:", "automatic", 0},
		{
			":memory:?access_mode=read_only",
			":memory:",
			"read_only",
			0,
		},
		{
			"/path/to/db.duck",
			"/path/to/db.duck",
			"automatic",
			0,
		},
		{
			"/path/to/db.duck?threads=8",
			"/path/to/db.duck",
			"automatic",
			8,
		},
	}

	for _, tt := range tests {
		config, err := ParseDSN(tt.dsn)
		require.NoError(
			t,
			err,
			"ParseDSN failed for: %s",
			tt.dsn,
		)
		assert.Equal(
			t,
			tt.expectedPath,
			config.Path,
			"path mismatch for: %s",
			tt.dsn,
		)
		assert.Equal(
			t,
			tt.accessMode,
			config.AccessMode,
			"access_mode mismatch for: %s",
			tt.dsn,
		)
		// For threads=0 in test, we expect the default (runtime.NumCPU()), not 0
		if tt.threads != 0 {
			assert.Equal(
				t,
				tt.threads,
				config.Threads,
				"threads mismatch for: %s",
				tt.dsn,
			)
		}
	}
}

func TestDataTypes(t *testing.T) {
	// Verify that key data types are defined with correct enum values
	// (These values match the DuckDB internal type enumeration 0-36)
	assert.Equal(t, Type(0), TYPE_INVALID)
	assert.Equal(t, Type(1), TYPE_BOOLEAN)
	assert.Equal(t, Type(4), TYPE_INTEGER)
	assert.Equal(t, Type(5), TYPE_BIGINT)
	assert.Equal(t, Type(18), TYPE_VARCHAR)
}
