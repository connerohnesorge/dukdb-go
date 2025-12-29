package dukdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/quartz"
)

// appenderMockState holds state for appender-specific mock testing
type appenderMockState struct {
	mu            sync.Mutex
	queryResponse []map[string]any
	queryColumns  []string
	queryError    error
	execError     error
	execCount     int
	lastQuery     string
}

// newAppenderMock creates a new mock backend connection for appender tests
func newAppenderMock() (*mockBackendConn, *appenderMockState) {
	state := &appenderMockState{}
	mock := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.lastQuery = query
			state.execCount++
			return 1, state.execError
		},
		queryFunc: func(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error) {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.lastQuery = query
			if state.queryError != nil {
				return nil, nil, state.queryError
			}
			return state.queryResponse, state.queryColumns, nil
		},
	}
	return mock, state
}

// setTableColumns configures the mock to return column info for a table
func (s *appenderMockState) setTableColumns(
	columns []string,
	types []string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryColumns = []string{
		"column_name",
		"data_type",
	}
	s.queryResponse = make(
		[]map[string]any,
		len(columns),
	)
	for i, col := range columns {
		s.queryResponse[i] = map[string]any{
			"column_name": col,
			"data_type":   types[i],
		}
	}
}

// createAppenderTestConn creates a test connection with a mock backend
func createAppenderTestConn(
	mock *mockBackendConn,
) *Conn {
	connector := &Connector{}
	return &Conn{
		connector:   connector,
		backendConn: mock,
		closed:      false,
		tx:          false,
	}
}

// TestAppenderStruct tests the Appender struct fields (Task 1.1)
func TestAppenderStruct(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name"},
		[]string{"INTEGER", "VARCHAR"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Verify struct fields
	if appender.conn != conn {
		t.Error("conn field not set correctly")
	}
	if appender.catalog != "memory" {
		t.Errorf(
			"catalog: got %q, want %q",
			appender.catalog,
			"memory",
		)
	}
	if appender.schema != "main" {
		t.Errorf(
			"schema: got %q, want %q",
			appender.schema,
			"main",
		)
	}
	if appender.table != "test_table" {
		t.Errorf(
			"table: got %q, want %q",
			appender.table,
			"test_table",
		)
	}
	if len(appender.columns) != 2 {
		t.Errorf(
			"columns count: got %d, want %d",
			len(appender.columns),
			2,
		)
	}
	if appender.Threshold() != DefaultAppenderThreshold {
		t.Errorf(
			"threshold: got %d, want %d",
			appender.Threshold(),
			DefaultAppenderThreshold,
		)
	}
	if appender.IsClosed() {
		t.Error(
			"appender should not be closed initially",
		)
	}
}

// TestNewAppenderFromConn tests the NewAppenderFromConn function (Task 1.2)
func TestNewAppenderFromConn(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name", "value"},
		[]string{"INTEGER", "VARCHAR", "DOUBLE"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	if appender.catalog != "memory" {
		t.Errorf(
			"catalog should default to 'memory', got %q",
			appender.catalog,
		)
	}
	if appender.schema != "main" {
		t.Errorf(
			"schema: got %q, want %q",
			appender.schema,
			"main",
		)
	}
	if len(appender.Columns()) != 3 {
		t.Errorf(
			"expected 3 columns, got %d",
			len(appender.Columns()),
		)
	}
}

// TestNewAppender tests the NewAppender function (Task 1.3)
func TestNewAppender(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"col1"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	// Test with explicit catalog
	appender, err := NewAppender(
		conn,
		"mydb",
		"myschema",
		"mytable",
	)
	if err != nil {
		t.Fatalf("NewAppender failed: %v", err)
	}
	if appender.catalog != "mydb" {
		t.Errorf(
			"catalog: got %q, want %q",
			appender.catalog,
			"mydb",
		)
	}
	if appender.schema != "myschema" {
		t.Errorf(
			"schema: got %q, want %q",
			appender.schema,
			"myschema",
		)
	}

	// Test with empty catalog (should default to "memory")
	state.setTableColumns(
		[]string{"col1"},
		[]string{"INTEGER"},
	)
	appender, err = NewAppender(
		conn,
		"",
		"",
		"mytable",
	)
	if err != nil {
		t.Fatalf(
			"NewAppender with empty catalog failed: %v",
			err,
		)
	}
	if appender.catalog != "memory" {
		t.Errorf(
			"catalog should default to 'memory', got %q",
			appender.catalog,
		)
	}
	if appender.schema != "main" {
		t.Errorf(
			"schema should default to 'main', got %q",
			appender.schema,
		)
	}
}

// TestNewAppenderWithThreshold tests the NewAppenderWithThreshold function (Task 1.4)
func TestNewAppenderWithThreshold(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	// Test valid threshold
	appender, err := NewAppenderWithThreshold(
		conn,
		"",
		"main",
		"test",
		100,
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderWithThreshold failed: %v",
			err,
		)
	}
	if appender.Threshold() != 100 {
		t.Errorf(
			"threshold: got %d, want %d",
			appender.Threshold(),
			100,
		)
	}

	// Test invalid threshold (< 1)
	_, err = NewAppenderWithThreshold(
		conn,
		"",
		"main",
		"test",
		0,
	)
	if err == nil {
		t.Error(
			"expected error for threshold < 1",
		)
	}
	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeInvalid {
			t.Errorf(
				"error type: got %v, want ErrorTypeInvalid",
				dukErr.Type,
			)
		}
	} else {
		t.Errorf("expected *Error, got %T", err)
	}

	// Test threshold = 1 (minimum valid)
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	appender, err = NewAppenderWithThreshold(
		conn,
		"",
		"main",
		"test",
		1,
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderWithThreshold with threshold=1 failed: %v",
			err,
		)
	}
	if appender.Threshold() != 1 {
		t.Errorf(
			"threshold: got %d, want %d",
			appender.Threshold(),
			1,
		)
	}
}

// TestAppendRow tests the AppendRow method (Task 2.1)
func TestAppendRow(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name"},
		[]string{"INTEGER", "VARCHAR"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Append a row
	err = appender.AppendRow(1, "Alice")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	if appender.BufferSize() != 1 {
		t.Errorf(
			"buffer size: got %d, want %d",
			appender.BufferSize(),
			1,
		)
	}

	// Append another row
	err = appender.AppendRow(2, "Bob")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	if appender.BufferSize() != 2 {
		t.Errorf(
			"buffer size: got %d, want %d",
			appender.BufferSize(),
			2,
		)
	}
}

// TestAppendRowNullHandling tests NULL handling in AppendRow (Task 2.3)
func TestAppendRowNullHandling(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name"},
		[]string{"INTEGER", "VARCHAR"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Append row with nil value (should become NULL)
	err = appender.AppendRow(1, nil)
	if err != nil {
		t.Fatalf(
			"AppendRow with nil failed: %v",
			err,
		)
	}

	// Flush and verify the INSERT statement contains NULL
	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if state.lastQuery == "" {
		t.Fatal("expected a query to be executed")
	}
	// Verify NULL is in the query
	if !strings.Contains(
		state.lastQuery,
		"NULL",
	) {
		t.Errorf(
			"expected NULL in query, got: %s",
			state.lastQuery,
		)
	}
}

// TestFlush tests the Flush method (Task 3.1)
func TestFlush(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name"},
		[]string{"INTEGER", "VARCHAR"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Append some rows
	for i := 0; i < 5; i++ {
		err = appender.AppendRow(i, "test")
		if err != nil {
			t.Fatalf(
				"AppendRow %d failed: %v",
				i,
				err,
			)
		}
	}

	if appender.BufferSize() != 5 {
		t.Errorf(
			"buffer size before flush: got %d, want %d",
			appender.BufferSize(),
			5,
		)
	}

	// Flush
	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if appender.BufferSize() != 0 {
		t.Errorf(
			"buffer size after flush: got %d, want %d",
			appender.BufferSize(),
			0,
		)
	}

	// Verify exec was called
	if state.execCount != 1 {
		t.Errorf(
			"exec count: got %d, want %d",
			state.execCount,
			1,
		)
	}

	// Flush empty buffer (should be no-op)
	err = appender.Flush()
	if err != nil {
		t.Fatalf(
			"Flush empty buffer failed: %v",
			err,
		)
	}

	if state.execCount != 1 {
		t.Errorf(
			"exec count after empty flush: got %d, want %d",
			state.execCount,
			1,
		)
	}
}

// TestClose tests the Close method (Task 3.2)
func TestClose(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Append a row
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Close (should flush remaining data)
	err = appender.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !appender.IsClosed() {
		t.Error(
			"appender should be closed after Close()",
		)
	}

	// Verify the data was flushed
	if state.execCount != 1 {
		t.Errorf(
			"exec count: got %d, want %d (flush on close)",
			state.execCount,
			1,
		)
	}
}

// TestAutoFlushThreshold tests auto-flush when threshold is reached (Task 3.3 / 5.2)
func TestAutoFlushThreshold(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	// Create appender with small threshold
	appender, err := NewAppenderWithThreshold(
		conn,
		"",
		"main",
		"test",
		3,
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderWithThreshold failed: %v",
			err,
		)
	}

	// Add rows up to threshold (should not trigger auto-flush yet)
	for i := 0; i < 3; i++ {
		err = appender.AppendRow(i)
		if err != nil {
			t.Fatalf(
				"AppendRow %d failed: %v",
				i,
				err,
			)
		}
	}

	// Buffer should be at threshold
	if appender.BufferSize() != 3 {
		t.Errorf(
			"buffer size: got %d, want %d",
			appender.BufferSize(),
			3,
		)
	}
	if state.execCount != 0 {
		t.Errorf(
			"exec count before threshold exceeded: got %d, want %d",
			state.execCount,
			0,
		)
	}

	// Add one more row - should trigger auto-flush
	err = appender.AppendRow(3)
	if err != nil {
		t.Fatalf("AppendRow 3 failed: %v", err)
	}

	// Should have flushed and now have 1 row in buffer
	if state.execCount != 1 {
		t.Errorf(
			"exec count after threshold exceeded: got %d, want %d",
			state.execCount,
			1,
		)
	}
	if appender.BufferSize() != 1 {
		t.Errorf(
			"buffer size after auto-flush: got %d, want %d",
			appender.BufferSize(),
			1,
		)
	}
}

// TestTableNotFoundError tests error when table doesn't exist (Task 4.1)
func TestTableNotFoundError(t *testing.T) {
	mock, state := newAppenderMock()
	// Empty query response means table not found
	state.queryResponse = []map[string]any{}
	state.queryColumns = []string{
		"column_name",
		"data_type",
	}
	conn := createAppenderTestConn(mock)

	_, err := NewAppenderFromConn(
		conn,
		"main",
		"nonexistent_table",
	)
	if err == nil {
		t.Fatal(
			"expected error for nonexistent table",
		)
	}

	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeCatalog {
			t.Errorf(
				"error type: got %v, want ErrorTypeCatalog",
				dukErr.Type,
			)
		}
	} else {
		t.Errorf("expected *Error, got %T", err)
	}
}

// TestColumnCountMismatchError tests error when column count doesn't match (Task 4.2)
func TestColumnCountMismatchError(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name", "value"},
		[]string{"INTEGER", "VARCHAR", "DOUBLE"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Try to append row with wrong number of values
	err = appender.AppendRow(
		1,
		"test",
	) // Only 2 values, but table has 3 columns
	if err == nil {
		t.Fatal(
			"expected error for column count mismatch",
		)
	}

	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeInvalid {
			t.Errorf(
				"error type: got %v, want ErrorTypeInvalid",
				dukErr.Type,
			)
		}
	} else {
		t.Errorf("expected *Error, got %T", err)
	}
}

// TestAppendAfterCloseError tests error when appending after close (Task 4.3)
func TestAppendAfterCloseError(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Close the appender
	err = appender.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to append after close
	err = appender.AppendRow(1)
	if err == nil {
		t.Fatal(
			"expected error for append after close",
		)
	}

	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeClosed {
			t.Errorf(
				"error type: got %v, want ErrorTypeClosed",
				dukErr.Type,
			)
		}
	} else {
		t.Errorf("expected *Error, got %T", err)
	}

	// Try to flush after close
	err = appender.Flush()
	if err == nil {
		t.Fatal(
			"expected error for flush after close",
		)
	}
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeClosed {
			t.Errorf(
				"error type: got %v, want ErrorTypeClosed",
				dukErr.Type,
			)
		}
	}

	// Try to close again (double close)
	err = appender.Close()
	if err == nil {
		t.Fatal("expected error for double close")
	}
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeClosed {
			t.Errorf(
				"error type: got %v, want ErrorTypeClosed",
				dukErr.Type,
			)
		}
	}
}

// TestFlushErrorPreservation tests that buffer is preserved on flush error (Task 4.4)
func TestFlushErrorPreservation(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Add some rows
	for i := 0; i < 5; i++ {
		err = appender.AppendRow(i)
		if err != nil {
			t.Fatalf(
				"AppendRow %d failed: %v",
				i,
				err,
			)
		}
	}

	// Set exec to fail
	state.mu.Lock()
	state.execError = errors.New("database error")
	state.mu.Unlock()

	// Try to flush - should fail
	err = appender.Flush()
	if err == nil {
		t.Fatal("expected error from flush")
	}

	// Buffer should be preserved for retry
	if appender.BufferSize() != 5 {
		t.Errorf(
			"buffer size after failed flush: got %d, want %d",
			appender.BufferSize(),
			5,
		)
	}

	// Clear the error and retry
	state.mu.Lock()
	state.execError = nil
	state.mu.Unlock()
	err = appender.Flush()
	if err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Now buffer should be cleared
	if appender.BufferSize() != 0 {
		t.Errorf(
			"buffer size after successful flush: got %d, want %d",
			appender.BufferSize(),
			0,
		)
	}
}

// TestTypeConversion tests type conversion in AppendRow (Task 5.3)
func TestTypeConversion(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{
			"bool_col",
			"int_col",
			"float_col",
			"str_col",
			"time_col",
			"blob_col",
		},
		[]string{
			"BOOLEAN",
			"INTEGER",
			"DOUBLE",
			"VARCHAR",
			"TIMESTAMP",
			"BLOB",
		},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Test various types
	testTime := time.Date(
		2024,
		1,
		15,
		10,
		30,
		0,
		0,
		time.UTC,
	)
	testBlob := []byte{0x01, 0x02, 0x03}

	err = appender.AppendRow(
		true,
		int64(42),
		3.14,
		"hello",
		testTime,
		testBlob,
	)
	if err != nil {
		t.Fatalf(
			"AppendRow with various types failed: %v",
			err,
		)
	}

	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify the query was built correctly
	if state.lastQuery == "" {
		t.Fatal("expected query to be executed")
	}

	// Check for expected values in the query
	expectedParts := []string{
		"TRUE",
		"42",
		"3.14",
		"'hello'",
		"X'010203'",
	}
	for _, part := range expectedParts {
		if !strings.Contains(
			state.lastQuery,
			part,
		) {
			t.Errorf(
				"query missing expected part %q\nQuery: %s",
				part,
				state.lastQuery,
			)
		}
	}
}

// TestConcurrentAccess tests thread safety (Task 5.5)
func TestConcurrentAccess(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderWithThreshold(
		conn,
		"",
		"main",
		"test",
		1000,
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderWithThreshold failed: %v",
			err,
		)
	}

	// Run concurrent appends
	const numGoroutines = 10
	const rowsPerGoroutine = 100

	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < rowsPerGoroutine; i++ {
				if err := appender.AppendRow(gid*rowsPerGoroutine + i); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf(
			"concurrent append error: %v",
			err,
		)
	}

	// Flush remaining
	err = appender.Flush()
	if err != nil {
		t.Fatalf("final flush failed: %v", err)
	}

	// Close
	err = appender.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// TestParseDataType tests the parseDataType function
func TestParseDataType(t *testing.T) {
	tests := []struct {
		input    string
		expected Type
	}{
		{"BOOLEAN", TYPE_BOOLEAN},
		{"BOOL", TYPE_BOOLEAN},
		{"INTEGER", TYPE_INTEGER},
		{"INT", TYPE_INTEGER},
		{"INT4", TYPE_INTEGER},
		{"BIGINT", TYPE_BIGINT},
		{"INT8", TYPE_BIGINT},
		{"VARCHAR", TYPE_VARCHAR},
		{"TEXT", TYPE_VARCHAR},
		{"STRING", TYPE_VARCHAR},
		{"DOUBLE", TYPE_DOUBLE},
		{"FLOAT8", TYPE_DOUBLE},
		{"FLOAT", TYPE_FLOAT},
		{"FLOAT4", TYPE_FLOAT},
		{"REAL", TYPE_FLOAT},
		{"TIMESTAMP", TYPE_TIMESTAMP},
		{"TIMESTAMPTZ", TYPE_TIMESTAMP_TZ},
		{"DATE", TYPE_DATE},
		{"TIME", TYPE_TIME},
		{"TIMETZ", TYPE_TIME_TZ},
		{"BLOB", TYPE_BLOB},
		{"BYTEA", TYPE_BLOB},
		{"UUID", TYPE_UUID},
		{"DECIMAL", TYPE_DECIMAL},
		{"DECIMAL(10,2)", TYPE_DECIMAL},
		{"VARCHAR(255)", TYPE_VARCHAR},
		{"INTERVAL", TYPE_INTERVAL},
		{"HUGEINT", TYPE_HUGEINT},
		{"UHUGEINT", TYPE_UHUGEINT},
		{"LIST", TYPE_LIST},
		{"STRUCT", TYPE_STRUCT},
		{"MAP", TYPE_MAP},
		{"ARRAY", TYPE_ARRAY},
		{"UNION", TYPE_UNION},
		{"ENUM", TYPE_ENUM},
		{"UNKNOWN_TYPE", TYPE_INVALID},
		{"", TYPE_INVALID},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := parseDataType(tc.input)
			if result != tc.expected {
				t.Errorf(
					"parseDataType(%q): got %v, want %v",
					tc.input,
					result,
					tc.expected,
				)
			}
		})
	}
}

// TestQuoteIdentifier tests the quoteIdentifier function
func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "\"simple\""},
		{"with space", "\"with space\""},
		{"with\"quote", "\"with\"\"quote\""},
		{"", "\"\""},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := quoteIdentifier(tc.input)
			if result != tc.expected {
				t.Errorf(
					"quoteIdentifier(%q): got %q, want %q",
					tc.input,
					result,
					tc.expected,
				)
			}
		})
	}
}

// TestBuildInsert tests the buildInsert method
func TestBuildInsert(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name"},
		[]string{"INTEGER", "VARCHAR"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	// Append some rows
	err = appender.AppendRow(1, "Alice")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}
	err = appender.AppendRow(2, "Bob")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}
	err = appender.AppendRow(3, nil) // NULL value
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Flush to generate the INSERT statement
	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify the generated query
	expectedContains := []string{
		`INSERT INTO "main"."test_table"`,
		`("id", "name")`,
		`VALUES`,
		`(1, 'Alice')`,
		`(2, 'Bob')`,
		`(3, NULL)`,
	}

	for _, substr := range expectedContains {
		if !strings.Contains(
			state.lastQuery,
			substr,
		) {
			t.Errorf(
				"query missing expected substring %q\nQuery: %s",
				substr,
				state.lastQuery,
			)
		}
	}
}

// TestAppenderColumns tests the Columns() accessor
func TestAppenderColumns(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name", "created_at"},
		[]string{
			"INTEGER",
			"VARCHAR",
			"TIMESTAMP",
		},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	cols := appender.Columns()
	if len(cols) != 3 {
		t.Errorf(
			"expected 3 columns, got %d",
			len(cols),
		)
	}
	expected := []string{
		"id",
		"name",
		"created_at",
	}
	for i, col := range cols {
		if col != expected[i] {
			t.Errorf(
				"column %d: got %q, want %q",
				i,
				col,
				expected[i],
			)
		}
	}
}

// TestAppenderColumnTypes tests the ColumnTypes() accessor
func TestAppenderColumnTypes(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id", "name", "value"},
		[]string{"INTEGER", "VARCHAR", "DOUBLE"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf(
			"NewAppenderFromConn failed: %v",
			err,
		)
	}

	types := appender.ColumnTypes()
	if len(types) != 3 {
		t.Errorf(
			"expected 3 column types, got %d",
			len(types),
		)
	}
	expected := []Type{
		TYPE_INTEGER,
		TYPE_VARCHAR,
		TYPE_DOUBLE,
	}
	for i, typ := range types {
		if typ != expected[i] {
			t.Errorf(
				"column type %d: got %v, want %v",
				i,
				typ,
				expected[i],
			)
		}
	}
}

// === Query Appender Tests ===

// TestNewQueryAppender_EmptyQuery tests error for empty query
func TestNewQueryAppender_EmptyQuery(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	_, err := NewQueryAppender(
		conn,
		"", // empty query
		"appended_data",
		[]TypeInfo{intInfo},
		[]string{"id"},
	)

	if err == nil {
		t.Fatal("expected error for empty query")
	}
	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeInvalid {
			t.Errorf(
				"error type: got %v, want ErrorTypeInvalid",
				dukErr.Type,
			)
		}
	}
}

// TestNewQueryAppender_EmptyColTypes tests error for empty column types
func TestNewQueryAppender_EmptyColTypes(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	_, err := NewQueryAppender(
		conn,
		"INSERT INTO test SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{}, // empty column types
		[]string{},
	)

	if err == nil {
		t.Fatal("expected error for empty column types")
	}
	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeInvalid {
			t.Errorf(
				"error type: got %v, want ErrorTypeInvalid",
				dukErr.Type,
			)
		}
	}
}

// TestNewQueryAppender_ColNamesMismatch tests error for column name/type mismatch
func TestNewQueryAppender_ColNamesMismatch(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	_, err := NewQueryAppender(
		conn,
		"INSERT INTO test SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo},      // 1 type
		[]string{"col1", "col2"}, // 2 names - mismatch
	)

	if err == nil {
		t.Fatal("expected error for column names/types mismatch")
	}
	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeInvalid {
			t.Errorf(
				"error type: got %v, want ErrorTypeInvalid",
				dukErr.Type,
			)
		}
	}
}

// TestNewQueryAppender_DefaultTableName tests default table name
func TestNewQueryAppender_DefaultTableName(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO test SELECT * FROM appended_data",
		"", // empty table name - should default to "appended_data"
		[]TypeInfo{intInfo},
		[]string{"id"},
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	if appender.tempTableName != "appended_data" {
		t.Errorf(
			"tempTableName: got %q, want %q",
			appender.tempTableName,
			"appended_data",
		)
	}
}

// TestNewQueryAppender_DefaultColumnNames tests default column names
func TestNewQueryAppender_DefaultColumnNames(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO test SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo, varcharInfo},
		[]string{}, // empty column names - should default to col1, col2
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	if len(appender.queryColNames) != 2 {
		t.Fatalf("expected 2 column names, got %d", len(appender.queryColNames))
	}
	if appender.queryColNames[0] != "col1" {
		t.Errorf("column 0: got %q, want %q", appender.queryColNames[0], "col1")
	}
	if appender.queryColNames[1] != "col2" {
		t.Errorf("column 1: got %q, want %q", appender.queryColNames[1], "col2")
	}
}

// TestNewQueryAppender_Valid tests valid query appender creation
func TestNewQueryAppender_Valid(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO target SELECT * FROM my_data",
		"my_data",
		[]TypeInfo{intInfo, varcharInfo},
		[]string{"id", "name"},
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	if !appender.isQueryAppender {
		t.Error("expected isQueryAppender to be true")
	}
	if appender.query != "INSERT INTO target SELECT * FROM my_data" {
		t.Errorf("query: got %q", appender.query)
	}
	if appender.tempTableName != "my_data" {
		t.Errorf("tempTableName: got %q, want %q", appender.tempTableName, "my_data")
	}
	if len(appender.queryColTypes) != 2 {
		t.Errorf("queryColTypes length: got %d, want %d", len(appender.queryColTypes), 2)
	}
	if len(appender.queryColNames) != 2 {
		t.Errorf("queryColNames length: got %d, want %d", len(appender.queryColNames), 2)
	}
}

// TestQueryAppender_FlushWithInsert tests flush with INSERT query
func TestQueryAppender_FlushWithInsert(t *testing.T) {
	mock, state := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo, varcharInfo},
		[]string{"id", "name"},
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	// Append rows
	err = appender.AppendRow(1, "Alice")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}
	err = appender.AppendRow(2, "Bob")
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Flush
	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify the sequence of queries executed:
	// 1. CREATE TEMP TABLE
	// 2. DELETE FROM (truncate)
	// 3. INSERT INTO temp table
	// 4. Execute user query
	if state.execCount != 4 {
		t.Errorf("exec count: got %d, want 4 (CREATE, DELETE, INSERT, user query)", state.execCount)
	}

	// Verify buffer is cleared
	if appender.BufferSize() != 0 {
		t.Errorf("buffer size after flush: got %d, want 0", appender.BufferSize())
	}
}

// TestQueryAppender_Close tests proper cleanup on close
func TestQueryAppender_Close(t *testing.T) {
	mock, state := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo},
		[]string{"id"},
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	// Append a row and flush to create the temp table
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}
	err = appender.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Now close - should drop the temp table
	initialExecCount := state.execCount
	err = appender.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify DROP TABLE was executed
	if state.execCount <= initialExecCount {
		t.Error("expected DROP TABLE to be executed on close")
	}
	if !strings.Contains(state.lastQuery, "DROP TABLE") {
		t.Errorf("expected DROP TABLE in last query, got: %s", state.lastQuery)
	}
}

// TestQueryAppender_AutoFlush tests auto-flush behavior
func TestQueryAppender_AutoFlush(t *testing.T) {
	mock, state := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	appender, err := NewQueryAppenderWithThreshold(
		conn,
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo},
		[]string{"id"},
		3, // threshold of 3
	)

	if err != nil {
		t.Fatalf("NewQueryAppenderWithThreshold failed: %v", err)
	}

	// Append 3 rows - at threshold but not exceeded
	for i := 0; i < 3; i++ {
		err = appender.AppendRow(i)
		if err != nil {
			t.Fatalf("AppendRow %d failed: %v", i, err)
		}
	}

	if state.execCount != 0 {
		t.Errorf("exec count before threshold exceeded: got %d, want 0", state.execCount)
	}

	// Append one more - should trigger auto-flush
	err = appender.AppendRow(3)
	if err != nil {
		t.Fatalf("AppendRow 3 failed: %v", err)
	}

	if state.execCount == 0 {
		t.Error("expected auto-flush to execute queries")
	}
}

// TestQueryAppender_NestedTypesSchema tests temp table creation with nested types
func TestQueryAppender_NestedTypesSchema(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	listInfo, _ := NewListInfo(intInfo)
	mapInfo, _ := NewMapInfo(varcharInfo, intInfo)

	// Verify that query appender creation works with nested types
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo, listInfo, mapInfo},
		[]string{"id", "numbers", "metadata"},
	)

	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	// Verify the types are stored correctly
	if len(appender.queryColTypes) != 3 {
		t.Errorf("queryColTypes length: got %d, want 3", len(appender.queryColTypes))
	}

	// Verify SQLType() produces correct strings for nested types
	expected := []string{"INTEGER", "INTEGER[]", "MAP(VARCHAR, INTEGER)"}
	for i, typeInfo := range appender.queryColTypes {
		sqlType := typeInfo.SQLType()
		if sqlType != expected[i] {
			t.Errorf("SQLType[%d]: got %q, want %q", i, sqlType, expected[i])
		}
	}
}

// TestQueryAppender_InvalidConnection tests error for non-*Conn
func TestQueryAppender_InvalidConnection(t *testing.T) {
	// Create a mock that doesn't implement driver.Conn properly
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	_, err := NewQueryAppender(
		nil, // nil connection
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo},
		[]string{"id"},
	)

	if err == nil {
		t.Fatal("expected error for nil connection")
	}
	var dukErr *Error
	if errors.As(err, &dukErr) {
		if dukErr.Type != ErrorTypeConnection {
			t.Errorf(
				"error type: got %v, want ErrorTypeConnection",
				dukErr.Type,
			)
		}
	}
}

// === Deterministic Testing Integration Tests ===

// TestFlushWithContext_Timeout tests deterministic timeout behavior using quartz.Mock
func TestFlushWithContext_Timeout(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf("NewAppenderFromConn failed: %v", err)
	}

	// Append a row
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Create a mock clock
	mClock := quartz.NewMock(t)

	// Create a deadline based on mock clock time
	deadline := mClock.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Advance the clock past the deadline
	mClock.Advance(2 * time.Second)

	// Now the clock says we're past the deadline
	appCtx := NewAppenderContext(ctx, mClock)
	err = appender.FlushWithContext(appCtx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}

	// Buffer should be preserved
	if appender.BufferSize() != 1 {
		t.Errorf("buffer size after timeout: got %d, want 1", appender.BufferSize())
	}
}

// TestFlushWithContext_Success tests successful flush with context
func TestFlushWithContext_Success(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf("NewAppenderFromConn failed: %v", err)
	}

	// Append a row
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Use context without deadline - should succeed
	mClock := quartz.NewMock(t)
	ctx := context.Background()
	appCtx := NewAppenderContext(ctx, mClock)

	err = appender.FlushWithContext(appCtx)
	if err != nil {
		t.Fatalf("FlushWithContext failed: %v", err)
	}

	// Buffer should be cleared
	if appender.BufferSize() != 0 {
		t.Errorf("buffer size after flush: got %d, want 0", appender.BufferSize())
	}
}

// TestFlushWithContext_BeforeDeadline tests that flush succeeds before deadline
func TestFlushWithContext_BeforeDeadline(t *testing.T) {
	mock, state := newAppenderMock()
	state.setTableColumns(
		[]string{"id"},
		[]string{"INTEGER"},
	)
	conn := createAppenderTestConn(mock)

	appender, err := NewAppenderFromConn(
		conn,
		"main",
		"test_table",
	)
	if err != nil {
		t.Fatalf("NewAppenderFromConn failed: %v", err)
	}

	// Append a row
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Create a mock clock
	mClock := quartz.NewMock(t)

	// Create a deadline in the future (1 hour from mock clock now)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Hour))
	defer cancel()

	// Don't advance the clock - we're still before deadline
	appCtx := NewAppenderContext(ctx, mClock)
	err = appender.FlushWithContext(appCtx)
	if err != nil {
		t.Fatalf("FlushWithContext failed: %v", err)
	}

	// Buffer should be cleared
	if appender.BufferSize() != 0 {
		t.Errorf("buffer size after flush: got %d, want 0", appender.BufferSize())
	}
}

// TestFlushWithContext_QueryAppenderTimeout tests timeout for query appender
func TestFlushWithContext_QueryAppenderTimeout(t *testing.T) {
	mock, _ := newAppenderMock()
	conn := createAppenderTestConn(mock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	appender, err := NewQueryAppender(
		conn,
		"INSERT INTO target SELECT * FROM appended_data",
		"appended_data",
		[]TypeInfo{intInfo},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("NewQueryAppender failed: %v", err)
	}

	// Append a row
	err = appender.AppendRow(1)
	if err != nil {
		t.Fatalf("AppendRow failed: %v", err)
	}

	// Create a mock clock
	mClock := quartz.NewMock(t)

	// Create a deadline and advance past it
	deadline := mClock.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	mClock.Advance(2 * time.Second)

	appCtx := NewAppenderContext(ctx, mClock)
	err = appender.FlushWithContext(appCtx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}
}

// TestAppenderContext_NilClock tests that nil clock uses real clock
func TestAppenderContext_NilClock(t *testing.T) {
	ctx := context.Background()
	appCtx := NewAppenderContext(ctx, nil)

	// Verify that the clock is not nil (uses real clock)
	if appCtx.clock == nil {
		t.Error("expected non-nil clock when passing nil")
	}
}
