package dukdb

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

// mockBackendConn implements BackendConn for testing
type mockBackendConn struct {
	executeFunc func(ctx context.Context, query string, args []driver.NamedValue) (int64, error)
	queryFunc   func(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error)
	prepareFunc func(ctx context.Context, query string) (BackendStmt, error)
	closeFunc   func() error
	pingFunc    func(ctx context.Context) error
}

func (m *mockBackendConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, query, args)
	}
	return 0, nil
}

func (m *mockBackendConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query, args)
	}
	return nil, nil, nil
}

func (m *mockBackendConn) Prepare(
	ctx context.Context,
	query string,
) (BackendStmt, error) {
	if m.prepareFunc != nil {
		return m.prepareFunc(ctx, query)
	}
	return nil, nil
}

func (m *mockBackendConn) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockBackendConn) Ping(
	ctx context.Context,
) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func (m *mockBackendConn) AppendDataChunk(
	ctx context.Context,
	schema, table string,
	chunk *DataChunk,
) (int64, error) {
	return 0, nil
}

func (m *mockBackendConn) GetTableTypeInfos(
	schema, table string,
) ([]TypeInfo, []string, error) {
	return nil, nil, nil
}

// mockBackendStmt implements BackendStmt for testing
type mockBackendStmt struct {
	executeFunc func(ctx context.Context, args []driver.NamedValue) (int64, error)
	queryFunc   func(ctx context.Context, args []driver.NamedValue) ([]map[string]any, []string, error)
	closeFunc   func() error
	numInput    int
}

func (m *mockBackendStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, args)
	}
	return 0, nil
}

func (m *mockBackendStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, args)
	}
	return nil, nil, nil
}

func (m *mockBackendStmt) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockBackendStmt) NumInput() int {
	return m.numInput
}

// TestTransactionBegin tests that BeginTx starts a transaction
func TestTransactionBegin(t *testing.T) {
	var executedQueries []string

	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			executedQueries = append(
				executedQueries,
				query,
			)
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
	}

	tx, err := conn.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx == nil {
		t.Fatal("expected tx to be non-nil")
	}

	if len(executedQueries) != 1 {
		t.Fatalf(
			"expected 1 query executed, got %d",
			len(executedQueries),
		)
	}

	if executedQueries[0] != "BEGIN TRANSACTION" {
		t.Errorf(
			"expected BEGIN TRANSACTION, got %q",
			executedQueries[0],
		)
	}

	if !conn.tx {
		t.Error("expected conn.tx to be true")
	}
}

// TestTransactionCommit tests that Commit sends COMMIT
func TestTransactionCommit(t *testing.T) {
	var executedQueries []string

	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			executedQueries = append(
				executedQueries,
				query,
			)
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
		tx:          true,
	}

	txObj := &tx{conn: conn}

	err := txObj.Commit()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(executedQueries) != 1 {
		t.Fatalf(
			"expected 1 query executed, got %d",
			len(executedQueries),
		)
	}

	if executedQueries[0] != "COMMIT" {
		t.Errorf(
			"expected COMMIT, got %q",
			executedQueries[0],
		)
	}

	if conn.tx {
		t.Error(
			"expected conn.tx to be false after commit",
		)
	}

	if !txObj.done {
		t.Error(
			"expected tx.done to be true after commit",
		)
	}
}

// TestTransactionRollback tests that Rollback sends ROLLBACK
func TestTransactionRollback(t *testing.T) {
	var executedQueries []string

	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			executedQueries = append(
				executedQueries,
				query,
			)
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
		tx:          true,
	}

	txObj := &tx{conn: conn}

	err := txObj.Rollback()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(executedQueries) != 1 {
		t.Fatalf(
			"expected 1 query executed, got %d",
			len(executedQueries),
		)
	}

	if executedQueries[0] != "ROLLBACK" {
		t.Errorf(
			"expected ROLLBACK, got %q",
			executedQueries[0],
		)
	}

	if conn.tx {
		t.Error(
			"expected conn.tx to be false after rollback",
		)
	}

	if !txObj.done {
		t.Error(
			"expected tx.done to be true after rollback",
		)
	}
}

// TestTransactionDoubleCommit tests that double commit returns an error
func TestTransactionDoubleCommit(t *testing.T) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
		tx:          true,
	}

	txObj := &tx{conn: conn}

	// First commit should succeed
	err := txObj.Commit()
	if err != nil {
		t.Fatalf(
			"unexpected error on first commit: %v",
			err,
		)
	}

	// Second commit should fail
	err = txObj.Commit()
	if err == nil {
		t.Error(
			"expected error on second commit, got nil",
		)
	}

	dukErr, ok := err.(*Error)
	if !ok {
		t.Errorf("expected *Error, got %T", err)
	} else {
		if dukErr.Type != ErrorTypeTransaction {
			t.Errorf("expected ErrorTypeTransaction, got %d", dukErr.Type)
		}
		if dukErr.Msg != "transaction already completed" {
			t.Errorf("unexpected error message: %s", dukErr.Msg)
		}
	}
}

// TestTransactionDoubleRollback tests that double rollback returns an error
func TestTransactionDoubleRollback(t *testing.T) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
		tx:          true,
	}

	txObj := &tx{conn: conn}

	// First rollback should succeed
	err := txObj.Rollback()
	if err != nil {
		t.Fatalf(
			"unexpected error on first rollback: %v",
			err,
		)
	}

	// Second rollback should fail
	err = txObj.Rollback()
	if err == nil {
		t.Error(
			"expected error on second rollback, got nil",
		)
	}

	dukErr, ok := err.(*Error)
	if !ok {
		t.Errorf("expected *Error, got %T", err)
	} else {
		if dukErr.Type != ErrorTypeTransaction {
			t.Errorf("expected ErrorTypeTransaction, got %d", dukErr.Type)
		}
	}
}

// TestTransactionCommitThenRollback tests that rollback after commit fails
func TestTransactionCommitThenRollback(
	t *testing.T,
) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
		tx:          true,
	}

	txObj := &tx{conn: conn}

	// Commit
	err := txObj.Commit()
	if err != nil {
		t.Fatalf(
			"unexpected error on commit: %v",
			err,
		)
	}

	// Rollback should fail
	err = txObj.Rollback()
	if err == nil {
		t.Error(
			"expected error on rollback after commit, got nil",
		)
	}
}

// TestTransactionIsolationLevel tests that non-default isolation level is rejected
func TestTransactionIsolationLevel(t *testing.T) {
	mockConn := &mockBackendConn{}

	conn := &Conn{
		backendConn: mockConn,
	}

	_, err := conn.BeginTx(
		context.Background(),
		driver.TxOptions{
			Isolation: driver.IsolationLevel(
				1,
			), // Non-default
		},
	)
	if err == nil {
		t.Error(
			"expected error for non-default isolation level, got nil",
		)
	}
}

// TestTransactionReadOnly tests that read-only transactions are rejected
func TestTransactionReadOnly(t *testing.T) {
	mockConn := &mockBackendConn{}

	conn := &Conn{
		backendConn: mockConn,
	}

	_, err := conn.BeginTx(
		context.Background(),
		driver.TxOptions{
			ReadOnly: true,
		},
	)
	if err == nil {
		t.Error(
			"expected error for read-only transaction, got nil",
		)
	}
}

// TestTransactionMultiple tests that multiple transactions on same connection are rejected
func TestTransactionMultiple(t *testing.T) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
	}

	// Start first transaction
	_, err := conn.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
	if err != nil {
		t.Fatalf(
			"unexpected error on first BeginTx: %v",
			err,
		)
	}

	// Try to start second transaction
	_, err = conn.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
	if err == nil {
		t.Error(
			"expected error for multiple transactions, got nil",
		)
	}
}

// TestContextCancellationExecContext tests that cancelled context is detected in ExecContext
func TestContextCancellationExecContext(
	t *testing.T,
) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			// This should never be called because context is already cancelled
			t.Error(
				"Execute should not be called with cancelled context",
			)
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := conn.ExecContext(
		ctx,
		"SELECT 1",
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for cancelled context, got nil",
		)
	}

	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

// TestContextCancellationQueryContext tests that cancelled context is detected in QueryContext
func TestContextCancellationQueryContext(
	t *testing.T,
) {
	mockConn := &mockBackendConn{
		queryFunc: func(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error) {
			// This should never be called because context is already cancelled
			t.Error(
				"Query should not be called with cancelled context",
			)
			return nil, nil, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := conn.QueryContext(
		ctx,
		"SELECT 1",
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for cancelled context, got nil",
		)
	}

	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

// TestContextDeadlineExceededExecContext tests that deadline exceeded context is detected
func TestContextDeadlineExceededExecContext(
	t *testing.T,
) {
	mockConn := &mockBackendConn{
		executeFunc: func(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
			t.Error(
				"Execute should not be called with expired context",
			)
			return 0, nil
		},
	}

	conn := &Conn{
		backendConn: mockConn,
	}

	// Create context that has already expired
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	_, err := conn.ExecContext(
		ctx,
		"SELECT 1",
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for expired context, got nil",
		)
	}

	if err != context.DeadlineExceeded {
		t.Errorf(
			"expected context.DeadlineExceeded, got %v",
			err,
		)
	}
}

// TestStmtContextCancellationExecContext tests context cancellation in Stmt.ExecContext
func TestStmtContextCancellationExecContext(
	t *testing.T,
) {
	mockStmt := &mockBackendStmt{
		executeFunc: func(ctx context.Context, args []driver.NamedValue) (int64, error) {
			t.Error(
				"Execute should not be called with cancelled context",
			)
			return 0, nil
		},
	}

	stmt := &Stmt{
		conn:        &Conn{},
		backendStmt: mockStmt,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := stmt.ExecContext(ctx, nil)
	if err == nil {
		t.Error(
			"expected error for cancelled context, got nil",
		)
	}

	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

// TestStmtContextCancellationQueryContext tests context cancellation in Stmt.QueryContext
func TestStmtContextCancellationQueryContext(
	t *testing.T,
) {
	mockStmt := &mockBackendStmt{
		queryFunc: func(ctx context.Context, args []driver.NamedValue) ([]map[string]any, []string, error) {
			t.Error(
				"Query should not be called with cancelled context",
			)
			return nil, nil, nil
		},
	}

	stmt := &Stmt{
		conn:        &Conn{},
		backendStmt: mockStmt,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := stmt.QueryContext(ctx, nil)
	if err == nil {
		t.Error(
			"expected error for cancelled context, got nil",
		)
	}

	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

// TestStmtClosedExecContext tests that closed statement returns error
func TestStmtClosedExecContext(t *testing.T) {
	stmt := &Stmt{
		closed: true,
	}

	_, err := stmt.ExecContext(
		context.Background(),
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for closed statement, got nil",
		)
	}
}

// TestStmtClosedQueryContext tests that closed statement returns error
func TestStmtClosedQueryContext(t *testing.T) {
	stmt := &Stmt{
		closed: true,
	}

	_, err := stmt.QueryContext(
		context.Background(),
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for closed statement, got nil",
		)
	}
}

// TestConnClosedExecContext tests that closed connection returns error
func TestConnClosedExecContext(t *testing.T) {
	conn := &Conn{
		closed: true,
	}

	_, err := conn.ExecContext(
		context.Background(),
		"SELECT 1",
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for closed connection, got nil",
		)
	}
}

// TestConnClosedQueryContext tests that closed connection returns error
func TestConnClosedQueryContext(t *testing.T) {
	conn := &Conn{
		closed: true,
	}

	_, err := conn.QueryContext(
		context.Background(),
		"SELECT 1",
		nil,
	)
	if err == nil {
		t.Error(
			"expected error for closed connection, got nil",
		)
	}
}

// TestResultLastInsertId tests that LastInsertId returns 0 (not supported)
func TestResultLastInsertId(t *testing.T) {
	r := &result{rowsAffected: 5}

	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if id != 0 {
		t.Errorf(
			"expected LastInsertId to be 0, got %d",
			id,
		)
	}
}

// TestResultRowsAffected tests that RowsAffected returns the correct value
func TestResultRowsAffected(t *testing.T) {
	r := &result{rowsAffected: 42}

	affected, err := r.RowsAffected()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if affected != 42 {
		t.Errorf(
			"expected RowsAffected to be 42, got %d",
			affected,
		)
	}
}
