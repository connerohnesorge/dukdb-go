package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"reflect"
	"sync"
)

// Conn implements the database/sql/driver connection interfaces.
// It wraps a BackendConn to provide database/sql compatibility.
type Conn struct {
	connector   *Connector
	backendConn BackendConn
	closed      bool
	tx          bool
}

// Prepare returns a prepared statement, bound to this connection.
// Implements the driver.Conn interface.
func (c *Conn) Prepare(
	query string,
) (driver.Stmt, error) {
	return c.PrepareContext(
		context.Background(),
		query,
	)
}

// PrepareContext returns a prepared statement, bound to this connection.
// Implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	if c.closed {
		return nil, errClosedCon
	}

	backendStmt, err := c.backendConn.Prepare(
		ctx,
		query,
	)
	if err != nil {
		return nil, err
	}

	return &Stmt{
		conn:        c,
		backendStmt: backendStmt,
	}, nil
}

// Close closes the connection.
// Implements the driver.Conn interface.
func (c *Conn) Close() error {
	if c.closed {
		return errClosedCon
	}
	c.closed = true
	return nil
}

// Begin starts and returns a new transaction.
// Deprecated: Use BeginTx instead.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
}

// BeginTx starts and returns a new transaction.
// Implements the driver.ConnBeginTx interface.
func (c *Conn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions,
) (driver.Tx, error) {
	if c.closed {
		return nil, errClosedCon
	}
	if c.tx {
		return nil, errors.Join(
			errBeginTx,
			errMultipleTx,
		)
	}

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	default:
		return nil, errors.Join(
			errBeginTx,
			errors.New(
				"isolation level not supported",
			),
		)
	}

	if opts.ReadOnly {
		return nil, errors.Join(
			errBeginTx,
			errors.New(
				"read-only transactions not supported",
			),
		)
	}

	if _, err := c.ExecContext(ctx, "BEGIN TRANSACTION", nil); err != nil {
		return nil, err
	}
	c.tx = true

	return &tx{conn: c}, nil
}

// ExecContext executes a query that doesn't return rows.
// Implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	if c.closed {
		return nil, errClosedCon
	}

	// Pre-execution context check
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	rowsAffected, err := c.backendConn.Execute(
		ctx,
		query,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &result{
		rowsAffected: rowsAffected,
	}, nil
}

// QueryContext executes a query that returns rows.
// Implements the driver.QueryerContext interface.
func (c *Conn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	if c.closed {
		return nil, errClosedCon
	}

	// Pre-execution context check
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	data, columns, err := c.backendConn.Query(
		ctx,
		query,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &rows{
		columns: columns,
		data:    data,
		pos:     0,
	}, nil
}

// CheckNamedValue implements the driver.NamedValueChecker interface.
func (c *Conn) CheckNamedValue(
	nv *driver.NamedValue,
) error {
	switch nv.Value.(type) {
	case *big.Int, []any, []bool, []int8, []int16, []int32, []int64, []int,
		[]uint8, []uint16, []uint32, []uint64, []uint, []float32, []float64,
		[]string, map[string]any:
		return nil
	}

	vo := reflect.ValueOf(nv.Value)
	switch vo.Kind() {
	case reflect.Interface,
		reflect.Slice,
		reflect.Map,
		reflect.Array:
		return nil
	}
	return driver.ErrSkip
}

// Ping verifies the connection is still alive.
// Implements the driver.Pinger interface.
func (c *Conn) Ping(ctx context.Context) error {
	if c.closed {
		return errClosedCon
	}
	return c.backendConn.Ping(ctx)
}

// ResetSession resets the session state.
// This is called by the connection pool before returning the connection to the pool.
// If a transaction is active, it will be rolled back.
// Implements the driver.SessionResetter interface.
func (c *Conn) ResetSession(
	ctx context.Context,
) error {
	if c.closed {
		return errClosedCon
	}

	// Rollback any active transaction
	if c.tx {
		_, _ = c.backendConn.Execute(
			context.Background(),
			"ROLLBACK",
			nil,
		)
		c.tx = false
	}

	return nil
}

// IsValid reports whether the connection is still valid.
// This is called by the connection pool to check if the connection should be discarded.
// Implements the driver.Validator interface.
func (c *Conn) IsValid() bool {
	return !c.closed && c.backendConn != nil
}

// tx implements the driver.Tx interface.
// It uses a mutex for thread-safe state management.
type tx struct {
	conn *Conn
	done bool
	mu   sync.Mutex
}

// Commit commits the transaction.
func (t *tx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return &Error{
			Type: ErrorTypeTransaction,
			Msg:  "transaction already completed",
		}
	}
	if !t.conn.tx {
		return &Error{
			Type: ErrorTypeTransaction,
			Msg:  "not in transaction",
		}
	}

	t.done = true
	t.conn.tx = false
	_, err := t.conn.ExecContext(
		context.Background(),
		"COMMIT",
		nil,
	)
	return err
}

// Rollback aborts the transaction.
func (t *tx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return &Error{
			Type: ErrorTypeTransaction,
			Msg:  "transaction already completed",
		}
	}
	if !t.conn.tx {
		return &Error{
			Type: ErrorTypeTransaction,
			Msg:  "not in transaction",
		}
	}

	t.done = true
	t.conn.tx = false
	_, err := t.conn.ExecContext(
		context.Background(),
		"ROLLBACK",
		nil,
	)
	return err
}

// result implements the driver.Result interface.
type result struct {
	rowsAffected int64
}

// LastInsertId returns the database's auto-generated ID.
// DuckDB does not support LastInsertId, so this always returns 0.
func (r *result) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// rows implements the driver.Rows interface.
type rows struct {
	columns []string
	data    []map[string]any
	pos     int
}

// Columns returns the names of the columns.
func (r *rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into the provided slice.
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return errors.New("EOF")
	}

	row := r.data[r.pos]
	for i, col := range r.columns {
		dest[i] = row[col]
	}
	r.pos++
	return nil
}

// Stmt implements the driver.Stmt interface.
type Stmt struct {
	conn        *Conn
	backendStmt BackendStmt
	closed      bool
}

// Close closes the statement.
func (s *Stmt) Close() error {
	if s.closed {
		return errClosedStmt
	}
	s.closed = true
	return s.backendStmt.Close()
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return s.backendStmt.NumInput()
}

// Exec executes a query that doesn't return rows.
// Deprecated: Use ExecContext instead.
func (s *Stmt) Exec(
	args []driver.Value,
) (driver.Result, error) {
	return s.ExecContext(
		context.Background(),
		valuesToNamedValues(args),
	)
}

// ExecContext executes a query that doesn't return rows.
func (s *Stmt) ExecContext(
	ctx context.Context,
	args []driver.NamedValue,
) (driver.Result, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	// Pre-execution context check
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	rowsAffected, err := s.backendStmt.Execute(
		ctx,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &result{
		rowsAffected: rowsAffected,
	}, nil
}

// Query executes a query that returns rows.
// Deprecated: Use QueryContext instead.
func (s *Stmt) Query(
	args []driver.Value,
) (driver.Rows, error) {
	return s.QueryContext(
		context.Background(),
		valuesToNamedValues(args),
	)
}

// QueryContext executes a query that returns rows.
func (s *Stmt) QueryContext(
	ctx context.Context,
	args []driver.NamedValue,
) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	// Pre-execution context check
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	data, columns, err := s.backendStmt.Query(
		ctx,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &rows{
		columns: columns,
		data:    data,
		pos:     0,
	}, nil
}

// ParamName returns the name of the parameter at the given index (1-based).
func (s *Stmt) ParamName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	// This is a stub - actual implementation depends on backend
	return "", nil
}

// ColumnCount returns the number of columns in the result set.
func (s *Stmt) ColumnCount() (int, error) {
	if s.closed {
		return 0, errClosedStmt
	}
	// This is a stub - actual implementation depends on backend
	return 0, nil
}

// ColumnName returns the name of the column at the given index (0-based).
func (s *Stmt) ColumnName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	// This is a stub - actual implementation depends on backend
	return "", nil
}

// ColumnType returns the type of the column at the given index (0-based).
func (s *Stmt) ColumnType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	// This is a stub - actual implementation depends on backend
	return TYPE_INVALID, nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	// This is a stub - actual implementation depends on backend
	return STATEMENT_TYPE_INVALID, nil
}

// valuesToNamedValues converts driver.Value slice to driver.NamedValue slice.
func valuesToNamedValues(
	args []driver.Value,
) []driver.NamedValue {
	namedArgs := make(
		[]driver.NamedValue,
		len(args),
	)
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return namedArgs
}

// Ensure interfaces are implemented.
var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.NamedValueChecker  = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.Validator          = (*Conn)(nil)
	_ driver.Tx                 = (*tx)(nil)
	_ driver.Result             = (*result)(nil)
	_ driver.Rows               = (*rows)(nil)
	_ driver.Stmt               = (*Stmt)(nil)
)
