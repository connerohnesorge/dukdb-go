package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"reflect"
	"sync"

	"github.com/coder/quartz"
)

// Conn implements the database/sql/driver connection interfaces.
// It wraps a BackendConn to provide database/sql compatibility.
type Conn struct {
	connector   *Connector
	backendConn BackendConn
	closed      bool
	tx          bool

	// scalarFuncs holds registered scalar UDFs for this connection.
	scalarFuncs *scalarFuncRegistry

	// aggregateFuncs holds registered aggregate UDFs for this connection.
	aggregateFuncs *aggregateFuncRegistry

	// tableUDFs holds registered table UDFs for this connection.
	tableUDFs *tableFunctionRegistry

	// profiling holds the profiling context for this connection.
	profiling *ProfilingContext

	// replacementScan holds the replacement scan context for this connection.
	replacementScan *ReplacementScanContext
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

// getVirtualTableRegistry returns the virtual table registry from the backend connection if supported.
// Returns nil if the backend does not support virtual table registration.
func (c *Conn) getVirtualTableRegistry() VirtualTableRegistry {
	if catalogConn, ok := c.backendConn.(BackendConnCatalog); ok {
		if registry := catalogConn.GetCatalog(); registry != nil {
			if typedRegistry, ok := registry.(VirtualTableRegistry); ok {
				return typedRegistry
			}
		}
	}
	return nil
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
	boundParams map[int]any // Bound parameters for Bind/ExecBound/QueryBound
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
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ParamName(n), nil
	}
	return "", nil
}

// ParamType returns the type of the parameter at the given index (1-based).
func (s *Stmt) ParamType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ParamType(n), nil
	}
	return TYPE_ANY, nil
}

// ColumnCount returns the number of columns in the result set.
func (s *Stmt) ColumnCount() (int, error) {
	if s.closed {
		return 0, errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ColumnCount(), nil
	}
	return 0, nil
}

// ColumnName returns the name of the column at the given index (0-based).
func (s *Stmt) ColumnName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ColumnName(n), nil
	}
	return "", nil
}

// ColumnType returns the type of the column at the given index (0-based).
func (s *Stmt) ColumnType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ColumnType(n), nil
	}
	return TYPE_INVALID, nil
}

// ColumnTypeInfo returns extended type info for the column at the given index (0-based).
func (s *Stmt) ColumnTypeInfo(n int) (TypeInfo, error) {
	if s.closed {
		return nil, errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.ColumnTypeInfo(n), nil
	}
	return nil, nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		return intro.StatementType(), nil
	}
	return STATEMENT_TYPE_INVALID, nil
}

// Properties returns metadata about the statement's behavior.
func (s *Stmt) Properties() (StmtProperties, error) {
	if s.closed {
		return StmtProperties{}, errClosedStmt
	}
	// First, try BackendStmtProperties interface
	if props, ok := s.backendStmt.(BackendStmtProperties); ok {
		return props.Properties(), nil
	}
	// Fallback: compute from StatementType if introspector available
	if intro, ok := s.backendStmt.(BackendStmtIntrospector); ok {
		stmtType := intro.StatementType()
		return StmtProperties{
			Type:        stmtType,
			ReturnType:  stmtType.ReturnType(),
			IsReadOnly:  !stmtType.ModifiesData(),
			IsStreaming: stmtType.IsQuery(),
			ColumnCount: intro.ColumnCount(),
			ParamCount:  intro.NumInput(),
		}, nil
	}
	return StmtProperties{}, &Error{
		Type: ErrorTypeNotImplemented,
		Msg:  "statement properties not supported",
	}
}

// IsReadOnly returns true if the statement doesn't modify data.
func (s *Stmt) IsReadOnly() (bool, error) {
	props, err := s.Properties()
	if err != nil {
		return false, err
	}
	return props.IsReadOnly, nil
}

// IsQuery returns true if the statement returns a result set.
func (s *Stmt) IsQuery() (bool, error) {
	props, err := s.Properties()
	if err != nil {
		return false, err
	}
	return props.ReturnType == RETURN_QUERY_RESULT, nil
}

// Bind binds a value to the parameter at the given index (1-based).
// Bound parameters are used by ExecBound and QueryBound.
func (s *Stmt) Bind(index int, value any) error {
	if s.closed {
		return errClosedStmt
	}
	if index < 1 {
		return &Error{
			Type: ErrorTypeInvalidInput,
			Msg:  "parameter index must be >= 1",
		}
	}
	numInput := s.NumInput()
	if numInput > 0 && index > numInput {
		return &Error{
			Type: ErrorTypeInvalidInput,
			Msg:  "parameter index out of range",
		}
	}
	if s.boundParams == nil {
		s.boundParams = make(map[int]any)
	}
	s.boundParams[index] = value
	return nil
}

// ClearBound clears all bound parameters.
func (s *Stmt) ClearBound() {
	s.boundParams = nil
}

// ExecBound executes the statement with bound parameters.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) ExecBound() (driver.Result, error) {
	return s.ExecBoundContext(context.Background())
}

// ExecBoundContext executes the statement with bound parameters and context.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) ExecBoundContext(ctx context.Context) (driver.Result, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	args := s.boundParamsToNamedValues()
	return s.ExecContext(ctx, args)
}

// QueryBound executes the statement with bound parameters and returns rows.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) QueryBound() (driver.Rows, error) {
	return s.QueryBoundContext(context.Background())
}

// QueryBoundContext executes the statement with bound parameters and context.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) QueryBoundContext(ctx context.Context) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	args := s.boundParamsToNamedValues()
	return s.QueryContext(ctx, args)
}

// ExecBoundContextClock executes the statement with bound parameters using a clock
// for deterministic deadline checking. This is primarily for testing.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) ExecBoundContextClock(ctx context.Context, clock quartz.Clock) (driver.Result, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	// Check deadline using the injected clock for deterministic testing
	if deadline, ok := ctx.Deadline(); ok {
		if clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	args := s.boundParamsToNamedValues()
	return s.ExecContext(ctx, args)
}

// QueryBoundContextClock executes the statement with bound parameters using a clock
// for deterministic deadline checking. This is primarily for testing.
// Parameters must be bound using Bind before calling this method.
func (s *Stmt) QueryBoundContextClock(ctx context.Context, clock quartz.Clock) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedStmt
	}

	// Check deadline using the injected clock for deterministic testing
	if deadline, ok := ctx.Deadline(); ok {
		if clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	args := s.boundParamsToNamedValues()
	return s.QueryContext(ctx, args)
}

// boundParamsToNamedValues converts bound parameters to driver.NamedValue slice.
func (s *Stmt) boundParamsToNamedValues() []driver.NamedValue {
	if len(s.boundParams) == 0 {
		return nil
	}

	// Find the maximum index to size the slice
	maxIndex := 0
	for idx := range s.boundParams {
		if idx > maxIndex {
			maxIndex = idx
		}
	}

	args := make([]driver.NamedValue, maxIndex)
	for idx, val := range s.boundParams {
		args[idx-1] = driver.NamedValue{
			Ordinal: idx,
			Value:   val,
		}
	}
	return args
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
