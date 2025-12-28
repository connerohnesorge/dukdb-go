package engine

import (
	"context"
	"database/sql/driver"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/executor"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// EngineConn represents a connection to the engine.
// It implements the BackendConn interface.
type EngineConn struct {
	mu     sync.Mutex
	engine *Engine
	txn    *Transaction
	closed bool
}

// Execute executes a query that doesn't return rows.
func (c *EngineConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, dukdb.ErrConnectionClosed
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return 0, err
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return 0, err
	}

	// Plan the statement
	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return 0, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected, nil
}

// Query executes a query that returns rows.
func (c *EngineConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, nil, err
	}

	// Plan the statement
	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return nil, nil, err
	}

	return result.Rows, result.Columns, nil
}

// Prepare prepares a statement for execution.
func (c *EngineConn) Prepare(
	ctx context.Context,
	query string,
) (dukdb.BackendStmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, dukdb.ErrConnectionClosed
	}

	// Parse the query to validate it
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	// Count parameters
	numParams := parser.CountParameters(stmt)

	return &EngineStmt{
		conn:      c,
		query:     query,
		stmt:      stmt,
		numParams: numParams,
	}, nil
}

// Close closes the connection.
func (c *EngineConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Rollback any active transaction
	if c.txn != nil && c.txn.IsActive() {
		_ = c.engine.txnMgr.Rollback(c.txn)
	}

	return nil
}

// Ping verifies that the connection is still alive.
func (c *EngineConn) Ping(
	ctx context.Context,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return dukdb.ErrConnectionClosed
	}

	return nil
}

// EngineStmt represents a prepared statement.
type EngineStmt struct {
	mu        sync.Mutex
	conn      *EngineConn
	query     string
	stmt      parser.Statement
	numParams int
	closed    bool
}

// Execute executes the prepared statement.
func (s *EngineStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Execute(ctx, s.query, args)
}

// Query executes the prepared statement and returns rows.
func (s *EngineStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Query(ctx, s.query, args)
}

// Close closes the statement.
func (s *EngineStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *EngineStmt) NumInput() int {
	return s.numParams
}

// Verify interface implementations
var (
	_ dukdb.BackendConn = (*EngineConn)(nil)
	_ dukdb.BackendStmt = (*EngineStmt)(nil)
)
