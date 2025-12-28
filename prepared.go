package dukdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync"
)

// PreparedStmt implements client-side prepared statement functionality.
// Since DuckDB CLI doesn't support server-side prepared statements,
// this is a client-side implementation that stores the query and
// validates parameter counts before delegating to the connection's
// ExecContext/QueryContext methods which handle parameter binding.
//
// PreparedStmt is safe for concurrent use by multiple goroutines.
type PreparedStmt struct {
	conn      *Conn
	query     string
	numParams int
	closed    bool
	mu        sync.Mutex
}

// countPlaceholders counts the number of placeholder parameters in a query.
// It returns the expected number of arguments for NumInput():
// - For positional placeholders ($1, $2, etc.): returns the maximum index found
// - For named placeholders (@name): returns the count of unique names
// - For mixed placeholders: returns the sum (execution will fail later)
// - Placeholders inside string literals are ignored
func countPlaceholders(query string) int {
	positional := extractPositionalPlaceholders(
		query,
	)
	named := extractNamedPlaceholders(query)

	// If both positional and named found, return sum (mixed mode - will fail at execution)
	if len(positional) > 0 && len(named) > 0 {
		// Count max positional index
		maxIndex := 0
		for _, p := range positional {
			idx, err := strconv.Atoi(p.name)
			if err == nil && idx > maxIndex {
				maxIndex = idx
			}
		}
		// Count unique named params
		uniqueNamed := make(map[string]bool)
		for _, p := range named {
			uniqueNamed[p.name] = true
		}
		return maxIndex + len(uniqueNamed)
	}

	// Positional only: return max index (e.g., $1, $3 -> 3)
	if len(positional) > 0 {
		maxIndex := 0
		for _, p := range positional {
			idx, err := strconv.Atoi(p.name)
			if err == nil && idx > maxIndex {
				maxIndex = idx
			}
		}
		return maxIndex
	}

	// Named only: return count of unique names
	if len(named) > 0 {
		uniqueNames := make(map[string]bool)
		for _, p := range named {
			uniqueNames[p.name] = true
		}
		return len(uniqueNames)
	}

	return 0
}

// Prepare creates a new prepared statement bound to this connection.
// The query string may contain $1, $2, etc. for positional parameters
// or @name for named parameters, but not both styles mixed.
//
// This is a client-side prepared statement - the query is stored locally
// and parameters are bound at execution time.
func (c *Conn) PreparePreparedStmt(
	query string,
) (*PreparedStmt, error) {
	if c.closed {
		return nil, &Error{
			Type: ErrorTypeConnection,
			Msg:  "connection is closed",
		}
	}

	numParams := countPlaceholders(query)

	return &PreparedStmt{
		conn:      c,
		query:     query,
		numParams: numParams,
	}, nil
}

// Close closes the prepared statement.
// Close is idempotent - calling it multiple times is safe and will not return an error.
// After Close is called, any attempt to use the statement will return an error.
func (s *PreparedStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true // Idempotent - no error on multiple closes
	return nil
}

// NumInput returns the number of placeholder parameters in the query.
// This value is determined at Prepare time and does not change.
//
// For positional placeholders ($1, $2, etc.), this returns the maximum index.
// For example, "SELECT $1, $3" returns 3 (not 2).
//
// For named placeholders (@name), this returns the count of unique names.
// For example, "SELECT @foo, @foo, @bar" returns 2.
func (s *PreparedStmt) NumInput() int {
	// No lock needed - numParams is immutable after creation
	return s.numParams
}

// ExecContext executes the prepared statement with the given arguments.
// It returns a driver.Result summarizing the effect of the statement.
//
// The args slice must contain exactly NumInput() arguments, with either:
// - Positional args: Ordinal field set to 1, 2, 3, etc.
// - Named args: Name field set to the parameter name (without @)
func (s *PreparedStmt) ExecContext(
	ctx context.Context,
	args []driver.NamedValue,
) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, &Error{
			Type: ErrorTypeClosed,
			Msg:  "statement is closed",
		}
	}
	if s.conn.closed {
		return nil, &Error{
			Type: ErrorTypeConnection,
			Msg:  "connection is closed",
		}
	}
	if len(args) != s.numParams {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"expected %d parameter(s), got %d",
				s.numParams,
				len(args),
			),
		}
	}

	// Check context cancellation before execution
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Delegate to connection's ExecContext which handles parameter binding
	return s.conn.ExecContext(ctx, s.query, args)
}

// QueryContext executes the prepared statement with the given arguments
// and returns the query results as driver.Rows.
//
// The args slice must contain exactly NumInput() arguments, with either:
// - Positional args: Ordinal field set to 1, 2, 3, etc.
// - Named args: Name field set to the parameter name (without @)
func (s *PreparedStmt) QueryContext(
	ctx context.Context,
	args []driver.NamedValue,
) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, &Error{
			Type: ErrorTypeClosed,
			Msg:  "statement is closed",
		}
	}
	if s.conn.closed {
		return nil, &Error{
			Type: ErrorTypeConnection,
			Msg:  "connection is closed",
		}
	}
	if len(args) != s.numParams {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"expected %d parameter(s), got %d",
				s.numParams,
				len(args),
			),
		}
	}

	// Check context cancellation before execution
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Delegate to connection's QueryContext which handles parameter binding
	return s.conn.QueryContext(ctx, s.query, args)
}

// Exec executes the prepared statement with the given arguments.
// Deprecated: Use ExecContext instead.
func (s *PreparedStmt) Exec(
	args []driver.Value,
) (driver.Result, error) {
	return s.ExecContext(
		context.Background(),
		valuesToNamedValues(args),
	)
}

// Query executes the prepared statement with the given arguments.
// Deprecated: Use QueryContext instead.
func (s *PreparedStmt) Query(
	args []driver.Value,
) (driver.Rows, error) {
	return s.QueryContext(
		context.Background(),
		valuesToNamedValues(args),
	)
}

// Ensure PreparedStmt implements the driver.Stmt interface.
var (
	_ driver.Stmt = (*PreparedStmt)(
		nil,
	)
	_ driver.StmtExecContext = (*PreparedStmt)(
		nil,
	)
	_ driver.StmtQueryContext = (*PreparedStmt)(
		nil,
	)
)
