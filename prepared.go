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
	conn            *Conn
	query           string
	numParams       int
	stmtType        StmtType
	extractedParams []placeholder // extracted parameters in order
	boundParams     []driver.NamedValue
	closed          bool
	mu              sync.Mutex
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

// extractOrderedParams extracts unique parameters from a query and returns them
// in canonical order. For positional parameters, they are sorted by index ($1, $2, $3).
// For named parameters, they are returned in first-occurrence order.
func extractOrderedParams(
	query string,
) []placeholder {
	positional := extractPositionalPlaceholders(
		query,
	)
	named := extractNamedPlaceholders(query)

	// Mixed mode - not supported for introspection, return empty
	if len(positional) > 0 && len(named) > 0 {
		return nil
	}

	// Positional parameters: create entries for each index 1..max
	if len(positional) > 0 {
		maxIndex := 0
		for _, p := range positional {
			idx, err := strconv.Atoi(p.name)
			if err == nil && idx > maxIndex {
				maxIndex = idx
			}
		}

		result := make([]placeholder, maxIndex)
		for i := range result {
			result[i] = placeholder{
				name:         strconv.Itoa(i + 1),
				isPositional: true,
			}
		}

		return result
	}

	// Named parameters: return unique names in first-occurrence order
	if len(named) > 0 {
		seen := make(map[string]bool)
		var result []placeholder
		for _, p := range named {
			if !seen[p.name] {
				seen[p.name] = true
				result = append(result, p)
			}
		}

		return result
	}

	return nil
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
	stmtType := detectStatementType(query)
	extractedParams := extractOrderedParams(query)

	// Initialize boundParams with the same capacity as numParams
	var boundParams []driver.NamedValue
	if numParams > 0 {
		boundParams = make(
			[]driver.NamedValue,
			numParams,
		)
	}

	return &PreparedStmt{
		conn:            c,
		query:           query,
		numParams:       numParams,
		stmtType:        stmtType,
		extractedParams: extractedParams,
		boundParams:     boundParams,
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

// StatementType returns the type of SQL statement (SELECT, INSERT, UPDATE, etc.).
// This value is determined at Prepare time using simple keyword detection.
//
// The detection is based on the first keyword in the SQL query after stripping
// leading comments and whitespace. For complex cases where full parsing is
// needed, use the parser package directly.
func (s *PreparedStmt) StatementType() StmtType {
	// No lock needed - stmtType is immutable after creation
	return s.stmtType
}

// ParamCount returns the number of placeholder parameters in the query.
// This is an alias for NumInput() for consistency with the introspection API.
func (s *PreparedStmt) ParamCount() int {
	return s.numParams
}

// ParamName returns the name of the parameter at the given index (0-based).
// For positional parameters ($1, $2), returns "1", "2", etc.
// For named parameters (@name), returns the parameter name without the @ prefix.
// Returns an error if the index is out of bounds.
func (s *PreparedStmt) ParamName(
	idx int,
) (string, error) {
	if idx < 0 || idx >= len(s.extractedParams) {
		return "", &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"parameter index %d out of range [0, %d)",
				idx,
				len(s.extractedParams),
			),
		}
	}

	return s.extractedParams[idx].name, nil
}

// Bind stores a parameter value for later execution with ExecBound() or QueryBound().
// The index is 0-based and must be in range [0, ParamCount()).
// For positional parameters ($1, $2), idx 0 corresponds to $1, idx 1 to $2, etc.
// For named parameters (@name), idx corresponds to the order of first occurrence.
//
// Bind is not thread-safe - do not call concurrently with other Bind calls
// or with ExecBound/QueryBound.
func (s *PreparedStmt) Bind(
	idx int,
	value any,
) error {
	if s.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "statement is closed",
		}
	}
	if idx < 0 || idx >= s.numParams {
		return &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"parameter index %d out of range [0, %d)",
				idx,
				s.numParams,
			),
		}
	}

	// Store the bound value
	if len(s.extractedParams) > idx {
		// Named parameter - use the name
		if !s.extractedParams[idx].isPositional {
			s.boundParams[idx] = driver.NamedValue{
				Name:  s.extractedParams[idx].name,
				Value: value,
			}
		} else {
			// Positional parameter - use ordinal (1-based)
			s.boundParams[idx] = driver.NamedValue{
				Ordinal: idx + 1,
				Value:   value,
			}
		}
	} else {
		// Default to positional (1-based ordinal)
		s.boundParams[idx] = driver.NamedValue{
			Ordinal: idx + 1,
			Value:   value,
		}
	}

	return nil
}

// ClearBindings resets all bound parameter values.
// After calling ClearBindings, you must call Bind for each parameter
// before calling ExecBound or QueryBound.
func (s *PreparedStmt) ClearBindings() {
	for i := range s.boundParams {
		s.boundParams[i] = driver.NamedValue{}
	}
}

// allParamsBound checks if all parameters have been bound.
func (s *PreparedStmt) allParamsBound() bool {
	for i := range s.numParams {
		// Check if this slot has been set (Value is non-nil or Ordinal > 0 or Name non-empty)
		if s.boundParams[i].Ordinal == 0 &&
			s.boundParams[i].Name == "" &&
			s.boundParams[i].Value == nil {
			return false
		}
	}

	return true
}

// ExecBound executes the prepared statement using previously bound parameters.
// All parameters must be bound using Bind() before calling this method.
// Returns an error if any parameter has not been bound.
func (s *PreparedStmt) ExecBound(
	ctx context.Context,
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
	if !s.allParamsBound() {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "not all parameters have been bound",
		}
	}

	// Check context cancellation before execution
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Make a copy of boundParams to avoid data races
	args := make(
		[]driver.NamedValue,
		len(s.boundParams),
	)
	copy(args, s.boundParams)

	return s.conn.ExecContext(ctx, s.query, args)
}

// QueryBound executes the prepared statement using previously bound parameters
// and returns the query results as driver.Rows.
// All parameters must be bound using Bind() before calling this method.
// Returns an error if any parameter has not been bound.
func (s *PreparedStmt) QueryBound(
	ctx context.Context,
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
	if !s.allParamsBound() {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "not all parameters have been bound",
		}
	}

	// Check context cancellation before execution
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Make a copy of boundParams to avoid data races
	args := make(
		[]driver.NamedValue,
		len(s.boundParams),
	)
	copy(args, s.boundParams)

	return s.conn.QueryContext(ctx, s.query, args)
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
