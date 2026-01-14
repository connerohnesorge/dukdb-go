package server

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
)

// sessionIDCounter is used to generate unique session IDs.
var sessionIDCounter atomic.Uint64

// init initializes the session ID counter to start at 1.
func init() {
	sessionIDCounter.Store(1)
}

// generateSessionID generates a unique session ID.
func generateSessionID() uint64 {
	return sessionIDCounter.Add(1) - 1
}

// Session represents a client session connected to the PostgreSQL server.
// Each session maintains its own database connection and transaction state.
type Session struct {
	mu sync.RWMutex

	// id is the unique session identifier.
	id uint64

	// server is the parent server instance.
	server *Server

	// username is the authenticated username for this session.
	username string

	// database is the database name for this session.
	database string

	// remoteAddr is the remote address of the client.
	remoteAddr string

	// ctx is the session context.
	ctx context.Context

	// cancelFunc cancels the session context.
	cancelFunc context.CancelFunc

	// closed indicates whether the session has been closed.
	closed bool

	// attributes holds custom session attributes.
	attributes map[string]any

	// inTransaction indicates if an explicit transaction is active.
	inTransaction bool

	// transactionReadOnly indicates if the current transaction is read-only.
	transactionReadOnly bool

	// transactionAborted indicates if the current transaction has been aborted due to an error.
	transactionAborted bool

	// isolationLevel is the current transaction isolation level.
	isolationLevel string

	// variables holds session-scoped variables (SET variable = value).
	variables map[string]string

	// localVariables holds transaction-local variables (SET LOCAL variable = value).
	// These are cleared when the transaction ends.
	localVariables map[string]string

	// preparedStatements holds SQL-level prepared statements (from PREPARE statement).
	// This is separate from the wire protocol prepared statements handled by psql-wire.
	preparedStatements *PreparedStmtCache

	// portals holds bound prepared statements with parameters.
	portals *PortalCache
}

// NewSession creates a new session for the given server.
func NewSession(server *Server, username, database, remoteAddr string) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		id:                 generateSessionID(),
		server:             server,
		username:           username,
		database:           database,
		remoteAddr:         remoteAddr,
		ctx:                ctx,
		cancelFunc:         cancel,
		attributes:         make(map[string]any),
		variables:          make(map[string]string),
		localVariables:     make(map[string]string),
		isolationLevel:     "read committed", // PostgreSQL default
		preparedStatements: NewPreparedStmtCache(),
		portals:            NewPortalCache(),
	}
}

// ID returns the unique session identifier.
func (s *Session) ID() uint64 {
	return s.id
}

// Username returns the authenticated username.
func (s *Session) Username() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.username
}

// Database returns the database name.
func (s *Session) Database() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.database
}

// RemoteAddr returns the remote address of the client.
func (s *Session) RemoteAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.remoteAddr
}

// Context returns the session context.
func (s *Session) Context() context.Context {
	return s.ctx
}

// IsClosed returns whether the session has been closed.
func (s *Session) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// Close closes the session and releases resources.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.cancelFunc()

	// Close prepared statements cache
	if s.preparedStatements != nil {
		_ = s.preparedStatements.Close()
	}

	// Close portals cache
	if s.portals != nil {
		_ = s.portals.Close()
	}

	return nil
}

// GetAttribute retrieves a custom session attribute.
func (s *Session) GetAttribute(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.attributes[key]
	return val, ok
}

// SetAttribute sets a custom session attribute.
func (s *Session) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attributes[key] = value
}

// DeleteAttribute removes a custom session attribute.
func (s *Session) DeleteAttribute(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.attributes, key)
}

// InTransaction returns whether an explicit transaction is active.
func (s *Session) InTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inTransaction
}

// SetInTransaction sets the transaction state.
func (s *Session) SetInTransaction(inTxn bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inTransaction = inTxn
}

// IsTransactionReadOnly returns whether the current transaction is read-only.
func (s *Session) IsTransactionReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transactionReadOnly
}

// SetTransactionReadOnly sets the read-only state of the current transaction.
func (s *Session) SetTransactionReadOnly(readOnly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactionReadOnly = readOnly
}

// IsTransactionAborted returns whether the current transaction has been aborted.
func (s *Session) IsTransactionAborted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transactionAborted
}

// SetTransactionAborted sets the aborted state of the current transaction.
func (s *Session) SetTransactionAborted(aborted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactionAborted = aborted
}

// GetIsolationLevel returns the current transaction isolation level.
func (s *Session) GetIsolationLevel() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isolationLevel
}

// SetIsolationLevel sets the transaction isolation level.
func (s *Session) SetIsolationLevel(level string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isolationLevel = level
}

// GetVariable returns a session variable value.
// It checks local variables first (for transaction-scoped settings), then session variables.
func (s *Session) GetVariable(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check local variables first (transaction-scoped)
	if val, ok := s.localVariables[name]; ok {
		return val, true
	}

	// Check session variables
	if val, ok := s.variables[name]; ok {
		return val, true
	}

	return "", false
}

// SetVariable sets a session variable.
func (s *Session) SetVariable(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.variables[name] = value
}

// SetLocalVariable sets a transaction-local variable.
// These variables are cleared when the transaction ends.
func (s *Session) SetLocalVariable(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localVariables[name] = value
}

// DeleteVariable deletes a session variable.
func (s *Session) DeleteVariable(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.variables, name)
	delete(s.localVariables, name)
}

// ResetVariables resets all session variables to their defaults.
func (s *Session) ResetVariables() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.variables = make(map[string]string)
	s.localVariables = make(map[string]string)
	s.isolationLevel = "read committed"
}

// ClearLocalVariables clears all transaction-local variables.
// Called when a transaction ends (COMMIT or ROLLBACK).
func (s *Session) ClearLocalVariables() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localVariables = make(map[string]string)
}

// GetAllVariables returns a copy of all session variables (both session and local).
func (s *Session) GetAllVariables() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string, len(s.variables)+len(s.localVariables))

	// Copy session variables
	for k, v := range s.variables {
		result[k] = v
	}

	// Override with local variables (transaction-scoped take precedence)
	for k, v := range s.localVariables {
		result[k] = v
	}

	return result
}

// Execute executes a query that doesn't return rows using the underlying engine.
func (s *Session) Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrSessionClosed
	}
	server := s.server
	s.mu.RUnlock()

	if server == nil || server.conn == nil {
		return 0, ErrNoConnection
	}

	return server.conn.Execute(ctx, query, args)
}

// Query executes a query that returns rows using the underlying engine.
func (s *Session) Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, nil, ErrSessionClosed
	}
	server := s.server
	s.mu.RUnlock()

	if server == nil || server.conn == nil {
		return nil, nil, ErrNoConnection
	}

	return server.conn.Query(ctx, query, args)
}

// PreparedStatements returns the session's prepared statement cache.
func (s *Session) PreparedStatements() *PreparedStmtCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.preparedStatements
}

// Portals returns the session's portal cache.
func (s *Session) Portals() *PortalCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.portals
}

// GetTransactionState returns the current transaction state for ReadyForQuery messages.
// Returns 'I' (idle), 'T' (in transaction), or 'E' (failed transaction).
func (s *Session) GetTransactionState() TransactionState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.inTransaction {
		return TxStateIdle
	}

	if s.transactionAborted {
		return TxStateFailed
	}

	return TxStateInTx
}

// sessionContextKey is the context key for storing session data.
type sessionContextKey struct{}

// SessionFromContext retrieves the session from the context.
func SessionFromContext(ctx context.Context) (*Session, bool) {
	session, ok := ctx.Value(sessionContextKey{}).(*Session)
	return session, ok
}

// ContextWithSession adds the session to the context.
func ContextWithSession(ctx context.Context, session *Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, session)
}
