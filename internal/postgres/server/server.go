package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	wire "github.com/jeroenrinzema/psql-wire"
)

// Common errors.
var (
	// ErrServerClosed is returned when the server has been closed.
	ErrServerClosed = errors.New("server closed")

	// ErrSessionClosed is returned when the session has been closed.
	ErrSessionClosed = errors.New("session closed")

	// ErrNoConnection is returned when no database connection is available.
	ErrNoConnection = errors.New("no database connection available")

	// ErrAlreadyStarted is returned when trying to start an already running server.
	ErrAlreadyStarted = errors.New("server already started")
)

// Server represents a PostgreSQL-compatible wire protocol server.
// It accepts connections from PostgreSQL clients and routes queries
// to the dukdb engine.
type Server struct {
	mu sync.RWMutex

	// config holds the server configuration.
	config *Config

	// wireServer is the underlying psql-wire server.
	wireServer *wire.Server

	// listener is the network listener for incoming connections.
	listener net.Listener

	// conn is the dukdb backend connection.
	conn dukdb.BackendConn

	// engine is the dukdb backend engine.
	engine dukdb.Backend

	// handler handles query execution.
	handler *Handler

	// sessions tracks active sessions.
	sessions map[uint64]*Session

	// sessionsMu protects the sessions map.
	sessionsMu sync.RWMutex

	// started indicates if the server has been started.
	started atomic.Bool

	// closed indicates if the server has been closed.
	closed atomic.Bool

	// logger is the server logger.
	logger *slog.Logger

	// connCount tracks the number of active connections.
	connCount atomic.Int64

	// catalogHandler handles information_schema and pg_catalog queries.
	catalogHandler *CatalogHandler

	// notificationHub manages LISTEN/NOTIFY channel subscriptions.
	notificationHub *NotificationHub

	// connectionManager manages connection pooling, limits, and statistics.
	connectionManager *ConnectionManager

	// cancellationManager manages query cancellation for the PostgreSQL protocol.
	cancellationManager *CancellationManager

	// observability manages metrics collection, tracing, and Prometheus export.
	observability *ObservabilityManager
}

// NewServer creates a new PostgreSQL wire protocol server with the given configuration.
func NewServer(config *Config) (*Server, error) {
	if config == nil {
		config = NewConfig()
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	s := &Server{
		config:          config,
		sessions:        make(map[uint64]*Session),
		logger:          slog.Default(),
		notificationHub: NewNotificationHub(),
	}

	// Initialize the cancellation manager
	s.cancellationManager = NewCancellationManager(s)

	// Initialize observability manager for metrics and tracing
	s.observability = NewObservabilityManager(nil, s)

	// Initialize connection manager if pooling is enabled
	if config.EnableConnectionPooling {
		s.connectionManager = NewConnectionManager(config, config.GetConnectionTimeouts())
		if config.MaxQueueSize > 0 {
			s.connectionManager.SetMaxQueueSize(config.MaxQueueSize)
		}
	}

	// Create the query handler
	s.handler = NewHandler(s)

	return s, nil
}

// SetLogger sets the server logger.
func (s *Server) SetLogger(logger *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger = logger
}

// SetEngine sets the dukdb backend engine.
// This must be called before Start().
func (s *Server) SetEngine(engine dukdb.Backend) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engine = engine
}

// Start starts the PostgreSQL wire protocol server.
// It opens a database connection and begins accepting client connections.
func (s *Server) Start() error {
	if s.started.Load() {
		return ErrAlreadyStarted
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get or create the engine
	if s.engine == nil {
		s.engine = dukdb.GetBackend()
	}
	if s.engine == nil {
		return errors.New("no backend engine available")
	}

	// Open a connection to the database
	conn, err := s.engine.Open(":memory:", nil)
	if err != nil {
		return err
	}
	s.conn = conn

	// Initialize the catalog handler for information_schema and pg_catalog queries
	dbName := s.config.Database
	if dbName == "" {
		dbName = "dukdb"
	}
	s.catalogHandler = initCatalogHandler(conn, dbName)

	// Set up monitoring providers for pg_stat_activity, pg_stat_statements, pg_locks
	if s.catalogHandler != nil && s.observability != nil {
		activityProvider := NewServerActivityProvider(s)
		statementsProvider := NewServerStatementStatsProvider(s.observability.MetricsCollector())
		lockProvider := NewServerLockProvider(s)
		s.catalogHandler.SetMonitoringProviders(activityProvider, statementsProvider, lockProvider)
	}

	// Build psql-wire server options
	var options []wire.OptionFn

	// Set the logger
	if s.logger != nil {
		options = append(options, wire.Logger(s.logger))
	}

	// Set server version
	if s.config.ServerVersion != "" {
		options = append(options, wire.Version(s.config.ServerVersion))
	}

	// Set shutdown timeout
	if s.config.ShutdownTimeout > 0 {
		options = append(options, wire.WithShutdownTimeout(s.config.ShutdownTimeout))
	}

	// Configure authentication if required
	if s.config.RequireAuth {
		authStrategy := wire.ClearTextPassword(s.validateCredentials)
		options = append(options, wire.SessionAuthStrategy(authStrategy))
	}

	// Configure TLS if provided
	if s.config.TLSConfig != nil {
		options = append(options, wire.TLSConfig(s.config.TLSConfig))
	}

	// Set global parameters
	params := wire.Parameters{
		wire.ParamServerVersion:  s.config.ServerVersion,
		wire.ParamServerEncoding: "UTF8",
		wire.ParamClientEncoding: "UTF8",
		wire.ParamDatabase:       s.config.Database,
	}
	options = append(options, wire.GlobalParameters(params))

	// Set session middleware for session tracking
	options = append(options, wire.SessionMiddleware(s.sessionMiddleware))

	// Set terminate connection handler
	options = append(options, wire.TerminateConn(s.onConnectionClose))

	// Set up BackendKeyData handler for query cancellation
	options = append(options, wire.BackendKeyData(s.generateBackendKeyData))

	// Set up CancelRequest handler for query cancellation
	options = append(options, wire.CancelRequest(s.handleCancelRequest))

	// Create the psql-wire server
	wireServer, err := wire.NewServer(s.handler.Parse, options...)
	if err != nil {
		_ = conn.Close()
		s.conn = nil
		return err
	}
	s.wireServer = wireServer

	s.started.Store(true)
	return nil
}

// ListenAndServe starts the server and listens for connections on the configured address.
// This method blocks until the server is shut down.
func (s *Server) ListenAndServe() error {
	// Start the server if not already started
	if !s.started.Load() {
		if err := s.Start(); err != nil {
			return err
		}
	}

	s.mu.RLock()
	wireServer := s.wireServer
	config := s.config
	s.mu.RUnlock()

	if wireServer == nil {
		return ErrServerClosed
	}

	address := config.Address()
	if s.logger != nil {
		s.logger.Info("starting PostgreSQL wire protocol server", "address", address)
	}

	return wireServer.ListenAndServe(address)
}

// Serve accepts incoming connections on the listener.
// This method blocks until the server is shut down.
func (s *Server) Serve(listener net.Listener) error {
	// Start the server if not already started
	if !s.started.Load() {
		if err := s.Start(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.listener = listener
	wireServer := s.wireServer
	s.mu.Unlock()

	if wireServer == nil {
		return ErrServerClosed
	}

	if s.logger != nil {
		s.logger.Info("serving PostgreSQL wire protocol", "address", listener.Addr().String())
	}

	return wireServer.Serve(listener)
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	return s.Shutdown(context.Background())
}

// Shutdown gracefully shuts down the server with the given context.
// It waits for active connections to finish or until the context is cancelled.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.closed.Load() {
		return nil
	}

	s.mu.Lock()
	wireServer := s.wireServer
	conn := s.conn
	connMgr := s.connectionManager
	s.mu.Unlock()

	if wireServer != nil {
		if err := wireServer.Shutdown(ctx); err != nil {
			if s.logger != nil {
				s.logger.Warn("error during server shutdown", "error", err)
			}
		}
	}

	// Close the connection manager
	if connMgr != nil {
		if err := connMgr.Close(); err != nil {
			if s.logger != nil {
				s.logger.Warn("error closing connection manager", "error", err)
			}
		}
	}

	// Close all sessions
	s.closeAllSessions()

	// Close the database connection
	if conn != nil {
		if err := conn.Close(); err != nil {
			if s.logger != nil {
				s.logger.Warn("error closing database connection", "error", err)
			}
		}
	}

	s.mu.Lock()
	s.wireServer = nil
	s.conn = nil
	s.listener = nil
	s.connectionManager = nil
	s.mu.Unlock()

	s.closed.Store(true)
	s.started.Store(false)

	if s.logger != nil {
		s.logger.Info("PostgreSQL wire protocol server stopped")
	}

	return nil
}

// Close immediately closes the server without waiting for connections.
func (s *Server) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.mu.Lock()
	wireServer := s.wireServer
	conn := s.conn
	listener := s.listener
	connMgr := s.connectionManager
	s.mu.Unlock()

	var errs []error

	if wireServer != nil {
		if err := wireServer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if listener != nil {
		if err := listener.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close the connection manager
	if connMgr != nil {
		if err := connMgr.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close all sessions
	s.closeAllSessions()

	// Close the database connection
	if conn != nil {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	s.mu.Lock()
	s.wireServer = nil
	s.conn = nil
	s.listener = nil
	s.connectionManager = nil
	s.mu.Unlock()

	s.closed.Store(true)
	s.started.Store(false)

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// IsRunning returns whether the server is currently running.
func (s *Server) IsRunning() bool {
	return s.started.Load() && !s.closed.Load()
}

// Config returns the server configuration.
func (s *Server) Config() *Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// ConnectionCount returns the number of active connections.
func (s *Server) ConnectionCount() int64 {
	return s.connCount.Load()
}

// validateCredentials validates the provided credentials during authentication.
func (s *Server) validateCredentials(
	ctx context.Context,
	database, username, password string,
) (context.Context, bool, error) {
	s.mu.RLock()
	config := s.config
	s.mu.RUnlock()

	if config == nil {
		return ctx, false, errors.New("server not configured")
	}

	// Get the authenticator from config
	authenticator := config.GetAuthenticator()
	if authenticator == nil {
		// This shouldn't happen if config is properly set up
		return ctx, false, errors.New("no authenticator configured")
	}

	// Use the authenticator to validate credentials
	success, err := authenticator.Authenticate(ctx, username, password, database)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("authentication error",
				"username", username,
				"database", database,
				"error", err,
			)
		}
		return ctx, false, err
	}

	if !success && s.logger != nil {
		s.logger.Info("authentication failed",
			"username", username,
			"database", database,
		)
	}

	return ctx, success, nil
}

// sessionMiddleware is called when a new session is established.
func (s *Server) sessionMiddleware(ctx context.Context) (context.Context, error) {
	// Check connection limit and acquire slot if using connection manager
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		// Try to acquire a connection slot (may queue if at limit)
		if err := connMgr.AcquireSlot(ctx); err != nil {
			if s.logger != nil {
				s.logger.Warn("connection rejected", "error", err)
			}
			return ctx, err
		}
	} else {
		// Legacy connection limit check without connection manager
		s.mu.RLock()
		maxConns := s.config.MaxConnections
		s.mu.RUnlock()

		currentConns := s.connCount.Load()
		if currentConns >= int64(maxConns) {
			if s.logger != nil {
				s.logger.Warn("connection rejected: too many connections",
					"current", currentConns,
					"max", maxConns,
				)
			}
			return ctx, ErrTooManyConnections
		}
	}

	// Get client parameters from psql-wire context
	clientParams := wire.ClientParameters(ctx)
	username := clientParams[wire.ParamUsername]
	database := clientParams[wire.ParamDatabase]

	// Get remote address
	remoteAddr := ""
	if addr := wire.RemoteAddress(ctx); addr != nil {
		remoteAddr = addr.String()
	}

	// Create a new session
	session := NewSession(s, username, database, remoteAddr)

	// Store additional startup parameters in session attributes
	if appName, ok := clientParams["application_name"]; ok && appName != "" {
		session.SetAttribute("application_name", appName)
	}
	if clientEncoding, ok := clientParams["client_encoding"]; ok && clientEncoding != "" {
		session.SetAttribute("client_encoding", clientEncoding)
	}

	// Store all client parameters for reference
	session.SetAttribute("startup_params", clientParams)

	// Track the session
	s.sessionsMu.Lock()
	s.sessions[session.ID()] = session
	s.sessionsMu.Unlock()

	// Register with connection manager if enabled
	if connMgr != nil {
		connMgr.RegisterConnection(session)
	}

	// Increment connection count
	s.connCount.Add(1)

	// Log startup parameters if enabled
	s.mu.RLock()
	logStartupParams := s.config != nil && s.config.LogStartupParams
	s.mu.RUnlock()

	if s.logger != nil {
		if logStartupParams {
			s.logger.Info("new session established",
				"session_id", session.ID(),
				"username", username,
				"database", database,
				"remote_addr", remoteAddr,
				"startup_params", clientParams,
			)
		} else {
			s.logger.Info("new session established",
				"session_id", session.ID(),
				"username", username,
				"database", database,
				"remote_addr", remoteAddr,
			)
		}
	}

	// Add session to context
	return ContextWithSession(ctx, session), nil
}

// onConnectionClose is called when a connection is terminated.
func (s *Server) onConnectionClose(ctx context.Context) error {
	// Get session from context
	session, ok := SessionFromContext(ctx)
	if !ok {
		return nil
	}

	// Remove session from tracking
	s.sessionsMu.Lock()
	delete(s.sessions, session.ID())
	s.sessionsMu.Unlock()

	// Unregister from connection manager if enabled
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		connMgr.UnregisterConnection(session.ID())
	}

	// Remove notification subscriptions for this session
	if s.notificationHub != nil {
		s.notificationHub.RemoveSession(session.ID())
	}

	// Remove cancel key for this session
	s.mu.RLock()
	cm := s.cancellationManager
	s.mu.RUnlock()
	if cm != nil {
		cm.RemoveCancelKey(session.ID())
	}

	// Decrement connection count
	s.connCount.Add(-1)

	// Close the session
	if err := session.Close(); err != nil {
		if s.logger != nil {
			s.logger.Warn("error closing session", "session_id", session.ID(), "error", err)
		}
	}

	if s.logger != nil {
		s.logger.Info("session closed", "session_id", session.ID())
	}

	return nil
}

// closeAllSessions closes all active sessions.
func (s *Server) closeAllSessions() {
	s.sessionsMu.Lock()
	sessions := make([]*Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.sessions = make(map[uint64]*Session)
	s.sessionsMu.Unlock()

	for _, session := range sessions {
		if err := session.Close(); err != nil {
			if s.logger != nil {
				s.logger.Warn("error closing session", "session_id", session.ID(), "error", err)
			}
		}
	}
}

// GetSession returns a session by ID.
func (s *Server) GetSession(id uint64) (*Session, bool) {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	session, ok := s.sessions[id]
	return session, ok
}

// SessionIDs returns the IDs of all active sessions.
func (s *Server) SessionIDs() []uint64 {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()

	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	return ids
}

// WaitUntilReady waits until the server is ready to accept connections
// or until the timeout expires.
func (s *Server) WaitUntilReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.IsRunning() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("server did not become ready within timeout")
}

// NotificationHub returns the server's notification hub for LISTEN/NOTIFY support.
func (s *Server) NotificationHub() *NotificationHub {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.notificationHub
}

// ConnectionManager returns the server's connection manager.
// Returns nil if connection pooling is disabled.
func (s *Server) ConnectionManager() *ConnectionManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connectionManager
}

// GetConnectionStats returns current connection statistics.
// If connection pooling is disabled, returns basic stats from the legacy counter.
func (s *Server) GetConnectionStats() ConnectionStats {
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		return connMgr.GetStats()
	}

	// Return basic stats from legacy counter
	return ConnectionStats{
		ActiveConnections: s.connCount.Load(),
		LastUpdated:       time.Now(),
	}
}

// MarkConnectionActive marks a session's connection as active.
// This should be called when a session starts processing a query.
func (s *Server) MarkConnectionActive(sessionID uint64) {
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		connMgr.MarkActive(sessionID)
	}
}

// MarkConnectionIdle marks a session's connection as idle.
// This should be called when a session finishes processing a query.
func (s *Server) MarkConnectionIdle(sessionID uint64) {
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		connMgr.MarkIdle(sessionID)
	}
}

// UpdateConnectionActivity updates the last activity timestamp for a session.
// This should be called whenever a session has activity to prevent idle timeout.
func (s *Server) UpdateConnectionActivity(sessionID uint64) {
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		connMgr.UpdateActivity(sessionID)
	}
}

// ForceCloseIdleConnections forces immediate cleanup of idle connections.
// Returns the number of connections closed.
func (s *Server) ForceCloseIdleConnections(count int) int {
	s.mu.RLock()
	connMgr := s.connectionManager
	s.mu.RUnlock()

	if connMgr != nil {
		return connMgr.ForceCloseIdleConnections(count)
	}
	return 0
}

// CancellationManager returns the server's cancellation manager.
func (s *Server) CancellationManager() *CancellationManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cancellationManager
}

// Observability returns the server's observability manager.
func (s *Server) Observability() *ObservabilityManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.observability
}

// StartMetricsServer starts the Prometheus metrics HTTP endpoint.
// The addr should be in the format "host:port" (e.g., ":9090").
func (s *Server) StartMetricsServer(addr string) error {
	s.mu.RLock()
	om := s.observability
	s.mu.RUnlock()

	if om == nil {
		return nil
	}

	return om.StartMetricsServer(addr)
}

// StopMetricsServer stops the Prometheus metrics HTTP endpoint.
func (s *Server) StopMetricsServer(ctx context.Context) error {
	s.mu.RLock()
	om := s.observability
	s.mu.RUnlock()

	if om == nil {
		return nil
	}

	return om.StopMetricsServer(ctx)
}

// generateBackendKeyData generates the backend key data for a new session.
// This is called by psql-wire during the handshake to get the process ID
// and secret key that will be sent to the client in the BackendKeyData message.
func (s *Server) generateBackendKeyData(ctx context.Context) (processID int32, secretKey int32) {
	// Get session from context
	session, ok := SessionFromContext(ctx)
	if !ok {
		// If no session in context yet, generate a temporary key
		// This shouldn't happen in normal flow, but handle it gracefully
		return 0, 0
	}

	s.mu.RLock()
	cm := s.cancellationManager
	s.mu.RUnlock()

	if cm == nil {
		return int32(session.ID()), 0
	}

	return cm.GenerateCancelKey(session.ID())
}

// handleCancelRequest handles a cancel request from a PostgreSQL client.
// This is called by psql-wire when a CancelRequest message is received.
func (s *Server) handleCancelRequest(ctx context.Context, processID int32, secretKey int32) error {
	s.mu.RLock()
	cm := s.cancellationManager
	logger := s.logger
	s.mu.RUnlock()

	if cm == nil {
		if logger != nil {
			logger.Warn("cancel request received but cancellation manager not initialized",
				"process_id", processID,
			)
		}
		return ErrNoRunningQuery
	}

	if logger != nil {
		logger.Debug("processing cancel request",
			"process_id", processID,
		)
	}

	err := cm.HandleCancelRequest(ctx, processID, secretKey)
	if err != nil {
		if logger != nil {
			logger.Debug("cancel request failed",
				"process_id", processID,
				"error", err,
			)
		}
		return err
	}

	if logger != nil {
		logger.Info("query cancelled via cancel request",
			"process_id", processID,
		)
	}

	return nil
}

// CancelQuery cancels a running query for a session.
// This is a convenience method that delegates to the cancellation manager.
func (s *Server) CancelQuery(sessionID uint64) error {
	s.mu.RLock()
	cm := s.cancellationManager
	s.mu.RUnlock()

	if cm == nil {
		return ErrNoRunningQuery
	}

	return cm.CancelQuery(sessionID)
}
