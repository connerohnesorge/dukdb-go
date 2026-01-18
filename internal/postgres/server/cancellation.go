// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Query cancellation errors.
var (
	// ErrInvalidCancelKey is returned when the cancel key doesn't match.
	ErrInvalidCancelKey = errors.New("invalid cancel key")

	// ErrSessionNotFound is returned when the target session is not found.
	ErrSessionNotFound = errors.New("session not found for cancel request")

	// ErrNoRunningQuery is returned when there's no query to cancel.
	ErrNoRunningQuery = errors.New("no running query to cancel")

	// ErrStatementTimeout is returned when a query exceeds the statement timeout.
	ErrStatementTimeout = errors.New("canceling statement due to statement timeout")

	// ErrLockTimeout is returned when waiting for a lock exceeds the lock timeout.
	ErrLockTimeout = errors.New("canceling statement due to lock timeout")
)

// CancelKey holds the process ID and secret key for query cancellation.
// These are sent to the client in the BackendKeyData message and must
// match for a cancel request to be valid.
type CancelKey struct {
	// ProcessID is the backend process identifier (we use session ID).
	ProcessID int32

	// SecretKey is a random secret key generated for each session.
	SecretKey int32
}

// QueryContext tracks a running query that can be cancelled.
type QueryContext struct {
	// ctx is the query's context.
	ctx context.Context

	// cancelFunc cancels the query context.
	cancelFunc context.CancelFunc

	// startTime is when the query started.
	startTime time.Time

	// query is the SQL query being executed (for logging).
	query string

	// statementTimeout is the timeout for this specific query.
	// Zero means no timeout (use session default).
	statementTimeout time.Duration

	// timer is the timeout timer if statement_timeout is set.
	timer *time.Timer
}

// CancellationManager manages query cancellation for the PostgreSQL server.
// It tracks cancel keys for each session and manages running query contexts.
type CancellationManager struct {
	mu sync.RWMutex

	// cancelKeys maps session ID to cancel key.
	cancelKeys map[uint64]*CancelKey

	// runningQueries maps session ID to current query context.
	runningQueries map[uint64]*QueryContext

	// server is the parent server for session lookup.
	server *Server
}

// NewCancellationManager creates a new cancellation manager.
func NewCancellationManager(server *Server) *CancellationManager {
	return &CancellationManager{
		cancelKeys:     make(map[uint64]*CancelKey),
		runningQueries: make(map[uint64]*QueryContext),
		server:         server,
	}
}

// GenerateCancelKey generates and stores a new cancel key for a session.
// Returns the process ID and secret key to be sent to the client.
func (cm *CancellationManager) GenerateCancelKey(
	sessionID uint64,
) (processID int32, secretKey int32) {
	// Generate a random secret key
	var secretBytes [4]byte
	if _, err := rand.Read(secretBytes[:]); err != nil {
		// Fallback to using session ID as entropy if crypto/rand fails
		secretKey = int32(sessionID ^ 0xDEADBEEF)
	} else {
		secretKey = int32(binary.BigEndian.Uint32(secretBytes[:]))
	}

	// Use session ID as process ID (truncated to int32)
	processID = int32(sessionID)

	key := &CancelKey{
		ProcessID: processID,
		SecretKey: secretKey,
	}

	cm.mu.Lock()
	cm.cancelKeys[sessionID] = key
	cm.mu.Unlock()

	return processID, secretKey
}

// RemoveCancelKey removes the cancel key for a session.
// Should be called when a session is closed.
func (cm *CancellationManager) RemoveCancelKey(sessionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.cancelKeys, sessionID)
	delete(cm.runningQueries, sessionID)
}

// ValidateCancelKey validates a cancel request.
// Returns the session ID if valid, or an error if invalid.
func (cm *CancellationManager) ValidateCancelKey(processID, secretKey int32) (uint64, error) {
	// Process ID is the session ID
	sessionID := uint64(processID)
	if processID < 0 {
		// Handle sign extension for negative process IDs
		sessionID = uint64(uint32(processID))
	}

	cm.mu.RLock()
	key, exists := cm.cancelKeys[sessionID]
	cm.mu.RUnlock()

	if !exists {
		return 0, ErrSessionNotFound
	}

	if key.SecretKey != secretKey {
		return 0, ErrInvalidCancelKey
	}

	return sessionID, nil
}

// GetCancelKey returns the cancel key for a session.
func (cm *CancellationManager) GetCancelKey(sessionID uint64) (*CancelKey, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	key, ok := cm.cancelKeys[sessionID]
	return key, ok
}

// StartQuery registers a new running query for a session.
// Returns a context that will be cancelled if a cancel request is received
// or if the statement timeout is exceeded.
func (cm *CancellationManager) StartQuery(
	parentCtx context.Context,
	sessionID uint64,
	query string,
	statementTimeout time.Duration,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parentCtx)

	qc := &QueryContext{
		ctx:              ctx,
		cancelFunc:       cancel,
		startTime:        time.Now(),
		query:            query,
		statementTimeout: statementTimeout,
	}

	// Set up statement timeout if configured
	if statementTimeout > 0 {
		qc.timer = time.AfterFunc(statementTimeout, func() {
			cm.mu.RLock()
			currentQC, exists := cm.runningQueries[sessionID]
			cm.mu.RUnlock()

			// Only cancel if this is still the same query
			if exists && currentQC == qc {
				cancel()
			}
		})
	}

	cm.mu.Lock()
	cm.runningQueries[sessionID] = qc
	cm.mu.Unlock()

	return ctx, func() {
		// Stop the timer if it exists
		if qc.timer != nil {
			qc.timer.Stop()
		}

		// Cancel the context
		cancel()

		// Remove from running queries
		cm.mu.Lock()
		if cm.runningQueries[sessionID] == qc {
			delete(cm.runningQueries, sessionID)
		}
		cm.mu.Unlock()
	}
}

// EndQuery removes a running query for a session.
// This is a convenience method when you have the cancel function.
func (cm *CancellationManager) EndQuery(sessionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if qc, exists := cm.runningQueries[sessionID]; exists {
		if qc.timer != nil {
			qc.timer.Stop()
		}
		delete(cm.runningQueries, sessionID)
	}
}

// CancelQuery cancels the running query for a session.
// Returns an error if no query is running or the session doesn't exist.
func (cm *CancellationManager) CancelQuery(sessionID uint64) error {
	cm.mu.RLock()
	qc, exists := cm.runningQueries[sessionID]
	cm.mu.RUnlock()

	if !exists {
		return ErrNoRunningQuery
	}

	// Cancel the query context
	qc.cancelFunc()

	return nil
}

// HandleCancelRequest processes a cancel request from the PostgreSQL protocol.
// This is called when a client sends a CancelRequest message.
func (cm *CancellationManager) HandleCancelRequest(
	ctx context.Context,
	processID, secretKey int32,
) error {
	// Validate the cancel key
	sessionID, err := cm.ValidateCancelKey(processID, secretKey)
	if err != nil {
		return err
	}

	// Cancel the running query
	return cm.CancelQuery(sessionID)
}

// GetQueryDuration returns how long the current query has been running.
// Returns 0 if no query is running.
func (cm *CancellationManager) GetQueryDuration(sessionID uint64) time.Duration {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if qc, exists := cm.runningQueries[sessionID]; exists {
		return time.Since(qc.startTime)
	}
	return 0
}

// IsQueryRunning returns whether a query is currently running for a session.
func (cm *CancellationManager) IsQueryRunning(sessionID uint64) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.runningQueries[sessionID]
	return exists
}

// GetRunningQueryInfo returns information about a running query.
func (cm *CancellationManager) GetRunningQueryInfo(
	sessionID uint64,
) (query string, duration time.Duration, exists bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if qc, ok := cm.runningQueries[sessionID]; ok {
		return qc.query, time.Since(qc.startTime), true
	}
	return "", 0, false
}
