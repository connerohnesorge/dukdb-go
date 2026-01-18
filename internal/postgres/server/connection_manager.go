// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Connection management errors.
var (
	// ErrTooManyConnections is returned when the maximum connection limit is reached.
	ErrTooManyConnections = errors.New("too many connections")

	// ErrConnectionTimeout is returned when a connection request times out.
	ErrConnectionTimeout = errors.New("connection timeout")

	// ErrConnectionQueueFull is returned when the connection queue is full.
	ErrConnectionQueueFull = errors.New("connection queue full")

	// ErrConnectionManagerClosed is returned when the connection manager is closed.
	ErrConnectionManagerClosed = errors.New("connection manager closed")
)

// ConnectionTimeouts holds timeout configuration for connections.
type ConnectionTimeouts struct {
	// ConnectTimeout is the maximum time to establish a connection.
	// Zero means no timeout.
	ConnectTimeout time.Duration

	// IdleTimeout is the maximum time a connection can remain idle before being closed.
	// Zero means connections are never closed due to being idle.
	IdleTimeout time.Duration

	// SessionTimeout is the maximum duration for a session.
	// Zero means sessions have no maximum duration.
	SessionTimeout time.Duration

	// QueueTimeout is the maximum time to wait in queue for a connection slot.
	// Zero means use a default of 30 seconds.
	QueueTimeout time.Duration
}

// DefaultConnectionTimeouts returns default timeout configuration.
func DefaultConnectionTimeouts() *ConnectionTimeouts {
	return &ConnectionTimeouts{
		ConnectTimeout: 30 * time.Second,
		IdleTimeout:    5 * time.Minute,
		SessionTimeout: 0, // No session timeout by default
		QueueTimeout:   30 * time.Second,
	}
}

// ConnectionStats holds connection statistics for monitoring.
type ConnectionStats struct {
	// TotalConnections is the total number of connections established since server start.
	TotalConnections int64

	// ActiveConnections is the current number of active connections.
	ActiveConnections int64

	// IdleConnections is the current number of idle connections.
	IdleConnections int64

	// QueuedConnections is the current number of connections waiting in queue.
	QueuedConnections int64

	// RejectedConnections is the total number of rejected connections due to limits.
	RejectedConnections int64

	// TimedOutConnections is the total number of connections that timed out.
	TimedOutConnections int64

	// ClosedDueToIdle is the total number of connections closed due to idle timeout.
	ClosedDueToIdle int64

	// ClosedDueToSession is the total number of connections closed due to session timeout.
	ClosedDueToSession int64

	// MaxConnectionsReached is the peak number of concurrent connections.
	MaxConnectionsReached int64

	// ConnectionRate is the number of connections per second (calculated over 1 minute).
	ConnectionRate float64

	// AvgWaitTime is the average time spent waiting in queue (in milliseconds).
	AvgWaitTime float64

	// LastUpdated is the timestamp when stats were last updated.
	LastUpdated time.Time
}

// ManagedConnection represents a connection tracked by the connection manager.
type ManagedConnection struct {
	// ID is the unique connection identifier.
	ID uint64

	// Session is the associated session.
	Session *Session

	// CreatedAt is when the connection was established.
	CreatedAt time.Time

	// LastActivityAt is when the connection was last active.
	LastActivityAt time.Time

	// State indicates whether the connection is active or idle.
	State ConnectionState

	// mu protects the connection state.
	mu sync.RWMutex
}

// ConnectionState represents the state of a connection.
type ConnectionState int

const (
	// ConnectionStateActive indicates the connection is actively processing queries.
	ConnectionStateActive ConnectionState = iota

	// ConnectionStateIdle indicates the connection is idle.
	ConnectionStateIdle
)

// queuedRequest represents a connection request waiting in queue.
type queuedRequest struct {
	ctx      context.Context
	resultCh chan error
	addedAt  time.Time
}

// ConnectionManager manages connection pooling, limits, timeouts, and statistics.
type ConnectionManager struct {
	mu sync.RWMutex

	// config holds the server configuration.
	config *Config

	// timeouts holds timeout configuration.
	timeouts *ConnectionTimeouts

	// connections tracks all managed connections by session ID.
	connections map[uint64]*ManagedConnection

	// connectionQueue is the FIFO queue for waiting connections.
	connectionQueue *list.List

	// queueMu protects the connection queue.
	queueMu sync.Mutex

	// queueCond is used to signal waiting goroutines.
	queueCond *sync.Cond

	// stats holds connection statistics.
	stats ConnectionStats

	// statsMu protects stats updates.
	statsMu sync.RWMutex

	// closed indicates if the manager is closed.
	closed atomic.Bool

	// stopCleanup signals the cleanup goroutine to stop.
	stopCleanup chan struct{}

	// cleanupDone signals when cleanup goroutine has stopped.
	cleanupDone chan struct{}

	// recentConnections tracks connection times for rate calculation.
	recentConnections []time.Time

	// recentMu protects recentConnections.
	recentMu sync.Mutex

	// totalWaitTime tracks cumulative wait time for average calculation.
	totalWaitTime int64

	// totalWaitCount tracks number of waits for average calculation.
	totalWaitCount int64

	// maxQueueSize is the maximum size of the connection queue.
	// Zero means unlimited.
	maxQueueSize int
}

// NewConnectionManager creates a new connection manager.
func NewConnectionManager(config *Config, timeouts *ConnectionTimeouts) *ConnectionManager {
	if timeouts == nil {
		timeouts = DefaultConnectionTimeouts()
	}

	cm := &ConnectionManager{
		config:            config,
		timeouts:          timeouts,
		connections:       make(map[uint64]*ManagedConnection),
		connectionQueue:   list.New(),
		stopCleanup:       make(chan struct{}),
		cleanupDone:       make(chan struct{}),
		recentConnections: make([]time.Time, 0, 1000),
		maxQueueSize:      1000, // Default max queue size
	}

	cm.queueCond = sync.NewCond(&cm.queueMu)

	// Start the idle cleanup goroutine
	go cm.idleCleanupLoop()

	return cm
}

// AcquireSlot attempts to acquire a connection slot.
// If at the connection limit, it will queue the request and wait.
// Returns nil if a slot is acquired, or an error if it times out or fails.
func (cm *ConnectionManager) AcquireSlot(ctx context.Context) error {
	if cm.closed.Load() {
		return ErrConnectionManagerClosed
	}

	cm.mu.RLock()
	maxConns := cm.config.MaxConnections
	currentConns := int(cm.stats.ActiveConnections + cm.stats.IdleConnections)
	cm.mu.RUnlock()

	// If we're under the limit, acquire immediately
	if currentConns < maxConns {
		return nil
	}

	// We're at the limit, need to queue
	return cm.waitForSlot(ctx)
}

// waitForSlot waits in the queue for a connection slot to become available.
func (cm *ConnectionManager) waitForSlot(ctx context.Context) error {
	cm.queueMu.Lock()

	// Check if queue is full
	if cm.maxQueueSize > 0 && cm.connectionQueue.Len() >= cm.maxQueueSize {
		cm.queueMu.Unlock()
		cm.incrementRejected()
		return ErrConnectionQueueFull
	}

	// Create a queued request
	resultCh := make(chan error, 1)
	req := &queuedRequest{
		ctx:      ctx,
		resultCh: resultCh,
		addedAt:  time.Now(),
	}

	// Add to queue
	elem := cm.connectionQueue.PushBack(req)
	cm.updateQueuedCount(1)
	cm.queueMu.Unlock()

	// Determine timeout
	timeout := cm.timeouts.QueueTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// Create a timeout context if needed
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Wait for result or timeout
	select {
	case err := <-resultCh:
		waitTime := time.Since(req.addedAt)
		cm.recordWaitTime(waitTime)
		return err

	case <-ctx.Done():
		// Remove from queue
		cm.queueMu.Lock()
		cm.connectionQueue.Remove(elem)
		cm.updateQueuedCount(-1)
		cm.queueMu.Unlock()

		cm.incrementTimedOut()
		return ErrConnectionTimeout
	}
}

// ReleaseSlot releases a connection slot, potentially allowing a queued request through.
func (cm *ConnectionManager) ReleaseSlot() {
	cm.queueMu.Lock()
	defer cm.queueMu.Unlock()

	// If there are queued requests, let one through
	if cm.connectionQueue.Len() > 0 {
		elem := cm.connectionQueue.Front()
		if elem != nil {
			req := elem.Value.(*queuedRequest)
			cm.connectionQueue.Remove(elem)
			cm.updateQueuedCount(-1)

			// Signal the waiting goroutine
			select {
			case req.resultCh <- nil:
			default:
			}
		}
	}
}

// RegisterConnection registers a new connection with the manager.
func (cm *ConnectionManager) RegisterConnection(session *Session) *ManagedConnection {
	if cm.closed.Load() {
		return nil
	}

	now := time.Now()
	conn := &ManagedConnection{
		ID:             session.ID(),
		Session:        session,
		CreatedAt:      now,
		LastActivityAt: now,
		State:          ConnectionStateActive,
	}

	cm.mu.Lock()
	cm.connections[session.ID()] = conn
	cm.mu.Unlock()

	// Update stats
	cm.incrementTotal()
	cm.updateActiveCount(1)
	cm.recordConnection(now)

	return conn
}

// UnregisterConnection removes a connection from the manager.
func (cm *ConnectionManager) UnregisterConnection(sessionID uint64) {
	cm.mu.Lock()
	conn, exists := cm.connections[sessionID]
	if exists {
		delete(cm.connections, sessionID)
	}
	cm.mu.Unlock()

	if exists {
		conn.mu.RLock()
		state := conn.State
		conn.mu.RUnlock()

		if state == ConnectionStateActive {
			cm.updateActiveCount(-1)
		} else {
			cm.updateIdleCount(-1)
		}

		// Release the slot for queued connections
		cm.ReleaseSlot()
	}
}

// MarkActive marks a connection as active.
func (cm *ConnectionManager) MarkActive(sessionID uint64) {
	cm.mu.RLock()
	conn, exists := cm.connections[sessionID]
	cm.mu.RUnlock()

	if !exists {
		return
	}

	conn.mu.Lock()
	wasIdle := conn.State == ConnectionStateIdle
	conn.State = ConnectionStateActive
	conn.LastActivityAt = time.Now()
	conn.mu.Unlock()

	if wasIdle {
		cm.updateIdleCount(-1)
		cm.updateActiveCount(1)
	}
}

// MarkIdle marks a connection as idle.
func (cm *ConnectionManager) MarkIdle(sessionID uint64) {
	cm.mu.RLock()
	conn, exists := cm.connections[sessionID]
	cm.mu.RUnlock()

	if !exists {
		return
	}

	conn.mu.Lock()
	wasActive := conn.State == ConnectionStateActive
	conn.State = ConnectionStateIdle
	conn.LastActivityAt = time.Now()
	conn.mu.Unlock()

	if wasActive {
		cm.updateActiveCount(-1)
		cm.updateIdleCount(1)
	}
}

// UpdateActivity updates the last activity time for a connection.
func (cm *ConnectionManager) UpdateActivity(sessionID uint64) {
	cm.mu.RLock()
	conn, exists := cm.connections[sessionID]
	cm.mu.RUnlock()

	if exists {
		conn.mu.Lock()
		conn.LastActivityAt = time.Now()
		conn.mu.Unlock()
	}
}

// GetConnection returns a managed connection by session ID.
func (cm *ConnectionManager) GetConnection(sessionID uint64) (*ManagedConnection, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	conn, ok := cm.connections[sessionID]
	return conn, ok
}

// GetStats returns a copy of the current connection statistics.
func (cm *ConnectionManager) GetStats() ConnectionStats {
	cm.statsMu.RLock()
	defer cm.statsMu.RUnlock()

	stats := cm.stats
	stats.LastUpdated = time.Now()

	// Calculate connection rate
	cm.recentMu.Lock()
	now := time.Now()
	oneMinuteAgo := now.Add(-1 * time.Minute)

	// Filter to connections in the last minute
	filtered := make([]time.Time, 0, len(cm.recentConnections))
	for _, t := range cm.recentConnections {
		if t.After(oneMinuteAgo) {
			filtered = append(filtered, t)
		}
	}
	cm.recentConnections = filtered
	stats.ConnectionRate = float64(len(filtered)) / 60.0
	cm.recentMu.Unlock()

	// Calculate average wait time
	if cm.totalWaitCount > 0 {
		stats.AvgWaitTime = float64(
			atomic.LoadInt64(&cm.totalWaitTime),
		) / float64(
			atomic.LoadInt64(&cm.totalWaitCount),
		)
	}

	return stats
}

// idleCleanupLoop periodically checks for and closes idle connections.
func (cm *ConnectionManager) idleCleanupLoop() {
	defer close(cm.cleanupDone)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCleanup:
			return
		case <-ticker.C:
			cm.cleanupIdleConnections()
			cm.cleanupExpiredSessions()
		}
	}
}

// cleanupIdleConnections closes connections that have been idle too long.
func (cm *ConnectionManager) cleanupIdleConnections() {
	if cm.timeouts.IdleTimeout == 0 {
		return
	}

	now := time.Now()
	idleThreshold := now.Add(-cm.timeouts.IdleTimeout)

	cm.mu.RLock()
	var toClose []*ManagedConnection
	for _, conn := range cm.connections {
		conn.mu.RLock()
		if conn.State == ConnectionStateIdle && conn.LastActivityAt.Before(idleThreshold) {
			toClose = append(toClose, conn)
		}
		conn.mu.RUnlock()
	}
	cm.mu.RUnlock()

	// Close idle connections
	for _, conn := range toClose {
		if conn.Session != nil {
			_ = conn.Session.Close()
		}
		cm.UnregisterConnection(conn.ID)
		cm.incrementClosedDueToIdle()
	}
}

// cleanupExpiredSessions closes sessions that have exceeded their maximum duration.
func (cm *ConnectionManager) cleanupExpiredSessions() {
	if cm.timeouts.SessionTimeout == 0 {
		return
	}

	now := time.Now()
	sessionThreshold := now.Add(-cm.timeouts.SessionTimeout)

	cm.mu.RLock()
	var toClose []*ManagedConnection
	for _, conn := range cm.connections {
		if conn.CreatedAt.Before(sessionThreshold) {
			toClose = append(toClose, conn)
		}
	}
	cm.mu.RUnlock()

	// Close expired sessions
	for _, conn := range toClose {
		if conn.Session != nil {
			_ = conn.Session.Close()
		}
		cm.UnregisterConnection(conn.ID)
		cm.incrementClosedDueToSession()
	}
}

// Close stops the connection manager and cleans up resources.
func (cm *ConnectionManager) Close() error {
	if cm.closed.Swap(true) {
		return nil
	}

	// Stop the cleanup goroutine
	close(cm.stopCleanup)
	<-cm.cleanupDone

	// Wake up any waiting goroutines
	cm.queueCond.Broadcast()

	// Drain the queue and reject all waiting requests
	cm.queueMu.Lock()
	for elem := cm.connectionQueue.Front(); elem != nil; elem = elem.Next() {
		req := elem.Value.(*queuedRequest)
		select {
		case req.resultCh <- ErrConnectionManagerClosed:
		default:
		}
	}
	cm.connectionQueue.Init()
	cm.queueMu.Unlock()

	return nil
}

// SetMaxQueueSize sets the maximum size of the connection queue.
func (cm *ConnectionManager) SetMaxQueueSize(size int) {
	cm.queueMu.Lock()
	cm.maxQueueSize = size
	cm.queueMu.Unlock()
}

// SetTimeouts updates the timeout configuration.
func (cm *ConnectionManager) SetTimeouts(timeouts *ConnectionTimeouts) {
	cm.mu.Lock()
	cm.timeouts = timeouts
	cm.mu.Unlock()
}

// GetTimeouts returns the current timeout configuration.
func (cm *ConnectionManager) GetTimeouts() *ConnectionTimeouts {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.timeouts
}

// Helper methods for updating statistics

func (cm *ConnectionManager) incrementTotal() {
	cm.statsMu.Lock()
	cm.stats.TotalConnections++
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) incrementRejected() {
	cm.statsMu.Lock()
	cm.stats.RejectedConnections++
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) incrementTimedOut() {
	cm.statsMu.Lock()
	cm.stats.TimedOutConnections++
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) incrementClosedDueToIdle() {
	cm.statsMu.Lock()
	cm.stats.ClosedDueToIdle++
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) incrementClosedDueToSession() {
	cm.statsMu.Lock()
	cm.stats.ClosedDueToSession++
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) updateActiveCount(delta int64) {
	cm.statsMu.Lock()
	cm.stats.ActiveConnections += delta
	if cm.stats.ActiveConnections > cm.stats.MaxConnectionsReached {
		cm.stats.MaxConnectionsReached = cm.stats.ActiveConnections
	}
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) updateIdleCount(delta int64) {
	cm.statsMu.Lock()
	cm.stats.IdleConnections += delta
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) updateQueuedCount(delta int64) {
	cm.statsMu.Lock()
	cm.stats.QueuedConnections += delta
	cm.statsMu.Unlock()
}

func (cm *ConnectionManager) recordConnection(t time.Time) {
	cm.recentMu.Lock()
	cm.recentConnections = append(cm.recentConnections, t)
	// Keep only the last 10000 connections for rate calculation
	if len(cm.recentConnections) > 10000 {
		cm.recentConnections = cm.recentConnections[len(cm.recentConnections)-10000:]
	}
	cm.recentMu.Unlock()
}

func (cm *ConnectionManager) recordWaitTime(d time.Duration) {
	atomic.AddInt64(&cm.totalWaitTime, d.Milliseconds())
	atomic.AddInt64(&cm.totalWaitCount, 1)
}

// ConnectionCount returns the current number of connections (active + idle).
func (cm *ConnectionManager) ConnectionCount() int64 {
	cm.statsMu.RLock()
	defer cm.statsMu.RUnlock()
	return cm.stats.ActiveConnections + cm.stats.IdleConnections
}

// ActiveCount returns the current number of active connections.
func (cm *ConnectionManager) ActiveCount() int64 {
	cm.statsMu.RLock()
	defer cm.statsMu.RUnlock()
	return cm.stats.ActiveConnections
}

// IdleCount returns the current number of idle connections.
func (cm *ConnectionManager) IdleCount() int64 {
	cm.statsMu.RLock()
	defer cm.statsMu.RUnlock()
	return cm.stats.IdleConnections
}

// QueueLength returns the current number of connections waiting in queue.
func (cm *ConnectionManager) QueueLength() int {
	cm.queueMu.Lock()
	defer cm.queueMu.Unlock()
	return cm.connectionQueue.Len()
}

// IsAtLimit returns true if the connection limit has been reached.
func (cm *ConnectionManager) IsAtLimit() bool {
	cm.mu.RLock()
	maxConns := cm.config.MaxConnections
	cm.mu.RUnlock()

	return cm.ConnectionCount() >= int64(maxConns)
}

// ForceCloseIdleConnections forces immediate cleanup of idle connections.
// This can be called when approaching the connection limit.
func (cm *ConnectionManager) ForceCloseIdleConnections(count int) int {
	cm.mu.RLock()
	var idleConns []*ManagedConnection
	for _, conn := range cm.connections {
		conn.mu.RLock()
		if conn.State == ConnectionStateIdle {
			idleConns = append(idleConns, conn)
		}
		conn.mu.RUnlock()
		if len(idleConns) >= count {
			break
		}
	}
	cm.mu.RUnlock()

	closed := 0
	for _, conn := range idleConns {
		if conn.Session != nil {
			_ = conn.Session.Close()
		}
		cm.UnregisterConnection(conn.ID)
		cm.incrementClosedDueToIdle()
		closed++
	}

	return closed
}
