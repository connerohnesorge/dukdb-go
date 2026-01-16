package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionManager_NewConnectionManager(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	assert.NotNil(t, cm)
	assert.Equal(t, int64(0), cm.ConnectionCount())
	assert.Equal(t, int64(0), cm.ActiveCount())
	assert.Equal(t, int64(0), cm.IdleCount())
}

func TestConnectionManager_AcquireSlot(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 2

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	ctx := context.Background()

	// Should acquire slots without issues when under limit
	err := cm.AcquireSlot(ctx)
	assert.NoError(t, err)

	err = cm.AcquireSlot(ctx)
	assert.NoError(t, err)
}

func TestConnectionManager_RegisterConnection(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	// Create a mock session
	mockServer := &Server{config: config}
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")

	// Register the connection
	conn := cm.RegisterConnection(session)
	assert.NotNil(t, conn)
	assert.Equal(t, session.ID(), conn.ID)
	assert.Equal(t, ConnectionStateActive, conn.State)

	// Check stats
	assert.Equal(t, int64(1), cm.ActiveCount())
	assert.Equal(t, int64(0), cm.IdleCount())
	assert.Equal(t, int64(1), cm.ConnectionCount())

	stats := cm.GetStats()
	assert.Equal(t, int64(1), stats.TotalConnections)
	assert.Equal(t, int64(1), stats.ActiveConnections)
}

func TestConnectionManager_UnregisterConnection(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	// Create and register a mock session
	mockServer := &Server{config: config}
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)

	assert.Equal(t, int64(1), cm.ConnectionCount())

	// Unregister the connection
	cm.UnregisterConnection(session.ID())

	assert.Equal(t, int64(0), cm.ConnectionCount())
}

func TestConnectionManager_MarkActiveIdle(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	// Create and register a mock session
	mockServer := &Server{config: config}
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)

	// Initially active
	assert.Equal(t, int64(1), cm.ActiveCount())
	assert.Equal(t, int64(0), cm.IdleCount())

	// Mark as idle
	cm.MarkIdle(session.ID())
	assert.Equal(t, int64(0), cm.ActiveCount())
	assert.Equal(t, int64(1), cm.IdleCount())

	// Mark as active again
	cm.MarkActive(session.ID())
	assert.Equal(t, int64(1), cm.ActiveCount())
	assert.Equal(t, int64(0), cm.IdleCount())
}

func TestConnectionManager_MaxConnectionsEnforcement(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 2

	timeouts := &ConnectionTimeouts{
		QueueTimeout: 100 * time.Millisecond,
	}

	cm := NewConnectionManager(config, timeouts)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register two connections (at limit)
	session1 := NewSession(mockServer, "user1", "test_db", "127.0.0.1:12345")
	session2 := NewSession(mockServer, "user2", "test_db", "127.0.0.1:12346")

	cm.RegisterConnection(session1)
	cm.RegisterConnection(session2)

	assert.Equal(t, int64(2), cm.ConnectionCount())
	assert.True(t, cm.IsAtLimit())

	// Try to acquire another slot - should timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := cm.AcquireSlot(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrConnectionTimeout, err)
}

func TestConnectionManager_ConnectionQueuing(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 1

	timeouts := &ConnectionTimeouts{
		QueueTimeout: 2 * time.Second,
	}

	cm := NewConnectionManager(config, timeouts)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register one connection (at limit)
	session1 := NewSession(mockServer, "user1", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session1)

	// Start a goroutine that will try to acquire a slot
	var wg sync.WaitGroup
	var acquireErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		acquireErr = cm.AcquireSlot(ctx)
	}()

	// Give the goroutine time to queue
	time.Sleep(100 * time.Millisecond)

	// Check queue length
	assert.Equal(t, 1, cm.QueueLength())

	// Release the first connection
	cm.UnregisterConnection(session1.ID())

	// Wait for the goroutine
	wg.Wait()

	// The queued request should have succeeded
	assert.NoError(t, acquireErr)
}

func TestConnectionManager_ConnectionQueueFull(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 1

	timeouts := &ConnectionTimeouts{
		QueueTimeout: 2 * time.Second,
	}

	cm := NewConnectionManager(config, timeouts)
	cm.SetMaxQueueSize(1) // Only allow 1 in queue
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register one connection (at limit)
	session1 := NewSession(mockServer, "user1", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session1)

	// First queued request
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		_ = cm.AcquireSlot(ctx)
	}()

	// Give time to queue
	time.Sleep(50 * time.Millisecond)

	// Second request should fail with queue full
	ctx := context.Background()
	err := cm.AcquireSlot(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrConnectionQueueFull, err)

	// Cleanup
	cm.UnregisterConnection(session1.ID())
	wg.Wait()
}

func TestConnectionManager_IdleTimeout(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	timeouts := &ConnectionTimeouts{
		IdleTimeout: 100 * time.Millisecond,
	}

	cm := NewConnectionManager(config, timeouts)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register a connection and mark it idle
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)
	cm.MarkIdle(session.ID())

	// Manually trigger cleanup
	cm.cleanupIdleConnections()

	// Connection should still be there (not enough time passed)
	assert.Equal(t, int64(1), cm.ConnectionCount())

	// Wait for idle timeout
	time.Sleep(150 * time.Millisecond)

	// Manually trigger cleanup again
	cm.cleanupIdleConnections()

	// Connection should be removed
	assert.Equal(t, int64(0), cm.ConnectionCount())
}

func TestConnectionManager_SessionTimeout(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	timeouts := &ConnectionTimeouts{
		SessionTimeout: 100 * time.Millisecond,
	}

	cm := NewConnectionManager(config, timeouts)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register a connection
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)

	// Wait for session timeout
	time.Sleep(150 * time.Millisecond)

	// Manually trigger cleanup
	cm.cleanupExpiredSessions()

	// Connection should be removed
	assert.Equal(t, int64(0), cm.ConnectionCount())
}

func TestConnectionManager_UpdateActivity(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)

	// Get initial activity time
	conn, ok := cm.GetConnection(session.ID())
	require.True(t, ok)

	initialActivity := conn.LastActivityAt

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Update activity
	cm.UpdateActivity(session.ID())

	// Check activity time was updated
	conn.mu.RLock()
	newActivity := conn.LastActivityAt
	conn.mu.RUnlock()

	assert.True(t, newActivity.After(initialActivity))
}

func TestConnectionManager_GetStats(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register some connections
	session1 := NewSession(mockServer, "user1", "test_db", "127.0.0.1:12345")
	session2 := NewSession(mockServer, "user2", "test_db", "127.0.0.1:12346")
	session3 := NewSession(mockServer, "user3", "test_db", "127.0.0.1:12347")

	cm.RegisterConnection(session1)
	cm.RegisterConnection(session2)
	cm.RegisterConnection(session3)

	// Mark some as idle
	cm.MarkIdle(session2.ID())
	cm.MarkIdle(session3.ID())

	stats := cm.GetStats()

	assert.Equal(t, int64(3), stats.TotalConnections)
	assert.Equal(t, int64(1), stats.ActiveConnections)
	assert.Equal(t, int64(2), stats.IdleConnections)
	// MaxConnectionsReached is 3 because all 3 were active when registered
	assert.Equal(t, int64(3), stats.MaxConnectionsReached)
}

func TestConnectionManager_ForceCloseIdleConnections(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	// Register and idle some connections
	session1 := NewSession(mockServer, "user1", "test_db", "127.0.0.1:12345")
	session2 := NewSession(mockServer, "user2", "test_db", "127.0.0.1:12346")
	session3 := NewSession(mockServer, "user3", "test_db", "127.0.0.1:12347")

	cm.RegisterConnection(session1)
	cm.RegisterConnection(session2)
	cm.RegisterConnection(session3)

	cm.MarkIdle(session1.ID())
	cm.MarkIdle(session2.ID())
	// session3 stays active

	assert.Equal(t, int64(3), cm.ConnectionCount())
	assert.Equal(t, int64(2), cm.IdleCount())

	// Force close 1 idle connection
	closed := cm.ForceCloseIdleConnections(1)
	assert.Equal(t, 1, closed)
	assert.Equal(t, int64(2), cm.ConnectionCount())
	assert.Equal(t, int64(1), cm.IdleCount())
}

func TestConnectionManager_Close(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 10

	cm := NewConnectionManager(config, nil)

	mockServer := &Server{config: config}
	session := NewSession(mockServer, "test_user", "test_db", "127.0.0.1:12345")
	cm.RegisterConnection(session)

	// Close the manager
	err := cm.Close()
	assert.NoError(t, err)

	// Further operations should fail
	err = cm.AcquireSlot(context.Background())
	assert.Equal(t, ErrConnectionManagerClosed, err)
}

func TestConnectionManager_ConcurrentAccess(t *testing.T) {
	config := NewConfig()
	config.MaxConnections = 100

	cm := NewConnectionManager(config, nil)
	defer func() { _ = cm.Close() }()

	mockServer := &Server{config: config}

	var wg sync.WaitGroup
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			session := NewSession(mockServer, "user", "test_db", "127.0.0.1:12345")
			cm.RegisterConnection(session)

			// Simulate some activity
			cm.MarkActive(session.ID())
			time.Sleep(10 * time.Millisecond)
			cm.UpdateActivity(session.ID())
			cm.MarkIdle(session.ID())
			time.Sleep(10 * time.Millisecond)

			cm.UnregisterConnection(session.ID())
		}(i)
	}

	wg.Wait()

	// All connections should be cleaned up
	assert.Equal(t, int64(0), cm.ConnectionCount())

	stats := cm.GetStats()
	assert.Equal(t, int64(numGoroutines), stats.TotalConnections)
}

func TestDefaultConnectionTimeouts(t *testing.T) {
	timeouts := DefaultConnectionTimeouts()

	assert.Equal(t, 30*time.Second, timeouts.ConnectTimeout)
	assert.Equal(t, 5*time.Minute, timeouts.IdleTimeout)
	assert.Equal(t, time.Duration(0), timeouts.SessionTimeout)
	assert.Equal(t, 30*time.Second, timeouts.QueueTimeout)
}

func TestConfig_GetConnectionTimeouts(t *testing.T) {
	config := NewConfig()
	config.ConnectTimeout = 10 * time.Second
	config.IdleTimeout = 2 * time.Minute
	config.SessionTimeout = 1 * time.Hour
	config.QueueTimeout = 15 * time.Second

	timeouts := config.GetConnectionTimeouts()

	assert.Equal(t, 10*time.Second, timeouts.ConnectTimeout)
	assert.Equal(t, 2*time.Minute, timeouts.IdleTimeout)
	assert.Equal(t, 1*time.Hour, timeouts.SessionTimeout)
	assert.Equal(t, 15*time.Second, timeouts.QueueTimeout)
}

func TestServer_ConnectionManagerIntegration(t *testing.T) {
	config := NewConfig()
	config.EnableConnectionPooling = true
	config.MaxConnections = 5

	server, err := NewServer(config)
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	// Check connection manager is initialized
	cm := server.ConnectionManager()
	assert.NotNil(t, cm)

	// Get stats
	stats := server.GetConnectionStats()
	assert.Equal(t, int64(0), stats.ActiveConnections)
}

func TestServer_ConnectionManagerDisabled(t *testing.T) {
	config := NewConfig()
	config.EnableConnectionPooling = false

	server, err := NewServer(config)
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	// Check connection manager is nil
	cm := server.ConnectionManager()
	assert.Nil(t, cm)

	// GetConnectionStats should still work
	stats := server.GetConnectionStats()
	assert.Equal(t, int64(0), stats.ActiveConnections)
}
