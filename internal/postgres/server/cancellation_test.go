package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancellationManager_GenerateCancelKey(t *testing.T) {
	cm := NewCancellationManager(nil)

	// Generate keys for multiple sessions
	pid1, key1 := cm.GenerateCancelKey(1)
	pid2, key2 := cm.GenerateCancelKey(2)
	pid3, key3 := cm.GenerateCancelKey(1) // Same session, new key

	// Process IDs should match session IDs
	assert.Equal(t, int32(1), pid1)
	assert.Equal(t, int32(2), pid2)
	assert.Equal(t, int32(1), pid3)

	// Keys should be different (with very high probability)
	assert.NotEqual(t, key1, key2)
	// New key for same session should overwrite
	assert.NotEqual(t, key1, key3)
}

func TestCancellationManager_ValidateCancelKey(t *testing.T) {
	cm := NewCancellationManager(nil)

	// Generate a key
	pid, key := cm.GenerateCancelKey(42)

	// Valid key should succeed
	sessionID, err := cm.ValidateCancelKey(pid, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), sessionID)

	// Invalid key should fail
	_, err = cm.ValidateCancelKey(pid, key+1)
	assert.Equal(t, ErrInvalidCancelKey, err)

	// Non-existent session should fail
	_, err = cm.ValidateCancelKey(999, key)
	assert.Equal(t, ErrSessionNotFound, err)
}

func TestCancellationManager_RemoveCancelKey(t *testing.T) {
	cm := NewCancellationManager(nil)

	// Generate and then remove
	pid, key := cm.GenerateCancelKey(1)
	cm.RemoveCancelKey(1)

	// Should fail after removal
	_, err := cm.ValidateCancelKey(pid, key)
	assert.Equal(t, ErrSessionNotFound, err)
}

func TestCancellationManager_StartQuery(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Start a query
	queryCtx, cancel := cm.StartQuery(ctx, sessionID, "SELECT 1", 0)
	defer cancel()

	// Query should be running
	assert.True(t, cm.IsQueryRunning(sessionID))

	// Get query info
	query, duration, exists := cm.GetRunningQueryInfo(sessionID)
	assert.True(t, exists)
	assert.Equal(t, "SELECT 1", query)
	assert.True(t, duration >= 0)

	// Context should not be cancelled
	assert.Nil(t, queryCtx.Err())

	// End the query
	cancel()

	// Small delay to let cleanup run
	time.Sleep(10 * time.Millisecond)

	// Query should no longer be running
	assert.False(t, cm.IsQueryRunning(sessionID))
}

func TestCancellationManager_CancelQuery(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Start a query
	queryCtx, cancel := cm.StartQuery(ctx, sessionID, "SELECT pg_sleep(60)", 0)
	defer cancel()

	// Cancel via manager
	err := cm.CancelQuery(sessionID)
	require.NoError(t, err)

	// Context should be cancelled
	select {
	case <-queryCtx.Done():
		assert.Equal(t, context.Canceled, queryCtx.Err())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("query context was not cancelled")
	}
}

func TestCancellationManager_HandleCancelRequest(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Generate cancel key
	pid, key := cm.GenerateCancelKey(sessionID)

	// Start a query
	queryCtx, cancel := cm.StartQuery(ctx, sessionID, "SELECT 1", 0)
	defer cancel()

	// Handle cancel request
	err := cm.HandleCancelRequest(ctx, pid, key)
	require.NoError(t, err)

	// Context should be cancelled
	select {
	case <-queryCtx.Done():
		assert.Equal(t, context.Canceled, queryCtx.Err())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("query context was not cancelled")
	}
}

func TestCancellationManager_HandleCancelRequest_InvalidKey(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Generate cancel key
	pid, key := cm.GenerateCancelKey(sessionID)

	// Start a query
	_, cancel := cm.StartQuery(ctx, sessionID, "SELECT 1", 0)
	defer cancel()

	// Try with wrong key
	err := cm.HandleCancelRequest(ctx, pid, key+1)
	assert.Equal(t, ErrInvalidCancelKey, err)

	// Try with wrong session
	err = cm.HandleCancelRequest(ctx, 999, key)
	assert.Equal(t, ErrSessionNotFound, err)
}

func TestCancellationManager_StatementTimeout(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Start a query with 50ms timeout
	queryCtx, cancel := cm.StartQuery(ctx, sessionID, "SELECT pg_sleep(60)", 50*time.Millisecond)
	defer cancel()

	// Wait for timeout
	select {
	case <-queryCtx.Done():
		// Context was cancelled (expected)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("query was not cancelled by timeout")
	}
}

func TestCancellationManager_StatementTimeout_DisabledWithZero(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// Start a query with 0 timeout (no timeout)
	queryCtx, cancel := cm.StartQuery(ctx, sessionID, "SELECT 1", 0)
	defer cancel()

	// Wait briefly - context should not be cancelled
	select {
	case <-queryCtx.Done():
		t.Fatal("query was unexpectedly cancelled")
	case <-time.After(50 * time.Millisecond):
		// Expected - no timeout
	}

	// Context should still be valid
	assert.Nil(t, queryCtx.Err())
}

func TestCancellationManager_GetQueryDuration(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()
	sessionID := uint64(1)

	// No query running
	assert.Equal(t, time.Duration(0), cm.GetQueryDuration(sessionID))

	// Start a query
	_, cancel := cm.StartQuery(ctx, sessionID, "SELECT 1", 0)

	// Wait a bit
	time.Sleep(20 * time.Millisecond)

	// Duration should be > 0
	duration := cm.GetQueryDuration(sessionID)
	assert.True(t, duration >= 20*time.Millisecond)

	cancel()
}

func TestCancellationManager_ConcurrentQueries(t *testing.T) {
	cm := NewCancellationManager(nil)

	ctx := context.Background()

	// Start queries for multiple sessions
	_, cancel1 := cm.StartQuery(ctx, 1, "SELECT 1", 0)
	_, cancel2 := cm.StartQuery(ctx, 2, "SELECT 2", 0)
	_, cancel3 := cm.StartQuery(ctx, 3, "SELECT 3", 0)

	defer cancel1()
	defer cancel2()
	defer cancel3()

	// All should be running
	assert.True(t, cm.IsQueryRunning(1))
	assert.True(t, cm.IsQueryRunning(2))
	assert.True(t, cm.IsQueryRunning(3))

	// Cancel one
	err := cm.CancelQuery(2)
	require.NoError(t, err)

	// Others should still be running
	time.Sleep(10 * time.Millisecond)
	assert.True(t, cm.IsQueryRunning(1))
	assert.True(t, cm.IsQueryRunning(3))
}

func TestParseTimeoutValue(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"0", 0, false},
		{"", 0, false},
		{"1000", 1000 * time.Millisecond, false},
		{"1s", 1 * time.Second, false},
		{"1000ms", 1000 * time.Millisecond, false},
		{"5s", 5 * time.Second, false},
		{"1m", 1 * time.Minute, false},
		{"1min", 1 * time.Minute, false},
		{"1h", 1 * time.Hour, false},
		{"500", 500 * time.Millisecond, false},
		{"2500", 2500 * time.Millisecond, false},
		{"abc", 0, true},
		{"1x", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseTimeoutValue(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFormatTimeoutValue(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{0, "0"},
		{1000 * time.Millisecond, "1000"},
		{5 * time.Second, "5000"},
		{1 * time.Minute, "60000"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatTimeoutValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSession_StatementTimeout(t *testing.T) {
	session := NewSession(nil, "test", "testdb", "127.0.0.1")
	defer func() { _ = session.Close() }()

	// Default should be 0
	assert.Equal(t, time.Duration(0), session.GetStatementTimeout())

	// Set and get
	session.SetStatementTimeout(5 * time.Second)
	assert.Equal(t, 5*time.Second, session.GetStatementTimeout())

	// Reset
	session.SetStatementTimeout(0)
	assert.Equal(t, time.Duration(0), session.GetStatementTimeout())
}

func TestSession_LockTimeout(t *testing.T) {
	session := NewSession(nil, "test", "testdb", "127.0.0.1")
	defer func() { _ = session.Close() }()

	// Default should be 0
	assert.Equal(t, time.Duration(0), session.GetLockTimeout())

	// Set and get
	session.SetLockTimeout(1 * time.Second)
	assert.Equal(t, 1*time.Second, session.GetLockTimeout())

	// Reset
	session.SetLockTimeout(0)
	assert.Equal(t, time.Duration(0), session.GetLockTimeout())
}

func TestSession_CancelCurrentQuery(t *testing.T) {
	session := NewSession(nil, "test", "testdb", "127.0.0.1")
	defer func() { _ = session.Close() }()

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Set as current query cancel
	session.SetCurrentQueryCancel(cancel)

	// Cancel via session
	session.CancelCurrentQuery()

	// Context should be cancelled
	select {
	case <-ctx.Done():
		assert.Equal(t, context.Canceled, ctx.Err())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context was not cancelled")
	}

	// Calling again should be safe (no panic)
	session.CancelCurrentQuery()
}
