// Package tests provides integration tests for dukdb-go that require the full backend engine.
// These tests are in a separate package to avoid import cycles.
package tests

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"

	"github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/require"
)

// TestConnId_Basic tests basic functionality of ConnId.
func TestConnId_Basic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	id, err := dukdb.ConnId(conn)
	require.NoError(t, err)
	require.NotZero(t, id, "connection ID should be non-zero")
}

// TestConnId_NilConnection tests that nil connection returns an error.
func TestConnId_NilConnection(t *testing.T) {
	id, err := dukdb.ConnId(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil")
	require.Zero(t, id, "ID should be 0 for error cases")
}

// TestConnId_ClosedConnection tests that closed connection returns an error.
func TestConnId_ClosedConnection(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	// Close the connection
	err = conn.Close()
	require.NoError(t, err)

	// Try to get ID from closed connection
	id, err := dukdb.ConnId(conn)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "closed")
	require.Zero(t, id, "ID should be 0 for error cases")
}

// TestConnId_Uniqueness tests that connections from different databases have different IDs.
// Note: Connections from the same database may share the same underlying backend connection
// and therefore have the same ID. This test verifies that separate database instances
// have distinct connection IDs.
func TestConnId_Uniqueness(t *testing.T) {
	const numDatabases = 5
	ids := make(map[uint64]bool)

	for i := range numDatabases {
		// Create a new database (and therefore a new backend connection) for each iteration
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)
		require.NotZero(t, id, "database %d should have non-zero connection ID", i)

		require.False(t, ids[id], "duplicate ID found: %d at database %d", id, i)
		ids[id] = true

		_ = conn.Close()
		_ = db.Close()
	}

	require.Equal(t, numDatabases, len(ids), "should have unique IDs for all databases")
}

// TestConnId_Stability tests that the same connection always returns the same ID.
func TestConnId_Stability(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	// Get ID multiple times
	var firstID uint64

	for i := range 100 {
		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)

		if i == 0 {
			firstID = id
		} else {
			require.Equal(t, firstID, id, "ID should be stable across calls")
		}
	}
}

// TestConnId_ErrorReturnsZero verifies that error cases return 0 for ID.
func TestConnId_ErrorReturnsZero(t *testing.T) {
	tests := []struct {
		name string
		conn *sql.Conn
	}{
		{"nil connection", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := dukdb.ConnId(tt.conn)
			require.Error(t, err)
			require.Zero(t, id, "ID should be 0 for error cases")
		})
	}
}

// TestConnId_DifferentDatabases verifies connections to different databases have different IDs.
func TestConnId_DifferentDatabases(t *testing.T) {
	// Open first database
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db1.Close() }()

	conn1, err := db1.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn1.Close() }()

	id1, err := dukdb.ConnId(conn1)
	require.NoError(t, err)

	// Open second database (creates new backend connection)
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db2.Close() }()

	conn2, err := db2.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn2.Close() }()

	id2, err := dukdb.ConnId(conn2)
	require.NoError(t, err)

	require.NotEqual(
		t,
		id1,
		id2,
		"different databases should have different IDs (id1=%d, id2=%d)",
		id1,
		id2,
	)
}

// TestConnId_SameDatabase verifies connections to the same database share the same backend ID.
// This is expected behavior since connections to the same database share the underlying backend.
func TestConnId_SameDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	// Get two connections from the same database
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn1.Close() }()

	conn2, err := db.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn2.Close() }()

	id1, err := dukdb.ConnId(conn1)
	require.NoError(t, err)

	id2, err := dukdb.ConnId(conn2)
	require.NoError(t, err)

	// Both connections share the same backend connection, so they have the same ID
	require.Equal(t, id1, id2, "connections to same database share backend ID")
}

// TestConnId_Uniqueness100 creates 100 connections and verifies all IDs are unique.
// Task 3.2: Create 100 connections, verify all IDs unique.
func TestConnId_Uniqueness100(t *testing.T) {
	const numConnections = 100
	ids := make(map[uint64]bool)

	for i := range numConnections {
		// Create a new database (and therefore a new backend connection) for each iteration
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)
		require.NotZero(t, id, "connection %d should have non-zero ID", i)

		require.False(t, ids[id], "duplicate ID found: %d at connection %d", id, i)
		ids[id] = true

		_ = conn.Close()
		_ = db.Close()
	}

	require.Equal(t, numConnections, len(ids), "should have %d unique IDs", numConnections)
}

// TestConnId_Sequential verifies IDs increment sequentially (1, 2, 3, ...).
// Task 3.11: Verify IDs increment sequentially.
func TestConnId_Sequential(t *testing.T) {
	var prevID uint64

	for i := range 10 {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)

		if i > 0 {
			// IDs should be strictly increasing
			require.Greater(t, id, prevID,
				"ID should be greater than previous (prev=%d, current=%d)", prevID, id)
		}

		prevID = id

		_ = conn.Close()
		_ = db.Close()
	}
}

// TestConnId_AfterReopen closes and reopens a connection, verifying a new ID is assigned.
// Task 3.12: Close and reopen connection, verify new ID assigned.
func TestConnId_AfterReopen(t *testing.T) {
	// Create first database and get its ID
	db1, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	conn1, err := db1.Conn(context.Background())
	require.NoError(t, err)

	id1, err := dukdb.ConnId(conn1)
	require.NoError(t, err)

	// Close the first connection and database
	_ = conn1.Close()
	_ = db1.Close()

	// Create a new database (new backend connection)
	db2, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db2.Close() }()

	conn2, err := db2.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn2.Close() }()

	id2, err := dukdb.ConnId(conn2)
	require.NoError(t, err)

	// The new connection should have a different (higher) ID
	require.NotEqual(t, id1, id2, "new connection after reopen should have different ID")
	require.Greater(t, id2, id1, "new ID should be greater than old ID (old=%d, new=%d)", id1, id2)
}

// TestConnId_Concurrent creates 100 connections concurrently and verifies all IDs are unique.
// Task 3.8: Create 100 connections concurrently, verify all IDs unique.
func TestConnId_Concurrent(t *testing.T) {
	const numConnections = 100

	var wg sync.WaitGroup

	var mu sync.Mutex

	ids := make(map[uint64]bool)
	errors := make([]error, 0)

	wg.Add(numConnections)

	for i := range numConnections {
		go func(idx int) {
			defer wg.Done()

			db, err := sql.Open("dukdb", ":memory:")
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()

				return
			}

			defer func() { _ = db.Close() }()

			conn, err := db.Conn(context.Background())
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()

				return
			}

			defer func() { _ = conn.Close() }()

			id, err := dukdb.ConnId(conn)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()

				return
			}

			mu.Lock()
			if ids[id] {
				t.Errorf("duplicate ID found: %d at goroutine %d", id, idx)
			}

			ids[id] = true
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	require.Empty(t, errors, "should have no errors during concurrent connection creation")
	require.Equal(t, numConnections, len(ids), "should have %d unique IDs", numConnections)
}

// TestConnId_ConcurrentCalls calls ConnId() on the same connection from 10 goroutines.
// Task 3.9: Call ConnId() on same connection from 10 goroutines, verify same ID.
func TestConnId_ConcurrentCalls(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	// Get the expected ID first
	expectedID, err := dukdb.ConnId(conn)
	require.NoError(t, err)

	const numGoroutines = 10
	const callsPerGoroutine = 10

	var wg sync.WaitGroup

	var mu sync.Mutex

	errors := make([]error, 0)
	allIDs := make([]uint64, 0, numGoroutines*callsPerGoroutine)

	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()

			for range callsPerGoroutine {
				id, err := dukdb.ConnId(conn)
				mu.Lock()

				if err != nil {
					errors = append(errors, err)
				} else {
					allIDs = append(allIDs, id)
				}

				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	require.Empty(t, errors, "should have no errors during concurrent calls")
	require.Len(t, allIDs, numGoroutines*callsPerGoroutine, "should have all IDs")

	// All IDs should be the same
	for i, id := range allIDs {
		require.Equal(t, expectedID, id, "ID at index %d should equal expected ID", i)
	}
}
