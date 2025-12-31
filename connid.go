package dukdb

import (
	"database/sql"
	"errors"
)

// ConnId returns the unique connection ID for a database connection.
//
// Each connection is assigned a unique, monotonically increasing ID when
// created. This ID remains stable throughout the connection's lifetime and
// is never reused within a single process.
//
// # Use Cases
//
// The ID is useful for:
//   - Debugging and tracing queries to specific connections
//   - Connection pool management and monitoring
//   - Correlating connection-specific operations
//   - Query tracing with connection attribution
//   - Multi-tenant connection isolation
//
// # Thread Safety
//
// ConnId is safe to call concurrently from multiple goroutines. The
// underlying ID is a uint64 stored in the backend connection and is
// assigned atomically during connection creation.
//
// # ID Space and Wraparound
//
// Connection IDs are 64-bit unsigned integers starting at 1. With a uint64,
// wraparound would only occur after 18 quintillion (2^64) connections,
// which is not a practical concern for any real-world application.
// IDs increment monotonically and are never reused within a process lifetime.
//
// # Errors
//
// Returns an error if:
//   - The connection is nil ("connection is nil")
//   - The connection has been closed ("closed connection")
//   - The connection is not a dukdb connection ("not a DukDB driver connection")
//   - The backend does not support identification ("backend does not support connection identification")
//
// On error, the returned ID is always 0 (an invalid ID value, since IDs start at 1).
//
// # Example
//
//	db, _ := sql.Open("dukdb", ":memory:")
//	defer db.Close()
//
//	conn, _ := db.Conn(context.Background())
//	defer conn.Close()
//
//	id, err := dukdb.ConnId(conn)
//	if err != nil {
//	    log.Fatalf("failed to get connection ID: %v", err)
//	}
//	fmt.Printf("Connection ID: %d\n", id)
//
// # Example: Connection Pool Tracking
//
//	// Track active connections in a pool
//	activeConns := make(map[uint64]*sql.Conn)
//	var mu sync.Mutex
//
//	conn, _ := db.Conn(ctx)
//	id, _ := dukdb.ConnId(conn)
//
//	mu.Lock()
//	activeConns[id] = conn
//	mu.Unlock()
//
// # Example: Query Tracing
//
//	func execWithTrace(conn *sql.Conn, query string) error {
//	    id, _ := dukdb.ConnId(conn)
//	    log.Printf("[conn:%d] executing: %s", id, query)
//	    _, err := conn.ExecContext(context.Background(), query)
//	    log.Printf("[conn:%d] completed (err=%v)", id, err)
//	    return err
//	}
func ConnId(c *sql.Conn) (uint64, error) {
	if c == nil {
		return 0, errors.New("connection is nil")
	}

	var id uint64
	err := c.Raw(func(driverConn any) error {
		// Type assert to our driver's *Conn
		conn, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("not a DukDB driver connection")
		}

		// Check if connection is closed
		if conn.closed {
			return errClosedCon
		}

		// Get the backend connection and check if it supports identification
		identifiable, ok := conn.backendConn.(BackendConnIdentifiable)
		if !ok {
			return errors.New("backend does not support connection identification")
		}

		// Check if backend reports connection as closed
		if identifiable.IsClosed() {
			return errClosedCon
		}

		id = identifiable.ID()

		return nil
	})

	return id, err
}
