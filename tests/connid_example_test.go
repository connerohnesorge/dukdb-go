// Package tests provides integration tests and examples for dukdb-go.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// Example_connId demonstrates basic usage of the ConnId function
// to get a unique identifier for a database connection.
func Example_connId() {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	id, err := dukdb.ConnId(conn)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Connection ID is non-zero: %v\n", id > 0)
	// Output: Connection ID is non-zero: true
}

// Example_connId_stability demonstrates that ConnId returns the same
// ID for the same connection across multiple calls.
func Example_connId_stability() {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Get ID multiple times
	id1, _ := dukdb.ConnId(conn)
	id2, _ := dukdb.ConnId(conn)
	id3, _ := dukdb.ConnId(conn)

	fmt.Printf("ID is stable: %v\n", id1 == id2 && id2 == id3)
	// Output: ID is stable: true
}

// Example_connId_uniqueness demonstrates that different databases
// get different connection IDs.
func Example_connId_uniqueness() {
	db1, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db1.Close() }()
	conn1, _ := db1.Conn(context.Background())
	defer func() { _ = conn1.Close() }()

	db2, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db2.Close() }()
	conn2, _ := db2.Conn(context.Background())
	defer func() { _ = conn2.Close() }()

	id1, _ := dukdb.ConnId(conn1)
	id2, _ := dukdb.ConnId(conn2)

	fmt.Printf("IDs are different: %v\n", id1 != id2)
	// Output: IDs are different: true
}

// Example_connId_poolTracking demonstrates using connection IDs
// to track connections in a pool for monitoring purposes.
func Example_connId_poolTracking() {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = db.Close() }()

	// Simulated connection pool tracker
	activeConns := make(map[uint64]struct{})
	var mu sync.Mutex

	// Acquire a connection and track it
	conn, _ := db.Conn(context.Background())
	id, _ := dukdb.ConnId(conn)

	mu.Lock()
	activeConns[id] = struct{}{}
	mu.Unlock()

	fmt.Printf("Connection tracked: %v\n", len(activeConns) == 1)

	// Release the connection
	mu.Lock()
	delete(activeConns, id)
	mu.Unlock()
	_ = conn.Close()

	fmt.Printf("Connection released: %v\n", len(activeConns) == 0)
	// Output:
	// Connection tracked: true
	// Connection released: true
}

// Example_connId_queryTracing demonstrates using connection IDs
// for query tracing and debugging.
func Example_connId_queryTracing() {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	id, _ := dukdb.ConnId(conn)

	// Simulated query trace log (ID printed as non-zero to be test-order independent)
	traceQuery := func(connID uint64, query string) {
		fmt.Printf("[conn:non-zero=%v] Query: %s\n", connID > 0, query)
	}

	// Execute with tracing
	traceQuery(id, "SELECT 1")
	_, _ = conn.ExecContext(context.Background(), "SELECT 1")

	fmt.Println("Query traced successfully")
	// Output:
	// [conn:non-zero=true] Query: SELECT 1
	// Query traced successfully
}

// Example_connId_errorHandling demonstrates proper error handling
// for ConnId with nil and closed connections.
func Example_connId_errorHandling() {
	// Nil connection
	_, err := dukdb.ConnId(nil)
	fmt.Printf("Nil connection error: %v\n", err != nil)

	// Closed connection
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()
	conn, _ := db.Conn(context.Background())
	_ = conn.Close()

	_, err = dukdb.ConnId(conn)
	fmt.Printf("Closed connection error: %v\n", err != nil)
	// Output:
	// Nil connection error: true
	// Closed connection error: true
}
