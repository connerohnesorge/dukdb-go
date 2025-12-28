package engine

import (
	"context"
	"sync"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
)

func TestEngineOpen(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	if conn == nil {
		t.Error("Connection should not be nil")
	}
}

func TestEnginePing(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	err = conn.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestEngineCreateAndQuery(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	affected, err := conn.Execute(
		context.Background(),
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if affected != 1 {
		t.Errorf(
			"Expected 1 row affected, got %d",
			affected,
		)
	}

	// Query data
	rows, columns, err := conn.Query(
		context.Background(),
		"SELECT * FROM users",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf(
			"Expected 1 row, got %d",
			len(rows),
		)
	}

	if len(columns) != 2 {
		t.Errorf(
			"Expected 2 columns, got %d",
			len(columns),
		)
	}
}

func TestEngineMultipleOperations(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows
	inserts := []string{
		"INSERT INTO products (id, name, price) VALUES (1, 'Apple', 1.50)",
		"INSERT INTO products (id, name, price) VALUES (2, 'Banana', 0.75)",
		"INSERT INTO products (id, name, price) VALUES (3, 'Orange', 2.00)",
	}

	for _, sql := range inserts {
		_, err = conn.Execute(
			context.Background(),
			sql,
			nil,
		)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query with filter
	rows, _, err := conn.Query(
		context.Background(),
		"SELECT * FROM products WHERE price > 1.0",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf(
			"Expected 2 rows where price > 1.0, got %d",
			len(rows),
		)
	}

	// Query with order
	rows, _, err = conn.Query(
		context.Background(),
		"SELECT * FROM products ORDER BY price",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf(
			"Expected 3 rows, got %d",
			len(rows),
		)
	}

	// Query with limit
	rows, _, err = conn.Query(
		context.Background(),
		"SELECT * FROM products LIMIT 2",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf(
			"Expected 2 rows with LIMIT, got %d",
			len(rows),
		)
	}

	// Drop table
	_, err = conn.Execute(
		context.Background(),
		"DROP TABLE products",
		nil,
	)
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
}

func TestEnginePreparedStatement(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Prepare statement
	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	if stmt.NumInput() != 0 {
		t.Errorf(
			"Expected 0 parameters, got %d",
			stmt.NumInput(),
		)
	}
}

func TestEngineCloseConnection(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations on closed connection should fail
	err = conn.Ping(context.Background())
	if err != dukdb.ErrConnectionClosed {
		t.Errorf(
			"Expected ErrConnectionClosed, got %v",
			err,
		)
	}
}

func TestEngineConcurrentQueries(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE counters (id INTEGER, val INTEGER)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert initial data
	_, err = conn.Execute(
		context.Background(),
		"INSERT INTO counters (id, val) VALUES (1, 0)",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Run concurrent queries
	var wg sync.WaitGroup
	numGoroutines := 10
	numQueries := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numQueries; j++ {
				_, _, err := conn.Query(
					context.Background(),
					"SELECT * FROM counters",
					nil,
				)
				if err != nil {
					t.Errorf(
						"Concurrent query failed: %v",
						err,
					)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestEngineMultipleConnections(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	// Open multiple connections
	var conns []dukdb.BackendConn
	for i := 0; i < 5; i++ {
		conn, err := engine.Open(":memory:", nil)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		conns = append(conns, conn)
	}

	// Close all connections
	for _, conn := range conns {
		err := conn.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}
}

func TestEngineConfig(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	config := &dukdb.Config{
		AccessMode: "read_write",
		Threads:    4,
		MaxMemory:  "1GB",
	}

	conn, err := engine.Open(":memory:", config)
	if err != nil {
		t.Fatalf(
			"Open with config failed: %v",
			err,
		)
	}
	defer conn.Close()

	// Verify connection works
	err = conn.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestEngineExpressionQuery(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Query without table
	rows, _, err := conn.Query(
		context.Background(),
		"SELECT 1 + 2",
		nil,
	)
	if err != nil {
		t.Fatalf(
			"SELECT expression failed: %v",
			err,
		)
	}

	if len(rows) != 1 {
		t.Errorf(
			"Expected 1 row, got %d",
			len(rows),
		)
	}
}

func TestTransactionManager(t *testing.T) {
	tm := NewTransactionManager()

	// Begin transaction
	txn := tm.Begin()
	if !txn.IsActive() {
		t.Error(
			"Transaction should be active after Begin",
		)
	}

	// Commit transaction
	err := tm.Commit(txn)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	if txn.IsActive() {
		t.Error(
			"Transaction should not be active after Commit",
		)
	}

	// Commit again should fail
	err = tm.Commit(txn)
	if err != dukdb.ErrTransactionAlreadyEnded {
		t.Errorf(
			"Expected ErrTransactionAlreadyEnded, got %v",
			err,
		)
	}
}

func TestTransactionRollback(t *testing.T) {
	tm := NewTransactionManager()

	// Begin transaction
	txn := tm.Begin()

	// Rollback transaction
	err := tm.Rollback(txn)
	if err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	if txn.IsActive() {
		t.Error(
			"Transaction should not be active after Rollback",
		)
	}
}

func TestBackendInterface(t *testing.T) {
	// Verify that Engine implements Backend interface
	var _ dukdb.Backend = (*Engine)(nil)

	// Verify that EngineConn implements BackendConn interface
	var _ dukdb.BackendConn = (*EngineConn)(nil)

	// Verify that EngineStmt implements BackendStmt interface
	var _ dukdb.BackendStmt = (*EngineStmt)(nil)
}
