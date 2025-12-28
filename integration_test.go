package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"time"
)

// testMockBackend implements Backend for testing purposes.
// Named with test prefix to avoid conflict with tx_test.go
type testMockBackend struct {
	openCalled  int
	closeCalled int
	mu          sync.Mutex
}

func (m *testMockBackend) Open(
	path string,
	config *Config,
) (BackendConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openCalled++
	return &testMockBackendConn{}, nil
}

func (m *testMockBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled++
	return nil
}

// testMockBackendConn implements BackendConn for testing purposes.
// Named with test prefix to avoid conflict with tx_test.go
type testMockBackendConn struct {
	executeCalled int
	queryCalled   int
	pingCalled    int
	closed        bool
	mu            sync.Mutex
}

func (m *testMockBackendConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executeCalled++
	return 0, nil
}

func (m *testMockBackendConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryCalled++

	// Return mock data for SELECT 1
	if query == "SELECT 1" {
		return []map[string]any{
				{"1": int64(1)},
			}, []string{
				"1",
			}, nil
	}
	return nil, nil, nil
}

func (m *testMockBackendConn) Prepare(
	ctx context.Context,
	query string,
) (BackendStmt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &testMockBackendStmt{query: query}, nil
}

func (m *testMockBackendConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *testMockBackendConn) Ping(
	ctx context.Context,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pingCalled++
	return nil
}

// testMockBackendStmt implements BackendStmt for testing purposes.
// Named with test prefix to avoid conflict with tx_test.go
type testMockBackendStmt struct {
	query  string
	closed bool
}

func (m *testMockBackendStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	return 0, nil
}

func (m *testMockBackendStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	return nil, nil, nil
}

func (m *testMockBackendStmt) Close() error {
	m.closed = true
	return nil
}

func (m *testMockBackendStmt) NumInput() int {
	return 0
}

// setupTestMockBackend registers a mock backend for testing.
func setupTestMockBackend() *testMockBackend {
	backend := &testMockBackend{}
	RegisterBackend(backend)
	return backend
}

func TestDriverOpen(t *testing.T) {
	setupTestMockBackend()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Driver.Open failed: %v", err)
	}
	defer conn.Close()

	// Verify connection works
	if conn == nil {
		t.Error(
			"Driver.Open returned nil connection",
		)
	}
}

func TestDriverOpenConnector(t *testing.T) {
	setupTestMockBackend()

	d := &Driver{}
	connector, err := d.OpenConnector(":memory:")
	if err != nil {
		t.Fatalf(
			"Driver.OpenConnector failed: %v",
			err,
		)
	}

	// Verify connector works
	if connector == nil {
		t.Error(
			"Driver.OpenConnector returned nil connector",
		)
	}

	// Verify Driver() method
	if connector.Driver() == nil {
		t.Error("Connector.Driver() returned nil")
	}
}

func TestConnectorConnect(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	defer conn.Close()

	if conn == nil {
		t.Error(
			"Connector.Connect returned nil connection",
		)
	}
}

func TestConnectorLazyInit(t *testing.T) {
	backend := setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	// Backend should not be opened yet
	backend.mu.Lock()
	if backend.openCalled != 0 {
		t.Errorf(
			"Backend opened before Connect: openCalled = %d",
			backend.openCalled,
		)
	}
	backend.mu.Unlock()

	// First Connect should open backend
	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	conn.Close()

	backend.mu.Lock()
	if backend.openCalled != 1 {
		t.Errorf(
			"Backend open count after first Connect: got %d, want 1",
			backend.openCalled,
		)
	}
	backend.mu.Unlock()

	// Second Connect should not open backend again
	conn2, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect (2nd) failed: %v",
			err,
		)
	}
	conn2.Close()

	backend.mu.Lock()
	if backend.openCalled != 1 {
		t.Errorf(
			"Backend open count after second Connect: got %d, want 1",
			backend.openCalled,
		)
	}
	backend.mu.Unlock()
}

func TestConnectorContextCancellation(
	t *testing.T,
) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	// Initialize the backend first
	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	conn.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	// Connect with cancelled context should fail
	_, err = connector.Connect(ctx)
	if err != context.Canceled {
		t.Errorf(
			"Connector.Connect with cancelled context: got %v, want context.Canceled",
			err,
		)
	}
}

func TestConnectorClose(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}

	// Initialize by connecting
	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	conn.Close()

	// Close connector
	if err := connector.Close(); err != nil {
		t.Errorf(
			"Connector.Close failed: %v",
			err,
		)
	}

	// Connect after close should fail
	_, err = connector.Connect(
		context.Background(),
	)
	if err != errClosedCon {
		t.Errorf(
			"Connector.Connect after close: got %v, want errClosedCon",
			err,
		)
	}

	// Close again should be idempotent
	if err := connector.Close(); err != nil {
		t.Errorf(
			"Connector.Close (2nd) failed: %v",
			err,
		)
	}
}

func TestConnPingIntegration(t *testing.T) {
	backend := setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	defer conn.Close()

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatal(
			"Connection does not implement driver.Pinger",
		)
	}

	if err := pinger.Ping(context.Background()); err != nil {
		t.Errorf("Conn.Ping failed: %v", err)
	}

	// Get the mock backend connection to verify ping was called
	mockConn := connector.backendConn.(*testMockBackendConn)
	mockConn.mu.Lock()
	if mockConn.pingCalled < 1 {
		t.Error("Backend Ping was not called")
	}
	mockConn.mu.Unlock()

	// Suppress unused variable warning for backend
	_ = backend
}

func TestConnResetSession(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	defer conn.Close()

	resetter, ok := conn.(driver.SessionResetter)
	if !ok {
		t.Fatal(
			"Connection does not implement driver.SessionResetter",
		)
	}

	if err := resetter.ResetSession(context.Background()); err != nil {
		t.Errorf(
			"Conn.ResetSession failed: %v",
			err,
		)
	}
}

func TestConnResetSessionWithTransaction(
	t *testing.T,
) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Start a transaction
	_, err = c.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if !c.tx {
		t.Error(
			"Transaction flag not set after BeginTx",
		)
	}

	// Reset session should rollback the transaction
	if err := c.ResetSession(context.Background()); err != nil {
		t.Errorf("ResetSession failed: %v", err)
	}

	if c.tx {
		t.Error(
			"Transaction flag still set after ResetSession",
		)
	}
}

func TestConnIsValid(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}

	validator, ok := conn.(driver.Validator)
	if !ok {
		t.Fatal(
			"Connection does not implement driver.Validator",
		)
	}

	// Should be valid initially
	if !validator.IsValid() {
		t.Error(
			"Connection should be valid initially",
		)
	}

	// Close connection
	conn.Close()

	// Should be invalid after close
	if validator.IsValid() {
		t.Error(
			"Connection should be invalid after close",
		)
	}
}

func TestConnInitCallback(t *testing.T) {
	setupTestMockBackend()

	initCalled := false
	initFn := func(conn driver.ExecerContext) error {
		initCalled = true
		return nil
	}

	connector, err := NewConnector(
		":memory:",
		initFn,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}
	defer conn.Close()

	if !initCalled {
		t.Error(
			"Connection init callback was not called",
		)
	}
}

func TestDriverThreadSafety(t *testing.T) {
	setupTestMockBackend()

	d := &Driver{}
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Open multiple connections concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := d.Open(":memory:")
			if err != nil {
				errors <- err
				return
			}
			conn.Close()
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf(
			"Concurrent Open failed: %v",
			err,
		)
	}
}

func TestConnectorThreadSafety(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Connect multiple times concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := connector.Connect(
				context.Background(),
			)
			if err != nil {
				errors <- err
				return
			}
			conn.Close()
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf(
			"Concurrent Connect failed: %v",
			err,
		)
	}
}

func TestConnectionPooling(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	// Configure pool
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute)

	// Use connections
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			defer cancel()

			if err := db.PingContext(ctx); err != nil {
				t.Errorf("Ping failed: %v", err)
			}
		}()
	}

	wg.Wait()

	// Verify pool stats
	stats := db.Stats()
	if stats.MaxOpenConnections != 5 {
		t.Errorf(
			"MaxOpenConnections = %d, want 5",
			stats.MaxOpenConnections,
		)
	}
}

func TestHealthCheckWithPool(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	// Ping to establish connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Close and reopen to test health checks
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNewConnectorWithConfig(t *testing.T) {
	setupTestMockBackend()

	config := &Config{
		Path:       "/test/path.db",
		AccessMode: "read_only",
		Threads:    4,
		MaxMemory:  "2GB",
	}

	connector, err := NewConnectorWithConfig(
		config,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"NewConnectorWithConfig failed: %v",
			err,
		)
	}
	defer connector.Close()

	if connector.Config().Path != config.Path {
		t.Errorf(
			"Config.Path = %q, want %q",
			connector.Config().Path,
			config.Path,
		)
	}
	if connector.Config().AccessMode != config.AccessMode {
		t.Errorf(
			"Config.AccessMode = %q, want %q",
			connector.Config().AccessMode,
			config.AccessMode,
		)
	}
}

func TestNewConnectorWithNilConfig(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnectorWithConfig(
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"NewConnectorWithConfig(nil) failed: %v",
			err,
		)
	}
	defer connector.Close()

	// Should use default config with :memory:
	if connector.Config().Path != ":memory:" {
		t.Errorf(
			"Config.Path = %q, want \":memory:\"",
			connector.Config().Path,
		)
	}
}

func TestConnPingAfterClose(t *testing.T) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}

	// Close the connection
	conn.Close()

	// Ping should fail
	pinger := conn.(driver.Pinger)
	if err := pinger.Ping(context.Background()); err != errClosedCon {
		t.Errorf(
			"Ping after close: got %v, want errClosedCon",
			err,
		)
	}
}

func TestConnResetSessionAfterClose(
	t *testing.T,
) {
	setupTestMockBackend()

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Connector.Connect failed: %v",
			err,
		)
	}

	// Close the connection
	conn.Close()

	// ResetSession should fail
	resetter := conn.(driver.SessionResetter)
	if err := resetter.ResetSession(context.Background()); err != errClosedCon {
		t.Errorf(
			"ResetSession after close: got %v, want errClosedCon",
			err,
		)
	}
}

func TestInterfaceCompliance(t *testing.T) {
	// Verify Driver implements required interfaces
	var _ driver.Driver = (*Driver)(nil)
	var _ driver.DriverContext = (*Driver)(nil)

	// Verify Connector implements required interface
	var _ driver.Connector = (*Connector)(nil)

	// Verify Conn implements required interfaces
	var _ driver.Conn = (*Conn)(nil)
	var _ driver.Pinger = (*Conn)(nil)
	var _ driver.SessionResetter = (*Conn)(nil)
	var _ driver.Validator = (*Conn)(nil)
}
