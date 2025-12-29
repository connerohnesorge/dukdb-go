package dukdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
)

// Connector implements the driver.Connector interface.
// It holds the database configuration and can create new connections.
// The backend is initialized lazily on the first Connect call using sync.Once.
type Connector struct {
	config           *Config
	driver           *Driver
	backend          Backend
	backendConn      BackendConn
	connInitFn       func(driver.ExecerContext) error
	once             sync.Once
	initErr          error
	closed           bool
	mu               sync.RWMutex
	replacementScans *replacementScanRegistry
}

// NewConnector creates a new Connector for a DuckDB database.
// The dsn parameter specifies the database location (":memory:" for in-memory,
// or a file path for persistent storage).
// The connInitFn callback is called after each connection is established
// to perform any additional initialization.
func NewConnector(
	dsn string,
	connInitFn func(driver.ExecerContext) error,
) (*Connector, error) {
	config, err := ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %v",
			errParseDSN,
			err,
		)
	}

	return &Connector{
		config:     config,
		driver:     &Driver{},
		connInitFn: connInitFn,
	}, nil
}

// NewConnectorWithConfig creates a new Connector with the given configuration.
// This allows for direct configuration without DSN parsing.
func NewConnectorWithConfig(
	config *Config,
	connInitFn func(driver.ExecerContext) error,
) (*Connector, error) {
	if config == nil {
		config = NewConfig()
		config.Path = ":memory:"
	}

	return &Connector{
		config:     config,
		driver:     &Driver{},
		connInitFn: connInitFn,
	}, nil
}

// Driver returns the underlying Driver of the Connector.
// Implements driver.Connector.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Connect returns a new connection to the database.
// The backend is initialized lazily on the first call.
// The context parameter can be used for connection timeout.
// Implements driver.Connector.
func (c *Connector) Connect(
	ctx context.Context,
) (driver.Conn, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errClosedCon
	}
	c.mu.RUnlock()

	// Lazy initialization of backend
	c.once.Do(func() {
		c.backend = GetBackend()
		if c.backend == nil {
			c.initErr = errNoBackend
			return
		}

		var err error
		c.backendConn, err = c.backend.Open(
			c.config.Path,
			c.config,
		)
		if err != nil {
			c.initErr = fmt.Errorf(
				"%w: %v",
				errConnect,
				err,
			)
			return
		}
	})

	if c.initErr != nil {
		return nil, c.initErr
	}

	// Check context before creating connection
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	conn := &Conn{
		connector:   c,
		backendConn: c.backendConn,
	}

	if c.connInitFn != nil {
		if err := c.connInitFn(conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

// Close closes the Connector and releases any associated resources.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.backendConn != nil {
		return c.backendConn.Close()
	}
	return nil
}

// Config returns the configuration used by this connector.
func (c *Connector) Config() *Config {
	return c.config
}

// Ensure Connector implements driver.Connector.
var _ driver.Connector = (*Connector)(nil)
