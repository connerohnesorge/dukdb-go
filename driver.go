package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("dukdb", &Driver{})
}

// Driver implements the database/sql/driver.Driver and driver.DriverContext interfaces.
// It is stateless and safe for concurrent use.
type Driver struct{}

// Open returns a new connection to the database.
// The name is a connection string (DSN), e.g., ":memory:" for an in-memory database
// or a file path for a persistent database.
//
// DSN format: path?option=value&option2=value2
// Supported options:
//   - access_mode: "automatic", "read_only", "read_write" (default: "automatic")
//   - threads: number of threads, 1-128 (default: runtime.NumCPU())
//   - max_memory: memory limit, e.g., "4GB", "1024MB", "80%" (default: "80%")
//
// Implements driver.Driver.
func (d *Driver) Open(
	dsn string,
) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

// OpenConnector returns a new Connector for the given DSN.
// The Connector can be used with sql.OpenDB for connection pooling.
// Implements driver.DriverContext.
func (d *Driver) OpenConnector(
	dsn string,
) (driver.Connector, error) {
	return NewConnector(dsn, nil)
}

// Ensure Driver implements the required interfaces.
var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)
