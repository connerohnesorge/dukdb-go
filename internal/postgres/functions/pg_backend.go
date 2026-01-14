package functions

import (
	"os"
	"sync/atomic"
)

// connectionCounter is used to generate unique connection IDs.
var connectionCounter uint64

// PgBackendPid returns the backend process ID.
// In dukdb-go, we return a unique connection ID.
type PgBackendPid struct {
	ConnectionID uint64
}

// NewPgBackendPid creates a new PgBackendPid function with a unique connection ID.
func NewPgBackendPid() *PgBackendPid {
	return &PgBackendPid{
		ConnectionID: atomic.AddUint64(&connectionCounter, 1),
	}
}

// Evaluate returns the backend process ID (connection ID).
func (f *PgBackendPid) Evaluate() int {
	// Use process ID combined with connection counter for uniqueness
	return int(uint64(os.Getpid())<<32 | f.ConnectionID)
}

// PgCancelBackend attempts to cancel a backend process.
// In dukdb-go, this is a no-op that always returns false.
type PgCancelBackend struct{}

// Evaluate always returns false (cancel not supported).
func (*PgCancelBackend) Evaluate(pid int) bool {
	_ = pid // unused

	return false
}

// PgTerminateBackend attempts to terminate a backend process.
// In dukdb-go, this is a no-op that always returns false.
type PgTerminateBackend struct{}

// Evaluate always returns false (terminate not supported).
func (*PgTerminateBackend) Evaluate(pid int) bool {
	_ = pid // unused

	return false
}

// InetClientAddr returns the client IP address.
// In dukdb-go, returns nil/empty as we don't track this.
type InetClientAddr struct {
	ClientAddr string
}

// Evaluate returns the client IP address.
func (f *InetClientAddr) Evaluate() *string {
	if f.ClientAddr == "" {
		return nil
	}

	return &f.ClientAddr
}

// InetClientPort returns the client port.
type InetClientPort struct {
	ClientPort int
}

// Evaluate returns the client port.
func (f *InetClientPort) Evaluate() *int {
	if f.ClientPort == 0 {
		return nil
	}

	return &f.ClientPort
}

// InetServerAddr returns the server IP address.
type InetServerAddr struct {
	ServerAddr string
}

// Evaluate returns the server IP address.
func (f *InetServerAddr) Evaluate() *string {
	if f.ServerAddr == "" {
		return nil
	}

	return &f.ServerAddr
}

// InetServerPort returns the server port.
type InetServerPort struct {
	ServerPort int
}

// Evaluate returns the server port.
func (f *InetServerPort) Evaluate() *int {
	if f.ServerPort == 0 {
		return nil
	}

	return &f.ServerPort
}
