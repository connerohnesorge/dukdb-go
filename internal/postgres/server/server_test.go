package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/postgres/server/auth"

	// Import engine package to register the backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func TestNewConfig(t *testing.T) {
	config := NewConfig()

	assert.Equal(t, DefaultHost, config.Host)
	assert.Equal(t, DefaultPort, config.Port)
	assert.Equal(t, DefaultMaxConnections, config.MaxConnections)
	assert.Equal(t, DefaultShutdownTimeout, config.ShutdownTimeout)
	assert.Equal(t, DefaultServerVersion, config.ServerVersion)
	assert.Equal(t, "dukdb", config.Database)
	assert.False(t, config.RequireAuth)
}

func TestConfigAddress(t *testing.T) {
	config := NewConfig()
	assert.Equal(t, "127.0.0.1:5432", config.Address())

	config.Host = "0.0.0.0"
	config.Port = 15432
	assert.Equal(t, "0.0.0.0:15432", config.Address())
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid default config",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "invalid port zero",
			modify:  func(c *Config) { c.Port = 0 },
			wantErr: true,
			errMsg:  "Port",
		},
		{
			name:    "invalid port too high",
			modify:  func(c *Config) { c.Port = 70000 },
			wantErr: true,
			errMsg:  "Port",
		},
		{
			name:    "invalid max connections",
			modify:  func(c *Config) { c.MaxConnections = 0 },
			wantErr: true,
			errMsg:  "MaxConnections",
		},
		{
			name:    "negative shutdown timeout",
			modify:  func(c *Config) { c.ShutdownTimeout = -1 },
			wantErr: true,
			errMsg:  "ShutdownTimeout",
		},
		{
			name: "auth enabled without username",
			modify: func(c *Config) {
				c.RequireAuth = true
				c.Username = ""
			},
			wantErr: true,
			errMsg:  "Username",
		},
		{
			name: "auth enabled with username",
			modify: func(c *Config) {
				c.RequireAuth = true
				c.Username = "testuser"
				c.Password = "testpass"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConfig()
			tt.modify(config)

			err := config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewServer(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		server, err := NewServer(nil)
		require.NoError(t, err)
		require.NotNil(t, server)

		assert.Equal(t, DefaultHost, server.Config().Host)
		assert.Equal(t, DefaultPort, server.Config().Port)
	})

	t.Run("with custom config", func(t *testing.T) {
		config := NewConfig()
		config.Host = "0.0.0.0"
		config.Port = 15432
		config.Database = "testdb"

		server, err := NewServer(config)
		require.NoError(t, err)
		require.NotNil(t, server)

		assert.Equal(t, "0.0.0.0", server.Config().Host)
		assert.Equal(t, 15432, server.Config().Port)
		assert.Equal(t, "testdb", server.Config().Database)
	})

	t.Run("with invalid config returns error", func(t *testing.T) {
		config := NewConfig()
		config.Port = 0 // Invalid

		server, err := NewServer(config)
		require.Error(t, err)
		assert.Nil(t, server)
	})
}

func TestServerStartStop(t *testing.T) {
	config := NewConfig()
	config.Port = 15432 // Use non-standard port to avoid conflicts

	server, err := NewServer(config)
	require.NoError(t, err)

	// Server should not be running initially
	assert.False(t, server.IsRunning())

	// Start the server
	err = server.Start()
	require.NoError(t, err)
	assert.True(t, server.started.Load())

	// Starting again should return error
	err = server.Start()
	assert.Equal(t, ErrAlreadyStarted, err)

	// Stop the server
	err = server.Stop()
	require.NoError(t, err)
	assert.False(t, server.IsRunning())
}

func TestServerServe(t *testing.T) {
	config := NewConfig()

	server, err := NewServer(config)
	require.NoError(t, err)

	// Create a listener on an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	// Start serving in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(listener)
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	// Server should be running
	assert.True(t, server.IsRunning())

	// Test that we can connect to the server
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err == nil {
		_ = conn.Close()
	}

	// Stop the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	require.NoError(t, err)

	// Server should not be running
	assert.False(t, server.IsRunning())
}

func TestServerConnectionCount(t *testing.T) {
	config := NewConfig()

	server, err := NewServer(config)
	require.NoError(t, err)

	// Connection count should start at 0
	assert.Equal(t, int64(0), server.ConnectionCount())
}

func TestSession(t *testing.T) {
	config := NewConfig()

	server, err := NewServer(config)
	require.NoError(t, err)

	// Create a session
	session := NewSession(server, "testuser", "testdb", "127.0.0.1:12345")
	require.NotNil(t, session)

	assert.NotEqual(t, uint64(0), session.ID())
	assert.Equal(t, "testuser", session.Username())
	assert.Equal(t, "testdb", session.Database())
	assert.Equal(t, "127.0.0.1:12345", session.RemoteAddr())
	assert.False(t, session.IsClosed())

	// Test attributes
	session.SetAttribute("key1", "value1")
	val, ok := session.GetAttribute("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	session.DeleteAttribute("key1")
	_, ok = session.GetAttribute("key1")
	assert.False(t, ok)

	// Test transaction state
	assert.False(t, session.InTransaction())
	session.SetInTransaction(true)
	assert.True(t, session.InTransaction())

	// Close session
	err = session.Close()
	require.NoError(t, err)
	assert.True(t, session.IsClosed())
}

func TestSessionContext(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	session := NewSession(server, "user", "db", "addr")
	ctx := context.Background()

	// Session should not be in context initially
	_, ok := SessionFromContext(ctx)
	assert.False(t, ok)

	// Add session to context
	ctx = ContextWithSession(ctx, session)

	// Session should be retrievable
	retrievedSession, ok := SessionFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, session.ID(), retrievedSession.ID())
}

func TestItoa(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{123, "123"},
		{5432, "5432"},
		{65535, "65535"},
		{-1, "-1"},
		{-123, "-123"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := itoa(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItoa64(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{123, "123"},
		{1000000, "1000000"},
		{-1, "-1"},
		{-123, "-123"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := itoa64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHandler(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	handler := NewHandler(server)
	require.NotNil(t, handler)
	assert.Equal(t, server, handler.server)
}

func TestGetCommandTag(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	handler := NewHandler(server)

	tests := []struct {
		query        string
		rowsAffected int64
		expected     string
	}{
		{"INSERT INTO test VALUES (1)", 1, "INSERT 0 1"},
		{"INSERT INTO test VALUES (1), (2), (3)", 3, "INSERT 0 3"},
		{"UPDATE test SET a = 1", 5, "UPDATE 5"},
		{"DELETE FROM test", 10, "DELETE 10"},
		{"CREATE TABLE test (id INT)", 0, "CREATE TABLE"},
		{"CREATE INDEX idx ON test(id)", 0, "CREATE INDEX"},
		{"CREATE VIEW v AS SELECT 1", 0, "CREATE VIEW"},
		{"CREATE SCHEMA s", 0, "CREATE SCHEMA"},
		{"CREATE SEQUENCE seq", 0, "CREATE SEQUENCE"},
		{"DROP TABLE test", 0, "DROP TABLE"},
		{"DROP INDEX idx", 0, "DROP INDEX"},
		{"DROP VIEW v", 0, "DROP VIEW"},
		{"DROP SCHEMA s", 0, "DROP SCHEMA"},
		{"DROP SEQUENCE seq", 0, "DROP SEQUENCE"},
		{"ALTER TABLE test ADD COLUMN x INT", 0, "ALTER TABLE"},
		{"BEGIN", 0, "BEGIN"},
		{"COMMIT", 0, "COMMIT"},
		{"ROLLBACK", 0, "ROLLBACK"},
		{"SET search_path = public", 0, "SET"},
		{"COPY test FROM '/tmp/data.csv'", 100, "COPY 100"},
		{"UNKNOWN COMMAND", 0, "OK"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := handler.getCommandTag(tt.query, tt.rowsAffected)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDukdbTypeToOid(t *testing.T) {
	// Import dukdb types by using the type constants
	// Test a few key type mappings
	// Type values from type_enum.go:
	// TYPE_BOOLEAN = 1, TYPE_INTEGER = 4, TYPE_BIGINT = 5, TYPE_DOUBLE = 11, TYPE_VARCHAR = 18

	assert.Equal(t, OidBool, dukdbTypeToOid(1))     // TYPE_BOOLEAN = 1
	assert.Equal(t, OidInt4, dukdbTypeToOid(4))     // TYPE_INTEGER = 4
	assert.Equal(t, OidInt8, dukdbTypeToOid(5))     // TYPE_BIGINT = 5
	assert.Equal(t, OidFloat8, dukdbTypeToOid(11))  // TYPE_DOUBLE = 11
	assert.Equal(t, OidVarchar, dukdbTypeToOid(18)) // TYPE_VARCHAR = 18
}

func TestConfigWithUserProvider(t *testing.T) {
	t.Run("config with user provider validates", func(t *testing.T) {
		config := NewConfig()
		config.RequireAuth = true
		config.UserProvider = auth.NewMemoryProvider().
			WithDefaultUser("admin", "secret", true)

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("config with authenticator validates", func(t *testing.T) {
		config := NewConfig()
		config.RequireAuth = true
		config.Authenticator = auth.NewSimpleAuthenticator("user", "pass")

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("config without auth needs no credentials", func(t *testing.T) {
		config := NewConfig()
		config.RequireAuth = false

		err := config.Validate()
		require.NoError(t, err)
	})
}

func TestConfigGetAuthenticator(t *testing.T) {
	t.Run("returns custom authenticator if set", func(t *testing.T) {
		customAuth := auth.NewSimpleAuthenticator("custom", "auth")
		config := NewConfig()
		config.Authenticator = customAuth

		authenticator := config.GetAuthenticator()
		assert.Equal(t, customAuth, authenticator)
	})

	t.Run("returns no-auth when RequireAuth is false", func(t *testing.T) {
		config := NewConfig()
		config.RequireAuth = false

		authenticator := config.GetAuthenticator()
		assert.Equal(t, auth.MethodNone, authenticator.Method())
	})

	t.Run("creates password authenticator from user provider", func(t *testing.T) {
		provider := auth.NewMemoryProvider().
			WithDefaultUser("user", "pass", false)

		config := NewConfig()
		config.RequireAuth = true
		config.UserProvider = provider

		authenticator := config.GetAuthenticator()
		assert.Equal(t, auth.MethodPassword, authenticator.Method())

		// Test that authentication works
		ctx := context.Background()
		success, err := authenticator.Authenticate(ctx, "user", "pass", "anydb")
		require.NoError(t, err)
		assert.True(t, success)

		success, err = authenticator.Authenticate(ctx, "user", "wrongpass", "anydb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("creates simple authenticator from username/password", func(t *testing.T) {
		config := NewConfig()
		config.RequireAuth = true
		config.Username = "simpleuser"
		config.Password = "simplepass"

		authenticator := config.GetAuthenticator()
		assert.Equal(t, auth.MethodPassword, authenticator.Method())

		// Test that authentication works
		ctx := context.Background()
		success, err := authenticator.Authenticate(ctx, "simpleuser", "simplepass", "anydb")
		require.NoError(t, err)
		assert.True(t, success)

		success, err = authenticator.Authenticate(ctx, "simpleuser", "wrongpass", "anydb")
		require.NoError(t, err)
		assert.False(t, success)
	})
}

func TestServerAuthenticationWithUserProvider(t *testing.T) {
	provider := auth.NewMemoryProvider().
		WithDefaultUser("testuser", "testpass", false).
		WithDefaultUser("admin", "adminpass", true)

	config := NewConfig()
	config.RequireAuth = true
	config.UserProvider = provider
	config.Port = 15433 // Use non-standard port

	server, err := NewServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)

	// The server should validate successfully
	err = server.Start()
	require.NoError(t, err)

	// Clean up
	err = server.Stop()
	require.NoError(t, err)
}

func TestServerNoAuthMode(t *testing.T) {
	config := NewConfig()
	config.RequireAuth = false
	config.Port = 15434 // Use non-standard port

	server, err := NewServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)

	// The server should validate successfully without auth
	err = server.Start()
	require.NoError(t, err)

	// Clean up
	err = server.Stop()
	require.NoError(t, err)
}

func TestSessionAttributes(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Test setting and getting startup params
	session.SetAttribute("application_name", "psql")
	session.SetAttribute("client_encoding", "UTF8")

	appName, ok := session.GetAttribute("application_name")
	assert.True(t, ok)
	assert.Equal(t, "psql", appName)

	encoding, ok := session.GetAttribute("client_encoding")
	assert.True(t, ok)
	assert.Equal(t, "UTF8", encoding)

	// Test storing map of startup params
	params := map[string]string{
		"user":             "testuser",
		"database":         "testdb",
		"application_name": "myapp",
	}
	session.SetAttribute("startup_params", params)

	retrieved, ok := session.GetAttribute("startup_params")
	assert.True(t, ok)
	assert.Equal(t, params, retrieved)
}

func TestConfigLogStartupParams(t *testing.T) {
	config := NewConfig()

	// Default should be false
	assert.False(t, config.LogStartupParams)

	// Should be settable
	config.LogStartupParams = true
	assert.True(t, config.LogStartupParams)
}
