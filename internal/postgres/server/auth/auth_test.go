package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUser_CanAccessDatabase(t *testing.T) {
	tests := []struct {
		name     string
		user     *User
		database string
		expected bool
	}{
		{
			name: "superuser can access any database",
			user: &User{
				Username:  "admin",
				Superuser: true,
				Databases: []string{"db1"},
				Enabled:   true,
			},
			database: "db2",
			expected: true,
		},
		{
			name:     "empty databases means all access",
			user:     &User{Username: "user", Databases: []string{}, Enabled: true},
			database: "anydb",
			expected: true,
		},
		{
			name:     "user can access allowed database",
			user:     &User{Username: "user", Databases: []string{"db1", "db2"}, Enabled: true},
			database: "db1",
			expected: true,
		},
		{
			name:     "user cannot access disallowed database",
			user:     &User{Username: "user", Databases: []string{"db1", "db2"}, Enabled: true},
			database: "db3",
			expected: false,
		},
		{
			name:     "disabled user cannot access any database",
			user:     &User{Username: "user", Databases: []string{}, Enabled: false},
			database: "anydb",
			expected: false,
		},
		{
			name:     "user with superuser role can access any database",
			user:     &User{Username: "roleuser", Roles: []Role{RoleSuperuser}, Enabled: true},
			database: "anydb",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.user.CanAccessDatabase(tt.database)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMemoryProvider(t *testing.T) {
	t.Run("add and get user", func(t *testing.T) {
		provider := NewMemoryProvider()

		user := &User{
			Username:  "testuser",
			Databases: []string{"testdb"},
			Superuser: false,
		}

		err := provider.AddUser(user, "testpass")
		require.NoError(t, err)

		retrieved, err := provider.GetUser("testuser")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		assert.Equal(t, "testuser", retrieved.Username)
		assert.Equal(t, []string{"testdb"}, retrieved.Databases)
		assert.False(t, retrieved.Superuser)
	})

	t.Run("get nonexistent user returns nil", func(t *testing.T) {
		provider := NewMemoryProvider()

		user, err := provider.GetUser("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, user)
	})

	t.Run("validate password success", func(t *testing.T) {
		provider := NewMemoryProvider()
		err := provider.AddUser(&User{Username: "user1"}, "pass1")
		require.NoError(t, err)

		valid, err := provider.ValidatePassword("user1", "pass1")
		require.NoError(t, err)
		assert.True(t, valid)
	})

	t.Run("validate password failure", func(t *testing.T) {
		provider := NewMemoryProvider()
		err := provider.AddUser(&User{Username: "user1"}, "pass1")
		require.NoError(t, err)

		valid, err := provider.ValidatePassword("user1", "wrongpass")
		require.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("validate password nonexistent user", func(t *testing.T) {
		provider := NewMemoryProvider()

		valid, err := provider.ValidatePassword("nonexistent", "pass")
		require.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("remove user", func(t *testing.T) {
		provider := NewMemoryProvider()
		err := provider.AddUser(&User{Username: "user1"}, "pass1")
		require.NoError(t, err)

		err = provider.RemoveUser("user1")
		require.NoError(t, err)

		user, err := provider.GetUser("user1")
		require.NoError(t, err)
		assert.Nil(t, user)
	})

	t.Run("update password", func(t *testing.T) {
		provider := NewMemoryProvider()
		err := provider.AddUser(&User{Username: "user1"}, "oldpass")
		require.NoError(t, err)

		err = provider.UpdatePassword("user1", "newpass")
		require.NoError(t, err)

		valid, err := provider.ValidatePassword("user1", "newpass")
		require.NoError(t, err)
		assert.True(t, valid)

		valid, err = provider.ValidatePassword("user1", "oldpass")
		require.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("update password nonexistent user", func(t *testing.T) {
		provider := NewMemoryProvider()

		err := provider.UpdatePassword("nonexistent", "pass")
		assert.Equal(t, ErrUserNotFound, err)
	})

	t.Run("list users", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "user1"}, "pass1")
		_ = provider.AddUser(&User{Username: "user2"}, "pass2")

		users := provider.ListUsers()
		assert.Len(t, users, 2)
		assert.Contains(t, users, "user1")
		assert.Contains(t, users, "user2")
	})

	t.Run("user count", func(t *testing.T) {
		provider := NewMemoryProvider()
		assert.Equal(t, 0, provider.UserCount())

		_ = provider.AddUser(&User{Username: "user1"}, "pass1")
		assert.Equal(t, 1, provider.UserCount())

		_ = provider.AddUser(&User{Username: "user2"}, "pass2")
		assert.Equal(t, 2, provider.UserCount())
	})

	t.Run("clear", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "user1"}, "pass1")
		_ = provider.AddUser(&User{Username: "user2"}, "pass2")

		provider.Clear()
		assert.Equal(t, 0, provider.UserCount())
	})

	t.Run("with default user", func(t *testing.T) {
		provider := NewMemoryProvider().
			WithDefaultUser("admin", "adminpass", true).
			WithDefaultUser("user", "userpass", false)

		assert.Equal(t, 2, provider.UserCount())

		admin, err := provider.GetUser("admin")
		require.NoError(t, err)
		assert.True(t, admin.Superuser)

		user, err := provider.GetUser("user")
		require.NoError(t, err)
		assert.False(t, user.Superuser)
	})

	t.Run("add nil user returns error", func(t *testing.T) {
		provider := NewMemoryProvider()
		err := provider.AddUser(nil, "pass")
		assert.Equal(t, ErrUserNotFound, err)
	})
}

func TestPasswordAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticate success", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "user1"}, "pass1")

		auth := NewPasswordAuthenticator(provider)
		success, err := auth.Authenticate(ctx, "user1", "pass1", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("authenticate wrong password", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "user1"}, "pass1")

		auth := NewPasswordAuthenticator(provider)
		success, err := auth.Authenticate(ctx, "user1", "wrongpass", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("authenticate unknown user", func(t *testing.T) {
		provider := NewMemoryProvider()

		auth := NewPasswordAuthenticator(provider)
		success, err := auth.Authenticate(ctx, "unknown", "pass", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("authenticate database access denied", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{
			Username:  "user1",
			Databases: []string{"db1", "db2"},
		}, "pass1")

		auth := NewPasswordAuthenticator(provider)

		// Allowed database
		success, err := auth.Authenticate(ctx, "user1", "pass1", "db1")
		require.NoError(t, err)
		assert.True(t, success)

		// Disallowed database
		success, err = auth.Authenticate(ctx, "user1", "pass1", "db3")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("authenticate nil provider", func(t *testing.T) {
		auth := NewPasswordAuthenticator(nil)
		success, err := auth.Authenticate(ctx, "user", "pass", "db")
		assert.Error(t, err)
		assert.False(t, success)
	})

	t.Run("method returns password", func(t *testing.T) {
		auth := NewPasswordAuthenticator(nil)
		assert.Equal(t, MethodPassword, auth.Method())
	})
}

func TestNoAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("always succeeds", func(t *testing.T) {
		auth := NewNoAuthenticator()

		success, err := auth.Authenticate(ctx, "", "", "")
		require.NoError(t, err)
		assert.True(t, success)

		success, err = auth.Authenticate(ctx, "anyuser", "anypass", "anydb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("method returns none", func(t *testing.T) {
		auth := NewNoAuthenticator()
		assert.Equal(t, MethodNone, auth.Method())
	})
}

func TestSimpleAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticate success", func(t *testing.T) {
		auth := NewSimpleAuthenticator("user", "pass")

		success, err := auth.Authenticate(ctx, "user", "pass", "db")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("authenticate wrong credentials", func(t *testing.T) {
		auth := NewSimpleAuthenticator("user", "pass")

		success, err := auth.Authenticate(ctx, "user", "wrongpass", "db")
		require.NoError(t, err)
		assert.False(t, success)

		success, err = auth.Authenticate(ctx, "wronguser", "pass", "db")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("method returns password", func(t *testing.T) {
		auth := NewSimpleAuthenticator("user", "pass")
		assert.Equal(t, MethodPassword, auth.Method())
	})
}

func TestStartupParameters(t *testing.T) {
	t.Run("new startup parameters has defaults", func(t *testing.T) {
		params := NewStartupParameters()

		assert.Equal(t, "UTF8", params.ClientEncoding)
		assert.NotNil(t, params.Extra)
	})

	t.Run("get and set standard parameters", func(t *testing.T) {
		params := NewStartupParameters()

		params.Set("user", "testuser")
		params.Set("database", "testdb")
		params.Set("application_name", "myapp")
		params.Set("client_encoding", "LATIN1")

		assert.Equal(t, "testuser", params.Get("user"))
		assert.Equal(t, "testdb", params.Get("database"))
		assert.Equal(t, "myapp", params.Get("application_name"))
		assert.Equal(t, "LATIN1", params.Get("client_encoding"))
	})

	t.Run("get and set extra parameters", func(t *testing.T) {
		params := NewStartupParameters()

		params.Set("custom_param", "custom_value")
		assert.Equal(t, "custom_value", params.Get("custom_param"))
	})

	t.Run("get nonexistent parameter returns empty string", func(t *testing.T) {
		params := NewStartupParameters()
		assert.Equal(t, "", params.Get("nonexistent"))
	})
}

// =============================================================================
// SCRAM-SHA-256 Tests (Task 16.1)
// =============================================================================

func TestSCRAMCredentials(t *testing.T) {
	t.Run("generate and verify credentials", func(t *testing.T) {
		password := "testpassword123"

		creds, err := GenerateSCRAMCredentials(password)
		require.NoError(t, err)
		require.NotNil(t, creds)

		assert.Len(t, creds.Salt, SCRAMSaltLength)
		assert.Len(t, creds.StoredKey, 32) // SHA-256 output
		assert.Len(t, creds.ServerKey, 32) // SHA-256 output
		assert.Equal(t, SCRAMIterationCount, creds.Iterations)
	})

	t.Run("encode and parse credentials", func(t *testing.T) {
		password := "testpassword123"

		creds, err := GenerateSCRAMCredentials(password)
		require.NoError(t, err)

		encoded := creds.Encode()
		assert.True(t, len(encoded) > 0)
		assert.Contains(t, encoded, "SCRAM-SHA-256$")

		parsed, err := ParseSCRAMCredentials(encoded)
		require.NoError(t, err)

		assert.Equal(t, creds.Salt, parsed.Salt)
		assert.Equal(t, creds.StoredKey, parsed.StoredKey)
		assert.Equal(t, creds.ServerKey, parsed.ServerKey)
		assert.Equal(t, creds.Iterations, parsed.Iterations)
	})

	t.Run("parse invalid credentials fails", func(t *testing.T) {
		_, err := ParseSCRAMCredentials("invalid")
		assert.Error(t, err)

		_, err = ParseSCRAMCredentials("SCRAM-SHA-256$invalid")
		assert.Error(t, err)
	})

	t.Run("custom iteration count", func(t *testing.T) {
		customIterations := 8192
		creds, err := GenerateSCRAMCredentialsWithIterations("password", customIterations)
		require.NoError(t, err)
		assert.Equal(t, customIterations, creds.Iterations)
	})
}

func TestSCRAMAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticate with SCRAM credentials", func(t *testing.T) {
		provider := NewMemoryProvider()
		user := &User{Username: "scramuser", Enabled: true}
		password := "scrampassword"

		// Add user with SCRAM credentials
		err := provider.AddUserWithSCRAM(user, password)
		require.NoError(t, err)

		auth := NewSCRAMAuthenticator(provider)

		// Should authenticate with correct password
		success, err := auth.Authenticate(ctx, "scramuser", password, "testdb")
		require.NoError(t, err)
		assert.True(t, success)

		// Should fail with wrong password
		success, err = auth.Authenticate(ctx, "scramuser", "wrongpass", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("authenticate unknown user fails", func(t *testing.T) {
		provider := NewMemoryProvider()
		auth := NewSCRAMAuthenticator(provider)

		success, err := auth.Authenticate(ctx, "unknown", "pass", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("method returns scram-sha-256", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(nil)
		assert.Equal(t, MethodSCRAMSHA256, auth.Method())
	})

	t.Run("fallback to password auth when no SCRAM credentials", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "regularuser", Enabled: true}, "regularpass")

		auth := NewSCRAMAuthenticator(provider)

		// Should fall back to password validation
		success, err := auth.Authenticate(ctx, "regularuser", "regularpass", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})
}

func TestSCRAMProtocol(t *testing.T) {
	t.Run("process client first message", func(t *testing.T) {
		provider := NewMemoryProvider()
		user := &User{Username: "testuser", Enabled: true}
		_ = provider.AddUserWithSCRAM(user, "testpass")

		auth := NewSCRAMAuthenticator(provider)

		// Client first message format: n,,n=<username>,r=<client-nonce>
		clientFirst := "n,,n=testuser,r=clientnonce123"

		serverFirst, err := auth.ProcessClientFirst("session1", clientFirst)
		require.NoError(t, err)
		require.NotEmpty(t, serverFirst)

		// Server first should contain combined nonce, salt, and iterations
		assert.Contains(t, serverFirst, "r=clientnonce123") // nonce starts with client nonce
		assert.Contains(t, serverFirst, "s=")               // salt
		assert.Contains(t, serverFirst, "i=")               // iteration count
	})

	t.Run("invalid client first message", func(t *testing.T) {
		provider := NewMemoryProvider()
		auth := NewSCRAMAuthenticator(provider)

		_, err := auth.ProcessClientFirst("session1", "invalid")
		assert.Error(t, err)

		_, err = auth.ProcessClientFirst("session1", "n,,r=nonce") // missing username
		assert.Error(t, err)
	})

	t.Run("cleanup state", func(t *testing.T) {
		provider := NewMemoryProvider()
		auth := NewSCRAMAuthenticator(provider)

		// Start a session
		_ = provider.AddUserWithSCRAM(&User{Username: "user", Enabled: true}, "pass")
		_, _ = auth.ProcessClientFirst("session1", "n,,n=user,r=nonce")

		// Cleanup should not panic
		auth.CleanupState("session1")
		auth.CleanupState("nonexistent")
	})
}

// =============================================================================
// Certificate Authentication Tests (Task 16.2)
// =============================================================================

func TestCertificateAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticate with matching CN", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "certuser", Enabled: true}, "")

		auth := NewCertificateAuthenticator(provider)

		// Add context with cert CN
		ctxWithCert := ContextWithCertCN(ctx, "certuser")

		success, err := auth.Authenticate(ctxWithCert, "certuser", "", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("authenticate with CN mapping", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "dbuser", Enabled: true}, "")

		auth := NewCertificateAuthenticator(provider)
		auth.MapCNToUser("cert_cn_name", "dbuser")

		ctxWithCert := ContextWithCertCN(ctx, "cert_cn_name")

		success, err := auth.Authenticate(ctxWithCert, "dbuser", "", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("reject when CN does not match user", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "user1", Enabled: true}, "")

		auth := NewCertificateAuthenticator(provider)

		ctxWithCert := ContextWithCertCN(ctx, "wrongcn")

		success, err := auth.Authenticate(ctxWithCert, "user1", "", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("reject when no certificate provided", func(t *testing.T) {
		provider := NewMemoryProvider()
		auth := NewCertificateAuthenticator(provider)

		success, err := auth.Authenticate(ctx, "user", "", "testdb")
		assert.Error(t, err)
		assert.False(t, success)
	})

	t.Run("verify user expected CN", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{
			Username:      "secureuser",
			CertificateCN: "expected_cn",
			Enabled:       true,
		}, "")

		auth := NewCertificateAuthenticator(provider)
		// Map the expected_cn to the secureuser
		auth.MapCNToUser("expected_cn", "secureuser")

		// Correct CN
		ctxWithCert := ContextWithCertCN(ctx, "expected_cn")
		success, err := auth.Authenticate(ctxWithCert, "secureuser", "", "testdb")
		require.NoError(t, err)
		assert.True(t, success)

		// Wrong CN - should fail because user has a specific expected CN
		ctxWithWrongCert := ContextWithCertCN(ctx, "wrong_cn")
		auth.MapCNToUser("wrong_cn", "secureuser") // Map wrong CN to user
		success, err = auth.Authenticate(ctxWithWrongCert, "secureuser", "", "testdb")
		require.NoError(t, err)
		assert.False(t, success) // Should fail because CertificateCN doesn't match
	})

	t.Run("method returns cert", func(t *testing.T) {
		auth := NewCertificateAuthenticator(nil)
		assert.Equal(t, MethodCert, auth.Method())
	})

	t.Run("remove CN mapping", func(t *testing.T) {
		auth := NewCertificateAuthenticator(nil)
		auth.MapCNToUser("cn1", "user1")
		auth.RemoveCNMapping("cn1")

		// After removal, CN should map to itself
		username := auth.getUsernameFromCN("cn1")
		assert.Equal(t, "cn1", username)
	})
}

// =============================================================================
// LDAP Authentication Tests (Task 16.3)
// =============================================================================

func TestLDAPAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("build user DN", func(t *testing.T) {
		config := &LDAPConfig{
			BaseDN:        "dc=example,dc=com",
			UserAttribute: "uid",
		}

		auth := NewLDAPAuthenticator(config, nil)
		dn := auth.buildUserDN("testuser")

		assert.Equal(t, "uid=testuser,dc=example,dc=com", dn)
	})

	t.Run("authenticate with mock LDAP bind", func(t *testing.T) {
		config := &LDAPConfig{
			Server:        "ldap.example.com",
			Port:          389,
			BaseDN:        "dc=example,dc=com",
			UserAttribute: "uid",
		}

		auth := NewLDAPAuthenticator(config, nil)

		// Mock successful bind
		auth.SetLDAPBindFunc(
			func(server string, port int, bindDN, password string, useTLS bool) error {
				if password == "correctpass" {
					return nil
				}
				return ErrLDAPAuthFailed
			},
		)

		// Should succeed with correct password
		success, err := auth.Authenticate(ctx, "testuser", "correctpass", "testdb")
		require.NoError(t, err)
		assert.True(t, success)

		// Should fail with wrong password
		success, err = auth.Authenticate(ctx, "testuser", "wrongpass", "testdb")
		require.NoError(t, err)
		assert.False(t, success)
	})

	t.Run("method returns ldap", func(t *testing.T) {
		auth := NewLDAPAuthenticator(nil, nil)
		assert.Equal(t, MethodLDAP, auth.Method())
	})

	t.Run("default config", func(t *testing.T) {
		config := NewLDAPConfig()
		assert.Equal(t, 389, config.Port)
		assert.Equal(t, "(uid=%s)", config.UserSearchFilter)
		assert.Equal(t, "uid", config.UserAttribute)
	})
}

// =============================================================================
// HBA (Host-Based Access) Tests (Task 16.4)
// =============================================================================

func TestHBARule(t *testing.T) {
	t.Run("create rule", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "all", "all", MethodPassword)
		assert.Equal(t, HBAHost, rule.Type)
		assert.Equal(t, "all", rule.Database)
		assert.Equal(t, "all", rule.User)
		assert.Equal(t, MethodPassword, rule.Method)
	})

	t.Run("rule with address", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "all", "all", MethodPassword).
			WithAddress("192.168.1.0/24")

		assert.Equal(t, "192.168.1.0/24", rule.Address)
		assert.NotNil(t, rule.parsedNet)
	})

	t.Run("rule matches database", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "db1,db2", "all", MethodPassword).
			WithAddress("0.0.0.0/0")

		assert.True(t, rule.matchesDatabase("db1"))
		assert.True(t, rule.matchesDatabase("db2"))
		assert.False(t, rule.matchesDatabase("db3"))
	})

	t.Run("rule matches user", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "all", "user1,user2", MethodPassword).
			WithAddress("0.0.0.0/0")

		assert.True(t, rule.matchesUser("user1"))
		assert.True(t, rule.matchesUser("user2"))
		assert.False(t, rule.matchesUser("user3"))
	})

	t.Run("rule matches IP address", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "all", "all", MethodPassword).
			WithAddress("192.168.1.0/24")

		assert.True(t, rule.matchesAddress("192.168.1.100"))
		assert.True(t, rule.matchesAddress("192.168.1.1"))
		assert.False(t, rule.matchesAddress("192.168.2.1"))
		assert.False(t, rule.matchesAddress("10.0.0.1"))
	})

	t.Run("rule matches all addresses", func(t *testing.T) {
		rule := NewHBARule(HBAHost, "all", "all", MethodPassword).
			WithAddress("all")

		assert.True(t, rule.matchesAddress("192.168.1.100"))
		assert.True(t, rule.matchesAddress("10.0.0.1"))
	})

	t.Run("rule matches SSL", func(t *testing.T) {
		sslRule := NewHBARule(HBAHostSSL, "all", "all", MethodPassword)
		noSslRule := NewHBARule(HBAHostNoSSL, "all", "all", MethodPassword)

		assert.True(t, sslRule.matchesType(HBAHost, true))
		assert.False(t, sslRule.matchesType(HBAHost, false))

		assert.False(t, noSslRule.matchesType(HBAHost, true))
		assert.True(t, noSslRule.matchesType(HBAHost, false))
	})
}

func TestHBAController(t *testing.T) {
	ctx := context.Background()

	t.Run("add and find rule", func(t *testing.T) {
		hba := NewHBAController()

		rule := NewHBARule(HBAHost, "all", "all", MethodTrust).
			WithAddress("0.0.0.0/0")
		hba.AddRule(rule)

		found := hba.FindMatchingRule(HBAHost, "testdb", "testuser", "192.168.1.1", false)
		require.NotNil(t, found)
		assert.Equal(t, MethodTrust, found.Method)
	})

	t.Run("rules evaluated in order", func(t *testing.T) {
		hba := NewHBAController()

		// First rule: reject user "blocked"
		hba.AddRule(NewHBARule(HBAHost, "all", "blocked", MethodReject).WithAddress("0.0.0.0/0"))
		// Second rule: allow all
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodTrust).WithAddress("0.0.0.0/0"))

		// "blocked" user should match first rule
		rule := hba.FindMatchingRule(HBAHost, "testdb", "blocked", "192.168.1.1", false)
		require.NotNil(t, rule)
		assert.Equal(t, MethodReject, rule.Method)

		// Other users should match second rule
		rule = hba.FindMatchingRule(HBAHost, "testdb", "otheruser", "192.168.1.1", false)
		require.NotNil(t, rule)
		assert.Equal(t, MethodTrust, rule.Method)
	})

	t.Run("authenticate with trust method", func(t *testing.T) {
		hba := NewHBAController()
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodTrust).WithAddress("0.0.0.0/0"))

		success, err := hba.Authenticate(
			ctx,
			HBAHost,
			"testdb",
			"user",
			"pass",
			"192.168.1.1",
			false,
		)
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("authenticate with reject method", func(t *testing.T) {
		hba := NewHBAController()
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodReject).WithAddress("0.0.0.0/0"))

		success, err := hba.Authenticate(
			ctx,
			HBAHost,
			"testdb",
			"user",
			"pass",
			"192.168.1.1",
			false,
		)
		assert.Equal(t, ErrAccessDenied, err)
		assert.False(t, success)
	})

	t.Run("no matching rule returns access denied", func(t *testing.T) {
		hba := NewHBAController()
		// No rules added

		success, err := hba.Authenticate(
			ctx,
			HBAHost,
			"testdb",
			"user",
			"pass",
			"192.168.1.1",
			false,
		)
		assert.Equal(t, ErrAccessDenied, err)
		assert.False(t, success)
	})

	t.Run("insert and remove rules", func(t *testing.T) {
		hba := NewHBAController()

		rule1 := NewHBARule(HBAHost, "all", "all", MethodPassword).WithAddress("0.0.0.0/0")
		rule2 := NewHBARule(HBAHost, "all", "all", MethodTrust).WithAddress("0.0.0.0/0")

		hba.AddRule(rule1)
		hba.InsertRule(0, rule2) // Insert at beginning

		rules := hba.Rules()
		require.Len(t, rules, 2)
		assert.Equal(t, MethodTrust, rules[0].Method)    // Inserted at 0
		assert.Equal(t, MethodPassword, rules[1].Method) // Moved to 1

		hba.RemoveRule(0)
		rules = hba.Rules()
		require.Len(t, rules, 1)
		assert.Equal(t, MethodPassword, rules[0].Method)
	})

	t.Run("clear rules", func(t *testing.T) {
		hba := NewHBAController()
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodTrust).WithAddress("0.0.0.0/0"))
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodPassword).WithAddress("0.0.0.0/0"))

		hba.ClearRules()
		assert.Len(t, hba.Rules(), 0)
	})

	t.Run("register and use authenticator", func(t *testing.T) {
		hba := NewHBAController()

		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "testuser", Enabled: true}, "testpass")

		hba.RegisterAuthenticator(MethodPassword, NewPasswordAuthenticator(provider))
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodPassword).WithAddress("0.0.0.0/0"))

		success, err := hba.Authenticate(
			ctx,
			HBAHost,
			"testdb",
			"testuser",
			"testpass",
			"192.168.1.1",
			false,
		)
		require.NoError(t, err)
		assert.True(t, success)

		success, err = hba.Authenticate(
			ctx,
			HBAHost,
			"testdb",
			"testuser",
			"wrongpass",
			"192.168.1.1",
			false,
		)
		require.NoError(t, err)
		assert.False(t, success)
	})
}

// =============================================================================
// Role-Based Access Control Tests (Task 16.5)
// =============================================================================

func TestUserRoles(t *testing.T) {
	t.Run("add and check roles", func(t *testing.T) {
		user := NewUser("testuser")

		user.AddRole(RoleReadOnly)
		assert.True(t, user.HasRole(RoleReadOnly))
		assert.False(t, user.HasRole(RoleAdmin))

		user.AddRole(RoleAdmin)
		assert.True(t, user.HasRole(RoleAdmin))
	})

	t.Run("remove role", func(t *testing.T) {
		user := NewUser("testuser")
		user.AddRole(RoleReadOnly)
		user.AddRole(RoleAdmin)

		user.RemoveRole(RoleReadOnly)
		assert.False(t, user.HasRole(RoleReadOnly))
		assert.True(t, user.HasRole(RoleAdmin))
	})

	t.Run("duplicate role not added twice", func(t *testing.T) {
		user := NewUser("testuser")
		user.AddRole(RoleReadOnly)
		user.AddRole(RoleReadOnly)

		assert.Len(t, user.Roles, 1)
	})
}

func TestUserPermissions(t *testing.T) {
	t.Run("superuser has all permissions", func(t *testing.T) {
		user := &User{Username: "admin", Superuser: true, Enabled: true}

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermInsert))
		assert.True(t, user.HasPermission(PermDelete))
		assert.True(t, user.HasPermission(PermCreateDB))
		assert.True(t, user.HasPermission(PermSuperuser))
	})

	t.Run("superuser role has all permissions", func(t *testing.T) {
		user := NewUser("superrole")
		user.AddRole(RoleSuperuser)

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermCreateDB))
		assert.True(t, user.HasPermission(PermSuperuser))
	})

	t.Run("admin role permissions", func(t *testing.T) {
		user := NewUser("adminuser")
		user.AddRole(RoleAdmin)

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermInsert))
		assert.True(t, user.HasPermission(PermUpdate))
		assert.True(t, user.HasPermission(PermDelete))
		assert.True(t, user.HasPermission(PermCreateDB))
		assert.True(t, user.HasPermission(PermCreateRole))
		assert.False(t, user.HasPermission(PermSuperuser))
	})

	t.Run("readwrite role permissions", func(t *testing.T) {
		user := NewUser("rwuser")
		user.AddRole(RoleReadWrite)

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermInsert))
		assert.True(t, user.HasPermission(PermUpdate))
		assert.True(t, user.HasPermission(PermDelete))
		assert.True(t, user.HasPermission(PermConnect))
		assert.False(t, user.HasPermission(PermCreateDB))
		assert.False(t, user.HasPermission(PermTruncate))
	})

	t.Run("readonly role permissions", func(t *testing.T) {
		user := NewUser("rouser")
		user.AddRole(RoleReadOnly)

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermConnect))
		assert.True(t, user.HasPermission(PermExecute))
		assert.False(t, user.HasPermission(PermInsert))
		assert.False(t, user.HasPermission(PermUpdate))
		assert.False(t, user.HasPermission(PermDelete))
	})

	t.Run("connect role permissions", func(t *testing.T) {
		user := NewUser("connectuser")
		user.AddRole(RoleConnect)

		assert.True(t, user.HasPermission(PermConnect))
		assert.False(t, user.HasPermission(PermSelect))
		assert.False(t, user.HasPermission(PermInsert))
	})

	t.Run("grant and revoke explicit permissions", func(t *testing.T) {
		user := NewUser("testuser")

		user.GrantPermission(PermSelect)
		assert.True(t, user.HasPermission(PermSelect))

		user.RevokePermission(PermSelect)
		assert.False(t, user.HasPermission(PermSelect))
	})

	t.Run("ALL permission grants everything", func(t *testing.T) {
		user := NewUser("alluser")
		user.GrantPermission(PermAll)

		assert.True(t, user.HasPermission(PermSelect))
		assert.True(t, user.HasPermission(PermInsert))
		assert.True(t, user.HasPermission(PermUpdate))
	})
}

// =============================================================================
// Combined Authenticator Tests
// =============================================================================

func TestCombinedAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticate with HBA rules", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "testuser", Enabled: true}, "testpass")

		auth := NewCombinedAuthenticator(provider)
		hba := auth.HBAController()

		// Add HBA rule for password auth
		hba.AddRule(NewHBARule(HBAHost, "all", "all", MethodPassword).WithAddress("0.0.0.0/0"))

		// Add connection info to context
		testCtx := ContextWithClientIP(ctx, "192.168.1.1")
		testCtx = ContextWithConnType(testCtx, HBAHost)
		testCtx = ContextWithIsSSL(testCtx, false)

		success, err := auth.Authenticate(testCtx, "testuser", "testpass", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("fallback to default when no HBA rules", func(t *testing.T) {
		provider := NewMemoryProvider()
		_ = provider.AddUser(&User{Username: "testuser", Enabled: true}, "testpass")

		auth := NewCombinedAuthenticator(provider)
		// No HBA rules added

		success, err := auth.Authenticate(ctx, "testuser", "testpass", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})

	t.Run("set default authenticator", func(t *testing.T) {
		auth := NewCombinedAuthenticator(nil)

		// Set a simple authenticator as default
		auth.SetDefaultAuthenticator(NewSimpleAuthenticator("user", "pass"))

		success, err := auth.Authenticate(ctx, "user", "pass", "testdb")
		require.NoError(t, err)
		assert.True(t, success)
	})
}

// =============================================================================
// Context Helpers Tests
// =============================================================================

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("client IP context", func(t *testing.T) {
		ctxWithIP := ContextWithClientIP(ctx, "192.168.1.100")
		ip := ClientIPFromContext(ctxWithIP)
		assert.Equal(t, "192.168.1.100", ip)

		// Empty context returns empty string
		assert.Equal(t, "", ClientIPFromContext(ctx))
	})

	t.Run("SSL context", func(t *testing.T) {
		ctxWithSSL := ContextWithIsSSL(ctx, true)
		assert.True(t, IsSSLFromContext(ctxWithSSL))

		ctxWithoutSSL := ContextWithIsSSL(ctx, false)
		assert.False(t, IsSSLFromContext(ctxWithoutSSL))
	})

	t.Run("connection type context", func(t *testing.T) {
		ctxWithType := ContextWithConnType(ctx, HBAHostSSL)
		connType := ConnTypeFromContext(ctxWithType)
		assert.Equal(t, HBAHostSSL, connType)
	})

	t.Run("cert CN context", func(t *testing.T) {
		ctxWithCN := ContextWithCertCN(ctx, "test.example.com")
		cn := CertCNFromContext(ctxWithCN)
		assert.Equal(t, "test.example.com", cn)
	})
}
