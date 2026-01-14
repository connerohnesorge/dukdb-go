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
			name:     "superuser can access any database",
			user:     &User{Username: "admin", Superuser: true, Databases: []string{"db1"}},
			database: "db2",
			expected: true,
		},
		{
			name:     "empty databases means all access",
			user:     &User{Username: "user", Databases: []string{}},
			database: "anydb",
			expected: true,
		},
		{
			name:     "user can access allowed database",
			user:     &User{Username: "user", Databases: []string{"db1", "db2"}},
			database: "db1",
			expected: true,
		},
		{
			name:     "user cannot access disallowed database",
			user:     &User{Username: "user", Databases: []string{"db1", "db2"}},
			database: "db3",
			expected: false,
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
