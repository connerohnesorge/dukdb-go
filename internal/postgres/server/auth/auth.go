// Package auth provides authentication interfaces and implementations for the
// PostgreSQL wire protocol server.
package auth

import (
	"context"
	"errors"
)

// Common authentication errors.
var (
	// ErrAuthenticationFailed is returned when authentication fails.
	ErrAuthenticationFailed = errors.New("authentication failed")

	// ErrUserNotFound is returned when a user is not found.
	ErrUserNotFound = errors.New("user not found")

	// ErrInvalidCredentials is returned when credentials are invalid.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrDatabaseAccessDenied is returned when the user does not have access to the database.
	ErrDatabaseAccessDenied = errors.New("database access denied")
)

// Method represents the authentication method type.
type Method string

// Supported authentication methods.
const (
	// MethodNone indicates no authentication is required.
	MethodNone Method = "none"

	// MethodPassword indicates clear text password authentication.
	MethodPassword Method = "password"

	// MethodMD5 indicates MD5 password authentication (future).
	MethodMD5 Method = "md5"

	// MethodSCRAMSHA256 indicates SCRAM-SHA-256 authentication (future).
	MethodSCRAMSHA256 Method = "scram-sha-256"
)

// User represents a database user with authentication information.
type User struct {
	// Username is the unique username for this user.
	Username string

	// PasswordHash is the hashed password (for future hashed password support).
	// Currently stores the plain password for MethodPassword.
	PasswordHash string

	// Databases is the list of databases this user can access.
	// An empty slice means access to all databases.
	Databases []string

	// Superuser indicates if this user has superuser privileges.
	Superuser bool
}

// CanAccessDatabase returns true if the user can access the given database.
func (u *User) CanAccessDatabase(database string) bool {
	// Superusers can access all databases
	if u.Superuser {
		return true
	}

	// Empty database list means access to all databases
	if len(u.Databases) == 0 {
		return true
	}

	// Check if database is in the allowed list
	for _, db := range u.Databases {
		if db == database {
			return true
		}
	}

	return false
}

// Authenticator defines the authentication strategy interface.
// Implementations can provide different authentication mechanisms.
type Authenticator interface {
	// Authenticate validates credentials and returns success/failure.
	// The context can be used to pass authentication-related data.
	// Returns true if authentication succeeded, false otherwise.
	// An error is returned only for system failures, not authentication failures.
	Authenticate(ctx context.Context, username, password, database string) (bool, error)

	// Method returns the authentication method used by this authenticator.
	Method() Method
}

// UserProvider provides user information for authentication.
// This interface separates user storage from authentication logic.
type UserProvider interface {
	// GetUser returns user info, or nil if not found.
	// An error is returned only for system failures.
	GetUser(username string) (*User, error)

	// ValidatePassword checks if the password is correct for the user.
	// Returns true if the password is valid, false otherwise.
	// An error is returned only for system failures.
	ValidatePassword(username, password string) (bool, error)
}

// Result contains the result of an authentication attempt.
type Result struct {
	// Success indicates if authentication was successful.
	Success bool

	// User is the authenticated user (nil if authentication failed).
	User *User

	// Error is any error that occurred during authentication.
	Error error

	// Message is a human-readable message about the result.
	Message string
}

// PasswordAuthenticator implements Authenticator using a UserProvider.
// This is the default authenticator for password-based authentication.
type PasswordAuthenticator struct {
	provider UserProvider
}

// NewPasswordAuthenticator creates a new password authenticator with the given provider.
func NewPasswordAuthenticator(provider UserProvider) *PasswordAuthenticator {
	return &PasswordAuthenticator{provider: provider}
}

// Authenticate implements the Authenticator interface.
func (a *PasswordAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	if a.provider == nil {
		return false, errors.New("no user provider configured")
	}

	// Get the user
	user, err := a.provider.GetUser(username)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil
	}

	// Validate the password
	valid, err := a.provider.ValidatePassword(username, password)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, nil
	}

	// Check database access
	if !user.CanAccessDatabase(database) {
		return false, nil
	}

	return true, nil
}

// Method implements the Authenticator interface.
func (a *PasswordAuthenticator) Method() Method {
	return MethodPassword
}

// NoAuthenticator implements Authenticator that always succeeds.
// Use this when no authentication is required.
type NoAuthenticator struct{}

// NewNoAuthenticator creates a new no-op authenticator.
func NewNoAuthenticator() *NoAuthenticator {
	return &NoAuthenticator{}
}

// Authenticate implements the Authenticator interface.
// It always returns true for any credentials.
func (a *NoAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	return true, nil
}

// Method implements the Authenticator interface.
func (a *NoAuthenticator) Method() Method {
	return MethodNone
}

// SimpleAuthenticator implements Authenticator with a single username/password.
// This is a simple authenticator for basic configurations.
type SimpleAuthenticator struct {
	username string
	password string
}

// NewSimpleAuthenticator creates a new simple authenticator.
func NewSimpleAuthenticator(username, password string) *SimpleAuthenticator {
	return &SimpleAuthenticator{
		username: username,
		password: password,
	}
}

// Authenticate implements the Authenticator interface.
func (a *SimpleAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	return username == a.username && password == a.password, nil
}

// Method implements the Authenticator interface.
func (a *SimpleAuthenticator) Method() Method {
	return MethodPassword
}
