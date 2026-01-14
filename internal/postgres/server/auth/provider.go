package auth

// This file contains additional provider interfaces and helper types
// for managing user credentials.

// CredentialStore provides read-write access to user credentials.
// This extends UserProvider with mutation methods.
type CredentialStore interface {
	UserProvider

	// AddUser adds a new user with the given password.
	// If the user already exists, it will be updated.
	AddUser(user *User, password string) error

	// RemoveUser removes a user by username.
	// Returns nil if the user does not exist.
	RemoveUser(username string) error

	// UpdatePassword updates the password for a user.
	// Returns ErrUserNotFound if the user does not exist.
	UpdatePassword(username, newPassword string) error

	// ListUsers returns all usernames.
	ListUsers() []string
}

// Credentials holds username and password for authentication.
type Credentials struct {
	Username string
	Password string
	Database string
}

// StartupParameters holds parameters received during startup.
type StartupParameters struct {
	// User is the username from startup message.
	User string

	// Database is the requested database name.
	Database string

	// ApplicationName is the client application name.
	ApplicationName string

	// ClientEncoding is the client character encoding.
	ClientEncoding string

	// Extra contains any additional startup parameters.
	Extra map[string]string
}

// NewStartupParameters creates a new StartupParameters with default values.
func NewStartupParameters() *StartupParameters {
	return &StartupParameters{
		ClientEncoding: "UTF8",
		Extra:          make(map[string]string),
	}
}

// Get returns the value of a parameter by name.
func (p *StartupParameters) Get(name string) string {
	switch name {
	case "user":
		return p.User
	case "database":
		return p.Database
	case "application_name":
		return p.ApplicationName
	case "client_encoding":
		return p.ClientEncoding
	default:
		if p.Extra != nil {
			return p.Extra[name]
		}
		return ""
	}
}

// Set sets the value of a parameter by name.
func (p *StartupParameters) Set(name, value string) {
	switch name {
	case "user":
		p.User = value
	case "database":
		p.Database = value
	case "application_name":
		p.ApplicationName = value
	case "client_encoding":
		p.ClientEncoding = value
	default:
		if p.Extra == nil {
			p.Extra = make(map[string]string)
		}
		p.Extra[name] = value
	}
}
