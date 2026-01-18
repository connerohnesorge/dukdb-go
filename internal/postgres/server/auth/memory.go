package auth

import (
	"sync"
)

// MemoryProvider implements UserProvider and CredentialStore using an in-memory map.
// This is useful for testing and simple configurations.
type MemoryProvider struct {
	mu               sync.RWMutex
	users            map[string]*User
	passwords        map[string]string            // username -> plaintext password
	scramCredentials map[string]*SCRAMCredentials // username -> SCRAM credentials
}

// NewMemoryProvider creates a new in-memory credential provider.
func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		users:            make(map[string]*User),
		passwords:        make(map[string]string),
		scramCredentials: make(map[string]*SCRAMCredentials),
	}
}

// GetUser returns user info, or nil if not found.
func (p *MemoryProvider) GetUser(username string) (*User, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	user, exists := p.users[username]
	if !exists {
		return nil, nil
	}

	// Return a copy to prevent external modification
	copyUser := &User{
		Username:        user.Username,
		PasswordHash:    user.PasswordHash,
		Databases:       append([]string{}, user.Databases...),
		Superuser:       user.Superuser,
		Roles:           append([]Role{}, user.Roles...),
		CertificateCN:   user.CertificateCN,
		LDAPBindDN:      user.LDAPBindDN,
		Enabled:         user.Enabled,
		ConnectionLimit: user.ConnectionLimit,
		ValidUntil:      user.ValidUntil,
	}

	// Copy permissions
	if user.Permissions != nil {
		copyUser.Permissions = make(map[Permission]bool)
		for k, v := range user.Permissions {
			copyUser.Permissions[k] = v
		}
	}

	// Copy SCRAM credentials if present
	if creds, ok := p.scramCredentials[username]; ok {
		copyUser.SCRAMCredentials = &SCRAMCredentials{
			Salt:       append([]byte{}, creds.Salt...),
			StoredKey:  append([]byte{}, creds.StoredKey...),
			ServerKey:  append([]byte{}, creds.ServerKey...),
			Iterations: creds.Iterations,
		}
	}

	return copyUser, nil
}

// ValidatePassword checks if the password is correct for the user.
func (p *MemoryProvider) ValidatePassword(username, password string) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	storedPassword, exists := p.passwords[username]
	if !exists {
		return false, nil
	}

	return storedPassword == password, nil
}

// AddUser adds a new user with the given password.
// If the user already exists, it will be updated.
func (p *MemoryProvider) AddUser(user *User, password string) error {
	if user == nil {
		return ErrUserNotFound
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Store a copy of the user
	copyUser := &User{
		Username:        user.Username,
		PasswordHash:    user.PasswordHash,
		Databases:       append([]string{}, user.Databases...),
		Superuser:       user.Superuser,
		Roles:           append([]Role{}, user.Roles...),
		CertificateCN:   user.CertificateCN,
		LDAPBindDN:      user.LDAPBindDN,
		Enabled:         user.Enabled,
		ConnectionLimit: user.ConnectionLimit,
		ValidUntil:      user.ValidUntil,
	}

	// Copy permissions
	if user.Permissions != nil {
		copyUser.Permissions = make(map[Permission]bool)
		for k, v := range user.Permissions {
			copyUser.Permissions[k] = v
		}
	}

	// Set default enabled state
	if !copyUser.Enabled && user.Username != "" {
		copyUser.Enabled = true
	}

	p.users[user.Username] = copyUser
	p.passwords[user.Username] = password

	return nil
}

// AddUserWithSCRAM adds a user with SCRAM-SHA-256 credentials.
func (p *MemoryProvider) AddUserWithSCRAM(user *User, password string) error {
	if err := p.AddUser(user, password); err != nil {
		return err
	}

	// Generate SCRAM credentials
	creds, err := GenerateSCRAMCredentials(password)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.scramCredentials[user.Username] = creds
	p.mu.Unlock()

	return nil
}

// SetSCRAMCredentials sets SCRAM credentials for a user directly.
func (p *MemoryProvider) SetSCRAMCredentials(username string, creds *SCRAMCredentials) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.users[username]; !exists {
		return ErrUserNotFound
	}

	p.scramCredentials[username] = creds
	return nil
}

// GetSCRAMCredentials returns SCRAM credentials for a user.
func (p *MemoryProvider) GetSCRAMCredentials(username string) (*SCRAMCredentials, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	creds, ok := p.scramCredentials[username]
	if !ok {
		return nil, nil
	}

	// Return a copy
	return &SCRAMCredentials{
		Salt:       append([]byte{}, creds.Salt...),
		StoredKey:  append([]byte{}, creds.StoredKey...),
		ServerKey:  append([]byte{}, creds.ServerKey...),
		Iterations: creds.Iterations,
	}, nil
}

// RemoveUser removes a user by username.
func (p *MemoryProvider) RemoveUser(username string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.users, username)
	delete(p.passwords, username)
	delete(p.scramCredentials, username)

	return nil
}

// UpdatePassword updates the password for a user.
func (p *MemoryProvider) UpdatePassword(username, newPassword string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.users[username]; !exists {
		return ErrUserNotFound
	}

	p.passwords[username] = newPassword
	return nil
}

// ListUsers returns all usernames.
func (p *MemoryProvider) ListUsers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	users := make([]string, 0, len(p.users))
	for username := range p.users {
		users = append(users, username)
	}
	return users
}

// UserCount returns the number of users.
func (p *MemoryProvider) UserCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.users)
}

// Clear removes all users.
func (p *MemoryProvider) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.users = make(map[string]*User)
	p.passwords = make(map[string]string)
	p.scramCredentials = make(map[string]*SCRAMCredentials)
}

// WithDefaultUser adds a default user and returns the provider.
// This is useful for chaining during initialization.
func (p *MemoryProvider) WithDefaultUser(
	username, password string,
	superuser bool,
) *MemoryProvider {
	_ = p.AddUser(&User{
		Username:  username,
		Superuser: superuser,
	}, password)
	return p
}

// Ensure MemoryProvider implements the interfaces.
var (
	_ UserProvider    = (*MemoryProvider)(nil)
	_ CredentialStore = (*MemoryProvider)(nil)
)
