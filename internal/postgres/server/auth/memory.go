package auth

import (
	"sync"
)

// MemoryProvider implements UserProvider and CredentialStore using an in-memory map.
// This is useful for testing and simple configurations.
type MemoryProvider struct {
	mu        sync.RWMutex
	users     map[string]*User
	passwords map[string]string // username -> plaintext password
}

// NewMemoryProvider creates a new in-memory credential provider.
func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		users:     make(map[string]*User),
		passwords: make(map[string]string),
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
	return &User{
		Username:     user.Username,
		PasswordHash: user.PasswordHash,
		Databases:    append([]string{}, user.Databases...),
		Superuser:    user.Superuser,
	}, nil
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
	p.users[user.Username] = &User{
		Username:     user.Username,
		PasswordHash: user.PasswordHash,
		Databases:    append([]string{}, user.Databases...),
		Superuser:    user.Superuser,
	}
	p.passwords[user.Username] = password

	return nil
}

// RemoveUser removes a user by username.
func (p *MemoryProvider) RemoveUser(username string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.users, username)
	delete(p.passwords, username)

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
}

// WithDefaultUser adds a default user and returns the provider.
// This is useful for chaining during initialization.
func (p *MemoryProvider) WithDefaultUser(username, password string, superuser bool) *MemoryProvider {
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
