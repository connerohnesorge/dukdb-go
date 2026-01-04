package secret

import (
	"sync"
)

// Catalog provides persistent storage for secrets.
type Catalog interface {
	// GetSecret retrieves a secret by name from persistent storage.
	GetSecret(name string) (*Secret, error)
	// SetSecret stores or updates a secret in persistent storage.
	SetSecret(s Secret) error
	// DeleteSecret removes a secret from persistent storage.
	DeleteSecret(name string) error
	// ListSecrets retrieves all secrets from persistent storage.
	ListSecrets() ([]Secret, error)
}

// MemoryCatalog implements Catalog using in-memory storage.
// This is useful for testing or when persistence is not required.
// For actual database persistence, a DuckDB-backed implementation
// would store secrets in the catalog tables.
type MemoryCatalog struct {
	mu      sync.RWMutex
	secrets map[string]Secret
}

// NewMemoryCatalog creates a new in-memory catalog.
func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{
		secrets: make(map[string]Secret),
	}
}

// GetSecret retrieves a secret by name.
func (c *MemoryCatalog) GetSecret(name string) (*Secret, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	secret, exists := c.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	return secret.Clone(), nil
}

// SetSecret stores or updates a secret.
func (c *MemoryCatalog) SetSecret(s Secret) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.secrets[s.Name] = s

	return nil
}

// DeleteSecret removes a secret.
func (c *MemoryCatalog) DeleteSecret(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.secrets[name]; !exists {
		return ErrSecretNotFound
	}

	delete(c.secrets, name)

	return nil
}

// ListSecrets retrieves all secrets.
func (c *MemoryCatalog) ListSecrets() ([]Secret, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]Secret, 0, len(c.secrets))
	for name := range c.secrets {
		s := c.secrets[name]
		result = append(result, *s.Clone())
	}

	return result, nil
}

// Clear removes all secrets from the catalog.
func (c *MemoryCatalog) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.secrets = make(map[string]Secret)
}

// Count returns the number of secrets in the catalog.
func (c *MemoryCatalog) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.secrets)
}

// Verify MemoryCatalog implements Catalog interface.
var _ Catalog = (*MemoryCatalog)(nil)
