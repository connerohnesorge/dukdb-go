package secret

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// schemeSeparatorLen is the length of "://".
const schemeSeparatorLen = 3

// Common errors for secret operations.
var (
	// ErrSecretNotFound is returned when a secret cannot be found.
	ErrSecretNotFound = errors.New("secret not found")
	// ErrSecretExists is returned when attempting to create a secret that already exists.
	ErrSecretExists = errors.New("secret already exists")
	// ErrInvalidSecretName is returned when a secret name is empty or invalid.
	ErrInvalidSecretName = errors.New("invalid secret name")
	// ErrInvalidSecretType is returned when a secret type is invalid.
	ErrInvalidSecretType = errors.New("invalid secret type")
)

// Manager provides operations for managing secrets.
type Manager interface {
	// CreateSecret creates a new secret.
	// Returns ErrSecretExists if a secret with the same name already exists.
	CreateSecret(ctx context.Context, s Secret) error

	// DropSecret removes a secret by name.
	// If ifExists is true, no error is returned if the secret doesn't exist.
	// Returns ErrSecretNotFound if the secret doesn't exist and ifExists is false.
	DropSecret(ctx context.Context, name string, ifExists bool) error

	// AlterSecret updates the options of an existing secret.
	// Returns ErrSecretNotFound if the secret doesn't exist.
	AlterSecret(ctx context.Context, name string, opts SecretOptions) error

	// GetSecret finds the best matching secret for a URL and secret type.
	// It uses longest prefix match to find the most specific matching secret.
	// Returns ErrSecretNotFound if no matching secret is found.
	GetSecret(ctx context.Context, url string, secretType SecretType) (*Secret, error)

	// GetSecretByName retrieves a secret by its name.
	// Returns ErrSecretNotFound if the secret doesn't exist.
	GetSecretByName(ctx context.Context, name string) (*Secret, error)

	// ListSecrets lists all secrets, optionally filtered by scope.
	// If scope.Type is empty, all secrets are returned.
	ListSecrets(ctx context.Context, scope SecretScope) ([]Secret, error)
}

// manager implements the Manager interface.
type manager struct {
	mu      sync.RWMutex
	secrets map[string]Secret
	catalog Catalog
}

// NewManager creates a new secret manager.
// If catalog is nil, secrets are only stored in memory.
func NewManager(catalog Catalog) Manager {
	return &manager{
		secrets: make(map[string]Secret),
		catalog: catalog,
	}
}

// CreateSecret creates a new secret.
func (m *manager) CreateSecret(_ context.Context, s Secret) error {
	if s.Name == "" {
		return ErrInvalidSecretName
	}

	if s.Type == "" {
		return ErrInvalidSecretType
	}

	// Set default values
	if s.Provider == "" {
		s.Provider = ProviderConfig
	}

	if s.Scope.Type == "" {
		s.Scope.Type = ScopeGlobal
	}

	now := time.Now()
	s.CreatedAt = now
	s.UpdatedAt = now

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if secret already exists
	if _, exists := m.secrets[s.Name]; exists {
		return ErrSecretExists
	}

	// Store in memory
	m.secrets[s.Name] = s

	// Store in catalog if persistent and catalog is available
	if s.Persistent && m.catalog != nil {
		if err := m.catalog.SetSecret(s); err != nil {
			// Rollback memory store
			delete(m.secrets, s.Name)

			return err
		}
	}

	return nil
}

// DropSecret removes a secret by name.
func (m *manager) DropSecret(_ context.Context, name string, ifExists bool) error {
	if name == "" {
		return ErrInvalidSecretName
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	secret, exists := m.secrets[name]
	if !exists {
		if ifExists {
			return nil
		}

		return ErrSecretNotFound
	}

	// Remove from memory
	delete(m.secrets, name)

	// Remove from catalog if persistent and catalog is available
	if secret.Persistent && m.catalog != nil {
		if err := m.catalog.DeleteSecret(name); err != nil {
			// Rollback memory delete
			m.secrets[name] = secret

			return err
		}
	}

	return nil
}

// AlterSecret updates the options of an existing secret.
func (m *manager) AlterSecret(_ context.Context, name string, opts SecretOptions) error {
	if name == "" {
		return ErrInvalidSecretName
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	secret, exists := m.secrets[name]
	if !exists {
		return ErrSecretNotFound
	}

	// Clone the secret to avoid modifying the original while updating
	original := secret

	// Update options
	if secret.Options == nil {
		secret.Options = make(SecretOptions)
	}

	for k, v := range opts {
		secret.Options[k] = v
	}

	secret.UpdatedAt = time.Now()

	// Update in memory
	m.secrets[name] = secret

	// Update in catalog if persistent and catalog is available
	if secret.Persistent && m.catalog != nil {
		if err := m.catalog.SetSecret(secret); err != nil {
			// Rollback
			m.secrets[name] = original

			return err
		}
	}

	return nil
}

// GetSecret finds the best matching secret for a URL and secret type.
func (m *manager) GetSecret(_ context.Context, url string, secretType SecretType) (*Secret, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var bestMatch *Secret
	var bestMatchLen int

	for name := range m.secrets {
		s := m.secrets[name]

		// Skip secrets of different types
		if s.Type != secretType {
			continue
		}

		matchLen := m.matchScope(s.Scope, url)
		if matchLen < 0 {
			continue
		}

		// Use longest prefix match (higher matchLen is better)
		// For global scope, matchLen is 0, so more specific matches take precedence
		if bestMatch == nil || matchLen > bestMatchLen {
			secretCopy := s
			bestMatch = &secretCopy
			bestMatchLen = matchLen
		}
	}

	if bestMatch == nil {
		return nil, ErrSecretNotFound
	}

	return bestMatch.Clone(), nil
}

// GetSecretByName retrieves a secret by its name.
func (m *manager) GetSecretByName(_ context.Context, name string) (*Secret, error) {
	if name == "" {
		return nil, ErrInvalidSecretName
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	secret, exists := m.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	return secret.Clone(), nil
}

// ListSecrets lists all secrets, optionally filtered by scope.
func (m *manager) ListSecrets(_ context.Context, scope SecretScope) ([]Secret, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Secret, 0, len(m.secrets))

	for name := range m.secrets {
		s := m.secrets[name]

		// If scope filter is specified, check if it matches
		if scope.Type != "" && !m.scopeMatches(s.Scope, scope) {
			continue
		}

		result = append(result, *s.Clone())
	}

	return result, nil
}

// matchScope returns the match length for a scope against a URL.
// Returns -1 if there's no match, 0 for global scope, or the prefix length for path/host scope.
func (*manager) matchScope(scope SecretScope, url string) int {
	switch scope.Type {
	case ScopeGlobal:
		// Global scope matches all URLs
		return 0

	case ScopePath:
		// Path scope matches if the URL has the same prefix
		if strings.HasPrefix(strings.ToLower(url), strings.ToLower(scope.Prefix)) {
			return len(scope.Prefix)
		}

		return -1

	case ScopeHost:
		// Host scope matches if the URL has the same host/bucket
		// Extract host from URL for comparison
		urlHost := extractHost(url)
		scopeHost := extractHost(scope.Prefix)

		if strings.EqualFold(urlHost, scopeHost) {
			return len(scopeHost)
		}

		return -1

	default:
		return -1
	}
}

// scopeMatches checks if a secret's scope matches the filter scope.
func (*manager) scopeMatches(secretScope, filterScope SecretScope) bool {
	if filterScope.Type == "" {
		return true
	}

	if secretScope.Type != filterScope.Type {
		return false
	}

	if filterScope.Prefix == "" {
		return true
	}

	return strings.EqualFold(secretScope.Prefix, filterScope.Prefix)
}

// extractHost extracts the host/bucket from a URL.
func extractHost(url string) string {
	// Remove scheme if present
	rest := url
	if idx := strings.Index(url, "://"); idx >= 0 {
		rest = url[idx+schemeSeparatorLen:]
	}

	// Get the host part (before the first slash or query string)
	if idx := strings.IndexAny(rest, "/?#"); idx >= 0 {
		return rest[:idx]
	}

	return rest
}

// LoadFromCatalog loads all persistent secrets from the catalog into memory.
func (m *manager) LoadFromCatalog() error {
	if m.catalog == nil {
		return nil
	}

	secrets, err := m.catalog.ListSecrets()
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range secrets {
		m.secrets[secrets[i].Name] = secrets[i]
	}

	return nil
}

// Verify manager implements Manager interface.
var _ Manager = (*manager)(nil)
