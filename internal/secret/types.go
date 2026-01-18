// Package secret provides secret management for cloud storage credentials.
// It implements DuckDB-compatible secret storage with path-based scope matching
// for S3, GCS, Azure, and HTTP credentials.
package secret

import (
	"time"
)

// SecretType represents the type of cloud service the secret is for.
type SecretType string

// Supported secret types for cloud storage.
const (
	SecretTypeS3          SecretType = "S3"
	SecretTypeHTTP        SecretType = "HTTP"
	SecretTypeAzure       SecretType = "AZURE"
	SecretTypeGCS         SecretType = "GCS"
	SecretTypeHuggingFace SecretType = "HUGGINGFACE"
)

// ProviderType represents the credential provider type.
type ProviderType string

// Supported credential provider types.
const (
	// ProviderConfig uses explicitly provided credentials (access key, secret key).
	ProviderConfig ProviderType = "CONFIG"
	// ProviderEnv uses credentials from environment variables.
	ProviderEnv ProviderType = "ENV"
	// ProviderCredentialChain uses the default credential provider chain.
	ProviderCredentialChain ProviderType = "CREDENTIAL_CHAIN"
	// ProviderIAM uses IAM role credentials (e.g., from EC2 instance metadata).
	ProviderIAM ProviderType = "IAM"
)

// ScopeType represents the type of secret scope.
type ScopeType string

// Supported scope types for secret matching.
const (
	// ScopeGlobal matches all URLs of the secret type.
	ScopeGlobal ScopeType = "GLOBAL"
	// ScopePath matches URLs with a specific path prefix (e.g., s3://bucket/path).
	ScopePath ScopeType = "PATH"
	// ScopeHost matches URLs with a specific host (e.g., s3://bucket).
	ScopeHost ScopeType = "HOST"
)

// SecretScope defines the scope for which a secret applies.
type SecretScope struct {
	// Type is the scope type (GLOBAL, PATH, or HOST).
	Type ScopeType
	// Prefix is the URL prefix this scope matches (for PATH and HOST scopes).
	Prefix string
}

// SecretOptions contains key-value pairs of secret configuration options.
// Common options include:
//   - key_id: AWS access key ID
//   - secret: AWS secret access key
//   - session_token: AWS session token
//   - region: AWS region
//   - endpoint: Custom endpoint URL
//   - account_name: Azure storage account name
//   - account_key: Azure storage account key
//   - connection_string: Azure connection string
//   - service_account_json: GCS service account JSON
type SecretOptions map[string]string

// Secret represents a stored credential secret.
type Secret struct {
	// Name is the unique identifier for the secret.
	Name string
	// Type is the type of cloud service (S3, GCS, AZURE, HTTP, HUGGINGFACE).
	Type SecretType
	// Provider is the credential provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM).
	Provider ProviderType
	// Scope defines the URL scope this secret applies to.
	Scope SecretScope
	// Options contains the secret configuration options.
	Options SecretOptions
	// Persistent indicates whether the secret should be stored persistently.
	Persistent bool
	// CreatedAt is the time the secret was created.
	CreatedAt time.Time
	// UpdatedAt is the time the secret was last updated.
	UpdatedAt time.Time
}

// Clone creates a deep copy of the secret.
func (s *Secret) Clone() *Secret {
	clone := &Secret{
		Name:       s.Name,
		Type:       s.Type,
		Provider:   s.Provider,
		Scope:      s.Scope,
		Options:    make(SecretOptions, len(s.Options)),
		Persistent: s.Persistent,
		CreatedAt:  s.CreatedAt,
		UpdatedAt:  s.UpdatedAt,
	}

	for k, v := range s.Options {
		clone.Options[k] = v
	}

	return clone
}

// GetOption returns the value of an option, or empty string if not set.
func (s *Secret) GetOption(key string) string {
	if s.Options == nil {
		return ""
	}

	return s.Options[key]
}

// SetOption sets an option value.
func (s *Secret) SetOption(key, value string) {
	if s.Options == nil {
		s.Options = make(SecretOptions)
	}

	s.Options[key] = value
}

// HasOption returns true if the option is set.
func (s *Secret) HasOption(key string) bool {
	if s.Options == nil {
		return false
	}

	_, ok := s.Options[key]

	return ok
}

// Common secret option keys.
const (
	// S3/AWS options
	OptionKeyID        = "key_id"
	OptionSecret       = "secret"
	OptionSessionToken = "session_token"
	OptionRegion       = "region"
	OptionEndpoint     = "endpoint"
	OptionURLStyle     = "url_style"
	OptionUseSsl       = "use_ssl"

	// Azure options
	OptionAccountName      = "account_name"
	OptionAccountKey       = "account_key"
	OptionConnectionString = "connection_string"
	OptionTenantID         = "tenant_id"
	OptionClientID         = "client_id"
	OptionClientSecret     = "client_secret"

	// GCS options
	OptionServiceAccountJSON = "service_account_json"
	OptionProjectID          = "project_id"

	// HTTP options
	OptionBearerToken  = "bearer_token"
	OptionExtraHeaders = "extra_http_headers"
)
