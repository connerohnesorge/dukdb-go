// Package secret provides secret management for cloud storage credentials.
package secret

import (
	"context"
	"strings"
	"time"
)

// BindCreateSecret binds a CREATE SECRET statement to a Secret.
// It parses the statement options and creates a Secret object
// that can be stored in the SecretManager.
func BindCreateSecret(
	ctx context.Context,
	name string,
	secretType string,
	provider string,
	scope string,
	persistent bool,
	options map[string]string,
) (*Secret, error) {
	// Validate required fields
	if name == "" {
		return nil, ErrInvalidSecretName
	}

	// Normalize secret type
	normalizedType := SecretType(strings.ToUpper(secretType))
	if !isValidSecretType(normalizedType) {
		return nil, ErrInvalidSecretType
	}

	// Normalize provider (default to CONFIG if not specified)
	normalizedProvider := ProviderConfig
	if provider != "" {
		normalizedProvider = ProviderType(strings.ToUpper(provider))
	}

	// Parse scope
	secretScope := ParseScope(scope)

	// Create the secret
	now := time.Now()
	secret := &Secret{
		Name:       name,
		Type:       normalizedType,
		Provider:   normalizedProvider,
		Scope:      secretScope,
		Options:    make(SecretOptions, len(options)),
		Persistent: persistent,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	// Copy options
	for k, v := range options {
		secret.Options[k] = v
	}

	return secret, nil
}

// BindDropSecret validates a DROP SECRET statement.
// It returns the secret name and whether IF EXISTS was specified.
func BindDropSecret(name string, ifExists bool) (string, bool, error) {
	if name == "" {
		return "", false, ErrInvalidSecretName
	}

	return name, ifExists, nil
}

// BindAlterSecret validates an ALTER SECRET statement and returns
// the options to update.
func BindAlterSecret(name string, options map[string]string) (string, SecretOptions, error) {
	if name == "" {
		return "", nil, ErrInvalidSecretName
	}

	if len(options) == 0 {
		return "", nil, ErrInvalidSecretName
	}

	secretOptions := make(SecretOptions, len(options))
	for k, v := range options {
		secretOptions[k] = v
	}

	return name, secretOptions, nil
}

// ParseScope parses a scope string and returns a SecretScope.
// Supported formats:
//   - Empty string: Global scope
//   - "s3://bucket/path": Path scope for S3
//   - "gs://bucket/path": Path scope for GCS
//   - "azure://container/path": Path scope for Azure
//   - "https://example.com/path": Path scope for HTTP
//   - Other: Host scope
func ParseScope(scope string) SecretScope {
	if scope == "" {
		return SecretScope{Type: ScopeGlobal}
	}

	// Check for known URL schemes
	knownSchemes := []string{
		"s3://",
		"s3a://",
		"s3n://",
		"gs://",
		"gcs://",
		"azure://",
		"az://",
		"http://",
		"https://",
	}
	for _, scheme := range knownSchemes {
		if strings.HasPrefix(strings.ToLower(scope), scheme) {
			return SecretScope{
				Type:   ScopePath,
				Prefix: scope,
			}
		}
	}

	// Default to host scope for other formats
	return SecretScope{
		Type:   ScopeHost,
		Prefix: scope,
	}
}

// isValidSecretType checks if a secret type is valid.
func isValidSecretType(t SecretType) bool {
	switch t {
	case SecretTypeS3, SecretTypeHTTP, SecretTypeAzure, SecretTypeGCS, SecretTypeHuggingFace:
		return true
	default:
		return false
	}
}

// LookupSecretForURL finds the best matching secret for a given URL.
// This is a helper function that wraps the Manager's GetSecret method.
func LookupSecretForURL(ctx context.Context, mgr Manager, url string) (*Secret, error) {
	// Determine secret type from URL scheme
	secretType := SecretTypeFromURL(url)
	if secretType == "" {
		return nil, ErrSecretNotFound
	}

	return mgr.GetSecret(ctx, url, secretType)
}

// SecretTypeFromURL determines the secret type from a URL.
func SecretTypeFromURL(url string) SecretType {
	lowerURL := strings.ToLower(url)

	switch {
	case strings.HasPrefix(lowerURL, "s3://"),
		strings.HasPrefix(lowerURL, "s3a://"),
		strings.HasPrefix(lowerURL, "s3n://"):
		return SecretTypeS3
	case strings.HasPrefix(lowerURL, "gs://"),
		strings.HasPrefix(lowerURL, "gcs://"):
		return SecretTypeGCS
	case strings.HasPrefix(lowerURL, "azure://"),
		strings.HasPrefix(lowerURL, "az://"):
		return SecretTypeAzure
	case strings.HasPrefix(lowerURL, "http://"),
		strings.HasPrefix(lowerURL, "https://"):
		return SecretTypeHTTP
	case strings.HasPrefix(lowerURL, "hf://"),
		strings.HasPrefix(lowerURL, "huggingface://"):
		return SecretTypeHuggingFace
	default:
		return ""
	}
}
