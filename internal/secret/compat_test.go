// Package secret provides secret management for cloud storage credentials.
// This file contains compatibility tests to verify dukdb-go's secret implementation
// is compatible with official DuckDB's secret syntax and behavior.
package secret

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDuckDBSecretSyntaxCompatibility tests that various CREATE SECRET syntaxes
// are supported and parse correctly, matching DuckDB's official syntax.
func TestDuckDBSecretSyntaxCompatibility(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		secretName  string
		secretType  string
		provider    string
		scope       string
		persistent  bool
		options     map[string]string
		description string
	}{
		{
			name:        "Basic CREATE SECRET",
			secretName:  "my_secret",
			secretType:  "S3",
			provider:    "CONFIG",
			scope:       "",
			persistent:  false,
			options:     map[string]string{},
			description: "CREATE SECRET my_secret (TYPE S3)",
		},
		{
			name:        "CREATE PERSISTENT SECRET",
			secretName:  "persistent_secret",
			secretType:  "S3",
			provider:    "CONFIG",
			scope:       "",
			persistent:  true,
			options:     map[string]string{"key_id": "test"},
			description: "CREATE PERSISTENT SECRET persistent_secret (TYPE S3, KEY_ID 'test')",
		},
		{
			name:        "CREATE TEMPORARY SECRET",
			secretName:  "temp_secret",
			secretType:  "S3",
			provider:    "CONFIG",
			scope:       "",
			persistent:  false,
			options:     map[string]string{},
			description: "CREATE TEMPORARY SECRET temp_secret (TYPE S3)",
		},
		{
			name:        "CREATE SECRET with SCOPE",
			secretName:  "scoped_secret",
			secretType:  "S3",
			provider:    "CONFIG",
			scope:       "s3://my-bucket/path/",
			persistent:  false,
			options:     map[string]string{},
			description: "CREATE SECRET scoped_secret (TYPE S3, SCOPE 's3://my-bucket/path/')",
		},
		{
			name:        "CREATE SECRET with all S3 options",
			secretName:  "full_s3_secret",
			secretType:  "S3",
			provider:    "CONFIG",
			scope:       "",
			persistent:  false,
			options: map[string]string{
				"key_id":        "AKIAIOSFODNN7EXAMPLE",
				"secret":        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"session_token": "FwoGZXIvYXdzEBYaDH...",
				"region":        "us-east-1",
				"endpoint":      "https://s3.amazonaws.com",
				"url_style":     "path",
			},
			description: "CREATE SECRET full_s3_secret (TYPE S3, KEY_ID ..., SECRET ..., etc.)",
		},
		{
			name:        "CREATE SECRET with ENV provider",
			secretName:  "env_secret",
			secretType:  "S3",
			provider:    "ENV",
			scope:       "",
			persistent:  false,
			options:     map[string]string{},
			description: "CREATE SECRET env_secret (TYPE S3, PROVIDER ENV)",
		},
		{
			name:        "CREATE SECRET with CREDENTIAL_CHAIN provider",
			secretName:  "chain_secret",
			secretType:  "S3",
			provider:    "CREDENTIAL_CHAIN",
			scope:       "",
			persistent:  false,
			options:     map[string]string{},
			description: "CREATE SECRET chain_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret, err := BindCreateSecret(
				ctx,
				tt.secretName,
				tt.secretType,
				tt.provider,
				tt.scope,
				tt.persistent,
				tt.options,
			)

			require.NoError(t, err, "Failed to bind secret for: %s", tt.description)
			require.NotNil(t, secret)

			assert.Equal(t, tt.secretName, secret.Name)
			assert.Equal(t, SecretType(strings.ToUpper(tt.secretType)), secret.Type)
			assert.Equal(t, tt.persistent, secret.Persistent)

			// Verify provider is normalized
			expectedProvider := tt.provider
			if expectedProvider == "" {
				expectedProvider = "CONFIG"
			}
			assert.Equal(t, ProviderType(strings.ToUpper(expectedProvider)), secret.Provider)

			// Verify scope is parsed correctly
			if tt.scope == "" {
				assert.Equal(t, ScopeGlobal, secret.Scope.Type)
			} else {
				assert.NotEqual(t, ScopeGlobal, secret.Scope.Type)
			}

			// Verify all options are stored
			for k, v := range tt.options {
				assert.Equal(t, v, secret.Options[k], "Option %s mismatch", k)
			}
		})
	}
}

// TestDuckDBSecretTypes tests that all secret types supported by DuckDB
// are recognized and can be created.
func TestDuckDBSecretTypes(t *testing.T) {
	ctx := context.Background()

	// All secret types that DuckDB supports
	types := []struct {
		typeName    string
		description string
	}{
		{"S3", "Amazon S3 / S3-compatible storage"},
		{"GCS", "Google Cloud Storage"},
		{"AZURE", "Azure Blob Storage"},
		{"HTTP", "HTTP/HTTPS endpoints"},
		{"HUGGINGFACE", "HuggingFace Hub"},
	}

	for _, tt := range types {
		t.Run(tt.typeName, func(t *testing.T) {
			secret, err := BindCreateSecret(
				ctx,
				"test_"+strings.ToLower(tt.typeName),
				tt.typeName,
				"CONFIG",
				"",
				false,
				nil,
			)

			require.NoError(t, err, "Secret type %s should be valid", tt.typeName)
			assert.Equal(t, SecretType(tt.typeName), secret.Type)
		})
	}

	// Also test lowercase normalization
	for _, tt := range types {
		t.Run(tt.typeName+"_lowercase", func(t *testing.T) {
			secret, err := BindCreateSecret(
				ctx,
				"test_lower_"+strings.ToLower(tt.typeName),
				strings.ToLower(tt.typeName),
				"CONFIG",
				"",
				false,
				nil,
			)

			require.NoError(t, err, "Lowercase type %s should be normalized", tt.typeName)
			assert.Equal(t, SecretType(tt.typeName), secret.Type)
		})
	}
}

// TestDuckDBSecretProviders tests that all provider types supported by DuckDB
// are recognized and can be used.
func TestDuckDBSecretProviders(t *testing.T) {
	ctx := context.Background()

	// All provider types that DuckDB supports
	providers := []struct {
		provider    string
		description string
	}{
		{"CONFIG", "Explicit configuration via options"},
		{"ENV", "Environment variables"},
		{"CREDENTIAL_CHAIN", "Default credential provider chain"},
		// Note: IAM is an additional provider we support
		{"IAM", "IAM role credentials (EC2 instance metadata)"},
	}

	for _, tt := range providers {
		t.Run(tt.provider, func(t *testing.T) {
			secret, err := BindCreateSecret(
				ctx,
				"test_provider_"+strings.ToLower(tt.provider),
				"S3",
				tt.provider,
				"",
				false,
				nil,
			)

			require.NoError(t, err, "Provider %s should be valid", tt.provider)
			assert.Equal(t, ProviderType(tt.provider), secret.Provider)
		})
	}

	// Test lowercase normalization
	for _, tt := range providers {
		t.Run(tt.provider+"_lowercase", func(t *testing.T) {
			secret, err := BindCreateSecret(
				ctx,
				"test_lower_provider_"+strings.ToLower(tt.provider),
				"S3",
				strings.ToLower(tt.provider),
				"",
				false,
				nil,
			)

			require.NoError(t, err, "Lowercase provider %s should be normalized", tt.provider)
			assert.Equal(t, ProviderType(tt.provider), secret.Provider)
		})
	}

	// Test empty provider defaults to CONFIG
	t.Run("empty_provider_defaults_to_config", func(t *testing.T) {
		secret, err := BindCreateSecret(
			ctx,
			"test_default_provider",
			"S3",
			"",
			"",
			false,
			nil,
		)

		require.NoError(t, err)
		assert.Equal(t, ProviderConfig, secret.Provider, "Empty provider should default to CONFIG")
	})
}

// TestDuckDBSecretOptions tests that all option names used by DuckDB
// for different secret types are recognized.
func TestDuckDBSecretOptions(t *testing.T) {
	ctx := context.Background()

	t.Run("S3_options", func(t *testing.T) {
		// DuckDB S3 options: KEY_ID, SECRET, REGION, SESSION_TOKEN, ENDPOINT, URL_STYLE, USE_SSL
		s3Options := map[string]string{
			"key_id":        "AKIAIOSFODNN7EXAMPLE",
			"secret":        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"region":        "us-east-1",
			"session_token": "FwoGZXIvYXdzEBYaDHKM...",
			"endpoint":      "https://s3.amazonaws.com",
			"url_style":     "vhost",
			"use_ssl":       "true",
		}

		secret, err := BindCreateSecret(
			ctx,
			"s3_all_options",
			"S3",
			"CONFIG",
			"",
			false,
			s3Options,
		)

		require.NoError(t, err)

		// Verify all options are stored
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", secret.GetOption(OptionKeyID))
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", secret.GetOption(OptionSecret))
		assert.Equal(t, "us-east-1", secret.GetOption(OptionRegion))
		assert.Equal(t, "FwoGZXIvYXdzEBYaDHKM...", secret.GetOption(OptionSessionToken))
		assert.Equal(t, "https://s3.amazonaws.com", secret.GetOption(OptionEndpoint))
		assert.Equal(t, "vhost", secret.GetOption(OptionURLStyle))
		assert.Equal(t, "true", secret.GetOption(OptionUseSsl))
	})

	t.Run("GCS_options", func(t *testing.T) {
		// DuckDB GCS options: KEY_FILE (service account JSON), PROJECT_ID
		gcsOptions := map[string]string{
			"service_account_json": `{"type": "service_account", "project_id": "my-project"}`,
			"project_id":           "my-project-id",
		}

		secret, err := BindCreateSecret(
			ctx,
			"gcs_all_options",
			"GCS",
			"CONFIG",
			"",
			false,
			gcsOptions,
		)

		require.NoError(t, err)
		assert.Equal(t, `{"type": "service_account", "project_id": "my-project"}`, secret.GetOption(OptionServiceAccountJSON))
		assert.Equal(t, "my-project-id", secret.GetOption(OptionProjectID))
	})

	t.Run("Azure_options", func(t *testing.T) {
		// DuckDB Azure options: ACCOUNT_NAME, ACCOUNT_KEY, CONNECTION_STRING, TENANT_ID, CLIENT_ID, CLIENT_SECRET
		azureOptions := map[string]string{
			"account_name":      "mystorageaccount",
			"account_key":       "base64encodedkey==",
			"connection_string": "DefaultEndpointsProtocol=https;AccountName=...",
			"tenant_id":         "00000000-0000-0000-0000-000000000000",
			"client_id":         "11111111-1111-1111-1111-111111111111",
			"client_secret":     "my-client-secret",
		}

		secret, err := BindCreateSecret(
			ctx,
			"azure_all_options",
			"AZURE",
			"CONFIG",
			"",
			false,
			azureOptions,
		)

		require.NoError(t, err)
		assert.Equal(t, "mystorageaccount", secret.GetOption(OptionAccountName))
		assert.Equal(t, "base64encodedkey==", secret.GetOption(OptionAccountKey))
		assert.Equal(t, "DefaultEndpointsProtocol=https;AccountName=...", secret.GetOption(OptionConnectionString))
		assert.Equal(t, "00000000-0000-0000-0000-000000000000", secret.GetOption(OptionTenantID))
		assert.Equal(t, "11111111-1111-1111-1111-111111111111", secret.GetOption(OptionClientID))
		assert.Equal(t, "my-client-secret", secret.GetOption(OptionClientSecret))
	})

	t.Run("HTTP_options", func(t *testing.T) {
		// DuckDB HTTP options: BEARER_TOKEN, EXTRA_HTTP_HEADERS
		httpOptions := map[string]string{
			"bearer_token":       "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
			"extra_http_headers": `{"Authorization": "Bearer token", "X-Custom": "value"}`,
		}

		secret, err := BindCreateSecret(
			ctx,
			"http_all_options",
			"HTTP",
			"CONFIG",
			"",
			false,
			httpOptions,
		)

		require.NoError(t, err)
		assert.Equal(t, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...", secret.GetOption(OptionBearerToken))
		assert.Equal(t, `{"Authorization": "Bearer token", "X-Custom": "value"}`, secret.GetOption(OptionExtraHeaders))
	})
}

// TestDuckDBSecretScopeMatching tests that scope matching behavior
// matches DuckDB's implementation.
func TestDuckDBSecretScopeMatching(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secrets with different scopes
	secrets := []Secret{
		{
			Name:  "global_s3",
			Type:  SecretTypeS3,
			Scope: SecretScope{Type: ScopeGlobal},
			Options: SecretOptions{
				"key_id": "global_key",
			},
		},
		{
			Name: "bucket_scoped",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://my-bucket/",
			},
			Options: SecretOptions{
				"key_id": "bucket_key",
			},
		},
		{
			Name: "path_scoped",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://my-bucket/data/",
			},
			Options: SecretOptions{
				"key_id": "path_key",
			},
		},
		{
			Name: "deep_path_scoped",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://my-bucket/data/parquet/",
			},
			Options: SecretOptions{
				"key_id": "deep_path_key",
			},
		},
	}

	for _, s := range secrets {
		err := manager.CreateSecret(ctx, s)
		require.NoError(t, err)
	}

	testCases := []struct {
		name           string
		url            string
		secretType     SecretType
		expectedSecret string
		expectedKeyID  string
	}{
		{
			name:           "Global scope matches unrelated bucket",
			url:            "s3://other-bucket/file.csv",
			secretType:     SecretTypeS3,
			expectedSecret: "global_s3",
			expectedKeyID:  "global_key",
		},
		{
			name:           "Bucket scope matches bucket root",
			url:            "s3://my-bucket/file.csv",
			secretType:     SecretTypeS3,
			expectedSecret: "bucket_scoped",
			expectedKeyID:  "bucket_key",
		},
		{
			name:           "Path scope matches data directory",
			url:            "s3://my-bucket/data/file.csv",
			secretType:     SecretTypeS3,
			expectedSecret: "path_scoped",
			expectedKeyID:  "path_key",
		},
		{
			name:           "Deep path scope matches parquet directory",
			url:            "s3://my-bucket/data/parquet/file.parquet",
			secretType:     SecretTypeS3,
			expectedSecret: "deep_path_scoped",
			expectedKeyID:  "deep_path_key",
		},
		{
			name:           "Path scope beats bucket scope for data path",
			url:            "s3://my-bucket/data/subdir/file.csv",
			secretType:     SecretTypeS3,
			expectedSecret: "path_scoped",
			expectedKeyID:  "path_key",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			secret, err := manager.GetSecret(ctx, tc.url, tc.secretType)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedSecret, secret.Name)
			assert.Equal(t, tc.expectedKeyID, secret.GetOption(OptionKeyID))
		})
	}
}

// TestDuckDBSecretPrecedence tests that more specific scopes take precedence
// over less specific ones, matching DuckDB's longest-prefix-match behavior.
func TestDuckDBSecretPrecedence(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secrets in non-intuitive order to test precedence
	secrets := []Secret{
		{
			Name:  "least_specific",
			Type:  SecretTypeS3,
			Scope: SecretScope{Type: ScopeGlobal},
			Options: SecretOptions{
				"marker": "global",
			},
		},
		{
			Name: "more_specific",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://bucket/",
			},
			Options: SecretOptions{
				"marker": "bucket",
			},
		},
		{
			Name: "most_specific",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://bucket/path/to/data/",
			},
			Options: SecretOptions{
				"marker": "deep_path",
			},
		},
	}

	// Create in reverse order to ensure precedence isn't based on creation order
	for i := len(secrets) - 1; i >= 0; i-- {
		err := manager.CreateSecret(ctx, secrets[i])
		require.NoError(t, err)
	}

	testCases := []struct {
		name           string
		url            string
		expectedMarker string
	}{
		{
			name:           "Most specific wins for deep path",
			url:            "s3://bucket/path/to/data/file.parquet",
			expectedMarker: "deep_path",
		},
		{
			name:           "Bucket scope wins for shallow path",
			url:            "s3://bucket/other/file.csv",
			expectedMarker: "bucket",
		},
		{
			name:           "Global wins for different bucket",
			url:            "s3://other-bucket/file.csv",
			expectedMarker: "global",
		},
		{
			name:           "Path that matches both bucket and path prefix picks longer",
			url:            "s3://bucket/path/to/data/subdir/nested/file.json",
			expectedMarker: "deep_path",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			secret, err := manager.GetSecret(ctx, tc.url, SecretTypeS3)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedMarker, secret.GetOption("marker"),
				"Expected marker %q but got %q for URL %s",
				tc.expectedMarker, secret.GetOption("marker"), tc.url)
		})
	}
}

// TestDuckDBDropSecretCompatibility tests DROP SECRET behavior matches DuckDB.
func TestDuckDBDropSecretCompatibility(t *testing.T) {
	ctx := context.Background()

	t.Run("DROP SECRET removes secret", func(t *testing.T) {
		manager := NewManager(nil)

		// Create a secret
		err := manager.CreateSecret(ctx, Secret{
			Name: "to_drop",
			Type: SecretTypeS3,
		})
		require.NoError(t, err)

		// Verify it exists
		_, err = manager.GetSecretByName(ctx, "to_drop")
		require.NoError(t, err)

		// Drop it
		name, ifExists, err := BindDropSecret("to_drop", false)
		require.NoError(t, err)
		assert.Equal(t, "to_drop", name)
		assert.False(t, ifExists)

		err = manager.DropSecret(ctx, name, ifExists)
		require.NoError(t, err)

		// Verify it's gone
		_, err = manager.GetSecretByName(ctx, "to_drop")
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})

	t.Run("DROP SECRET IF EXISTS succeeds for non-existent", func(t *testing.T) {
		manager := NewManager(nil)

		name, ifExists, err := BindDropSecret("nonexistent", true)
		require.NoError(t, err)
		assert.True(t, ifExists)

		err = manager.DropSecret(ctx, name, ifExists)
		assert.NoError(t, err, "DROP SECRET IF EXISTS should not error for missing secret")
	})

	t.Run("DROP SECRET without IF EXISTS fails for non-existent", func(t *testing.T) {
		manager := NewManager(nil)

		err := manager.DropSecret(ctx, "nonexistent", false)
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})
}

// TestDuckDBCreateOrReplaceCompatibility tests CREATE OR REPLACE SECRET behavior.
// Note: This tests the semantic - actual CREATE OR REPLACE requires first
// checking existence, then either creating or updating.
func TestDuckDBCreateOrReplaceCompatibility(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create initial secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "replaceable",
		Type: SecretTypeS3,
		Options: SecretOptions{
			"key_id": "original_key",
		},
	})
	require.NoError(t, err)

	// Verify original
	secret, err := manager.GetSecretByName(ctx, "replaceable")
	require.NoError(t, err)
	assert.Equal(t, "original_key", secret.GetOption("key_id"))

	// Simulating CREATE OR REPLACE: drop and recreate
	err = manager.DropSecret(ctx, "replaceable", true)
	require.NoError(t, err)

	err = manager.CreateSecret(ctx, Secret{
		Name: "replaceable",
		Type: SecretTypeS3,
		Options: SecretOptions{
			"key_id": "new_key",
		},
	})
	require.NoError(t, err)

	// Verify replacement
	secret, err = manager.GetSecretByName(ctx, "replaceable")
	require.NoError(t, err)
	assert.Equal(t, "new_key", secret.GetOption("key_id"))
}

// TestDuckDBCaseInsensitiveScopes tests that scope matching is case-insensitive.
func TestDuckDBCaseInsensitiveScopes(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret with lowercase scope
	err := manager.CreateSecret(ctx, Secret{
		Name: "case_test",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://my-bucket/path/",
		},
	})
	require.NoError(t, err)

	testCases := []struct {
		url       string
		shouldErr bool
	}{
		{"s3://my-bucket/path/file.csv", false},
		{"s3://My-Bucket/Path/file.csv", false},
		{"S3://MY-BUCKET/PATH/file.csv", false},
		{"s3://MY-bucket/PATH/nested/file.csv", false},
		{"s3://other-bucket/path/file.csv", true},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			_, err := manager.GetSecret(ctx, tc.url, SecretTypeS3)
			if tc.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDuckDBMultipleSecretTypes tests that secrets of different types
// are isolated and don't match for wrong types.
func TestDuckDBMultipleSecretTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("global_secrets_match_any_URL_of_same_type", func(t *testing.T) {
		manager := NewManager(nil)

		// Create global secrets for different types
		types := []SecretType{SecretTypeS3, SecretTypeGCS, SecretTypeAzure, SecretTypeHTTP}
		for _, st := range types {
			err := manager.CreateSecret(ctx, Secret{
				Name:  "secret_" + string(st),
				Type:  st,
				Scope: SecretScope{Type: ScopeGlobal},
			})
			require.NoError(t, err)
		}

		// Test that S3 secret type returns S3 secret
		secret, err := manager.GetSecret(ctx, "s3://bucket/file", SecretTypeS3)
		require.NoError(t, err)
		assert.Equal(t, SecretTypeS3, secret.Type)

		// Test that GCS secret type returns GCS secret
		secret, err = manager.GetSecret(ctx, "gs://bucket/file", SecretTypeGCS)
		require.NoError(t, err)
		assert.Equal(t, SecretTypeGCS, secret.Type)

		// Test that Azure secret type returns Azure secret
		secret, err = manager.GetSecret(ctx, "azure://container/blob", SecretTypeAzure)
		require.NoError(t, err)
		assert.Equal(t, SecretTypeAzure, secret.Type)

		// Test that HTTP secret type returns HTTP secret
		secret, err = manager.GetSecret(ctx, "https://example.com/file", SecretTypeHTTP)
		require.NoError(t, err)
		assert.Equal(t, SecretTypeHTTP, secret.Type)

		// NOTE: Global scope means the secret matches any URL when the correct type is specified.
		// A global GCS secret will match ANY URL if you ask for SecretTypeGCS.
		// This is expected DuckDB behavior - global scope is truly global.
		secret, err = manager.GetSecret(ctx, "s3://bucket/file", SecretTypeGCS)
		require.NoError(t, err)
		assert.Equal(t, SecretTypeGCS, secret.Type)
	})

	t.Run("missing_type_returns_not_found", func(t *testing.T) {
		manager := NewManager(nil)

		// Create only an S3 secret
		err := manager.CreateSecret(ctx, Secret{
			Name:  "s3_only",
			Type:  SecretTypeS3,
			Scope: SecretScope{Type: ScopeGlobal},
		})
		require.NoError(t, err)

		// Looking for GCS type should fail - no GCS secrets exist
		_, err = manager.GetSecret(ctx, "gs://bucket/file", SecretTypeGCS)
		assert.ErrorIs(t, err, ErrSecretNotFound)

		// Looking for Azure type should fail - no Azure secrets exist
		_, err = manager.GetSecret(ctx, "azure://container/blob", SecretTypeAzure)
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})

	t.Run("scoped_secrets_respect_type_and_path", func(t *testing.T) {
		manager := NewManager(nil)

		// Create scoped secrets for different types
		err := manager.CreateSecret(ctx, Secret{
			Name: "s3_bucket_secret",
			Type: SecretTypeS3,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "s3://my-bucket/",
			},
		})
		require.NoError(t, err)

		err = manager.CreateSecret(ctx, Secret{
			Name: "gcs_bucket_secret",
			Type: SecretTypeGCS,
			Scope: SecretScope{
				Type:   ScopePath,
				Prefix: "gs://my-bucket/",
			},
		})
		require.NoError(t, err)

		// S3 secret matches S3 URL in scope
		secret, err := manager.GetSecret(ctx, "s3://my-bucket/file", SecretTypeS3)
		require.NoError(t, err)
		assert.Equal(t, "s3_bucket_secret", secret.Name)

		// GCS secret matches GCS URL in scope
		secret, err = manager.GetSecret(ctx, "gs://my-bucket/file", SecretTypeGCS)
		require.NoError(t, err)
		assert.Equal(t, "gcs_bucket_secret", secret.Name)

		// S3 secret doesn't match different bucket
		_, err = manager.GetSecret(ctx, "s3://other-bucket/file", SecretTypeS3)
		assert.ErrorIs(t, err, ErrSecretNotFound)

		// Asking for GCS type with S3 URL doesn't match (scope doesn't match)
		_, err = manager.GetSecret(ctx, "s3://my-bucket/file", SecretTypeGCS)
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})
}

// TestDuckDBSecretURLSchemes tests that secret type detection from URL works
// for all supported URL schemes.
func TestDuckDBSecretURLSchemes(t *testing.T) {
	testCases := []struct {
		url          string
		expectedType SecretType
	}{
		// S3 schemes
		{"s3://bucket/key", SecretTypeS3},
		{"s3a://bucket/key", SecretTypeS3},
		{"s3n://bucket/key", SecretTypeS3},
		{"S3://bucket/key", SecretTypeS3}, // uppercase

		// GCS schemes
		{"gs://bucket/object", SecretTypeGCS},
		{"gcs://bucket/object", SecretTypeGCS},
		{"GS://bucket/object", SecretTypeGCS}, // uppercase

		// Azure schemes
		{"azure://container/blob", SecretTypeAzure},
		{"az://container/blob", SecretTypeAzure},
		{"AZURE://container/blob", SecretTypeAzure}, // uppercase

		// HTTP schemes
		{"http://example.com/file", SecretTypeHTTP},
		{"https://example.com/file", SecretTypeHTTP},
		{"HTTP://example.com/file", SecretTypeHTTP}, // uppercase

		// HuggingFace schemes
		{"hf://dataset/file", SecretTypeHuggingFace},
		{"huggingface://dataset/file", SecretTypeHuggingFace},

		// Unknown should return empty
		{"ftp://server/file", ""},
		{"sftp://server/file", ""},
		{"file:///path/to/file", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			secretType := SecretTypeFromURL(tc.url)
			assert.Equal(t, tc.expectedType, secretType)
		})
	}
}

// TestDuckDBParseScopeCompatibility tests scope parsing for various URL formats.
func TestDuckDBParseScopeCompatibility(t *testing.T) {
	testCases := []struct {
		scope          string
		expectedType   ScopeType
		expectedPrefix string
	}{
		// Empty is global
		{"", ScopeGlobal, ""},

		// S3 URLs
		{"s3://bucket", ScopePath, "s3://bucket"},
		{"s3://bucket/", ScopePath, "s3://bucket/"},
		{"s3://bucket/path/to/prefix/", ScopePath, "s3://bucket/path/to/prefix/"},

		// S3A/S3N variants
		{"s3a://bucket/path", ScopePath, "s3a://bucket/path"},
		{"s3n://bucket/path", ScopePath, "s3n://bucket/path"},

		// GCS URLs
		{"gs://bucket/object", ScopePath, "gs://bucket/object"},
		{"gcs://bucket/object", ScopePath, "gcs://bucket/object"},

		// Azure URLs
		{"azure://container/blob", ScopePath, "azure://container/blob"},
		{"az://container/blob", ScopePath, "az://container/blob"},

		// HTTP URLs
		{"http://example.com/path", ScopePath, "http://example.com/path"},
		{"https://api.example.com/v1/", ScopePath, "https://api.example.com/v1/"},

		// Unknown schemes become host scope
		{"ftp://server/path", ScopeHost, "ftp://server/path"},
	}

	for _, tc := range testCases {
		t.Run(tc.scope, func(t *testing.T) {
			scope := ParseScope(tc.scope)
			assert.Equal(t, tc.expectedType, scope.Type)
			assert.Equal(t, tc.expectedPrefix, scope.Prefix)
		})
	}
}

// TestDuckDBAlterSecretCompatibility tests ALTER SECRET behavior.
func TestDuckDBAlterSecretCompatibility(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "alterable",
		Type: SecretTypeS3,
		Options: SecretOptions{
			"key_id": "original_key",
			"region": "us-east-1",
		},
	})
	require.NoError(t, err)

	// Alter secret
	name, opts, err := BindAlterSecret("alterable", map[string]string{
		"key_id": "new_key",
		"secret": "new_secret",
	})
	require.NoError(t, err)

	err = manager.AlterSecret(ctx, name, opts)
	require.NoError(t, err)

	// Verify changes
	secret, err := manager.GetSecretByName(ctx, "alterable")
	require.NoError(t, err)
	assert.Equal(t, "new_key", secret.GetOption("key_id"))
	assert.Equal(t, "new_secret", secret.GetOption("secret"))
	assert.Equal(t, "us-east-1", secret.GetOption("region"), "Unmodified options should be preserved")
}

// TestDuckDBWhichSecretCompatibility tests that which_secret lookup behavior
// is compatible with DuckDB.
func TestDuckDBWhichSecretCompatibility(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create multiple secrets with different scopes
	err := manager.CreateSecret(ctx, Secret{
		Name: "global_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type: ScopeGlobal,
		},
	})
	require.NoError(t, err)

	err = manager.CreateSecret(ctx, Secret{
		Name: "bucket_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://specific-bucket/",
		},
	})
	require.NoError(t, err)

	// Test which_secret behavior via LookupSecretForURL
	t.Run("finds most specific match", func(t *testing.T) {
		secret, err := LookupSecretForURL(ctx, manager, "s3://specific-bucket/file.csv")
		require.NoError(t, err)
		assert.Equal(t, "bucket_secret", secret.Name)
	})

	t.Run("falls back to global", func(t *testing.T) {
		secret, err := LookupSecretForURL(ctx, manager, "s3://other-bucket/file.csv")
		require.NoError(t, err)
		assert.Equal(t, "global_secret", secret.Name)
	})

	t.Run("returns error for unknown type", func(t *testing.T) {
		_, err := LookupSecretForURL(ctx, manager, "ftp://server/file")
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})
}

// TestDuckDBInvalidInputHandling tests error handling for invalid inputs.
func TestDuckDBInvalidInputHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("empty secret name", func(t *testing.T) {
		_, err := BindCreateSecret(ctx, "", "S3", "CONFIG", "", false, nil)
		assert.ErrorIs(t, err, ErrInvalidSecretName)
	})

	t.Run("invalid secret type", func(t *testing.T) {
		_, err := BindCreateSecret(ctx, "test", "INVALID", "CONFIG", "", false, nil)
		assert.ErrorIs(t, err, ErrInvalidSecretType)
	})

	t.Run("drop empty name", func(t *testing.T) {
		_, _, err := BindDropSecret("", false)
		assert.ErrorIs(t, err, ErrInvalidSecretName)
	})

	t.Run("alter empty name", func(t *testing.T) {
		_, _, err := BindAlterSecret("", map[string]string{"key": "value"})
		assert.ErrorIs(t, err, ErrInvalidSecretName)
	})

	t.Run("alter empty options", func(t *testing.T) {
		_, _, err := BindAlterSecret("test", map[string]string{})
		assert.ErrorIs(t, err, ErrInvalidSecretName)
	})
}

// TestDuckDBPersistentSecretBehavior tests persistent vs temporary secret behavior.
func TestDuckDBPersistentSecretBehavior(t *testing.T) {
	ctx := context.Background()
	catalog := NewMemoryCatalog()
	manager := NewManager(catalog)

	t.Run("persistent secrets are stored in catalog", func(t *testing.T) {
		err := manager.CreateSecret(ctx, Secret{
			Name:       "persistent_test",
			Type:       SecretTypeS3,
			Persistent: true,
		})
		require.NoError(t, err)

		// Check catalog has the secret
		catalogSecret, err := catalog.GetSecret("persistent_test")
		require.NoError(t, err)
		assert.Equal(t, "persistent_test", catalogSecret.Name)
	})

	t.Run("temporary secrets are not in catalog", func(t *testing.T) {
		err := manager.CreateSecret(ctx, Secret{
			Name:       "temporary_test",
			Type:       SecretTypeS3,
			Persistent: false,
		})
		require.NoError(t, err)

		// Check catalog does NOT have the secret
		_, err = catalog.GetSecret("temporary_test")
		assert.ErrorIs(t, err, ErrSecretNotFound)

		// But manager should have it
		_, err = manager.GetSecretByName(ctx, "temporary_test")
		assert.NoError(t, err)
	})

	t.Run("dropping persistent removes from catalog", func(t *testing.T) {
		// Create persistent
		err := manager.CreateSecret(ctx, Secret{
			Name:       "to_drop_persistent",
			Type:       SecretTypeS3,
			Persistent: true,
		})
		require.NoError(t, err)

		// Verify in catalog
		_, err = catalog.GetSecret("to_drop_persistent")
		require.NoError(t, err)

		// Drop it
		err = manager.DropSecret(ctx, "to_drop_persistent", false)
		require.NoError(t, err)

		// Verify removed from catalog
		_, err = catalog.GetSecret("to_drop_persistent")
		assert.ErrorIs(t, err, ErrSecretNotFound)
	})
}

// TestDuckDBOptionConstants verifies the option constant values match expected DuckDB naming.
func TestDuckDBOptionConstants(t *testing.T) {
	// These constants should match DuckDB's expected option names
	// DuckDB uses snake_case for options internally
	assert.Equal(t, "key_id", OptionKeyID)
	assert.Equal(t, "secret", OptionSecret)
	assert.Equal(t, "session_token", OptionSessionToken)
	assert.Equal(t, "region", OptionRegion)
	assert.Equal(t, "endpoint", OptionEndpoint)
	assert.Equal(t, "url_style", OptionURLStyle)
	assert.Equal(t, "use_ssl", OptionUseSsl)

	assert.Equal(t, "account_name", OptionAccountName)
	assert.Equal(t, "account_key", OptionAccountKey)
	assert.Equal(t, "connection_string", OptionConnectionString)
	assert.Equal(t, "tenant_id", OptionTenantID)
	assert.Equal(t, "client_id", OptionClientID)
	assert.Equal(t, "client_secret", OptionClientSecret)

	assert.Equal(t, "service_account_json", OptionServiceAccountJSON)
	assert.Equal(t, "project_id", OptionProjectID)

	assert.Equal(t, "bearer_token", OptionBearerToken)
	assert.Equal(t, "extra_http_headers", OptionExtraHeaders)
}

// TestDuckDBSecretTypeConstants verifies the secret type constant values.
func TestDuckDBSecretTypeConstants(t *testing.T) {
	assert.Equal(t, SecretType("S3"), SecretTypeS3)
	assert.Equal(t, SecretType("GCS"), SecretTypeGCS)
	assert.Equal(t, SecretType("AZURE"), SecretTypeAzure)
	assert.Equal(t, SecretType("HTTP"), SecretTypeHTTP)
	assert.Equal(t, SecretType("HUGGINGFACE"), SecretTypeHuggingFace)
}

// TestDuckDBProviderTypeConstants verifies the provider type constant values.
func TestDuckDBProviderTypeConstants(t *testing.T) {
	assert.Equal(t, ProviderType("CONFIG"), ProviderConfig)
	assert.Equal(t, ProviderType("ENV"), ProviderEnv)
	assert.Equal(t, ProviderType("CREDENTIAL_CHAIN"), ProviderCredentialChain)
	assert.Equal(t, ProviderType("IAM"), ProviderIAM)
}

// TestDuckDBScopeTypeConstants verifies the scope type constant values.
func TestDuckDBScopeTypeConstants(t *testing.T) {
	assert.Equal(t, ScopeType("GLOBAL"), ScopeGlobal)
	assert.Equal(t, ScopeType("PATH"), ScopePath)
	assert.Equal(t, ScopeType("HOST"), ScopeHost)
}
