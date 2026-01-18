// Package compat provides compatibility testing between dukdb-go and the
// official DuckDB go-duckdb driver.
//
// This file contains spec compliance tests that document expected DuckDB
// behavior and verify that dukdb-go matches the specification. These tests
// run regardless of CGO availability.
package compat

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import dukdb-go driver
	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"

	"github.com/dukdb/dukdb-go/internal/secret"
)

// openDB opens an in-memory dukdb-go database for testing.
func openDB() (*sql.DB, error) {
	return sql.Open("dukdb", ":memory:")
}

// TestSpecDuckDBSecretSyntax documents and tests expected DuckDB secret syntax.
// Based on: https://duckdb.org/docs/configuration/secrets_manager
func TestSpecDuckDBSecretSyntax(t *testing.T) {
	ctx := context.Background()

	t.Run("CREATE_SECRET_syntax", func(t *testing.T) {
		// DuckDB Spec: CREATE [PERSISTENT|TEMPORARY] SECRET [IF NOT EXISTS] name
		//              [IN storage] (options)
		// Options include: TYPE, PROVIDER, SCOPE, and type-specific options

		testCases := []struct {
			name        string
			secretName  string
			secretType  string
			provider    string
			scope       string
			persistent  bool
			options     map[string]string
			expectError bool
		}{
			{
				name:       "minimal_secret",
				secretName: "minimal",
				secretType: "S3",
				options:    map[string]string{},
			},
			{
				name:       "s3_with_credentials",
				secretName: "s3_creds",
				secretType: "S3",
				provider:   "CONFIG",
				options: map[string]string{
					"key_id": "AKIAIOSFODNN7EXAMPLE",
					"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
			},
			{
				name:       "s3_with_region_and_endpoint",
				secretName: "s3_region",
				secretType: "S3",
				provider:   "CONFIG",
				options: map[string]string{
					"key_id":   "test",
					"secret":   "test",
					"region":   "us-west-2",
					"endpoint": "https://s3.us-west-2.amazonaws.com",
				},
			},
			{
				name:       "s3_with_session_token",
				secretName: "s3_session",
				secretType: "S3",
				options: map[string]string{
					"key_id":        "test",
					"secret":        "test",
					"session_token": "FwoGZXIvYXdzEB...",
				},
			},
			{
				name:       "s3_credential_chain",
				secretName: "s3_chain",
				secretType: "S3",
				provider:   "CREDENTIAL_CHAIN",
			},
			{
				name:       "s3_env_provider",
				secretName: "s3_env",
				secretType: "S3",
				provider:   "ENV",
			},
			{
				name:       "s3_with_scope",
				secretName: "s3_scoped",
				secretType: "S3",
				scope:      "s3://my-bucket/data/",
			},
			{
				name:       "persistent_secret",
				secretName: "persistent",
				secretType: "S3",
				persistent: true,
			},
			{
				name:       "gcs_secret",
				secretName: "gcs",
				secretType: "GCS",
				options: map[string]string{
					"service_account_json": `{"type":"service_account"}`,
				},
			},
			{
				name:       "azure_account_key",
				secretName: "azure_key",
				secretType: "AZURE",
				options: map[string]string{
					"account_name": "myaccount",
					"account_key":  "base64key==",
				},
			},
			{
				name:       "azure_connection_string",
				secretName: "azure_conn",
				secretType: "AZURE",
				options: map[string]string{
					"connection_string": "DefaultEndpointsProtocol=https;AccountName=...",
				},
			},
			{
				name:       "azure_service_principal",
				secretName: "azure_sp",
				secretType: "AZURE",
				options: map[string]string{
					"tenant_id":     "tenant-guid",
					"client_id":     "client-guid",
					"client_secret": "secret",
				},
			},
			{
				name:       "http_bearer_token",
				secretName: "http_bearer",
				secretType: "HTTP",
				options: map[string]string{
					"bearer_token": "token123",
				},
			},
			{
				name:       "http_with_headers",
				secretName: "http_headers",
				secretType: "HTTP",
				options: map[string]string{
					"extra_http_headers": `{"X-Custom":"value"}`,
				},
			},
			{
				name:       "huggingface_token",
				secretName: "hf_token",
				secretType: "HUGGINGFACE",
				options: map[string]string{
					"bearer_token": "hf_xxxxx",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s, err := secret.BindCreateSecret(
					ctx,
					tc.secretName,
					tc.secretType,
					tc.provider,
					tc.scope,
					tc.persistent,
					tc.options,
				)

				if tc.expectError {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.Equal(t, tc.secretName, s.Name)
				assert.Equal(t, secret.SecretType(strings.ToUpper(tc.secretType)), s.Type)
				assert.Equal(t, tc.persistent, s.Persistent)

				// Verify options are stored
				for k, v := range tc.options {
					assert.Equal(t, v, s.GetOption(k), "Option %s mismatch", k)
				}
			})
		}
	})

	t.Run("DROP_SECRET_syntax", func(t *testing.T) {
		// DuckDB Spec: DROP [PERSISTENT|TEMPORARY] SECRET [IF EXISTS] name [FROM storage]

		testCases := []struct {
			name        string
			secretName  string
			ifExists    bool
			expectError bool
		}{
			{
				name:       "drop_existing",
				secretName: "existing_secret",
				ifExists:   false,
			},
			{
				name:       "drop_if_exists",
				secretName: "maybe_exists",
				ifExists:   true,
			},
			{
				name:        "empty_name_error",
				secretName:  "",
				ifExists:    false,
				expectError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				name, ifExists, err := secret.BindDropSecret(tc.secretName, tc.ifExists)

				if tc.expectError {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.Equal(t, tc.secretName, name)
				assert.Equal(t, tc.ifExists, ifExists)
			})
		}
	})
}

// TestSpecSecretTypeMapping documents secret type to URL scheme mapping.
// Based on DuckDB documentation for supported cloud storage.
func TestSpecSecretTypeMapping(t *testing.T) {
	// DuckDB Spec: Secret types map to URL schemes
	testCases := []struct {
		url          string
		expectedType secret.SecretType
		description  string
	}{
		// S3 and variants
		{"s3://bucket/key", secret.SecretTypeS3, "Standard S3"},
		{"s3a://bucket/key", secret.SecretTypeS3, "S3A (Hadoop)"},
		{"s3n://bucket/key", secret.SecretTypeS3, "S3N (Hadoop legacy)"},
		{"S3://BUCKET/KEY", secret.SecretTypeS3, "S3 uppercase"},

		// Google Cloud Storage
		{"gs://bucket/object", secret.SecretTypeGCS, "GCS standard"},
		{"gcs://bucket/object", secret.SecretTypeGCS, "GCS alternative"},
		{"GS://BUCKET/OBJECT", secret.SecretTypeGCS, "GCS uppercase"},

		// Azure Blob Storage
		{"azure://container/blob", secret.SecretTypeAzure, "Azure standard"},
		{"az://container/blob", secret.SecretTypeAzure, "Azure short"},
		{"AZURE://CONTAINER/BLOB", secret.SecretTypeAzure, "Azure uppercase"},

		// HTTP/HTTPS
		{"http://example.com/file", secret.SecretTypeHTTP, "HTTP"},
		{"https://example.com/file", secret.SecretTypeHTTP, "HTTPS"},
		{"HTTP://EXAMPLE.COM/FILE", secret.SecretTypeHTTP, "HTTP uppercase"},

		// HuggingFace
		{"hf://dataset/file", secret.SecretTypeHuggingFace, "HuggingFace short"},
		{"huggingface://dataset/file", secret.SecretTypeHuggingFace, "HuggingFace full"},

		// Unsupported schemes return empty
		{"ftp://server/file", "", "FTP not supported"},
		{"file:///path/to/file", "", "Local file not a cloud secret"},
		{"sftp://server/file", "", "SFTP not supported"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			secretType := secret.SecretTypeFromURL(tc.url)
			assert.Equal(t, tc.expectedType, secretType,
				"URL %q should map to secret type %q", tc.url, tc.expectedType)
		})
	}
}

// TestSpecSecretScopeMatching documents scope matching behavior.
// Based on DuckDB's longest-prefix-match algorithm.
func TestSpecSecretScopeMatching(t *testing.T) {
	ctx := context.Background()
	mgr := secret.NewManager(nil)

	// Setup: Create secrets with different scope specificity
	secrets := []secret.Secret{
		{
			Name:    "global",
			Type:    secret.SecretTypeS3,
			Scope:   secret.SecretScope{Type: secret.ScopeGlobal},
			Options: secret.SecretOptions{"marker": "global"},
		},
		{
			Name: "bucket_scope",
			Type: secret.SecretTypeS3,
			Scope: secret.SecretScope{
				Type:   secret.ScopePath,
				Prefix: "s3://my-bucket/",
			},
			Options: secret.SecretOptions{"marker": "bucket"},
		},
		{
			Name: "path_scope",
			Type: secret.SecretTypeS3,
			Scope: secret.SecretScope{
				Type:   secret.ScopePath,
				Prefix: "s3://my-bucket/data/",
			},
			Options: secret.SecretOptions{"marker": "path"},
		},
		{
			Name: "deep_path_scope",
			Type: secret.SecretTypeS3,
			Scope: secret.SecretScope{
				Type:   secret.ScopePath,
				Prefix: "s3://my-bucket/data/parquet/2024/",
			},
			Options: secret.SecretOptions{"marker": "deep"},
		},
	}

	for _, s := range secrets {
		err := mgr.CreateSecret(ctx, s)
		require.NoError(t, err)
	}

	// DuckDB Spec: Use longest prefix match to select most specific secret
	testCases := []struct {
		url            string
		expectedMarker string
		description    string
	}{
		// Global scope matches anything without more specific match
		{
			url:            "s3://other-bucket/file.csv",
			expectedMarker: "global",
			description:    "Global scope for unmatched bucket",
		},
		// Bucket scope beats global
		{
			url:            "s3://my-bucket/readme.txt",
			expectedMarker: "bucket",
			description:    "Bucket scope beats global",
		},
		// Path scope beats bucket scope
		{
			url:            "s3://my-bucket/data/file.csv",
			expectedMarker: "path",
			description:    "Path scope beats bucket scope",
		},
		// Deeper path scope beats shallower path scope
		{
			url:            "s3://my-bucket/data/parquet/2024/01/data.parquet",
			expectedMarker: "deep",
			description:    "Deeper path beats shallower path",
		},
		// Path prefix must match exactly
		{
			url:            "s3://my-bucket/data/csv/data.csv",
			expectedMarker: "path",
			description:    "Non-matching deep path falls back to path scope",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s, err := mgr.GetSecret(ctx, tc.url, secret.SecretTypeS3)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedMarker, s.GetOption("marker"),
				"URL %q should match secret with marker %q", tc.url, tc.expectedMarker)
		})
	}
}

// TestSpecSecretProviders documents supported credential provider types.
// Based on DuckDB documentation for credential providers.
func TestSpecSecretProviders(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: Supported provider types
	providers := []struct {
		provider    string
		description string
	}{
		{"CONFIG", "Explicit credentials via options (KEY_ID, SECRET, etc.)"},
		{"ENV", "Credentials from environment variables (AWS_ACCESS_KEY_ID, etc.)"},
		{"CREDENTIAL_CHAIN", "AWS SDK default credential chain"},
		{"IAM", "IAM role credentials (EC2 instance metadata)"},
	}

	for _, tc := range providers {
		t.Run(tc.provider, func(t *testing.T) {
			s, err := secret.BindCreateSecret(
				ctx,
				"test_"+strings.ToLower(tc.provider),
				"S3",
				tc.provider,
				"",
				false,
				nil,
			)

			require.NoError(t, err, "Provider %s should be valid: %s", tc.provider, tc.description)
			assert.Equal(t, secret.ProviderType(tc.provider), s.Provider)
		})
	}

	t.Run("default_provider_is_CONFIG", func(t *testing.T) {
		// DuckDB Spec: Default provider is CONFIG when not specified
		s, err := secret.BindCreateSecret(ctx, "default_provider", "S3", "", "", false, nil)
		require.NoError(t, err)
		assert.Equal(t, secret.ProviderConfig, s.Provider)
	})

	t.Run("provider_case_insensitive", func(t *testing.T) {
		// Provider names should be case-insensitive
		for _, p := range []string{"config", "Config", "CONFIG", "ConFiG"} {
			s, err := secret.BindCreateSecret(ctx, "case_test_"+p, "S3", p, "", false, nil)
			require.NoError(t, err)
			assert.Equal(t, secret.ProviderConfig, s.Provider)
		}
	})
}

// TestSpecS3Options documents S3-specific secret options.
// Based on DuckDB httpfs extension documentation.
func TestSpecS3Options(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: S3 secret options
	s3Options := map[string]string{
		"key_id":        "AWS Access Key ID",
		"secret":        "AWS Secret Access Key",
		"session_token": "AWS Session Token (for temporary credentials)",
		"region":        "AWS Region (e.g., us-east-1)",
		"endpoint":      "Custom S3 endpoint URL",
		"url_style":     "URL style: 'path' or 'vhost'",
		"use_ssl":       "Use SSL/TLS: 'true' or 'false'",
	}

	options := make(map[string]string)
	for key := range s3Options {
		options[key] = "test_value_" + key
	}

	s, err := secret.BindCreateSecret(ctx, "s3_all_options", "S3", "CONFIG", "", false, options)
	require.NoError(t, err)

	for key := range s3Options {
		assert.Equal(t, "test_value_"+key, s.GetOption(key),
			"Option %s should be stored", key)
	}
}

// TestSpecAzureOptions documents Azure-specific secret options.
// Based on DuckDB Azure extension documentation.
func TestSpecAzureOptions(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: Azure secret options
	t.Run("account_key_auth", func(t *testing.T) {
		options := map[string]string{
			"account_name": "myaccount",
			"account_key":  "base64encodedkey==",
		}
		s, err := secret.BindCreateSecret(ctx, "azure_key", "AZURE", "CONFIG", "", false, options)
		require.NoError(t, err)
		assert.Equal(t, "myaccount", s.GetOption("account_name"))
		assert.Equal(t, "base64encodedkey==", s.GetOption("account_key"))
	})

	t.Run("connection_string_auth", func(t *testing.T) {
		options := map[string]string{
			"connection_string": "DefaultEndpointsProtocol=https;AccountName=myaccount;...",
		}
		s, err := secret.BindCreateSecret(ctx, "azure_conn", "AZURE", "CONFIG", "", false, options)
		require.NoError(t, err)
		assert.NotEmpty(t, s.GetOption("connection_string"))
	})

	t.Run("service_principal_auth", func(t *testing.T) {
		options := map[string]string{
			"tenant_id":     "00000000-0000-0000-0000-000000000000",
			"client_id":     "11111111-1111-1111-1111-111111111111",
			"client_secret": "my-client-secret",
		}
		s, err := secret.BindCreateSecret(ctx, "azure_sp", "AZURE", "CONFIG", "", false, options)
		require.NoError(t, err)
		assert.Equal(t, "00000000-0000-0000-0000-000000000000", s.GetOption("tenant_id"))
		assert.Equal(t, "11111111-1111-1111-1111-111111111111", s.GetOption("client_id"))
		assert.Equal(t, "my-client-secret", s.GetOption("client_secret"))
	})
}

// TestSpecGCSOptions documents GCS-specific secret options.
// Based on DuckDB GCS extension documentation.
func TestSpecGCSOptions(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: GCS secret options
	options := map[string]string{
		"service_account_json": `{"type":"service_account","project_id":"my-project"}`,
		"project_id":           "my-project-id",
	}

	s, err := secret.BindCreateSecret(ctx, "gcs_options", "GCS", "CONFIG", "", false, options)
	require.NoError(t, err)

	assert.Contains(t, s.GetOption("service_account_json"), "service_account")
	assert.Equal(t, "my-project-id", s.GetOption("project_id"))
}

// TestSpecHTTPOptions documents HTTP-specific secret options.
// Based on DuckDB httpfs extension documentation.
func TestSpecHTTPOptions(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: HTTP secret options
	t.Run("bearer_token", func(t *testing.T) {
		options := map[string]string{
			"bearer_token": "eyJhbGciOiJIUzI1NiIs...",
		}
		s, err := secret.BindCreateSecret(ctx, "http_bearer", "HTTP", "CONFIG", "", false, options)
		require.NoError(t, err)
		assert.Equal(t, "eyJhbGciOiJIUzI1NiIs...", s.GetOption("bearer_token"))
	})

	t.Run("extra_headers", func(t *testing.T) {
		options := map[string]string{
			"extra_http_headers": `{"X-Api-Key":"key123","X-Custom":"value"}`,
		}
		s, err := secret.BindCreateSecret(ctx, "http_headers", "HTTP", "CONFIG", "", false, options)
		require.NoError(t, err)
		assert.Contains(t, s.GetOption("extra_http_headers"), "X-Api-Key")
	})
}

// TestSpecScopeParsing documents scope URL parsing behavior.
func TestSpecScopeParsing(t *testing.T) {
	// DuckDB Spec: Scope parsing rules
	testCases := []struct {
		scope          string
		expectedType   secret.ScopeType
		expectedPrefix string
		description    string
	}{
		// Empty is global
		{"", secret.ScopeGlobal, "", "Empty string is global scope"},

		// Cloud URLs become path scope
		{"s3://bucket", secret.ScopePath, "s3://bucket", "S3 bucket"},
		{"s3://bucket/", secret.ScopePath, "s3://bucket/", "S3 bucket with slash"},
		{"s3://bucket/path/", secret.ScopePath, "s3://bucket/path/", "S3 path"},
		{"gs://bucket/path", secret.ScopePath, "gs://bucket/path", "GCS path"},
		{"azure://container/blob", secret.ScopePath, "azure://container/blob", "Azure path"},
		{"https://example.com/api/", secret.ScopePath, "https://example.com/api/", "HTTPS path"},

		// Hadoop variants
		{"s3a://bucket/path", secret.ScopePath, "s3a://bucket/path", "S3A path"},
		{"s3n://bucket/path", secret.ScopePath, "s3n://bucket/path", "S3N path"},

		// Alternative schemes
		{"gcs://bucket/path", secret.ScopePath, "gcs://bucket/path", "GCS alternative scheme"},
		{"az://container/blob", secret.ScopePath, "az://container/blob", "Azure short scheme"},

		// Unknown schemes become host scope
		{"ftp://server/path", secret.ScopeHost, "ftp://server/path", "Unknown scheme becomes host"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			scope := secret.ParseScope(tc.scope)
			assert.Equal(t, tc.expectedType, scope.Type,
				"Scope %q should have type %v", tc.scope, tc.expectedType)
			assert.Equal(t, tc.expectedPrefix, scope.Prefix,
				"Scope %q should have prefix %q", tc.scope, tc.expectedPrefix)
		})
	}
}

// TestSpecSecretLifecycle documents secret lifecycle operations.
func TestSpecSecretLifecycle(t *testing.T) {
	ctx := context.Background()
	mgr := secret.NewManager(nil)

	t.Run("create_and_get", func(t *testing.T) {
		// DuckDB Spec: Create secret and retrieve by name
		err := mgr.CreateSecret(ctx, secret.Secret{
			Name: "lifecycle_test",
			Type: secret.SecretTypeS3,
			Options: secret.SecretOptions{
				"key_id": "test_key",
			},
		})
		require.NoError(t, err)

		s, err := mgr.GetSecretByName(ctx, "lifecycle_test")
		require.NoError(t, err)
		assert.Equal(t, "test_key", s.GetOption("key_id"))
	})

	t.Run("duplicate_name_error", func(t *testing.T) {
		// DuckDB Spec: Duplicate secret name is an error
		err := mgr.CreateSecret(ctx, secret.Secret{
			Name: "duplicate_test",
			Type: secret.SecretTypeS3,
		})
		require.NoError(t, err)

		err = mgr.CreateSecret(ctx, secret.Secret{
			Name: "duplicate_test",
			Type: secret.SecretTypeS3,
		})
		assert.ErrorIs(t, err, secret.ErrSecretExists)
	})

	t.Run("alter_secret", func(t *testing.T) {
		// DuckDB Spec: ALTER SECRET updates options
		err := mgr.CreateSecret(ctx, secret.Secret{
			Name: "alter_test",
			Type: secret.SecretTypeS3,
			Options: secret.SecretOptions{
				"key_id": "original",
				"region": "us-east-1",
			},
		})
		require.NoError(t, err)

		err = mgr.AlterSecret(ctx, "alter_test", secret.SecretOptions{
			"key_id": "updated",
		})
		require.NoError(t, err)

		s, err := mgr.GetSecretByName(ctx, "alter_test")
		require.NoError(t, err)
		assert.Equal(t, "updated", s.GetOption("key_id"))
		assert.Equal(t, "us-east-1", s.GetOption("region"), "Unmodified options preserved")
	})

	t.Run("drop_secret", func(t *testing.T) {
		// DuckDB Spec: DROP SECRET removes the secret
		err := mgr.CreateSecret(ctx, secret.Secret{
			Name: "drop_test",
			Type: secret.SecretTypeS3,
		})
		require.NoError(t, err)

		err = mgr.DropSecret(ctx, "drop_test", false)
		require.NoError(t, err)

		_, err = mgr.GetSecretByName(ctx, "drop_test")
		assert.ErrorIs(t, err, secret.ErrSecretNotFound)
	})

	t.Run("drop_if_exists", func(t *testing.T) {
		// DuckDB Spec: DROP SECRET IF EXISTS doesn't error for missing
		err := mgr.DropSecret(ctx, "nonexistent_secret", true)
		assert.NoError(t, err, "IF EXISTS should not error")
	})

	t.Run("drop_without_if_exists_errors", func(t *testing.T) {
		// DuckDB Spec: DROP SECRET without IF EXISTS errors for missing
		err := mgr.DropSecret(ctx, "nonexistent_secret", false)
		assert.ErrorIs(t, err, secret.ErrSecretNotFound)
	})
}

// TestSpecURLFormats documents supported URL format patterns.
func TestSpecURLFormats(t *testing.T) {
	// DuckDB Spec: URL formats for cloud storage

	t.Run("s3_url_formats", func(t *testing.T) {
		validS3URLs := []string{
			"s3://bucket/key",
			"s3://bucket/path/to/key",
			"s3://bucket/path/to/key.parquet",
			"s3://my-bucket-name/data/file.csv",
			"s3://bucket-with-dots.example.com/key",
			"s3://bucket/key?versionId=abc123",
		}

		for _, url := range validS3URLs {
			assert.Equal(t, secret.SecretTypeS3, secret.SecretTypeFromURL(url),
				"URL %q should be recognized as S3", url)
		}
	})

	t.Run("gcs_url_formats", func(t *testing.T) {
		validGCSURLs := []string{
			"gs://bucket/object",
			"gs://bucket/path/to/object",
			"gcs://bucket/object",
		}

		for _, url := range validGCSURLs {
			assert.Equal(t, secret.SecretTypeGCS, secret.SecretTypeFromURL(url),
				"URL %q should be recognized as GCS", url)
		}
	})

	t.Run("azure_url_formats", func(t *testing.T) {
		validAzureURLs := []string{
			"azure://container/blob",
			"azure://container/path/to/blob",
			"az://container/blob",
		}

		for _, url := range validAzureURLs {
			assert.Equal(t, secret.SecretTypeAzure, secret.SecretTypeFromURL(url),
				"URL %q should be recognized as Azure", url)
		}
	})

	t.Run("http_url_formats", func(t *testing.T) {
		validHTTPURLs := []string{
			"http://example.com/file.csv",
			"https://example.com/file.csv",
			"https://api.example.com/v1/data.json",
			"https://example.com:8080/file",
			"http://user:pass@example.com/file",
		}

		for _, url := range validHTTPURLs {
			assert.Equal(t, secret.SecretTypeHTTP, secret.SecretTypeFromURL(url),
				"URL %q should be recognized as HTTP", url)
		}
	})
}

// TestSpecErrorConditions documents expected error conditions.
func TestSpecErrorConditions(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_secret_name", func(t *testing.T) {
		_, err := secret.BindCreateSecret(ctx, "", "S3", "", "", false, nil)
		assert.ErrorIs(t, err, secret.ErrInvalidSecretName)
	})

	t.Run("invalid_secret_type", func(t *testing.T) {
		_, err := secret.BindCreateSecret(ctx, "test", "INVALID_TYPE", "", "", false, nil)
		assert.ErrorIs(t, err, secret.ErrInvalidSecretType)
	})

	t.Run("alter_nonexistent", func(t *testing.T) {
		mgr := secret.NewManager(nil)
		err := mgr.AlterSecret(ctx, "nonexistent", secret.SecretOptions{"key": "value"})
		assert.ErrorIs(t, err, secret.ErrSecretNotFound)
	})

	t.Run("get_nonexistent", func(t *testing.T) {
		mgr := secret.NewManager(nil)
		_, err := mgr.GetSecretByName(ctx, "nonexistent")
		assert.ErrorIs(t, err, secret.ErrSecretNotFound)
	})
}

// TestSpecTypeNormalization documents type name normalization behavior.
func TestSpecTypeNormalization(t *testing.T) {
	ctx := context.Background()

	// DuckDB Spec: Type names are case-insensitive
	typeVariants := []struct {
		input    string
		expected secret.SecretType
	}{
		{"s3", secret.SecretTypeS3},
		{"S3", secret.SecretTypeS3},
		{"gcs", secret.SecretTypeGCS},
		{"GCS", secret.SecretTypeGCS},
		{"Gcs", secret.SecretTypeGCS},
		{"azure", secret.SecretTypeAzure},
		{"AZURE", secret.SecretTypeAzure},
		{"Azure", secret.SecretTypeAzure},
		{"http", secret.SecretTypeHTTP},
		{"HTTP", secret.SecretTypeHTTP},
		{"Http", secret.SecretTypeHTTP},
		{"huggingface", secret.SecretTypeHuggingFace},
		{"HUGGINGFACE", secret.SecretTypeHuggingFace},
		{"HuggingFace", secret.SecretTypeHuggingFace},
	}

	for _, tc := range typeVariants {
		t.Run(tc.input, func(t *testing.T) {
			s, err := secret.BindCreateSecret(
				ctx,
				"type_test_"+tc.input,
				tc.input,
				"",
				"",
				false,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, s.Type)
		})
	}
}

// TestSpecListSecrets documents LIST SECRETS behavior.
func TestSpecListSecrets(t *testing.T) {
	ctx := context.Background()
	mgr := secret.NewManager(nil)

	// Create multiple secrets
	for _, name := range []string{"secret1", "secret2", "secret3"} {
		err := mgr.CreateSecret(ctx, secret.Secret{
			Name: name,
			Type: secret.SecretTypeS3,
		})
		require.NoError(t, err)
	}

	// List all secrets
	secrets, err := mgr.ListSecrets(ctx, secret.SecretScope{})
	require.NoError(t, err)
	assert.Len(t, secrets, 3, "Should list all secrets")

	// Verify secret names
	names := make(map[string]bool)
	for _, s := range secrets {
		names[s.Name] = true
	}
	assert.True(t, names["secret1"])
	assert.True(t, names["secret2"])
	assert.True(t, names["secret3"])
}
