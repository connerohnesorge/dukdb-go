package secret

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindCreateSecret(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		secretName string
		secretType string
		provider   string
		scope      string
		persistent bool
		options    map[string]string
		wantErr    bool
		errType    error
	}{
		{
			name:       "S3 secret with all options",
			secretName: "my_s3_secret",
			secretType: "S3",
			provider:   "CONFIG",
			scope:      "s3://my-bucket/path",
			persistent: true,
			options: map[string]string{
				"key_id": "AKIAIOSFODNN7EXAMPLE",
				"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"region": "us-east-1",
			},
			wantErr: false,
		},
		{
			name:       "GCS secret",
			secretName: "my_gcs_secret",
			secretType: "GCS",
			provider:   "CONFIG",
			scope:      "gs://my-bucket",
			persistent: false,
			options: map[string]string{
				"service_account_json": `{"type": "service_account"}`,
			},
			wantErr: false,
		},
		{
			name:       "Azure secret",
			secretName: "my_azure_secret",
			secretType: "AZURE",
			provider:   "CONFIG",
			scope:      "azure://my-container",
			persistent: false,
			options: map[string]string{
				"account_name": "mystorageaccount",
				"account_key":  "base64encodedkey",
			},
			wantErr: false,
		},
		{
			name:       "HTTP secret",
			secretName: "my_http_secret",
			secretType: "HTTP",
			provider:   "CONFIG",
			scope:      "https://example.com/api",
			persistent: false,
			options: map[string]string{
				"bearer_token": "my-token-123",
			},
			wantErr: false,
		},
		{
			name:       "Empty name",
			secretName: "",
			secretType: "S3",
			wantErr:    true,
			errType:    ErrInvalidSecretName,
		},
		{
			name:       "Invalid type",
			secretName: "test",
			secretType: "INVALID",
			wantErr:    true,
			errType:    ErrInvalidSecretType,
		},
		{
			name:       "Lowercase type (normalized to uppercase)",
			secretName: "test",
			secretType: "s3",
			provider:   "config",
			persistent: false,
			options:    map[string]string{},
			wantErr:    false,
		},
		{
			name:       "Default provider",
			secretName: "test",
			secretType: "S3",
			provider:   "",
			persistent: false,
			options:    map[string]string{},
			wantErr:    false,
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

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, secret)

			assert.Equal(t, tt.secretName, secret.Name)
			// Type should be normalized to uppercase
			assert.Equal(t, SecretType(strings.ToUpper(tt.secretType)), secret.Type)
			assert.Equal(t, tt.persistent, secret.Persistent)

			// Check options are copied
			for k, v := range tt.options {
				assert.Equal(t, v, secret.Options[k])
			}

			// Check timestamps are set
			assert.False(t, secret.CreatedAt.IsZero())
			assert.False(t, secret.UpdatedAt.IsZero())
		})
	}
}

func TestBindDropSecret(t *testing.T) {
	tests := []struct {
		name     string
		secret   string
		ifExists bool
		wantErr  bool
		errType  error
	}{
		{
			name:     "Valid drop",
			secret:   "my_secret",
			ifExists: false,
			wantErr:  false,
		},
		{
			name:     "Drop with IF EXISTS",
			secret:   "my_secret",
			ifExists: true,
			wantErr:  false,
		},
		{
			name:    "Empty name",
			secret:  "",
			wantErr: true,
			errType: ErrInvalidSecretName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ifExists, err := BindDropSecret(tt.secret, tt.ifExists)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.secret, name)
			assert.Equal(t, tt.ifExists, ifExists)
		})
	}
}

func TestBindAlterSecret(t *testing.T) {
	tests := []struct {
		name    string
		secret  string
		options map[string]string
		wantErr bool
		errType error
	}{
		{
			name:   "Valid alter",
			secret: "my_secret",
			options: map[string]string{
				"region": "us-west-2",
			},
			wantErr: false,
		},
		{
			name:   "Alter multiple options",
			secret: "my_secret",
			options: map[string]string{
				"key_id": "new_key",
				"secret": "new_secret",
			},
			wantErr: false,
		},
		{
			name:    "Empty name",
			secret:  "",
			options: map[string]string{"region": "us-west-2"},
			wantErr: true,
			errType: ErrInvalidSecretName,
		},
		{
			name:    "Empty options",
			secret:  "my_secret",
			options: map[string]string{},
			wantErr: true,
			errType: ErrInvalidSecretName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, opts, err := BindAlterSecret(tt.secret, tt.options)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.secret, name)
			assert.Len(t, opts, len(tt.options))
			for k, v := range tt.options {
				assert.Equal(t, v, opts[k])
			}
		})
	}
}

func TestParseScope(t *testing.T) {
	tests := []struct {
		name       string
		scope      string
		wantType   ScopeType
		wantPrefix string
	}{
		{
			name:       "Empty scope is global",
			scope:      "",
			wantType:   ScopeGlobal,
			wantPrefix: "",
		},
		{
			name:       "S3 URL",
			scope:      "s3://my-bucket/path",
			wantType:   ScopePath,
			wantPrefix: "s3://my-bucket/path",
		},
		{
			name:       "S3A URL",
			scope:      "s3a://my-bucket/path",
			wantType:   ScopePath,
			wantPrefix: "s3a://my-bucket/path",
		},
		{
			name:       "GCS URL",
			scope:      "gs://my-bucket",
			wantType:   ScopePath,
			wantPrefix: "gs://my-bucket",
		},
		{
			name:       "Azure URL",
			scope:      "azure://my-container/path",
			wantType:   ScopePath,
			wantPrefix: "azure://my-container/path",
		},
		{
			name:       "HTTP URL",
			scope:      "https://example.com/api",
			wantType:   ScopePath,
			wantPrefix: "https://example.com/api",
		},
		{
			name:       "Unknown scheme becomes host scope",
			scope:      "ftp://server/path",
			wantType:   ScopeHost,
			wantPrefix: "ftp://server/path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := ParseScope(tt.scope)
			assert.Equal(t, tt.wantType, scope.Type)
			assert.Equal(t, tt.wantPrefix, scope.Prefix)
		})
	}
}

func TestSecretTypeFromURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantType SecretType
	}{
		{"S3 URL", "s3://bucket/key", SecretTypeS3},
		{"S3A URL", "s3a://bucket/key", SecretTypeS3},
		{"S3N URL", "s3n://bucket/key", SecretTypeS3},
		{"GS URL", "gs://bucket/key", SecretTypeGCS},
		{"GCS URL", "gcs://bucket/key", SecretTypeGCS},
		{"Azure URL", "azure://container/blob", SecretTypeAzure},
		{"Az URL", "az://container/blob", SecretTypeAzure},
		{"HTTP URL", "http://example.com/file", SecretTypeHTTP},
		{"HTTPS URL", "https://example.com/file", SecretTypeHTTP},
		{"HuggingFace URL", "hf://dataset/file", SecretTypeHuggingFace},
		{"Unknown URL", "ftp://server/file", ""},
		{"Uppercase", "S3://bucket/key", SecretTypeS3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SecretTypeFromURL(tt.url)
			assert.Equal(t, tt.wantType, got)
		})
	}
}

func TestIsValidSecretType(t *testing.T) {
	validTypes := []SecretType{
		SecretTypeS3,
		SecretTypeGCS,
		SecretTypeAzure,
		SecretTypeHTTP,
		SecretTypeHuggingFace,
	}

	for _, st := range validTypes {
		assert.True(t, isValidSecretType(st), "expected %s to be valid", st)
	}

	invalidTypes := []SecretType{
		"INVALID",
		"FOO",
		"",
	}

	for _, st := range invalidTypes {
		assert.False(t, isValidSecretType(st), "expected %s to be invalid", st)
	}
}

// TestSecretTimestamps verifies that timestamps are correctly set
func TestSecretTimestamps(t *testing.T) {
	ctx := context.Background()
	before := time.Now()

	secret, err := BindCreateSecret(
		ctx,
		"test",
		"S3",
		"CONFIG",
		"",
		false,
		map[string]string{},
	)

	require.NoError(t, err)
	after := time.Now()

	// CreatedAt should be between before and after
	assert.True(t, !secret.CreatedAt.Before(before), "CreatedAt should be >= test start time")
	assert.True(t, !secret.CreatedAt.After(after), "CreatedAt should be <= test end time")

	// UpdatedAt should equal CreatedAt for new secrets
	assert.Equal(t, secret.CreatedAt, secret.UpdatedAt)
}
