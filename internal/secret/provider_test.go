package secret

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMockToken is the mock token used in IMDS tests.
const testMockToken = "mock-token"

// TestCredentials_IsExpired tests the Credentials.IsExpired method.
func TestCredentials_IsExpired(t *testing.T) {
	tests := []struct {
		name       string
		expiration time.Time
		expected   bool
	}{
		{
			name:       "zero time means no expiration",
			expiration: time.Time{},
			expected:   false,
		},
		{
			name:       "future expiration is not expired",
			expiration: time.Now().Add(1 * time.Hour),
			expected:   false,
		},
		{
			name:       "past expiration is expired",
			expiration: time.Now().Add(-1 * time.Hour),
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds := &Credentials{Expiration: tt.expiration}
			assert.Equal(t, tt.expected, creds.IsExpired())
		})
	}
}

// TestCredentials_IsValid tests the Credentials.IsValid method.
func TestCredentials_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		creds    Credentials
		expected bool
	}{
		{
			name:     "empty credentials are invalid",
			creds:    Credentials{},
			expected: false,
		},
		{
			name:     "only access key is invalid",
			creds:    Credentials{AccessKeyID: "AKID"},
			expected: false,
		},
		{
			name:     "only secret key is invalid",
			creds:    Credentials{SecretAccessKey: "secret"},
			expected: false,
		},
		{
			name: "both keys are valid",
			creds: Credentials{
				AccessKeyID:     "AKID",
				SecretAccessKey: "secret",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.creds.IsValid())
		})
	}
}

// TestConfigProvider tests the ConfigProvider.
func TestConfigProvider(t *testing.T) {
	ctx := context.Background()

	t.Run("valid credentials", func(t *testing.T) {
		provider := NewConfigProvider("AKID", "secret", "token", "us-west-2")

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "AKID", creds.AccessKeyID)
		assert.Equal(t, "secret", creds.SecretAccessKey)
		assert.Equal(t, "token", creds.SessionToken)
		assert.Equal(t, "us-west-2", creds.Region)
	})

	t.Run("empty credentials returns error", func(t *testing.T) {
		provider := NewConfigProvider("", "", "", "")

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("partial credentials returns error", func(t *testing.T) {
		provider := NewConfigProvider("AKID", "", "", "")

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("name returns ConfigProvider", func(t *testing.T) {
		provider := NewConfigProvider("", "", "", "")
		assert.Equal(t, "ConfigProvider", provider.Name())
	})
}

// TestConfigProviderFromSecret tests creating ConfigProvider from a Secret.
func TestConfigProviderFromSecret(t *testing.T) {
	ctx := context.Background()

	secret := &Secret{
		Name: "test",
		Type: SecretTypeS3,
		Options: SecretOptions{
			OptionKeyID:        "AKIAIOSFODNN7EXAMPLE",
			OptionSecret:       "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			OptionSessionToken: "session-token",
			OptionRegion:       "eu-west-1",
		},
	}

	provider := NewConfigProviderFromSecret(secret)

	creds, err := provider.Retrieve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", creds.AccessKeyID)
	assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", creds.SecretAccessKey)
	assert.Equal(t, "session-token", creds.SessionToken)
	assert.Equal(t, "eu-west-1", creds.Region)
}

// TestEnvProvider tests the EnvProvider.
func TestEnvProvider(t *testing.T) {
	ctx := context.Background()
	provider := NewEnvProvider()

	t.Run("with valid environment variables", func(t *testing.T) {
		t.Setenv(EnvAccessKeyID, "ENV_AKID")
		t.Setenv(EnvSecretAccessKey, "ENV_SECRET")
		t.Setenv(EnvSessionToken, "ENV_TOKEN")
		t.Setenv(EnvRegion, "ap-northeast-1")

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "ENV_AKID", creds.AccessKeyID)
		assert.Equal(t, "ENV_SECRET", creds.SecretAccessKey)
		assert.Equal(t, "ENV_TOKEN", creds.SessionToken)
		assert.Equal(t, "ap-northeast-1", creds.Region)
	})

	t.Run("with AWS_DEFAULT_REGION fallback", func(t *testing.T) {
		t.Setenv(EnvAccessKeyID, "AKID")
		t.Setenv(EnvSecretAccessKey, "SECRET")
		t.Setenv(EnvRegion, "")
		t.Setenv(EnvDefaultRegion, "us-east-1")

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "us-east-1", creds.Region)
	})

	t.Run("without environment variables", func(t *testing.T) {
		t.Setenv(EnvAccessKeyID, "")
		t.Setenv(EnvSecretAccessKey, "")

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("name returns EnvProvider", func(t *testing.T) {
		assert.Equal(t, "EnvProvider", provider.Name())
	})
}

// TestSharedConfigProvider tests the SharedConfigProvider.
func TestSharedConfigProvider(t *testing.T) {
	ctx := context.Background()

	// Create a temp directory for test config files
	tempDir := t.TempDir()

	t.Run("with valid credentials file", func(t *testing.T) {
		credsFile := filepath.Join(tempDir, "credentials")
		credsContent := `[default]
aws_access_key_id = FILE_AKID
aws_secret_access_key = FILE_SECRET
aws_session_token = FILE_TOKEN
`
		err := os.WriteFile(credsFile, []byte(credsContent), 0o600)
		require.NoError(t, err)

		configFile := filepath.Join(tempDir, "config")
		configContent := `[default]
region = us-west-2
`
		err = os.WriteFile(configFile, []byte(configContent), 0o600)
		require.NoError(t, err)

		provider := NewSharedConfigProvider(
			WithCredentialsFile(credsFile),
			WithConfigFile(configFile),
		)

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FILE_AKID", creds.AccessKeyID)
		assert.Equal(t, "FILE_SECRET", creds.SecretAccessKey)
		assert.Equal(t, "FILE_TOKEN", creds.SessionToken)
		assert.Equal(t, "us-west-2", creds.Region)
	})

	t.Run("with named profile", func(t *testing.T) {
		credsFile := filepath.Join(tempDir, "credentials_profile")
		credsContent := `[default]
aws_access_key_id = DEFAULT_AKID
aws_secret_access_key = DEFAULT_SECRET

[production]
aws_access_key_id = PROD_AKID
aws_secret_access_key = PROD_SECRET
`
		err := os.WriteFile(credsFile, []byte(credsContent), 0o600)
		require.NoError(t, err)

		configFile := filepath.Join(tempDir, "config_profile")
		configContent := `[default]
region = us-east-1

[profile production]
region = eu-central-1
`
		err = os.WriteFile(configFile, []byte(configContent), 0o600)
		require.NoError(t, err)

		provider := NewSharedConfigProvider(
			WithCredentialsFile(credsFile),
			WithConfigFile(configFile),
			WithProfile("production"),
		)

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "PROD_AKID", creds.AccessKeyID)
		assert.Equal(t, "PROD_SECRET", creds.SecretAccessKey)
		assert.Equal(t, "eu-central-1", creds.Region)
	})

	t.Run("with credentials in config file only", func(t *testing.T) {
		configFile := filepath.Join(tempDir, "config_with_creds")
		configContent := `[default]
region = sa-east-1
aws_access_key_id = CONFIG_AKID
aws_secret_access_key = CONFIG_SECRET
`
		err := os.WriteFile(configFile, []byte(configContent), 0o600)
		require.NoError(t, err)

		// Use non-existent credentials file
		credsFile := filepath.Join(tempDir, "nonexistent_creds")

		provider := NewSharedConfigProvider(
			WithCredentialsFile(credsFile),
			WithConfigFile(configFile),
		)

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "CONFIG_AKID", creds.AccessKeyID)
		assert.Equal(t, "CONFIG_SECRET", creds.SecretAccessKey)
		assert.Equal(t, "sa-east-1", creds.Region)
	})

	t.Run("with no credentials", func(t *testing.T) {
		provider := NewSharedConfigProvider(
			WithCredentialsFile(filepath.Join(tempDir, "nonexistent1")),
			WithConfigFile(filepath.Join(tempDir, "nonexistent2")),
		)

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("with AWS_PROFILE environment variable", func(t *testing.T) {
		credsFile := filepath.Join(tempDir, "credentials_env_profile")
		credsContent := `[default]
aws_access_key_id = DEFAULT_AKID
aws_secret_access_key = DEFAULT_SECRET

[staging]
aws_access_key_id = STAGING_AKID
aws_secret_access_key = STAGING_SECRET
`
		err := os.WriteFile(credsFile, []byte(credsContent), 0o600)
		require.NoError(t, err)

		t.Setenv("AWS_PROFILE", "staging")

		provider := NewSharedConfigProvider(
			WithCredentialsFile(credsFile),
			WithConfigFile(filepath.Join(tempDir, "empty_config")),
		)

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "STAGING_AKID", creds.AccessKeyID)
	})

	t.Run("name returns SharedConfigProvider", func(t *testing.T) {
		provider := NewSharedConfigProvider()
		assert.Equal(t, "SharedConfigProvider", provider.Name())
	})
}

// TestParseINI tests the INI file parser.
func TestParseINI(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		section  string
		expected map[string]string
	}{
		{
			name: "simple section",
			content: `[default]
key = value
other = data
`,
			section: "default",
			expected: map[string]string{
				"key":   "value",
				"other": "data",
			},
		},
		{
			name: "with comments",
			content: `# This is a comment
[default]
; Another comment
key = value
`,
			section:  "default",
			expected: map[string]string{"key": "value"},
		},
		{
			name: "case insensitive section",
			content: `[DEFAULT]
key = value
`,
			section:  "default",
			expected: map[string]string{"key": "value"},
		},
		{
			name: "multiple sections",
			content: `[section1]
key1 = value1

[section2]
key2 = value2
`,
			section:  "section2",
			expected: map[string]string{"key2": "value2"},
		},
		{
			name: "empty section",
			content: `[default]
[other]
key = value
`,
			section:  "default",
			expected: map[string]string{},
		},
		{
			name: "whitespace handling",
			content: `[default]
  key  =  value with spaces
`,
			section:  "default",
			expected: map[string]string{"key": "value with spaces"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseINI(strings.NewReader(tt.content), tt.section)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIMDSv2Provider tests the IMDSv2Provider with a mock server.
func TestIMDSv2Provider(t *testing.T) {
	ctx := context.Background()

	t.Run("successful credential retrieval", func(t *testing.T) {
		// Create mock IMDS server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case IMDSTokenPath:
				// Return a mock token
				if r.Method != http.MethodPut {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				_, _ = w.Write([]byte(testMockToken))

			case IMDSCredentialsPath:
				// Return the role name
				if r.Header.Get("X-aws-ec2-metadata-token") != testMockToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte("test-role"))

			case IMDSCredentialsPath + "test-role":
				// Return credentials
				if r.Header.Get("X-aws-ec2-metadata-token") != testMockToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				resp := imdsCredentialsResponse{
					Code:            "Success",
					AccessKeyID:     "IMDS_AKID",
					SecretAccessKey: "IMDS_SECRET",
					Token:           "IMDS_TOKEN",
					Expiration:      time.Now().Add(6 * time.Hour),
				}
				_ = json.NewEncoder(w).Encode(resp)

			case IMDSRegionPath:
				// Return region
				if r.Header.Get("X-aws-ec2-metadata-token") != testMockToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte("us-west-2"))

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		provider := NewIMDSv2Provider(
			WithIMDSEndpoint(server.URL),
		)

		creds, err := provider.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "IMDS_AKID", creds.AccessKeyID)
		assert.Equal(t, "IMDS_SECRET", creds.SecretAccessKey)
		assert.Equal(t, "IMDS_TOKEN", creds.SessionToken)
		assert.Equal(t, "us-west-2", creds.Region)
		assert.False(t, creds.IsExpired())
	})

	t.Run("token request fails", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		provider := NewIMDSv2Provider(WithIMDSEndpoint(server.URL))

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get IMDSv2 token")
	})

	t.Run("no IAM role attached", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case IMDSTokenPath:
				_, _ = w.Write([]byte(testMockToken))
			case IMDSCredentialsPath:
				// Return empty role list
				_, _ = w.Write([]byte(""))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		provider := NewIMDSv2Provider(WithIMDSEndpoint(server.URL))

		_, err := provider.Retrieve(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no IAM role attached")
	})

	t.Run("name returns IMDSv2Provider", func(t *testing.T) {
		provider := NewIMDSv2Provider()
		assert.Equal(t, "IMDSv2Provider", provider.Name())
	})
}

// TestCredentialChain tests the CredentialChain.
func TestCredentialChain(t *testing.T) {
	ctx := context.Background()

	t.Run("first provider succeeds", func(t *testing.T) {
		chain := NewCredentialChain(
			NewConfigProvider("FIRST_AKID", "FIRST_SECRET", "", ""),
			NewConfigProvider("SECOND_AKID", "SECOND_SECRET", "", ""),
		)

		creds, err := chain.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FIRST_AKID", creds.AccessKeyID)
	})

	t.Run("first provider fails, second succeeds", func(t *testing.T) {
		chain := NewCredentialChain(
			NewConfigProvider("", "", "", ""), // Will fail
			NewConfigProvider("SECOND_AKID", "SECOND_SECRET", "", ""),
		)

		creds, err := chain.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SECOND_AKID", creds.AccessKeyID)
	})

	t.Run("all providers fail", func(t *testing.T) {
		chain := NewCredentialChain(
			NewConfigProvider("", "", "", ""),
			NewConfigProvider("", "", "", ""),
		)

		_, err := chain.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("empty chain", func(t *testing.T) {
		chain := NewCredentialChain()

		_, err := chain.Retrieve(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoCredentials)
	})

	t.Run("name returns CredentialChain", func(t *testing.T) {
		chain := NewCredentialChain()
		assert.Equal(t, "CredentialChain", chain.Name())
	})
}

// TestNewDefaultProviderChain tests the default provider chain.
func TestNewDefaultProviderChain(t *testing.T) {
	chain := NewDefaultProviderChain()
	require.NotNil(t, chain)
	assert.Len(t, chain.providers, 3)

	// Verify the provider types
	assert.IsType(t, &EnvProvider{}, chain.providers[0])
	assert.IsType(t, &SharedConfigProvider{}, chain.providers[1])
	assert.IsType(t, &IMDSv2Provider{}, chain.providers[2])
}

// TestNewProviderFromSecret tests creating providers from secrets.
func TestNewProviderFromSecret(t *testing.T) {
	t.Run("nil secret returns error", func(t *testing.T) {
		_, err := NewProviderFromSecret(nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrProviderNotSupported)
	})

	t.Run("CONFIG provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderConfig,
			Options: SecretOptions{
				OptionKeyID:  "AKID",
				OptionSecret: "SECRET",
			},
		}

		provider, err := NewProviderFromSecret(secret)
		require.NoError(t, err)
		assert.IsType(t, &ConfigProvider{}, provider)
	})

	t.Run("ENV provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderEnv,
		}

		provider, err := NewProviderFromSecret(secret)
		require.NoError(t, err)
		assert.IsType(t, &EnvProvider{}, provider)
	})

	t.Run("CREDENTIAL_CHAIN provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderCredentialChain,
		}

		provider, err := NewProviderFromSecret(secret)
		require.NoError(t, err)
		assert.IsType(t, &CredentialChain{}, provider)
	})

	t.Run("CREDENTIAL_CHAIN with config credentials", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderCredentialChain,
			Options: SecretOptions{
				OptionKeyID:  "AKID",
				OptionSecret: "SECRET",
			},
		}

		provider, err := NewProviderFromSecret(secret)
		require.NoError(t, err)

		chain, ok := provider.(*CredentialChain)
		require.True(t, ok)
		// Should have ConfigProvider + EnvProvider + SharedConfigProvider + IMDSv2Provider
		assert.Len(t, chain.providers, 4)
		assert.IsType(t, &ConfigProvider{}, chain.providers[0])
	})

	t.Run("IAM provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderIAM,
		}

		provider, err := NewProviderFromSecret(secret)
		require.NoError(t, err)
		assert.IsType(t, &IMDSv2Provider{}, provider)
	})

	t.Run("unsupported provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderType("UNKNOWN"),
		}

		_, err := NewProviderFromSecret(secret)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrProviderNotSupported)
	})
}

// TestConfigFromSecret tests the helper function for creating credentials from a secret.
func TestConfigFromSecret(t *testing.T) {
	ctx := context.Background()

	t.Run("with CONFIG provider", func(t *testing.T) {
		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderConfig,
			Options: SecretOptions{
				OptionKeyID:  "AKID",
				OptionSecret: "SECRET",
				OptionRegion: "eu-west-1",
			},
		}

		creds, err := ConfigFromSecret(ctx, secret)
		require.NoError(t, err)
		assert.Equal(t, "AKID", creds.AccessKeyID)
		assert.Equal(t, "SECRET", creds.SecretAccessKey)
		assert.Equal(t, "eu-west-1", creds.Region)
	})

	t.Run("with ENV provider", func(t *testing.T) {
		t.Setenv(EnvAccessKeyID, "ENV_AKID")
		t.Setenv(EnvSecretAccessKey, "ENV_SECRET")
		t.Setenv(EnvRegion, "")

		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderEnv,
			Options: SecretOptions{
				OptionRegion: "ap-southeast-1", // Should override empty region
			},
		}

		creds, err := ConfigFromSecret(ctx, secret)
		require.NoError(t, err)
		assert.Equal(t, "ENV_AKID", creds.AccessKeyID)
		assert.Equal(t, "ap-southeast-1", creds.Region)
	})

	t.Run("provider error", func(t *testing.T) {
		t.Setenv(EnvAccessKeyID, "")
		t.Setenv(EnvSecretAccessKey, "")

		secret := &Secret{
			Name:     "test",
			Type:     SecretTypeS3,
			Provider: ProviderEnv,
		}

		_, err := ConfigFromSecret(ctx, secret)
		require.Error(t, err)
	})
}

// mockProvider is a mock CredentialProvider for testing.
type mockProvider struct {
	creds *Credentials
	err   error
	name  string
}

func (m *mockProvider) Retrieve(_ context.Context) (*Credentials, error) {
	return m.creds, m.err
}

func (m *mockProvider) Name() string {
	return m.name
}

// TestCredentialChain_WithMockProviders tests the chain with custom mock providers.
func TestCredentialChain_WithMockProviders(t *testing.T) {
	ctx := context.Background()

	t.Run("skips providers with invalid credentials", func(t *testing.T) {
		chain := NewCredentialChain(
			&mockProvider{
				creds: &Credentials{AccessKeyID: "AKID"}, // Invalid - missing secret
				err:   nil,
				name:  "mock1",
			},
			&mockProvider{
				creds: &Credentials{AccessKeyID: "AKID", SecretAccessKey: "SECRET"},
				err:   nil,
				name:  "mock2",
			},
		)

		creds, err := chain.Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "AKID", creds.AccessKeyID)
		assert.Equal(t, "SECRET", creds.SecretAccessKey)
	})
}
