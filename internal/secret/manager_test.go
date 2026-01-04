package secret

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSecret tests creating secrets.
func TestCreateSecret(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create a basic secret
	err := manager.CreateSecret(ctx, Secret{
		Name:     "test_secret",
		Type:     SecretTypeS3,
		Provider: ProviderConfig,
		Scope:    SecretScope{Type: ScopeGlobal},
		Options: SecretOptions{
			OptionKeyID:  "AKIAIOSFODNN7EXAMPLE",
			OptionSecret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
	})
	require.NoError(t, err)

	// Verify secret was created
	secret, err := manager.GetSecretByName(ctx, "test_secret")
	require.NoError(t, err)
	assert.Equal(t, "test_secret", secret.Name)
	assert.Equal(t, SecretTypeS3, secret.Type)
	assert.Equal(t, ProviderConfig, secret.Provider)
	assert.Equal(t, ScopeGlobal, secret.Scope.Type)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", secret.Options[OptionKeyID])
}

// TestCreateSecret_AlreadyExists tests creating a secret that already exists.
func TestCreateSecret_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create first secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "test_secret",
		Type: SecretTypeS3,
	})
	require.NoError(t, err)

	// Try to create duplicate
	err = manager.CreateSecret(ctx, Secret{
		Name: "test_secret",
		Type: SecretTypeS3,
	})
	assert.ErrorIs(t, err, ErrSecretExists)
}

// TestCreateSecret_InvalidName tests creating a secret with an invalid name.
func TestCreateSecret_InvalidName(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.CreateSecret(ctx, Secret{
		Name: "",
		Type: SecretTypeS3,
	})
	assert.ErrorIs(t, err, ErrInvalidSecretName)
}

// TestCreateSecret_InvalidType tests creating a secret with an invalid type.
func TestCreateSecret_InvalidType(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.CreateSecret(ctx, Secret{
		Name: "test_secret",
		Type: "",
	})
	assert.ErrorIs(t, err, ErrInvalidSecretType)
}

// TestCreateSecret_DefaultValues tests that default values are set.
func TestCreateSecret_DefaultValues(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.CreateSecret(ctx, Secret{
		Name: "test_secret",
		Type: SecretTypeS3,
	})
	require.NoError(t, err)

	secret, err := manager.GetSecretByName(ctx, "test_secret")
	require.NoError(t, err)
	assert.Equal(t, ProviderConfig, secret.Provider)
	assert.Equal(t, ScopeGlobal, secret.Scope.Type)
	assert.False(t, secret.CreatedAt.IsZero())
	assert.False(t, secret.UpdatedAt.IsZero())
}

// TestDropSecret tests dropping a secret.
func TestDropSecret(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create and then drop
	err := manager.CreateSecret(ctx, Secret{
		Name: "to_drop",
		Type: SecretTypeS3,
	})
	require.NoError(t, err)

	err = manager.DropSecret(ctx, "to_drop", false)
	require.NoError(t, err)

	// Verify it's gone
	_, err = manager.GetSecretByName(ctx, "to_drop")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestDropSecret_NotFound tests dropping a non-existent secret.
func TestDropSecret_NotFound(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.DropSecret(ctx, "nonexistent", false)
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestDropSecret_IfExists tests dropping a non-existent secret with IF EXISTS.
func TestDropSecret_IfExists(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.DropSecret(ctx, "nonexistent", true)
	assert.NoError(t, err)
}

// TestDropSecret_InvalidName tests dropping with invalid name.
func TestDropSecret_InvalidName(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.DropSecret(ctx, "", false)
	assert.ErrorIs(t, err, ErrInvalidSecretName)
}

// TestAlterSecret tests altering a secret's options.
func TestAlterSecret(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "to_alter",
		Type: SecretTypeS3,
		Options: SecretOptions{
			OptionKeyID:  "original_key",
			OptionRegion: "us-west-1",
		},
	})
	require.NoError(t, err)

	// Alter the secret
	err = manager.AlterSecret(ctx, "to_alter", SecretOptions{
		OptionKeyID:  "new_key",
		OptionSecret: "new_secret",
	})
	require.NoError(t, err)

	// Verify changes
	secret, err := manager.GetSecretByName(ctx, "to_alter")
	require.NoError(t, err)
	assert.Equal(t, "new_key", secret.Options[OptionKeyID])
	assert.Equal(t, "new_secret", secret.Options[OptionSecret])
	assert.Equal(t, "us-west-1", secret.Options[OptionRegion]) // Should be preserved
}

// TestAlterSecret_NotFound tests altering a non-existent secret.
func TestAlterSecret_NotFound(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.AlterSecret(ctx, "nonexistent", SecretOptions{
		OptionKeyID: "new_key",
	})
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestAlterSecret_InvalidName tests altering with invalid name.
func TestAlterSecret_InvalidName(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	err := manager.AlterSecret(ctx, "", SecretOptions{})
	assert.ErrorIs(t, err, ErrInvalidSecretName)
}

// TestGetSecret_GlobalScope tests getting a secret with global scope.
func TestGetSecret_GlobalScope(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create global scope secret
	err := manager.CreateSecret(ctx, Secret{
		Name:  "global_secret",
		Type:  SecretTypeS3,
		Scope: SecretScope{Type: ScopeGlobal},
		Options: SecretOptions{
			OptionKeyID: "global_key",
		},
	})
	require.NoError(t, err)

	// Should match any S3 URL
	secret, err := manager.GetSecret(ctx, "s3://any-bucket/any/path", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "global_secret", secret.Name)
}

// TestGetSecret_PathScope tests getting a secret with path scope.
func TestGetSecret_PathScope(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create path scope secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "path_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://my-bucket/data/",
		},
		Options: SecretOptions{
			OptionKeyID: "path_key",
		},
	})
	require.NoError(t, err)

	// Should match URLs with the prefix
	secret, err := manager.GetSecret(ctx, "s3://my-bucket/data/file.parquet", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "path_secret", secret.Name)

	// Should not match URLs without the prefix
	_, err = manager.GetSecret(ctx, "s3://other-bucket/file.parquet", SecretTypeS3)
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestGetSecret_HostScope tests getting a secret with host scope.
func TestGetSecret_HostScope(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create host scope secret
	err := manager.CreateSecret(ctx, Secret{
		Name: "host_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopeHost,
			Prefix: "s3://my-bucket",
		},
		Options: SecretOptions{
			OptionKeyID: "host_key",
		},
	})
	require.NoError(t, err)

	// Should match any path in the bucket
	secret, err := manager.GetSecret(ctx, "s3://my-bucket/any/path/file.parquet", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "host_secret", secret.Name)

	// Should not match other buckets
	_, err = manager.GetSecret(ctx, "s3://other-bucket/file.parquet", SecretTypeS3)
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestGetSecret_LongestPrefixMatch tests that the most specific scope wins.
func TestGetSecret_LongestPrefixMatch(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create global scope secret
	err := manager.CreateSecret(ctx, Secret{
		Name:  "global_secret",
		Type:  SecretTypeS3,
		Scope: SecretScope{Type: ScopeGlobal},
		Options: SecretOptions{
			OptionKeyID: "global_key",
		},
	})
	require.NoError(t, err)

	// Create bucket-level host scope secret
	err = manager.CreateSecret(ctx, Secret{
		Name: "bucket_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopeHost,
			Prefix: "s3://my-bucket",
		},
		Options: SecretOptions{
			OptionKeyID: "bucket_key",
		},
	})
	require.NoError(t, err)

	// Create path-specific secret
	err = manager.CreateSecret(ctx, Secret{
		Name: "path_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://my-bucket/specific/path/",
		},
		Options: SecretOptions{
			OptionKeyID: "path_key",
		},
	})
	require.NoError(t, err)

	// URL matching path_secret should return path_secret
	secret, err := manager.GetSecret(ctx, "s3://my-bucket/specific/path/file.parquet", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "path_secret", secret.Name)

	// URL matching bucket but not path should return bucket_secret
	secret, err = manager.GetSecret(ctx, "s3://my-bucket/other/path/file.parquet", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "bucket_secret", secret.Name)

	// URL not matching any bucket should return global_secret
	secret, err = manager.GetSecret(ctx, "s3://other-bucket/file.parquet", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "global_secret", secret.Name)
}

// TestGetSecret_TypeMismatch tests that secrets of different types don't match.
func TestGetSecret_TypeMismatch(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create S3 secret
	err := manager.CreateSecret(ctx, Secret{
		Name:  "s3_secret",
		Type:  SecretTypeS3,
		Scope: SecretScope{Type: ScopeGlobal},
	})
	require.NoError(t, err)

	// Should not find it when looking for GCS type
	_, err = manager.GetSecret(ctx, "gs://bucket/file", SecretTypeGCS)
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestGetSecret_NotFound tests when no matching secret exists.
func TestGetSecret_NotFound(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	_, err := manager.GetSecret(ctx, "s3://bucket/file", SecretTypeS3)
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestListSecrets tests listing all secrets.
func TestListSecrets(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create multiple secrets
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name:  "secret1",
		Type:  SecretTypeS3,
		Scope: SecretScope{Type: ScopeGlobal},
	}))
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name:  "secret2",
		Type:  SecretTypeGCS,
		Scope: SecretScope{Type: ScopeGlobal},
	}))
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name: "secret3",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://bucket/",
		},
	}))

	// List all secrets
	secrets, err := manager.ListSecrets(ctx, SecretScope{})
	require.NoError(t, err)
	assert.Len(t, secrets, 3)
}

// TestListSecrets_FilterByScope tests listing secrets filtered by scope.
func TestListSecrets_FilterByScope(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secrets with different scopes
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name:  "global1",
		Type:  SecretTypeS3,
		Scope: SecretScope{Type: ScopeGlobal},
	}))
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name:  "global2",
		Type:  SecretTypeGCS,
		Scope: SecretScope{Type: ScopeGlobal},
	}))
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name: "path1",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://bucket/",
		},
	}))

	// Filter by global scope
	secrets, err := manager.ListSecrets(ctx, SecretScope{Type: ScopeGlobal})
	require.NoError(t, err)
	assert.Len(t, secrets, 2)

	// Filter by path scope
	secrets, err = manager.ListSecrets(ctx, SecretScope{Type: ScopePath})
	require.NoError(t, err)
	assert.Len(t, secrets, 1)
	assert.Equal(t, "path1", secrets[0].Name)
}

// TestGetSecretByName tests getting a secret by name.
func TestGetSecretByName(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name: "named_secret",
		Type: SecretTypeS3,
	}))

	// Get by name
	secret, err := manager.GetSecretByName(ctx, "named_secret")
	require.NoError(t, err)
	assert.Equal(t, "named_secret", secret.Name)
}

// TestGetSecretByName_NotFound tests getting a non-existent secret by name.
func TestGetSecretByName_NotFound(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	_, err := manager.GetSecretByName(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestGetSecretByName_InvalidName tests getting with an invalid name.
func TestGetSecretByName_InvalidName(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	_, err := manager.GetSecretByName(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidSecretName)
}

// TestSecretClone tests that returned secrets are clones.
func TestSecretClone(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret
	require.NoError(t, manager.CreateSecret(ctx, Secret{
		Name: "clone_test",
		Type: SecretTypeS3,
		Options: SecretOptions{
			OptionKeyID: "original",
		},
	}))

	// Get secret and modify it
	secret1, err := manager.GetSecretByName(ctx, "clone_test")
	require.NoError(t, err)
	secret1.Options[OptionKeyID] = "modified"

	// Get secret again - should still have original value
	secret2, err := manager.GetSecretByName(ctx, "clone_test")
	require.NoError(t, err)
	assert.Equal(t, "original", secret2.Options[OptionKeyID])
}

// TestConcurrentAccess tests thread safety of the manager.
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create initial secrets
	for i := range 10 {
		require.NoError(t, manager.CreateSecret(ctx, Secret{
			Name:  "secret_" + string(rune('0'+i)),
			Type:  SecretTypeS3,
			Scope: SecretScope{Type: ScopeGlobal},
		}))
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for range 100 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			_, _ = manager.GetSecret(ctx, "s3://bucket/file", SecretTypeS3)
		}()
	}

	// Concurrent list
	for range 50 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			_, _ = manager.ListSecrets(ctx, SecretScope{})
		}()
	}

	// Concurrent alters
	for i := range 10 {
		wg.Add(1)
		idx := i

		go func() {
			defer wg.Done()
			_ = manager.AlterSecret(ctx, "secret_"+string(rune('0'+idx)), SecretOptions{
				"key": "value",
			})
		}()
	}

	wg.Wait()
}

// TestManagerWithCatalog tests manager with catalog persistence.
func TestManagerWithCatalog(t *testing.T) {
	ctx := context.Background()
	catalog := NewMemoryCatalog()
	manager := NewManager(catalog)

	// Create persistent secret
	err := manager.CreateSecret(ctx, Secret{
		Name:       "persistent_secret",
		Type:       SecretTypeS3,
		Persistent: true,
	})
	require.NoError(t, err)

	// Verify it's in catalog
	catalogSecret, err := catalog.GetSecret("persistent_secret")
	require.NoError(t, err)
	assert.Equal(t, "persistent_secret", catalogSecret.Name)

	// Drop and verify it's removed from catalog
	err = manager.DropSecret(ctx, "persistent_secret", false)
	require.NoError(t, err)

	_, err = catalog.GetSecret("persistent_secret")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestManagerNonPersistent tests that non-persistent secrets aren't stored in catalog.
func TestManagerNonPersistent(t *testing.T) {
	ctx := context.Background()
	catalog := NewMemoryCatalog()
	manager := NewManager(catalog)

	// Create non-persistent secret
	err := manager.CreateSecret(ctx, Secret{
		Name:       "non_persistent",
		Type:       SecretTypeS3,
		Persistent: false,
	})
	require.NoError(t, err)

	// Verify it's in manager but not in catalog
	secret, err := manager.GetSecretByName(ctx, "non_persistent")
	require.NoError(t, err)
	assert.Equal(t, "non_persistent", secret.Name)

	_, err = catalog.GetSecret("non_persistent")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestSecret_Clone tests the Secret.Clone method.
func TestSecret_Clone(t *testing.T) {
	original := &Secret{
		Name:     "test",
		Type:     SecretTypeS3,
		Provider: ProviderConfig,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://bucket/",
		},
		Options: SecretOptions{
			"key":    "value",
			"secret": "sensitive",
		},
		Persistent: true,
	}

	clone := original.Clone()

	// Verify values are equal
	assert.Equal(t, original.Name, clone.Name)
	assert.Equal(t, original.Type, clone.Type)
	assert.Equal(t, original.Provider, clone.Provider)
	assert.Equal(t, original.Scope, clone.Scope)
	assert.Equal(t, original.Options["key"], clone.Options["key"])
	assert.Equal(t, original.Persistent, clone.Persistent)

	// Modify clone and verify original is unchanged
	clone.Options["key"] = "modified"
	assert.Equal(t, "value", original.Options["key"])
}

// TestSecret_GetSetHasOption tests the Secret option helper methods.
func TestSecret_GetSetHasOption(t *testing.T) {
	secret := &Secret{
		Name: "test",
		Type: SecretTypeS3,
	}

	// Initially no options
	assert.Equal(t, "", secret.GetOption("key"))
	assert.False(t, secret.HasOption("key"))

	// Set option
	secret.SetOption("key", "value")
	assert.Equal(t, "value", secret.GetOption("key"))
	assert.True(t, secret.HasOption("key"))

	// Set another option
	secret.SetOption("other", "data")
	assert.Equal(t, "data", secret.GetOption("other"))
}

// TestExtractHost tests the host extraction helper.
func TestExtractHost(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"s3://my-bucket/path/file", "my-bucket"},
		{"s3://my-bucket", "my-bucket"},
		{"gs://gcs-bucket/object", "gcs-bucket"},
		{"https://example.com/path", "example.com"},
		{"https://example.com:8080/path", "example.com:8080"},
		{"azure://container/blob", "container"},
		{"my-bucket/path", "my-bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := extractHost(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMemoryCatalog tests the MemoryCatalog implementation.
func TestMemoryCatalog(t *testing.T) {
	catalog := NewMemoryCatalog()

	// Set a secret
	err := catalog.SetSecret(Secret{
		Name: "catalog_secret",
		Type: SecretTypeS3,
		Options: SecretOptions{
			"key": "value",
		},
	})
	require.NoError(t, err)

	// Get the secret
	secret, err := catalog.GetSecret("catalog_secret")
	require.NoError(t, err)
	assert.Equal(t, "catalog_secret", secret.Name)
	assert.Equal(t, "value", secret.Options["key"])

	// List secrets
	secrets, err := catalog.ListSecrets()
	require.NoError(t, err)
	assert.Len(t, secrets, 1)

	// Count
	assert.Equal(t, 1, catalog.Count())

	// Update secret
	err = catalog.SetSecret(Secret{
		Name: "catalog_secret",
		Type: SecretTypeS3,
		Options: SecretOptions{
			"key": "new_value",
		},
	})
	require.NoError(t, err)

	secret, err = catalog.GetSecret("catalog_secret")
	require.NoError(t, err)
	assert.Equal(t, "new_value", secret.Options["key"])

	// Delete secret
	err = catalog.DeleteSecret("catalog_secret")
	require.NoError(t, err)

	_, err = catalog.GetSecret("catalog_secret")
	assert.ErrorIs(t, err, ErrSecretNotFound)

	// Count after delete
	assert.Equal(t, 0, catalog.Count())

	// Clear
	err = catalog.SetSecret(Secret{Name: "s1", Type: SecretTypeS3})
	require.NoError(t, err)
	err = catalog.SetSecret(Secret{Name: "s2", Type: SecretTypeS3})
	require.NoError(t, err)

	assert.Equal(t, 2, catalog.Count())
	catalog.Clear()
	assert.Equal(t, 0, catalog.Count())
}

// TestMemoryCatalog_DeleteNotFound tests deleting a non-existent secret from catalog.
func TestMemoryCatalog_DeleteNotFound(t *testing.T) {
	catalog := NewMemoryCatalog()

	err := catalog.DeleteSecret("nonexistent")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

// TestCaseInsensitiveMatching tests that scope matching is case-insensitive.
func TestCaseInsensitiveMatching(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	// Create secret with lowercase prefix
	err := manager.CreateSecret(ctx, Secret{
		Name: "lower_secret",
		Type: SecretTypeS3,
		Scope: SecretScope{
			Type:   ScopePath,
			Prefix: "s3://my-bucket/path/",
		},
	})
	require.NoError(t, err)

	// Should match with different case
	secret, err := manager.GetSecret(ctx, "S3://MY-BUCKET/PATH/file", SecretTypeS3)
	require.NoError(t, err)
	assert.Equal(t, "lower_secret", secret.Name)
}

// TestAllSecretTypes tests creating secrets for all supported types.
func TestAllSecretTypes(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	types := []SecretType{
		SecretTypeS3,
		SecretTypeGCS,
		SecretTypeAzure,
		SecretTypeHTTP,
		SecretTypeHuggingFace,
	}

	for _, secretType := range types {
		t.Run(string(secretType), func(t *testing.T) {
			err := manager.CreateSecret(ctx, Secret{
				Name:  "secret_" + string(secretType),
				Type:  secretType,
				Scope: SecretScope{Type: ScopeGlobal},
			})
			require.NoError(t, err)

			secret, err := manager.GetSecretByName(ctx, "secret_"+string(secretType))
			require.NoError(t, err)
			assert.Equal(t, secretType, secret.Type)
		})
	}
}

// TestAllProviderTypes tests creating secrets with all provider types.
func TestAllProviderTypes(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil)

	providers := []ProviderType{
		ProviderConfig,
		ProviderEnv,
		ProviderCredentialChain,
		ProviderIAM,
	}

	for _, provider := range providers {
		t.Run(string(provider), func(t *testing.T) {
			err := manager.CreateSecret(ctx, Secret{
				Name:     "secret_" + string(provider),
				Type:     SecretTypeS3,
				Provider: provider,
			})
			require.NoError(t, err)

			secret, err := manager.GetSecretByName(ctx, "secret_"+string(provider))
			require.NoError(t, err)
			assert.Equal(t, provider, secret.Provider)
		})
	}
}
