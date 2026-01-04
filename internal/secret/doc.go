// Package secret provides secret management for cloud storage credentials.
//
// The secret package implements DuckDB-compatible secret storage with support for
// multiple cloud providers (S3, GCS, Azure, HTTP) and path-based scope matching.
//
// # Secret Types
//
// Secrets can be created for different cloud storage providers:
//   - S3: Amazon S3 and S3-compatible storage
//   - GCS: Google Cloud Storage
//   - Azure: Azure Blob Storage
//   - HTTP: HTTP/HTTPS URLs with authentication
//   - HuggingFace: HuggingFace Hub credentials
//
// # Credential Providers
//
// Secrets support different credential provider types:
//   - CONFIG: Explicitly provided credentials (access key, secret key)
//   - ENV: Credentials from environment variables
//   - CREDENTIAL_CHAIN: Default credential provider chain
//   - IAM: IAM role credentials (from instance metadata)
//
// # Scope Matching
//
// Secrets use path-based scope matching to determine which secret applies to a URL:
//   - GLOBAL: Matches all URLs of the secret type
//   - PATH: Matches URLs with a specific prefix (e.g., s3://bucket/path)
//   - HOST: Matches URLs with a specific host/bucket
//
// When multiple secrets match a URL, the most specific match (longest prefix) is used.
//
// # Example Usage
//
//	// Create a secret manager with in-memory catalog
//	catalog := secret.NewMemoryCatalog()
//	manager := secret.NewManager(catalog)
//
//	// Create an S3 secret with global scope
//	err := manager.CreateSecret(ctx, secret.Secret{
//	    Name:     "my_s3_secret",
//	    Type:     secret.SecretTypeS3,
//	    Provider: secret.ProviderConfig,
//	    Scope:    secret.SecretScope{Type: secret.ScopeGlobal},
//	    Options: secret.SecretOptions{
//	        "key_id":  "AKIAIOSFODNN7EXAMPLE",
//	        "secret":  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//	        "region":  "us-west-2",
//	    },
//	    Persistent: true,
//	})
//
//	// Create a bucket-specific secret with path scope
//	err = manager.CreateSecret(ctx, secret.Secret{
//	    Name:     "bucket_secret",
//	    Type:     secret.SecretTypeS3,
//	    Provider: secret.ProviderConfig,
//	    Scope:    secret.SecretScope{
//	        Type:   secret.ScopePath,
//	        Prefix: "s3://my-bucket/data/",
//	    },
//	    Options: secret.SecretOptions{
//	        "key_id":  "AKIAIOSFODNN7OTHER",
//	        "secret":  "differentKey",
//	        "region":  "us-east-1",
//	    },
//	})
//
//	// Get the best matching secret for a URL
//	// This will return bucket_secret because it has a more specific scope
//	secret, err := manager.GetSecret(ctx, "s3://my-bucket/data/file.parquet", secret.SecretTypeS3)
//
// # Persistence
//
// Secrets can be stored persistently using a Catalog implementation.
// The MemoryCatalog provides in-memory storage for testing.
// Production use would typically use a DuckDB catalog-backed implementation.
package secret
