package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/secret"
)

// testSecretManager wraps secret.Manager and implements BOTH the executor's SecretManager interface
// and secret.Manager interface. This allows it to be used in tests where getSecretManager() attempts
// to cast to secret.Manager.
type testSecretManager struct {
	mgr secret.Manager
}

func newTestSecretManager() *testSecretManager {
	return &testSecretManager{
		mgr: secret.NewManager(nil),
	}
}

// --- Executor's SecretManager interface methods ---

// Create implements the executor's SecretManager interface.
func (t *testSecretManager) Create(ctx context.Context, name string, secretType string, provider string, scope string, persistent bool, options map[string]string) error {
	s := secret.Secret{
		Name:       name,
		Type:       secret.SecretType(secretType),
		Provider:   secret.ProviderType(provider),
		Persistent: persistent,
		Options:    make(secret.SecretOptions),
	}
	for k, v := range options {
		s.Options[k] = v
	}
	// Parse scope
	if scope == "" {
		s.Scope = secret.SecretScope{Type: secret.ScopeGlobal}
	} else {
		s.Scope = secret.SecretScope{Type: secret.ScopePath, Prefix: scope}
	}
	return t.mgr.CreateSecret(ctx, s)
}

// Delete implements the executor's SecretManager interface.
func (t *testSecretManager) Delete(ctx context.Context, name string) error {
	return t.mgr.DropSecret(ctx, name, false)
}

// Update implements the executor's SecretManager interface.
func (t *testSecretManager) Update(ctx context.Context, name string, options map[string]string) error {
	opts := make(secret.SecretOptions)
	for k, v := range options {
		opts[k] = v
	}
	return t.mgr.AlterSecret(ctx, name, opts)
}

// Get implements the executor's SecretManager interface.
func (t *testSecretManager) Get(ctx context.Context, name string) (interface{}, error) {
	return t.mgr.GetSecretByName(ctx, name)
}

// Exists implements the executor's SecretManager interface.
func (t *testSecretManager) Exists(ctx context.Context, name string) bool {
	_, err := t.mgr.GetSecretByName(ctx, name)
	return err == nil
}

// --- secret.Manager interface methods (for getSecretManager() casting) ---

// CreateSecret implements secret.Manager.
func (t *testSecretManager) CreateSecret(ctx context.Context, s secret.Secret) error {
	return t.mgr.CreateSecret(ctx, s)
}

// DropSecret implements secret.Manager.
func (t *testSecretManager) DropSecret(ctx context.Context, name string, ifExists bool) error {
	return t.mgr.DropSecret(ctx, name, ifExists)
}

// AlterSecret implements secret.Manager.
func (t *testSecretManager) AlterSecret(ctx context.Context, name string, opts secret.SecretOptions) error {
	return t.mgr.AlterSecret(ctx, name, opts)
}

// GetSecret implements secret.Manager.
func (t *testSecretManager) GetSecret(ctx context.Context, url string, secretType secret.SecretType) (*secret.Secret, error) {
	return t.mgr.GetSecret(ctx, url, secretType)
}

// GetSecretByName implements secret.Manager.
func (t *testSecretManager) GetSecretByName(ctx context.Context, name string) (*secret.Secret, error) {
	return t.mgr.GetSecretByName(ctx, name)
}

// ListSecrets implements secret.Manager.
func (t *testSecretManager) ListSecrets(ctx context.Context, scope secret.SecretScope) ([]secret.Secret, error) {
	return t.mgr.ListSecrets(ctx, scope)
}

// Ensure testSecretManager implements both interfaces
var _ SecretManager = (*testSecretManager)(nil)
var _ secret.Manager = (*testSecretManager)(nil)

// TestWhichSecret_Found tests which_secret when a matching secret exists.
func TestWhichSecret_Found(t *testing.T) {
	mgr := newTestSecretManager()

	// Create a test secret
	err := mgr.CreateSecret(context.Background(), secret.Secret{
		Name:       "my_s3_secret",
		Type:       secret.SecretTypeS3,
		Provider:   secret.ProviderConfig,
		Scope:      secret.SecretScope{Type: secret.ScopeGlobal},
		Persistent: true,
	})
	require.NoError(t, err)

	// Create executor with secret manager
	e := &Executor{
		secretManager: mgr,
	}

	// Create execution context
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create plan with arguments
	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg0": "s3://my-bucket/data/file.csv",
			"arg1": "S3",
		},
	}

	// Execute
	result, err := e.executeWhichSecret(ctx, plan)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Verify result
	assert.Equal(t, "my_s3_secret", result.Rows[0]["name"])
	assert.Equal(t, "local_file", result.Rows[0]["persistent"])
	assert.Equal(t, "local_file", result.Rows[0]["storage"])
}

// TestWhichSecret_NotFound tests which_secret when no matching secret exists.
func TestWhichSecret_NotFound(t *testing.T) {
	mgr := newTestSecretManager()

	// Create executor with secret manager
	e := &Executor{
		secretManager: mgr,
	}

	// Create execution context
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create plan with arguments
	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg0": "s3://my-bucket/data/file.csv",
			"arg1": "S3",
		},
	}

	// Execute
	result, err := e.executeWhichSecret(ctx, plan)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

// TestWhichSecret_NoSecretManager tests which_secret when no secret manager is set.
func TestWhichSecret_NoSecretManager(t *testing.T) {
	e := &Executor{
		secretManager: nil,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg0": "s3://my-bucket/data/file.csv",
			"arg1": "S3",
		},
	}

	// Execute - should return empty result
	result, err := e.executeWhichSecret(ctx, plan)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

// TestWhichSecret_MissingPath tests which_secret with missing path argument.
func TestWhichSecret_MissingPath(t *testing.T) {
	mgr := newTestSecretManager()
	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg1": "S3",
		},
	}

	_, err := e.executeWhichSecret(ctx, plan)
	assert.Error(t, err)
}

// TestWhichSecret_MissingType tests which_secret with missing type argument.
func TestWhichSecret_MissingType(t *testing.T) {
	mgr := newTestSecretManager()
	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg0": "s3://my-bucket/data/file.csv",
		},
	}

	_, err := e.executeWhichSecret(ctx, plan)
	assert.Error(t, err)
}

// TestWhichSecret_MemoryStorage tests which_secret with non-persistent secret.
func TestWhichSecret_MemoryStorage(t *testing.T) {
	mgr := newTestSecretManager()

	// Create a non-persistent secret
	err := mgr.CreateSecret(context.Background(), secret.Secret{
		Name:       "temp_secret",
		Type:       secret.SecretTypeS3,
		Scope:      secret.SecretScope{Type: secret.ScopeGlobal},
		Persistent: false,
	})
	require.NoError(t, err)

	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "which_secret",
		Options: map[string]any{
			"arg0": "s3://bucket/file",
			"arg1": "S3",
		},
	}

	result, err := e.executeWhichSecret(ctx, plan)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Non-persistent secrets should show "memory" storage
	assert.Equal(t, "memory", result.Rows[0]["persistent"])
	assert.Equal(t, "memory", result.Rows[0]["storage"])
}

// TestDuckDBSecrets_Empty tests duckdb_secrets when no secrets exist.
func TestDuckDBSecrets_Empty(t *testing.T) {
	mgr := newTestSecretManager()
	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "duckdb_secrets",
		Options:      map[string]any{},
	}

	result, err := e.executeDuckDBSecrets(ctx, plan)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
	assert.Equal(t, []string{"name", "type", "provider", "persistent", "storage", "scope", "secret_string"}, result.Columns)
}

// TestDuckDBSecrets_WithSecrets tests duckdb_secrets with multiple secrets.
func TestDuckDBSecrets_WithSecrets(t *testing.T) {
	mgr := newTestSecretManager()

	// Create multiple secrets
	err := mgr.CreateSecret(context.Background(), secret.Secret{
		Name:       "s3_secret",
		Type:       secret.SecretTypeS3,
		Provider:   secret.ProviderConfig,
		Scope:      secret.SecretScope{Type: secret.ScopeGlobal},
		Persistent: true,
		Options: secret.SecretOptions{
			"key_id": "AKIAIOSFODNN7EXAMPLE",
			"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"region": "us-west-2",
		},
	})
	require.NoError(t, err)

	err = mgr.CreateSecret(context.Background(), secret.Secret{
		Name:       "gcs_secret",
		Type:       secret.SecretTypeGCS,
		Provider:   secret.ProviderCredentialChain,
		Scope:      secret.SecretScope{Type: secret.ScopePath, Prefix: "gs://my-bucket/"},
		Persistent: false,
	})
	require.NoError(t, err)

	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "duckdb_secrets",
		Options:      map[string]any{},
	}

	result, err := e.executeDuckDBSecrets(ctx, plan)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)

	// Find the S3 secret row
	var s3Row map[string]any
	var gcsRow map[string]any
	for _, row := range result.Rows {
		if row["name"] == "s3_secret" {
			s3Row = row
		} else if row["name"] == "gcs_secret" {
			gcsRow = row
		}
	}

	require.NotNil(t, s3Row)
	require.NotNil(t, gcsRow)

	// Verify S3 secret
	assert.Equal(t, "S3", s3Row["type"])
	assert.Equal(t, "CONFIG", s3Row["provider"])
	assert.Equal(t, true, s3Row["persistent"])
	assert.Equal(t, "local_file", s3Row["storage"])

	// Verify GCS secret
	assert.Equal(t, "GCS", gcsRow["type"])
	assert.Equal(t, "CREDENTIAL_CHAIN", gcsRow["provider"])
	assert.Equal(t, false, gcsRow["persistent"])
	assert.Equal(t, "memory", gcsRow["storage"])

	// Verify scope for GCS (should have a prefix)
	gcsScope, ok := gcsRow["scope"].([]string)
	require.True(t, ok)
	assert.Contains(t, gcsScope, "gs://my-bucket/")
}

// TestDuckDBSecrets_NoSecretManager tests duckdb_secrets with no secret manager.
func TestDuckDBSecrets_NoSecretManager(t *testing.T) {
	e := &Executor{
		secretManager: nil,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "duckdb_secrets",
		Options:      map[string]any{},
	}

	result, err := e.executeDuckDBSecrets(ctx, plan)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

// TestDuckDBSecrets_SecretStringRedaction tests that sensitive data is redacted.
func TestDuckDBSecrets_SecretStringRedaction(t *testing.T) {
	mgr := newTestSecretManager()

	// Create a secret with sensitive options
	err := mgr.CreateSecret(context.Background(), secret.Secret{
		Name:     "redact_test",
		Type:     secret.SecretTypeS3,
		Provider: secret.ProviderConfig,
		Scope:    secret.SecretScope{Type: secret.ScopeGlobal},
		Options: secret.SecretOptions{
			"key_id": "AKIAIOSFODNN7EXAMPLE",
			"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"region": "us-west-2",
		},
	})
	require.NoError(t, err)

	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	plan := &planner.PhysicalTableFunctionScan{
		FunctionName: "duckdb_secrets",
		Options:      map[string]any{},
	}

	result, err := e.executeDuckDBSecrets(ctx, plan)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	secretStr, ok := result.Rows[0]["secret_string"].(string)
	require.True(t, ok)

	// Verify that secret is redacted (should contain ***** not the actual key)
	assert.Contains(t, secretStr, "secret=*****")
	// Verify that key_id is partially redacted
	assert.Contains(t, secretStr, "key_id=AKI***")
	// Verify that region is NOT redacted (non-sensitive)
	assert.Contains(t, secretStr, "region=us-west-2")
}

// TestRedactValue tests the redaction logic for various keys.
func TestRedactValue(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		expected string
	}{
		// Fully sensitive keys
		{"secret", "mysecretvalue", "*****"},
		{"secret_access_key", "mykey", "*****"},
		{"account_key", "myaccountkey", "*****"},
		{"bearer_token", "mytoken", "*****"},
		{"password", "mypassword", "*****"},

		// Partially sensitive keys
		{"key_id", "AKIAIOSFODNN7EXAMPLE", "AKI***"},
		{"access_key_id", "AKIAIOSFODNN7EXAMPLE", "AKI***"},
		{"account_name", "myaccount", "mya***"},
		{"client_id", "client123", "cli***"},

		// Short values (should not cause panic)
		{"key_id", "AK", "AK"},

		// Non-sensitive keys
		{"region", "us-west-2", "us-west-2"},
		{"endpoint", "https://s3.amazonaws.com", "https://s3.amazonaws.com"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"_"+tt.value, func(t *testing.T) {
			result := redactValue(tt.key, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsSecretSystemFunction tests the function name checker.
func TestIsSecretSystemFunction(t *testing.T) {
	assert.True(t, IsSecretSystemFunction("which_secret"))
	assert.True(t, IsSecretSystemFunction("WHICH_SECRET"))
	assert.True(t, IsSecretSystemFunction("Which_Secret"))
	assert.True(t, IsSecretSystemFunction("duckdb_secrets"))
	assert.True(t, IsSecretSystemFunction("DUCKDB_SECRETS"))

	assert.False(t, IsSecretSystemFunction("read_csv"))
	assert.False(t, IsSecretSystemFunction("read_parquet"))
	assert.False(t, IsSecretSystemFunction("unknown"))
}

// TestWhichSecret_DifferentTypes tests which_secret with different secret types.
func TestWhichSecret_DifferentTypes(t *testing.T) {
	mgr := newTestSecretManager()

	// Create secrets for different types
	secrets := []struct {
		name       string
		secretType secret.SecretType
	}{
		{"s3_secret", secret.SecretTypeS3},
		{"gcs_secret", secret.SecretTypeGCS},
		{"azure_secret", secret.SecretTypeAzure},
		{"http_secret", secret.SecretTypeHTTP},
	}

	for _, s := range secrets {
		err := mgr.CreateSecret(context.Background(), secret.Secret{
			Name:  s.name,
			Type:  s.secretType,
			Scope: secret.SecretScope{Type: secret.ScopeGlobal},
		})
		require.NoError(t, err)
	}

	e := &Executor{
		secretManager: mgr,
	}

	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Test each type
	for _, s := range secrets {
		t.Run(string(s.secretType), func(t *testing.T) {
			plan := &planner.PhysicalTableFunctionScan{
				FunctionName: "which_secret",
				Options: map[string]any{
					"arg0": "some://url/path",
					"arg1": string(s.secretType),
				},
			}

			result, err := e.executeWhichSecret(ctx, plan)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, s.name, result.Rows[0]["name"])
		})
	}
}

// TestRedactedSecretString tests the complete secret string generation.
func TestRedactedSecretString(t *testing.T) {
	s := &secret.Secret{
		Name:     "test_secret",
		Type:     secret.SecretTypeS3,
		Provider: secret.ProviderConfig,
		Scope: secret.SecretScope{
			Type:   secret.ScopePath,
			Prefix: "s3://my-bucket/",
		},
		Options: secret.SecretOptions{
			"key_id": "AKIAIOSFODNN7EXAMPLE",
			"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
	}

	result := redactedSecretString(s)

	// Check that result contains expected components
	assert.Contains(t, result, "type=S3")
	assert.Contains(t, result, "provider=CONFIG")
	assert.Contains(t, result, "scope=s3://my-bucket/")
	assert.Contains(t, result, "key_id=AKI***")
	assert.Contains(t, result, "secret=*****")

	// Verify the actual secret value is NOT in the output
	assert.NotContains(t, result, "wJalrXUtnFEMI")
	assert.NotContains(t, result, "AKIAIOSFODNN7EXAMPLE")
}
