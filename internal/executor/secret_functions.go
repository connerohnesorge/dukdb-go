// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/secret"
)

// whichSecret finds the secret that would be used for a given path and type.
// This implements the which_secret(path, type) table function.
//
// Returns a result with columns: name, persistent, storage
// If no matching secret is found, returns an empty result set.
func (e *Executor) executeWhichSecret(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Get the arguments from plan options
	path, pathOk := plan.Options["arg0"].(string)
	if !pathOk || path == "" {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "which_secret requires a path string as first argument",
		}
	}

	secretTypeStr, typeOk := plan.Options["arg1"].(string)
	if !typeOk || secretTypeStr == "" {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "which_secret requires a type string as second argument",
		}
	}

	// Get the secret manager
	mgr := e.getSecretManager()
	if mgr == nil {
		// No secret manager - return empty result
		return &ExecutionResult{
			Rows:    []map[string]any{},
			Columns: []string{"name", "persistent", "storage"},
		}, nil
	}

	// Convert string type to SecretType
	secretType := secret.SecretType(strings.ToUpper(secretTypeStr))

	// Look up the secret
	foundSecret, err := mgr.GetSecret(ctx.Context, path, secretType)
	if err != nil {
		// No matching secret found - return empty result
		if err == secret.ErrSecretNotFound {
			return &ExecutionResult{
				Rows:    []map[string]any{},
				Columns: []string{"name", "persistent", "storage"},
			}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "failed to lookup secret: " + err.Error(),
		}
	}

	// Determine storage type
	storageType := "memory"
	if foundSecret.Persistent {
		storageType = "local_file"
	}

	// Return result with the found secret
	result := &ExecutionResult{
		Rows: []map[string]any{
			{
				"name":       foundSecret.Name,
				"persistent": storageType,
				"storage":    storageType,
			},
		},
		Columns: []string{"name", "persistent", "storage"},
	}

	return result, nil
}

// duckdbSecrets returns all registered secrets with redacted sensitive information.
// This implements the duckdb_secrets() table function.
//
// Returns a result with columns: name, type, provider, persistent, storage, scope, secret_string
// Sensitive information is redacted (e.g., keys shown as "***").
func (e *Executor) executeDuckDBSecrets(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Define result columns
	columns := []string{
		"name",
		"type",
		"provider",
		"persistent",
		"storage",
		"scope",
		"secret_string",
	}

	// Get the secret manager
	mgr := e.getSecretManager()
	if mgr == nil {
		// No secret manager - return empty result
		return &ExecutionResult{
			Rows:    []map[string]any{},
			Columns: columns,
		}, nil
	}

	// List all secrets (no scope filter)
	secrets, err := mgr.ListSecrets(ctx.Context, secret.SecretScope{})
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "failed to list secrets: " + err.Error(),
		}
	}

	// Build result rows
	rows := make([]map[string]any, 0, len(secrets))
	for _, s := range secrets {
		// Determine storage type
		storageType := "memory"
		if s.Persistent {
			storageType = "local_file"
		}

		// Build scope as array of strings
		var scope []string
		if s.Scope.Prefix != "" {
			scope = []string{s.Scope.Prefix}
		} else {
			scope = []string{}
		}

		// Build redacted secret string
		secretStr := redactedSecretString(&s)

		row := map[string]any{
			"name":          s.Name,
			"type":          string(s.Type),
			"provider":      string(s.Provider),
			"persistent":    s.Persistent,
			"storage":       storageType,
			"scope":         scope,
			"secret_string": secretStr,
		}
		rows = append(rows, row)
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: columns,
	}, nil
}

// redactedSecretString builds a redacted string representation of a secret.
// Sensitive values are replaced with asterisks or partial values.
func redactedSecretString(s *secret.Secret) string {
	// Build a key=value representation with redacted values
	var parts []string

	// Add type
	parts = append(parts, "type="+string(s.Type))

	// Add provider
	parts = append(parts, "provider="+string(s.Provider))

	// Add scope if present
	if s.Scope.Prefix != "" {
		parts = append(parts, "scope="+s.Scope.Prefix)
	}

	// Add options with redaction
	for key, value := range s.Options {
		redactedValue := redactValue(key, value)
		parts = append(parts, key+"="+redactedValue)
	}

	return strings.Join(parts, ";")
}

// redactValue returns a redacted version of a secret option value.
// Sensitive keys have their values hidden or partially shown.
func redactValue(key, value string) string {
	// List of sensitive keys that should be fully redacted
	sensitiveKeys := map[string]bool{
		"secret":               true,
		"secret_access_key":    true,
		"account_key":          true,
		"client_secret":        true,
		"bearer_token":         true,
		"connection_string":    true,
		"service_account_json": true,
		"session_token":        true,
		"password":             true,
		"api_key":              true,
		"token":                true,
	}

	// List of keys that should be partially redacted
	partialKeys := map[string]bool{
		"key_id":        true,
		"access_key_id": true,
		"account_name":  true,
		"client_id":     true,
		"tenant_id":     true,
	}

	keyLower := strings.ToLower(key)

	// Check if fully sensitive
	if sensitiveKeys[keyLower] {
		return "*****"
	}

	// Check if partially sensitive
	if partialKeys[keyLower] && len(value) > 3 {
		// Show first 3 characters followed by asterisks
		return value[:3] + "***"
	}

	// Non-sensitive value - return as-is
	return value
}

// getWhichSecretColumns returns the column definitions for the which_secret table function.
func getWhichSecretColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("persistent", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("storage", dukdb.TYPE_VARCHAR),
	}
}

// getDuckDBSecretsColumns returns the column definitions for the duckdb_secrets table function.
func getDuckDBSecretsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("provider", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("persistent", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("storage", dukdb.TYPE_VARCHAR),
		// Note: scope is a list type, but we represent it as VARCHAR for simplicity
		catalog.NewColumnDef("scope", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("secret_string", dukdb.TYPE_VARCHAR),
	}
}

// IsSecretSystemFunction checks if a function name is a secret system function.
func IsSecretSystemFunction(name string) bool {
	switch strings.ToLower(name) {
	case "which_secret", "duckdb_secrets":
		return true
	default:
		return false
	}
}
