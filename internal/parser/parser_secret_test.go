package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCreateSecret(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantName    string
		wantType    string
		wantErr     bool
		persistent  bool
		orReplace   bool
		ifNotExists bool
		options     map[string]string
	}{
		{
			name: "Simple S3 secret",
			sql: `CREATE SECRET my_secret (
				TYPE S3,
				KEY_ID 'AKIAIOSFODNN7EXAMPLE',
				SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
				REGION 'us-east-1'
			)`,
			wantName: "my_secret",
			wantType: "S3",
			options: map[string]string{
				"key_id": "AKIAIOSFODNN7EXAMPLE",
				"secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"region": "us-east-1",
			},
		},
		{
			name: "Persistent secret",
			sql: `CREATE PERSISTENT SECRET my_persistent_secret (
				TYPE S3,
				KEY_ID 'key123'
			)`,
			wantName:   "my_persistent_secret",
			wantType:   "S3",
			persistent: true,
			options: map[string]string{
				"key_id": "key123",
			},
		},
		{
			name: "Temporary secret",
			sql: `CREATE TEMPORARY SECRET my_temp_secret (
				TYPE HTTP,
				BEARER_TOKEN 'token123'
			)`,
			wantName:   "my_temp_secret",
			wantType:   "HTTP",
			persistent: false,
			options: map[string]string{
				"bearer_token": "token123",
			},
		},
		{
			name: "OR REPLACE secret",
			sql: `CREATE OR REPLACE SECRET my_secret (
				TYPE GCS,
				PROJECT_ID 'my-project'
			)`,
			wantName:  "my_secret",
			wantType:  "GCS",
			orReplace: true,
			options: map[string]string{
				"project_id": "my-project",
			},
		},
		{
			name: "IF NOT EXISTS secret",
			sql: `CREATE SECRET IF NOT EXISTS my_secret (
				TYPE AZURE,
				ACCOUNT_NAME 'myaccount'
			)`,
			wantName:    "my_secret",
			wantType:    "AZURE",
			ifNotExists: true,
			options: map[string]string{
				"account_name": "myaccount",
			},
		},
		{
			name: "Secret with scope",
			sql: `CREATE SECRET scoped_secret (
				TYPE S3,
				SCOPE 's3://my-bucket/path',
				KEY_ID 'key123'
			)`,
			wantName: "scoped_secret",
			wantType: "S3",
			options: map[string]string{
				"key_id": "key123",
			},
		},
		{
			name: "Secret with provider",
			sql: `CREATE SECRET my_secret (
				TYPE S3,
				PROVIDER CONFIG,
				KEY_ID 'key123'
			)`,
			wantName: "my_secret",
			wantType: "S3",
			options: map[string]string{
				"key_id": "key123",
			},
		},
		{
			name:    "Missing TYPE",
			sql:     `CREATE SECRET my_secret (KEY_ID 'key123')`,
			wantErr: true,
		},
		{
			name:    "Missing parentheses",
			sql:     `CREATE SECRET my_secret TYPE S3`,
			wantErr: true,
		},
		{
			name:    "Missing secret name",
			sql:     `CREATE SECRET (TYPE S3)`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			createSecret, ok := stmt.(*CreateSecretStmt)
			require.True(t, ok, "expected CreateSecretStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, createSecret.Name)
			assert.Equal(t, tt.wantType, createSecret.SecretType)
			assert.Equal(t, tt.persistent, createSecret.Persistent)
			assert.Equal(t, tt.orReplace, createSecret.OrReplace)
			assert.Equal(t, tt.ifNotExists, createSecret.IfNotExists)

			// Check options
			for k, v := range tt.options {
				assert.Equal(t, v, createSecret.Options[k], "option %s mismatch", k)
			}
		})
	}
}

func TestParseDropSecret(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		ifExists bool
		wantErr  bool
	}{
		{
			name:     "Simple drop",
			sql:      "DROP SECRET my_secret",
			wantName: "my_secret",
			ifExists: false,
		},
		{
			name:     "Drop with IF EXISTS",
			sql:      "DROP SECRET IF EXISTS my_secret",
			wantName: "my_secret",
			ifExists: true,
		},
		{
			name:    "Missing secret name",
			sql:     "DROP SECRET",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			dropSecret, ok := stmt.(*DropSecretStmt)
			require.True(t, ok, "expected DropSecretStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, dropSecret.Name)
			assert.Equal(t, tt.ifExists, dropSecret.IfExists)
		})
	}
}

func TestParseAlterSecret(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		options  map[string]string
		wantErr  bool
	}{
		{
			name:     "Alter single option",
			sql:      "ALTER SECRET my_secret (REGION 'us-west-2')",
			wantName: "my_secret",
			options: map[string]string{
				"region": "us-west-2",
			},
		},
		{
			name:     "Alter multiple options",
			sql:      "ALTER SECRET my_secret (KEY_ID 'newkey', SECRET 'newsecret')",
			wantName: "my_secret",
			options: map[string]string{
				"key_id": "newkey",
				"secret": "newsecret",
			},
		},
		{
			name:    "Missing secret name",
			sql:     "ALTER SECRET (REGION 'us-west-2')",
			wantErr: true,
		},
		{
			name:    "Missing options",
			sql:     "ALTER SECRET my_secret",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			alterSecret, ok := stmt.(*AlterSecretStmt)
			require.True(t, ok, "expected AlterSecretStmt, got %T", stmt)

			assert.Equal(t, tt.wantName, alterSecret.Name)

			// Check options
			for k, v := range tt.options {
				assert.Equal(t, v, alterSecret.Options[k], "option %s mismatch", k)
			}
		})
	}
}

func TestSecretStatementType(t *testing.T) {
	// Test that secret statements return correct statement types
	tests := []struct {
		sql      string
		stmtType string
	}{
		{
			sql:      "CREATE SECRET s (TYPE S3)",
			stmtType: "CREATE",
		},
		{
			sql:      "DROP SECRET s",
			stmtType: "DROP",
		},
		{
			sql:      "ALTER SECRET s (REGION 'us-east-1')",
			stmtType: "ALTER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, stmt)

			switch s := stmt.(type) {
			case *CreateSecretStmt:
				assert.Equal(t, "CREATE", tt.stmtType)
				_ = s
			case *DropSecretStmt:
				assert.Equal(t, "DROP", tt.stmtType)
			case *AlterSecretStmt:
				assert.Equal(t, "ALTER", tt.stmtType)
			default:
				t.Fatalf("unexpected statement type: %T", stmt)
			}
		})
	}
}
