package parser

import (
	"testing"
)

// TestParseCreateViewWithQualifiedColumns specifically tests that
// CREATE VIEW statements can handle qualified column names (table.column).
func TestParseCreateViewWithQualifiedColumns(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "CREATE VIEW with simple qualified columns",
			sql:     "CREATE VIEW v AS SELECT t.id, t.name FROM t",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW with JOIN and qualified columns",
			sql:     "CREATE VIEW user_posts AS SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW with mixed qualified and unqualified columns",
			sql:     "CREATE VIEW v AS SELECT t.id, name, t.status FROM t",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW with complex query and qualified columns",
			sql:     "CREATE VIEW v AS SELECT u.id, p.title, COUNT(*) as post_count FROM users u JOIN posts p ON u.id = p.user_id GROUP BY u.id, p.title",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && stmt == nil {
				t.Error("Parse() returned nil statement")
			}

			// Verify it's a CreateViewStmt
			if !tt.wantErr {
				_, ok := stmt.(*CreateViewStmt)
				if !ok {
					t.Errorf("Parse() returned %T, expected *CreateViewStmt", stmt)
				}
			}
		})
	}
}
