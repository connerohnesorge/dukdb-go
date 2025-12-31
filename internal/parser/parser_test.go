package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple select",
			sql:     "SELECT 1",
			wantErr: false,
		},
		{
			name:    "select from table",
			sql:     "SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "select columns",
			sql:     "SELECT id, name, age FROM users",
			wantErr: false,
		},
		{
			name:    "select with alias",
			sql:     "SELECT id AS user_id, name FROM users u",
			wantErr: false,
		},
		{
			name:    "select with where",
			sql:     "SELECT * FROM users WHERE age > 18",
			wantErr: false,
		},
		{
			name:    "select with multiple conditions",
			sql:     "SELECT * FROM users WHERE age > 18 AND active = true",
			wantErr: false,
		},
		{
			name:    "select with order by",
			sql:     "SELECT * FROM users ORDER BY name ASC",
			wantErr: false,
		},
		{
			name:    "select with limit",
			sql:     "SELECT * FROM users LIMIT 10",
			wantErr: false,
		},
		{
			name:    "select with limit and offset",
			sql:     "SELECT * FROM users LIMIT 10 OFFSET 20",
			wantErr: false,
		},
		{
			name:    "select with group by",
			sql:     "SELECT department, COUNT(*) FROM employees GROUP BY department",
			wantErr: false,
		},
		{
			name:    "select with having",
			sql:     "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
			wantErr: false,
		},
		{
			name:    "select distinct",
			sql:     "SELECT DISTINCT name FROM users",
			wantErr: false,
		},
		{
			name:    "select with join",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "select with left join",
			sql:     "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "select with multiple joins",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id",
			wantErr: false,
		},
		{
			name:    "select with subquery",
			sql:     "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantErr: false,
		},
		{
			name:    "select with between",
			sql:     "SELECT * FROM products WHERE price BETWEEN 10 AND 100",
			wantErr: false,
		},
		{
			name:    "select with like",
			sql:     "SELECT * FROM users WHERE name LIKE 'John%'",
			wantErr: false,
		},
		{
			name:    "select with is null",
			sql:     "SELECT * FROM users WHERE email IS NULL",
			wantErr: false,
		},
		{
			name:    "select with is not null",
			sql:     "SELECT * FROM users WHERE email IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "select with case",
			sql:     "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
			wantErr: false,
		},
		{
			name:    "select with aggregate functions",
			sql:     "SELECT COUNT(*), SUM(amount), AVG(price), MIN(age), MAX(age) FROM users",
			wantErr: false,
		},
		{
			name:    "empty query",
			sql:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr && stmt == nil {
				t.Error(
					"Parse() returned nil statement",
				)
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple insert",
			sql:     "INSERT INTO users (name) VALUES ('John')",
			wantErr: false,
		},
		{
			name:    "insert multiple columns",
			sql:     "INSERT INTO users (name, age) VALUES ('John', 25)",
			wantErr: false,
		},
		{
			name:    "insert multiple rows",
			sql:     "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)",
			wantErr: false,
		},
		{
			name:    "insert without column list",
			sql:     "INSERT INTO users VALUES (1, 'John', 25)",
			wantErr: false,
		},
		{
			name:    "insert with select",
			sql:     "INSERT INTO users_backup SELECT * FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr {
				if stmt == nil {
					t.Error(
						"Parse() returned nil statement",
					)
				} else if stmt.Type() != dukdb.STATEMENT_TYPE_INSERT {
					t.Errorf("Expected INSERT statement, got %v", stmt.Type())
				}
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple update",
			sql:     "UPDATE users SET name = 'John'",
			wantErr: false,
		},
		{
			name:    "update with where",
			sql:     "UPDATE users SET name = 'John' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "update multiple columns",
			sql:     "UPDATE users SET name = 'John', age = 25 WHERE id = 1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr {
				if stmt == nil {
					t.Error(
						"Parse() returned nil statement",
					)
				} else if stmt.Type() != dukdb.STATEMENT_TYPE_UPDATE {
					t.Errorf("Expected UPDATE statement, got %v", stmt.Type())
				}
			}
		})
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple delete",
			sql:     "DELETE FROM users",
			wantErr: false,
		},
		{
			name:    "delete with where",
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "delete with complex where",
			sql:     "DELETE FROM users WHERE age > 18 AND active = false",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr {
				if stmt == nil {
					t.Error(
						"Parse() returned nil statement",
					)
				} else if stmt.Type() != dukdb.STATEMENT_TYPE_DELETE {
					t.Errorf("Expected DELETE statement, got %v", stmt.Type())
				}
			}
		})
	}
}

func TestParseCreateTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple create table",
			sql:     "CREATE TABLE users (id INTEGER, name VARCHAR)",
			wantErr: false,
		},
		{
			name:    "create table with not null",
			sql:     "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR NOT NULL)",
			wantErr: false,
		},
		{
			name:    "create table with primary key",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)",
			wantErr: false,
		},
		{
			name:    "create table if not exists",
			sql:     "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)",
			wantErr: false,
		},
		{
			name:    "create table with table level primary key",
			sql:     "CREATE TABLE users (id INTEGER, name VARCHAR, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "create table with default",
			sql:     "CREATE TABLE users (id INTEGER, active BOOLEAN DEFAULT true)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr {
				if stmt == nil {
					t.Error(
						"Parse() returned nil statement",
					)
				} else if stmt.Type() != dukdb.STATEMENT_TYPE_CREATE {
					t.Errorf("Expected CREATE statement, got %v", stmt.Type())
				}
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple drop table",
			sql:     "DROP TABLE users",
			wantErr: false,
		},
		{
			name:    "drop table if exists",
			sql:     "DROP TABLE IF EXISTS users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr {
				if stmt == nil {
					t.Error(
						"Parse() returned nil statement",
					)
				} else if stmt.Type() != dukdb.STATEMENT_TYPE_DROP {
					t.Errorf("Expected DROP statement, got %v", stmt.Type())
				}
			}
		})
	}
}

func TestParseExpressions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "arithmetic expression",
			sql:     "SELECT 1 + 2 * 3",
			wantErr: false,
		},
		{
			name:    "comparison expression",
			sql:     "SELECT * FROM users WHERE age > 18",
			wantErr: false,
		},
		{
			name:    "logical expression",
			sql:     "SELECT * FROM users WHERE age > 18 AND name = 'John'",
			wantErr: false,
		},
		{
			name:    "not expression",
			sql:     "SELECT * FROM users WHERE NOT active",
			wantErr: false,
		},
		{
			name:    "parenthesized expression",
			sql:     "SELECT (1 + 2) * 3",
			wantErr: false,
		},
		{
			name:    "negative number",
			sql:     "SELECT -5",
			wantErr: false,
		},
		{
			name:    "string concatenation",
			sql:     "SELECT 'Hello' || ' ' || 'World'",
			wantErr: false,
		},
		{
			name:    "function call",
			sql:     "SELECT UPPER(name) FROM users",
			wantErr: false,
		},
		{
			name:    "cast expression",
			sql:     "SELECT CAST(age AS VARCHAR) FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Parse() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}
			if !tt.wantErr && stmt == nil {
				t.Error(
					"Parse() returned nil statement",
				)
			}
		})
	}
}

func TestCountParameters(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		count int
	}{
		{
			name:  "no parameters",
			sql:   "SELECT * FROM users",
			count: 0,
		},
		{
			name:  "single question mark",
			sql:   "SELECT * FROM users WHERE id = ?",
			count: 1,
		},
		{
			name:  "multiple question marks",
			sql:   "SELECT * FROM users WHERE id = ? AND name = ?",
			count: 2,
		},
		{
			name:  "dollar sign parameters",
			sql:   "SELECT * FROM users WHERE id = $1 AND name = $2",
			count: 2,
		},
		{
			name:  "mixed parameters",
			sql:   "SELECT * FROM users WHERE id = $1 AND name = $2 AND age > $3",
			count: 3,
		},
		{
			name:  "parameters in insert",
			sql:   "INSERT INTO users (name, age) VALUES ($1, $2)",
			count: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf(
					"Parse() error = %v",
					err,
				)
			}
			count := CountParameters(stmt)
			if count != tt.count {
				t.Errorf(
					"CountParameters() = %v, want %v",
					count,
					tt.count,
				)
			}
		})
	}
}

func TestParseTypeName(t *testing.T) {
	tests := []struct {
		typeName string
		expected dukdb.Type
	}{
		{"INTEGER", dukdb.TYPE_INTEGER},
		{"INT", dukdb.TYPE_INTEGER},
		{"BIGINT", dukdb.TYPE_BIGINT},
		{"VARCHAR", dukdb.TYPE_VARCHAR},
		{"TEXT", dukdb.TYPE_VARCHAR},
		{"BOOLEAN", dukdb.TYPE_BOOLEAN},
		{"BOOL", dukdb.TYPE_BOOLEAN},
		{"DOUBLE", dukdb.TYPE_DOUBLE},
		{"FLOAT", dukdb.TYPE_FLOAT},
		{"DATE", dukdb.TYPE_DATE},
		{"TIME", dukdb.TYPE_TIME},
		{"TIMESTAMP", dukdb.TYPE_TIMESTAMP},
		{"UUID", dukdb.TYPE_UUID},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := parseTypeName(tt.typeName)
			if result != tt.expected {
				t.Errorf(
					"parseTypeName(%s) = %v, want %v",
					tt.typeName,
					result,
					tt.expected,
				)
			}
		})
	}
}
