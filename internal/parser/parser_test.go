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
		{
			name:    "extract year",
			sql:     "SELECT EXTRACT(YEAR FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract month",
			sql:     "SELECT EXTRACT(MONTH FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract day",
			sql:     "SELECT EXTRACT(DAY FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract hour",
			sql:     "SELECT EXTRACT(HOUR FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract minute",
			sql:     "SELECT EXTRACT(MINUTE FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract second",
			sql:     "SELECT EXTRACT(SECOND FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract epoch",
			sql:     "SELECT EXTRACT(EPOCH FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract from expression",
			sql:     "SELECT EXTRACT(YEAR FROM NOW())",
			wantErr: false,
		},
		{
			name:    "extract quarter",
			sql:     "SELECT EXTRACT(QUARTER FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract week",
			sql:     "SELECT EXTRACT(WEEK FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract dayofweek",
			sql:     "SELECT EXTRACT(DAYOFWEEK FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract dayofyear",
			sql:     "SELECT EXTRACT(DAYOFYEAR FROM created_at) FROM events",
			wantErr: false,
		},
		{
			name:    "extract invalid part",
			sql:     "SELECT EXTRACT(INVALID FROM created_at) FROM events",
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

// Phase 1: WHERE Clause Integration - Parser Tests

// Task 1.1: Verify parser correctly captures WHERE clauses in UpdateStmt AST nodes
func TestParseUpdateWithWhere(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		hasWhere bool
	}{
		{
			name:     "UPDATE without WHERE",
			sql:      "UPDATE users SET name = 'John'",
			hasWhere: false,
		},
		{
			name:     "UPDATE with simple WHERE",
			sql:      "UPDATE users SET name = 'John' WHERE id = 1",
			hasWhere: true,
		},
		{
			name:     "UPDATE with complex WHERE",
			sql:      "UPDATE users SET name = 'John' WHERE age > 18 AND active = true",
			hasWhere: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			updateStmt, ok := stmt.(*UpdateStmt)
			if !ok {
				t.Fatalf("Expected UpdateStmt, got %T", stmt)
			}

			if tt.hasWhere && updateStmt.Where == nil {
				t.Error("Expected WHERE clause but got nil")
			}
			if !tt.hasWhere && updateStmt.Where != nil {
				t.Error("Expected no WHERE clause but got one")
			}
		})
	}
}

// Task 1.2: Verify parser correctly captures WHERE clauses in DeleteStmt AST nodes
func TestParseDeleteWithWhere(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		hasWhere bool
	}{
		{
			name:     "DELETE without WHERE",
			sql:      "DELETE FROM users",
			hasWhere: false,
		},
		{
			name:     "DELETE with simple WHERE",
			sql:      "DELETE FROM users WHERE id = 1",
			hasWhere: true,
		},
		{
			name:     "DELETE with complex WHERE",
			sql:      "DELETE FROM users WHERE age > 18 AND active = false",
			hasWhere: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			deleteStmt, ok := stmt.(*DeleteStmt)
			if !ok {
				t.Fatalf("Expected DeleteStmt, got %T", stmt)
			}

			if tt.hasWhere && deleteStmt.Where == nil {
				t.Error("Expected WHERE clause but got nil")
			}
			if !tt.hasWhere && deleteStmt.Where != nil {
				t.Error("Expected no WHERE clause but got one")
			}
		})
	}
}

// Task 1.3: Test: Parse "DELETE FROM t WHERE x > 5" and verify WHERE expression in AST
func TestParseDeleteWhereExpression(t *testing.T) {
	sql := "DELETE FROM t WHERE x > 5"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	deleteStmt, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Expected DeleteStmt, got %T", stmt)
	}

	if deleteStmt.Where == nil {
		t.Fatal("Expected WHERE clause but got nil")
	}

	// Verify it's a binary expression (x > 5)
	binExpr, ok := deleteStmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected BinaryExpr, got %T", deleteStmt.Where)
	}

	if binExpr.Op != OpGt {
		t.Errorf("Expected Op = OpGt, got %v", binExpr.Op)
	}

	// Verify left side is column reference (x)
	leftCol, ok := binExpr.Left.(*ColumnRef)
	if !ok {
		t.Fatalf("Expected ColumnRef on left, got %T", binExpr.Left)
	}
	if leftCol.Column != "x" {
		t.Errorf("Expected column 'x', got '%s'", leftCol.Column)
	}

	// Verify right side is literal (5)
	rightLit, ok := binExpr.Right.(*Literal)
	if !ok {
		t.Fatalf("Expected Literal on right, got %T", binExpr.Right)
	}
	if rightLit.Value != int64(5) {
		t.Errorf("Expected value 5, got %v", rightLit.Value)
	}
}

// Task 1.4: Test: Parse "UPDATE t SET a = 1 WHERE b IN (1,2,3)" and verify complex WHERE
func TestParseUpdateWhereInList(t *testing.T) {
	sql := "UPDATE t SET a = 1 WHERE b IN (1,2,3)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Expected UpdateStmt, got %T", stmt)
	}

	if updateStmt.Where == nil {
		t.Fatal("Expected WHERE clause but got nil")
	}

	// Verify it's an IN list expression
	inExpr, ok := updateStmt.Where.(*InListExpr)
	if !ok {
		t.Fatalf("Expected InListExpr, got %T", updateStmt.Where)
	}

	// Verify column is 'b'
	colRef, ok := inExpr.Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("Expected ColumnRef, got %T", inExpr.Expr)
	}
	if colRef.Column != "b" {
		t.Errorf("Expected column 'b', got '%s'", colRef.Column)
	}

	// Verify list has 3 values
	if len(inExpr.Values) != 3 {
		t.Errorf("Expected 3 values in IN list, got %d", len(inExpr.Values))
	}

	// Verify values are 1, 2, 3
	expectedValues := []int64{1, 2, 3}
	for i, expected := range expectedValues {
		lit, ok := inExpr.Values[i].(*Literal)
		if !ok {
			t.Errorf("Value %d: expected Literal, got %T", i, inExpr.Values[i])
			continue
		}
		if lit.Value != expected {
			t.Errorf("Value %d: expected %d, got %v", i, expected, lit.Value)
		}
	}

	// Verify SET clause
	if len(updateStmt.Set) != 1 {
		t.Fatalf("Expected 1 SET clause, got %d", len(updateStmt.Set))
	}
	if updateStmt.Set[0].Column != "a" {
		t.Errorf("Expected column 'a', got '%s'", updateStmt.Set[0].Column)
	}
}

// Test set operations (UNION, INTERSECT, EXCEPT)
func TestParseSetOperations(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantOp   SetOpType
		wantErr  bool
	}{
		{
			name:    "UNION",
			sql:     "SELECT * FROM users UNION SELECT * FROM admins",
			wantOp:  SetOpUnion,
			wantErr: false,
		},
		{
			name:    "UNION ALL",
			sql:     "SELECT * FROM users UNION ALL SELECT * FROM admins",
			wantOp:  SetOpUnionAll,
			wantErr: false,
		},
		{
			name:    "INTERSECT",
			sql:     "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
			wantOp:  SetOpIntersect,
			wantErr: false,
		},
		{
			name:    "INTERSECT ALL",
			sql:     "SELECT * FROM t1 INTERSECT ALL SELECT * FROM t2",
			wantOp:  SetOpIntersectAll,
			wantErr: false,
		},
		{
			name:    "EXCEPT",
			sql:     "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
			wantOp:  SetOpExcept,
			wantErr: false,
		},
		{
			name:    "EXCEPT ALL",
			sql:     "SELECT * FROM t1 EXCEPT ALL SELECT * FROM t2",
			wantOp:  SetOpExceptAll,
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
			if tt.wantErr {
				return
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if selectStmt.SetOp != tt.wantOp {
				t.Errorf("SetOp = %v, want %v", selectStmt.SetOp, tt.wantOp)
			}

			if selectStmt.Right == nil {
				t.Error("Right side of set operation is nil")
			}
		})
	}
}

// Test table extraction with set operations
func TestTableExtractorSetOperations(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "UNION extracts both tables",
			sql:      "SELECT * FROM users UNION SELECT * FROM admins",
			expected: []string{"admins", "users"},
		},
		{
			name:     "INTERSECT extracts both tables",
			sql:      "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
			expected: []string{"t1", "t2"},
		},
		{
			name:     "EXCEPT extracts both tables",
			sql:      "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
			expected: []string{"t1", "t2"},
		},
		{
			name:     "chained UNION extracts all tables",
			sql:      "SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION SELECT * FROM t3",
			expected: []string{"t1", "t2", "t3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			extractor := NewTableExtractor(false)
			selectStmt.Accept(extractor)
			tables := extractor.GetTables()

			if len(tables) != len(tt.expected) {
				t.Errorf("Got %d tables %v, expected %d tables %v", len(tables), tables, len(tt.expected), tt.expected)
				return
			}

			for i, expected := range tt.expected {
				if tables[i] != expected {
					t.Errorf("Table %d: got %s, expected %s", i, tables[i], expected)
				}
			}
		})
	}
}

// ---------- Window Function Parser Tests ----------

// Test ROW_NUMBER() OVER () parsing
func TestParseWindowRowNumber(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER () FROM users"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
	}

	windowExpr, ok := selectStmt.Columns[0].Expr.(*WindowExpr)
	if !ok {
		t.Fatalf("Expected WindowExpr, got %T", selectStmt.Columns[0].Expr)
	}

	if windowExpr.Function.Name != "ROW_NUMBER" {
		t.Errorf("Expected function name ROW_NUMBER, got %s", windowExpr.Function.Name)
	}

	if len(windowExpr.PartitionBy) != 0 {
		t.Errorf("Expected no PARTITION BY, got %d expressions", len(windowExpr.PartitionBy))
	}

	if len(windowExpr.OrderBy) != 0 {
		t.Errorf("Expected no ORDER BY, got %d expressions", len(windowExpr.OrderBy))
	}
}

// Test RANK() OVER (PARTITION BY x ORDER BY y) parsing
func TestParseWindowRankWithPartitionAndOrder(t *testing.T) {
	sql := "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Function.Name != "RANK" {
		t.Errorf("Expected function name RANK, got %s", windowExpr.Function.Name)
	}

	if len(windowExpr.PartitionBy) != 1 {
		t.Fatalf("Expected 1 PARTITION BY expression, got %d", len(windowExpr.PartitionBy))
	}

	partCol, ok := windowExpr.PartitionBy[0].(*ColumnRef)
	if !ok || partCol.Column != "dept" {
		t.Errorf("Expected PARTITION BY dept, got %v", windowExpr.PartitionBy[0])
	}

	if len(windowExpr.OrderBy) != 1 {
		t.Fatalf("Expected 1 ORDER BY expression, got %d", len(windowExpr.OrderBy))
	}

	orderCol, ok := windowExpr.OrderBy[0].Expr.(*ColumnRef)
	if !ok || orderCol.Column != "salary" {
		t.Errorf("Expected ORDER BY salary, got %v", windowExpr.OrderBy[0].Expr)
	}
}

// Test SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
func TestParseWindowSumWithRowsFrame(t *testing.T) {
	sql := "SELECT SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM orders"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Function.Name != "SUM" {
		t.Errorf("Expected function name SUM, got %s", windowExpr.Function.Name)
	}

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Type != FrameTypeRows {
		t.Errorf("Expected ROWS frame, got %v", windowExpr.Frame.Type)
	}

	if windowExpr.Frame.Start.Type != BoundPreceding {
		t.Errorf("Expected start bound PRECEDING, got %v", windowExpr.Frame.Start.Type)
	}

	startOffset, ok := windowExpr.Frame.Start.Offset.(*Literal)
	if !ok || startOffset.Value != int64(1) {
		t.Errorf("Expected start offset 1, got %v", windowExpr.Frame.Start.Offset)
	}

	if windowExpr.Frame.End.Type != BoundFollowing {
		t.Errorf("Expected end bound FOLLOWING, got %v", windowExpr.Frame.End.Type)
	}

	endOffset, ok := windowExpr.Frame.End.Offset.(*Literal)
	if !ok || endOffset.Value != int64(1) {
		t.Errorf("Expected end offset 1, got %v", windowExpr.Frame.End.Offset)
	}
}

// Test GROUPS BETWEEN parsing
func TestParseWindowGroupsFrame(t *testing.T) {
	sql := "SELECT AVG(val) OVER (ORDER BY id GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Type != FrameTypeGroups {
		t.Errorf("Expected GROUPS frame, got %v", windowExpr.Frame.Type)
	}
}

// Test EXCLUDE clause parsing
func TestParseWindowExcludeClause(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		excludeMode ExcludeMode
	}{
		{
			name:        "EXCLUDE NO OTHERS",
			sql:         "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) FROM t",
			excludeMode: ExcludeNoOthers,
		},
		{
			name:        "EXCLUDE CURRENT ROW",
			sql:         "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
			excludeMode: ExcludeCurrentRow,
		},
		{
			name:        "EXCLUDE GROUP",
			sql:         "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM t",
			excludeMode: ExcludeGroup,
		},
		{
			name:        "EXCLUDE TIES",
			sql:         "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
			excludeMode: ExcludeTies,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

			if windowExpr.Frame == nil {
				t.Fatal("Expected frame specification")
			}

			if windowExpr.Frame.Exclude != tt.excludeMode {
				t.Errorf("Expected exclude mode %v, got %v", tt.excludeMode, windowExpr.Frame.Exclude)
			}
		})
	}
}

// Test NULLS FIRST/LAST parsing
func TestParseWindowNullsFirstLast(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		desc       bool
		nullsFirst bool
	}{
		{
			name:       "ORDER BY x NULLS FIRST",
			sql:        "SELECT ROW_NUMBER() OVER (ORDER BY x NULLS FIRST) FROM t",
			desc:       false,
			nullsFirst: true,
		},
		{
			name:       "ORDER BY x NULLS LAST",
			sql:        "SELECT ROW_NUMBER() OVER (ORDER BY x NULLS LAST) FROM t",
			desc:       false,
			nullsFirst: false,
		},
		{
			name:       "ORDER BY x DESC NULLS FIRST",
			sql:        "SELECT ROW_NUMBER() OVER (ORDER BY x DESC NULLS FIRST) FROM t",
			desc:       true,
			nullsFirst: true,
		},
		{
			name:       "ORDER BY x DESC NULLS LAST",
			sql:        "SELECT ROW_NUMBER() OVER (ORDER BY x DESC NULLS LAST) FROM t",
			desc:       true,
			nullsFirst: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

			if len(windowExpr.OrderBy) != 1 {
				t.Fatalf("Expected 1 ORDER BY expression, got %d", len(windowExpr.OrderBy))
			}

			if windowExpr.OrderBy[0].Desc != tt.desc {
				t.Errorf("Expected Desc = %v, got %v", tt.desc, windowExpr.OrderBy[0].Desc)
			}

			if windowExpr.OrderBy[0].NullsFirst != tt.nullsFirst {
				t.Errorf("Expected NullsFirst = %v, got %v", tt.nullsFirst, windowExpr.OrderBy[0].NullsFirst)
			}
		})
	}
}

// Test IGNORE NULLS parsing
func TestParseWindowIgnoreNulls(t *testing.T) {
	sql := "SELECT LAG(x) IGNORE NULLS OVER (ORDER BY id) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if !windowExpr.IgnoreNulls {
		t.Error("Expected IgnoreNulls = true")
	}
}

// Test RESPECT NULLS parsing (explicit default)
func TestParseWindowRespectNulls(t *testing.T) {
	sql := "SELECT LAG(x) RESPECT NULLS OVER (ORDER BY id) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.IgnoreNulls {
		t.Error("Expected IgnoreNulls = false (RESPECT NULLS is default)")
	}
}

// Test FILTER clause parsing
func TestParseWindowFilterClause(t *testing.T) {
	sql := "SELECT COUNT(*) FILTER (WHERE x > 5) OVER (PARTITION BY y) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Filter == nil {
		t.Fatal("Expected FILTER clause")
	}

	binExpr, ok := windowExpr.Filter.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected BinaryExpr in FILTER, got %T", windowExpr.Filter)
	}

	if binExpr.Op != OpGt {
		t.Errorf("Expected Op = OpGt, got %v", binExpr.Op)
	}
}

// Test DISTINCT aggregate window parsing
func TestParseWindowDistinctAggregate(t *testing.T) {
	sql := "SELECT COUNT(DISTINCT x) OVER () FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if !windowExpr.Distinct {
		t.Error("Expected Distinct = true")
	}

	if !windowExpr.Function.Distinct {
		t.Error("Expected Function.Distinct = true")
	}
}

// Test multiple window functions in single SELECT
func TestParseMultipleWindowFunctions(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)

	if len(selectStmt.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(selectStmt.Columns))
	}

	// Check first window function
	windowExpr1, ok := selectStmt.Columns[0].Expr.(*WindowExpr)
	if !ok {
		t.Fatalf("Expected WindowExpr for column 0, got %T", selectStmt.Columns[0].Expr)
	}
	if windowExpr1.Function.Name != "ROW_NUMBER" {
		t.Errorf("Expected ROW_NUMBER, got %s", windowExpr1.Function.Name)
	}

	// Check second window function
	windowExpr2, ok := selectStmt.Columns[1].Expr.(*WindowExpr)
	if !ok {
		t.Fatalf("Expected WindowExpr for column 1, got %T", selectStmt.Columns[1].Expr)
	}
	if windowExpr2.Function.Name != "RANK" {
		t.Errorf("Expected RANK, got %s", windowExpr2.Function.Name)
	}
}

// Test window function with alias
func TestParseWindowFunctionWithAlias(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM users"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
	}

	if selectStmt.Columns[0].Alias != "row_num" {
		t.Errorf("Expected alias 'row_num', got '%s'", selectStmt.Columns[0].Alias)
	}

	_, ok := selectStmt.Columns[0].Expr.(*WindowExpr)
	if !ok {
		t.Fatalf("Expected WindowExpr, got %T", selectStmt.Columns[0].Expr)
	}
}

// Test RANGE BETWEEN parsing
func TestParseWindowRangeFrame(t *testing.T) {
	sql := "SELECT SUM(x) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Type != FrameTypeRange {
		t.Errorf("Expected RANGE frame, got %v", windowExpr.Frame.Type)
	}

	if windowExpr.Frame.Start.Type != BoundUnboundedPreceding {
		t.Errorf("Expected start bound UNBOUNDED PRECEDING, got %v", windowExpr.Frame.Start.Type)
	}

	if windowExpr.Frame.End.Type != BoundCurrentRow {
		t.Errorf("Expected end bound CURRENT ROW, got %v", windowExpr.Frame.End.Type)
	}
}

// Test single-bound shorthand (e.g., ROWS 3 PRECEDING)
func TestParseWindowSingleBoundShorthand(t *testing.T) {
	sql := "SELECT SUM(x) OVER (ORDER BY id ROWS 3 PRECEDING) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Type != FrameTypeRows {
		t.Errorf("Expected ROWS frame, got %v", windowExpr.Frame.Type)
	}

	if windowExpr.Frame.Start.Type != BoundPreceding {
		t.Errorf("Expected start bound PRECEDING, got %v", windowExpr.Frame.Start.Type)
	}

	startOffset, ok := windowExpr.Frame.Start.Offset.(*Literal)
	if !ok || startOffset.Value != int64(3) {
		t.Errorf("Expected start offset 3, got %v", windowExpr.Frame.Start.Offset)
	}

	// Single-bound shorthand should default end to CURRENT ROW
	if windowExpr.Frame.End.Type != BoundCurrentRow {
		t.Errorf("Expected end bound CURRENT ROW, got %v", windowExpr.Frame.End.Type)
	}
}

// Test LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE with IGNORE NULLS
func TestParseWindowValueFunctionsWithIgnoreNulls(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		funcName string
	}{
		{"LAG", "SELECT LAG(x) IGNORE NULLS OVER (ORDER BY id) FROM t", "LAG"},
		{"LEAD", "SELECT LEAD(x) IGNORE NULLS OVER (ORDER BY id) FROM t", "LEAD"},
		{"FIRST_VALUE", "SELECT FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY id) FROM t", "FIRST_VALUE"},
		{"LAST_VALUE", "SELECT LAST_VALUE(x) IGNORE NULLS OVER (ORDER BY id) FROM t", "LAST_VALUE"},
		{"NTH_VALUE", "SELECT NTH_VALUE(x, 2) IGNORE NULLS OVER (ORDER BY id) FROM t", "NTH_VALUE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

			if windowExpr.Function.Name != tt.funcName {
				t.Errorf("Expected function name %s, got %s", tt.funcName, windowExpr.Function.Name)
			}

			if !windowExpr.IgnoreNulls {
				t.Error("Expected IgnoreNulls = true")
			}
		})
	}
}

// Test COUNT, SUM, AVG with FILTER
func TestParseWindowAggregatesWithFilter(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		funcName string
	}{
		{"COUNT", "SELECT COUNT(*) FILTER (WHERE x > 5) OVER () FROM t", "COUNT"},
		{"SUM", "SELECT SUM(amount) FILTER (WHERE status = 'active') OVER () FROM t", "SUM"},
		{"AVG", "SELECT AVG(score) FILTER (WHERE valid = true) OVER () FROM t", "AVG"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

			if windowExpr.Function.Name != tt.funcName {
				t.Errorf("Expected function name %s, got %s", tt.funcName, windowExpr.Function.Name)
			}

			if windowExpr.Filter == nil {
				t.Error("Expected FILTER clause")
			}
		})
	}
}

// Test COUNT(DISTINCT x) OVER, SUM(DISTINCT x) OVER
func TestParseWindowDistinctAggregates(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		funcName string
	}{
		{"COUNT DISTINCT", "SELECT COUNT(DISTINCT x) OVER () FROM t", "COUNT"},
		{"SUM DISTINCT", "SELECT SUM(DISTINCT x) OVER () FROM t", "SUM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

			if windowExpr.Function.Name != tt.funcName {
				t.Errorf("Expected function name %s, got %s", tt.funcName, windowExpr.Function.Name)
			}

			if !windowExpr.Distinct {
				t.Error("Expected Distinct = true")
			}
		})
	}
}

// Test UNBOUNDED PRECEDING/FOLLOWING
func TestParseWindowUnboundedBounds(t *testing.T) {
	sql := "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Start.Type != BoundUnboundedPreceding {
		t.Errorf("Expected start UNBOUNDED PRECEDING, got %v", windowExpr.Frame.Start.Type)
	}

	if windowExpr.Frame.End.Type != BoundUnboundedFollowing {
		t.Errorf("Expected end UNBOUNDED FOLLOWING, got %v", windowExpr.Frame.End.Type)
	}
}

// Test CURRENT ROW bound
func TestParseWindowCurrentRowBound(t *testing.T) {
	sql := "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	windowExpr := selectStmt.Columns[0].Expr.(*WindowExpr)

	if windowExpr.Frame == nil {
		t.Fatal("Expected frame specification")
	}

	if windowExpr.Frame.Start.Type != BoundCurrentRow {
		t.Errorf("Expected start CURRENT ROW, got %v", windowExpr.Frame.Start.Type)
	}

	if windowExpr.Frame.End.Type != BoundFollowing {
		t.Errorf("Expected end FOLLOWING, got %v", windowExpr.Frame.End.Type)
	}

	endOffset, ok := windowExpr.Frame.End.Offset.(*Literal)
	if !ok || endOffset.Value != int64(5) {
		t.Errorf("Expected end offset 5, got %v", windowExpr.Frame.End.Offset)
	}
}

// Test comprehensive window function parsing
func TestParseWindowFunctions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "ROW_NUMBER simple",
			sql:     "SELECT ROW_NUMBER() OVER () FROM t",
			wantErr: false,
		},
		{
			name:    "RANK with partition and order",
			sql:     "SELECT RANK() OVER (PARTITION BY dept ORDER BY sal DESC) FROM emp",
			wantErr: false,
		},
		{
			name:    "DENSE_RANK",
			sql:     "SELECT DENSE_RANK() OVER (ORDER BY score) FROM students",
			wantErr: false,
		},
		{
			name:    "NTILE",
			sql:     "SELECT NTILE(4) OVER (ORDER BY id) FROM t",
			wantErr: false,
		},
		{
			name:    "LAG with default",
			sql:     "SELECT LAG(price, 1, 0) OVER (ORDER BY date) FROM prices",
			wantErr: false,
		},
		{
			name:    "LEAD",
			sql:     "SELECT LEAD(value, 2) OVER (PARTITION BY category ORDER BY id) FROM t",
			wantErr: false,
		},
		{
			name:    "FIRST_VALUE",
			sql:     "SELECT FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY hire_date) FROM emp",
			wantErr: false,
		},
		{
			name:    "LAST_VALUE with frame",
			sql:     "SELECT LAST_VALUE(price) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "NTH_VALUE",
			sql:     "SELECT NTH_VALUE(name, 3) OVER (ORDER BY score DESC) FROM students",
			wantErr: false,
		},
		{
			name:    "PERCENT_RANK",
			sql:     "SELECT PERCENT_RANK() OVER (ORDER BY score) FROM students",
			wantErr: false,
		},
		{
			name:    "CUME_DIST",
			sql:     "SELECT CUME_DIST() OVER (ORDER BY salary DESC) FROM emp",
			wantErr: false,
		},
		{
			name:    "SUM OVER with frame",
			sql:     "SELECT SUM(amount) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM orders",
			wantErr: false,
		},
		{
			name:    "COUNT OVER",
			sql:     "SELECT COUNT(*) OVER (PARTITION BY category) FROM products",
			wantErr: false,
		},
		{
			name:    "AVG OVER with RANGE",
			sql:     "SELECT AVG(price) OVER (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "MIN OVER",
			sql:     "SELECT MIN(value) OVER (PARTITION BY group_id) FROM t",
			wantErr: false,
		},
		{
			name:    "MAX OVER",
			sql:     "SELECT MAX(score) OVER (ORDER BY id) FROM t",
			wantErr: false,
		},
		{
			name:    "Multiple partitions",
			sql:     "SELECT SUM(x) OVER (PARTITION BY a, b, c ORDER BY d) FROM t",
			wantErr: false,
		},
		{
			name:    "Multiple order by columns",
			sql:     "SELECT ROW_NUMBER() OVER (ORDER BY a DESC, b ASC, c DESC NULLS FIRST) FROM t",
			wantErr: false,
		},
		{
			name:    "Complex expression in partition by",
			sql:     "SELECT SUM(x) OVER (PARTITION BY a + b ORDER BY c) FROM t",
			wantErr: false,
		},
		{
			name:    "Case insensitive keywords",
			sql:     "SELECT row_number() over (partition by x order by y) FROM t",
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
		})
	}
}

// Test INTERVAL literal parsing
func TestParseIntervalLiteral(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantMonths  int32
		wantDays    int32
		wantMicros  int64
		wantErr     bool
	}{
		{
			name:        "INTERVAL with DAY unit keyword",
			sql:         "SELECT INTERVAL '5' DAY",
			wantMonths:  0,
			wantDays:    5,
			wantMicros:  0,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with HOUR unit keyword",
			sql:         "SELECT INTERVAL '2' HOUR",
			wantMonths:  0,
			wantDays:    0,
			wantMicros:  2 * 60 * 60 * 1_000_000,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with MONTH unit keyword",
			sql:         "SELECT INTERVAL '3' MONTH",
			wantMonths:  3,
			wantDays:    0,
			wantMicros:  0,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with YEAR unit keyword",
			sql:         "SELECT INTERVAL '1' YEAR",
			wantMonths:  12,
			wantDays:    0,
			wantMicros:  0,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with inline unit",
			sql:         "SELECT INTERVAL '5 days'",
			wantMonths:  0,
			wantDays:    5,
			wantMicros:  0,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with compound units",
			sql:         "SELECT INTERVAL '2 hours 30 minutes'",
			wantMonths:  0,
			wantDays:    0,
			wantMicros:  2*60*60*1_000_000 + 30*60*1_000_000,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with WEEK unit",
			sql:         "SELECT INTERVAL '2' WEEKS",
			wantMonths:  0,
			wantDays:    14,
			wantMicros:  0,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with MINUTE unit",
			sql:         "SELECT INTERVAL '45' MINUTE",
			wantMonths:  0,
			wantDays:    0,
			wantMicros:  45 * 60 * 1_000_000,
			wantErr:     false,
		},
		{
			name:        "INTERVAL with SECOND unit",
			sql:         "SELECT INTERVAL '30' SECOND",
			wantMonths:  0,
			wantDays:    0,
			wantMicros:  30 * 1_000_000,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if len(selectStmt.Columns) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
			}

			interval, ok := selectStmt.Columns[0].Expr.(*IntervalLiteral)
			if !ok {
				t.Fatalf("Expected IntervalLiteral, got %T", selectStmt.Columns[0].Expr)
			}

			if interval.Months != tt.wantMonths {
				t.Errorf("Months = %d, want %d", interval.Months, tt.wantMonths)
			}
			if interval.Days != tt.wantDays {
				t.Errorf("Days = %d, want %d", interval.Days, tt.wantDays)
			}
			if interval.Micros != tt.wantMicros {
				t.Errorf("Micros = %d, want %d", interval.Micros, tt.wantMicros)
			}
		})
	}
}

// Test qualified column names (table.column syntax)
func TestParseQualifiedColumns(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple qualified column",
			sql:     "SELECT t.id FROM t",
			wantErr: false,
		},
		{
			name:    "multiple qualified columns",
			sql:     "SELECT users.id, users.name FROM users",
			wantErr: false,
		},
		{
			name:    "mixed qualified and unqualified",
			sql:     "SELECT users.id, name FROM users",
			wantErr: false,
		},
		{
			name:    "qualified columns in JOIN",
			sql:     "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
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
		})
	}
}

// ---------- Excel Table Function Parser Tests ----------

// TestParseExcelTableFunctions tests basic parsing success for read_excel and read_excel_auto
// with various options
func TestParseExcelTableFunctions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "basic read_excel",
			sql:     "SELECT * FROM read_excel('file.xlsx')",
			wantErr: false,
		},
		{
			name:    "read_excel_auto basic",
			sql:     "SELECT * FROM read_excel_auto('file.xlsx')",
			wantErr: false,
		},
		{
			name:    "read_excel with sheet name",
			sql:     "SELECT * FROM read_excel('file.xlsx', sheet='Sales')",
			wantErr: false,
		},
		{
			name:    "read_excel with sheet number",
			sql:     "SELECT * FROM read_excel('file.xlsx', sheet=1)",
			wantErr: false,
		},
		{
			name:    "read_excel with range",
			sql:     "SELECT * FROM read_excel('file.xlsx', range='A1:Z100')",
			wantErr: false,
		},
		{
			name:    "read_excel with boolean header option",
			sql:     "SELECT * FROM read_excel('file.xlsx', header=true)",
			wantErr: false,
		},
		{
			name:    "read_excel with header_row option",
			sql:     "SELECT * FROM read_excel('file.xlsx', header_row=0)",
			wantErr: false,
		},
		{
			name:    "read_excel with skip_rows option",
			sql:     "SELECT * FROM read_excel('file.xlsx', skip_rows=5)",
			wantErr: false,
		},
		{
			name:    "read_excel with multiple options combined",
			sql:     "SELECT * FROM read_excel('file.xlsx', sheet='Sheet1', range='A1:C10', header=true, skip_rows=2)",
			wantErr: false,
		},
		{
			name:    "read_excel_auto with sheet and range",
			sql:     "SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10')",
			wantErr: false,
		},
		{
			name:    "read_excel with unknown option - should parse without error",
			sql:     "SELECT * FROM read_excel('file.xlsx', unknown_opt='val')",
			wantErr: false,
		},
		{
			name:    "read_excel_auto with header true",
			sql:     "SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10', header=true)",
			wantErr: false,
		},
		{
			name:    "read_excel with columns selection",
			sql:     "SELECT col1, col2 FROM read_excel('file.xlsx')",
			wantErr: false,
		},
		{
			name:    "read_excel with WHERE clause",
			sql:     "SELECT * FROM read_excel('file.xlsx') WHERE amount > 100",
			wantErr: false,
		},
		{
			name:    "read_excel with ORDER BY",
			sql:     "SELECT * FROM read_excel('file.xlsx') ORDER BY date DESC",
			wantErr: false,
		},
		{
			name:    "read_excel with LIMIT",
			sql:     "SELECT * FROM read_excel('file.xlsx') LIMIT 10",
			wantErr: false,
		},
		{
			name:    "read_excel with alias",
			sql:     "SELECT e.* FROM read_excel('file.xlsx') AS e",
			wantErr: false,
		},
		{
			name:    "read_excel in subquery",
			sql:     "SELECT * FROM (SELECT * FROM read_excel('file.xlsx')) sub",
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
		})
	}
}

// TestParseExcelTableFunctionAST tests that the AST nodes are correctly populated
// with function name, positional args, and named args
func TestParseExcelTableFunctionAST(t *testing.T) {
	t.Run("basic read_excel AST", func(t *testing.T) {
		sql := "SELECT * FROM read_excel('data.xlsx')"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("Expected SelectStmt, got %T", stmt)
		}

		if selectStmt.From == nil || len(selectStmt.From.Tables) != 1 {
			t.Fatal("Expected 1 table in FROM clause")
		}

		tableRef := selectStmt.From.Tables[0]
		if tableRef.TableFunction == nil {
			t.Fatal("Expected TableFunction in TableRef")
		}

		tableFunc := tableRef.TableFunction
		if tableFunc.Name != "read_excel" {
			t.Errorf("Expected function name 'read_excel', got '%s'", tableFunc.Name)
		}

		if len(tableFunc.Args) != 1 {
			t.Fatalf("Expected 1 positional arg, got %d", len(tableFunc.Args))
		}

		arg, ok := tableFunc.Args[0].(*Literal)
		if !ok {
			t.Fatalf("Expected Literal arg, got %T", tableFunc.Args[0])
		}
		if arg.Value != "data.xlsx" {
			t.Errorf("Expected arg value 'data.xlsx', got '%v'", arg.Value)
		}
	})

	t.Run("read_excel_auto with options AST", func(t *testing.T) {
		sql := "SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10')"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt := stmt.(*SelectStmt)
		tableFunc := selectStmt.From.Tables[0].TableFunction

		if tableFunc.Name != "read_excel_auto" {
			t.Errorf("Expected function name 'read_excel_auto', got '%s'", tableFunc.Name)
		}

		// Check file path arg
		if len(tableFunc.Args) != 1 {
			t.Fatalf("Expected 1 positional arg, got %d", len(tableFunc.Args))
		}

		// Check named args
		if tableFunc.NamedArgs == nil {
			t.Fatal("Expected NamedArgs to be initialized")
		}

		sheetArg, exists := tableFunc.NamedArgs["sheet"]
		if !exists {
			t.Fatal("Expected 'sheet' in NamedArgs")
		}
		sheetLit, ok := sheetArg.(*Literal)
		if !ok {
			t.Fatalf("Expected Literal for sheet, got %T", sheetArg)
		}
		if sheetLit.Value != "Sheet1" {
			t.Errorf("Expected sheet='Sheet1', got '%v'", sheetLit.Value)
		}

		rangeArg, exists := tableFunc.NamedArgs["range"]
		if !exists {
			t.Fatal("Expected 'range' in NamedArgs")
		}
		rangeLit, ok := rangeArg.(*Literal)
		if !ok {
			t.Fatalf("Expected Literal for range, got %T", rangeArg)
		}
		if rangeLit.Value != "A1:C10" {
			t.Errorf("Expected range='A1:C10', got '%v'", rangeLit.Value)
		}
	})

	t.Run("read_excel with sheet/range/header AST", func(t *testing.T) {
		sql := "SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10', header=true)"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt := stmt.(*SelectStmt)
		tableFunc := selectStmt.From.Tables[0].TableFunction

		// Check sheet
		sheetArg := tableFunc.NamedArgs["sheet"]
		sheetLit := sheetArg.(*Literal)
		if sheetLit.Value != "Sheet1" {
			t.Errorf("Expected sheet='Sheet1', got '%v'", sheetLit.Value)
		}

		// Check range
		rangeArg := tableFunc.NamedArgs["range"]
		rangeLit := rangeArg.(*Literal)
		if rangeLit.Value != "A1:C10" {
			t.Errorf("Expected range='A1:C10', got '%v'", rangeLit.Value)
		}

		// Check header
		headerArg := tableFunc.NamedArgs["header"]
		headerLit := headerArg.(*Literal)
		if headerLit.Value != true {
			t.Errorf("Expected header=true, got '%v'", headerLit.Value)
		}
	})

	t.Run("unknown Excel option stored in NamedArgs", func(t *testing.T) {
		sql := "SELECT * FROM read_excel('file.xlsx', unknown_opt='val')"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt := stmt.(*SelectStmt)
		tableFunc := selectStmt.From.Tables[0].TableFunction

		unknownArg, exists := tableFunc.NamedArgs["unknown_opt"]
		if !exists {
			t.Fatal("Expected 'unknown_opt' in NamedArgs - unknown options should be stored without parse error")
		}
		unknownLit, ok := unknownArg.(*Literal)
		if !ok {
			t.Fatalf("Expected Literal for unknown_opt, got %T", unknownArg)
		}
		if unknownLit.Value != "val" {
			t.Errorf("Expected unknown_opt='val', got '%v'", unknownLit.Value)
		}
	})

	t.Run("read_excel with sheet number", func(t *testing.T) {
		sql := "SELECT * FROM read_excel('file.xlsx', sheet=1)"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt := stmt.(*SelectStmt)
		tableFunc := selectStmt.From.Tables[0].TableFunction

		sheetArg := tableFunc.NamedArgs["sheet"]
		sheetLit := sheetArg.(*Literal)
		// Sheet number should be parsed as integer
		if sheetLit.Value != int64(1) {
			t.Errorf("Expected sheet=1 (int64), got '%v' (%T)", sheetLit.Value, sheetLit.Value)
		}
	})

	t.Run("read_excel with header_row and skip_rows", func(t *testing.T) {
		sql := "SELECT * FROM read_excel('file.xlsx', header_row=0, skip_rows=5)"
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		selectStmt := stmt.(*SelectStmt)
		tableFunc := selectStmt.From.Tables[0].TableFunction

		headerRowArg := tableFunc.NamedArgs["header_row"]
		headerRowLit := headerRowArg.(*Literal)
		if headerRowLit.Value != int64(0) {
			t.Errorf("Expected header_row=0, got '%v'", headerRowLit.Value)
		}

		skipRowsArg := tableFunc.NamedArgs["skip_rows"]
		skipRowsLit := skipRowsArg.(*Literal)
		if skipRowsLit.Value != int64(5) {
			t.Errorf("Expected skip_rows=5, got '%v'", skipRowsLit.Value)
		}
	})
}

// TestTableExtractorExcelFunctions tests that TableExtractor correctly extracts Excel table function
// names as table references (they appear in table list as the function name is stored in TableName)
func TestTableExtractorExcelFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "read_excel extracted as table ref",
			sql:      "SELECT * FROM read_excel('file.xlsx')",
			expected: []string{"read_excel"},
		},
		{
			name:     "read_excel_auto extracted as table ref",
			sql:      "SELECT * FROM read_excel_auto('data.xlsx')",
			expected: []string{"read_excel_auto"},
		},
		{
			name:     "read_excel with options extracted as table ref",
			sql:      "SELECT * FROM read_excel('file.xlsx', sheet='Sheet1', header=true)",
			expected: []string{"read_excel"},
		},
		{
			name:     "mixed: real table with Excel function subquery",
			sql:      "SELECT * FROM users WHERE id IN (SELECT id FROM read_excel('ids.xlsx'))",
			expected: []string{"read_excel", "users"},
		},
		{
			name:     "join with Excel function - both tables extracted",
			sql:      "SELECT u.name FROM users u JOIN read_excel('data.xlsx') e ON u.id = e.id",
			expected: []string{"read_excel", "users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			extractor := NewTableExtractor(false)
			selectStmt.Accept(extractor)
			tables := extractor.GetTables()

			if len(tables) != len(tt.expected) {
				t.Errorf("Got %d tables %v, expected %d tables %v",
					len(tables), tables, len(tt.expected), tt.expected)
				return
			}

			for i, expected := range tt.expected {
				if tables[i] != expected {
					t.Errorf("Table %d: got %s, expected %s", i, tables[i], expected)
				}
			}
		})
	}
}

// TestCountParametersExcelTableFunction tests parameter counting in Excel table function queries
// Note: Currently the parameter counter does not traverse FROM clause table functions,
// so parameters inside read_excel() args are not counted. Only WHERE clause params are counted.
func TestCountParametersExcelTableFunction(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		count int
	}{
		{
			name:  "no parameters in read_excel",
			sql:   "SELECT * FROM read_excel('file.xlsx')",
			count: 0,
		},
		{
			name:  "parameter in WHERE clause with read_excel",
			sql:   "SELECT * FROM read_excel('file.xlsx') WHERE amount > $1",
			count: 1,
		},
		{
			name:  "multiple parameters in WHERE with read_excel",
			sql:   "SELECT * FROM read_excel('file.xlsx') WHERE col1 = $1 AND col2 = $2",
			count: 2,
		},
		{
			name:  "dollar parameters in WHERE clause",
			sql:   "SELECT * FROM read_excel('file.xlsx') WHERE id = $1 AND status = $2 AND amount > $3",
			count: 3,
		},
		{
			name:  "question mark parameters in WHERE clause",
			sql:   "SELECT * FROM read_excel('file.xlsx') WHERE value > ?",
			count: 1,
		},
		{
			name:  "mixed query with read_excel and WHERE params",
			sql:   "SELECT col1, col2 FROM read_excel('file.xlsx', sheet='Sheet1') WHERE date > $1 ORDER BY col1 LIMIT $2",
			count: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			count := CountParameters(stmt)
			if count != tt.count {
				t.Errorf("CountParameters() = %v, want %v", count, tt.count)
			}
		})
	}
}
