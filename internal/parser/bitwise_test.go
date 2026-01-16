package parser

import (
	"testing"
)

// TestParseBitwiseOperators verifies that bitwise operators are correctly tokenized and parsed.
func TestParseBitwiseOperators(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// Bitwise AND
		{
			name:    "bitwise AND",
			sql:     "SELECT 5 & 3",
			wantErr: false,
		},
		{
			name:    "bitwise AND with column",
			sql:     "SELECT flags & 1 FROM users",
			wantErr: false,
		},
		// Bitwise OR
		{
			name:    "bitwise OR",
			sql:     "SELECT 5 | 3",
			wantErr: false,
		},
		{
			name:    "bitwise OR with column",
			sql:     "SELECT flags | 1 FROM users",
			wantErr: false,
		},
		// Bitwise XOR
		{
			name:    "bitwise XOR",
			sql:     "SELECT 5 ^ 3",
			wantErr: false,
		},
		{
			name:    "bitwise XOR with column",
			sql:     "SELECT flags ^ 1 FROM users",
			wantErr: false,
		},
		// Bitwise NOT
		{
			name:    "bitwise NOT",
			sql:     "SELECT ~5",
			wantErr: false,
		},
		{
			name:    "bitwise NOT with column",
			sql:     "SELECT ~flags FROM users",
			wantErr: false,
		},
		// Left shift
		{
			name:    "left shift",
			sql:     "SELECT 1 << 4",
			wantErr: false,
		},
		{
			name:    "left shift with column",
			sql:     "SELECT value << 2 FROM data",
			wantErr: false,
		},
		// Right shift
		{
			name:    "right shift",
			sql:     "SELECT 16 >> 2",
			wantErr: false,
		},
		{
			name:    "right shift with column",
			sql:     "SELECT value >> 2 FROM data",
			wantErr: false,
		},
		// Complex expressions
		{
			name:    "combined bitwise ops",
			sql:     "SELECT (5 & 3) | 1",
			wantErr: false,
		},
		{
			name:    "bitwise in WHERE clause",
			sql:     "SELECT * FROM users WHERE flags & 1 = 1",
			wantErr: false,
		},
		{
			name:    "bitwise with arithmetic",
			sql:     "SELECT (a + b) & 255 FROM data",
			wantErr: false,
		},
		{
			name:    "nested bitwise NOT",
			sql:     "SELECT ~~value FROM data",
			wantErr: false,
		},
		{
			name:    "shift and mask",
			sql:     "SELECT (value >> 8) & 0xFF FROM data",
			wantErr: false,
		},
		{
			name:    "all bitwise operators",
			sql:     "SELECT ~(a & b) | (c ^ d) << 2 >> 1 FROM t",
			wantErr: false,
		},
		// String concatenation still works (||)
		{
			name:    "string concat still works",
			sql:     "SELECT 'hello' || ' world'",
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

// TestBitwiseOperatorAST verifies the AST structure for bitwise expressions.
func TestBitwiseOperatorAST(t *testing.T) {
	t.Run("bitwise AND", func(t *testing.T) {
		stmt, err := Parse("SELECT 5 & 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		if len(sel.Columns) != 1 {
			t.Fatalf("Expected 1 column, got %d", len(sel.Columns))
		}
		binExpr, ok := sel.Columns[0].Expr.(*BinaryExpr)
		if !ok {
			t.Fatalf("Expected BinaryExpr, got %T", sel.Columns[0].Expr)
		}
		if binExpr.Op != OpBitwiseAnd {
			t.Errorf("Expected OpBitwiseAnd, got %v", binExpr.Op)
		}
	})

	t.Run("bitwise OR", func(t *testing.T) {
		stmt, err := Parse("SELECT 5 | 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseOr {
			t.Errorf("Expected OpBitwiseOr, got %v", binExpr.Op)
		}
	})

	t.Run("bitwise XOR", func(t *testing.T) {
		stmt, err := Parse("SELECT 5 ^ 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseXor {
			t.Errorf("Expected OpBitwiseXor, got %v", binExpr.Op)
		}
	})

	t.Run("left shift", func(t *testing.T) {
		stmt, err := Parse("SELECT 1 << 4")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpShiftLeft {
			t.Errorf("Expected OpShiftLeft, got %v", binExpr.Op)
		}
	})

	t.Run("right shift", func(t *testing.T) {
		stmt, err := Parse("SELECT 16 >> 2")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpShiftRight {
			t.Errorf("Expected OpShiftRight, got %v", binExpr.Op)
		}
	})

	t.Run("bitwise NOT", func(t *testing.T) {
		stmt, err := Parse("SELECT ~5")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		unaryExpr, ok := sel.Columns[0].Expr.(*UnaryExpr)
		if !ok {
			t.Fatalf("Expected UnaryExpr, got %T", sel.Columns[0].Expr)
		}
		if unaryExpr.Op != OpBitwiseNot {
			t.Errorf("Expected OpBitwiseNot, got %v", unaryExpr.Op)
		}
	})
}

// TestBitwiseOperatorPrecedence verifies that operator precedence is correct.
// Precedence from lowest to highest: | < ^ < & < <<, >> < +, - < *, / < unary ~
func TestBitwiseOperatorPrecedence(t *testing.T) {
	t.Run("OR lower than XOR", func(t *testing.T) {
		// a | b ^ c should parse as a | (b ^ c)
		stmt, err := Parse("SELECT 1 | 2 ^ 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseOr {
			t.Errorf("Expected top-level OpBitwiseOr, got %v", binExpr.Op)
		}
		// Right side should be XOR
		rightExpr, ok := binExpr.Right.(*BinaryExpr)
		if !ok {
			t.Fatalf("Expected BinaryExpr on right, got %T", binExpr.Right)
		}
		if rightExpr.Op != OpBitwiseXor {
			t.Errorf("Expected right OpBitwiseXor, got %v", rightExpr.Op)
		}
	})

	t.Run("XOR lower than AND", func(t *testing.T) {
		// a ^ b & c should parse as a ^ (b & c)
		stmt, err := Parse("SELECT 1 ^ 2 & 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseXor {
			t.Errorf("Expected top-level OpBitwiseXor, got %v", binExpr.Op)
		}
		rightExpr := binExpr.Right.(*BinaryExpr)
		if rightExpr.Op != OpBitwiseAnd {
			t.Errorf("Expected right OpBitwiseAnd, got %v", rightExpr.Op)
		}
	})

	t.Run("AND lower than shift", func(t *testing.T) {
		// a & b << c should parse as a & (b << c)
		stmt, err := Parse("SELECT 1 & 2 << 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseAnd {
			t.Errorf("Expected top-level OpBitwiseAnd, got %v", binExpr.Op)
		}
		rightExpr := binExpr.Right.(*BinaryExpr)
		if rightExpr.Op != OpShiftLeft {
			t.Errorf("Expected right OpShiftLeft, got %v", rightExpr.Op)
		}
	})

	t.Run("shift lower than addition", func(t *testing.T) {
		// a << b + c should parse as a << (b + c)
		stmt, err := Parse("SELECT 1 << 2 + 3")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpShiftLeft {
			t.Errorf("Expected top-level OpShiftLeft, got %v", binExpr.Op)
		}
		rightExpr := binExpr.Right.(*BinaryExpr)
		if rightExpr.Op != OpAdd {
			t.Errorf("Expected right OpAdd, got %v", rightExpr.Op)
		}
	})

	t.Run("unary NOT binds tightly", func(t *testing.T) {
		// ~a & b should parse as (~a) & b
		stmt, err := Parse("SELECT ~1 & 2")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Columns[0].Expr.(*BinaryExpr)
		if binExpr.Op != OpBitwiseAnd {
			t.Errorf("Expected top-level OpBitwiseAnd, got %v", binExpr.Op)
		}
		// Left side should be unary NOT
		leftExpr, ok := binExpr.Left.(*UnaryExpr)
		if !ok {
			t.Fatalf("Expected UnaryExpr on left, got %T", binExpr.Left)
		}
		if leftExpr.Op != OpBitwiseNot {
			t.Errorf("Expected left OpBitwiseNot, got %v", leftExpr.Op)
		}
	})

	t.Run("comparison has lower precedence than bitwise", func(t *testing.T) {
		// a & b = c should parse as (a & b) = c
		stmt, err := Parse("SELECT 1 WHERE 5 & 1 = 1")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		sel := stmt.(*SelectStmt)
		binExpr := sel.Where.(*BinaryExpr)
		if binExpr.Op != OpEq {
			t.Errorf("Expected top-level OpEq, got %v", binExpr.Op)
		}
		// Left side should be bitwise AND
		leftExpr := binExpr.Left.(*BinaryExpr)
		if leftExpr.Op != OpBitwiseAnd {
			t.Errorf("Expected left OpBitwiseAnd, got %v", leftExpr.Op)
		}
	})
}

// TestBitwiseTokenTypes verifies that the tokenizer correctly identifies bitwise tokens.
func TestBitwiseTokenTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected tokenType
	}{
		{"&", tokenAmpersand},
		{"|", tokenPipe},
		{"^", tokenCaret},
		{"~", tokenTilde},
		{"<<", tokenShiftLeft},
		{">>", tokenShiftRight},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			p := newParser(tt.input)
			if len(p.tokens) < 2 { // token + EOF
				t.Fatalf("Expected at least 2 tokens, got %d", len(p.tokens))
			}
			if p.tokens[0].typ != tt.expected {
				t.Errorf("Expected token type %v, got %v", tt.expected, p.tokens[0].typ)
			}
			if p.tokens[0].value != tt.input {
				t.Errorf("Expected token value %q, got %q", tt.input, p.tokens[0].value)
			}
		})
	}
}

// TestBitwiseWithStringConcat ensures || still works as string concatenation.
func TestBitwiseWithStringConcat(t *testing.T) {
	stmt, err := Parse("SELECT 'hello' || ' world'")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	sel := stmt.(*SelectStmt)
	binExpr := sel.Columns[0].Expr.(*BinaryExpr)
	if binExpr.Op != OpConcat {
		t.Errorf("Expected OpConcat for ||, got %v", binExpr.Op)
	}
}

// TestBitwiseInWhereClause tests bitwise operations in WHERE clauses.
func TestBitwiseInWhereClause(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "check bit flag",
			sql:  "SELECT * FROM users WHERE permissions & 4 = 4",
		},
		{
			name: "mask and compare",
			sql:  "SELECT * FROM data WHERE (flags >> 4) & 15 = 5",
		},
		{
			name: "set bit",
			sql:  "UPDATE users SET flags = flags | 1 WHERE id = 5",
		},
		{
			name: "clear bit",
			sql:  "UPDATE users SET flags = flags & ~1 WHERE id = 5",
		},
		{
			name: "toggle bit",
			sql:  "UPDATE users SET flags = flags ^ 1 WHERE id = 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("Parse error: %v", err)
			}
		})
	}
}
