// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// Statement represents a parsed SQL statement.
type Statement interface {
	stmtNode()
	Type() dukdb.StmtType
}

// Expr represents a parsed expression.
type Expr interface {
	exprNode()
}

// ---------- Statement Types ----------

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Distinct   bool
	Columns    []SelectColumn
	From       *FromClause
	Where      Expr
	GroupBy    []Expr
	Having     Expr
	OrderBy    []OrderByExpr
	Limit      Expr
	Offset     Expr
	IsSubquery bool
}

func (*SelectStmt) stmtNode() {}

func (*SelectStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }

func (*SelectStmt) exprNode() {} // SelectStmt can be used as subquery expression

// Accept implements the Visitor pattern for SelectStmt.
func (s *SelectStmt) Accept(v Visitor) {
	v.VisitSelectStmt(s)
}

// SelectColumn represents a column in the SELECT list.
type SelectColumn struct {
	Expr  Expr
	Alias string
	Star  bool // true if this is a * column
}

// FromClause represents the FROM clause.
type FromClause struct {
	Tables []TableRef
	Joins  []JoinClause
}

// TableRef represents a table reference.
type TableRef struct {
	Catalog   string // Optional catalog name (e.g., "main")
	Schema    string
	TableName string
	Alias     string
	Subquery  *SelectStmt
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Type      JoinType
	Table     TableRef
	Condition Expr
}

// JoinType represents the type of join.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
)

// OrderByExpr represents an ORDER BY expression.
type OrderByExpr struct {
	Expr Expr
	Desc bool
}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Schema  string
	Table   string
	Columns []string
	Values  [][]Expr
	Select  *SelectStmt
}

func (*InsertStmt) stmtNode() {}

func (*InsertStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_INSERT }

// Accept implements the Visitor pattern for InsertStmt.
func (s *InsertStmt) Accept(v Visitor) {
	v.VisitInsertStmt(s)
}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Schema string
	Table  string
	Set    []SetClause
	Where  Expr
}

func (*UpdateStmt) stmtNode() {}

func (*UpdateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_UPDATE }

// Accept implements the Visitor pattern for UpdateStmt.
func (s *UpdateStmt) Accept(v Visitor) {
	v.VisitUpdateStmt(s)
}

// SetClause represents a SET clause in an UPDATE statement.
type SetClause struct {
	Column string
	Value  Expr
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Schema string
	Table  string
	Where  Expr
}

func (*DeleteStmt) stmtNode() {}

func (*DeleteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// Accept implements the Visitor pattern for DeleteStmt.
func (s *DeleteStmt) Accept(v Visitor) {
	v.VisitDeleteStmt(s)
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Schema      string
	Table       string
	IfNotExists bool
	Columns     []ColumnDefClause
	PrimaryKey  []string
}

func (*CreateTableStmt) stmtNode() {}

func (*CreateTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateTableStmt.
func (s *CreateTableStmt) Accept(v Visitor) {
	v.VisitCreateTableStmt(s)
}

// ColumnDefClause represents a column definition in CREATE TABLE.
type ColumnDefClause struct {
	Name       string
	DataType   dukdb.Type
	NotNull    bool
	Default    Expr
	PrimaryKey bool
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	Schema   string
	Table    string
	IfExists bool
}

func (*DropTableStmt) stmtNode() {}

func (*DropTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropTableStmt.
func (s *DropTableStmt) Accept(v Visitor) {
	v.VisitDropTableStmt(s)
}

// ---------- Expression Types ----------

// ColumnRef represents a column reference.
type ColumnRef struct {
	Table  string
	Column string
}

func (*ColumnRef) exprNode() {}

// Literal represents a literal value.
type Literal struct {
	Value any
	Type  dukdb.Type
}

func (*Literal) exprNode() {}

// Parameter represents a parameter placeholder ($1, $2, etc. or ?, ?)
type Parameter struct {
	Position int  // 1-based position
	Named    bool // true if this is a named parameter
	Name     string
}

func (*Parameter) exprNode() {}

// BinaryExpr represents a binary expression (a op b).
type BinaryExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
}

func (*BinaryExpr) exprNode() {}

// BinaryOp represents a binary operator.
type BinaryOp int

const (
	// Arithmetic operators
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpMod

	// Comparison operators
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe

	// Logical operators
	OpAnd
	OpOr

	// String operators
	OpLike
	OpILike
	OpNotLike
	OpNotILike

	// Other operators
	OpIn
	OpNotIn
	OpIs
	OpIsNot
	OpConcat
)

// UnaryExpr represents a unary expression (op a).
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expr
}

func (*UnaryExpr) exprNode() {}

// UnaryOp represents a unary operator.
type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
	OpPos
	OpIsNull
	OpIsNotNull
)

// FunctionCall represents a function call.
type FunctionCall struct {
	Name     string
	Args     []Expr
	Distinct bool // for aggregate functions like COUNT(DISTINCT x)
	Star     bool // for COUNT(*)
}

func (*FunctionCall) exprNode() {}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr       Expr
	TargetType dukdb.Type
}

func (*CastExpr) exprNode() {}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Operand Expr // nil for searched CASE
	Whens   []WhenClause
	Else    Expr
}

func (*CaseExpr) exprNode() {}

// WhenClause represents a WHEN clause in a CASE expression.
type WhenClause struct {
	Condition Expr
	Result    Expr
}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
	Not  bool
}

func (*BetweenExpr) exprNode() {}

// InListExpr represents an IN expression with a list.
type InListExpr struct {
	Expr   Expr
	Values []Expr
	Not    bool
}

func (*InListExpr) exprNode() {}

// InSubqueryExpr represents an IN expression with a subquery.
type InSubqueryExpr struct {
	Expr     Expr
	Subquery *SelectStmt
	Not      bool
}

func (*InSubqueryExpr) exprNode() {}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *SelectStmt
	Not      bool
}

func (*ExistsExpr) exprNode() {}

// StarExpr represents a * expression in SELECT.
type StarExpr struct {
	Table string // optional table prefix (e.g., t.*)
}

func (*StarExpr) exprNode() {}

// BeginStmt represents a BEGIN TRANSACTION statement.
type BeginStmt struct{}

func (*BeginStmt) stmtNode() {}

func (*BeginStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for BeginStmt.
func (s *BeginStmt) Accept(v Visitor) {
	v.VisitBeginStmt(s)
}

// CommitStmt represents a COMMIT statement.
type CommitStmt struct{}

func (*CommitStmt) stmtNode() {}

func (*CommitStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for CommitStmt.
func (s *CommitStmt) Accept(v Visitor) {
	v.VisitCommitStmt(s)
}

// RollbackStmt represents a ROLLBACK statement.
type RollbackStmt struct{}

func (*RollbackStmt) stmtNode() {}

func (*RollbackStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for RollbackStmt.
func (s *RollbackStmt) Accept(v Visitor) {
	v.VisitRollbackStmt(s)
}
