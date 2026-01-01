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

// CTE represents a Common Table Expression (WITH clause).
type CTE struct {
	Name    string      // CTE name (e.g., "tmp" in WITH tmp AS ...)
	Columns []string    // Optional column names
	Query   *SelectStmt // The CTE query
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	CTEs       []CTE // Common Table Expressions (WITH clause)
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
	SetOp      SetOpType   // Type of set operation (UNION, INTERSECT, EXCEPT)
	Right      *SelectStmt // Right side of set operation
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
	Catalog       string // Optional catalog name (e.g., "main")
	Schema        string
	TableName     string
	Alias         string
	Subquery      *SelectStmt
	TableFunction *TableFunctionRef // Table function call (e.g., read_csv('file.csv'))
}

// TableFunctionRef represents a table function call in a FROM clause.
// Example: read_csv('file.csv', delimiter=',', header=true)
type TableFunctionRef struct {
	// Name is the function name (e.g., "read_csv", "read_json", "read_parquet").
	Name string
	// Args are positional arguments passed to the function.
	Args []Expr
	// NamedArgs are named arguments (key=value pairs).
	NamedArgs map[string]Expr
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

// SetOpType represents the type of set operation.
type SetOpType int

const (
	SetOpNone SetOpType = iota
	SetOpUnion
	SetOpUnionAll
	SetOpIntersect
	SetOpIntersectAll
	SetOpExcept
	SetOpExceptAll
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
	From   *FromClause // Optional FROM clause for UPDATE...FROM
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
	AsSelect    *SelectStmt // For CREATE TABLE ... AS SELECT
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

// ExtractExpr represents an EXTRACT(part FROM source) expression.
// This is SQL standard syntax that extracts a date/time field from a temporal value.
type ExtractExpr struct {
	Part   string // The part to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.)
	Source Expr   // The source expression (date, timestamp, or time value)
}

func (*ExtractExpr) exprNode() {}

// IntervalLiteral represents an INTERVAL literal expression.
// Supports various DuckDB syntaxes:
//   - INTERVAL 'n' UNIT (e.g., INTERVAL '5' DAY)
//   - INTERVAL 'n unit' (e.g., INTERVAL '5 days')
//   - INTERVAL 'n units m units' (e.g., INTERVAL '2 hours 30 minutes')
//
// The Value field contains the parsed interval components.
type IntervalLiteral struct {
	Months int32 // Number of months (includes years * 12)
	Days   int32 // Number of days
	Micros int64 // Number of microseconds (sub-day time)
}

func (*IntervalLiteral) exprNode() {}

// CopyStmt represents a COPY statement for importing/exporting data.
// Supports:
//   - COPY table FROM 'path' (OPTIONS)
//   - COPY table TO 'path' (OPTIONS)
//   - COPY (SELECT...) TO 'path' (OPTIONS)
type CopyStmt struct {
	// TableName is the target table name for COPY FROM/TO table.
	TableName string
	// Schema is the optional schema name (default "main").
	Schema string
	// Columns is an optional column list for partial import/export.
	Columns []string
	// FilePath is the file path to read from or write to.
	FilePath string
	// IsFrom is true for COPY FROM (import), false for COPY TO (export).
	IsFrom bool
	// Query is set for COPY (SELECT...) TO syntax.
	Query *SelectStmt
	// Options contains COPY options like DELIMITER, HEADER, FORMAT, etc.
	Options map[string]any
}

func (*CopyStmt) stmtNode() {}

func (*CopyStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_COPY }

// Accept implements the Visitor pattern for CopyStmt.
func (s *CopyStmt) Accept(v Visitor) {
	v.VisitCopyStmt(s)
}

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

// ---------- Window Function Types ----------

// WindowExpr represents a window function expression.
// Example: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
type WindowExpr struct {
	Function    *FunctionCall   // The window function (ROW_NUMBER, RANK, etc.)
	PartitionBy []Expr          // PARTITION BY expressions
	OrderBy     []WindowOrderBy // ORDER BY within window (with NULLS FIRST/LAST)
	Frame       *WindowFrame    // Optional frame specification
	IgnoreNulls bool            // IGNORE NULLS modifier (for LAG, LEAD, FIRST_VALUE, etc.)
	Filter      Expr            // FILTER (WHERE ...) clause for aggregate windows
	Distinct    bool            // DISTINCT modifier for aggregate windows
}

func (*WindowExpr) exprNode() {}

// WindowOrderBy extends OrderByExpr with NULLS FIRST/LAST support.
type WindowOrderBy struct {
	Expr       Expr
	Desc       bool
	NullsFirst bool // true for NULLS FIRST, false for NULLS LAST (default)
}

// WindowFrame represents ROWS/RANGE/GROUPS BETWEEN specification.
type WindowFrame struct {
	Type    FrameType   // ROWS, RANGE, or GROUPS
	Start   WindowBound // Start boundary
	End     WindowBound // End boundary
	Exclude ExcludeMode // EXCLUDE clause
}

// FrameType distinguishes ROWS vs RANGE vs GROUPS semantics.
type FrameType int

const (
	FrameTypeRows   FrameType = iota // ROWS BETWEEN (physical offset)
	FrameTypeRange                   // RANGE BETWEEN (logical offset)
	FrameTypeGroups                  // GROUPS BETWEEN (peer group offset)
)

// WindowBound represents a frame boundary.
type WindowBound struct {
	Type   BoundType // UNBOUNDED, CURRENT, or OFFSET
	Offset Expr      // For N PRECEDING / N FOLLOWING (must be non-negative constant)
}

// BoundType represents the type of window frame boundary.
type BoundType int

const (
	BoundUnboundedPreceding BoundType = iota
	BoundPreceding                    // N PRECEDING
	BoundCurrentRow                   // CURRENT ROW
	BoundFollowing                    // N FOLLOWING
	BoundUnboundedFollowing
)

// ExcludeMode specifies which rows to exclude from frame.
type ExcludeMode int

const (
	ExcludeNoOthers   ExcludeMode = iota // EXCLUDE NO OTHERS (default)
	ExcludeCurrentRow                    // EXCLUDE CURRENT ROW
	ExcludeGroup                         // EXCLUDE GROUP (current row's peer group)
	ExcludeTies                          // EXCLUDE TIES (peers but not current row)
)
