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
	Name           string      // CTE name (e.g., "tmp" in WITH tmp AS ...)
	Columns        []string    // Optional column names
	Query          *SelectStmt // The CTE query (anchor/base case for recursive CTEs)
	RecursiveQuery *SelectStmt // The recursive case query (only for recursive CTEs)
	Recursive      bool        // True for WITH RECURSIVE (allows self-referencing in the CTE query)
	UsingKey       []string    // Optional USING KEY columns for recursive CTE cycle detection
	SetOp          SetOpType   // Type of set operation (UNION vs UNION ALL) for recursive CTE
	MaxRecursion   int         // Maximum recursion iterations (-1 means use default of 1000)
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	CTEs       []CTE // Common Table Expressions (WITH clause)
	Distinct   bool
	DistinctOn []Expr // DISTINCT ON (col1, col2) expressions - selects first row per distinct group
	Columns    []SelectColumn
	From       *FromClause
	Where      Expr
	GroupBy    []Expr
	Having     Expr
	Qualify    Expr // QUALIFY clause - filters rows after window function evaluation
	OrderBy    []OrderByExpr
	Limit      Expr
	Offset     Expr
	Sample     *SampleOptions // SAMPLE clause - for sampling a subset of rows
	Options    *RecursionOption
	IsSubquery bool
	SetOp      SetOpType   // Type of set operation (UNION, INTERSECT, EXCEPT)
	Right      *SelectStmt // Right side of set operation
}

// RecursionOption represents recursion control options for a SELECT query.
// Currently supports OPTION (MAX_RECURSION N).
type RecursionOption struct {
	MaxRecursion int
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
	PivotRef      *PivotStmt        // PIVOT table reference (when PIVOT is used in FROM clause)
	UnpivotRef    *UnpivotStmt      // UNPIVOT table reference (when UNPIVOT is used in FROM clause)
	Lateral       bool              // LATERAL join (subquery can reference columns from outer scope)
	TimeTravel    *TimeTravelClause // Time travel clause (AS OF TIMESTAMP, AS OF SNAPSHOT, etc.)
}

// TimeTravelType represents the type of time travel specification.
type TimeTravelType int

const (
	// TimeTravelTimestamp specifies time travel by timestamp.
	// Example: AS OF TIMESTAMP '2024-01-15 10:00:00'
	TimeTravelTimestamp TimeTravelType = iota

	// TimeTravelSnapshot specifies time travel by snapshot ID.
	// Example: AS OF SNAPSHOT 1234567890
	TimeTravelSnapshot

	// TimeTravelBranch specifies time travel by branch name (future use).
	// Example: AS OF BRANCH main
	TimeTravelBranch

	// TimeTravelVersion specifies time travel by explicit version number.
	// Example: AT (VERSION => 3)
	TimeTravelVersion
)

// String returns the human-readable name of the time travel type.
func (tt TimeTravelType) String() string {
	switch tt {
	case TimeTravelTimestamp:
		return "TIMESTAMP"
	case TimeTravelSnapshot:
		return "SNAPSHOT"
	case TimeTravelBranch:
		return "BRANCH"
	case TimeTravelVersion:
		return "VERSION"
	default:
		return "UNKNOWN"
	}
}

// TimeTravelClause represents a time travel specification for Iceberg tables.
// Supports:
//   - AS OF TIMESTAMP '2024-01-15 10:00:00' - time travel by timestamp
//   - AS OF SNAPSHOT 1234567890 - time travel by snapshot ID
//   - AS OF BRANCH main - time travel by branch name (future)
//   - AT (VERSION => 3) - explicit version selection
type TimeTravelClause struct {
	Type  TimeTravelType // The type of time travel specification
	Value Expr           // The value: timestamp string, snapshot ID (int), branch name (string), or version number (int)
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
	Using     []string // USING (col1, col2)
}

// JoinType represents the type of join.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
	JoinTypeNatural     // NATURAL JOIN
	JoinTypeNaturalLeft // NATURAL LEFT JOIN
	JoinTypeNaturalRight // NATURAL RIGHT JOIN
	JoinTypeNaturalFull // NATURAL FULL JOIN
	JoinTypeAsOf        // ASOF JOIN
	JoinTypeAsOfLeft    // ASOF LEFT JOIN
	JoinTypePositional  // POSITIONAL JOIN
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
	SetOpUnionByName
	SetOpUnionAllByName
)

// IsolationLevel represents the transaction isolation level.
// The isolation level determines what data a transaction can see
// when other transactions are running concurrently.
type IsolationLevel int

const (
	// IsolationLevelSerializable is the default and strictest isolation level.
	// It prevents dirty reads, non-repeatable reads, and phantom reads.
	// Transactions appear to execute serially.
	IsolationLevelSerializable IsolationLevel = iota

	// IsolationLevelRepeatableRead prevents dirty reads and non-repeatable reads.
	// A snapshot is taken at transaction start; phantom reads may occur.
	IsolationLevelRepeatableRead

	// IsolationLevelReadCommitted prevents dirty reads only.
	// Each statement sees data committed before that statement began.
	// Non-repeatable reads and phantom reads may occur.
	IsolationLevelReadCommitted

	// IsolationLevelReadUncommitted allows dirty reads.
	// Transactions can see uncommitted changes from other transactions.
	IsolationLevelReadUncommitted
)

// String returns the human-readable name of the isolation level.
func (il IsolationLevel) String() string {
	switch il {
	case IsolationLevelSerializable:
		return "SERIALIZABLE"
	case IsolationLevelRepeatableRead:
		return "REPEATABLE READ"
	case IsolationLevelReadCommitted:
		return "READ COMMITTED"
	case IsolationLevelReadUncommitted:
		return "READ UNCOMMITTED"
	default:
		return "UNKNOWN"
	}
}

// OrderByExpr represents an ORDER BY expression.
// Supports NULLS FIRST/LAST to control NULL ordering.
type OrderByExpr struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool  // nil = default, true = NULLS FIRST, false = NULLS LAST
	Collation  string // COLLATE collation_name (empty = default)
}

// OnConflictAction specifies what to do when an INSERT conflicts.
type OnConflictAction int

const (
	// OnConflictDoNothing skips the conflicting row.
	OnConflictDoNothing OnConflictAction = iota
	// OnConflictDoUpdate updates the existing row with new values.
	OnConflictDoUpdate
)

// OnConflictClause represents the ON CONFLICT clause of an INSERT statement.
type OnConflictClause struct {
	ConflictColumns []string         // Columns that define conflict target (empty = infer from PK)
	Action          OnConflictAction // DO NOTHING or DO UPDATE
	UpdateSet       []SetClause      // SET assignments for DO UPDATE
	UpdateWhere     Expr             // Optional WHERE for DO UPDATE
}

// InsertStmt represents an INSERT statement.
// Supports the optional ON CONFLICT and RETURNING clauses.
//
// Example:
//
//	INSERT INTO t (a, b) VALUES (1, 2) RETURNING *;
//	INSERT INTO t (a, b) VALUES (1, 2) RETURNING id, a, b;
//	INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING;
//	INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b;
type InsertStmt struct {
	Schema  string
	Table   string
	Columns []string
	Values  [][]Expr
	Select  *SelectStmt
	// OnConflict specifies the upsert behavior when a primary key conflict occurs.
	// nil when no ON CONFLICT clause is present.
	OnConflict *OnConflictClause
	// Returning specifies columns to return after the insert operation.
	// If non-empty, the INSERT becomes a query that returns the specified columns
	// from the newly inserted rows. Use Star=true in SelectColumn for RETURNING *.
	Returning []SelectColumn
}

func (*InsertStmt) stmtNode() {}

func (*InsertStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_INSERT }

// Accept implements the Visitor pattern for InsertStmt.
func (s *InsertStmt) Accept(v Visitor) {
	v.VisitInsertStmt(s)
}

// UpdateStmt represents an UPDATE statement.
// Supports the optional RETURNING clause to return values from updated rows.
//
// Example:
//
//	UPDATE t SET x = 1 WHERE id = 5 RETURNING id, x;
//	UPDATE t SET x = 1 WHERE id = 5 RETURNING *;
type UpdateStmt struct {
	Schema string
	Table  string
	Set    []SetClause
	From   *FromClause // Optional FROM clause for UPDATE...FROM
	Where  Expr
	// Returning specifies columns to return after the update operation.
	// If non-empty, the UPDATE becomes a query that returns the specified columns
	// from the updated rows. Use Star=true in SelectColumn for RETURNING *.
	Returning []SelectColumn
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
// Supports the optional RETURNING clause to return values from deleted rows.
//
// Example:
//
//	DELETE FROM t WHERE id = 5 RETURNING *;
//	DELETE FROM t WHERE id = 5 RETURNING id, name;
type DeleteStmt struct {
	Schema string
	Table  string
	Where  Expr
	// Returning specifies columns to return after the delete operation.
	// If non-empty, the DELETE becomes a query that returns the specified columns
	// from the deleted rows. Use Star=true in SelectColumn for RETURNING *.
	Returning []SelectColumn
}

func (*DeleteStmt) stmtNode() {}

func (*DeleteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// Accept implements the Visitor pattern for DeleteStmt.
func (s *DeleteStmt) Accept(v Visitor) {
	v.VisitDeleteStmt(s)
}

// TableConstraint represents a table-level constraint in CREATE TABLE.
type TableConstraint struct {
	Name       string   // Optional CONSTRAINT name
	Type       string   // "UNIQUE", "CHECK"
	Columns    []string // Column names (for UNIQUE)
	Expression Expr     // For CHECK constraints
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Schema      string
	Table       string
	IfNotExists bool
	Columns     []ColumnDefClause
	PrimaryKey  []string
	Constraints []TableConstraint // Table-level constraints (UNIQUE, CHECK)
	AsSelect    *SelectStmt       // For CREATE TABLE ... AS SELECT
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
	TypeInfo   dukdb.TypeInfo
	NotNull    bool
	Default    Expr
	PrimaryKey bool
	Unique     bool   // Column-level UNIQUE constraint
	Check      Expr   // Column-level CHECK expression
	Collation  string // COLLATE collation_name (empty = default)
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

// ---------- View DDL Statements ----------

// CreateViewStmt represents a CREATE VIEW statement.
type CreateViewStmt struct {
	Schema      string
	View        string
	IfNotExists bool
	Query       *SelectStmt // The view definition
}

func (*CreateViewStmt) stmtNode() {}

func (*CreateViewStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateViewStmt.
func (s *CreateViewStmt) Accept(v Visitor) {
	v.VisitCreateViewStmt(s)
}

// DropViewStmt represents a DROP VIEW statement.
type DropViewStmt struct {
	Schema   string
	View     string
	IfExists bool
}

func (*DropViewStmt) stmtNode() {}

func (*DropViewStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropViewStmt.
func (s *DropViewStmt) Accept(v Visitor) {
	v.VisitDropViewStmt(s)
}

// ---------- Index DDL Statements ----------

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	Schema      string
	Table       string
	Index       string
	IfNotExists bool
	Columns     []string
	IsUnique    bool
}

func (*CreateIndexStmt) stmtNode() {}

func (*CreateIndexStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateIndexStmt.
func (s *CreateIndexStmt) Accept(v Visitor) {
	v.VisitCreateIndexStmt(s)
}

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	Schema   string
	Index    string
	IfExists bool
}

func (*DropIndexStmt) stmtNode() {}

func (*DropIndexStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropIndexStmt.
func (s *DropIndexStmt) Accept(v Visitor) {
	v.VisitDropIndexStmt(s)
}

// ---------- Sequence DDL Statements ----------

// CreateSequenceStmt represents a CREATE SEQUENCE statement.
type CreateSequenceStmt struct {
	Schema      string
	Sequence    string
	IfNotExists bool
	StartWith   int64
	IncrementBy int64
	MinValue    *int64 // nil means NO MINVALUE
	MaxValue    *int64 // nil means NO MAXVALUE
	IsCycle     bool
}

func (*CreateSequenceStmt) stmtNode() {}

func (*CreateSequenceStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateSequenceStmt.
func (s *CreateSequenceStmt) Accept(v Visitor) {
	v.VisitCreateSequenceStmt(s)
}

// DropSequenceStmt represents a DROP SEQUENCE statement.
type DropSequenceStmt struct {
	Schema   string
	Sequence string
	IfExists bool
}

func (*DropSequenceStmt) stmtNode() {}

func (*DropSequenceStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropSequenceStmt.
func (s *DropSequenceStmt) Accept(v Visitor) {
	v.VisitDropSequenceStmt(s)
}

// ---------- Type DDL Statements ----------

// CreateTypeStmt represents CREATE TYPE name AS ENUM (...).
type CreateTypeStmt struct {
	Name        string
	Schema      string
	TypeKind    string   // "ENUM"
	EnumValues  []string
	IfNotExists bool
}

func (*CreateTypeStmt) stmtNode() {}

func (*CreateTypeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateTypeStmt.
func (s *CreateTypeStmt) Accept(v Visitor) {
	v.VisitCreateTypeStmt(s)
}

// DropTypeStmt represents DROP TYPE [IF EXISTS] name.
type DropTypeStmt struct {
	Name     string
	Schema   string
	IfExists bool
}

func (*DropTypeStmt) stmtNode() {}

func (*DropTypeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropTypeStmt.
func (s *DropTypeStmt) Accept(v Visitor) {
	v.VisitDropTypeStmt(s)
}

// ---------- Schema DDL Statements ----------

// CreateSchemaStmt represents a CREATE SCHEMA statement.
type CreateSchemaStmt struct {
	Schema      string
	IfNotExists bool
}

func (*CreateSchemaStmt) stmtNode() {}

func (*CreateSchemaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateSchemaStmt.
func (s *CreateSchemaStmt) Accept(v Visitor) {
	v.VisitCreateSchemaStmt(s)
}

// DropSchemaStmt represents a DROP SCHEMA statement.
type DropSchemaStmt struct {
	Schema   string
	IfExists bool
	Cascade  bool // If true, drop all objects in schema
}

func (*DropSchemaStmt) stmtNode() {}

func (*DropSchemaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropSchemaStmt.
func (s *DropSchemaStmt) Accept(v Visitor) {
	v.VisitDropSchemaStmt(s)
}

// ---------- ALTER TABLE Statement ----------

// AlterTableOp represents the type of ALTER TABLE operation.
type AlterTableOp int

const (
	AlterTableRenameTo AlterTableOp = iota
	AlterTableRenameColumn
	AlterTableDropColumn
	AlterTableAddColumn
	AlterTableSetOption
)

// AlterTableStmt represents an ALTER TABLE statement.
type AlterTableStmt struct {
	Schema    string
	Table     string
	IfExists  bool
	Operation AlterTableOp
	// Operation-specific fields:
	NewTableName string           // RENAME TO
	OldColumn    string           // RENAME COLUMN
	NewColumn    string           // RENAME COLUMN
	DropColumn   string           // DROP COLUMN
	AddColumn    *ColumnDefClause // ADD COLUMN
}

func (*AlterTableStmt) stmtNode() {}

func (*AlterTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }

// Accept implements the Visitor pattern for AlterTableStmt.
func (s *AlterTableStmt) Accept(v Visitor) {
	v.VisitAlterTableStmt(s)
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

// LambdaExpr represents a lambda expression: x -> expr or (x, y) -> expr.
// Lambda expressions are only valid as function arguments (e.g., list_transform, list_filter).
type LambdaExpr struct {
	Params []string // Parameter names
	Body   Expr     // Body expression
}

func (*LambdaExpr) exprNode() {}

// SimilarToExpr represents a SIMILAR TO expression with optional ESCAPE clause.
type SimilarToExpr struct {
	Expr    Expr   // Left-hand expression
	Pattern Expr   // The SQL regex pattern
	Escape  string // Escape character (empty means default '\')
	Not     bool   // true for NOT SIMILAR TO
}

func (*SimilarToExpr) exprNode() {}

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
	OpSimilarTo
	OpNotSimilarTo

	// Other operators
	OpIn
	OpNotIn
	OpIs
	OpIsNot
	OpConcat

	// JSON operators
	OpJSONExtract // -> (JSON extract, returns JSON)
	OpJSONText    // ->> (JSON extract as text, returns VARCHAR)

	// Bitwise operators
	OpBitwiseAnd // & (bitwise AND)
	OpBitwiseOr  // | (bitwise OR)
	OpBitwiseXor // ^ (bitwise XOR)
	OpShiftLeft  // << (left shift)
	OpShiftRight // >> (right shift)
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
	OpBitwiseNot // ~ (bitwise NOT)
)

// FunctionCall represents a function call.
type FunctionCall struct {
	Name      string
	Args      []Expr
	NamedArgs map[string]Expr // Named arguments for functions like struct_pack(name := 'Alice')
	Distinct  bool            // for aggregate functions like COUNT(DISTINCT x)
	Star      bool            // for COUNT(*)
	OrderBy   []OrderByExpr   // for aggregate functions like STRING_AGG(x, ',' ORDER BY y)
}

func (*FunctionCall) exprNode() {}

// NamedArgExpr represents a named argument in a function call (e.g., name := 'Alice' in struct_pack).
type NamedArgExpr struct {
	Name  string // The argument name
	Value Expr   // The argument value expression
}

func (*NamedArgExpr) exprNode() {}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr       Expr
	TargetType dukdb.Type
	TryCast    bool
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

// ArrayExpr represents an array literal expression.
// Used for array syntax in table functions like ['file1.csv', 'file2.csv'].
// Example: SELECT * FROM read_csv(['file1.csv', 'file2.csv'])
type ArrayExpr struct {
	Elements []Expr // The array elements
}

func (*ArrayExpr) exprNode() {}

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
// Supports optional isolation level specification:
//   - BEGIN
//   - BEGIN TRANSACTION
//   - BEGIN TRANSACTION ISOLATION LEVEL <level>
//
// Where <level> is one of: READ UNCOMMITTED, READ COMMITTED,
// REPEATABLE READ, or SERIALIZABLE.
type BeginStmt struct {
	// IsolationLevel specifies the isolation level for this transaction.
	// If not explicitly specified in the SQL, this defaults to
	// IsolationLevelSerializable (the zero value).
	IsolationLevel IsolationLevel

	// HasExplicitIsolation indicates whether the isolation level was
	// explicitly specified in the SQL statement (e.g., "BEGIN TRANSACTION
	// ISOLATION LEVEL SERIALIZABLE"). When false, the connection's default
	// isolation level should be used instead.
	HasExplicitIsolation bool
}

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

// ---------- Savepoint Statements ----------

// SavepointStmt represents a SAVEPOINT statement.
// Syntax: SAVEPOINT <name>
type SavepointStmt struct {
	Name string // The savepoint name
}

func (*SavepointStmt) stmtNode() {}

func (*SavepointStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for SavepointStmt.
func (s *SavepointStmt) Accept(v Visitor) {
	v.VisitSavepointStmt(s)
}

// RollbackToSavepointStmt represents a ROLLBACK TO SAVEPOINT statement.
// Syntax: ROLLBACK TO SAVEPOINT <name>
type RollbackToSavepointStmt struct {
	Name string // The savepoint name
}

func (*RollbackToSavepointStmt) stmtNode() {}

func (*RollbackToSavepointStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for RollbackToSavepointStmt.
func (s *RollbackToSavepointStmt) Accept(v Visitor) {
	v.VisitRollbackToSavepointStmt(s)
}

// ReleaseSavepointStmt represents a RELEASE SAVEPOINT statement.
// Syntax: RELEASE SAVEPOINT <name>
type ReleaseSavepointStmt struct {
	Name string // The savepoint name
}

func (*ReleaseSavepointStmt) stmtNode() {}

func (*ReleaseSavepointStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for ReleaseSavepointStmt.
func (s *ReleaseSavepointStmt) Accept(v Visitor) {
	v.VisitReleaseSavepointStmt(s)
}

// ---------- Prepared Statement Types ----------

// PrepareStmt represents PREPARE name AS statement.
type PrepareStmt struct {
	Name  string    // Prepared statement name
	Inner Statement // The statement to prepare
}

func (*PrepareStmt) stmtNode() {}

func (*PrepareStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PREPARE }

// ExecuteStmt represents EXECUTE name or EXECUTE name(param1, param2, ...).
type ExecuteStmt struct {
	Name   string // Prepared statement name
	Params []Expr // Parameter values
}

func (*ExecuteStmt) stmtNode() {}

func (*ExecuteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXECUTE }

// DeallocateStmt represents DEALLOCATE [PREPARE] name or DEALLOCATE ALL.
type DeallocateStmt struct {
	Name string // Empty string means DEALLOCATE ALL
	All  bool   // True for DEALLOCATE ALL
}

func (*DeallocateStmt) stmtNode() {}

func (*DeallocateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DEALLOCATE }

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

// ---------- Sampling Types ----------

// ---------- Secret DDL Statements ----------

// CreateSecretStmt represents a CREATE SECRET statement.
// DuckDB syntax:
//
//	CREATE [OR REPLACE] [PERSISTENT | TEMPORARY] SECRET [IF NOT EXISTS] name (
//	    TYPE secret_type,
//	    [PROVIDER provider_type,]
//	    [SCOPE scope_path,]
//	    option_name option_value, ...
//	)
type CreateSecretStmt struct {
	Name        string            // Secret name
	IfNotExists bool              // IF NOT EXISTS clause
	OrReplace   bool              // OR REPLACE clause
	Persistent  bool              // PERSISTENT (survives restarts) vs TEMPORARY
	SecretType  string            // Type of secret (S3, GCS, AZURE, HTTP, HUGGINGFACE)
	Provider    string            // Provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
	Scope       string            // Optional scope path (e.g., s3://bucket/path)
	Options     map[string]string // Key-value options (key_id, secret, region, etc.)
}

func (*CreateSecretStmt) stmtNode() {}

func (*CreateSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateSecretStmt.
func (s *CreateSecretStmt) Accept(v Visitor) {
	v.VisitCreateSecretStmt(s)
}

// DropSecretStmt represents a DROP SECRET statement.
// Syntax: DROP SECRET [IF EXISTS] name
type DropSecretStmt struct {
	Name     string // Secret name
	IfExists bool   // IF EXISTS clause
}

func (*DropSecretStmt) stmtNode() {}

func (*DropSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropSecretStmt.
func (s *DropSecretStmt) Accept(v Visitor) {
	v.VisitDropSecretStmt(s)
}

// AlterSecretStmt represents an ALTER SECRET statement.
// Syntax: ALTER SECRET name (option_name option_value, ...)
type AlterSecretStmt struct {
	Name    string            // Secret name
	Options map[string]string // Options to update
}

func (*AlterSecretStmt) stmtNode() {}

func (*AlterSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }

// Accept implements the Visitor pattern for AlterSecretStmt.
func (s *AlterSecretStmt) Accept(v Visitor) {
	v.VisitAlterSecretStmt(s)
}

// SampleMethod represents the method used for sampling rows from a table.
type SampleMethod int

const (
	SampleBernoulli SampleMethod = iota // BERNOULLI - probabilistic row sampling (each row has independent probability)
	SampleSystem                        // SYSTEM - block-level sampling (faster, less random)
	SampleReservoir                     // RESERVOIR - reservoir sampling (fixed row count)
)

// SampleOptions represents the SAMPLE clause configuration.
// The SAMPLE clause allows sampling a subset of rows from a table.
// Examples:
//   - SELECT * FROM t USING SAMPLE 10%
//   - SELECT * FROM t USING SAMPLE BERNOULLI(10%)
//   - SELECT * FROM t USING SAMPLE RESERVOIR(100 ROWS)
//   - SELECT * FROM t USING SAMPLE 10% (SEED 42)
type SampleOptions struct {
	Method     SampleMethod // Sampling method (BERNOULLI, SYSTEM, or RESERVOIR)
	Percentage float64      // For BERNOULLI/SYSTEM - percentage of rows to sample (0-100)
	Rows       int          // For RESERVOIR - fixed number of rows to sample
	Seed       *int64       // Optional seed for reproducible sampling
}

// ---------- Export/Import Database Statements ----------

// ExportDatabaseStmt represents an EXPORT DATABASE 'path' (OPTIONS) statement.
// Exports all tables, views, and schemas to a directory for full backup.
type ExportDatabaseStmt struct {
	Path    string
	Options map[string]string // FORMAT, DELIMITER, etc.
}

func (*ExportDatabaseStmt) stmtNode() {}

func (*ExportDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXPORT }

// Accept implements the Visitor pattern for ExportDatabaseStmt.
func (s *ExportDatabaseStmt) Accept(v Visitor) {
	v.VisitExportDatabaseStmt(s)
}

// ImportDatabaseStmt represents an IMPORT DATABASE 'path' statement.
// Imports a previously exported database from a directory.
type ImportDatabaseStmt struct {
	Path string
}

func (*ImportDatabaseStmt) stmtNode() {}

func (*ImportDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_COPY }

// Accept implements the Visitor pattern for ImportDatabaseStmt.
func (s *ImportDatabaseStmt) Accept(v Visitor) {
	v.VisitImportDatabaseStmt(s)
}

// ---------- Database Maintenance Statements ----------

// PragmaStmt represents a PRAGMA statement.
// Supports various syntaxes:
//   - PRAGMA pragma_name
//   - PRAGMA pragma_name(arg1, arg2, ...)
//   - PRAGMA pragma_name = value
type PragmaStmt struct {
	Name  string // Pragma name (e.g., "database_size", "table_info")
	Args  []Expr // Pragma arguments for function-style syntax
	Value Expr   // For SET PRAGMA name = value
}

func (*PragmaStmt) stmtNode() {}

func (*PragmaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PRAGMA }

// Accept implements the Visitor pattern for PragmaStmt.
func (s *PragmaStmt) Accept(v Visitor) {
	v.VisitPragmaStmt(s)
}

// ExplainStmt represents an EXPLAIN or EXPLAIN ANALYZE statement.
// Wraps another statement to show its execution plan.
type ExplainStmt struct {
	Query   Statement // The statement to explain (typically SelectStmt)
	Analyze bool      // true for EXPLAIN ANALYZE (execute and show actual metrics)
}

func (*ExplainStmt) stmtNode() {}

func (*ExplainStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXPLAIN }

// Accept implements the Visitor pattern for ExplainStmt.
func (s *ExplainStmt) Accept(v Visitor) {
	v.VisitExplainStmt(s)
}

// VacuumStmt represents a VACUUM statement.
// Reclaims space from deleted rows and optimizes storage.
type VacuumStmt struct {
	Schema    string // Optional schema name
	TableName string // Optional table name (empty for entire database)
}

func (*VacuumStmt) stmtNode() {}

func (*VacuumStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_VACUUM }

// Accept implements the Visitor pattern for VacuumStmt.
func (s *VacuumStmt) Accept(v Visitor) {
	v.VisitVacuumStmt(s)
}

// AnalyzeStmt represents an ANALYZE statement.
// Collects statistics about table columns for query optimization.
type AnalyzeStmt struct {
	Schema    string // Optional schema name
	TableName string // Optional table name (empty for all tables)
}

func (*AnalyzeStmt) stmtNode() {}

func (*AnalyzeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ANALYZE }

// Accept implements the Visitor pattern for AnalyzeStmt.
func (s *AnalyzeStmt) Accept(v Visitor) {
	v.VisitAnalyzeStmt(s)
}

// CheckpointStmt represents a CHECKPOINT statement.
// Forces a database checkpoint, writing pending changes to disk.
type CheckpointStmt struct {
	Database string // Optional database name
	Force    bool   // FORCE flag to force checkpoint even if not needed
}

func (*CheckpointStmt) stmtNode() {}

func (*CheckpointStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// Accept implements the Visitor pattern for CheckpointStmt.
func (s *CheckpointStmt) Accept(v Visitor) {
	v.VisitCheckpointStmt(s)
}

// ---------- Macro DDL Statements ----------

// MacroParam represents a macro parameter (parser-level).
type MacroParam struct {
	Name       string
	Default    Expr   // Parsed default expression (nil if no default)
	DefaultSQL string // Raw SQL default for storage
}

// CreateMacroStmt represents a CREATE MACRO statement.
type CreateMacroStmt struct {
	Schema       string
	Name         string
	Params       []MacroParam
	IsTableMacro bool
	OrReplace    bool
	Body         Expr        // Expression body for scalar macros
	BodySQL      string      // Raw SQL body string
	Query        *SelectStmt // Query body for table macros
	QuerySQL     string      // Raw SQL query string
}

func (*CreateMacroStmt) stmtNode() {}

func (*CreateMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateMacroStmt.
func (s *CreateMacroStmt) Accept(v Visitor) {
	v.VisitCreateMacroStmt(s)
}

// DropMacroStmt represents a DROP MACRO statement.
type DropMacroStmt struct {
	Schema       string
	Name         string
	IfExists     bool
	IsTableMacro bool
}

func (*DropMacroStmt) stmtNode() {}

func (*DropMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropMacroStmt.
func (s *DropMacroStmt) Accept(v Visitor) {
	v.VisitDropMacroStmt(s)
}

// ---------- Function DDL Statements ----------

// VolatilityType represents the volatility of a user-defined function.
// This affects optimizer decisions about when the function can be evaluated.
type VolatilityType int

const (
	// VolatilityVolatile is the default - function may return different results
	// for the same arguments (e.g., random(), now()).
	VolatilityVolatile VolatilityType = iota
	// VolatilityStable means the function returns consistent results within a
	// single query execution but may change between queries.
	VolatilityStable
	// VolatilityImmutable means the function always returns the same result
	// for the same arguments (e.g., mathematical functions).
	VolatilityImmutable
)

// FuncParam represents a parameter in a user-defined function.
type FuncParam struct {
	Name string     // Parameter name (e.g., "a", "b")
	Type dukdb.Type // Parameter type (e.g., INTEGER, VARCHAR)
	Info dukdb.TypeInfo
}

// CreateFunctionStmt represents a CREATE FUNCTION statement for scalar UDFs.
// Syntax:
//
//	CREATE [OR REPLACE] FUNCTION name(params) RETURNS type
//	    [LANGUAGE lang] [IMMUTABLE|STABLE|VOLATILE] [STRICT] [LEAKPROOF]
//	    [PARALLEL SAFE|UNSAFE|RESTRICTED]
//	    AS 'body' | AS $$body$$
type CreateFunctionStmt struct {
	Schema       string      // Optional schema name
	Name         string      // Function name
	OrReplace    bool        // OR REPLACE clause
	Params       []FuncParam // Function parameters
	Returns      dukdb.Type  // Return type
	ReturnsInfo  dukdb.TypeInfo
	Language     string         // Language (default "sql")
	Body         string         // Function body
	Volatility   VolatilityType // VOLATILE, STABLE, or IMMUTABLE
	Strict       bool           // STRICT (returns NULL if any argument is NULL)
	Leakproof    bool           // LEAKPROOF (doesn't leak information)
	ParallelSafe string         // PARALLEL SAFE, UNSAFE, or RESTRICTED (empty = default)
}

func (*CreateFunctionStmt) stmtNode() {}

func (*CreateFunctionStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateFunctionStmt.
func (s *CreateFunctionStmt) Accept(v Visitor) {
	v.VisitCreateFunctionStmt(s)
}

// ---------- Extension Statements ----------

// InstallStmt represents an INSTALL extension_name statement.
type InstallStmt struct {
	Name string
}

func (*InstallStmt) stmtNode() {}

func (*InstallStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_LOAD }

// Accept implements the Visitor pattern for InstallStmt.
func (s *InstallStmt) Accept(v Visitor) {
	v.VisitInstallStmt(s)
}

// LoadStmt represents a LOAD extension_name statement.
type LoadStmt struct {
	Name string
}

func (*LoadStmt) stmtNode() {}

func (*LoadStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_LOAD }

// Accept implements the Visitor pattern for LoadStmt.
func (s *LoadStmt) Accept(v Visitor) {
	v.VisitLoadStmt(s)
}

// ---------- Database Management Statements ----------

// AttachStmt represents ATTACH [DATABASE] 'path' [AS alias] [(options)].
type AttachStmt struct {
	Path     string
	Alias    string
	ReadOnly bool
	Options  map[string]string
}

func (*AttachStmt) stmtNode() {}

func (*AttachStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ATTACH }

// Accept implements the Visitor pattern for AttachStmt.
func (s *AttachStmt) Accept(v Visitor) {
	v.VisitAttachStmt(s)
}

// DetachStmt represents DETACH [DATABASE] [IF EXISTS] name.
type DetachStmt struct {
	Name     string
	IfExists bool
}

func (*DetachStmt) stmtNode() {}

func (*DetachStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DETACH }

// Accept implements the Visitor pattern for DetachStmt.
func (s *DetachStmt) Accept(v Visitor) {
	v.VisitDetachStmt(s)
}

// UseStmt represents USE database[.schema].
type UseStmt struct {
	Database string
	Schema   string
}

func (*UseStmt) stmtNode() {}

func (*UseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SET }

// Accept implements the Visitor pattern for UseStmt.
func (s *UseStmt) Accept(v Visitor) {
	v.VisitUseStmt(s)
}

// CreateDatabaseStmt represents CREATE DATABASE [IF NOT EXISTS] name.
type CreateDatabaseStmt struct {
	Name        string
	IfNotExists bool
}

func (*CreateDatabaseStmt) stmtNode() {}

func (*CreateDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// Accept implements the Visitor pattern for CreateDatabaseStmt.
func (s *CreateDatabaseStmt) Accept(v Visitor) {
	v.VisitCreateDatabaseStmt(s)
}

// DropDatabaseStmt represents DROP DATABASE [IF EXISTS] name.
type DropDatabaseStmt struct {
	Name     string
	IfExists bool
}

func (*DropDatabaseStmt) stmtNode() {}

func (*DropDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// Accept implements the Visitor pattern for DropDatabaseStmt.
func (s *DropDatabaseStmt) Accept(v Visitor) {
	v.VisitDropDatabaseStmt(s)
}

// ---------- SET/SHOW Statements ----------

// SetStmt represents a SET statement for session configuration.
// Supports:
//   - SET default_transaction_isolation = 'level'
//   - SET variable = value
type SetStmt struct {
	// Variable is the name of the configuration variable to set.
	// For isolation levels: "default_transaction_isolation" or "transaction_isolation"
	Variable string
	// Value is the string value to set.
	Value string
}

func (*SetStmt) stmtNode() {}

func (*SetStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SET }

// Accept implements the Visitor pattern for SetStmt.
func (s *SetStmt) Accept(v Visitor) {
	v.VisitSetStmt(s)
}

// ShowStmt represents a SHOW statement for querying session configuration.
// Supports:
//   - SHOW transaction_isolation
//   - SHOW variable
type ShowStmt struct {
	// Variable is the name of the configuration variable to show.
	// For isolation levels: "transaction_isolation" or "default_transaction_isolation"
	Variable string
}

func (*ShowStmt) stmtNode() {}

func (*ShowStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }

// Accept implements the Visitor pattern for ShowStmt.
func (s *ShowStmt) Accept(v Visitor) {
	v.VisitShowStmt(s)
}
