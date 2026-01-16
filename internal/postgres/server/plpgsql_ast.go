// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// This file defines the AST (Abstract Syntax Tree) types for PL/pgSQL parsing.
// These types represent the structure of PL/pgSQL blocks, statements, and expressions.

package server

// PLpgSQLNodeType identifies the type of PL/pgSQL AST node.
type PLpgSQLNodeType int

const (
	// Block types
	PLpgSQLNodeBlock PLpgSQLNodeType = iota
	PLpgSQLNodeDeclare
	PLpgSQLNodeException

	// Statement types
	PLpgSQLNodeAssign
	PLpgSQLNodeReturn
	PLpgSQLNodeReturnQuery
	PLpgSQLNodeRaise
	PLpgSQLNodeExecute
	PLpgSQLNodeIf
	PLpgSQLNodeCase
	PLpgSQLNodeLoop
	PLpgSQLNodeWhile
	PLpgSQLNodeFor
	PLpgSQLNodeForQuery
	PLpgSQLNodeForeach
	PLpgSQLNodeExit
	PLpgSQLNodeContinue
	PLpgSQLNodePerform
	PLpgSQLNodeGetDiagnostics
	PLpgSQLNodeNull
	PLpgSQLNodeOpen
	PLpgSQLNodeFetch
	PLpgSQLNodeClose
	PLpgSQLNodeCall
)

// PLpgSQLRaiseLevel indicates the severity level of a RAISE statement.
type PLpgSQLRaiseLevel int

const (
	RaiseLevelDebug PLpgSQLRaiseLevel = iota
	RaiseLevelLog
	RaiseLevelInfo
	RaiseLevelNotice
	RaiseLevelWarning
	RaiseLevelException
)

// String returns the string representation of the raise level.
func (l PLpgSQLRaiseLevel) String() string {
	switch l {
	case RaiseLevelDebug:
		return "DEBUG"
	case RaiseLevelLog:
		return "LOG"
	case RaiseLevelInfo:
		return "INFO"
	case RaiseLevelNotice:
		return "NOTICE"
	case RaiseLevelWarning:
		return "WARNING"
	case RaiseLevelException:
		return "EXCEPTION"
	default:
		return "UNKNOWN"
	}
}

// PLpgSQLNode is the interface for all PL/pgSQL AST nodes.
type PLpgSQLNode interface {
	nodeType() PLpgSQLNodeType
}

// PLpgSQLExpr represents an expression in PL/pgSQL.
// Expressions can be SQL expressions, variable references, or literals.
type PLpgSQLExpr struct {
	// Raw is the raw SQL expression text.
	Raw string

	// IsVariable indicates if this is a simple variable reference.
	IsVariable bool

	// VarName is the variable name if IsVariable is true.
	VarName string
}

// PLpgSQLVarDecl represents a variable declaration in the DECLARE block.
type PLpgSQLVarDecl struct {
	// Name is the variable name.
	Name string

	// DataType is the data type as a string.
	DataType string

	// NotNull indicates if the variable cannot be NULL.
	NotNull bool

	// Constant indicates if this is a constant (cannot be reassigned).
	Constant bool

	// Default is the default value expression (may be nil).
	Default *PLpgSQLExpr

	// Collate is the collation (may be empty).
	Collate string

	// RowType indicates this is a table%ROWTYPE declaration.
	RowType bool

	// RowTypeTable is the table name for %ROWTYPE.
	RowTypeTable string

	// CursorFor indicates this is a cursor declaration.
	CursorFor bool

	// CursorQuery is the query for a cursor declaration.
	CursorQuery string
}

// PLpgSQLDeclareBlock represents the DECLARE section of a block.
type PLpgSQLDeclareBlock struct {
	Declarations []*PLpgSQLVarDecl
}

func (b *PLpgSQLDeclareBlock) nodeType() PLpgSQLNodeType { return PLpgSQLNodeDeclare }

// PLpgSQLBlockStmt represents a complete PL/pgSQL block.
// This is the top-level AST node for a function body.
type PLpgSQLBlockStmt struct {
	// Label is the optional block label (may be empty).
	Label string

	// Declare is the optional DECLARE section.
	Declare *PLpgSQLDeclareBlock

	// Body contains the statements in the BEGIN...END block.
	Body []PLpgSQLStmt

	// Exception contains the EXCEPTION handlers (may be nil).
	Exception *PLpgSQLExceptionBlock
}

func (b *PLpgSQLBlockStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeBlock }
func (b *PLpgSQLBlockStmt) plpgsqlStatement()         {}

// PLpgSQLExceptionBlock represents the EXCEPTION section of a block.
type PLpgSQLExceptionBlock struct {
	Handlers []*PLpgSQLExcHandler
}

func (b *PLpgSQLExceptionBlock) nodeType() PLpgSQLNodeType { return PLpgSQLNodeException }

// PLpgSQLExcHandler represents an exception handler in the EXCEPTION block.
type PLpgSQLExcHandler struct {
	// Conditions lists the exception conditions this handler catches.
	// e.g., "division_by_zero", "unique_violation", "OTHERS", "SQLSTATE '22012'"
	Conditions []string

	// Statements contains the statements to execute when the exception is caught.
	Statements []PLpgSQLStmt
}

// PLpgSQLStmt is the interface for all PL/pgSQL statements.
type PLpgSQLStmt interface {
	PLpgSQLNode
	plpgsqlStatement()
}

// PLpgSQLAssignStmt represents an assignment statement: variable := expression
type PLpgSQLAssignStmt struct {
	// Variable is the target variable name.
	Variable string

	// Expr is the expression to assign.
	Expr *PLpgSQLExpr
}

func (s *PLpgSQLAssignStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeAssign }
func (s *PLpgSQLAssignStmt) plpgsqlStatement()         {}

// PLpgSQLReturnStmt represents a RETURN statement.
type PLpgSQLReturnStmt struct {
	// Expr is the expression to return (may be nil for RETURN with no value).
	Expr *PLpgSQLExpr

	// IsReturnNext indicates RETURN NEXT for set-returning functions.
	IsReturnNext bool

	// IsReturnQuery indicates RETURN QUERY.
	IsReturnQuery bool

	// Query is the query for RETURN QUERY.
	Query string
}

func (s *PLpgSQLReturnStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeReturn }
func (s *PLpgSQLReturnStmt) plpgsqlStatement()         {}

// PLpgSQLRaiseStmt represents a RAISE statement for messages and exceptions.
type PLpgSQLRaiseStmt struct {
	// Level is the severity level (DEBUG, LOG, INFO, NOTICE, WARNING, EXCEPTION).
	Level PLpgSQLRaiseLevel

	// Message is the message format string.
	Message string

	// Params are the parameters to substitute in the message (%).
	Params []*PLpgSQLExpr

	// Options contains USING options (ERRCODE, HINT, DETAIL, etc.).
	Options map[string]string
}

func (s *PLpgSQLRaiseStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeRaise }
func (s *PLpgSQLRaiseStmt) plpgsqlStatement()         {}

// PLpgSQLPerformStmt represents a PERFORM statement (execute query, discard results).
type PLpgSQLPerformStmt struct {
	// Query is the SQL query to execute.
	Query string
}

func (s *PLpgSQLPerformStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodePerform }
func (s *PLpgSQLPerformStmt) plpgsqlStatement()         {}

// PLpgSQLExecuteStmt represents EXECUTE for dynamic SQL.
type PLpgSQLExecuteStmt struct {
	// QueryExpr is the expression containing the SQL query.
	QueryExpr *PLpgSQLExpr

	// Into lists the variables to store results into.
	Into []string

	// Strict indicates if exactly one row is expected.
	Strict bool

	// UsingParams are the parameters for USING clause.
	UsingParams []*PLpgSQLExpr
}

func (s *PLpgSQLExecuteStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeExecute }
func (s *PLpgSQLExecuteStmt) plpgsqlStatement()         {}

// PLpgSQLIfStmt represents an IF...THEN...ELSIF...ELSE...END IF statement.
type PLpgSQLIfStmt struct {
	// Condition is the IF condition expression.
	Condition *PLpgSQLExpr

	// ThenBody contains the statements for the THEN clause.
	ThenBody []PLpgSQLStmt

	// ElsifClauses contains the ELSIF clauses.
	ElsifClauses []*PLpgSQLElsifClause

	// ElseBody contains the statements for the ELSE clause (may be nil).
	ElseBody []PLpgSQLStmt
}

func (s *PLpgSQLIfStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeIf }
func (s *PLpgSQLIfStmt) plpgsqlStatement()         {}

// PLpgSQLElsifClause represents an ELSIF clause in an IF statement.
type PLpgSQLElsifClause struct {
	Condition *PLpgSQLExpr
	Body      []PLpgSQLStmt
}

// PLpgSQLCaseStmt represents a CASE statement.
type PLpgSQLCaseStmt struct {
	// Expr is the expression to compare (may be nil for searched CASE).
	Expr *PLpgSQLExpr

	// WhenClauses contains the WHEN clauses.
	WhenClauses []*PLpgSQLWhenClause

	// ElseBody contains the statements for ELSE (may be nil).
	ElseBody []PLpgSQLStmt
}

func (s *PLpgSQLCaseStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeCase }
func (s *PLpgSQLCaseStmt) plpgsqlStatement()         {}

// PLpgSQLWhenClause represents a WHEN clause in a CASE statement.
type PLpgSQLWhenClause struct {
	Exprs []*PLpgSQLExpr
	Body  []PLpgSQLStmt
}

// PLpgSQLLoopStmt represents a simple LOOP...END LOOP statement.
type PLpgSQLLoopStmt struct {
	// Label is the optional loop label.
	Label string

	// Body contains the loop body statements.
	Body []PLpgSQLStmt
}

func (s *PLpgSQLLoopStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeLoop }
func (s *PLpgSQLLoopStmt) plpgsqlStatement()         {}

// PLpgSQLWhileStmt represents a WHILE...LOOP...END LOOP statement.
type PLpgSQLWhileStmt struct {
	// Label is the optional loop label.
	Label string

	// Condition is the WHILE condition expression.
	Condition *PLpgSQLExpr

	// Body contains the loop body statements.
	Body []PLpgSQLStmt
}

func (s *PLpgSQLWhileStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeWhile }
func (s *PLpgSQLWhileStmt) plpgsqlStatement()         {}

// PLpgSQLForStmt represents a FOR...IN...LOOP...END LOOP statement (numeric range).
type PLpgSQLForStmt struct {
	// Label is the optional loop label.
	Label string

	// Variable is the loop variable name.
	Variable string

	// Reverse indicates if the loop is in reverse.
	Reverse bool

	// LowerBound is the lower bound expression.
	LowerBound *PLpgSQLExpr

	// UpperBound is the upper bound expression.
	UpperBound *PLpgSQLExpr

	// Step is the step expression (defaults to 1).
	Step *PLpgSQLExpr

	// Body contains the loop body statements.
	Body []PLpgSQLStmt
}

func (s *PLpgSQLForStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeFor }
func (s *PLpgSQLForStmt) plpgsqlStatement()         {}

// PLpgSQLForQueryStmt represents a FOR...IN (query) LOOP...END LOOP statement.
type PLpgSQLForQueryStmt struct {
	// Label is the optional loop label.
	Label string

	// Variable is the record/row variable name.
	Variable string

	// Query is the SQL query to iterate over.
	Query string

	// Body contains the loop body statements.
	Body []PLpgSQLStmt
}

func (s *PLpgSQLForQueryStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeForQuery }
func (s *PLpgSQLForQueryStmt) plpgsqlStatement()         {}

// PLpgSQLForeachStmt represents a FOREACH...IN ARRAY...LOOP...END LOOP statement.
type PLpgSQLForeachStmt struct {
	// Label is the optional loop label.
	Label string

	// Variable is the element variable name.
	Variable string

	// Slice is the slice dimension (for multi-dimensional arrays).
	Slice int

	// ArrayExpr is the array expression to iterate.
	ArrayExpr *PLpgSQLExpr

	// Body contains the loop body statements.
	Body []PLpgSQLStmt
}

func (s *PLpgSQLForeachStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeForeach }
func (s *PLpgSQLForeachStmt) plpgsqlStatement()         {}

// PLpgSQLExitStmt represents an EXIT statement.
type PLpgSQLExitStmt struct {
	// Label is the optional label to exit to.
	Label string

	// Condition is the optional WHEN condition.
	Condition *PLpgSQLExpr
}

func (s *PLpgSQLExitStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeExit }
func (s *PLpgSQLExitStmt) plpgsqlStatement()         {}

// PLpgSQLContinueStmt represents a CONTINUE statement.
type PLpgSQLContinueStmt struct {
	// Label is the optional label to continue.
	Label string

	// Condition is the optional WHEN condition.
	Condition *PLpgSQLExpr
}

func (s *PLpgSQLContinueStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeContinue }
func (s *PLpgSQLContinueStmt) plpgsqlStatement()         {}

// PLpgSQLGetDiagnosticsStmt represents a GET DIAGNOSTICS or GET STACKED DIAGNOSTICS statement.
type PLpgSQLGetDiagnosticsStmt struct {
	// Stacked indicates GET STACKED DIAGNOSTICS (for exception handlers).
	Stacked bool

	// Items contains the diagnostic items to retrieve.
	Items []*PLpgSQLDiagnosticsItem
}

func (s *PLpgSQLGetDiagnosticsStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeGetDiagnostics }
func (s *PLpgSQLGetDiagnosticsStmt) plpgsqlStatement()         {}

// PLpgSQLDiagnosticsItem represents a single item in GET DIAGNOSTICS.
type PLpgSQLDiagnosticsItem struct {
	// Variable is the target variable name.
	Variable string

	// Kind is the diagnostic item kind (e.g., "ROW_COUNT", "SQLSTATE", "MESSAGE_TEXT").
	Kind string
}

// PLpgSQLNullStmt represents a NULL statement (no-op).
type PLpgSQLNullStmt struct{}

func (s *PLpgSQLNullStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeNull }
func (s *PLpgSQLNullStmt) plpgsqlStatement()         {}

// PLpgSQLOpenStmt represents an OPEN cursor statement.
type PLpgSQLOpenStmt struct {
	// CursorVar is the cursor variable name.
	CursorVar string

	// Bound indicates if this is for a bound cursor.
	Bound bool

	// Query is the query for unbound cursors.
	Query string

	// Arguments are the arguments for bound cursors.
	Arguments []*PLpgSQLExpr
}

func (s *PLpgSQLOpenStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeOpen }
func (s *PLpgSQLOpenStmt) plpgsqlStatement()         {}

// PLpgSQLFetchStmt represents a FETCH cursor statement.
type PLpgSQLFetchStmt struct {
	// CursorVar is the cursor variable name.
	CursorVar string

	// Direction is the fetch direction (NEXT, PRIOR, FIRST, LAST, etc.).
	Direction string

	// Count is the number of rows to fetch.
	Count int

	// Into lists the target variables.
	Into []string
}

func (s *PLpgSQLFetchStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeFetch }
func (s *PLpgSQLFetchStmt) plpgsqlStatement()         {}

// PLpgSQLCloseStmt represents a CLOSE cursor statement.
type PLpgSQLCloseStmt struct {
	// CursorVar is the cursor variable name.
	CursorVar string
}

func (s *PLpgSQLCloseStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeClose }
func (s *PLpgSQLCloseStmt) plpgsqlStatement()         {}

// PLpgSQLCallStmt represents a CALL statement for procedures.
type PLpgSQLCallStmt struct {
	// ProcName is the procedure name.
	ProcName string

	// Schema is the schema name (may be empty).
	Schema string

	// Arguments are the procedure arguments.
	Arguments []*PLpgSQLExpr

	// Into lists the target variables for OUT parameters.
	Into []string
}

func (s *PLpgSQLCallStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeCall }
func (s *PLpgSQLCallStmt) plpgsqlStatement()         {}

// PLpgSQLSQLStmt represents an embedded SQL statement to execute.
type PLpgSQLSQLStmt struct {
	// SQL is the raw SQL text.
	SQL string

	// Into lists the target variables for SELECT INTO.
	Into []string

	// Strict indicates if exactly one row is expected.
	Strict bool
}

func (s *PLpgSQLSQLStmt) nodeType() PLpgSQLNodeType { return PLpgSQLNodeExecute }
func (s *PLpgSQLSQLStmt) plpgsqlStatement()         {}
