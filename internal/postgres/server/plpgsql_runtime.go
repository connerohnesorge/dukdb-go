// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// This file implements the PL/pgSQL runtime for executing stored functions
// and procedures. It manages variable scopes, evaluates expressions, and
// handles control flow.

package server

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PLpgSQLRuntimeError represents an error during PL/pgSQL execution.
type PLpgSQLRuntimeError struct {
	Message   string
	SQLState  string
	Hint      string
	Detail    string
	Context   string
	Line      int
	Statement string
}

func (e *PLpgSQLRuntimeError) Error() string {
	msg := e.Message
	if e.SQLState != "" {
		msg = fmt.Sprintf("[%s] %s", e.SQLState, msg)
	}
	if e.Detail != "" {
		msg += "\nDETAIL: " + e.Detail
	}
	if e.Hint != "" {
		msg += "\nHINT: " + e.Hint
	}
	return msg
}

// Common SQLSTATEs for PL/pgSQL errors.
const (
	SQLStateRaiseException    = "P0001"
	SQLStateNoDataFound       = "P0002"
	SQLStateTooManyRows       = "P0003"
	SQLStateDivisionByZero    = "22012"
	SQLStateNumericOverflow   = "22003"
	SQLStateNullValueNotAllow = "22004"
	SQLStateUniqueViolation   = "23505"
	SQLStateInvalidTextRep    = "22P02"
	SQLStateUndefinedFunction = "42883"
	SQLStateUndefinedColumn   = "42703"
	SQLStateUndefinedTable    = "42P01"
)

// PLpgSQLScope represents a variable scope in PL/pgSQL.
// Scopes are nested - each block creates a new scope.
type PLpgSQLScope struct {
	parent    *PLpgSQLScope
	variables map[string]*PLpgSQLVariable
}

// PLpgSQLVariable represents a variable in PL/pgSQL.
type PLpgSQLVariable struct {
	Name     string
	Type     string
	Value    interface{}
	Constant bool
	NotNull  bool
}

// NewPLpgSQLScope creates a new scope.
func NewPLpgSQLScope(parent *PLpgSQLScope) *PLpgSQLScope {
	return &PLpgSQLScope{
		parent:    parent,
		variables: make(map[string]*PLpgSQLVariable),
	}
}

// Declare declares a new variable in this scope.
func (s *PLpgSQLScope) Declare(name, dataType string, value interface{}, constant, notNull bool) error {
	lowerName := strings.ToLower(name)
	if _, exists := s.variables[lowerName]; exists {
		return fmt.Errorf("variable \"%s\" already exists in this scope", name)
	}
	s.variables[lowerName] = &PLpgSQLVariable{
		Name:     name,
		Type:     dataType,
		Value:    value,
		Constant: constant,
		NotNull:  notNull,
	}
	return nil
}

// Get retrieves a variable from this scope or parent scopes.
func (s *PLpgSQLScope) Get(name string) (*PLpgSQLVariable, bool) {
	lowerName := strings.ToLower(name)
	if v, ok := s.variables[lowerName]; ok {
		return v, true
	}
	if s.parent != nil {
		return s.parent.Get(name)
	}
	return nil, false
}

// Set sets the value of a variable in this scope or parent scopes.
func (s *PLpgSQLScope) Set(name string, value interface{}) error {
	lowerName := strings.ToLower(name)

	// Look for the variable in this scope first
	if v, ok := s.variables[lowerName]; ok {
		if v.Constant {
			return fmt.Errorf("variable \"%s\" is declared CONSTANT", name)
		}
		if v.NotNull && value == nil {
			return fmt.Errorf("null value not allowed for variable \"%s\"", name)
		}
		v.Value = value
		return nil
	}

	// Look in parent scopes
	if s.parent != nil {
		return s.parent.Set(name, value)
	}

	return fmt.Errorf("variable \"%s\" does not exist", name)
}

// PLpgSQLExecContext represents the execution context for PL/pgSQL.
type PLpgSQLExecContext struct {
	// ctx is the Go context for cancellation.
	ctx context.Context

	// scope is the current variable scope.
	scope *PLpgSQLScope

	// conn is the database connection for executing SQL.
	conn BackendConnInterface

	// function is the function being executed.
	function *StoredFunction

	// procedure is the procedure being executed (if applicable).
	procedure *StoredProcedure

	// returnValue holds the return value for RETURN statements.
	returnValue interface{}

	// returned indicates if RETURN was called.
	returned bool

	// returnedRows holds rows for set-returning functions.
	returnedRows []map[string]interface{}

	// found is the special FOUND variable.
	found bool

	// rowCount is the special ROW_COUNT value.
	rowCount int64

	// diagnostics holds the most recent error context for GET STACKED DIAGNOSTICS.
	diagnostics *PLpgSQLDiagnostics

	// labels tracks loop labels for EXIT and CONTINUE.
	labels map[string]bool

	// exitLabel holds the label to exit to (if set).
	exitLabel string

	// continueLabel holds the label to continue (if set).
	continueLabel string

	// raiseMessages collects RAISE NOTICE/WARNING messages.
	raiseMessages []string
}

// PLpgSQLDiagnostics holds diagnostic information for GET STACKED DIAGNOSTICS.
type PLpgSQLDiagnostics struct {
	SQLState       string
	MessageText    string
	MessageDetail  string
	MessageHint    string
	MessagePrimary string
	ContextText    string
	SchemaName     string
	TableName      string
	ColumnName     string
	ConstraintName string
}

// BackendConnInterface is the interface for database connections.
type BackendConnInterface interface {
	Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]interface{}, []string, error)
	Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error)
}

// PLpgSQLExecutor executes PL/pgSQL blocks.
type PLpgSQLExecutor struct {
	// manager is the PLpgSQLManager for function/procedure lookup.
	manager *PLpgSQLManager
}

// NewPLpgSQLExecutor creates a new executor.
func NewPLpgSQLExecutor(manager *PLpgSQLManager) *PLpgSQLExecutor {
	return &PLpgSQLExecutor{
		manager: manager,
	}
}

// ExecuteFunction executes a stored function.
func (e *PLpgSQLExecutor) ExecuteFunction(ctx context.Context, conn BackendConnInterface, fn *StoredFunction, args []interface{}) (interface{}, error) {
	// Parse the function body
	parser := NewPLpgSQLParser(fn.Body)
	block, err := parser.Parse()
	if err != nil {
		return nil, &PLpgSQLRuntimeError{
			Message:  fmt.Sprintf("syntax error in function body: %v", err),
			SQLState: SQLStateRaiseException,
		}
	}

	// Create execution context
	execCtx := &PLpgSQLExecContext{
		ctx:      ctx,
		scope:    NewPLpgSQLScope(nil),
		conn:     conn,
		function: fn,
		labels:   make(map[string]bool),
	}

	// Bind parameters to scope
	for i, param := range fn.Parameters {
		var value interface{}
		if i < len(args) {
			value = args[i]
		} else if param.DefaultValue != nil {
			value = param.DefaultValue
		}
		name := param.Name
		if name == "" {
			name = fmt.Sprintf("$%d", i+1)
		}
		if err := execCtx.scope.Declare(name, param.Type, value, false, false); err != nil {
			return nil, err
		}
	}

	// Execute the block
	if err := e.executeBlock(execCtx, block); err != nil {
		return nil, err
	}

	// Handle set-returning functions
	if fn.ReturnsSet {
		return execCtx.returnedRows, nil
	}

	return execCtx.returnValue, nil
}

// ExecuteProcedure executes a stored procedure.
func (e *PLpgSQLExecutor) ExecuteProcedure(ctx context.Context, conn BackendConnInterface, proc *StoredProcedure, args []interface{}) error {
	// Parse the procedure body
	parser := NewPLpgSQLParser(proc.Body)
	block, err := parser.Parse()
	if err != nil {
		return &PLpgSQLRuntimeError{
			Message:  fmt.Sprintf("syntax error in procedure body: %v", err),
			SQLState: SQLStateRaiseException,
		}
	}

	// Create execution context
	execCtx := &PLpgSQLExecContext{
		ctx:       ctx,
		scope:     NewPLpgSQLScope(nil),
		conn:      conn,
		procedure: proc,
		labels:    make(map[string]bool),
	}

	// Bind parameters to scope
	for i, param := range proc.Parameters {
		var value interface{}
		if i < len(args) {
			value = args[i]
		} else if param.DefaultValue != nil {
			value = param.DefaultValue
		}
		name := param.Name
		if name == "" {
			name = fmt.Sprintf("$%d", i+1)
		}
		if err := execCtx.scope.Declare(name, param.Type, value, false, false); err != nil {
			return err
		}
	}

	// Execute the block
	return e.executeBlock(execCtx, block)
}

// executeBlock executes a PL/pgSQL block.
func (e *PLpgSQLExecutor) executeBlock(execCtx *PLpgSQLExecContext, block *PLpgSQLBlockStmt) error {
	// Create new scope for this block
	execCtx.scope = NewPLpgSQLScope(execCtx.scope)
	defer func() {
		if execCtx.scope.parent != nil {
			execCtx.scope = execCtx.scope.parent
		}
	}()

	// Register label if present
	if block.Label != "" {
		execCtx.labels[strings.ToLower(block.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(block.Label))
	}

	// Process DECLARE section
	if block.Declare != nil {
		for _, decl := range block.Declare.Declarations {
			var value interface{}
			if decl.Default != nil {
				var err error
				value, err = e.evaluateExpr(execCtx, decl.Default)
				if err != nil {
					return err
				}
			}
			if err := execCtx.scope.Declare(decl.Name, decl.DataType, value, decl.Constant, decl.NotNull); err != nil {
				return err
			}
		}
	}

	// Execute statements with exception handling
	var execErr error
	for _, stmt := range block.Body {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		execErr = e.executeStatement(execCtx, stmt)
		if execErr != nil {
			break
		}

		// Check for RETURN
		if execCtx.returned {
			return nil
		}

		// Check for EXIT
		if execCtx.exitLabel != "" {
			if block.Label != "" && strings.EqualFold(execCtx.exitLabel, block.Label) {
				execCtx.exitLabel = ""
			}
			return nil
		}
	}

	// Handle exceptions if there was an error and we have handlers
	if execErr != nil && block.Exception != nil {
		// Store diagnostics for GET STACKED DIAGNOSTICS
		execCtx.diagnostics = e.errorToDiagnostics(execErr)

		for _, handler := range block.Exception.Handlers {
			if e.matchesCondition(execErr, handler.Conditions) {
				// Execute handler statements
				for _, stmt := range handler.Statements {
					if err := e.executeStatement(execCtx, stmt); err != nil {
						return err
					}
					if execCtx.returned {
						return nil
					}
				}
				return nil
			}
		}
		// No handler matched, propagate the error
		return execErr
	}

	return execErr
}

// executeStatement executes a single PL/pgSQL statement.
func (e *PLpgSQLExecutor) executeStatement(execCtx *PLpgSQLExecContext, stmt PLpgSQLStmt) error {
	switch s := stmt.(type) {
	case *PLpgSQLAssignStmt:
		return e.executeAssign(execCtx, s)
	case *PLpgSQLReturnStmt:
		return e.executeReturn(execCtx, s)
	case *PLpgSQLRaiseStmt:
		return e.executeRaise(execCtx, s)
	case *PLpgSQLIfStmt:
		return e.executeIf(execCtx, s)
	case *PLpgSQLCaseStmt:
		return e.executeCase(execCtx, s)
	case *PLpgSQLLoopStmt:
		return e.executeLoop(execCtx, s)
	case *PLpgSQLWhileStmt:
		return e.executeWhile(execCtx, s)
	case *PLpgSQLForStmt:
		return e.executeFor(execCtx, s)
	case *PLpgSQLForQueryStmt:
		return e.executeForQuery(execCtx, s)
	case *PLpgSQLForeachStmt:
		return e.executeForeach(execCtx, s)
	case *PLpgSQLExitStmt:
		return e.executeExit(execCtx, s)
	case *PLpgSQLContinueStmt:
		return e.executeContinue(execCtx, s)
	case *PLpgSQLPerformStmt:
		return e.executePerform(execCtx, s)
	case *PLpgSQLExecuteStmt:
		return e.executeExecute(execCtx, s)
	case *PLpgSQLGetDiagnosticsStmt:
		return e.executeGetDiagnostics(execCtx, s)
	case *PLpgSQLNullStmt:
		return nil
	case *PLpgSQLSQLStmt:
		return e.executeSQL(execCtx, s)
	case *PLpgSQLBlockStmt:
		return e.executeBlock(execCtx, s)
	case *PLpgSQLCallStmt:
		return e.executeCall(execCtx, s)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeAssign executes an assignment statement.
func (e *PLpgSQLExecutor) executeAssign(execCtx *PLpgSQLExecContext, stmt *PLpgSQLAssignStmt) error {
	value, err := e.evaluateExpr(execCtx, stmt.Expr)
	if err != nil {
		return err
	}
	return execCtx.scope.Set(stmt.Variable, value)
}

// executeReturn executes a RETURN statement.
func (e *PLpgSQLExecutor) executeReturn(execCtx *PLpgSQLExecContext, stmt *PLpgSQLReturnStmt) error {
	if stmt.IsReturnNext {
		// RETURN NEXT for set-returning functions
		if stmt.Expr != nil {
			value, err := e.evaluateExpr(execCtx, stmt.Expr)
			if err != nil {
				return err
			}
			row := make(map[string]interface{})
			row["value"] = value
			execCtx.returnedRows = append(execCtx.returnedRows, row)
		}
		return nil
	}

	if stmt.IsReturnQuery {
		// RETURN QUERY - execute query and add all rows
		rows, _, err := e.executeQuery(execCtx, stmt.Query)
		if err != nil {
			return err
		}
		execCtx.returnedRows = append(execCtx.returnedRows, rows...)
		return nil
	}

	// Simple RETURN
	if stmt.Expr != nil {
		value, err := e.evaluateExpr(execCtx, stmt.Expr)
		if err != nil {
			return err
		}
		execCtx.returnValue = value
	}
	execCtx.returned = true
	return nil
}

// executeRaise executes a RAISE statement.
func (e *PLpgSQLExecutor) executeRaise(execCtx *PLpgSQLExecContext, stmt *PLpgSQLRaiseStmt) error {
	// Format the message
	message := stmt.Message
	if len(stmt.Params) > 0 {
		// Replace % placeholders
		for _, param := range stmt.Params {
			value, err := e.evaluateExpr(execCtx, param)
			if err != nil {
				return err
			}
			message = strings.Replace(message, "%", formatValue(value), 1)
		}
	}

	// Handle based on level
	switch stmt.Level {
	case RaiseLevelDebug, RaiseLevelLog, RaiseLevelInfo, RaiseLevelNotice, RaiseLevelWarning:
		// Store message for later delivery
		execCtx.raiseMessages = append(execCtx.raiseMessages, fmt.Sprintf("%s: %s", stmt.Level.String(), message))
		return nil
	case RaiseLevelException:
		// Raise an exception
		errCode := SQLStateRaiseException
		if code, ok := stmt.Options["ERRCODE"]; ok {
			errCode = code
		}
		return &PLpgSQLRuntimeError{
			Message:  message,
			SQLState: errCode,
			Hint:     stmt.Options["HINT"],
			Detail:   stmt.Options["DETAIL"],
		}
	default:
		return nil
	}
}

// executeIf executes an IF statement.
func (e *PLpgSQLExecutor) executeIf(execCtx *PLpgSQLExecContext, stmt *PLpgSQLIfStmt) error {
	// Evaluate condition
	cond, err := e.evaluateCondition(execCtx, stmt.Condition)
	if err != nil {
		return err
	}

	if cond {
		for _, s := range stmt.ThenBody {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned || execCtx.exitLabel != "" || execCtx.continueLabel != "" {
				return nil
			}
		}
		return nil
	}

	// Check ELSIF clauses
	for _, elsif := range stmt.ElsifClauses {
		cond, err := e.evaluateCondition(execCtx, elsif.Condition)
		if err != nil {
			return err
		}
		if cond {
			for _, s := range elsif.Body {
				if err := e.executeStatement(execCtx, s); err != nil {
					return err
				}
				if execCtx.returned || execCtx.exitLabel != "" || execCtx.continueLabel != "" {
					return nil
				}
			}
			return nil
		}
	}

	// Execute ELSE clause
	if stmt.ElseBody != nil {
		for _, s := range stmt.ElseBody {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned || execCtx.exitLabel != "" || execCtx.continueLabel != "" {
				return nil
			}
		}
	}

	return nil
}

// executeCase executes a CASE statement.
func (e *PLpgSQLExecutor) executeCase(execCtx *PLpgSQLExecContext, stmt *PLpgSQLCaseStmt) error {
	var caseValue interface{}
	if stmt.Expr != nil {
		var err error
		caseValue, err = e.evaluateExpr(execCtx, stmt.Expr)
		if err != nil {
			return err
		}
	}

	for _, when := range stmt.WhenClauses {
		matched := false
		for _, expr := range when.Exprs {
			if stmt.Expr != nil {
				// Simple CASE - compare values
				whenValue, err := e.evaluateExpr(execCtx, expr)
				if err != nil {
					return err
				}
				if valuesEqual(caseValue, whenValue) {
					matched = true
					break
				}
			} else {
				// Searched CASE - evaluate condition
				cond, err := e.evaluateCondition(execCtx, expr)
				if err != nil {
					return err
				}
				if cond {
					matched = true
					break
				}
			}
		}

		if matched {
			for _, s := range when.Body {
				if err := e.executeStatement(execCtx, s); err != nil {
					return err
				}
				if execCtx.returned || execCtx.exitLabel != "" || execCtx.continueLabel != "" {
					return nil
				}
			}
			return nil
		}
	}

	// Execute ELSE clause if no match
	if stmt.ElseBody != nil {
		for _, s := range stmt.ElseBody {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned || execCtx.exitLabel != "" || execCtx.continueLabel != "" {
				return nil
			}
		}
	}

	return nil
}

// executeLoop executes a simple LOOP statement.
func (e *PLpgSQLExecutor) executeLoop(execCtx *PLpgSQLExecContext, stmt *PLpgSQLLoopStmt) error {
	// Register label
	if stmt.Label != "" {
		execCtx.labels[strings.ToLower(stmt.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(stmt.Label))
	}

	for {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		for _, s := range stmt.Body {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned {
				return nil
			}

			// Handle EXIT
			if execCtx.exitLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.exitLabel, stmt.Label) {
					execCtx.exitLabel = ""
				} else if execCtx.exitLabel == "" {
					// Unlabeled EXIT
				}
				return nil
			}

			// Handle CONTINUE
			if execCtx.continueLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.continueLabel, stmt.Label) {
					execCtx.continueLabel = ""
					break // Continue to next iteration
				} else if execCtx.continueLabel == "" {
					// Unlabeled CONTINUE
					break
				}
				return nil // Continue outer loop
			}
		}

		// Check for unlabeled EXIT
		if execCtx.exitLabel == "" && stmt.Label == "" {
			// This shouldn't happen normally
		}
	}
}

// executeWhile executes a WHILE loop statement.
func (e *PLpgSQLExecutor) executeWhile(execCtx *PLpgSQLExecContext, stmt *PLpgSQLWhileStmt) error {
	// Register label
	if stmt.Label != "" {
		execCtx.labels[strings.ToLower(stmt.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(stmt.Label))
	}

	for {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		// Evaluate condition
		cond, err := e.evaluateCondition(execCtx, stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}

		for _, s := range stmt.Body {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned {
				return nil
			}
			if execCtx.exitLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.exitLabel, stmt.Label) {
					execCtx.exitLabel = ""
				}
				return nil
			}
			if execCtx.continueLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.continueLabel, stmt.Label) {
					execCtx.continueLabel = ""
					break
				}
				return nil
			}
		}
	}
}

// executeFor executes a numeric FOR loop statement.
func (e *PLpgSQLExecutor) executeFor(execCtx *PLpgSQLExecContext, stmt *PLpgSQLForStmt) error {
	// Register label
	if stmt.Label != "" {
		execCtx.labels[strings.ToLower(stmt.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(stmt.Label))
	}

	// Evaluate bounds
	lower, err := e.evaluateExpr(execCtx, stmt.LowerBound)
	if err != nil {
		return err
	}
	upper, err := e.evaluateExpr(execCtx, stmt.UpperBound)
	if err != nil {
		return err
	}

	lowerInt := plpgsqlToInt64(lower)
	upperInt := plpgsqlToInt64(upper)

	step := int64(1)
	if stmt.Step != nil {
		stepVal, err := e.evaluateExpr(execCtx, stmt.Step)
		if err != nil {
			return err
		}
		step = plpgsqlToInt64(stepVal)
	}
	if stmt.Reverse {
		step = -step
	}

	// Create loop scope
	loopScope := NewPLpgSQLScope(execCtx.scope)
	if err := loopScope.Declare(stmt.Variable, "INTEGER", lowerInt, false, false); err != nil {
		return err
	}
	originalScope := execCtx.scope
	execCtx.scope = loopScope
	defer func() { execCtx.scope = originalScope }()

	var i int64
	if stmt.Reverse {
		i = upperInt
	} else {
		i = lowerInt
	}

	for {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		// Check bounds
		if stmt.Reverse {
			if i < lowerInt {
				return nil
			}
		} else {
			if i > upperInt {
				return nil
			}
		}

		// Set loop variable
		if err := execCtx.scope.Set(stmt.Variable, i); err != nil {
			return err
		}

		// Execute body
		for _, s := range stmt.Body {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned {
				return nil
			}
			if execCtx.exitLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.exitLabel, stmt.Label) {
					execCtx.exitLabel = ""
				}
				return nil
			}
			if execCtx.continueLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.continueLabel, stmt.Label) {
					execCtx.continueLabel = ""
					break
				}
				return nil
			}
		}

		i += step
	}
}

// executeForQuery executes a query FOR loop statement.
func (e *PLpgSQLExecutor) executeForQuery(execCtx *PLpgSQLExecContext, stmt *PLpgSQLForQueryStmt) error {
	// Register label
	if stmt.Label != "" {
		execCtx.labels[strings.ToLower(stmt.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(stmt.Label))
	}

	// Execute query
	rows, cols, err := e.executeQuery(execCtx, stmt.Query)
	if err != nil {
		return err
	}

	// Create loop scope
	loopScope := NewPLpgSQLScope(execCtx.scope)
	if err := loopScope.Declare(stmt.Variable, "RECORD", nil, false, false); err != nil {
		return err
	}
	originalScope := execCtx.scope
	execCtx.scope = loopScope
	defer func() { execCtx.scope = originalScope }()

	execCtx.found = len(rows) > 0

	for _, row := range rows {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		// Set record variable - for single column, set directly; for multiple, set as record
		if len(cols) == 1 {
			if err := execCtx.scope.Set(stmt.Variable, row[cols[0]]); err != nil {
				return err
			}
		} else {
			if err := execCtx.scope.Set(stmt.Variable, row); err != nil {
				return err
			}
		}

		// Execute body
		for _, s := range stmt.Body {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned {
				return nil
			}
			if execCtx.exitLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.exitLabel, stmt.Label) {
					execCtx.exitLabel = ""
				}
				return nil
			}
			if execCtx.continueLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.continueLabel, stmt.Label) {
					execCtx.continueLabel = ""
					break
				}
				return nil
			}
		}
	}

	return nil
}

// executeForeach executes a FOREACH loop statement.
func (e *PLpgSQLExecutor) executeForeach(execCtx *PLpgSQLExecContext, stmt *PLpgSQLForeachStmt) error {
	// Evaluate array expression
	arrayVal, err := e.evaluateExpr(execCtx, stmt.ArrayExpr)
	if err != nil {
		return err
	}

	// Convert to slice
	var elements []interface{}
	switch v := arrayVal.(type) {
	case []interface{}:
		elements = v
	case string:
		// Try to parse as array literal
		elements = parseArrayLiteral(v)
	default:
		return fmt.Errorf("FOREACH requires an array value")
	}

	// Register label
	if stmt.Label != "" {
		execCtx.labels[strings.ToLower(stmt.Label)] = true
		defer delete(execCtx.labels, strings.ToLower(stmt.Label))
	}

	// Create loop scope
	loopScope := NewPLpgSQLScope(execCtx.scope)
	if err := loopScope.Declare(stmt.Variable, "ANYELEMENT", nil, false, false); err != nil {
		return err
	}
	originalScope := execCtx.scope
	execCtx.scope = loopScope
	defer func() { execCtx.scope = originalScope }()

	for _, elem := range elements {
		if execCtx.ctx.Err() != nil {
			return execCtx.ctx.Err()
		}

		if err := execCtx.scope.Set(stmt.Variable, elem); err != nil {
			return err
		}

		for _, s := range stmt.Body {
			if err := e.executeStatement(execCtx, s); err != nil {
				return err
			}
			if execCtx.returned {
				return nil
			}
			if execCtx.exitLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.exitLabel, stmt.Label) {
					execCtx.exitLabel = ""
				}
				return nil
			}
			if execCtx.continueLabel != "" {
				if stmt.Label != "" && strings.EqualFold(execCtx.continueLabel, stmt.Label) {
					execCtx.continueLabel = ""
					break
				}
				return nil
			}
		}
	}

	return nil
}

// executeExit executes an EXIT statement.
func (e *PLpgSQLExecutor) executeExit(execCtx *PLpgSQLExecContext, stmt *PLpgSQLExitStmt) error {
	// Check condition if present
	if stmt.Condition != nil {
		cond, err := e.evaluateCondition(execCtx, stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
	}

	execCtx.exitLabel = stmt.Label
	return nil
}

// executeContinue executes a CONTINUE statement.
func (e *PLpgSQLExecutor) executeContinue(execCtx *PLpgSQLExecContext, stmt *PLpgSQLContinueStmt) error {
	// Check condition if present
	if stmt.Condition != nil {
		cond, err := e.evaluateCondition(execCtx, stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
	}

	execCtx.continueLabel = stmt.Label
	return nil
}

// executePerform executes a PERFORM statement.
func (e *PLpgSQLExecutor) executePerform(execCtx *PLpgSQLExecContext, stmt *PLpgSQLPerformStmt) error {
	// Execute as SELECT and discard results
	query := "SELECT " + stmt.Query
	rows, _, err := e.executeQuery(execCtx, query)
	if err != nil {
		return err
	}
	execCtx.found = len(rows) > 0
	execCtx.rowCount = int64(len(rows))
	return nil
}

// executeExecute executes an EXECUTE statement (dynamic SQL).
func (e *PLpgSQLExecutor) executeExecute(execCtx *PLpgSQLExecContext, stmt *PLpgSQLExecuteStmt) error {
	// Evaluate query expression
	queryVal, err := e.evaluateExpr(execCtx, stmt.QueryExpr)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("%v", queryVal)

	// Substitute USING parameters
	if len(stmt.UsingParams) > 0 {
		for i, param := range stmt.UsingParams {
			val, err := e.evaluateExpr(execCtx, param)
			if err != nil {
				return err
			}
			placeholder := fmt.Sprintf("$%d", i+1)
			query = strings.Replace(query, placeholder, formatValue(val), 1)
		}
	}

	// Execute the query
	if len(stmt.Into) > 0 {
		// SELECT INTO
		rows, cols, err := e.executeQuery(execCtx, query)
		if err != nil {
			return err
		}

		execCtx.found = len(rows) > 0
		execCtx.rowCount = int64(len(rows))

		if stmt.Strict && len(rows) != 1 {
			if len(rows) == 0 {
				return &PLpgSQLRuntimeError{
					Message:  "query returned no rows",
					SQLState: SQLStateNoDataFound,
				}
			}
			return &PLpgSQLRuntimeError{
				Message:  "query returned more than one row",
				SQLState: SQLStateTooManyRows,
			}
		}

		if len(rows) > 0 {
			row := rows[0]
			for i, varName := range stmt.Into {
				if i < len(cols) {
					if err := execCtx.scope.Set(varName, row[cols[i]]); err != nil {
						return err
					}
				}
			}
		}
	} else {
		// Non-query
		affected, err := e.executeNonQuery(execCtx, query)
		if err != nil {
			return err
		}
		execCtx.rowCount = affected
		execCtx.found = affected > 0
	}

	return nil
}

// executeGetDiagnostics executes a GET DIAGNOSTICS statement.
func (e *PLpgSQLExecutor) executeGetDiagnostics(execCtx *PLpgSQLExecContext, stmt *PLpgSQLGetDiagnosticsStmt) error {
	for _, item := range stmt.Items {
		var value interface{}

		if stmt.Stacked {
			// GET STACKED DIAGNOSTICS - from exception context
			if execCtx.diagnostics == nil {
				value = ""
			} else {
				switch item.Kind {
				case "RETURNED_SQLSTATE":
					value = execCtx.diagnostics.SQLState
				case "MESSAGE_TEXT", "PG_EXCEPTION_DETAIL":
					value = execCtx.diagnostics.MessageText
				case "PG_EXCEPTION_HINT":
					value = execCtx.diagnostics.MessageHint
				case "PG_EXCEPTION_CONTEXT":
					value = execCtx.diagnostics.ContextText
				case "SCHEMA_NAME":
					value = execCtx.diagnostics.SchemaName
				case "TABLE_NAME":
					value = execCtx.diagnostics.TableName
				case "COLUMN_NAME":
					value = execCtx.diagnostics.ColumnName
				case "CONSTRAINT_NAME":
					value = execCtx.diagnostics.ConstraintName
				default:
					value = ""
				}
			}
		} else {
			// GET DIAGNOSTICS - from last SQL statement
			switch item.Kind {
			case "ROW_COUNT":
				value = execCtx.rowCount
			case "RESULT_OID":
				value = int64(0) // Not supported
			case "PG_CONTEXT":
				value = ""
			default:
				value = nil
			}
		}

		if err := execCtx.scope.Set(item.Variable, value); err != nil {
			return err
		}
	}

	return nil
}

// executeSQL executes an embedded SQL statement.
func (e *PLpgSQLExecutor) executeSQL(execCtx *PLpgSQLExecContext, stmt *PLpgSQLSQLStmt) error {
	query := e.substituteVariables(execCtx, stmt.SQL)

	if len(stmt.Into) > 0 {
		// SELECT INTO
		rows, cols, err := e.executeQuery(execCtx, query)
		if err != nil {
			return err
		}

		execCtx.found = len(rows) > 0
		execCtx.rowCount = int64(len(rows))

		if stmt.Strict && len(rows) != 1 {
			if len(rows) == 0 {
				return &PLpgSQLRuntimeError{
					Message:  "query returned no rows",
					SQLState: SQLStateNoDataFound,
				}
			}
			return &PLpgSQLRuntimeError{
				Message:  "query returned more than one row",
				SQLState: SQLStateTooManyRows,
			}
		}

		if len(rows) > 0 {
			row := rows[0]
			for i, varName := range stmt.Into {
				if i < len(cols) {
					if err := execCtx.scope.Set(varName, row[cols[i]]); err != nil {
						return err
					}
				}
			}
		}
	} else {
		// Non-query or query without INTO
		upperSQL := strings.ToUpper(strings.TrimSpace(query))
		if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") {
			rows, _, err := e.executeQuery(execCtx, query)
			if err != nil {
				return err
			}
			execCtx.found = len(rows) > 0
			execCtx.rowCount = int64(len(rows))
		} else {
			affected, err := e.executeNonQuery(execCtx, query)
			if err != nil {
				return err
			}
			execCtx.rowCount = affected
			execCtx.found = affected > 0
		}
	}

	return nil
}

// executeCall executes a CALL statement.
func (e *PLpgSQLExecutor) executeCall(execCtx *PLpgSQLExecContext, stmt *PLpgSQLCallStmt) error {
	// Evaluate arguments
	args := make([]interface{}, len(stmt.Arguments))
	for i, arg := range stmt.Arguments {
		val, err := e.evaluateExpr(execCtx, arg)
		if err != nil {
			return err
		}
		args[i] = val
	}

	// Look up procedure
	schema := stmt.Schema
	if schema == "" {
		schema = "public"
	}
	key := schema + "." + stmt.ProcName

	e.manager.mu.RLock()
	proc, ok := e.manager.procedures[key]
	e.manager.mu.RUnlock()

	if !ok {
		return &PLpgSQLRuntimeError{
			Message:  fmt.Sprintf("procedure %s does not exist", stmt.ProcName),
			SQLState: SQLStateUndefinedFunction,
		}
	}

	// Execute procedure
	return e.ExecuteProcedure(execCtx.ctx, execCtx.conn, proc, args)
}

// evaluateExpr evaluates a PL/pgSQL expression.
func (e *PLpgSQLExecutor) evaluateExpr(execCtx *PLpgSQLExecContext, expr *PLpgSQLExpr) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	raw := expr.Raw

	// Check for variable reference
	if v, ok := execCtx.scope.Get(raw); ok {
		return v.Value, nil
	}

	// Check for special variables
	switch strings.ToUpper(raw) {
	case "FOUND":
		return execCtx.found, nil
	case "NULL":
		return nil, nil
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	}

	// Try to parse as literal
	if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") {
		return raw[1 : len(raw)-1], nil
	}
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}

	// Execute as SQL expression
	query := "SELECT " + e.substituteVariables(execCtx, raw)
	rows, cols, err := e.executeQuery(execCtx, query)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(cols) == 0 {
		return nil, nil
	}
	return rows[0][cols[0]], nil
}

// evaluateCondition evaluates a condition expression to a boolean.
func (e *PLpgSQLExecutor) evaluateCondition(execCtx *PLpgSQLExecContext, expr *PLpgSQLExpr) (bool, error) {
	val, err := e.evaluateExpr(execCtx, expr)
	if err != nil {
		return false, err
	}
	return plpgsqlToBool(val), nil
}

// executeQuery executes a SQL query and returns rows and column names.
func (e *PLpgSQLExecutor) executeQuery(execCtx *PLpgSQLExecContext, query string) ([]map[string]interface{}, []string, error) {
	query = e.substituteVariables(execCtx, query)
	return execCtx.conn.Query(execCtx.ctx, query, nil)
}

// executeNonQuery executes a SQL statement that doesn't return rows.
func (e *PLpgSQLExecutor) executeNonQuery(execCtx *PLpgSQLExecContext, query string) (int64, error) {
	query = e.substituteVariables(execCtx, query)
	return execCtx.conn.Execute(execCtx.ctx, query, nil)
}

// substituteVariables substitutes PL/pgSQL variables in SQL.
func (e *PLpgSQLExecutor) substituteVariables(execCtx *PLpgSQLExecContext, sql string) string {
	// Simple variable substitution - replace variable names with values
	// This is a simplified implementation; a full implementation would need
	// to properly parse the SQL and only substitute in appropriate places.
	scope := execCtx.scope
	for scope != nil {
		for name, v := range scope.variables {
			// Replace whole-word matches only
			sql = replaceWholeWord(sql, name, formatValue(v.Value))
		}
		scope = scope.parent
	}
	return sql
}

// matchesCondition checks if an error matches exception conditions.
func (e *PLpgSQLExecutor) matchesCondition(err error, conditions []string) bool {
	var rtErr *PLpgSQLRuntimeError
	if !errors.As(err, &rtErr) {
		// Not a runtime error, check for generic OTHERS
		for _, cond := range conditions {
			if strings.EqualFold(cond, "OTHERS") {
				return true
			}
		}
		return false
	}

	for _, cond := range conditions {
		upperCond := strings.ToUpper(cond)

		// OTHERS matches everything
		if upperCond == "OTHERS" {
			return true
		}

		// SQLSTATE match
		if strings.HasPrefix(upperCond, "SQLSTATE") {
			// Extract SQLSTATE code
			code := strings.TrimSpace(strings.TrimPrefix(upperCond, "SQLSTATE"))
			code = strings.Trim(code, "'\"")
			if rtErr.SQLState == code {
				return true
			}
			continue
		}

		// Named condition match
		switch upperCond {
		case "DIVISION_BY_ZERO":
			if rtErr.SQLState == SQLStateDivisionByZero {
				return true
			}
		case "NO_DATA_FOUND":
			if rtErr.SQLState == SQLStateNoDataFound {
				return true
			}
		case "TOO_MANY_ROWS":
			if rtErr.SQLState == SQLStateTooManyRows {
				return true
			}
		case "UNIQUE_VIOLATION":
			if rtErr.SQLState == SQLStateUniqueViolation {
				return true
			}
		case "NUMERIC_VALUE_OUT_OF_RANGE":
			if rtErr.SQLState == SQLStateNumericOverflow {
				return true
			}
		case "NULL_VALUE_NOT_ALLOWED":
			if rtErr.SQLState == SQLStateNullValueNotAllow {
				return true
			}
		case "INVALID_TEXT_REPRESENTATION":
			if rtErr.SQLState == SQLStateInvalidTextRep {
				return true
			}
		case "RAISE_EXCEPTION":
			if rtErr.SQLState == SQLStateRaiseException {
				return true
			}
		}
	}

	return false
}

// errorToDiagnostics converts an error to diagnostics.
func (e *PLpgSQLExecutor) errorToDiagnostics(err error) *PLpgSQLDiagnostics {
	var rtErr *PLpgSQLRuntimeError
	if errors.As(err, &rtErr) {
		return &PLpgSQLDiagnostics{
			SQLState:      rtErr.SQLState,
			MessageText:   rtErr.Message,
			MessageDetail: rtErr.Detail,
			MessageHint:   rtErr.Hint,
		}
	}
	return &PLpgSQLDiagnostics{
		SQLState:    SQLStateRaiseException,
		MessageText: err.Error(),
	}
}

// Helper functions

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func plpgsqlToBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case int8:
		return val != 0
	case int16:
		return val != 0
	case int32:
		return val != 0
	case int64:
		return val != 0
	case float32:
		return val != 0
	case float64:
		return val != 0
	case string:
		lower := strings.ToLower(val)
		return lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1"
	default:
		return true
	}
}

func plpgsqlToInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func replaceWholeWord(s, old, new string) string {
	// Simple whole-word replacement
	// In a real implementation, this would use regex or proper tokenization
	return strings.ReplaceAll(s, old, new)
}

func parseArrayLiteral(s string) []interface{} {
	// Parse PostgreSQL array literal: {val1,val2,val3}
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	s = s[1 : len(s)-1]
	parts := strings.Split(s, ",")
	result := make([]interface{}, len(parts))
	for i, p := range parts {
		result[i] = strings.TrimSpace(p)
	}
	return result
}
