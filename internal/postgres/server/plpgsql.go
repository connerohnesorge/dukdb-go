// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// This file provides foundation types and the main PLpgSQLManager for PL/pgSQL
// procedural language compatibility. The manager coordinates function and
// procedure storage, lookup, and execution.

package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// =============================================================================
// PL/pgSQL COMPATIBILITY DOCUMENTATION
// =============================================================================
//
// PL/pgSQL Overview
// -----------------
//
// PL/pgSQL (Procedural Language/PostgreSQL) is PostgreSQL's default procedural
// language. It enables:
//
//   - Stored procedures and functions with complex logic
//   - Control flow statements (IF, LOOP, WHILE, FOR)
//   - Exception handling with structured error management
//   - Variable declarations and assignments
//   - Cursor operations for row-by-row processing
//   - Triggers for automatic execution on data changes
//
// Syntax Structure
// ----------------
//
// Basic function structure:
//
//   CREATE FUNCTION function_name(param1 type1, param2 type2)
//   RETURNS return_type AS $$
//   DECLARE
//       variable1 type1;
//       variable2 type2 := default_value;
//   BEGIN
//       -- statements
//       RETURN result;
//   END;
//   $$ LANGUAGE plpgsql;
//
// Procedure structure (PostgreSQL 11+):
//
//   CREATE PROCEDURE procedure_name(param1 type1)
//   LANGUAGE plpgsql AS $$
//   BEGIN
//       -- statements (no RETURN value)
//   END;
//   $$;
//
// Control Flow Statements
// -----------------------
//
// 1. Conditionals:
//
//   IF condition THEN
//       statements;
//   ELSIF condition THEN
//       statements;
//   ELSE
//       statements;
//   END IF;
//
//   CASE expression
//       WHEN value1 THEN statements;
//       WHEN value2 THEN statements;
//       ELSE statements;
//   END CASE;
//
// 2. Loops:
//
//   LOOP
//       statements;
//       EXIT WHEN condition;
//   END LOOP;
//
//   WHILE condition LOOP
//       statements;
//   END LOOP;
//
//   FOR i IN 1..10 LOOP
//       statements;
//   END LOOP;
//
//   FOR record IN SELECT * FROM table LOOP
//       statements;
//   END LOOP;
//
//   FOREACH element IN ARRAY array_var LOOP
//       statements;
//   END LOOP;
//
// 3. Loop Control:
//
//   EXIT [label] [WHEN condition];
//   CONTINUE [label] [WHEN condition];
//
// Exception Handling
// ------------------
//
//   BEGIN
//       -- statements that might fail
//   EXCEPTION
//       WHEN division_by_zero THEN
//           -- handle division by zero
//       WHEN unique_violation THEN
//           -- handle unique constraint violation
//       WHEN OTHERS THEN
//           -- catch-all handler
//           RAISE NOTICE 'Error: %', SQLERRM;
//   END;
//
// RAISE Statement:
//
//   RAISE DEBUG 'debug message';
//   RAISE LOG 'log message';
//   RAISE INFO 'info message';
//   RAISE NOTICE 'notice message';
//   RAISE WARNING 'warning message';
//   RAISE EXCEPTION 'error message';
//
//   RAISE EXCEPTION 'Value % is invalid', some_value
//       USING HINT = 'Check the input data',
//             ERRCODE = 'invalid_parameter_value';
//
// Cursor Operations
// -----------------
//
//   DECLARE
//       my_cursor CURSOR FOR SELECT * FROM table;
//       my_record table%ROWTYPE;
//   BEGIN
//       OPEN my_cursor;
//       LOOP
//           FETCH my_cursor INTO my_record;
//           EXIT WHEN NOT FOUND;
//           -- process record
//       END LOOP;
//       CLOSE my_cursor;
//   END;
//
// Special Variables
// -----------------
//
//   FOUND      - Boolean set by various statements
//   ROW_COUNT  - Number of rows affected by last statement
//   SQLSTATE   - SQLSTATE error code of last error
//   SQLERRM    - Error message of last error
//   TG_*       - Trigger-related variables
//
// Implementation Considerations for DukDB
// ---------------------------------------
//
// Implementing PL/pgSQL in DukDB requires several components:
//
// 1. Parser Extension:
//    - Parse PL/pgSQL block structure
//    - Handle dollar-quoted strings ($$)
//    - Parse DECLARE, BEGIN, EXCEPTION blocks
//    - Parse control flow statements
//
// 2. Runtime Environment:
//    - Variable scope management
//    - Type system integration
//    - Expression evaluation
//    - Query execution within procedures
//
// 3. Catalog Integration:
//    - Store function/procedure definitions
//    - Manage function signatures and overloads
//    - Handle function search path
//
// 4. Execution Engine:
//    - Interpret PL/pgSQL bytecode or AST
//    - Manage execution stack
//    - Handle exceptions and error propagation
//
// 5. Trigger Support:
//    - Register triggers on tables
//    - Invoke trigger functions on DML operations
//    - Pass OLD/NEW row values
//
// Alternative Approaches
// ----------------------
//
// Instead of full PL/pgSQL implementation, consider:
//
// 1. SQL-only functions (LANGUAGE SQL):
//    - Simpler to implement
//    - Inline expansion possible
//    - Sufficient for many use cases
//
// 2. Go-based UDFs:
//    - Register Go functions as SQL functions
//    - Full Go power available
//    - Type-safe interface
//
// 3. Limited PL/pgSQL subset:
//    - Basic variable assignment
//    - Simple IF/THEN/ELSE
//    - RETURN statements
//    - No cursors or exceptions initially
//
// References
// ----------
//
//   - PL/pgSQL Docs: https://www.postgresql.org/docs/current/plpgsql.html
//   - Syntax: https://www.postgresql.org/docs/current/plpgsql-structure.html
//   - Control: https://www.postgresql.org/docs/current/plpgsql-control-structures.html
//   - Triggers: https://www.postgresql.org/docs/current/plpgsql-trigger.html
//
// =============================================================================

// Common errors for PL/pgSQL operations.
var (
	// ErrPLpgSQLNotSupported indicates that PL/pgSQL is not yet implemented.
	ErrPLpgSQLNotSupported = errors.New("PL/pgSQL is not yet supported")

	// ErrFunctionNotFound indicates that the requested function does not exist.
	ErrFunctionNotFound = errors.New("function not found")

	// ErrProcedureNotFound indicates that the requested procedure does not exist.
	ErrProcedureNotFound = errors.New("procedure not found")

	// ErrInvalidFunctionDefinition indicates a syntax error in function body.
	ErrInvalidFunctionDefinition = errors.New("invalid function definition")

	// ErrVariableNotDeclared indicates use of an undeclared variable.
	ErrVariableNotDeclared = errors.New("variable not declared")

	// ErrTypeMismatch indicates a type mismatch in assignment or return.
	ErrTypeMismatch = errors.New("type mismatch")
)

// PLpgSQLLanguage represents the PL/pgSQL procedural language.
const PLpgSQLLanguage = "plpgsql"

// FunctionVolatility indicates how a function interacts with the database.
type FunctionVolatility int

const (
	// VolatilityVolatile indicates the function can return different results
	// for the same inputs (default).
	VolatilityVolatile FunctionVolatility = iota

	// VolatilityStable indicates the function returns the same result for
	// the same inputs within a single statement.
	VolatilityStable

	// VolatilityImmutable indicates the function always returns the same
	// result for the same inputs.
	VolatilityImmutable
)

// String returns the string representation of volatility.
func (v FunctionVolatility) String() string {
	switch v {
	case VolatilityStable:
		return "STABLE"
	case VolatilityImmutable:
		return "IMMUTABLE"
	default:
		return "VOLATILE"
	}
}

// FunctionParameter represents a parameter in a function definition.
type FunctionParameter struct {
	// Name is the parameter name (may be empty for unnamed parameters).
	Name string

	// Type is the parameter's data type.
	Type string

	// Mode indicates if the parameter is IN, OUT, INOUT, or VARIADIC.
	Mode ParameterMode

	// DefaultValue is the default value expression (nil if no default).
	DefaultValue interface{}
}

// ParameterMode indicates the parameter direction.
type ParameterMode int

const (
	// ModeIn indicates an input-only parameter (default).
	ModeIn ParameterMode = iota

	// ModeOut indicates an output-only parameter.
	ModeOut

	// ModeInOut indicates a parameter that is both input and output.
	ModeInOut

	// ModeVariadic indicates a variadic parameter.
	ModeVariadic
)

// String returns the string representation of the parameter mode.
func (m ParameterMode) String() string {
	switch m {
	case ModeOut:
		return "OUT"
	case ModeInOut:
		return "INOUT"
	case ModeVariadic:
		return "VARIADIC"
	default:
		return "IN"
	}
}

// StoredFunction represents a stored function definition.
type StoredFunction struct {
	// Schema is the schema containing this function.
	Schema string

	// Name is the function name.
	Name string

	// Parameters defines the function's parameter list.
	Parameters []FunctionParameter

	// ReturnType is the return type (empty for procedures).
	ReturnType string

	// ReturnsSet indicates if the function returns a set of values.
	ReturnsSet bool

	// Language is the implementation language (e.g., "plpgsql", "sql").
	Language string

	// Body is the function body source code.
	Body string

	// Volatility indicates the function's volatility category.
	Volatility FunctionVolatility

	// Strict indicates if the function returns NULL on any NULL input.
	Strict bool

	// SecurityDefiner indicates if the function runs as the owner.
	SecurityDefiner bool

	// Cost is the estimated execution cost (in cpu_operator_cost units).
	Cost float64

	// Rows is the estimated number of rows returned (for set-returning functions).
	Rows float64
}

// StoredProcedure represents a stored procedure definition (PostgreSQL 11+).
type StoredProcedure struct {
	// Schema is the schema containing this procedure.
	Schema string

	// Name is the procedure name.
	Name string

	// Parameters defines the procedure's parameter list.
	Parameters []FunctionParameter

	// Language is the implementation language.
	Language string

	// Body is the procedure body source code.
	Body string

	// SecurityDefiner indicates if the procedure runs as the owner.
	SecurityDefiner bool
}

// TriggerTiming indicates when a trigger fires relative to the event.
type TriggerTiming int

const (
	// TriggerBefore fires before the event.
	TriggerBefore TriggerTiming = iota

	// TriggerAfter fires after the event.
	TriggerAfter

	// TriggerInsteadOf fires instead of the event (for views).
	TriggerInsteadOf
)

// TriggerEvent indicates which events fire the trigger.
type TriggerEvent int

const (
	// TriggerOnInsert fires on INSERT.
	TriggerOnInsert TriggerEvent = 1 << iota

	// TriggerOnUpdate fires on UPDATE.
	TriggerOnUpdate

	// TriggerOnDelete fires on DELETE.
	TriggerOnDelete

	// TriggerOnTruncate fires on TRUNCATE.
	TriggerOnTruncate
)

// TriggerLevel indicates whether the trigger fires per-row or per-statement.
type TriggerLevel int

const (
	// TriggerForEachRow fires once per affected row.
	TriggerForEachRow TriggerLevel = iota

	// TriggerForEachStatement fires once per statement.
	TriggerForEachStatement
)

// Trigger represents a trigger definition.
type Trigger struct {
	// Schema is the schema containing the trigger.
	Schema string

	// Name is the trigger name.
	Name string

	// Table is the table this trigger is attached to.
	Table string

	// Timing is when the trigger fires (BEFORE, AFTER, INSTEAD OF).
	Timing TriggerTiming

	// Events is a bitmask of events that fire the trigger.
	Events TriggerEvent

	// Level is whether the trigger fires per-row or per-statement.
	Level TriggerLevel

	// Function is the trigger function to invoke.
	Function string

	// WhenClause is an optional condition for firing (may be empty).
	WhenClause string

	// Enabled indicates if the trigger is enabled.
	Enabled bool
}

// PLpgSQLBlock represents a parsed PL/pgSQL block structure.
// This is a stub for future AST representation.
type PLpgSQLBlock struct {
	// Declarations contains variable declarations.
	Declarations []PLpgSQLDeclaration

	// Statements contains the block's statements.
	Statements []PLpgSQLStatement

	// ExceptionHandlers contains exception handlers.
	ExceptionHandlers []PLpgSQLExceptionHandler
}

// PLpgSQLDeclaration represents a variable declaration.
type PLpgSQLDeclaration struct {
	// Name is the variable name.
	Name string

	// Type is the variable type.
	Type string

	// NotNull indicates if the variable cannot be NULL.
	NotNull bool

	// DefaultExpr is the default value expression.
	DefaultExpr string
}

// PLpgSQLStatement is an interface for PL/pgSQL statements.
// This is a marker interface for the AST.
type PLpgSQLStatement interface {
	plpgsqlStatement()
}

// PLpgSQLExceptionHandler represents an exception handler in a block.
type PLpgSQLExceptionHandler struct {
	// Conditions lists the exception conditions handled.
	Conditions []string

	// Statements contains the handler statements.
	Statements []PLpgSQLStatement
}

// PLpgSQLRuntime manages PL/pgSQL function execution.
// This is a stub interface for future implementation.
type PLpgSQLRuntime interface {
	// ExecuteFunction executes a stored function.
	ExecuteFunction(ctx context.Context, schema, name string, args []interface{}) (interface{}, error)

	// ExecuteProcedure executes a stored procedure.
	ExecuteProcedure(ctx context.Context, schema, name string, args []interface{}) error

	// CreateFunction registers a new function.
	CreateFunction(ctx context.Context, fn *StoredFunction) error

	// CreateProcedure registers a new procedure.
	CreateProcedure(ctx context.Context, proc *StoredProcedure) error

	// DropFunction removes a function.
	DropFunction(ctx context.Context, schema, name string, argTypes []string, ifExists bool) error

	// DropProcedure removes a procedure.
	DropProcedure(ctx context.Context, schema, name string, argTypes []string, ifExists bool) error

	// CreateTrigger registers a new trigger.
	CreateTrigger(ctx context.Context, trigger *Trigger) error

	// DropTrigger removes a trigger.
	DropTrigger(ctx context.Context, schema, name, table string, ifExists bool) error
}

// PLpgSQLManager coordinates PL/pgSQL functionality.
// This is the main entry point for procedural language operations.
type PLpgSQLManager struct {
	mu sync.RWMutex

	// functions stores function definitions by schema.name.
	functions map[string]*StoredFunction

	// procedures stores procedure definitions by schema.name.
	procedures map[string]*StoredProcedure

	// triggers stores trigger definitions by schema.table.name.
	triggers map[string]*Trigger

	// executor handles PL/pgSQL execution.
	executor *PLpgSQLExecutor

	// enabled indicates if PL/pgSQL is enabled.
	enabled bool
}

// NewPLpgSQLManager creates a new PL/pgSQL manager.
func NewPLpgSQLManager() *PLpgSQLManager {
	pm := &PLpgSQLManager{
		functions:  make(map[string]*StoredFunction),
		procedures: make(map[string]*StoredProcedure),
		triggers:   make(map[string]*Trigger),
		enabled:    true,
	}
	pm.executor = NewPLpgSQLExecutor(pm)
	return pm
}

// IsPLpgSQLSupported returns whether PL/pgSQL is currently supported.
func (pm *PLpgSQLManager) IsPLpgSQLSupported() bool {
	return pm.enabled
}

// Enable enables PL/pgSQL support.
func (pm *PLpgSQLManager) Enable() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.enabled = true
}

// Disable disables PL/pgSQL support.
func (pm *PLpgSQLManager) Disable() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.enabled = false
}

// =============================================================================
// FUNCTION MANAGEMENT
// =============================================================================

// CreateFunction creates a new stored function.
func (pm *PLpgSQLManager) CreateFunction(_ context.Context, fn *StoredFunction) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.enabled {
		return ErrPLpgSQLNotSupported
	}

	if fn == nil {
		return errors.New("function definition is nil")
	}

	// Validate function
	if fn.Name == "" {
		return errors.New("function name is required")
	}
	if fn.Language == "" {
		fn.Language = "plpgsql"
	}

	// Default schema
	schema := fn.Schema
	if schema == "" {
		schema = "public"
	}

	// Create key
	key := schema + "." + strings.ToLower(fn.Name)

	pm.functions[key] = fn
	return nil
}

// CreateOrReplaceFunction creates or replaces a stored function.
func (pm *PLpgSQLManager) CreateOrReplaceFunction(_ context.Context, fn *StoredFunction) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.enabled {
		return ErrPLpgSQLNotSupported
	}

	if fn == nil {
		return errors.New("function definition is nil")
	}

	if fn.Name == "" {
		return errors.New("function name is required")
	}
	if fn.Language == "" {
		fn.Language = "plpgsql"
	}

	schema := fn.Schema
	if schema == "" {
		schema = "public"
	}

	key := schema + "." + strings.ToLower(fn.Name)
	pm.functions[key] = fn
	return nil
}

// GetFunction retrieves a stored function by name.
func (pm *PLpgSQLManager) GetFunction(schema, name string) (*StoredFunction, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)
	fn, ok := pm.functions[key]
	return fn, ok
}

// DropFunction removes a function.
func (pm *PLpgSQLManager) DropFunction(_ context.Context, schema, name string, argTypes []string, ifExists bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)

	if _, ok := pm.functions[key]; !ok {
		if ifExists {
			return nil
		}
		return ErrFunctionNotFound
	}

	delete(pm.functions, key)
	return nil
}

// ListFunctions returns all stored functions.
func (pm *PLpgSQLManager) ListFunctions() []*StoredFunction {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	fns := make([]*StoredFunction, 0, len(pm.functions))
	for _, fn := range pm.functions {
		fns = append(fns, fn)
	}
	return fns
}

// ExecuteFunction executes a stored function.
func (pm *PLpgSQLManager) ExecuteFunction(ctx context.Context, conn BackendConnInterface, schema, name string, args []interface{}) (interface{}, error) {
	pm.mu.RLock()
	if !pm.enabled {
		pm.mu.RUnlock()
		return nil, ErrPLpgSQLNotSupported
	}

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)
	fn, ok := pm.functions[key]
	pm.mu.RUnlock()

	if !ok {
		return nil, ErrFunctionNotFound
	}

	// Check for STRICT - return NULL if any argument is NULL
	if fn.Strict {
		for _, arg := range args {
			if arg == nil {
				return nil, nil
			}
		}
	}

	// Handle SQL language functions differently
	if strings.EqualFold(fn.Language, "sql") {
		return pm.executeSQLFunction(ctx, conn, fn, args)
	}

	// Execute PL/pgSQL function
	return pm.executor.ExecuteFunction(ctx, conn, fn, args)
}

// executeSQLFunction executes a LANGUAGE SQL function.
func (pm *PLpgSQLManager) executeSQLFunction(ctx context.Context, conn BackendConnInterface, fn *StoredFunction, args []interface{}) (interface{}, error) {
	// Substitute parameter references ($1, $2, etc.)
	query := fn.Body
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		query = strings.ReplaceAll(query, placeholder, formatValue(arg))
	}

	// Execute the query
	rows, cols, err := conn.Query(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	if fn.ReturnsSet {
		return rows, nil
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// Return first column of first row for scalar functions
	if len(cols) > 0 {
		return rows[0][cols[0]], nil
	}
	return nil, nil
}

// =============================================================================
// PROCEDURE MANAGEMENT
// =============================================================================

// CreateProcedure creates a new stored procedure.
func (pm *PLpgSQLManager) CreateProcedure(_ context.Context, proc *StoredProcedure) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.enabled {
		return ErrPLpgSQLNotSupported
	}

	if proc == nil {
		return errors.New("procedure definition is nil")
	}

	if proc.Name == "" {
		return errors.New("procedure name is required")
	}
	if proc.Language == "" {
		proc.Language = "plpgsql"
	}

	schema := proc.Schema
	if schema == "" {
		schema = "public"
	}

	key := schema + "." + strings.ToLower(proc.Name)
	pm.procedures[key] = proc
	return nil
}

// CreateOrReplaceProcedure creates or replaces a stored procedure.
func (pm *PLpgSQLManager) CreateOrReplaceProcedure(_ context.Context, proc *StoredProcedure) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.enabled {
		return ErrPLpgSQLNotSupported
	}

	if proc == nil {
		return errors.New("procedure definition is nil")
	}

	if proc.Name == "" {
		return errors.New("procedure name is required")
	}
	if proc.Language == "" {
		proc.Language = "plpgsql"
	}

	schema := proc.Schema
	if schema == "" {
		schema = "public"
	}

	key := schema + "." + strings.ToLower(proc.Name)
	pm.procedures[key] = proc
	return nil
}

// GetProcedure retrieves a stored procedure by name.
func (pm *PLpgSQLManager) GetProcedure(schema, name string) (*StoredProcedure, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)
	proc, ok := pm.procedures[key]
	return proc, ok
}

// DropProcedure removes a procedure.
func (pm *PLpgSQLManager) DropProcedure(_ context.Context, schema, name string, argTypes []string, ifExists bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)

	if _, ok := pm.procedures[key]; !ok {
		if ifExists {
			return nil
		}
		return ErrProcedureNotFound
	}

	delete(pm.procedures, key)
	return nil
}

// ListProcedures returns all stored procedures.
func (pm *PLpgSQLManager) ListProcedures() []*StoredProcedure {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	procs := make([]*StoredProcedure, 0, len(pm.procedures))
	for _, proc := range pm.procedures {
		procs = append(procs, proc)
	}
	return procs
}

// ExecuteProcedure executes a stored procedure.
func (pm *PLpgSQLManager) ExecuteProcedure(ctx context.Context, conn BackendConnInterface, schema, name string, args []interface{}) error {
	pm.mu.RLock()
	if !pm.enabled {
		pm.mu.RUnlock()
		return ErrPLpgSQLNotSupported
	}

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + strings.ToLower(name)
	proc, ok := pm.procedures[key]
	pm.mu.RUnlock()

	if !ok {
		return ErrProcedureNotFound
	}

	// Handle SQL language procedures
	if strings.EqualFold(proc.Language, "sql") {
		return pm.executeSQLProcedure(ctx, conn, proc, args)
	}

	// Execute PL/pgSQL procedure
	return pm.executor.ExecuteProcedure(ctx, conn, proc, args)
}

// executeSQLProcedure executes a LANGUAGE SQL procedure.
func (pm *PLpgSQLManager) executeSQLProcedure(ctx context.Context, conn BackendConnInterface, proc *StoredProcedure, args []interface{}) error {
	// Substitute parameter references
	query := proc.Body
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		query = strings.ReplaceAll(query, placeholder, formatValue(arg))
	}

	// Execute the query
	_, err := conn.Execute(ctx, query, nil)
	return err
}

// =============================================================================
// TRIGGER MANAGEMENT
// =============================================================================

// CreateTrigger creates a new trigger.
func (pm *PLpgSQLManager) CreateTrigger(_ context.Context, trigger *Trigger) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.enabled {
		return ErrPLpgSQLNotSupported
	}

	if trigger == nil {
		return errors.New("trigger definition is nil")
	}

	if trigger.Name == "" {
		return errors.New("trigger name is required")
	}
	if trigger.Table == "" {
		return errors.New("trigger table is required")
	}
	if trigger.Function == "" {
		return errors.New("trigger function is required")
	}

	schema := trigger.Schema
	if schema == "" {
		schema = "public"
	}

	key := schema + "." + trigger.Table + "." + strings.ToLower(trigger.Name)
	pm.triggers[key] = trigger
	return nil
}

// GetTrigger retrieves a trigger by name.
func (pm *PLpgSQLManager) GetTrigger(schema, table, name string) (*Trigger, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + table + "." + strings.ToLower(name)
	trigger, ok := pm.triggers[key]
	return trigger, ok
}

// DropTrigger removes a trigger.
func (pm *PLpgSQLManager) DropTrigger(_ context.Context, schema, name, table string, ifExists bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if schema == "" {
		schema = "public"
	}
	key := schema + "." + table + "." + strings.ToLower(name)

	if _, ok := pm.triggers[key]; !ok {
		if ifExists {
			return nil
		}
		return errors.New("trigger not found")
	}

	delete(pm.triggers, key)
	return nil
}

// ListTriggers returns all triggers.
func (pm *PLpgSQLManager) ListTriggers() []*Trigger {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	triggers := make([]*Trigger, 0, len(pm.triggers))
	for _, trigger := range pm.triggers {
		triggers = append(triggers, trigger)
	}
	return triggers
}

// GetTriggersForTable returns all triggers for a table.
func (pm *PLpgSQLManager) GetTriggersForTable(schema, table string) []*Trigger {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if schema == "" {
		schema = "public"
	}
	prefix := schema + "." + table + "."

	var triggers []*Trigger
	for key, trigger := range pm.triggers {
		if strings.HasPrefix(key, prefix) {
			triggers = append(triggers, trigger)
		}
	}
	return triggers
}

// =============================================================================
// SQL PARSING FOR CREATE FUNCTION/PROCEDURE
// =============================================================================

// ParseCreateFunction parses a CREATE FUNCTION statement.
func ParseCreateFunction(sql string) (*StoredFunction, error) {
	fn := &StoredFunction{
		Schema:     "public",
		Volatility: VolatilityVolatile,
		Cost:       100,
		Rows:       1000,
	}

	upper := strings.ToUpper(sql)

	// Check for CREATE OR REPLACE
	orReplace := strings.Contains(upper, "OR REPLACE")
	_ = orReplace // Not used in parsing, but caller may need to know

	// Extract function name
	namePattern := regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:(\w+)\.)?(\w+)\s*\(`)
	nameMatches := namePattern.FindStringSubmatch(sql)
	if len(nameMatches) < 3 {
		return nil, errors.New("could not parse function name")
	}
	if nameMatches[1] != "" {
		fn.Schema = nameMatches[1]
	}
	fn.Name = nameMatches[2]

	// Extract parameters
	paramStart := strings.Index(sql, "(")
	paramEnd := findMatchingParen(sql, paramStart)
	if paramStart == -1 || paramEnd == -1 {
		return nil, errors.New("could not parse function parameters")
	}
	paramStr := sql[paramStart+1 : paramEnd]
	fn.Parameters = parseParameters(paramStr)

	// Extract return type
	returnPattern := regexp.MustCompile(`(?i)RETURNS\s+(SETOF\s+)?(\w+(?:\[\])?)`)
	returnMatches := returnPattern.FindStringSubmatch(sql)
	if len(returnMatches) >= 3 {
		if returnMatches[1] != "" {
			fn.ReturnsSet = true
		}
		fn.ReturnType = returnMatches[2]
	}

	// Extract language
	langPattern := regexp.MustCompile(`(?i)LANGUAGE\s+(\w+)`)
	langMatches := langPattern.FindStringSubmatch(sql)
	if len(langMatches) >= 2 {
		fn.Language = strings.ToLower(langMatches[1])
	}

	// Extract volatility
	if strings.Contains(upper, "IMMUTABLE") {
		fn.Volatility = VolatilityImmutable
	} else if strings.Contains(upper, "STABLE") {
		fn.Volatility = VolatilityStable
	}

	// Check for STRICT
	fn.Strict = strings.Contains(upper, "STRICT") || strings.Contains(upper, "RETURNS NULL ON NULL INPUT")

	// Check for SECURITY DEFINER
	fn.SecurityDefiner = strings.Contains(upper, "SECURITY DEFINER")

	// Extract body - look for dollar-quoted string
	fn.Body = extractDollarQuotedString(sql)
	if fn.Body == "" {
		// Try single-quoted body (less common)
		singlePattern := regexp.MustCompile(`AS\s+'((?:[^']|'')*)'`)
		singleMatches := singlePattern.FindStringSubmatch(sql)
		if len(singleMatches) >= 2 {
			fn.Body = strings.ReplaceAll(singleMatches[1], "''", "'")
		}
	}

	if fn.Body == "" {
		return nil, errors.New("could not parse function body")
	}

	return fn, nil
}

// extractDollarQuotedString extracts a dollar-quoted string from SQL.
// Handles $$ or $tag$ patterns.
func extractDollarQuotedString(sql string) string {
	// Find the first $
	start := strings.Index(sql, "$")
	if start == -1 {
		return ""
	}

	// Find the tag (may be empty for $$)
	tagEnd := start + 1
	for tagEnd < len(sql) && (sql[tagEnd] == '_' || (sql[tagEnd] >= 'a' && sql[tagEnd] <= 'z') || (sql[tagEnd] >= 'A' && sql[tagEnd] <= 'Z') || (sql[tagEnd] >= '0' && sql[tagEnd] <= '9')) {
		tagEnd++
	}

	// Check for closing $
	if tagEnd >= len(sql) || sql[tagEnd] != '$' {
		return ""
	}

	tag := sql[start : tagEnd+1] // e.g., "$$" or "$body$"
	bodyStart := tagEnd + 1

	// Find the closing tag
	bodyEnd := strings.Index(sql[bodyStart:], tag)
	if bodyEnd == -1 {
		return ""
	}

	return strings.TrimSpace(sql[bodyStart : bodyStart+bodyEnd])
}

// ParseCreateProcedure parses a CREATE PROCEDURE statement.
func ParseCreateProcedure(sql string) (*StoredProcedure, error) {
	proc := &StoredProcedure{
		Schema: "public",
	}

	// Extract procedure name
	namePattern := regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(?:(\w+)\.)?(\w+)\s*\(`)
	nameMatches := namePattern.FindStringSubmatch(sql)
	if len(nameMatches) < 3 {
		return nil, errors.New("could not parse procedure name")
	}
	if nameMatches[1] != "" {
		proc.Schema = nameMatches[1]
	}
	proc.Name = nameMatches[2]

	// Extract parameters
	paramStart := strings.Index(sql, "(")
	paramEnd := findMatchingParen(sql, paramStart)
	if paramStart == -1 || paramEnd == -1 {
		return nil, errors.New("could not parse procedure parameters")
	}
	paramStr := sql[paramStart+1 : paramEnd]
	proc.Parameters = parseParameters(paramStr)

	// Extract language
	langPattern := regexp.MustCompile(`(?i)LANGUAGE\s+(\w+)`)
	langMatches := langPattern.FindStringSubmatch(sql)
	if len(langMatches) >= 2 {
		proc.Language = strings.ToLower(langMatches[1])
	} else {
		proc.Language = "plpgsql"
	}

	// Check for SECURITY DEFINER
	upper := strings.ToUpper(sql)
	proc.SecurityDefiner = strings.Contains(upper, "SECURITY DEFINER")

	// Extract body
	proc.Body = extractDollarQuotedString(sql)
	if proc.Body == "" {
		singlePattern := regexp.MustCompile(`AS\s+'((?:[^']|'')*)'`)
		singleMatches := singlePattern.FindStringSubmatch(sql)
		if len(singleMatches) >= 2 {
			proc.Body = strings.ReplaceAll(singleMatches[1], "''", "'")
		}
	}

	if proc.Body == "" {
		return nil, errors.New("could not parse procedure body")
	}

	return proc, nil
}

// parseParameters parses a parameter list string.
func parseParameters(paramStr string) []FunctionParameter {
	paramStr = strings.TrimSpace(paramStr)
	if paramStr == "" {
		return nil
	}

	var params []FunctionParameter
	parts := splitParams(paramStr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		param := FunctionParameter{Mode: ModeIn}

		// Check for mode prefix
		upperPart := strings.ToUpper(part)
		if strings.HasPrefix(upperPart, "INOUT ") {
			param.Mode = ModeInOut
			part = strings.TrimSpace(part[6:])
		} else if strings.HasPrefix(upperPart, "OUT ") {
			param.Mode = ModeOut
			part = strings.TrimSpace(part[4:])
		} else if strings.HasPrefix(upperPart, "IN ") {
			param.Mode = ModeIn
			part = strings.TrimSpace(part[3:])
		} else if strings.HasPrefix(upperPart, "VARIADIC ") {
			param.Mode = ModeVariadic
			part = strings.TrimSpace(part[9:])
		}

		// Parse name and type
		tokens := strings.Fields(part)
		if len(tokens) >= 2 {
			param.Name = tokens[0]
			param.Type = strings.Join(tokens[1:], " ")

			// Check for DEFAULT
			upperType := strings.ToUpper(param.Type)
			if idx := strings.Index(upperType, " DEFAULT "); idx != -1 {
				param.DefaultValue = strings.TrimSpace(param.Type[idx+9:])
				param.Type = strings.TrimSpace(param.Type[:idx])
			} else if idx := strings.Index(param.Type, "="); idx != -1 {
				param.DefaultValue = strings.TrimSpace(param.Type[idx+1:])
				param.Type = strings.TrimSpace(param.Type[:idx])
			}
		} else if len(tokens) == 1 {
			// Anonymous parameter (type only)
			param.Type = tokens[0]
		}

		params = append(params, param)
	}

	return params
}

// splitParams splits a parameter string by commas, respecting parentheses.
func splitParams(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
		} else if c == ',' && depth == 0 {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// findMatchingParen finds the matching closing parenthesis.
func findMatchingParen(s string, start int) int {
	if start < 0 || start >= len(s) || s[start] != '(' {
		return -1
	}

	depth := 1
	for i := start + 1; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// =============================================================================
// IMPLEMENTATION STATUS
// =============================================================================
//
// The PL/pgSQL implementation includes:
//
// - Variable scope management with block-level scoping (plpgsql_runtime.go)
// - Expression evaluation within PL/pgSQL context
// - Query execution from within functions
// - Return value handling
//
// Parser (plpgsql_parser.go):
// - Parse PL/pgSQL block structure (DECLARE, BEGIN, EXCEPTION, END)
// - Parse variable declarations (name type [:= default_value])
// - Parse assignments (variable := expression)
// - Parse RETURN statements
// - Parse RAISE statements (NOTICE, WARNING, EXCEPTION)
//
// Functions and Procedures (plpgsql.go):
// - CREATE FUNCTION name(params) RETURNS type AS $$ ... $$ LANGUAGE plpgsql
// - CREATE PROCEDURE name(params) AS $$ ... $$ LANGUAGE plpgsql
// - Store functions/procedures in catalog
// - Call functions in expressions
// - Call procedures with CALL statement
// - DROP FUNCTION/PROCEDURE support
//
// Control Flow (plpgsql_runtime.go):
// - IF ... THEN ... ELSIF ... ELSE ... END IF
// - Simple LOOP ... END LOOP
// - WHILE ... LOOP ... END LOOP
// - FOR ... IN ... LOOP ... END LOOP (numeric range)
// - FOR ... IN query LOOP ... END LOOP (query iteration)
// - EXIT and CONTINUE statements
//
// Exception Handling (plpgsql_runtime.go):
// - EXCEPTION WHEN condition THEN handler
// - Common conditions: OTHERS, NO_DATA_FOUND, TOO_MANY_ROWS, etc.
// - SQLSTATE matching
// - RAISE EXCEPTION 'message'
// - GET STACKED DIAGNOSTICS (basic support)
//
// =============================================================================
