package engine

import (
	"context"
	"database/sql/driver"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/executor"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// EngineConn represents a connection to the engine.
// It implements the BackendConn interface.
type EngineConn struct {
	mu     sync.Mutex
	engine *Engine
	txn    *Transaction
	closed bool
}

// Execute executes a query that doesn't return rows.
func (c *EngineConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, dukdb.ErrConnectionClosed
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return 0, err
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return 0, err
	}

	// Plan the statement
	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return 0, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected, nil
}

// Query executes a query that returns rows.
func (c *EngineConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, nil, err
	}

	// Plan the statement
	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return nil, nil, err
	}

	return result.Rows, result.Columns, nil
}

// Prepare prepares a statement for execution.
func (c *EngineConn) Prepare(
	ctx context.Context,
	query string,
) (dukdb.BackendStmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, dukdb.ErrConnectionClosed
	}

	// Parse the query to validate it
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	// Count and collect parameters
	numParams := parser.CountParameters(stmt)
	params := parser.CollectParameters(stmt)

	engineStmt := &EngineStmt{
		conn:       c,
		query:      query,
		stmt:       stmt,
		numParams:  numParams,
		params:     params,
		paramTypes: make(map[int]dukdb.Type),
	}

	// Bind the statement to get parameter types and column metadata
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, bindErr := b.Bind(stmt)
	if bindErr == nil {
		// Extract inferred parameter types from binder
		engineStmt.paramTypes = b.GetParamTypes()

		// For SELECT statements, also extract column metadata
		if boundSelect, ok := boundStmt.(*binder.BoundSelectStmt); ok {
			engineStmt.columns = make([]columnInfo, 0, len(boundSelect.Columns))
			for _, col := range boundSelect.Columns {
				name := col.Alias
				if name == "" && col.Expr != nil {
					// Try to infer name from expression
					if colRef, ok := col.Expr.(*binder.BoundColumnRef); ok {
						name = colRef.Column
					}
				}
				var colType dukdb.Type
				if col.Expr != nil {
					colType = col.Expr.ResultType()
				}
				engineStmt.columns = append(engineStmt.columns, columnInfo{
					name:    name,
					colType: colType,
				})
			}
		}
	}

	return engineStmt, nil
}

// Close closes the connection.
func (c *EngineConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Rollback any active transaction
	if c.txn != nil && c.txn.IsActive() {
		_ = c.engine.txnMgr.Rollback(c.txn)
	}

	return nil
}

// Ping verifies that the connection is still alive.
func (c *EngineConn) Ping(
	ctx context.Context,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return dukdb.ErrConnectionClosed
	}

	return nil
}

// AppendDataChunk appends a DataChunk directly to a table, bypassing SQL parsing.
// This provides efficient bulk data loading for the Appender.
func (c *EngineConn) AppendDataChunk(
	ctx context.Context,
	schema, table string,
	chunk *dukdb.DataChunk,
) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, dukdb.ErrConnectionClosed
	}

	// Get the table from storage
	tableKey := schema + "." + table
	storageTable, ok := c.engine.storage.GetTable(tableKey)
	if !ok {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "table not found: " + tableKey,
		}
	}

	// Convert dukdb.DataChunk to storage.DataChunk
	// Get the number of rows in the chunk
	rowCount := chunk.GetSize()
	if rowCount == 0 {
		return 0, nil
	}

	// Create a storage DataChunk with the same column types
	colTypes := storageTable.ColumnTypes()
	storageChunk := storage.NewDataChunkWithCapacity(colTypes, rowCount)

	// Copy data from dukdb.DataChunk to storage.DataChunk
	colCount := chunk.GetColumnCount()
	for row := 0; row < rowCount; row++ {
		values := make([]any, colCount)
		for col := 0; col < colCount; col++ {
			val, err := chunk.GetValue(col, row)
			if err != nil {
				return 0, err
			}
			values[col] = val
		}
		storageChunk.AppendRow(values)
	}

	// Append the storage chunk to the table
	if err := storageTable.AppendChunk(storageChunk); err != nil {
		return 0, err
	}

	return int64(rowCount), nil
}

// GetTableTypeInfos returns the TypeInfo for all columns in a table.
// This is used by the Appender to create DataChunks with the correct types.
func (c *EngineConn) GetTableTypeInfos(
	schema, table string,
) ([]dukdb.TypeInfo, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Get the table definition from catalog
	tableDef, ok := c.engine.catalog.GetTableInSchema(schema, table)
	if !ok {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "table not found: " + schema + "." + table,
		}
	}

	// Get TypeInfos and column names
	typeInfos := tableDef.ColumnTypeInfos()
	colNames := tableDef.ColumnNames()

	return typeInfos, colNames, nil
}

// EngineStmt represents a prepared statement.
type EngineStmt struct {
	mu        sync.Mutex
	conn      *EngineConn
	query     string
	stmt      parser.Statement
	numParams int
	closed    bool

	// Introspection metadata
	params     []parser.ParameterInfo
	paramTypes map[int]dukdb.Type // position -> inferred type
	columns    []columnInfo       // Populated after binding for SELECT statements
}

// columnInfo holds result column metadata.
type columnInfo struct {
	name    string
	colType dukdb.Type
}

// Execute executes the prepared statement.
func (s *EngineStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Execute(ctx, s.query, args)
}

// Query executes the prepared statement and returns rows.
func (s *EngineStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Query(ctx, s.query, args)
}

// Close closes the statement.
func (s *EngineStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *EngineStmt) NumInput() int {
	return s.numParams
}

// StatementType returns the type of the prepared statement.
func (s *EngineStmt) StatementType() dukdb.StmtType {
	if s.closed || s.stmt == nil {
		return dukdb.STATEMENT_TYPE_INVALID
	}
	return s.stmt.Type()
}

// ParamName returns the name of the parameter at the given index (1-based).
// Returns empty string for positional parameters.
func (s *EngineStmt) ParamName(index int) string {
	if index < 1 || index > len(s.params) {
		return ""
	}
	return s.params[index-1].Name
}

// ParamType returns the inferred type of the parameter at the given index (1-based).
// Returns TYPE_ANY if the type could not be inferred from context.
func (s *EngineStmt) ParamType(index int) dukdb.Type {
	if index < 1 || index > s.numParams {
		return dukdb.TYPE_INVALID
	}
	if typ, ok := s.paramTypes[index]; ok {
		return typ
	}
	return dukdb.TYPE_ANY
}

// ColumnCount returns the number of result columns.
// Returns 0 for non-SELECT statements.
func (s *EngineStmt) ColumnCount() int {
	return len(s.columns)
}

// ColumnName returns the name of the result column at the given index (0-based).
func (s *EngineStmt) ColumnName(index int) string {
	if index < 0 || index >= len(s.columns) {
		return ""
	}
	return s.columns[index].name
}

// ColumnType returns the type of the result column at the given index (0-based).
func (s *EngineStmt) ColumnType(index int) dukdb.Type {
	if index < 0 || index >= len(s.columns) {
		return dukdb.TYPE_INVALID
	}
	return s.columns[index].colType
}

// ColumnTypeInfo returns extended type info for the result column at the given index (0-based).
func (s *EngineStmt) ColumnTypeInfo(index int) dukdb.TypeInfo {
	if index < 0 || index >= len(s.columns) {
		return nil
	}
	colType := s.columns[index].colType
	// For primitive types, create TypeInfo using NewTypeInfo
	// Complex types would need additional metadata from the binder
	info, err := dukdb.NewTypeInfo(colType)
	if err != nil {
		// For complex types where NewTypeInfo fails, return a basic wrapper
		// This is a limitation - full complex type info requires binder enhancement
		return &basicTypeInfo{typ: colType}
	}
	return info
}

// basicTypeInfo is a simple TypeInfo wrapper for types that don't have
// specialized constructors available.
type basicTypeInfo struct {
	typ dukdb.Type
}

func (b *basicTypeInfo) InternalType() dukdb.Type {
	return b.typ
}

func (b *basicTypeInfo) Details() dukdb.TypeDetails {
	return nil
}

func (b *basicTypeInfo) SQLType() string {
	return b.typ.String()
}

// Properties returns metadata about the prepared statement.
func (s *EngineStmt) Properties() dukdb.StmtProperties {
	stmtType := s.StatementType()

	return dukdb.StmtProperties{
		Type:        stmtType,
		ReturnType:  stmtType.ReturnType(),
		IsReadOnly:  s.isReadOnly(),
		IsStreaming: stmtType.ReturnType() == dukdb.RETURN_QUERY_RESULT,
		ColumnCount: s.ColumnCount(),
		ParamCount:  s.NumInput(),
	}
}

// isReadOnly returns true if the statement doesn't modify any data.
func (s *EngineStmt) isReadOnly() bool {
	switch s.StatementType() {
	case dukdb.STATEMENT_TYPE_SELECT, dukdb.STATEMENT_TYPE_EXPLAIN,
		dukdb.STATEMENT_TYPE_PRAGMA, dukdb.STATEMENT_TYPE_PREPARE,
		dukdb.STATEMENT_TYPE_RELATION, dukdb.STATEMENT_TYPE_LOGICAL_PLAN:
		return true
	default:
		return false
	}
}

// GetCatalog returns the catalog for virtual table registration.
// Implements the BackendConnCatalog interface.
func (c *EngineConn) GetCatalog() any {
	return c.engine.Catalog()
}

// Verify interface implementations
var (
	_ dukdb.BackendConn        = (*EngineConn)(nil)
	_ dukdb.BackendConnCatalog = (*EngineConn)(nil)
	_ dukdb.BackendStmt        = (*EngineStmt)(nil)
	_ dukdb.BackendStmtIntrospector = (*EngineStmt)(nil)
	_ dukdb.BackendStmtProperties   = (*EngineStmt)(nil)
)
