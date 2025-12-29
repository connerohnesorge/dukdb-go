# Implementation Tasks: Statement Introspection API

## Phase 1: Statement Type Detection

### Task 1: Implement Simple SQL Parser
- [ ] Create `internal/parser/simple_detector.go`
- [ ] Implement detectStatementType(sql string) StmtType function
- [ ] Handle leading comments and whitespace
- [ ] Map first keyword to StmtType (SELECT → STATEMENT_TYPE_SELECT, etc.)
- [ ] **Validation**: Detects SELECT, INSERT, UPDATE, DELETE, CREATE, DROP correctly

### Task 2: Add StatementType() Method to PreparedStmt
- [ ] Add stmtType field to PreparedStmt struct
- [ ] Parse statement type in PreparePreparedStmt()
- [ ] Implement StatementType() method
- [ ] **Validation**: All statement type scenarios pass

## Phase 2: Parameter Metadata

### Task 3: Refactor Placeholder Extraction
- [ ] Extract placeholder parsing into reusable functions
- [ ] Add extractedParams []placeholder field to PreparedStmt
- [ ] Store parameter names during Prepare()
- [ ] **Validation**: Parameter extraction consistent with NumInput()

### Task 4: Implement ParamName() Method
- [ ] Implement ParamName(idx int) (string, error) method
- [ ] Return parameter name by index
- [ ] Handle both positional ($1) and named (@name) parameters
- [ ] Return error for out-of-range index
- [ ] **Validation**: All param name scenarios pass

### Task 5: Add ParamCount() Alias
- [ ] Implement ParamCount() as alias for NumInput()
- [ ] Update documentation
- [ ] **Validation**: Returns same value as NumInput()

## Phase 3: Explicit Binding API

### Task 6: Add Bound Parameters Storage
- [ ] Add boundParams []driver.NamedValue field to PreparedStmt
- [ ] Initialize in PreparePreparedStmt()
- [ ] **Validation**: Storage allocated correctly

### Task 7: Implement Bind() Method
- [ ] Implement Bind(idx int, value any) error method
- [ ] Validate index is within bounds
- [ ] Store value in boundParams
- [ ] Handle both positional and named parameters
- [ ] **Validation**: Bind stores parameters correctly

### Task 8: Implement ExecBound() Method
- [ ] Implement ExecBound(ctx context.Context) (driver.Result, error) method
- [ ] Validate all parameters are bound
- [ ] Delegate to ExecContext with boundParams
- [ ] **Validation**: Executes DML with bound parameters

### Task 9: Implement QueryBound() Method
- [ ] Implement QueryBound(ctx context.Context) (driver.Rows, error) method
- [ ] Validate all parameters are bound
- [ ] Delegate to QueryContext with boundParams
- [ ] **Validation**: Returns query results with bound parameters

## Phase 4: Testing and Integration

### Task 10: Unit Tests for Statement Type Detection
- [ ] Test each statement type (SELECT, INSERT, UPDATE, DELETE, etc.)
- [ ] Test with leading comments
- [ ] Test with whitespace variations
- [ ] Test invalid/unknown statements
- [ ] **Validation**: All statement type scenarios pass

### Task 11: Unit Tests for Parameter Metadata
- [ ] Test ParamName() with positional parameters
- [ ] Test ParamName() with named parameters
- [ ] Test ParamName() with out-of-range index
- [ ] Test ParamCount() consistency
- [ ] **Validation**: All param metadata scenarios pass

### Task 12: Unit Tests for Binding API
- [ ] Test Bind() with all parameter types
- [ ] Test ExecBound() executes correctly
- [ ] Test QueryBound() returns results
- [ ] Test partial binding errors
- [ ] Test out-of-bounds binding errors
- [ ] **Validation**: All binding scenarios pass

### Task 13: Backward Compatibility Tests
- [ ] Verify ExecContext still works
- [ ] Verify QueryContext still works
- [ ] Verify NumInput() unchanged
- [ ] Verify existing tests pass
- [ ] **Validation**: No regressions, 100% backward compatible

### Task 14: Integration Tests
- [ ] Test end-to-end statement introspection workflow
- [ ] Test binding with real database
- [ ] Test all statement types with real execution
- [ ] **Validation**: Integration tests pass

## Deferred to P0-4 (Execution Engine Required)

### Task 15: Parameter Type Inference (DEFERRED)
- [ ] Implement ParamType(idx) method
- [ ] Requires full SQL parser to infer types
- [ ] **Blocked by**: P0-4 SQL Execution Engine

### Task 16: Column Metadata (DEFERRED)
- [ ] Implement ColumnCount() method
- [ ] Implement ColumnName(idx) method
- [ ] Implement ColumnType(idx) method
- [ ] Implement ColumnTypeInfo(idx) method
- [ ] Requires query preparation in execution engine
- [ ] **Blocked by**: P0-4 SQL Execution Engine

## Success Criteria

All tasks completed when:
- [ ] StatementType() correctly detects all statement types
- [ ] ParamName() returns correct names for all parameters
- [ ] ParamCount() matches NumInput()
- [ ] Bind() stores parameters correctly
- [ ] ExecBound() executes DML statements
- [ ] QueryBound() returns query results
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Backward compatibility verified (100% existing tests pass)
- [ ] Documentation updated with P0-4 dependency note

**Deferred Success Criteria** (P0-4):
- [ ] ParamType() infers parameter types
- [ ] Column metadata methods return correct schema
