# Tasks: Add Statement Type Detection and Properties

## Phase 1: Constants and Enums

- [ ] **1.1** Add missing statement type constants to `backend.go`
  - Add STATEMENT_TYPE_MERGE_INTO = 28
  - Add STATEMENT_TYPE_UPDATE_EXTENSIONS = 29
  - Add STATEMENT_TYPE_COPY_DATABASE = 30
  - Update any existing type range checks

- [ ] **1.2** Add StmtReturnType enum to `backend.go`
  - Define RETURN_QUERY_RESULT, RETURN_CHANGED_ROWS, RETURN_NOTHING
  - Add documentation for each constant

- [ ] **1.3** Add StmtTypeName function
  - Map all 30 statement types to string names
  - Add String() method on StmtType for Stringer interface

- [ ] **1.4** Test constants and enums
  - Verify all 30 constants have unique values
  - Test StmtTypeName for all types
  - Test String() method

## Phase 2: Classification Methods

- [ ] **2.1** Add ReturnType() method on StmtType
  - Map each statement type to its return type
  - Handle edge cases (EXECUTE, CALL vary)

- [ ] **2.2** Add IsDML() method on StmtType
  - Return true for INSERT, UPDATE, DELETE, MERGE_INTO

- [ ] **2.3** Add IsDDL() method on StmtType
  - Return true for CREATE, DROP, ALTER

- [ ] **2.4** Add IsQuery() method on StmtType
  - Return true for statements returning QUERY_RESULT

- [ ] **2.5** Add ModifiesData() method on StmtType
  - Return true for DML and DDL statements

- [ ] **2.6** Add IsTransaction() method on StmtType
  - Return true for TRANSACTION type

- [ ] **2.7** Test classification methods
  - Test IsDML() for all DML types
  - Test IsDDL() for all DDL types
  - Test IsQuery() for all query types
  - Test edge cases (EXECUTE, CALL)

## Phase 3: Properties Interface

- [ ] **3.1** Define StmtProperties struct in `backend.go`
  - Type, ReturnType, IsReadOnly, IsStreaming
  - ColumnCount, ParamCount fields

- [ ] **3.2** Define BackendStmtProperties interface
  - Properties() StmtProperties method
  - Extends BackendStmt

- [ ] **3.3** Implement Properties() in EngineStmt
  - Compute all properties from existing methods
  - Implement isReadOnly() helper

- [ ] **3.4** Test Properties implementation
  - Verify all property values for SELECT
  - Verify all property values for INSERT
  - Verify all property values for DDL

## Phase 4: Public API

- [ ] **4.1** Add Properties() to Stmt type
  - Handle closed statement error
  - Try BackendStmtProperties first
  - Fall back to computing from introspector

- [ ] **4.2** Add IsReadOnly() convenience method
  - Return bool with error
  - Use Properties() internally

- [ ] **4.3** Add IsQuery() convenience method
  - Return bool with error
  - Check ReturnType == RETURN_QUERY_RESULT

- [ ] **4.4** Test public API
  - Test Properties() returns correct values
  - Test error on closed statement
  - Test convenience methods

## Phase 5: Parser Integration

- [ ] **5.1** Add MergeStmt to parser (if not exists)
  - Parse MERGE INTO syntax
  - Add Type() returning STATEMENT_TYPE_MERGE_INTO

- [ ] **5.2** Add UpdateExtensionsStmt to parser (if not exists)
  - Parse UPDATE EXTENSIONS syntax
  - Add Type() returning STATEMENT_TYPE_UPDATE_EXTENSIONS

- [ ] **5.3** Add CopyDatabaseStmt to parser (if not exists)
  - Parse COPY DATABASE syntax
  - Add Type() returning STATEMENT_TYPE_COPY_DATABASE

- [ ] **5.4** Verify all AST nodes have Type() method
  - Audit all statement types
  - Add missing Type() methods

## Phase 6: Integration Testing

- [ ] **6.1** Create statement type detection test suite
  - Test all 30 statement types
  - Use table-driven tests

- [ ] **6.2** Test return type classification
  - Verify QUERY_RESULT for SELECT, EXPLAIN, PRAGMA
  - Verify CHANGED_ROWS for INSERT, UPDATE, DELETE
  - Verify NOTHING for DDL, SET

- [ ] **6.3** Test read-only classification
  - Verify read-only for SELECT, EXPLAIN
  - Verify modifying for INSERT, UPDATE, DELETE, CREATE

- [ ] **6.4** Test statement lifecycle
  - Test Properties() on prepared statement
  - Test error on closed statement
  - Test multiple calls return same values

## Validation Criteria

- [ ] All 30 statement types have constants
- [ ] All classification methods work correctly
- [ ] Properties() returns accurate information
- [ ] Public API matches specification
- [ ] All tests pass with deterministic results
- [ ] No performance regression in statement preparation
