# Tasks: Add Parameter Type Inference

## Phase 1: Infrastructure

- [ ] **1.1** Add expectedType parameter to bindExpr
  - Modify signature: `bindExpr(expr Expr, expectedType Type)`
  - Update all call sites to pass TYPE_UNKNOWN initially
  - Verify existing tests still pass

- [ ] **1.2** Add parameter tracking to BinderScope
  - Add `params map[int]Type` field
  - Initialize in NewBinder
  - Clear between statements

- [ ] **1.3** Update BoundParameter struct
  - Add ParamType field (replace constant TYPE_ANY)
  - Accept expectedType in bindParameter
  - Store in scope.params

- [ ] **1.4** Add type inference helper functions
  - `updateParamType(expr BoundExpr, newType Type)`
  - `resolveParamType(pos int) Type`
  - `hasTypeConflict(existing, new Type) bool`

## Phase 2: Column Comparison Inference

- [ ] **2.1** Implement binary expression type propagation
  - For EQ, NE, LT, LE, GT, GE operators
  - Bind left side first
  - Use left's type for right's expected type
  - Handle reverse case (param on left)

- [ ] **2.2** Add column type lookup
  - Implement `lookupColumnType(table, column string) Type`
  - Add column type cache to BinderScope
  - Handle missing tables/columns (return ANY)

- [ ] **2.3** Test column comparison inference
  - Test `WHERE col = ?` scenarios
  - Test `WHERE ? = col` scenarios
  - Test multiple columns same type
  - Test multiple columns different types

## Phase 3: DML Statement Inference

- [ ] **3.1** Implement INSERT value inference
  - Get table schema from catalog
  - Bind each value with column type
  - Handle column count mismatch

- [ ] **3.2** Implement UPDATE value inference
  - Get SET clause column types
  - Bind each value with column type
  - Handle WHERE clause separately

- [ ] **3.3** Test DML inference
  - Test INSERT with typed columns
  - Test UPDATE SET with typed columns
  - Test mixed column types

## Phase 4: Expression Context Inference

- [ ] **4.1** Implement arithmetic inference
  - For +, -, *, / operators
  - Use DOUBLE as expected type for operands
  - Handle mixed numeric contexts

- [ ] **4.2** Implement BETWEEN inference
  - Use column type for both bounds
  - Bind all three expressions with same type

- [ ] **4.3** Implement IN list inference
  - Get type from column
  - Apply to all list elements

- [ ] **4.4** Implement LIKE pattern inference
  - Pattern and string must be VARCHAR
  - Set VARCHAR for pattern parameter

## Phase 5: Function Argument Inference

- [ ] **5.1** Add function signature lookup
  - Extend catalog with function parameter types
  - Handle variadic functions
  - Handle overloaded functions

- [ ] **5.2** Implement function call binding
  - Look up function signature
  - Bind each argument with signature type
  - Handle missing signature (return ANY)

- [ ] **5.3** Test function inference
  - Test built-in functions (abs, length, etc.)
  - Test aggregate functions
  - Test unknown functions

## Phase 6: Integration

- [ ] **6.1** Wire to EngineStmt
  - Extract params from binder scope
  - Store in EngineStmt.paramTypes
  - Update ParamType() to use stored types

- [ ] **6.2** Wire to public API
  - Ensure Stmt.ParamType() returns inferred types
  - Update documentation

- [ ] **6.3** Comprehensive integration tests
  - Test all inference contexts together
  - Test edge cases (nulls, literals)
  - Verify match with duckdb-go behavior

## Validation Criteria

- [ ] Column comparison inference returns column type
- [ ] INSERT/UPDATE inference returns column type
- [ ] Arithmetic context returns DOUBLE
- [ ] Function arguments use signature types
- [ ] Conflicting contexts return ANY
- [ ] Unknown contexts return ANY
- [ ] All tests pass with deterministic results
- [ ] No performance regression in prepare
