# Tasks: Recursive CTEs and Lateral Joins v1.4.3

## 1. Recursive CTE Parser Support

- [x] 1.1. Implement WITH RECURSIVE syntax parsing
- [x] 1.2. Add RecursiveCTE AST node definitions
- [x] 1.3. Support UNION ALL in recursive member

## 2. Recursive CTE Binder

- [x] 2.1. Implement CTE binding with column resolution
- [x] 2.2. Support multiple CTEs in WITH clause
- [x] 2.3. Validate recursive references

## 3. Recursive CTE Planner

- [x] 3.1. Implement PhysicalRecursiveCTE plan node
- [x] 3.2. Integrate recursive CTE into logical planning
- [x] 3.3. Support CTE references in physical plan generation

## 4. Recursive CTE Executor

- [x] 4.1. Implement recursive CTE execution engine (physical_recursive_cte.go)
- [x] 4.2. Implement work table management for iterative execution
- [x] 4.3. Implement cycle detection and termination

## 5. Lateral Join Support

- [x] 5.1. Implement LATERAL keyword parsing in FROM clause
- [x] 5.2. Implement lateral join binding with outer reference resolution
- [x] 5.3. Implement PhysicalLateralJoin plan node
- [x] 5.4. Implement lateral join executor (physical_lateral.go)

## 6. Testing

- [x] 6.1. Add CTE binder tests (TestBindCTEBasic, TestBoundCTEStructure, TestBindCTEWithMultipleCTEs)
- [x] 6.2. Add lateral join binder test (TestBoundTableRefLateral)
- [x] 6.3. Add recursive CTE executor tests (TestPhysicalRecursiveCTEStructure, TestCTEExecution, TestCTEWithTable)
- [x] 6.4. Add lateral join executor test (TestPhysicalLateralJoinStructure)
- [x] 6.5. Add multiple CTE test (TestMultipleCTEs)
