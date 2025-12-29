# Change: Implement SQL Execution Engine

## Why

**Current State**: dukdb-go HAS substantial execution engine code in internal/:
- `internal/parser/parser.go` (2219 lines) - SQL parser with AST
- `internal/binder/binder.go` (1672 lines) - Name/type resolution
- `internal/planner/logical.go` (379 lines) - Logical plan nodes
- `internal/planner/physical.go` (1011 lines) - Physical plan operators
- `internal/executor/operator.go` (1062 lines) - Operator implementations
- `internal/executor/expr.go` (1075 lines) - Expression evaluation
- `internal/engine/engine.go` (336 lines) - Engine framework
- `internal/engine/conn.go` (495 lines) - **Pipeline integration (Parse→Bind→Plan→Execute)**
- **Total**: 8576 lines of execution engine code

**KEY FINDING**: The pipeline IS ALREADY WIRED. `internal/engine/conn.go:25-115` implements:
```go
// Execute() - lines 38-68
stmt, err := parser.Parse(query)           // ✅ Parser integration
boundStmt, err := b.Bind(stmt)             // ✅ Binder integration
plan, err := p.Plan(boundStmt)             // ✅ Planner integration
result, err := exec.Execute(ctx, plan)     // ✅ Executor integration
```

**So what's the problem?** The pipeline works, but the RESULT FORMAT is wrong:

**Current Pipeline**:
```
Parse → Bind → Plan → Execute → []map[string]any (row format)
                                      ↑
                                   PROBLEM: Loses columnar benefits
```

**Problem Areas** (NOT "critical gaps" - these are refinements):
1. **Result Set Format**: `Query()` returns `[]map[string]any` instead of DataChunk-backed driver.Rows
2. **Incomplete Operators**: UPDATE/DELETE return `ErrorTypeNotImplemented` (operator.go:884-905)
3. **Expression Evaluator**: Row-at-a-time eval (1075 lines) - vectorization would require major rewrite
4. **Missing Tests**: No end-to-end tests, no JOIN/GROUP BY tests, no performance benchmarks
5. **Type Metadata**: Column types available but not exposed via driver.Rows for P0-3

**What Actually Needs Work**:
- ✅ Parser→Binder→Planner→Executor wiring: **ALREADY DONE** (conn.go)
- ✅ Error propagation: **ALREADY DONE** (conn.go shows error handling)
- ❌ Result Set: Need driver.Rows wrapping DataChunks (REAL GAP)
- ❌ Operator Completion: Implement aggregate, join, UPDATE/DELETE operators (REAL WORK)
- ❌ Testing: Comprehensive end-to-end tests (REAL GAP)

**This Proposal**: Complete operator implementations, implement ResultSet, add comprehensive testing.

## What

Integrate existing execution components into end-to-end query execution:

1. **Parser-to-Executor Pipeline** - Connect all stages
   - Wire Parse() → Bind() → Plan() → Execute() → Results
   - Add PipelineContext to track state across stages
   - Implement error propagation through pipeline

2. **DataChunk Integration** - Use P0-2 columnar storage
   - Replace ad-hoc row storage with DataChunk
   - Update all operators to produce/consume DataChunks
   - Integrate TypeInfo from P0-1a for type metadata

3. **EngineConn Query Execution** - Implement query methods
   - ExecContext() routes through execution pipeline
   - QueryContext() returns result set wrapped in Rows
   - PrepareContext() creates prepared statements with plans

4. **Result Set Implementation** - Return query results
   - Implement Rows interface with DataChunk backing
   - Support Scan() for row-by-row access
   - Support ColumnTypeDatabaseTypeName() for P0-3

5. **Operator Refinement** - Fix operator implementations
   - Scan: Read from storage, produce DataChunks
   - Filter: Evaluate predicates on DataChunks
   - Project: Select columns, produce new DataChunks
   - Aggregate: Group-by and aggregation functions
   - Join: Hash join implementation

6. **Error Handling** - Unified error types
   - Map parser/binder/planner errors to dukdb.Error
   - Add context to errors (line numbers, column names)
   - Propagate through pipeline consistently

**Scope Limitation**: This implements basic SELECT/INSERT/UPDATE/DELETE. Advanced features (CTEs, window functions, subqueries) deferred to P1.

## Implementation Strategy

This is a **REFACTORING & INTEGRATION** proposal, not greenfield.

**Existing Implementation** (internal/parser, internal/binder, internal/planner, internal/executor, internal/engine):
- ✅ SQL Parser (2219 lines) - SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.
- ✅ AST nodes (SelectStmt, InsertStmt, BinaryExpr, etc.)
- ✅ Binder (1672 lines) - Name resolution, type checking
- ✅ Logical planner (379 lines) - LogicalScan, LogicalFilter, LogicalProject
- ✅ Physical planner (1011 lines) - PhysicalScan, PhysicalFilter, PhysicalProject
- ✅ Operators (1062 lines) - Scan, Filter, Project partially implemented
- ✅ Expression evaluator (1075 lines) - BinaryOp, Function calls
- ✅ Engine framework (336 lines) - Catalog, Storage, Transactions
- ✅ Catalog (existing) - Schema, tables, columns
- ✅ Storage (existing) - Row groups, data blocks
- ❌ No pipeline connecting parser → binder → planner → executor
- ❌ No DataChunk integration (operators use ad-hoc [][]any)
- ❌ No result set implementation
- ❌ No EngineConn query routing
- ❌ No end-to-end tests

**Refactoring Goals (P0-4)**:
1. **Pipeline Integration**: Wire Parse() → Bind() → Plan() → Execute()
   - Add PipelineContext struct to carry state
   - Implement ExecPipeline(sql) that calls all stages
   - Add error handling at each stage

2. **DataChunk Conversion**: Replace [][]any with DataChunk
   - Update Scan operator to produce DataChunks
   - Update Filter/Project/Join to consume/produce DataChunks
   - Use P0-1a TypeInfo for column types

3. **Result Set**: Implement database/sql/driver.Rows
   - ResultSet wraps []DataChunk
   - Implements Columns(), Next(), Close()
   - Supports Scan() for row access

4. **EngineConn Integration**: Implement query methods
   - ExecContext() calls pipeline for DML
   - QueryContext() calls pipeline, returns ResultSet
   - PrepareContext() creates PreparedPlan

5. **Operator Refinement**: Fix incomplete operators
   - Scan: Integrate with storage layer
   - Filter: Evaluate on DataChunk validity masks
   - Project: Column projection
   - Aggregate: Group-by with hash table
   - Join: Hash join with build/probe phases

6. **Column Metadata for P0-3**: Expose result schema
   - Add ColumnTypeInfo() to PreparedPlan
   - Implement after binder resolves types

**Migration Path** (phased implementation):
- **Phase A**: DataChunk Integration (60-80h)
  - Update Scan operator to produce DataChunks
  - Update Filter/Project operators
  - Add TypeInfo integration

- **Phase B**: Result Set (20-30h)
  - Implement ResultSet wrapping DataChunks
  - Support row-by-row scanning
  - Add column metadata for P0-3

- **Phase C**: Operator Completion (80-100h)
  - Aggregate operator (SUM, COUNT, AVG, MIN, MAX)
  - GROUP BY with hash table
  - Join operator (hash join)
  - ORDER BY, LIMIT
  - DML operators (UPDATE, DELETE, INSERT)

- **Phase D**: Testing & Integration (30-40h)
  - End-to-end query tests
  - DML tests (INSERT/UPDATE/DELETE)
  - Concurrent access tests
  - Performance benchmarks
  - Error handling tests

**Total**: 190-250 hours (5-6 weeks single engineer, 4-5 weeks two engineers)

**Breaking Changes**: **NONE** (internal refactoring only, public API unchanged)

## Impact

### Users
- ✅ **Enables**: Full SQL query execution (SELECT, INSERT, UPDATE, DELETE)
- ✅ **Enables**: Result set iteration with Scan()
- ✅ **Enables**: Prepared statements with parameter binding
- ✅ **Completes**: P0-3 column metadata (ColumnTypeInfo available)
- ⚠️ **Breaking**: None (enabling feature, no API changes)

### Codebase
- **Refactored Files** (existing → improved):
  - `internal/parser/parser.go` → Add AST visitor patterns for pipeline
  - `internal/binder/binder.go` → Accept parser AST, return bound plan
  - `internal/planner/*.go` → Accept bound plan, return physical plan
  - `internal/executor/operator.go` → Produce/consume DataChunks
  - `internal/engine/conn.go` → Route queries through pipeline
- **New Files**:
  - `internal/executor/pipeline.go` - Pipeline orchestration
  - `internal/executor/result_set.go` - Rows implementation
  - `internal/executor/prepared_plan.go` - Prepared statement with plan
- **Dependencies**:
  - **Requires**: P0-1a TypeInfo ✅
  - **Requires**: P0-2 Vector/DataChunk ✅
  - **Uses**: P0-1b Binary Format (for persistence)
- **Enables**: P0-3 column metadata completion

### Risks
- **Integration Complexity**: Wiring 7400+ lines of code is error-prone
- **Performance**: DataChunk conversion may have overhead
- **Testing**: Need comprehensive end-to-end tests
- **Mitigation**: Phased rollout (A→B→C→D→E), extensive testing at each phase

### Alternatives Considered
1. **Rewrite from scratch** - Rejected: 7400 lines already exist and work
2. **Keep row-oriented** - Rejected: Defeats columnar storage benefits
3. **Partial integration** - Rejected: Database must fully work for P0

## Success Criteria

### Phase A: Pipeline Integration
- [ ] Parse() → Bind() → Plan() → Execute() wired together
- [ ] EngineConn.ExecContext() routes through pipeline
- [ ] EngineConn.QueryContext() routes through pipeline
- [ ] Simple SELECT 1 query executes end-to-end

### Phase B: DataChunk Integration
- [ ] Scan operator produces DataChunks
- [ ] Filter operator consumes/produces DataChunks
- [ ] Project operator consumes/produces DataChunks
- [ ] TypeInfo integrated for column types
- [ ] SELECT * FROM table query returns DataChunk results

### Phase C: Result Set
- [ ] ResultSet implements driver.Rows interface
- [ ] Columns() returns column names
- [ ] Next() iterates rows
- [ ] Scan() copies row values
- [ ] ColumnTypeDatabaseTypeName() returns type names (enables P0-3)

### Phase D: Operator Completion
- [ ] Aggregate operator: SUM, COUNT, AVG, MIN, MAX
- [ ] GROUP BY with hash table
- [ ] Hash join operator
- [ ] ORDER BY sorting
- [ ] LIMIT/OFFSET support

### Phase E: Testing & Validation
- [ ] All execution-engine spec scenarios pass (292 lines)
- [ ] End-to-end SELECT tests
- [ ] End-to-end INSERT/UPDATE/DELETE tests
- [ ] Concurrent query execution tests
- [ ] Error handling tests for all error types
- [ ] Performance: 1M row scan <1 second

## Dependencies

### Required Before
- ✅ P0-1a: Core TypeInfo (COMPLETED)
- ✅ P0-2: Vector/DataChunk (APPROVED, ready for implementation)
- ✅ Existing parser/binder/planner/executor code (EXISTS, 7418 lines)

### Enables After
- **P0-3 Completion**: Column metadata methods (ColumnTypeInfo, ColumnType, ColumnName)
- **Full Database**: End-to-end SQL execution
- **Prepared Statements**: Full prepared statement support with plans
- **Query Optimization**: Foundation for P1 query optimizer

## Related Specs

- `execution-engine` - IMPLEMENTS 292-line spec (all 11 requirements)
- `query-execution` - IMPLEMENTS query execution requirements
- `result-handling` - IMPLEMENTS result set requirements
- `prepared-statements` - EXTENDS with prepared plan support
- `data-chunk-api` - USES for columnar storage

## Rollout Plan

### Phase A: DataChunk Integration (60-80 hours, 1.5-2 weeks)
- Update Scan operator for DataChunk output
- Update Filter/Project operators
- Integrate TypeInfo from P0-1a
- **Milestone**: SELECT * FROM table returns DataChunk results

### Phase B: Result Set (20-30 hours, 0.5-0.75 weeks)
- Implement ResultSet wrapping DataChunks
- Implement driver.Rows interface
- Add column metadata methods for P0-3
- **Milestone**: Result sets iterable with Scan()

### Phase C: Operator Completion (80-100 hours, 2-2.5 weeks)
- Implement Aggregate operator (SUM, COUNT, AVG, MIN, MAX)
- Implement GROUP BY with hash table
- Implement Join operator (hash join)
- Add ORDER BY, LIMIT support
- Complete DML operators (UPDATE, DELETE, INSERT)
- **Milestone**: Complex queries work (GROUP BY, JOIN, ORDER BY, DML)

### Phase D: Testing & Integration (30-40 hours, 0.75-1 week)
- End-to-end test suite (JOIN, GROUP BY, DML)
- Performance benchmarks with CI tracking
- Concurrent access tests (race detection)
- Error handling tests with specific SQL scenarios
- **Milestone**: All spec scenarios pass, production-ready

**Total Timeline**: 190-250 hours (5-6 weeks single engineer)
**Parallel Timeline**: 4-5 weeks with 2 engineers (20% speedup through within-phase parallelization)

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec validated (spectr validate implement-execution-engine)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed (P0-1a ✅, P0-2 ✅)
- [ ] Phased approach approved (A→B→C→D)
- [ ] Testing strategy approved (end-to-end tests per phase)
- [ ] Timeline realistic (5-6 weeks single engineer, 4-5 weeks two engineers)
