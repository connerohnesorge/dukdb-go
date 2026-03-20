## 1. Parser: PREPARE/EXECUTE/DEALLOCATE Grammar

- [ ] 1.1 Add `PrepareStmt`, `ExecuteStmt`, `DeallocateStmt` AST nodes to `internal/parser/ast.go`
- [ ] 1.2 Implement PREPARE parsing: `PREPARE name AS statement` (recursive inner statement parse)
- [ ] 1.3 Implement EXECUTE parsing: `EXECUTE name` and `EXECUTE name(expr_list)`
- [ ] 1.4 Implement DEALLOCATE parsing: `DEALLOCATE [PREPARE] name` and `DEALLOCATE ALL`
- [ ] 1.5 Add StmtType constants (STATEMENT_TYPE_PREPARE/EXECUTE/DEALLOCATE) to root package
- [ ] 1.6 Update `paramCollector.collectStmt()` and `paramCounter.countStmt()` in parameters.go for new types
- [ ] 1.7 Add parser unit tests for all syntax variants

## 2. Engine: Connection-Level Storage

- [ ] 2.1 Add `sqlPreparedStatement` struct to `internal/engine/conn.go`
- [ ] 2.2 Add `sqlPrepared map[string]*sqlPreparedStatement` field to `EngineConn`
- [ ] 2.3 Initialize map in connection creation
- [ ] 2.4 Clean up prepared statements on connection close

## 3. Planner: Physical Plan Nodes

- [ ] 3.1 Add `PhysicalPrepare`, `PhysicalExecute`, `PhysicalDeallocate` to `internal/planner/physical.go`
- [ ] 3.2 Wire binding and planning for PREPARE/EXECUTE/DEALLOCATE statements

## 4. Executor: Statement Handlers

- [ ] 4.1 Implement `executePrepare`: parse inner stmt, bind, plan, store in connection map
- [ ] 4.2 Implement `executeExecute`: lookup plan, evaluate params, substitute, execute
- [ ] 4.3 Implement `executeDeallocate`: remove from map (or clear all)
- [ ] 4.4 Add duplicate name validation for PREPARE
- [ ] 4.5 Add parameter count validation for EXECUTE
- [ ] 4.6 Add missing name error handling for EXECUTE and DEALLOCATE

## 5. Integration Tests

- [ ] 5.1 End-to-end test: PREPARE and EXECUTE SELECT with parameters
- [ ] 5.2 End-to-end test: PREPARE INSERT and EXECUTE multiple times
- [ ] 5.3 End-to-end test: DEALLOCATE and verify removal
- [ ] 5.4 End-to-end test: DEALLOCATE ALL
- [ ] 5.5 End-to-end test: Error on duplicate PREPARE name
- [ ] 5.6 End-to-end test: Error on EXECUTE with wrong parameter count
- [ ] 5.7 End-to-end test: Connection-scoped isolation
- [ ] 5.8 End-to-end test: Connection close cleanup
