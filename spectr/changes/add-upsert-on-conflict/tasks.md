## 1. Parser: ON CONFLICT AST and Grammar

- [ ] 1.1 Add `OnConflictAction` enum and `OnConflictClause` struct to `internal/parser/ast.go`
- [ ] 1.2 Add `OnConflict *OnConflictClause` field to `InsertStmt`
- [ ] 1.3 Implement ON CONFLICT parsing in `internal/parser/parser.go` (after VALUES/SELECT, before RETURNING)
- [ ] 1.4 Support `DO NOTHING` and `DO UPDATE SET` action parsing
- [ ] 1.5 Support optional conflict target column list `(col1, col2)`
- [ ] 1.6 Support optional WHERE on conflict target and on update action
- [ ] 1.7 Add parser unit tests for all ON CONFLICT syntax variants

## 2. Binder: Conflict Resolution and EXCLUDED Scope

- [ ] 2.1 Add `BoundOnConflictClause` struct to `internal/binder/statements.go`
- [ ] 2.2 Add `BoundExcludedColumnRef` expression type to `internal/binder/expressions.go`
- [ ] 2.3 Add `OnConflict *BoundOnConflictClause` field to `BoundInsertStmt`
- [ ] 2.4 Implement conflict column validation: resolve against UNIQUE index or PK
- [ ] 2.5 Implement EXCLUDED pseudo-table binding scope for DO UPDATE SET expressions
- [ ] 2.6 Validate EXCLUDED references are only used within ON CONFLICT context
- [ ] 2.7 Handle inferred conflict target (when no columns specified, use PK)
- [ ] 2.8 Add binder unit tests for conflict resolution and EXCLUDED binding

## 3. Planner: Extend PhysicalInsert

- [ ] 3.1 Add `OnConflict *binder.BoundOnConflictClause` to `PhysicalInsert` in `internal/planner/physical.go`
- [ ] 3.2 Thread OnConflict through the planning pipeline in logical→physical translation
- [ ] 3.3 Verify OutputColumns works correctly when RETURNING + ON CONFLICT are combined

## 4. Executor: Upsert Execution

- [ ] 4.1 Add `excludedScope` struct and EXCLUDED evaluation helpers to executor (add case in evaluateExpr switch in expr.go)
- [ ] 4.2 Implement check-before-insert conflict detection using PK key map
- [ ] 4.3 Implement DO NOTHING path: skip conflicting rows
- [ ] 4.4 Implement DO UPDATE path: evaluate SET expressions with EXCLUDED bindings, apply UPDATE
- [ ] 4.5 Implement UPDATE WHERE filter for conditional updates
- [ ] 4.6 Support UNIQUE index conflict detection (beyond PK)
- [ ] 4.7 Implement batch-optimized conflict detection for bulk inserts
- [ ] 4.8 Integrate RETURNING clause with upsert (return inserted + updated rows)
- [ ] 4.9 Add WAL logging for upsert (INSERT entries for new rows, UPDATE entries for conflicts)

## 5. Storage: Unique Index Lookup Helpers and Catalog Bridge

- [ ] 5.1 Add `LookupUniqueIndex` method to storage Table for key existence checking
- [ ] 5.2 Add `GetUniqueIndexForColumns` method to find matching unique index by column set
- [ ] 5.3 Bridge catalog IndexDef to storage HashIndex (executor resolves index name → HashIndex at runtime)
- [ ] 5.4 Add unit tests for unique index lookup helpers

## 6. Integration Tests

- [ ] 6.1 End-to-end test: INSERT ... ON CONFLICT DO NOTHING with PK
- [ ] 6.2 End-to-end test: INSERT ... ON CONFLICT DO UPDATE SET with EXCLUDED
- [ ] 6.3 End-to-end test: DO UPDATE with WHERE filter
- [ ] 6.4 End-to-end test: INSERT ... SELECT ... ON CONFLICT
- [ ] 6.5 End-to-end test: UPSERT with RETURNING clause
- [ ] 6.6 End-to-end test: Composite key conflict detection
- [ ] 6.7 End-to-end test: UNIQUE index conflict (non-PK)
- [ ] 6.8 End-to-end test: Error on invalid conflict target
- [ ] 6.9 End-to-end test: Bulk upsert with mixed inserts and updates
- [ ] 6.10 End-to-end test: Concurrent upserts under SERIALIZABLE isolation
- [ ] 6.11 End-to-end test: DO NOTHING with RETURNING returns empty result for skipped rows
- [ ] 6.12 End-to-end test: NULL values in UNIQUE conflict columns allow duplicate NULLs
- [ ] 6.13 End-to-end test: DO UPDATE preserves non-updated column values (no DEFAULT applied)
- [ ] 6.14 End-to-end test: Error on partial composite key conflict target
- [ ] 6.15 End-to-end test: Error when DO UPDATE SET modifies conflict target column
