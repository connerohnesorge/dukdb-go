## 1. Parser: BY NAME Keyword

- [ ] 1.1 Add `SetOpUnionByName` and `SetOpUnionAllByName` to SetOpType enum
- [ ] 1.2 Parse `BY NAME` keywords after `UNION` and `UNION ALL`
- [ ] 1.3 Add parser unit tests for UNION BY NAME and UNION ALL BY NAME

## 2. Binder: Column Name Matching

- [ ] 2.1 Implement column name collection from both sides of UNION BY NAME
- [ ] 2.2 Build output column list (left columns first, then right-only columns)
- [ ] 2.3 Create left/right column mapping arrays (index → source index or -1)
- [ ] 2.4 Implement type promotion for matching columns
- [ ] 2.5 Handle case-insensitive column name matching
- [ ] 2.6 Error on duplicate column names within one side

## 3. Planner/Executor: Column Reordering

- [ ] 3.1 Add SetOpUnionByName, SetOpUnionAllByName to planner's SetOpType enum (logical.go)
- [ ] 3.2 Add conversion cases in parser→planner SetOpType switch (physical.go)
- [ ] 3.3 Add LeftMapping, RightMapping, OutputTypes fields to PhysicalSetOp struct
- [ ] 3.4 Add setOpName() cases for new types in binder (bind_stmt.go)
- [ ] 3.5 Add executor switch cases for new types in executeSetOp (operator.go)
- [ ] 3.6 Implement column reordering and NULL padding in set operation executor
- [ ] 3.7 Apply UNION (distinct) or UNION ALL (keep all) semantics after reordering

## 4. Integration Tests

- [ ] 4.1 Test: UNION ALL BY NAME with partially overlapping columns
- [ ] 4.2 Test: UNION BY NAME with deduplication
- [ ] 4.3 Test: No overlapping columns (all NULL padded)
- [ ] 4.4 Test: Same columns different order
- [ ] 4.5 Test: Type promotion for matching columns
- [ ] 4.6 Test: Case-insensitive column matching
- [ ] 4.7 Test: Chained UNION BY NAME (3+ queries)
