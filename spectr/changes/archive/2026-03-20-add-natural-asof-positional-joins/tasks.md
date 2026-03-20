## 1. Parser Changes

- [ ] 1.1 Add `JoinTypeNatural`, `JoinTypeNaturalLeft`, `JoinTypeNaturalRight`, `JoinTypeNaturalFull`, `JoinTypeAsOf`, `JoinTypeAsOfLeft`, `JoinTypePositional` to JoinType enum
- [ ] 1.2 Add `Using []string` field to `JoinClause` struct
- [ ] 1.3 Parse NATURAL [LEFT|RIGHT|FULL] JOIN syntax
- [ ] 1.4 Parse ASOF [LEFT] JOIN ... ON ... syntax
- [ ] 1.5 Parse POSITIONAL JOIN syntax (no ON/USING clause)
- [ ] 1.6 Parse USING (col1, col2, ...) clause for all join types
- [ ] 1.7 Enforce ON/USING mutual exclusion in parser: error if both present, error if NATURAL has ON/USING, error if POSITIONAL has ON/USING
- [ ] 1.8 Write parser tests for all new join syntax variants (including mutual exclusion error cases)

## 2. Binder Changes

- [ ] 2.1 Implement NATURAL JOIN binding: find common columns between left and right tables, generate equi-join conditions, validate type coercibility of common columns (error on incompatible types), handle column deduplication in output
- [ ] 2.2 Implement USING clause binding: transform USING columns to explicit ON conditions, handle column deduplication in output
- [ ] 2.3 Implement ASOF JOIN binding: flatten ON clause into AND conjuncts, reject top-level OR, classify into equality vs inequality, validate exactly one inequality condition (>=, >, <=, or <), normalize so left-table operand is on left side
- [ ] 2.4 Implement POSITIONAL JOIN binding: no conditions needed, verify no ON/USING clause present
- [ ] 2.5 Write binder tests for all new join types

## 3. Planner Changes

- [ ] 3.1 Add logical plan node types for ASOF JOIN and POSITIONAL JOIN (NATURAL and USING rewrite to existing join types in binder)
- [ ] 3.2 Add physical plan node types: PhysicalAsOfJoin, PhysicalPositionalJoin
- [ ] 3.3 Wire planner to produce correct physical nodes for new join types
- [ ] 3.4 Planner inserts PhysicalSort operators below PhysicalAsOfJoin when inputs are not already sorted by equality keys + inequality key (ASC for >=, DESC for <=)

## 4. Executor: NATURAL JOIN and USING

- [ ] 4.1 Verify that NATURAL JOIN correctly executes after binder rewrite (should use existing hash join)
- [ ] 4.2 Verify that USING clause correctly executes after binder rewrite
- [ ] 4.3 Implement result column deduplication for NATURAL and USING joins via PhysicalProjection after join operator (common columns appear once, then remaining left, then remaining right)
- [ ] 4.4 Write integration tests for NATURAL JOIN with various column overlaps
- [ ] 4.5 Write integration tests for USING clause

## 5. Executor: ASOF JOIN

- [ ] 5.1 Create `internal/executor/asof_join.go` with `PhysicalAsOfJoinExecutor`
- [ ] 5.2 Implement right-side materialization and sorting by equality key + inequality key
- [ ] 5.3 Implement binary search within equality groups for nearest match
- [ ] 5.4 Support both >= and <= inequality operators
- [ ] 5.5 Implement ASOF LEFT JOIN (emit NULL for unmatched left rows)
- [ ] 5.6 Write integration tests for ASOF JOIN with time-series data
- [ ] 5.7 Write tests for edge cases: empty tables, single row, all matches, no matches

## 6. Executor: POSITIONAL JOIN

- [ ] 6.1 Create `internal/executor/positional_join.go` with `PhysicalPositionalJoinExecutor`
- [ ] 6.2 Implement zip-merge of left and right sides by position with persistent position counter across chunk boundaries (global ordinal matching)
- [ ] 6.3 Handle unequal lengths (NULL padding for shorter side)
- [ ] 6.4 Write integration tests for POSITIONAL JOIN
- [ ] 6.5 Write tests for edge cases: empty tables, unequal lengths, single row
