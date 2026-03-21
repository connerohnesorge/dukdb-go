# Tasks: Add IS DISTINCT FROM / IS NOT DISTINCT FROM

- [ ] 1. Add BinaryOp enum values — In `internal/parser/ast.go`, add `OpIsDistinctFrom` and `OpIsNotDistinctFrom` to the BinaryOp const block after `OpShiftRight` (line 788). Validate: AST compiles.

- [ ] 2. Parse IS [NOT] DISTINCT FROM syntax — In the parser's IS keyword handling (internal/parser/parser.go), after detecting IS [NOT], check for DISTINCT keyword followed by FROM. Create `BinaryExpr` with `OpIsDistinctFrom` or `OpIsNotDistinctFrom` and parse the right-hand expression. Validate: `SELECT 1 IS DISTINCT FROM 2` parses without error. `SELECT NULL IS NOT DISTINCT FROM NULL` parses correctly.

- [ ] 3. Evaluate IS [NOT] DISTINCT FROM in executor — In `internal/executor/expr.go`, in both NULL-handling blocks (around lines 337-355 and 502-508), add cases for `OpIsDistinctFrom` and `OpIsNotDistinctFrom`. IS DISTINCT FROM: both NULL → false, one NULL → true, both non-NULL → `compareValues(left, right) != 0`. IS NOT DISTINCT FROM: both NULL → true, one NULL → false, both non-NULL → `compareValues(left, right) == 0`. Validate: `SELECT 1 IS DISTINCT FROM 1` → false, `SELECT NULL IS DISTINCT FROM NULL` → false, `SELECT 1 IS DISTINCT FROM NULL` → true.

- [ ] 4. Add type inference in binder — Add cases for `OpIsDistinctFrom` and `OpIsNotDistinctFrom` returning `dukdb.TYPE_BOOLEAN` in the binary operator type inference in `internal/binder/`. Validate: queries compile without type errors.

- [ ] 5. Integration tests — Write tests covering: both non-NULL equal/different, left NULL, right NULL, both NULL, IS NOT DISTINCT FROM (all combinations), use in WHERE clause, use in JOIN ON condition, with various types (int, string, date). Verify no regressions in IS NULL/IS NOT NULL/IS TRUE/IS FALSE tests.
