# Tasks: Add IS DISTINCT FROM / IS NOT DISTINCT FROM

- [ ] 1. Add BinaryOp enum values — In `internal/parser/ast.go`, add `OpIsDistinctFrom` and `OpIsNotDistinctFrom` to the BinaryOp const block after `OpShiftRight` (line 788). Validate: AST compiles.

- [ ] 2. Parse IS [NOT] DISTINCT FROM syntax — In `parseComparisonExpr()` at `internal/parser/parser.go:4121`, restructure the IS keyword handler. After consuming IS [NOT], check for DISTINCT keyword BEFORE the existing NULL expectation. When DISTINCT is found, consume it, expect FROM keyword, parse right-hand expression via `parseBitwiseOrExpr()`, and return `BinaryExpr{Left: left, Op: OpIsDistinctFrom, Right: right}` (or OpIsNotDistinctFrom for IS NOT DISTINCT FROM). The existing IS [NOT] NULL path remains unchanged as the fallthrough. Validate: `SELECT 1 IS DISTINCT FROM 2` parses without error. `SELECT NULL IS NOT DISTINCT FROM NULL` parses correctly. `SELECT x IS NULL` still works.

- [ ] 3. Evaluate IS [NOT] DISTINCT FROM in executor — In `internal/executor/expr.go`, in both NULL-handling blocks (evaluateBinaryExpr around lines 347-352, and evaluateParserBinaryOp around lines 502-505), add cases for `OpIsDistinctFrom` and `OpIsNotDistinctFrom`. IS DISTINCT FROM: both NULL → false, one NULL → true, both non-NULL → `compareValues(left, right) != 0` (compareValues is at expr.go:3766). IS NOT DISTINCT FROM: both NULL → true, one NULL → false, both non-NULL → `compareValues(left, right) == 0`. Validate: `SELECT 1 IS DISTINCT FROM 1` → false, `SELECT NULL IS DISTINCT FROM NULL` → false, `SELECT 1 IS DISTINCT FROM NULL` → true.

- [ ] 4. Add type inference in binder — In `internal/binder/bind_expr.go:254-256`, add `parser.OpIsDistinctFrom` and `parser.OpIsNotDistinctFrom` to the existing OpIs/OpIsNot case that returns `dukdb.TYPE_BOOLEAN`. Validate: queries compile without type errors.

- [ ] 5. Integration tests — Write tests covering: both non-NULL equal/different, left NULL, right NULL, both NULL, IS NOT DISTINCT FROM (all combinations), use in WHERE clause, use in JOIN ON condition, with various types (int, string, date). Verify no regressions in IS NULL/IS NOT NULL tests.
