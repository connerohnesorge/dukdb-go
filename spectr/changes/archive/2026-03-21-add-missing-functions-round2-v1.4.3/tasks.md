# Tasks: Missing Functions Round 2

- [ ] 1. Implement SHA1 — In `internal/executor/hash.go`, add `sha1Value()` following SHA256 pattern (line 24). Import `crypto/sha1`. In expr.go, add case "SHA1" near SHA256 (line 1749). In binder utils.go, add "SHA1" to VARCHAR return case. Validate: `SELECT SHA1('hello')` returns correct hash.

- [ ] 2. Implement SETSEED — In `internal/executor/expr.go`, add case "SETSEED" near RANDOM (line 953). Accept float between 0 and 1, store as connection setting via `ctx.conn.SetSetting("random_seed", ...)`. Update `randomValue()` in math.go to check for set seed. Validate: `SELECT SETSEED(0.5)` followed by `SELECT RANDOM()` produces deterministic output.

- [ ] 3. Implement LIST_VALUE / LIST_PACK — In `internal/executor/expr.go`, add case "LIST_VALUE", "LIST_PACK" near MAP (line 2222). Return `[]any` containing all arguments including NULLs. In binder, return TYPE_ANY. Validate: `SELECT LIST_VALUE(1, 2, 3)` → [1, 2, 3].

- [ ] 4. Implement ANY_VALUE aggregate — In `internal/executor/physical_aggregate.go`, add case "ANY_VALUE" after FIRST (line 707). Reuse `computeFirst()`. Register "ANY_VALUE" in aggregate list at operator.go:100-115. In binder, return first arg's type. Validate: `SELECT ANY_VALUE(name) FROM emp` returns a non-NULL name.

- [ ] 5. Implement HISTOGRAM aggregate — In `internal/executor/physical_aggregate.go`, add case "HISTOGRAM". Create `computeHistogram()` that builds `map[string]any` with value→count. Register in operator.go. In binder, return TYPE_ANY. Validate: `SELECT HISTOGRAM(x) FROM (VALUES (1), (1), (2)) t(x)` → {1: 2, 2: 1}.

- [ ] 6. Add ARG_MIN / ARG_MAX aliases — In `internal/executor/physical_aggregate.go`, change line 727 from `case "ARGMIN", "MIN_BY":` to `case "ARGMIN", "ARG_MIN", "MIN_BY":`. Same for ARGMAX at line 742. Register aliases in operator.go and binder. Validate: `SELECT ARG_MIN(name, age) FROM emp` → same result as ARGMIN.

- [ ] 7. Integration tests — Test all functions with edge cases: NULL inputs, empty tables, type validation. Verify SHA1 against known test vectors. Verify SETSEED reproducibility. Verify HISTOGRAM with various types.
