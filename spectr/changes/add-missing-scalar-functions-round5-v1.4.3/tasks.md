# Tasks: Missing Scalar Functions Round 5

- [ ] 1. Math constants — Add E(), INF()/INFINITY(), NAN() cases near PI() at expr.go:1030. Return math.E, math.Inf(1), math.NaN(). Add type inference (TYPE_DOUBLE) in binder/utils.go:347. Validate: `SELECT E()` returns ~2.718.

- [ ] 2. UUID generation — Add UUID()/GEN_RANDOM_UUID() case near RANDOM at expr.go:1039. Import and use uuid.New().String() from github.com/google/uuid (already in go.mod). Add UUID/GEN_RANDOM_UUID to volatileFuncs at query_cache.go:211. Add type inference (TYPE_VARCHAR). Validate: `SELECT UUID()` returns valid UUID.

- [ ] 3. SPLIT_PART — Add SPLIT_PART(string, delim, idx) case near LENGTH at expr.go:1257. Use strings.Split(), 1-based indexing, negative index from end, out-of-range returns empty string. Add type inference (TYPE_VARCHAR). Validate: `SELECT SPLIT_PART('a-b-c', '-', 2)` returns 'b'.

- [ ] 4. LOG 2-arg variant — Modify existing LOG case at expr.go:840 to accept 1 or 2 arguments. When 2 args: math.Log(x)/math.Log(base). Preserve existing 1-arg LOG10 behavior. Validate: `SELECT LOG(8, 2)` returns 3.0 AND `SELECT LOG(100)` still returns 2.0.

- [ ] 5. SHA512 — Add sha512Value() to hash.go following sha256Value() pattern at line 25. Use crypto/sha512.Sum512(). Add SHA512 case near SHA256 at expr.go:1967. Add type inference (TYPE_VARCHAR). Validate: `SELECT LENGTH(SHA512('hello'))` returns 128.

- [ ] 6. MILLISECOND/MICROSECOND — Add both cases near SECOND at expr.go:2093. Use toTime() at temporal_functions.go:708, then ts.Nanosecond()/1_000_000 for ms, ts.Nanosecond()/1_000 for us. Add type inference (TYPE_INTEGER). Validate: `SELECT MILLISECOND(TIMESTAMP '2024-01-01 12:34:56.789')` returns 789.

- [ ] 7. Integration tests — Test all 9 functions with valid inputs, NULL propagation, edge cases (NaN equality, negative SPLIT_PART index, LOG with base=1, UUID uniqueness).
