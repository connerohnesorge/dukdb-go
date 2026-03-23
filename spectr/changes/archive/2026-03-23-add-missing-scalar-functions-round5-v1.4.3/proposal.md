# Proposal: Add Missing Scalar Functions Round 5

## Summary

Implement 9 missing DuckDB v1.4.3 scalar functions: E(), INF(), NAN(), UUID()/GEN_RANDOM_UUID(), SPLIT_PART(), LOG(x, base) 2-argument variant, SHA512(), MILLISECOND(), MICROSECOND(). These are confirmed absent from evaluateFunctionCall() dispatch at internal/executor/expr.go:659.

## Motivation

These are standard SQL or DuckDB-specific functions that applications expect to be available. UUID generation is critical for ID-based applications. SPLIT_PART is a commonly-used PostgreSQL/DuckDB string function. Math constants (E, INF, NAN) complete the mathematical constant set (PI already exists). SHA512 completes the hash function library (MD5, SHA1, SHA256 exist). MILLISECOND/MICROSECOND complete the temporal extraction set.

## Scope

- **Executor**: Add 9 function cases in evaluateFunctionCall() main switch at expr.go:717
- **Binder**: Add type inference entries in inferFunctionResultType() at binder/utils.go:347
- **Query Cache**: Add UUID/GEN_RANDOM_UUID to volatileFuncs at query_cache.go:211-217
- **Hash helpers**: Add sha512Value() following sha256Value() pattern at hash.go:25

## Files Affected

- `internal/executor/expr.go` — evaluateFunctionCall() dispatch (line 717)
- `internal/executor/hash.go` — sha512Value() helper (after sha256Value at line 25)
- `internal/binder/utils.go` — inferFunctionResultType() (line 347)
- `internal/engine/query_cache.go` — volatileFuncs map (line 211)
