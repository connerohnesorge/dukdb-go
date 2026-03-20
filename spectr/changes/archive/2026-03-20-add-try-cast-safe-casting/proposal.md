# Change: Add TRY_CAST Safe Casting and :: Operator

## Why

DuckDB v1.4.3 supports `TRY_CAST(expr AS type)` which returns NULL on cast failure instead of raising an error, and the `::` operator as shorthand for CAST. The current dukdb-go parser has `CastExpr` but lacks a TryCast flag, meaning examples in the codebase that use `TRY_CAST` cannot execute correctly. Adding these constructs brings dukdb-go closer to full DuckDB SQL compatibility and enables safe data cleaning workflows.

## What Changes

- Extend `CastExpr` in `internal/parser/ast.go` with a `TryCast bool` field
- Add parser support for `TRY_CAST(expr AS type)` syntax
- Add parser support for the `::` postfix cast operator (syntactic sugar for `CAST`)
- Extend `BoundCastExpr` in `internal/binder/expressions.go` with a `TryCast bool` field
- Propagate the `TryCast` flag through the binder in `internal/binder/bind_expr.go`
- Modify executor cast evaluation in `internal/executor/expr.go` to catch conversion errors and return NULL when `TryCast` is true
- Add tests for TRY_CAST and `::` operator at parser, binder, and executor levels

## Impact

- Affected specs: `type-casting` (new capability)
- Affected code:
  - `internal/parser/ast.go` - Add `TryCast` field to `CastExpr`
  - `internal/parser/parser.go` - Parse `TRY_CAST` keyword and `::` operator
  - `internal/binder/expressions.go` - Add `TryCast` field to `BoundCastExpr`
  - `internal/binder/bind_expr.go` - Propagate `TryCast` flag
  - `internal/executor/expr.go` - Handle `TryCast` in cast evaluation
