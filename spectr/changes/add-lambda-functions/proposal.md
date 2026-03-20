# Change: Add Lambda Function Support for List Operations

## Why

DuckDB v1.4.3 supports lambda expressions as first-class arguments to list manipulation functions (`list_transform`, `list_filter`, `list_reduce`, `list_sort`). dukdb-go already has `TYPE_LAMBDA` in the type system and can store lambda expression strings in columns, but there is no parser support for lambda syntax (`x -> expr`, `(x, y) -> expr`) and no executor support for evaluating lambdas inline within function calls. This means users cannot write analytical queries that transform, filter, or reduce lists -- a core DuckDB feature for working with complex nested data.

## What Changes

- **Parser**: Add `LambdaExpr` AST node and parse `x -> expr` and `(x, y) -> expr` syntax within function argument positions
- **Binder**: Add `BoundLambdaExpr` to represent a lambda with bound parameter names and a bound body expression
- **Executor**: Implement lambda evaluation by binding lambda parameters to list element values during iteration
- **List functions**: Implement `list_transform`, `list_filter`, `list_reduce`, and `list_sort` as built-in functions that accept lambda arguments
- **Error handling**: Produce clear errors for arity mismatches (e.g., single-param lambda passed to `list_reduce`), type mismatches in lambda bodies, and invalid lambda syntax

## Impact

- Affected specs: `lambda-functions` (new capability)
- Affected code:
  - `internal/parser/ast.go` -- new `LambdaExpr` AST node
  - `internal/parser/parser.go` -- lambda arrow (`->`) parsing in expression contexts
  - `internal/binder/expressions.go` -- new `BoundLambdaExpr` type
  - `internal/binder/bind_expr.go` -- bind lambda parameters and body
  - `internal/executor/expr.go` -- evaluate `BoundLambdaExpr` and list lambda functions
  - `internal/executor/functions.go` (or equivalent) -- `list_transform`, `list_filter`, `list_reduce`, `list_sort` implementations
