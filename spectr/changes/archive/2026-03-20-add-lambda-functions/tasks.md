# Lambda Functions Implementation Tasks

## Phase 1: AST and Parser

### 1.1 LambdaExpr AST Node
- [ ] 1.1.1 Add `LambdaExpr` struct to `internal/parser/ast.go` with `Params []string` and `Body Expr`
- [ ] 1.1.2 Implement `exprNode()` marker method on `LambdaExpr`
- [ ] 1.1.3 Add `LambdaExpr` case to any AST visitor/walker utilities that enumerate expression types

### 1.2 Parser: Arrow Token Disambiguation (Lookahead-Based)
- [ ] 1.2.1 Implement `peekTokenAt(n)` helper that returns the token n positions ahead without consuming
- [ ] 1.2.2 Implement lookahead check in `parseFunctionArg()`: when `identifier -> non-string-non-integer`, parse as lambda; otherwise fall through to `parseExpr()` for JSON extract
- [ ] 1.2.3 Implement `tryParseParenthesizedIdentList()` for multi-parameter lambda detection with backtracking
- [ ] 1.2.4 Verify that `identifier -> string_literal` and `identifier -> integer_literal` inside function args are still parsed as JSON extract

### 1.3 Parser: Lambda Expression Parsing
- [ ] 1.3.1 Implement `parseFunctionArg()` method that handles both single-parameter lambdas (`x -> expr`) and regular expressions
- [ ] 1.3.2 Extend `parseFunctionArg()` for multi-parameter lambdas (`(x, y) -> expr`)
- [ ] 1.3.3 Integrate `parseFunctionArg()` into function argument parsing path (called from `parseFunctionArgs` instead of `parseExpr`)
- [ ] 1.3.4 Ensure `->` retains JSON extract semantics outside function argument contexts (no changes needed to general expression parser)
- [ ] 1.3.5 Write parser unit tests for `list_transform([1,2,3], x -> x * 2)`
- [ ] 1.3.6 Write parser unit tests for `list_reduce([1,2,3], (x, y) -> x + y)`
- [ ] 1.3.7 Write parser unit tests for nested expressions in lambda body (`x -> x * 2 + 1`)
- [ ] 1.3.8 Write parser unit tests verifying `->` still works for JSON extract outside function args

---

## Phase 2: Binder

### 2.1 BoundLambdaExpr Type
- [ ] 2.1.1 Add `BoundLambdaExpr` struct to `internal/binder/expressions.go` with `Params`, `Body`, `ResType`
- [ ] 2.1.2 Implement `boundExprNode()` and `ResultType()` methods
- [ ] 2.1.3 Add `BoundLambdaExpr` case to any binder expression utilities

### 2.2 Lambda Binding Logic (Function-Level Context)
- [ ] 2.2.1 Implement `pushLambdaScope()` to introduce lambda parameter names as temporary column references in the binder
- [ ] 2.2.2 Implement `popLambdaScope()` to remove lambda parameter names from the binder
- [ ] 2.2.3 Implement `bindLambdaExpr()` that pushes scope, binds body, pops scope (called ONLY from bindFunctionCall, never from generic bindExpr)
- [ ] 2.2.4 Implement `bindListLambdaFunc()` in `bindFunctionCall()` that: (a) binds the list arg first, (b) infers element type, (c) binds the lambda with that type context
- [ ] 2.2.5 Implement `inferListElementType()` using BoundArrayExpr.ElemType for literals, column type metadata for column refs, VARCHAR fallback for complex expressions
- [ ] 2.2.6 Add error case in generic `bindExpr()` for LambdaExpr ("lambda expression only valid as function argument")
- [ ] 2.2.7 Write binder unit tests for single-parameter lambda binding
- [ ] 2.2.8 Write binder unit tests for two-parameter lambda binding (list_reduce)
- [ ] 2.2.9 Write binder error tests for arity mismatch (e.g., two-param lambda in list_filter)
- [ ] 2.2.10 Write binder error test for lambda in generic bindExpr context

---

## Phase 3: Executor

### 3.1 Lambda Invocation Infrastructure (Scope Stack)
- [ ] 3.1.1 Add `LambdaScope` struct and `LambdaScopes []LambdaScope` field to `ExecutionContext`
- [ ] 3.1.2 Implement `pushLambdaScope()`, `popLambdaScope()`, and `resolveLambdaParam()` on `ExecutionContext`
- [ ] 3.1.3 Modify column reference resolution in expression evaluator to check `ctx.resolveLambdaParam(name)` before row map lookup
- [ ] 3.1.4 Implement `invokeLambda()` on Executor using scope stack (push scope, evaluate body, defer pop scope -- no row map mutation)
- [ ] 3.1.5 Add `BoundLambdaExpr` case to `evaluateExpr` switch (return error: cannot evaluate directly)
- [ ] 3.1.6 Special-case lambda-accepting functions in `evaluateFunctionCall` BEFORE eager arg evaluation, passing `[]BoundExpr` directly
- [ ] 3.1.7 Write unit test for `invokeLambda` with single parameter
- [ ] 3.1.8 Write unit test for `invokeLambda` with two parameters
- [ ] 3.1.9 Write unit test for parameter shadowing via scope stack (lambda param shadows column name, column restored after)

### 3.2 list_transform Implementation
- [ ] 3.2.1 Implement `evalListTransform()` -- apply single-param lambda to each list element
- [ ] 3.2.2 Register `list_transform` in function dispatcher
- [ ] 3.2.3 Write integration test: `SELECT list_transform([1,2,3], x -> x * 2)` returns `[2,4,6]`
- [ ] 3.2.4 Write integration test: `SELECT list_transform([1,2,3], x -> x + 10)` returns `[11,12,13]`
- [ ] 3.2.5 Write integration test: list_transform with empty list returns empty list
- [ ] 3.2.6 Write integration test: list_transform with NULL elements preserves NULLs
- [ ] 3.2.7 Write error test: list_transform with non-list first argument

### 3.3 list_filter Implementation
- [ ] 3.3.1 Implement `evalListFilter()` -- keep elements where lambda returns true
- [ ] 3.3.2 Register `list_filter` in function dispatcher
- [ ] 3.3.3 Write integration test: `SELECT list_filter([1,2,3,4], x -> x > 2)` returns `[3,4]`
- [ ] 3.3.4 Write integration test: `SELECT list_filter([1,2,3,4], x -> x % 2 = 0)` returns `[2,4]`
- [ ] 3.3.5 Write integration test: list_filter with no matches returns empty list
- [ ] 3.3.6 Write integration test: list_filter with all matches returns original list
- [ ] 3.3.7 Write error test: list_filter lambda returning non-boolean

### 3.4 list_reduce Implementation
- [ ] 3.4.1 Implement `evalListReduce()` -- accumulate with two-param lambda
- [ ] 3.4.2 Register `list_reduce` in function dispatcher
- [ ] 3.4.3 Write integration test: `SELECT list_reduce([1,2,3], (x, y) -> x + y)` returns `6`
- [ ] 3.4.4 Write integration test: `SELECT list_reduce([1,2,3,4], (x, y) -> x * y)` returns `24`
- [ ] 3.4.5 Write integration test: list_reduce with single element returns that element
- [ ] 3.4.6 Write integration test: list_reduce with empty list returns NULL
- [ ] 3.4.7 Write error test: list_reduce with single-param lambda

### 3.5 list_sort Implementation
- [ ] 3.5.1 Implement `evalListSort()` -- sort by lambda-derived key
- [ ] 3.5.2 Register `list_sort` in function dispatcher
- [ ] 3.5.3 Write integration test: `SELECT list_sort([3,1,2], x -> x)` returns `[1,2,3]`
- [ ] 3.5.4 Write integration test: `SELECT list_sort([3,1,2], x -> -x)` returns `[3,2,1]`
- [ ] 3.5.5 Write integration test: list_sort without lambda (natural sort)
- [ ] 3.5.6 Write integration test: list_sort with empty list returns empty list
- [ ] 3.5.7 Write integration test: list_sort stability (equal keys preserve order)

---

## Phase 4: Integration and Edge Cases

### 4.1 Type Support
- [ ] 4.1.1 Verify list_transform works with integer lists
- [ ] 4.1.2 Verify list_transform works with string lists (`x -> upper(x)`)
- [ ] 4.1.3 Verify list_filter works with float lists
- [ ] 4.1.4 Verify list_reduce works with mixed numeric types (int + float)
- [ ] 4.1.5 Verify list_sort works with string keys

### 4.2 NULL Handling
- [ ] 4.2.1 Test list_transform with NULL list argument returns NULL
- [ ] 4.2.2 Test list_filter with NULL elements in list
- [ ] 4.2.3 Test list_reduce with NULL elements in list
- [ ] 4.2.4 Test lambda body that produces NULL values

### 4.3 Complex Expressions in Lambda Body
- [ ] 4.3.1 Test lambda body with function calls: `x -> abs(x)`
- [ ] 4.3.2 Test lambda body with CASE expressions: `x -> CASE WHEN x > 0 THEN x ELSE 0 END`
- [ ] 4.3.3 Test lambda body referencing outer columns: `x -> x + outer_col`
- [ ] 4.3.4 Test lambda body with nested arithmetic: `x -> (x * 2) + (x / 3)`

### 4.4 Error Handling
- [ ] 4.4.1 Test error for lambda outside function arguments (should parse as JSON extract or error)
- [ ] 4.4.2 Test error for wrong lambda arity in list_reduce (1-param instead of 2)
- [ ] 4.4.3 Test error for wrong lambda arity in list_transform (2-param instead of 1)
- [ ] 4.4.4 Test error for non-list first argument to all list functions

---

## Phase 5: Quality and Validation

### 5.1 Testing
- [ ] 5.1.1 Run full test suite: `nix develop -c tests`
- [ ] 5.1.2 Run linter: `nix develop -c lint`
- [ ] 5.1.3 Verify no regressions in existing JSON extract (`->`) tests
- [ ] 5.1.4 Verify no regressions in existing lambda column storage tests

### 5.2 Documentation
- [ ] 5.2.1 Add godoc comments to `LambdaExpr`, `BoundLambdaExpr`, and all list functions
- [ ] 5.2.2 Add inline code comments explaining `->` disambiguation logic
- [ ] 5.2.3 Add example query comments in test files

---

## Notes on Implementation Order

**Recommended sequence:**

1. **Phase 1 first**: Parser is the foundation; without lambda AST nodes, nothing else works
2. **Phase 2 next**: Binder must resolve lambda parameters before execution
3. **Phase 3 next**: Executor implements the actual list functions
4. **Phase 4 after**: Integration and edge case testing validates correctness
5. **Phase 5 last**: Quality gates and documentation

**Key infrastructure already present:**
- `TYPE_LAMBDA` in type system
- LIST type support (`[]any` representation)
- `FunctionCall` AST node and `evaluateFunctionCall` dispatcher
- `compareValues` for sort comparisons
- `->` token lexing (as JSON extract)

**What needs implementation:**
- `LambdaExpr` AST node
- `parseFunctionArg()` with lookahead-based lambda detection (no global parser flags)
- `peekTokenAt(n)` helper for lookahead
- `BoundLambdaExpr` and lambda scope management in binder
- `bindListLambdaFunc()` and `inferListElementType()` in `bindFunctionCall()`
- Error case for `LambdaExpr` in generic `bindExpr()`
- `LambdaScope` stack on `ExecutionContext` with `pushLambdaScope`/`popLambdaScope`/`resolveLambdaParam`
- Column reference resolution change to check lambda scope stack before row map
- Special-cased lazy dispatch in `evaluateFunctionCall` for lambda-accepting functions
- `invokeLambda` evaluation helper using scope stack (no row map mutation)
- Four list function implementations with `[]BoundExpr` signature
