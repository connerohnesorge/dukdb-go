# Lambda Functions Specification

## ADDED Requirements

### Requirement: Lambda Expression Parsing

The parser SHALL recognize lambda expression syntax within function argument positions using lookahead-based disambiguation in `parseFunctionArg()`. Single-parameter lambdas use the form `identifier -> expr` where the right-hand side of `->` is NOT a string literal or integer literal. Multi-parameter lambdas use the form `(identifier, identifier, ...) -> expr`. The `->` token SHALL retain its JSON extract semantics in all non-function-argument contexts and when followed by a string or integer literal inside function arguments (e.g., `func(col->'key')` remains JSON extract).

#### Scenario: Parse single-parameter lambda in function call

- WHEN the parser encounters `list_transform([1,2,3], x -> x * 2)`
- THEN a `FunctionCall` AST node is produced with name `list_transform`
- AND the second argument is a `LambdaExpr` with `Params: ["x"]` and body `x * 2`

#### Scenario: Parse multi-parameter lambda in function call

- WHEN the parser encounters `list_reduce([1,2,3], (x, y) -> x + y)`
- THEN a `FunctionCall` AST node is produced with name `list_reduce`
- AND the second argument is a `LambdaExpr` with `Params: ["x", "y"]` and body `x + y`

#### Scenario: Preserve JSON extract outside function arguments

- WHEN the parser encounters `SELECT col -> 'key' FROM t`
- THEN a `BinaryExpr` with `Op: OpJSONExtract` is produced
- AND no `LambdaExpr` is generated

#### Scenario: JSON extract inside function argument (not a lambda)

- WHEN the parser encounters `some_func(col -> 'key')`
- THEN a `BinaryExpr` with `Op: OpJSONExtract` is produced for that argument
- AND no `LambdaExpr` is generated (because `->` is followed by a string literal)

#### Scenario: JSON extract with integer index inside function argument

- WHEN the parser encounters `some_func(col -> 0)`
- THEN a `BinaryExpr` with `Op: OpJSONExtract` is produced for that argument
- AND no `LambdaExpr` is generated (because `->` is followed by an integer literal)

#### Scenario: Lambda body with complex expression

- WHEN the parser encounters `list_transform([1,2,3], x -> x * 2 + 1)`
- THEN the lambda body is parsed as `(x * 2) + 1` respecting operator precedence

#### Scenario: Lambda body with function call

- WHEN the parser encounters `list_transform(['a','b'], x -> upper(x))`
- THEN the lambda body contains a `FunctionCall` to `upper` with argument referencing lambda parameter `x`

### Requirement: Lambda Expression Binding

The binder SHALL bind lambda expressions exclusively within `bindFunctionCall()` for known lambda-accepting functions (list_transform, list_filter, list_reduce, list_sort), NOT in generic `bindExpr()`. The binder SHALL first bind the list argument to determine its element type, then use that type context to bind the lambda parameters. Parameter types SHALL be inferred from: (a) `BoundArrayExpr.ElemType` for array literals, (b) column type metadata for column references, or (c) VARCHAR as a fallback. Lambda parameter names SHALL be introduced as temporary column references in a scoped context. The binder SHALL produce a `BoundLambdaExpr` containing the bound body expression and its result type. Encountering a `LambdaExpr` in generic `bindExpr()` SHALL produce an error ("lambda expression only valid as function argument").

#### Scenario: Bind single-parameter lambda with integer list

- WHEN binding `list_transform([1,2,3], x -> x * 2)`
- THEN lambda parameter `x` is bound with type INTEGER
- AND the body `x * 2` is bound as a `BoundBinaryExpr` with INTEGER result type

#### Scenario: Bind two-parameter lambda for list_reduce

- WHEN binding `list_reduce([1,2,3], (x, y) -> x + y)`
- THEN lambda parameters `x` and `y` are both bound with type INTEGER
- AND the body `x + y` is bound as a `BoundBinaryExpr`

#### Scenario: Lambda parameter arity mismatch produces error

- WHEN binding `list_filter([1,2,3], (x, y) -> x > y)`
- THEN a binding error is produced indicating `list_filter` expects a 1-parameter lambda but got 2

#### Scenario: Lambda body references outer column

- GIVEN a table `t` with column `threshold` of type INTEGER
- WHEN binding `SELECT list_filter(values, x -> x > threshold) FROM t`
- THEN lambda parameter `x` resolves to the list element type
- AND `threshold` resolves to the column reference from table `t`

### Requirement: list_transform Function

The executor SHALL implement the `list_transform(list, lambda)` function, which applies a single-parameter lambda expression to each element of a list and returns a new list containing the transformed values. The result list has the same length as the input list. NULL elements in the input list are passed to the lambda (the lambda decides the output for NULLs). If the input list is NULL, the result is NULL.

#### Scenario: Transform integers by doubling

- WHEN executing `SELECT list_transform([1, 2, 3], x -> x * 2)`
- THEN the result is `[2, 4, 6]`

#### Scenario: Transform integers by adding constant

- WHEN executing `SELECT list_transform([10, 20, 30], x -> x + 5)`
- THEN the result is `[15, 25, 35]`

#### Scenario: Transform with function call in lambda body

- WHEN executing `SELECT list_transform([-1, 2, -3], x -> abs(x))`
- THEN the result is `[1, 2, 3]`

#### Scenario: Transform empty list

- WHEN executing `SELECT list_transform([], x -> x * 2)`
- THEN the result is `[]` (empty list)

#### Scenario: Transform NULL list

- WHEN executing `SELECT list_transform(NULL, x -> x * 2)`
- THEN the result is NULL

#### Scenario: Transform list with NULL elements

- WHEN executing `SELECT list_transform([1, NULL, 3], x -> x * 2)`
- THEN the result is `[2, NULL, 6]`

#### Scenario: Transform with arithmetic expression

- WHEN executing `SELECT list_transform([1, 2, 3], x -> x * x + 1)`
- THEN the result is `[2, 5, 10]`

### Requirement: list_filter Function

The executor SHALL implement the `list_filter(list, lambda)` function, which applies a single-parameter lambda expression to each element of a list and returns a new list containing only the elements for which the lambda returns true. The lambda MUST return a boolean value. If the input list is NULL, the result is NULL. Elements that are NULL are passed to the lambda; if the lambda returns NULL or false for them, they are excluded.

#### Scenario: Filter integers greater than threshold

- WHEN executing `SELECT list_filter([1, 2, 3, 4, 5], x -> x > 2)`
- THEN the result is `[3, 4, 5]`

#### Scenario: Filter even numbers

- WHEN executing `SELECT list_filter([1, 2, 3, 4], x -> x % 2 = 0)`
- THEN the result is `[2, 4]`

#### Scenario: Filter with no matches

- WHEN executing `SELECT list_filter([1, 2, 3], x -> x > 10)`
- THEN the result is `[]` (empty list)

#### Scenario: Filter with all matches

- WHEN executing `SELECT list_filter([1, 2, 3], x -> x > 0)`
- THEN the result is `[1, 2, 3]`

#### Scenario: Filter empty list

- WHEN executing `SELECT list_filter([], x -> x > 0)`
- THEN the result is `[]` (empty list)

#### Scenario: Filter NULL list

- WHEN executing `SELECT list_filter(NULL, x -> x > 0)`
- THEN the result is NULL

#### Scenario: Filter with outer column reference

- GIVEN a table with column `min_val` containing value 3
- WHEN executing `SELECT list_filter([1, 2, 3, 4, 5], x -> x >= min_val)`
- THEN the result is `[3, 4, 5]`

### Requirement: list_reduce Function

The executor SHALL implement the `list_reduce(list, lambda)` function, which reduces a list to a single value by applying a two-parameter lambda expression cumulatively. The first parameter is the accumulator (initialized to the first list element) and the second parameter is the current element (starting from the second element). If the list has a single element, that element is returned. If the list is empty, NULL is returned. If the list is NULL, NULL is returned.

#### Scenario: Sum of integers

- WHEN executing `SELECT list_reduce([1, 2, 3], (x, y) -> x + y)`
- THEN the result is `6`

#### Scenario: Product of integers

- WHEN executing `SELECT list_reduce([1, 2, 3, 4], (x, y) -> x * y)`
- THEN the result is `24`

#### Scenario: Reduce single-element list

- WHEN executing `SELECT list_reduce([42], (x, y) -> x + y)`
- THEN the result is `42`

#### Scenario: Reduce empty list

- WHEN executing `SELECT list_reduce([], (x, y) -> x + y)`
- THEN the result is NULL

#### Scenario: Reduce NULL list

- WHEN executing `SELECT list_reduce(NULL, (x, y) -> x + y)`
- THEN the result is NULL

#### Scenario: Reduce with subtraction

- WHEN executing `SELECT list_reduce([10, 3, 2], (x, y) -> x - y)`
- THEN the result is `5` (10 - 3 - 2)

#### Scenario: Reduce arity mismatch produces error

- WHEN executing `SELECT list_reduce([1, 2, 3], x -> x + 1)`
- THEN an error is produced indicating list_reduce requires a 2-parameter lambda

### Requirement: list_sort Function

The executor SHALL implement the `list_sort(list, lambda)` function, which sorts a list using a single-parameter lambda as a key extraction function. The lambda is applied to each element to derive a sort key, and elements are ordered by their sort keys in ascending order. The sort MUST be stable (equal keys preserve original order). If the input list is NULL, the result is NULL. If the input list is empty, the result is an empty list. `list_sort` MAY also be called without a lambda argument for natural ascending sort.

#### Scenario: Sort by identity (natural order)

- WHEN executing `SELECT list_sort([3, 1, 2], x -> x)`
- THEN the result is `[1, 2, 3]`

#### Scenario: Sort descending via negation

- WHEN executing `SELECT list_sort([3, 1, 2], x -> -x)`
- THEN the result is `[3, 2, 1]`

#### Scenario: Sort empty list

- WHEN executing `SELECT list_sort([], x -> x)`
- THEN the result is `[]` (empty list)

#### Scenario: Sort NULL list

- WHEN executing `SELECT list_sort(NULL, x -> x)`
- THEN the result is NULL

#### Scenario: Sort single-element list

- WHEN executing `SELECT list_sort([42], x -> x)`
- THEN the result is `[42]`

#### Scenario: Sort stability with equal keys

- GIVEN a list `[3, 1, 2, 1]` where the two `1` values have distinct positions
- WHEN executing `SELECT list_sort([3, 1, 2, 1], x -> x)`
- THEN the result is `[1, 1, 2, 3]`
- AND the relative order of the two `1` values is preserved

#### Scenario: Sort without lambda (natural sort)

- WHEN executing `SELECT list_sort([3, 1, 2])`
- THEN the result is `[1, 2, 3]`

### Requirement: Lambda Parameter Scoping

During lambda execution, lambda parameter names SHALL be resolved via a lambda scope stack on the `ExecutionContext`, NOT by mutating the row map. The executor SHALL push a new scope containing parameter bindings before evaluating the lambda body and pop it after (using defer for error safety). Column reference resolution SHALL check the lambda scope stack (top-first) before falling back to the row map. This ensures that lambda parameters take precedence within the lambda body, are cleanly isolated from the row context, and support future nested lambda scenarios.

#### Scenario: Lambda parameter shadows column name

- GIVEN a table `t` with column `x` containing value 100
- WHEN executing `SELECT x, list_transform([1, 2, 3], x -> x * 2) FROM t`
- THEN the outer `x` column returns `100`
- AND the lambda `x` refers to each list element, producing `[2, 4, 6]`

#### Scenario: Lambda parameter does not leak after evaluation

- GIVEN a table `t` with columns `a` and `values`
- WHEN executing `SELECT a, list_transform(values, a -> a + 1), a FROM t`
- THEN both occurrences of column `a` outside the lambda return the column value
- AND the lambda `a` refers to each list element

#### Scenario: Multi-parameter lambda preserves outer context

- GIVEN a table `t` with column `x` containing value 10 and column `y` containing value 20
- WHEN executing `SELECT x, y, list_reduce([1, 2, 3], (x, y) -> x + y) FROM t`
- THEN the outer `x` returns `10` and outer `y` returns `20`
- AND the lambda computes `1 + 2 + 3 = 6`

### Requirement: Lambda Error Handling

The system SHALL produce clear error messages for invalid lambda usage. Errors MUST be reported for: lambda expressions used outside function argument contexts, wrong lambda parameter arity for the target function, non-boolean lambda return in `list_filter`, and non-list first argument to list functions.

#### Scenario: Wrong arity for list_transform

- WHEN executing `SELECT list_transform([1, 2], (x, y) -> x + y)`
- THEN an error is produced stating list_transform requires a 1-parameter lambda

#### Scenario: Wrong arity for list_reduce

- WHEN executing `SELECT list_reduce([1, 2, 3], x -> x + 1)`
- THEN an error is produced stating list_reduce requires a 2-parameter lambda

#### Scenario: Non-list first argument

- WHEN executing `SELECT list_transform(42, x -> x * 2)`
- THEN an error is produced stating the first argument must be a list

#### Scenario: Non-boolean lambda in list_filter

- WHEN executing `SELECT list_filter([1, 2, 3], x -> x * 2)`
- THEN an error is produced stating the list_filter lambda must return a boolean value

#### Scenario: Lambda outside function argument context

- WHEN binding a lambda expression that appears outside a function argument (e.g., in a SELECT list or WHERE clause directly)
- THEN a binding error is produced stating "lambda expression only valid as function argument"
