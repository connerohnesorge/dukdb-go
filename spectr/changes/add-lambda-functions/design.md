# Lambda Functions Implementation Design

## Overview

This document describes the implementation strategy for adding lambda expression parsing and execution to dukdb-go, enabling list manipulation functions (`list_transform`, `list_filter`, `list_reduce`, `list_sort`) that accept inline lambda arguments.

## Current State

- `TYPE_LAMBDA` (106) exists in the type system (`type_enum.go`)
- Lambda expressions can be stored as strings in `LAMBDA`-typed columns
- The parser recognizes `->` as `OpJSONExtract` (JSON extract operator)
- No AST node exists for lambda expressions
- No list manipulation functions exist that accept lambda arguments
- `FunctionCall` AST node carries `Args []Expr` but lambdas are not valid expressions yet

## Implementation Details

### 1. LambdaExpr AST Node

Add to `internal/parser/ast.go`:

```go
// LambdaExpr represents a lambda expression: x -> expr or (x, y) -> expr.
// Lambda expressions are used as arguments to higher-order list functions
// such as list_transform, list_filter, list_reduce, and list_sort.
type LambdaExpr struct {
    Params []string // Parameter names: ["x"] or ["x", "y"]
    Body   Expr     // The body expression evaluated for each element
}

func (*LambdaExpr) exprNode() {}
```

The `Params` field holds one or more parameter names. Single-parameter lambdas use `x -> expr` syntax; multi-parameter lambdas use `(x, y) -> expr` syntax.

### 2. Parser Changes

Lambda expressions appear exclusively as arguments inside function calls. The parser must distinguish the `->` token between JSON extract (`col->key`) and lambda arrow (`x -> x * 2`).

**Disambiguation strategy: lookahead in parseFunctionArgs().** The `->` operator is normally parsed at JSON expression precedence before function argument context is known, which makes a simple `parsingFunctionArgs` flag unreliable. Instead, disambiguation is performed entirely inside `parseFunctionArgs()` using lookahead before invoking the general expression parser:

**Single-parameter lambda detection:** When the current token is an identifier and the next token is `->`, check the token *after* `->`. If the right-hand side is NOT a string literal and NOT an integer literal (both of which indicate JSON extract: `col->'key'` or `col->0`), parse as lambda. If the right-hand side IS a string literal or integer literal, fall through to `parseExpr()` which will handle it as JSON extract.

**Multi-parameter lambda detection:** When the current token is `(` followed by a comma-separated list of bare identifiers, `)`, then `->`, parse as a multi-parameter lambda. This pattern is unambiguous since parenthesized identifier lists followed by `->` have no other valid SQL interpretation.

**Outside function arguments:** `->` is always JSON extract. No changes needed to the general expression parser.

Implementation approach in `internal/parser/parser.go`:

```go
// parseFunctionArg attempts to parse a single function argument, which may
// be a lambda expression. Called from parseFunctionArgs for each argument.
//
// Lambda detection uses lookahead to distinguish from JSON extract:
//   - identifier '->' (non-string, non-integer) => lambda
//   - identifier '->' string_literal            => JSON extract (fall through)
//   - identifier '->' integer_literal           => JSON extract (fall through)
//   - '(' ident_list ')' '->'                   => multi-param lambda
//
// Grammar:
//   lambda_expr := identifier '->' expr
//               | '(' identifier (',' identifier)* ')' '->' expr
func (p *Parser) parseFunctionArg() (Expr, error) {
    // Single-parameter lambda: identifier '->' <non-literal>
    if p.currentTokenIs(TokenIdent) && p.peekTokenIs(TokenArrow) {
        // Look at the token after '->' (2 positions ahead)
        afterArrow := p.peekTokenAt(2)
        if afterArrow.Type != TokenString && afterArrow.Type != TokenInteger {
            // This is a lambda: x -> expr
            paramName := p.currentToken().Value
            p.advance() // consume identifier
            p.advance() // consume '->'
            body, err := p.parseExpr()
            if err != nil {
                return nil, err
            }
            return &LambdaExpr{
                Params: []string{paramName},
                Body:   body,
            }, nil
        }
        // Otherwise fall through: it's JSON extract like col->'key' or col->0
    }

    // Multi-parameter lambda: '(' ident, ident, ... ')' '->'
    if p.currentTokenIs(TokenLParen) {
        saved := p.savePosition()
        params, ok := p.tryParseParenthesizedIdentList()
        if ok && p.currentTokenIs(TokenArrow) {
            p.advance() // consume '->'
            body, err := p.parseExpr()
            if err != nil {
                return nil, err
            }
            return &LambdaExpr{
                Params: params,
                Body:   body,
            }, nil
        }
        p.restorePosition(saved)
    }

    // Not a lambda, parse as regular expression
    return p.parseExpr()
}
```

The `->` token is already lexed (for JSON extract). The parser uses targeted lookahead within `parseFunctionArgs()` to decide between lambda and JSON extract. No global parser state flag is needed. Each argument in a function call is parsed with `parseFunctionArg()` instead of `parseExpr()`.

Key detail: `peekTokenAt(n)` returns the token n positions ahead of the current position without consuming. The critical check is that `identifier -> string_literal` and `identifier -> integer_literal` are always JSON extract (DuckDB syntax: `col->'key'` or `col->0`), while `identifier -> <anything else>` inside a function argument is a lambda.

### 3. BoundLambdaExpr in Binder

Add to `internal/binder/expressions.go`:

```go
// BoundLambdaExpr represents a bound lambda expression.
// The parameter names are placeholders that will be resolved at execution time
// when the lambda is invoked with concrete values from list elements.
type BoundLambdaExpr struct {
    Params  []string   // Parameter names (e.g., ["x"] or ["x", "y"])
    Body    BoundExpr  // The bound body expression
    ResType dukdb.Type // Result type of the body expression
}

func (*BoundLambdaExpr) boundExprNode() {}

func (l *BoundLambdaExpr) ResultType() dukdb.Type { return l.ResType }
```

**Critical: Lambda binding happens inside bindFunctionCall(), not in generic bindExpr().** Lambda parameter types can only be inferred from the function signature (e.g., `list_transform` expects a 1-param lambda typed to the list's element type). A generic `bindExpr()` call has no function context and cannot determine parameter types. Therefore:

1. `bindExpr()` does NOT handle `LambdaExpr` -- encountering one there is an error ("lambda expression only valid as function argument")
2. `bindFunctionCall()` recognizes known lambda-accepting functions and calls `bindLambdaExpr()` with the correct parameter types derived from the already-bound list argument

**Element type inference strategy:** The binder determines lambda parameter types from the list argument's bound type:

1. **Static inference (preferred):** If the list argument is a `BoundArrayExpr` (e.g., `[1, 2, 3]`), use its `ElemType` field directly. This gives a concrete type at bind time.
2. **Column reference inference:** If the list argument is a `BoundColumnRef` with a LIST type, extract the element type from the column's type metadata.
3. **Fallback to VARCHAR:** If the element type cannot be determined statically (e.g., the list comes from a complex expression), use VARCHAR as a safe default. At runtime, Go's dynamic typing (`any`) will handle the actual values.

```go
// bindFunctionCall handles binding for function calls. For known lambda-accepting
// functions, it binds lambda arguments with function-specific type context.
func (b *Binder) bindFunctionCall(fc *parser.FunctionCall) (BoundExpr, error) {
    switch strings.ToLower(fc.Name) {
    case "list_transform", "list_filter":
        return b.bindListLambdaFunc(fc, 1) // expects 1-param lambda
    case "list_reduce":
        return b.bindListLambdaFunc(fc, 2) // expects 2-param lambda
    case "list_sort":
        if len(fc.Args) > 1 {
            return b.bindListLambdaFunc(fc, 1) // optional 1-param lambda
        }
        // No lambda argument; bind normally
        return b.bindRegularFunctionCall(fc)
    default:
        return b.bindRegularFunctionCall(fc)
    }
}

// bindListLambdaFunc binds a list function that takes a lambda argument.
// It first binds the list argument to determine the element type, then
// uses that type context to bind the lambda's parameters.
func (b *Binder) bindListLambdaFunc(
    fc *parser.FunctionCall,
    expectedLambdaParams int,
) (BoundExpr, error) {
    // Step 1: Bind the list argument first (always the first arg)
    boundListArg, err := b.bindExpr(fc.Args[0])
    if err != nil {
        return nil, fmt.Errorf("%s: error binding list argument: %w", fc.Name, err)
    }

    // Step 2: Infer element type from the bound list argument
    elemType := b.inferListElementType(boundListArg)

    // Step 3: Verify second argument is a lambda
    lambdaExpr, ok := fc.Args[1].(*parser.LambdaExpr)
    if !ok {
        return nil, fmt.Errorf("%s: second argument must be a lambda expression", fc.Name)
    }
    if len(lambdaExpr.Params) != expectedLambdaParams {
        return nil, fmt.Errorf(
            "%s: lambda must have exactly %d parameter(s), got %d",
            fc.Name, expectedLambdaParams, len(lambdaExpr.Params),
        )
    }

    // Step 4: Build parameter type map and bind the lambda
    paramTypes := make(map[string]dukdb.Type, expectedLambdaParams)
    for _, param := range lambdaExpr.Params {
        paramTypes[param] = elemType
    }
    boundLambda, err := b.bindLambdaExpr(lambdaExpr, paramTypes)
    if err != nil {
        return nil, err
    }

    return &BoundFunctionCall{
        Name: fc.Name,
        Args: []BoundExpr{boundListArg, boundLambda},
        // ... result type depends on function
    }, nil
}

// inferListElementType extracts the element type from a bound list expression.
// Uses static type information when available, falls back to VARCHAR.
func (b *Binder) inferListElementType(boundList BoundExpr) dukdb.Type {
    switch bl := boundList.(type) {
    case *BoundArrayExpr:
        // Array literal: use the declared element type
        return bl.ElemType
    case *BoundColumnRef:
        // Column reference: extract element type from LIST column metadata
        if bl.ColType.IsList() {
            return bl.ColType.ListElementType()
        }
    }
    // Fallback: VARCHAR is safe; Go's dynamic typing handles actual values at runtime
    return dukdb.TYPE_VARCHAR
}

// bindLambdaExpr binds a lambda expression given the expected parameter types.
// paramTypes maps parameter name to expected type (inferred from the list's element type).
// This is called ONLY from bindFunctionCall, never from generic bindExpr.
func (b *Binder) bindLambdaExpr(
    lambdaExpr *parser.LambdaExpr,
    paramTypes map[string]dukdb.Type,
) (*BoundLambdaExpr, error) {
    if len(lambdaExpr.Params) != len(paramTypes) {
        return nil, fmt.Errorf(
            "lambda expects %d parameters but %d provided",
            len(paramTypes), len(lambdaExpr.Params),
        )
    }

    // Push lambda parameter scope
    b.pushLambdaScope(lambdaExpr.Params, paramTypes)
    defer b.popLambdaScope()

    // Bind the body expression
    body, err := b.bindExpr(lambdaExpr.Body)
    if err != nil {
        return nil, fmt.Errorf("error binding lambda body: %w", err)
    }

    return &BoundLambdaExpr{
        Params:  lambdaExpr.Params,
        Body:    body,
        ResType: body.ResultType(),
    }, nil
}
```

### 4. Executor: Lambda Evaluation

Lambda evaluation uses a **scoped evaluation context** (lambda scope stack) rather than direct row map mutation. This avoids scope leakage issues where lambda parameters could corrupt the outer row context if evaluation errors occur partway through, or where concurrent lambda invocations (e.g., nested lambdas in future work) would conflict.

Add to `internal/executor/expr.go`:

```go
case *binder.BoundLambdaExpr:
    // Lambda expressions are not directly evaluated; they are invoked
    // by list functions with concrete argument values.
    return nil, fmt.Errorf("lambda expression cannot be evaluated directly")
```

**Lambda scope stack design:**

The `ExecutionContext` is extended with a stack of lambda scopes. Each scope is a `map[string]any` mapping parameter names to their current values. When evaluating a column reference, the evaluator checks the lambda scope stack (top-first) before falling back to the row map. This ensures clean scoping without mutating the row.

```go
// LambdaScope holds parameter bindings for a single lambda invocation.
type LambdaScope struct {
    Bindings map[string]any // parameter name -> current value
}

// ExecutionContext is extended with a lambda scope stack.
// Add to the existing ExecutionContext struct:
//   LambdaScopes []LambdaScope

// pushLambdaScope pushes a new scope with the given parameter bindings.
func (ctx *ExecutionContext) pushLambdaScope(params []string, args []any) {
    bindings := make(map[string]any, len(params))
    for i, param := range params {
        bindings[param] = args[i]
    }
    ctx.LambdaScopes = append(ctx.LambdaScopes, LambdaScope{Bindings: bindings})
}

// popLambdaScope removes the topmost lambda scope.
func (ctx *ExecutionContext) popLambdaScope() {
    if len(ctx.LambdaScopes) > 0 {
        ctx.LambdaScopes = ctx.LambdaScopes[:len(ctx.LambdaScopes)-1]
    }
}

// resolveLambdaParam checks the lambda scope stack (top-first) for a column name.
// Returns the value and true if found in any lambda scope, or nil and false otherwise.
func (ctx *ExecutionContext) resolveLambdaParam(name string) (any, bool) {
    for i := len(ctx.LambdaScopes) - 1; i >= 0; i-- {
        if val, ok := ctx.LambdaScopes[i].Bindings[name]; ok {
            return val, true
        }
    }
    return nil, false
}
```

**Column reference resolution change:** In the expression evaluator, when resolving a `BoundColumnRef` (or equivalent), check `ctx.resolveLambdaParam(name)` first. If it returns a value, use it; otherwise fall back to the row map lookup. This is a single-point change in the evaluator.

**Lambda invocation:**

```go
// invokeLambda evaluates a lambda's body expression with the given parameter values.
// Uses the lambda scope stack for clean scoping -- no row map mutation.
func (e *Executor) invokeLambda(
    ctx *ExecutionContext,
    lambda *binder.BoundLambdaExpr,
    row map[string]any,
    args ...any,
) (any, error) {
    if len(args) != len(lambda.Params) {
        return nil, fmt.Errorf(
            "lambda expects %d arguments but got %d",
            len(lambda.Params), len(args),
        )
    }

    // Push a new lambda scope -- does NOT modify the row map
    ctx.pushLambdaScope(lambda.Params, args)
    defer ctx.popLambdaScope()

    // Evaluate the body; column refs will check lambda scopes first
    return e.evaluateExpr(ctx, lambda.Body, row)
}
```

This stack-based approach ensures:
- No row map mutation (the row is read-only during lambda evaluation)
- Clean error recovery (defer guarantees scope is popped even on error)
- Correct nesting if nested lambdas are added in the future
- Lambda parameters shadow outer columns by virtue of top-first stack search

### 5. List Function Implementations

Each list function is dispatched from `evaluateFunctionCall` BEFORE eager argument evaluation (see Section 6). They receive `[]BoundExpr` and are responsible for evaluating the list argument eagerly while extracting the lambda without evaluation.

#### list_transform(list, lambda)

Applies a single-parameter lambda to each element, returning a new list. (See Section 6 for the full implementation with the new signature pattern.)

#### list_filter(list, lambda)

Applies a single-parameter lambda (must return boolean) to each element, keeping only elements where lambda returns true.

```go
func (e *Executor) evalListFilter(
    ctx *ExecutionContext,
    boundArgs []binder.BoundExpr,
    row map[string]any,
) (any, error) {
    // Step 1: Eagerly evaluate the list argument
    listVal, err := e.evaluateExpr(ctx, boundArgs[0], row)
    if err != nil {
        return nil, err
    }
    if listVal == nil {
        return nil, nil // NULL list -> NULL result
    }
    list, ok := listVal.([]any)
    if !ok {
        return nil, fmt.Errorf("list_filter: first argument must be a list")
    }

    // Step 2: Extract the lambda (do NOT evaluate it)
    lambda, ok := boundArgs[1].(*binder.BoundLambdaExpr)
    if !ok {
        return nil, fmt.Errorf("list_filter: second argument must be a lambda")
    }
    if len(lambda.Params) != 1 {
        return nil, fmt.Errorf("list_filter: lambda must have exactly 1 parameter")
    }

    // Step 3: Invoke lambda per-element, keeping elements where result is true
    var result []any
    for _, elem := range list {
        val, err := e.invokeLambda(ctx, lambda, row, elem)
        if err != nil {
            return nil, err
        }
        keep, err := toBoolValue(val)
        if err != nil {
            return nil, fmt.Errorf("list_filter: lambda must return boolean, got %T", val)
        }
        if keep {
            result = append(result, elem)
        }
    }
    if result == nil {
        result = []any{}
    }
    return result, nil
}
```

#### list_reduce(list, lambda)

Applies a two-parameter lambda to accumulate a result across all list elements. The first element is the initial accumulator value.

```go
func (e *Executor) evalListReduce(
    ctx *ExecutionContext,
    boundArgs []binder.BoundExpr,
    row map[string]any,
) (any, error) {
    // Step 1: Eagerly evaluate the list argument
    listVal, err := e.evaluateExpr(ctx, boundArgs[0], row)
    if err != nil {
        return nil, err
    }
    if listVal == nil {
        return nil, nil // NULL list -> NULL result
    }
    list, ok := listVal.([]any)
    if !ok {
        return nil, fmt.Errorf("list_reduce: first argument must be a list")
    }
    if len(list) == 0 {
        return nil, nil // NULL for empty list
    }

    // Step 2: Extract the lambda (do NOT evaluate it)
    lambda, ok := boundArgs[1].(*binder.BoundLambdaExpr)
    if !ok {
        return nil, fmt.Errorf("list_reduce: second argument must be a lambda")
    }
    if len(lambda.Params) != 2 {
        return nil, fmt.Errorf("list_reduce: lambda must have exactly 2 parameters")
    }

    // Step 3: Accumulate using lambda
    accumulator := list[0]
    for i := 1; i < len(list); i++ {
        val, err := e.invokeLambda(ctx, lambda, row, accumulator, list[i])
        if err != nil {
            return nil, fmt.Errorf("list_reduce element %d: %w", i, err)
        }
        accumulator = val
    }
    return accumulator, nil
}
```

#### list_sort(list, lambda)

Sorts a list using a single-parameter lambda as a key function. The lambda extracts a sort key from each element.

```go
func (e *Executor) evalListSort(
    ctx *ExecutionContext,
    boundArgs []binder.BoundExpr,
    row map[string]any,
) (any, error) {
    // Step 1: Eagerly evaluate the list argument
    listVal, err := e.evaluateExpr(ctx, boundArgs[0], row)
    if err != nil {
        return nil, err
    }
    if listVal == nil {
        return nil, nil // NULL list -> NULL result
    }
    list, ok := listVal.([]any)
    if !ok {
        return nil, fmt.Errorf("list_sort: first argument must be a list")
    }

    // list_sort with no lambda argument sorts naturally
    if len(boundArgs) < 2 {
        return sortListNatural(list)
    }

    // Step 2: Extract the lambda (do NOT evaluate it)
    lambda, ok := boundArgs[1].(*binder.BoundLambdaExpr)
    if !ok {
        return nil, fmt.Errorf("list_sort: second argument must be a lambda")
    }
    if len(lambda.Params) != 1 {
        return nil, fmt.Errorf("list_sort: lambda must have exactly 1 parameter")
    }

    // Step 3: Compute sort keys via lambda
    type keyedElement struct {
        original any
        key      any
    }
    keyed := make([]keyedElement, len(list))
    for i, elem := range list {
        key, err := e.invokeLambda(ctx, lambda, row, elem)
        if err != nil {
            return nil, fmt.Errorf("list_sort key %d: %w", i, err)
        }
        keyed[i] = keyedElement{original: elem, key: key}
    }

    // Sort by key using compareValues (existing comparison infrastructure)
    sort.SliceStable(keyed, func(i, j int) bool {
        cmp := compareValues(keyed[i].key, keyed[j].key)
        return cmp < 0
    })

    result := make([]any, len(keyed))
    for i, k := range keyed {
        result[i] = k.original
    }
    return result, nil
}
```

### 6. Function Registration and Lazy Lambda Dispatch

The list functions are registered alongside existing built-in functions. **Critical:** the existing `evaluateFunctionCall` eagerly evaluates all arguments to `[]any` before dispatching. This will fail for lambda arguments, since `BoundLambdaExpr` cannot be evaluated to a value (it is code, not data). The fix is to special-case lambda-accepting functions *before* eager argument evaluation, similar to how CASE/WHEN expressions short-circuit evaluation.

```go
// In evaluateFunctionCall, BEFORE the standard eager-eval path:
func (e *Executor) evaluateFunctionCall(
    ctx *ExecutionContext,
    fc *binder.BoundFunctionCall,
    row map[string]any,
) (any, error) {
    // Special-case: lambda-accepting functions receive BoundExpr args directly.
    // These functions control evaluation of each argument individually.
    switch strings.ToLower(fc.Name) {
    case "list_transform":
        return e.evalListTransform(ctx, fc.Args, row)
    case "list_filter":
        return e.evalListFilter(ctx, fc.Args, row)
    case "list_reduce":
        return e.evalListReduce(ctx, fc.Args, row)
    case "list_sort":
        return e.evalListSort(ctx, fc.Args, row)
    }

    // Standard path: eagerly evaluate all arguments for non-lambda functions
    evaluatedArgs := make([]any, len(fc.Args))
    for i, arg := range fc.Args {
        val, err := e.evaluateExpr(ctx, arg, row)
        if err != nil {
            return nil, err
        }
        evaluatedArgs[i] = val
    }
    return e.dispatchBuiltinFunction(fc.Name, evaluatedArgs)
}
```

Each list function implementation receives `[]BoundExpr` and is responsible for:
1. Eagerly evaluating the list argument (index 0) to get `[]any`
2. Extracting the `*BoundLambdaExpr` from the remaining args (NOT evaluating it)
3. Invoking the lambda per-element via `invokeLambda()`

Updated `evalListTransform` signature to match:

```go
func (e *Executor) evalListTransform(
    ctx *ExecutionContext,
    boundArgs []binder.BoundExpr,
    row map[string]any,
) (any, error) {
    // Step 1: Eagerly evaluate the list argument
    listVal, err := e.evaluateExpr(ctx, boundArgs[0], row)
    if err != nil {
        return nil, err
    }
    if listVal == nil {
        return nil, nil // NULL list -> NULL result
    }
    list, ok := listVal.([]any)
    if !ok {
        return nil, fmt.Errorf("list_transform: first argument must be a list")
    }

    // Step 2: Extract the lambda (do NOT evaluate it)
    lambda, ok := boundArgs[1].(*binder.BoundLambdaExpr)
    if !ok {
        return nil, fmt.Errorf("list_transform: second argument must be a lambda")
    }

    // Step 3: Invoke lambda per-element
    result := make([]any, len(list))
    for i, elem := range list {
        val, err := e.invokeLambda(ctx, lambda, row, elem)
        if err != nil {
            return nil, fmt.Errorf("list_transform element %d: %w", i, err)
        }
        result[i] = val
    }
    return result, nil
}
```

The same pattern applies to `evalListFilter`, `evalListReduce`, and `evalListSort` -- each eagerly evaluates the list arg, extracts the lambda from `boundArgs`, and invokes it per-element.

### 7. Disambiguating `->` in the Parser

The `->` token currently maps to `OpJSONExtract`. For lambda support, disambiguation is handled entirely by **lookahead in `parseFunctionArg()`** (see Section 2 above). No global parser state flags are needed.

Summary of the disambiguation rules:

1. **Inside function arguments (`parseFunctionArg`):**
   - `identifier -> string_literal` (e.g., `col -> 'key'`) => JSON extract (parsed by `parseExpr`)
   - `identifier -> integer_literal` (e.g., `col -> 0`) => JSON extract (parsed by `parseExpr`)
   - `identifier -> <other>` (e.g., `x -> x * 2`) => Lambda expression
   - `(ident, ident, ...) -> expr` => Multi-parameter lambda expression
2. **Outside function arguments:**
   - `->` is always JSON extract. The general expression parser is unchanged.

This approach avoids the fragile `parsingFunctionArgs` flag pattern, which would require threading state through multiple parser methods and could misfire if `->` appears in nested sub-expressions within function arguments (e.g., `func(a, b->>'key', x -> x + 1)`). Instead, the lookahead check is localized to `parseFunctionArg()` and only fires at the top level of each argument.

Edge cases:
- `func(col->'key')`: Identifier followed by `->` then string literal -- correctly parsed as JSON extract.
- `func(col->0)`: Identifier followed by `->` then integer -- correctly parsed as JSON extract.
- `func(x -> x + 1)`: Identifier followed by `->` then identifier -- correctly parsed as lambda.
- `func(x -> upper(x))`: Identifier followed by `->` then identifier (function call) -- correctly parsed as lambda.
- `func((x, y) -> x + y)`: Parenthesized ident list followed by `->` -- correctly parsed as multi-param lambda.

## Context

### Constraints

- No CGO dependencies
- Must work with existing LIST type representation (`[]any` in Go)
- Lambda parameters shadow column names within the lambda body (restored after evaluation)
- The `->` token must remain valid for JSON extract outside lambda contexts

### Stakeholders

- Users writing analytical queries with nested list data
- Users migrating from DuckDB who use `list_transform`/`list_filter` patterns

## Goals / Non-Goals

**Goals:**
- Parse and execute `x -> expr` and `(x, y) -> expr` lambda syntax
- Implement `list_transform`, `list_filter`, `list_reduce`, `list_sort` with lambda arguments
- Produce clear error messages for lambda arity and type mismatches
- Match DuckDB v1.4.3 behavior for these functions

**Non-Goals:**
- General-purpose lambda/closure support outside list functions
- Lambda expressions as standalone values (e.g., storing computed lambdas)
- `list_apply` (alias for `list_transform` -- can be added trivially later)
- `list_any_value` (not a lambda function, separate concern)
- Nested lambda expressions (lambdas within lambda bodies)

## Decisions

### Decision 1: Lambda Parsing via Lookahead in parseFunctionArg()

**Decision:** Parse lambdas only within function argument positions using lookahead-based disambiguation in `parseFunctionArg()`. No global parser state flags.

**Rationale:** The `->` operator is parsed at JSON expression precedence before function argument context is known if a global flag approach is used. A lookahead approach in `parseFunctionArg()` is more reliable: when seeing `identifier ->` where the right side is NOT a string/integer literal, parse as lambda. When `->` appears outside function args, always JSON extract. This avoids threading state through multiple parser methods and correctly handles mixed expressions like `func(col->'key', x -> x + 1)`.

**Alternatives considered:** `parsingFunctionArgs` boolean flag (rejected: fragile, fails when `->` appears in nested sub-expressions within function arguments). Always parse `->` as lambda and require `->` for JSON (rejected: breaking change). Use a different arrow token like `=>` (rejected: not DuckDB-compatible).

### Decision 2: Lambda Parameter Binding via Scope Stack on ExecutionContext

**Decision:** Use a lambda scope stack on `ExecutionContext` rather than direct row map mutation. Column reference resolution checks the scope stack (top-first) before falling back to the row map.

**Rationale:** Direct row map mutation has scope leakage issues: if evaluation errors occur partway through, saved values may not be restored correctly. A scope stack provides clean isolation -- `defer popLambdaScope()` guarantees cleanup. It also naturally supports future nested lambda scenarios and avoids mutating shared state.

**Alternatives considered:** Direct row map injection with save/restore (rejected: error-prone with partial failures, mutates shared state). Separate evaluation context per lambda (rejected: would require modifying all expression evaluators to accept a different context type).

### Decision 3: Special-Cased Lazy Lambda Dispatch in evaluateFunctionCall

**Decision:** `evaluateFunctionCall` special-cases lambda-accepting functions (list_transform, list_filter, list_reduce, list_sort) BEFORE the standard eager-evaluation path, passing them `[]BoundExpr` args directly. Each list function eagerly evaluates only the list argument and extracts the lambda `BoundExpr` without evaluating it.

**Rationale:** The existing `evaluateFunctionCall` eagerly evaluates all arguments to `[]any`. `BoundLambdaExpr` cannot be evaluated to a value -- it is code, not data. Special-casing before eager eval (similar to how CASE/WHEN short-circuits) is the minimal change. Each list function controls when to invoke the lambda per-element.

**Alternatives considered:** Adding a `LazyArg` wrapper that defers evaluation (rejected: over-engineered, only lambdas need this). Passing both evaluated args and bound exprs to all functions (rejected: wasteful, confusing API for non-lambda functions).

### Decision 4: Lambda Binding in bindFunctionCall(), Not Generic bindExpr()

**Decision:** Lambda expressions are bound exclusively inside `bindFunctionCall()` for known lambda-accepting functions. `bindExpr()` encountering a `LambdaExpr` produces an error.

**Rationale:** Lambda parameter types can only be inferred from the function signature (e.g., `list_transform` expects a 1-param lambda typed to the list's element type). A generic `bindExpr()` has no function context and cannot determine parameter types. Binding in `bindFunctionCall()` ensures the list argument is bound first, its element type is extracted, and that type is used to bind the lambda parameters.

**Alternatives considered:** Generic `bindExpr()` with type inference from context (rejected: no context available in generic path). Deferred type resolution (rejected: would push type errors to runtime).

### Decision 5: Element Type Inference Strategy

**Decision:** Infer lambda parameter types from the bound list argument: use `BoundArrayExpr.ElemType` for array literals, column type metadata for column references, and fall back to VARCHAR for complex expressions.

**Rationale:** Lists are `[]any` at runtime with no type metadata, making runtime-only inference unreliable (empty lists have no elements to inspect). Static inference during binding provides type safety and clear error messages. The VARCHAR fallback is safe because Go's `any` type handles actual values dynamically at runtime.

**Alternatives considered:** Runtime inference from first non-null element (rejected as sole strategy: fails for empty lists, defers errors to runtime). Type annotations on lambdas (rejected: not DuckDB-compatible syntax).

### Decision 4: list_reduce Accumulator Initialization

**Decision:** Use the first list element as the initial accumulator value (matching DuckDB behavior). Empty lists return NULL.

**Rationale:** Matches DuckDB v1.4.3 semantics. An alternative with an explicit initial value could be added later as `list_reduce(list, initial, lambda)`.

## Risks / Trade-offs

- **Risk:** `->` disambiguation may have edge cases where a column named like a lambda parameter precedes `->` in a function argument and the right side is not a string/integer literal. **Mitigation:** The lookahead rule (`identifier -> non-literal` = lambda) means that `col -> col2` inside a function argument would be parsed as a lambda, not JSON extract. This is acceptable because JSON extract with a non-literal right side (`col -> col2`) is not valid DuckDB syntax -- JSON extract always uses string literals (`col->'key'`) or integer literals (`col->0`).
- **Risk:** Lambda parameter shadowing could mask column names unexpectedly. **Mitigation:** The scope stack ensures lambda parameters take precedence within the lambda body but are automatically cleaned up after. The scope stack is searched top-first, so nested lambdas correctly shadow outer lambdas.
- **Risk:** Element type inference may fall back to VARCHAR for complex list expressions, leading to less precise type checking at bind time. **Mitigation:** The VARCHAR fallback is safe for correctness (Go's `any` handles runtime types). As the type system evolves, more inference paths can be added.
- **Risk:** Performance of per-element lambda invocation for large lists (scope stack push/pop per element). **Mitigation:** Acceptable for initial implementation; the scope stack operations are O(1) map allocation. Vectorized evaluation can be added later.

## Open Questions

- Should `list_apply` be added as an alias for `list_transform`? (Low priority, trivial to add.)
- Should nested lambdas be supported (e.g., `list_transform(list, x -> list_filter(x, y -> y > 0))`)? This requires recursive lambda scope management and is deferred to a follow-up change.
