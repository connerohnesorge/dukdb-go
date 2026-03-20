## Implementation Details

### 1. AST Changes (`internal/parser/ast.go`)

Add a `TryCast` boolean field to `CastExpr` to distinguish `CAST` from `TRY_CAST`:

```go
// CastExpr represents a CAST or TRY_CAST expression.
type CastExpr struct {
	Expr       Expr
	TargetType dukdb.Type
	TryCast    bool // true for TRY_CAST (returns NULL on failure)
}
```

When `TryCast` is false, behavior is unchanged (errors propagate). When true, conversion failures return NULL instead of an error.

### 2. Parser Changes (`internal/parser/parser.go`)

#### Signature Change for parseCast

The existing `parseCast` function signature changes from:

```go
func (p *parser) parseCast() (Expr, error)
```

to:

```go
func (p *parser) parseCast(tryCast bool) (Expr, error)
```

The existing call site for `CAST` must be updated to pass `false`:

```go
case "CAST":
	return p.parseCast(false)
```

#### TRY_CAST Keyword

In `parseIdentExpr()`, the parser calls `p.advance()` first and then switches on `strings.ToUpper(name)`. TRY_CAST is added as a new case in this switch:

```go
// In parseIdentExpr(), inside the switch strings.ToUpper(name) block:
case "TRY_CAST":
	return p.parseCast(true)
```

The refactored `parseCast` function accepts a `tryCast bool` parameter and sets the field on the resulting AST node:

```go
func (p *parser) parseCast(tryCast bool) (Expr, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}
	// ... type parsing (unchanged) ...
	return &CastExpr{
		Expr:       expr,
		TargetType: targetType,
		TryCast:    tryCast,
	}, nil
}
```

#### :: Operator (PostgreSQL-style Cast) - Already Implemented

The `::` operator is already parsed in `parsePostfixExpr()` at parser.go line 3782. The existing code constructs `CastExpr` without a `TryCast` field. When the `TryCast bool` field is added to `CastExpr`, the `::` code path must be updated to explicitly set `TryCast: false`:

```go
// In parsePostfixExpr() - existing code at line 3811, add TryCast field:
expr = &CastExpr{
	Expr:       expr,
	TargetType: targetType,
	TryCast:    false, // :: is always a strict cast
}
```

The `::` operator is purely syntactic sugar for `CAST(expr AS type)`. It always produces a strict cast (TryCast = false). Chaining is supported: `x::VARCHAR::INTEGER` produces nested CastExpr nodes. No new parsing logic is needed for `::` - only the addition of the `TryCast: false` field to the existing `CastExpr` construction.

### 3. Binder Changes (`internal/binder/expressions.go`, `internal/binder/bind_expr.go`)

Add the `TryCast` field to `BoundCastExpr`:

```go
// BoundCastExpr represents a bound CAST expression.
type BoundCastExpr struct {
	Expr       BoundExpr
	TargetType dukdb.Type
	TryCast    bool // true for TRY_CAST (returns NULL on failure)
}
```

In the binder, propagate the flag:

```go
case *parser.CastExpr:
	inner, err := b.bindExpr(e.Expr, dukdb.TYPE_INVALID)
	if err != nil {
		return nil, err
	}
	return &BoundCastExpr{
		Expr:       inner,
		TargetType: e.TargetType,
		TryCast:    e.TryCast,
	}, nil
```

### DDL Serialization (`internal/binder/bind_ddl.go`)

The `serializeExpr()` function in `bind_ddl.go` currently outputs `CAST(...)` for all `parser.CastExpr` nodes. It must be updated to output `TRY_CAST(...)` when `TryCast` is true:

```go
case *parser.CastExpr:
	keyword := "CAST"
	if e.TryCast {
		keyword = "TRY_CAST"
	}
	return fmt.Sprintf("%s(%s AS %s)",
		keyword,
		serializeExpr(e.Expr),
		e.TargetType.String())
```

Similarly, `formatFilterExpression()` in `internal/executor/physical_maintenance.go` handles `BoundCastExpr` and must also respect the `TryCast` field:

```go
case *binder.BoundCastExpr:
	operand := formatFilterExpression(e.Expr)
	keyword := "CAST"
	if e.TryCast {
		keyword = "TRY_CAST"
	}
	return fmt.Sprintf("%s(%s AS %s)", keyword, operand, e.TargetType.String())
```

### 4. Executor Changes (`internal/executor/expr.go`)

The executor handles `BoundCastExpr` by evaluating the inner expression then calling `castValue`. For TRY_CAST, wrap the cast in error recovery:

```go
case *binder.BoundCastExpr:
	val, err := e.evaluateExpr(ctx, ex.Expr, row)
	if err != nil {
		if ex.TryCast {
			return nil, nil // TRY_CAST: return NULL on evaluation error
		}
		return nil, err
	}
	if val == nil {
		return nil, nil // NULL input always produces NULL output
	}
	result, err := castValue(val, ex.TargetType)
	if err != nil {
		if ex.TryCast {
			return nil, nil // TRY_CAST: return NULL on cast failure
		}
		return nil, err
	}
	return result, nil
```

Key behavior: When `TryCast` is true, any error from `castValue` is caught and NULL is returned. NULL inputs always produce NULL regardless of the TryCast flag (standard SQL NULL propagation).

### 5. RewriteExpr Propagation (`internal/planner/rewrite/expr.go`)

The `RewriteExpr` function reconstructs `BoundCastExpr` when its inner expression is rewritten. The current code at line 45 does not include the `TryCast` field, which would silently drop the flag during optimization rewrites:

```go
// Current (loses TryCast):
expr = &binder.BoundCastExpr{Expr: inner, TargetType: node.TargetType}

// Fixed (preserves TryCast):
expr = &binder.BoundCastExpr{Expr: inner, TargetType: node.TargetType, TryCast: node.TryCast}
```

This is critical: without this fix, a `TRY_CAST` that passes through the optimizer/rewriter would silently become a strict `CAST`, causing unexpected errors on invalid conversions.

### 6. Error Recovery Strategy

The TRY_CAST implementation catches errors at two points:

1. **Inner expression evaluation errors** - If the sub-expression itself fails (rare but possible with nested expressions), TRY_CAST returns NULL.
2. **Cast conversion errors** - The primary case. When `castValue` returns an error (e.g., casting `'abc'` to INTEGER), TRY_CAST returns NULL instead of propagating the error.

This matches DuckDB's behavior where TRY_CAST is a "safe" version of CAST that never raises a type conversion error.

## Context

DuckDB supports three casting syntaxes:
- `CAST(expr AS type)` - Standard SQL explicit cast, raises error on failure
- `TRY_CAST(expr AS type)` - Safe cast, returns NULL on failure
- `expr::type` - PostgreSQL-style shorthand for CAST

All three are commonly used in DuckDB SQL. The `::` operator is particularly prevalent in PostgreSQL-compatible queries and is already used in some existing test files.

## Goals / Non-Goals

- Goals:
  - Full TRY_CAST support matching DuckDB v1.4.3 behavior
  - `::` operator as syntactic sugar for CAST
  - NULL-safe behavior (NULL input always yields NULL output)
  - Proper error recovery for all supported type conversions
- Non-Goals:
  - TRY_CAST with custom default values (DuckDB does not support this)
  - Implicit cast changes or coercion rule modifications
  - New type conversion paths (uses existing `castValue` function)

## Decisions

- Decision: Add `TryCast bool` field to existing CastExpr rather than creating a separate TryCastExpr AST node.
  - Rationale: Minimizes code duplication. CAST and TRY_CAST share identical parsing, binding, and planning logic; they differ only at execution time.
- Decision: `::` operator always produces strict cast (TryCast = false).
  - Rationale: Matches PostgreSQL and DuckDB behavior. There is no `TRY` variant of the `::` operator.
- Decision: Catch errors from both inner expression evaluation and castValue.
  - Rationale: Defensive approach ensures TRY_CAST never raises a type conversion error regardless of where the failure occurs.

## Risks / Trade-offs

- Risk: Performance overhead of error recovery in TRY_CAST path.
  - Mitigation: The check is a single boolean comparison; error recovery only triggers on the failure path which is inherently slow anyway.
- Risk: Masking genuine bugs by swallowing errors.
  - Mitigation: Only conversion errors are caught. Panics, context cancellations, and other non-conversion errors should still propagate. Implementation should be careful to only catch errors from `castValue`, not from unrelated operations.

## Open Questions

- Should TRY_CAST return NULL for overflow errors (e.g., casting 999999999999 to TINYINT)? DuckDB does; we should match that behavior.
- Should the `::` operator bind tighter than other postfix operators? Need to verify DuckDB precedence rules.
