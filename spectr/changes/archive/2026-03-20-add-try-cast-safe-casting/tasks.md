## 1. Parser Changes

- [ ] 1.1 Add `TryCast bool` field to `CastExpr` in `internal/parser/ast.go`
- [ ] 1.2 Change `parseCast()` signature from `func (p *parser) parseCast() (Expr, error)` to `func (p *parser) parseCast(tryCast bool) (Expr, error)` and update existing `CAST` call site to pass `false`
- [ ] 1.3 Add `TRY_CAST` case to `parseIdentExpr()` switch: `case "TRY_CAST": return p.parseCast(true)`
- [ ] 1.4 Add `TryCast: false` field to existing `::` operator `CastExpr` construction in `parsePostfixExpr()` (line 3811)
- [ ] 1.5 Add parser unit tests for `TRY_CAST(expr AS type)` syntax
- [ ] 1.6 Add parser unit tests for `::` operator including chained casts (e.g., `x::VARCHAR::INTEGER`)

## 2. Binder Changes

- [ ] 2.1 Add `TryCast bool` field to `BoundCastExpr` in `internal/binder/expressions.go`
- [ ] 2.2 Propagate `TryCast` flag from `parser.CastExpr` to `BoundCastExpr` in `internal/binder/bind_expr.go`
- [ ] 2.3 Update any binder code that constructs `BoundCastExpr` to handle the new field

## 3. Executor Changes

- [ ] 3.1 Modify `BoundCastExpr` handling in `internal/executor/expr.go` to catch errors and return NULL when `TryCast` is true
- [ ] 3.2 Ensure NULL input propagation works correctly for both CAST and TRY_CAST
- [ ] 3.3 Add executor unit tests for TRY_CAST returning NULL on invalid conversions
- [ ] 3.4 Add executor unit tests for TRY_CAST succeeding on valid conversions

## 4. Integration and Propagation

- [ ] 4.1 Update `internal/parser/table_extractor.go` if CastExpr handling needs the new field
- [ ] 4.2 Update `internal/parser/parameters.go` if CastExpr handling needs the new field
- [ ] 4.3 Update `internal/planner/rewrite/expr.go` to propagate `TryCast` field when reconstructing `BoundCastExpr` (line 45)
- [ ] 4.4 Update `internal/optimizer/decorrelation.go` if CastExpr handling needs the new field
- [ ] 4.5 Update `internal/engine/query_cache.go` if BoundCastExpr handling needs the new field
- [ ] 4.6 Update `serializeExpr()` in `internal/binder/bind_ddl.go` to output `TRY_CAST(...)` when `TryCast` is true
- [ ] 4.7 Update `formatFilterExpression()` in `internal/executor/physical_maintenance.go` to output `TRY_CAST(...)` when `TryCast` is true

## 5. End-to-End Tests

- [ ] 5.1 Add integration test: `SELECT TRY_CAST('abc' AS INTEGER)` returns NULL
- [ ] 5.2 Add integration test: `SELECT TRY_CAST('42' AS INTEGER)` returns 42
- [ ] 5.3 Add integration test: `SELECT 42::VARCHAR` returns '42'
- [ ] 5.4 Add integration test: `SELECT TRY_CAST(NULL AS INTEGER)` returns NULL
- [ ] 5.5 Add integration test: nested `TRY_CAST(TRY_CAST('abc' AS INTEGER) AS VARCHAR)` returns NULL
- [ ] 5.6 Add integration test: `SELECT '123'::INTEGER + 1` returns 124
