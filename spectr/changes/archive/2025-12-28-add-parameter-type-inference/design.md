# Design: Parameter Type Inference

## Context

Currently, dukdb-go's `ParamType()` returns `TYPE_ANY` for all parameters. This limits type validation and compatibility with duckdb-go which uses the C API to infer actual types.

**Stakeholders**:
- Application developers needing type validation
- ORM frameworks optimizing binding
- Tools generating documentation

**Constraints**:
- Must work without actual value binding (preparation phase only)
- Must handle ambiguous cases gracefully (return TYPE_ANY)
- Must not break existing code
- Pure static analysis (no execution)

## Goals / Non-Goals

**Goals**:
1. Infer parameter types from column comparisons
2. Infer parameter types from INSERT/UPDATE contexts
3. Infer parameter types from function signatures
4. Return TYPE_ANY for ambiguous cases
5. Match duckdb-go behavior for common patterns

**Non-Goals**:
1. Full SQL type system implementation
2. Cross-statement type inference
3. Runtime type checking during execution
4. Type coercion or casting logic
5. Complex constraint solving

## Decisions

### Decision 1: Inference Strategy

**Options**:
A. Constraint-based type solver
B. Top-down expected type propagation
C. Bottom-up type inference
D. Multi-pass analysis

**Choice**: B - Top-down expected type propagation

**Rationale**:
- Simpler implementation
- Natural fit with recursive descent binding
- Matches how schema context flows down
- Sufficient for most practical cases

```go
func (b *Binder) bindExpr(expr Expr, expectedType Type) (BoundExpr, error)
```

### Decision 2: Type Conflict Resolution

**Options**:
A. First type wins
B. Most specific type wins
C. Return error on conflict
D. Return TYPE_ANY on conflict

**Choice**: D - Return TYPE_ANY on conflict

**Rationale**:
- Safe fallback
- No false positives
- Matches duckdb-go behavior
- User can always provide type at binding

```go
// If parameter used in conflicting contexts, return ANY
if param.ParamType != TYPE_ANY && param.ParamType != newType {
    param.ParamType = TYPE_ANY // conflict
}
```

### Decision 3: Unknown Type Handling

**Options**:
A. Treat as error
B. Treat as TYPE_ANY
C. Treat as TYPE_UNKNOWN (separate value)
D. Propagate and resolve later

**Choice**: B - Treat as TYPE_ANY

**Rationale**:
- TYPE_UNKNOWN already exists but has different semantics
- TYPE_ANY means "accepts anything" which is appropriate
- Simpler API for consumers

### Decision 4: Parameter Storage Location

**Options**:
A. In BinderScope (temporary during binding)
B. In BoundStatement (after binding)
C. In EngineStmt (final location)
D. All of the above (flow through)

**Choice**: D - Flow through all stages

**Rationale**:
- BinderScope collects during binding
- BoundStatement carries through planning
- EngineStmt exposes via API

```go
// Flow: BinderScope → BoundStatement.ParamTypes → EngineStmt.paramTypes
```

### Decision 5: Column Type Lookup

**Options**:
A. Inline catalog lookup during binding
B. Pre-resolve all tables before binding
C. Lazy resolution with caching
D. Require explicit schema in binder

**Choice**: C - Lazy resolution with caching

**Rationale**:
- Only look up columns that are referenced
- Cache prevents repeated lookups
- Works with existing binder structure

```go
func (b *Binder) lookupColumnType(table, column string) Type {
    if cached, ok := b.columnTypeCache[key]; ok {
        return cached
    }
    typ := b.catalog.LookupColumnType(table, column)
    b.columnTypeCache[key] = typ
    return typ
}
```

### Decision 6: Arithmetic Expression Inference

**Options**:
A. Infer based on operands
B. Always assume DOUBLE
C. Use common numeric type
D. Don't infer (return ANY)

**Choice**: B - Always assume DOUBLE

**Rationale**:
- Safest numeric type
- Handles integer and float
- Matches SQL standard implicit conversions
- Simple implementation

### Decision 7: Function Signature Source

**Options**:
A. Hardcoded function signatures
B. Catalog function registry
C. No function type inference
D. Signature inference from first call

**Choice**: B - Catalog function registry

**Rationale**:
- Functions already registered for execution
- Registry already tracks parameter types
- Consistent with column lookup pattern

### Decision 8: Multiple Parameter References

**Options**:
A. Error if same parameter has different types
B. Return ANY for conflicted parameter
C. Use first occurrence type
D. Use most specific type

**Choice**: B - Return ANY for conflicted parameter

**Rationale**:
- Same as Decision 2 (conflict resolution)
- Consistent behavior
- No false type assertions

```go
// Same parameter used in multiple contexts
"WHERE a = $1 AND b = $1" // a is INT, b is VARCHAR → $1 is ANY
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Incomplete inference | Medium | Return ANY for unresolved |
| Catalog dependency | Low | Graceful fallback if no catalog |
| Performance overhead | Low | Lazy lookup with caching |
| Complex expressions | Medium | Conservative ANY fallback |

## Performance Considerations

1. **Lazy column lookup**: Only look up referenced columns
2. **Scope-level caching**: Cache column types in binder scope
3. **No extra passes**: Inference happens during normal binding
4. **Minimal allocation**: Reuse scope maps

## Migration Plan

### Phase 1: Infrastructure
1. Add expectedType parameter to bindExpr
2. Add params map to BinderScope
3. Update BoundParameter with inferred type

### Phase 2: Column Inference
1. Implement column comparison inference
2. Implement INSERT value inference
3. Implement UPDATE value inference

### Phase 3: Function Inference
1. Add function lookup to catalog
2. Implement function argument inference

### Phase 4: Integration
1. Wire parameter types to EngineStmt
2. Update ParamType() to return inferred types
3. Add comprehensive tests

## Open Questions (Resolved)

1. **Should we infer types for subqueries?**
   - Answer: Not in first phase, return ANY for subquery contexts

2. **How to handle UNION with different branch types?**
   - Answer: Return ANY for all parameters in UNION

3. **Should LIKE pattern be VARCHAR?**
   - Answer: Yes, LIKE patterns are always string context

4. **What about NULL literals?**
   - Answer: NULL doesn't affect inference, parameter keeps expected type
