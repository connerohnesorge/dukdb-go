## Implementation Details

### AST Changes (`internal/parser/ast.go`)

```go
// PrepareStmt represents PREPARE name AS statement.
// Implements Stmt interface: stmtNode(), Type() returns STATEMENT_TYPE_PREPARE.
type PrepareStmt struct {
    Name  string // Prepared statement name
    Inner Stmt   // The statement to prepare (SELECT, INSERT, UPDATE, DELETE, etc.)
}

func (*PrepareStmt) stmtNode() {}
func (*PrepareStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PREPARE }

// ExecuteStmt represents EXECUTE name or EXECUTE name(param1, param2, ...).
// Implements Stmt interface: stmtNode(), Type() returns STATEMENT_TYPE_EXECUTE.
type ExecuteStmt struct {
    Name   string // Prepared statement name
    Params []Expr // Parameter values (positional, matching $1, $2, etc.)
}

func (*ExecuteStmt) stmtNode() {}
func (*ExecuteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXECUTE }

// DeallocateStmt represents DEALLOCATE [PREPARE] name.
// Implements Stmt interface: stmtNode(), Type() returns STATEMENT_TYPE_DEALLOCATE.
type DeallocateStmt struct {
    Name string // Prepared statement name; empty string means DEALLOCATE ALL
}

func (*DeallocateStmt) stmtNode() {}
func (*DeallocateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DEALLOCATE }
```

**StmtType constants**: Add to root package (`stmt_type.go` or constants file):
```go
STATEMENT_TYPE_PREPARE    StmtType = 30
STATEMENT_TYPE_EXECUTE    StmtType = 31
STATEMENT_TYPE_DEALLOCATE StmtType = 32
```

These values must not conflict with existing StmtType constants. Check existing
values in `stmt_type.go` and use the next available range.

### Parser Grammar (`internal/parser/parser.go`)

```
prepare_stmt
    : PREPARE identifier AS statement
    ;

execute_stmt
    : EXECUTE identifier
    | EXECUTE identifier '(' expr_list ')'
    ;

deallocate_stmt
    : DEALLOCATE identifier
    | DEALLOCATE PREPARE identifier
    | DEALLOCATE ALL
    ;
```

The parser recognizes `PREPARE` as a keyword and parses the inner statement
recursively using the existing statement parser. The inner statement may
contain `$1`, `$2` parameter placeholders (already supported by the parser).

`EXECUTE` optionally takes a parenthesized list of expressions as parameter
values. Each expression is evaluated at execution time.

`DEALLOCATE ALL` removes all prepared statements from the connection.

### Prepared Statement Lifecycle

```
PREPARE my_query AS SELECT * FROM t WHERE id = $1
    ↓
    1. Parser produces PrepareStmt { Name: "my_query", Inner: SelectStmt{...} }
    2. Binder binds the inner statement (with parameter placeholders unresolved)
    3. Planner creates a physical plan
    4. Store { name, parsedStmt, boundStmt, physicalPlan, paramCount } in conn.preparedStatements
    5. Return success (no rows, no execution)

EXECUTE my_query(42)
    ↓
    1. Parser produces ExecuteStmt { Name: "my_query", Params: [IntLit(42)] }
    2. Executor looks up "my_query" in conn.preparedStatements
    3. Evaluate parameter expressions: [42]
    4. Substitute parameters into the cached physical plan
    5. Execute the plan with substituted parameters
    6. Return results

DEALLOCATE my_query
    ↓
    1. Parser produces DeallocateStmt { Name: "my_query" }
    2. Remove "my_query" from conn.preparedStatements
    3. Return success
```

### Connection Storage (`internal/engine/conn.go`)

```go
// sqlPreparedStatement holds a named SQL-level prepared statement.
type sqlPreparedStatement struct {
    name       string
    query      string            // Original SQL for debugging/EXPLAIN
    stmt       parser.Stmt       // Parsed AST of the inner statement
    boundStmt  binder.BoundStmt  // Bound statement with parameter placeholders
    plan       planner.PhysicalPlan // Cached physical plan
    paramCount int               // Number of $N parameters expected
}
```

Add to `EngineConn`:
```go
type EngineConn struct {
    // ... existing fields ...
    sqlPrepared map[string]*sqlPreparedStatement // Named SQL-level prepared statements
}
```

Initialize `sqlPrepared` as `make(map[string]*sqlPreparedStatement)` in
connection creation. The map is per-connection, not shared across connections.

### Parameter Substitution Mechanism

The existing parameter binding infrastructure (used by `database/sql` Prepare)
already supports `$1`, `$2` placeholders. For SQL-level EXECUTE, we reuse this:

1. The inner statement is parsed with `$N` placeholders preserved in the AST
2. At EXECUTE time, parameter expressions are evaluated to concrete values
3. The executor creates a parameter map: `{1: value1, 2: value2, ...}`
4. The plan is executed with this parameter map, using the same parameter
   substitution path as `EngineStmt.ExecContext`/`QueryContext`

This means we do NOT re-parse, re-bind, or re-plan on each EXECUTE. The
cached plan is reused with different parameter values.

#### Parameter Collection Updates

`internal/parser/parameters.go` must be updated to handle new statement types:

```go
// In paramCollector.collectStmt():
case *PrepareStmt:
    c.collectStmt(s.Inner) // Collect parameters from inner statement

case *ExecuteStmt:
    for _, param := range s.Params {
        c.collectExpr(param) // Collect from parameter expressions
    }

case *DeallocateStmt:
    // No parameters to collect
```

Similarly update `paramCounter.countStmt()` with matching cases.

#### EXECUTE Parameter Expression Scope

Parameter expressions in `EXECUTE name(expr1, expr2)` are evaluated in the
**current query scope**, NOT the prepared statement's scope. This means:
- Literal values work: `EXECUTE q(42, 'hello')`
- Expressions work: `EXECUTE q(1 + 2, CURRENT_DATE)`
- Scalar functions work: `EXECUTE q(length('abc'))`
- Column references do NOT work: `EXECUTE q(t.col)` — error, no table scope
- Subqueries do NOT work: `EXECUTE q((SELECT max(id) FROM t))` — error

Parameter expressions are bound and evaluated as standalone expressions before
being passed as values to the cached plan.

#### Execution Flow Detail

```go
func (e *Executor) executeExecute(ctx *ExecutionContext, plan *PhysicalExecute) {
    // 1. Look up prepared statement by name in connection
    prep := ctx.conn.sqlPrepared[plan.Name]
    if prep == nil {
        return error("prepared statement does not exist")
    }

    // 2. Validate parameter count
    if len(plan.ParamExprs) != prep.paramCount {
        return error("expected N parameters, got M")
    }

    // 3. Evaluate each parameter expression to a concrete value
    paramValues := make([]driver.NamedValue, len(plan.ParamExprs))
    for i, expr := range plan.ParamExprs {
        val := e.evaluateExpr(ctx, expr, nil)
        paramValues[i] = driver.NamedValue{Ordinal: i + 1, Value: val}
    }

    // 4. Execute the cached plan with parameter values
    //    Reuse the same path as EngineStmt.ExecContext/QueryContext
    return e.executePlanWithParams(ctx, prep.plan, paramValues)
}
```

### Plan Caching Strategy

The physical plan is computed once during PREPARE and cached. This provides:
- **Parse savings**: No re-parsing on each EXECUTE
- **Bind savings**: No re-resolution of table names, column types, etc.
- **Plan savings**: No re-optimization (join ordering, index selection, etc.)

The cached plan is invalidated when:
- `DEALLOCATE` is called (explicit removal)
- The connection is closed (implicit cleanup)
- A DDL statement modifies a referenced table (future work — initially the
  cached plan may become stale after DDL; this matches PostgreSQL behavior
  where prepared statements are invalidated after DDL in the same session)

Initially, we do NOT invalidate on DDL changes. If a table is altered after
PREPARE, the EXECUTE may fail with a runtime error. This is acceptable for
the initial implementation and matches common database behavior.

### Error Handling

| Condition | Error |
|-----------|-------|
| PREPARE with duplicate name | Error: prepared statement "name" already exists |
| EXECUTE with unknown name | Error: prepared statement "name" does not exist |
| EXECUTE with wrong param count | Error: expected N parameters, got M |
| DEALLOCATE with unknown name | Error: prepared statement "name" does not exist |
| EXECUTE after connection close | Error: connection is closed |

### Planner Changes (`internal/planner/physical.go`)

```go
type PhysicalPrepare struct {
    Name  string
    Inner planner.PhysicalPlan
    Query string // Original SQL text for display
}

type PhysicalExecute struct {
    Name       string
    ParamExprs []binder.BoundExpr // Bound parameter expressions
}

type PhysicalDeallocate struct {
    Name string // Empty means ALL
}
```

`PhysicalPrepare` and `PhysicalDeallocate` are handled entirely by the
executor (they modify connection state). `PhysicalExecute` triggers plan
lookup and execution with parameter substitution.

## Context

The existing `EngineConn.Prepare()` method (conn.go:1322) handles the
`database/sql` driver-level prepared statements. This creates `EngineStmt`
objects that are used through the Go driver interface. The SQL-level
PREPARE/EXECUTE is a separate mechanism that stores named plans within the
connection's SQL session, accessible only through SQL statements.

Both mechanisms share the same parameter infrastructure (`$1`, `$2` placeholders,
`parser.CollectParameters`, `parser.CountParameters`).

## Goals / Non-Goals

**Goals:**
- `PREPARE name AS statement` for all statement types (SELECT, INSERT, etc.)
- `EXECUTE name(params)` with parameter substitution
- `DEALLOCATE [PREPARE] name` and `DEALLOCATE ALL`
- Plan caching (parse + bind + plan once, execute many times)
- Correct parameter count validation

**Non-Goals:**
- Automatic plan invalidation on DDL changes (future work)
- PREPARE with explicit type annotations (`PREPARE name (type1, type2) AS ...`)
- Named parameters in PREPARE (only positional $N supported)
- Cross-connection prepared statement sharing

## Decisions

- **Reuse existing parameter infrastructure**: The `$1`/`$2` placeholder
  system, `CollectParameters`, and parameter binding in the executor already
  work. SQL-level EXECUTE just provides values for these placeholders.

- **Store plan, not just AST**: Caching the physical plan (not just parsed AST)
  provides the full performance benefit of prepared statements. Re-binding and
  re-planning would negate most of the advantage.

- **Per-connection storage**: Prepared statements are scoped to the connection,
  matching PostgreSQL and DuckDB semantics. No sharing across connections.

- **DEALLOCATE ALL**: Included for convenience and compatibility. Clears all
  prepared statements on the connection.

## Risks / Trade-offs

- **Stale plans after DDL**: If a table is dropped/altered after PREPARE, the
  cached plan may reference invalid schema. Mitigation: runtime errors during
  EXECUTE will indicate the issue; users can DEALLOCATE and re-PREPARE.

- **Memory usage**: Each prepared statement holds a full physical plan tree in
  memory. Mitigation: plans are typically small; DEALLOCATE ALL provides a
  manual cleanup mechanism.

## Open Questions

- Should `PREPARE name (type1, type2) AS ...` explicit type annotations be
  supported? Decision: No, defer to future work. DuckDB supports this but
  it's not commonly used.
