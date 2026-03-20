# CREATE MACRO / TABLE MACRO - Design Details

## Implementation Details

### Catalog Storage

Macros are stored in the catalog alongside views, sequences, and other schema objects. Each schema maintains a map of macro definitions.

```go
// MacroParam represents a parameter in a macro definition.
type MacroParam struct {
    // Name is the parameter name (e.g., "a", "b").
    Name string

    // DefaultExpr is the optional default value expression as a raw SQL string.
    // Empty string means no default (parameter is required).
    DefaultExpr string

    // HasDefault indicates whether this parameter has a default value.
    HasDefault bool
}

// MacroType distinguishes scalar macros from table macros.
type MacroType int

const (
    // MacroTypeScalar is a macro that returns a single value (expression macro).
    MacroTypeScalar MacroType = iota

    // MacroTypeTable is a macro that returns a table (table macro).
    MacroTypeTable
)

// MacroDef represents a macro definition in the catalog.
type MacroDef struct {
    // Name is the macro name.
    Name string

    // Schema is the schema this macro belongs to.
    Schema string

    // Params is the ordered list of macro parameters.
    Params []MacroParam

    // Type indicates whether this is a scalar or table macro.
    Type MacroType

    // Body is the raw SQL expression for scalar macros.
    // For example, for `CREATE MACRO add(a, b) AS a + b`, Body is "a + b".
    Body string

    // Query is the raw SQL query string for table macros.
    // For example, for `CREATE MACRO my_range(x) AS TABLE SELECT * FROM range(x)`,
    // Query is "SELECT * FROM range(x)".
    Query string
}
```

**Catalog Integration**:

The `Catalog` struct gains a `macros` map per schema, similar to how views and sequences are stored:

```go
// In catalog.go, within the schema structure:
type schemaObjects struct {
    tables    map[string]*TableDef
    views     map[string]*ViewDef
    indexes   map[string]*IndexDef
    sequences map[string]*SequenceDef
    macros    map[string]*MacroDef  // NEW
}
```

Methods added to Catalog:
- `CreateMacro(schema string, def *MacroDef, orReplace bool) error`
- `DropMacro(schema, name string, ifExists bool) error`
- `GetMacro(schema, name string) (*MacroDef, error)`
- `ListMacros(schema string) []*MacroDef`

**DropSchemaIfExists Update**:

`catalog.go` `DropSchemaIfExists` currently checks `len(schema.tables) > 0 || len(schema.views) > 0 || len(schema.indexes) > 0 || len(schema.sequences) > 0` to determine if a schema contains objects. This check MUST be updated to also include `len(schema.macros) > 0`, otherwise dropping a schema that only contains macros would silently succeed without CASCADE, violating the "contains objects" guard.

### AST Nodes

```go
// CreateMacroStmt represents a CREATE MACRO statement.
type CreateMacroStmt struct {
    Schema      string        // Schema name (empty for default)
    Name        string        // Macro name
    Params      []MacroParam  // Ordered parameters with optional defaults
    IsTableMacro bool         // True for TABLE macros
    OrReplace   bool          // True for CREATE OR REPLACE MACRO
    Body        Expr          // Expression body for scalar macros (parsed AST)
    BodySQL     string        // Raw SQL body string for storage
    Query       *SelectStmt   // Query body for table macros
    QuerySQL    string        // Raw SQL query string for storage
}

func (*CreateMacroStmt) stmtNode() {}
func (*CreateMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// MacroParam is defined in the parser package as well for AST use.
type MacroParam struct {
    Name       string
    Default    Expr   // Parsed default expression (nil if no default)
    DefaultSQL string // Raw SQL default expression for storage
}

// DropMacroStmt represents a DROP MACRO statement.
type DropMacroStmt struct {
    Schema       string
    Name         string
    IfExists     bool
    IsTableMacro bool // True for DROP MACRO TABLE name
}

func (*DropMacroStmt) stmtNode() {}
func (*DropMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }
```

### Bound Statement Types

The binder produces bound statement types for macro DDL, following the same pattern as `BoundCreateViewStmt`/`BoundDropViewStmt` (see `internal/binder/statements.go`). These bound types are consumed by the planner to produce physical plan nodes.

```go
// BoundCreateMacroStmt represents a bound CREATE MACRO statement.
// Located in internal/binder/statements.go alongside other Bound*Stmt types.
type BoundCreateMacroStmt struct {
    Schema       string
    Name         string
    Params       []catalog.MacroParam
    IsTableMacro bool
    OrReplace    bool
    BodySQL      string // Raw SQL body for scalar macros
    QuerySQL     string // Raw SQL query for table macros
}

func (*BoundCreateMacroStmt) boundStmtNode() {}
func (*BoundCreateMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropMacroStmt represents a bound DROP MACRO statement.
type BoundDropMacroStmt struct {
    Schema       string
    Name         string
    IfExists     bool
    IsTableMacro bool
}

func (*BoundDropMacroStmt) boundStmtNode() {}
func (*BoundDropMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }
```

**Binder DDL binding** (in `internal/binder/bind_ddl.go`, alongside existing `bindCreateView`/`bindDropView`):

```go
func (b *Binder) bindCreateMacro(stmt *parser.CreateMacroStmt) (*BoundCreateMacroStmt, error) {
    schema := b.resolveSchema(stmt.Schema)

    // Check for existing macro (unless OR REPLACE)
    if !stmt.OrReplace {
        if _, err := b.catalog.GetMacro(schema, stmt.Name); err == nil {
            return nil, b.errorf("macro %s already exists", stmt.Name)
        }
    }

    // Validate parameter defaults ordering (required before optional)
    seenDefault := false
    for _, p := range stmt.Params {
        if p.Default != nil {
            seenDefault = true
        } else if seenDefault {
            return nil, b.errorf("required parameters must precede default parameters")
        }
    }

    return &BoundCreateMacroStmt{
        Schema:       schema,
        Name:         stmt.Name,
        Params:       convertParamsToCatalog(stmt.Params),
        IsTableMacro: stmt.IsTableMacro,
        OrReplace:    stmt.OrReplace,
        BodySQL:      stmt.BodySQL,
        QuerySQL:     stmt.QuerySQL,
    }, nil
}

func (b *Binder) bindDropMacro(stmt *parser.DropMacroStmt) (*BoundDropMacroStmt, error) {
    schema := b.resolveSchema(stmt.Schema)

    if !stmt.IfExists {
        if _, err := b.catalog.GetMacro(schema, stmt.Name); err != nil {
            return nil, b.errorf("macro %s does not exist", stmt.Name)
        }
    }

    return &BoundDropMacroStmt{
        Schema:       schema,
        Name:         stmt.Name,
        IfExists:     stmt.IfExists,
        IsTableMacro: stmt.IsTableMacro,
    }, nil
}
```

### Parsing

The parser handles the following syntax forms:

```sql
-- Scalar macro
CREATE MACRO name(param1, param2) AS expression;
CREATE MACRO name(param1, param2 := default_expr) AS expression;
CREATE OR REPLACE MACRO name(params) AS expression;

-- Table macro
CREATE MACRO name(params) AS TABLE select_statement;

-- Drop
DROP MACRO name;
DROP MACRO IF EXISTS name;
DROP MACRO TABLE name;
DROP MACRO TABLE IF EXISTS name;
```

**Parsing Strategy**:

1. After parsing `CREATE`, check for `MACRO` keyword
2. Parse macro name (optionally schema-qualified)
3. Parse parameter list in parentheses:
   - Each parameter is an identifier
   - Optional `:=` or `DEFAULT` followed by a default expression
   - Parameters with defaults must come after parameters without defaults
4. Consume `AS` keyword
5. If next token is `TABLE`, parse a `SelectStmt` for the table macro body
6. Otherwise, parse an expression for the scalar macro body

### Macro Expansion in the Binder

Macro expansion happens during the binding phase, similar to view expansion. When the binder encounters a function call that matches a macro name, it expands it inline.

**Scalar Macro Expansion**:

```
SQL: SELECT add(x, 1) FROM t;
Macro: CREATE MACRO add(a, b) AS a + b;

Expansion:
1. Binder sees function call add(x, 1)
2. Looks up "add" in catalog macros
3. Finds scalar macro with params [a, b] and body "a + b"
4. Substitutes: a -> x, b -> 1
5. Result expression: x + 1
6. Replaces the function call node with the substituted expression
```

Implementation approach:
- Re-parse the macro body SQL each time it is expanded (ensures fresh AST)
- Walk the parsed expression tree and replace `ColumnRef` nodes matching parameter names with the corresponding argument expressions
- If fewer arguments than parameters, fill from defaults (error if no default)
- If more arguments than parameters, raise an error

```go
// expandScalarMacro replaces a function call with the macro's expanded expression.
// Uses parser.Parse() with type assertion, matching the view expansion pattern
// (see bindViewRef in bind_stmt.go).
func (b *Binder) expandScalarMacro(macro *catalog.MacroDef, args []Expr) (Expr, error) {
    // 1. Parse macro body as "SELECT <body>" and extract the expression.
    //    parser.ParseExpr() does not exist; we wrap in SELECT and type-assert.
    wrapSQL := "SELECT " + macro.Body
    stmt, err := parser.Parse(wrapSQL)
    if err != nil {
        return nil, fmt.Errorf("invalid macro body: %w", err)
    }
    selectStmt, ok := stmt.(*parser.SelectStmt)
    if !ok || len(selectStmt.Columns) == 0 {
        return nil, fmt.Errorf("invalid macro body: expected expression")
    }
    bodyExpr := selectStmt.Columns[0].Expr

    // 2. Build parameter -> argument substitution map
    subst := make(map[string]Expr)
    for i, param := range macro.Params {
        if i < len(args) {
            subst[param.Name] = args[i]
        } else if param.HasDefault {
            defSQL := "SELECT " + param.DefaultExpr
            defStmt, err := parser.Parse(defSQL)
            if err != nil {
                return nil, fmt.Errorf("invalid default for param %s: %w", param.Name, err)
            }
            defSelect, ok := defStmt.(*parser.SelectStmt)
            if !ok || len(defSelect.Columns) == 0 {
                return nil, fmt.Errorf("invalid default for param %s: expected expression", param.Name)
            }
            subst[param.Name] = defSelect.Columns[0].Expr
        } else {
            return nil, fmt.Errorf("missing argument for parameter %s", param.Name)
        }
    }

    // 3. Substitute parameters in body expression
    return substituteParams(bodyExpr, subst), nil
}
```

**Table Macro Expansion**:

Table macros expand in the FROM clause. When the binder encounters a table function reference that matches a table macro, it expands it to a subquery.

```
SQL: SELECT * FROM my_range(10);
Macro: CREATE MACRO my_range(x) AS TABLE SELECT * FROM range(x);

Expansion:
1. Binder sees table function my_range(10)
2. Looks up "my_range" in catalog macros
3. Finds table macro with params [x] and query "SELECT * FROM range(x)"
4. Re-parses query, substitutes x -> 10
5. Result: subquery (SELECT * FROM range(10))
6. Replaces the table function reference with the subquery
```

```go
// expandTableMacro replaces a table function ref with the macro's expanded subquery.
// Uses parser.Parse() with type assertion, matching the view expansion pattern
// (see bindViewRef in bind_stmt.go).
func (b *Binder) expandTableMacro(macro *catalog.MacroDef, args []Expr) (*SelectStmt, error) {
    // 1. Parse macro query using parser.Parse() and assert *parser.SelectStmt.
    //    parser.ParseSelect() does not exist; use the same pattern as view expansion.
    stmt, err := parser.Parse(macro.Query)
    if err != nil {
        return nil, fmt.Errorf("invalid table macro query: %w", err)
    }
    query, ok := stmt.(*parser.SelectStmt)
    if !ok {
        return nil, fmt.Errorf("invalid table macro query: expected SELECT statement")
    }

    // 2. Build substitution map (same as scalar)
    subst := buildSubstMap(macro.Params, args)

    // 3. Walk the SelectStmt and substitute parameter references
    return substituteParamsInSelect(query, subst), nil
}
```

### DDL Execution

Macro DDL follows the same pipeline as views: parser AST -> binder (BoundStatement) -> planner (PhysicalPlan node) -> executor. The executor receives physical plan nodes, NOT raw parser AST. This matches the existing Create/Drop View pattern (see `PhysicalCreateView`/`PhysicalDropView` in `internal/planner/physical.go` and `executeCreateView`/`executeDropView` in `internal/executor/ddl.go`).

**Physical Plan Nodes** (in `internal/planner/physical.go`):

```go
// PhysicalCreateMacro represents a physical CREATE MACRO operation.
type PhysicalCreateMacro struct {
    Schema       string
    Name         string
    Params       []catalog.MacroParam
    IsTableMacro bool
    OrReplace    bool
    BodySQL      string // Raw SQL body for scalar macros
    QuerySQL     string // Raw SQL query for table macros
}

func (*PhysicalCreateMacro) physicalPlanNode()                  {}
func (*PhysicalCreateMacro) Children() []PhysicalPlan           { return nil }
func (*PhysicalCreateMacro) OutputColumns() []ColumnBinding     { return nil }

// PhysicalDropMacro represents a physical DROP MACRO operation.
type PhysicalDropMacro struct {
    Schema       string
    Name         string
    IfExists     bool
    IsTableMacro bool
}

func (*PhysicalDropMacro) physicalPlanNode()                    {}
func (*PhysicalDropMacro) Children() []PhysicalPlan             { return nil }
func (*PhysicalDropMacro) OutputColumns() []ColumnBinding       { return nil }
```

**Planner mapping** (in `internal/planner/physical.go`, alongside existing view/index/sequence cases):

```go
case *binder.BoundCreateMacroStmt:
    return &PhysicalCreateMacro{
        Schema:       s.Schema,
        Name:         s.Name,
        Params:       s.Params,
        IsTableMacro: s.IsTableMacro,
        OrReplace:    s.OrReplace,
        BodySQL:      s.BodySQL,
        QuerySQL:     s.QuerySQL,
    }, nil
case *binder.BoundDropMacroStmt:
    return &PhysicalDropMacro{
        Schema:       s.Schema,
        Name:         s.Name,
        IfExists:     s.IfExists,
        IsTableMacro: s.IsTableMacro,
    }, nil
```

**Executor dispatch** (in `internal/executor/operator.go`, alongside existing view cases):

```go
case *planner.PhysicalCreateMacro:
    return e.executeCreateMacro(ctx, p)
case *planner.PhysicalDropMacro:
    return e.executeDropMacro(ctx, p)
```

**Executor implementation** (in `internal/executor/ddl.go`):

```go
func (e *Executor) executeCreateMacro(
    ctx *ExecutionContext,
    plan *planner.PhysicalCreateMacro,
) (*ExecutionResult, error) {
    def := &catalog.MacroDef{
        Name:   plan.Name,
        Schema: plan.Schema,
        Params: plan.Params,
        Type:   macroTypeFromPlan(plan),
        Body:   plan.BodySQL,
        Query:  plan.QuerySQL,
    }
    return &ExecutionResult{RowsAffected: 0},
        e.catalog.CreateMacro(plan.Schema, def, plan.OrReplace)
}

func (e *Executor) executeDropMacro(
    ctx *ExecutionContext,
    plan *planner.PhysicalDropMacro,
) (*ExecutionResult, error) {
    return &ExecutionResult{RowsAffected: 0},
        e.catalog.DropMacro(plan.Schema, plan.Name, plan.IfExists)
}
```

## Context

**Architecture Alignment**:
- Follows the same pattern as views: catalog storage + inline expansion in binder
- Reuses existing `Expr` and `SelectStmt` AST infrastructure
- Integrates with existing DDL execution path in `executor/ddl.go`
- Macro names live in the same namespace resolution as functions

**Key Differences from Views**:
- Views are expanded in the FROM clause only; scalar macros expand in expressions
- Macros accept parameters; views do not
- Table macros are conceptually similar to parameterized views
- Macros store raw SQL and re-parse on each expansion (simpler, avoids AST serialization)

**Integration Points**:
1. `parser.Parse()` - recognize CREATE/DROP MACRO
2. `catalog.CreateMacro()` / `catalog.DropMacro()` - store/remove definitions
3. `binder.bindFunctionCall()` (`internal/binder/bind_expr.go`) - check macros BEFORE UDF/built-in lookup (see below)
4. `binder.resolveTableRef()` - check table macros before built-in table functions
5. `planner.createPhysicalPlan()` - map `BoundCreateMacroStmt`/`BoundDropMacroStmt` to physical plan nodes
6. `executor` operator dispatch - handle `PhysicalCreateMacro`/`PhysicalDropMacro`
7. `catalog.DropSchemaIfExists()` - include `len(schema.macros) > 0` in object check

**Scalar Macro Lookup in bindFunctionCall**:

The macro check MUST be inserted in `bindFunctionCall()` (in `internal/binder/bind_expr.go`) BEFORE the existing UDF/built-in function argument binding logic. This ensures macros shadow built-in functions, matching DuckDB semantics. The insertion point is after the GROUPING check and before `getFunctionArgTypes`:

```go
func (b *Binder) bindFunctionCall(f *parser.FunctionCall) (BoundExpr, error) {
    funcNameUpper := strings.ToUpper(f.Name)

    // Handle sequence functions (NEXTVAL, CURRVAL)
    if funcNameUpper == "NEXTVAL" || funcNameUpper == "CURRVAL" {
        return b.bindSequenceCall(f)
    }

    // Handle GROUPING() function
    if funcNameUpper == "GROUPING" {
        return b.bindGroupingCall(f)
    }

    // === NEW: Check for scalar macro BEFORE built-in/UDF lookup ===
    if macro, err := b.catalog.GetMacro("", f.Name); err == nil && macro.Type == catalog.MacroTypeScalar {
        // Convert parser args to unbound Expr slice for expansion
        expanded, err := b.expandScalarMacro(macro, f.Args)
        if err != nil {
            return nil, err
        }
        // Bind the expanded expression (enables nested macro expansion)
        return b.bindExpr(expanded, dukdb.TYPE_ANY)
    }
    // === END NEW ===

    // Get expected argument types from function signature if available
    argTypes := getFunctionArgTypes(f.Name, len(f.Args))
    // ... existing logic continues ...
}
```

**Table Macro Lookup in resolveTableRef**:

Similarly, table macro lookup must be checked when resolving table references in FROM clauses, before falling through to table/view lookup.

## Goals / Non-Goals

**Goals**:
- Full DuckDB-compatible CREATE MACRO syntax for scalar macros
- Full DuckDB-compatible CREATE MACRO ... AS TABLE syntax for table macros
- Parameter substitution with support for default values
- CREATE OR REPLACE MACRO support
- DROP MACRO / DROP MACRO IF EXISTS support
- Macro definitions persisted in catalog (in-memory, same as other objects)

**Non-Goals**:
- Macro overloading (multiple macros with same name, different parameter counts)
- Recursive macro expansion (macro calling itself)
- Macro type checking at definition time (types checked at expansion time)
- WAL persistence of macro definitions (future work)
- Cross-database macro references

## Decisions

**Decision 1: Store Raw SQL vs Parsed AST**
- **Choice**: Store raw SQL strings, re-parse on each expansion
- **Rationale**: Simpler serialization, avoids AST versioning issues, matches view pattern
- **Alternative**: Store serialized AST - more complex, faster expansion

**Decision 2: Expansion Phase**
- **Choice**: Expand in binder (before planning)
- **Rationale**: Macro expansion is name resolution, which is the binder's job. Expanding early means the planner and optimizer see the expanded query and can optimize it fully.
- **Alternative**: Expand in executor - misses optimization opportunities

**Decision 3: Namespace**
- **Choice**: Macros share namespace with built-in functions; macro takes precedence
- **Rationale**: Matches DuckDB behavior where user macros shadow built-in functions
- **Alternative**: Separate namespace - would require explicit qualification

**Decision 4: Parameter Passing**
- **Choice**: Positional parameters with optional named defaults using `:=`
- **Rationale**: Matches DuckDB syntax
- **Alternative**: Named-only parameters - breaks DuckDB compatibility

## Risks / Trade-offs

**Risk 1: Re-parsing overhead on every macro call**
- **Mitigation**: Macro bodies are typically small expressions; parsing overhead is negligible
- **Trade-off**: Could cache parsed ASTs per macro if this becomes a bottleneck

**Risk 2: Parameter name collisions with column names**
- **Mitigation**: Parameter substitution only replaces unqualified column references matching parameter names, which matches DuckDB semantics
- **Trade-off**: Users must avoid parameter names that collide with column names in unexpected ways

**Risk 3: Infinite expansion with nested macros**
- **Mitigation**: Set a maximum expansion depth (e.g., 32 levels) and error if exceeded
- **Trade-off**: Deeply nested macros will fail, but this is a reasonable limit

## Migration Plan

This is a purely additive change. No existing functionality is modified.

1. **Phase 1**: Add catalog `MacroDef` and storage methods
2. **Phase 2**: Add parser support for CREATE/DROP MACRO syntax
3. **Phase 3**: Add DDL execution for CREATE/DROP MACRO
4. **Phase 4**: Add scalar macro expansion in binder
5. **Phase 5**: Add table macro expansion in binder
6. **Phase 6**: Comprehensive testing
