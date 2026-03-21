# Design: Named Window Definitions (WINDOW Clause)

## Architecture

This change touches two layers:

1. **Parser** (`internal/parser/ast.go`, `internal/parser/parser.go`): Add WindowDef AST node, parse WINDOW clause, support OVER with named reference
2. **Binder** (`internal/binder/bind_expr.go`, `internal/binder/bind_stmt.go`): Resolve named window references, merge inherited specs

No executor changes — the executor already operates on `BoundWindowExpr` which will be fully resolved by the binder.

## 1. AST Changes (internal/parser/ast.go)

### New WindowDef struct

Add after the existing WindowExpr type (around line 1122):

```go
// WindowDef represents a named window definition in a WINDOW clause.
// Example: WINDOW w AS (PARTITION BY dept ORDER BY salary)
type WindowDef struct {
    Name string      // Window name (e.g., "w")
    Spec *WindowSpec // Window specification
}

// WindowSpec holds the reusable parts of a window definition.
// Separated from WindowExpr because WindowExpr includes Function which
// is per-usage, while WindowSpec is the shared definition.
type WindowSpec struct {
    RefName     string          // Optional base window name for inheritance
    PartitionBy []Expr          // PARTITION BY expressions
    OrderBy     []WindowOrderBy // ORDER BY within window
    Frame       *WindowFrame    // Optional frame specification
}
```

### Extend SelectStmt

Add `Windows []WindowDef` field to `SelectStmt` (ast.go:34-53):

```go
type SelectStmt struct {
    // ... existing fields ...
    Sample     *SampleOptions
    Windows    []WindowDef     // NEW: WINDOW clause definitions
    Options    *RecursionOption
    // ... rest ...
}
```

### Extend WindowExpr

Add `RefName string` field to `WindowExpr` (ast.go:1114-1122) for the case where OVER references a named window:

```go
type WindowExpr struct {
    Function    *FunctionCall
    RefName     string          // NEW: named window reference (e.g., OVER w)
    PartitionBy []Expr
    OrderBy     []WindowOrderBy
    Frame       *WindowFrame
    IgnoreNulls bool
    Filter      Expr
    Distinct    bool
}
```

When `RefName` is set and no other fields are populated, it means `OVER w` (bare reference).
When `RefName` is set AND other fields are populated, it means `OVER (w ORDER BY ...)` (inheritance).

## 2. Parser Changes (internal/parser/parser.go)

### Parse WINDOW clause in parseSelect()

After QUALIFY clause (line 384) and before ORDER BY (line 386), add WINDOW parsing:

```go
// WINDOW clause - named window definitions
if p.isKeyword("WINDOW") {
    p.advance()
    windows, err := p.parseWindowDefs()
    if err != nil {
        return nil, err
    }
    stmt.Windows = windows
}
```

SQL standard clause ordering: `... QUALIFY ... WINDOW ... ORDER BY ... LIMIT ...`

### New parseWindowDefs() function

```go
// parseWindowDefs parses WINDOW name AS (spec) [, name AS (spec) ...]
func (p *parser) parseWindowDefs() ([]WindowDef, error) {
    var defs []WindowDef
    for {
        // Parse window name
        name, err := p.expect(tokenIdent)
        if err != nil {
            return nil, p.errorf("expected window name")
        }

        if err := p.expectKeyword("AS"); err != nil {
            return nil, err
        }

        if _, err := p.expect(tokenLParen); err != nil {
            return nil, err
        }

        // Parse window specification (reuse parseWindowSpec logic)
        spec := &WindowSpec{}
        if err := p.parseWindowSpecForDef(spec); err != nil {
            return nil, err
        }

        if _, err := p.expect(tokenRParen); err != nil {
            return nil, err
        }

        defs = append(defs, WindowDef{Name: name.value, Spec: spec})

        // Check for comma (more definitions)
        if p.current().typ != tokenComma {
            break
        }
        p.advance() // consume comma
    }
    return defs, nil
}
```

### New parseWindowSpecForDef() function

Similar to existing `parseWindowSpec()` at line 5325 but populates a `WindowSpec` instead of `WindowExpr`. Can share internal logic:

```go
func (p *parser) parseWindowSpecForDef(spec *WindowSpec) error {
    // Check for base window reference (inheritance)
    if p.current().typ == tokenIdent && !p.isKeyword("PARTITION") &&
       !p.isKeyword("ORDER") && !p.isKeyword("ROWS") &&
       !p.isKeyword("RANGE") && !p.isKeyword("GROUPS") {
        spec.RefName = p.advance().value
    }

    // PARTITION BY
    if p.isKeyword("PARTITION") {
        p.advance()
        if err := p.expectKeyword("BY"); err != nil {
            return err
        }
        partBy, err := p.parseWindowExprList()
        if err != nil {
            return err
        }
        spec.PartitionBy = partBy
    }

    // ORDER BY
    if p.isKeyword("ORDER") {
        p.advance()
        if err := p.expectKeyword("BY"); err != nil {
            return err
        }
        orderBy, err := p.parseWindowOrderBy()
        if err != nil {
            return err
        }
        spec.OrderBy = orderBy
    }

    // Frame specification (ROWS/RANGE/GROUPS)
    if p.isKeyword("ROWS") || p.isKeyword("RANGE") || p.isKeyword("GROUPS") {
        frame, err := p.parseWindowFrame()
        if err != nil {
            return err
        }
        spec.Frame = frame
    }

    return nil
}
```

**Note:** `parseWindowExprList()` exists at line 5366, `parseWindowOrderBy()` at line 5399, and the frame parsing logic is already in `parseWindowSpec()` starting at line 5325. The new function follows the same pattern.

### Modify OVER clause parsing for named references

In `maybeParseWindowExpr()` at line 5306, after consuming OVER, allow either `(` for inline spec or identifier for named reference:

```go
// Parse OVER clause
p.advance() // consume OVER

// Check for named window reference: OVER w
if p.current().typ == tokenIdent {
    // Bare name reference: OVER w
    windowExpr.RefName = p.advance().value
    return windowExpr, nil
}

// Check for window spec with optional base: OVER (w ORDER BY ...)
if _, err := p.expect(tokenLParen); err != nil {
    return nil, err
}

// Inside parens, check for base window reference before spec
if p.current().typ == tokenIdent && !p.isKeyword("PARTITION") &&
   !p.isKeyword("ORDER") && !p.isKeyword("ROWS") &&
   !p.isKeyword("RANGE") && !p.isKeyword("GROUPS") {
    windowExpr.RefName = p.advance().value
}

// Parse remaining window specification
if err := p.parseWindowSpec(windowExpr); err != nil {
    return nil, err
}

if _, err := p.expect(tokenRParen); err != nil {
    return nil, err
}

return windowExpr, nil
```

This replaces the current lines 5308-5321 which only handle `(spec)`.

### Update keyword stop conditions

Multiple places in the parser check for keywords to stop parsing expressions. Add `"WINDOW"` to these keyword lists alongside existing entries like `"HAVING"`, `"QUALIFY"`:

- Line 654: `!p.isKeyword("QUALIFY") &&` → add `!p.isKeyword("WINDOW") &&`
- Line 686: same pattern
- Line 864: same pattern
- Line 903: same pattern
- Line 952: same pattern

## 3. Binder Changes

### Store window definitions during binding

In `bindSelect()` (bind_stmt.go), after binding the FROM clause and before binding SELECT columns, collect window definitions:

```go
// Collect WINDOW clause definitions into a map for resolution
windowDefs := make(map[string]*parser.WindowSpec)
for _, wd := range stmt.Windows {
    if _, exists := windowDefs[wd.Name]; exists {
        return nil, fmt.Errorf("duplicate window name: %q", wd.Name)
    }
    windowDefs[wd.Name] = wd.Spec
}
// Store on binder for use during expression binding
b.windowDefs = windowDefs
```

Add `windowDefs map[string]*parser.WindowSpec` field to the `Binder` struct.

### Resolve named references in bindWindowExpr()

In `bindWindowExpr()` at `bind_expr.go:785`, before processing PartitionBy/OrderBy/Frame, check for RefName and merge:

```go
func (b *Binder) bindWindowExpr(e *parser.WindowExpr) (*BoundWindowExpr, error) {
    // Resolve named window reference
    if e.RefName != "" {
        spec, ok := b.windowDefs[e.RefName]
        if !ok {
            return nil, fmt.Errorf("window %q is not defined", e.RefName)
        }

        // Merge: base spec provides defaults, inline overrides extend
        if len(e.PartitionBy) == 0 && len(spec.PartitionBy) > 0 {
            e.PartitionBy = spec.PartitionBy
        } else if len(e.PartitionBy) > 0 && len(spec.PartitionBy) > 0 {
            return nil, fmt.Errorf("cannot override PARTITION BY of window %q", e.RefName)
        }

        if len(e.OrderBy) == 0 && len(spec.OrderBy) > 0 {
            e.OrderBy = spec.OrderBy
        } else if len(e.OrderBy) > 0 && len(spec.OrderBy) > 0 {
            return nil, fmt.Errorf("cannot override ORDER BY of window %q", e.RefName)
        }

        if e.Frame == nil && spec.Frame != nil {
            e.Frame = spec.Frame
        } else if e.Frame != nil && spec.Frame != nil {
            return nil, fmt.Errorf("cannot override frame of window %q", e.RefName)
        }

        // Handle transitive references (base window references another)
        if spec.RefName != "" {
            // Recursively resolve — but check for cycles
            // Implementation: resolve all defs in topological order during bindSelect
        }
    }

    // ... rest of existing bindWindowExpr logic unchanged ...
}
```

### Cycle detection for transitive references

A named window can reference another named window:
```sql
WINDOW w1 AS (PARTITION BY dept),
       w2 AS (w1 ORDER BY salary)
```

During `bindSelect()`, resolve transitive references before binding expressions. Check for cycles:

```go
func resolveWindowDefs(defs map[string]*parser.WindowSpec) error {
    visited := make(map[string]bool)
    resolving := make(map[string]bool)

    var resolve func(name string) error
    resolve = func(name string) error {
        if resolving[name] {
            return fmt.Errorf("circular window reference: %q", name)
        }
        if visited[name] {
            return nil
        }
        resolving[name] = true
        spec := defs[name]
        if spec.RefName != "" {
            base, ok := defs[spec.RefName]
            if !ok {
                return fmt.Errorf("window %q references undefined window %q", name, spec.RefName)
            }
            if err := resolve(spec.RefName); err != nil {
                return err
            }
            // Merge base into spec
            mergeWindowSpec(spec, base)
        }
        visited[name] = true
        resolving[name] = false
        return nil
    }

    for name := range defs {
        if err := resolve(name); err != nil {
            return err
        }
    }
    return nil
}
```

## 4. Keyword Stop Conditions

The parser uses keyword checks to determine when an expression ends. The `WINDOW` keyword must be added as a stop condition in all relevant locations so that expressions like `... QUALIFY expr WINDOW w AS (...)` correctly stop parsing the QUALIFY expression before consuming WINDOW.

Locations to update (all in `parser.go`):
- Line 654: add `!p.isKeyword("WINDOW")`
- Line 686: add `!p.isKeyword("WINDOW")`
- Line 864: add `!p.isKeyword("WINDOW")`
- Line 903: add `!p.isKeyword("WINDOW")`
- Line 952: add `!p.isKeyword("WINDOW")`

## Helper Signatures Reference

- `parseWindowExprList()` — `parser.go:5366` — parses comma-separated expression list
- `parseWindowOrderBy()` — `parser.go:5399` — parses ORDER BY with NULLS FIRST/LAST
- `parseWindowSpec(windowExpr *WindowExpr)` — `parser.go:5325` — parses window spec into WindowExpr
- `bindWindowExpr(e *parser.WindowExpr)` — `bind_expr.go:785` — binds WindowExpr to BoundWindowExpr

## Testing Strategy

Integration tests via `database/sql`:
1. Named window with multiple functions: `SELECT ROW_NUMBER() OVER w, SUM(x) OVER w FROM t WINDOW w AS (ORDER BY x)`
2. Window inheritance: `SELECT SUM(x) OVER (w ORDER BY y) FROM t WINDOW w AS (PARTITION BY dept)`
3. Multiple named windows: `WINDOW w1 AS (...), w2 AS (...)`
4. Transitive reference: `WINDOW w1 AS (...), w2 AS (w1 ORDER BY x)`
5. Error: undefined window name
6. Error: circular reference
7. Error: override existing clause
8. Named window with frame spec
9. Subquery with own WINDOW clause
