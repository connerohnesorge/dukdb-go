# Design: SELECT * Modifiers for DuckDB v1.4.3

## Architecture

This change touches three layers:

1. **Parser** (`internal/parser/ast.go`, `internal/parser/parser.go`): Extend AST nodes and parsing logic
2. **Binder** (`internal/binder/bind_expr.go`, `internal/binder/bind_stmt.go`): Apply modifiers during star expansion
3. No executor changes — modifiers are fully resolved at bind time

## 1. AST Changes (internal/parser/ast.go)

### Extend StarExpr (currently at line 892)

```go
type StarExpr struct {
    Table    string           // optional table prefix (e.g., t.*)
    Exclude  []string         // EXCLUDE(col1, col2, ...)
    Replace  []ReplaceColumn  // REPLACE(expr AS col, ...)
}

// ReplaceColumn represents a column replacement in SELECT * REPLACE(...)
type ReplaceColumn struct {
    Expr    Expr   // replacement expression
    Column  string // column name to replace
}
```

### Add ColumnsExpr (new type)

```go
// ColumnsExpr represents COLUMNS('regex') — selects columns matching a pattern
type ColumnsExpr struct {
    Pattern string // regex pattern to match column names
}
```

`ColumnsExpr` must implement the `Expr` interface (provide a `String()` method or whatever the interface requires).

## 2. Parser Changes (internal/parser/parser.go)

### EXCLUDE and REPLACE parsing

In `parseSelectColumns()` at line 576, after creating the `StarExpr` (lines 585-586), check for EXCLUDE and REPLACE keywords:

```go
// After creating starExpr at line 586:
if p.isKeyword("EXCLUDE") {
    p.advance() // consume EXCLUDE
    p.expect(tokenLParen)
    for {
        colName := p.expectIdentifier()
        starExpr.Exclude = append(starExpr.Exclude, colName)
        if p.current().typ != tokenComma {
            break
        }
        p.advance() // consume comma
    }
    p.expect(tokenRParen)
}
if p.isKeyword("REPLACE") {
    p.advance() // consume REPLACE
    p.expect(tokenLParen)
    for {
        expr := p.parseExpr()
        p.expectKeyword("AS")
        colName := p.expectIdentifier()
        starExpr.Replace = append(starExpr.Replace, ReplaceColumn{
            Expr:   expr,
            Column: colName,
        })
        if p.current().typ != tokenComma {
            break
        }
        p.advance() // consume comma
    }
    p.expect(tokenRParen)
}
```

**Keyword conflict resolution:** The EXCLUDE keyword also appears in window frame specifications (`ROWS BETWEEN ... EXCLUDE CURRENT ROW`). This is not a conflict because:
- Star EXCLUDE appears immediately after `*` or `t.*` in the SELECT column list
- Window EXCLUDE appears inside `OVER(...)` clause after frame specification
- The parser context is different — `parseSelectColumns()` vs `parseWindowSpec()`

### COLUMNS expression parsing

In the expression parser (`parseUnaryOrPrimary()` around line 4793), add a check for COLUMNS keyword:

```go
if p.isKeyword("COLUMNS") {
    p.advance() // consume COLUMNS
    p.expect(tokenLParen)
    // Expect a string literal for the regex pattern
    pattern := p.expectString()
    p.expect(tokenRParen)
    return &ColumnsExpr{Pattern: pattern}
}
```

This should be placed before the general function call parsing to avoid COLUMNS being treated as a regular function.

## 3. Binder Changes

### Star EXCLUDE/REPLACE (internal/binder/bind_expr.go)

Modify `bindStarExpr()` at line 643 to apply modifiers after column collection:

```go
func (b *Binder) bindStarExpr(e *parser.StarExpr) (*BoundStarExpr, error) {
    bound := &BoundStarExpr{Table: e.Table}

    // Collect columns (existing logic at lines 648-663)
    if e.Table != "" {
        tableRef, ok := b.scope.tables[e.Table]
        if !ok {
            return nil, b.errorf("table not found: %s", e.Table)
        }
        bound.Columns = tableRef.Columns
    } else {
        for _, tableRef := range b.scope.tables {
            bound.Columns = append(bound.Columns, tableRef.Columns...)
        }
    }

    // NEW: Apply EXCLUDE filter
    if len(e.Exclude) > 0 {
        excludeSet := make(map[string]bool, len(e.Exclude))
        for _, col := range e.Exclude {
            excludeSet[strings.ToUpper(col)] = true
        }
        var filtered []*BoundColumn
        for _, col := range bound.Columns {
            if !excludeSet[strings.ToUpper(col.Column)] {
                filtered = append(filtered, col)
            }
        }
        // Validate all excluded columns exist
        for _, col := range e.Exclude {
            found := false
            for _, bc := range bound.Columns {
                if strings.EqualFold(bc.Column, col) {
                    found = true
                    break
                }
            }
            if !found {
                return nil, b.errorf("EXCLUDE column not found: %s", col)
            }
        }
        bound.Columns = filtered
    }

    // NEW: Apply REPLACE substitutions
    // Replacements are stored on the BoundStarExpr and applied when
    // the star is expanded to individual select columns in bindSelect()
    if len(e.Replace) > 0 {
        bound.Replacements = make(map[string]BoundExpr)
        for _, rep := range e.Replace {
            // Bind the replacement expression
            boundExpr, err := b.bindExpr(rep.Expr)
            if err != nil {
                return nil, err
            }
            bound.Replacements[strings.ToUpper(rep.Column)] = boundExpr
            // Validate column exists
            found := false
            for _, bc := range bound.Columns {
                if strings.EqualFold(bc.Column, rep.Column) {
                    found = true
                    break
                }
            }
            if !found {
                return nil, b.errorf("REPLACE column not found: %s", rep.Column)
            }
        }
    }

    return bound, nil
}
```

### BoundStarExpr extension (internal/binder/expressions.go)

Extend `BoundStarExpr` (line 188) to hold replacements:

```go
type BoundStarExpr struct {
    Table        string
    Columns      []*BoundColumn
    Replacements map[string]BoundExpr // NEW: column name → replacement expression
}
```

### Star expansion in bindSelect (internal/binder/bind_stmt.go)

In `bindSelect()` at line 85-108, when expanding star columns, apply replacements:

```go
// Line 94-107: when iterating bound.Columns
for _, col := range bound.Columns {
    if bound.Replacements != nil {
        if replExpr, ok := bound.Replacements[strings.ToUpper(col.Column)]; ok {
            // Use replacement expression instead of column reference
            selectExprs = append(selectExprs, &BoundSelectColumn{
                Expr:  replExpr,
                Alias: col.Column, // keep original column name
            })
            continue
        }
    }
    // Normal column reference (existing logic)
    selectExprs = append(selectExprs, &BoundSelectColumn{
        Expr:  &BoundColumnRef{Column: col},
        Alias: col.Column,
    })
}
```

### COLUMNS expression binding

Add a new case in `bindExpr()` (bind_expr.go):

```go
case *parser.ColumnsExpr:
    return b.bindColumnsExpr(e)
```

New function:

```go
func (b *Binder) bindColumnsExpr(e *parser.ColumnsExpr) (BoundExpr, error) {
    re, err := regexp.Compile(e.Pattern)
    if err != nil {
        return nil, b.errorf("COLUMNS: invalid regex pattern: %v", err)
    }

    // Collect all columns from all tables in scope
    var allCols []*BoundColumn
    for _, tableRef := range b.scope.tables {
        allCols = append(allCols, tableRef.Columns...)
    }

    // Filter by regex
    var matched []*BoundColumn
    for _, col := range allCols {
        if re.MatchString(col.Column) {
            matched = append(matched, col)
        }
    }
    if len(matched) == 0 {
        return nil, b.errorf("COLUMNS('%s'): no columns match pattern", e.Pattern)
    }

    // Return as BoundStarExpr (reuse star expansion mechanism)
    return &BoundStarExpr{
        Columns: matched,
    }, nil
}
```

**COLUMNS in function calls:** When COLUMNS appears inside a function like `SELECT MIN(COLUMNS('*'))`, the binder should expand it similarly to how star expansion works — the function gets applied to each matching column, producing multiple output columns. This requires `bindSelect()` to detect BoundStarExpr results from function argument binding and expand accordingly.

## Import Dependencies

- `regexp` — for COLUMNS pattern matching (Go standard library)
- `strings` — for case-insensitive column comparison (already imported)

## Testing Strategy

1. **Parser tests:** Parse EXCLUDE, REPLACE, COLUMNS syntax correctly
2. **Binder tests:** Star expansion with modifiers produces correct column list
3. **Integration tests:** Full query execution via database/sql
4. **Error tests:** Invalid column names in EXCLUDE/REPLACE, invalid regex in COLUMNS
