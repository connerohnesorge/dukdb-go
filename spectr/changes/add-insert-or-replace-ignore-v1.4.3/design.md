# Design: INSERT OR REPLACE/IGNORE Syntax

## Architecture

Pure parser change — desugars `INSERT OR REPLACE/IGNORE` into the existing OnConflictClause mechanism. No executor changes needed since ON CONFLICT DO UPDATE/DO NOTHING already works.

## Parser Change (parser.go:1509-1515)

Current parseInsert() expects `INSERT INTO`. Need to intercept `OR` between INSERT and INTO.

### Current flow (lines 1510-1515):

```go
func (p *parser) parseInsert() (*InsertStmt, error) {
    if err := p.expectKeyword("INSERT"); err != nil {
        return nil, err
    }
    if err := p.expectKeyword("INTO"); err != nil {
        return nil, err
    }
    // ...
```

### New flow:

```go
func (p *parser) parseInsert() (*InsertStmt, error) {
    if err := p.expectKeyword("INSERT"); err != nil {
        return nil, err
    }

    // Check for INSERT OR REPLACE / INSERT OR IGNORE
    var orAction string
    if p.isKeyword("OR") {
        p.advance() // consume OR
        if p.isKeyword("REPLACE") {
            p.advance()
            orAction = "REPLACE"
        } else if p.isKeyword("IGNORE") {
            p.advance()
            orAction = "IGNORE"
        } else {
            return nil, p.errorf("expected REPLACE or IGNORE after INSERT OR")
        }
    }

    if err := p.expectKeyword("INTO"); err != nil {
        return nil, err
    }

    stmt := &InsertStmt{}
    // ... rest of parsing unchanged ...

    // At the end, before returning, apply OR action if no explicit ON CONFLICT:
    if orAction != "" && stmt.OnConflict == nil {
        switch orAction {
        case "REPLACE":
            // INSERT OR REPLACE → ON CONFLICT DO UPDATE SET all columns
            stmt.OnConflict = &OnConflictClause{
                Action: OnConflictDoUpdate,
                // UpdateSet is nil — executor should update all columns with new values
                // This is equivalent to DELETE + INSERT in SQLite semantics
            }
        case "IGNORE":
            // INSERT OR IGNORE → ON CONFLICT DO NOTHING
            stmt.OnConflict = &OnConflictClause{
                Action: OnConflictDoNothing,
            }
        }
    }

    return stmt, nil
}
```

### Semantics

**INSERT OR IGNORE** maps directly to `ON CONFLICT DO NOTHING` — skip conflicting rows.

**INSERT OR REPLACE** maps to `ON CONFLICT DO UPDATE SET <all columns>`. The existing ON CONFLICT DO UPDATE mechanism already handles this. When UpdateSet is empty/nil with DoUpdate action, the executor should interpret this as "replace all columns with the new values" (full row replacement).

If the executor doesn't support empty UpdateSet for DoUpdate, we need to either:
1. Set UpdateSet to all columns explicitly (requires knowing the column list at parse time, which we have from the INSERT column list), or
2. Add a flag like `ReplaceAll bool` to OnConflictClause

Option 1 is simpler:

```go
case "REPLACE":
    // Build SET clause for all columns
    updates := make([]OnConflictUpdate, len(stmt.Columns))
    for i, col := range stmt.Columns {
        updates[i] = OnConflictUpdate{
            Column: col,
            Expr:   &ColumnRef{Name: col, Table: "EXCLUDED"},
        }
    }
    stmt.OnConflict = &OnConflictClause{
        Action:    OnConflictDoUpdate,
        UpdateSet: updates,
    }
```

Note: Must check the OnConflictClause and OnConflictUpdate structures to verify field names.

### Verification of OnConflictClause structure

```go
// From ast.go:278:
type OnConflictClause struct {
    Columns         []string           // conflict target columns
    Action          OnConflictAction   // DO NOTHING or DO UPDATE
    Updates         []OnConflictUpdate // SET clauses for DO UPDATE
    Where           Expr               // optional WHERE for DO UPDATE
}
```

Need to verify exact field names by reading ast.go:278-283.

## Helper Signatures Reference (Verified)

- parseInsert() — parser.go:1509 — INSERT statement parser entry
- INSERT keyword — parser.go:1510 — `p.expectKeyword("INSERT")`
- INTO keyword — parser.go:1513 — `p.expectKeyword("INTO")`
- InsertStmt — ast.go:294-307 — INSERT AST node
- OnConflict field — ast.go:302 — existing ON CONFLICT clause pointer
- OnConflictClause — ast.go:278 — conflict resolution clause
- OnConflictDoNothing — ast.go:272 — skip conflicting rows
- OnConflictDoUpdate — ast.go:274 — update existing rows
- `p.isKeyword()` — parser helper for keyword checking
- Error pattern: `p.errorf("expected ...")`

## Testing Strategy

1. `INSERT OR IGNORE INTO t VALUES (1, 'a')` with conflict → row skipped
2. `INSERT OR IGNORE INTO t VALUES (1, 'a')` without conflict → row inserted
3. `INSERT OR REPLACE INTO t VALUES (1, 'new')` with conflict → row replaced
4. `INSERT OR REPLACE INTO t VALUES (1, 'a')` without conflict → row inserted
5. `INSERT OR REPLACE INTO t(id, name) VALUES (1, 'new')` → verifies column mapping
6. Verify existing `INSERT INTO ... ON CONFLICT DO NOTHING` still works
7. Verify existing `INSERT INTO ... ON CONFLICT DO UPDATE` still works
