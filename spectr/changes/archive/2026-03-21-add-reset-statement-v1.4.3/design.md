# Design: RESET Statement

## Architecture

RESET follows the same pattern as SET (parser_pragma.go:187). It's a configuration statement handled at the connection level.

## 1. AST (ast.go)

Add after SetStmt or ShowStmt:

```go
type ResetStmt struct {
    Variable string  // variable name, or "" for RESET ALL
    All      bool    // true for RESET ALL
}

func (*ResetStmt) stmtNode() {}
func (*ResetStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SET }
```

## 2. Parser (parser.go + parser_pragma.go)

Add to main dispatch at parser.go:47-129 (before `default:`):

```go
case p.isKeyword("RESET"):
    stmt, err = p.parseReset()
```

Implementation in parser_pragma.go (add after parseSet at line 187):

```go
func (p *parser) parseReset() (*ResetStmt, error) {
    p.advance() // consume RESET
    stmt := &ResetStmt{}

    // RESET ALL
    if p.isKeyword("ALL") {
        p.advance()
        stmt.All = true
        return stmt, nil
    }

    // RESET variable
    if p.current().typ != tokenIdent {  // tokenIdent at parser_tokens.go:8
        return nil, p.errorf("expected variable name after RESET")
    }
    stmt.Variable = p.advance().value
    return stmt, nil
}
```

## 3. Executor (engine/conn.go)

Handle ResetStmt alongside the existing SET/SHOW handling. Reset the variable to its default value:

```go
case *parser.ResetStmt:
    if resetStmt.All {
        // Reset all variables to defaults
        c.resetAllSettings()
    } else {
        // Reset specific variable to default
        if err := c.resetSetting(resetStmt.Variable); err != nil {
            return nil, nil, &dukdb.Error{
                Type: dukdb.ErrorTypeExecutor,
                Msg:  fmt.Sprintf("unknown variable: %s", resetStmt.Variable),
            }
        }
    }
    return nil, nil, nil
```

Default values for known variables:
- `transaction_isolation` → "serializable"
- `default_transaction_isolation` → "serializable"
- `search_path` → "main"
- Other variables → remove from settings map (revert to hardcoded defaults)

## Helper Signatures Reference (Verified)

- `parseSet()` — parser_pragma.go:187 — SET pattern to follow
- `parseShow()` — parser_pragma.go:278 — SHOW pattern reference
- Main parser dispatch — parser.go:47-135 — keyword switch
- tokenIdent — parser_tokens.go:8 — identifier token type
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SET x = 'value'; RESET x; SHOW x` → should show default value
2. `RESET transaction_isolation` → should reset to "serializable"
3. `RESET ALL` → should reset all settings
4. `RESET nonexistent_var` → should error gracefully
