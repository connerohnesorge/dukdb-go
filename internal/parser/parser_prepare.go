package parser

import "strings"

// parsePrepareStmt parses: PREPARE name AS statement
func (p *parser) parsePrepareStmt() (Statement, error) {
	// Consume PREPARE keyword
	if err := p.expectKeyword("PREPARE"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected prepared statement name after PREPARE")
	}
	name := p.advance().value

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	// Parse the inner statement using the same dispatch logic
	inner, err := p.parseInnerStatement()
	if err != nil {
		return nil, err
	}

	return &PrepareStmt{Name: name, Inner: inner}, nil
}

// parseExecuteStmt parses: EXECUTE name or EXECUTE name(expr_list)
func (p *parser) parseExecuteStmt() (Statement, error) {
	// Consume EXECUTE keyword
	if err := p.expectKeyword("EXECUTE"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected prepared statement name after EXECUTE")
	}
	name := p.advance().value

	var params []Expr
	if p.current().typ == tokenLParen {
		p.advance() // consume (
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			params = append(params, expr)
			if p.current().typ != tokenComma {
				break
			}
			p.advance() // consume ,
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	return &ExecuteStmt{Name: name, Params: params}, nil
}

// parseDeallocateStmt parses: DEALLOCATE [PREPARE] name | DEALLOCATE ALL
func (p *parser) parseDeallocateStmt() (Statement, error) {
	// Consume DEALLOCATE keyword
	if err := p.expectKeyword("DEALLOCATE"); err != nil {
		return nil, err
	}

	// Check for optional PREPARE keyword
	if p.current().typ == tokenIdent && strings.EqualFold(p.current().value, "PREPARE") {
		p.advance() // consume PREPARE
	}

	// Check for ALL
	if p.current().typ == tokenIdent && strings.EqualFold(p.current().value, "ALL") {
		p.advance()
		return &DeallocateStmt{All: true}, nil
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected prepared statement name or ALL after DEALLOCATE")
	}
	name := p.advance().value

	return &DeallocateStmt{Name: name}, nil
}

// parseInnerStatement parses a statement without consuming EOF or trailing semicolons.
// This is used by PREPARE to parse the inner statement.
func (p *parser) parseInnerStatement() (Statement, error) {
	switch {
	case p.isKeyword("WITH"):
		return p.parseWithSelect()
	case p.isKeyword("SELECT"):
		return p.parseSelect()
	case p.isKeyword("INSERT"):
		return p.parseInsert()
	case p.isKeyword("UPDATE"):
		return p.parseUpdate()
	case p.isKeyword("DELETE"):
		return p.parseDelete()
	case p.isKeyword("CREATE"):
		return p.parseCreate()
	case p.isKeyword("DROP"):
		return p.parseDrop()
	case p.isKeyword("ALTER"):
		p.advance()
		return p.parseAlter()
	default:
		return nil, p.errorf("expected a SQL statement after AS")
	}
}
