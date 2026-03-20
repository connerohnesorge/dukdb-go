package parser

import "strings"

// parseCreateMacro parses a CREATE MACRO statement.
// Syntax:
//
//	CREATE [OR REPLACE] MACRO [schema.]name(params) AS expression
//	CREATE [OR REPLACE] MACRO [schema.]name(params) AS TABLE select_statement
//
// Parameters can have optional defaults using := or DEFAULT syntax.
func (p *parser) parseCreateMacro(orReplace bool) (Statement, error) {
	p.advance() // consume MACRO

	// Parse macro name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected macro name")
	}

	name := p.advance().value
	schema := ""

	// Check for schema.name syntax
	if p.current().typ == tokenDot {
		p.advance()

		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected macro name after schema")
		}

		schema = name
		name = p.advance().value
	}

	// Parse parameter list
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	var params []MacroParam

	if p.current().typ != tokenRParen {
		for {
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected parameter name")
			}

			param := MacroParam{Name: p.advance().value}

			// Check for default value (:= or DEFAULT)
			if p.current().typ == tokenOperator && p.current().value == ":=" {
				p.advance() // consume :=

				startPos := p.tokPos

				defExpr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}

				param.Default = defExpr
				param.DefaultSQL = p.extractTokenRange(startPos, p.tokPos)
			} else if p.isKeyword("DEFAULT") {
				p.advance()

				startPos := p.tokPos

				defExpr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}

				param.Default = defExpr
				param.DefaultSQL = p.extractTokenRange(startPos, p.tokPos)
			}

			params = append(params, param)

			if p.current().typ != tokenComma {
				break
			}

			p.advance() // consume comma
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Expect AS
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	// Check for TABLE macro
	if p.isKeyword("TABLE") {
		p.advance()

		startPos := p.tokPos

		query, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		querySQL := p.extractTokenRange(startPos, p.tokPos)

		return &CreateMacroStmt{
			Schema:       schema,
			Name:         name,
			Params:       params,
			IsTableMacro: true,
			OrReplace:    orReplace,
			Query:        query,
			QuerySQL:     querySQL,
		}, nil
	}

	// Scalar macro: parse expression body
	startPos := p.tokPos

	body, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	bodySQL := p.extractTokenRange(startPos, p.tokPos)

	return &CreateMacroStmt{
		Schema:    schema,
		Name:      name,
		Params:    params,
		OrReplace: orReplace,
		Body:      body,
		BodySQL:   bodySQL,
	}, nil
}

// parseDropMacro parses a DROP MACRO statement.
// Syntax:
//
//	DROP MACRO [IF EXISTS] [schema.]name
//	DROP MACRO TABLE [IF EXISTS] [schema.]name
func (p *parser) parseDropMacro() (Statement, error) {
	p.advance() // consume MACRO

	isTableMacro := false

	if p.isKeyword("TABLE") {
		p.advance()

		isTableMacro = true
	}

	ifExists := false

	if p.isKeyword("IF") {
		p.advance()

		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}

		ifExists = true
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected macro name")
	}

	name := p.advance().value
	schema := ""

	if p.current().typ == tokenDot {
		p.advance()

		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected macro name after schema")
		}

		schema = name
		name = p.advance().value
	}

	return &DropMacroStmt{
		Schema:       schema,
		Name:         name,
		IfExists:     ifExists,
		IsTableMacro: isTableMacro,
	}, nil
}

// extractTokenRange reconstructs SQL from a range of tokens.
func (p *parser) extractTokenRange(start, end int) string {
	var parts []string
	for i := start; i < end && i < len(p.tokens); i++ {
		parts = append(parts, p.tokens[i].value)
	}

	return strings.Join(parts, " ")
}
