package parser

// parsePragma parses a PRAGMA statement.
// Supports:
//   - PRAGMA pragma_name
//   - PRAGMA pragma_name(arg1, arg2, ...)
//   - PRAGMA pragma_name = value
func (p *parser) parsePragma() (*PragmaStmt, error) {
	if err := p.expectKeyword("PRAGMA"); err != nil {
		return nil, err
	}

	stmt := &PragmaStmt{}

	// Parse pragma name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected pragma name")
	}
	stmt.Name = p.advance().value

	// Check for optional arguments or assignment
	switch p.current().typ {
	case tokenLParen:
		// PRAGMA name(args...)
		p.advance()
		if p.current().typ != tokenRParen {
			args, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			stmt.Args = args
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	case tokenOperator:
		// PRAGMA name = value
		if p.current().value == "=" {
			p.advance()
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Value = val
		}
	}

	return stmt, nil
}

// parseExplain parses an EXPLAIN or EXPLAIN ANALYZE statement.
func (p *parser) parseExplain() (*ExplainStmt, error) {
	if err := p.expectKeyword("EXPLAIN"); err != nil {
		return nil, err
	}

	stmt := &ExplainStmt{}

	// Check for ANALYZE keyword
	if p.isKeyword("ANALYZE") {
		p.advance()
		stmt.Analyze = true
	}

	// Parse the underlying query
	var query Statement
	var err error

	switch {
	case p.isKeyword("SELECT"):
		query, err = p.parseSelect()
	case p.isKeyword("WITH"):
		query, err = p.parseWithSelect()
	case p.isKeyword("INSERT"):
		query, err = p.parseInsert()
	case p.isKeyword("UPDATE"):
		query, err = p.parseUpdate()
	case p.isKeyword("DELETE"):
		query, err = p.parseDelete()
	default:
		return nil, p.errorf("expected SELECT, INSERT, UPDATE, or DELETE after EXPLAIN")
	}

	if err != nil {
		return nil, err
	}
	stmt.Query = query

	return stmt, nil
}

// parseVacuum parses a VACUUM statement.
func (p *parser) parseVacuum() (*VacuumStmt, error) {
	if err := p.expectKeyword("VACUUM"); err != nil {
		return nil, err
	}

	stmt := &VacuumStmt{}

	// Optional table name
	if p.current().typ == tokenIdent {
		name := p.advance().value

		// Check for schema.table
		if p.current().typ == tokenDot {
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected table name after schema")
			}
			stmt.Schema = name
			stmt.TableName = p.advance().value
		} else {
			stmt.TableName = name
		}
	}

	return stmt, nil
}

// parseAnalyze parses an ANALYZE statement.
func (p *parser) parseAnalyze() (*AnalyzeStmt, error) {
	if err := p.expectKeyword("ANALYZE"); err != nil {
		return nil, err
	}

	stmt := &AnalyzeStmt{}

	// Optional table name
	if p.current().typ == tokenIdent {
		name := p.advance().value

		// Check for schema.table
		if p.current().typ == tokenDot {
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected table name after schema")
			}
			stmt.Schema = name
			stmt.TableName = p.advance().value
		} else {
			stmt.TableName = name
		}
	}

	return stmt, nil
}

// parseCheckpoint parses a CHECKPOINT statement.
func (p *parser) parseCheckpoint() (*CheckpointStmt, error) {
	if err := p.expectKeyword("CHECKPOINT"); err != nil {
		return nil, err
	}

	stmt := &CheckpointStmt{}

	// Check for optional database name or FORCE flag
	if p.current().typ == tokenIdent {
		name := p.current().value
		if name == "FORCE" || name == "force" {
			p.advance()
			stmt.Force = true
		} else {
			stmt.Database = p.advance().value

			// Check for FORCE after database name
			if p.current().typ == tokenIdent {
				if p.current().value == "FORCE" || p.current().value == "force" {
					p.advance()
					stmt.Force = true
				}
			}
		}
	}

	return stmt, nil
}
