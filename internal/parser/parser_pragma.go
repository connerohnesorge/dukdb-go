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

// parseSet parses a SET statement.
// Supports:
//   - SET variable = value
//   - SET variable = 'value'
//   - SET default_transaction_isolation = 'READ UNCOMMITTED'
//   - SET default_transaction_isolation = 'READ COMMITTED'
//   - SET default_transaction_isolation = 'REPEATABLE READ'
//   - SET default_transaction_isolation = 'SERIALIZABLE'
//   - SET default_transaction_isolation TO 'level'
func (p *parser) parseSet() (*SetStmt, error) {
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	stmt := &SetStmt{}

	// Parse variable name (may contain underscores, e.g., default_transaction_isolation)
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected variable name after SET")
	}
	stmt.Variable = p.advance().value

	// Check for = or TO
	if p.current().typ == tokenOperator && p.current().value == "=" {
		p.advance()
	} else if p.isKeyword("TO") {
		p.advance()
	} else {
		return nil, p.errorf("expected '=' or 'TO' after variable name")
	}

	// Parse value - can be a string literal or identifier(s)
	// For isolation levels, the value may be multiple tokens like "READ UNCOMMITTED"
	value, err := p.parseSetValue()
	if err != nil {
		return nil, err
	}
	stmt.Value = value

	return stmt, nil
}

// parseSetValue parses the value part of a SET statement.
// This handles:
//   - String literals: 'value'
//   - Numeric literals: 50000, -1
//   - Identifiers: value
//   - Multi-word identifiers: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ
func (p *parser) parseSetValue() (string, error) {
	// Check for string literal
	if p.current().typ == tokenString {
		val := p.advance().value
		// Remove quotes
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			return val[1 : len(val)-1], nil
		}
		return val, nil
	}

	// Check for numeric literal (integer or decimal)
	if p.current().typ == tokenNumber {
		return p.advance().value, nil
	}

	// Parse identifier(s) - may be multi-word for isolation levels
	if p.current().typ != tokenIdent {
		return "", p.errorf("expected value after SET")
	}

	// Collect identifier tokens until we hit something that's not an identifier
	// This handles "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"
	var parts []string
	for p.current().typ == tokenIdent {
		parts = append(parts, p.advance().value)
		// Stop if we hit end of statement indicators
		if p.current().typ == tokenSemicolon || p.current().typ == tokenEOF {
			break
		}
	}

	// Join with spaces and return
	result := ""
	for i, part := range parts {
		if i > 0 {
			result += " "
		}
		result += part
	}

	return result, nil
}

// parseShow parses a SHOW statement.
// Supports:
//   - SHOW variable
//   - SHOW transaction_isolation
//   - SHOW default_transaction_isolation
//   - SHOW TABLES
//   - SHOW ALL TABLES
//   - SHOW COLUMNS FROM table
func (p *parser) parseShow() (*ShowStmt, error) {
	if err := p.expectKeyword("SHOW"); err != nil {
		return nil, err
	}

	stmt := &ShowStmt{}

	// SHOW ALL TABLES or SHOW ALL (settings)
	if p.isKeyword("ALL") {
		p.advance()
		if p.isKeyword("TABLES") {
			p.advance()
			stmt.Variable = "__all_tables"
			return stmt, nil
		}
		stmt.Variable = "__all_settings"
		return stmt, nil
	}

	// SHOW TABLES
	if p.isKeyword("TABLES") {
		p.advance()
		stmt.Variable = "__tables"
		return stmt, nil
	}

	// SHOW COLUMNS FROM table
	if p.isKeyword("COLUMNS") {
		p.advance()
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
		tableName, err := p.expect(tokenIdent)
		if err != nil {
			return nil, p.errorf("expected table name after SHOW COLUMNS FROM")
		}
		stmt.Variable = "__columns"
		stmt.TableName = tableName.value
		return stmt, nil
	}

	// SHOW variable (existing behavior)
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected variable name after SHOW")
	}
	stmt.Variable = p.advance().value

	return stmt, nil
}

// parseDescribe parses a DESCRIBE or DESC statement.
// Supports:
//   - DESCRIBE tablename
//   - DESCRIBE schema.tablename
//   - DESCRIBE SELECT ...
func (p *parser) parseDescribe() (*DescribeStmt, error) {
	p.advance() // consume DESCRIBE/DESC
	stmt := &DescribeStmt{}

	// DESCRIBE SELECT ... or DESCRIBE WITH ...
	if p.isKeyword("SELECT") || p.isKeyword("WITH") {
		var query Statement
		var err error
		if p.isKeyword("SELECT") {
			query, err = p.parseSelect()
		} else {
			query, err = p.parseWithSelect()
		}
		if err != nil {
			return nil, err
		}
		stmt.Query = query
		return stmt, nil
	}

	// DESCRIBE [schema.]table
	name, err := p.expect(tokenIdent)
	if err != nil {
		return nil, p.errorf("expected table name or SELECT after DESCRIBE")
	}
	stmt.TableName = name.value

	if p.current().typ == tokenDot {
		p.advance()
		tableName, err := p.expect(tokenIdent)
		if err != nil {
			return nil, err
		}
		stmt.Schema = stmt.TableName
		stmt.TableName = tableName.value
	}

	return stmt, nil
}

// parseSummarize parses a SUMMARIZE statement.
// Supports:
//   - SUMMARIZE tablename
//   - SUMMARIZE schema.tablename
//   - SUMMARIZE SELECT ...
//   - SUMMARIZE WITH ...
func (p *parser) parseSummarize() (*SummarizeStmt, error) {
	p.advance() // consume SUMMARIZE
	stmt := &SummarizeStmt{}

	// SUMMARIZE SELECT ... or SUMMARIZE WITH ...
	if p.isKeyword("SELECT") || p.isKeyword("WITH") {
		var query Statement
		var err error
		if p.isKeyword("SELECT") {
			query, err = p.parseSelect()
		} else {
			query, err = p.parseWithSelect()
		}
		if err != nil {
			return nil, err
		}
		stmt.Query = query
		return stmt, nil
	}

	// SUMMARIZE [schema.]table
	name, err := p.expect(tokenIdent)
	if err != nil {
		return nil, p.errorf("expected table name or SELECT after SUMMARIZE")
	}
	stmt.TableName = name.value

	if p.current().typ == tokenDot {
		p.advance()
		tableName, err := p.expect(tokenIdent)
		if err != nil {
			return nil, err
		}
		stmt.Schema = stmt.TableName
		stmt.TableName = tableName.value
	}

	return stmt, nil
}

// parseCall parses a CALL function(args...) statement.
func (p *parser) parseCall() (*CallStmt, error) {
	p.advance() // consume CALL
	stmt := &CallStmt{}

	// Function name
	name, err := p.expect(tokenIdent)
	if err != nil {
		return nil, p.errorf("expected function name after CALL")
	}
	stmt.FunctionName = name.value

	// Arguments in parentheses
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, p.errorf("expected '(' after function name")
	}

	// Parse arguments
	if p.current().typ != tokenRParen {
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Args = append(stmt.Args, expr)
			if p.current().typ != tokenComma {
				break
			}
			p.advance() // consume comma
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, p.errorf("expected ')' after function arguments")
	}

	return stmt, nil
}
