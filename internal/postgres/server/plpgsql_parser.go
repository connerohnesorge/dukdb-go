// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// This file implements the PL/pgSQL parser for parsing function and procedure bodies.
// It parses the block structure including DECLARE, BEGIN, EXCEPTION, and END blocks,
// as well as control flow statements and variable assignments.

package server

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// PLpgSQLParser parses PL/pgSQL function bodies.
type PLpgSQLParser struct {
	input   string
	pos     int
	line    int
	col     int
	lastErr error
}

// NewPLpgSQLParser creates a new PL/pgSQL parser.
func NewPLpgSQLParser(input string) *PLpgSQLParser {
	return &PLpgSQLParser{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Parse parses a PL/pgSQL block and returns the AST.
func (p *PLpgSQLParser) Parse() (*PLpgSQLBlockStmt, error) {
	p.skipWhitespaceAndComments()
	block, err := p.parseBlock("")
	if err != nil {
		return nil, err
	}
	return block, nil
}

// Error returns the last parse error.
func (p *PLpgSQLParser) Error() error {
	return p.lastErr
}

// remaining returns the remaining unparsed input.
func (p *PLpgSQLParser) remaining() string {
	if p.pos >= len(p.input) {
		return ""
	}
	return p.input[p.pos:]
}

// peek returns the next character without consuming it.
func (p *PLpgSQLParser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

// advance moves the position forward by one character.
func (p *PLpgSQLParser) advance() {
	if p.pos < len(p.input) {
		if p.input[p.pos] == '\n' {
			p.line++
			p.col = 1
		} else {
			p.col++
		}
		p.pos++
	}
}

// skipWhitespaceAndComments skips whitespace and SQL comments.
func (p *PLpgSQLParser) skipWhitespaceAndComments() {
	for p.pos < len(p.input) {
		c := p.peek()
		if unicode.IsSpace(rune(c)) {
			p.advance()
			continue
		}
		// Skip -- line comments
		if c == '-' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '-' {
			for p.pos < len(p.input) && p.peek() != '\n' {
				p.advance()
			}
			continue
		}
		// Skip /* block comments */
		if c == '/' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '*' {
			p.advance() // /
			p.advance() // *
			for p.pos+1 < len(p.input) {
				if p.peek() == '*' && p.input[p.pos+1] == '/' {
					p.advance() // *
					p.advance() // /
					break
				}
				p.advance()
			}
			continue
		}
		break
	}
}

// matchKeyword checks if the remaining input starts with a keyword (case-insensitive).
func (p *PLpgSQLParser) matchKeyword(keyword string) bool {
	remaining := p.remaining()
	if len(remaining) < len(keyword) {
		return false
	}
	if !strings.EqualFold(remaining[:len(keyword)], keyword) {
		return false
	}
	// Ensure it's a complete keyword (followed by non-alpha)
	if len(remaining) > len(keyword) {
		next := remaining[len(keyword)]
		if unicode.IsLetter(rune(next)) || next == '_' || unicode.IsDigit(rune(next)) {
			return false
		}
	}
	return true
}

// consumeKeyword consumes a keyword if it matches.
func (p *PLpgSQLParser) consumeKeyword(keyword string) bool {
	if p.matchKeyword(keyword) {
		for i := 0; i < len(keyword); i++ {
			p.advance()
		}
		return true
	}
	return false
}

// parseIdentifier parses an identifier (variable name, type name, etc.).
func (p *PLpgSQLParser) parseIdentifier() (string, error) {
	p.skipWhitespaceAndComments()

	start := p.pos
	// Handle quoted identifiers
	if p.peek() == '"' {
		p.advance()
		for p.pos < len(p.input) && p.peek() != '"' {
			if p.peek() == '"' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '"' {
				p.advance()
			}
			p.advance()
		}
		if p.peek() != '"' {
			return "", errors.New("unterminated quoted identifier")
		}
		p.advance()
		return p.input[start:p.pos], nil
	}

	// Regular identifier
	if !unicode.IsLetter(rune(p.peek())) && p.peek() != '_' {
		return "", errors.New("expected identifier")
	}

	for p.pos < len(p.input) {
		c := p.peek()
		if unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c)) || c == '_' || c == '$' {
			p.advance()
		} else {
			break
		}
	}

	return p.input[start:p.pos], nil
}

// parseDataType parses a data type (e.g., "INTEGER", "VARCHAR(100)", "users.id%TYPE").
func (p *PLpgSQLParser) parseDataType() (string, error) {
	p.skipWhitespaceAndComments()

	start := p.pos
	// Parse type name(s) - could be "schema.type" or just "type"
	for {
		// Parse identifier part
		if !unicode.IsLetter(rune(p.peek())) && p.peek() != '_' && p.peek() != '"' {
			break
		}
		_, err := p.parseIdentifier()
		if err != nil {
			break
		}
		p.skipWhitespaceAndComments()

		// Check for "." separator or %TYPE/%ROWTYPE
		if p.peek() == '.' {
			p.advance()
			p.skipWhitespaceAndComments()
		} else if p.peek() == '%' {
			p.advance()
			if p.matchKeyword("TYPE") || p.matchKeyword("ROWTYPE") {
				if p.matchKeyword("ROWTYPE") {
					p.consumeKeyword("ROWTYPE")
				} else {
					p.consumeKeyword("TYPE")
				}
			}
			break
		} else if p.peek() == '[' {
			// Array type
			for p.peek() == '[' {
				p.advance()
				p.skipWhitespaceAndComments()
				if p.peek() == ']' {
					p.advance()
				}
			}
			break
		} else if p.peek() == '(' {
			// Type with parameters like VARCHAR(100) or NUMERIC(10,2)
			p.advance()
			parenDepth := 1
			for p.pos < len(p.input) && parenDepth > 0 {
				if p.peek() == '(' {
					parenDepth++
				} else if p.peek() == ')' {
					parenDepth--
				}
				p.advance()
			}
			break
		} else {
			break
		}
	}

	if p.pos == start {
		return "", errors.New("expected data type")
	}

	return strings.TrimSpace(p.input[start:p.pos]), nil
}

// parseExpression parses an expression until a delimiter.
func (p *PLpgSQLParser) parseExpression(delimiters ...string) (*PLpgSQLExpr, error) {
	p.skipWhitespaceAndComments()

	start := p.pos
	parenDepth := 0
	inString := false
	stringChar := byte(0)

	for p.pos < len(p.input) {
		c := p.peek()

		// Handle strings
		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			p.advance()
			continue
		}
		if inString {
			if c == stringChar {
				// Check for escape
				if p.pos+1 < len(p.input) && p.input[p.pos+1] == stringChar {
					p.advance()
				} else {
					inString = false
				}
			}
			p.advance()
			continue
		}

		// Handle parentheses
		if c == '(' {
			parenDepth++
			p.advance()
			continue
		}
		if c == ')' {
			if parenDepth > 0 {
				parenDepth--
				p.advance()
				continue
			}
		}

		// Check for delimiters (only at depth 0)
		if parenDepth == 0 {
			for _, delim := range delimiters {
				// Handle special symbol delimiters like ".."
				if delim == ".." && c == '.' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '.' {
					raw := strings.TrimSpace(p.input[start:p.pos])
					return &PLpgSQLExpr{Raw: raw}, nil
				}
				if p.matchKeyword(delim) || (delim == ";" && c == ';') {
					raw := strings.TrimSpace(p.input[start:p.pos])
					return &PLpgSQLExpr{Raw: raw}, nil
				}
			}
		}

		// Check for semicolon
		if c == ';' && parenDepth == 0 {
			raw := strings.TrimSpace(p.input[start:p.pos])
			return &PLpgSQLExpr{Raw: raw}, nil
		}

		p.advance()
	}

	raw := strings.TrimSpace(p.input[start:p.pos])
	if raw == "" {
		return nil, errors.New("expected expression")
	}
	return &PLpgSQLExpr{Raw: raw}, nil
}

// parseBlock parses a PL/pgSQL block.
func (p *PLpgSQLParser) parseBlock(label string) (*PLpgSQLBlockStmt, error) {
	block := &PLpgSQLBlockStmt{Label: label}

	p.skipWhitespaceAndComments()

	// Check for DECLARE section
	if p.matchKeyword("DECLARE") {
		p.consumeKeyword("DECLARE")
		declare, err := p.parseDeclareSection()
		if err != nil {
			return nil, err
		}
		block.Declare = declare
	}

	p.skipWhitespaceAndComments()

	// Expect BEGIN
	if !p.consumeKeyword("BEGIN") {
		return nil, errors.New("expected BEGIN")
	}

	// Parse statements until END or EXCEPTION
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("END") {
			break
		}

		if p.matchKeyword("EXCEPTION") {
			p.consumeKeyword("EXCEPTION")
			excBlock, err := p.parseExceptionSection()
			if err != nil {
				return nil, err
			}
			block.Exception = excBlock
			p.skipWhitespaceAndComments()
			break
		}

		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if stmt != nil {
			block.Body = append(block.Body, stmt)
		}
	}

	// Consume END and optional label
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END")
	}

	p.skipWhitespaceAndComments()

	// Optional label after END
	if label != "" && p.matchKeyword(label) {
		p.consumeKeyword(label)
	}

	// Consume optional semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return block, nil
}

// parseDeclareSection parses the DECLARE section.
func (p *PLpgSQLParser) parseDeclareSection() (*PLpgSQLDeclareBlock, error) {
	declare := &PLpgSQLDeclareBlock{}

	for {
		p.skipWhitespaceAndComments()

		// Check for BEGIN (end of DECLARE section)
		if p.matchKeyword("BEGIN") {
			break
		}

		// Parse variable declaration
		decl, err := p.parseVariableDeclaration()
		if err != nil {
			return nil, err
		}
		if decl != nil {
			declare.Declarations = append(declare.Declarations, decl)
		}
	}

	return declare, nil
}

// parseVariableDeclaration parses a single variable declaration.
func (p *PLpgSQLParser) parseVariableDeclaration() (*PLpgSQLVarDecl, error) {
	decl := &PLpgSQLVarDecl{}

	// Parse variable name
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	decl.Name = name

	p.skipWhitespaceAndComments()

	// Check for CONSTANT
	if p.matchKeyword("CONSTANT") {
		p.consumeKeyword("CONSTANT")
		decl.Constant = true
		p.skipWhitespaceAndComments()
	}

	// Check for CURSOR
	if p.matchKeyword("CURSOR") {
		p.consumeKeyword("CURSOR")
		p.skipWhitespaceAndComments()
		if p.matchKeyword("FOR") {
			p.consumeKeyword("FOR")
			decl.CursorFor = true
			// Parse cursor query until semicolon
			start := p.pos
			for p.pos < len(p.input) && p.peek() != ';' {
				p.advance()
			}
			decl.CursorQuery = strings.TrimSpace(p.input[start:p.pos])
			if p.peek() == ';' {
				p.advance()
			}
			return decl, nil
		}
	}

	// Parse data type
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	decl.DataType = dataType

	// Check for %ROWTYPE
	if strings.HasSuffix(strings.ToUpper(dataType), "%ROWTYPE") {
		decl.RowType = true
		decl.RowTypeTable = strings.TrimSuffix(strings.TrimSuffix(dataType, "%ROWTYPE"), "%rowtype")
	}

	p.skipWhitespaceAndComments()

	// Check for COLLATE
	if p.matchKeyword("COLLATE") {
		p.consumeKeyword("COLLATE")
		p.skipWhitespaceAndComments()
		collate, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		decl.Collate = collate
		p.skipWhitespaceAndComments()
	}

	// Check for NOT NULL
	if p.matchKeyword("NOT") {
		p.consumeKeyword("NOT")
		p.skipWhitespaceAndComments()
		if p.matchKeyword("NULL") {
			p.consumeKeyword("NULL")
			decl.NotNull = true
		}
		p.skipWhitespaceAndComments()
	}

	// Check for DEFAULT or :=
	if p.matchKeyword("DEFAULT") ||
		(p.peek() == ':' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '=') {
		if p.matchKeyword("DEFAULT") {
			p.consumeKeyword("DEFAULT")
		} else {
			p.advance() // :
			p.advance() // =
		}
		p.skipWhitespaceAndComments()

		defaultExpr, err := p.parseExpression(";")
		if err != nil {
			return nil, err
		}
		decl.Default = defaultExpr
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return decl, nil
}

// parseExceptionSection parses the EXCEPTION section.
func (p *PLpgSQLParser) parseExceptionSection() (*PLpgSQLExceptionBlock, error) {
	excBlock := &PLpgSQLExceptionBlock{}

	for {
		p.skipWhitespaceAndComments()

		// Check for END (end of EXCEPTION section)
		if p.matchKeyword("END") {
			break
		}

		// Expect WHEN
		if !p.matchKeyword("WHEN") {
			return nil, errors.New("expected WHEN in EXCEPTION block")
		}
		p.consumeKeyword("WHEN")

		handler := &PLpgSQLExcHandler{}

		// Parse conditions (can be multiple separated by OR)
		for {
			p.skipWhitespaceAndComments()

			// Parse condition
			condition, err := p.parseExceptionCondition()
			if err != nil {
				return nil, err
			}
			handler.Conditions = append(handler.Conditions, condition)

			p.skipWhitespaceAndComments()

			// Check for OR
			if p.matchKeyword("OR") {
				p.consumeKeyword("OR")
				continue
			}
			break
		}

		p.skipWhitespaceAndComments()

		// Expect THEN
		if !p.consumeKeyword("THEN") {
			return nil, errors.New("expected THEN after WHEN conditions")
		}

		// Parse handler body until WHEN or END
		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("WHEN") || p.matchKeyword("END") {
				break
			}

			stmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if stmt != nil {
				handler.Statements = append(handler.Statements, stmt)
			}
		}

		excBlock.Handlers = append(excBlock.Handlers, handler)
	}

	return excBlock, nil
}

// parseExceptionCondition parses an exception condition.
func (p *PLpgSQLParser) parseExceptionCondition() (string, error) {
	p.skipWhitespaceAndComments()

	// Check for SQLSTATE 'code'
	if p.matchKeyword("SQLSTATE") {
		p.consumeKeyword("SQLSTATE")
		p.skipWhitespaceAndComments()
		// Parse the SQLSTATE code
		if p.peek() == '\'' {
			p.advance()
			start := p.pos
			for p.pos < len(p.input) && p.peek() != '\'' {
				p.advance()
			}
			code := p.input[start:p.pos]
			if p.peek() == '\'' {
				p.advance()
			}
			return "SQLSTATE '" + code + "'", nil
		}
	}

	// Parse condition name (e.g., division_by_zero, OTHERS)
	name, err := p.parseIdentifier()
	if err != nil {
		return "", err
	}

	return name, nil
}

// parseStatement parses a single PL/pgSQL statement.
func (p *PLpgSQLParser) parseStatement() (PLpgSQLStmt, error) {
	p.skipWhitespaceAndComments()

	// Check for NULL statement
	if p.matchKeyword("NULL") {
		p.consumeKeyword("NULL")
		p.skipWhitespaceAndComments()
		if p.peek() == ';' {
			p.advance()
		}
		return &PLpgSQLNullStmt{}, nil
	}

	// Check for RETURN
	if p.matchKeyword("RETURN") {
		return p.parseReturnStatement()
	}

	// Check for RAISE
	if p.matchKeyword("RAISE") {
		return p.parseRaiseStatement()
	}

	// Check for IF
	if p.matchKeyword("IF") {
		return p.parseIfStatement()
	}

	// Check for CASE
	if p.matchKeyword("CASE") {
		return p.parseCaseStatement()
	}

	// Check for LOOP
	if p.matchKeyword("LOOP") {
		return p.parseLoopStatement("")
	}

	// Check for WHILE
	if p.matchKeyword("WHILE") {
		return p.parseWhileStatement("")
	}

	// Check for FOR
	if p.matchKeyword("FOR") {
		return p.parseForStatement("")
	}

	// Check for FOREACH
	if p.matchKeyword("FOREACH") {
		return p.parseForeachStatement("")
	}

	// Check for EXIT
	if p.matchKeyword("EXIT") {
		return p.parseExitStatement()
	}

	// Check for CONTINUE
	if p.matchKeyword("CONTINUE") {
		return p.parseContinueStatement()
	}

	// Check for PERFORM
	if p.matchKeyword("PERFORM") {
		return p.parsePerformStatement()
	}

	// Check for EXECUTE
	if p.matchKeyword("EXECUTE") {
		return p.parseExecuteStatement()
	}

	// Check for GET DIAGNOSTICS
	if p.matchKeyword("GET") {
		return p.parseGetDiagnosticsStatement()
	}

	// Check for OPEN (cursor)
	if p.matchKeyword("OPEN") {
		return p.parseOpenStatement()
	}

	// Check for FETCH (cursor)
	if p.matchKeyword("FETCH") {
		return p.parseFetchStatement()
	}

	// Check for CLOSE (cursor)
	if p.matchKeyword("CLOSE") {
		return p.parseCloseStatement()
	}

	// Check for CALL
	if p.matchKeyword("CALL") {
		return p.parseCallStatement()
	}

	// Check for labeled block or loop
	// <<label>> or identifier:
	if p.peek() == '<' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '<' {
		label, err := p.parseLabel()
		if err != nil {
			return nil, err
		}
		return p.parseLabeledStatement(label)
	}

	// Check for assignment: variable := expression
	// Or SQL statement
	return p.parseAssignmentOrSQL()
}

// parseLabel parses a <<label>>.
func (p *PLpgSQLParser) parseLabel() (string, error) {
	if p.peek() != '<' || p.pos+1 >= len(p.input) || p.input[p.pos+1] != '<' {
		return "", errors.New("expected <<label>>")
	}
	p.advance() // <
	p.advance() // <

	p.skipWhitespaceAndComments()

	name, err := p.parseIdentifier()
	if err != nil {
		return "", err
	}

	p.skipWhitespaceAndComments()

	if p.peek() != '>' || p.pos+1 >= len(p.input) || p.input[p.pos+1] != '>' {
		return "", errors.New("expected >> after label name")
	}
	p.advance() // >
	p.advance() // >

	return name, nil
}

// parseLabeledStatement parses a statement with a label.
func (p *PLpgSQLParser) parseLabeledStatement(label string) (PLpgSQLStmt, error) {
	p.skipWhitespaceAndComments()

	if p.matchKeyword("DECLARE") || p.matchKeyword("BEGIN") {
		return p.parseBlock(label)
	}
	if p.matchKeyword("LOOP") {
		return p.parseLoopStatement(label)
	}
	if p.matchKeyword("WHILE") {
		return p.parseWhileStatement(label)
	}
	if p.matchKeyword("FOR") {
		return p.parseForStatement(label)
	}
	if p.matchKeyword("FOREACH") {
		return p.parseForeachStatement(label)
	}

	return nil, errors.New("expected block or loop after label")
}

// parseReturnStatement parses a RETURN statement.
func (p *PLpgSQLParser) parseReturnStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("RETURN")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLReturnStmt{}

	// Check for RETURN NEXT
	if p.matchKeyword("NEXT") {
		p.consumeKeyword("NEXT")
		stmt.IsReturnNext = true
		p.skipWhitespaceAndComments()
		if p.peek() != ';' {
			expr, err := p.parseExpression(";")
			if err != nil {
				return nil, err
			}
			stmt.Expr = expr
		}
	} else if p.matchKeyword("QUERY") {
		// RETURN QUERY
		p.consumeKeyword("QUERY")
		stmt.IsReturnQuery = true
		p.skipWhitespaceAndComments()

		// Parse the query
		start := p.pos
		for p.pos < len(p.input) && p.peek() != ';' {
			p.advance()
		}
		stmt.Query = strings.TrimSpace(p.input[start:p.pos])
	} else if p.peek() != ';' {
		// RETURN expression
		expr, err := p.parseExpression(";")
		if err != nil {
			return nil, err
		}
		stmt.Expr = expr
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseRaiseStatement parses a RAISE statement.
func (p *PLpgSQLParser) parseRaiseStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("RAISE")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLRaiseStmt{
		Level:   RaiseLevelException, // Default to EXCEPTION
		Options: make(map[string]string),
	}

	// Parse optional level
	levels := []struct {
		keyword string
		level   PLpgSQLRaiseLevel
	}{
		{"DEBUG", RaiseLevelDebug},
		{"LOG", RaiseLevelLog},
		{"INFO", RaiseLevelInfo},
		{"NOTICE", RaiseLevelNotice},
		{"WARNING", RaiseLevelWarning},
		{"EXCEPTION", RaiseLevelException},
	}

	for _, l := range levels {
		if p.matchKeyword(l.keyword) {
			p.consumeKeyword(l.keyword)
			stmt.Level = l.level
			p.skipWhitespaceAndComments()
			break
		}
	}

	// Check for message (string literal)
	if p.peek() == '\'' {
		msg, err := p.parseStringLiteral()
		if err != nil {
			return nil, err
		}
		stmt.Message = msg
		p.skipWhitespaceAndComments()

		// Parse parameters (%, % ...)
		for p.peek() == ',' {
			p.advance()
			p.skipWhitespaceAndComments()

			// Check for USING
			if p.matchKeyword("USING") {
				break
			}

			expr, err := p.parseExpression(",", "USING", ";")
			if err != nil {
				return nil, err
			}
			stmt.Params = append(stmt.Params, expr)
		}
	} else if p.matchKeyword("USING") {
		// RAISE with no message, just USING
	} else if p.peek() != ';' {
		// RAISE condition_name
		condName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.Message = condName
	}

	p.skipWhitespaceAndComments()

	// Parse USING options
	if p.matchKeyword("USING") {
		p.consumeKeyword("USING")
		for {
			p.skipWhitespaceAndComments()

			// Parse option name (ERRCODE, HINT, DETAIL, MESSAGE, etc.)
			optName, err := p.parseIdentifier()
			if err != nil {
				break
			}
			p.skipWhitespaceAndComments()

			// Expect =
			if p.peek() != '=' {
				return nil, errors.New("expected = after USING option name")
			}
			p.advance()
			p.skipWhitespaceAndComments()

			// Parse option value
			var optVal string
			if p.peek() == '\'' {
				optVal, err = p.parseStringLiteral()
				if err != nil {
					return nil, err
				}
			} else {
				expr, err := p.parseExpression(",", ";")
				if err != nil {
					return nil, err
				}
				optVal = expr.Raw
			}

			stmt.Options[strings.ToUpper(optName)] = optVal

			p.skipWhitespaceAndComments()

			// Check for comma or end
			if p.peek() == ',' {
				p.advance()
				continue
			}
			break
		}
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseStringLiteral parses a single-quoted string literal.
func (p *PLpgSQLParser) parseStringLiteral() (string, error) {
	if p.peek() != '\'' {
		return "", errors.New("expected string literal")
	}
	p.advance()

	var result strings.Builder
	for p.pos < len(p.input) {
		c := p.peek()
		if c == '\'' {
			// Check for escape
			if p.pos+1 < len(p.input) && p.input[p.pos+1] == '\'' {
				result.WriteByte('\'')
				p.advance()
				p.advance()
				continue
			}
			p.advance()
			return result.String(), nil
		}
		result.WriteByte(c)
		p.advance()
	}

	return "", errors.New("unterminated string literal")
}

// parseIfStatement parses an IF statement.
func (p *PLpgSQLParser) parseIfStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("IF")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLIfStmt{}

	// Parse condition
	cond, err := p.parseExpression("THEN")
	if err != nil {
		return nil, err
	}
	stmt.Condition = cond

	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("THEN") {
		return nil, errors.New("expected THEN after IF condition")
	}

	// Parse THEN body
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("ELSIF") || p.matchKeyword("ELSE") || p.matchKeyword("END") {
			break
		}

		bodyStmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if bodyStmt != nil {
			stmt.ThenBody = append(stmt.ThenBody, bodyStmt)
		}
	}

	// Parse ELSIF clauses
	for p.matchKeyword("ELSIF") {
		p.consumeKeyword("ELSIF")
		p.skipWhitespaceAndComments()

		elsif := &PLpgSQLElsifClause{}

		elsifCond, err := p.parseExpression("THEN")
		if err != nil {
			return nil, err
		}
		elsif.Condition = elsifCond

		p.skipWhitespaceAndComments()
		if !p.consumeKeyword("THEN") {
			return nil, errors.New("expected THEN after ELSIF condition")
		}

		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("ELSIF") || p.matchKeyword("ELSE") || p.matchKeyword("END") {
				break
			}

			bodyStmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				elsif.Body = append(elsif.Body, bodyStmt)
			}
		}

		stmt.ElsifClauses = append(stmt.ElsifClauses, elsif)
	}

	// Parse ELSE clause
	if p.matchKeyword("ELSE") {
		p.consumeKeyword("ELSE")

		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("END") {
				break
			}

			bodyStmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				stmt.ElseBody = append(stmt.ElseBody, bodyStmt)
			}
		}
	}

	// Expect END IF
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END IF")
	}
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("IF") {
		return nil, errors.New("expected IF after END")
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseCaseStatement parses a CASE statement.
func (p *PLpgSQLParser) parseCaseStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("CASE")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLCaseStmt{}

	// Check for simple CASE (CASE expr WHEN ...) vs searched CASE (CASE WHEN ...)
	if !p.matchKeyword("WHEN") {
		expr, err := p.parseExpression("WHEN")
		if err != nil {
			return nil, err
		}
		stmt.Expr = expr
	}

	// Parse WHEN clauses
	for p.matchKeyword("WHEN") {
		p.consumeKeyword("WHEN")
		p.skipWhitespaceAndComments()

		when := &PLpgSQLWhenClause{}

		// Parse WHEN expressions (comma-separated for simple CASE)
		for {
			expr, err := p.parseExpression("THEN", ",")
			if err != nil {
				return nil, err
			}
			when.Exprs = append(when.Exprs, expr)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				p.skipWhitespaceAndComments()
				continue
			}
			break
		}

		if !p.consumeKeyword("THEN") {
			return nil, errors.New("expected THEN after WHEN")
		}

		// Parse body
		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("WHEN") || p.matchKeyword("ELSE") || p.matchKeyword("END") {
				break
			}

			bodyStmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				when.Body = append(when.Body, bodyStmt)
			}
		}

		stmt.WhenClauses = append(stmt.WhenClauses, when)
	}

	// Parse ELSE clause
	if p.matchKeyword("ELSE") {
		p.consumeKeyword("ELSE")

		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("END") {
				break
			}

			bodyStmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				stmt.ElseBody = append(stmt.ElseBody, bodyStmt)
			}
		}
	}

	// Expect END CASE
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END CASE")
	}
	p.skipWhitespaceAndComments()
	if p.matchKeyword("CASE") {
		p.consumeKeyword("CASE")
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseLoopStatement parses a simple LOOP statement.
func (p *PLpgSQLParser) parseLoopStatement(label string) (PLpgSQLStmt, error) {
	p.consumeKeyword("LOOP")

	stmt := &PLpgSQLLoopStmt{Label: label}

	// Parse body
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("END") {
			break
		}

		bodyStmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if bodyStmt != nil {
			stmt.Body = append(stmt.Body, bodyStmt)
		}
	}

	// Expect END LOOP
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END LOOP")
	}
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP after END")
	}

	// Optional label
	p.skipWhitespaceAndComments()
	if label != "" && p.matchKeyword(label) {
		p.consumeKeyword(label)
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseWhileStatement parses a WHILE loop statement.
func (p *PLpgSQLParser) parseWhileStatement(label string) (PLpgSQLStmt, error) {
	p.consumeKeyword("WHILE")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLWhileStmt{Label: label}

	// Parse condition
	cond, err := p.parseExpression("LOOP")
	if err != nil {
		return nil, err
	}
	stmt.Condition = cond

	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP after WHILE condition")
	}

	// Parse body
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("END") {
			break
		}

		bodyStmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if bodyStmt != nil {
			stmt.Body = append(stmt.Body, bodyStmt)
		}
	}

	// Expect END LOOP
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END LOOP")
	}
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP after END")
	}

	// Optional label
	p.skipWhitespaceAndComments()
	if label != "" && p.matchKeyword(label) {
		p.consumeKeyword(label)
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseForStatement parses a FOR loop statement.
func (p *PLpgSQLParser) parseForStatement(label string) (PLpgSQLStmt, error) {
	p.consumeKeyword("FOR")
	p.skipWhitespaceAndComments()

	// Parse loop variable
	varName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	p.skipWhitespaceAndComments()

	if !p.consumeKeyword("IN") {
		return nil, errors.New("expected IN after FOR variable")
	}

	p.skipWhitespaceAndComments()

	// Check for REVERSE
	reverse := false
	if p.matchKeyword("REVERSE") {
		p.consumeKeyword("REVERSE")
		reverse = true
		p.skipWhitespaceAndComments()
	}

	// Now we need to determine if this is:
	// 1. FOR i IN lower..upper LOOP (numeric range)
	// 2. FOR rec IN query LOOP (query loop)

	// Look ahead to see if we have a .. for numeric range
	savedPos := p.pos

	// Try to parse as expression and look for ..
	lowerExpr, err := p.parseExpression("..", "LOOP")
	if err != nil {
		return nil, err
	}

	p.skipWhitespaceAndComments()

	// Check if we have ..
	if p.peek() == '.' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '.' {
		// Numeric range FOR
		p.advance() // .
		p.advance() // .

		stmt := &PLpgSQLForStmt{
			Label:      label,
			Variable:   varName,
			Reverse:    reverse,
			LowerBound: lowerExpr,
		}

		p.skipWhitespaceAndComments()

		// Parse upper bound
		upperExpr, err := p.parseExpression("BY", "LOOP")
		if err != nil {
			return nil, err
		}
		stmt.UpperBound = upperExpr

		p.skipWhitespaceAndComments()

		// Check for BY step
		if p.matchKeyword("BY") {
			p.consumeKeyword("BY")
			p.skipWhitespaceAndComments()
			stepExpr, err := p.parseExpression("LOOP")
			if err != nil {
				return nil, err
			}
			stmt.Step = stepExpr
		}

		p.skipWhitespaceAndComments()
		if !p.consumeKeyword("LOOP") {
			return nil, errors.New("expected LOOP in FOR statement")
		}

		// Parse body
		for {
			p.skipWhitespaceAndComments()

			if p.matchKeyword("END") {
				break
			}

			bodyStmt, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				stmt.Body = append(stmt.Body, bodyStmt)
			}
		}

		// Expect END LOOP
		if !p.consumeKeyword("END") {
			return nil, errors.New("expected END LOOP")
		}
		p.skipWhitespaceAndComments()
		if !p.consumeKeyword("LOOP") {
			return nil, errors.New("expected LOOP after END")
		}

		// Optional label
		p.skipWhitespaceAndComments()
		if label != "" && p.matchKeyword(label) {
			p.consumeKeyword(label)
		}

		// Consume semicolon
		p.skipWhitespaceAndComments()
		if p.peek() == ';' {
			p.advance()
		}

		return stmt, nil
	}

	// Query FOR loop - reset and parse query
	p.pos = savedPos

	stmt := &PLpgSQLForQueryStmt{
		Label:    label,
		Variable: varName,
	}

	// Parse query until LOOP
	start := p.pos
	for p.pos < len(p.input) && !p.matchKeyword("LOOP") {
		p.advance()
	}
	stmt.Query = strings.TrimSpace(p.input[start:p.pos])

	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP in FOR statement")
	}

	// Parse body
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("END") {
			break
		}

		bodyStmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if bodyStmt != nil {
			stmt.Body = append(stmt.Body, bodyStmt)
		}
	}

	// Expect END LOOP
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END LOOP")
	}
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP after END")
	}

	// Optional label
	p.skipWhitespaceAndComments()
	if label != "" && p.matchKeyword(label) {
		p.consumeKeyword(label)
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseForeachStatement parses a FOREACH loop statement.
func (p *PLpgSQLParser) parseForeachStatement(label string) (PLpgSQLStmt, error) {
	p.consumeKeyword("FOREACH")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLForeachStmt{Label: label}

	// Parse variable
	varName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Variable = varName

	p.skipWhitespaceAndComments()

	// Optional SLICE
	if p.matchKeyword("SLICE") {
		p.consumeKeyword("SLICE")
		p.skipWhitespaceAndComments()
		// Parse slice number
		start := p.pos
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.peek())) {
			p.advance()
		}
		if p.pos > start {
			slice, _ := strconv.Atoi(p.input[start:p.pos])
			stmt.Slice = slice
		}
		p.skipWhitespaceAndComments()
	}

	if !p.consumeKeyword("IN") {
		return nil, errors.New("expected IN in FOREACH")
	}
	p.skipWhitespaceAndComments()

	if !p.consumeKeyword("ARRAY") {
		return nil, errors.New("expected ARRAY in FOREACH")
	}
	p.skipWhitespaceAndComments()

	// Parse array expression
	arrayExpr, err := p.parseExpression("LOOP")
	if err != nil {
		return nil, err
	}
	stmt.ArrayExpr = arrayExpr

	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP in FOREACH")
	}

	// Parse body
	for {
		p.skipWhitespaceAndComments()

		if p.matchKeyword("END") {
			break
		}

		bodyStmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if bodyStmt != nil {
			stmt.Body = append(stmt.Body, bodyStmt)
		}
	}

	// Expect END LOOP
	if !p.consumeKeyword("END") {
		return nil, errors.New("expected END LOOP")
	}
	p.skipWhitespaceAndComments()
	if !p.consumeKeyword("LOOP") {
		return nil, errors.New("expected LOOP after END")
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseExitStatement parses an EXIT statement.
func (p *PLpgSQLParser) parseExitStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("EXIT")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLExitStmt{}

	// Optional label
	if p.peek() != ';' && !p.matchKeyword("WHEN") {
		label, err := p.parseIdentifier()
		if err == nil {
			stmt.Label = label
		}
		p.skipWhitespaceAndComments()
	}

	// Optional WHEN condition
	if p.matchKeyword("WHEN") {
		p.consumeKeyword("WHEN")
		p.skipWhitespaceAndComments()

		cond, err := p.parseExpression(";")
		if err != nil {
			return nil, err
		}
		stmt.Condition = cond
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseContinueStatement parses a CONTINUE statement.
func (p *PLpgSQLParser) parseContinueStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("CONTINUE")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLContinueStmt{}

	// Optional label
	if p.peek() != ';' && !p.matchKeyword("WHEN") {
		label, err := p.parseIdentifier()
		if err == nil {
			stmt.Label = label
		}
		p.skipWhitespaceAndComments()
	}

	// Optional WHEN condition
	if p.matchKeyword("WHEN") {
		p.consumeKeyword("WHEN")
		p.skipWhitespaceAndComments()

		cond, err := p.parseExpression(";")
		if err != nil {
			return nil, err
		}
		stmt.Condition = cond
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parsePerformStatement parses a PERFORM statement.
func (p *PLpgSQLParser) parsePerformStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("PERFORM")
	p.skipWhitespaceAndComments()

	// Parse query until semicolon
	start := p.pos
	for p.pos < len(p.input) && p.peek() != ';' {
		p.advance()
	}

	stmt := &PLpgSQLPerformStmt{
		Query: strings.TrimSpace(p.input[start:p.pos]),
	}

	// Consume semicolon
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseExecuteStatement parses an EXECUTE statement.
func (p *PLpgSQLParser) parseExecuteStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("EXECUTE")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLExecuteStmt{}

	// Parse query expression
	queryExpr, err := p.parseExpression("INTO", "USING", ";")
	if err != nil {
		return nil, err
	}
	stmt.QueryExpr = queryExpr

	p.skipWhitespaceAndComments()

	// Check for INTO
	if p.matchKeyword("INTO") {
		p.consumeKeyword("INTO")
		p.skipWhitespaceAndComments()

		// Check for STRICT
		if p.matchKeyword("STRICT") {
			p.consumeKeyword("STRICT")
			stmt.Strict = true
			p.skipWhitespaceAndComments()
		}

		// Parse target variables
		for {
			varName, err := p.parseIdentifier()
			if err != nil {
				break
			}
			stmt.Into = append(stmt.Into, varName)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				p.skipWhitespaceAndComments()
				continue
			}
			break
		}
	}

	p.skipWhitespaceAndComments()

	// Check for USING
	if p.matchKeyword("USING") {
		p.consumeKeyword("USING")
		p.skipWhitespaceAndComments()

		for {
			param, err := p.parseExpression(",", ";")
			if err != nil {
				break
			}
			stmt.UsingParams = append(stmt.UsingParams, param)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				p.skipWhitespaceAndComments()
				continue
			}
			break
		}
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseGetDiagnosticsStatement parses a GET DIAGNOSTICS statement.
func (p *PLpgSQLParser) parseGetDiagnosticsStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("GET")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLGetDiagnosticsStmt{}

	// Check for STACKED
	if p.matchKeyword("STACKED") {
		p.consumeKeyword("STACKED")
		stmt.Stacked = true
		p.skipWhitespaceAndComments()
	}

	if !p.consumeKeyword("DIAGNOSTICS") {
		return nil, errors.New("expected DIAGNOSTICS after GET")
	}

	// Parse diagnostic items
	for {
		p.skipWhitespaceAndComments()

		varName, err := p.parseIdentifier()
		if err != nil {
			break
		}

		p.skipWhitespaceAndComments()

		// Expect = or :=
		if p.peek() == '=' ||
			(p.peek() == ':' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '=') {
			if p.peek() == ':' {
				p.advance()
			}
			p.advance()
		} else {
			return nil, errors.New("expected = in GET DIAGNOSTICS")
		}

		p.skipWhitespaceAndComments()

		// Parse diagnostic kind
		kind, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		stmt.Items = append(stmt.Items, &PLpgSQLDiagnosticsItem{
			Variable: varName,
			Kind:     strings.ToUpper(kind),
		})

		p.skipWhitespaceAndComments()
		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseOpenStatement parses an OPEN cursor statement.
func (p *PLpgSQLParser) parseOpenStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("OPEN")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLOpenStmt{}

	// Parse cursor variable
	cursorVar, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.CursorVar = cursorVar

	p.skipWhitespaceAndComments()

	// Check for bound cursor arguments or FOR query
	if p.matchKeyword("FOR") {
		p.consumeKeyword("FOR")
		p.skipWhitespaceAndComments()

		// Parse query
		start := p.pos
		for p.pos < len(p.input) && p.peek() != ';' {
			p.advance()
		}
		stmt.Query = strings.TrimSpace(p.input[start:p.pos])
	} else if p.peek() == '(' {
		// Bound cursor with arguments
		stmt.Bound = true
		p.advance()

		for {
			p.skipWhitespaceAndComments()
			if p.peek() == ')' {
				p.advance()
				break
			}

			arg, err := p.parseExpression(",", ")")
			if err != nil {
				return nil, err
			}
			stmt.Arguments = append(stmt.Arguments, arg)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				continue
			}
			if p.peek() == ')' {
				p.advance()
				break
			}
		}
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseFetchStatement parses a FETCH cursor statement.
func (p *PLpgSQLParser) parseFetchStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("FETCH")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLFetchStmt{
		Direction: "NEXT",
		Count:     1,
	}

	// Check for direction
	directions := []string{
		"NEXT",
		"PRIOR",
		"FIRST",
		"LAST",
		"ABSOLUTE",
		"RELATIVE",
		"FORWARD",
		"BACKWARD",
	}
	for _, dir := range directions {
		if p.matchKeyword(dir) {
			p.consumeKeyword(dir)
			stmt.Direction = dir
			p.skipWhitespaceAndComments()

			// Check for count (for ABSOLUTE, RELATIVE, FORWARD, BACKWARD)
			if dir == "ABSOLUTE" || dir == "RELATIVE" || dir == "FORWARD" || dir == "BACKWARD" {
				start := p.pos
				for p.pos < len(p.input) && (unicode.IsDigit(rune(p.peek())) || p.peek() == '-') {
					p.advance()
				}
				if p.pos > start {
					count, _ := strconv.Atoi(p.input[start:p.pos])
					stmt.Count = count
				}
				p.skipWhitespaceAndComments()
			}
			break
		}
	}

	// Parse cursor variable
	if p.matchKeyword("FROM") || p.matchKeyword("IN") {
		if p.matchKeyword("FROM") {
			p.consumeKeyword("FROM")
		} else {
			p.consumeKeyword("IN")
		}
		p.skipWhitespaceAndComments()
	}

	cursorVar, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.CursorVar = cursorVar

	p.skipWhitespaceAndComments()

	// Parse INTO clause
	if p.matchKeyword("INTO") {
		p.consumeKeyword("INTO")
		p.skipWhitespaceAndComments()

		for {
			varName, err := p.parseIdentifier()
			if err != nil {
				break
			}
			stmt.Into = append(stmt.Into, varName)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				p.skipWhitespaceAndComments()
				continue
			}
			break
		}
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseCloseStatement parses a CLOSE cursor statement.
func (p *PLpgSQLParser) parseCloseStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("CLOSE")
	p.skipWhitespaceAndComments()

	cursorVar, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	stmt := &PLpgSQLCloseStmt{
		CursorVar: cursorVar,
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseCallStatement parses a CALL statement.
func (p *PLpgSQLParser) parseCallStatement() (PLpgSQLStmt, error) {
	p.consumeKeyword("CALL")
	p.skipWhitespaceAndComments()

	stmt := &PLpgSQLCallStmt{}

	// Parse procedure name (may be schema.name)
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	p.skipWhitespaceAndComments()

	if p.peek() == '.' {
		p.advance()
		stmt.Schema = name
		name, err = p.parseIdentifier()
		if err != nil {
			return nil, err
		}
	}
	stmt.ProcName = name

	p.skipWhitespaceAndComments()

	// Parse arguments
	if p.peek() == '(' {
		p.advance()

		for {
			p.skipWhitespaceAndComments()
			if p.peek() == ')' {
				p.advance()
				break
			}

			arg, err := p.parseExpression(",", ")")
			if err != nil {
				return nil, err
			}
			stmt.Arguments = append(stmt.Arguments, arg)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				continue
			}
			if p.peek() == ')' {
				p.advance()
				break
			}
		}
	}

	p.skipWhitespaceAndComments()

	// Check for INTO clause
	if p.matchKeyword("INTO") {
		p.consumeKeyword("INTO")
		p.skipWhitespaceAndComments()

		for {
			varName, err := p.parseIdentifier()
			if err != nil {
				break
			}
			stmt.Into = append(stmt.Into, varName)

			p.skipWhitespaceAndComments()
			if p.peek() == ',' {
				p.advance()
				p.skipWhitespaceAndComments()
				continue
			}
			break
		}
	}

	// Consume semicolon
	p.skipWhitespaceAndComments()
	if p.peek() == ';' {
		p.advance()
	}

	return stmt, nil
}

// parseAssignmentOrSQL parses an assignment statement or embedded SQL.
func (p *PLpgSQLParser) parseAssignmentOrSQL() (PLpgSQLStmt, error) {
	p.skipWhitespaceAndComments()

	start := p.pos

	// Try to parse as identifier for assignment
	savedPos := p.pos
	varName, err := p.parseIdentifier()
	if err == nil {
		p.skipWhitespaceAndComments()

		// Check for assignment operator :=
		if p.peek() == ':' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '=' {
			p.advance() // :
			p.advance() // =

			p.skipWhitespaceAndComments()

			expr, err := p.parseExpression(";")
			if err != nil {
				return nil, err
			}

			// Consume semicolon
			p.skipWhitespaceAndComments()
			if p.peek() == ';' {
				p.advance()
			}

			return &PLpgSQLAssignStmt{
				Variable: varName,
				Expr:     expr,
			}, nil
		}
	}

	// Not an assignment, parse as SQL statement
	p.pos = savedPos

	// Parse until semicolon
	for p.pos < len(p.input) && p.peek() != ';' {
		p.advance()
	}

	sql := strings.TrimSpace(p.input[start:p.pos])

	// Consume semicolon
	if p.peek() == ';' {
		p.advance()
	}

	if sql == "" {
		return nil, nil
	}

	// Check if this is a SELECT INTO statement
	upperSQL := strings.ToUpper(sql)
	stmt := &PLpgSQLSQLStmt{SQL: sql}

	if strings.HasPrefix(upperSQL, "SELECT") && strings.Contains(upperSQL, " INTO ") {
		// Extract INTO targets
		intoMatch := regexp.MustCompile(`(?i)\bINTO\s+(STRICT\s+)?(.+?)\s+FROM\b`)
		if matches := intoMatch.FindStringSubmatch(sql); len(matches) > 0 {
			if matches[1] != "" {
				stmt.Strict = true
			}
			targets := strings.Split(matches[2], ",")
			for _, t := range targets {
				stmt.Into = append(stmt.Into, strings.TrimSpace(t))
			}
		}
	}

	return stmt, nil
}
