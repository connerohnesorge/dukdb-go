package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	dukdb "github.com/dukdb/dukdb-go"
)

// Parse parses a SQL string and returns a Statement.
func Parse(sql string) (Statement, error) {
	p := newParser(sql)
	return p.parse()
}

// CountParameters counts the number of parameter placeholders in a statement.
func CountParameters(stmt Statement) int {
	counter := &paramCounter{}
	counter.countStmt(stmt)
	return counter.count
}

type paramCounter struct {
	count int
}

func (c *paramCounter) countStmt(stmt Statement) {
	switch s := stmt.(type) {
	case *SelectStmt:
		for _, col := range s.Columns {
			c.countExpr(col.Expr)
		}
		if s.Where != nil {
			c.countExpr(s.Where)
		}
		for _, g := range s.GroupBy {
			c.countExpr(g)
		}
		if s.Having != nil {
			c.countExpr(s.Having)
		}
		for _, o := range s.OrderBy {
			c.countExpr(o.Expr)
		}
		if s.Limit != nil {
			c.countExpr(s.Limit)
		}
		if s.Offset != nil {
			c.countExpr(s.Offset)
		}
	case *InsertStmt:
		for _, row := range s.Values {
			for _, val := range row {
				c.countExpr(val)
			}
		}
		if s.Select != nil {
			c.countStmt(s.Select)
		}
	case *UpdateStmt:
		for _, set := range s.Set {
			c.countExpr(set.Value)
		}
		if s.Where != nil {
			c.countExpr(s.Where)
		}
	case *DeleteStmt:
		if s.Where != nil {
			c.countExpr(s.Where)
		}
	}
}

func (c *paramCounter) countExpr(expr Expr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *Parameter:
		// For positional ? parameters, Position is 0, so we increment count
		if e.Position == 0 {
			c.count++
		} else if e.Position > c.count {
			c.count = e.Position
		}
	case *BinaryExpr:
		c.countExpr(e.Left)
		c.countExpr(e.Right)
	case *UnaryExpr:
		c.countExpr(e.Expr)
	case *FunctionCall:
		for _, arg := range e.Args {
			c.countExpr(arg)
		}
	case *CastExpr:
		c.countExpr(e.Expr)
	case *CaseExpr:
		c.countExpr(e.Operand)
		for _, w := range e.Whens {
			c.countExpr(w.Condition)
			c.countExpr(w.Result)
		}
		c.countExpr(e.Else)
	case *BetweenExpr:
		c.countExpr(e.Expr)
		c.countExpr(e.Low)
		c.countExpr(e.High)
	case *InListExpr:
		c.countExpr(e.Expr)
		for _, v := range e.Values {
			c.countExpr(v)
		}
	case *InSubqueryExpr:
		c.countExpr(e.Expr)
		c.countStmt(e.Subquery)
	case *ExistsExpr:
		c.countStmt(e.Subquery)
	case *SelectStmt:
		c.countStmt(e)
	}
}

// Token types
type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenOperator
	tokenLParen
	tokenRParen
	tokenComma
	tokenSemicolon
	tokenStar
	tokenDot
	tokenParameter
)

type token struct {
	typ   tokenType
	value string
	pos   int
}

type parser struct {
	input  string
	pos    int
	tokens []token
	tokPos int
}

func newParser(input string) *parser {
	p := &parser{input: input}
	p.tokenize()
	return p
}

func (p *parser) tokenize() {
	for p.pos < len(p.input) {
		p.skipWhitespace()
		if p.pos >= len(p.input) {
			break
		}

		ch := p.input[p.pos]

		switch {
		case ch == '(':
			p.tokens = append(
				p.tokens,
				token{tokenLParen, "(", p.pos},
			)
			p.pos++
		case ch == ')':
			p.tokens = append(
				p.tokens,
				token{tokenRParen, ")", p.pos},
			)
			p.pos++
		case ch == ',':
			p.tokens = append(
				p.tokens,
				token{tokenComma, ",", p.pos},
			)
			p.pos++
		case ch == ';':
			p.tokens = append(
				p.tokens,
				token{tokenSemicolon, ";", p.pos},
			)
			p.pos++
		case ch == '*':
			p.tokens = append(
				p.tokens,
				token{tokenStar, "*", p.pos},
			)
			p.pos++
		case ch == '.':
			p.tokens = append(
				p.tokens,
				token{tokenDot, ".", p.pos},
			)
			p.pos++
		case ch == '$' || ch == '?':
			p.scanParameter()
		case ch == '\'' || ch == '"':
			p.scanString(ch)
		case isDigit(ch):
			p.scanNumber()
		case isLetter(ch) || ch == '_':
			p.scanIdent()
		case isOperatorChar(ch):
			p.scanOperator()
		default:
			p.pos++
		}
	}
	p.tokens = append(
		p.tokens,
		token{tokenEOF, "", p.pos},
	)
}

func (p *parser) skipWhitespace() {
	for p.pos < len(p.input) {
		ch := p.input[p.pos]
		if ch == ' ' || ch == '\t' ||
			ch == '\n' ||
			ch == '\r' {
			p.pos++
		} else if p.pos+1 < len(p.input) && ch == '-' && p.input[p.pos+1] == '-' {
			// Line comment
			for p.pos < len(p.input) && p.input[p.pos] != '\n' {
				p.pos++
			}
		} else if p.pos+1 < len(p.input) && ch == '/' && p.input[p.pos+1] == '*' {
			// Block comment
			p.pos += 2
			for p.pos+1 < len(p.input) {
				if p.input[p.pos] == '*' && p.input[p.pos+1] == '/' {
					p.pos += 2
					break
				}
				p.pos++
			}
		} else {
			break
		}
	}
}

func (p *parser) scanParameter() {
	start := p.pos
	if p.input[p.pos] == '?' {
		p.pos++
		p.tokens = append(
			p.tokens,
			token{tokenParameter, "?", start},
		)
	} else {
		// $1, $2, etc.
		p.pos++ // skip $
		for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
			p.pos++
		}
		p.tokens = append(p.tokens, token{tokenParameter, p.input[start:p.pos], start})
	}
}

func (p *parser) scanString(quote byte) {
	start := p.pos
	p.pos++ // skip opening quote
	for p.pos < len(p.input) {
		if p.input[p.pos] == quote {
			if p.pos+1 < len(p.input) &&
				p.input[p.pos+1] == quote {
				// Escaped quote
				p.pos += 2
			} else {
				p.pos++
				break
			}
		} else {
			p.pos++
		}
	}
	p.tokens = append(
		p.tokens,
		token{
			tokenString,
			p.input[start:p.pos],
			start,
		},
	)
}

func (p *parser) scanNumber() {
	start := p.pos
	for p.pos < len(p.input) && (isDigit(p.input[p.pos]) || p.input[p.pos] == '.') {
		p.pos++
	}
	// Handle scientific notation
	if p.pos < len(p.input) &&
		(p.input[p.pos] == 'e' || p.input[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.input) &&
			(p.input[p.pos] == '+' || p.input[p.pos] == '-') {
			p.pos++
		}
		for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
			p.pos++
		}
	}
	p.tokens = append(
		p.tokens,
		token{
			tokenNumber,
			p.input[start:p.pos],
			start,
		},
	)
}

func (p *parser) scanIdent() {
	start := p.pos
	for p.pos < len(p.input) && (isLetter(p.input[p.pos]) || isDigit(p.input[p.pos]) || p.input[p.pos] == '_') {
		p.pos++
	}
	p.tokens = append(
		p.tokens,
		token{
			tokenIdent,
			p.input[start:p.pos],
			start,
		},
	)
}

func (p *parser) scanOperator() {
	start := p.pos
	// Handle multi-character operators
	if p.pos+1 < len(p.input) {
		two := p.input[p.pos : p.pos+2]
		switch two {
		case "<=", ">=", "<>", "!=", "||", "::":
			p.pos += 2
			p.tokens = append(
				p.tokens,
				token{tokenOperator, two, start},
			)
			return
		}
	}
	p.tokens = append(
		p.tokens,
		token{
			tokenOperator,
			string(p.input[p.pos]),
			start,
		},
	)
	p.pos++
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}

func isOperatorChar(ch byte) bool {
	return ch == '+' || ch == '-' || ch == '/' || ch == '%' ||
		ch == '<' || ch == '>' || ch == '=' ||
		ch == '!' ||
		ch == '|' ||
		ch == ':'
}

// Parser methods

func (p *parser) current() token {
	if p.tokPos >= len(p.tokens) {
		return token{tokenEOF, "", len(p.input)}
	}
	return p.tokens[p.tokPos]
}

func (p *parser) peek() token {
	if p.tokPos+1 >= len(p.tokens) {
		return token{tokenEOF, "", len(p.input)}
	}
	return p.tokens[p.tokPos+1]
}

func (p *parser) advance() token {
	tok := p.current()
	p.tokPos++
	return tok
}

func (p *parser) expect(
	typ tokenType,
) (token, error) {
	tok := p.current()
	if tok.typ != typ {
		return tok, p.errorf(
			"expected %v, got %v",
			typ,
			tok.typ,
		)
	}
	p.tokPos++
	return tok, nil
}

func (p *parser) expectKeyword(
	keyword string,
) error {
	tok := p.current()
	if tok.typ != tokenIdent ||
		!strings.EqualFold(tok.value, keyword) {
		return p.errorf(
			"expected %s, got %s",
			keyword,
			tok.value,
		)
	}
	p.tokPos++
	return nil
}

func (p *parser) isKeyword(keyword string) bool {
	tok := p.current()
	return tok.typ == tokenIdent &&
		strings.EqualFold(tok.value, keyword)
}

func (p *parser) errorf(
	format string,
	args ...any,
) error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeParser,
		Msg: fmt.Sprintf(
			"Parser Error: "+format,
			args...),
	}
}

func (p *parser) parse() (Statement, error) {
	if p.current().typ == tokenEOF {
		return nil, p.errorf("empty query")
	}

	var stmt Statement
	var err error

	switch {
	case p.isKeyword("SELECT"):
		stmt, err = p.parseSelect()
	case p.isKeyword("INSERT"):
		stmt, err = p.parseInsert()
	case p.isKeyword("UPDATE"):
		stmt, err = p.parseUpdate()
	case p.isKeyword("DELETE"):
		stmt, err = p.parseDelete()
	case p.isKeyword("CREATE"):
		stmt, err = p.parseCreate()
	case p.isKeyword("DROP"):
		stmt, err = p.parseDrop()
	default:
		return nil, p.errorf(
			"unexpected token: %s",
			p.current().value,
		)
	}

	if err != nil {
		return nil, err
	}

	// Skip optional semicolon
	if p.current().typ == tokenSemicolon {
		p.advance()
	}

	return stmt, nil
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	stmt := &SelectStmt{}

	// DISTINCT
	if p.isKeyword("DISTINCT") {
		p.advance()
		stmt.Distinct = true
	}

	// Columns
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if p.isKeyword("FROM") {
		from, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// WHERE
	if p.isKeyword("WHERE") {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// GROUP BY
	if p.isKeyword("GROUP") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		groupBy, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy

		// HAVING
		if p.isKeyword("HAVING") {
			p.advance()
			having, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Having = having
		}
	}

	// ORDER BY
	if p.isKeyword("ORDER") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// LIMIT
	if p.isKeyword("LIMIT") {
		p.advance()
		limit, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit

		// OFFSET
		if p.isKeyword("OFFSET") {
			p.advance()
			offset, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Offset = offset
		}
	}

	return stmt, nil
}

func (p *parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		if p.current().typ == tokenStar {
			p.advance()
			cols = append(
				cols,
				SelectColumn{
					Star: true,
					Expr: &StarExpr{},
				},
			)
		} else {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expr: expr}

			// AS alias or just alias
			if p.isKeyword("AS") {
				p.advance()
				if p.current().typ != tokenIdent {
					return nil, p.errorf("expected identifier after AS")
				}
				col.Alias = p.advance().value
			} else if p.current().typ == tokenIdent && !p.isKeyword("FROM") &&
				!p.isKeyword("WHERE") && !p.isKeyword("GROUP") &&
				!p.isKeyword("ORDER") && !p.isKeyword("LIMIT") &&
				!p.isKeyword("HAVING") && !p.isKeyword("INNER") &&
				!p.isKeyword("LEFT") && !p.isKeyword("RIGHT") &&
				!p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
				!p.isKeyword("JOIN") && !p.isKeyword("ON") {
				col.Alias = p.advance().value
			}

			cols = append(cols, col)
		}

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	return cols, nil
}

func (p *parser) parseFrom() (*FromClause, error) {
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	from := &FromClause{}

	// Parse first table
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	from.Tables = append(from.Tables, table)

	// Parse additional tables and joins
	for {
		if p.current().typ == tokenComma {
			p.advance()
			table, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			from.Tables = append(
				from.Tables,
				table,
			)
		} else if p.isKeyword("JOIN") || p.isKeyword("INNER") || p.isKeyword("LEFT") ||
			p.isKeyword("RIGHT") || p.isKeyword("FULL") || p.isKeyword("CROSS") {
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			from.Joins = append(from.Joins, join)
		} else {
			break
		}
	}

	return from, nil
}

func (p *parser) parseTableRef() (TableRef, error) {
	var ref TableRef

	if p.current().typ == tokenLParen {
		// Subquery
		p.advance()
		subquery, err := p.parseSelect()
		if err != nil {
			return ref, err
		}
		ref.Subquery = subquery
		if _, err := p.expect(tokenRParen); err != nil {
			return ref, err
		}
	} else if p.current().typ == tokenIdent {
		ref.TableName = p.advance().value
		if p.current().typ == tokenDot {
			p.advance()
			ref.Schema = ref.TableName
			if p.current().typ != tokenIdent {
				return ref, p.errorf("expected table name after dot")
			}
			ref.TableName = p.advance().value
		}
	} else {
		return ref, p.errorf("expected table name or subquery")
	}

	// Alias
	if p.isKeyword("AS") {
		p.advance()
		if p.current().typ != tokenIdent {
			return ref, p.errorf(
				"expected alias after AS",
			)
		}
		ref.Alias = p.advance().value
	} else if p.current().typ == tokenIdent && !p.isKeyword("WHERE") &&
		!p.isKeyword("GROUP") && !p.isKeyword("ORDER") && !p.isKeyword("LIMIT") &&
		!p.isKeyword("JOIN") && !p.isKeyword("INNER") && !p.isKeyword("LEFT") &&
		!p.isKeyword("RIGHT") && !p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
		!p.isKeyword("ON") && !p.isKeyword("HAVING") {
		ref.Alias = p.advance().value
	}

	return ref, nil
}

func (p *parser) parseJoin() (JoinClause, error) {
	var join JoinClause

	// Join type
	switch {
	case p.isKeyword("INNER"):
		p.advance()
		join.Type = JoinTypeInner
	case p.isKeyword("LEFT"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeLeft
	case p.isKeyword("RIGHT"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeRight
	case p.isKeyword("FULL"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeFull
	case p.isKeyword("CROSS"):
		p.advance()
		join.Type = JoinTypeCross
	default:
		join.Type = JoinTypeInner
	}

	if err := p.expectKeyword("JOIN"); err != nil {
		return join, err
	}

	// Table
	table, err := p.parseTableRef()
	if err != nil {
		return join, err
	}
	join.Table = table

	// ON condition (not for CROSS JOIN)
	if join.Type != JoinTypeCross {
		if err := p.expectKeyword("ON"); err != nil {
			return join, err
		}
		cond, err := p.parseExpr()
		if err != nil {
			return join, err
		}
		join.Condition = cond
	}

	return join, nil
}

func (p *parser) parseOrderBy() ([]OrderByExpr, error) {
	var orderBy []OrderByExpr

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		order := OrderByExpr{Expr: expr}

		if p.isKeyword("DESC") {
			p.advance()
			order.Desc = true
		} else if p.isKeyword("ASC") {
			p.advance()
		}

		orderBy = append(orderBy, order)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	return orderBy, nil
}

func (p *parser) parseInsert() (*InsertStmt, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}

	stmt := &InsertStmt{}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf(
			"expected table name",
		)
	}
	stmt.Table = p.advance().value

	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected table name after dot",
			)
		}
		stmt.Table = p.advance().value
	}

	// Optional column list
	if p.current().typ == tokenLParen {
		p.advance()
		for {
			if p.current().typ != tokenIdent {
				return nil, p.errorf(
					"expected column name",
				)
			}
			stmt.Columns = append(
				stmt.Columns,
				p.advance().value,
			)

			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	// VALUES or SELECT
	if p.isKeyword("VALUES") {
		p.advance()
		for {
			if _, err := p.expect(tokenLParen); err != nil {
				return nil, err
			}

			values, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			stmt.Values = append(
				stmt.Values,
				values,
			)

			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}

			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
	} else if p.isKeyword("SELECT") {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel
	} else {
		return nil, p.errorf("expected VALUES or SELECT")
	}

	return stmt, nil
}

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}

	stmt := &UpdateStmt{}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf(
			"expected table name",
		)
	}
	stmt.Table = p.advance().value

	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected table name after dot",
			)
		}
		stmt.Table = p.advance().value
	}

	// SET
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	for {
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected column name",
			)
		}
		col := p.advance().value

		if p.current().typ != tokenOperator ||
			p.current().value != "=" {
			return nil, p.errorf(
				"expected = after column name",
			)
		}
		p.advance()

		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		stmt.Set = append(
			stmt.Set,
			SetClause{Column: col, Value: val},
		)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	// WHERE
	if p.isKeyword("WHERE") {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *parser) parseDelete() (*DeleteStmt, error) {
	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	stmt := &DeleteStmt{}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf(
			"expected table name",
		)
	}
	stmt.Table = p.advance().value

	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected table name after dot",
			)
		}
		stmt.Table = p.advance().value
	}

	// WHERE
	if p.isKeyword("WHERE") {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *parser) parseCreate() (Statement, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}

	if p.isKeyword("TABLE") {
		return p.parseCreateTable()
	}

	return nil, p.errorf(
		"expected TABLE after CREATE",
	)
}

func (p *parser) parseCreateTable() (*CreateTableStmt, error) {
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{}

	// IF NOT EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf(
			"expected table name",
		)
	}
	stmt.Table = p.advance().value

	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected table name after dot",
			)
		}
		stmt.Table = p.advance().value
	}

	// Column definitions
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	for {
		// Check for PRIMARY KEY constraint
		if p.isKeyword("PRIMARY") {
			p.advance()
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			if _, err := p.expect(tokenLParen); err != nil {
				return nil, err
			}
			for {
				if p.current().typ != tokenIdent {
					return nil, p.errorf(
						"expected column name in PRIMARY KEY",
					)
				}
				stmt.PrimaryKey = append(
					stmt.PrimaryKey,
					p.advance().value,
				)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col)
		}

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *parser) parseColumnDef() (ColumnDefClause, error) {
	var col ColumnDefClause

	if p.current().typ != tokenIdent {
		return col, p.errorf(
			"expected column name",
		)
	}
	col.Name = p.advance().value

	// Data type
	if p.current().typ != tokenIdent {
		return col, p.errorf("expected data type")
	}
	typeName := strings.ToUpper(p.advance().value)
	col.DataType = parseTypeName(typeName)

	// Type modifiers (e.g., VARCHAR(100), DECIMAL(10,2))
	if p.current().typ == tokenLParen {
		p.advance()
		// Skip type parameters for now
		depth := 1
		for depth > 0 && p.current().typ != tokenEOF {
			if p.current().typ == tokenLParen {
				depth++
			} else if p.current().typ == tokenRParen {
				depth--
			}
			if depth > 0 {
				p.advance()
			}
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return col, err
		}
	}

	// Column constraints
	for p.current().typ == tokenIdent {
		switch {
		case p.isKeyword("NOT"):
			p.advance()
			if err := p.expectKeyword("NULL"); err != nil {
				return col, err
			}
			col.NotNull = true
		case p.isKeyword("NULL"):
			p.advance()
			col.NotNull = false
		case p.isKeyword("PRIMARY"):
			p.advance()
			if err := p.expectKeyword("KEY"); err != nil {
				return col, err
			}
			col.PrimaryKey = true
			col.NotNull = true
		case p.isKeyword("DEFAULT"):
			p.advance()
			def, err := p.parseExpr()
			if err != nil {
				return col, err
			}
			col.Default = def
		default:
			// Unknown constraint, stop parsing constraints
			goto done
		}
	}
done:

	return col, nil
}

func (p *parser) parseDrop() (Statement, error) {
	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}

	if p.isKeyword("TABLE") {
		return p.parseDropTable()
	}

	return nil, p.errorf(
		"expected TABLE after DROP",
	)
}

func (p *parser) parseDropTable() (*DropTableStmt, error) {
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	stmt := &DropTableStmt{}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf(
			"expected table name",
		)
	}
	stmt.Table = p.advance().value

	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected table name after dot",
			)
		}
		stmt.Table = p.advance().value
	}

	return stmt, nil
}

func (p *parser) parseExprList() ([]Expr, error) {
	var exprs []Expr

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	return exprs, nil
}

func (p *parser) parseExpr() (Expr, error) {
	return p.parseOrExpr()
}

func (p *parser) parseOrExpr() (Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("OR") {
		p.advance()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpOr,
			Right: right,
		}
	}

	return left, nil
}

func (p *parser) parseAndExpr() (Expr, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("AND") {
		p.advance()
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpAnd,
			Right: right,
		}
	}

	return left, nil
}

func (p *parser) parseNotExpr() (Expr, error) {
	if p.isKeyword("NOT") {
		p.advance()
		expr, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{
			Op:   OpNot,
			Expr: expr,
		}, nil
	}

	return p.parseComparisonExpr()
}

func (p *parser) parseComparisonExpr() (Expr, error) {
	left, err := p.parseAddExpr()
	if err != nil {
		return nil, err
	}

	// IS NULL / IS NOT NULL
	if p.isKeyword("IS") {
		p.advance()
		not := false
		if p.isKeyword("NOT") {
			p.advance()
			not = true
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return nil, err
		}
		if not {
			return &UnaryExpr{
				Op:   OpIsNotNull,
				Expr: left,
			}, nil
		}
		return &UnaryExpr{
			Op:   OpIsNull,
			Expr: left,
		}, nil
	}

	// BETWEEN
	if p.isKeyword("BETWEEN") {
		p.advance()
		low, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{
			Expr: left,
			Low:  low,
			High: high,
		}, nil
	}

	// NOT BETWEEN
	if p.isKeyword("NOT") &&
		p.peek().typ == tokenIdent &&
		strings.EqualFold(
			p.peek().value,
			"BETWEEN",
		) {
		p.advance() // NOT
		p.advance() // BETWEEN
		low, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{
			Expr: left,
			Low:  low,
			High: high,
			Not:  true,
		}, nil
	}

	// IN
	if p.isKeyword("IN") {
		p.advance()
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if p.isKeyword("SELECT") {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			return &InSubqueryExpr{
				Expr:     left,
				Subquery: subquery,
			}, nil
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &InListExpr{
			Expr:   left,
			Values: values,
		}, nil
	}

	// NOT IN
	if p.isKeyword("NOT") &&
		p.peek().typ == tokenIdent &&
		strings.EqualFold(p.peek().value, "IN") {
		p.advance() // NOT
		p.advance() // IN
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if p.isKeyword("SELECT") {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			return &InSubqueryExpr{
				Expr:     left,
				Subquery: subquery,
				Not:      true,
			}, nil
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &InListExpr{
			Expr:   left,
			Values: values,
			Not:    true,
		}, nil
	}

	// LIKE
	if p.isKeyword("LIKE") {
		p.advance()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:  left,
			Op:    OpLike,
			Right: right,
		}, nil
	}

	// NOT LIKE
	if p.isKeyword("NOT") &&
		p.peek().typ == tokenIdent &&
		strings.EqualFold(
			p.peek().value,
			"LIKE",
		) {
		p.advance() // NOT
		p.advance() // LIKE
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:  left,
			Op:    OpNotLike,
			Right: right,
		}, nil
	}

	// Comparison operators
	if p.current().typ == tokenOperator {
		var op BinaryOp
		switch p.current().value {
		case "=":
			op = OpEq
		case "<>", "!=":
			op = OpNe
		case "<":
			op = OpLt
		case "<=":
			op = OpLe
		case ">":
			op = OpGt
		case ">=":
			op = OpGe
		default:
			return left, nil
		}
		p.advance()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:  left,
			Op:    op,
			Right: right,
		}, nil
	}

	return left, nil
}

func (p *parser) parseAddExpr() (Expr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenOperator {
		var op BinaryOp
		switch p.current().value {
		case "+":
			op = OpAdd
		case "-":
			op = OpSub
		case "||":
			op = OpConcat
		default:
			return left, nil
		}
		p.advance()
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}

	return left, nil
}

func (p *parser) parseMulExpr() (Expr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenOperator || p.current().typ == tokenStar {
		var op BinaryOp
		switch p.current().value {
		case "*":
			op = OpMul
		case "/":
			op = OpDiv
		case "%":
			op = OpMod
		default:
			return left, nil
		}
		p.advance()
		right, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}

	return left, nil
}

func (p *parser) parseUnaryExpr() (Expr, error) {
	if p.current().typ == tokenOperator {
		switch p.current().value {
		case "-":
			p.advance()
			expr, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{
				Op:   OpNeg,
				Expr: expr,
			}, nil
		case "+":
			p.advance()
			expr, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{
				Op:   OpPos,
				Expr: expr,
			}, nil
		}
	}

	return p.parsePrimaryExpr()
}

func (p *parser) parsePrimaryExpr() (Expr, error) {
	tok := p.current()

	switch tok.typ {
	case tokenNumber:
		p.advance()
		return p.parseNumber(tok.value)

	case tokenString:
		p.advance()
		// Remove quotes and unescape
		s := tok.value[1 : len(tok.value)-1]
		s = strings.ReplaceAll(s, "''", "'")
		s = strings.ReplaceAll(s, "\"\"", "\"")
		return &Literal{
			Value: s,
			Type:  dukdb.TYPE_VARCHAR,
		}, nil

	case tokenParameter:
		p.advance()
		return p.parseParameter(tok.value)

	case tokenLParen:
		p.advance()
		if p.isKeyword("SELECT") {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			subquery.IsSubquery = true
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			return subquery, nil
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return expr, nil

	case tokenIdent:
		return p.parseIdentExpr()

	case tokenStar:
		p.advance()
		return &StarExpr{}, nil

	default:
		return nil, p.errorf(
			"unexpected token: %s",
			tok.value,
		)
	}
}

func (p *parser) parseNumber(
	s string,
) (Expr, error) {
	if strings.Contains(s, ".") ||
		strings.ContainsAny(s, "eE") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, p.errorf(
				"invalid number: %s",
				s,
			)
		}
		return &Literal{
			Value: f,
			Type:  dukdb.TYPE_DOUBLE,
		}, nil
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, p.errorf(
			"invalid number: %s",
			s,
		)
	}
	return &Literal{
		Value: i,
		Type:  dukdb.TYPE_BIGINT,
	}, nil
}

func (p *parser) parseParameter(
	s string,
) (Expr, error) {
	if s == "?" {
		// Positional parameter - position will be assigned during binding
		return &Parameter{Position: 0}, nil
	}
	// $1, $2, etc.
	pos, err := strconv.Atoi(s[1:])
	if err != nil {
		return nil, p.errorf(
			"invalid parameter: %s",
			s,
		)
	}
	return &Parameter{Position: pos}, nil
}

func (p *parser) parseIdentExpr() (Expr, error) {
	name := p.advance().value

	// Check for special keywords
	switch strings.ToUpper(name) {
	case "NULL":
		return &Literal{
			Value: nil,
			Type:  dukdb.TYPE_SQLNULL,
		}, nil
	case "TRUE":
		return &Literal{
			Value: true,
			Type:  dukdb.TYPE_BOOLEAN,
		}, nil
	case "FALSE":
		return &Literal{
			Value: false,
			Type:  dukdb.TYPE_BOOLEAN,
		}, nil
	case "CASE":
		return p.parseCase()
	case "CAST":
		return p.parseCast()
	case "EXISTS":
		return p.parseExists()
	}

	// Function call or column reference
	if p.current().typ == tokenLParen {
		return p.parseFunctionCall(name)
	}

	// Column reference with table prefix
	if p.current().typ == tokenDot {
		p.advance()
		if p.current().typ == tokenStar {
			p.advance()
			return &StarExpr{Table: name}, nil
		}
		if p.current().typ != tokenIdent {
			return nil, p.errorf(
				"expected column name after dot",
			)
		}
		col := p.advance().value
		return &ColumnRef{
			Table:  name,
			Column: col,
		}, nil
	}

	return &ColumnRef{Column: name}, nil
}

func (p *parser) parseFunctionCall(
	name string,
) (Expr, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	fn := &FunctionCall{
		Name: strings.ToUpper(name),
	}

	// Check for COUNT(*)
	if p.current().typ == tokenStar {
		p.advance()
		fn.Star = true
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return fn, nil
	}

	// Check for DISTINCT
	if p.isKeyword("DISTINCT") {
		p.advance()
		fn.Distinct = true
	}

	// Parse arguments
	if p.current().typ != tokenRParen {
		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		fn.Args = args
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return fn, nil
}

func (p *parser) parseCase() (Expr, error) {
	caseExpr := &CaseExpr{}

	// Simple CASE (CASE expr WHEN val THEN result ...)
	// or Searched CASE (CASE WHEN cond THEN result ...)
	if !p.isKeyword("WHEN") {
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		caseExpr.Operand = operand
	}

	// WHEN clauses
	for p.isKeyword("WHEN") {
		p.advance()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		caseExpr.Whens = append(
			caseExpr.Whens,
			WhenClause{
				Condition: cond,
				Result:    result,
			},
		)
	}

	// ELSE
	if p.isKeyword("ELSE") {
		p.advance()
		elseExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		caseExpr.Else = elseExpr
	}

	if err := p.expectKeyword("END"); err != nil {
		return nil, err
	}

	return caseExpr, nil
}

func (p *parser) parseCast() (Expr, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected type name")
	}
	typeName := strings.ToUpper(p.advance().value)
	targetType := parseTypeName(typeName)

	// Skip type parameters
	if p.current().typ == tokenLParen {
		p.advance()
		depth := 1
		for depth > 0 && p.current().typ != tokenEOF {
			if p.current().typ == tokenLParen {
				depth++
			} else if p.current().typ == tokenRParen {
				depth--
			}
			if depth > 0 {
				p.advance()
			}
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &CastExpr{
		Expr:       expr,
		TargetType: targetType,
	}, nil
}

func (p *parser) parseExists() (Expr, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	subquery, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &ExistsExpr{Subquery: subquery}, nil
}

// parseTypeName converts a type name string to a Type.
func parseTypeName(name string) dukdb.Type {
	// Normalize the name
	name = strings.ToUpper(
		strings.TrimSpace(name),
	)

	// Map type names to types
	switch name {
	case "BOOLEAN", "BOOL":
		return dukdb.TYPE_BOOLEAN
	case "TINYINT", "INT1":
		return dukdb.TYPE_TINYINT
	case "SMALLINT", "INT2":
		return dukdb.TYPE_SMALLINT
	case "INTEGER", "INT", "INT4":
		return dukdb.TYPE_INTEGER
	case "BIGINT", "INT8":
		return dukdb.TYPE_BIGINT
	case "UTINYINT":
		return dukdb.TYPE_UTINYINT
	case "USMALLINT":
		return dukdb.TYPE_USMALLINT
	case "UINTEGER":
		return dukdb.TYPE_UINTEGER
	case "UBIGINT":
		return dukdb.TYPE_UBIGINT
	case "FLOAT", "FLOAT4", "REAL":
		return dukdb.TYPE_FLOAT
	case "DOUBLE", "FLOAT8":
		return dukdb.TYPE_DOUBLE
	case "VARCHAR",
		"TEXT",
		"STRING",
		"CHAR",
		"BPCHAR":
		return dukdb.TYPE_VARCHAR
	case "BLOB", "BYTEA":
		return dukdb.TYPE_BLOB
	case "DATE":
		return dukdb.TYPE_DATE
	case "TIME":
		return dukdb.TYPE_TIME
	case "TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "INTERVAL":
		return dukdb.TYPE_INTERVAL
	case "DECIMAL", "NUMERIC":
		return dukdb.TYPE_DECIMAL
	case "UUID":
		return dukdb.TYPE_UUID
	case "HUGEINT":
		return dukdb.TYPE_HUGEINT
	case "UHUGEINT":
		return dukdb.TYPE_UHUGEINT
	default:
		return dukdb.TYPE_VARCHAR // Default to VARCHAR for unknown types
	}
}

// GetTypeName returns the SQL type name for a Type.
func GetTypeName(t dukdb.Type) string {
	switch t {
	case dukdb.TYPE_BOOLEAN:
		return "BOOLEAN"
	case dukdb.TYPE_TINYINT:
		return "TINYINT"
	case dukdb.TYPE_SMALLINT:
		return "SMALLINT"
	case dukdb.TYPE_INTEGER:
		return "INTEGER"
	case dukdb.TYPE_BIGINT:
		return "BIGINT"
	case dukdb.TYPE_UTINYINT:
		return "UTINYINT"
	case dukdb.TYPE_USMALLINT:
		return "USMALLINT"
	case dukdb.TYPE_UINTEGER:
		return "UINTEGER"
	case dukdb.TYPE_UBIGINT:
		return "UBIGINT"
	case dukdb.TYPE_FLOAT:
		return "FLOAT"
	case dukdb.TYPE_DOUBLE:
		return "DOUBLE"
	case dukdb.TYPE_VARCHAR:
		return "VARCHAR"
	case dukdb.TYPE_BLOB:
		return "BLOB"
	case dukdb.TYPE_DATE:
		return "DATE"
	case dukdb.TYPE_TIME:
		return "TIME"
	case dukdb.TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case dukdb.TYPE_INTERVAL:
		return "INTERVAL"
	case dukdb.TYPE_DECIMAL:
		return "DECIMAL"
	case dukdb.TYPE_UUID:
		return "UUID"
	case dukdb.TYPE_HUGEINT:
		return "HUGEINT"
	case dukdb.TYPE_UHUGEINT:
		return "UHUGEINT"
	default:
		return "UNKNOWN"
	}
}

// make unicode package used
var _ = unicode.IsSpace
