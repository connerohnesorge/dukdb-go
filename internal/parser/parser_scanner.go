package parser

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

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
		case ch == '$':
			// Check for $$ dollar-quoted string
			if p.pos+1 < len(p.input) && p.input[p.pos+1] == '$' {
				p.scanDollarString()
			} else {
				p.scanParameter()
			}
		case ch == '?':
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

// scanDollarString scans a $$dollar-quoted string$$ for multi-line function bodies.
// Dollar-quoted strings preserve newlines and don't need escape sequences.
func (p *parser) scanDollarString() {
	start := p.pos
	p.pos += 2 // skip opening $$

	// Find the closing $$
	for p.pos+1 < len(p.input) {
		if p.input[p.pos] == '$' && p.input[p.pos+1] == '$' {
			p.pos += 2 // skip closing $$
			// Store with the $$ delimiters included so we can extract the body later
			p.tokens = append(p.tokens, token{tokenString, p.input[start:p.pos], start})
			return
		}
		p.pos++
	}

	// Unterminated dollar-quoted string
	p.tokenErr = &dukdb.Error{
		Type: dukdb.ErrorTypeParser,
		Msg: fmt.Sprintf(
			"Parser Error: unterminated dollar-quoted string at or near %q",
			p.input[start:p.pos],
		),
	}
	p.tokens = append(p.tokens, token{tokenString, p.input[start:p.pos], start})
}

func (p *parser) scanString(quote byte) {
	start := p.pos
	p.pos++ // skip opening quote
	terminated := false
	for p.pos < len(p.input) {
		if p.input[p.pos] == quote {
			if p.pos+1 < len(p.input) &&
				p.input[p.pos+1] == quote {
				// Escaped quote
				p.pos += 2
			} else {
				p.pos++
				terminated = true

				break
			}
		} else {
			p.pos++
		}
	}
	if !terminated {
		// Unterminated string literal
		p.tokenErr = &dukdb.Error{
			Type: dukdb.ErrorTypeParser,
			Msg: fmt.Sprintf(
				"Parser Error: unterminated quoted string at or near %q",
				p.input[start:p.pos],
			),
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
	if p.pos+2 < len(p.input) {
		three := p.input[p.pos : p.pos+3]
		if three == "->>" {
			p.pos += 3
			p.tokens = append(
				p.tokens,
				token{tokenOperator, "->>", start},
			)
			return
		}
	}
	if p.pos+1 < len(p.input) {
		two := p.input[p.pos : p.pos+2]
		switch two {
		case "<=", ">=", "<>", "!=", "||", "::", "->":
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

// errorAtPosition creates a parser error with position information.
// This helps users locate syntax errors in their SQL statements.
func (p *parser) errorAtPosition(
	pos int,
	format string,
	args ...any,
) error {
	// Calculate line and column from position
	line := 1
	col := 1
	for i := 0; i < pos && i < len(p.input); i++ {
		if p.input[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}

	return &dukdb.Error{
		Type: dukdb.ErrorTypeParser,
		Msg: fmt.Sprintf(
			"Parser Error at line %d, column %d: "+format,
			append([]any{line, col}, args...)...),
	}
}
