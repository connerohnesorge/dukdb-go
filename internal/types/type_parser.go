package types

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/dukdb/dukdb-go"
)

type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenComma
	tokenLParen
	tokenRParen
	tokenLBracket
	tokenRBracket
	tokenColon
)

type typeToken struct {
	typ   tokenType
	value string
}

type typeScanner struct {
	input []rune
	pos   int
}

func newTypeScanner(input string) *typeScanner {
	return &typeScanner{input: []rune(input)}
}

func (s *typeScanner) next() rune {
	if s.pos >= len(s.input) {
		return 0
	}
	ch := s.input[s.pos]
	s.pos++
	return ch
}

func (s *typeScanner) peek() rune {
	if s.pos >= len(s.input) {
		return 0
	}
	return s.input[s.pos]
}

func (s *typeScanner) scanToken() (typeToken, error) {
	for unicode.IsSpace(s.peek()) {
		s.next()
	}

	ch := s.next()
	switch ch {
	case 0:
		return typeToken{typ: tokenEOF}, nil
	case ',':
		return typeToken{typ: tokenComma, value: ","}, nil
	case '(':
		return typeToken{typ: tokenLParen, value: "("}, nil
	case ')':
		return typeToken{typ: tokenRParen, value: ")"}, nil
	case '[':
		return typeToken{typ: tokenLBracket, value: "["}, nil
	case ']':
		return typeToken{typ: tokenRBracket, value: "]"}, nil
	case ':':
		return typeToken{typ: tokenColon, value: ":"}, nil
	case '\'', '"':
		quote := ch
		var b strings.Builder
		for {
			next := s.next()
			if next == 0 {
				return typeToken{}, fmt.Errorf("unterminated string")
			}
			if next == quote {
				break
			}
			if next == '\\' {
				escaped := s.next()
				if escaped == 0 {
					return typeToken{}, fmt.Errorf("unterminated string")
				}
				b.WriteRune(escaped)
				continue
			}
			b.WriteRune(next)
		}
		return typeToken{typ: tokenString, value: b.String()}, nil
	}

	if unicode.IsDigit(ch) {
		var b strings.Builder
		b.WriteRune(ch)
		for unicode.IsDigit(s.peek()) {
			b.WriteRune(s.next())
		}
		return typeToken{typ: tokenNumber, value: b.String()}, nil
	}

	if unicode.IsLetter(ch) || ch == '_' {
		var b strings.Builder
		b.WriteRune(ch)
		for {
			next := s.peek()
			if unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' {
				b.WriteRune(s.next())
				continue
			}
			break
		}
		return typeToken{typ: tokenIdent, value: b.String()}, nil
	}

	return typeToken{}, fmt.Errorf("unexpected character %q", ch)
}

type typeParser struct {
	tokens []typeToken
	pos    int
	system *TypeSystem
}

func newTypeParser(input string) *typeParser {
	scanner := newTypeScanner(input)
	tokens := make([]typeToken, 0, 16)
	for {
		tok, err := scanner.scanToken()
		if err != nil {
			tokens = append(tokens, typeToken{typ: tokenEOF})
			break
		}
		tokens = append(tokens, tok)
		if tok.typ == tokenEOF {
			break
		}
	}
	return &typeParser{tokens: tokens}
}

func (p *typeParser) parseType(ts *TypeSystem) (dukdb.TypeInfo, error) {
	p.system = ts
	tok := p.peek()
	if tok.typ != tokenIdent {
		return nil, fmt.Errorf("expected type name")
	}
	p.advance()

	name := strings.ToUpper(tok.value)
	if handler, ok := ts.parsers[name]; ok {
		return handler(p, name)
	}

	info, err := dukdb.NewTypeInfo(parseTypeName(name))
	if err != nil {
		return nil, err
	}

	if p.peek().typ == tokenLBracket {
		return p.parseArraySuffix(info)
	}
	if p.peek().typ == tokenLParen {
		p.skipParenGroup()
	}

	return info, nil
}

func (p *typeParser) parseArraySuffix(child dukdb.TypeInfo) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLBracket, "array"); err != nil {
		return nil, err
	}

	if p.peek().typ == tokenRBracket {
		p.advance()
		return dukdb.NewListInfo(child)
	}

	sizeTok, err := p.expect(tokenNumber, "array size")
	if err != nil {
		return nil, err
	}
	size, err := strconv.Atoi(sizeTok.value)
	if err != nil {
		return nil, fmt.Errorf("invalid array size %q", sizeTok.value)
	}
	if _, err := p.expect(tokenRBracket, "array"); err != nil {
		return nil, err
	}
	return dukdb.NewArrayInfo(child, uint64(size))
}

func (p *typeParser) skipParenGroup() {
	if p.peek().typ != tokenLParen {
		return
	}
	depth := 0
	for {
		tok := p.advance()
		if tok.typ == tokenLParen {
			depth++
		} else if tok.typ == tokenRParen {
			depth--
			if depth == 0 {
				return
			}
		} else if tok.typ == tokenEOF {
			return
		}
	}
}

func (p *typeParser) peek() typeToken {
	if p.pos >= len(p.tokens) {
		return typeToken{typ: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *typeParser) advance() typeToken {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *typeParser) expect(typ tokenType, context string) (typeToken, error) {
	tok := p.peek()
	if tok.typ != typ {
		return typeToken{}, fmt.Errorf(
			"expected %s token in %s",
			tokenName(typ),
			context,
		)
	}
	p.advance()
	return tok, nil
}

func tokenName(typ tokenType) string {
	switch typ {
	case tokenIdent:
		return "identifier"
	case tokenNumber:
		return "number"
	case tokenString:
		return "string"
	case tokenComma:
		return "comma"
	case tokenLParen:
		return "left parenthesis"
	case tokenRParen:
		return "right parenthesis"
	case tokenLBracket:
		return "left bracket"
	case tokenRBracket:
		return "right bracket"
	case tokenColon:
		return "colon"
	case tokenEOF:
		return "end of input"
	default:
		return "token"
	}
}

func parseTypeName(name string) dukdb.Type {
	switch strings.ToUpper(name) {
	case "BOOLEAN", "BOOL":
		return dukdb.TYPE_BOOLEAN
	case "TINYINT":
		return dukdb.TYPE_TINYINT
	case "SMALLINT":
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
	case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR":
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
	case "UUID":
		return dukdb.TYPE_UUID
	case "HUGEINT":
		return dukdb.TYPE_HUGEINT
	case "UHUGEINT":
		return dukdb.TYPE_UHUGEINT
	case "BIGNUM":
		return dukdb.TYPE_BIGNUM
	case "JSON":
		return dukdb.TYPE_JSON
	case "GEOMETRY":
		return dukdb.TYPE_GEOMETRY
	case "VARIANT":
		return dukdb.TYPE_VARIANT
	case "LAMBDA":
		return dukdb.TYPE_LAMBDA
	case "DECIMAL", "NUMERIC":
		return dukdb.TYPE_DECIMAL
	default:
		return dukdb.TYPE_VARCHAR
	}
}
