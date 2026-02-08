package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dukdb/dukdb-go"
)

type typeParserFunc func(p *typeParser, name string) (dukdb.TypeInfo, error)

// TypeSystem provides parsing for SQL type expressions.
type TypeSystem struct {
	parsers map[string]typeParserFunc
}

// NewTypeSystem creates a new type system with no registrations.
func NewTypeSystem() *TypeSystem {
	return &TypeSystem{
		parsers: make(map[string]typeParserFunc),
	}
}

// RegisterType registers a parser for the specified type name.
func (ts *TypeSystem) RegisterType(
	name string,
	parser typeParserFunc,
) {
	ts.parsers[strings.ToUpper(name)] = parser
}

// ParseTypeExpression parses a SQL type expression into a TypeInfo.
func (ts *TypeSystem) ParseTypeExpression(
	expr string,
) (dukdb.TypeInfo, error) {
	parser := newTypeParser(expr)
	parser.system = ts
	info, err := parser.parseType(ts)
	if err != nil {
		return nil, err
	}
	if parser.peek().typ != tokenEOF {
		return nil, fmt.Errorf(
			"unexpected token %q in type expression",
			parser.peek().value,
		)
	}
	return info, nil
}

var defaultTypeSystem = func() *TypeSystem {
	ts := NewTypeSystem()
	registerBuiltinTypes(ts)
	return ts
}()

// ParseTypeExpression parses a SQL type expression using the default type system.
func ParseTypeExpression(expr string) (dukdb.TypeInfo, error) {
	return defaultTypeSystem.ParseTypeExpression(expr)
}

func registerBuiltinTypes(ts *TypeSystem) {
	ts.RegisterType("STRUCT", parseStructType)
	ts.RegisterType("MAP", parseMapType)
	ts.RegisterType("UNION", parseUnionType)
	ts.RegisterType("LIST", parseListType)
	ts.RegisterType("ARRAY", parseArrayType)
	ts.RegisterType("DECIMAL", parseDecimalType)
	ts.RegisterType("NUMERIC", parseDecimalType)
	ts.RegisterType("ENUM", parseEnumType)
	ts.RegisterType("JSON", parseJSONType)
	ts.RegisterType("BIGNUM", parseBignumType)
	ts.RegisterType("GEOMETRY", parseGeometryType)
	ts.RegisterType("LAMBDA", parseLambdaType)
	ts.RegisterType("VARIANT", parseVariantType)
}

func parseJSONType(
	_ *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	return dukdb.NewJSONInfo()
}

func parseBignumType(
	_ *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	return dukdb.NewBignumInfo()
}

func parseGeometryType(
	_ *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	return dukdb.NewGeometryInfo()
}

func parseLambdaType(
	_ *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	return dukdb.NewLambdaInfo()
}

func parseVariantType(
	_ *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	return dukdb.NewVariantInfo()
}

func parseDecimalType(
	p *typeParser,
	name string,
) (dukdb.TypeInfo, error) {
	if p.peek().typ != tokenLParen {
		// Default to DECIMAL(18,3) when no precision/scale specified
		return dukdb.NewDecimalInfo(18, 3)
	}

	p.advance()
	widthTok, err := p.expect(tokenNumber, "decimal precision")
	if err != nil {
		return nil, err
	}
	width, err := strconv.Atoi(widthTok.value)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid decimal precision %q",
			widthTok.value,
		)
	}

	scale := 0
	if p.peek().typ == tokenComma {
		p.advance()
		scaleTok, err := p.expect(tokenNumber, "decimal scale")
		if err != nil {
			return nil, err
		}
		scale, err = strconv.Atoi(scaleTok.value)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid decimal scale %q",
				scaleTok.value,
			)
		}
	}

	if _, err := p.expect(tokenRParen, "decimal type"); err != nil {
		return nil, err
	}

	return dukdb.NewDecimalInfo(uint8(width), uint8(scale))
}

func parseEnumType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if p.peek().typ != tokenLParen {
		return nil, fmt.Errorf("ENUM requires values")
	}
	p.advance()

	values := []string{}
	for {
		tok := p.peek()
		if tok.typ != tokenString && tok.typ != tokenIdent {
			return nil, fmt.Errorf("expected enum value")
		}
		p.advance()
		values = append(values, tok.value)

		if p.peek().typ == tokenComma {
			p.advance()
			continue
		}
		break
	}

	if _, err := p.expect(tokenRParen, "enum type"); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("ENUM requires at least one value")
	}
	return dukdb.NewEnumInfo(values[0], values[1:]...)
}

func parseListType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLParen, "list type"); err != nil {
		return nil, err
	}
	child, err := p.parseType(p.system)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenRParen, "list type"); err != nil {
		return nil, err
	}
	return dukdb.NewListInfo(child)
}

func parseArrayType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLParen, "array type"); err != nil {
		return nil, err
	}
	child, err := p.parseType(p.system)
	if err != nil {
		return nil, err
	}

	if p.peek().typ == tokenComma {
		p.advance()
		sizeTok, err := p.expect(tokenNumber, "array size")
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(sizeTok.value)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid array size %q",
				sizeTok.value,
			)
		}
		if _, err := p.expect(tokenRParen, "array type"); err != nil {
			return nil, err
		}
		return dukdb.NewArrayInfo(child, uint64(size))
	}

	if _, err := p.expect(tokenRParen, "array type"); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("ARRAY requires a fixed size")
}

func parseStructType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLParen, "struct type"); err != nil {
		return nil, err
	}

	entries := []dukdb.StructEntry{}
	for {
		nameTok := p.peek()
		if nameTok.typ != tokenIdent && nameTok.typ != tokenString {
			return nil, fmt.Errorf("expected struct field name")
		}
		p.advance()

		if p.peek().typ == tokenColon {
			p.advance()
		}

		fieldType, err := p.parseType(p.system)
		if err != nil {
			return nil, err
		}

		entry, err := dukdb.NewStructEntry(fieldType, nameTok.value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)

		if p.peek().typ == tokenComma {
			p.advance()
			continue
		}
		break
	}

	if _, err := p.expect(tokenRParen, "struct type"); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("STRUCT must have at least one field")
	}
	return dukdb.NewStructInfo(entries[0], entries[1:]...)
}

func parseMapType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLParen, "map type"); err != nil {
		return nil, err
	}
	keyType, err := p.parseType(p.system)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenComma, "map type"); err != nil {
		return nil, err
	}
	valueType, err := p.parseType(p.system)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenRParen, "map type"); err != nil {
		return nil, err
	}
	return dukdb.NewMapInfo(keyType, valueType)
}

func parseUnionType(
	p *typeParser,
	_ string,
) (dukdb.TypeInfo, error) {
	if _, err := p.expect(tokenLParen, "union type"); err != nil {
		return nil, err
	}

	members := []dukdb.TypeInfo{}
	names := []string{}
	for {
		nameTok := p.peek()
		if nameTok.typ != tokenIdent && nameTok.typ != tokenString {
			return nil, fmt.Errorf("expected union member name")
		}
		p.advance()

		if p.peek().typ == tokenColon {
			p.advance()
		}

		memberType, err := p.parseType(p.system)
		if err != nil {
			return nil, err
		}
		names = append(names, nameTok.value)
		members = append(members, memberType)

		if p.peek().typ == tokenComma {
			p.advance()
			continue
		}
		break
	}

	if _, err := p.expect(tokenRParen, "union type"); err != nil {
		return nil, err
	}
	return dukdb.NewUnionInfo(members, names)
}
