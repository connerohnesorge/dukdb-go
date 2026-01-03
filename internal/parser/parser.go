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


type parser struct {
	input    string
	pos      int
	tokens   []token
	tokPos   int
	tokenErr error // Error encountered during tokenization
}

func newParser(input string) *parser {
	p := &parser{input: input}
	p.tokenize()

	return p
}



func (p *parser) parse() (Statement, error) {
	// Check for tokenization errors first
	if p.tokenErr != nil {
		return nil, p.tokenErr
	}

	if p.current().typ == tokenEOF {
		return nil, p.errorf("empty query")
	}

	var stmt Statement
	var err error

	switch {
	case p.isKeyword("WITH"):
		stmt, err = p.parseWithSelect()
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
	case p.isKeyword("ALTER"):
		p.advance()
		stmt, err = p.parseAlterTable()
	case p.isKeyword("BEGIN"):
		stmt, err = p.parseBegin()
	case p.isKeyword("COMMIT"):
		stmt = &CommitStmt{}
		p.advance()
	case p.isKeyword("ROLLBACK"):
		stmt = &RollbackStmt{}
		p.advance()
	case p.isKeyword("COPY"):
		stmt, err = p.parseCopy()
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

func (p *parser) parseWithSelect() (*SelectStmt, error) {
	if err := p.expectKeyword("WITH"); err != nil {
		return nil, err
	}

	var ctes []CTE

	// Parse CTEs
	for {
		// Parse CTE name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected CTE name")
		}
		cteName := p.advance().value

		cte := CTE{Name: cteName}

		// Optional column list: WITH cte_name(col1, col2) AS (...)
		if p.current().typ == tokenLParen {
			p.advance()
			for {
				if p.current().typ != tokenIdent {
					return nil, p.errorf("expected column name in CTE")
				}
				cte.Columns = append(cte.Columns, p.advance().value)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
		}

		// AS keyword
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}

		// CTE query in parentheses
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}

		// Parse the CTE query (must be a SELECT)
		if !p.isKeyword("SELECT") && !p.isKeyword("WITH") {
			return nil, p.errorf("expected SELECT in CTE")
		}

		var cteQuery *SelectStmt
		var err error
		if p.isKeyword("WITH") {
			// Nested CTE
			cteQuery, err = p.parseWithSelect()
		} else {
			cteQuery, err = p.parseSelect()
		}
		if err != nil {
			return nil, err
		}
		cte.Query = cteQuery

		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}

		ctes = append(ctes, cte)

		// Check for more CTEs (comma-separated)
		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	// Now parse the main SELECT
	if !p.isKeyword("SELECT") {
		return nil, p.errorf("expected SELECT after CTEs")
	}

	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	// Attach CTEs to the statement
	stmt.CTEs = ctes

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

	// Check for set operations (UNION, INTERSECT, EXCEPT)
	if p.isKeyword("UNION") || p.isKeyword("INTERSECT") || p.isKeyword("EXCEPT") {
		var setOp SetOpType
		switch {
		case p.isKeyword("UNION"):
			p.advance()
			if p.isKeyword("ALL") {
				p.advance()
				setOp = SetOpUnionAll
			} else {
				setOp = SetOpUnion
			}
		case p.isKeyword("INTERSECT"):
			p.advance()
			if p.isKeyword("ALL") {
				p.advance()
				setOp = SetOpIntersectAll
			} else {
				setOp = SetOpIntersect
			}
		case p.isKeyword("EXCEPT"):
			p.advance()
			if p.isKeyword("ALL") {
				p.advance()
				setOp = SetOpExceptAll
			} else {
				setOp = SetOpExcept
			}
		}

		// Parse the right side SELECT
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.SetOp = setOp
		stmt.Right = right
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
		name := p.advance().value

		// Check if this is a table function call (identifier followed by left paren)
		if p.current().typ == tokenLParen {
			// This is a table function call like read_csv('file.csv')
			tableFunc, err := p.parseTableFunction(name)
			if err != nil {
				return ref, err
			}
			ref.TableFunction = tableFunc
			ref.TableName = name // Use function name as table name for alias resolution
		} else if p.current().typ == tokenDot {
			p.advance()
			ref.Schema = name
			if p.current().typ != tokenIdent {
				return ref, p.errorf("expected table name after dot")
			}
			ref.TableName = p.advance().value
		} else {
			ref.TableName = name
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
		!p.isKeyword("ON") && !p.isKeyword("HAVING") &&
		!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") {
		ref.Alias = p.advance().value
	}

	return ref, nil
}

// parseTableFunction parses a table function call in a FROM clause.
// The function name has already been consumed.
// Example: read_csv('file.csv', delimiter=',', header=true)
func (p *parser) parseTableFunction(name string) (*TableFunctionRef, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	tableFunc := &TableFunctionRef{
		Name:      strings.ToLower(name), // Normalize to lowercase
		Args:      make([]Expr, 0),
		NamedArgs: make(map[string]Expr),
	}

	// Parse arguments
	if p.current().typ != tokenRParen {
		for {
			// Check if this is a named argument (identifier = value)
			if p.current().typ == tokenIdent && p.peek().typ == tokenOperator && p.peek().value == "=" {
				// Named argument
				argName := strings.ToLower(p.advance().value)
				p.advance() // consume '='
				argValue, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				tableFunc.NamedArgs[argName] = argValue
			} else {
				// Positional argument
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				tableFunc.Args = append(tableFunc.Args, arg)
			}

			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return tableFunc, nil
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

	// Optional alias (UPDATE users u SET ...)
	// Skip alias if present - it's an identifier that is NOT the SET keyword
	if p.current().typ == tokenIdent && !p.isKeyword("SET") {
		p.advance() // Skip the alias
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

	// FROM (UPDATE...FROM syntax)
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

	// Check for UNIQUE INDEX (special case)
	if p.isKeyword("UNIQUE") {
		return p.parseCreateIndex()
	}

	// Dispatch based on object type
	if p.isKeyword("TABLE") {
		return p.parseCreateTable()
	} else if p.isKeyword("VIEW") {
		return p.parseCreateView()
	} else if p.isKeyword("INDEX") {
		return p.parseCreateIndex()
	} else if p.isKeyword("SEQUENCE") {
		return p.parseCreateSequence()
	} else if p.isKeyword("SCHEMA") {
		return p.parseCreateSchema()
	}

	return nil, p.errorf(
		"expected TABLE, VIEW, INDEX, SEQUENCE, or SCHEMA after CREATE",
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

	// Check for AS SELECT (CREATE TABLE ... AS SELECT ...)
	if p.isKeyword("AS") {
		p.advance()
		if !p.isKeyword("SELECT") && !p.isKeyword("WITH") {
			return nil, p.errorf("expected SELECT or WITH after AS")
		}
		var selectStmt *SelectStmt
		var err error
		if p.isKeyword("WITH") {
			selectStmt, err = p.parseWithSelect()
		} else {
			selectStmt, err = p.parseSelect()
		}
		if err != nil {
			return nil, err
		}
		stmt.AsSelect = selectStmt

		return stmt, nil
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

	// Dispatch based on object type
	if p.isKeyword("TABLE") {
		return p.parseDropTable()
	} else if p.isKeyword("VIEW") {
		return p.parseDropView()
	} else if p.isKeyword("INDEX") {
		return p.parseDropIndex()
	} else if p.isKeyword("SEQUENCE") {
		return p.parseDropSequence()
	} else if p.isKeyword("SCHEMA") {
		return p.parseDropSchema()
	}

	return nil, p.errorf(
		"expected TABLE, VIEW, INDEX, SEQUENCE, or SCHEMA after DROP",
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

func (p *parser) parseBegin() (*BeginStmt, error) {
	p.advance() // consume BEGIN
	if p.isKeyword("TRANSACTION") {
		p.advance() // consume TRANSACTION (optional)
	}

	return &BeginStmt{}, nil
}

// parseCopy parses a COPY statement.
// Supports:
//   - COPY table FROM 'path' (OPTIONS)
//   - COPY table TO 'path' (OPTIONS)
//   - COPY table (col1, col2) FROM 'path' (OPTIONS)
//   - COPY table (col1, col2) TO 'path' (OPTIONS)
//   - COPY (SELECT...) TO 'path' (OPTIONS)
func (p *parser) parseCopy() (*CopyStmt, error) {
	if err := p.expectKeyword("COPY"); err != nil {
		return nil, err
	}

	stmt := &CopyStmt{
		Options: make(map[string]any),
	}

	// Check for COPY (SELECT...) TO syntax
	if p.current().typ == tokenLParen {
		p.advance() // consume '('
		if !p.isKeyword("SELECT") && !p.isKeyword("WITH") {
			return nil, p.errorf("expected SELECT after COPY (")
		}

		// Parse the SELECT query
		var query *SelectStmt
		var err error
		if p.isKeyword("WITH") {
			query, err = p.parseWithSelect()
		} else {
			query, err = p.parseSelect()
		}
		if err != nil {
			return nil, err
		}
		stmt.Query = query

		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}

		// Must be TO for query export
		if !p.isKeyword("TO") {
			return nil, p.errorf("expected TO after COPY (SELECT...)")
		}
		stmt.IsFrom = false
		p.advance() // consume TO
	} else {
		// Parse table name (possibly with schema)
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected table name after COPY")
		}
		name := p.advance().value

		// Check for schema.table
		if p.current().typ == tokenDot {
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected table name after dot")
			}
			stmt.Schema = name
			stmt.TableName = p.advance().value
		} else {
			stmt.TableName = name
		}

		// Optional column list
		if p.current().typ == tokenLParen {
			p.advance() // consume '('
			for {
				if p.current().typ != tokenIdent {
					return nil, p.errorf("expected column name")
				}
				stmt.Columns = append(stmt.Columns, p.advance().value)
				if p.current().typ != tokenComma {
					break
				}
				p.advance() // consume ','
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
		}

		// FROM or TO
		if p.isKeyword("FROM") {
			stmt.IsFrom = true
			p.advance()
		} else if p.isKeyword("TO") {
			stmt.IsFrom = false
			p.advance()
		} else {
			return nil, p.errorf("expected FROM or TO after table name")
		}
	}

	// Parse file path (string literal)
	if p.current().typ != tokenString {
		return nil, p.errorf("expected file path string")
	}
	pathTok := p.advance()
	// Remove quotes from the path
	stmt.FilePath = pathTok.value[1 : len(pathTok.value)-1]
	stmt.FilePath = strings.ReplaceAll(stmt.FilePath, "''", "'")

	// Parse optional options clause
	if p.current().typ == tokenLParen || p.isKeyword("WITH") {
		if p.isKeyword("WITH") {
			p.advance() // consume WITH
		}
		if err := p.parseCopyOptions(stmt); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

// parseCopyOptions parses the options clause for a COPY statement.
// Options are in the form (name value, name value, ...) or (name, name, ...)
// Supported options: DELIMITER, HEADER, FORMAT, NULL, CODEC, COMPRESSION,
// QUOTE, ESCAPE, ENCODING, SKIP, FORCE_QUOTE, FORCE_NOT_NULL, etc.
func (p *parser) parseCopyOptions(stmt *CopyStmt) error {
	if _, err := p.expect(tokenLParen); err != nil {
		return err
	}

	for p.current().typ != tokenRParen && p.current().typ != tokenEOF {
		// Parse option name
		if p.current().typ != tokenIdent {
			return p.errorf("expected option name")
		}
		optName := strings.ToUpper(p.advance().value)

		// Parse option value
		var optValue any = true // Default for boolean options

		// Check if there's a value (could be '=' or just the value)
		if p.current().typ == tokenOperator && p.current().value == "=" {
			p.advance() // consume '='
		}

		// Parse the value if present
		switch p.current().typ {
		case tokenString:
			tok := p.advance()
			s := tok.value[1 : len(tok.value)-1]
			s = strings.ReplaceAll(s, "''", "'")
			optValue = s
		case tokenNumber:
			tok := p.advance()
			if strings.Contains(tok.value, ".") {
				f, _ := strconv.ParseFloat(tok.value, 64)
				optValue = f
			} else {
				i, _ := strconv.ParseInt(tok.value, 10, 64)
				optValue = i
			}
		case tokenIdent:
			// Could be boolean (true/false) or identifier value (FORMAT PARQUET)
			val := p.current().value
			switch strings.ToUpper(val) {
			case "TRUE":
				optValue = true
				p.advance()
			case "FALSE":
				optValue = false
				p.advance()
			case "CSV", "PARQUET", "JSON", "NDJSON":
				// Format value
				optValue = strings.ToUpper(val)
				p.advance()
			case "GZIP", "ZSTD", "SNAPPY", "LZ4", "LZ4_RAW", "BROTLI", "UNCOMPRESSED", "NONE":
				// Compression/codec value
				optValue = strings.ToUpper(val)
				p.advance()
			case "UTF8", "UTF16", "LATIN1", "ASCII":
				// Encoding value
				optValue = strings.ToUpper(val)
				p.advance()
			default:
				// Check if it's an option that takes a column list
				if optName == "FORCE_QUOTE" || optName == "FORCE_NOT_NULL" || optName == "COLUMNS" {
					// Parse column list
					if p.current().typ == tokenLParen {
						p.advance()
						cols := make([]string, 0)
						for {
							if p.current().typ != tokenIdent {
								return p.errorf("expected column name in %s", optName)
							}
							cols = append(cols, p.advance().value)
							if p.current().typ != tokenComma {
								break
							}
							p.advance()
						}
						if _, err := p.expect(tokenRParen); err != nil {
							return err
						}
						optValue = cols
					} else if p.current().typ == tokenIdent {
						// Single column without parens
						optValue = []string{p.advance().value}
					}
				} else {
					// Treat as string value for unknown options
					optValue = val
					p.advance()
				}
			}
		case tokenLParen:
			// Column list for options like FORCE_QUOTE (col1, col2)
			p.advance()
			cols := make([]string, 0)
			for {
				if p.current().typ != tokenIdent {
					return p.errorf("expected column name")
				}
				cols = append(cols, p.advance().value)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return err
			}
			optValue = cols
		}

		// Normalize option names
		switch optName {
		case "DELIM", "SEP", "SEPARATOR":
			optName = "DELIMITER"
		case "NULL", "NULLSTR":
			optName = "NULL"
		case "COMPRESSION_LEVEL":
			optName = "COMPRESSION_LEVEL"
		case "ROW_GROUP_SIZE":
			optName = "ROW_GROUP_SIZE"
		}

		stmt.Options[optName] = optValue

		// Consume comma if present
		if p.current().typ == tokenComma {
			p.advance()
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return err
	}

	return nil
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
			// Special handling: if the next token is a numeric literal,
			// parse it as a negative literal to avoid overflow issues
			// (e.g., -2147483648 which doesn't fit as positive int32)
			if p.current().typ == tokenNumber {
				tok := p.advance()
				numStr := tok.value
				// Parse as negative number
				if strings.Contains(
					numStr,
					".",
				) ||
					strings.ContainsAny(numStr, "eE") {
					f, err := strconv.ParseFloat(
						numStr,
						64,
					)
					if err != nil {
						return nil, p.errorf(
							"invalid number: %s",
							numStr,
						)
					}

					return &Literal{
						Value: -f,
						Type:  dukdb.TYPE_DOUBLE,
					}, nil
				}
				i, err := strconv.ParseInt(
					numStr,
					10,
					64,
				)
				if err != nil {
					return nil, p.errorf(
						"invalid number: %s",
						numStr,
					)
				}

				return &Literal{
					Value: -i,
					Type:  dukdb.TYPE_BIGINT,
				}, nil
			}
			// Non-literal expression, use UnaryExpr
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
			// Similar handling for unary plus with literals
			if p.current().typ == tokenNumber {
				tok := p.advance()

				return p.parseNumber(tok.value)
			}
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
	case "EXTRACT":
		return p.parseExtract()
	case "INTERVAL":
		return p.parseInterval()
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

		// Check for window function OVER clause after COUNT(*)
		return p.maybeParseWindowExpr(fn)
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

	// Check for window function OVER clause
	return p.maybeParseWindowExpr(fn)
}

// maybeParseWindowExpr checks for IGNORE/RESPECT NULLS, FILTER, and OVER clauses
// after a function call and wraps it in a WindowExpr if OVER is found.
func (p *parser) maybeParseWindowExpr(fn *FunctionCall) (Expr, error) {
	windowExpr := &WindowExpr{
		Function: fn,
		Distinct: fn.Distinct, // Carry over DISTINCT from function call
	}

	// Check for IGNORE NULLS or RESPECT NULLS (before OVER)
	if p.isKeyword("IGNORE") {
		p.advance()
		if err := p.expectKeyword("NULLS"); err != nil {
			return nil, err
		}
		windowExpr.IgnoreNulls = true
	} else if p.isKeyword("RESPECT") {
		p.advance()
		if err := p.expectKeyword("NULLS"); err != nil {
			return nil, err
		}
		// RESPECT NULLS is the default, so we don't set IgnoreNulls
	}

	// Check for FILTER clause (before OVER)
	if p.isKeyword("FILTER") {
		p.advance()
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("WHERE"); err != nil {
			return nil, err
		}
		filterExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		windowExpr.Filter = filterExpr
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	// Check for OVER clause
	if !p.isKeyword("OVER") {
		// No OVER clause - if we had IGNORE NULLS or FILTER without OVER, it's an error
		if windowExpr.IgnoreNulls || windowExpr.Filter != nil {
			return nil, p.errorf("IGNORE NULLS and FILTER require OVER clause")
		}
		return fn, nil
	}

	// Parse OVER clause
	p.advance() // consume OVER

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse window specification
	if err := p.parseWindowSpec(windowExpr); err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return windowExpr, nil
}

// parseWindowSpec parses the contents of OVER (...).
func (p *parser) parseWindowSpec(windowExpr *WindowExpr) error {
	// Parse PARTITION BY (optional)
	if p.isKeyword("PARTITION") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		partitionBy, err := p.parseWindowExprList()
		if err != nil {
			return err
		}
		windowExpr.PartitionBy = partitionBy
	}

	// Parse ORDER BY (optional)
	if p.isKeyword("ORDER") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		orderBy, err := p.parseWindowOrderBy()
		if err != nil {
			return err
		}
		windowExpr.OrderBy = orderBy
	}

	// Parse frame specification (optional): ROWS/RANGE/GROUPS
	if p.isKeyword("ROWS") || p.isKeyword("RANGE") || p.isKeyword("GROUPS") {
		frame, err := p.parseFrameSpec()
		if err != nil {
			return err
		}
		windowExpr.Frame = frame
	}

	return nil
}

// parseWindowExprList parses a comma-separated list of expressions for PARTITION BY.
// It stops when it encounters ORDER, ROWS, RANGE, GROUPS, or closing paren.
func (p *parser) parseWindowExprList() ([]Expr, error) {
	var exprs []Expr

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		// Check for comma to continue
		if p.current().typ != tokenComma {
			break
		}

		// Look ahead to see if next token is a window keyword (ORDER, ROWS, etc.)
		// If so, stop parsing the expression list
		next := p.peek()
		if next.typ == tokenIdent {
			upperVal := strings.ToUpper(next.value)
			if upperVal == "ORDER" || upperVal == "ROWS" || upperVal == "RANGE" ||
				upperVal == "GROUPS" {
				break
			}
		}
		p.advance() // consume comma
	}

	return exprs, nil
}

// parseWindowOrderBy parses ORDER BY expressions within a window specification.
// Each expression can have ASC/DESC and NULLS FIRST/LAST.
func (p *parser) parseWindowOrderBy() ([]WindowOrderBy, error) {
	var orderBy []WindowOrderBy

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		order := WindowOrderBy{Expr: expr}

		// Check for ASC/DESC
		if p.isKeyword("DESC") {
			p.advance()
			order.Desc = true
		} else if p.isKeyword("ASC") {
			p.advance()
		}

		// Check for NULLS FIRST/LAST
		if p.isKeyword("NULLS") {
			p.advance()
			if p.isKeyword("FIRST") {
				p.advance()
				order.NullsFirst = true
			} else if p.isKeyword("LAST") {
				p.advance()
				order.NullsFirst = false
			} else {
				return nil, p.errorf("expected FIRST or LAST after NULLS")
			}
		}

		orderBy = append(orderBy, order)

		// Check for comma to continue
		if p.current().typ != tokenComma {
			break
		}

		// Look ahead to see if next token is a frame keyword
		next := p.peek()
		if next.typ == tokenIdent {
			upperVal := strings.ToUpper(next.value)
			if upperVal == "ROWS" || upperVal == "RANGE" || upperVal == "GROUPS" {
				break
			}
		}
		p.advance() // consume comma
	}

	return orderBy, nil
}

// parseFrameSpec parses ROWS/RANGE/GROUPS frame specification.
func (p *parser) parseFrameSpec() (*WindowFrame, error) {
	frame := &WindowFrame{}

	// Determine frame type
	switch {
	case p.isKeyword("ROWS"):
		p.advance()
		frame.Type = FrameTypeRows
	case p.isKeyword("RANGE"):
		p.advance()
		frame.Type = FrameTypeRange
	case p.isKeyword("GROUPS"):
		p.advance()
		frame.Type = FrameTypeGroups
	default:
		return nil, p.errorf("expected ROWS, RANGE, or GROUPS")
	}

	// Check for BETWEEN ... AND ... or single bound
	if p.isKeyword("BETWEEN") {
		p.advance()

		// Parse start bound
		start, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start = start

		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}

		// Parse end bound
		end, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.End = end
	} else {
		// Single bound shorthand: ROWS 3 PRECEDING means ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
		bound, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start = bound
		frame.End = WindowBound{Type: BoundCurrentRow}
	}

	// Parse EXCLUDE clause (optional)
	if p.isKeyword("EXCLUDE") {
		p.advance()

		switch {
		case p.isKeyword("NO"):
			p.advance()
			if err := p.expectKeyword("OTHERS"); err != nil {
				return nil, err
			}
			frame.Exclude = ExcludeNoOthers
		case p.isKeyword("CURRENT"):
			p.advance()
			if err := p.expectKeyword("ROW"); err != nil {
				return nil, err
			}
			frame.Exclude = ExcludeCurrentRow
		case p.isKeyword("GROUP"):
			p.advance()
			frame.Exclude = ExcludeGroup
		case p.isKeyword("TIES"):
			p.advance()
			frame.Exclude = ExcludeTies
		default:
			return nil, p.errorf("expected NO OTHERS, CURRENT ROW, GROUP, or TIES after EXCLUDE")
		}
	}

	return frame, nil
}

// parseFrameBound parses a single frame boundary.
func (p *parser) parseFrameBound() (WindowBound, error) {
	bound := WindowBound{}

	switch {
	case p.isKeyword("UNBOUNDED"):
		p.advance()
		if p.isKeyword("PRECEDING") {
			p.advance()
			bound.Type = BoundUnboundedPreceding
		} else if p.isKeyword("FOLLOWING") {
			p.advance()
			bound.Type = BoundUnboundedFollowing
		} else {
			return bound, p.errorf("expected PRECEDING or FOLLOWING after UNBOUNDED")
		}

	case p.isKeyword("CURRENT"):
		p.advance()
		if err := p.expectKeyword("ROW"); err != nil {
			return bound, err
		}
		bound.Type = BoundCurrentRow

	default:
		// N PRECEDING or N FOLLOWING - parse an expression for the offset
		expr, err := p.parseUnaryExpr() // Use unaryExpr to avoid consuming PRECEDING/FOLLOWING as expression
		if err != nil {
			return bound, err
		}
		bound.Offset = expr

		if p.isKeyword("PRECEDING") {
			p.advance()
			bound.Type = BoundPreceding
		} else if p.isKeyword("FOLLOWING") {
			p.advance()
			bound.Type = BoundFollowing
		} else {
			return bound, p.errorf("expected PRECEDING or FOLLOWING after offset expression")
		}
	}

	return bound, nil
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

// parseExtract parses an EXTRACT(part FROM source) expression.
// This is SQL standard syntax for extracting date/time fields.
// Valid parts: YEAR, QUARTER, MONTH, WEEK, DAY, DAYOFWEEK, DOW, DAYOFYEAR, DOY,
// HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, EPOCH
func (p *parser) parseExtract() (Expr, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse the part specifier (must be an identifier)
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected date part in EXTRACT, got %s", p.current().value)
	}
	part := strings.ToUpper(p.advance().value)

	// Validate the part specifier
	validParts := map[string]bool{
		"YEAR": true, "QUARTER": true, "MONTH": true, "WEEK": true,
		"DAY": true, "DAYOFWEEK": true, "DOW": true, "DAYOFYEAR": true, "DOY": true,
		"HOUR": true, "MINUTE": true, "SECOND": true,
		"MILLISECOND": true, "MICROSECOND": true, "EPOCH": true,
	}
	if !validParts[part] {
		return nil, p.errorf("invalid date part in EXTRACT: %s", part)
	}

	// Expect FROM keyword
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	// Parse the source expression
	source, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &ExtractExpr{
		Part:   part,
		Source: source,
	}, nil
}

// parseInterval parses an INTERVAL literal.
// Supports the following syntaxes:
//   - INTERVAL 'n' UNIT (e.g., INTERVAL '5' DAY)
//   - INTERVAL 'n unit' (e.g., INTERVAL '5 days')
//   - INTERVAL 'n units m units' (e.g., INTERVAL '2 hours 30 minutes')
func (p *parser) parseInterval() (Expr, error) {
	// We've already consumed 'INTERVAL' keyword

	// Expect a string literal containing the interval value
	if p.current().typ != tokenString {
		return nil, p.errorf("expected string after INTERVAL, got %s", p.current().value)
	}

	// Remove quotes from the string
	stringTok := p.advance()
	intervalStr := stringTok.value[1 : len(stringTok.value)-1]
	intervalStr = strings.ReplaceAll(intervalStr, "''", "'")

	// Check if there's a unit keyword following the string (INTERVAL '5' DAY syntax)
	var unitKeyword string
	if p.current().typ == tokenIdent {
		upper := strings.ToUpper(p.current().value)
		if isIntervalUnit(upper) {
			unitKeyword = upper
			p.advance()
		}
	}

	// Parse the interval
	months, days, micros, err := parseIntervalValue(intervalStr, unitKeyword)
	if err != nil {
		return nil, p.errorf("invalid interval: %v", err)
	}

	return &IntervalLiteral{
		Months: months,
		Days:   days,
		Micros: micros,
	}, nil
}

// isIntervalUnit checks if a string is a valid interval unit keyword.
func isIntervalUnit(s string) bool {
	switch s {
	case "YEAR", "YEARS", "MONTH", "MONTHS", "WEEK", "WEEKS",
		"DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES",
		"SECOND", "SECONDS", "MILLISECOND", "MILLISECONDS",
		"MICROSECOND", "MICROSECONDS":
		return true
	}
	return false
}

// parseIntervalValue parses an interval string value.
// If unitKeyword is provided, it's the INTERVAL '5' DAY syntax.
// Otherwise, parse 'n unit' or 'n units m units' from the string itself.
func parseIntervalValue(s string, unitKeyword string) (months int32, days int32, micros int64, err error) {
	s = strings.TrimSpace(s)

	// If unitKeyword is provided, parse as simple number + unit
	if unitKeyword != "" {
		val, parseErr := strconv.ParseInt(s, 10, 64)
		if parseErr != nil {
			// Try parsing as float for fractional values
			fval, fErr := strconv.ParseFloat(s, 64)
			if fErr != nil {
				return 0, 0, 0, fmt.Errorf("invalid interval number: %s", s)
			}
			return applyIntervalUnit(fval, unitKeyword)
		}
		return applyIntervalUnit(float64(val), unitKeyword)
	}

	// Parse compound interval string (e.g., "2 hours 30 minutes")
	tokens := tokenizeIntervalString(s)
	if len(tokens) == 0 {
		return 0, 0, 0, fmt.Errorf("empty interval string")
	}

	// Process tokens in pairs: number unit number unit ...
	i := 0
	for i < len(tokens) {
		// Expect a number
		val, parseErr := strconv.ParseFloat(tokens[i], 64)
		if parseErr != nil {
			return 0, 0, 0, fmt.Errorf("expected number in interval, got: %s", tokens[i])
		}
		i++

		if i >= len(tokens) {
			return 0, 0, 0, fmt.Errorf("expected unit after number in interval")
		}

		// Expect a unit
		unit := strings.ToUpper(tokens[i])
		if !isIntervalUnit(unit) {
			return 0, 0, 0, fmt.Errorf("unknown interval unit: %s", tokens[i])
		}
		i++

		// Apply this component
		m, d, u, applyErr := applyIntervalUnit(val, unit)
		if applyErr != nil {
			return 0, 0, 0, applyErr
		}
		months += m
		days += d
		micros += u
	}

	return months, days, micros, nil
}

// tokenizeIntervalString splits an interval string into tokens (numbers and units).
func tokenizeIntervalString(s string) []string {
	var tokens []string
	var current strings.Builder

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else if (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+' {
			// Part of a number
			if current.Len() > 0 {
				// Check if we're transitioning from letters to digits
				lastCh := current.String()[current.Len()-1]
				if (lastCh >= 'a' && lastCh <= 'z') || (lastCh >= 'A' && lastCh <= 'Z') {
					tokens = append(tokens, current.String())
					current.Reset()
				}
			}
			current.WriteByte(ch)
		} else if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			// Part of a unit name
			if current.Len() > 0 {
				// Check if we're transitioning from digits to letters
				lastCh := current.String()[current.Len()-1]
				if (lastCh >= '0' && lastCh <= '9') || lastCh == '.' {
					tokens = append(tokens, current.String())
					current.Reset()
				}
			}
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// applyIntervalUnit converts a value with a unit to months, days, and microseconds.
func applyIntervalUnit(val float64, unit string) (months int32, days int32, micros int64, err error) {
	switch strings.ToUpper(unit) {
	case "YEAR", "YEARS":
		months = int32(val * 12)
	case "MONTH", "MONTHS":
		months = int32(val)
	case "WEEK", "WEEKS":
		days = int32(val * 7)
	case "DAY", "DAYS":
		days = int32(val)
	case "HOUR", "HOURS":
		micros = int64(val * 60 * 60 * 1_000_000)
	case "MINUTE", "MINUTES":
		micros = int64(val * 60 * 1_000_000)
	case "SECOND", "SECONDS":
		micros = int64(val * 1_000_000)
	case "MILLISECOND", "MILLISECONDS":
		micros = int64(val * 1_000)
	case "MICROSECOND", "MICROSECONDS":
		micros = int64(val)
	default:
		return 0, 0, 0, fmt.Errorf("unknown interval unit: %s", unit)
	}
	return months, days, micros, nil
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
