package parser

import (
	"fmt"
	"strconv"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	internaltypes "github.com/dukdb/dukdb-go/internal/types"
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
	case p.isKeyword("TRUNCATE"):
		stmt, err = p.parseTruncate()
	case p.isKeyword("ALTER"):
		p.advance()
		stmt, err = p.parseAlter()
	case p.isKeyword("BEGIN"):
		stmt, err = p.parseBegin()
	case p.isKeyword("COMMIT"):
		stmt = &CommitStmt{}
		p.advance()
	case p.isKeyword("ROLLBACK"):
		stmt, err = p.parseRollback()
	case p.isKeyword("COPY"):
		stmt, err = p.parseCopy()
	case p.isKeyword("MERGE"):
		stmt, err = p.parseMerge()
	case p.isKeyword("PRAGMA"):
		stmt, err = p.parsePragma()
	case p.isKeyword("EXPLAIN"):
		stmt, err = p.parseExplain()
	case p.isKeyword("VACUUM"):
		stmt, err = p.parseVacuum()
	case p.isKeyword("ANALYZE"):
		stmt, err = p.parseAnalyze()
	case p.isKeyword("CHECKPOINT"):
		stmt, err = p.parseCheckpoint()
	case p.isKeyword("SAVEPOINT"):
		stmt, err = p.parseSavepoint()
	case p.isKeyword("RELEASE"):
		stmt, err = p.parseReleaseSavepoint()
	case p.isKeyword("RESET"):
		stmt, err = p.parseReset()
	case p.isKeyword("SET"):
		stmt, err = p.parseSet()
	case p.isKeyword("SHOW"):
		stmt, err = p.parseShow()
	case p.isKeyword("PREPARE"):
		stmt, err = p.parsePrepareStmt()
	case p.isKeyword("EXECUTE"):
		stmt, err = p.parseExecuteStmt()
	case p.isKeyword("DEALLOCATE"):
		stmt, err = p.parseDeallocateStmt()
	case p.isKeyword("EXPORT"):
		stmt, err = p.parseExportDatabase()
	case p.isKeyword("IMPORT"):
		stmt, err = p.parseImportDatabase()
	case p.isKeyword("INSTALL"):
		stmt, err = p.parseInstall()
	case p.isKeyword("LOAD"):
		stmt, err = p.parseLoad()
	case p.isKeyword("ATTACH"):
		stmt, err = p.parseAttach()
	case p.isKeyword("DETACH"):
		stmt, err = p.parseDetach()
	case p.isKeyword("USE"):
		stmt, err = p.parseUse()
	case p.isKeyword("COMMENT"):
		stmt, err = p.parseComment()
	case p.isKeyword("VALUES"):
		stmt, err = p.parseStandaloneValues()
	case p.isKeyword("DESCRIBE"), p.isKeyword("DESC"):
		stmt, err = p.parseDescribe()
	case p.isKeyword("SUMMARIZE"):
		stmt, err = p.parseSummarize()
	case p.isKeyword("CALL"):
		stmt, err = p.parseCall()
	default:
		tok := p.current()
		suggestion := suggestKeyword(tok.value)
		if suggestion != "" {
			return nil, p.errorAtPosition(tok.pos,
				"unexpected token %q (did you mean %s?)", tok.value, suggestion)
		}
		return nil, p.errorAtPosition(tok.pos,
			"unexpected token %q", tok.value)
	}

	if err != nil {
		return nil, err
	}

	// Skip optional semicolon
	if p.current().typ == tokenSemicolon {
		p.advance()
	}

	// Check for unconsumed tokens - this catches typos like "SELECT * FORM users"
	// where FORM is not recognized as FROM and gets left unconsumed
	if p.current().typ != tokenEOF {
		tok := p.current()
		suggestion := suggestKeyword(tok.value)
		if suggestion != "" {
			return nil, p.errorAtPosition(tok.pos,
				"unexpected token %q (did you mean %s?)", tok.value, suggestion)
		}
		return nil, p.errorAtPosition(tok.pos,
			"unexpected token %q at end of statement", tok.value)
	}

	return stmt, nil
}

func (p *parser) parseWithSelect() (*SelectStmt, error) {
	if err := p.expectKeyword("WITH"); err != nil {
		return nil, err
	}

	// Check for RECURSIVE keyword
	isRecursive := false
	if p.isKeyword("RECURSIVE") {
		p.advance()
		isRecursive = true
	}

	var ctes []CTE

	// Parse CTEs
	for {
		// Parse CTE name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected CTE name")
		}
		cteName := p.advance().value

		cte := CTE{Name: cteName, Recursive: isRecursive}

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

		// Optional USING KEY clause for recursive CTEs
		if p.isKeyword("USING") {
			p.advance()
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			if _, err := p.expect(tokenLParen); err != nil {
				return nil, err
			}
			for {
				if p.current().typ != tokenIdent {
					return nil, p.errorf("expected column name in USING KEY clause")
				}
				cte.UsingKey = append(cte.UsingKey, p.advance().value)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
		}

		// Optional recursion options after the CTE query
		option, err := p.parseRecursionOption()
		if err != nil {
			return nil, err
		}
		if option != nil {
			if cte.Query.Options != nil {
				return nil, p.errorf("duplicate recursion options")
			}
			cte.Query.Options = option
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

	// DISTINCT or DISTINCT ON (expr, ...)
	if p.isKeyword("DISTINCT") {
		p.advance()
		stmt.Distinct = true

		// Check for DISTINCT ON (expr, ...)
		if p.isKeyword("ON") {
			p.advance() // consume ON
			if _, err := p.expect(tokenLParen); err != nil {
				return nil, err
			}
			distinctOn, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			stmt.DistinctOn = distinctOn
		}
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

		// TABLESAMPLE clause (must come after table reference, before WHERE)
		if p.isKeyword("TABLESAMPLE") {
			sample, err := p.parseTablesample()
			if err != nil {
				return nil, err
			}
			stmt.Sample = sample
		}
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
		// Check for GROUP BY ALL
		if p.isKeyword("ALL") {
			// Disambiguate: if next token after ALL is comma, it's a column named "all"
			next := p.peek()
			if next.typ == tokenComma {
				// "GROUP BY all, ..." — "all" is a column name
				groupBy, err := p.parseGroupByList()
				if err != nil {
					return nil, err
				}
				stmt.GroupBy = groupBy
			} else {
				// GROUP BY ALL — auto-group
				p.advance() // consume ALL
				stmt.GroupByAll = true
			}
		} else {
			groupBy, err := p.parseGroupByList()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = groupBy
		}

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

	// QUALIFY - filter rows after window function evaluation
	if p.isKeyword("QUALIFY") {
		p.advance()
		qualify, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Qualify = qualify
	}

	// WINDOW - named window definitions
	if p.isKeyword("WINDOW") {
		p.advance()
		windowDefs, err := p.parseWindowDefs()
		if err != nil {
			return nil, err
		}
		stmt.Windows = windowDefs
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

		// PostgreSQL compatibility: LIMIT ALL means no limit (equivalent to omitting LIMIT)
		if p.isKeyword("ALL") {
			p.advance()
			// stmt.Limit remains nil, which means "no limit"
		} else {
			limit, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Limit = limit
		}

		// OFFSET (valid after both LIMIT <n> and LIMIT ALL)
		if p.isKeyword("OFFSET") {
			p.advance()
			offset, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Offset = offset
		}
	}

	// OFFSET without LIMIT (PostgreSQL allows standalone OFFSET)
	// Also supports SQL standard: OFFSET N {ROW|ROWS}
	// Only parse if OFFSET wasn't already parsed as part of LIMIT clause
	if stmt.Offset == nil && p.isKeyword("OFFSET") {
		p.advance()
		offset, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
		// Consume optional ROW or ROWS keyword (SQL standard syntax)
		if p.isKeyword("ROW") || p.isKeyword("ROWS") {
			p.advance()
		}
	}

	// FETCH {FIRST|NEXT} [N] {ROW|ROWS} {ONLY|WITH TIES}
	// SQL standard alternative to LIMIT
	if p.isKeyword("FETCH") {
		if stmt.Limit != nil {
			return nil, p.errorf("cannot use both LIMIT and FETCH FIRST")
		}
		p.advance() // consume FETCH
		if !p.isKeyword("FIRST") && !p.isKeyword("NEXT") {
			return nil, p.errorf("expected FIRST or NEXT after FETCH")
		}
		p.advance() // consume FIRST or NEXT

		// Parse optional count expression; default to 1 if ROW/ROWS follows directly
		if p.isKeyword("ROW") || p.isKeyword("ROWS") {
			// No count specified, default to 1
			stmt.Limit = &Literal{Value: int64(1), Type: dukdb.TYPE_BIGINT}
		} else {
			limit, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Limit = limit
		}

		// Expect ROW or ROWS
		if !p.isKeyword("ROW") && !p.isKeyword("ROWS") {
			return nil, p.errorf("expected ROW or ROWS in FETCH clause")
		}
		p.advance() // consume ROW/ROWS

		// Expect ONLY or WITH TIES
		if p.isKeyword("ONLY") {
			p.advance()
			stmt.FetchWithTies = false
		} else if p.isKeyword("WITH") {
			p.advance() // consume WITH
			if !p.isKeyword("TIES") {
				return nil, p.errorf("expected TIES after WITH in FETCH clause")
			}
			p.advance() // consume TIES
			stmt.FetchWithTies = true
		} else {
			return nil, p.errorf("expected ONLY or WITH TIES in FETCH clause")
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
				if p.isKeyword("BY") {
					p.advance()
					if err := p.expectKeyword("NAME"); err != nil {
						return nil, err
					}
					setOp = SetOpUnionAllByName
				} else {
					setOp = SetOpUnionAll
				}
			} else if p.isKeyword("BY") {
				p.advance()
				if err := p.expectKeyword("NAME"); err != nil {
					return nil, err
				}
				setOp = SetOpUnionByName
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

	option, err := p.parseRecursionOption()
	if err != nil {
		return nil, err
	}
	stmt.Options = option

	return stmt, nil
}

func (p *parser) parseRecursionOption() (*RecursionOption, error) {
	if !p.isKeyword("OPTION") {
		// No OPTION clause - return nil, executor will use default of 1000
		return nil, nil
	}

	p.advance()
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("MAX_RECURSION"); err != nil {
		return nil, err
	}
	if p.current().typ != tokenNumber {
		return nil, p.errorf("expected numeric value for MAX_RECURSION")
	}
	maxRecursion, err := parseIntLiteral(p.advance().value)
	if err != nil {
		return nil, p.errorf("invalid MAX_RECURSION value")
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return &RecursionOption{MaxRecursion: maxRecursion}, nil
}

// parseStarModifiers parses optional EXCLUDE and REPLACE modifiers after a star (*) expression.
func (p *parser) parseStarModifiers(star *StarExpr) error {
	if p.isKeyword("EXCLUDE") {
		p.advance()
		if _, err := p.expect(tokenLParen); err != nil {
			return err
		}
		for {
			if p.current().typ != tokenIdent {
				return p.errorf("expected column name in EXCLUDE")
			}
			star.Exclude = append(star.Exclude, p.advance().value)
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return err
		}
	}
	if p.isKeyword("REPLACE") {
		p.advance()
		if _, err := p.expect(tokenLParen); err != nil {
			return err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return err
			}
			if err := p.expectKeyword("AS"); err != nil {
				return p.errorf("expected AS in REPLACE clause")
			}
			if p.current().typ != tokenIdent {
				return p.errorf("expected column name after AS in REPLACE")
			}
			colName := p.advance().value
			star.Replace = append(star.Replace, ReplaceColumn{
				Expr:   expr,
				Column: colName,
			})
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return err
		}
	}
	return nil
}

func (p *parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		if p.current().typ == tokenStar {
			p.advance()
			starExpr := &StarExpr{}
			if err := p.parseStarModifiers(starExpr); err != nil {
				return nil, err
			}
			cols = append(
				cols,
				SelectColumn{
					Star: true,
					Expr: starExpr,
				},
			)

			// After star, check if the next token looks like a keyword typo (e.g., FORM instead of FROM)
			// This catches cases like "SELECT * FORM users" where FORM would otherwise be ignored
			if p.current().typ == tokenIdent && !p.isKeyword("FROM") &&
				!p.isKeyword("WHERE") && !p.isKeyword("GROUP") &&
				!p.isKeyword("ORDER") && !p.isKeyword("LIMIT") &&
				!p.isKeyword("OFFSET") && !p.isKeyword("FETCH") &&
				!p.isKeyword("HAVING") && !p.isKeyword("QUALIFY") &&
				!p.isKeyword("WINDOW") &&
				!p.isKeyword("EXCLUDE") && !p.isKeyword("REPLACE") &&
				!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") {
				tok := p.current()
				if isProbableKeywordTypo(tok.value) {
					suggestion := suggestKeyword(tok.value)
					if suggestion != "" {
						return nil, p.errorAtPosition(tok.pos,
							"unexpected token %q (did you mean %s?)", tok.value, suggestion)
					}
				}
			}
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
				!p.isKeyword("OFFSET") && // OFFSET can appear without LIMIT (PostgreSQL compatibility)
				!p.isKeyword("FETCH") && // FETCH FIRST/NEXT (SQL standard alternative to LIMIT)
				!p.isKeyword("HAVING") && !p.isKeyword("QUALIFY") && !p.isKeyword("WINDOW") &&
				!p.isKeyword("INNER") &&
				!p.isKeyword("LEFT") && !p.isKeyword("RIGHT") &&
				!p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
				!p.isKeyword("NATURAL") && !p.isKeyword("ASOF") && !p.isKeyword("POSITIONAL") &&
				!p.isKeyword("JOIN") && !p.isKeyword("ON") &&
				!p.isKeyword("RETURNING") &&
				!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") {
				// Check if this looks like a keyword typo
				tok := p.current()
				if isProbableKeywordTypo(tok.value) {
					suggestion := suggestKeyword(tok.value)
					if suggestion != "" {
						return nil, p.errorAtPosition(tok.pos,
							"unexpected token %q (did you mean %s?)", tok.value, suggestion)
					}
				} else {
					col.Alias = p.advance().value
				}
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
	err := p.expectKeyword("FROM")
	if err != nil {
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
			p.isKeyword("RIGHT") || p.isKeyword("FULL") || p.isKeyword("CROSS") ||
			p.isKeyword("NATURAL") || p.isKeyword("ASOF") || p.isKeyword("POSITIONAL") {
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

	// Check for LATERAL keyword before subquery or table function
	if p.isKeyword("LATERAL") {
		p.advance()
		ref.Lateral = true
	}

	if p.isKeyword("VALUES") {
		// VALUES clause in FROM: parse into ValuesRef
		vc, err := p.parseValuesClauseBody()
		if err != nil {
			return ref, err
		}
		ref.ValuesRef = vc
	} else if p.current().typ == tokenLParen {
		// Check if this is (VALUES ...) - parenthesized VALUES
		saved := p.tokPos
		p.advance() // consume '('
		if p.isKeyword("VALUES") {
			// Parse VALUES into ValuesRef
			vc, err := p.parseValuesClauseBody()
			if err != nil {
				return ref, err
			}
			ref.ValuesRef = vc
			if _, err := p.expect(tokenRParen); err != nil {
				return ref, err
			}
		} else {
			// Regular subquery - restore position and re-parse
			p.tokPos = saved
			p.advance() // consume '(' again
			subquery, err := p.parseSelect()
			if err != nil {
				return ref, err
			}
			ref.Subquery = subquery
			if _, err := p.expect(tokenRParen); err != nil {
				return ref, err
			}
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
	} else if p.current().typ == tokenString {
		// String literal in FROM position: SELECT * FROM 'file.csv'
		// This is a replacement scan that will be rewritten to a table function call.
		path := p.advance().value
		// Remove surrounding quotes from the token value
		if len(path) >= 2 && (path[0] == '\'' || path[0] == '"') {
			path = path[1 : len(path)-1]
		}
		ref.ReplacementScan = &ReplacementScan{Path: path}
		ref.TableName = path // For alias resolution
	} else {
		return ref, p.errorf("expected table name or subquery")
	}

	// Check for PIVOT or UNPIVOT transformation
	if p.isKeyword("PIVOT") {
		pivot, err := p.parsePivot(ref)
		if err != nil {
			return ref, err
		}
		// Create a new table ref with the PIVOT
		newRef := TableRef{
			PivotRef: pivot,
		}
		// Parse optional alias after PIVOT
		if p.isKeyword("AS") {
			p.advance()
			if p.current().typ != tokenIdent {
				return newRef, p.errorf("expected alias after AS")
			}
			newRef.Alias = p.advance().value
		} else if p.current().typ == tokenIdent && !p.isKeyword("WHERE") &&
			!p.isKeyword("GROUP") && !p.isKeyword("ORDER") && !p.isKeyword("LIMIT") &&
			!p.isKeyword("OFFSET") && // OFFSET can appear without LIMIT (PostgreSQL compatibility)
			!p.isKeyword("FETCH") && // FETCH FIRST/NEXT (SQL standard alternative to LIMIT)
			!p.isKeyword("JOIN") && !p.isKeyword("INNER") && !p.isKeyword("LEFT") &&
			!p.isKeyword("RIGHT") && !p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
			!p.isKeyword("NATURAL") && !p.isKeyword("ASOF") && !p.isKeyword("POSITIONAL") &&
			!p.isKeyword("ON") && !p.isKeyword("HAVING") && !p.isKeyword("QUALIFY") &&
			!p.isKeyword("WINDOW") &&
			!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") {
			// Check if this looks like a keyword typo
			tok := p.current()
			if isProbableKeywordTypo(tok.value) {
				suggestion := suggestKeyword(tok.value)
				if suggestion != "" {
					return newRef, p.errorAtPosition(tok.pos,
						"unexpected token %q (did you mean %s?)", tok.value, suggestion)
				}
			} else {
				newRef.Alias = p.advance().value
			}
		}
		return newRef, nil
	}

	if p.isKeyword("UNPIVOT") {
		unpivot, err := p.parseUnpivot(ref)
		if err != nil {
			return ref, err
		}
		// Create a new table ref with the UNPIVOT
		newRef := TableRef{
			UnpivotRef: unpivot,
		}
		// Parse optional alias after UNPIVOT
		if p.isKeyword("AS") {
			p.advance()
			if p.current().typ != tokenIdent {
				return newRef, p.errorf("expected alias after AS")
			}
			newRef.Alias = p.advance().value
		} else if p.current().typ == tokenIdent && !p.isKeyword("WHERE") &&
			!p.isKeyword("GROUP") && !p.isKeyword("ORDER") && !p.isKeyword("LIMIT") &&
			!p.isKeyword("OFFSET") && // OFFSET can appear without LIMIT (PostgreSQL compatibility)
			!p.isKeyword("FETCH") && // FETCH FIRST/NEXT (SQL standard alternative to LIMIT)
			!p.isKeyword("JOIN") && !p.isKeyword("INNER") && !p.isKeyword("LEFT") &&
			!p.isKeyword("RIGHT") && !p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
			!p.isKeyword("ON") && !p.isKeyword("HAVING") && !p.isKeyword("QUALIFY") &&
			!p.isKeyword("WINDOW") &&
			!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") {
			// Check if this looks like a keyword typo
			tok := p.current()
			if isProbableKeywordTypo(tok.value) {
				suggestion := suggestKeyword(tok.value)
				if suggestion != "" {
					return newRef, p.errorAtPosition(tok.pos,
						"unexpected token %q (did you mean %s?)", tok.value, suggestion)
				}
			} else {
				newRef.Alias = p.advance().value
			}
		}
		return newRef, nil
	}

	// Check for time travel clause (AS OF ... or AT (...))
	// This comes before the alias parsing since AS OF is different from AS alias
	if p.isKeyword("AS") && p.peek().typ == tokenIdent && strings.EqualFold(p.peek().value, "OF") {
		timeTravel, err := p.parseTimeTravelClause()
		if err != nil {
			return ref, err
		}
		ref.TimeTravel = timeTravel
	} else if p.isKeyword("AT") {
		timeTravel, err := p.parseTimeTravelAtClause()
		if err != nil {
			return ref, err
		}
		ref.TimeTravel = timeTravel
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
		!p.isKeyword("OFFSET") && // OFFSET can appear without LIMIT (PostgreSQL compatibility)
		!p.isKeyword("FETCH") && // FETCH FIRST/NEXT (SQL standard alternative to LIMIT)
		!p.isKeyword("JOIN") && !p.isKeyword("INNER") && !p.isKeyword("LEFT") &&
		!p.isKeyword("RIGHT") && !p.isKeyword("FULL") && !p.isKeyword("CROSS") &&
		!p.isKeyword("NATURAL") && !p.isKeyword("ASOF") && !p.isKeyword("POSITIONAL") &&
		!p.isKeyword("ON") && !p.isKeyword("HAVING") && !p.isKeyword("QUALIFY") &&
		!p.isKeyword("WINDOW") &&
		!p.isKeyword("UNION") && !p.isKeyword("INTERSECT") && !p.isKeyword("EXCEPT") &&
		!p.isKeyword("RETURNING") && !p.isKeyword("TABLESAMPLE") && !p.isKeyword("AT") {
		// Check if this looks like a keyword typo - if so, don't consume it as an alias
		tok := p.current()
		if isProbableKeywordTypo(tok.value) {
			suggestion := suggestKeyword(tok.value)
			if suggestion != "" {
				return ref, p.errorAtPosition(tok.pos,
					"unexpected token %q (did you mean %s?)", tok.value, suggestion)
			}
		} else {
			ref.Alias = p.advance().value
		}
	}

	return ref, nil
}

// parseTimeTravelClause parses AS OF TIMESTAMP/SNAPSHOT/BRANCH syntax.
// Syntax:
//   - AS OF TIMESTAMP '2024-01-15 10:00:00'
//   - AS OF SNAPSHOT 1234567890
//   - AS OF BRANCH main
func (p *parser) parseTimeTravelClause() (*TimeTravelClause, error) {
	// Consume AS
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	// Consume OF
	if err := p.expectKeyword("OF"); err != nil {
		return nil, err
	}

	clause := &TimeTravelClause{}

	// Determine the type: TIMESTAMP, SNAPSHOT, or BRANCH
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected TIMESTAMP, SNAPSHOT, or BRANCH after AS OF")
	}

	typeKeyword := strings.ToUpper(p.current().value)
	switch typeKeyword {
	case "TIMESTAMP":
		p.advance()
		clause.Type = TimeTravelTimestamp
		// Parse the timestamp value (should be a string literal)
		value, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		clause.Value = value

	case "SNAPSHOT":
		p.advance()
		clause.Type = TimeTravelSnapshot
		// Parse the snapshot ID (should be a numeric literal)
		value, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		clause.Value = value

	case "BRANCH":
		p.advance()
		clause.Type = TimeTravelBranch
		// Parse the branch name (should be an identifier or string)
		if p.current().typ == tokenString {
			// Quoted branch name
			value := p.advance().value
			// Remove quotes
			if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
				value = value[1 : len(value)-1]
			}
			clause.Value = &Literal{Value: value}
		} else if p.current().typ == tokenIdent {
			// Unquoted branch name
			clause.Value = &Literal{Value: p.advance().value}
		} else {
			return nil, p.errorf("expected branch name after AS OF BRANCH")
		}

	default:
		return nil, p.errorf(
			"expected TIMESTAMP, SNAPSHOT, or BRANCH after AS OF, got %s",
			typeKeyword,
		)
	}

	return clause, nil
}

// parseTimeTravelAtClause parses AT (VERSION => N) syntax.
// Syntax: AT (VERSION => 3)
func (p *parser) parseTimeTravelAtClause() (*TimeTravelClause, error) {
	// Consume AT
	if err := p.expectKeyword("AT"); err != nil {
		return nil, err
	}

	// Expect opening parenthesis
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	clause := &TimeTravelClause{}

	// Expect VERSION keyword
	if !p.isKeyword("VERSION") {
		return nil, p.errorf("expected VERSION after AT (")
	}
	p.advance()

	// Expect => operator
	if p.current().typ != tokenOperator || p.current().value != "=" {
		return nil, p.errorf("expected => after VERSION")
	}
	p.advance()
	if p.current().typ != tokenOperator || p.current().value != ">" {
		return nil, p.errorf("expected => after VERSION")
	}
	p.advance()

	clause.Type = TimeTravelVersion

	// Parse the version number
	value, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	clause.Value = value

	// Expect closing parenthesis
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return clause, nil
}

// parseTablesample parses a TABLESAMPLE clause.
// Syntax:
//   - TABLESAMPLE BERNOULLI(percentage)
//   - TABLESAMPLE SYSTEM(percentage)
//   - TABLESAMPLE RESERVOIR(rows)
//   - TABLESAMPLE method(value) REPEATABLE(seed)
func (p *parser) parseTablesample() (*SampleOptions, error) {
	err := p.expectKeyword("TABLESAMPLE")
	if err != nil {
		return nil, err
	}

	sample := &SampleOptions{}

	// Parse sampling method
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected sampling method (BERNOULLI, SYSTEM, or RESERVOIR)")
	}
	methodName := strings.ToUpper(p.advance().value)

	switch methodName {
	case "BERNOULLI":
		sample.Method = SampleBernoulli
	case "SYSTEM":
		sample.Method = SampleSystem
	case "RESERVOIR":
		sample.Method = SampleReservoir
	default:
		return nil, p.errorf(
			"unknown sampling method: %s (expected BERNOULLI, SYSTEM, or RESERVOIR)",
			methodName,
		)
	}

	// Parse percentage or row count in parentheses
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse the numeric value
	if p.current().typ != tokenNumber {
		return nil, p.errorf("expected numeric value for sample size")
	}
	valueStr := p.advance().value
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return nil, p.errorf("invalid sample value: %s", valueStr)
	}

	if sample.Method == SampleReservoir {
		// RESERVOIR uses row count
		sample.Rows = int(value)
	} else {
		// BERNOULLI and SYSTEM use percentage
		sample.Percentage = value
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Check for REPEATABLE(seed) clause
	if p.isKeyword("REPEATABLE") {
		p.advance()
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if p.current().typ != tokenNumber {
			return nil, p.errorf("expected seed value after REPEATABLE")
		}
		seedStr := p.advance().value
		seed, err := strconv.ParseInt(seedStr, 10, 64)
		if err != nil {
			return nil, p.errorf("invalid seed value: %s", seedStr)
		}
		sample.Seed = &seed
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	return sample, nil
}

// parseTableFunction parses a table function call in a FROM clause.
// The function name has already been consumed.
// Example: read_csv('file.csv', delimiter=',', header=true)
func (p *parser) parseTableFunction(name string) (*TableFunctionRef, error) {
	_, err := p.expect(tokenLParen)
	if err != nil {
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
			if p.current().typ == tokenIdent && p.peek().typ == tokenOperator &&
				p.peek().value == "=" {
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

	_, err = p.expect(tokenRParen)
	if err != nil {
		return nil, err
	}

	return tableFunc, nil
}

func (p *parser) parseJoin() (JoinClause, error) {
	var join JoinClause

	// Join type
	switch {
	case p.isKeyword("NATURAL"):
		p.advance()
		switch {
		case p.isKeyword("LEFT"):
			p.advance()
			if p.isKeyword("OUTER") {
				p.advance()
			}
			join.Type = JoinTypeNaturalLeft
		case p.isKeyword("RIGHT"):
			p.advance()
			if p.isKeyword("OUTER") {
				p.advance()
			}
			join.Type = JoinTypeNaturalRight
		case p.isKeyword("FULL"):
			p.advance()
			if p.isKeyword("OUTER") {
				p.advance()
			}
			join.Type = JoinTypeNaturalFull
		default:
			if p.isKeyword("INNER") {
				p.advance()
			}
			join.Type = JoinTypeNatural
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("ASOF"):
		p.advance()
		if p.isKeyword("LEFT") {
			p.advance()
			if p.isKeyword("OUTER") {
				p.advance()
			}
			join.Type = JoinTypeAsOfLeft
		} else {
			if p.isKeyword("INNER") {
				p.advance()
			}
			join.Type = JoinTypeAsOf
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("POSITIONAL"):
		p.advance()
		join.Type = JoinTypePositional
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("INNER"):
		p.advance()
		join.Type = JoinTypeInner
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("LEFT"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeLeft
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("RIGHT"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeRight
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("FULL"):
		p.advance()
		if p.isKeyword("OUTER") {
			p.advance()
		}
		join.Type = JoinTypeFull
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	case p.isKeyword("CROSS"):
		p.advance()
		join.Type = JoinTypeCross
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	default:
		join.Type = JoinTypeInner
		if err := p.expectKeyword("JOIN"); err != nil {
			return join, err
		}
	}

	// Table
	table, err := p.parseTableRef()
	if err != nil {
		return join, err
	}
	join.Table = table

	// ON/USING condition depends on join type
	switch join.Type {
	case JoinTypeCross, JoinTypePositional,
		JoinTypeNatural, JoinTypeNaturalLeft, JoinTypeNaturalRight, JoinTypeNaturalFull:
		// No ON clause for CROSS, POSITIONAL, or NATURAL joins
		// But USING is allowed for NATURAL joins (parser supports it)
		if p.isKeyword("USING") {
			p.advance()
			if _, err := p.expect(tokenLParen); err != nil {
				return join, err
			}
			for {
				if p.current().typ != tokenIdent {
					return join, p.errorf("expected column name in USING clause")
				}
				col := p.advance().value
				join.Using = append(join.Using, col)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return join, err
			}
		}
	default:
		// ON or USING clause
		if p.isKeyword("ON") {
			p.advance()
			cond, err := p.parseExpr()
			if err != nil {
				return join, err
			}
			join.Condition = cond
		} else if p.isKeyword("USING") {
			p.advance()
			if _, err := p.expect(tokenLParen); err != nil {
				return join, err
			}
			for {
				if p.current().typ != tokenIdent {
					return join, p.errorf("expected column name in USING clause")
				}
				col := p.advance().value
				join.Using = append(join.Using, col)
				if p.current().typ != tokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return join, err
			}
		} else {
			return join, p.errorf("expected ON or USING after JOIN")
		}
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

		// Parse COLLATE clause (before ASC/DESC per SQL standard)
		if p.isKeyword("COLLATE") {
			p.advance()
			collName, err := p.parseCollationName()
			if err != nil {
				return nil, err
			}
			order.Collation = collName
		}

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
				nullsFirst := true
				order.NullsFirst = &nullsFirst
			} else if p.isKeyword("LAST") {
				p.advance()
				nullsFirst := false
				order.NullsFirst = &nullsFirst
			} else {
				return nil, p.errorf("expected FIRST or LAST after NULLS")
			}
		}

		orderBy = append(orderBy, order)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	return orderBy, nil
}

// parseCollationName parses a collation name that may contain dots for chained
// modifiers (e.g., "de_DE.NOCASE.NOACCENT").
func (p *parser) parseCollationName() (string, error) {
	tok := p.current()
	if tok.typ != tokenIdent && tok.typ != tokenString {
		return "", p.errorf("expected collation name after COLLATE")
	}
	name := p.advance().value

	// Consume additional dotted parts (e.g., .NOCASE.NOACCENT)
	for p.current().typ == tokenDot {
		p.advance() // consume dot
		tok = p.current()
		if tok.typ != tokenIdent {
			return "", p.errorf("expected collation modifier after '.'")
		}
		name += "." + p.advance().value
	}

	return name, nil
}

func parseIntLiteral(value string) (int, error) {
	return strconv.Atoi(value)
}

func (p *parser) parseInsert() (*InsertStmt, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}

	// Check for OR REPLACE / OR IGNORE (SQLite-compatible syntax)
	var orAction string // "", "REPLACE", or "IGNORE"
	if p.isKeyword("OR") {
		p.advance()
		if p.isKeyword("REPLACE") {
			orAction = "REPLACE"
			p.advance()
		} else if p.isKeyword("IGNORE") {
			orAction = "IGNORE"
			p.advance()
		} else {
			return nil, p.errorf("expected REPLACE or IGNORE after INSERT OR")
		}
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

	// Parse optional ON CONFLICT clause
	if p.isKeyword("ON") {
		onConflict, err := p.parseOnConflict()
		if err != nil {
			return nil, err
		}
		stmt.OnConflict = onConflict
	}

	// Parse optional RETURNING clause
	if p.isKeyword("RETURNING") {
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	// Desugar OR REPLACE/IGNORE to ON CONFLICT clause
	if orAction != "" && stmt.OnConflict == nil {
		switch orAction {
		case "IGNORE":
			stmt.OnConflict = &OnConflictClause{
				Action: OnConflictDoNothing,
			}
		case "REPLACE":
			// OnConflictDoUpdate with empty UpdateSet signals the binder
			// to auto-generate SET col = EXCLUDED.col for all non-PK columns.
			stmt.OnConflict = &OnConflictClause{
				Action: OnConflictDoUpdate,
			}
		}
	} else if orAction != "" && stmt.OnConflict != nil {
		return nil, p.errorf("INSERT OR %s cannot be combined with ON CONFLICT clause", orAction)
	}

	return stmt, nil
}

// parseValuesRows parses the rows portion of a VALUES clause: (expr, ...), (expr, ...), ...
// Returns the parsed rows. Validates that all rows have the same number of columns.
func (p *parser) parseValuesRows() ([][]Expr, error) {
	var rows [][]Expr
	for {
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		rows = append(rows, values)
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	// Validate all rows have same column count
	if len(rows) > 1 {
		expected := len(rows[0])
		for i := 1; i < len(rows); i++ {
			if len(rows[i]) != expected {
				return nil, p.errorf("VALUES rows have different numbers of columns: row 1 has %d, row %d has %d",
					expected, i+1, len(rows[i]))
			}
		}
	}

	return rows, nil
}

// parseValuesClauseBody parses VALUES keyword and rows, returning a ValuesClause.
// Expects the VALUES keyword to be the current token.
func (p *parser) parseValuesClauseBody() (*ValuesClause, error) {
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}

	rows, err := p.parseValuesRows()
	if err != nil {
		return nil, err
	}

	return &ValuesClause{
		Rows: rows,
	}, nil
}

// parseStandaloneValues parses a standalone VALUES statement.
// VALUES (1, 'a'), (2, 'b') is treated as SELECT * FROM (VALUES ...) AS valueslist
func (p *parser) parseStandaloneValues() (*SelectStmt, error) {
	vc, err := p.parseValuesClauseBody()
	if err != nil {
		return nil, err
	}

	// Wrap as: SELECT * FROM (VALUES ...) AS valueslist
	outerSelect := &SelectStmt{
		Columns: []SelectColumn{
			{
				Expr: &StarExpr{},
				Star: true,
			},
		},
		From: &FromClause{
			Tables: []TableRef{
				{
					ValuesRef: vc,
					Alias:     "valueslist",
				},
			},
		},
	}

	return outerSelect, nil
}

// parseOnConflict parses an ON CONFLICT clause for INSERT statements.
func (p *parser) parseOnConflict() (*OnConflictClause, error) {
	p.advance() // consume ON
	if err := p.expectKeyword("CONFLICT"); err != nil {
		return nil, err
	}

	clause := &OnConflictClause{}

	// Optional conflict target: (col1, col2, ...)
	if p.current().typ == tokenLParen {
		p.advance()
		for {
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected column name in conflict target")
			}
			clause.ConflictColumns = append(clause.ConflictColumns, p.advance().value)
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	// DO keyword
	if err := p.expectKeyword("DO"); err != nil {
		return nil, err
	}

	// NOTHING or UPDATE
	if p.isKeyword("NOTHING") {
		p.advance()
		clause.Action = OnConflictDoNothing
		return clause, nil
	}

	if !p.isKeyword("UPDATE") {
		return nil, p.errorf("expected NOTHING or UPDATE after DO")
	}
	p.advance()
	clause.Action = OnConflictDoUpdate

	// SET clause
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	// Parse SET assignments
	for {
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected column name in SET clause")
		}
		col := p.advance().value

		if p.current().typ != tokenOperator || p.current().value != "=" {
			return nil, p.errorf("expected = after column name")
		}
		p.advance()

		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		clause.UpdateSet = append(clause.UpdateSet, SetClause{Column: col, Value: val})

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	// Optional WHERE clause for DO UPDATE
	if p.isKeyword("WHERE") {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		clause.UpdateWhere = where
	}

	return clause, nil
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

	// Parse optional RETURNING clause
	if p.isKeyword("RETURNING") {
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
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

	// Parse optional USING clause for multi-table delete
	if p.isKeyword("USING") {
		p.advance() // consume USING
		for {
			tableRef, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.Using = append(stmt.Using, tableRef)
			if p.current().typ != tokenComma {
				break
			}
			p.advance() // consume comma
		}
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

	// Parse optional RETURNING clause
	if p.isKeyword("RETURNING") {
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

// parseReturningClause parses the RETURNING clause for INSERT, UPDATE, and DELETE statements.
// The RETURNING keyword has already been checked but not consumed.
// It supports:
//   - RETURNING * (returns all columns)
//   - RETURNING col1, col2 (returns specific columns)
//   - RETURNING col1 AS alias, col2 (returns columns with aliases)
//   - RETURNING expr AS alias (returns expressions with aliases)
func (p *parser) parseReturningClause() ([]SelectColumn, error) {
	if err := p.expectKeyword("RETURNING"); err != nil {
		return nil, err
	}

	var cols []SelectColumn

	for {
		if p.current().typ == tokenStar {
			p.advance()
			cols = append(cols, SelectColumn{
				Star: true,
				Expr: &StarExpr{},
			})
		} else {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expr: expr}

			// Check for AS alias or implicit alias
			if p.isKeyword("AS") {
				p.advance()
				if p.current().typ != tokenIdent {
					return nil, p.errorf("expected identifier after AS in RETURNING clause")
				}
				col.Alias = p.advance().value
			} else if p.current().typ == tokenIdent &&
				p.current().typ != tokenComma &&
				p.current().typ != tokenSemicolon &&
				p.current().typ != tokenEOF {
				// Check if it's not a keyword that would end the clause
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

func (p *parser) parseCreate() (Statement, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}

	// Check for OR REPLACE
	orReplace := false
	if p.isKeyword("OR") {
		p.advance()
		if err := p.expectKeyword("REPLACE"); err != nil {
			return nil, err
		}
		orReplace = true
	}

	// Check for PERSISTENT or TEMPORARY (for SECRET)
	persistent := false
	temporary := false
	if p.isKeyword("PERSISTENT") {
		p.advance()
		persistent = true
	} else if p.isKeyword("TEMPORARY") || p.isKeyword("TEMP") {
		p.advance()
		temporary = true
	}

	// Check for UNIQUE INDEX (special case)
	if p.isKeyword("UNIQUE") {
		return p.parseCreateIndex()
	}

	// Dispatch based on object type
	if p.isKeyword("TABLE") {
		return p.parseCreateTable(orReplace, temporary)
	} else if p.isKeyword("VIEW") {
		return p.parseCreateView()
	} else if p.isKeyword("INDEX") {
		return p.parseCreateIndex()
	} else if p.isKeyword("SEQUENCE") {
		return p.parseCreateSequence()
	} else if p.isKeyword("SCHEMA") {
		return p.parseCreateSchema()
	} else if p.isKeyword("SECRET") {
		return p.parseCreateSecret(orReplace, persistent, temporary)
	} else if p.isKeyword("FUNCTION") {
		return p.parseCreateFunction(orReplace)
	} else if p.isKeyword("TYPE") {
		return p.parseCreateType()
	} else if p.isKeyword("MACRO") {
		return p.parseCreateMacro(orReplace)
	} else if p.isKeyword("DATABASE") {
		return p.parseCreateDatabase()
	}

	return nil, p.errorf(
		"expected TABLE, VIEW, INDEX, SEQUENCE, SCHEMA, SECRET, FUNCTION, TYPE, MACRO, or DATABASE after CREATE",
	)
}

func (p *parser) parseCreateTable(orReplace bool, temporary bool) (*CreateTableStmt, error) {
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{}
	stmt.OrReplace = orReplace
	stmt.Temporary = temporary

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
		// Check for table-level constraints
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
		} else if p.isKeyword("UNIQUE") || p.isKeyword("CHECK") || p.isKeyword("CONSTRAINT") || p.isKeyword("FOREIGN") {
			tc, err := p.parseTableConstraint()
			if err != nil {
				return nil, err
			}
			stmt.Constraints = append(stmt.Constraints, tc)
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

	// Normalize column-level constraints to table-level
	for _, col := range stmt.Columns {
		if col.Unique {
			stmt.Constraints = append(stmt.Constraints, TableConstraint{
				Type:    "UNIQUE",
				Columns: []string{col.Name},
			})
		}
		if col.Check != nil {
			stmt.Constraints = append(stmt.Constraints, TableConstraint{
				Type:       "CHECK",
				Expression: col.Check,
			})
		}
		if col.ForeignKey != nil {
			refCols := col.ForeignKey.Columns
			if len(refCols) == 0 {
				refCols = []string{col.Name}
			}
			stmt.Constraints = append(stmt.Constraints, TableConstraint{
				Type:       "FOREIGN_KEY",
				Columns:    []string{col.Name},
				RefTable:   col.ForeignKey.Table,
				RefColumns: refCols,
				OnDelete:   col.ForeignKey.OnDelete,
				OnUpdate:   col.ForeignKey.OnUpdate,
			})
		}
	}

	return stmt, nil
}

func (p *parser) parseTableConstraint() (TableConstraint, error) {
	tc := TableConstraint{}

	// Optional CONSTRAINT name
	if p.isKeyword("CONSTRAINT") {
		p.advance()
		if p.current().typ != tokenIdent {
			return tc, p.errorf("expected constraint name")
		}
		tc.Name = p.advance().value
	}

	if p.isKeyword("UNIQUE") {
		p.advance()
		tc.Type = "UNIQUE"
		if _, err := p.expect(tokenLParen); err != nil {
			return tc, err
		}
		for {
			if p.current().typ != tokenIdent {
				return tc, p.errorf("expected column name in UNIQUE constraint")
			}
			tc.Columns = append(tc.Columns, p.advance().value)
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return tc, err
		}
		return tc, nil
	}

	if p.isKeyword("CHECK") {
		p.advance()
		tc.Type = "CHECK"
		if _, err := p.expect(tokenLParen); err != nil {
			return tc, err
		}
		expr, err := p.parseExpr()
		if err != nil {
			return tc, err
		}
		tc.Expression = expr
		if _, err := p.expect(tokenRParen); err != nil {
			return tc, err
		}
		return tc, nil
	}

	if p.isKeyword("FOREIGN") {
		p.advance()
		if err := p.expectKeyword("KEY"); err != nil {
			return tc, err
		}
		tc.Type = "FOREIGN_KEY"
		// Parse child columns
		if _, err := p.expect(tokenLParen); err != nil {
			return tc, err
		}
		for {
			if p.current().typ != tokenIdent {
				return tc, p.errorf("expected column name in FOREIGN KEY")
			}
			tc.Columns = append(tc.Columns, p.advance().value)
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return tc, err
		}
		// Parse REFERENCES
		ref, err := p.parseForeignKeyRef()
		if err != nil {
			return tc, err
		}
		tc.RefTable = ref.Table
		tc.RefColumns = ref.Columns
		tc.OnDelete = ref.OnDelete
		tc.OnUpdate = ref.OnUpdate
		return tc, nil
	}

	return tc, p.errorf("expected UNIQUE, CHECK, or FOREIGN KEY constraint")
}

// parseForeignKeyRef parses REFERENCES table(col1, col2) [ON DELETE action] [ON UPDATE action]
func (p *parser) parseForeignKeyRef() (*ForeignKeyRef, error) {
	if err := p.expectKeyword("REFERENCES"); err != nil {
		return nil, err
	}

	ref := &ForeignKeyRef{}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected table name after REFERENCES")
	}
	ref.Table = p.advance().value

	// Column list
	if p.current().typ == tokenLParen {
		p.advance()
		for {
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected column name in REFERENCES")
			}
			ref.Columns = append(ref.Columns, p.advance().value)
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	// Optional ON DELETE/ON UPDATE actions
	for p.isKeyword("ON") {
		p.advance()
		if p.isKeyword("DELETE") {
			p.advance()
			action, err := p.parseFKAction()
			if err != nil {
				return nil, err
			}
			ref.OnDelete = action
		} else if p.isKeyword("UPDATE") {
			p.advance()
			action, err := p.parseFKAction()
			if err != nil {
				return nil, err
			}
			ref.OnUpdate = action
		} else {
			return nil, p.errorf("expected DELETE or UPDATE after ON")
		}
	}

	return ref, nil
}

// parseFKAction parses NO ACTION, RESTRICT (supported) or rejects CASCADE, SET NULL, SET DEFAULT.
func (p *parser) parseFKAction() (ForeignKeyAction, error) {
	if p.isKeyword("NO") {
		p.advance()
		if err := p.expectKeyword("ACTION"); err != nil {
			return FKActionNoAction, err
		}
		return FKActionNoAction, nil
	}
	if p.isKeyword("RESTRICT") {
		p.advance()
		return FKActionRestrict, nil
	}
	if p.isKeyword("CASCADE") {
		return FKActionNoAction, p.errorf("CASCADE action is not supported for foreign keys")
	}
	if p.isKeyword("SET") {
		p.advance()
		if p.isKeyword("NULL") {
			return FKActionNoAction, p.errorf("SET NULL action is not supported for foreign keys")
		}
		if p.isKeyword("DEFAULT") {
			return FKActionNoAction, p.errorf("SET DEFAULT action is not supported for foreign keys")
		}
		return FKActionNoAction, p.errorf("expected NULL or DEFAULT after SET")
	}
	return FKActionNoAction, p.errorf("expected NO ACTION, RESTRICT, CASCADE, SET NULL, or SET DEFAULT")
}

func (p *parser) parseColumnDef() (ColumnDefClause, error) {
	var col ColumnDefClause

	if p.current().typ != tokenIdent {
		return col, p.errorf(
			"expected column name",
		)
	}
	col.Name = p.advance().value

	typeSpec, err := p.collectTypeSpec(
		map[tokenType]bool{tokenComma: true, tokenRParen: true},
		map[string]bool{
			"NOT":        true,
			"NULL":       true,
			"PRIMARY":    true,
			"DEFAULT":    true,
			"UNIQUE":     true,
			"CHECK":      true,
			"REFERENCES": true,
			"COLLATE":    true,
		},
	)
	if err != nil {
		return col, err
	}
	info, err := internaltypes.ParseTypeExpression(typeSpec)
	if err != nil {
		return col, p.errorf("invalid type %q: %v", typeSpec, err)
	}
	col.DataType = info.InternalType()
	col.TypeInfo = info

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
		case p.isKeyword("UNIQUE"):
			p.advance()
			col.Unique = true
		case p.isKeyword("CHECK"):
			p.advance()
			if _, err := p.expect(tokenLParen); err != nil {
				return col, err
			}
			expr, err := p.parseExpr()
			if err != nil {
				return col, err
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return col, err
			}
			col.Check = expr
		case p.isKeyword("COLLATE"):
			p.advance()
			collName, err := p.parseCollationName()
			if err != nil {
				return col, err
			}
			col.Collation = collName
		case p.isKeyword("REFERENCES"):
			ref, err := p.parseForeignKeyRef()
			if err != nil {
				return col, err
			}
			col.ForeignKey = ref
		default:
			// Unknown constraint, stop parsing constraints
			goto done
		}
	}
done:

	return col, nil
}

func (p *parser) parseCreateType() (Statement, error) {
	p.advance() // consume TYPE

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	// Parse type name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected type name")
	}
	name := p.advance().value
	schema := ""

	// Check for schema-qualified name
	if p.current().typ == tokenDot {
		p.advance()
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected type name after schema")
		}
		schema = name
		name = p.advance().value
	}

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	if !p.isKeyword("ENUM") {
		return nil, p.errorf("expected ENUM after AS (only ENUM types are supported)")
	}
	p.advance()

	// Parse enum values: ('value1', 'value2', ...)
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	var values []string
	for {
		if p.current().typ != tokenString {
			return nil, p.errorf("expected string literal for enum value")
		}
		values = append(values, p.advance().value)
		if p.current().typ == tokenComma {
			p.advance()
			continue
		}
		break
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &CreateTypeStmt{
		Name:        name,
		Schema:      schema,
		TypeKind:    "ENUM",
		EnumValues:  values,
		IfNotExists: ifNotExists,
	}, nil
}

func (p *parser) parseDropType() (Statement, error) {
	p.advance() // consume TYPE

	ifExists := false
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected type name")
	}
	name := p.advance().value
	schema := ""

	if p.current().typ == tokenDot {
		p.advance()
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected type name after schema")
		}
		schema = name
		name = p.advance().value
	}

	return &DropTypeStmt{
		Name:     name,
		Schema:   schema,
		IfExists: ifExists,
	}, nil
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
	} else if p.isKeyword("SECRET") {
		return p.parseDropSecret()
	} else if p.isKeyword("TYPE") {
		return p.parseDropType()
	} else if p.isKeyword("MACRO") {
		return p.parseDropMacro()
	} else if p.isKeyword("DATABASE") {
		return p.parseDropDatabase()
	}

	return nil, p.errorf(
		"expected TABLE, VIEW, INDEX, SEQUENCE, SCHEMA, SECRET, TYPE, MACRO, or DATABASE after DROP",
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

// parseTruncate parses a TRUNCATE [TABLE] statement.
func (p *parser) parseTruncate() (*TruncateStmt, error) {
	if err := p.expectKeyword("TRUNCATE"); err != nil {
		return nil, err
	}

	// TABLE keyword is optional
	if p.isKeyword("TABLE") {
		p.advance()
	}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected table name")
	}

	stmt := &TruncateStmt{}
	stmt.Table = p.advance().value

	// Check for schema.table
	if p.current().typ == tokenDot {
		p.advance()
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected table name after dot")
		}
		stmt.Schema = stmt.Table
		stmt.Table = p.advance().value
	}

	return stmt, nil
}

// parseAlter dispatches to the appropriate ALTER statement parser.
func (p *parser) parseAlter() (Statement, error) {
	if p.isKeyword("TABLE") {
		return p.parseAlterTable()
	} else if p.isKeyword("SECRET") {
		return p.parseAlterSecret()
	}

	return nil, p.errorf("expected TABLE or SECRET after ALTER")
}

// parseCreateSecret parses a CREATE SECRET statement.
// Syntax: CREATE [OR REPLACE] [PERSISTENT | TEMPORARY] SECRET [IF NOT EXISTS] name (
//
//	TYPE secret_type,
//	[PROVIDER provider_type,]
//	[SCOPE scope_path,]
//	option_name option_value, ...
//
// )
func (p *parser) parseCreateSecret(
	orReplace, persistent, temporary bool,
) (*CreateSecretStmt, error) {
	if err := p.expectKeyword("SECRET"); err != nil {
		return nil, err
	}

	stmt := &CreateSecretStmt{
		OrReplace:  orReplace,
		Persistent: persistent && !temporary, // PERSISTENT unless TEMPORARY is specified
		Options:    make(map[string]string),
	}

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

	// Secret name (optional in DuckDB, but we require it)
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected secret name")
	}
	stmt.Name = p.advance().value

	// Options in parentheses
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse options
	for {
		if p.current().typ == tokenRParen {
			break
		}

		// Option name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected option name")
		}
		optName := strings.ToUpper(p.advance().value)

		// Option value (can be string literal or identifier)
		var optValue string
		if p.current().typ == tokenString {
			tok := p.advance()
			// Remove quotes from string value
			optValue = tok.value[1 : len(tok.value)-1]
			// Unescape doubled quotes
			optValue = strings.ReplaceAll(optValue, "''", "'")
		} else if p.current().typ == tokenIdent {
			optValue = p.advance().value
		} else if p.current().typ == tokenNumber {
			optValue = p.advance().value
		} else {
			return nil, p.errorf("expected option value for %s", optName)
		}

		// Handle special options
		switch optName {
		case "TYPE":
			stmt.SecretType = strings.ToUpper(optValue)
		case "PROVIDER":
			stmt.Provider = strings.ToUpper(optValue)
		case "SCOPE":
			stmt.Scope = optValue
		default:
			// Store as general option with lowercase key for consistency
			stmt.Options[strings.ToLower(optName)] = optValue
		}

		// Check for comma or end
		if p.current().typ == tokenComma {
			p.advance()
		} else if p.current().typ != tokenRParen {
			return nil, p.errorf("expected comma or ) in secret options")
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Validate required fields
	if stmt.SecretType == "" {
		return nil, p.errorf("TYPE is required for CREATE SECRET")
	}

	return stmt, nil
}

// parseCreateFunction parses a CREATE FUNCTION statement.
// Syntax: CREATE [OR REPLACE] FUNCTION name(params) RETURNS type
//
//	[LANGUAGE lang] [IMMUTABLE|STABLE|VOLATILE] [STRICT] [LEAKPROOF]
//	[PARALLEL SAFE|UNSAFE|RESTRICTED]
//	AS 'body' | AS $$body$$
func (p *parser) parseCreateFunction(orReplace bool) (*CreateFunctionStmt, error) {
	if err := p.expectKeyword("FUNCTION"); err != nil {
		return nil, err
	}

	stmt := &CreateFunctionStmt{
		OrReplace:  orReplace,
		Language:   "sql",              // Default language
		Volatility: VolatilityVolatile, // Default volatility
	}

	// Function name (possibly qualified: schema.name)
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected function name")
	}
	stmt.Name = p.advance().value

	// Check for schema qualification
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected function name after schema")
		}
		stmt.Name = p.advance().value
	}

	// Parameter list: (name type, ...)
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse parameters
	for p.current().typ != tokenRParen && p.current().typ != tokenEOF {
		param := FuncParam{}

		// Parameter name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected parameter name")
		}
		param.Name = p.advance().value

		typeSpec, err := p.collectTypeSpec(
			map[tokenType]bool{tokenComma: true, tokenRParen: true},
			nil,
		)
		if err != nil {
			return nil, err
		}
		info, err := internaltypes.ParseTypeExpression(typeSpec)
		if err != nil {
			return nil, p.errorf("invalid type %q: %v", typeSpec, err)
		}
		param.Type = info.InternalType()
		param.Info = info

		stmt.Params = append(stmt.Params, param)

		// Check for comma or end
		if p.current().typ == tokenComma {
			p.advance()
		} else if p.current().typ != tokenRParen {
			return nil, p.errorf("expected comma or ) in parameter list")
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// RETURNS type
	if err := p.expectKeyword("RETURNS"); err != nil {
		return nil, err
	}
	typeSpec, err := p.collectTypeSpec(
		nil,
		map[string]bool{
			"LANGUAGE":  true,
			"IMMUTABLE": true,
			"STABLE":    true,
			"VOLATILE":  true,
			"STRICT":    true,
			"LEAKPROOF": true,
			"PARALLEL":  true,
			"AS":        true,
		},
	)
	if err != nil {
		return nil, err
	}
	info, err := internaltypes.ParseTypeExpression(typeSpec)
	if err != nil {
		return nil, p.errorf("invalid return type %q: %v", typeSpec, err)
	}
	stmt.Returns = info.InternalType()
	stmt.ReturnsInfo = info

	// Parse optional attributes before AS
	// These can appear in any order: LANGUAGE, IMMUTABLE/STABLE/VOLATILE, STRICT, LEAKPROOF, PARALLEL
	for {
		if p.isKeyword("LANGUAGE") {
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected language name after LANGUAGE")
			}
			stmt.Language = strings.ToLower(p.advance().value)
		} else if p.isKeyword("IMMUTABLE") {
			p.advance()
			stmt.Volatility = VolatilityImmutable
		} else if p.isKeyword("STABLE") {
			p.advance()
			stmt.Volatility = VolatilityStable
		} else if p.isKeyword("VOLATILE") {
			p.advance()
			stmt.Volatility = VolatilityVolatile
		} else if p.isKeyword("STRICT") {
			p.advance()
			stmt.Strict = true
		} else if p.isKeyword("LEAKPROOF") {
			p.advance()
			stmt.Leakproof = true
		} else if p.isKeyword("PARALLEL") {
			p.advance()
			if p.isKeyword("SAFE") {
				p.advance()
				stmt.ParallelSafe = "SAFE"
			} else if p.isKeyword("UNSAFE") {
				p.advance()
				stmt.ParallelSafe = "UNSAFE"
			} else if p.isKeyword("RESTRICTED") {
				p.advance()
				stmt.ParallelSafe = "RESTRICTED"
			} else {
				return nil, p.errorf("expected SAFE, UNSAFE, or RESTRICTED after PARALLEL")
			}
		} else {
			break
		}
	}

	// AS clause with function body
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	// Function body: either 'single-quoted' or $$dollar-quoted$$
	if p.current().typ != tokenString {
		return nil, p.errorf("expected function body string after AS")
	}
	bodyTok := p.advance()
	body := bodyTok.value

	// Extract body content based on quote style
	if strings.HasPrefix(body, "$$") && strings.HasSuffix(body, "$$") {
		// Dollar-quoted string: strip $$ delimiters
		stmt.Body = body[2 : len(body)-2]
	} else if (strings.HasPrefix(body, "'") && strings.HasSuffix(body, "'")) ||
		(strings.HasPrefix(body, "\"") && strings.HasSuffix(body, "\"")) {
		// Single or double quoted: strip quotes and unescape
		stmt.Body = body[1 : len(body)-1]
		// Handle escaped quotes ('' -> ')
		if strings.HasPrefix(body, "'") {
			stmt.Body = strings.ReplaceAll(stmt.Body, "''", "'")
		} else {
			stmt.Body = strings.ReplaceAll(stmt.Body, "\"\"", "\"")
		}
	} else {
		stmt.Body = body
	}

	return stmt, nil
}

// parseDropSecret parses a DROP SECRET statement.
// Syntax: DROP SECRET [IF EXISTS] name
func (p *parser) parseDropSecret() (*DropSecretStmt, error) {
	if err := p.expectKeyword("SECRET"); err != nil {
		return nil, err
	}

	stmt := &DropSecretStmt{}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// Secret name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected secret name")
	}
	stmt.Name = p.advance().value

	return stmt, nil
}

// parseAlterSecret parses an ALTER SECRET statement.
// Syntax: ALTER SECRET name (option_name option_value, ...)
func (p *parser) parseAlterSecret() (*AlterSecretStmt, error) {
	if err := p.expectKeyword("SECRET"); err != nil {
		return nil, err
	}

	stmt := &AlterSecretStmt{
		Options: make(map[string]string),
	}

	// Secret name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected secret name")
	}
	stmt.Name = p.advance().value

	// Options in parentheses
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse options
	for {
		if p.current().typ == tokenRParen {
			break
		}

		// Option name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected option name")
		}
		optName := strings.ToLower(p.advance().value)

		// Option value
		var optValue string
		if p.current().typ == tokenString {
			tok := p.advance()
			// Remove quotes from string value
			optValue = tok.value[1 : len(tok.value)-1]
			// Unescape doubled quotes
			optValue = strings.ReplaceAll(optValue, "''", "'")
		} else if p.current().typ == tokenIdent {
			optValue = p.advance().value
		} else if p.current().typ == tokenNumber {
			optValue = p.advance().value
		} else {
			return nil, p.errorf("expected option value for %s", optName)
		}

		stmt.Options[optName] = optValue

		// Check for comma or end
		if p.current().typ == tokenComma {
			p.advance()
		} else if p.current().typ != tokenRParen {
			return nil, p.errorf("expected comma or ) in alter secret options")
		}
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *parser) parseBegin() (*BeginStmt, error) {
	p.advance() // consume BEGIN
	if p.isKeyword("TRANSACTION") {
		p.advance() // consume TRANSACTION (optional)
	}

	stmt := &BeginStmt{
		IsolationLevel: IsolationLevelSerializable, // default
	}

	// Parse optional ISOLATION LEVEL clause
	if p.isKeyword("ISOLATION") {
		p.advance() // consume ISOLATION
		if err := p.expectKeyword("LEVEL"); err != nil {
			return nil, err
		}

		// Parse the isolation level
		isolationLevel, err := p.parseIsolationLevel()
		if err != nil {
			return nil, err
		}
		stmt.IsolationLevel = isolationLevel
		stmt.HasExplicitIsolation = true
	}

	return stmt, nil
}

// parseIsolationLevel parses an isolation level specification.
// Supports: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
func (p *parser) parseIsolationLevel() (IsolationLevel, error) {
	if p.isKeyword("READ") {
		p.advance() // consume READ
		if p.isKeyword("UNCOMMITTED") {
			p.advance() // consume UNCOMMITTED
			return IsolationLevelReadUncommitted, nil
		} else if p.isKeyword("COMMITTED") {
			p.advance() // consume COMMITTED
			return IsolationLevelReadCommitted, nil
		}
		return IsolationLevelSerializable, p.errorf("expected UNCOMMITTED or COMMITTED after READ")
	} else if p.isKeyword("REPEATABLE") {
		p.advance() // consume REPEATABLE
		if err := p.expectKeyword("READ"); err != nil {
			return IsolationLevelSerializable, err
		}
		return IsolationLevelRepeatableRead, nil
	} else if p.isKeyword("SERIALIZABLE") {
		p.advance() // consume SERIALIZABLE
		return IsolationLevelSerializable, nil
	}

	return IsolationLevelSerializable, p.errorf(
		"expected isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, or SERIALIZABLE)",
	)
}

// parseRollback parses a ROLLBACK statement.
// Supports:
//   - ROLLBACK
//   - ROLLBACK TRANSACTION
//   - ROLLBACK TO SAVEPOINT <name>
func (p *parser) parseRollback() (Statement, error) {
	p.advance() // consume ROLLBACK

	// Check for ROLLBACK TO SAVEPOINT
	if p.isKeyword("TO") {
		p.advance() // consume TO
		if err := p.expectKeyword("SAVEPOINT"); err != nil {
			return nil, err
		}
		// Parse savepoint name
		if p.current().typ != tokenIdent && p.current().typ != tokenString {
			return nil, p.errorf("expected savepoint name")
		}
		name := p.advance().value
		// Remove quotes if it's a string literal
		if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
			name = name[1 : len(name)-1]
		}
		return &RollbackToSavepointStmt{Name: name}, nil
	}

	// Optional TRANSACTION keyword
	if p.isKeyword("TRANSACTION") {
		p.advance()
	}

	return &RollbackStmt{}, nil
}

// parseSavepoint parses a SAVEPOINT statement.
// Syntax: SAVEPOINT <name>
func (p *parser) parseSavepoint() (*SavepointStmt, error) {
	p.advance() // consume SAVEPOINT

	// Parse savepoint name
	if p.current().typ != tokenIdent && p.current().typ != tokenString {
		return nil, p.errorf("expected savepoint name")
	}
	name := p.advance().value
	// Remove quotes if it's a string literal
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		name = name[1 : len(name)-1]
	}

	return &SavepointStmt{Name: name}, nil
}

// parseReleaseSavepoint parses a RELEASE SAVEPOINT statement.
// Syntax: RELEASE SAVEPOINT <name>
func (p *parser) parseReleaseSavepoint() (*ReleaseSavepointStmt, error) {
	p.advance() // consume RELEASE

	if err := p.expectKeyword("SAVEPOINT"); err != nil {
		return nil, err
	}

	// Parse savepoint name
	if p.current().typ != tokenIdent && p.current().typ != tokenString {
		return nil, p.errorf("expected savepoint name")
	}
	name := p.advance().value
	// Remove quotes if it's a string literal
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		name = name[1 : len(name)-1]
	}

	return &ReleaseSavepointStmt{Name: name}, nil
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

// parseMerge parses a MERGE INTO statement.
// Supports the following syntax:
//
//	MERGE INTO target_table [AS alias]
//	USING source_table [AS alias]
//	ON join_condition
//	WHEN MATCHED [AND condition] THEN UPDATE SET col = val, ...
//	WHEN MATCHED [AND condition] THEN DELETE
//	WHEN NOT MATCHED [AND condition] THEN INSERT (cols) VALUES (vals)
//	[RETURNING columns]
func (p *parser) parseMerge() (*MergeStmt, error) {
	if err := p.expectKeyword("MERGE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}

	stmt := &MergeStmt{}

	// Parse target table reference
	target, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.Into = target

	// Parse USING clause
	if err := p.expectKeyword("USING"); err != nil {
		return nil, err
	}

	source, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.Using = source

	// Parse ON join condition
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	onCond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	stmt.On = onCond

	// Parse WHEN clauses (at least one required)
	for p.isKeyword("WHEN") {
		p.advance() // consume WHEN

		// Determine if MATCHED or NOT MATCHED
		if p.isKeyword("MATCHED") {
			p.advance() // consume MATCHED

			action, err := p.parseMergeAction(true)
			if err != nil {
				return nil, err
			}
			stmt.WhenMatched = append(stmt.WhenMatched, action)
		} else if p.isKeyword("NOT") {
			p.advance() // consume NOT
			if err := p.expectKeyword("MATCHED"); err != nil {
				return nil, err
			}

			// Check for BY SOURCE or BY TARGET (optional)
			bySource := false
			if p.isKeyword("BY") {
				p.advance() // consume BY
				if p.isKeyword("SOURCE") {
					p.advance()
					bySource = true
				} else if p.isKeyword("TARGET") {
					p.advance()
					// BY TARGET is the default for NOT MATCHED
				} else {
					return nil, p.errorf("expected SOURCE or TARGET after BY")
				}
			}

			action, err := p.parseMergeAction(false)
			if err != nil {
				return nil, err
			}

			if bySource {
				stmt.WhenNotMatchedBySource = append(stmt.WhenNotMatchedBySource, action)
			} else {
				stmt.WhenNotMatched = append(stmt.WhenNotMatched, action)
			}
		} else {
			return nil, p.errorf("expected MATCHED or NOT after WHEN")
		}
	}

	// Parse optional RETURNING clause
	if p.isKeyword("RETURNING") {
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

// parseMergeAction parses a single MERGE action (UPDATE, DELETE, INSERT, or DO NOTHING).
// The isMatched parameter indicates whether this is a WHEN MATCHED or WHEN NOT MATCHED clause.
func (p *parser) parseMergeAction(isMatched bool) (MergeAction, error) {
	action := MergeAction{}

	// Parse optional AND condition
	if p.isKeyword("AND") {
		p.advance() // consume AND
		cond, err := p.parseExpr()
		if err != nil {
			return action, err
		}
		action.Cond = cond
	}

	// Expect THEN
	if err := p.expectKeyword("THEN"); err != nil {
		return action, err
	}

	// Parse the action type
	switch {
	case p.isKeyword("UPDATE"):
		p.advance() // consume UPDATE
		if err := p.expectKeyword("SET"); err != nil {
			return action, err
		}
		action.Type = MergeActionUpdate

		// Parse SET clauses
		setClauses, err := p.parseMergeSetClauses()
		if err != nil {
			return action, err
		}
		action.Update = setClauses

	case p.isKeyword("DELETE"):
		p.advance() // consume DELETE
		action.Type = MergeActionDelete

	case p.isKeyword("INSERT"):
		p.advance() // consume INSERT
		action.Type = MergeActionInsert

		// Parse column list (optional)
		var columns []string
		if p.current().typ == tokenLParen {
			p.advance() // consume (
			for {
				if p.current().typ != tokenIdent {
					return action, p.errorf("expected column name")
				}
				columns = append(columns, p.advance().value)
				if p.current().typ != tokenComma {
					break
				}
				p.advance() // consume ,
			}
			if _, err := p.expect(tokenRParen); err != nil {
				return action, err
			}
		}

		// Parse VALUES clause
		if err := p.expectKeyword("VALUES"); err != nil {
			return action, err
		}
		if _, err := p.expect(tokenLParen); err != nil {
			return action, err
		}

		values, err := p.parseExprList()
		if err != nil {
			return action, err
		}

		if _, err := p.expect(tokenRParen); err != nil {
			return action, err
		}

		// Build SetClause pairs for INSERT
		if len(columns) > 0 && len(columns) != len(values) {
			return action, p.errorf(
				"column count (%d) does not match value count (%d)",
				len(columns),
				len(values),
			)
		}

		for i, val := range values {
			col := ""
			if len(columns) > 0 {
				col = columns[i]
			}
			action.Insert = append(action.Insert, SetClause{Column: col, Value: val})
		}

	case p.isKeyword("DO"):
		p.advance() // consume DO
		if err := p.expectKeyword("NOTHING"); err != nil {
			return action, err
		}
		action.Type = MergeActionDoNothing

	default:
		if isMatched {
			return action, p.errorf("expected UPDATE, DELETE, or DO NOTHING in WHEN MATCHED clause")
		}
		return action, p.errorf("expected INSERT or DO NOTHING in WHEN NOT MATCHED clause")
	}

	return action, nil
}

// parseMergeSetClauses parses SET column = value, ... in a MERGE UPDATE action.
// Stops when it encounters WHEN, RETURNING, semicolon, or EOF.
func (p *parser) parseMergeSetClauses() ([]SetClause, error) {
	var setClauses []SetClause

	for {
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected column name")
		}
		col := p.advance().value

		if p.current().typ != tokenOperator || p.current().value != "=" {
			return nil, p.errorf("expected = after column name")
		}
		p.advance() // consume =

		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		setClauses = append(setClauses, SetClause{Column: col, Value: val})

		// Check if we should continue
		if p.current().typ != tokenComma {
			break
		}

		// Look ahead - if next token after comma is WHEN or a terminator, stop
		next := p.peek()
		if next.typ == tokenIdent {
			upperVal := strings.ToUpper(next.value)
			if upperVal == "WHEN" || upperVal == "RETURNING" {
				break
			}
		}
		if next.typ == tokenSemicolon || next.typ == tokenEOF {
			break
		}

		p.advance() // consume comma
	}

	return setClauses, nil
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

// parsePivot parses a PIVOT clause.
// Syntax: PIVOT (aggregate FOR column IN (value1, value2, ...))
// Example: PIVOT (SUM(revenue) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
func (p *parser) parsePivot(source TableRef) (*PivotStmt, error) {
	if err := p.expectKeyword("PIVOT"); err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	pivot := &PivotStmt{
		Source: source,
	}

	// Parse aggregate function(s)
	// Format: SUM(expr) [AS alias], AVG(expr), etc.
	for {
		// Parse aggregate function name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected aggregate function name in PIVOT")
		}
		funcName := strings.ToUpper(p.advance().value)

		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}

		// Parse aggregate expression
		aggExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}

		agg := PivotAggregate{
			Function: funcName,
			Expr:     aggExpr,
		}

		// Check for optional alias
		if p.isKeyword("AS") {
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected alias after AS")
			}
			agg.Alias = p.advance().value
		}

		pivot.Using = append(pivot.Using, agg)

		// Check if there are more aggregates separated by comma
		if p.current().typ != tokenComma {
			break
		}
		p.advance()

		// Check if we've reached FOR keyword
		if p.isKeyword("FOR") {
			break
		}
	}

	// Parse FOR column
	if err := p.expectKeyword("FOR"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected column name after FOR")
	}
	pivot.ForColumn = p.advance().value

	// Parse IN (value1, value2, ...)
	if err := p.expectKeyword("IN"); err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse pivot values
	for {
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		pivot.PivotOn = append(pivot.PivotOn, val)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Close PIVOT clause
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return pivot, nil
}

// parseUnpivot parses an UNPIVOT clause.
// Syntax: UNPIVOT (value_column FOR name_column IN (col1, col2, ...))
// Example: UNPIVOT (value FOR month IN (jan, feb, mar))
func (p *parser) parseUnpivot(source TableRef) (*UnpivotStmt, error) {
	if err := p.expectKeyword("UNPIVOT"); err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	unpivot := &UnpivotStmt{
		Source: source,
	}

	// Parse value column name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected value column name in UNPIVOT")
	}
	unpivot.Into = p.advance().value

	// Parse FOR name_column
	if err := p.expectKeyword("FOR"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected name column after FOR")
	}
	unpivot.For = p.advance().value

	// Parse IN (col1, col2, ...)
	if err := p.expectKeyword("IN"); err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Parse column names to unpivot
	for {
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected column name in UNPIVOT IN clause")
		}
		unpivot.Using = append(unpivot.Using, p.advance().value)

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Close UNPIVOT clause
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return unpivot, nil
}

// parseGroupByList parses a GROUP BY clause, handling special grouping constructs
// like ROLLUP, CUBE, and GROUPING SETS before falling back to expression parsing.
func (p *parser) parseGroupByList() ([]Expr, error) {
	var exprs []Expr

	for {
		var expr Expr
		var err error

		// Check for special grouping constructs
		if p.current().typ == tokenIdent {
			upperVal := strings.ToUpper(p.current().value)
			switch upperVal {
			case "ROLLUP":
				expr, err = p.parseRollup()
			case "CUBE":
				expr, err = p.parseCube()
			case "GROUPING":
				// Check if this is "GROUPING SETS" or just a column named "GROUPING"
				next := p.peek()
				if next.typ == tokenIdent && strings.ToUpper(next.value) == "SETS" {
					expr, err = p.parseGroupingSets()
				} else {
					expr, err = p.parseExpr()
				}
			default:
				expr, err = p.parseExpr()
			}
		} else {
			expr, err = p.parseExpr()
		}

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

// parseRollup parses ROLLUP(expr, expr, ...) and returns a RollupExpr.
func (p *parser) parseRollup() (*RollupExpr, error) {
	p.advance() // consume ROLLUP

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	exprs, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &RollupExpr{Exprs: exprs}, nil
}

// parseCube parses CUBE(expr, expr, ...) and returns a CubeExpr.
func (p *parser) parseCube() (*CubeExpr, error) {
	p.advance() // consume CUBE

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	exprs, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &CubeExpr{Exprs: exprs}, nil
}

// parseGroupingSets parses GROUPING SETS((...), (...), ...) and returns a GroupingSetExpr.
func (p *parser) parseGroupingSets() (*GroupingSetExpr, error) {
	p.advance() // consume GROUPING
	p.advance() // consume SETS

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	var sets [][]Expr

	for {
		// Each grouping set is either:
		// - A parenthesized list of expressions: (a, b)
		// - An empty parenthesized group: ()
		// - A single column without parens (treated as a single-element set)
		if p.current().typ == tokenLParen {
			p.advance() // consume (

			var setExprs []Expr
			if p.current().typ != tokenRParen {
				var err error
				setExprs, err = p.parseExprList()
				if err != nil {
					return nil, err
				}
			}

			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}

			sets = append(sets, setExprs)
		} else {
			// Single expression without parens
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			sets = append(sets, []Expr{expr})
		}

		if p.current().typ != tokenComma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	return &GroupingSetExpr{
		Type:  GroupingSetSimple,
		Exprs: sets,
	}, nil
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
	left, err := p.parseBitwiseOrExpr()
	if err != nil {
		return nil, err
	}

	// IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM
	if p.isKeyword("IS") {
		p.advance()
		not := false
		if p.isKeyword("NOT") {
			p.advance()
			not = true
		}
		// IS [NOT] DISTINCT FROM
		if p.isKeyword("DISTINCT") {
			p.advance()
			if err := p.expectKeyword("FROM"); err != nil {
				return nil, err
			}
			right, err := p.parseBitwiseOrExpr()
			if err != nil {
				return nil, err
			}
			op := OpIsDistinctFrom
			if not {
				op = OpIsNotDistinctFrom
			}
			return &BinaryExpr{
				Left:  left,
				Op:    op,
				Right: right,
			}, nil
		}
		// IS [NOT] NULL
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
		low, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseBitwiseOrExpr()
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
		low, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseBitwiseOrExpr()
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
		right, err := p.parseBitwiseOrExpr()
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
		right, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}

		return &BinaryExpr{
			Left:  left,
			Op:    OpNotLike,
			Right: right,
		}, nil
	}

	// ILIKE (PostgreSQL case-insensitive LIKE)
	if p.isKeyword("ILIKE") {
		p.advance()
		right, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}

		return &BinaryExpr{
			Left:  left,
			Op:    OpILike,
			Right: right,
		}, nil
	}

	// NOT ILIKE (PostgreSQL case-insensitive NOT LIKE)
	if p.isKeyword("NOT") &&
		p.peek().typ == tokenIdent &&
		strings.EqualFold(
			p.peek().value,
			"ILIKE",
		) {
		p.advance() // NOT
		p.advance() // ILIKE
		right, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}

		return &BinaryExpr{
			Left:  left,
			Op:    OpNotILike,
			Right: right,
		}, nil
	}

	// SIMILAR TO
	if p.isKeyword("SIMILAR") {
		p.advance() // SIMILAR
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		right, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}
		// Check for ESCAPE clause
		if p.isKeyword("ESCAPE") {
			p.advance()
			escapeTok := p.current()
			if escapeTok.typ != tokenString {
				return nil, p.errorf("ESCAPE must be a single character string")
			}
			// Strip quotes from string token value
			escapeVal := escapeTok.value[1 : len(escapeTok.value)-1]
			if len(escapeVal) != 1 {
				return nil, p.errorf("ESCAPE must be a single character string")
			}
			p.advance()
			return &SimilarToExpr{
				Expr:    left,
				Pattern: right,
				Escape:  escapeVal,
				Not:     false,
			}, nil
		}
		return &BinaryExpr{
			Left:  left,
			Op:    OpSimilarTo,
			Right: right,
		}, nil
	}

	// NOT SIMILAR TO
	if p.isKeyword("NOT") &&
		p.peek().typ == tokenIdent &&
		strings.EqualFold(p.peek().value, "SIMILAR") {
		p.advance() // NOT
		p.advance() // SIMILAR
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		right, err := p.parseBitwiseOrExpr()
		if err != nil {
			return nil, err
		}
		// Check for ESCAPE clause
		if p.isKeyword("ESCAPE") {
			p.advance()
			escapeTok := p.current()
			if escapeTok.typ != tokenString {
				return nil, p.errorf("ESCAPE must be a single character string")
			}
			// Strip quotes from string token value
			escapeVal := escapeTok.value[1 : len(escapeTok.value)-1]
			if len(escapeVal) != 1 {
				return nil, p.errorf("ESCAPE must be a single character string")
			}
			p.advance()
			return &SimilarToExpr{
				Expr:    left,
				Pattern: right,
				Escape:  escapeVal,
				Not:     true,
			}, nil
		}
		return &BinaryExpr{
			Left:  left,
			Op:    OpNotSimilarTo,
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
		// Check for quantified comparison: op ANY|ALL|SOME (subquery)
		if p.current().typ == tokenIdent {
			upper := strings.ToUpper(p.current().value)
			if (upper == "ANY" || upper == "ALL" || upper == "SOME") && p.peek().typ == tokenLParen {
				quantifier := upper
				if quantifier == "SOME" {
					quantifier = "ANY"
				}
				p.advance() // consume ANY/ALL/SOME
				p.advance() // consume (
				subquery, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(tokenRParen); err != nil {
					return nil, err
				}
				return &QuantifiedComparisonExpr{
					Left:       left,
					Op:         op,
					Quantifier: quantifier,
					Subquery:   subquery,
				}, nil
			}
		}
		right, err := p.parseBitwiseOrExpr()
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

// parseBitwiseOrExpr parses bitwise OR expressions (|).
// This has the lowest precedence among bitwise operators.
// SQL standard precedence (from lowest to highest in bitwise category):
// | (OR) < ^ (XOR) < & (AND) < <<, >> (shifts)
func (p *parser) parseBitwiseOrExpr() (Expr, error) {
	left, err := p.parseBitwiseXorExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenPipe {
		p.advance()
		right, err := p.parseBitwiseXorExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpBitwiseOr,
			Right: right,
		}
	}

	return left, nil
}

// parseBitwiseXorExpr parses bitwise XOR expressions (^).
func (p *parser) parseBitwiseXorExpr() (Expr, error) {
	left, err := p.parseBitwiseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenCaret {
		p.advance()
		right, err := p.parseBitwiseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpBitwiseXor,
			Right: right,
		}
	}

	return left, nil
}

// parseBitwiseAndExpr parses bitwise AND expressions (&).
func (p *parser) parseBitwiseAndExpr() (Expr, error) {
	left, err := p.parseShiftExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenAmpersand {
		p.advance()
		right, err := p.parseShiftExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpBitwiseAnd,
			Right: right,
		}
	}

	return left, nil
}

// parseShiftExpr parses bitwise shift expressions (<< and >>).
// These have the highest precedence among bitwise operators.
func (p *parser) parseShiftExpr() (Expr, error) {
	left, err := p.parseAddExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenShiftLeft || p.current().typ == tokenShiftRight {
		var op BinaryOp
		if p.current().typ == tokenShiftLeft {
			op = OpShiftLeft
		} else {
			op = OpShiftRight
		}
		p.advance()
		right, err := p.parseAddExpr()
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

func (p *parser) parseAddExpr() (Expr, error) {
	left, err := p.parseJSONExpr()
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
		right, err := p.parseJSONExpr()
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

// parseJSONExpr parses JSON extraction operators (-> and ->>).
// These have higher precedence than arithmetic operators.
// Syntax: expr -> key, expr ->> key, expr -> index, expr ->> index
func (p *parser) parseJSONExpr() (Expr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.current().typ == tokenOperator {
		var op BinaryOp
		switch p.current().value {
		case "->":
			op = OpJSONExtract
		case "->>":
			op = OpJSONText
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
	// Handle bitwise NOT operator (~)
	if p.current().typ == tokenTilde {
		p.advance()
		expr, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}

		return &UnaryExpr{
			Op:   OpBitwiseNot,
			Expr: expr,
		}, nil
	}

	if p.current().typ == tokenOperator {
		switch p.current().value {
		case "-":
			p.advance()
			// Special handling: if the next token is a numeric literal,
			// parse it as a negative literal to avoid overflow issues
			// (e.g., -9223372036854775808 which is INT64 min and doesn't fit as positive int64)
			if p.current().typ == tokenNumber {
				tok := p.advance()
				numStr := tok.value
				// Parse as negative number by prepending the minus sign
				// This allows us to correctly parse INT64 min (-9223372036854775808)
				// which cannot be represented as a positive int64 then negated
				negNumStr := "-" + numStr
				if strings.Contains(
					numStr,
					".",
				) ||
					strings.ContainsAny(numStr, "eE") {
					f, err := strconv.ParseFloat(
						negNumStr,
						64,
					)
					if err != nil {
						return nil, p.errorf(
							"invalid number: %s",
							negNumStr,
						)
					}

					return &Literal{
						Value: f,
						Type:  dukdb.TYPE_DOUBLE,
					}, nil
				}
				i, err := strconv.ParseInt(
					negNumStr,
					10,
					64,
				)
				if err != nil {
					return nil, p.errorf(
						"invalid number: %s",
						negNumStr,
					)
				}

				return &Literal{
					Value: i,
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

	return p.parsePostfixExpr()
}

// parsePostfixExpr parses postfix operators, specifically the PostgreSQL :: type cast syntax.
// The :: operator has very high precedence and binds tighter than arithmetic operators.
// Syntax: expr::type, where type can include parameters like varchar(100), numeric(10,2)
// Chaining is supported: '123'::text::integer
func (p *parser) parsePostfixExpr() (Expr, error) {
	expr, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop to handle chained casts like '123'::text::integer
	for p.current().typ == tokenOperator && p.current().value == "::" {
		p.advance() // consume ::

		// Parse the type name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected type name after ::")
		}
		typeName := strings.ToUpper(p.advance().value)
		targetType := parseTypeName(typeName)

		// Skip optional type parameters like (100) for varchar(100) or (10,2) for numeric(10,2)
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

		expr = &CastExpr{
			Expr:       expr,
			TargetType: targetType,
			TryCast:    false,
		}
	}

	return expr, nil
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

	case tokenLBracket:
		// Array literal: ['file1.csv', 'file2.csv']
		return p.parseArrayLiteral()

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

// parseArrayLiteral parses an array literal expression: ['file1.csv', 'file2.csv']
// This is used for specifying multiple files in table functions like read_csv.
func (p *parser) parseArrayLiteral() (Expr, error) {
	// Consume the opening bracket
	if _, err := p.expect(tokenLBracket); err != nil {
		return nil, err
	}

	var elements []Expr

	// Check for empty array
	if p.current().typ == tokenRBracket {
		p.advance()
		return &ArrayExpr{Elements: elements}, nil
	}

	// Parse elements separated by commas
	for {
		// Parse the element expression
		elem, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)

		// Check for comma or closing bracket
		if p.current().typ == tokenComma {
			p.advance()
			// Allow trailing comma: ['a', 'b',]
			if p.current().typ == tokenRBracket {
				break
			}
		} else {
			break
		}
	}

	// Expect closing bracket
	if _, err := p.expect(tokenRBracket); err != nil {
		return nil, err
	}

	return &ArrayExpr{Elements: elements}, nil
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
		return p.parseCast(false)
	case "TRY_CAST":
		return p.parseCast(true)
	case "EXISTS":
		return p.parseExists()
	case "EXTRACT":
		return p.parseExtract()
	case "INTERVAL":
		return p.parseInterval()
	case "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP":
		// SQL standard: these keywords work without parentheses
		if p.current().typ != tokenLParen {
			return &FunctionCall{Name: strings.ToUpper(name)}, nil
		}
		// Fall through to parseFunctionCall for CURRENT_DATE() syntax
	case "COLUMNS":
		if p.current().typ == tokenLParen {
			p.advance()
			if p.current().typ != tokenString {
				return nil, p.errorf("COLUMNS requires a string pattern argument")
			}
			raw := p.advance().value
			// Strip surrounding quotes and unescape
			pattern := raw[1 : len(raw)-1]
			pattern = strings.ReplaceAll(pattern, "''", "'")
			if _, err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			return &ColumnsExpr{Pattern: pattern}, nil
		}
		// Fall through to treat as regular identifier
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
			starExpr := &StarExpr{Table: name}
			if err := p.parseStarModifiers(starExpr); err != nil {
				return nil, err
			}
			return starExpr, nil
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
	if p.current().typ != tokenRParen && !p.isKeyword("ORDER") {
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		// Separate named arguments from positional arguments
		var positionalArgs []Expr
		for _, arg := range args {
			if na, ok := arg.(*NamedArgExpr); ok {
				if fn.NamedArgs == nil {
					fn.NamedArgs = make(map[string]Expr)
				}
				fn.NamedArgs[na.Name] = na.Value
			} else {
				positionalArgs = append(positionalArgs, arg)
			}
		}
		fn.Args = positionalArgs
	}

	// Check for ORDER BY within aggregate function
	// Syntax: STRING_AGG(expr, delimiter ORDER BY expr [ASC|DESC])
	//         LIST(expr ORDER BY expr [ASC|DESC])
	if p.isKeyword("ORDER") {
		p.advance() // consume ORDER
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		fn.OrderBy = orderBy
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}

	// Check for WITHIN GROUP (ORDER BY ...) — ordered-set aggregate syntax
	if p.isKeyword("WITHIN") {
		p.advance() // consume WITHIN
		if err := p.expectKeyword("GROUP"); err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("ORDER"); err != nil {
			return nil, p.errorf("expected ORDER BY inside WITHIN GROUP")
		}
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		if len(fn.OrderBy) > 0 {
			return nil, p.errorf("cannot use both internal ORDER BY and WITHIN GROUP")
		}
		fn.OrderBy = orderBy
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	// Check for window function OVER clause
	return p.maybeParseWindowExpr(fn)
}

// parseFunctionArgs parses function arguments, stopping at ORDER BY or closing paren.
// This is different from parseExprList because it needs to handle ORDER BY within aggregates
// and lambda expressions (x -> expr) as function arguments.
func (p *parser) parseFunctionArgs() ([]Expr, error) {
	var exprs []Expr

	for {
		expr, err := p.parseFunctionArg()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		// Stop if we see closing paren or ORDER BY
		if p.current().typ != tokenComma {
			break
		}

		// Peek ahead: if the next token after comma is ORDER, stop here
		// This handles: STRING_AGG(name, ',' ORDER BY name)
		p.advance() // consume comma

		// If after consuming comma we see ORDER, we need to put comma back conceptually
		// Actually, the comma was already consumed, so if next is ORDER, we're done with args
		if p.isKeyword("ORDER") {
			break
		}
	}

	return exprs, nil
}

// parseFunctionArg parses a single function argument, which may be a lambda expression
// or a named argument (name := expr).
// Lambda expressions use the -> operator, which is also used for JSON extract.
// Disambiguation: inside function arguments, ident -> non-string-non-number is a lambda.
// ident -> string_literal or ident -> number_literal is JSON extract.
// (ident, ident, ...) -> expr is a multi-parameter lambda.
func (p *parser) parseFunctionArg() (Expr, error) {
	// Check for named argument: ident := expr (used by struct_pack etc.)
	if p.current().typ == tokenIdent && !p.isKeyword("CASE") && !p.isKeyword("NOT") {
		if p.peek().typ == tokenOperator && p.peek().value == ":=" {
			name := p.advance().value // consume ident
			p.advance()               // consume :=
			valExpr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			return &NamedArgExpr{Name: name, Value: valExpr}, nil
		}
	}

	// Check for single-param lambda: ident -> expr (where expr is not a string/number literal)
	if p.current().typ == tokenIdent && !p.isKeyword("CASE") && !p.isKeyword("NOT") {
		if p.peek().typ == tokenOperator && p.peek().value == "->" {
			// Look two tokens ahead to disambiguate from JSON extract
			thirdTok := p.peekAt(2)
			if thirdTok.typ != tokenString && thirdTok.typ != tokenNumber {
				// This is a lambda: ident -> expr
				paramName := p.advance().value // consume ident
				p.advance()                    // consume ->
				body, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				return &LambdaExpr{Params: []string{paramName}, Body: body}, nil
			}
		}
	}

	// Check for multi-param lambda: (ident, ident, ...) -> expr
	if p.current().typ == tokenLParen {
		if params, ok := p.tryParseParenthesizedIdentList(); ok {
			// We successfully parsed (ident, ident, ...) and the next token is ->
			if p.current().typ == tokenOperator && p.current().value == "->" {
				p.advance() // consume ->
				body, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				return &LambdaExpr{Params: params, Body: body}, nil
			}
			// Not a lambda -- need to restore position. But tryParseParenthesizedIdentList
			// already handles backtracking on failure, so if we get here it means we
			// parsed the parens but there's no ->. We need to backtrack.
			// This case should not happen because tryParseParenthesizedIdentList checks for ->.
		}
	}

	return p.parseExpr()
}

// tryParseParenthesizedIdentList attempts to parse (ident, ident, ...) for multi-param lambdas.
// Returns the parameter names and true if successful. On failure, restores parser position.
// This method only returns true if the closing paren is followed by ->.
func (p *parser) tryParseParenthesizedIdentList() ([]string, bool) {
	savedPos := p.tokPos

	p.tokPos++ // consume (

	var params []string
	for {
		if p.current().typ != tokenIdent {
			p.tokPos = savedPos
			return nil, false
		}
		params = append(params, p.advance().value)
		if p.current().typ != tokenComma {
			break
		}
		p.tokPos++ // consume comma
	}

	if len(params) == 0 || p.current().typ != tokenRParen {
		p.tokPos = savedPos
		return nil, false
	}
	p.tokPos++ // consume )

	// Check if followed by ->
	if p.current().typ != tokenOperator || p.current().value != "->" {
		p.tokPos = savedPos
		return nil, false
	}

	return params, true
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
		// FILTER without OVER is allowed for standalone aggregates
		if windowExpr.Filter != nil {
			fn.Filter = windowExpr.Filter
		}
		// IGNORE NULLS still requires OVER clause
		if windowExpr.IgnoreNulls {
			return nil, p.errorf("IGNORE NULLS requires OVER clause")
		}
		return fn, nil
	}

	// Parse OVER clause
	p.advance() // consume OVER

	// Check for bare identifier: OVER w (without parens)
	if p.current().typ == tokenIdent && p.current().typ != tokenLParen {
		// Check it's not a keyword that starts a window spec
		val := strings.ToUpper(p.current().value)
		if val != "PARTITION" && val != "ORDER" && val != "ROWS" && val != "RANGE" && val != "GROUPS" {
			windowExpr.RefName = p.advance().value
			return windowExpr, nil
		}
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	// Check for named window reference inside parens: OVER (w ...)
	if p.current().typ == tokenIdent && !p.isKeyword("PARTITION") && !p.isKeyword("ORDER") &&
		!p.isKeyword("ROWS") && !p.isKeyword("RANGE") && !p.isKeyword("GROUPS") {
		// Could be a base window reference or a column reference
		// If followed by ORDER, PARTITION, ROWS, RANGE, GROUPS, or ), it's a window ref
		next := p.peek()
		isWindowRef := next.typ == tokenRParen ||
			(next.typ == tokenIdent && (strings.EqualFold(next.value, "PARTITION") ||
				strings.EqualFold(next.value, "ORDER") ||
				strings.EqualFold(next.value, "ROWS") ||
				strings.EqualFold(next.value, "RANGE") ||
				strings.EqualFold(next.value, "GROUPS")))
		if isWindowRef {
			windowExpr.RefName = p.advance().value
		}
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

// parseWindowDefs parses WINDOW name AS (spec) [, name AS (spec) ...].
func (p *parser) parseWindowDefs() ([]WindowDef, error) {
	var defs []WindowDef

	for {
		// Parse window name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected window name")
		}
		name := p.advance().value

		// Expect AS
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}

		// Expect (
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, err
		}

		def := WindowDef{Name: name}

		// Check for base window reference (identifier before PARTITION/ORDER/ROWS/RANGE/GROUPS/))
		if p.current().typ == tokenIdent && !p.isKeyword("PARTITION") && !p.isKeyword("ORDER") &&
			!p.isKeyword("ROWS") && !p.isKeyword("RANGE") && !p.isKeyword("GROUPS") {
			def.RefName = p.advance().value
		}

		// Parse window spec contents (reuse parseWindowSpec with a temp WindowExpr)
		tempExpr := &WindowExpr{}
		if err := p.parseWindowSpec(tempExpr); err != nil {
			return nil, err
		}
		def.PartitionBy = tempExpr.PartitionBy
		def.OrderBy = tempExpr.OrderBy
		def.Frame = tempExpr.Frame

		// Expect )
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}

		defs = append(defs, def)

		// Check for comma to continue
		if p.current().typ != tokenComma {
			break
		}
		p.advance() // consume comma
	}

	return defs, nil
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

func (p *parser) parseCast(tryCast bool) (Expr, error) {
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
		TryCast:    tryCast,
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
func parseIntervalValue(
	s string,
	unitKeyword string,
) (months int32, days int32, micros int64, err error) {
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
func applyIntervalUnit(
	val float64,
	unit string,
) (months int32, days int32, micros int64, err error) {
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
	default:
		return dukdb.TYPE_VARCHAR // Default to VARCHAR for unknown types
	}
}

func (p *parser) collectTypeSpec(
	stopTokens map[tokenType]bool,
	stopKeywords map[string]bool,
) (string, error) {
	var b strings.Builder
	lastWasWord := false
	depthParen := 0
	depthBracket := 0

	for {
		tok := p.current()
		if tok.typ == tokenEOF {
			break
		}
		if depthParen == 0 && depthBracket == 0 {
			if stopTokens != nil && stopTokens[tok.typ] {
				break
			}
			if tok.typ == tokenIdent && stopKeywords != nil {
				if stopKeywords[strings.ToUpper(tok.value)] {
					break
				}
			}
		}

		switch tok.typ {
		case tokenIdent, tokenNumber:
			if lastWasWord {
				b.WriteByte(' ')
			}
			b.WriteString(tok.value)
			lastWasWord = true
		case tokenString:
			if lastWasWord {
				b.WriteByte(' ')
			}
			b.WriteByte('\'')
			b.WriteString(tok.value)
			b.WriteByte('\'')
			lastWasWord = true
		case tokenComma:
			b.WriteByte(',')
			b.WriteByte(' ')
			lastWasWord = false
		case tokenLParen:
			b.WriteByte('(')
			depthParen++
			lastWasWord = false
		case tokenRParen:
			b.WriteByte(')')
			depthParen--
			lastWasWord = false
		case tokenLBracket:
			b.WriteByte('[')
			depthBracket++
			lastWasWord = false
		case tokenRBracket:
			b.WriteByte(']')
			depthBracket--
			lastWasWord = false
		case tokenDot:
			b.WriteByte('.')
			lastWasWord = false
		case tokenOperator:
			if tok.value != ":" {
				return "", p.errorf("unexpected token %q in type", tok.value)
			}
			b.WriteByte(':')
			lastWasWord = false
		default:
			return "", p.errorf("unexpected token %q in type", tok.value)
		}

		p.advance()
	}

	if depthParen != 0 || depthBracket != 0 {
		return "", p.errorf("unterminated type expression")
	}
	if b.Len() == 0 {
		return "", p.errorf("expected data type")
	}
	return b.String(), nil
}

// GetTypeName returns the SQL type name for a Type.
func GetTypeName(t dukdb.Type) string {
	switch t {
	case dukdb.TYPE_INVALID:
		return "INVALID"
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
	case dukdb.TYPE_TIME_TZ:
		return "TIME WITH TIME ZONE"
	case dukdb.TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case dukdb.TYPE_TIMESTAMP_S:
		return "TIMESTAMP_S"
	case dukdb.TYPE_TIMESTAMP_MS:
		return "TIMESTAMP_MS"
	case dukdb.TYPE_TIMESTAMP_NS:
		return "TIMESTAMP_NS"
	case dukdb.TYPE_TIMESTAMP_TZ:
		return "TIMESTAMP WITH TIME ZONE"
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
	case dukdb.TYPE_ENUM:
		return "ENUM"
	case dukdb.TYPE_LIST:
		return "LIST"
	case dukdb.TYPE_STRUCT:
		return "STRUCT"
	case dukdb.TYPE_MAP:
		return "MAP"
	case dukdb.TYPE_ARRAY:
		return "ARRAY"
	case dukdb.TYPE_UNION:
		return "UNION"
	case dukdb.TYPE_BIT:
		return "BIT"
	case dukdb.TYPE_ANY:
		return "ANY"
	case dukdb.TYPE_BIGNUM:
		return "BIGNUM"
	case dukdb.TYPE_SQLNULL:
		return "NULL"
	case dukdb.TYPE_JSON:
		return "JSON"
	case dukdb.TYPE_GEOMETRY:
		return "GEOMETRY"
	case dukdb.TYPE_LAMBDA:
		return "LAMBDA"
	case dukdb.TYPE_VARIANT:
		return "VARIANT"
	}
	return "UNKNOWN"
}

// parseExportDatabase parses an EXPORT DATABASE 'path' [(options)] statement.
func (p *parser) parseExportDatabase() (*ExportDatabaseStmt, error) {
	p.advance() // consume EXPORT
	if err := p.expectKeyword("DATABASE"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenString {
		return nil, p.errorf("expected string path after EXPORT DATABASE")
	}
	pathTok := p.advance()
	// Remove quotes from the path
	path := pathTok.value[1 : len(pathTok.value)-1]
	path = strings.ReplaceAll(path, "''", "'")

	options := make(map[string]string)
	if p.current().typ == tokenLParen {
		p.advance() // consume (
		for {
			if p.current().typ == tokenRParen {
				break
			}
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected option name")
			}
			key := strings.ToUpper(p.advance().value)

			var value string
			if p.current().typ == tokenString {
				tok := p.advance()
				value = tok.value[1 : len(tok.value)-1]
				value = strings.ReplaceAll(value, "''", "'")
			} else if p.current().typ == tokenIdent {
				value = strings.ToUpper(p.advance().value)
			} else if p.current().typ == tokenNumber {
				value = p.advance().value
			} else {
				return nil, p.errorf("expected option value for %s", key)
			}

			options[key] = value

			if p.current().typ == tokenComma {
				p.advance()
			} else if p.current().typ != tokenRParen {
				return nil, p.errorf("expected comma or ) in export options")
			}
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	return &ExportDatabaseStmt{Path: path, Options: options}, nil
}

// parseImportDatabase parses an IMPORT DATABASE 'path' statement.
func (p *parser) parseImportDatabase() (*ImportDatabaseStmt, error) {
	p.advance() // consume IMPORT
	if err := p.expectKeyword("DATABASE"); err != nil {
		return nil, err
	}

	if p.current().typ != tokenString {
		return nil, p.errorf("expected string path after IMPORT DATABASE")
	}
	pathTok := p.advance()
	// Remove quotes from the path
	path := pathTok.value[1 : len(pathTok.value)-1]
	path = strings.ReplaceAll(path, "''", "'")

	return &ImportDatabaseStmt{Path: path}, nil
}

// parseInstall parses an INSTALL extension_name statement.
func (p *parser) parseInstall() (*InstallStmt, error) {
	p.advance() // consume INSTALL
	if p.current().typ != tokenIdent && p.current().typ != tokenString {
		return nil, p.errorf("expected extension name after INSTALL")
	}
	name := p.advance().value
	// Strip quotes from string tokens
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		name = name[1 : len(name)-1]
	}
	return &InstallStmt{Name: name}, nil
}

// parseLoad parses a LOAD extension_name statement.
func (p *parser) parseLoad() (*LoadStmt, error) {
	p.advance() // consume LOAD
	if p.current().typ != tokenIdent && p.current().typ != tokenString {
		return nil, p.errorf("expected extension name after LOAD")
	}
	name := p.advance().value
	// Strip quotes from string tokens
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		name = name[1 : len(name)-1]
	}
	return &LoadStmt{Name: name}, nil
}

// parseAttach parses an ATTACH [DATABASE] 'path' [AS alias] [(options)] statement.
func (p *parser) parseAttach() (Statement, error) {
	p.advance() // consume ATTACH
	// Optional DATABASE keyword
	if p.isKeyword("DATABASE") {
		p.advance()
	}

	// Path (string literal)
	if p.current().typ != tokenString {
		return nil, p.errorf("expected database path string")
	}
	pathTok := p.advance()
	path := pathTok.value
	// Remove quotes
	if len(path) >= 2 && (path[0] == '\'' || path[0] == '"') {
		path = path[1 : len(path)-1]
	}

	stmt := &AttachStmt{Path: path, Options: make(map[string]string)}

	// Optional AS alias
	if p.isKeyword("AS") {
		p.advance()
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected database alias")
		}
		stmt.Alias = p.advance().value
	}

	// Optional (READ_ONLY, TYPE 'duckdb', etc.)
	if p.current().typ == tokenLParen {
		p.advance()
		for p.current().typ != tokenRParen && p.current().typ != tokenEOF {
			if p.current().typ != tokenIdent {
				break
			}
			key := strings.ToUpper(p.advance().value)
			if key == "READ_ONLY" {
				stmt.ReadOnly = true
			} else {
				// key value pair
				if p.current().typ == tokenString {
					val := p.advance().value
					if len(val) >= 2 && (val[0] == '\'' || val[0] == '"') {
						val = val[1 : len(val)-1]
					}
					stmt.Options[key] = val
				} else if p.current().typ == tokenIdent {
					stmt.Options[key] = p.advance().value
				}
			}
			if p.current().typ != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

// parseDetach parses a DETACH [DATABASE] [IF EXISTS] name statement.
func (p *parser) parseDetach() (Statement, error) {
	p.advance() // consume DETACH
	if p.isKeyword("DATABASE") {
		p.advance()
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
		return nil, p.errorf("expected database name")
	}
	name := p.advance().value

	return &DetachStmt{Name: name, IfExists: ifExists}, nil
}

// parseUse parses a USE database[.schema] statement.
func (p *parser) parseUse() (Statement, error) {
	p.advance() // consume USE
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected database name")
	}
	db := p.advance().value

	schema := ""
	if p.current().typ == tokenDot {
		p.advance()
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected schema name")
		}
		schema = p.advance().value
	}

	return &UseStmt{Database: db, Schema: schema}, nil
}

// parseCreateDatabase parses CREATE DATABASE [IF NOT EXISTS] name.
func (p *parser) parseCreateDatabase() (Statement, error) {
	p.advance() // consume DATABASE

	ifNotExists := false
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected database name")
	}
	name := p.advance().value

	return &CreateDatabaseStmt{Name: name, IfNotExists: ifNotExists}, nil
}

// parseDropDatabase parses DROP DATABASE [IF EXISTS] name.
func (p *parser) parseDropDatabase() (Statement, error) {
	p.advance() // consume DATABASE

	ifExists := false
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected database name")
	}
	name := p.advance().value

	return &DropDatabaseStmt{Name: name, IfExists: ifExists}, nil
}
