// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import (
	"strconv"
)

// parseCreateView parses a CREATE VIEW statement.
// Syntax: CREATE VIEW [IF NOT EXISTS] [schema.]view AS select_statement
func (p *parser) parseCreateView() (*CreateViewStmt, error) {
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	stmt := &CreateViewStmt{
		Schema: "main", // default schema
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

	// View name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected view name")
	}
	stmt.View = p.advance().value

	// Check for schema.view syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.View
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected view name after dot")
		}
		stmt.View = p.advance().value
	}

	// AS
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	// SELECT statement
	var selectStmt *SelectStmt
	var err error
	if p.isKeyword("WITH") {
		selectStmt, err = p.parseWithSelect()
	} else if p.isKeyword("SELECT") {
		selectStmt, err = p.parseSelect()
	} else {
		return nil, p.errorf("expected SELECT or WITH after AS")
	}
	if err != nil {
		return nil, err
	}
	stmt.Query = selectStmt

	return stmt, nil
}

// parseDropView parses a DROP VIEW statement.
// Syntax: DROP VIEW [IF EXISTS] [schema.]view
func (p *parser) parseDropView() (*DropViewStmt, error) {
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	stmt := &DropViewStmt{
		Schema: "main", // default schema
	}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// View name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected view name")
	}
	stmt.View = p.advance().value

	// Check for schema.view syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.View
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected view name after dot")
		}
		stmt.View = p.advance().value
	}

	return stmt, nil
}

// parseCreateIndex parses a CREATE INDEX statement.
// Syntax: CREATE [UNIQUE] INDEX [IF NOT EXISTS] index ON table (column1, ...)
func (p *parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{
		Schema: "main", // default schema
	}

	// Check for UNIQUE
	if p.isKeyword("UNIQUE") {
		stmt.IsUnique = true
		p.advance()
	}

	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
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

	// Index name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected index name")
	}
	stmt.Index = p.advance().value

	// Check for schema.index syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Index
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected index name after dot")
		}
		stmt.Index = p.advance().value
	}

	// ON
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	// Table name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected table name")
	}
	stmt.Table = p.advance().value

	// Check for schema.table syntax
	if p.current().typ == tokenDot {
		p.advance()
		// Schema was specified with table name
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected table name after dot")
		}
		stmt.Table = p.advance().value
	}

	// Column list
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	for {
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected column name")
		}
		stmt.Columns = append(stmt.Columns, p.advance().value)

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

// parseDropIndex parses a DROP INDEX statement.
// Syntax: DROP INDEX [IF EXISTS] [schema.]index
func (p *parser) parseDropIndex() (*DropIndexStmt, error) {
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}

	stmt := &DropIndexStmt{
		Schema: "main", // default schema
	}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// Index name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected index name")
	}
	stmt.Index = p.advance().value

	// Check for schema.index syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Index
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected index name after dot")
		}
		stmt.Index = p.advance().value
	}

	return stmt, nil
}

// parseCreateSequence parses a CREATE SEQUENCE statement.
// Syntax: CREATE SEQUENCE [IF NOT EXISTS] [schema.]sequence
//
//	[START WITH n] [INCREMENT BY n] [MINVALUE n | NO MINVALUE]
//	[MAXVALUE n | NO MAXVALUE] [CYCLE | NO CYCLE]
func (p *parser) parseCreateSequence() (*CreateSequenceStmt, error) {
	if err := p.expectKeyword("SEQUENCE"); err != nil {
		return nil, err
	}

	stmt := &CreateSequenceStmt{
		Schema:      "main", // default schema
		StartWith:   1,
		IncrementBy: 1,
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

	// Sequence name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected sequence name")
	}
	stmt.Sequence = p.advance().value

	// Check for schema.sequence syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Sequence
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected sequence name after dot")
		}
		stmt.Sequence = p.advance().value
	}

	// Parse optional sequence options
	for {
		if p.isKeyword("START") {
			p.advance()
			if err := p.expectKeyword("WITH"); err != nil {
				return nil, err
			}
			if p.current().typ != tokenNumber {
				return nil, p.errorf("expected number after START WITH")
			}
			val, err := strconv.ParseInt(p.advance().value, 10, 64)
			if err != nil {
				return nil, p.errorf("invalid number for START WITH: %v", err)
			}
			stmt.StartWith = val
		} else if p.isKeyword("INCREMENT") {
			p.advance()
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			// Handle optional negative sign
			negative := false
			if p.current().typ == tokenOperator && p.current().value == "-" {
				negative = true
				p.advance()
			}
			if p.current().typ != tokenNumber {
				return nil, p.errorf("expected number after INCREMENT BY")
			}
			val, err := strconv.ParseInt(p.advance().value, 10, 64)
			if err != nil {
				return nil, p.errorf("invalid number for INCREMENT BY: %v", err)
			}
			if negative {
				val = -val
			}
			stmt.IncrementBy = val
		} else if p.isKeyword("MINVALUE") {
			p.advance()
			if p.current().typ != tokenNumber {
				return nil, p.errorf("expected number after MINVALUE")
			}
			val, err := strconv.ParseInt(p.advance().value, 10, 64)
			if err != nil {
				return nil, p.errorf("invalid number for MINVALUE: %v", err)
			}
			stmt.MinValue = &val
		} else if p.isKeyword("NO") {
			p.advance()
			if p.isKeyword("MINVALUE") {
				p.advance()
				stmt.MinValue = nil
			} else if p.isKeyword("MAXVALUE") {
				p.advance()
				stmt.MaxValue = nil
			} else if p.isKeyword("CYCLE") {
				p.advance()
				stmt.IsCycle = false
			} else {
				return nil, p.errorf("expected MINVALUE, MAXVALUE, or CYCLE after NO")
			}
		} else if p.isKeyword("MAXVALUE") {
			p.advance()
			if p.current().typ != tokenNumber {
				return nil, p.errorf("expected number after MAXVALUE")
			}
			val, err := strconv.ParseInt(p.advance().value, 10, 64)
			if err != nil {
				return nil, p.errorf("invalid number for MAXVALUE: %v", err)
			}
			stmt.MaxValue = &val
		} else if p.isKeyword("CYCLE") {
			p.advance()
			stmt.IsCycle = true
		} else {
			// No more sequence options
			break
		}
	}

	return stmt, nil
}

// parseDropSequence parses a DROP SEQUENCE statement.
// Syntax: DROP SEQUENCE [IF EXISTS] [schema.]sequence
func (p *parser) parseDropSequence() (*DropSequenceStmt, error) {
	if err := p.expectKeyword("SEQUENCE"); err != nil {
		return nil, err
	}

	stmt := &DropSequenceStmt{
		Schema: "main", // default schema
	}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// Sequence name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected sequence name")
	}
	stmt.Sequence = p.advance().value

	// Check for schema.sequence syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Sequence
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected sequence name after dot")
		}
		stmt.Sequence = p.advance().value
	}

	return stmt, nil
}

// parseCreateSchema parses a CREATE SCHEMA statement.
// Syntax: CREATE SCHEMA [IF NOT EXISTS] schema
func (p *parser) parseCreateSchema() (*CreateSchemaStmt, error) {
	if err := p.expectKeyword("SCHEMA"); err != nil {
		return nil, err
	}

	stmt := &CreateSchemaStmt{}

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

	// Schema name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected schema name")
	}
	stmt.Schema = p.advance().value

	return stmt, nil
}

// parseDropSchema parses a DROP SCHEMA statement.
// Syntax: DROP SCHEMA [IF EXISTS] schema [CASCADE | RESTRICT]
func (p *parser) parseDropSchema() (*DropSchemaStmt, error) {
	if err := p.expectKeyword("SCHEMA"); err != nil {
		return nil, err
	}

	stmt := &DropSchemaStmt{}

	// IF EXISTS
	if p.isKeyword("IF") {
		p.advance()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	// Schema name
	if p.current().typ != tokenIdent {
		return nil, p.errorf("expected schema name")
	}
	stmt.Schema = p.advance().value

	// CASCADE or RESTRICT
	if p.isKeyword("CASCADE") {
		p.advance()
		stmt.Cascade = true
	} else if p.isKeyword("RESTRICT") {
		p.advance()
		stmt.Cascade = false
	}

	return stmt, nil
}

// parseAlterTable parses an ALTER TABLE statement.
// Syntax: ALTER TABLE [IF EXISTS] [schema.]table
//
//	RENAME TO new_table
//	| RENAME COLUMN old_name TO new_name
//	| DROP COLUMN column_name
//	| ADD COLUMN column_def
func (p *parser) parseAlterTable() (*AlterTableStmt, error) {
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	stmt := &AlterTableStmt{
		Schema: "main", // default schema
	}

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
		return nil, p.errorf("expected table name")
	}
	stmt.Table = p.advance().value

	// Check for schema.table syntax
	if p.current().typ == tokenDot {
		p.advance()
		stmt.Schema = stmt.Table
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected table name after dot")
		}
		stmt.Table = p.advance().value
	}

	// Parse ALTER operation
	if p.isKeyword("RENAME") {
		p.advance()
		if p.isKeyword("TO") {
			// RENAME TO new_table_name
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected new table name")
			}
			stmt.Operation = AlterTableRenameTo
			stmt.NewTableName = p.advance().value
		} else if p.isKeyword("COLUMN") {
			// RENAME COLUMN old_name TO new_name
			p.advance()
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected column name")
			}
			stmt.Operation = AlterTableRenameColumn
			stmt.OldColumn = p.advance().value
			if err := p.expectKeyword("TO"); err != nil {
				return nil, err
			}
			if p.current().typ != tokenIdent {
				return nil, p.errorf("expected new column name")
			}
			stmt.NewColumn = p.advance().value
		} else {
			return nil, p.errorf("expected TO or COLUMN after RENAME")
		}
	} else if p.isKeyword("DROP") {
		p.advance()
		if err := p.expectKeyword("COLUMN"); err != nil {
			return nil, err
		}
		if p.current().typ != tokenIdent {
			return nil, p.errorf("expected column name")
		}
		stmt.Operation = AlterTableDropColumn
		stmt.DropColumn = p.advance().value
	} else if p.isKeyword("ADD") {
		p.advance()
		if err := p.expectKeyword("COLUMN"); err != nil {
			return nil, err
		}
		colDef, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Operation = AlterTableAddColumn
		stmt.AddColumn = &colDef
	} else if p.isKeyword("SET") {
		p.advance()
		stmt.Operation = AlterTableSetOption
		// SET option parsing would go here (future extension)
		return nil, p.errorf("ALTER TABLE SET not yet implemented")
	} else {
		return nil, p.errorf("expected RENAME, DROP, ADD, or SET after ALTER TABLE")
	}

	return stmt, nil
}
