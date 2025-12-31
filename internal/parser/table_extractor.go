// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import (
	"sort"
)

// TableExtractor implements visitor pattern to extract table references from SQL queries.
// It traverses the AST and collects all table references while handling subqueries recursively.
type TableExtractor struct {
	tables    map[EnhancedTableRef]struct{} // Use map for automatic deduplication
	qualified bool                          // Whether to resolve qualified names
}

// NewTableExtractor creates a new TableExtractor with the specified qualified name mode.
func NewTableExtractor(qualified bool) *TableExtractor {
	return &TableExtractor{
		tables:    make(map[EnhancedTableRef]struct{}),
		qualified: qualified,
	}
}

// GetTables returns the extracted table names as a sorted, deduplicated slice.
func (te *TableExtractor) GetTables() []string {
	// Convert EnhancedTableRef map to string slice
	var tableNames []string
	for ref := range te.tables {
		if te.qualified {
			// Return qualified name (schema.table or catalog.schema.table)
			tableNames = append(tableNames, ref.QualifiedName())
		} else {
			// Return just table name (unqualified)
			tableNames = append(tableNames, ref.Table)
		}
	}

	// Deduplicate unqualified names (CRITICAL FIX for Issue #7)
	// Multiple qualified names can map to same unqualified string
	seen := make(map[string]bool)
	dedupedNames := []string{}
	for _, name := range tableNames {
		if !seen[name] {
			seen[name] = true
			dedupedNames = append(dedupedNames, name)
		}
	}
	tableNames = dedupedNames

	// Sort for deterministic output (alphabetical order)
	sort.Strings(tableNames)

	return tableNames
}

// VisitSelectStmt extracts table references from SELECT statements.
func (te *TableExtractor) VisitSelectStmt(stmt *SelectStmt) {
	// Extract FROM clause tables
	if stmt.From != nil {
		// Extract tables from FROM clause
		for _, tableRef := range stmt.From.Tables {
			te.visitTableRef(&tableRef)
		}

		// Extract tables from JOIN clauses (all types: INNER, LEFT, RIGHT, FULL, CROSS)
		for _, join := range stmt.From.Joins {
			te.visitTableRef(&join.Table)
		}
	}

	// Recursively visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)
	}

	// Recursively visit HAVING clause subqueries
	if stmt.Having != nil {
		te.visitExpr(stmt.Having)
	}

	// Recursively visit SELECT list subqueries
	for _, col := range stmt.Columns {
		if col.Expr != nil {
			te.visitExpr(col.Expr)
		}
	}

	// NOTE: CTEs NOT supported - requires AST enhancement
	// Once SelectStmt has CTEs field, add CTE handling here
}

// VisitInsertStmt extracts table references from INSERT statements.
func (te *TableExtractor) VisitInsertStmt(stmt *InsertStmt) {
	// Extract INSERT INTO target table
	// Current AST: InsertStmt has Schema and Table fields (not full TableRef)
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// If INSERT...SELECT, visit SELECT subquery
	if stmt.Select != nil {
		te.VisitSelectStmt(stmt.Select)
	}
}

// VisitUpdateStmt extracts table references from UPDATE statements.
func (te *TableExtractor) VisitUpdateStmt(stmt *UpdateStmt) {
	// Extract UPDATE target table
	// Current AST: UpdateStmt has Schema and Table fields (not full TableRef)
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// NOTE: UPDATE...FROM NOT supported - requires AST enhancement
	// UpdateStmt currently has no From field

	// Visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)
	}

	// Visit SET clause subqueries (rare but possible)
	for _, setClause := range stmt.Set {
		if setClause.Value != nil {
			te.visitExpr(setClause.Value)
		}
	}
}

// VisitDeleteStmt extracts table references from DELETE statements.
func (te *TableExtractor) VisitDeleteStmt(stmt *DeleteStmt) {
	// Extract DELETE FROM target table
	// Current AST: DeleteStmt has Schema and Table fields (not full TableRef)
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// Visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)
	}
}

// visitTableRef extracts table from AST TableRef
func (te *TableExtractor) visitTableRef(astRef *TableRef) {
	// Current AST TableRef has: Catalog, Schema, TableName, Alias, Subquery
	if astRef.Subquery != nil {
		// This is a subquery in FROM clause, recurse into it
		te.VisitSelectStmt(astRef.Subquery)
		return
	}

	// This is a real table reference
	ref := EnhancedTableRef{
		Catalog: astRef.Catalog,
		Schema:  astRef.Schema,
		Table:   astRef.TableName, // AST uses "TableName" field, not "Table"
		Alias:   astRef.Alias,
	}

	te.tables[ref] = struct{}{} // Deduplication via map
}

// visitExpr traverses expressions to extract subqueries
// Handles: InSubqueryExpr, ExistsExpr, and SelectStmt as expression
func (te *TableExtractor) visitExpr(expr Expr) {
	switch e := expr.(type) {
	case *InSubqueryExpr:
		// Subquery in WHERE...IN clause
		te.VisitSelectStmt(e.Subquery)

	case *ExistsExpr:
		// Subquery in WHERE EXISTS clause
		te.VisitSelectStmt(e.Subquery)

	case *SelectStmt:
		// SelectStmt can be used as scalar subquery expression
		te.VisitSelectStmt(e)

	case *BinaryExpr:
		// Recurse into left and right sides for nested subqueries
		te.visitExpr(e.Left)
		te.visitExpr(e.Right)

	case *UnaryExpr:
		// Recurse into operand
		te.visitExpr(e.Expr)

	case *BetweenExpr:
		// Check all three expressions
		te.visitExpr(e.Expr)
		te.visitExpr(e.Low)
		te.visitExpr(e.High)

	case *InListExpr:
		// Check expression and all list values
		te.visitExpr(e.Expr)
		for _, val := range e.Values {
			te.visitExpr(val)
		}

	case *CaseExpr:
		// Check operand, all WHEN conditions/results, and ELSE
		if e.Operand != nil {
			te.visitExpr(e.Operand)
		}
		for _, when := range e.Whens {
			te.visitExpr(when.Condition)
			te.visitExpr(when.Result)
		}
		if e.Else != nil {
			te.visitExpr(e.Else)
		}

	case *FunctionCall:
		// Check all function arguments for subqueries
		for _, arg := range e.Args {
			te.visitExpr(arg)
		}

	case *CastExpr:
		te.visitExpr(e.Expr)

	// Leaf expression types (no recursion needed):
	case *ColumnRef, *Literal, *Parameter, *StarExpr:
		// No subqueries in these
		return

	default:
		// Unknown expression type - no action
		return
	}
}