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
	cteNames  map[string]struct{}           // Track CTE names to exclude from table references
}

// NewTableExtractor creates a new TableExtractor with the specified qualified name mode.
func NewTableExtractor(qualified bool) *TableExtractor {
	return &TableExtractor{
		tables:    make(map[EnhancedTableRef]struct{}),
		qualified: qualified,
		cteNames:  make(map[string]struct{}),
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
	// Handle CTEs (Common Table Expressions / WITH clause)
	// 1. Register CTE names so they are excluded from table references
	// 2. Visit CTE queries to extract tables from CTE definitions
	for _, cte := range stmt.CTEs {
		// Register CTE name to exclude from table references
		te.cteNames[cte.Name] = struct{}{}

		// Visit CTE query to extract tables from CTE definitions
		if cte.Query != nil {
			te.VisitSelectStmt(cte.Query)
		}
	}

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

	// Handle set operations (UNION/INTERSECT/EXCEPT)
	if stmt.Right != nil {
		te.VisitSelectStmt(stmt.Right)
	}
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

	// Extract tables from FROM clause if present (UPDATE...FROM syntax)
	if stmt.From != nil {
		for _, tableRef := range stmt.From.Tables {
			te.visitTableRef(&tableRef)
		}
		for _, join := range stmt.From.Joins {
			te.visitTableRef(&join.Table)
		}
	}

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

// VisitCreateTableStmt extracts table references from CREATE TABLE statements.
func (te *TableExtractor) VisitCreateTableStmt(stmt *CreateTableStmt) {
	// Extract the table being created
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// If CREATE TABLE AS SELECT, also extract from the SELECT
	if stmt.AsSelect != nil {
		te.VisitSelectStmt(stmt.AsSelect)
	}
}

// VisitDropTableStmt extracts table references from DROP TABLE statements.
func (te *TableExtractor) VisitDropTableStmt(stmt *DropTableStmt) {
	// Extract the table being dropped
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}
}

// VisitBeginStmt is a no-op for BEGIN statements (no table references).
func (te *TableExtractor) VisitBeginStmt(stmt *BeginStmt) {
	// No table references in BEGIN statements
}

// VisitCommitStmt is a no-op for COMMIT statements (no table references).
func (te *TableExtractor) VisitCommitStmt(stmt *CommitStmt) {
	// No table references in COMMIT statements
}

// VisitRollbackStmt is a no-op for ROLLBACK statements (no table references).
func (te *TableExtractor) VisitRollbackStmt(stmt *RollbackStmt) {
	// No table references in ROLLBACK statements
}

// VisitCopyStmt extracts table references from COPY statements.
func (te *TableExtractor) VisitCopyStmt(stmt *CopyStmt) {
	// Extract the table being copied to/from
	if stmt.TableName != "" {
		ref := EnhancedTableRef{
			Schema: stmt.Schema,
			Table:  stmt.TableName,
		}
		te.tables[ref] = struct{}{}
	}

	// If COPY (SELECT...) TO, visit the SELECT query
	if stmt.Query != nil {
		te.VisitSelectStmt(stmt.Query)
	}
}

// VisitCreateViewStmt extracts table references from CREATE VIEW statements.
func (te *TableExtractor) VisitCreateViewStmt(stmt *CreateViewStmt) {
	// Visit the view definition query to extract table references
	if stmt.Query != nil {
		te.VisitSelectStmt(stmt.Query)
	}
}

// VisitDropViewStmt is a no-op for DROP VIEW statements (no table references).
func (te *TableExtractor) VisitDropViewStmt(stmt *DropViewStmt) {
	// No table references in DROP VIEW statements
}

// VisitCreateIndexStmt extracts table references from CREATE INDEX statements.
func (te *TableExtractor) VisitCreateIndexStmt(stmt *CreateIndexStmt) {
	// Extract the table the index is being created on
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}
}

// VisitDropIndexStmt is a no-op for DROP INDEX statements (no table references).
func (te *TableExtractor) VisitDropIndexStmt(stmt *DropIndexStmt) {
	// No table references in DROP INDEX statements
}

// VisitCreateSequenceStmt is a no-op for CREATE SEQUENCE statements (no table references).
func (te *TableExtractor) VisitCreateSequenceStmt(stmt *CreateSequenceStmt) {
	// No table references in CREATE SEQUENCE statements
}

// VisitDropSequenceStmt is a no-op for DROP SEQUENCE statements (no table references).
func (te *TableExtractor) VisitDropSequenceStmt(stmt *DropSequenceStmt) {
	// No table references in DROP SEQUENCE statements
}

// VisitCreateSchemaStmt is a no-op for CREATE SCHEMA statements (no table references).
func (te *TableExtractor) VisitCreateSchemaStmt(stmt *CreateSchemaStmt) {
	// No table references in CREATE SCHEMA statements
}

// VisitDropSchemaStmt is a no-op for DROP SCHEMA statements (no table references).
func (te *TableExtractor) VisitDropSchemaStmt(stmt *DropSchemaStmt) {
	// No table references in DROP SCHEMA statements
}

// VisitAlterTableStmt extracts table references from ALTER TABLE statements.
func (te *TableExtractor) VisitAlterTableStmt(stmt *AlterTableStmt) {
	// Extract the table being altered
	ref := EnhancedTableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}
}

// VisitPivotStmt extracts table references from PIVOT statements.
func (te *TableExtractor) VisitPivotStmt(stmt *PivotStmt) {
	// Extract the source table being pivoted
	te.visitTableRef(&stmt.Source)

	// Visit expressions in PivotOn for any subqueries
	for _, expr := range stmt.PivotOn {
		te.visitExpr(expr)
	}

	// Visit expressions in Using aggregates for any subqueries
	for _, agg := range stmt.Using {
		if agg.Expr != nil {
			te.visitExpr(agg.Expr)
		}
	}

	// Visit GroupBy expressions for any subqueries
	for _, expr := range stmt.GroupBy {
		te.visitExpr(expr)
	}
}

// VisitUnpivotStmt extracts table references from UNPIVOT statements.
func (te *TableExtractor) VisitUnpivotStmt(stmt *UnpivotStmt) {
	// Extract the source table being unpivoted
	te.visitTableRef(&stmt.Source)
}

// VisitGroupingSetExpr visits GROUPING SETS, ROLLUP, or CUBE expressions.
// These don't contain table references directly, but their contained expressions might.
func (te *TableExtractor) VisitGroupingSetExpr(expr *GroupingSetExpr) {
	// Visit all expressions in each grouping set (Exprs is [][]Expr)
	for _, exprSet := range expr.Exprs {
		for _, e := range exprSet {
			te.visitExpr(e)
		}
	}
}

// VisitCreateSecretStmt is a no-op for CREATE SECRET statements (no table references).
func (te *TableExtractor) VisitCreateSecretStmt(stmt *CreateSecretStmt) {
	// No table references in CREATE SECRET statements
}

// VisitDropSecretStmt is a no-op for DROP SECRET statements (no table references).
func (te *TableExtractor) VisitDropSecretStmt(stmt *DropSecretStmt) {
	// No table references in DROP SECRET statements
}

// VisitAlterSecretStmt is a no-op for ALTER SECRET statements (no table references).
func (te *TableExtractor) VisitAlterSecretStmt(stmt *AlterSecretStmt) {
	// No table references in ALTER SECRET statements
}

// VisitPragmaStmt is a no-op for PRAGMA statements (no table references).
func (te *TableExtractor) VisitPragmaStmt(stmt *PragmaStmt) {
	// No table references in PRAGMA statements
}

// VisitExplainStmt extracts table references from EXPLAIN statements.
func (te *TableExtractor) VisitExplainStmt(stmt *ExplainStmt) {
	// Visit the underlying query to extract table references
	if stmt.Query != nil {
		switch q := stmt.Query.(type) {
		case *SelectStmt:
			te.VisitSelectStmt(q)
		case *InsertStmt:
			te.VisitInsertStmt(q)
		case *UpdateStmt:
			te.VisitUpdateStmt(q)
		case *DeleteStmt:
			te.VisitDeleteStmt(q)
		}
	}
}

// VisitVacuumStmt extracts table references from VACUUM statements.
func (te *TableExtractor) VisitVacuumStmt(stmt *VacuumStmt) {
	// Extract the table being vacuumed (if specified)
	if stmt.TableName != "" {
		ref := EnhancedTableRef{
			Schema: stmt.Schema,
			Table:  stmt.TableName,
		}
		te.tables[ref] = struct{}{}
	}
}

// VisitAnalyzeStmt extracts table references from ANALYZE statements.
func (te *TableExtractor) VisitAnalyzeStmt(stmt *AnalyzeStmt) {
	// Extract the table being analyzed (if specified)
	if stmt.TableName != "" {
		ref := EnhancedTableRef{
			Schema: stmt.Schema,
			Table:  stmt.TableName,
		}
		te.tables[ref] = struct{}{}
	}
}

// VisitCheckpointStmt is a no-op for CHECKPOINT statements (no table references).
func (te *TableExtractor) VisitCheckpointStmt(stmt *CheckpointStmt) {
	// No table references in CHECKPOINT statements
}

// VisitCreateFunctionStmt is a no-op for CREATE FUNCTION statements (no table references).
// UDF bodies are opaque strings - we don't parse them for table references.
func (te *TableExtractor) VisitCreateFunctionStmt(stmt *CreateFunctionStmt) {
	// No table references in CREATE FUNCTION statements
}

// VisitSavepointStmt is a no-op for SAVEPOINT statements (no table references).
func (te *TableExtractor) VisitSavepointStmt(stmt *SavepointStmt) {
	// No table references in SAVEPOINT statements
}

// VisitRollbackToSavepointStmt is a no-op for ROLLBACK TO SAVEPOINT statements (no table references).
func (te *TableExtractor) VisitRollbackToSavepointStmt(stmt *RollbackToSavepointStmt) {
	// No table references in ROLLBACK TO SAVEPOINT statements
}

// VisitReleaseSavepointStmt is a no-op for RELEASE SAVEPOINT statements (no table references).
func (te *TableExtractor) VisitReleaseSavepointStmt(stmt *ReleaseSavepointStmt) {
	// No table references in RELEASE SAVEPOINT statements
}

// VisitSetStmt is a no-op for SET statements (no table references).
func (te *TableExtractor) VisitSetStmt(stmt *SetStmt) {
	// No table references in SET statements
}

// VisitShowStmt is a no-op for SHOW statements (no table references).
func (te *TableExtractor) VisitShowStmt(stmt *ShowStmt) {
	// No table references in SHOW statements
}

// VisitPrepareStmt is a no-op for PREPARE statements (no table references at this level).
func (te *TableExtractor) VisitPrepareStmt(stmt *PrepareStmt) {
	// No table references in PREPARE statements
}

// VisitExecuteStmt is a no-op for EXECUTE statements (no table references).
func (te *TableExtractor) VisitExecuteStmt(stmt *ExecuteStmt) {
	// No table references in EXECUTE statements
}

// VisitDeallocateStmt is a no-op for DEALLOCATE statements (no table references).
func (te *TableExtractor) VisitDeallocateStmt(stmt *DeallocateStmt) {
	// No table references in DEALLOCATE statements
}

// VisitCreateTypeStmt is a no-op for CREATE TYPE statements (no table references).
func (te *TableExtractor) VisitCreateTypeStmt(stmt *CreateTypeStmt) {
	// No table references in CREATE TYPE statements
}

// VisitDropTypeStmt is a no-op for DROP TYPE statements (no table references).
func (te *TableExtractor) VisitDropTypeStmt(stmt *DropTypeStmt) {
	// No table references in DROP TYPE statements
}

// VisitCreateMacroStmt is a no-op for CREATE MACRO statements (no table references).
func (te *TableExtractor) VisitCreateMacroStmt(stmt *CreateMacroStmt) {
	// No table references in CREATE MACRO statements
}

// VisitDropMacroStmt is a no-op for DROP MACRO statements (no table references).
func (te *TableExtractor) VisitDropMacroStmt(stmt *DropMacroStmt) {
	// No table references in DROP MACRO statements
}

// VisitExportDatabaseStmt is a no-op for EXPORT DATABASE statements (no table references).
func (te *TableExtractor) VisitExportDatabaseStmt(stmt *ExportDatabaseStmt) {
	// No table references in EXPORT DATABASE statements
}

// VisitImportDatabaseStmt is a no-op for IMPORT DATABASE statements (no table references).
func (te *TableExtractor) VisitImportDatabaseStmt(stmt *ImportDatabaseStmt) {
	// No table references in IMPORT DATABASE statements
}

// VisitInstallStmt is a no-op for INSTALL statements (no table references).
func (te *TableExtractor) VisitInstallStmt(stmt *InstallStmt) {
	// No table references in INSTALL statements
}

// VisitLoadStmt is a no-op for LOAD statements (no table references).
func (te *TableExtractor) VisitLoadStmt(stmt *LoadStmt) {
	// No table references in LOAD statements
}

// VisitAttachStmt is a no-op for ATTACH statements (no table references).
func (te *TableExtractor) VisitAttachStmt(stmt *AttachStmt) {
	// No table references in ATTACH statements
}

// VisitDetachStmt is a no-op for DETACH statements (no table references).
func (te *TableExtractor) VisitDetachStmt(stmt *DetachStmt) {
	// No table references in DETACH statements
}

// VisitUseStmt is a no-op for USE statements (no table references).
func (te *TableExtractor) VisitUseStmt(stmt *UseStmt) {
	// No table references in USE statements
}

// VisitCreateDatabaseStmt is a no-op for CREATE DATABASE statements (no table references).
func (te *TableExtractor) VisitCreateDatabaseStmt(stmt *CreateDatabaseStmt) {
	// No table references in CREATE DATABASE statements
}

// VisitDropDatabaseStmt is a no-op for DROP DATABASE statements (no table references).
func (te *TableExtractor) VisitDropDatabaseStmt(stmt *DropDatabaseStmt) {
	// No table references in DROP DATABASE statements
}

// VisitMergeStmt extracts table references from MERGE INTO statements.
func (te *TableExtractor) VisitMergeStmt(stmt *MergeStmt) {
	// Extract the target table (INTO)
	te.visitTableRef(&stmt.Into)

	// Extract the source table (USING)
	te.visitTableRef(&stmt.Using)

	// Visit ON condition for any subqueries
	if stmt.On != nil {
		te.visitExpr(stmt.On)
	}

	// Visit WHEN MATCHED action conditions and expressions
	for _, action := range stmt.WhenMatched {
		if action.Cond != nil {
			te.visitExpr(action.Cond)
		}
		for _, setClause := range action.Update {
			if setClause.Value != nil {
				te.visitExpr(setClause.Value)
			}
		}
	}

	// Visit WHEN NOT MATCHED action conditions and expressions
	for _, action := range stmt.WhenNotMatched {
		if action.Cond != nil {
			te.visitExpr(action.Cond)
		}
		for _, setClause := range action.Insert {
			if setClause.Value != nil {
				te.visitExpr(setClause.Value)
			}
		}
	}

	// Visit WHEN NOT MATCHED BY SOURCE action conditions and expressions
	for _, action := range stmt.WhenNotMatchedBySource {
		if action.Cond != nil {
			te.visitExpr(action.Cond)
		}
		for _, setClause := range action.Update {
			if setClause.Value != nil {
				te.visitExpr(setClause.Value)
			}
		}
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

	// Skip CTE references (they are not real tables)
	// Only skip if table has no schema/catalog (CTE references are unqualified)
	if astRef.Schema == "" && astRef.Catalog == "" {
		if _, isCTE := te.cteNames[astRef.TableName]; isCTE {

			return
		}
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

	case *SimilarToExpr:
		te.visitExpr(e.Expr)
		te.visitExpr(e.Pattern)

	// Leaf expression types (no recursion needed):
	case *ColumnRef, *Literal, *Parameter, *StarExpr:
		// No subqueries in these
		return

	default:
		// Unknown expression type - no action
		return
	}
}
