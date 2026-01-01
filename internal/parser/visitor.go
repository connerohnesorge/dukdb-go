// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

// Visitor defines the interface for traversing the AST using the visitor pattern.
// Each AST node type has a corresponding Visit method.
type Visitor interface {
	// Statement visitors
	VisitSelectStmt(stmt *SelectStmt)
	VisitInsertStmt(stmt *InsertStmt)
	VisitUpdateStmt(stmt *UpdateStmt)
	VisitDeleteStmt(stmt *DeleteStmt)
	VisitCreateTableStmt(stmt *CreateTableStmt)
	VisitDropTableStmt(stmt *DropTableStmt)
	VisitBeginStmt(stmt *BeginStmt)
	VisitCommitStmt(stmt *CommitStmt)
	VisitRollbackStmt(stmt *RollbackStmt)
	VisitCopyStmt(stmt *CopyStmt)
}