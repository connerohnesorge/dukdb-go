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

	// DDL statement visitors
	VisitCreateViewStmt(stmt *CreateViewStmt)
	VisitDropViewStmt(stmt *DropViewStmt)
	VisitCreateIndexStmt(stmt *CreateIndexStmt)
	VisitDropIndexStmt(stmt *DropIndexStmt)
	VisitCreateSequenceStmt(stmt *CreateSequenceStmt)
	VisitDropSequenceStmt(stmt *DropSequenceStmt)
	VisitCreateSchemaStmt(stmt *CreateSchemaStmt)
	VisitDropSchemaStmt(stmt *DropSchemaStmt)
	VisitAlterTableStmt(stmt *AlterTableStmt)

	// PIVOT/UNPIVOT statement visitors
	VisitPivotStmt(stmt *PivotStmt)
	VisitUnpivotStmt(stmt *UnpivotStmt)

	// MERGE INTO statement visitor
	VisitMergeStmt(stmt *MergeStmt)

	// GROUPING SETS/ROLLUP/CUBE expression visitor
	VisitGroupingSetExpr(expr *GroupingSetExpr)

	// Secret statement visitors
	VisitCreateSecretStmt(stmt *CreateSecretStmt)
	VisitDropSecretStmt(stmt *DropSecretStmt)
	VisitAlterSecretStmt(stmt *AlterSecretStmt)

	// Database maintenance statement visitors
	VisitPragmaStmt(stmt *PragmaStmt)
	VisitExplainStmt(stmt *ExplainStmt)
	VisitVacuumStmt(stmt *VacuumStmt)
	VisitAnalyzeStmt(stmt *AnalyzeStmt)
	VisitCheckpointStmt(stmt *CheckpointStmt)
}