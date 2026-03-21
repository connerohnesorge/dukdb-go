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
	VisitTruncateStmt(stmt *TruncateStmt)
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
	VisitCommentStmt(stmt *CommentStmt)

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

	// Macro DDL statement visitors
	VisitCreateMacroStmt(stmt *CreateMacroStmt)
	VisitDropMacroStmt(stmt *DropMacroStmt)

	// Function DDL statement visitors
	VisitCreateFunctionStmt(stmt *CreateFunctionStmt)

	// Savepoint statement visitors
	VisitSavepointStmt(stmt *SavepointStmt)
	VisitRollbackToSavepointStmt(stmt *RollbackToSavepointStmt)
	VisitReleaseSavepointStmt(stmt *ReleaseSavepointStmt)

	// Session configuration statement visitors
	VisitSetStmt(stmt *SetStmt)
	VisitShowStmt(stmt *ShowStmt)

	// Prepared statement visitors
	VisitPrepareStmt(stmt *PrepareStmt)
	VisitExecuteStmt(stmt *ExecuteStmt)
	VisitDeallocateStmt(stmt *DeallocateStmt)

	// Type DDL statement visitors
	VisitCreateTypeStmt(stmt *CreateTypeStmt)
	VisitDropTypeStmt(stmt *DropTypeStmt)

	// Export/Import Database statement visitors
	VisitExportDatabaseStmt(stmt *ExportDatabaseStmt)
	VisitImportDatabaseStmt(stmt *ImportDatabaseStmt)

	// Extension statement visitors
	VisitInstallStmt(stmt *InstallStmt)
	VisitLoadStmt(stmt *LoadStmt)

	// Database management statement visitors
	VisitAttachStmt(stmt *AttachStmt)
	VisitDetachStmt(stmt *DetachStmt)
	VisitUseStmt(stmt *UseStmt)
	VisitCreateDatabaseStmt(stmt *CreateDatabaseStmt)
	VisitDropDatabaseStmt(stmt *DropDatabaseStmt)
}
