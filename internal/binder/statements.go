package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// ---------- Bound Statement Types ----------

// BoundSelectStmt represents a bound SELECT statement.
type BoundSelectStmt struct {
	CTEs            []*BoundCTE // Common Table Expressions (WITH clause)
	Distinct        bool
	DistinctOn      []BoundExpr // DISTINCT ON expressions (select first row per group)
	Columns         []*BoundSelectColumn
	From            []*BoundTableRef
	Joins           []*BoundJoin
	Where           BoundExpr
	GroupBy         []BoundExpr
	Having          BoundExpr
	Qualify         BoundExpr // QUALIFY clause (filter after window functions)
	OrderBy         []*BoundOrderBy
	Limit           BoundExpr
	Offset          BoundExpr
	WithTies        bool // true when FETCH ... WITH TIES was used
	Sample          *BoundSampleOptions // SAMPLE clause options
	RecursionOption *parser.RecursionOption
	SetOp           parser.SetOpType // Type of set operation (UNION, INTERSECT, EXCEPT)
	Right           *BoundSelectStmt // Right side of set operation
}

func (*BoundSelectStmt) boundStmtNode() {}

func (*BoundSelectStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }

func (*BoundSelectStmt) boundExprNode() {}

func (*BoundSelectStmt) ResultType() dukdb.Type { return dukdb.TYPE_ANY }

// BoundSelectColumn represents a bound column in SELECT.
type BoundSelectColumn struct {
	Expr  BoundExpr
	Alias string
	Star  bool
}

// BoundJoin represents a bound JOIN.
type BoundJoin struct {
	Type      parser.JoinType
	Table     *BoundTableRef
	Condition BoundExpr
	Using     []string // USING columns (resolved from parser)
}

// BoundOrderBy represents a bound ORDER BY expression.
type BoundOrderBy struct {
	Expr      BoundExpr
	Desc      bool
	Collation string // COLLATE collation_name (empty = default)
}

// BoundOnConflictClause represents a bound ON CONFLICT clause.
type BoundOnConflictClause struct {
	ConflictColumnIndices []int                // Resolved column indices in target table
	Action                parser.OnConflictAction
	UpdateSet             []*BoundSetClause // Bound SET assignments
	UpdateWhere           BoundExpr         // Bound WHERE filter (may be nil)
}

// BoundExcludedColumnRef references a column from the EXCLUDED pseudo-table
// in an ON CONFLICT DO UPDATE SET clause.
type BoundExcludedColumnRef struct {
	ColumnIndex int
	ColumnName  string
	DataType    dukdb.Type
}

func (*BoundExcludedColumnRef) boundExprNode() {}

// ResultType returns the data type of the excluded column reference.
func (e *BoundExcludedColumnRef) ResultType() dukdb.Type { return e.DataType }

// BoundInsertStmt represents a bound INSERT statement.
type BoundInsertStmt struct {
	Schema     string
	Table      string
	TableDef   *catalog.TableDef
	Columns    []int // Column indices
	Values     [][]BoundExpr
	Select     *BoundSelectStmt
	OnConflict *BoundOnConflictClause   // nil for plain INSERT
	Returning  []*BoundSelectColumn     // RETURNING clause columns
}

func (*BoundInsertStmt) boundStmtNode() {}

func (*BoundInsertStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_INSERT }

// BoundUpdateStmt represents a bound UPDATE statement.
type BoundUpdateStmt struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Set       []*BoundSetClause
	Where     BoundExpr
	Returning []*BoundSelectColumn // RETURNING clause columns
}

func (*BoundUpdateStmt) boundStmtNode() {}

func (*BoundUpdateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_UPDATE }

// BoundSetClause represents a bound SET clause.
type BoundSetClause struct {
	ColumnIdx int
	Value     BoundExpr
}

// BoundDeleteStmt represents a bound DELETE statement.
type BoundDeleteStmt struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Using     []*BoundTableRef     // USING clause bound table references
	Where     BoundExpr
	Returning []*BoundSelectColumn // RETURNING clause columns
}

func (*BoundDeleteStmt) boundStmtNode() {}

func (*BoundDeleteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// BoundCreateTableStmt represents a bound CREATE TABLE statement.
type BoundCreateTableStmt struct {
	Schema      string
	Table       string
	IfNotExists bool
	OrReplace   bool
	Temporary   bool
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
	Constraints []any // *catalog.UniqueConstraintDef, *catalog.CheckConstraintDef
}

func (*BoundCreateTableStmt) boundStmtNode() {}

func (*BoundCreateTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropTableStmt represents a bound DROP TABLE statement.
type BoundDropTableStmt struct {
	Schema   string
	Table    string
	IfExists bool
}

func (*BoundDropTableStmt) boundStmtNode() {}

func (*BoundDropTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundTruncateStmt represents a bound TRUNCATE TABLE statement.
type BoundTruncateStmt struct {
	Schema string
	Table  string
}

func (*BoundTruncateStmt) boundStmtNode() {}

func (*BoundTruncateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// BoundBeginStmt represents a bound BEGIN statement.
type BoundBeginStmt struct{}

func (*BoundBeginStmt) boundStmtNode() {}

func (*BoundBeginStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// BoundCommitStmt represents a bound COMMIT statement.
type BoundCommitStmt struct{}

func (*BoundCommitStmt) boundStmtNode() {}

func (*BoundCommitStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// BoundRollbackStmt represents a bound ROLLBACK statement.
type BoundRollbackStmt struct{}

func (*BoundRollbackStmt) boundStmtNode() {}

func (*BoundRollbackStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// BoundCopyStmt represents a bound COPY statement.
type BoundCopyStmt struct {
	// Schema is the schema name (default "main").
	Schema string
	// Table is the table name (for COPY table FROM/TO).
	Table string
	// TableDef is the table definition (for COPY FROM/TO table).
	TableDef *catalog.TableDef
	// Columns are the column indices to import/export (nil for all).
	Columns []int
	// FilePath is the file path.
	FilePath string
	// IsFrom is true for COPY FROM, false for COPY TO.
	IsFrom bool
	// Query is the bound SELECT query (for COPY (SELECT...) TO).
	Query *BoundSelectStmt
	// Options are the COPY options.
	Options map[string]any
}

func (*BoundCopyStmt) boundStmtNode() {}

func (*BoundCopyStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_COPY }

// ---------- DDL Bound Statement Types ----------

// BoundCreateViewStmt represents a bound CREATE VIEW statement.
type BoundCreateViewStmt struct {
	Schema      string
	View        string
	IfNotExists bool
	Query       *BoundSelectStmt // The bound view query
	QueryText   string           // The original query text for storage
}

func (*BoundCreateViewStmt) boundStmtNode() {}

func (*BoundCreateViewStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropViewStmt represents a bound DROP VIEW statement.
type BoundDropViewStmt struct {
	Schema   string
	View     string
	IfExists bool
}

func (*BoundDropViewStmt) boundStmtNode() {}

func (*BoundDropViewStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundCreateIndexStmt represents a bound CREATE INDEX statement.
type BoundCreateIndexStmt struct {
	Schema      string
	Table       string
	Index       string
	IfNotExists bool
	Columns     []string
	IsUnique    bool
	TableDef    *catalog.TableDef // For validation
}

func (*BoundCreateIndexStmt) boundStmtNode() {}

func (*BoundCreateIndexStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropIndexStmt represents a bound DROP INDEX statement.
type BoundDropIndexStmt struct {
	Schema   string
	Index    string
	IfExists bool
}

func (*BoundDropIndexStmt) boundStmtNode() {}

func (*BoundDropIndexStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundCreateSequenceStmt represents a bound CREATE SEQUENCE statement.
type BoundCreateSequenceStmt struct {
	Schema      string
	Sequence    string
	IfNotExists bool
	StartWith   int64
	IncrementBy int64
	MinValue    *int64
	MaxValue    *int64
	IsCycle     bool
}

func (*BoundCreateSequenceStmt) boundStmtNode() {}

func (*BoundCreateSequenceStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropSequenceStmt represents a bound DROP SEQUENCE statement.
type BoundDropSequenceStmt struct {
	Schema   string
	Sequence string
	IfExists bool
}

func (*BoundDropSequenceStmt) boundStmtNode() {}

func (*BoundDropSequenceStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundCreateSchemaStmt represents a bound CREATE SCHEMA statement.
type BoundCreateSchemaStmt struct {
	Schema      string
	IfNotExists bool
}

func (*BoundCreateSchemaStmt) boundStmtNode() {}

func (*BoundCreateSchemaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropSchemaStmt represents a bound DROP SCHEMA statement.
type BoundDropSchemaStmt struct {
	Schema   string
	IfExists bool
	Cascade  bool
}

func (*BoundDropSchemaStmt) boundStmtNode() {}

func (*BoundDropSchemaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundAlterTableStmt represents a bound ALTER TABLE statement.
type BoundAlterTableStmt struct {
	Schema       string
	Table        string
	TableDef     *catalog.TableDef
	Operation    parser.AlterTableOp
	IfExists     bool               // IF EXISTS modifier
	NewTableName  string             // RENAME TO
	OldColumn     string             // RENAME COLUMN
	NewColumn     string             // RENAME COLUMN
	DropColumn    string             // DROP COLUMN
	AddColumn      *catalog.ColumnDef    // ADD COLUMN
	AlterColumn    string                // ALTER COLUMN TYPE
	NewColumnType  dukdb.Type            // ALTER COLUMN TYPE
	ConstraintName string                // DROP CONSTRAINT
	Constraint     *parser.TableConstraint // ADD CONSTRAINT
	DefaultExpr    BoundExpr             // SET DEFAULT expression
}

func (*BoundAlterTableStmt) boundStmtNode() {}

func (*BoundAlterTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }

// BoundCommentStmt represents a bound COMMENT ON statement.
type BoundCommentStmt struct {
	ObjectType string
	Schema     string
	ObjectName string
	ColumnName string
	Comment    *string
}

func (*BoundCommentStmt) boundStmtNode() {}

func (*BoundCommentStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }

// ---------- MERGE INTO Bound Statement Types ----------

// BoundMergeActionType represents the type of bound MERGE action.
type BoundMergeActionType int

const (
	BoundMergeActionUpdate    BoundMergeActionType = iota // UPDATE existing row
	BoundMergeActionDelete                                // DELETE matched row
	BoundMergeActionInsert                                // INSERT new row
	BoundMergeActionDoNothing                             // DO NOTHING
)

// BoundMergeAction represents a bound action in a MERGE clause.
type BoundMergeAction struct {
	// Type specifies the action type (UPDATE, DELETE, INSERT, DO NOTHING).
	Type BoundMergeActionType
	// Cond is an optional additional condition (AND ...) for the action.
	Cond BoundExpr
	// Update contains the bound SET clauses for UPDATE actions.
	Update []*BoundSetClause
	// InsertColumns contains the column indices for INSERT actions.
	InsertColumns []int
	// InsertValues contains the bound expressions for INSERT values.
	InsertValues []BoundExpr
}

// BoundMergeStmt represents a bound MERGE INTO statement.
type BoundMergeStmt struct {
	// Schema is the schema name for the target table.
	Schema string
	// TargetTable is the name of the target table.
	TargetTable string
	// TargetTableDef is the table definition for the target.
	TargetTableDef *catalog.TableDef
	// TargetAlias is the alias for the target table.
	TargetAlias string
	// SourceRef is the bound source table reference.
	SourceRef *BoundTableRef
	// OnCondition is the bound join condition.
	OnCondition BoundExpr
	// WhenMatched contains bound actions for matched rows.
	WhenMatched []*BoundMergeAction
	// WhenNotMatched contains bound actions for non-matched source rows.
	WhenNotMatched []*BoundMergeAction
	// WhenNotMatchedBySource contains bound actions for target rows with no source match.
	WhenNotMatchedBySource []*BoundMergeAction
	// Returning contains the columns to return (if RETURNING clause is present).
	Returning []*BoundSelectColumn
}

func (*BoundMergeStmt) boundStmtNode() {}

func (*BoundMergeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_MERGE_INTO }

// ---------- PIVOT/UNPIVOT Bound Statement Types ----------

// BoundPivotAggregate represents a bound aggregate function in a PIVOT clause.
type BoundPivotAggregate struct {
	// Function is the aggregate function name (e.g., "SUM", "COUNT", "AVG").
	Function string
	// Expr is the bound expression to aggregate.
	Expr BoundExpr
	// Alias is the optional alias for the aggregated column.
	Alias string
}

// BoundPivotStmt represents a bound PIVOT statement.
// PIVOT transforms rows into columns by pivoting unique values from one column
// into multiple columns in the output, with optional aggregation.
type BoundPivotStmt struct {
	// Source is the bound source table reference.
	Source *BoundTableRef
	// ForColumn is the bound column reference whose values become column names.
	ForColumn *BoundColumnRef
	// InValues contains the literal values to pivot on (become column names).
	InValues []any
	// Aggregates contains the bound aggregate functions to apply.
	Aggregates []*BoundPivotAggregate
	// GroupBy contains the bound GROUP BY expressions (columns not pivoted or aggregated).
	GroupBy []BoundExpr
	// Alias is the optional alias for the pivoted result set.
	Alias string
}

func (*BoundPivotStmt) boundStmtNode() {}

func (*BoundPivotStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PIVOT }

// BoundUnpivotStmt represents a bound UNPIVOT statement.
// UNPIVOT transforms columns into rows (the inverse of PIVOT).
type BoundUnpivotStmt struct {
	// Source is the bound source table reference.
	Source *BoundTableRef
	// ValueColumn is the name of the column that will contain the unpivoted values.
	ValueColumn string
	// NameColumn is the name of the column that will contain the original column names.
	NameColumn string
	// UnpivotColumns contains the column names to unpivot.
	UnpivotColumns []string
	// Alias is the optional alias for the unpivoted result set.
	Alias string
}

func (*BoundUnpivotStmt) boundStmtNode() {}

func (*BoundUnpivotStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_UNPIVOT }

// ---------- Advanced SELECT Features ----------

// BoundSampleOptions represents bound SAMPLE clause options.
type BoundSampleOptions struct {
	Method     parser.SampleMethod // Sampling method (BERNOULLI, SYSTEM, or RESERVOIR)
	Percentage float64             // For BERNOULLI/SYSTEM - percentage of rows to sample (0-100)
	Rows       int                 // For RESERVOIR - fixed number of rows to sample
	Seed       *int64              // Optional seed for reproducible sampling
}

// ---------- CTE (Common Table Expression) Types ----------

// BoundCTE represents a bound Common Table Expression.
type BoundCTE struct {
	// Name is the CTE name used to reference it in the query.
	Name string
	// Columns are the optional column aliases for the CTE.
	Columns []string
	// Query is the bound SELECT query for the CTE (base case for recursive CTEs).
	Query *BoundSelectStmt
	// RecursiveQuery is the bound SELECT query for the recursive part of a recursive CTE.
	// This is only set when Recursive is true.
	RecursiveQuery *BoundSelectStmt
	// Recursive is true if this is a WITH RECURSIVE CTE.
	Recursive bool
	// UsingKey specifies USING KEY columns for cycle detection.
	UsingKey []string
	// SetOp captures UNION vs UNION ALL for recursive CTE semantics.
	SetOp parser.SetOpType
	// MaxRecursion is the recursion limit from OPTION (MAX_RECURSION N).
	MaxRecursion int
	// ResultTypes are the inferred types of the CTE columns.
	ResultTypes []dukdb.Type
	// ResultNames are the column names (either from aliases or inferred from query).
	ResultNames []string
}

// ---------- Secret Bound Statement Types ----------

// BoundCreateSecretStmt represents a bound CREATE SECRET statement.
type BoundCreateSecretStmt struct {
	Name        string            // Secret name
	IfNotExists bool              // IF NOT EXISTS clause
	OrReplace   bool              // OR REPLACE clause
	Persistent  bool              // PERSISTENT vs TEMPORARY
	SecretType  string            // Type of secret (S3, GCS, AZURE, HTTP, HUGGINGFACE)
	Provider    string            // Provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
	Scope       string            // Optional scope path (e.g., s3://bucket/path)
	Options     map[string]string // Key-value options
}

func (*BoundCreateSecretStmt) boundStmtNode() {}

func (*BoundCreateSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropSecretStmt represents a bound DROP SECRET statement.
type BoundDropSecretStmt struct {
	Name     string // Secret name
	IfExists bool   // IF EXISTS clause
}

func (*BoundDropSecretStmt) boundStmtNode() {}

func (*BoundDropSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundAlterSecretStmt represents a bound ALTER SECRET statement.
type BoundAlterSecretStmt struct {
	Name    string            // Secret name
	Options map[string]string // Options to update
}

func (*BoundAlterSecretStmt) boundStmtNode() {}

func (*BoundAlterSecretStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }

// ---------- SUMMARIZE Bound Statement Type ----------

// BoundSummarizeStmt represents a bound SUMMARIZE statement.
type BoundSummarizeStmt struct {
	Schema    string
	TableName string
	TableDef  *catalog.TableDef // Resolved table definition (nil for SUMMARIZE SELECT)
	Query     *BoundSelectStmt  // Bound inner query (nil for SUMMARIZE table)
}

func (*BoundSummarizeStmt) boundStmtNode() {}

func (*BoundSummarizeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }

// ---------- Export/Import Database Bound Statement Types ----------

// BoundExportDatabaseStmt represents a bound EXPORT DATABASE statement.
type BoundExportDatabaseStmt struct {
	Path    string
	Options map[string]string
}

func (*BoundExportDatabaseStmt) boundStmtNode() {}

func (*BoundExportDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXPORT }

// BoundImportDatabaseStmt represents a bound IMPORT DATABASE statement.
type BoundImportDatabaseStmt struct {
	Path string
}

func (*BoundImportDatabaseStmt) boundStmtNode() {}

func (*BoundImportDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_COPY_DATABASE }

// ---------- Database Maintenance Bound Statement Types ----------

// PragmaType represents the category of a PRAGMA statement.
type PragmaType int

const (
	// PragmaTypeInfo returns information (database_size, table_info, etc.)
	PragmaTypeInfo PragmaType = iota
	// PragmaTypeConfig sets configuration values
	PragmaTypeConfig
	// PragmaTypeProfiling controls profiling
	PragmaTypeProfiling
)

// BoundPragmaStmt represents a bound PRAGMA statement.
type BoundPragmaStmt struct {
	Name       string      // Pragma name (e.g., "database_size")
	PragmaType PragmaType  // Category of pragma
	Args       []BoundExpr // Bound arguments
	Value      BoundExpr   // For SET PRAGMA name = value
}

func (*BoundPragmaStmt) boundStmtNode() {}

func (*BoundPragmaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PRAGMA }

// BoundExplainStmt represents a bound EXPLAIN statement.
type BoundExplainStmt struct {
	Query   BoundStatement // The bound query to explain
	Analyze bool           // true for EXPLAIN ANALYZE
}

func (*BoundExplainStmt) boundStmtNode() {}

func (*BoundExplainStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_EXPLAIN }

// BoundVacuumStmt represents a bound VACUUM statement.
type BoundVacuumStmt struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for entire database)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*BoundVacuumStmt) boundStmtNode() {}

func (*BoundVacuumStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_VACUUM }

// BoundAnalyzeStmt represents a bound ANALYZE statement.
type BoundAnalyzeStmt struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for all tables)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*BoundAnalyzeStmt) boundStmtNode() {}

func (*BoundAnalyzeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ANALYZE }

// BoundCheckpointStmt represents a bound CHECKPOINT statement.
type BoundCheckpointStmt struct {
	Database string // Optional database name
	Force    bool   // FORCE flag
}

func (*BoundCheckpointStmt) boundStmtNode() {}

func (*BoundCheckpointStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// ---------- Type DDL Bound Statement Types ----------

// BoundCreateTypeStmt represents a bound CREATE TYPE statement.
type BoundCreateTypeStmt struct {
	Name        string
	Schema      string
	TypeKind    string
	EnumValues  []string
	IfNotExists bool
}

func (*BoundCreateTypeStmt) boundStmtNode() {}

func (*BoundCreateTypeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropTypeStmt represents a bound DROP TYPE statement.
type BoundDropTypeStmt struct {
	Name     string
	Schema   string
	IfExists bool
}

func (*BoundDropTypeStmt) boundStmtNode() {}

func (*BoundDropTypeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// ---------- Macro DDL Bound Statement Types ----------

// BoundCreateMacroStmt represents a bound CREATE MACRO statement.
type BoundCreateMacroStmt struct {
	Schema       string
	Name         string
	Params       []catalog.MacroParam
	IsTableMacro bool
	OrReplace    bool
	BodySQL      string
	QuerySQL     string
}

func (*BoundCreateMacroStmt) boundStmtNode() {}

func (*BoundCreateMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropMacroStmt represents a bound DROP MACRO statement.
type BoundDropMacroStmt struct {
	Schema       string
	Name         string
	IfExists     bool
	IsTableMacro bool
}

func (*BoundDropMacroStmt) boundStmtNode() {}

func (*BoundDropMacroStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// ---------- Database Management Bound Statement Types ----------

// BoundAttachStmt represents a bound ATTACH statement.
type BoundAttachStmt struct {
	Path     string
	Alias    string
	ReadOnly bool
	Options  map[string]string
}

func (*BoundAttachStmt) boundStmtNode() {}

func (*BoundAttachStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ATTACH }

// BoundDetachStmt represents a bound DETACH statement.
type BoundDetachStmt struct {
	Name     string
	IfExists bool
}

func (*BoundDetachStmt) boundStmtNode() {}

func (*BoundDetachStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DETACH }

// BoundUseStmt represents a bound USE statement.
type BoundUseStmt struct {
	Database string
	Schema   string
}

func (*BoundUseStmt) boundStmtNode() {}

func (*BoundUseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SET }

// BoundCreateDatabaseStmt represents a bound CREATE DATABASE statement.
type BoundCreateDatabaseStmt struct {
	Name        string
	IfNotExists bool
}

func (*BoundCreateDatabaseStmt) boundStmtNode() {}

func (*BoundCreateDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropDatabaseStmt represents a bound DROP DATABASE statement.
type BoundDropDatabaseStmt struct {
	Name     string
	IfExists bool
}

func (*BoundDropDatabaseStmt) boundStmtNode() {}

func (*BoundDropDatabaseStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }
