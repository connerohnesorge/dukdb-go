package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// ---------- Bound Statement Types ----------

// BoundSelectStmt represents a bound SELECT statement.
type BoundSelectStmt struct {
	Distinct bool
	Columns  []*BoundSelectColumn
	From     []*BoundTableRef
	Joins    []*BoundJoin
	Where    BoundExpr
	GroupBy  []BoundExpr
	Having   BoundExpr
	OrderBy  []*BoundOrderBy
	Limit    BoundExpr
	Offset   BoundExpr
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
}

// BoundOrderBy represents a bound ORDER BY expression.
type BoundOrderBy struct {
	Expr BoundExpr
	Desc bool
}

// BoundInsertStmt represents a bound INSERT statement.
type BoundInsertStmt struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int // Column indices
	Values   [][]BoundExpr
	Select   *BoundSelectStmt
}

func (*BoundInsertStmt) boundStmtNode() {}

func (*BoundInsertStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_INSERT }

// BoundUpdateStmt represents a bound UPDATE statement.
type BoundUpdateStmt struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Set      []*BoundSetClause
	Where    BoundExpr
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
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Where    BoundExpr
}

func (*BoundDeleteStmt) boundStmtNode() {}

func (*BoundDeleteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// BoundCreateTableStmt represents a bound CREATE TABLE statement.
type BoundCreateTableStmt struct {
	Schema      string
	Table       string
	IfNotExists bool
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
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
	IfExists     bool             // IF EXISTS modifier
	NewTableName string           // RENAME TO
	OldColumn    string           // RENAME COLUMN
	NewColumn    string           // RENAME COLUMN
	DropColumn   string           // DROP COLUMN
	AddColumn    *catalog.ColumnDef // ADD COLUMN
}

func (*BoundAlterTableStmt) boundStmtNode() {}

func (*BoundAlterTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_ALTER }
