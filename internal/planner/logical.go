// Package planner provides query planning for the native Go DuckDB implementation.
package planner

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// LogicalPlan represents a node in the logical query plan.
type LogicalPlan interface {
	logicalPlanNode()
	Children() []LogicalPlan
	OutputColumns() []ColumnBinding
}

// ColumnBinding represents a column in the plan output.
type ColumnBinding struct {
	Table     string
	Column    string
	Type      dukdb.Type
	TableIdx  int
	ColumnIdx int
}

// ---------- Logical Plan Nodes ----------

// LogicalScan represents a table scan.
type LogicalScan struct {
	Schema        string
	TableName     string
	Alias         string
	TableDef      *catalog.TableDef
	VirtualTable  *catalog.VirtualTableDef      // Set for virtual tables
	TableFunction *binder.BoundTableFunctionRef // Set for table functions
	Projections   []int                         // Column indices to project, nil for all
	columns       []ColumnBinding
}

func (*LogicalScan) logicalPlanNode() {}

func (*LogicalScan) Children() []LogicalPlan { return nil }

func (s *LogicalScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}

	// Handle table functions - columns are deferred until execution
	if s.TableFunction != nil {
		if s.TableFunction.Columns != nil {
			s.columns = make([]ColumnBinding, len(s.TableFunction.Columns))
			for i, col := range s.TableFunction.Columns {
				s.columns[i] = ColumnBinding{
					Table:     s.Alias,
					Column:    col.Name,
					Type:      col.Type,
					ColumnIdx: i,
				}
			}
		}
		return s.columns
	}

	cols := s.TableDef.Columns
	if s.Projections != nil {
		s.columns = make(
			[]ColumnBinding,
			len(s.Projections),
		)
		for i, idx := range s.Projections {
			s.columns[i] = ColumnBinding{
				Table:     s.Alias,
				Column:    cols[idx].Name,
				Type:      cols[idx].Type,
				ColumnIdx: idx,
			}
		}
	} else {
		s.columns = make([]ColumnBinding, len(cols))
		for i, col := range cols {
			s.columns[i] = ColumnBinding{
				Table:     s.Alias,
				Column:    col.Name,
				Type:      col.Type,
				ColumnIdx: i,
			}
		}
	}

	return s.columns
}

// LogicalValues represents a VALUES clause that produces inline rows.
type LogicalValues struct {
	Rows    [][]binder.BoundExpr // Bound expressions per row
	Columns []ColumnBinding      // Output column names and types
	Alias   string
}

func (*LogicalValues) logicalPlanNode() {}

func (*LogicalValues) Children() []LogicalPlan { return nil }

func (v *LogicalValues) OutputColumns() []ColumnBinding { return v.Columns }

// LogicalFilter represents a filter (WHERE clause).
type LogicalFilter struct {
	Child     LogicalPlan
	Condition binder.BoundExpr
}

func (*LogicalFilter) logicalPlanNode() {}

func (f *LogicalFilter) Children() []LogicalPlan { return []LogicalPlan{f.Child} }

func (f *LogicalFilter) OutputColumns() []ColumnBinding { return f.Child.OutputColumns() }

// LogicalProject represents a projection (SELECT columns).
type LogicalProject struct {
	Child       LogicalPlan
	Expressions []binder.BoundExpr
	Aliases     []string
	columns     []ColumnBinding
}

func (*LogicalProject) logicalPlanNode() {}

func (p *LogicalProject) Children() []LogicalPlan { return []LogicalPlan{p.Child} }

func (p *LogicalProject) OutputColumns() []ColumnBinding {
	if p.columns != nil {
		return p.columns
	}

	p.columns = make(
		[]ColumnBinding,
		len(p.Expressions),
	)
	for i, expr := range p.Expressions {
		alias := p.Aliases[i]
		if alias == "" {
			if colRef, ok := expr.(*binder.BoundColumnRef); ok {
				alias = colRef.Column
			} else {
				alias = ""
			}
		}
		p.columns[i] = ColumnBinding{
			Column:    alias,
			Type:      expr.ResultType(),
			ColumnIdx: i,
		}
	}

	return p.columns
}

// LogicalJoin represents a join operation.
type LogicalJoin struct {
	Left      LogicalPlan
	Right     LogicalPlan
	JoinType  JoinType
	Condition binder.BoundExpr
	columns   []ColumnBinding
}

// JoinType represents the type of join.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
	JoinTypeSemi       // SEMI JOIN: outputs left rows where right match exists (for EXISTS, IN subqueries)
	JoinTypeAnti       // ANTI JOIN: outputs left rows where right match does NOT exist (for NOT EXISTS, NOT IN)
	JoinTypePositional // POSITIONAL JOIN: matches rows by position
	JoinTypeAsOf       // ASOF JOIN: matches nearest row based on inequality
	JoinTypeAsOfLeft   // ASOF LEFT JOIN: like ASOF with LEFT semantics
)

func (*LogicalJoin) logicalPlanNode() {}

func (j *LogicalJoin) Children() []LogicalPlan { return []LogicalPlan{j.Left, j.Right} }

func (j *LogicalJoin) OutputColumns() []ColumnBinding {
	if j.columns != nil {
		return j.columns
	}

	leftCols := j.Left.OutputColumns()
	rightCols := j.Right.OutputColumns()
	j.columns = make(
		[]ColumnBinding,
		0,
		len(leftCols)+len(rightCols),
	)

	for i, col := range leftCols {
		j.columns = append(
			j.columns,
			ColumnBinding{
				Table:     col.Table,
				Column:    col.Column,
				Type:      col.Type,
				TableIdx:  0,
				ColumnIdx: i,
			},
		)
	}

	for i, col := range rightCols {
		j.columns = append(
			j.columns,
			ColumnBinding{
				Table:     col.Table,
				Column:    col.Column,
				Type:      col.Type,
				TableIdx:  1,
				ColumnIdx: i,
			},
		)
	}

	return j.columns
}

// LogicalAggregate represents an aggregation (GROUP BY).
type LogicalAggregate struct {
	Child      LogicalPlan
	GroupBy    []binder.BoundExpr
	Aggregates []binder.BoundExpr
	Aliases    []string
	columns    []ColumnBinding
	// GroupingSets contains the expanded grouping sets for GROUPING SETS/ROLLUP/CUBE.
	// Each inner slice is one grouping set - a subset of GROUP BY columns to aggregate on.
	// Empty slice means regular GROUP BY (no grouping sets).
	GroupingSets [][]binder.BoundExpr
	// GroupingCalls contains the GROUPING() function calls in the SELECT list.
	// These are evaluated during execution to compute the bitmask.
	GroupingCalls []*binder.BoundGroupingCall
}

func (*LogicalAggregate) logicalPlanNode() {}

func (a *LogicalAggregate) Children() []LogicalPlan { return []LogicalPlan{a.Child} }

func (a *LogicalAggregate) OutputColumns() []ColumnBinding {
	if a.columns != nil {
		return a.columns
	}

	numGroupBy := len(a.GroupBy)
	numAgg := len(a.Aggregates)
	a.columns = make(
		[]ColumnBinding,
		numGroupBy+numAgg,
	)

	for i, expr := range a.GroupBy {
		alias := ""
		if i < len(a.Aliases) {
			alias = a.Aliases[i]
		}
		if alias == "" {
			if colRef, ok := expr.(*binder.BoundColumnRef); ok {
				alias = colRef.Column
			}
		}
		a.columns[i] = ColumnBinding{
			Column:    alias,
			Type:      expr.ResultType(),
			ColumnIdx: i,
		}
	}

	for i, expr := range a.Aggregates {
		alias := ""
		if numGroupBy+i < len(a.Aliases) {
			alias = a.Aliases[numGroupBy+i]
		}
		a.columns[numGroupBy+i] = ColumnBinding{
			Column:    alias,
			Type:      expr.ResultType(),
			ColumnIdx: numGroupBy + i,
		}
	}

	return a.columns
}

// LogicalWindow represents a window function operator in the logical plan.
// It evaluates window expressions over partitioned and ordered data.
type LogicalWindow struct {
	Child       LogicalPlan               // Child plan
	WindowExprs []*binder.BoundWindowExpr // Window expressions to evaluate
	columns     []ColumnBinding
}

func (*LogicalWindow) logicalPlanNode() {}

func (w *LogicalWindow) Children() []LogicalPlan { return []LogicalPlan{w.Child} }

func (w *LogicalWindow) OutputColumns() []ColumnBinding {
	if w.columns != nil {
		return w.columns
	}

	// Window operator outputs all child columns plus window result columns
	childCols := w.Child.OutputColumns()
	w.columns = make([]ColumnBinding, len(childCols)+len(w.WindowExprs))

	// Copy child columns
	copy(w.columns, childCols)

	// Add window result columns
	for i, windowExpr := range w.WindowExprs {
		w.columns[len(childCols)+i] = ColumnBinding{
			Column:    windowExpr.FunctionName,
			Type:      windowExpr.ResType,
			ColumnIdx: len(childCols) + i,
		}
	}

	return w.columns
}

// LogicalSort represents a sort operation (ORDER BY).
type LogicalSort struct {
	Child   LogicalPlan
	OrderBy []*binder.BoundOrderBy
}

func (*LogicalSort) logicalPlanNode() {}

func (s *LogicalSort) Children() []LogicalPlan { return []LogicalPlan{s.Child} }

func (s *LogicalSort) OutputColumns() []ColumnBinding { return s.Child.OutputColumns() }

// LogicalLimit represents a limit operation (LIMIT/OFFSET).
type LogicalLimit struct {
	Child      LogicalPlan
	Limit      int64            // Static limit value (-1 means use LimitExpr)
	Offset     int64            // Static offset value (-1 means use OffsetExpr)
	LimitExpr  binder.BoundExpr // Dynamic limit expression (for LATERAL joins)
	OffsetExpr binder.BoundExpr // Dynamic offset expression (for LATERAL joins)
	WithTies   bool             // true when FETCH ... WITH TIES was used
	OrderBy    []*binder.BoundOrderBy // ORDER BY columns for WITH TIES comparison
}

func (*LogicalLimit) logicalPlanNode() {}

func (l *LogicalLimit) Children() []LogicalPlan { return []LogicalPlan{l.Child} }

func (l *LogicalLimit) OutputColumns() []ColumnBinding { return l.Child.OutputColumns() }

// LogicalDistinct represents a distinct operation.
type LogicalDistinct struct {
	Child LogicalPlan
}

func (*LogicalDistinct) logicalPlanNode() {}

func (d *LogicalDistinct) Children() []LogicalPlan { return []LogicalPlan{d.Child} }

func (d *LogicalDistinct) OutputColumns() []ColumnBinding { return d.Child.OutputColumns() }

// LogicalDistinctOn represents a DISTINCT ON operation.
// DISTINCT ON (col1, col2) keeps the first row for each unique combination of specified columns.
// The query must include an ORDER BY that starts with the DISTINCT ON columns to define "first".
type LogicalDistinctOn struct {
	Child      LogicalPlan            // Child plan
	DistinctOn []binder.BoundExpr     // Expressions to distinct on
	OrderBy    []*binder.BoundOrderBy // The ORDER BY clause (used to determine which row to keep)
}

func (*LogicalDistinctOn) logicalPlanNode() {}

func (d *LogicalDistinctOn) Children() []LogicalPlan { return []LogicalPlan{d.Child} }

func (d *LogicalDistinctOn) OutputColumns() []ColumnBinding { return d.Child.OutputColumns() }

// LogicalInsert represents an INSERT operation.
type LogicalInsert struct {
	Schema     string
	Table      string
	TableDef   *catalog.TableDef
	Columns    []int
	Values     [][]binder.BoundExpr
	Source     LogicalPlan                    // For INSERT ... SELECT
	OnConflict *binder.BoundOnConflictClause  // nil for plain INSERT
	Returning  []*binder.BoundSelectColumn    // RETURNING clause columns
}

func (*LogicalInsert) logicalPlanNode() {}
func (i *LogicalInsert) Children() []LogicalPlan {
	if i.Source != nil {
		return []LogicalPlan{i.Source}
	}

	return nil
}

func (*LogicalInsert) OutputColumns() []ColumnBinding { return nil }

// LogicalUpdate represents an UPDATE operation.
type LogicalUpdate struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Set       []*binder.BoundSetClause
	Source    LogicalPlan                 // Scan + Filter
	Returning []*binder.BoundSelectColumn // RETURNING clause columns
}

func (*LogicalUpdate) logicalPlanNode() {}
func (u *LogicalUpdate) Children() []LogicalPlan {
	if u.Source != nil {
		return []LogicalPlan{u.Source}
	}

	return nil
}

func (*LogicalUpdate) OutputColumns() []ColumnBinding { return nil }

// LogicalDelete represents a DELETE operation.
type LogicalDelete struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Source    LogicalPlan                 // Scan + Filter
	Returning []*binder.BoundSelectColumn // RETURNING clause columns
}

func (*LogicalDelete) logicalPlanNode() {}
func (d *LogicalDelete) Children() []LogicalPlan {
	if d.Source != nil {
		return []LogicalPlan{d.Source}
	}

	return nil
}

func (*LogicalDelete) OutputColumns() []ColumnBinding { return nil }

// LogicalCreateTable represents a CREATE TABLE operation.
type LogicalCreateTable struct {
	Schema      string
	Table       string
	IfNotExists bool
	OrReplace   bool
	Temporary   bool
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
	Constraints []any // *catalog.UniqueConstraintDef, *catalog.CheckConstraintDef
}

func (*LogicalCreateTable) logicalPlanNode() {}

func (*LogicalCreateTable) Children() []LogicalPlan { return nil }

func (*LogicalCreateTable) OutputColumns() []ColumnBinding { return nil }

// LogicalDropTable represents a DROP TABLE operation.
type LogicalDropTable struct {
	Schema   string
	Table    string
	IfExists bool
}

func (*LogicalDropTable) logicalPlanNode() {}

func (*LogicalDropTable) Children() []LogicalPlan { return nil }

func (*LogicalDropTable) OutputColumns() []ColumnBinding { return nil }

// LogicalTruncate represents a TRUNCATE TABLE operation.
type LogicalTruncate struct {
	Schema string
	Table  string
}

func (*LogicalTruncate) logicalPlanNode() {}

func (*LogicalTruncate) Children() []LogicalPlan { return nil }

func (*LogicalTruncate) OutputColumns() []ColumnBinding { return nil }

// LogicalDummyScan represents a scan that produces a single row with no columns.
// Used for queries like "SELECT 1" that don't reference any table.
type LogicalDummyScan struct{}

func (*LogicalDummyScan) logicalPlanNode() {}

func (*LogicalDummyScan) Children() []LogicalPlan { return nil }

func (*LogicalDummyScan) OutputColumns() []ColumnBinding { return nil }

// LogicalBegin represents a BEGIN TRANSACTION operation.
type LogicalBegin struct{}

func (*LogicalBegin) logicalPlanNode() {}

func (*LogicalBegin) Children() []LogicalPlan { return nil }

func (*LogicalBegin) OutputColumns() []ColumnBinding { return nil }

// LogicalCommit represents a COMMIT operation.
type LogicalCommit struct{}

func (*LogicalCommit) logicalPlanNode() {}

func (*LogicalCommit) Children() []LogicalPlan { return nil }

func (*LogicalCommit) OutputColumns() []ColumnBinding { return nil }

// LogicalRollback represents a ROLLBACK operation.
type LogicalRollback struct{}

func (*LogicalRollback) logicalPlanNode() {}

func (*LogicalRollback) Children() []LogicalPlan { return nil }

func (*LogicalRollback) OutputColumns() []ColumnBinding { return nil }

// LogicalCopyFrom represents a COPY FROM operation (import data from file).
type LogicalCopyFrom struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int // Column indices to import (nil for all)
	FilePath string
	Options  map[string]any
}

func (*LogicalCopyFrom) logicalPlanNode() {}

func (*LogicalCopyFrom) Children() []LogicalPlan { return nil }

func (*LogicalCopyFrom) OutputColumns() []ColumnBinding { return nil }

// LogicalCopyTo represents a COPY TO operation (export data to file).
type LogicalCopyTo struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int // Column indices to export (nil for all)
	FilePath string
	Options  map[string]any
	Source   LogicalPlan // For COPY (SELECT...) TO
}

func (*LogicalCopyTo) logicalPlanNode() {}

func (c *LogicalCopyTo) Children() []LogicalPlan {
	if c.Source != nil {
		return []LogicalPlan{c.Source}
	}
	return nil
}

func (*LogicalCopyTo) OutputColumns() []ColumnBinding { return nil }

// ---------- DDL Logical Plan Nodes ----------

// LogicalCreateView represents a CREATE VIEW operation.
type LogicalCreateView struct {
	Schema      string
	View        string
	IfNotExists bool
	OrReplace   bool
	Query       *binder.BoundSelectStmt
	QueryText   string
}

func (*LogicalCreateView) logicalPlanNode() {}

func (*LogicalCreateView) Children() []LogicalPlan { return nil }

func (*LogicalCreateView) OutputColumns() []ColumnBinding { return nil }

// LogicalDropView represents a DROP VIEW operation.
type LogicalDropView struct {
	Schema   string
	View     string
	IfExists bool
}

func (*LogicalDropView) logicalPlanNode() {}

func (*LogicalDropView) Children() []LogicalPlan { return nil }

func (*LogicalDropView) OutputColumns() []ColumnBinding { return nil }

// LogicalCreateIndex represents a CREATE INDEX operation.
type LogicalCreateIndex struct {
	Schema      string
	Table       string
	Index       string
	IfNotExists bool
	Columns     []string
	IsUnique    bool
	TableDef    *catalog.TableDef
}

func (*LogicalCreateIndex) logicalPlanNode() {}

func (*LogicalCreateIndex) Children() []LogicalPlan { return nil }

func (*LogicalCreateIndex) OutputColumns() []ColumnBinding { return nil }

// LogicalDropIndex represents a DROP INDEX operation.
type LogicalDropIndex struct {
	Schema   string
	Index    string
	IfExists bool
}

func (*LogicalDropIndex) logicalPlanNode() {}

func (*LogicalDropIndex) Children() []LogicalPlan { return nil }

func (*LogicalDropIndex) OutputColumns() []ColumnBinding { return nil }

// LogicalCreateSequence represents a CREATE SEQUENCE operation.
type LogicalCreateSequence struct {
	Schema      string
	Sequence    string
	IfNotExists bool
	StartWith   int64
	IncrementBy int64
	MinValue    *int64
	MaxValue    *int64
	IsCycle     bool
}

func (*LogicalCreateSequence) logicalPlanNode() {}

func (*LogicalCreateSequence) Children() []LogicalPlan { return nil }

func (*LogicalCreateSequence) OutputColumns() []ColumnBinding { return nil }

// LogicalDropSequence represents a DROP SEQUENCE operation.
type LogicalDropSequence struct {
	Schema   string
	Sequence string
	IfExists bool
}

func (*LogicalDropSequence) logicalPlanNode() {}

func (*LogicalDropSequence) Children() []LogicalPlan { return nil }

func (*LogicalDropSequence) OutputColumns() []ColumnBinding { return nil }

// LogicalCreateSchema represents a CREATE SCHEMA operation.
type LogicalCreateSchema struct {
	Schema      string
	IfNotExists bool
}

func (*LogicalCreateSchema) logicalPlanNode() {}

func (*LogicalCreateSchema) Children() []LogicalPlan { return nil }

func (*LogicalCreateSchema) OutputColumns() []ColumnBinding { return nil }

// LogicalDropSchema represents a DROP SCHEMA operation.
type LogicalDropSchema struct {
	Schema   string
	IfExists bool
	Cascade  bool
}

func (*LogicalDropSchema) logicalPlanNode() {}

func (*LogicalDropSchema) Children() []LogicalPlan { return nil }

func (*LogicalDropSchema) OutputColumns() []ColumnBinding { return nil }

// LogicalAlterTable represents an ALTER TABLE operation.
type LogicalAlterTable struct {
	Schema        string
	Table         string
	TableDef      *catalog.TableDef
	Operation     int                // AlterTableOp from parser
	IfExists      bool               // IF EXISTS modifier
	NewTableName  string             // RENAME TO
	OldColumn     string             // RENAME COLUMN
	NewColumn     string             // RENAME COLUMN
	DropColumn    string             // DROP COLUMN
	AddColumn      *catalog.ColumnDef      // ADD COLUMN
	AlterColumn    string                  // ALTER COLUMN TYPE
	NewColumnType  dukdb.Type              // ALTER COLUMN TYPE
	ConstraintName string                  // DROP CONSTRAINT
	Constraint     *parser.TableConstraint // ADD CONSTRAINT
	DefaultExpr    binder.BoundExpr       // SET DEFAULT expression
}

func (*LogicalAlterTable) logicalPlanNode() {}

func (*LogicalAlterTable) Children() []LogicalPlan { return nil }

func (*LogicalAlterTable) OutputColumns() []ColumnBinding { return nil }

// LogicalComment represents a COMMENT ON operation.
type LogicalComment struct {
	ObjectType string
	Schema     string
	ObjectName string
	ColumnName string
	Comment    *string
}

func (*LogicalComment) logicalPlanNode() {}

func (*LogicalComment) Children() []LogicalPlan { return nil }

func (*LogicalComment) OutputColumns() []ColumnBinding { return nil }

// ---------- Type DDL Logical Plan Nodes ----------

// LogicalCreateType represents a CREATE TYPE operation.
type LogicalCreateType struct {
	Name        string
	Schema      string
	TypeKind    string
	EnumValues  []string
	IfNotExists bool
}

func (*LogicalCreateType) logicalPlanNode() {}

func (*LogicalCreateType) Children() []LogicalPlan { return nil }

func (*LogicalCreateType) OutputColumns() []ColumnBinding { return nil }

// LogicalDropType represents a DROP TYPE operation.
type LogicalDropType struct {
	Name     string
	Schema   string
	IfExists bool
}

func (*LogicalDropType) logicalPlanNode() {}

func (*LogicalDropType) Children() []LogicalPlan { return nil }

func (*LogicalDropType) OutputColumns() []ColumnBinding { return nil }

// ---------- Macro DDL Logical Plan Nodes ----------

// LogicalCreateMacro represents a CREATE MACRO operation.
type LogicalCreateMacro struct {
	Schema       string
	Name         string
	Params       []catalog.MacroParam
	IsTableMacro bool
	OrReplace    bool
	BodySQL      string
	QuerySQL     string
}

func (*LogicalCreateMacro) logicalPlanNode() {}

func (*LogicalCreateMacro) Children() []LogicalPlan { return nil }

func (*LogicalCreateMacro) OutputColumns() []ColumnBinding { return nil }

// LogicalDropMacro represents a DROP MACRO operation.
type LogicalDropMacro struct {
	Schema       string
	Name         string
	IfExists     bool
	IsTableMacro bool
}

func (*LogicalDropMacro) logicalPlanNode() {}

func (*LogicalDropMacro) Children() []LogicalPlan { return nil }

func (*LogicalDropMacro) OutputColumns() []ColumnBinding { return nil }

// ---------- Secret DDL Logical Plan Nodes ----------

// LogicalCreateSecret represents a CREATE SECRET operation.
type LogicalCreateSecret struct {
	Name        string            // Secret name
	IfNotExists bool              // IF NOT EXISTS clause
	OrReplace   bool              // OR REPLACE clause
	Persistent  bool              // PERSISTENT vs TEMPORARY
	SecretType  string            // Type of secret (S3, GCS, AZURE, HTTP, HUGGINGFACE)
	Provider    string            // Provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
	Scope       string            // Optional scope path
	Options     map[string]string // Key-value options
}

func (*LogicalCreateSecret) logicalPlanNode() {}

func (*LogicalCreateSecret) Children() []LogicalPlan { return nil }

func (*LogicalCreateSecret) OutputColumns() []ColumnBinding { return nil }

// LogicalDropSecret represents a DROP SECRET operation.
type LogicalDropSecret struct {
	Name     string // Secret name
	IfExists bool   // IF EXISTS clause
}

func (*LogicalDropSecret) logicalPlanNode() {}

func (*LogicalDropSecret) Children() []LogicalPlan { return nil }

func (*LogicalDropSecret) OutputColumns() []ColumnBinding { return nil }

// LogicalAlterSecret represents an ALTER SECRET operation.
type LogicalAlterSecret struct {
	Name    string            // Secret name
	Options map[string]string // Options to update
}

func (*LogicalAlterSecret) logicalPlanNode() {}

func (*LogicalAlterSecret) Children() []LogicalPlan { return nil }

func (*LogicalAlterSecret) OutputColumns() []ColumnBinding { return nil }

// LogicalMerge represents a MERGE INTO operation.
type LogicalMerge struct {
	Schema                 string
	TargetTable            string
	TargetTableDef         *catalog.TableDef
	TargetAlias            string
	SourcePlan             LogicalPlan // The source table/subquery plan
	OnCondition            binder.BoundExpr
	WhenMatched            []*binder.BoundMergeAction
	WhenNotMatched         []*binder.BoundMergeAction
	WhenNotMatchedBySource []*binder.BoundMergeAction
	Returning              []*binder.BoundSelectColumn
}

func (*LogicalMerge) logicalPlanNode() {}

func (m *LogicalMerge) Children() []LogicalPlan {
	if m.SourcePlan != nil {
		return []LogicalPlan{m.SourcePlan}
	}
	return nil
}

func (*LogicalMerge) OutputColumns() []ColumnBinding { return nil }

// LogicalLateralJoin represents a LATERAL join operation.
// LATERAL joins allow the right side (subquery) to reference columns from the left side.
// The right side is re-evaluated for each row of the left side with the correlated values.
type LogicalLateralJoin struct {
	Left      LogicalPlan      // Outer table
	Right     LogicalPlan      // Correlated subquery (re-evaluated per left row)
	JoinType  JoinType         // Join type (CROSS, LEFT, etc.)
	Condition binder.BoundExpr // Optional join condition
	columns   []ColumnBinding
}

func (*LogicalLateralJoin) logicalPlanNode() {}

func (j *LogicalLateralJoin) Children() []LogicalPlan { return []LogicalPlan{j.Left, j.Right} }

func (j *LogicalLateralJoin) OutputColumns() []ColumnBinding {
	if j.columns != nil {
		return j.columns
	}

	leftCols := j.Left.OutputColumns()
	rightCols := j.Right.OutputColumns()
	j.columns = make(
		[]ColumnBinding,
		0,
		len(leftCols)+len(rightCols),
	)

	for i, col := range leftCols {
		j.columns = append(
			j.columns,
			ColumnBinding{
				Table:     col.Table,
				Column:    col.Column,
				Type:      col.Type,
				TableIdx:  0,
				ColumnIdx: i,
			},
		)
	}

	for i, col := range rightCols {
		j.columns = append(
			j.columns,
			ColumnBinding{
				Table:     col.Table,
				Column:    col.Column,
				Type:      col.Type,
				TableIdx:  1,
				ColumnIdx: i,
			},
		)
	}

	return j.columns
}

// ---------- Sample Logical Plan Node ----------

// LogicalSample represents a SAMPLE operation that samples a subset of rows.
// Supports BERNOULLI, SYSTEM, and RESERVOIR sampling methods.
type LogicalSample struct {
	Child  LogicalPlan
	Sample *binder.BoundSampleOptions
}

func (*LogicalSample) logicalPlanNode() {}

func (s *LogicalSample) Children() []LogicalPlan { return []LogicalPlan{s.Child} }

func (s *LogicalSample) OutputColumns() []ColumnBinding { return s.Child.OutputColumns() }

// ---------- CTE Logical Plan Nodes ----------

// LogicalRecursiveCTE represents a recursive Common Table Expression in the logical plan.
// A recursive CTE has a base case (anchor) and a recursive case that references the CTE itself.
// The execution semantics are:
// 1. Execute the base case to produce initial rows (work table)
// 2. Execute the recursive case using the work table as input
// 3. Append new results to the output and replace the work table
// 4. Repeat until no new rows are produced or max recursion is reached
type LogicalRecursiveCTE struct {
	// CTEName is the name of the CTE for reference
	CTEName string
	// BasePlan is the plan for the anchor (non-recursive) part
	BasePlan LogicalPlan
	// RecursivePlan is the plan for the recursive part (references the CTE)
	RecursivePlan LogicalPlan
	// Columns contains the output column information from the CTE
	Columns []ColumnBinding
	// UsingKey specifies USING KEY columns for recursive cycle detection.
	UsingKey []string
	// SetOp captures UNION vs UNION ALL for recursive CTE semantics.
	SetOp parser.SetOpType
	// MaxRecursion is the recursion limit from OPTION (MAX_RECURSION N).
	MaxRecursion int
}

func (*LogicalRecursiveCTE) logicalPlanNode() {}

func (r *LogicalRecursiveCTE) Children() []LogicalPlan {
	return []LogicalPlan{r.BasePlan, r.RecursivePlan}
}

func (r *LogicalRecursiveCTE) OutputColumns() []ColumnBinding {
	return r.Columns
}

// LogicalCTEScan represents a scan of a CTE in the logical plan.
// This is used when the main query references a CTE.
type LogicalCTEScan struct {
	// CTEName is the name of the CTE being referenced
	CTEName string
	// Alias is the alias for this reference
	Alias string
	// Columns contains the column information from the CTE
	Columns []ColumnBinding
	// CTEPlan is the plan for the CTE (may be nil for recursive self-reference)
	CTEPlan LogicalPlan
	// IsRecursive indicates if this is a recursive CTE
	IsRecursive bool
}

func (*LogicalCTEScan) logicalPlanNode() {}

func (c *LogicalCTEScan) Children() []LogicalPlan {
	if c.CTEPlan != nil {
		return []LogicalPlan{c.CTEPlan}
	}
	return nil
}

func (c *LogicalCTEScan) OutputColumns() []ColumnBinding {
	return c.Columns
}

// ---------- PIVOT/UNPIVOT Logical Plan Nodes ----------

// LogicalPivot represents a PIVOT operation in the logical plan.
// PIVOT transforms rows into columns using conditional aggregation.
type LogicalPivot struct {
	// Source is the source plan to pivot.
	Source LogicalPlan
	// ForColumn is the bound column reference whose values determine which pivot column to use.
	// In "FOR quarter IN ('Q1', 'Q2', ...)", this is the bound reference to "quarter".
	ForColumn *binder.BoundColumnRef
	// InValues contains the literal values to pivot on (become column names).
	InValues []any
	// Aggregates contains the bound aggregate functions to apply.
	Aggregates []*binder.BoundPivotAggregate
	// GroupBy contains the bound GROUP BY expressions.
	GroupBy []binder.BoundExpr
	// columns cache for OutputColumns
	columns []ColumnBinding
}

func (*LogicalPivot) logicalPlanNode() {}

func (p *LogicalPivot) Children() []LogicalPlan { return []LogicalPlan{p.Source} }

func (p *LogicalPivot) OutputColumns() []ColumnBinding {
	if p.columns != nil {
		return p.columns
	}

	// Output columns are: GROUP BY columns + (aggregate_for_each_pivot_value)
	var cols []ColumnBinding

	// Add GROUP BY columns
	for i, expr := range p.GroupBy {
		alias := ""
		if colRef, ok := expr.(*binder.BoundColumnRef); ok {
			alias = colRef.Column
		}
		cols = append(cols, ColumnBinding{
			Column:    alias,
			Type:      expr.ResultType(),
			ColumnIdx: i,
		})
	}

	// Add one column per (aggregate, pivot_value) combination
	aggIdx := len(p.GroupBy)
	for _, agg := range p.Aggregates {
		for _, val := range p.InValues {
			colName := formatPivotColumnName(agg.Function, agg.Alias, val)
			cols = append(cols, ColumnBinding{
				Column:    colName,
				Type:      agg.Expr.ResultType(),
				ColumnIdx: aggIdx,
			})
			aggIdx++
		}
	}

	p.columns = cols
	return p.columns
}

// formatPivotColumnName creates a column name for a pivot result column.
func formatPivotColumnName(funcName, alias string, pivotValue any) string {
	var valStr string
	switch v := pivotValue.(type) {
	case string:
		valStr = v
	default:
		valStr = formatAnyValue(v)
	}
	if alias != "" {
		return valStr + "_" + alias
	}
	return valStr
}

// formatAnyValue formats any value as a string for column naming.
func formatAnyValue(v any) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case string:
		return val
	case int64:
		return formatInt64(val)
	case float64:
		return formatFloat64(val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return "val"
	}
}

// LogicalUnpivot represents an UNPIVOT operation in the logical plan.
// UNPIVOT transforms columns into rows (the inverse of PIVOT).
type LogicalUnpivot struct {
	// Source is the source plan to unpivot.
	Source LogicalPlan
	// ValueColumn is the name of the column that will contain the unpivoted values.
	ValueColumn string
	// NameColumn is the name of the column that will contain the original column names.
	NameColumn string
	// UnpivotColumns contains the column names to unpivot.
	UnpivotColumns []string
	// columns cache for OutputColumns
	columns []ColumnBinding
}

func (*LogicalUnpivot) logicalPlanNode() {}

func (u *LogicalUnpivot) Children() []LogicalPlan { return []LogicalPlan{u.Source} }

func (u *LogicalUnpivot) OutputColumns() []ColumnBinding {
	if u.columns != nil {
		return u.columns
	}

	// Output columns are: non-unpivoted columns + name column + value column
	sourceCols := u.Source.OutputColumns()

	var cols []ColumnBinding
	idx := 0

	// Add non-unpivoted columns from source
	for _, col := range sourceCols {
		isUnpivot := false
		for _, unpivotCol := range u.UnpivotColumns {
			if col.Column == unpivotCol {
				isUnpivot = true
				break
			}
		}
		if !isUnpivot {
			cols = append(cols, ColumnBinding{
				Table:     col.Table,
				Column:    col.Column,
				Type:      col.Type,
				ColumnIdx: idx,
			})
			idx++
		}
	}

	// Add name column (VARCHAR)
	cols = append(cols, ColumnBinding{
		Column:    u.NameColumn,
		Type:      dukdb.TYPE_VARCHAR,
		ColumnIdx: idx,
	})
	idx++

	// Add value column (determine type from first unpivot column)
	valueType := dukdb.TYPE_ANY
	for _, col := range sourceCols {
		for _, unpivotCol := range u.UnpivotColumns {
			if col.Column == unpivotCol {
				valueType = col.Type
				break
			}
		}
		if valueType != dukdb.TYPE_ANY {
			break
		}
	}
	cols = append(cols, ColumnBinding{
		Column:    u.ValueColumn,
		Type:      valueType,
		ColumnIdx: idx,
	})

	u.columns = cols
	return u.columns
}

// ---------- SUMMARIZE Logical Plan Node ----------

// LogicalSummarize represents a logical SUMMARIZE operation.
type LogicalSummarize struct {
	Schema    string
	TableName string
	TableDef  *catalog.TableDef
	Query     LogicalPlan // Inner query plan (nil for SUMMARIZE table)
}

func (*LogicalSummarize) logicalPlanNode() {}

func (s *LogicalSummarize) Children() []LogicalPlan {
	if s.Query != nil {
		return []LogicalPlan{s.Query}
	}
	return nil
}

func (*LogicalSummarize) OutputColumns() []ColumnBinding { return nil }

// ---------- Export/Import Database Logical Plan Nodes ----------

// LogicalExportDatabase represents a logical EXPORT DATABASE operation.
type LogicalExportDatabase struct {
	Path    string
	Options map[string]string
}

func (*LogicalExportDatabase) logicalPlanNode() {}

func (*LogicalExportDatabase) Children() []LogicalPlan { return nil }

func (*LogicalExportDatabase) OutputColumns() []ColumnBinding { return nil }

// LogicalImportDatabase represents a logical IMPORT DATABASE operation.
type LogicalImportDatabase struct {
	Path string
}

func (*LogicalImportDatabase) logicalPlanNode() {}

func (*LogicalImportDatabase) Children() []LogicalPlan { return nil }

func (*LogicalImportDatabase) OutputColumns() []ColumnBinding { return nil }

// ---------- Database Maintenance Logical Plan Nodes ----------

// LogicalPragma represents a PRAGMA operation.
type LogicalPragma struct {
	Name       string             // Pragma name
	PragmaType binder.PragmaType  // Category of pragma
	Args       []binder.BoundExpr // Bound arguments
	Value      binder.BoundExpr   // For SET PRAGMA name = value
	columns    []ColumnBinding
}

func (*LogicalPragma) logicalPlanNode() {}

func (*LogicalPragma) Children() []LogicalPlan { return nil }

func (p *LogicalPragma) OutputColumns() []ColumnBinding {
	if p.columns != nil {
		return p.columns
	}
	// PRAGMA statements return varying columns depending on the pragma
	// The actual columns are determined at execution time
	return nil
}

// LogicalExplain represents an EXPLAIN operation.
type LogicalExplain struct {
	Child        LogicalPlan // The plan to explain
	Analyze      bool        // true for EXPLAIN ANALYZE
	RewriteStats *rewrite.Stats
}

func (*LogicalExplain) logicalPlanNode() {}

func (e *LogicalExplain) Children() []LogicalPlan { return []LogicalPlan{e.Child} }

func (*LogicalExplain) OutputColumns() []ColumnBinding {
	// EXPLAIN returns a single column with the plan text
	return []ColumnBinding{
		{Column: "explain_plan", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 0},
	}
}

// LogicalVacuum represents a VACUUM operation.
type LogicalVacuum struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for entire database)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*LogicalVacuum) logicalPlanNode() {}

func (*LogicalVacuum) Children() []LogicalPlan { return nil }

func (*LogicalVacuum) OutputColumns() []ColumnBinding { return nil }

// LogicalAnalyze represents an ANALYZE operation.
type LogicalAnalyze struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for all tables)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*LogicalAnalyze) logicalPlanNode() {}

func (*LogicalAnalyze) Children() []LogicalPlan { return nil }

func (*LogicalAnalyze) OutputColumns() []ColumnBinding { return nil }

// LogicalCheckpoint represents a CHECKPOINT operation.
type LogicalCheckpoint struct {
	Database string // Optional database name
	Force    bool   // FORCE flag
}

func (*LogicalCheckpoint) logicalPlanNode() {}

func (*LogicalCheckpoint) Children() []LogicalPlan { return nil }

func (*LogicalCheckpoint) OutputColumns() []ColumnBinding { return nil }

// ---------- Iceberg Logical Plan Nodes ----------

// TimeTravelType indicates the type of time travel clause.
type TimeTravelType int

const (
	// TimeTravelNone indicates no time travel.
	TimeTravelNone TimeTravelType = iota
	// TimeTravelSnapshot indicates time travel by snapshot ID.
	TimeTravelSnapshot
	// TimeTravelTimestamp indicates time travel by timestamp.
	TimeTravelTimestamp
	// TimeTravelBranch indicates time travel by branch name.
	TimeTravelBranch
	// TimeTravelVersion indicates time travel by metadata version.
	TimeTravelVersion
)

// TimeTravelClause represents a time travel specification for Iceberg tables.
type TimeTravelClause struct {
	// Type indicates the type of time travel.
	Type TimeTravelType
	// SnapshotID is the snapshot ID for TimeTravelSnapshot.
	SnapshotID *int64
	// Timestamp is the timestamp (milliseconds since epoch) for TimeTravelTimestamp.
	Timestamp *int64
	// BranchName is the branch name for TimeTravelBranch.
	BranchName string
	// Version is the metadata version for TimeTravelVersion.
	Version int
}

// LogicalIcebergScan represents a scan of an Apache Iceberg table.
// It supports time travel, partition pruning, and column projection.
type LogicalIcebergScan struct {
	// TablePath is the path to the Iceberg table location.
	TablePath string
	// Alias is the table alias for column reference qualification.
	Alias string
	// Columns contains the columns to project (nil = all columns).
	Columns []string
	// Filter contains the filter predicate for partition pruning.
	// This is pushed down to the Iceberg scan planner for partition pruning.
	Filter binder.BoundExpr
	// TimeTravel contains the time travel specification (nil = current snapshot).
	TimeTravel *TimeTravelClause
	// Options contains additional options for the Iceberg reader.
	Options map[string]any
	// columns caches the output column bindings.
	columns []ColumnBinding
	// ColumnTypes contains the types for each column (populated during planning).
	ColumnTypes []dukdb.Type
}

func (*LogicalIcebergScan) logicalPlanNode() {}

func (*LogicalIcebergScan) Children() []LogicalPlan { return nil }

func (s *LogicalIcebergScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}

	// Build column bindings from column names and types
	if len(s.Columns) > 0 && len(s.ColumnTypes) == len(s.Columns) {
		s.columns = make([]ColumnBinding, len(s.Columns))
		for i, colName := range s.Columns {
			s.columns[i] = ColumnBinding{
				Table:     s.Alias,
				Column:    colName,
				Type:      s.ColumnTypes[i],
				ColumnIdx: i,
			}
		}
	}

	return s.columns
}

// ---------- Database Management Logical Plan Nodes ----------

// LogicalAttach represents an ATTACH DATABASE operation.
type LogicalAttach struct {
	Path     string
	Alias    string
	ReadOnly bool
	Options  map[string]string
}

func (*LogicalAttach) logicalPlanNode() {}

func (*LogicalAttach) Children() []LogicalPlan { return nil }

func (*LogicalAttach) OutputColumns() []ColumnBinding { return nil }

// LogicalDetach represents a DETACH DATABASE operation.
type LogicalDetach struct {
	Name     string
	IfExists bool
}

func (*LogicalDetach) logicalPlanNode() {}

func (*LogicalDetach) Children() []LogicalPlan { return nil }

func (*LogicalDetach) OutputColumns() []ColumnBinding { return nil }

// LogicalUse represents a USE DATABASE operation.
type LogicalUse struct {
	Database string
	Schema   string
}

func (*LogicalUse) logicalPlanNode() {}

func (*LogicalUse) Children() []LogicalPlan { return nil }

func (*LogicalUse) OutputColumns() []ColumnBinding { return nil }

// LogicalCreateDatabase represents a CREATE DATABASE operation.
type LogicalCreateDatabase struct {
	Name        string
	IfNotExists bool
}

func (*LogicalCreateDatabase) logicalPlanNode() {}

func (*LogicalCreateDatabase) Children() []LogicalPlan { return nil }

func (*LogicalCreateDatabase) OutputColumns() []ColumnBinding { return nil }

// LogicalDropDatabase represents a DROP DATABASE operation.
type LogicalDropDatabase struct {
	Name     string
	IfExists bool
}

func (*LogicalDropDatabase) logicalPlanNode() {}

func (*LogicalDropDatabase) Children() []LogicalPlan { return nil }

func (*LogicalDropDatabase) OutputColumns() []ColumnBinding { return nil }

// ---------- Set Operation Logical Plan Nodes ----------

// SetOpType represents the type of set operation.
type SetOpType int

const (
	// SetOpUnion represents UNION (removes duplicates).
	SetOpUnion SetOpType = iota
	// SetOpUnionAll represents UNION ALL (preserves all rows).
	SetOpUnionAll
	// SetOpIntersect represents INTERSECT (removes duplicates).
	SetOpIntersect
	// SetOpIntersectAll represents INTERSECT ALL (preserves duplicates).
	SetOpIntersectAll
	// SetOpExcept represents EXCEPT (removes duplicates).
	SetOpExcept
	// SetOpExceptAll represents EXCEPT ALL (preserves duplicates).
	SetOpExceptAll
	// SetOpUnionByName represents UNION BY NAME.
	SetOpUnionByName
	// SetOpUnionAllByName represents UNION ALL BY NAME.
	SetOpUnionAllByName
)

// LogicalSetOp represents a set operation (UNION, INTERSECT, EXCEPT) in the logical plan.
type LogicalSetOp struct {
	// Left is the left side of the set operation.
	Left LogicalPlan
	// Right is the right side of the set operation.
	Right LogicalPlan
	// OpType is the type of set operation (UNION, UNION ALL, etc.).
	OpType SetOpType
	// columns caches the output column bindings.
	columns []ColumnBinding
}

func (*LogicalSetOp) logicalPlanNode() {}

func (s *LogicalSetOp) Children() []LogicalPlan {
	return []LogicalPlan{s.Left, s.Right}
}

func (s *LogicalSetOp) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}
	// Output columns are the same as the left side's columns
	s.columns = s.Left.OutputColumns()
	return s.columns
}
