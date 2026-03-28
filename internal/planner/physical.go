package planner

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
)

// PhysicalPlan represents a node in the physical query plan.
type PhysicalPlan interface {
	physicalPlanNode()
	// Children returns the child nodes of this plan.
	Children() []PhysicalPlan
	// OutputColumns returns the columns that this plan produces.
	OutputColumns() []ColumnBinding
}

// ---------- Physical Plan Nodes ----------

// PhysicalScan represents a physical table scan.
type PhysicalScan struct {
	Schema      string
	TableName   string
	Alias       string
	TableDef    *catalog.TableDef
	Projections []int
	columns     []ColumnBinding
}

func (*PhysicalScan) physicalPlanNode() {}

func (*PhysicalScan) Children() []PhysicalPlan { return nil }

func (s *PhysicalScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
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

// PhysicalIndexScan represents a physical index-based table scan.
// It uses an index to find matching rows rather than scanning the entire table.
type PhysicalIndexScan struct {
	// Table metadata
	Schema    string
	TableName string
	Alias     string
	TableDef  *catalog.TableDef

	// Index metadata
	IndexName string
	IndexDef  *catalog.IndexDef

	// LookupKeys are expressions that evaluate to the values to look up in the index.
	// For a single-column index, there's typically one key.
	// For composite indexes, there may be multiple keys (one per prefix column).
	// Used for point lookups (equality predicates).
	LookupKeys []binder.BoundExpr

	// Projections specifies which columns to return (nil = all columns)
	Projections []int

	// IsIndexOnly indicates if this is an index-only scan (covering index).
	// When true, all required columns can be satisfied from the index itself
	// without fetching the table row. Currently not fully supported since
	// HashIndex only stores RowIDs.
	IsIndexOnly bool

	// ResidualFilter contains any filter conditions that couldn't be pushed
	// into the index lookup and must be evaluated after fetching rows.
	// For example, if the index is on (a, b) but the query has WHERE a = 1 AND c > 5,
	// the "c > 5" predicate becomes a residual filter.
	ResidualFilter binder.BoundExpr

	// Range scan fields (for <, >, <=, >=, BETWEEN predicates)

	// IsRangeScan indicates this uses a range scan instead of point lookups.
	// When true, LowerBound and/or UpperBound are used instead of LookupKeys.
	IsRangeScan bool

	// LowerBound is the lower bound value expression for range scans.
	// May be nil for unbounded scans (e.g., col < 100 has no lower bound).
	LowerBound binder.BoundExpr

	// UpperBound is the upper bound value expression for range scans.
	// May be nil for unbounded scans (e.g., col > 10 has no upper bound).
	UpperBound binder.BoundExpr

	// LowerInclusive is true if the lower bound is inclusive (>=).
	LowerInclusive bool

	// UpperInclusive is true if the upper bound is inclusive (<=).
	UpperInclusive bool

	// RangeColumnIndex is the index of the range column within the composite index.
	// For single-column indexes, this is always 0.
	// For composite indexes with equality on prefix columns, this indicates
	// which column has the range predicate.
	RangeColumnIndex int

	// columns is a cache for OutputColumns()
	columns []ColumnBinding
}

func (*PhysicalIndexScan) physicalPlanNode() {}

func (*PhysicalIndexScan) Children() []PhysicalPlan { return nil }

func (s *PhysicalIndexScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
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

// PhysicalFilter represents a physical filter operation.
type PhysicalFilter struct {
	Child     PhysicalPlan
	Condition binder.BoundExpr
}

func (*PhysicalFilter) physicalPlanNode() {}

func (f *PhysicalFilter) Children() []PhysicalPlan { return []PhysicalPlan{f.Child} }

func (f *PhysicalFilter) OutputColumns() []ColumnBinding { return f.Child.OutputColumns() }

// PhysicalProject represents a physical projection.
type PhysicalProject struct {
	Child       PhysicalPlan
	Expressions []binder.BoundExpr
	Aliases     []string
	columns     []ColumnBinding
}

func (*PhysicalProject) physicalPlanNode() {}

func (p *PhysicalProject) Children() []PhysicalPlan { return []PhysicalPlan{p.Child} }

func (p *PhysicalProject) OutputColumns() []ColumnBinding {
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

// PhysicalHashJoin represents a physical hash join.
type PhysicalHashJoin struct {
	Left      PhysicalPlan
	Right     PhysicalPlan // Build side
	JoinType  JoinType
	Condition binder.BoundExpr
	columns   []ColumnBinding
}

func (*PhysicalHashJoin) physicalPlanNode() {}

func (j *PhysicalHashJoin) Children() []PhysicalPlan { return []PhysicalPlan{j.Left, j.Right} }

func (j *PhysicalHashJoin) OutputColumns() []ColumnBinding {
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

// PhysicalNestedLoopJoin represents a physical nested loop join.
type PhysicalNestedLoopJoin struct {
	Left      PhysicalPlan
	Right     PhysicalPlan
	JoinType  JoinType
	Condition binder.BoundExpr
	columns   []ColumnBinding
}

func (*PhysicalNestedLoopJoin) physicalPlanNode() {}

func (j *PhysicalNestedLoopJoin) Children() []PhysicalPlan { return []PhysicalPlan{j.Left, j.Right} }

func (j *PhysicalNestedLoopJoin) OutputColumns() []ColumnBinding {
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
	j.columns = append(j.columns, leftCols...)
	j.columns = append(j.columns, rightCols...)

	return j.columns
}

// PhysicalPositionalJoin represents a physical positional join.
// It matches rows by position (row 0 with row 0, row 1 with row 1, etc.).
type PhysicalPositionalJoin struct {
	Left    PhysicalPlan
	Right   PhysicalPlan
	columns []ColumnBinding
}

func (*PhysicalPositionalJoin) physicalPlanNode() {}

func (j *PhysicalPositionalJoin) Children() []PhysicalPlan {
	return []PhysicalPlan{j.Left, j.Right}
}

func (j *PhysicalPositionalJoin) OutputColumns() []ColumnBinding {
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
	j.columns = append(j.columns, leftCols...)
	j.columns = append(j.columns, rightCols...)

	return j.columns
}

// PhysicalAsOfJoin represents a physical ASOF join.
// It finds the nearest matching row based on an inequality condition.
type PhysicalAsOfJoin struct {
	Left      PhysicalPlan
	Right     PhysicalPlan
	JoinType  JoinType // JoinTypeAsOf or JoinTypeAsOfLeft
	Condition binder.BoundExpr
	columns   []ColumnBinding
}

func (*PhysicalAsOfJoin) physicalPlanNode() {}

func (j *PhysicalAsOfJoin) Children() []PhysicalPlan {
	return []PhysicalPlan{j.Left, j.Right}
}

func (j *PhysicalAsOfJoin) OutputColumns() []ColumnBinding {
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
	j.columns = append(j.columns, leftCols...)
	j.columns = append(j.columns, rightCols...)

	return j.columns
}

// PhysicalHashAggregate represents a physical hash aggregate.
type PhysicalHashAggregate struct {
	Child      PhysicalPlan
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

func (*PhysicalHashAggregate) physicalPlanNode() {}

func (a *PhysicalHashAggregate) Children() []PhysicalPlan { return []PhysicalPlan{a.Child} }

func (a *PhysicalHashAggregate) OutputColumns() []ColumnBinding {
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

// PhysicalWindow represents a physical window function operator.
// It evaluates window expressions over partitioned and ordered data.
type PhysicalWindow struct {
	Child       PhysicalPlan              // Child plan
	WindowExprs []*binder.BoundWindowExpr // Bound window expressions
	columns     []ColumnBinding
}

func (*PhysicalWindow) physicalPlanNode() {}

func (w *PhysicalWindow) Children() []PhysicalPlan { return []PhysicalPlan{w.Child} }

func (w *PhysicalWindow) OutputColumns() []ColumnBinding {
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

// PhysicalSort represents a physical sort operation.
type PhysicalSort struct {
	Child   PhysicalPlan
	OrderBy []*binder.BoundOrderBy
}

func (*PhysicalSort) physicalPlanNode() {}

func (s *PhysicalSort) Children() []PhysicalPlan { return []PhysicalPlan{s.Child} }

func (s *PhysicalSort) OutputColumns() []ColumnBinding { return s.Child.OutputColumns() }

// PhysicalLimit represents a physical limit operation.
type PhysicalLimit struct {
	Child      PhysicalPlan
	Limit      int64            // Static limit value (-1 means use LimitExpr)
	Offset     int64            // Static offset value (-1 means use OffsetExpr)
	LimitExpr  binder.BoundExpr // Dynamic limit expression (for LATERAL joins)
	OffsetExpr binder.BoundExpr // Dynamic offset expression (for LATERAL joins)
	WithTies   bool             // true when FETCH ... WITH TIES was used
	OrderBy    []*binder.BoundOrderBy // ORDER BY columns for WITH TIES comparison
}

func (*PhysicalLimit) physicalPlanNode() {}

func (l *PhysicalLimit) Children() []PhysicalPlan { return []PhysicalPlan{l.Child} }

func (l *PhysicalLimit) OutputColumns() []ColumnBinding { return l.Child.OutputColumns() }

// PhysicalDistinct represents a physical distinct operation.
type PhysicalDistinct struct {
	Child PhysicalPlan
}

func (*PhysicalDistinct) physicalPlanNode() {}

func (d *PhysicalDistinct) Children() []PhysicalPlan { return []PhysicalPlan{d.Child} }

func (d *PhysicalDistinct) OutputColumns() []ColumnBinding { return d.Child.OutputColumns() }

// PhysicalDistinctOn represents a physical DISTINCT ON operation.
// DISTINCT ON (col1, col2) keeps the first row for each unique combination of specified columns.
// The implementation sorts by DISTINCT ON columns, then filters to keep only the first row per group.
type PhysicalDistinctOn struct {
	Child      PhysicalPlan           // Child plan
	DistinctOn []binder.BoundExpr     // Expressions to distinct on
	OrderBy    []*binder.BoundOrderBy // The ORDER BY clause
}

func (*PhysicalDistinctOn) physicalPlanNode() {}

func (d *PhysicalDistinctOn) Children() []PhysicalPlan { return []PhysicalPlan{d.Child} }

func (d *PhysicalDistinctOn) OutputColumns() []ColumnBinding { return d.Child.OutputColumns() }

// PhysicalInsert represents a physical INSERT operation.
type PhysicalInsert struct {
	Schema     string
	Table      string
	TableDef   *catalog.TableDef
	Columns    []int
	Values     [][]binder.BoundExpr
	Source     PhysicalPlan
	OnConflict *binder.BoundOnConflictClause  // nil for plain INSERT
	Returning  []*binder.BoundSelectColumn    // RETURNING clause columns
}

func (*PhysicalInsert) physicalPlanNode() {}

func (i *PhysicalInsert) Children() []PhysicalPlan {
	if i.Source != nil {
		return []PhysicalPlan{i.Source}
	}

	return nil
}

func (*PhysicalInsert) OutputColumns() []ColumnBinding { return nil }

// PhysicalUpdate represents a physical UPDATE operation.
type PhysicalUpdate struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Set       []*binder.BoundSetClause
	Source    PhysicalPlan
	Returning []*binder.BoundSelectColumn // RETURNING clause columns
}

func (*PhysicalUpdate) physicalPlanNode() {}

func (u *PhysicalUpdate) Children() []PhysicalPlan {
	if u.Source != nil {
		return []PhysicalPlan{u.Source}
	}

	return nil
}

func (*PhysicalUpdate) OutputColumns() []ColumnBinding { return nil }

// PhysicalDelete represents a physical DELETE operation.
type PhysicalDelete struct {
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Source    PhysicalPlan
	Returning []*binder.BoundSelectColumn // RETURNING clause columns
}

func (*PhysicalDelete) physicalPlanNode() {}

func (d *PhysicalDelete) Children() []PhysicalPlan {
	if d.Source != nil {
		return []PhysicalPlan{d.Source}
	}

	return nil
}

func (*PhysicalDelete) OutputColumns() []ColumnBinding { return nil }

// PhysicalCreateTable represents a physical CREATE TABLE operation.
type PhysicalCreateTable struct {
	Schema      string
	Table       string
	IfNotExists bool
	OrReplace   bool
	Temporary   bool
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
	Constraints []any // *catalog.UniqueConstraintDef, *catalog.CheckConstraintDef
}

func (*PhysicalCreateTable) physicalPlanNode() {}

func (*PhysicalCreateTable) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateTable) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropTable represents a physical DROP TABLE operation.
type PhysicalDropTable struct {
	Schema   string
	Table    string
	IfExists bool
}

func (*PhysicalDropTable) physicalPlanNode() {}

func (*PhysicalDropTable) Children() []PhysicalPlan { return nil }

func (*PhysicalDropTable) OutputColumns() []ColumnBinding { return nil }

// PhysicalTruncate represents a physical TRUNCATE TABLE operation.
type PhysicalTruncate struct {
	Schema string
	Table  string
}

func (*PhysicalTruncate) physicalPlanNode() {}

func (*PhysicalTruncate) Children() []PhysicalPlan { return nil }

func (*PhysicalTruncate) OutputColumns() []ColumnBinding { return nil }

// PhysicalDummyScan represents a physical dummy scan.
type PhysicalDummyScan struct{}

func (*PhysicalDummyScan) physicalPlanNode() {}

func (*PhysicalDummyScan) Children() []PhysicalPlan { return nil }

func (*PhysicalDummyScan) OutputColumns() []ColumnBinding { return nil }

// PhysicalValues represents a VALUES clause that produces inline rows.
// Example: VALUES (1, 'a'), (2, 'b') produces two rows with two columns.
type PhysicalValues struct {
	Rows    [][]binder.BoundExpr // Bound expressions per row
	Columns []ColumnBinding      // Output column names and types
}

func (*PhysicalValues) physicalPlanNode() {}

func (*PhysicalValues) Children() []PhysicalPlan { return nil }

func (v *PhysicalValues) OutputColumns() []ColumnBinding { return v.Columns }

// PhysicalBegin represents a physical BEGIN TRANSACTION plan.
type PhysicalBegin struct{}

func (*PhysicalBegin) physicalPlanNode() {}

func (*PhysicalBegin) Children() []PhysicalPlan { return nil }

func (*PhysicalBegin) OutputColumns() []ColumnBinding { return nil }

// PhysicalCommit represents a physical COMMIT plan.
type PhysicalCommit struct{}

func (*PhysicalCommit) physicalPlanNode() {}

func (*PhysicalCommit) Children() []PhysicalPlan { return nil }

func (*PhysicalCommit) OutputColumns() []ColumnBinding { return nil }

// PhysicalRollback represents a physical ROLLBACK plan.
type PhysicalRollback struct{}

func (*PhysicalRollback) physicalPlanNode() {}

func (*PhysicalRollback) Children() []PhysicalPlan { return nil }

func (*PhysicalRollback) OutputColumns() []ColumnBinding { return nil }

// PhysicalCopyFrom represents a physical COPY FROM operation.
type PhysicalCopyFrom struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int
	FilePath string
	Options  map[string]any
}

func (*PhysicalCopyFrom) physicalPlanNode() {}

func (*PhysicalCopyFrom) Children() []PhysicalPlan { return nil }

func (*PhysicalCopyFrom) OutputColumns() []ColumnBinding { return nil }

// PhysicalCopyTo represents a physical COPY TO operation.
type PhysicalCopyTo struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int
	FilePath string
	Options  map[string]any
	Source   PhysicalPlan // For COPY (SELECT...) TO
}

func (*PhysicalCopyTo) physicalPlanNode() {}

func (c *PhysicalCopyTo) Children() []PhysicalPlan {
	if c.Source != nil {
		return []PhysicalPlan{c.Source}
	}
	return nil
}

func (*PhysicalCopyTo) OutputColumns() []ColumnBinding { return nil }

// ---------- DDL Physical Plan Nodes ----------

// PhysicalCreateView represents a physical CREATE VIEW operation.
type PhysicalCreateView struct {
	Schema      string
	View        string
	IfNotExists bool
	OrReplace   bool
	Query       *binder.BoundSelectStmt
	QueryText   string
}

func (*PhysicalCreateView) physicalPlanNode() {}

func (*PhysicalCreateView) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateView) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropView represents a physical DROP VIEW operation.
type PhysicalDropView struct {
	Schema   string
	View     string
	IfExists bool
}

func (*PhysicalDropView) physicalPlanNode() {}

func (*PhysicalDropView) Children() []PhysicalPlan { return nil }

func (*PhysicalDropView) OutputColumns() []ColumnBinding { return nil }

// PhysicalCreateIndex represents a physical CREATE INDEX operation.
type PhysicalCreateIndex struct {
	Schema      string
	Table       string
	Index       string
	IfNotExists bool
	Columns     []string
	IsUnique    bool
	TableDef    *catalog.TableDef
}

func (*PhysicalCreateIndex) physicalPlanNode() {}

func (*PhysicalCreateIndex) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateIndex) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropIndex represents a physical DROP INDEX operation.
type PhysicalDropIndex struct {
	Schema   string
	Index    string
	IfExists bool
}

func (*PhysicalDropIndex) physicalPlanNode() {}

func (*PhysicalDropIndex) Children() []PhysicalPlan { return nil }

func (*PhysicalDropIndex) OutputColumns() []ColumnBinding { return nil }

// PhysicalCreateSequence represents a physical CREATE SEQUENCE operation.
type PhysicalCreateSequence struct {
	Schema      string
	Sequence    string
	IfNotExists bool
	StartWith   int64
	IncrementBy int64
	MinValue    *int64
	MaxValue    *int64
	IsCycle     bool
}

func (*PhysicalCreateSequence) physicalPlanNode() {}

func (*PhysicalCreateSequence) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateSequence) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropSequence represents a physical DROP SEQUENCE operation.
type PhysicalDropSequence struct {
	Schema   string
	Sequence string
	IfExists bool
}

func (*PhysicalDropSequence) physicalPlanNode() {}

func (*PhysicalDropSequence) Children() []PhysicalPlan { return nil }

func (*PhysicalDropSequence) OutputColumns() []ColumnBinding { return nil }

// PhysicalCreateSchema represents a physical CREATE SCHEMA operation.
type PhysicalCreateSchema struct {
	Schema      string
	IfNotExists bool
}

func (*PhysicalCreateSchema) physicalPlanNode() {}

func (*PhysicalCreateSchema) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateSchema) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropSchema represents a physical DROP SCHEMA operation.
type PhysicalDropSchema struct {
	Schema   string
	IfExists bool
	Cascade  bool
}

func (*PhysicalDropSchema) physicalPlanNode() {}

func (*PhysicalDropSchema) Children() []PhysicalPlan { return nil }

func (*PhysicalDropSchema) OutputColumns() []ColumnBinding { return nil }

// PhysicalAlterTable represents a physical ALTER TABLE operation.
type PhysicalAlterTable struct {
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

func (*PhysicalAlterTable) physicalPlanNode() {}

func (*PhysicalAlterTable) Children() []PhysicalPlan { return nil }

func (*PhysicalAlterTable) OutputColumns() []ColumnBinding { return nil }

// PhysicalComment represents a physical COMMENT ON operation.
type PhysicalComment struct {
	ObjectType string
	Schema     string
	ObjectName string
	ColumnName string
	Comment    *string
}

func (*PhysicalComment) physicalPlanNode() {}

func (*PhysicalComment) Children() []PhysicalPlan { return nil }

func (*PhysicalComment) OutputColumns() []ColumnBinding { return nil }

// ---------- Type DDL Physical Plan Nodes ----------

// PhysicalCreateType represents a physical CREATE TYPE operation.
type PhysicalCreateType struct {
	Name        string
	Schema      string
	TypeKind    string
	EnumValues  []string
	IfNotExists bool
}

func (*PhysicalCreateType) physicalPlanNode() {}

func (*PhysicalCreateType) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateType) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropType represents a physical DROP TYPE operation.
type PhysicalDropType struct {
	Name     string
	Schema   string
	IfExists bool
}

func (*PhysicalDropType) physicalPlanNode() {}

func (*PhysicalDropType) Children() []PhysicalPlan { return nil }

func (*PhysicalDropType) OutputColumns() []ColumnBinding { return nil }

// ---------- Macro DDL Physical Plan Nodes ----------

// PhysicalCreateMacro represents a physical CREATE MACRO operation.
type PhysicalCreateMacro struct {
	Schema       string
	Name         string
	Params       []catalog.MacroParam
	IsTableMacro bool
	OrReplace    bool
	BodySQL      string
	QuerySQL     string
}

func (*PhysicalCreateMacro) physicalPlanNode() {}

func (*PhysicalCreateMacro) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateMacro) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropMacro represents a physical DROP MACRO operation.
type PhysicalDropMacro struct {
	Schema       string
	Name         string
	IfExists     bool
	IsTableMacro bool
}

func (*PhysicalDropMacro) physicalPlanNode() {}

func (*PhysicalDropMacro) Children() []PhysicalPlan { return nil }

func (*PhysicalDropMacro) OutputColumns() []ColumnBinding { return nil }

// ---------- Secret DDL Physical Plan Nodes ----------

// PhysicalCreateSecret represents a physical CREATE SECRET operation.
type PhysicalCreateSecret struct {
	Name        string            // Secret name
	IfNotExists bool              // IF NOT EXISTS clause
	OrReplace   bool              // OR REPLACE clause
	Persistent  bool              // PERSISTENT vs TEMPORARY
	SecretType  string            // Type of secret (S3, GCS, AZURE, HTTP, HUGGINGFACE)
	Provider    string            // Provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
	Scope       string            // Optional scope path
	Options     map[string]string // Key-value options
}

func (*PhysicalCreateSecret) physicalPlanNode() {}

func (*PhysicalCreateSecret) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateSecret) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropSecret represents a physical DROP SECRET operation.
type PhysicalDropSecret struct {
	Name     string // Secret name
	IfExists bool   // IF EXISTS clause
}

func (*PhysicalDropSecret) physicalPlanNode() {}

func (*PhysicalDropSecret) Children() []PhysicalPlan { return nil }

func (*PhysicalDropSecret) OutputColumns() []ColumnBinding { return nil }

// PhysicalAlterSecret represents a physical ALTER SECRET operation.
type PhysicalAlterSecret struct {
	Name    string            // Secret name
	Options map[string]string // Options to update
}

func (*PhysicalAlterSecret) physicalPlanNode() {}

func (*PhysicalAlterSecret) Children() []PhysicalPlan { return nil }

func (*PhysicalAlterSecret) OutputColumns() []ColumnBinding { return nil }

// PhysicalMerge represents a physical MERGE INTO operation.
type PhysicalMerge struct {
	Schema                 string
	TargetTable            string
	TargetTableDef         *catalog.TableDef
	TargetAlias            string
	SourcePlan             PhysicalPlan // The source table/subquery plan
	OnCondition            binder.BoundExpr
	WhenMatched            []*binder.BoundMergeAction
	WhenNotMatched         []*binder.BoundMergeAction
	WhenNotMatchedBySource []*binder.BoundMergeAction
	Returning              []*binder.BoundSelectColumn
}

func (*PhysicalMerge) physicalPlanNode() {}

func (m *PhysicalMerge) Children() []PhysicalPlan {
	if m.SourcePlan != nil {
		return []PhysicalPlan{m.SourcePlan}
	}
	return nil
}

func (*PhysicalMerge) OutputColumns() []ColumnBinding { return nil }

// PhysicalLateralJoin represents a physical LATERAL join operation.
// LATERAL joins re-evaluate the right side for each row of the left side,
// allowing the right side to reference columns from the left side.
type PhysicalLateralJoin struct {
	Left      PhysicalPlan     // Outer table (scanned once)
	Right     PhysicalPlan     // Correlated subquery (re-evaluated per left row)
	JoinType  JoinType         // Join type (CROSS, LEFT, etc.)
	Condition binder.BoundExpr // Optional join condition
	columns   []ColumnBinding
}

func (*PhysicalLateralJoin) physicalPlanNode() {}

func (j *PhysicalLateralJoin) Children() []PhysicalPlan { return []PhysicalPlan{j.Left, j.Right} }

func (j *PhysicalLateralJoin) OutputColumns() []ColumnBinding {
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

// PhysicalVirtualTableScan represents a physical virtual table scan.
type PhysicalVirtualTableScan struct {
	Schema       string
	TableName    string
	Alias        string
	VirtualTable *catalog.VirtualTableDef
	Projections  []int
	columns      []ColumnBinding
}

// PhysicalTableFunctionScan represents a physical table function scan.
// This is used for table functions like read_csv, read_json, read_parquet.
type PhysicalTableFunctionScan struct {
	FunctionName  string
	Alias         string
	Path          string
	Options       map[string]any
	Projections   []int
	columns       []ColumnBinding
	TableFunction *binder.BoundTableFunctionRef
}

func (*PhysicalVirtualTableScan) physicalPlanNode() {}

func (*PhysicalVirtualTableScan) Children() []PhysicalPlan { return nil }

func (s *PhysicalVirtualTableScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}

	cols := s.VirtualTable.Columns()
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

func (*PhysicalTableFunctionScan) physicalPlanNode() {}

func (*PhysicalTableFunctionScan) Children() []PhysicalPlan { return nil }

func (s *PhysicalTableFunctionScan) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}

	// Columns are determined dynamically at execution time
	// If we have bound columns from the table function, use those
	if s.TableFunction != nil && s.TableFunction.Columns != nil {
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

// ---------- Database Management Physical Plan Nodes ----------

// PhysicalAttach represents a physical ATTACH DATABASE operation.
type PhysicalAttach struct {
	Path     string
	Alias    string
	ReadOnly bool
	Options  map[string]string
}

func (*PhysicalAttach) physicalPlanNode() {}

func (*PhysicalAttach) Children() []PhysicalPlan { return nil }

func (*PhysicalAttach) OutputColumns() []ColumnBinding { return nil }

// PhysicalDetach represents a physical DETACH DATABASE operation.
type PhysicalDetach struct {
	Name     string
	IfExists bool
}

func (*PhysicalDetach) physicalPlanNode() {}

func (*PhysicalDetach) Children() []PhysicalPlan { return nil }

func (*PhysicalDetach) OutputColumns() []ColumnBinding { return nil }

// PhysicalUse represents a physical USE DATABASE operation.
type PhysicalUse struct {
	Database string
	Schema   string
}

func (*PhysicalUse) physicalPlanNode() {}

func (*PhysicalUse) Children() []PhysicalPlan { return nil }

func (*PhysicalUse) OutputColumns() []ColumnBinding { return nil }

// PhysicalCreateDatabase represents a physical CREATE DATABASE operation.
type PhysicalCreateDatabase struct {
	Name        string
	IfNotExists bool
}

func (*PhysicalCreateDatabase) physicalPlanNode() {}

func (*PhysicalCreateDatabase) Children() []PhysicalPlan { return nil }

func (*PhysicalCreateDatabase) OutputColumns() []ColumnBinding { return nil }

// PhysicalDropDatabase represents a physical DROP DATABASE operation.
type PhysicalDropDatabase struct {
	Name     string
	IfExists bool
}

func (*PhysicalDropDatabase) physicalPlanNode() {}

func (*PhysicalDropDatabase) Children() []PhysicalPlan { return nil }

func (*PhysicalDropDatabase) OutputColumns() []ColumnBinding { return nil }

// ---------- Sample Physical Plan Node ----------

// PhysicalSample represents a physical sample operation.
// Supports BERNOULLI, SYSTEM, and RESERVOIR sampling methods.
type PhysicalSample struct {
	Child  PhysicalPlan
	Sample *binder.BoundSampleOptions
}

func (*PhysicalSample) physicalPlanNode() {}

func (s *PhysicalSample) Children() []PhysicalPlan { return []PhysicalPlan{s.Child} }

func (s *PhysicalSample) OutputColumns() []ColumnBinding { return s.Child.OutputColumns() }

// ---------- Set Operation Physical Plan Nodes ----------

// PhysicalSetOp represents a physical set operation (UNION, INTERSECT, EXCEPT).
type PhysicalSetOp struct {
	// Left is the left side of the set operation.
	Left PhysicalPlan
	// Right is the right side of the set operation.
	Right PhysicalPlan
	// OpType is the type of set operation (UNION, UNION ALL, etc.).
	OpType SetOpType
	// columns caches the output column bindings.
	columns []ColumnBinding
}

func (*PhysicalSetOp) physicalPlanNode() {}

func (s *PhysicalSetOp) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Left, s.Right}
}

func (s *PhysicalSetOp) OutputColumns() []ColumnBinding {
	if s.columns != nil {
		return s.columns
	}
	// Output columns are the same as the left side's columns
	s.columns = s.Left.OutputColumns()
	return s.columns
}

// ---------- CTE Physical Plan Nodes ----------

// PhysicalRecursiveCTE represents a physical recursive CTE operation.
// The execution algorithm is:
// 1. Execute the base plan to produce initial rows (work table)
// 2. Execute the recursive plan using the work table as input
// 3. Append new results to the output and replace the work table
// 4. Repeat until no new rows are produced or max recursion is reached
type PhysicalRecursiveCTE struct {
	// CTEName is the name of the CTE for reference
	CTEName string
	// BasePlan is the plan for the anchor (non-recursive) part
	BasePlan PhysicalPlan
	// RecursivePlan is the plan for the recursive part (references the CTE)
	RecursivePlan PhysicalPlan
	// Columns contains the output column information from the CTE
	Columns []ColumnBinding
	// UsingKey specifies USING KEY columns for recursive cycle detection.
	UsingKey []string
	// SetOp captures UNION vs UNION ALL for recursive CTE semantics.
	SetOp parser.SetOpType
	// MaxRecursion is the maximum number of recursion iterations (default 1000)
	MaxRecursion int
}

func (*PhysicalRecursiveCTE) physicalPlanNode() {}

func (r *PhysicalRecursiveCTE) Children() []PhysicalPlan {
	return []PhysicalPlan{r.BasePlan, r.RecursivePlan}
}

func (r *PhysicalRecursiveCTE) OutputColumns() []ColumnBinding {
	return r.Columns
}

// PhysicalCTEScan represents a physical scan of a CTE.
// This is used when the main query references a CTE.
type PhysicalCTEScan struct {
	// CTEName is the name of the CTE being referenced
	CTEName string
	// Alias is the alias for this reference
	Alias string
	// Columns contains the column information from the CTE
	Columns []ColumnBinding
	// CTEPlan is the plan for the CTE (may be nil for recursive self-reference)
	CTEPlan PhysicalPlan
	// IsRecursive indicates if this is a recursive CTE
	IsRecursive bool
}

func (*PhysicalCTEScan) physicalPlanNode() {}

func (c *PhysicalCTEScan) Children() []PhysicalPlan {
	if c.CTEPlan != nil {
		return []PhysicalPlan{c.CTEPlan}
	}
	return nil
}

func (c *PhysicalCTEScan) OutputColumns() []ColumnBinding {
	return c.Columns
}

// ---------- PIVOT/UNPIVOT Physical Plan Nodes ----------

// PhysicalPivot represents a physical PIVOT operation.
// PIVOT transforms rows into columns using conditional aggregation.
type PhysicalPivot struct {
	// Source is the child physical plan.
	Source PhysicalPlan
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

func (*PhysicalPivot) physicalPlanNode() {}

func (p *PhysicalPivot) Children() []PhysicalPlan { return []PhysicalPlan{p.Source} }

func (p *PhysicalPivot) OutputColumns() []ColumnBinding {
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

// PhysicalUnpivot represents a physical UNPIVOT operation.
// UNPIVOT transforms columns into rows (the inverse of PIVOT).
type PhysicalUnpivot struct {
	// Source is the child physical plan.
	Source PhysicalPlan
	// ValueColumn is the name of the column that will contain the unpivoted values.
	ValueColumn string
	// NameColumn is the name of the column that will contain the original column names.
	NameColumn string
	// UnpivotColumns contains the column names to unpivot.
	UnpivotColumns []string
	// columns cache for OutputColumns
	columns []ColumnBinding
}

func (*PhysicalUnpivot) physicalPlanNode() {}

func (u *PhysicalUnpivot) Children() []PhysicalPlan { return []PhysicalPlan{u.Source} }

func (u *PhysicalUnpivot) OutputColumns() []ColumnBinding {
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

// Planner converts bound statements to physical plans.
type Planner struct {
	catalog       *catalog.Catalog
	hints         *OptimizationHints // Optional optimization hints from CBO
	joinIndex     int                // Counter for generating join hint keys
	rewriteConfig rewrite.Config
}

// NewPlanner creates a new Planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	return &Planner{catalog: cat, rewriteConfig: rewrite.DefaultConfig()}
}

// SetHints sets optimization hints for physical plan selection.
// The hints guide join method selection and access path choices.
func (p *Planner) SetHints(hints *OptimizationHints) {
	p.hints = hints
}

// Plan creates a physical plan from a bound statement.
func (p *Planner) Plan(
	stmt binder.BoundStatement,
) (PhysicalPlan, error) {
	// Reset join index for each planning session
	p.joinIndex = 0

	// First create logical plan, then convert to physical
	logical, err := p.createLogicalPlan(stmt)
	if err != nil {
		return nil, err
	}

	if rewritten, _ := p.applyRewrites(logical); rewritten != nil {
		logical = rewritten
	}

	return p.createPhysicalPlan(logical)
}

func (p *Planner) createLogicalPlan(
	stmt binder.BoundStatement,
) (LogicalPlan, error) {
	switch s := stmt.(type) {
	case *binder.BoundSelectStmt:
		return p.planSelect(s)
	case *binder.BoundInsertStmt:
		return p.planInsert(s)
	case *binder.BoundUpdateStmt:
		return p.planUpdate(s)
	case *binder.BoundDeleteStmt:
		return p.planDelete(s)
	case *binder.BoundCreateTableStmt:
		return p.planCreateTable(s)
	case *binder.BoundDropTableStmt:
		return p.planDropTable(s)
	case *binder.BoundTruncateStmt:
		return &LogicalTruncate{
			Schema: s.Schema,
			Table:  s.Table,
		}, nil
	case *binder.BoundBeginStmt:
		return &LogicalBegin{}, nil
	case *binder.BoundCommitStmt:
		return &LogicalCommit{}, nil
	case *binder.BoundRollbackStmt:
		return &LogicalRollback{}, nil
	case *binder.BoundCopyStmt:
		return p.planCopy(s)
	// DDL statements
	case *binder.BoundCreateViewStmt:
		return p.planCreateView(s)
	case *binder.BoundDropViewStmt:
		return p.planDropView(s)
	case *binder.BoundCreateIndexStmt:
		return p.planCreateIndex(s)
	case *binder.BoundDropIndexStmt:
		return p.planDropIndex(s)
	case *binder.BoundCreateSequenceStmt:
		return p.planCreateSequence(s)
	case *binder.BoundDropSequenceStmt:
		return p.planDropSequence(s)
	case *binder.BoundCreateSchemaStmt:
		return p.planCreateSchema(s)
	case *binder.BoundDropSchemaStmt:
		return p.planDropSchema(s)
	case *binder.BoundAlterTableStmt:
		return p.planAlterTable(s)
	case *binder.BoundCommentStmt:
		return p.planComment(s)
	// Type DDL statements
	case *binder.BoundCreateTypeStmt:
		return &LogicalCreateType{
			Name:        s.Name,
			Schema:      s.Schema,
			TypeKind:    s.TypeKind,
			EnumValues:  s.EnumValues,
			IfNotExists: s.IfNotExists,
		}, nil
	case *binder.BoundDropTypeStmt:
		return &LogicalDropType{
			Name:     s.Name,
			Schema:   s.Schema,
			IfExists: s.IfExists,
		}, nil
	// Macro DDL statements
	case *binder.BoundCreateMacroStmt:
		return &LogicalCreateMacro{
			Schema:       s.Schema,
			Name:         s.Name,
			Params:       s.Params,
			IsTableMacro: s.IsTableMacro,
			OrReplace:    s.OrReplace,
			BodySQL:      s.BodySQL,
			QuerySQL:     s.QuerySQL,
		}, nil
	case *binder.BoundDropMacroStmt:
		return &LogicalDropMacro{
			Schema:       s.Schema,
			Name:         s.Name,
			IfExists:     s.IfExists,
			IsTableMacro: s.IsTableMacro,
		}, nil
	case *binder.BoundMergeStmt:
		return p.planMerge(s)
	case *binder.BoundPivotStmt:
		return p.planPivot(s)
	case *binder.BoundUnpivotStmt:
		return p.planUnpivot(s)
	// Secret DDL statements
	case *binder.BoundCreateSecretStmt:
		return p.planCreateSecret(s)
	case *binder.BoundDropSecretStmt:
		return p.planDropSecret(s)
	case *binder.BoundAlterSecretStmt:
		return p.planAlterSecret(s)
	// Database management statements
	case *binder.BoundAttachStmt:
		return &LogicalAttach{
			Path:     s.Path,
			Alias:    s.Alias,
			ReadOnly: s.ReadOnly,
			Options:  s.Options,
		}, nil
	case *binder.BoundDetachStmt:
		return &LogicalDetach{
			Name:     s.Name,
			IfExists: s.IfExists,
		}, nil
	case *binder.BoundUseStmt:
		return &LogicalUse{
			Database: s.Database,
			Schema:   s.Schema,
		}, nil
	case *binder.BoundCreateDatabaseStmt:
		return &LogicalCreateDatabase{
			Name:        s.Name,
			IfNotExists: s.IfNotExists,
		}, nil
	case *binder.BoundDropDatabaseStmt:
		return &LogicalDropDatabase{
			Name:     s.Name,
			IfExists: s.IfExists,
		}, nil
	// Database maintenance statements
	case *binder.BoundExportDatabaseStmt:
		return &LogicalExportDatabase{Path: s.Path, Options: s.Options}, nil
	case *binder.BoundImportDatabaseStmt:
		return &LogicalImportDatabase{Path: s.Path}, nil
	case *binder.BoundSummarizeStmt:
		return p.planSummarize(s)
	case *binder.BoundPragmaStmt:
		return p.planPragma(s)
	case *binder.BoundExplainStmt:
		return p.planExplain(s)
	case *binder.BoundVacuumStmt:
		return p.planVacuum(s)
	case *binder.BoundAnalyzeStmt:
		return p.planAnalyze(s)
	case *binder.BoundCheckpointStmt:
		return p.planCheckpoint(s)
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg:  "unsupported statement type",
		}
	}
}

func (p *Planner) planSelect(
	s *binder.BoundSelectStmt,
) (LogicalPlan, error) {
	var plan LogicalPlan

	// Start with FROM clause
	if len(s.From) > 0 {
		// First table
		plan = p.createScanForBoundTableRef(s.From[0])

		// Additional tables (implicit cross join)
		for i := 1; i < len(s.From); i++ {
			tableRef := s.From[i]
			right := p.createScanForBoundTableRef(tableRef)

			// Check if this is a LATERAL join (table ref can reference previous tables)
			if tableRef.Lateral {
				plan = &LogicalLateralJoin{
					Left:     plan,
					Right:    right,
					JoinType: JoinTypeCross,
				}
			} else {
				plan = &LogicalJoin{
					Left:     plan,
					Right:    right,
					JoinType: JoinTypeCross,
				}
			}
		}

		// Explicit JOINs
		for _, join := range s.Joins {
			right := p.createScanForBoundTableRef(join.Table)

			joinType := mapParserJoinType(join.Type)

			// Check if this is a LATERAL join
			if join.Table.Lateral {
				plan = &LogicalLateralJoin{
					Left:      plan,
					Right:     right,
					JoinType:  joinType,
					Condition: join.Condition,
				}
			} else {
				plan = &LogicalJoin{
					Left:      plan,
					Right:     right,
					JoinType:  joinType,
					Condition: join.Condition,
				}
			}
		}
	} else {
		// No FROM clause - use dummy scan for expressions
		plan = &LogicalDummyScan{}
	}

	// WHERE
	if s.Where != nil {
		plan = &LogicalFilter{
			Child:     plan,
			Condition: s.Where,
		}
	}

	// GROUP BY / Aggregates (only non-window aggregates)
	// projectionExprs holds the final expressions for the PROJECT node (may be
	// rewritten to replace nested aggregate calls with column references).
	projectionExprs := make([]binder.BoundExpr, len(s.Columns))
	for i, col := range s.Columns {
		projectionExprs[i] = col.Expr
	}

	if len(s.GroupBy) > 0 ||
		hasNonWindowAggregates(s.Columns) {
		// Extract grouping sets if present
		groupingSets, regularGroupBy := extractGroupingSets(s.GroupBy)
		groupBy := regularGroupBy
		aggregates := extractNonWindowAggregates(s.Columns)

		// Extract GROUPING() function calls from the select columns
		groupingCalls := extractGroupingCalls(s.Columns)

		// Assign aliases for each aggregate expression.
		// For direct aggregates (top-level expr is the agg), use the column alias.
		// For nested aggregates (agg inside wrapper like ROUND), use a synthetic alias.
		aggAliases := assignAggregateAliases(s.Columns, regularGroupBy, aggregates, groupingCalls)

		// Build the full aliases list: [groupBy aliases..., agg aliases..., groupingCall aliases...]
		aliases := buildFullAliases(s.Columns, regularGroupBy, aggregates, groupingCalls, aggAliases)

		// Rewrite projection expressions to replace nested aggregate calls with
		// BoundColumnRef nodes referencing the aggregate result columns.
		projectionExprs = liftNestedAggregates(s.Columns, len(regularGroupBy), aggregates, aggAliases)

		plan = &LogicalAggregate{
			Child:         plan,
			GroupBy:       groupBy,
			Aggregates:    aggregates,
			Aliases:       aliases,
			GroupingSets:  groupingSets,
			GroupingCalls: groupingCalls,
		}

		// HAVING
		if s.Having != nil {
			plan = &LogicalFilter{
				Child:     plan,
				Condition: s.Having,
			}
		}
	}

	// WINDOW - detect and add window expressions
	// Window goes AFTER aggregation/filter, BEFORE projection.
	// We must also include any window functions embedded directly in the QUALIFY clause
	// (e.g., "QUALIFY RANK() OVER (...) = 1") so that their results are computed and
	// available when the QUALIFY filter is applied.
	windowExprs := extractWindowExprs(s.Columns)
	if s.Qualify != nil {
		rewrite.WalkExpr(s.Qualify, func(expr binder.BoundExpr) {
			if we, ok := expr.(*binder.BoundWindowExpr); ok {
				windowExprs = append(windowExprs, we)
			}
		})
	}
	if len(windowExprs) > 0 {
		plan = &LogicalWindow{
			Child:       plan,
			WindowExprs: windowExprs,
		}
	}

	// QUALIFY - filter rows after window functions are evaluated
	// This is specific to DuckDB/Snowflake and allows filtering on window function results
	if s.Qualify != nil {
		plan = &LogicalFilter{
			Child:     plan,
			Condition: s.Qualify,
		}
	}

	// PROJECT
	projAliases := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		projAliases[i] = col.Alias
	}

	plan = &LogicalProject{
		Child:       plan,
		Expressions: projectionExprs,
		Aliases:     projAliases,
	}

	// DISTINCT ON - keep first row per group of DISTINCT ON columns
	// This must be applied after projection and before regular DISTINCT
	// DISTINCT ON requires sorting by the DISTINCT ON columns first
	if len(s.DistinctOn) > 0 {
		plan = &LogicalDistinctOn{
			Child:      plan,
			DistinctOn: s.DistinctOn,
			OrderBy:    s.OrderBy, // Use the ORDER BY to determine which row to keep
		}
	}

	// DISTINCT (regular DISTINCT, not DISTINCT ON)
	if s.Distinct && len(s.DistinctOn) == 0 {
		plan = &LogicalDistinct{Child: plan}
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		plan = &LogicalSort{
			Child:   plan,
			OrderBy: s.OrderBy,
		}
	}

	// LIMIT/OFFSET
	if s.Limit != nil || s.Offset != nil {
		limit := int64(-1)
		offset := int64(0)
		var limitExpr, offsetExpr binder.BoundExpr

		if s.Limit != nil {
			if lit, ok := s.Limit.(*binder.BoundLiteral); ok {
				limit = toInt64(lit.Value)
			} else {
				// Non-literal limit expression (e.g., correlated column reference)
				limitExpr = s.Limit
			}
		}
		if s.Offset != nil {
			if lit, ok := s.Offset.(*binder.BoundLiteral); ok {
				offset = toInt64(lit.Value)
			} else {
				// Non-literal offset expression (e.g., correlated column reference)
				offsetExpr = s.Offset
			}
		}

		plan = &LogicalLimit{
			Child:      plan,
			Limit:      limit,
			Offset:     offset,
			LimitExpr:  limitExpr,
			OffsetExpr: offsetExpr,
			WithTies:   s.WithTies,
			OrderBy:    s.OrderBy,
		}
	}

	// SAMPLE - apply sampling after everything else
	if s.Sample != nil {
		plan = &LogicalSample{
			Child:  plan,
			Sample: s.Sample,
		}
	}

	// Set operations (UNION, INTERSECT, EXCEPT)
	if s.SetOp != parser.SetOpNone && s.Right != nil {
		rightPlan, err := p.planSelect(s.Right)
		if err != nil {
			return nil, err
		}

		// Convert parser.SetOpType to planner.SetOpType
		var opType SetOpType
		switch s.SetOp {
		case parser.SetOpUnion:
			opType = SetOpUnion
		case parser.SetOpUnionAll:
			opType = SetOpUnionAll
		case parser.SetOpIntersect:
			opType = SetOpIntersect
		case parser.SetOpIntersectAll:
			opType = SetOpIntersectAll
		case parser.SetOpExcept:
			opType = SetOpExcept
		case parser.SetOpExceptAll:
			opType = SetOpExceptAll
		case parser.SetOpUnionByName:
			opType = SetOpUnionByName
		case parser.SetOpUnionAllByName:
			opType = SetOpUnionAllByName
		}

		plan = &LogicalSetOp{
			Left:   plan,
			Right:  rightPlan,
			OpType: opType,
		}
	}

	return plan, nil
}

func (p *Planner) planInsert(
	s *binder.BoundInsertStmt,
) (LogicalPlan, error) {
	var source LogicalPlan

	if s.Select != nil {
		var err error
		source, err = p.planSelect(s.Select)
		if err != nil {
			return nil, err
		}
	}

	return &LogicalInsert{
		Schema:     s.Schema,
		Table:      s.Table,
		TableDef:   s.TableDef,
		Columns:    s.Columns,
		Values:     s.Values,
		Source:     source,
		OnConflict: s.OnConflict,
		Returning:  s.Returning,
	}, nil
}

func (p *Planner) planUpdate(
	s *binder.BoundUpdateStmt,
) (LogicalPlan, error) {
	// Create scan
	scan := &LogicalScan{
		Schema:    s.Schema,
		TableName: s.Table,
		Alias:     s.Table,
		TableDef:  s.TableDef,
	}

	var source LogicalPlan = scan

	// Add filter if WHERE clause exists
	if s.Where != nil {
		source = &LogicalFilter{
			Child:     source,
			Condition: s.Where,
		}
	}

	return &LogicalUpdate{
		Schema:    s.Schema,
		Table:     s.Table,
		TableDef:  s.TableDef,
		Set:       s.Set,
		Source:    source,
		Returning: s.Returning,
	}, nil
}

func (p *Planner) planDelete(
	s *binder.BoundDeleteStmt,
) (LogicalPlan, error) {
	// Create scan of the target table
	var source LogicalPlan = &LogicalScan{
		Schema:    s.Schema,
		TableName: s.Table,
		Alias:     s.Table,
		TableDef:  s.TableDef,
	}

	// Add USING table scans as cross joins
	if len(s.Using) > 0 {
		for _, usingRef := range s.Using {
			usingScan := &LogicalScan{
				Schema:    usingRef.Schema,
				TableName: usingRef.TableName,
				Alias:     usingRef.Alias,
				TableDef:  usingRef.TableDef,
			}
			source = &LogicalJoin{
				Left:     source,
				Right:    usingScan,
				JoinType: JoinTypeCross,
			}
		}
	}

	// Add filter if WHERE clause exists
	if s.Where != nil {
		source = &LogicalFilter{
			Child:     source,
			Condition: s.Where,
		}
	}

	return &LogicalDelete{
		Schema:    s.Schema,
		Table:     s.Table,
		TableDef:  s.TableDef,
		Source:    source,
		Returning: s.Returning,
	}, nil
}

func (p *Planner) planCreateTable(
	s *binder.BoundCreateTableStmt,
) (LogicalPlan, error) {
	return &LogicalCreateTable{
		Schema:      s.Schema,
		Table:       s.Table,
		IfNotExists: s.IfNotExists,
		OrReplace:   s.OrReplace,
		Temporary:   s.Temporary,
		Columns:     s.Columns,
		PrimaryKey:  s.PrimaryKey,
		Constraints: s.Constraints,
	}, nil
}

func (p *Planner) planDropTable(
	s *binder.BoundDropTableStmt,
) (LogicalPlan, error) {
	return &LogicalDropTable{
		Schema:   s.Schema,
		Table:    s.Table,
		IfExists: s.IfExists,
	}, nil
}

func (p *Planner) planCopy(
	s *binder.BoundCopyStmt,
) (LogicalPlan, error) {
	if s.IsFrom {
		// COPY FROM - import data from file
		return &LogicalCopyFrom{
			Schema:   s.Schema,
			Table:    s.Table,
			TableDef: s.TableDef,
			Columns:  s.Columns,
			FilePath: s.FilePath,
			Options:  s.Options,
		}, nil
	}

	// COPY TO - export data to file
	plan := &LogicalCopyTo{
		Schema:   s.Schema,
		Table:    s.Table,
		TableDef: s.TableDef,
		Columns:  s.Columns,
		FilePath: s.FilePath,
		Options:  s.Options,
	}

	// If there's a query, plan it as the source
	if s.Query != nil {
		source, err := p.planSelect(s.Query)
		if err != nil {
			return nil, err
		}
		plan.Source = source
	}

	return plan, nil
}

// DDL planning functions

func (p *Planner) planCreateView(
	s *binder.BoundCreateViewStmt,
) (LogicalPlan, error) {
	return &LogicalCreateView{
		Schema:      s.Schema,
		View:        s.View,
		IfNotExists: s.IfNotExists,
		OrReplace:   s.OrReplace,
		Query:       s.Query,
		QueryText:   s.QueryText,
	}, nil
}

func (p *Planner) planDropView(
	s *binder.BoundDropViewStmt,
) (LogicalPlan, error) {
	return &LogicalDropView{
		Schema:   s.Schema,
		View:     s.View,
		IfExists: s.IfExists,
	}, nil
}

func (p *Planner) planCreateIndex(
	s *binder.BoundCreateIndexStmt,
) (LogicalPlan, error) {
	return &LogicalCreateIndex{
		Schema:      s.Schema,
		Table:       s.Table,
		Index:       s.Index,
		IfNotExists: s.IfNotExists,
		Columns:     s.Columns,
		IsUnique:    s.IsUnique,
		TableDef:    s.TableDef,
	}, nil
}

func (p *Planner) planDropIndex(
	s *binder.BoundDropIndexStmt,
) (LogicalPlan, error) {
	return &LogicalDropIndex{
		Schema:   s.Schema,
		Index:    s.Index,
		IfExists: s.IfExists,
	}, nil
}

func (p *Planner) planCreateSequence(
	s *binder.BoundCreateSequenceStmt,
) (LogicalPlan, error) {
	return &LogicalCreateSequence{
		Schema:      s.Schema,
		Sequence:    s.Sequence,
		IfNotExists: s.IfNotExists,
		StartWith:   s.StartWith,
		IncrementBy: s.IncrementBy,
		MinValue:    s.MinValue,
		MaxValue:    s.MaxValue,
		IsCycle:     s.IsCycle,
	}, nil
}

func (p *Planner) planDropSequence(
	s *binder.BoundDropSequenceStmt,
) (LogicalPlan, error) {
	return &LogicalDropSequence{
		Schema:   s.Schema,
		Sequence: s.Sequence,
		IfExists: s.IfExists,
	}, nil
}

func (p *Planner) planCreateSchema(
	s *binder.BoundCreateSchemaStmt,
) (LogicalPlan, error) {
	return &LogicalCreateSchema{
		Schema:      s.Schema,
		IfNotExists: s.IfNotExists,
	}, nil
}

func (p *Planner) planDropSchema(
	s *binder.BoundDropSchemaStmt,
) (LogicalPlan, error) {
	return &LogicalDropSchema{
		Schema:   s.Schema,
		IfExists: s.IfExists,
		Cascade:  s.Cascade,
	}, nil
}

func (p *Planner) planAlterTable(
	s *binder.BoundAlterTableStmt,
) (LogicalPlan, error) {
	return &LogicalAlterTable{
		Schema:         s.Schema,
		Table:          s.Table,
		TableDef:       s.TableDef,
		Operation:      int(s.Operation),
		IfExists:       s.IfExists,
		NewTableName:   s.NewTableName,
		OldColumn:      s.OldColumn,
		NewColumn:      s.NewColumn,
		DropColumn:     s.DropColumn,
		AddColumn:      s.AddColumn,
		AlterColumn:    s.AlterColumn,
		NewColumnType:  s.NewColumnType,
		ConstraintName: s.ConstraintName,
		Constraint:     s.Constraint,
		DefaultExpr:    s.DefaultExpr,
	}, nil
}

func (p *Planner) planComment(
	s *binder.BoundCommentStmt,
) (LogicalPlan, error) {
	return &LogicalComment{
		ObjectType: s.ObjectType,
		Schema:     s.Schema,
		ObjectName: s.ObjectName,
		ColumnName: s.ColumnName,
		Comment:    s.Comment,
	}, nil
}

// ---------- Secret DDL Planning Functions ----------

func (p *Planner) planCreateSecret(
	s *binder.BoundCreateSecretStmt,
) (LogicalPlan, error) {
	return &LogicalCreateSecret{
		Name:        s.Name,
		IfNotExists: s.IfNotExists,
		OrReplace:   s.OrReplace,
		Persistent:  s.Persistent,
		SecretType:  s.SecretType,
		Provider:    s.Provider,
		Scope:       s.Scope,
		Options:     s.Options,
	}, nil
}

func (p *Planner) planDropSecret(
	s *binder.BoundDropSecretStmt,
) (LogicalPlan, error) {
	return &LogicalDropSecret{
		Name:     s.Name,
		IfExists: s.IfExists,
	}, nil
}

func (p *Planner) planAlterSecret(
	s *binder.BoundAlterSecretStmt,
) (LogicalPlan, error) {
	return &LogicalAlterSecret{
		Name:    s.Name,
		Options: s.Options,
	}, nil
}

// planSummarize creates a logical plan for a SUMMARIZE statement.
func (p *Planner) planSummarize(s *binder.BoundSummarizeStmt) (LogicalPlan, error) {
	if s.Query != nil {
		queryPlan, err := p.planSelect(s.Query)
		if err != nil {
			return nil, err
		}
		return &LogicalSummarize{Query: queryPlan}, nil
	}
	return &LogicalSummarize{
		Schema:    s.Schema,
		TableName: s.TableName,
		TableDef:  s.TableDef,
	}, nil
}

// planPragma creates a logical plan for a PRAGMA statement.
func (p *Planner) planPragma(s *binder.BoundPragmaStmt) (LogicalPlan, error) {
	return &LogicalPragma{
		Name:       s.Name,
		PragmaType: s.PragmaType,
		Args:       s.Args,
		Value:      s.Value,
	}, nil
}

// planExplain creates a logical plan for an EXPLAIN statement.
func (p *Planner) planExplain(s *binder.BoundExplainStmt) (LogicalPlan, error) {
	// Create a plan for the underlying query
	childPlan, err := p.createLogicalPlan(s.Query)
	if err != nil {
		return nil, err
	}

	return &LogicalExplain{
		Child:   childPlan,
		Analyze: s.Analyze,
	}, nil
}

// planVacuum creates a logical plan for a VACUUM statement.
func (p *Planner) planVacuum(s *binder.BoundVacuumStmt) (LogicalPlan, error) {
	return &LogicalVacuum{
		Schema:    s.Schema,
		TableName: s.TableName,
		TableDef:  s.TableDef,
	}, nil
}

// planAnalyze creates a logical plan for an ANALYZE statement.
func (p *Planner) planAnalyze(s *binder.BoundAnalyzeStmt) (LogicalPlan, error) {
	return &LogicalAnalyze{
		Schema:    s.Schema,
		TableName: s.TableName,
		TableDef:  s.TableDef,
	}, nil
}

// planCheckpoint creates a logical plan for a CHECKPOINT statement.
func (p *Planner) planCheckpoint(s *binder.BoundCheckpointStmt) (LogicalPlan, error) {
	return &LogicalCheckpoint{
		Database: s.Database,
		Force:    s.Force,
	}, nil
}

func (p *Planner) planMerge(
	s *binder.BoundMergeStmt,
) (LogicalPlan, error) {
	// Create a plan for the source table/subquery
	var sourcePlan LogicalPlan
	if s.SourceRef != nil {
		sourcePlan = p.createScanForBoundTableRef(s.SourceRef)
	}

	return &LogicalMerge{
		Schema:                 s.Schema,
		TargetTable:            s.TargetTable,
		TargetTableDef:         s.TargetTableDef,
		TargetAlias:            s.TargetAlias,
		SourcePlan:             sourcePlan,
		OnCondition:            s.OnCondition,
		WhenMatched:            s.WhenMatched,
		WhenNotMatched:         s.WhenNotMatched,
		WhenNotMatchedBySource: s.WhenNotMatchedBySource,
		Returning:              s.Returning,
	}, nil
}

// planPivot creates a logical plan for a PIVOT statement.
func (p *Planner) planPivot(s *binder.BoundPivotStmt) (LogicalPlan, error) {
	// Create a plan for the source table
	var sourcePlan LogicalPlan
	if s.Source != nil {
		sourcePlan = p.createScanForBoundTableRef(s.Source)
	}

	return &LogicalPivot{
		Source:     sourcePlan,
		ForColumn:  s.ForColumn,
		InValues:   s.InValues,
		Aggregates: s.Aggregates,
		GroupBy:    s.GroupBy,
	}, nil
}

// planUnpivot creates a logical plan for an UNPIVOT statement.
func (p *Planner) planUnpivot(s *binder.BoundUnpivotStmt) (LogicalPlan, error) {
	// Create a plan for the source table
	var sourcePlan LogicalPlan
	if s.Source != nil {
		sourcePlan = p.createScanForBoundTableRef(s.Source)
	}

	return &LogicalUnpivot{
		Source:         sourcePlan,
		ValueColumn:    s.ValueColumn,
		NameColumn:     s.NameColumn,
		UnpivotColumns: s.UnpivotColumns,
	}, nil
}

// createScanForBoundTableRef creates a LogicalScan for a bound table reference.
// This handles regular tables, virtual tables, table functions, views, and CTEs.
func (p *Planner) createScanForBoundTableRef(ref *binder.BoundTableRef) LogicalPlan {
	// Handle CTE references
	if ref.CTERef != nil {
		// Build column bindings for the CTE scan
		columns := make([]ColumnBinding, len(ref.Columns))
		for i, col := range ref.Columns {
			columns[i] = ColumnBinding{
				Table:     ref.Alias,
				Column:    col.Column,
				Type:      col.Type,
				ColumnIdx: i,
			}
		}

		// For self-references within recursive CTEs (the reference inside the recursive part),
		// return a CTE scan that will be replaced with work table data at execution time.
		// We check IsCTESelfRef which was captured at bind time.
		if ref.IsCTESelfRef {
			return &LogicalCTEScan{
				CTEName:     ref.CTERef.Name,
				Alias:       ref.Alias,
				Columns:     columns,
				CTEPlan:     nil,
				IsRecursive: true,
			}
		}

		// For recursive CTEs (main query reference, not self-reference), create a LogicalRecursiveCTE node
		if ref.CTERef.Recursive && ref.CTERef.Query != nil && ref.CTERef.RecursiveQuery != nil {
			// Plan the base case
			basePlan, err := p.planSelect(ref.CTERef.Query)
			if err != nil {
				// Fall back to CTE scan if planning fails
				return &LogicalCTEScan{
					CTEName:     ref.CTERef.Name,
					Alias:       ref.Alias,
					Columns:     columns,
					CTEPlan:     nil,
					IsRecursive: true,
				}
			}

			// Plan the recursive case
			recursivePlan, err := p.planSelect(ref.CTERef.RecursiveQuery)
			if err != nil {
				// Fall back to CTE scan if planning fails
				return &LogicalCTEScan{
					CTEName:     ref.CTERef.Name,
					Alias:       ref.Alias,
					Columns:     columns,
					CTEPlan:     basePlan,
					IsRecursive: true,
				}
			}

			return &LogicalRecursiveCTE{
				CTEName:       ref.CTERef.Name,
				BasePlan:      basePlan,
				RecursivePlan: recursivePlan,
				Columns:       columns,
				UsingKey:      ref.CTERef.UsingKey,
				SetOp:         ref.CTERef.SetOp,
				MaxRecursion:  ref.CTERef.MaxRecursion,
			}
		}

		// For non-recursive CTEs, plan the CTE query
		var ctePlan LogicalPlan
		if ref.CTERef.Query != nil {
			var err error
			ctePlan, err = p.planSelect(ref.CTERef.Query)
			if err != nil {
				// If planning fails, return a CTE scan without the plan
				// This will be handled at execution time
				ctePlan = nil
			}
		}

		return &LogicalCTEScan{
			CTEName:     ref.CTERef.Name,
			Alias:       ref.Alias,
			Columns:     columns,
			CTEPlan:     ctePlan,
			IsRecursive: false,
		}
	}

	// Handle subqueries (including LATERAL subqueries)
	if ref.Subquery != nil {
		// Plan the subquery
		plan, err := p.planSelect(ref.Subquery)
		if err != nil {
			// If planning fails, return an empty scan (will fail later with better error)
			return &LogicalScan{
				Schema:    ref.Schema,
				TableName: ref.Alias,
				Alias:     ref.Alias,
			}
		}
		return plan
	}

	// Handle VALUES clause
	if ref.ValuesRows != nil {
		var columns []ColumnBinding
		for _, col := range ref.Columns {
			columns = append(columns, ColumnBinding{
				Table:  ref.Alias,
				Column: col.Column,
				Type:   col.Type,
			})
		}
		return &LogicalValues{
			Rows:    ref.ValuesRows,
			Columns: columns,
			Alias:   ref.Alias,
		}
	}

	// Handle views by expanding the view query
	if ref.ViewDef != nil && ref.ViewQuery != nil {
		// Plan the view's bound query directly
		// This effectively inlines the view's query into the plan
		plan, err := p.planSelect(ref.ViewQuery)
		if err != nil {
			// If planning fails, fall back to regular scan (will fail later with better error)
			return &LogicalScan{
				Schema:    ref.Schema,
				TableName: ref.TableName,
				Alias:     ref.Alias,
				TableDef:  ref.TableDef,
			}
		}
		return plan
	}

	// Handle PIVOT table references
	if ref.PivotStmt != nil {
		pivotStmt := ref.PivotStmt
		// Create a plan for the source table
		var sourcePlan LogicalPlan
		if pivotStmt.Source != nil {
			sourcePlan = p.createScanForBoundTableRef(pivotStmt.Source)
		}
		return &LogicalPivot{
			Source:     sourcePlan,
			ForColumn:  pivotStmt.ForColumn,
			InValues:   pivotStmt.InValues,
			Aggregates: pivotStmt.Aggregates,
			GroupBy:    pivotStmt.GroupBy,
		}
	}

	// Handle UNPIVOT table references
	if ref.UnpivotStmt != nil {
		unpivotStmt := ref.UnpivotStmt
		// Create a plan for the source table
		var sourcePlan LogicalPlan
		if unpivotStmt.Source != nil {
			sourcePlan = p.createScanForBoundTableRef(unpivotStmt.Source)
		}
		return &LogicalUnpivot{
			Source:         sourcePlan,
			ValueColumn:    unpivotStmt.ValueColumn,
			NameColumn:     unpivotStmt.NameColumn,
			UnpivotColumns: unpivotStmt.UnpivotColumns,
		}
	}

	return &LogicalScan{
		Schema:        ref.Schema,
		TableName:     ref.TableName,
		Alias:         ref.Alias,
		TableDef:      ref.TableDef,
		VirtualTable:  ref.VirtualTable,
		TableFunction: ref.TableFunction,
	}
}

func (p *Planner) createPhysicalPlan(
	logical LogicalPlan,
) (PhysicalPlan, error) {
	switch l := logical.(type) {
	case *LogicalScan:
		// Check if this is a table function scan
		if l.TableFunction != nil {
			return &PhysicalTableFunctionScan{
				FunctionName:  l.TableFunction.Name,
				Alias:         l.Alias,
				Path:          l.TableFunction.Path,
				Options:       l.TableFunction.Options,
				Projections:   l.Projections,
				TableFunction: l.TableFunction,
			}, nil
		}

		// Check if this is a virtual table scan
		if l.VirtualTable != nil {
			return &PhysicalVirtualTableScan{
				Schema:       l.Schema,
				TableName:    l.TableName,
				Alias:        l.Alias,
				VirtualTable: l.VirtualTable,
				Projections:  l.Projections,
			}, nil
		}

		// Check for index scan hint from optimizer
		// Try alias first (for queries with table aliases), then table name
		if p.hints != nil {
			var hint AccessHint
			var hasHint bool

			if l.Alias != "" {
				hint, hasHint = p.hints.GetAccessHint(l.Alias)
			}
			if !hasHint {
				hint, hasHint = p.hints.GetAccessHint(l.TableName)
			}

			if hasHint && (hint.Method == "IndexScan" || hint.Method == "IndexRangeScan") {
				return p.createPhysicalIndexScan(l, &hint)
			}
		}

		// Default: Sequential scan
		return &PhysicalScan{
			Schema:      l.Schema,
			TableName:   l.TableName,
			Alias:       l.Alias,
			TableDef:    l.TableDef,
			Projections: l.Projections,
		}, nil

	case *LogicalValues:
		return &PhysicalValues{
			Rows:    l.Rows,
			Columns: l.Columns,
		}, nil

	case *LogicalFilter:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalFilter{
			Child:     child,
			Condition: l.Condition,
		}, nil

	case *LogicalProject:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalProject{
			Child:       child,
			Expressions: l.Expressions,
			Aliases:     l.Aliases,
		}, nil

	case *LogicalJoin:
		left, err := p.createPhysicalPlan(l.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.createPhysicalPlan(l.Right)
		if err != nil {
			return nil, err
		}

		// Handle special join types
		if l.JoinType == JoinTypePositional {
			return &PhysicalPositionalJoin{
				Left:  left,
				Right: right,
			}, nil
		}
		if l.JoinType == JoinTypeAsOf || l.JoinType == JoinTypeAsOfLeft {
			return &PhysicalAsOfJoin{
				Left:      left,
				Right:     right,
				JoinType:  l.JoinType,
				Condition: l.Condition,
			}, nil
		}

		// Check for optimization hints for this join
		joinKey := fmt.Sprintf("join_%d", p.joinIndex)
		p.joinIndex++

		// If we have a hint for this join, use it
		if hint, ok := p.hints.GetJoinHint(joinKey); ok {
			return p.createPhysicalJoinFromHint(left, right, l.JoinType, l.Condition, hint)
		}

		// Default: Use hash join for equi-joins, nested loop for others
		if isEquiJoin(l.Condition) {
			return &PhysicalHashJoin{
				Left:      left,
				Right:     right,
				JoinType:  l.JoinType,
				Condition: l.Condition,
			}, nil
		}

		return &PhysicalNestedLoopJoin{
			Left:      left,
			Right:     right,
			JoinType:  l.JoinType,
			Condition: l.Condition,
		}, nil

	case *LogicalLateralJoin:
		left, err := p.createPhysicalPlan(l.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.createPhysicalPlan(l.Right)
		if err != nil {
			return nil, err
		}

		// LATERAL joins always use PhysicalLateralJoin since the right side
		// needs to be re-evaluated for each row of the left side
		return &PhysicalLateralJoin{
			Left:      left,
			Right:     right,
			JoinType:  l.JoinType,
			Condition: l.Condition,
		}, nil

	case *LogicalSample:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalSample{
			Child:  child,
			Sample: l.Sample,
		}, nil

	case *LogicalAggregate:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalHashAggregate{
			Child:         child,
			GroupBy:       l.GroupBy,
			Aggregates:    l.Aggregates,
			Aliases:       l.Aliases,
			GroupingSets:  l.GroupingSets,
			GroupingCalls: l.GroupingCalls,
		}, nil

	case *LogicalWindow:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalWindow{
			Child:       child,
			WindowExprs: l.WindowExprs,
		}, nil

	case *LogicalSort:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalSort{
			Child:   child,
			OrderBy: l.OrderBy,
		}, nil

	case *LogicalLimit:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalLimit{
			Child:      child,
			Limit:      l.Limit,
			Offset:     l.Offset,
			LimitExpr:  l.LimitExpr,
			OffsetExpr: l.OffsetExpr,
			WithTies:   l.WithTies,
			OrderBy:    l.OrderBy,
		}, nil

	case *LogicalDistinct:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalDistinct{Child: child}, nil

	case *LogicalDistinctOn:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalDistinctOn{
			Child:      child,
			DistinctOn: l.DistinctOn,
			OrderBy:    l.OrderBy,
		}, nil

	case *LogicalInsert:
		var source PhysicalPlan
		if l.Source != nil {
			var err error
			source, err = p.createPhysicalPlan(l.Source)
			if err != nil {
				return nil, err
			}
		}

		return &PhysicalInsert{
			Schema:     l.Schema,
			Table:      l.Table,
			TableDef:   l.TableDef,
			Columns:    l.Columns,
			Values:     l.Values,
			Source:     source,
			OnConflict: l.OnConflict,
			Returning:  l.Returning,
		}, nil

	case *LogicalUpdate:
		var source PhysicalPlan
		if l.Source != nil {
			var err error
			source, err = p.createPhysicalPlan(l.Source)
			if err != nil {
				return nil, err
			}
		}

		return &PhysicalUpdate{
			Schema:    l.Schema,
			Table:     l.Table,
			TableDef:  l.TableDef,
			Set:       l.Set,
			Source:    source,
			Returning: l.Returning,
		}, nil

	case *LogicalDelete:
		var source PhysicalPlan
		if l.Source != nil {
			var err error
			source, err = p.createPhysicalPlan(l.Source)
			if err != nil {
				return nil, err
			}
		}

		return &PhysicalDelete{
			Schema:    l.Schema,
			Table:     l.Table,
			TableDef:  l.TableDef,
			Source:    source,
			Returning: l.Returning,
		}, nil

	case *LogicalCreateTable:
		return &PhysicalCreateTable{
			Schema:      l.Schema,
			Table:       l.Table,
			IfNotExists: l.IfNotExists,
			OrReplace:   l.OrReplace,
			Temporary:   l.Temporary,
			Columns:     l.Columns,
			PrimaryKey:  l.PrimaryKey,
			Constraints: l.Constraints,
		}, nil

	case *LogicalDropTable:
		return &PhysicalDropTable{
			Schema:   l.Schema,
			Table:    l.Table,
			IfExists: l.IfExists,
		}, nil

	case *LogicalTruncate:
		return &PhysicalTruncate{
			Schema: l.Schema,
			Table:  l.Table,
		}, nil

	case *LogicalDummyScan:
		return &PhysicalDummyScan{}, nil

	case *LogicalBegin:
		return &PhysicalBegin{}, nil

	case *LogicalCommit:
		return &PhysicalCommit{}, nil

	case *LogicalRollback:
		return &PhysicalRollback{}, nil

	case *LogicalCopyFrom:
		return &PhysicalCopyFrom{
			Schema:   l.Schema,
			Table:    l.Table,
			TableDef: l.TableDef,
			Columns:  l.Columns,
			FilePath: l.FilePath,
			Options:  l.Options,
		}, nil

	case *LogicalCopyTo:
		var source PhysicalPlan
		if l.Source != nil {
			var err error
			source, err = p.createPhysicalPlan(l.Source)
			if err != nil {
				return nil, err
			}
		}
		return &PhysicalCopyTo{
			Schema:   l.Schema,
			Table:    l.Table,
			TableDef: l.TableDef,
			Columns:  l.Columns,
			FilePath: l.FilePath,
			Options:  l.Options,
			Source:   source,
		}, nil

	// DDL logical to physical mappings
	case *LogicalCreateView:
		return &PhysicalCreateView{
			Schema:      l.Schema,
			View:        l.View,
			IfNotExists: l.IfNotExists,
			OrReplace:   l.OrReplace,
			Query:       l.Query,
			QueryText:   l.QueryText,
		}, nil

	case *LogicalDropView:
		return &PhysicalDropView{
			Schema:   l.Schema,
			View:     l.View,
			IfExists: l.IfExists,
		}, nil

	case *LogicalCreateIndex:
		return &PhysicalCreateIndex{
			Schema:      l.Schema,
			Table:       l.Table,
			Index:       l.Index,
			IfNotExists: l.IfNotExists,
			Columns:     l.Columns,
			IsUnique:    l.IsUnique,
			TableDef:    l.TableDef,
		}, nil

	case *LogicalDropIndex:
		return &PhysicalDropIndex{
			Schema:   l.Schema,
			Index:    l.Index,
			IfExists: l.IfExists,
		}, nil

	case *LogicalCreateSequence:
		return &PhysicalCreateSequence{
			Schema:      l.Schema,
			Sequence:    l.Sequence,
			IfNotExists: l.IfNotExists,
			StartWith:   l.StartWith,
			IncrementBy: l.IncrementBy,
			MinValue:    l.MinValue,
			MaxValue:    l.MaxValue,
			IsCycle:     l.IsCycle,
		}, nil

	case *LogicalDropSequence:
		return &PhysicalDropSequence{
			Schema:   l.Schema,
			Sequence: l.Sequence,
			IfExists: l.IfExists,
		}, nil

	case *LogicalCreateSchema:
		return &PhysicalCreateSchema{
			Schema:      l.Schema,
			IfNotExists: l.IfNotExists,
		}, nil

	case *LogicalDropSchema:
		return &PhysicalDropSchema{
			Schema:   l.Schema,
			IfExists: l.IfExists,
			Cascade:  l.Cascade,
		}, nil

	case *LogicalAlterTable:
		return &PhysicalAlterTable{
			Schema:         l.Schema,
			Table:          l.Table,
			TableDef:       l.TableDef,
			Operation:      l.Operation,
			IfExists:       l.IfExists,
			NewTableName:   l.NewTableName,
			OldColumn:      l.OldColumn,
			NewColumn:      l.NewColumn,
			DropColumn:     l.DropColumn,
			AddColumn:      l.AddColumn,
			AlterColumn:    l.AlterColumn,
			NewColumnType:  l.NewColumnType,
			ConstraintName: l.ConstraintName,
			Constraint:     l.Constraint,
			DefaultExpr:    l.DefaultExpr,
		}, nil

	case *LogicalComment:
		return &PhysicalComment{
			ObjectType: l.ObjectType,
			Schema:     l.Schema,
			ObjectName: l.ObjectName,
			ColumnName: l.ColumnName,
			Comment:    l.Comment,
		}, nil

	// Type DDL logical to physical mappings
	case *LogicalCreateType:
		return &PhysicalCreateType{
			Name:        l.Name,
			Schema:      l.Schema,
			TypeKind:    l.TypeKind,
			EnumValues:  l.EnumValues,
			IfNotExists: l.IfNotExists,
		}, nil

	case *LogicalDropType:
		return &PhysicalDropType{
			Name:     l.Name,
			Schema:   l.Schema,
			IfExists: l.IfExists,
		}, nil

	// Macro DDL logical to physical mappings
	case *LogicalCreateMacro:
		return &PhysicalCreateMacro{
			Schema:       l.Schema,
			Name:         l.Name,
			Params:       l.Params,
			IsTableMacro: l.IsTableMacro,
			OrReplace:    l.OrReplace,
			BodySQL:      l.BodySQL,
			QuerySQL:     l.QuerySQL,
		}, nil

	case *LogicalDropMacro:
		return &PhysicalDropMacro{
			Schema:       l.Schema,
			Name:         l.Name,
			IfExists:     l.IfExists,
			IsTableMacro: l.IsTableMacro,
		}, nil

	// Secret DDL logical to physical mappings
	case *LogicalCreateSecret:
		return &PhysicalCreateSecret{
			Name:        l.Name,
			IfNotExists: l.IfNotExists,
			OrReplace:   l.OrReplace,
			Persistent:  l.Persistent,
			SecretType:  l.SecretType,
			Provider:    l.Provider,
			Scope:       l.Scope,
			Options:     l.Options,
		}, nil

	case *LogicalDropSecret:
		return &PhysicalDropSecret{
			Name:     l.Name,
			IfExists: l.IfExists,
		}, nil

	case *LogicalAlterSecret:
		return &PhysicalAlterSecret{
			Name:    l.Name,
			Options: l.Options,
		}, nil

	case *LogicalMerge:
		var sourcePlan PhysicalPlan
		if l.SourcePlan != nil {
			var err error
			sourcePlan, err = p.createPhysicalPlan(l.SourcePlan)
			if err != nil {
				return nil, err
			}
		}
		return &PhysicalMerge{
			Schema:                 l.Schema,
			TargetTable:            l.TargetTable,
			TargetTableDef:         l.TargetTableDef,
			TargetAlias:            l.TargetAlias,
			SourcePlan:             sourcePlan,
			OnCondition:            l.OnCondition,
			WhenMatched:            l.WhenMatched,
			WhenNotMatched:         l.WhenNotMatched,
			WhenNotMatchedBySource: l.WhenNotMatchedBySource,
			Returning:              l.Returning,
		}, nil

	// CTE logical to physical mappings
	case *LogicalRecursiveCTE:
		basePlan, err := p.createPhysicalPlan(l.BasePlan)
		if err != nil {
			return nil, err
		}
		recursivePlan, err := p.createPhysicalPlan(l.RecursivePlan)
		if err != nil {
			return nil, err
		}
		return &PhysicalRecursiveCTE{
			CTEName:       l.CTEName,
			BasePlan:      basePlan,
			RecursivePlan: recursivePlan,
			Columns:       l.Columns,
			UsingKey:      l.UsingKey,
			SetOp:         l.SetOp,
			MaxRecursion:  l.MaxRecursion,
		}, nil

	case *LogicalCTEScan:
		var ctePlan PhysicalPlan
		if l.CTEPlan != nil {
			var err error
			ctePlan, err = p.createPhysicalPlan(l.CTEPlan)
			if err != nil {
				return nil, err
			}
		}
		return &PhysicalCTEScan{
			CTEName:     l.CTEName,
			Alias:       l.Alias,
			Columns:     l.Columns,
			CTEPlan:     ctePlan,
			IsRecursive: l.IsRecursive,
		}, nil

	case *LogicalPivot:
		source, err := p.createPhysicalPlan(l.Source)
		if err != nil {
			return nil, err
		}
		return &PhysicalPivot{
			Source:     source,
			ForColumn:  l.ForColumn,
			InValues:   l.InValues,
			Aggregates: l.Aggregates,
			GroupBy:    l.GroupBy,
		}, nil

	case *LogicalUnpivot:
		source, err := p.createPhysicalPlan(l.Source)
		if err != nil {
			return nil, err
		}
		return &PhysicalUnpivot{
			Source:         source,
			ValueColumn:    l.ValueColumn,
			NameColumn:     l.NameColumn,
			UnpivotColumns: l.UnpivotColumns,
		}, nil

	// Database maintenance logical nodes
	case *LogicalPragma:
		return &PhysicalPragma{
			Name:       l.Name,
			PragmaType: l.PragmaType,
			Args:       l.Args,
			Value:      l.Value,
		}, nil

	case *LogicalExplain:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}
		return &PhysicalExplain{
			Child:        child,
			Analyze:      l.Analyze,
			RewriteStats: l.RewriteStats,
		}, nil

	case *LogicalVacuum:
		return &PhysicalVacuum{
			Schema:    l.Schema,
			TableName: l.TableName,
			TableDef:  l.TableDef,
		}, nil

	case *LogicalAnalyze:
		return &PhysicalAnalyze{
			Schema:    l.Schema,
			TableName: l.TableName,
			TableDef:  l.TableDef,
		}, nil

	case *LogicalCheckpoint:
		return &PhysicalCheckpoint{
			Database: l.Database,
			Force:    l.Force,
		}, nil

	// Database management logical to physical mappings
	case *LogicalAttach:
		return &PhysicalAttach{
			Path:     l.Path,
			Alias:    l.Alias,
			ReadOnly: l.ReadOnly,
			Options:  l.Options,
		}, nil

	case *LogicalDetach:
		return &PhysicalDetach{
			Name:     l.Name,
			IfExists: l.IfExists,
		}, nil

	case *LogicalUse:
		return &PhysicalUse{
			Database: l.Database,
			Schema:   l.Schema,
		}, nil

	case *LogicalCreateDatabase:
		return &PhysicalCreateDatabase{
			Name:        l.Name,
			IfNotExists: l.IfNotExists,
		}, nil

	case *LogicalDropDatabase:
		return &PhysicalDropDatabase{
			Name:     l.Name,
			IfExists: l.IfExists,
		}, nil

	case *LogicalIcebergScan:
		return p.createPhysicalIcebergScan(l)

	case *LogicalExportDatabase:
		return &PhysicalExportDatabase{Path: l.Path, Options: l.Options}, nil

	case *LogicalImportDatabase:
		return &PhysicalImportDatabase{Path: l.Path}, nil

	case *LogicalSummarize:
		var queryPlan PhysicalPlan
		if l.Query != nil {
			var err error
			queryPlan, err = p.createPhysicalPlan(l.Query)
			if err != nil {
				return nil, err
			}
		}
		return &PhysicalSummarize{
			Schema:    l.Schema,
			TableName: l.TableName,
			TableDef:  l.TableDef,
			Query:     queryPlan,
		}, nil

	case *LogicalSetOp:
		left, err := p.createPhysicalPlan(l.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.createPhysicalPlan(l.Right)
		if err != nil {
			return nil, err
		}
		return &PhysicalSetOp{
			Left:   left,
			Right:  right,
			OpType: l.OpType,
		}, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg:  "unsupported logical plan node",
		}
	}
}

// createPhysicalIcebergScan converts a LogicalIcebergScan to a PhysicalIcebergScan.
// It extracts partition filters and prepares column projection for the Iceberg reader.
func (p *Planner) createPhysicalIcebergScan(l *LogicalIcebergScan) (PhysicalPlan, error) {
	// Create the Iceberg planner for filter extraction
	icebergPlanner := NewIcebergPlanner()

	// Extract partition filters from the filter predicate
	var partitionFilters []PartitionFilter
	var residualFilter binder.BoundExpr

	if l.Filter != nil {
		partitionFilters, residualFilter = icebergPlanner.ExtractPartitionFilters(l.Filter)
	}

	return &PhysicalIcebergScan{
		TablePath:        l.TablePath,
		Alias:            l.Alias,
		Columns:          l.Columns,
		Filter:           l.Filter,
		TimeTravel:       l.TimeTravel,
		Options:          l.Options,
		PartitionFilters: partitionFilters,
		ResidualFilter:   residualFilter,
		ColumnTypes:      l.ColumnTypes,
	}, nil
}

// createPhysicalJoinFromHint creates a physical join node based on optimization hints.
// It uses the hint to determine the join method and potentially swap sides for hash join build.
func (p *Planner) createPhysicalJoinFromHint(
	left, right PhysicalPlan,
	joinType JoinType,
	condition binder.BoundExpr,
	hint JoinHint,
) (PhysicalPlan, error) {
	switch hint.Method {
	case "HashJoin":
		// For hash joins, the hint may specify which side should be the build side.
		// By default, the right side is the build side.
		// If hint.BuildSide is "left", we swap the sides.
		if hint.BuildSide == "left" {
			// Swap left and right so left becomes build side
			return &PhysicalHashJoin{
				Left:      right, // Right becomes probe side
				Right:     left,  // Left becomes build side
				JoinType:  joinType,
				Condition: condition,
			}, nil
		}
		return &PhysicalHashJoin{
			Left:      left,
			Right:     right,
			JoinType:  joinType,
			Condition: condition,
		}, nil

	case "NestedLoopJoin":
		return &PhysicalNestedLoopJoin{
			Left:      left,
			Right:     right,
			JoinType:  joinType,
			Condition: condition,
		}, nil

	case "SortMergeJoin":
		// TODO: Implement PhysicalSortMergeJoin when it exists in the planner
		// For now, fall back to hash join if it's an equi-join, otherwise nested loop
		if isEquiJoin(condition) {
			return &PhysicalHashJoin{
				Left:      left,
				Right:     right,
				JoinType:  joinType,
				Condition: condition,
			}, nil
		}
		return &PhysicalNestedLoopJoin{
			Left:      left,
			Right:     right,
			JoinType:  joinType,
			Condition: condition,
		}, nil

	default:
		// Unknown hint method, use default behavior
		if isEquiJoin(condition) {
			return &PhysicalHashJoin{
				Left:      left,
				Right:     right,
				JoinType:  joinType,
				Condition: condition,
			}, nil
		}
		return &PhysicalNestedLoopJoin{
			Left:      left,
			Right:     right,
			JoinType:  joinType,
			Condition: condition,
		}, nil
	}
}

// createPhysicalIndexScan creates a PhysicalIndexScan node from a LogicalScan and access hint.
// This is called when the optimizer has determined that an index scan is more efficient
// than a sequential scan for the given query predicates.
//
// The method validates the index exists and matches the table being scanned,
// then creates a PhysicalIndexScan node ready for the executor.
func (p *Planner) createPhysicalIndexScan(l *LogicalScan, hint *AccessHint) (PhysicalPlan, error) {
	// Validate hint has an index name
	if hint.IndexName == "" {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg: fmt.Sprintf(
				"index scan hint for table %q in schema %q has empty index name; "+
					"this indicates an internal optimizer error",
				l.TableName, l.Schema,
			),
		}
	}

	// Lookup index in catalog
	indexDef, ok := p.catalog.GetIndexInSchema(l.Schema, hint.IndexName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg: fmt.Sprintf(
				"index %q not found in schema %q for table %q (referenced in optimizer hint); "+
					"verify the index exists with: CREATE INDEX %s ON %s (...)",
				hint.IndexName, l.Schema, l.TableName, hint.IndexName, l.TableName,
			),
		}
	}

	// Validate index is for the correct table (case-insensitive comparison)
	if !strings.EqualFold(indexDef.Table, l.TableName) {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg: fmt.Sprintf(
				"index %q is defined on table %q but query scans table %q in schema %q; "+
					"ensure the optimizer hint references an index for the correct table",
				hint.IndexName, indexDef.Table, l.TableName, l.Schema,
			),
		}
	}

	// Compute whether this could be an index-only scan
	// Index-only scan is possible when all projected columns are in the index
	// Note: Current HashIndex only stores RowIDs, so true index-only scan
	// is not yet possible. This is for future use.
	isIndexOnly := isIndexOnlyScan(indexDef, l.Projections, l.TableDef)

	// Convert LookupKeys from []any to []binder.BoundExpr if possible
	// The hint.LookupKeys contains optimizer.PredicateExpr values stored as []any.
	// We attempt to type-assert them to binder.BoundExpr if they implement that interface,
	// otherwise we leave LookupKeys as nil and the executor will handle it.
	var lookupKeys []binder.BoundExpr
	for _, key := range hint.LookupKeys {
		if boundExpr, ok := key.(binder.BoundExpr); ok {
			lookupKeys = append(lookupKeys, boundExpr)
		}
		// If not a BoundExpr, we skip it - the executor will need to
		// reconstruct lookup keys from the original filter condition
	}

	// Convert ResidualFilter from any to binder.BoundExpr if possible
	// The hint.ResidualFilter may be a []any containing predicates or a single expression.
	var residualFilter binder.BoundExpr
	if hint.ResidualFilter != nil {
		if boundExpr, ok := hint.ResidualFilter.(binder.BoundExpr); ok {
			residualFilter = boundExpr
		}
		// If it's a []any of predicates, the executor will need to handle it
	}

	// Extract range scan bounds if this is a range scan
	var lowerBound, upperBound binder.BoundExpr
	var lowerInclusive, upperInclusive bool
	var rangeColumnIndex int
	isRangeScan := hint.IsRangeScan && hint.RangeBounds != nil

	if isRangeScan {
		// Convert lower bound to BoundExpr if possible
		if hint.RangeBounds.LowerBound != nil {
			lowerBound = convertToBoundExpr(hint.RangeBounds.LowerBound)
		}
		// Convert upper bound to BoundExpr if possible
		if hint.RangeBounds.UpperBound != nil {
			upperBound = convertToBoundExpr(hint.RangeBounds.UpperBound)
		}
		lowerInclusive = hint.RangeBounds.LowerInclusive
		upperInclusive = hint.RangeBounds.UpperInclusive
		rangeColumnIndex = hint.RangeBounds.RangeColumnIndex
	}

	// Create the PhysicalIndexScan node
	// Note: LookupKeys and ResidualFilter may be nil if the optimizer predicates
	// don't directly implement binder.BoundExpr. In that case, the executor
	// will need to extract lookup values from the original filter or handle
	// residual filtering separately.
	return &PhysicalIndexScan{
		Schema:           l.Schema,
		TableName:        l.TableName,
		Alias:            l.Alias,
		TableDef:         l.TableDef,
		IndexName:        hint.IndexName,
		IndexDef:         indexDef,
		LookupKeys:       lookupKeys,
		ResidualFilter:   residualFilter,
		Projections:      l.Projections,
		IsIndexOnly:      isIndexOnly,
		IsRangeScan:      isRangeScan,
		LowerBound:       lowerBound,
		UpperBound:       upperBound,
		LowerInclusive:   lowerInclusive,
		UpperInclusive:   upperInclusive,
		RangeColumnIndex: rangeColumnIndex,
	}, nil
}

// literalValueExpr is an interface for expressions that provide a literal value.
// This matches the optimizer.LiteralPredicateExpr interface without importing optimizer.
type literalValueExpr interface {
	PredicateLiteralValue() any
}

// convertToBoundExpr attempts to convert an any value to a binder.BoundExpr.
// It handles:
// 1. Direct binder.BoundExpr values
// 2. LiteralPredicateExpr from the optimizer (via the literalValueExpr interface)
// 3. Raw literal values (wrapped in BoundLiteral)
func convertToBoundExpr(v any) binder.BoundExpr {
	if v == nil {
		return nil
	}

	// Case 1: Already a BoundExpr
	if boundExpr, ok := v.(binder.BoundExpr); ok {
		return boundExpr
	}

	// Case 2: Optimizer's LiteralPredicateExpr (has PredicateLiteralValue method)
	if litExpr, ok := v.(literalValueExpr); ok {
		litVal := litExpr.PredicateLiteralValue()
		return wrapLiteralValue(litVal)
	}

	// Case 3: Raw literal value
	return wrapLiteralValue(v)
}

// wrapLiteralValue wraps a raw value in a BoundLiteral with appropriate type.
func wrapLiteralValue(v any) *binder.BoundLiteral {
	if v == nil {
		return nil
	}

	var valType dukdb.Type
	switch v.(type) {
	case int, int8, int16, int32, int64:
		valType = dukdb.TYPE_BIGINT
	case uint, uint8, uint16, uint32, uint64:
		valType = dukdb.TYPE_UBIGINT
	case float32, float64:
		valType = dukdb.TYPE_DOUBLE
	case string:
		valType = dukdb.TYPE_VARCHAR
	case bool:
		valType = dukdb.TYPE_BOOLEAN
	case []byte:
		valType = dukdb.TYPE_BLOB
	default:
		valType = dukdb.TYPE_ANY
	}

	return &binder.BoundLiteral{
		Value:   v,
		ValType: valType,
	}
}

// isIndexOnlyScan determines if all projected columns can be satisfied from the index.
// Returns true if the index covers all columns that need to be retrieved.
// Note: Current HashIndex only stores RowIDs, so true index-only scan
// is not yet possible. This is for future optimization.
func isIndexOnlyScan(
	indexDef *catalog.IndexDef,
	projections []int,
	tableDef *catalog.TableDef,
) bool {
	// If projecting all columns (nil projections means SELECT *),
	// we can only do index-only if index has all columns
	if projections == nil {
		// SELECT * - index must contain all table columns
		if len(indexDef.Columns) != len(tableDef.Columns) {
			return false
		}
		// Check all table columns are in the index
		indexCols := make(map[string]bool)
		for _, col := range indexDef.Columns {
			indexCols[strings.ToLower(col)] = true
		}
		for _, col := range tableDef.Columns {
			if !indexCols[strings.ToLower(col.Name)] {
				return false
			}
		}

		return true
	}

	// Check if all projected columns are in the index
	indexCols := make(map[string]bool)
	for _, col := range indexDef.Columns {
		indexCols[strings.ToLower(col)] = true
	}

	for _, projIdx := range projections {
		if projIdx < 0 || projIdx >= len(tableDef.Columns) {
			return false
		}
		colName := strings.ToLower(tableDef.Columns[projIdx].Name)
		if !indexCols[colName] {
			return false
		}
	}

	return true
}

// Helper functions

func hasAggregates(
	columns []*binder.BoundSelectColumn,
) bool {
	for _, col := range columns {
		if containsAggregate(col.Expr) {
			return true
		}
	}

	return false
}

// isAggregateFunction checks if the function name is an aggregate function.
// This list should match the functions implemented in executor/expr.go computeAggregate.
func isAggregateFunction(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX",
		// Statistical aggregates
		"MEDIAN", "MODE", "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC",
		"ENTROPY", "SKEWNESS", "KURTOSIS",
		"VAR_POP", "VAR_SAMP", "VARIANCE", "STDDEV_POP", "STDDEV_SAMP", "STDDEV",
		// Approximate aggregates
		"APPROX_COUNT_DISTINCT", "APPROX_MEDIAN", "APPROX_QUANTILE",
		// String/list aggregates
		"STRING_AGG", "GROUP_CONCAT", "LISTAGG", "LIST", "ARRAY_AGG", "LIST_DISTINCT",
		// JSON aggregates
		"JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT",
		// Time series aggregates
		"COUNT_IF", "SUM_IF", "AVG_IF", "MIN_IF", "MAX_IF", "FIRST", "LAST", "ANY_VALUE",
		"ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY",
		"HISTOGRAM",
		// Boolean aggregates
		"BOOL_AND", "BOOL_OR", "EVERY",
		// Bitwise aggregates
		"BIT_AND", "BIT_OR", "BIT_XOR",
		// Regression aggregates
		"REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2",
		"CORR", "COVAR_POP", "COVAR_SAMP",
		"REGR_COUNT", "REGR_AVGX", "REGR_AVGY",
		"REGR_SXX", "REGR_SYY", "REGR_SXY",
		// Multiplicative, deviation, and precision aggregates
		"PRODUCT", "MAD", "FAVG", "FSUM", "BITSTRING_AGG",
		// Aliases and geometric/weighted aggregates
		"ARBITRARY", "MEAN", "GEOMETRIC_MEAN", "GEOMEAN", "WEIGHTED_AVG":
		return true
	}
	return false
}

func containsAggregate(
	expr binder.BoundExpr,
) bool {
	switch e := expr.(type) {
	case *binder.BoundFunctionCall:
		return isAggregateFunction(e.Name)
	}

	return false
}

func extractAggregates(
	columns []*binder.BoundSelectColumn,
) []binder.BoundExpr {
	var aggs []binder.BoundExpr
	for _, col := range columns {
		if fn, ok := col.Expr.(*binder.BoundFunctionCall); ok {
			if isAggregateFunction(fn.Name) {
				aggs = append(aggs, col.Expr)
			}
		}
	}

	return aggs
}

func extractAliases(
	columns []*binder.BoundSelectColumn,
) []string {
	aliases := make([]string, len(columns))
	for i, col := range columns {
		aliases[i] = col.Alias
	}

	return aliases
}

// extractAggregateAliases extracts aliases from SELECT columns reordered to match
// the internal aggregate layout: [groupBy..., aggregates..., groupingCalls...].
// SELECT columns can have these types interleaved in any order, but the aggregate
// node stores them grouped by type.
func extractAggregateAliases(
	columns []*binder.BoundSelectColumn,
	numGroupBy, numAgg, numGroupingCalls int,
) []string {
	total := numGroupBy + numAgg + numGroupingCalls
	aliases := make([]string, total)

	var gbIdx, aggIdx, gcIdx int
	for _, col := range columns {
		switch e := col.Expr.(type) {
		case *binder.BoundGroupingCall:
			if numGroupBy+numAgg+gcIdx < total {
				aliases[numGroupBy+numAgg+gcIdx] = col.Alias
				gcIdx++
			}
		case *binder.BoundWindowExpr:
			// Window expressions are handled separately, skip them
			_ = e
		case *binder.BoundFunctionCall:
			if isAggregateFunction(e.Name) {
				if numGroupBy+aggIdx < total {
					aliases[numGroupBy+aggIdx] = col.Alias
					aggIdx++
				}
			} else {
				// Non-aggregate function treated as group-by column
				if gbIdx < numGroupBy {
					aliases[gbIdx] = col.Alias
					gbIdx++
				}
			}
		default:
			// Group-by column reference
			if gbIdx < numGroupBy {
				aliases[gbIdx] = col.Alias
				gbIdx++
			}
		}
	}

	return aliases
}

// mapParserJoinType converts a parser.JoinType to a planner.JoinType.
// This is needed because the two enums have different orderings
// (planner has Semi/Anti between Cross and the new join types).
func mapParserJoinType(pt parser.JoinType) JoinType {
	switch pt {
	case parser.JoinTypeInner:
		return JoinTypeInner
	case parser.JoinTypeLeft:
		return JoinTypeLeft
	case parser.JoinTypeRight:
		return JoinTypeRight
	case parser.JoinTypeFull:
		return JoinTypeFull
	case parser.JoinTypeCross:
		return JoinTypeCross
	case parser.JoinTypePositional:
		return JoinTypePositional
	case parser.JoinTypeAsOf:
		return JoinTypeAsOf
	case parser.JoinTypeAsOfLeft:
		return JoinTypeAsOfLeft
	default:
		return JoinTypeInner
	}
}

func isEquiJoin(condition binder.BoundExpr) bool {
	if condition == nil {
		return false
	}

	binExpr, ok := condition.(*binder.BoundBinaryExpr)
	if !ok {
		return false
	}

	// Check for equality comparison (OpEq = 5 from parser)
	if binExpr.Op == 5 {
		// Check if both sides are column references
		_, leftIsCol := binExpr.Left.(*binder.BoundColumnRef)
		_, rightIsCol := binExpr.Right.(*binder.BoundColumnRef)

		return leftIsCol && rightIsCol
	}

	return false
}

// OpEq is the equality operator value from parser
const OpEq = 5

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

// hasNonWindowAggregates checks if any column contains a non-window aggregate.
func hasNonWindowAggregates(
	columns []*binder.BoundSelectColumn,
) bool {
	for _, col := range columns {
		if containsNonWindowAggregate(col.Expr) {
			return true
		}
	}
	return false
}

// containsNonWindowAggregate checks if an expression contains a non-window aggregate,
// including aggregates nested inside non-aggregate function calls (e.g., ROUND(AVG(x), 2)).
func containsNonWindowAggregate(
	expr binder.BoundExpr,
) bool {
	// Window expressions are handled separately
	if _, ok := expr.(*binder.BoundWindowExpr); ok {
		return false
	}
	switch e := expr.(type) {
	case *binder.BoundFunctionCall:
		if isAggregateFunction(e.Name) {
			return true
		}
		// Recursively check arguments for nested aggregates (e.g., ROUND(AVG(x), 2))
		for _, arg := range e.Args {
			if containsNonWindowAggregate(arg) {
				return true
			}
		}
	}
	return false
}

// extractNonWindowAggregates extracts non-window aggregate expressions from columns.
// It handles both direct aggregates (AVG(x)) and nested aggregates (ROUND(AVG(x), 2)).
func extractNonWindowAggregates(
	columns []*binder.BoundSelectColumn,
) []binder.BoundExpr {
	var aggs []binder.BoundExpr
	seen := make(map[binder.BoundExpr]bool)
	for _, col := range columns {
		// Skip window expressions
		if _, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			continue
		}
		collectNestedAggregates(col.Expr, &aggs, seen)
	}
	return aggs
}

// collectNestedAggregates recursively collects all aggregate function calls from an expression.
func collectNestedAggregates(
	expr binder.BoundExpr,
	aggs *[]binder.BoundExpr,
	seen map[binder.BoundExpr]bool,
) {
	if expr == nil || seen[expr] {
		return
	}
	if _, ok := expr.(*binder.BoundWindowExpr); ok {
		return
	}
	if fn, ok := expr.(*binder.BoundFunctionCall); ok {
		if isAggregateFunction(fn.Name) {
			if !seen[expr] {
				seen[expr] = true
				*aggs = append(*aggs, expr)
			}
			return // Don't recurse into aggregate arguments
		}
		// Non-aggregate function: recurse into args
		for _, arg := range fn.Args {
			collectNestedAggregates(arg, aggs, seen)
		}
	}
}

// assignAggregateAliases assigns an alias to each aggregate expression.
// For a direct aggregate (top-level column expr is the agg), the column's alias is used.
// For nested aggregates (agg inside a wrapper like ROUND), a synthetic alias is generated.
func assignAggregateAliases(
	columns []*binder.BoundSelectColumn,
	groupBy []binder.BoundExpr,
	aggregates []binder.BoundExpr,
	groupingCalls []*binder.BoundGroupingCall,
) []string {
	// Build a map from aggregate expression pointer -> column alias (for direct aggs)
	directAggAlias := make(map[binder.BoundExpr]string)
	for _, col := range columns {
		if _, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			continue
		}
		if fn, ok := col.Expr.(*binder.BoundFunctionCall); ok {
			if isAggregateFunction(fn.Name) {
				directAggAlias[col.Expr] = col.Alias
			}
		}
	}

	aliases := make([]string, len(aggregates))
	syntheticIdx := 0
	for i, agg := range aggregates {
		if alias, ok := directAggAlias[agg]; ok && alias != "" {
			aliases[i] = alias
		} else {
			// Synthetic alias for nested aggregates
			aliases[i] = fmt.Sprintf("__agg_%d", syntheticIdx)
			syntheticIdx++
		}
	}
	return aliases
}

// buildFullAliases builds the aliases array for the aggregate node in the layout:
// [groupBy aliases..., aggregate aliases..., groupingCall aliases...]
func buildFullAliases(
	columns []*binder.BoundSelectColumn,
	groupBy []binder.BoundExpr,
	aggregates []binder.BoundExpr,
	groupingCalls []*binder.BoundGroupingCall,
	aggAliases []string,
) []string {
	numGroupBy := len(groupBy)
	numAgg := len(aggregates)
	numGC := len(groupingCalls)
	total := numGroupBy + numAgg + numGC
	aliases := make([]string, total)

	// Group-by aliases: use column aliases for columns that reference group-by exprs
	gbIdx := 0
	gcIdx := 0
	for _, col := range columns {
		if _, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			continue
		}
		if _, ok := col.Expr.(*binder.BoundGroupingCall); ok {
			if numGroupBy+numAgg+gcIdx < total {
				aliases[numGroupBy+numAgg+gcIdx] = col.Alias
				gcIdx++
			}
			continue
		}
		if fn, ok := col.Expr.(*binder.BoundFunctionCall); ok {
			if isAggregateFunction(fn.Name) {
				continue // aggregate aliases already handled via aggAliases
			}
		}
		// If this column is not an aggregate and not a window expr, it's likely a group-by column
		if containsNonWindowAggregate(col.Expr) {
			continue // contains nested agg, handled separately
		}
		if gbIdx < numGroupBy {
			aliases[gbIdx] = col.Alias
			gbIdx++
		}
	}

	// Aggregate aliases
	for i, alias := range aggAliases {
		if numGroupBy+i < total {
			aliases[numGroupBy+i] = alias
		}
	}

	return aliases
}

// liftNestedAggregates transforms column expressions so that nested aggregate calls
// are replaced with BoundColumnRef references to the aggregate result columns.
// For example, ROUND(AVG(value), 4) becomes ROUND(__agg_N, 4) where __agg_N is the
// internal column name for AVG(value) in the aggregate output.
//
// Parameters:
//   - columns: SELECT column list
//   - groupByAliases: aliases for the GROUP BY columns (index 0..numGroupBy-1)
//   - aggAliases: aliases assigned to each aggregate in extractNonWindowAggregates order
//   - aggregates: the list of aggregate expressions (from extractNonWindowAggregates)
//
// Returns the transformed projection expressions (one per SELECT column).
func liftNestedAggregates(
	columns []*binder.BoundSelectColumn,
	numGroupBy int,
	aggregates []binder.BoundExpr,
	aggAliases []string,
) []binder.BoundExpr {
	// Build a map from aggregate expression pointer -> column alias in aggregate output
	aggToAlias := make(map[binder.BoundExpr]string, len(aggregates))
	for i, agg := range aggregates {
		aggToAlias[agg] = aggAliases[i]
	}

	result := make([]binder.BoundExpr, len(columns))
	for i, col := range columns {
		result[i] = replaceAggregatesWithRefs(col.Expr, aggToAlias)
	}
	return result
}

// replaceAggregatesWithRefs replaces aggregate function calls in an expression
// with BoundColumnRef nodes that reference the pre-computed aggregate result columns.
func replaceAggregatesWithRefs(
	expr binder.BoundExpr,
	aggToAlias map[binder.BoundExpr]string,
) binder.BoundExpr {
	if expr == nil {
		return nil
	}

	// Check if this expression IS a known aggregate
	if alias, ok := aggToAlias[expr]; ok {
		return &binder.BoundColumnRef{
			Column:  alias,
			ColType: expr.ResultType(),
		}
	}

	// For non-aggregate function calls, recursively replace in arguments
	if fn, ok := expr.(*binder.BoundFunctionCall); ok {
		if isAggregateFunction(fn.Name) {
			// This aggregate wasn't in our map (shouldn't happen), leave as-is
			return expr
		}
		// Check if any argument contains an aggregate
		hasAggArg := false
		for _, arg := range fn.Args {
			if containsNestedAgg(arg, aggToAlias) {
				hasAggArg = true
				break
			}
		}
		if !hasAggArg {
			return expr
		}
		// Rebuild function call with transformed args
		newArgs := make([]binder.BoundExpr, len(fn.Args))
		for j, arg := range fn.Args {
			newArgs[j] = replaceAggregatesWithRefs(arg, aggToAlias)
		}
		return &binder.BoundFunctionCall{
			Name:     fn.Name,
			Args:     newArgs,
			Distinct: fn.Distinct,
			Star:     fn.Star,
		}
	}

	return expr
}

// containsNestedAgg checks if an expression contains a reference to a known aggregate.
func containsNestedAgg(expr binder.BoundExpr, aggToAlias map[binder.BoundExpr]string) bool {
	if expr == nil {
		return false
	}
	if _, ok := aggToAlias[expr]; ok {
		return true
	}
	if fn, ok := expr.(*binder.BoundFunctionCall); ok {
		for _, arg := range fn.Args {
			if containsNestedAgg(arg, aggToAlias) {
				return true
			}
		}
	}
	return false
}

// extractWindowExprs extracts window expressions from SELECT columns.
// It also copies the column alias to the window expression for QUALIFY clause support.
func extractWindowExprs(
	columns []*binder.BoundSelectColumn,
) []*binder.BoundWindowExpr {
	var windowExprs []*binder.BoundWindowExpr
	for _, col := range columns {
		if windowExpr, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			// Copy the column alias to the window expression so that QUALIFY
			// can reference the window result by its alias (e.g., "rn" in "QUALIFY rn <= 2")
			windowExpr.Alias = col.Alias
			windowExprs = append(windowExprs, windowExpr)
		}
	}
	return windowExprs
}


// extractGroupingSets extracts grouping sets from GROUP BY expressions.
// It returns the expanded grouping sets and the regular GROUP BY columns.
// If there are no grouping set expressions, it returns nil and the original expressions.
func extractGroupingSets(
	groupBy []binder.BoundExpr,
) ([][]binder.BoundExpr, []binder.BoundExpr) {
	var groupingSets [][]binder.BoundExpr
	var regularCols []binder.BoundExpr
	var prefixCols []binder.BoundExpr // Regular columns that appear before grouping sets

	for _, expr := range groupBy {
		if gsExpr, ok := expr.(*binder.BoundGroupingSetExpr); ok {
			// Found a grouping set expression - use its expanded sets
			groupingSets = gsExpr.Sets
		} else {
			// Regular GROUP BY expression
			prefixCols = append(prefixCols, expr)
		}
	}

	if groupingSets == nil {
		// No grouping sets found - return as regular GROUP BY
		return nil, prefixCols
	}

	// Collect all unique columns across grouping sets AND prefix columns
	colMap := make(map[string]bool)
	for _, col := range prefixCols {
		key := getExprKey(col)
		if key != "" {
			colMap[key] = true
		}
		regularCols = append(regularCols, col)
	}
	for _, set := range groupingSets {
		for _, col := range set {
			key := getExprKey(col)
			if key != "" && !colMap[key] {
				colMap[key] = true
				regularCols = append(regularCols, col)
			}
		}
	}

	// Prepend prefix columns to each grouping set
	if len(prefixCols) > 0 {
		for i, set := range groupingSets {
			newSet := make([]binder.BoundExpr, 0, len(prefixCols)+len(set))
			newSet = append(newSet, prefixCols...)
			newSet = append(newSet, set...)
			groupingSets[i] = newSet
		}
	}

	return groupingSets, regularCols
}

// getExprKey returns a unique key for an expression (used for deduplication).
func getExprKey(expr binder.BoundExpr) string {
	switch e := expr.(type) {
	case *binder.BoundColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Column
		}
		return e.Column
	case *binder.BoundLiteral:
		return formatLiteralKey(e.Value)
	default:
		// For other expression types, use pointer-based identity
		return ""
	}
}

// formatLiteralKey formats a literal value as a key.
func formatLiteralKey(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "s:" + val
	case int64:
		return "i:" + formatInt64(val)
	case float64:
		return "f:" + formatFloat64(val)
	case bool:
		if val {
			return "b:true"
		}
		return "b:false"
	default:
		return ""
	}
}

// formatInt64 formats an int64 as a string (without importing strconv).
func formatInt64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// formatFloat64 formats a float64 as a string (simple version).
func formatFloat64(v float64) string {
	return formatInt64(int64(v * 1000000))
}

// extractGroupingCalls extracts GROUPING() function calls from SELECT columns.
func extractGroupingCalls(
	columns []*binder.BoundSelectColumn,
) []*binder.BoundGroupingCall {
	var calls []*binder.BoundGroupingCall
	for _, col := range columns {
		if gc, ok := col.Expr.(*binder.BoundGroupingCall); ok {
			calls = append(calls, gc)
		}
	}
	return calls
}

// ---------- SUMMARIZE Physical Plan Node ----------

// PhysicalSummarize represents a physical SUMMARIZE operation.
type PhysicalSummarize struct {
	Schema    string
	TableName string
	TableDef  *catalog.TableDef
	Query     PhysicalPlan // Inner query plan (nil for SUMMARIZE table)
}

func (*PhysicalSummarize) physicalPlanNode() {}

func (s *PhysicalSummarize) Children() []PhysicalPlan {
	if s.Query != nil {
		return []PhysicalPlan{s.Query}
	}
	return nil
}

func (*PhysicalSummarize) OutputColumns() []ColumnBinding { return nil }

// ---------- Export/Import Database Physical Plan Nodes ----------

// PhysicalExportDatabase represents a physical EXPORT DATABASE operation.
type PhysicalExportDatabase struct {
	Path    string
	Options map[string]string
}

func (*PhysicalExportDatabase) physicalPlanNode() {}

func (*PhysicalExportDatabase) Children() []PhysicalPlan { return nil }

func (*PhysicalExportDatabase) OutputColumns() []ColumnBinding { return nil }

// PhysicalImportDatabase represents a physical IMPORT DATABASE operation.
type PhysicalImportDatabase struct {
	Path string
}

func (*PhysicalImportDatabase) physicalPlanNode() {}

func (*PhysicalImportDatabase) Children() []PhysicalPlan { return nil }

func (*PhysicalImportDatabase) OutputColumns() []ColumnBinding { return nil }

// ---------- Database Maintenance Physical Plan Nodes ----------

// PhysicalPragma represents a physical PRAGMA operation.
type PhysicalPragma struct {
	Name       string             // Pragma name
	PragmaType binder.PragmaType  // Category of pragma
	Args       []binder.BoundExpr // Bound arguments
	Value      binder.BoundExpr   // For SET PRAGMA name = value
}

func (*PhysicalPragma) physicalPlanNode() {}

func (*PhysicalPragma) Children() []PhysicalPlan { return nil }

func (*PhysicalPragma) OutputColumns() []ColumnBinding {
	// PRAGMA statements return varying columns depending on the pragma
	// The actual columns are determined at execution time
	return nil
}

// PhysicalExplain represents a physical EXPLAIN operation.
type PhysicalExplain struct {
	Child        PhysicalPlan // The plan to explain
	Analyze      bool         // true for EXPLAIN ANALYZE
	RewriteStats *rewrite.Stats
}

func (*PhysicalExplain) physicalPlanNode() {}

func (e *PhysicalExplain) Children() []PhysicalPlan { return []PhysicalPlan{e.Child} }

func (*PhysicalExplain) OutputColumns() []ColumnBinding {
	// EXPLAIN returns a single column with the plan text
	return []ColumnBinding{
		{Column: "explain_plan", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 0},
	}
}

// PhysicalVacuum represents a physical VACUUM operation.
type PhysicalVacuum struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for entire database)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*PhysicalVacuum) physicalPlanNode() {}

func (*PhysicalVacuum) Children() []PhysicalPlan { return nil }

func (*PhysicalVacuum) OutputColumns() []ColumnBinding { return nil }

// PhysicalAnalyze represents a physical ANALYZE operation.
type PhysicalAnalyze struct {
	Schema    string            // Optional schema name
	TableName string            // Optional table name (empty for all tables)
	TableDef  *catalog.TableDef // Table definition if table specified
}

func (*PhysicalAnalyze) physicalPlanNode() {}

func (*PhysicalAnalyze) Children() []PhysicalPlan { return nil }

func (*PhysicalAnalyze) OutputColumns() []ColumnBinding { return nil }

// PhysicalCheckpoint represents a physical CHECKPOINT operation.
type PhysicalCheckpoint struct {
	Database string // Optional database name
	Force    bool   // FORCE flag
}

func (*PhysicalCheckpoint) physicalPlanNode() {}

func (*PhysicalCheckpoint) Children() []PhysicalPlan { return nil }

func (*PhysicalCheckpoint) OutputColumns() []ColumnBinding { return nil }

// ---------- Iceberg Physical Plan Nodes ----------

// PartitionFilter represents a filter that can be pushed to Iceberg partition pruning.
type PartitionFilter struct {
	// FieldName is the partition field name.
	FieldName string
	// Operator is the comparison operator ("=", "<", ">", "<=", ">=", "!=", "IN").
	Operator string
	// Value is the filter value (or slice for IN operator).
	Value any
	// Transform is the partition transform applied to this field (identity, year, month, day, hour, bucket, truncate).
	Transform string
	// TransformArg is the argument for bucket/truncate transforms.
	TransformArg int
}

// ColumnStat contains column statistics for pruning decisions.
type ColumnStat struct {
	// ColumnName is the column name.
	ColumnName string
	// NullCount is the number of null values.
	NullCount int64
	// DistinctCount is the number of distinct values.
	DistinctCount int64
	// MinValue is the minimum value (if available).
	MinValue any
	// MaxValue is the maximum value (if available).
	MaxValue any
}

// PhysicalIcebergScan represents a physical scan of an Apache Iceberg table.
// It contains all the information needed to execute the scan with partition
// pruning and column projection.
type PhysicalIcebergScan struct {
	// TablePath is the path to the Iceberg table location.
	TablePath string
	// Alias is the table alias for column reference qualification.
	Alias string
	// Columns contains the columns to project (nil = all columns).
	Columns []string
	// Filter contains the original filter predicate.
	Filter binder.BoundExpr
	// TimeTravel contains the time travel specification (nil = current snapshot).
	TimeTravel *TimeTravelClause
	// Options contains additional options for the Iceberg reader.
	Options map[string]any
	// PartitionFilters contains filters extracted for partition pruning.
	PartitionFilters []PartitionFilter
	// ColumnStats contains column statistics for additional pruning.
	ColumnStats []ColumnStat
	// ResidualFilter contains filters that cannot be pushed to partition pruning.
	ResidualFilter binder.BoundExpr
	// EstimatedRows is the estimated number of rows after pruning.
	EstimatedRows int64
	// ColumnTypes contains the types for each column (populated during planning).
	ColumnTypes []dukdb.Type
	// columns caches the output column bindings.
	columns []ColumnBinding
}

func (*PhysicalIcebergScan) physicalPlanNode() {}

func (*PhysicalIcebergScan) Children() []PhysicalPlan { return nil }

func (s *PhysicalIcebergScan) OutputColumns() []ColumnBinding {
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
