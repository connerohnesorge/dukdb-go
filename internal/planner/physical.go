package planner

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
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
	Schema    string
	Table     string
	TableDef  *catalog.TableDef
	Columns   []int
	Values    [][]binder.BoundExpr
	Source    PhysicalPlan
	Returning []*binder.BoundSelectColumn // RETURNING clause columns
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
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
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

// PhysicalDummyScan represents a physical dummy scan.
type PhysicalDummyScan struct{}

func (*PhysicalDummyScan) physicalPlanNode() {}

func (*PhysicalDummyScan) Children() []PhysicalPlan { return nil }

func (*PhysicalDummyScan) OutputColumns() []ColumnBinding { return nil }

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
	Schema       string
	Table        string
	TableDef     *catalog.TableDef
	Operation    int                // AlterTableOp from parser
	IfExists     bool               // IF EXISTS modifier
	NewTableName string             // RENAME TO
	OldColumn    string             // RENAME COLUMN
	NewColumn    string             // RENAME COLUMN
	DropColumn   string             // DROP COLUMN
	AddColumn    *catalog.ColumnDef // ADD COLUMN
}

func (*PhysicalAlterTable) physicalPlanNode() {}

func (*PhysicalAlterTable) Children() []PhysicalPlan { return nil }

func (*PhysicalAlterTable) OutputColumns() []ColumnBinding { return nil }

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
	catalog *catalog.Catalog
}

// NewPlanner creates a new Planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	return &Planner{catalog: cat}
}

// Plan creates a physical plan from a bound statement.
func (p *Planner) Plan(
	stmt binder.BoundStatement,
) (PhysicalPlan, error) {
	// First create logical plan, then convert to physical
	logical, err := p.createLogicalPlan(stmt)
	if err != nil {
		return nil, err
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
	case *binder.BoundMergeStmt:
		return p.planMerge(s)
	case *binder.BoundPivotStmt:
		return p.planPivot(s)
	case *binder.BoundUnpivotStmt:
		return p.planUnpivot(s)
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

			joinType := JoinType(join.Type)

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
	if len(s.GroupBy) > 0 ||
		hasNonWindowAggregates(s.Columns) {
		// Extract grouping sets if present
		groupingSets, regularGroupBy := extractGroupingSets(s.GroupBy)
		groupBy := regularGroupBy
		aggregates := extractNonWindowAggregates(s.Columns)
		aliases := extractAliases(s.Columns)

		// Extract GROUPING() function calls from the select columns
		groupingCalls := extractGroupingCalls(s.Columns)

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
	// Window goes AFTER aggregation/filter, BEFORE projection
	windowExprs := extractWindowExprs(s.Columns)
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
	expressions := make(
		[]binder.BoundExpr,
		len(s.Columns),
	)
	aliases := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		expressions[i] = col.Expr
		aliases[i] = col.Alias
	}

	plan = &LogicalProject{
		Child:       plan,
		Expressions: expressions,
		Aliases:     aliases,
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
		}
	}

	// SAMPLE - apply sampling after everything else
	if s.Sample != nil {
		plan = &LogicalSample{
			Child:  plan,
			Sample: s.Sample,
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
		Schema:    s.Schema,
		Table:     s.Table,
		TableDef:  s.TableDef,
		Columns:   s.Columns,
		Values:    s.Values,
		Source:    source,
		Returning: s.Returning,
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
		Columns:     s.Columns,
		PrimaryKey:  s.PrimaryKey,
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
		Schema:       s.Schema,
		Table:        s.Table,
		TableDef:     s.TableDef,
		Operation:    int(s.Operation),
		IfExists:     s.IfExists,
		NewTableName: s.NewTableName,
		OldColumn:    s.OldColumn,
		NewColumn:    s.NewColumn,
		DropColumn:   s.DropColumn,
		AddColumn:    s.AddColumn,
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

		return &PhysicalScan{
			Schema:      l.Schema,
			TableName:   l.TableName,
			Alias:       l.Alias,
			TableDef:    l.TableDef,
			Projections: l.Projections,
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

		// Use hash join for equi-joins, nested loop for others
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
			Schema:    l.Schema,
			Table:     l.Table,
			TableDef:  l.TableDef,
			Columns:   l.Columns,
			Values:    l.Values,
			Source:    source,
			Returning: l.Returning,
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
			Columns:     l.Columns,
			PrimaryKey:  l.PrimaryKey,
		}, nil

	case *LogicalDropTable:
		return &PhysicalDropTable{
			Schema:   l.Schema,
			Table:    l.Table,
			IfExists: l.IfExists,
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
			Schema:       l.Schema,
			Table:        l.Table,
			TableDef:     l.TableDef,
			Operation:    l.Operation,
			IfExists:     l.IfExists,
			NewTableName: l.NewTableName,
			OldColumn:    l.OldColumn,
			NewColumn:    l.NewColumn,
			DropColumn:   l.DropColumn,
			AddColumn:    l.AddColumn,
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
			MaxRecursion:  1000, // Default max recursion limit
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

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypePlanner,
			Msg:  "unsupported logical plan node",
		}
	}
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

func containsAggregate(
	expr binder.BoundExpr,
) bool {
	switch e := expr.(type) {
	case *binder.BoundFunctionCall:
		name := e.Name
		switch name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	}

	return false
}

func extractAggregates(
	columns []*binder.BoundSelectColumn,
) []binder.BoundExpr {
	var aggs []binder.BoundExpr
	for _, col := range columns {
		if fn, ok := col.Expr.(*binder.BoundFunctionCall); ok {
			switch fn.Name {
			case "COUNT",
				"SUM",
				"AVG",
				"MIN",
				"MAX":
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

// containsNonWindowAggregate checks if an expression is a non-window aggregate.
func containsNonWindowAggregate(
	expr binder.BoundExpr,
) bool {
	// Window expressions are handled separately
	if _, ok := expr.(*binder.BoundWindowExpr); ok {
		return false
	}
	switch e := expr.(type) {
	case *binder.BoundFunctionCall:
		name := e.Name
		switch name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	}
	return false
}

// extractNonWindowAggregates extracts non-window aggregate expressions from columns.
func extractNonWindowAggregates(
	columns []*binder.BoundSelectColumn,
) []binder.BoundExpr {
	var aggs []binder.BoundExpr
	for _, col := range columns {
		// Skip window expressions
		if _, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			continue
		}
		if fn, ok := col.Expr.(*binder.BoundFunctionCall); ok {
			switch fn.Name {
			case "COUNT", "SUM", "AVG", "MIN", "MAX":
				aggs = append(aggs, col.Expr)
			}
		}
	}
	return aggs
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

	for _, expr := range groupBy {
		if gsExpr, ok := expr.(*binder.BoundGroupingSetExpr); ok {
			// Found a grouping set expression - use its expanded sets
			groupingSets = gsExpr.Sets
			// The columns for the grouping set are all unique expressions in the sets
			colMap := make(map[string]binder.BoundExpr)
			for _, set := range gsExpr.Sets {
				for _, col := range set {
					// Use a simple key based on column identity
					key := getExprKey(col)
					if _, exists := colMap[key]; !exists {
						colMap[key] = col
						regularCols = append(regularCols, col)
					}
				}
			}
		} else {
			// Regular GROUP BY expression
			regularCols = append(regularCols, expr)
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
