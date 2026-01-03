// Package planner provides query planning for the native Go DuckDB implementation.
package planner

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
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
	VirtualTable  *catalog.VirtualTableDef  // Set for virtual tables
	TableFunction *binder.BoundTableFunctionRef // Set for table functions
	Projections   []int                     // Column indices to project, nil for all
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
	Child  LogicalPlan
	Limit  int64
	Offset int64
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

// LogicalInsert represents an INSERT operation.
type LogicalInsert struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int
	Values   [][]binder.BoundExpr
	Source   LogicalPlan // For INSERT ... SELECT
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
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Set      []*binder.BoundSetClause
	Source   LogicalPlan // Scan + Filter
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
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Source   LogicalPlan // Scan + Filter
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
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
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
	Columns  []int          // Column indices to import (nil for all)
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
	Columns  []int          // Column indices to export (nil for all)
	FilePath string
	Options  map[string]any
	Source   LogicalPlan    // For COPY (SELECT...) TO
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
	Schema       string
	Table        string
	TableDef     *catalog.TableDef
	Operation    int    // AlterTableOp from parser
	IfExists     bool   // IF EXISTS modifier
	NewTableName string // RENAME TO
	OldColumn    string // RENAME COLUMN
	NewColumn    string // RENAME COLUMN
	DropColumn   string // DROP COLUMN
	AddColumn    *catalog.ColumnDef // ADD COLUMN
}

func (*LogicalAlterTable) logicalPlanNode() {}

func (*LogicalAlterTable) Children() []LogicalPlan { return nil }

func (*LogicalAlterTable) OutputColumns() []ColumnBinding { return nil }
