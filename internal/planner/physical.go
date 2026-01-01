package planner

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// PhysicalPlan represents a node in the physical query plan.
type PhysicalPlan interface {
	physicalPlanNode()
	Children() []PhysicalPlan
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
	Child  PhysicalPlan
	Limit  int64
	Offset int64
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

// PhysicalInsert represents a physical INSERT operation.
type PhysicalInsert struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int
	Values   [][]binder.BoundExpr
	Source   PhysicalPlan
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
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Set      []*binder.BoundSetClause
	Source   PhysicalPlan
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
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Source   PhysicalPlan
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
			right := p.createScanForBoundTableRef(s.From[i])
			plan = &LogicalJoin{
				Left:     plan,
				Right:    right,
				JoinType: JoinTypeCross,
			}
		}

		// Explicit JOINs
		for _, join := range s.Joins {
			right := p.createScanForBoundTableRef(join.Table)

			joinType := JoinType(join.Type)
			plan = &LogicalJoin{
				Left:      plan,
				Right:     right,
				JoinType:  joinType,
				Condition: join.Condition,
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
		groupBy := s.GroupBy
		aggregates := extractNonWindowAggregates(s.Columns)
		aliases := extractAliases(s.Columns)

		plan = &LogicalAggregate{
			Child:      plan,
			GroupBy:    groupBy,
			Aggregates: aggregates,
			Aliases:    aliases,
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

	// DISTINCT
	if s.Distinct {
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

		if s.Limit != nil {
			if lit, ok := s.Limit.(*binder.BoundLiteral); ok {
				limit = toInt64(lit.Value)
			}
		}
		if s.Offset != nil {
			if lit, ok := s.Offset.(*binder.BoundLiteral); ok {
				offset = toInt64(lit.Value)
			}
		}

		plan = &LogicalLimit{
			Child:  plan,
			Limit:  limit,
			Offset: offset,
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
		Schema:   s.Schema,
		Table:    s.Table,
		TableDef: s.TableDef,
		Columns:  s.Columns,
		Values:   s.Values,
		Source:   source,
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
		Schema:   s.Schema,
		Table:    s.Table,
		TableDef: s.TableDef,
		Set:      s.Set,
		Source:   source,
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
		Schema:   s.Schema,
		Table:    s.Table,
		TableDef: s.TableDef,
		Source:   source,
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

// createScanForBoundTableRef creates a LogicalScan for a bound table reference.
// This handles regular tables, virtual tables, and table functions.
func (p *Planner) createScanForBoundTableRef(ref *binder.BoundTableRef) LogicalPlan {
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

	case *LogicalAggregate:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalHashAggregate{
			Child:      child,
			GroupBy:    l.GroupBy,
			Aggregates: l.Aggregates,
			Aliases:    l.Aliases,
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
			Child:  child,
			Limit:  l.Limit,
			Offset: l.Offset,
		}, nil

	case *LogicalDistinct:
		child, err := p.createPhysicalPlan(l.Child)
		if err != nil {
			return nil, err
		}

		return &PhysicalDistinct{Child: child}, nil

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
			Schema:   l.Schema,
			Table:    l.Table,
			TableDef: l.TableDef,
			Columns:  l.Columns,
			Values:   l.Values,
			Source:   source,
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
			Schema:   l.Schema,
			Table:    l.Table,
			TableDef: l.TableDef,
			Set:      l.Set,
			Source:   source,
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
			Schema:   l.Schema,
			Table:    l.Table,
			TableDef: l.TableDef,
			Source:   source,
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
func extractWindowExprs(
	columns []*binder.BoundSelectColumn,
) []*binder.BoundWindowExpr {
	var windowExprs []*binder.BoundWindowExpr
	for _, col := range columns {
		if windowExpr, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			windowExprs = append(windowExprs, windowExpr)
		}
	}
	return windowExprs
}

// containsWindowExpr checks if any column contains a window expression.
func containsWindowExpr(
	columns []*binder.BoundSelectColumn,
) bool {
	for _, col := range columns {
		if _, ok := col.Expr.(*binder.BoundWindowExpr); ok {
			return true
		}
	}
	return false
}
