package parser

// CountParameters counts the number of parameter placeholders in a statement.
func CountParameters(stmt Statement) int {
	counter := &paramCounter{}
	counter.countStmt(stmt)

	return counter.count
}

// ParameterInfo contains metadata about a parameter placeholder.
type ParameterInfo struct {
	Position int    // 1-based position
	Name     string // Empty for positional parameters
}

// CollectParameters collects all parameter placeholders from a statement.
func CollectParameters(
	stmt Statement,
) []ParameterInfo {
	collector := &paramCollector{
		params: make(map[int]ParameterInfo),
	}
	collector.collectStmt(stmt)

	// Convert map to slice ordered by position
	result := make(
		[]ParameterInfo,
		0,
		len(collector.params),
	)
	for i := 1; i <= len(collector.params); i++ {
		if p, ok := collector.params[i]; ok {
			result = append(result, p)
		}
	}

	return result
}

type paramCollector struct {
	params   map[int]ParameterInfo
	position int // For positional parameters
}

func (c *paramCollector) collectStmt(
	stmt Statement,
) {
	switch s := stmt.(type) {
	case *SelectStmt:
		for _, col := range s.Columns {
			c.collectExpr(col.Expr)
		}
		if s.Where != nil {
			c.collectExpr(s.Where)
		}
		for _, g := range s.GroupBy {
			c.collectExpr(g)
		}
		if s.Having != nil {
			c.collectExpr(s.Having)
		}
		for _, o := range s.OrderBy {
			c.collectExpr(o.Expr)
		}
		if s.Limit != nil {
			c.collectExpr(s.Limit)
		}
		if s.Offset != nil {
			c.collectExpr(s.Offset)
		}
	case *InsertStmt:
		for _, row := range s.Values {
			for _, val := range row {
				c.collectExpr(val)
			}
		}
		if s.Select != nil {
			c.collectStmt(s.Select)
		}
	case *UpdateStmt:
		for _, set := range s.Set {
			c.collectExpr(set.Value)
		}
		if s.Where != nil {
			c.collectExpr(s.Where)
		}
	case *DeleteStmt:
		if s.Where != nil {
			c.collectExpr(s.Where)
		}
	case *CreateViewStmt:
		// Collect parameters from the view's SELECT query
		if s.Query != nil {
			c.collectStmt(s.Query)
		}
	case *CreateSequenceStmt:
		// No parameters in CREATE SEQUENCE
	case *CreateIndexStmt:
		// No parameters in CREATE INDEX
	case *CreateSchemaStmt:
		// No parameters in CREATE SCHEMA
	case *DropViewStmt, *DropIndexStmt, *DropSequenceStmt, *DropSchemaStmt, *DropTableStmt, *DropTypeStmt, *DropMacroStmt:
		// No parameters in DROP statements
	case *CreateTypeStmt:
		// No parameters in CREATE TYPE
	case *CreateMacroStmt:
		// No parameters in CREATE MACRO
	case *AlterTableStmt:
		// No parameters in ALTER TABLE (could add column defaults with params in future)
	case *PrepareStmt:
		c.collectStmt(s.Inner)
	case *ExecuteStmt:
		for _, param := range s.Params {
			c.collectExpr(param)
		}
	case *DeallocateStmt:
		// No parameters
	case *ExportDatabaseStmt:
		// No parameters in EXPORT DATABASE
	case *ImportDatabaseStmt:
		// No parameters in IMPORT DATABASE
	case *InstallStmt:
		// No parameters in INSTALL
	case *LoadStmt:
		// No parameters in LOAD
	case *AttachStmt:
		// No parameters in ATTACH
	case *DetachStmt:
		// No parameters in DETACH
	case *UseStmt:
		// No parameters in USE
	case *CreateDatabaseStmt:
		// No parameters in CREATE DATABASE
	case *DropDatabaseStmt:
		// No parameters in DROP DATABASE
	}
}

func (c *paramCollector) collectExpr(expr Expr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *Parameter:
		pos := e.Position
		if pos == 0 {
			c.position++
			pos = c.position
		}
		c.params[pos] = ParameterInfo{
			Position: pos,
			Name:     e.Name,
		}
	case *BinaryExpr:
		c.collectExpr(e.Left)
		c.collectExpr(e.Right)
	case *UnaryExpr:
		c.collectExpr(e.Expr)
	case *FunctionCall:
		for _, arg := range e.Args {
			c.collectExpr(arg)
		}
	case *WindowExpr:
		// Collect from function arguments
		for _, arg := range e.Function.Args {
			c.collectExpr(arg)
		}
		// Collect from partition by
		for _, p := range e.PartitionBy {
			c.collectExpr(p)
		}
		// Collect from order by
		for _, o := range e.OrderBy {
			c.collectExpr(o.Expr)
		}
		// Collect from frame bounds
		if e.Frame != nil {
			c.collectExpr(e.Frame.Start.Offset)
			c.collectExpr(e.Frame.End.Offset)
		}
		// Collect from filter
		c.collectExpr(e.Filter)
	case *CastExpr:
		c.collectExpr(e.Expr)
	case *CaseExpr:
		c.collectExpr(e.Operand)
		for _, w := range e.Whens {
			c.collectExpr(w.Condition)
			c.collectExpr(w.Result)
		}
		c.collectExpr(e.Else)
	case *BetweenExpr:
		c.collectExpr(e.Expr)
		c.collectExpr(e.Low)
		c.collectExpr(e.High)
	case *InListExpr:
		c.collectExpr(e.Expr)
		for _, v := range e.Values {
			c.collectExpr(v)
		}
	case *InSubqueryExpr:
		c.collectExpr(e.Expr)
		c.collectStmt(e.Subquery)
	case *ExistsExpr:
		c.collectStmt(e.Subquery)
	case *SimilarToExpr:
		c.collectExpr(e.Expr)
		c.collectExpr(e.Pattern)
	case *LambdaExpr:
		c.collectExpr(e.Body)
	case *SelectStmt:
		c.collectStmt(e)
	}
}

type paramCounter struct {
	count int
}

func (c *paramCounter) countStmt(stmt Statement) {
	switch s := stmt.(type) {
	case *SelectStmt:
		for _, col := range s.Columns {
			c.countExpr(col.Expr)
		}
		if s.Where != nil {
			c.countExpr(s.Where)
		}
		for _, g := range s.GroupBy {
			c.countExpr(g)
		}
		if s.Having != nil {
			c.countExpr(s.Having)
		}
		for _, o := range s.OrderBy {
			c.countExpr(o.Expr)
		}
		if s.Limit != nil {
			c.countExpr(s.Limit)
		}
		if s.Offset != nil {
			c.countExpr(s.Offset)
		}
	case *InsertStmt:
		for _, row := range s.Values {
			for _, val := range row {
				c.countExpr(val)
			}
		}
		if s.Select != nil {
			c.countStmt(s.Select)
		}
	case *UpdateStmt:
		for _, set := range s.Set {
			c.countExpr(set.Value)
		}
		if s.Where != nil {
			c.countExpr(s.Where)
		}
	case *DeleteStmt:
		if s.Where != nil {
			c.countExpr(s.Where)
		}
	case *CreateViewStmt:
		// Count parameters from the view's SELECT query
		if s.Query != nil {
			c.countStmt(s.Query)
		}
	case *CreateSequenceStmt:
		// No parameters in CREATE SEQUENCE
	case *CreateIndexStmt:
		// No parameters in CREATE INDEX
	case *CreateSchemaStmt:
		// No parameters in CREATE SCHEMA
	case *DropViewStmt, *DropIndexStmt, *DropSequenceStmt, *DropSchemaStmt, *DropTableStmt, *DropTypeStmt, *DropMacroStmt:
		// No parameters in DROP statements
	case *CreateTypeStmt:
		// No parameters in CREATE TYPE
	case *CreateMacroStmt:
		// No parameters in CREATE MACRO
	case *AlterTableStmt:
		// No parameters in ALTER TABLE (could add column defaults with params in future)
	case *PrepareStmt:
		c.countStmt(s.Inner)
	case *ExecuteStmt:
		for _, param := range s.Params {
			c.countExpr(param)
		}
	case *DeallocateStmt:
		// No parameters
	case *ExportDatabaseStmt:
		// No parameters in EXPORT DATABASE
	case *ImportDatabaseStmt:
		// No parameters in IMPORT DATABASE
	case *InstallStmt:
		// No parameters in INSTALL
	case *LoadStmt:
		// No parameters in LOAD
	case *AttachStmt:
		// No parameters in ATTACH
	case *DetachStmt:
		// No parameters in DETACH
	case *UseStmt:
		// No parameters in USE
	case *CreateDatabaseStmt:
		// No parameters in CREATE DATABASE
	case *DropDatabaseStmt:
		// No parameters in DROP DATABASE
	}
}

func (c *paramCounter) countExpr(expr Expr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *Parameter:
		// For positional ? parameters, Position is 0, so we increment count
		if e.Position == 0 {
			c.count++
		} else if e.Position > c.count {
			c.count = e.Position
		}
	case *BinaryExpr:
		c.countExpr(e.Left)
		c.countExpr(e.Right)
	case *UnaryExpr:
		c.countExpr(e.Expr)
	case *FunctionCall:
		for _, arg := range e.Args {
			c.countExpr(arg)
		}
	case *WindowExpr:
		// Count from function arguments
		for _, arg := range e.Function.Args {
			c.countExpr(arg)
		}
		// Count from partition by
		for _, p := range e.PartitionBy {
			c.countExpr(p)
		}
		// Count from order by
		for _, o := range e.OrderBy {
			c.countExpr(o.Expr)
		}
		// Count from frame bounds
		if e.Frame != nil {
			c.countExpr(e.Frame.Start.Offset)
			c.countExpr(e.Frame.End.Offset)
		}
		// Count from filter
		c.countExpr(e.Filter)
	case *CastExpr:
		c.countExpr(e.Expr)
	case *CaseExpr:
		c.countExpr(e.Operand)
		for _, w := range e.Whens {
			c.countExpr(w.Condition)
			c.countExpr(w.Result)
		}
		c.countExpr(e.Else)
	case *BetweenExpr:
		c.countExpr(e.Expr)
		c.countExpr(e.Low)
		c.countExpr(e.High)
	case *InListExpr:
		c.countExpr(e.Expr)
		for _, v := range e.Values {
			c.countExpr(v)
		}
	case *InSubqueryExpr:
		c.countExpr(e.Expr)
		c.countStmt(e.Subquery)
	case *ExistsExpr:
		c.countStmt(e.Subquery)
	case *SimilarToExpr:
		c.countExpr(e.Expr)
		c.countExpr(e.Pattern)
	case *LambdaExpr:
		c.countExpr(e.Body)
	case *SelectStmt:
		c.countStmt(e)
	}
}
