// Package binder provides name and type resolution for parsed SQL statements.
package binder

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// Binder resolves names and checks types in parsed statements.
type Binder struct {
	catalog     *catalog.Catalog
	scope       *BindScope
	udfResolver ScalarUDFResolver
}

// BindScope represents the current binding scope with available tables and columns.
type BindScope struct {
	parent     *BindScope
	tables     map[string]*BoundTableRef
	aliases    map[string]string // alias -> table name
	paramCount int
	params     map[int]dukdb.Type     // position -> inferred type
	ctes       map[string]*CTEBinding // CTE name -> binding info
}

// CTEBinding represents a CTE that is available for reference in the current scope.
type CTEBinding struct {
	// Name is the CTE name.
	Name string
	// Columns contains the column information for the CTE.
	Columns []*BoundColumn
	// Types contains the result types of the CTE columns.
	Types []dukdb.Type
	// Names contains the column names.
	Names []string
	// IsSelfReference is true if this is a placeholder binding for recursive self-reference.
	IsSelfReference bool
	// Query is the bound CTE query (nil for self-reference placeholder).
	// For recursive CTEs, this is the base case query.
	Query *BoundSelectStmt
	// RecursiveQuery is the bound recursive part of a recursive CTE.
	// Only set when Recursive is true.
	RecursiveQuery *BoundSelectStmt
	// Recursive indicates this is a recursive CTE.
	Recursive bool
	// UsingKey specifies USING KEY columns for recursive cycle detection.
	UsingKey []string
	// SetOp captures UNION vs UNION ALL for recursive CTE semantics.
	SetOp parser.SetOpType
	// MaxRecursion is the recursion limit from OPTION (MAX_RECURSION N).
	MaxRecursion int
}

// BoundTableRef represents a bound table reference.
type BoundTableRef struct {
	Schema        string
	TableName     string
	Alias         string
	TableDef      *catalog.TableDef
	VirtualTable  *catalog.VirtualTableDef // Set for virtual tables
	ViewDef       *catalog.ViewDef         // Set for views
	ViewQuery     *BoundSelectStmt         // Bound query for views (set when ViewDef is set)
	TableFunction *BoundTableFunctionRef   // Set for table functions (read_csv, etc.)
	CTERef        *CTEBinding              // Set for CTE references
	Subquery      *BoundSelectStmt         // Set for subqueries in FROM clause (including LATERAL)
	PivotStmt     *BoundPivotStmt          // Set for PIVOT table references
	UnpivotStmt   *BoundUnpivotStmt        // Set for UNPIVOT table references
	Columns       []*BoundColumn
	Lateral       bool // LATERAL flag (subquery can reference outer scope)
	IsCTESelfRef  bool // True if this is a self-reference within a recursive CTE's recursive part
}

// BoundTableFunctionRef represents a bound table function call.
type BoundTableFunctionRef struct {
	// Name is the function name (e.g., "read_csv", "read_json").
	Name string
	// Path is the file path for file-reading functions (single path case).
	Path string
	// Paths is a list of file paths when array syntax is used.
	// Example: read_csv(['file1.csv', 'file2.csv'])
	// When Paths is non-empty, Path is ignored.
	Paths []string
	// Options contains parsed options for the table function.
	Options map[string]any
	// Columns contains the schema determined by the table function.
	Columns []*catalog.ColumnDef
}

// BoundColumn represents a bound column reference.
type BoundColumn struct {
	Table      string // Table alias or name
	Column     string
	ColumnIdx  int
	Type       dukdb.Type
	SourceType string // "table", "subquery", "function"
}

// NewBinder creates a new Binder.
func NewBinder(cat *catalog.Catalog) *Binder {
	return &Binder{
		catalog: cat,
		scope:   newBindScope(nil),
	}
}

// WithUDFResolver sets the scalar UDF resolver and returns the binder.
func (b *Binder) WithUDFResolver(
	resolver ScalarUDFResolver,
) *Binder {
	b.udfResolver = resolver

	return b
}

func newBindScope(parent *BindScope) *BindScope {
	return &BindScope{
		parent:  parent,
		tables:  make(map[string]*BoundTableRef),
		aliases: make(map[string]string),
		params:  make(map[int]dukdb.Type),
		ctes:    make(map[string]*CTEBinding),
	}
}

// newLateralScope creates a new scope for LATERAL subqueries that inherits
// the parent scope's table bindings. This allows LATERAL subqueries to reference
// columns from tables that appear earlier in the FROM clause.
func newLateralScope(parent *BindScope) *BindScope {
	scope := &BindScope{
		parent:  parent,
		tables:  make(map[string]*BoundTableRef),
		aliases: make(map[string]string),
		params:  make(map[int]dukdb.Type),
		ctes:    make(map[string]*CTEBinding),
	}

	// Copy parent's tables into the lateral scope.
	// This makes preceding tables from the FROM clause visible to the LATERAL subquery.
	if parent != nil {
		for name, ref := range parent.tables {
			scope.tables[name] = ref
		}
		for alias, name := range parent.aliases {
			scope.aliases[alias] = name
		}
		// Also inherit CTEs from parent scope
		for name, cte := range parent.ctes {
			scope.ctes[name] = cte
		}
		// Inherit parameter count
		scope.paramCount = parent.paramCount
	}

	return scope
}

// getCTE looks up a CTE by name in this scope or parent scopes.
func (s *BindScope) getCTE(name string) *CTEBinding {
	if cte, ok := s.ctes[name]; ok {
		return cte
	}
	if s.parent != nil {
		return s.parent.getCTE(name)
	}
	return nil
}

// addCTE adds a CTE binding to the current scope.
func (s *BindScope) addCTE(cte *CTEBinding) {
	s.ctes[cte.Name] = cte
}

// Bind binds a parsed statement to the catalog.
func (b *Binder) Bind(
	stmt parser.Statement,
) (BoundStatement, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return b.bindSelect(s)
	case *parser.InsertStmt:
		return b.bindInsert(s)
	case *parser.UpdateStmt:
		return b.bindUpdate(s)
	case *parser.DeleteStmt:
		return b.bindDelete(s)
	case *parser.CreateTableStmt:
		return b.bindCreateTable(s)
	case *parser.DropTableStmt:
		return b.bindDropTable(s)
	case *parser.CreateViewStmt:
		return b.bindCreateView(s)
	case *parser.DropViewStmt:
		return b.bindDropView(s)
	case *parser.CreateIndexStmt:
		return b.bindCreateIndex(s)
	case *parser.DropIndexStmt:
		return b.bindDropIndex(s)
	case *parser.CreateSequenceStmt:
		return b.bindCreateSequence(s)
	case *parser.DropSequenceStmt:
		return b.bindDropSequence(s)
	case *parser.CreateSchemaStmt:
		return b.bindCreateSchema(s)
	case *parser.DropSchemaStmt:
		return b.bindDropSchema(s)
	case *parser.AlterTableStmt:
		return b.bindAlterTable(s)
	case *parser.BeginStmt:
		return &BoundBeginStmt{}, nil
	case *parser.CommitStmt:
		return &BoundCommitStmt{}, nil
	case *parser.RollbackStmt:
		return &BoundRollbackStmt{}, nil
	case *parser.CopyStmt:
		return b.bindCopy(s)
	case *parser.MergeStmt:
		return b.bindMerge(s)
	case *parser.PivotStmt:
		return b.bindPivot(s)
	case *parser.UnpivotStmt:
		return b.bindUnpivot(s)
	case *parser.CreateSecretStmt:
		return b.bindCreateSecret(s)
	case *parser.DropSecretStmt:
		return b.bindDropSecret(s)
	case *parser.AlterSecretStmt:
		return b.bindAlterSecret(s)
	case *parser.PragmaStmt:
		return b.bindPragma(s)
	case *parser.ExplainStmt:
		return b.bindExplain(s)
	case *parser.VacuumStmt:
		return b.bindVacuum(s)
	case *parser.AnalyzeStmt:
		return b.bindAnalyze(s)
	case *parser.CheckpointStmt:
		return b.bindCheckpoint(s)
	default:
		return nil, b.errorf("unsupported statement type: %T", stmt)
	}
}

func (*Binder) errorf(
	format string,
	args ...any,
) error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeBinder,
		Msg: fmt.Sprintf(
			"Binder Error: "+format,
			args...),
	}
}

// GetParamTypes returns the inferred parameter types after binding.
func (b *Binder) GetParamTypes() map[int]dukdb.Type {
	if b.scope == nil {
		return nil
	}

	return b.scope.params
}
