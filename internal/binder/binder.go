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
	params     map[int]dukdb.Type // position -> inferred type
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
	Columns       []*BoundColumn
}

// BoundTableFunctionRef represents a bound table function call.
type BoundTableFunctionRef struct {
	// Name is the function name (e.g., "read_csv", "read_json").
	Name string
	// Path is the file path for file-reading functions.
	Path string
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
	}
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