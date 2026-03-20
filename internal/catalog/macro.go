package catalog

// MacroType distinguishes scalar macros from table macros.
type MacroType int

const (
	// MacroTypeScalar represents a scalar macro that returns a single value.
	MacroTypeScalar MacroType = iota
	// MacroTypeTable represents a table macro that returns a table.
	MacroTypeTable
)

// MacroParam represents a parameter in a macro definition.
type MacroParam struct {
	Name        string
	DefaultExpr string
	HasDefault  bool
}

// MacroDef represents a macro definition in the catalog.
type MacroDef struct {
	Name   string
	Schema string
	Params []MacroParam
	Type   MacroType
	Body   string // Raw SQL expression body for scalar macros
	Query  string // Raw SQL query for table macros
}
