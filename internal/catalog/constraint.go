package catalog

// ConstraintType identifies the kind of table constraint.
type ConstraintType int

const (
	// ConstraintUnique represents a UNIQUE constraint.
	ConstraintUnique ConstraintType = iota
	// ConstraintCheck represents a CHECK constraint.
	ConstraintCheck
	// ConstraintForeignKey represents a FOREIGN KEY constraint (not enforced yet).
	ConstraintForeignKey
)

// UniqueConstraintDef represents a UNIQUE constraint on one or more columns.
type UniqueConstraintDef struct {
	Name    string   // Optional constraint name
	Columns []string // Column names that must be unique together
}

// Clone creates a deep copy of the UniqueConstraintDef.
func (u *UniqueConstraintDef) Clone() *UniqueConstraintDef {
	cols := make([]string, len(u.Columns))
	copy(cols, u.Columns)
	return &UniqueConstraintDef{
		Name:    u.Name,
		Columns: cols,
	}
}

// CheckConstraintDef represents a CHECK constraint with a boolean expression.
type CheckConstraintDef struct {
	Name       string // Optional constraint name
	Expression string // Raw SQL expression text
}

// Clone creates a deep copy of the CheckConstraintDef.
func (c *CheckConstraintDef) Clone() *CheckConstraintDef {
	return &CheckConstraintDef{
		Name:       c.Name,
		Expression: c.Expression,
	}
}

// ForeignKeyAction represents the action taken on DELETE or UPDATE of referenced rows.
type ForeignKeyAction int

const (
	// FKActionNoAction means no action is taken (default).
	FKActionNoAction ForeignKeyAction = iota
	// FKActionRestrict means the operation is rejected if referenced rows exist.
	FKActionRestrict
)

// ForeignKeyConstraintDef represents a FOREIGN KEY constraint.
type ForeignKeyConstraintDef struct {
	Name       string           // Optional constraint name
	Columns    []string         // Child table column names
	RefTable   string           // Referenced parent table name
	RefColumns []string         // Referenced parent column names
	OnDelete   ForeignKeyAction // Action on DELETE of parent row
	OnUpdate   ForeignKeyAction // Action on UPDATE of parent row
}

// Clone creates a deep copy of the ForeignKeyConstraintDef.
func (f *ForeignKeyConstraintDef) Clone() *ForeignKeyConstraintDef {
	cols := make([]string, len(f.Columns))
	copy(cols, f.Columns)
	refCols := make([]string, len(f.RefColumns))
	copy(refCols, f.RefColumns)
	return &ForeignKeyConstraintDef{
		Name:       f.Name,
		Columns:    cols,
		RefTable:   f.RefTable,
		RefColumns: refCols,
		OnDelete:   f.OnDelete,
		OnUpdate:   f.OnUpdate,
	}
}
