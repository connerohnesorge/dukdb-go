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
