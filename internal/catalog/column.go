package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// ColumnDef represents a column definition in a table.
type ColumnDef struct {
	// Name is the column name.
	Name string

	// Type is the column data type.
	Type dukdb.Type

	// Nullable indicates whether the column allows NULL values.
	Nullable bool

	// DefaultValue is the default value for the column (if any).
	DefaultValue any

	// HasDefault indicates whether a default value is set.
	HasDefault bool
}

// NewColumnDef creates a new ColumnDef instance.
func NewColumnDef(
	name string,
	typ dukdb.Type,
) *ColumnDef {
	return &ColumnDef{
		Name:     name,
		Type:     typ,
		Nullable: true, // Columns are nullable by default
	}
}

// WithNullable sets the nullable flag and returns the ColumnDef.
func (c *ColumnDef) WithNullable(
	nullable bool,
) *ColumnDef {
	c.Nullable = nullable
	return c
}

// WithDefault sets the default value and returns the ColumnDef.
func (c *ColumnDef) WithDefault(
	value any,
) *ColumnDef {
	c.DefaultValue = value
	c.HasDefault = true
	return c
}

// Clone creates a deep copy of the ColumnDef.
func (c *ColumnDef) Clone() *ColumnDef {
	return &ColumnDef{
		Name:         c.Name,
		Type:         c.Type,
		Nullable:     c.Nullable,
		DefaultValue: c.DefaultValue,
		HasDefault:   c.HasDefault,
	}
}
