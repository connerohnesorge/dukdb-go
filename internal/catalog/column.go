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

	// TypeInfo provides extended type information for complex types.
	// For primitive types, this may be nil and Type should be used.
	TypeInfo dukdb.TypeInfo

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
		TypeInfo:     c.TypeInfo,
		Nullable:     c.Nullable,
		DefaultValue: c.DefaultValue,
		HasDefault:   c.HasDefault,
	}
}

// GetTypeInfo returns the TypeInfo for this column.
// If TypeInfo is not set, it creates a basic TypeInfo from the Type.
func (c *ColumnDef) GetTypeInfo() dukdb.TypeInfo {
	if c.TypeInfo != nil {
		return c.TypeInfo
	}
	// Create basic TypeInfo from Type for primitive types
	info, err := dukdb.NewTypeInfo(c.Type)
	if err != nil {
		// Return a basic wrapper for unsupported types
		return &basicTypeInfo{typ: c.Type}
	}
	return info
}

// basicTypeInfo is a simple TypeInfo wrapper for types that don't have
// specialized constructors available.
type basicTypeInfo struct {
	typ dukdb.Type
}

func (b *basicTypeInfo) InternalType() dukdb.Type {
	return b.typ
}

func (b *basicTypeInfo) Details() dukdb.TypeDetails {
	return nil
}

func (b *basicTypeInfo) SQLType() string {
	return b.typ.String()
}
