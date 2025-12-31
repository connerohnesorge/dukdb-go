package catalog

import dukdb "github.com/dukdb/dukdb-go"

// VirtualTableDef wraps a dukdb.VirtualTable to provide a TableDef-compatible interface.
// This allows virtual tables to be used in table resolution.
type VirtualTableDef struct {
	vt      dukdb.VirtualTable
	columns []*ColumnDef
}

// NewVirtualTableDef creates a VirtualTableDef from a dukdb.VirtualTable.
func NewVirtualTableDef(
	vt dukdb.VirtualTable,
) *VirtualTableDef {
	schema := vt.Schema()
	columns := make([]*ColumnDef, len(schema))
	for i, col := range schema {
		columns[i] = &ColumnDef{
			Name:     col.Name,
			Type:     col.Type,
			TypeInfo: col.TypeInfo,
			Nullable: col.Nullable,
		}
	}

	return &VirtualTableDef{
		vt:      vt,
		columns: columns,
	}
}

// ToTableDef converts the virtual table to a TableDef for use in binding.
func (v *VirtualTableDef) ToTableDef() *TableDef {
	return NewTableDef(v.vt.Name(), v.columns)
}

// VirtualTable returns the underlying dukdb.VirtualTable.
func (v *VirtualTableDef) VirtualTable() dukdb.VirtualTable {
	return v.vt
}

// Name returns the table name.
func (v *VirtualTableDef) Name() string {
	return v.vt.Name()
}

// Columns returns the column definitions.
func (v *VirtualTableDef) Columns() []*ColumnDef {
	return v.columns
}

// GetColumn returns a column by name.
func (v *VirtualTableDef) GetColumn(
	name string,
) (*ColumnDef, bool) {
	for _, col := range v.columns {
		if col.Name == name {
			return col, true
		}
	}

	return nil, false
}

// GetColumnIndex returns the index of a column by name.
func (v *VirtualTableDef) GetColumnIndex(
	name string,
) (int, bool) {
	for i, col := range v.columns {
		if col.Name == name {
			return i, true
		}
	}

	return -1, false
}

// ColumnCount returns the number of columns.
func (v *VirtualTableDef) ColumnCount() int {
	return len(v.columns)
}

// ColumnNames returns the names of all columns.
func (v *VirtualTableDef) ColumnNames() []string {
	names := make([]string, len(v.columns))
	for i, col := range v.columns {
		names[i] = col.Name
	}

	return names
}

// ColumnTypes returns the types of all columns.
func (v *VirtualTableDef) ColumnTypes() []dukdb.Type {
	types := make([]dukdb.Type, len(v.columns))
	for i, col := range v.columns {
		types[i] = col.Type
	}

	return types
}
