package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// TableDef represents a table definition in the catalog.
type TableDef struct {
	// Name is the table name.
	Name string

	// Schema is the schema name.
	Schema string

	// Columns is the list of column definitions.
	Columns []*ColumnDef

	// PrimaryKey is the list of column indices that form the primary key.
	PrimaryKey []int

	// columnIndex maps column names to their indices for fast lookup.
	columnIndex map[string]int
}

// NewTableDef creates a new TableDef instance.
func NewTableDef(
	name string,
	columns []*ColumnDef,
) *TableDef {
	t := &TableDef{
		Name:        name,
		Columns:     columns,
		columnIndex: make(map[string]int),
	}

	for i, col := range columns {
		t.columnIndex[col.Name] = i
	}

	return t
}

// GetColumn returns a column by name.
func (t *TableDef) GetColumn(
	name string,
) (*ColumnDef, bool) {
	idx, ok := t.columnIndex[name]
	if !ok {
		return nil, false
	}

	return t.Columns[idx], true
}

// GetColumnIndex returns the index of a column by name.
func (t *TableDef) GetColumnIndex(
	name string,
) (int, bool) {
	idx, ok := t.columnIndex[name]

	return idx, ok
}

// ColumnCount returns the number of columns.
func (t *TableDef) ColumnCount() int {
	return len(t.Columns)
}

// ColumnNames returns the names of all columns.
func (t *TableDef) ColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}

	return names
}

// ColumnTypes returns the types of all columns.
func (t *TableDef) ColumnTypes() []dukdb.Type {
	types := make([]dukdb.Type, len(t.Columns))
	for i, col := range t.Columns {
		types[i] = col.Type
	}

	return types
}

// ColumnTypeInfos returns the TypeInfo for all columns.
func (t *TableDef) ColumnTypeInfos() []dukdb.TypeInfo {
	infos := make(
		[]dukdb.TypeInfo,
		len(t.Columns),
	)
	for i, col := range t.Columns {
		infos[i] = col.GetTypeInfo()
	}

	return infos
}

// SetPrimaryKey sets the primary key columns by name.
func (t *TableDef) SetPrimaryKey(
	columnNames []string,
) error {
	indices := make([]int, len(columnNames))
	for i, name := range columnNames {
		idx, ok := t.columnIndex[name]
		if !ok {
			return dukdb.ErrColumnNotFound
		}
		indices[i] = idx
	}
	t.PrimaryKey = indices

	return nil
}

// HasPrimaryKey returns whether the table has a primary key.
func (t *TableDef) HasPrimaryKey() bool {
	return len(t.PrimaryKey) > 0
}

// Clone creates a deep copy of the TableDef.
func (t *TableDef) Clone() *TableDef {
	columns := make([]*ColumnDef, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = col.Clone()
	}

	newTable := NewTableDef(t.Name, columns)
	newTable.Schema = t.Schema

	if len(t.PrimaryKey) > 0 {
		newTable.PrimaryKey = make(
			[]int,
			len(t.PrimaryKey),
		)
		copy(newTable.PrimaryKey, t.PrimaryKey)
	}

	return newTable
}
