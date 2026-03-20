package catalog

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/optimizer"
)

// normalizeColumnKey converts a column name to lowercase for case-insensitive lookup.
func normalizeColumnKey(name string) string {
	return strings.ToLower(name)
}

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

	// Constraints holds UNIQUE and CHECK constraint definitions.
	// Each element is a *UniqueConstraintDef or *CheckConstraintDef.
	Constraints []any

	// Statistics contains optimizer statistics for this table.
	// May be nil if table has not been analyzed.
	Statistics *optimizer.TableStatistics
}

// NewTableDef creates a new TableDef instance.
// Column names are stored in their original case but indexed case-insensitively.
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
		t.columnIndex[normalizeColumnKey(col.Name)] = i
	}

	return t
}

// GetColumn returns a column by name (case-insensitive).
func (t *TableDef) GetColumn(
	name string,
) (*ColumnDef, bool) {
	idx, ok := t.columnIndex[normalizeColumnKey(name)]
	if !ok {
		return nil, false
	}

	return t.Columns[idx], true
}

// GetColumnIndex returns the index of a column by name (case-insensitive).
func (t *TableDef) GetColumnIndex(
	name string,
) (int, bool) {
	idx, ok := t.columnIndex[normalizeColumnKey(name)]

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

// SetPrimaryKey sets the primary key columns by name (case-insensitive).
func (t *TableDef) SetPrimaryKey(
	columnNames []string,
) error {
	indices := make([]int, len(columnNames))
	for i, name := range columnNames {
		idx, ok := t.columnIndex[normalizeColumnKey(name)]
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

	// Clone constraints
	if len(t.Constraints) > 0 {
		newTable.Constraints = make([]any, len(t.Constraints))
		for i, c := range t.Constraints {
			switch ct := c.(type) {
			case *UniqueConstraintDef:
				newTable.Constraints[i] = ct.Clone()
			case *CheckConstraintDef:
				newTable.Constraints[i] = ct.Clone()
			default:
				newTable.Constraints[i] = c
			}
		}
	}

	// Clone statistics if present
	if t.Statistics != nil {
		// For now, just assign pointer since TableStatistics is read-heavy
		// and rarely modified after creation
		newTable.Statistics = t.Statistics
	}

	return newTable
}

// RenameColumn renames a column and updates the column index.
// Column names are case-insensitive for lookup; the new name is stored in its original case.
func (t *TableDef) RenameColumn(oldName, newName string) error {
	oldKey := normalizeColumnKey(oldName)
	newKey := normalizeColumnKey(newName)

	idx, ok := t.columnIndex[oldKey]
	if !ok {
		return dukdb.ErrColumnNotFound
	}

	// Check if new name already exists (unless renaming to same case-insensitive name)
	if oldKey != newKey {
		if _, exists := t.columnIndex[newKey]; exists {
			return &dukdb.Error{
				Type: dukdb.ErrorTypeCatalog,
				Msg:  "column already exists: " + newName,
			}
		}
	}

	// Update column name
	t.Columns[idx].Name = newName

	// Update columnIndex map
	delete(t.columnIndex, oldKey)
	t.columnIndex[newKey] = idx

	return nil
}

// DropColumn removes a column and updates the column index.
// Column name is case-insensitive.
func (t *TableDef) DropColumn(name string) error {
	idx, ok := t.columnIndex[normalizeColumnKey(name)]
	if !ok {
		return dukdb.ErrColumnNotFound
	}

	// Cannot drop the last column
	if len(t.Columns) == 1 {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "cannot drop last column of table",
		}
	}

	// Remove column from slice
	t.Columns = append(t.Columns[:idx], t.Columns[idx+1:]...)

	// Rebuild columnIndex map with normalized keys
	t.columnIndex = make(map[string]int)
	for i, col := range t.Columns {
		t.columnIndex[normalizeColumnKey(col.Name)] = i
	}

	// Update primary key indices if needed
	if len(t.PrimaryKey) > 0 {
		newPK := make([]int, 0, len(t.PrimaryKey))
		for _, pkIdx := range t.PrimaryKey {
			if pkIdx < idx {
				// Index unchanged
				newPK = append(newPK, pkIdx)
			} else if pkIdx > idx {
				// Index shifted down by 1
				newPK = append(newPK, pkIdx-1)
			}
			// If pkIdx == idx, skip (dropping a PK column)
		}
		t.PrimaryKey = newPK
	}

	return nil
}

// AddColumn adds a column and updates the column index.
// Column names are case-insensitive for duplicate check; the original case is preserved.
func (t *TableDef) AddColumn(col *ColumnDef) error {
	// Check if column already exists (case-insensitive)
	key := normalizeColumnKey(col.Name)
	if _, exists := t.columnIndex[key]; exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "column already exists: " + col.Name,
		}
	}

	// Add column
	t.Columns = append(t.Columns, col)
	t.columnIndex[key] = len(t.Columns) - 1

	return nil
}

// GetStatistics returns the table's optimizer statistics.
// Returns nil if the table has not been analyzed.
// This method satisfies the optimizer.TableInfo interface.
func (t *TableDef) GetStatistics() *optimizer.TableStatistics {
	return t.Statistics
}

// GetColumns returns the table's columns as optimizer.ColumnInfo interfaces.
// This method satisfies the optimizer.TableInfo interface.
func (t *TableDef) GetColumns() []optimizer.ColumnInfo {
	result := make([]optimizer.ColumnInfo, len(t.Columns))
	for i, col := range t.Columns {
		result[i] = col
	}
	return result
}

// GetColumnInfo returns a column by name as an optimizer.ColumnInfo interface.
// This method satisfies the optimizer.TableInfo interface.
func (t *TableDef) GetColumnInfo(name string) (optimizer.ColumnInfo, bool) {
	col, ok := t.GetColumn(name)
	if !ok {
		return nil, false
	}
	return col, true
}
