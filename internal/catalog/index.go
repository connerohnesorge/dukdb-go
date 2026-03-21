package catalog

// IndexDef represents an index definition in the catalog.
type IndexDef struct {
	// Name is the index name.
	Name string

	// Schema is the schema name.
	Schema string

	// Table is the name of the table being indexed.
	Table string

	// Columns is the list of column names included in the index.
	Columns []string

	// IsUnique indicates whether this is a unique index.
	IsUnique bool

	// IsPrimary indicates whether this is a primary key index.
	IsPrimary bool

	// Comment is an optional user-defined comment for the index.
	Comment string
}

// NewIndexDef creates a new IndexDef instance.
func NewIndexDef(name, schema, table string, columns []string, isUnique bool) *IndexDef {
	return &IndexDef{
		Name:      name,
		Schema:    schema,
		Table:     table,
		Columns:   columns,
		IsUnique:  isUnique,
		IsPrimary: false,
	}
}

// Clone creates a deep copy of the index definition.
func (i *IndexDef) Clone() *IndexDef {
	columns := make([]string, len(i.Columns))
	copy(columns, i.Columns)

	return &IndexDef{
		Name:      i.Name,
		Schema:    i.Schema,
		Table:     i.Table,
		Columns:   columns,
		IsUnique:  i.IsUnique,
		IsPrimary: i.IsPrimary,
	}
}

// GetName returns the index name.
// This method satisfies the optimizer.IndexDef interface.
func (i *IndexDef) GetName() string {
	return i.Name
}

// GetTable returns the table name the index is on.
// This method satisfies the optimizer.IndexDef interface.
func (i *IndexDef) GetTable() string {
	return i.Table
}

// GetColumns returns the column names included in the index.
// This method satisfies the optimizer.IndexDef interface.
func (i *IndexDef) GetColumns() []string {
	return i.Columns
}

// GetIsUnique returns true if this is a unique index.
// This method satisfies the optimizer.IndexDef interface.
func (i *IndexDef) GetIsUnique() bool {
	return i.IsUnique
}
